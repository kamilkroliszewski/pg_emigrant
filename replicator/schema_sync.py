"""Schema introspection and synchronization between source and target."""

from __future__ import annotations

import asyncpg

from replicator.utils import get_logger, qi, qt

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Introspection queries
# ---------------------------------------------------------------------------

_TABLES_SQL = """
SELECT c.relname AS table_name, n.nspname AS schema_name
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY($1::text[])
  AND c.relkind = 'r'
ORDER BY n.nspname, c.relname;
"""

_COLUMNS_SQL = """
SELECT
    a.attname          AS column_name,
    a.attnum           AS ordinal,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
    a.attnotnull       AS not_null,
    pg_get_expr(d.adbin, d.adrelid) AS column_default
FROM pg_attribute a
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = $1::regclass
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;
"""

_INDEXES_SQL = """
SELECT
    i.relname               AS index_name,
    pg_get_indexdef(i.oid)  AS index_def,
    ix.indisprimary         AS is_primary,
    ix.indisunique          AS is_unique
FROM pg_index ix
JOIN pg_class i ON i.oid = ix.indexrelid
WHERE ix.indrelid = $1::regclass
ORDER BY i.relname;
"""

_CONSTRAINTS_SQL = """
SELECT
    con.conname     AS constraint_name,
    con.contype     AS constraint_type,
    pg_get_constraintdef(con.oid) AS constraint_def
FROM pg_constraint con
WHERE con.conrelid = $1::regclass
ORDER BY con.conname;
"""

_SEQUENCES_SQL = """
SELECT
    n.nspname AS schema_name,
    c.relname AS sequence_name,
    pg_get_serial_sequence(quote_ident(n2.nspname) || '.' || quote_ident(t.relname), a.attname) AS owned_by
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_depend d ON d.objid = c.oid AND d.deptype = 'a'
LEFT JOIN pg_class t ON t.oid = d.refobjid
LEFT JOIN pg_namespace n2 ON n2.oid = t.relnamespace
LEFT JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
WHERE n.nspname = ANY($1::text[])
  AND c.relkind = 'S'
ORDER BY n.nspname, c.relname;
"""

_SEQUENCE_STATE_SQL = """
SELECT last_value, is_called FROM {seq};
"""

_ENUM_TYPES_SQL = """
SELECT
    n.nspname                                              AS schema_name,
    t.typname                                              AS type_name,
    array_agg(e.enumlabel ORDER BY e.enumsortorder)        AS labels
FROM pg_type t
JOIN pg_namespace n ON n.oid = t.typnamespace
JOIN pg_enum e ON e.enumtypid = t.oid
WHERE n.nspname = ANY($1::text[])
  AND t.typtype = 'e'
GROUP BY n.nspname, t.typname
ORDER BY n.nspname, t.typname;
"""


# ---------------------------------------------------------------------------
# Data classes (plain dicts for simplicity)
# ---------------------------------------------------------------------------

async def get_tables(
    conn: asyncpg.Connection, schemas: list[str]
) -> list[dict]:
    """Return a list of tables in the given schemas."""
    return [dict(r) for r in await conn.fetch(_TABLES_SQL, schemas)]


async def get_columns(
    conn: asyncpg.Connection, fqn: str
) -> list[dict]:
    """Return columns for a fully-qualified table."""
    return [dict(r) for r in await conn.fetch(_COLUMNS_SQL, fqn)]


async def get_indexes(
    conn: asyncpg.Connection, fqn: str
) -> list[dict]:
    """Return indexes (including primary keys) for a table."""
    return [dict(r) for r in await conn.fetch(_INDEXES_SQL, fqn)]


async def get_constraints(
    conn: asyncpg.Connection, fqn: str
) -> list[dict]:
    """Return constraints for a table."""
    return [dict(r) for r in await conn.fetch(_CONSTRAINTS_SQL, fqn)]


async def get_sequences(
    conn: asyncpg.Connection, schemas: list[str]
) -> list[dict]:
    """Return sequences in the given schemas."""
    return [dict(r) for r in await conn.fetch(_SEQUENCES_SQL, schemas)]


async def get_enum_types(
    conn: asyncpg.Connection, schemas: list[str]
) -> list[dict]:
    """Return enum types in the given schemas with their ordered label lists."""
    return [dict(r) for r in await conn.fetch(_ENUM_TYPES_SQL, schemas)]


async def get_sequence_value(
    conn: asyncpg.Connection, schema: str, name: str
) -> dict:
    fqn = qt(schema, name)
    row = await conn.fetchrow(f"SELECT last_value, is_called FROM {fqn}")
    return dict(row)


# ---------------------------------------------------------------------------
# DDL generation helpers
# ---------------------------------------------------------------------------

def _is_nextval_default(default: str | None) -> bool:
    """Return True when the column default is a sequence nextval() call."""
    return default is not None and default.startswith("nextval(")


async def _generate_create_table_ddl(
    conn: asyncpg.Connection, schema: str, table: str
) -> str:
    """Generate a CREATE TABLE statement by introspecting the source.

    Serial / sequence-backed columns are rendered as
    ``GENERATED BY DEFAULT AS IDENTITY`` so that no named sequences need to
    exist on the target beforehand.
    """
    fqn = f"{qi(schema)}.{qi(table)}"
    columns = await get_columns(conn, fqn)

    col_defs = []
    for c in columns:
        if _is_nextval_default(c["column_default"]):
            # Replace SERIAL-style column with SQL-standard identity column.
            # GENERATED BY DEFAULT allows replication to supply explicit values.
            col_defs.append(
                f"{qi(c['column_name'])} {c['data_type']} GENERATED BY DEFAULT AS IDENTITY"
            )
        else:
            parts = [qi(c["column_name"]), c["data_type"]]
            if c["not_null"]:
                parts.append("NOT NULL")
            if c["column_default"] is not None:
                parts.append(f"DEFAULT {c['column_default']}")
            col_defs.append(" ".join(parts))

    ddl = f"CREATE TABLE IF NOT EXISTS {fqn} (\n  " + ",\n  ".join(col_defs) + "\n);"
    return ddl


async def _generate_constraint_ddl(
    conn: asyncpg.Connection, schema: str, table: str
) -> list[str]:
    """Generate ALTER TABLE ADD CONSTRAINT statements."""
    fqn = f"{qi(schema)}.{qi(table)}"
    constraints = await get_constraints(conn, fqn)
    stmts: list[str] = []
    for c in constraints:
        stmt = (
            f"ALTER TABLE {fqn} ADD CONSTRAINT {qi(c['constraint_name'])} "
            f"{c['constraint_def']};"
        )
        stmts.append(stmt)
    return stmts


async def _generate_index_ddl(
    conn: asyncpg.Connection, schema: str, table: str
) -> list[str]:
    """Return CREATE INDEX statements (excluding primary-key indexes)."""
    fqn = f"{qi(schema)}.{qi(table)}"
    indexes = await get_indexes(conn, fqn)
    return [
        idx["index_def"] + ";"
        for idx in indexes
        if not idx["is_primary"]
    ]


async def generate_full_table_ddl(
    conn: asyncpg.Connection, schema: str, table: str
) -> str:
    """Generate complete DDL to recreate a table: CREATE TABLE + constraints + indexes."""
    parts = [await _generate_create_table_ddl(conn, schema, table)]
    parts.extend(await _generate_constraint_ddl(conn, schema, table))
    parts.extend(await _generate_index_ddl(conn, schema, table))
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Synchronization logic
# ---------------------------------------------------------------------------

async def ensure_schema_exists(conn: asyncpg.Connection, schema: str) -> None:
    """Create schema on target if it doesn't exist."""
    await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {qi(schema)};")
    log.debug("Ensured schema %s exists", schema)


async def sync_table(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schema: str,
    table: str,
) -> None:
    """Synchronize a single table's structure from source to target.

    Creates the table if missing.  If the table exists, adds any missing columns.
    Then synchronizes constraints and indexes.
    """
    fqn = f"{qi(schema)}.{qi(table)}"
    log.info("Syncing table %s.%s", schema, table)

    # Check if table exists on target
    exists = await target_conn.fetchval(
        "SELECT EXISTS ("
        "  SELECT 1 FROM information_schema.tables"
        "  WHERE table_schema = $1 AND table_name = $2"
        ")",
        schema,
        table,
    )

    if not exists:
        ddl = await _generate_create_table_ddl(source_conn, schema, table)
        log.info("Creating table %s", fqn)
        await target_conn.execute(ddl)
    else:
        # Add missing columns
        src_cols = await get_columns(source_conn, fqn)
        tgt_cols = await get_columns(target_conn, fqn)
        tgt_col_names = {c["column_name"] for c in tgt_cols}

        for col in src_cols:
            if col["column_name"] not in tgt_col_names:
                if _is_nextval_default(col["column_default"]):
                    col_def = f"{col['data_type']} GENERATED BY DEFAULT AS IDENTITY"
                else:
                    parts = [col["data_type"]]
                    if col["not_null"] and col["column_default"] is not None:
                        parts.append(f"DEFAULT {col['column_default']}")
                    if col["not_null"]:
                        parts.append("NOT NULL")
                    col_def = " ".join(parts)
                alter = (
                    f"ALTER TABLE {fqn} ADD COLUMN "
                    f"{qi(col['column_name'])} {col_def};"
                )
                log.info("Adding column %s to %s", col["column_name"], fqn)
                await target_conn.execute(alter)

    # Synchronize constraints
    await _sync_constraints(source_conn, target_conn, schema, table)

    # Synchronize indexes
    await _sync_indexes(source_conn, target_conn, schema, table)


async def _sync_constraints(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schema: str,
    table: str,
) -> None:
    fqn = f"{qi(schema)}.{qi(table)}"
    src_constraints = await get_constraints(source_conn, fqn)
    tgt_constraints = await get_constraints(target_conn, fqn)
    tgt_names = {c["constraint_name"] for c in tgt_constraints}

    for c in src_constraints:
        if c["constraint_name"] not in tgt_names:
            stmt = (
                f"ALTER TABLE {fqn} ADD CONSTRAINT {qi(c['constraint_name'])} "
                f"{c['constraint_def']};"
            )
            log.info("Adding constraint %s on %s", c["constraint_name"], fqn)
            try:
                await target_conn.execute(stmt)
            except asyncpg.DuplicateObjectError:
                log.debug("Constraint %s already exists", c["constraint_name"])


async def _sync_indexes(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schema: str,
    table: str,
    *,
    deferred: bool = False,
) -> None:
    """Synchronize indexes for a single table.

    deferred=False (default, schema-sync phase): creates only UNIQUE non-PK
    indexes, which are needed to back constraints and FK references before data
    is loaded.

    deferred=True (post-COPY phase): creates only non-unique, non-PK indexes.
    Building them after COPY is significantly faster because PostgreSQL can do
    a single sequential scan instead of incrementally updating each index on
    every inserted row.
    """
    fqn = f"{qi(schema)}.{qi(table)}"
    src_indexes = await get_indexes(source_conn, fqn)
    tgt_indexes = await get_indexes(target_conn, fqn)
    tgt_names = {i["index_name"] for i in tgt_indexes}

    for idx in src_indexes:
        if idx["is_primary"] or idx["index_name"] in tgt_names:
            continue
        is_non_unique = not idx["is_unique"]
        # deferred phase  → only non-unique indexes
        # schema-sync phase → only unique indexes
        if deferred and not is_non_unique:
            continue
        if not deferred and is_non_unique:
            continue
        stmt = idx["index_def"] + ";"
        log.info("Creating index %s on %s", idx["index_name"], fqn)
        try:
            await target_conn.execute(stmt)
        except asyncpg.DuplicateObjectError:
            log.debug("Index %s already exists", idx["index_name"])


async def sync_sequence(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schema: str,
    seq_name: str,
) -> None:
    """Create sequence on target if it doesn't exist, and copy its current value."""
    fqn = qt(schema, seq_name)
    exists = await target_conn.fetchval(
        "SELECT EXISTS ("
        "  SELECT 1 FROM information_schema.sequences"
        "  WHERE sequence_schema = $1 AND sequence_name = $2"
        ")",
        schema,
        seq_name,
    )
    if not exists:
        # Grab full sequence definition from source
        meta = await source_conn.fetchrow(
            "SELECT * FROM pg_sequences WHERE schemaname = $1 AND sequencename = $2",
            schema,
            seq_name,
        )
        if meta is None:
            log.warning("Sequence %s not found on source", fqn)
            return

        create_sql = (
            f"CREATE SEQUENCE IF NOT EXISTS {fqn}"
            f" INCREMENT BY {meta['increment_by']}"
            f" MINVALUE {meta['min_value']}"
            f" MAXVALUE {meta['max_value']}"
            f" START WITH {meta['start_value']}"
            f" CACHE {meta['cache_size']}"
            f"{' CYCLE' if meta['cycle'] else ' NO CYCLE'};"
        )
        log.info("Creating sequence %s", fqn)
        await target_conn.execute(create_sql)

    # Copy current value
    src_val = await get_sequence_value(source_conn, schema, seq_name)
    if src_val["is_called"]:
        await target_conn.execute(
            f"SELECT setval('{schema}.{seq_name}', $1, true);",
            src_val["last_value"],
        )
    log.debug("Sequence %s synced to %s", fqn, src_val["last_value"])


async def sync_enum_types(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> None:
    """Create missing enum types on the target before tables are created."""
    rows = await source_conn.fetch(_ENUM_TYPES_SQL, schemas)
    for row in rows:
        schema = row["schema_name"]
        type_name = row["type_name"]
        labels: list[str] = list(row["labels"])

        exists = await target_conn.fetchval(
            "SELECT EXISTS ("
            "  SELECT 1 FROM pg_type t"
            "  JOIN pg_namespace n ON n.oid = t.typnamespace"
            "  WHERE n.nspname = $1 AND t.typname = $2 AND t.typtype = 'e'"
            ")",
            schema,
            type_name,
        )
        if not exists:
            labels_sql = ", ".join(f"'{label}'" for label in labels)
            ddl = f"CREATE TYPE {qi(schema)}.{qi(type_name)} AS ENUM ({labels_sql});"
            log.info("Creating enum type %s.%s", schema, type_name)
            await target_conn.execute(ddl)
        else:
            # Ensure all source labels exist on target (enums can only grow)
            tgt_labels: list[str] = list(
                await target_conn.fetchval(
                    "SELECT array_agg(enumlabel ORDER BY enumsortorder) "
                    "FROM pg_enum e "
                    "JOIN pg_type t ON t.oid = e.enumtypid "
                    "JOIN pg_namespace n ON n.oid = t.typnamespace "
                    "WHERE n.nspname = $1 AND t.typname = $2",
                    schema,
                    type_name,
                )
                or []
            )
            tgt_label_set = set(tgt_labels)
            for label in labels:
                if label not in tgt_label_set:
                    ddl = (
                        f"ALTER TYPE {qi(schema)}.{qi(type_name)} "
                        f"ADD VALUE IF NOT EXISTS '{label}';"
                    )
                    log.info("Adding enum value '%s' to %s.%s", label, schema, type_name)
                    await target_conn.execute(ddl)


async def sync_schemas(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> None:
    """Full schema synchronization: schemas → enums → sequences → tables → FKs."""
    # Ensure target schemas exist
    for s in schemas:
        await ensure_schema_exists(target_conn, s)

    # Sync enum types BEFORE tables so columns with enum types can be created
    await sync_enum_types(source_conn, target_conn, schemas)

    # Sync sequences FIRST so that table DDL with DEFAULT nextval(...) succeeds
    sequences = await get_sequences(source_conn, schemas)
    for seq in sequences:
        await sync_sequence(
            source_conn, target_conn,
            seq["schema_name"], seq["sequence_name"],
        )

    # Sync tables (without foreign keys first to avoid dependency issues)
    tables = await get_tables(source_conn, schemas)
    for t in tables:
        # First pass: create table + columns + PKs + unique constraints
        await _sync_table_structure(source_conn, target_conn, t["schema_name"], t["table_name"])

    # Second pass: foreign keys
    for t in tables:
        await _sync_foreign_keys(source_conn, target_conn, t["schema_name"], t["table_name"])

    log.info("Schema synchronization complete for schemas: %s", schemas)


async def _sync_table_structure(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schema: str,
    table: str,
) -> None:
    """Create/update table + columns + non-FK constraints + indexes."""
    fqn = f"{qi(schema)}.{qi(table)}"

    exists = await target_conn.fetchval(
        "SELECT EXISTS ("
        "  SELECT 1 FROM information_schema.tables"
        "  WHERE table_schema = $1 AND table_name = $2"
        ")",
        schema,
        table,
    )

    if not exists:
        ddl = await _generate_create_table_ddl(source_conn, schema, table)
        log.info("Creating table %s", fqn)
        await target_conn.execute(ddl)
    else:
        # Add missing columns
        src_cols = await get_columns(source_conn, fqn)
        tgt_cols = await get_columns(target_conn, fqn)
        tgt_col_names = {c["column_name"] for c in tgt_cols}

        for col in src_cols:
            if col["column_name"] not in tgt_col_names:
                if _is_nextval_default(col["column_default"]):
                    col_def = f"{col['data_type']} GENERATED BY DEFAULT AS IDENTITY"
                else:
                    parts = [col["data_type"]]
                    if col["not_null"] and col["column_default"] is not None:
                        parts.append(f"DEFAULT {col['column_default']}")
                    if col["not_null"]:
                        parts.append("NOT NULL")
                    col_def = " ".join(parts)
                alter = (
                    f"ALTER TABLE {fqn} ADD COLUMN "
                    f"{qi(col['column_name'])} {col_def};"
                )
                log.info("Adding column %s to %s", col["column_name"], fqn)
                await target_conn.execute(alter)

    # Non-FK constraints (PK, unique, check)
    src_constraints = await get_constraints(source_conn, fqn)
    tgt_constraints = await get_constraints(target_conn, fqn)
    tgt_names = {c["constraint_name"] for c in tgt_constraints}

    for c in src_constraints:
        is_fk = str(c["constraint_type"]).strip() == "f" or "FOREIGN KEY" in str(c["constraint_def"] or "")
        if is_fk:  # skip FK for now
            continue
        if c["constraint_name"] not in tgt_names:
            stmt = (
                f"ALTER TABLE {fqn} ADD CONSTRAINT {qi(c['constraint_name'])} "
                f"{c['constraint_def']};"
            )
            log.info("Adding constraint %s on %s", c["constraint_name"], fqn)
            try:
                await target_conn.execute(stmt)
            except asyncpg.DuplicateObjectError:
                pass

    # Unique indexes only — non-unique indexes are deferred until after COPY
    await _sync_indexes(source_conn, target_conn, schema, table, deferred=False)


_OBJECT_OWNERS_SQL = """
SELECT
    n.nspname   AS schema_name,
    c.relname   AS object_name,
    CASE c.relkind
        WHEN 'r' THEN 'table'
        WHEN 'S' THEN 'sequence'
    END         AS kind,
    r.rolname   AS owner
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_roles r     ON r.oid = c.relowner
WHERE n.nspname = ANY($1::text[])
  AND c.relkind IN ('r', 'S')
ORDER BY n.nspname, c.relkind, c.relname;
"""

_SCHEMA_OWNERS_SQL = """
SELECT n.nspname AS schema_name, r.rolname AS owner
FROM pg_namespace n
JOIN pg_roles r ON r.oid = n.nspowner
WHERE n.nspname = ANY($1::text[])
ORDER BY n.nspname;
"""


async def get_object_owners(
    conn: asyncpg.Connection, schemas: list[str]
) -> list[dict]:
    """Return owner metadata for tables, sequences, and schemas in *schemas*."""
    rows = [dict(r) for r in await conn.fetch(_OBJECT_OWNERS_SQL, schemas)]
    schema_rows = [
        {"schema_name": r["schema_name"], "object_name": r["schema_name"],
         "kind": "schema", "owner": r["owner"]}
        for r in await conn.fetch(_SCHEMA_OWNERS_SQL, schemas)
    ]
    return schema_rows + rows


async def sync_ownership(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> int:
    """Synchronize ownership of tables, sequences, and schemas from source to target.

    Emits ``ALTER TABLE/SEQUENCE/SCHEMA ... OWNER TO`` for every object whose
    owner on the target differs from the source.  Roles that do not exist on the
    target are skipped with a warning — create them manually beforehand if needed.

    Returns the number of ownership changes applied.
    """
    src_owners = {
        (r["schema_name"], r["object_name"], r["kind"]): r["owner"]
        for r in await get_object_owners(source_conn, schemas)
    }
    tgt_owners = {
        (r["schema_name"], r["object_name"], r["kind"]): r["owner"]
        for r in await get_object_owners(target_conn, schemas)
    }

    # Pre-fetch roles that exist on target to avoid erroring on missing roles
    existing_roles = {
        r["rolname"]
        for r in await target_conn.fetch("SELECT rolname FROM pg_roles")
    }

    applied = 0
    for (schema, obj, kind), src_owner in src_owners.items():
        tgt_owner = tgt_owners.get((schema, obj, kind))
        if tgt_owner == src_owner:
            continue
        if src_owner not in existing_roles:
            log.warning(
                "Skipping ownership of %s %s.%s — role '%s' does not exist on target",
                kind, schema, obj, src_owner,
            )
            continue
        if kind == "table":
            stmt = f"ALTER TABLE {qi(schema)}.{qi(obj)} OWNER TO {qi(src_owner)};"
        elif kind == "sequence":
            stmt = f"ALTER SEQUENCE {qi(schema)}.{qi(obj)} OWNER TO {qi(src_owner)};"
        else:  # schema
            stmt = f"ALTER SCHEMA {qi(obj)} OWNER TO {qi(src_owner)};"
        try:
            await target_conn.execute(stmt)
            log.info("Set owner of %s %s.%s to %s", kind, schema, obj, src_owner)
            applied += 1
        except Exception as exc:
            log.error("Failed to set owner of %s %s.%s — %s", kind, schema, obj, exc)

    return applied


async def sync_deferred_indexes(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> None:
    """Create non-unique indexes for all tables in *schemas*.

    Called after the initial data COPY so that PostgreSQL builds each index in
    a single pass rather than updating it row-by-row during bulk insert.
    PK and UNIQUE indexes are already present from the schema-sync phase.
    """
    tables = await get_tables(source_conn, schemas)
    for t in tables:
        await _sync_indexes(
            source_conn, target_conn,
            t["schema_name"], t["table_name"],
            deferred=True,
        )
    log.info("Deferred index creation complete for schemas: %s", schemas)


async def _sync_foreign_keys(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schema: str,
    table: str,
) -> None:
    """Add missing foreign keys (second pass after all tables exist)."""
    fqn = f"{qi(schema)}.{qi(table)}"
    src_constraints = await get_constraints(source_conn, fqn)
    tgt_constraints = await get_constraints(target_conn, fqn)
    tgt_names = {c["constraint_name"] for c in tgt_constraints}

    for c in src_constraints:
        is_fk = str(c["constraint_type"]).strip() == "f" or "FOREIGN KEY" in str(c["constraint_def"] or "")
        if not is_fk:
            continue
        if c["constraint_name"] not in tgt_names:
            stmt = (
                f"ALTER TABLE {fqn} ADD CONSTRAINT {qi(c['constraint_name'])} "
                f"{c['constraint_def']};"
            )
            log.info("Adding FK %s on %s", c["constraint_name"], fqn)
            try:
                await target_conn.execute(stmt)
            except asyncpg.DuplicateObjectError:
                pass
