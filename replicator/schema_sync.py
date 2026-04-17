"""Schema introspection and synchronization between source and target."""

from __future__ import annotations

import asyncpg

from replicator.utils import console, get_logger, qi, qt

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
    pg_get_expr(d.adbin, d.adrelid) AS column_default,
    (a.attgenerated = 's') AS is_generated
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

_FUNCTIONS_SQL = """
SELECT
    n.nspname                        AS schema_name,
    p.proname                        AS func_name,
    pg_get_functiondef(p.oid)        AS func_def,
    p.oid                            AS func_oid
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = ANY($1::text[])
  AND p.prokind IN ('f', 'p')
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.classid = 'pg_proc'::regclass
        AND d.objid = p.oid
        AND d.deptype = 'e'
  )
ORDER BY n.nspname, p.proname;
"""

_TRIGGERS_SQL = """
SELECT
    n.nspname                                          AS schema_name,
    c.relname                                          AS table_name,
    t.tgname                                           AS trigger_name,
    pg_get_triggerdef(t.oid)                           AS trigger_def
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY($1::text[])
  AND NOT t.tgisinternal
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.classid = 'pg_trigger'::regclass
        AND d.objid = t.oid
        AND d.deptype = 'e'
  )
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.classid = 'pg_proc'::regclass
        AND d.objid = t.tgfoid
        AND d.deptype = 'e'
  )
ORDER BY n.nspname, c.relname, t.tgname;
"""

_VIEWS_SQL = """
SELECT
    n.nspname                               AS schema_name,
    c.relname                               AS view_name,
    c.relkind                               AS relkind,
    pg_get_viewdef(c.oid, true)             AS view_def
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY($1::text[])
  AND c.relkind IN ('v', 'm')
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.classid = 'pg_class'::regclass
        AND d.objid = c.oid
        AND d.deptype = 'e'
  )
ORDER BY n.nspname, c.relname;
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


async def get_functions(
    conn: asyncpg.Connection, schemas: list[str]
) -> list[dict]:
    """Return user-defined functions and procedures in the given schemas."""
    return [dict(r) for r in await conn.fetch(_FUNCTIONS_SQL, schemas)]


async def get_triggers(
    conn: asyncpg.Connection, schemas: list[str]
) -> list[dict]:
    """Return triggers (excluding internal ones) in the given schemas."""
    return [dict(r) for r in await conn.fetch(_TRIGGERS_SQL, schemas)]


async def get_views(
    conn: asyncpg.Connection, schemas: list[str]
) -> list[dict]:
    """Return views and materialized views in the given schemas."""
    return [dict(r) for r in await conn.fetch(_VIEWS_SQL, schemas)]


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
            # Keep the original nextval() default — the named sequence is created
            # explicitly by sync_sequence() before this table DDL runs.
            # Do NOT convert to GENERATED BY DEFAULT AS IDENTITY: PostgreSQL would
            # auto-create an internal identity sequence and, because the named
            # sequence already exists, it would be given a _seq1 suffix — a
            # phantom sequence that sync-sequences never updates, causing
            # duplicate-key errors after cutover.
            parts = [qi(c["column_name"]), c["data_type"]]
            if c["not_null"]:
                parts.append("NOT NULL")
            parts.append(f"DEFAULT {c['column_default']}")
            col_defs.append(" ".join(parts))
        elif c.get("is_generated"):
            # Stored generated column: GENERATED ALWAYS AS (expr) STORED
            col_defs.append(
                f"{qi(c['column_name'])} {c['data_type']}"
                f" GENERATED ALWAYS AS ({c['column_default']}) STORED"
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
                    parts = [col["data_type"]]
                    if col["not_null"]:
                        parts.append("NOT NULL")
                    parts.append(f"DEFAULT {col['column_default']}")
                    col_def = " ".join(parts)
                elif col.get("is_generated"):
                    col_def = (
                        f"{col['data_type']}"
                        f" GENERATED ALWAYS AS ({col['column_default']}) STORED"
                    )
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
            except (asyncpg.DuplicateObjectError, asyncpg.WrongObjectTypeError):
                log.debug("Skipping constraint %s on %s (duplicate or view)", c["constraint_name"], fqn)


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
        except Exception as exc:
            log.warning(
                "Skipping index %s on %s: %s",
                idx["index_name"], fqn, exc,
            )


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
        "  SELECT 1 FROM pg_class c"
        "  JOIN pg_namespace n ON n.oid = c.relnamespace"
        "  WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'S'"
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
        try:
            await target_conn.execute(
                f"SELECT setval('{schema}.{seq_name}', $1, true);",
                src_val["last_value"],
            )
        except Exception as exc:
            log.warning(
                "Could not set value for sequence %s: %s — sequence will start from its initial value",
                fqn, exc,
            )
            return
    log.debug("Sequence %s synced to %s", fqn, src_val["last_value"])


async def sync_functions(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> None:
    """Create or replace user-defined functions/procedures on the target.

    Uses CREATE OR REPLACE so it is idempotent.  Functions must be synced
    before tables because generated columns or DEFAULT expressions may call them.
    """
    rows = await source_conn.fetch(_FUNCTIONS_SQL, schemas)
    for row in rows:
        func_def: str = row["func_def"]
        # pg_get_functiondef always returns CREATE OR REPLACE FUNCTION …
        try:
            await target_conn.execute(func_def)
            log.debug("Synced function %s.%s", row["schema_name"], row["func_name"])
        except Exception as exc:
            log.warning(
                "Could not sync function %s.%s: %s",
                row["schema_name"], row["func_name"], exc,
            )


async def sync_triggers(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> None:
    """Create or replace triggers on the target.

    Triggers must be synced AFTER tables and functions because they depend on both.
    Any existing trigger with the same name on the same table is dropped first so
    that definition changes are picked up (triggers have no CREATE OR REPLACE).
    """
    rows = await source_conn.fetch(_TRIGGERS_SQL, schemas)
    for row in rows:
        schema = row["schema_name"]
        table = row["table_name"]
        trigger_name = row["trigger_name"]
        trigger_def: str = row["trigger_def"]
        fqn = f"{qi(schema)}.{qi(table)}"
        drop_stmt = f"DROP TRIGGER IF EXISTS {qi(trigger_name)} ON {fqn};"
        try:
            await target_conn.execute(drop_stmt)
            await target_conn.execute(trigger_def + ";")
            log.debug("Synced trigger %s on %s.%s", trigger_name, schema, table)
        except Exception as exc:
            log.warning(
                "Could not sync trigger %s on %s.%s: %s",
                trigger_name, schema, table, exc,
            )


async def sync_views(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> None:
    """Create or replace views and materialized views on the target.

    Regular views use CREATE OR REPLACE.  Materialized views are dropped and
    recreated when their definition changes (they hold no persistent data that
    cannot be rebuilt with REFRESH MATERIALIZED VIEW).

    Views are attempted in source-OID order (creation order) and retried up to
    three times so that view-on-view dependency chains resolve without needing
    an explicit topological sort.  Any still-failing views after all retries are
    logged as warnings.

    Note: materialized view data is NOT replicated via logical replication.
    Run REFRESH MATERIALIZED VIEW on the target after migration.
    """
    rows = await source_conn.fetch(_VIEWS_SQL, schemas)
    tgt_rows = await target_conn.fetch(_VIEWS_SQL, schemas)
    tgt_view_map = {
        (r["schema_name"], r["view_name"]): r["view_def"] for r in tgt_rows
    }

    pending = list(rows)
    for _attempt in range(3):
        if not pending:
            break
        failed = []
        for row in pending:
            schema = row["schema_name"]
            view_name = row["view_name"]
            relkind = row["relkind"]
            view_def: str = row["view_def"]
            fqn = f"{qi(schema)}.{qi(view_name)}"
            tgt_def = tgt_view_map.get((schema, view_name))

            if relkind == "v":
                if tgt_def == view_def:
                    continue
                ddl = f"CREATE OR REPLACE VIEW {fqn} AS\n{view_def}"
                try:
                    await target_conn.execute(ddl)
                    log.debug("Synced view %s.%s", schema, view_name)
                except Exception as exc:
                    log.debug("Failed to sync view %s.%s (will retry): %s", schema, view_name, exc)
                    failed.append(row)
            else:  # materialized view
                if tgt_def == view_def:
                    continue
                try:
                    await target_conn.execute(f"DROP MATERIALIZED VIEW IF EXISTS {fqn};")
                    await target_conn.execute(
                        f"CREATE MATERIALIZED VIEW {fqn} AS\n{view_def.rstrip().rstrip(';')} WITH NO DATA;"
                    )
                    log.debug("Synced materialized view %s.%s", schema, view_name)
                except Exception as exc:
                    log.debug("Failed to sync materialized view %s.%s (will retry): %s", schema, view_name, exc)
                    failed.append(row)
        pending = failed

    for row in pending:
        log.warning(
            "Could not sync view %s.%s after retries — check for unresolvable dependencies",
            row["schema_name"], row["view_name"],
        )


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


async def sync_replica_identity(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> None:
    """Set REPLICA IDENTITY FULL on tables that have no primary key.

    Logical replication requires a way to identify updated/deleted rows.
    Without a PK (or a unique index marked as replica identity), PostgreSQL
    rejects DML on the subscriber.  REPLICA IDENTITY FULL makes PostgreSQL
    include all columns in the WAL record so rows can be matched by value.
    This is set on BOTH source and target so the publication and subscription
    are consistent.
    """
    no_pk_tables = await source_conn.fetch(
        """
        SELECT n.nspname AS schema_name, c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY($1::text[])
          AND c.relkind = 'r'
          AND NOT EXISTS (
              SELECT 1 FROM pg_index ix
              WHERE ix.indrelid = c.oid AND ix.indisprimary
          )
        ORDER BY n.nspname, c.relname
        """,
        schemas,
    )
    for row in no_pk_tables:
        fqn = f"{qi(row['schema_name'])}.{qi(row['table_name'])}"
        console.print(
            f"  [bold yellow]⚠ No PRIMARY KEY:[/bold yellow] {fqn} — setting REPLICA IDENTITY FULL"
        )
        stmt = f"ALTER TABLE {fqn} REPLICA IDENTITY FULL;"
        for conn, label in ((source_conn, "source"), (target_conn, "target")):
            try:
                await conn.execute(stmt)
                log.debug(
                    "Set REPLICA IDENTITY FULL on %s (%s)", fqn, label
                )
            except Exception as exc:
                log.warning(
                    "Could not set REPLICA IDENTITY FULL on %s (%s): %s",
                    fqn, label, exc,
                )


async def sync_extensions(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
) -> None:
    """Ensure all extensions present on the source are installed on the target.

    Extensions like btree_gin or pg_trgm provide operator classes required by
    indexes.  Without them, creating GIN indexes on types like UUID fails with
    'no default operator class'.  Failures are logged as warnings because some
    extensions (e.g. timescaledb) may not be available on the target system.
    """
    rows = await source_conn.fetch(
        """
        SELECT extname
        FROM pg_extension
        WHERE extname NOT IN ('plpgsql')
        ORDER BY extname
        """
    )
    for row in rows:
        extname = row["extname"]
        try:
            await target_conn.execute(
                f"CREATE EXTENSION IF NOT EXISTS {qi(extname)};"
            )
            log.debug("Ensured extension %s is installed", extname)
        except Exception as exc:
            log.warning("Could not install extension %s: %s", extname, exc)


async def sync_schemas(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> None:
    """Full schema synchronization: schemas → enums → sequences → tables → FKs."""
    # Sync extensions FIRST — extensions like timescaledb create their own schemas
    # on installation; creating those schemas beforehand causes the install to fail.
    await sync_extensions(source_conn, target_conn)

    # Ensure remaining target schemas exist (extension-owned ones were created above)
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

    # Sync functions BEFORE tables — generated columns / defaults may call them
    await sync_functions(source_conn, target_conn, schemas)

    # Sync tables (without foreign keys first to avoid dependency issues)
    tables = await get_tables(source_conn, schemas)
    for t in tables:
        # First pass: create table + columns + PKs + unique constraints
        await _sync_table_structure(
            source_conn, target_conn, t["schema_name"], t["table_name"],
        )

    # Second pass: foreign keys
    for t in tables:
        await _sync_foreign_keys(source_conn, target_conn, t["schema_name"], t["table_name"])

    # Sync triggers AFTER tables and functions — triggers depend on both
    await sync_triggers(source_conn, target_conn, schemas)

    # Sync views and materialized views AFTER tables (views may reference them)
    await sync_views(source_conn, target_conn, schemas)

    # Set REPLICA IDENTITY FULL on tables without PK on both sides so that
    # logical replication can handle UPDATE/DELETE without a unique row identifier.
    await sync_replica_identity(source_conn, target_conn, schemas)

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
        # Add missing columns (only for non-view objects)
        obj_kind = await target_conn.fetchval(
            "SELECT c.relkind FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = $1 AND c.relname = $2",
            schema, table,
        )
        if obj_kind != "r":
            log.debug("Skipping column/constraint sync for non-table %s (kind=%s)", fqn, obj_kind)
            return

        src_cols = await get_columns(source_conn, fqn)
        tgt_cols = await get_columns(target_conn, fqn)
        tgt_col_names = {c["column_name"] for c in tgt_cols}

        for col in src_cols:
            if col["column_name"] not in tgt_col_names:
                if _is_nextval_default(col["column_default"]):
                    parts = [col["data_type"]]
                    if col["not_null"]:
                        parts.append("NOT NULL")
                    parts.append(f"DEFAULT {col['column_default']}")
                    col_def = " ".join(parts)
                elif col.get("is_generated"):
                    col_def = (
                        f"{col['data_type']}"
                        f" GENERATED ALWAYS AS ({col['column_default']}) STORED"
                    )
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
            except (asyncpg.DuplicateObjectError, asyncpg.WrongObjectTypeError):
                log.debug("Skipping constraint %s on %s (duplicate or view)", c["constraint_name"], fqn)

    # Unique indexes only — non-unique indexes are deferred until after COPY
    await _sync_indexes(source_conn, target_conn, schema, table, deferred=False)


_OBJECT_OWNERS_SQL = """
SELECT
    n.nspname   AS schema_name,
    c.relname   AS object_name,
    CASE c.relkind
        WHEN 'r' THEN 'table'
        WHEN 'S' THEN 'sequence'
        WHEN 'v' THEN 'view'
        WHEN 'm' THEN 'materialized_view'
    END         AS kind,
    r.rolname   AS owner
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_roles r     ON r.oid = c.relowner
WHERE n.nspname = ANY($1::text[])
  AND c.relkind IN ('r', 'S', 'v', 'm')
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.classid = 'pg_class'::regclass
        AND d.objid = c.oid
        AND d.deptype = 'e'
  )
ORDER BY n.nspname, c.relkind, c.relname;
"""

_SCHEMA_OWNERS_SQL = """
SELECT n.nspname AS schema_name, r.rolname AS owner
FROM pg_namespace n
JOIN pg_roles r ON r.oid = n.nspowner
WHERE n.nspname = ANY($1::text[])
ORDER BY n.nspname;
"""

_TYPE_OWNERS_SQL = """
SELECT
    n.nspname   AS schema_name,
    t.typname   AS object_name,
    'type'      AS kind,
    r.rolname   AS owner
FROM pg_type t
JOIN pg_namespace n ON n.oid = t.typnamespace
JOIN pg_roles r     ON r.oid = t.typowner
WHERE n.nspname = ANY($1::text[])
  AND t.typtype IN ('e', 'd')
  AND t.typname NOT LIKE '\\_%'
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.classid = 'pg_type'::regclass
        AND d.objid = t.oid
        AND d.deptype = 'e'
  )
ORDER BY n.nspname, t.typname;
"""

_FUNCTION_OWNERS_SQL = """
SELECT
    n.nspname                                        AS schema_name,
    p.proname                                        AS func_name,
    pg_get_function_identity_arguments(p.oid)        AS func_args,
    p.prokind                                        AS prokind,
    r.rolname                                        AS owner
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
JOIN pg_roles r     ON r.oid = p.proowner
WHERE n.nspname = ANY($1::text[])
  AND p.prokind IN ('f', 'p')
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.classid = 'pg_proc'::regclass
        AND d.objid = p.oid
        AND d.deptype = 'e'
  )
ORDER BY n.nspname, p.proname;
"""


async def get_object_owners(
    conn: asyncpg.Connection,
    schemas: list[str],
    *,
    dbname: str | None = None,
) -> list[dict]:
    """Return owner metadata for all schema objects in *schemas*.

    Covers tables, sequences, views, materialized views, schemas,
    user-defined types (enums, domains), and functions/procedures.
    When *dbname* is given the database-level owner is also included.
    """
    rows: list[dict] = [dict(r) for r in await conn.fetch(_OBJECT_OWNERS_SQL, schemas)]

    schema_rows = [
        {"schema_name": r["schema_name"], "object_name": r["schema_name"],
         "kind": "schema", "owner": r["owner"]}
        for r in await conn.fetch(_SCHEMA_OWNERS_SQL, schemas)
    ]

    type_rows = [
        {"schema_name": r["schema_name"], "object_name": r["object_name"],
         "kind": "type", "owner": r["owner"]}
        for r in await conn.fetch(_TYPE_OWNERS_SQL, schemas)
    ]

    func_rows: list[dict] = []
    for r in await conn.fetch(_FUNCTION_OWNERS_SQL, schemas):
        func_rows.append({
            "schema_name": r["schema_name"],
            "object_name": f"{r['func_name']}({r['func_args']})",
            "kind": "procedure" if r["prokind"] == "p" else "function",
            "owner": r["owner"],
            "func_name": r["func_name"],
            "func_args": r["func_args"],
        })

    db_rows: list[dict] = []
    if dbname:
        db_row = await conn.fetchrow(
            "SELECT r.rolname AS owner"
            " FROM pg_database d"
            " JOIN pg_roles r ON r.oid = d.datdba"
            " WHERE d.datname = $1",
            dbname,
        )
        if db_row:
            db_rows.append({"schema_name": "", "object_name": dbname,
                            "kind": "database", "owner": db_row["owner"]})

    return schema_rows + rows + type_rows + func_rows + db_rows


def make_owner_fix_ddl(rec: dict, owner: str) -> str:
    """Generate the ALTER … OWNER TO statement for one owner record."""
    schema = rec["schema_name"]
    obj = rec["object_name"]
    kind = rec["kind"]
    if kind == "table":
        return f"ALTER TABLE {qi(schema)}.{qi(obj)} OWNER TO {qi(owner)};"
    if kind == "sequence":
        return f"ALTER SEQUENCE {qi(schema)}.{qi(obj)} OWNER TO {qi(owner)};"
    if kind == "view":
        return f"ALTER VIEW {qi(schema)}.{qi(obj)} OWNER TO {qi(owner)};"
    if kind == "materialized_view":
        return f"ALTER MATERIALIZED VIEW {qi(schema)}.{qi(obj)} OWNER TO {qi(owner)};"
    if kind in ("function", "procedure"):
        return (
            f"ALTER ROUTINE {qi(schema)}.{qi(rec['func_name'])}"
            f"({rec['func_args']}) OWNER TO {qi(owner)};"
        )
    if kind == "type":
        return f"ALTER TYPE {qi(schema)}.{qi(obj)} OWNER TO {qi(owner)};"
    if kind == "schema":
        return f"ALTER SCHEMA {qi(obj)} OWNER TO {qi(owner)};"
    if kind == "database":
        return f"ALTER DATABASE {qi(obj)} OWNER TO {qi(owner)};"
    return ""


async def sync_ownership(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
    *,
    dbname: str | None = None,
) -> int:
    """Synchronize ownership of all schema objects from source to target.

    Covers tables, sequences, views, materialized views, schemas,
    user-defined types (enums, domains), functions, procedures, and
    (when *dbname* is supplied) the database itself.

    Roles that do not exist on the target are skipped with a warning —
    create them manually beforehand if needed.

    Returns the number of ownership changes applied.
    """
    src_list = await get_object_owners(source_conn, schemas, dbname=dbname)
    tgt_list = await get_object_owners(target_conn, schemas, dbname=dbname)

    src_owners = {(r["schema_name"], r["object_name"], r["kind"]): r for r in src_list}
    tgt_owners = {(r["schema_name"], r["object_name"], r["kind"]): r["owner"] for r in tgt_list}

    existing_roles = {
        r["rolname"]
        for r in await target_conn.fetch("SELECT rolname FROM pg_roles")
    }

    applied = 0
    for (schema, obj, kind), src_rec in src_owners.items():
        src_owner = src_rec["owner"]
        tgt_owner = tgt_owners.get((schema, obj, kind))
        if tgt_owner == src_owner:
            continue
        if src_owner not in existing_roles:
            log.warning(
                "Skipping ownership of %s %s.%s — role '%s' does not exist on target",
                kind, schema, obj, src_owner,
            )
            continue
        stmt = make_owner_fix_ddl(src_rec, src_owner)
        if not stmt:
            continue
        try:
            await target_conn.execute(stmt)
            log.info("Set owner of %s %s.%s to %s", kind, schema, obj, src_owner)
            applied += 1
        except Exception as exc:
            log.warning("Could not set owner of %s %s.%s — %s", kind, schema, obj, exc)

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
            except (asyncpg.DuplicateObjectError, asyncpg.WrongObjectTypeError):
                log.debug("Skipping FK %s on %s (duplicate or view)", c["constraint_name"], fqn)
