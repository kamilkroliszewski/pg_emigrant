"""Schema introspection and synchronization between source and target."""

from __future__ import annotations

import asyncio

import asyncpg

from pg_emigrant.utils import console, get_logger, qi, ql, qt

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Introspection queries
# ---------------------------------------------------------------------------

_TABLES_SQL = """
WITH RECURSIVE part_tree AS (
    -- Roots: ordinary tables and partitioned parents that are not themselves
    -- partitions.  Depth 0.
    SELECT c.oid, 0 AS depth
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = ANY($1::text[])
      AND c.relkind IN ('r', 'p')
      AND NOT c.relispartition
    UNION ALL
    -- Descend into partitions; each level deeper gets a higher depth so that a
    -- parent is always emitted before its partition children.
    SELECT child.oid, pt.depth + 1
    FROM part_tree pt
    JOIN pg_inherits i ON i.inhparent = pt.oid
    JOIN pg_class child ON child.oid = i.inhrelid AND child.relispartition
)
SELECT
    c.relname             AS table_name,
    n.nspname             AS schema_name,
    c.relkind::text       AS relkind,
    c.relispartition      AS is_partition,
    COALESCE(pt.depth, 0) AS part_depth
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN part_tree pt ON pt.oid = c.oid
WHERE n.nspname = ANY($1::text[])
  AND c.relkind IN ('r', 'p')
-- Depth FIRST, then schema: a partition child living in an alphabetically
-- earlier schema than its parent must still be created after the parent
-- (its PARTITION OF DDL requires the parent to exist).
ORDER BY COALESCE(pt.depth, 0), n.nspname, c.relname;
"""

_COLUMNS_SQL = """
SELECT
    a.attname          AS column_name,
    a.attnum           AS ordinal,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
    a.attnotnull       AS not_null,
    pg_get_expr(d.adbin, d.adrelid) AS column_default,
    (a.attgenerated = 's') AS is_generated,
    a.attidentity::text AS identity,
    -- The identity sequence's own parameters.  Omitting them from the
    -- recreated column would silently reset e.g. INCREMENT BY 5 to 1 on the
    -- target — a collision generator for multi-writer id schemes.
    CASE WHEN a.attidentity IN ('a', 'd') THEN (
        SELECT format(
            'INCREMENT BY %s MINVALUE %s MAXVALUE %s START WITH %s CACHE %s%s',
            s.seqincrement, s.seqmin, s.seqmax, s.seqstart, s.seqcache,
            CASE WHEN s.seqcycle THEN ' CYCLE' ELSE '' END)
        FROM pg_sequence s
        JOIN pg_depend dep ON dep.classid = 'pg_class'::regclass
                          AND dep.objid = s.seqrelid
                          AND dep.refclassid = 'pg_class'::regclass
                          AND dep.refobjid = a.attrelid
                          AND dep.refobjsubid = a.attnum
                          AND dep.deptype = 'i'
    ) END AS identity_options,
    -- Only non-NULL when the column's collation differs from its type's
    -- default (mirrors pg_dump: omit COLLATE entirely for the common case).
    CASE WHEN a.attcollation <> 0 AND a.attcollation <> t.typcollation
         THEN co.collname END AS collation_name,
    CASE WHEN a.attcollation <> 0 AND a.attcollation <> t.typcollation
         THEN cn.nspname END AS collation_schema
FROM pg_attribute a
JOIN pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
LEFT JOIN pg_collation co ON co.oid = a.attcollation
LEFT JOIN pg_namespace cn ON cn.oid = co.collnamespace
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
    con.conname       AS constraint_name,
    con.contype::text AS constraint_type,
    pg_get_constraintdef(con.oid) AS constraint_def
FROM pg_constraint con
WHERE con.conrelid = $1::regclass
ORDER BY con.conname;
"""

_SEQUENCES_SQL = """
SELECT
    n.nspname AS schema_name,
    c.relname AS sequence_name,
    pg_get_serial_sequence(quote_ident(n2.nspname) || '.' || quote_ident(t.relname), a.attname) AS owned_by,
    EXISTS (
        SELECT 1 FROM pg_depend di
        WHERE di.classid = 'pg_class'::regclass
          AND di.objid = c.oid
          AND di.deptype = 'i'
    ) AS is_identity
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

_COMPOSITE_TYPES_SQL = """
SELECT
    n.nspname   AS schema_name,
    t.typname   AS type_name,
    a.attname   AS col_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS col_type
FROM pg_type t
JOIN pg_namespace n ON n.oid = t.typnamespace
JOIN pg_class c     ON c.oid = t.typrelid AND c.relkind = 'c'
JOIN pg_attribute a ON a.attrelid = c.oid
                    AND a.attnum > 0
                    AND NOT a.attisdropped
WHERE n.nspname = ANY($1::text[])
  AND t.typtype = 'c'
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.classid = 'pg_type'::regclass
        AND d.objid = t.oid
        AND d.deptype = 'e'
  )
ORDER BY n.nspname, t.typname, a.attnum;
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
  -- Exclude auto-generated companion functions (e.g. a range type's
  -- constructor/multirange functions, deptype='i') — they are created
  -- automatically by their owning object's DDL (CREATE TYPE ... AS RANGE)
  -- and re-issuing CREATE OR REPLACE for them is redundant at best, and at
  -- worst fights the object that actually owns them.  Same principle as
  -- excluding identity-backed sequences from pre-creation.
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend di
      WHERE di.classid = 'pg_proc'::regclass
        AND di.objid = p.oid
        AND di.deptype = 'i'
  )
ORDER BY n.nspname, p.proname;
"""

_TRIGGERS_SQL = """
SELECT
    n.nspname                                          AS schema_name,
    c.relname                                          AS table_name,
    t.tgname                                           AS trigger_name,
    pg_get_triggerdef(t.oid)                           AS trigger_def,
    -- 'O' = enabled (default), 'D' = disabled, 'R' = ENABLE REPLICA,
    -- 'A' = ENABLE ALWAYS.  pg_get_triggerdef never includes this state.
    t.tgenabled::text                                  AS enabled
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY($1::text[])
  AND NOT t.tgisinternal
  -- Skip clones on partition children: they are created automatically when
  -- the parent's trigger is created, and creating/dropping them directly
  -- either fails or leaves a conflicting standalone trigger on the child.
  AND t.tgparentid = 0
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
    c.relkind::text                         AS relkind,
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
    """Return views and materialized views in the given schemas.

    Timescaledb continuous aggregates are excluded — they are backed by
    internal hypertables and cannot be re-created via standard DDL.
    """
    rows = [dict(r) for r in await conn.fetch(_VIEWS_SQL, schemas)]
    try:
        cagg_rows = await conn.fetch(
            "SELECT user_view_schema, user_view_name"
            " FROM _timescaledb_catalog.continuous_agg"
        )
        caggs = {(r["user_view_schema"], r["user_view_name"]) for r in cagg_rows}
        if caggs:
            rows = [
                r for r in rows
                if (r["schema_name"], r["view_name"]) not in caggs
            ]
    except asyncpg.PostgresError:
        pass  # timescaledb not installed or catalog not accessible
    return rows


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


def _qualified_collation(schema: str | None, name: str) -> str:
    """Schema-qualify a collation reference unless it lives in pg_catalog.

    Built-in collations ("C", "en_US", …) resolve everywhere; user-defined
    collations must be qualified or a different search_path on the target
    silently binds the wrong one (or fails)."""
    if schema and schema != "pg_catalog":
        return f"{qi(schema)}.{qi(name)}"
    return qi(name)


def _collate_clause(col: dict) -> str:
    """Return ``'COLLATE "name" '`` when the column has a non-default
    collation, else ``''``.  Trailing space lets callers splice it directly
    between the data type and what follows without extra bookkeeping."""
    name = col.get("collation_name")
    if not name:
        return ""
    return f"COLLATE {_qualified_collation(col.get('collation_schema'), name)} "


async def _get_partition_info(
    conn: asyncpg.Connection, schema: str, table: str
) -> dict:
    """Return partition metadata for a relation.

    Keys:
      is_partitioned — True when the relation is a partitioned parent (relkind 'p')
      is_partition   — True when the relation is itself a partition of a parent
      partition_by   — the ``PARTITION BY …`` clause body (parents only) or None
      bound          — the ``FOR VALUES …`` / ``DEFAULT`` bound (partitions only)
      parent_fqn     — schema-qualified parent name (partitions only) or None
    """
    row = await conn.fetchrow(
        """
        SELECT
            (c.relkind = 'p')                                       AS is_partitioned,
            c.relispartition                                        AS is_partition,
            CASE WHEN c.relkind = 'p'
                 THEN pg_get_partkeydef(c.oid) END                  AS partition_by,
            CASE WHEN c.relispartition
                 THEN pg_get_expr(c.relpartbound, c.oid) END        AS bound,
            parent_n.nspname                                        AS parent_schema,
            parent_c.relname                                        AS parent_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_inherits i ON i.inhrelid = c.oid AND c.relispartition
        LEFT JOIN pg_class parent_c ON parent_c.oid = i.inhparent
        LEFT JOIN pg_namespace parent_n ON parent_n.oid = parent_c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2
        """,
        schema, table,
    )
    if not row:
        return {}
    info = dict(row)
    if info.get("parent_name"):
        info["parent_fqn"] = f"{qi(info['parent_schema'])}.{qi(info['parent_name'])}"
    else:
        info["parent_fqn"] = None
    return info


async def _generate_create_table_ddl(
    conn: asyncpg.Connection, schema: str, table: str
) -> str:
    """Generate a CREATE TABLE statement by introspecting the source.

    Serial / sequence-backed columns are rendered as
    ``GENERATED BY DEFAULT AS IDENTITY`` so that no named sequences need to
    exist on the target beforehand.

    Partitioned parents get a trailing ``PARTITION BY …`` clause; partition
    children are emitted as ``CREATE TABLE … PARTITION OF parent FOR VALUES …``
    (columns, constraints and indexes are inherited from the parent, so they
    are not repeated here).
    """
    fqn = f"{qi(schema)}.{qi(table)}"

    part = await _get_partition_info(conn, schema, table)

    # Partition child: attach to its parent.  The parent must already exist
    # (callers iterate tables ordered by partition depth, parents first).
    if part.get("is_partition") and part.get("parent_fqn"):
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {fqn} "
            f"PARTITION OF {part['parent_fqn']} {part['bound']}"
        )
        if part.get("is_partitioned") and part.get("partition_by"):
            # Sub-partitioned: this child is itself partitioned further.
            ddl += f" PARTITION BY {part['partition_by']}"
        return ddl + ";"

    columns = await get_columns(conn, fqn)

    col_defs = []
    for c in columns:
        if c.get("identity") in ("a", "d"):
            # GENERATED ALWAYS / BY DEFAULT AS IDENTITY column, reproducing
            # the identity sequence's parameters (INCREMENT/CACHE/…).
            identity_clause = (
                "GENERATED ALWAYS AS IDENTITY"
                if c["identity"] == "a"
                else "GENERATED BY DEFAULT AS IDENTITY"
            )
            if c.get("identity_options"):
                identity_clause += f" ({c['identity_options']})"
            col_defs.append(f"{qi(c['column_name'])} {c['data_type']} {identity_clause}")
        elif _is_nextval_default(c["column_default"]):
            # Keep the original nextval() default — the named sequence is created
            # explicitly by sync_sequence() before this table DDL runs.
            # Do NOT convert to GENERATED BY DEFAULT AS IDENTITY: PostgreSQL would
            # auto-create an internal identity sequence and, because the named
            # sequence already exists, it would be given a _seq1 suffix — a
            # phantom sequence that sync-sequences never updates, causing
            # duplicate-key errors after cutover.
            parts = [qi(c["column_name"]), c["data_type"], _collate_clause(c).strip()]
            if c["not_null"]:
                parts.append("NOT NULL")
            parts.append(f"DEFAULT {c['column_default']}")
            col_defs.append(" ".join(p for p in parts if p))
        elif c.get("is_generated"):
            # Stored generated column: GENERATED ALWAYS AS (expr) STORED
            col_defs.append(
                f"{qi(c['column_name'])} {c['data_type']} {_collate_clause(c)}"
                f"GENERATED ALWAYS AS ({c['column_default']}) STORED"
            )
        else:
            parts = [qi(c["column_name"]), c["data_type"], _collate_clause(c).strip()]
            if c["not_null"]:
                parts.append("NOT NULL")
            if c["column_default"] is not None:
                parts.append(f"DEFAULT {c['column_default']}")
            col_defs.append(" ".join(p for p in parts if p))

    ddl = f"CREATE TABLE IF NOT EXISTS {fqn} (\n  " + ",\n  ".join(col_defs) + "\n)"
    if part.get("is_partitioned") and part.get("partition_by"):
        ddl += f" PARTITION BY {part['partition_by']}"
    return ddl + ";"


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
    # Partition children inherit columns, constraints, PK and indexes from the
    # parent — emitting them again would fail with duplicate-object errors.
    part = await _get_partition_info(conn, schema, table)
    if not part.get("is_partition"):
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
            # Use fqn (already double-quoted) so mixed-case names like
            # "Platforms_id_seq" are not silently folded to lowercase.
            await target_conn.execute(
                f"SELECT setval('{fqn}', $1, true);",
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
    *,
    silent: bool = False,
) -> list[str]:
    """Create or replace user-defined functions/procedures on the target.

    Uses CREATE OR REPLACE so it is idempotent.  Function bodies are created
    with ``check_function_bodies = off`` (exactly what pg_dump emits), so a
    function does not fail just because a table/view it references does not
    exist yet.  SQL-standard bodies (``BEGIN ATOMIC``) are still parsed
    against existing objects regardless — those rely on the retry passes.

    Call with ``silent=True`` for a best-effort pre-pass before tables are
    created (needed for generated columns / DEFAULT expressions that call
    functions).  Failures in that pass are expected and logged at DEBUG level.
    Call again after tables are created (``silent=False``, the default) so
    that functions referencing tables succeed and any remaining failures are
    logged as warnings.

    Returns ``"schema.name: error"`` entries for functions that failed
    (always empty in silent mode — those failures are expected and retried).
    """
    rows = await source_conn.fetch(_FUNCTIONS_SQL, schemas)
    if not rows:
        return []
    failures: list[str] = []
    await target_conn.execute("SET check_function_bodies = off;")
    try:
        for row in rows:
            func_def: str = row["func_def"]
            # pg_get_functiondef always returns CREATE OR REPLACE FUNCTION …
            try:
                await target_conn.execute(func_def)
                log.debug("Synced function %s.%s", row["schema_name"], row["func_name"])
            except Exception as exc:
                if silent:
                    log.debug(
                        "Pre-pass: could not sync function %s.%s (will retry after tables): %s",
                        row["schema_name"], row["func_name"], exc,
                    )
                else:
                    log.warning(
                        "Could not sync function %s.%s: %s",
                        row["schema_name"], row["func_name"], exc,
                    )
                    err = (str(exc) or repr(exc)).splitlines()[0]
                    failures.append(
                        f"{row['schema_name']}.{row['func_name']}: {err}"
                    )
    finally:
        await target_conn.execute("RESET check_function_bodies;")
    return failures


def make_trigger_enable_ddl(enabled: str, fqn: str, trigger_name: str) -> str | None:
    """Return the ALTER TABLE statement reproducing a trigger's ENABLE state.

    ``pg_get_triggerdef`` never includes ``tgenabled``, so without this a
    trigger *disabled* on the source would come back enabled on the target
    (and fire unexpectedly at cutover), and an ``ENABLE ALWAYS`` trigger
    would silently lose its fire-during-replication-apply semantics.
    Returns ``None`` for the default state ('O') — nothing to add.
    """
    if enabled == "D":
        return f"ALTER TABLE {fqn} DISABLE TRIGGER {qi(trigger_name)};"
    if enabled == "R":
        return f"ALTER TABLE {fqn} ENABLE REPLICA TRIGGER {qi(trigger_name)};"
    if enabled == "A":
        return f"ALTER TABLE {fqn} ENABLE ALWAYS TRIGGER {qi(trigger_name)};"
    return None


async def sync_triggers(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> list[str]:
    """Create or replace triggers on the target.

    Triggers must be synced AFTER tables, functions and views because they may
    depend on all of them — callers run this as the LAST object pass.
    Any existing trigger with the same name on the same table is dropped first so
    that definition changes are picked up (triggers have no CREATE OR REPLACE).
    The source's ENABLE state (DISABLED / ENABLE REPLICA / ENABLE ALWAYS) is
    reproduced after creation — see :func:`make_trigger_enable_ddl`.

    Returns ``"trigger on schema.table: error"`` entries for failed triggers.
    """
    rows = await source_conn.fetch(_TRIGGERS_SQL, schemas)
    failures: list[str] = []
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
            enable_ddl = make_trigger_enable_ddl(row["enabled"], fqn, trigger_name)
            if enable_ddl:
                await target_conn.execute(enable_ddl)
            log.debug("Synced trigger %s on %s.%s", trigger_name, schema, table)
        except Exception as exc:
            log.warning(
                "Could not sync trigger %s on %s.%s: %s",
                trigger_name, schema, table, exc,
            )
            err = (str(exc) or repr(exc)).splitlines()[0]
            failures.append(f"{trigger_name} on {schema}.{table}: {err}")
    return failures


_ROW_SECURITY_TABLES_SQL = """
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    c.relrowsecurity      AS rowsecurity,
    c.relforcerowsecurity AS force_rowsecurity
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY($1::text[])
  AND c.relkind IN ('r', 'p')
ORDER BY 1, 2;
"""

_POLICIES_SQL = """
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    pol.polname AS policy_name,
    pol.polcmd::text AS polcmd,
    pol.polpermissive AS permissive,
    CASE WHEN pol.polroles = ARRAY[0]::oid[] THEN ARRAY['PUBLIC']
         ELSE ARRAY(
             SELECT rolname FROM pg_roles WHERE oid = ANY(pol.polroles) ORDER BY rolname
         )
    END AS roles,
    pg_get_expr(pol.polqual, pol.polrelid)      AS using_expr,
    pg_get_expr(pol.polwithcheck, pol.polrelid) AS check_expr
FROM pg_policy pol
JOIN pg_class c ON c.oid = pol.polrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY($1::text[])
ORDER BY 1, 2, 3;
"""

_POLICY_CMD_KEYWORD = {"r": "SELECT", "a": "INSERT", "w": "UPDATE", "d": "DELETE", "*": "ALL"}


async def get_row_security_tables(conn: asyncpg.Connection, schemas: list[str]) -> list[dict]:
    """Return RLS enable/force flags for every table in *schemas*."""
    return [dict(r) for r in await conn.fetch(_ROW_SECURITY_TABLES_SQL, schemas)]


async def get_policies(conn: asyncpg.Connection, schemas: list[str]) -> list[dict]:
    """Return row-security policies for every table in *schemas*."""
    return [dict(r) for r in await conn.fetch(_POLICIES_SQL, schemas)]


def make_policy_ddl(row: dict) -> str:
    fqn = f"{qi(row['schema_name'])}.{qi(row['table_name'])}"
    parts = [
        f"CREATE POLICY {qi(row['policy_name'])} ON {fqn}",
        "AS PERMISSIVE" if row["permissive"] else "AS RESTRICTIVE",
        f"FOR {_POLICY_CMD_KEYWORD.get(row['polcmd'], 'ALL')}",
    ]
    roles = row["roles"] or ["PUBLIC"]
    role_list = ", ".join("PUBLIC" if r == "PUBLIC" else qi(r) for r in roles)
    parts.append(f"TO {role_list}")
    if row["using_expr"]:
        parts.append(f"USING ({row['using_expr']})")
    if row["check_expr"]:
        parts.append(f"WITH CHECK ({row['check_expr']})")
    return " ".join(parts) + ";"


async def sync_row_security(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> list[str]:
    """Reproduce row-level security on the target: ``ENABLE``/``FORCE ROW LEVEL
    SECURITY`` per table, and every policy (``CREATE POLICY``).

    Run this AFTER the initial data copy, never before: if the migration role
    connecting to the target is the table owner but not a superuser, a table
    with ``FORCE ROW LEVEL SECURITY`` enabled too early would have the bulk
    COPY silently blocked or filtered by its own policies (superusers always
    bypass RLS, but table owners only do when FORCE is off).

    Any existing policy with the same name on the same table is dropped first
    so that definition changes are picked up (policies have no CREATE OR
    REPLACE, like triggers).

    Returns ``"policy on schema.table: error"`` entries for failures.
    """
    failures: list[str] = []

    tables = await get_row_security_tables(source_conn, schemas)
    for t in tables:
        if not (t["rowsecurity"] or t["force_rowsecurity"]):
            continue
        fqn = f"{qi(t['schema_name'])}.{qi(t['table_name'])}"
        try:
            if t["rowsecurity"]:
                await target_conn.execute(f"ALTER TABLE {fqn} ENABLE ROW LEVEL SECURITY;")
            if t["force_rowsecurity"]:
                await target_conn.execute(f"ALTER TABLE {fqn} FORCE ROW LEVEL SECURITY;")
        except Exception as exc:
            log.warning(
                "Could not enable row security on %s.%s: %s",
                t["schema_name"], t["table_name"], exc,
            )

    policies = await get_policies(source_conn, schemas)
    for row in policies:
        schema, table, name = row["schema_name"], row["table_name"], row["policy_name"]
        fqn = f"{qi(schema)}.{qi(table)}"
        try:
            await target_conn.execute(f"DROP POLICY IF EXISTS {qi(name)} ON {fqn};")
            await target_conn.execute(make_policy_ddl(row))
            log.debug("Synced policy %s on %s.%s", name, schema, table)
        except Exception as exc:
            log.warning("Could not sync policy %s on %s.%s: %s", name, schema, table, exc)
            err = (str(exc) or repr(exc)).splitlines()[0]
            failures.append(f"{name} on {schema}.{table}: {err}")

    return failures


async def sync_views(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> list[str]:
    """Create or replace views and materialized views on the target.

    Returns ``"schema.view: error"`` entries for views that still failed
    after all retries.

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

    import re as _re
    def _nv(s: str) -> str:
        """Normalize whitespace for view definition comparison."""
        return _re.sub(r"\s+", " ", (s or "").strip())

    def _is_tsdb_cagg(view_def: str) -> bool:
        """Return True if this view is a TimescaleDB continuous aggregate.

        Continuous aggregates are backed by an internal hypertable
        (_timescaledb_internal._materialized_hypertable_N) and cannot be
        created with plain CREATE MATERIALIZED VIEW DDL.
        """
        return "_timescaledb_internal._materialized_hypertable_" in (view_def or "")

    pending = list(rows)
    last_err: dict[tuple[str, str], str] = {}
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

            if _is_tsdb_cagg(view_def):
                log.debug(
                    "Skipping %s.%s — TimescaleDB continuous aggregate (managed by timescaledb)",
                    schema, view_name,
                )
                continue

            if relkind == "v":
                if _nv(tgt_def) == _nv(view_def):
                    continue
                ddl = f"CREATE OR REPLACE VIEW {fqn} AS\n{view_def}"
                try:
                    await target_conn.execute(ddl)
                    log.debug("Synced view %s.%s", schema, view_name)
                except Exception as exc:
                    log.debug("Failed to sync view %s.%s (will retry): %s", schema, view_name, exc)
                    last_err[(schema, view_name)] = (str(exc) or repr(exc)).splitlines()[0]
                    failed.append(row)
            else:  # materialized view
                if _nv(tgt_def) == _nv(view_def):
                    continue
                try:
                    await target_conn.execute(f"DROP MATERIALIZED VIEW IF EXISTS {fqn};")
                    await target_conn.execute(
                        f"CREATE MATERIALIZED VIEW {fqn} AS\n{view_def.rstrip().rstrip(';')} WITH NO DATA;"
                    )
                    log.debug("Synced materialized view %s.%s", schema, view_name)
                except Exception as exc:
                    log.debug("Failed to sync materialized view %s.%s (will retry): %s", schema, view_name, exc)
                    last_err[(schema, view_name)] = (str(exc) or repr(exc)).splitlines()[0]
                    failed.append(row)
        pending = failed

    failed_keys = {(row["schema_name"], row["view_name"]) for row in pending}
    failures: list[str] = []
    for row in pending:
        key = (row["schema_name"], row["view_name"])
        err = last_err.get(key, "unresolvable dependencies")
        log.warning(
            "Could not sync view %s.%s after retries — %s", key[0], key[1], err,
        )
        failures.append(f"{key[0]}.{key[1]}: {err}")

    # Materialized views were created WITH NO DATA above — populate them now
    # via REFRESH, then build their indexes (faster on already-loaded data
    # than incrementally during REFRESH — the same reasoning as deferring
    # regular tables' indexes until after the initial COPY).  Every matview
    # present on source is refreshed here, not just ones whose definition
    # changed this run: sync_views only runs during bootstrap, whose job is
    # to populate what was just created WITH NO DATA.
    for row in rows:
        if row["relkind"] != "m":
            continue
        schema, view_name = row["schema_name"], row["view_name"]
        if (schema, view_name) in failed_keys or _is_tsdb_cagg(row["view_def"]):
            continue
        fqn = f"{qi(schema)}.{qi(view_name)}"
        try:
            await target_conn.execute(f"REFRESH MATERIALIZED VIEW {fqn};")
            log.debug("Refreshed materialized view %s.%s", schema, view_name)
        except Exception as exc:
            log.warning(
                "Could not refresh materialized view %s.%s: %s", schema, view_name, exc,
            )
            failures.append(f"{schema}.{view_name}: refresh failed — {exc}")
            continue
        await _sync_indexes(source_conn, target_conn, schema, view_name, deferred=False)
        await _sync_indexes(source_conn, target_conn, schema, view_name, deferred=True)

    return failures


_COLLATIONS_SQL_TMPL = """
SELECT
    n.nspname                 AS schema_name,
    c.collname                AS coll_name,
    c.collprovider::text      AS provider,
    c.collisdeterministic     AS deterministic,
    c.collcollate,
    c.collctype,
    {loc_col}                 AS provider_locale
FROM pg_collation c
JOIN pg_namespace n ON n.oid = c.collnamespace
WHERE n.nspname = ANY($1::text[])
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.classid = 'pg_collation'::regclass
        AND d.objid = c.oid
        AND d.deptype = 'e'
  )
ORDER BY n.nspname, c.collname;
"""


def _collation_ddl(r: dict) -> str:
    fqn = f"{qi(r['schema_name'])}.{qi(r['coll_name'])}"
    opts = []
    if r["provider"] == "i":
        opts.append("PROVIDER = icu")
        opts.append(f"LOCALE = {ql(r['provider_locale'] or '')}")
    elif r["provider"] == "b":
        opts.append("PROVIDER = builtin")
        opts.append(f"LOCALE = {ql(r['provider_locale'] or '')}")
    else:  # 'c' — libc
        opts.append("PROVIDER = libc")
        if r["collcollate"] == r["collctype"]:
            opts.append(f"LOCALE = {ql(r['collcollate'])}")
        else:
            opts.append(f"LC_COLLATE = {ql(r['collcollate'])}")
            opts.append(f"LC_CTYPE = {ql(r['collctype'])}")
    if not r["deterministic"]:
        opts.append("DETERMINISTIC = false")
    return f"CREATE COLLATION IF NOT EXISTS {fqn} (" + ", ".join(opts) + ");"


async def sync_collations(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> None:
    """Create user-defined collations (``CREATE COLLATION``) on the target.

    Runs BEFORE types and tables: columns, domains and indexes may reference
    a user-defined collation, and without it their DDL fails outright (or,
    worse, a same-named collation with different rules resolves silently).
    Built-in collations all live in ``pg_catalog``, which is never in the
    migrated schema list, so only genuinely user-created collations match.

    ICU collations need the ICU locale available on the target's ICU build;
    libc collations need the OS locale installed — failures are logged as
    warnings (platform prerequisites, same policy as database locales).
    """
    # The ICU locale column moved twice: PG ≤14 stores it in collcollate
    # (shared with the libc locale), PG15 added colliculocale, PG17 renamed
    # it to colllocale.  Querying the wrong one is an UndefinedColumnError.
    src_major = source_conn.get_server_version().major
    if src_major >= 17:
        loc_col = "c.colllocale"
    elif src_major >= 15:
        loc_col = "c.colliculocale"
    else:
        loc_col = "c.collcollate"
    rows = await source_conn.fetch(
        _COLLATIONS_SQL_TMPL.format(loc_col=loc_col), schemas
    )
    for r in rows:
        schema, name = r["schema_name"], r["coll_name"]
        if r["provider"] == "d":
            log.debug("Skipping collation %s.%s (database-default provider)", schema, name)
            continue
        exists = await target_conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_collation c"
            " JOIN pg_namespace n ON n.oid = c.collnamespace"
            " WHERE n.nspname = $1 AND c.collname = $2)",
            schema, name,
        )
        if exists:
            continue
        try:
            await target_conn.execute(_collation_ddl(dict(r)))
            log.info("Created collation %s.%s", schema, name)
        except Exception as exc:
            log.warning(
                "Could not create collation %s.%s: %s — objects referencing "
                "it will fail to create; install the matching ICU/OS locale "
                "on the target and re-run 'detect-ddl --apply'.",
                schema, name, exc,
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
            # ql(): a label may legally contain a single quote — unescaped it
            # would break (or inject into) the generated DDL.
            labels_sql = ", ".join(ql(label) for label in labels)
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
                        f"ADD VALUE IF NOT EXISTS {ql(label)};"
                    )
                    log.info("Adding enum value '%s' to %s.%s", label, schema, type_name)
                    await target_conn.execute(ddl)


async def sync_composite_types(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> None:
    """Create missing composite (row) types on the target.

    Composite types may reference other composite types, so creation is
    attempted up to three times to resolve ordering dependencies.
    """
    rows = await source_conn.fetch(_COMPOSITE_TYPES_SQL, schemas)

    # Group columns by (schema_name, type_name) preserving attribute order
    type_cols: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for row in rows:
        key = (row["schema_name"], row["type_name"])
        if key not in type_cols:
            type_cols[key] = []
        type_cols[key].append((row["col_name"], row["col_type"]))

    if not type_cols:
        return

    pending = list(type_cols.items())
    for _attempt in range(3):
        if not pending:
            break
        failed = []
        for (schema, type_name), cols in pending:
            exists = await target_conn.fetchval(
                "SELECT EXISTS ("
                "  SELECT 1 FROM pg_type t"
                "  JOIN pg_namespace n ON n.oid = t.typnamespace"
                "  WHERE n.nspname = $1 AND t.typname = $2 AND t.typtype = 'c'"
                ")",
                schema, type_name,
            )
            if exists:
                continue
            col_defs = ", ".join(f"{qi(cn)} {ct}" for cn, ct in cols)
            ddl = f"CREATE TYPE {qi(schema)}.{qi(type_name)} AS ({col_defs});"
            try:
                await target_conn.execute(ddl)
                log.info("Created composite type %s.%s", schema, type_name)
            except Exception as exc:
                log.debug(
                    "Could not create composite type %s.%s (will retry): %s",
                    schema, type_name, exc,
                )
                failed.append(((schema, type_name), cols))
        pending = failed

    for (schema, type_name), _ in pending:
        log.warning(
            "Could not sync composite type %s.%s — check for unresolvable type dependencies",
            schema, type_name,
        )


_DOMAINS_SQL = """
SELECT
    n.nspname                                          AS schema_name,
    t.typname                                           AS type_name,
    pg_catalog.format_type(t.typbasetype, t.typtypmod)  AS base_type,
    t.typnotnull                                         AS not_null,
    t.typdefault                                         AS default_expr,
    CASE WHEN t.typcollation <> 0 AND t.typcollation <> bt.typcollation
         THEN co.collname END                            AS collation_name,
    CASE WHEN t.typcollation <> 0 AND t.typcollation <> bt.typcollation
         THEN co_n.nspname END                           AS collation_schema
FROM pg_type t
JOIN pg_namespace n ON n.oid = t.typnamespace
LEFT JOIN pg_type bt ON bt.oid = t.typbasetype
LEFT JOIN pg_collation co ON co.oid = t.typcollation
LEFT JOIN pg_namespace co_n ON co_n.oid = co.collnamespace
WHERE t.typtype = 'd'
  AND n.nspname = ANY($1::text[])
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.classid = 'pg_type'::regclass
        AND d.objid = t.oid
        AND d.deptype = 'e'
  )
ORDER BY n.nspname, t.typname;
"""

_DOMAIN_CONSTRAINTS_SQL = """
SELECT con.conname AS constraint_name, pg_get_constraintdef(con.oid) AS constraint_def
FROM pg_constraint con
JOIN pg_type t ON t.oid = con.contypid
JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname = $1 AND t.typname = $2
ORDER BY con.conname;
"""


async def get_domains(conn: asyncpg.Connection, schemas: list[str]) -> list[dict]:
    """Return domain types (``CREATE DOMAIN``) in the given schemas, each with
    its CHECK constraints under a ``constraints`` key."""
    domains = [dict(r) for r in await conn.fetch(_DOMAINS_SQL, schemas)]
    for d in domains:
        d["constraints"] = [
            dict(r) for r in await conn.fetch(
                _DOMAIN_CONSTRAINTS_SQL, d["schema_name"], d["type_name"]
            )
        ]
    return domains


def _domain_ddl(d: dict) -> str:
    fqn = f"{qi(d['schema_name'])}.{qi(d['type_name'])}"
    parts = [f"CREATE DOMAIN {fqn} AS {d['base_type']}"]
    if d.get("collation_name"):
        parts.append(
            f"COLLATE {_qualified_collation(d.get('collation_schema'), d['collation_name'])}"
        )
    if d.get("default_expr"):
        parts.append(f"DEFAULT {d['default_expr']}")
    if d["not_null"]:
        parts.append("NOT NULL")
    for c in d["constraints"]:
        parts.append(f"CONSTRAINT {qi(c['constraint_name'])} {c['constraint_def']}")
    return " ".join(parts) + ";"


async def sync_domains(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> None:
    """Create missing domain types (``CREATE DOMAIN``) on the target.

    A domain's base type may itself be another domain or a composite/enum
    type, so creation is retried up to three times to resolve ordering
    dependencies — the same approach used for composite types above.
    """
    domains = await get_domains(source_conn, schemas)
    if not domains:
        return

    pending = list(domains)
    for _attempt in range(3):
        if not pending:
            break
        failed = []
        for d in pending:
            schema, type_name = d["schema_name"], d["type_name"]
            exists = await target_conn.fetchval(
                "SELECT EXISTS ("
                "  SELECT 1 FROM pg_type t"
                "  JOIN pg_namespace n ON n.oid = t.typnamespace"
                "  WHERE n.nspname = $1 AND t.typname = $2 AND t.typtype = 'd'"
                ")",
                schema, type_name,
            )
            if exists:
                continue
            try:
                await target_conn.execute(_domain_ddl(d))
                log.info("Created domain %s.%s", schema, type_name)
            except Exception as exc:
                log.debug(
                    "Could not create domain %s.%s (will retry): %s", schema, type_name, exc,
                )
                failed.append(d)
        pending = failed

    for d in pending:
        log.warning(
            "Could not sync domain %s.%s — check for unresolvable type dependencies",
            d["schema_name"], d["type_name"],
        )


_RANGE_TYPES_SQL = """
SELECT
    n.nspname                                       AS schema_name,
    t.typname                                        AS type_name,
    pg_catalog.format_type(r.rngsubtype, NULL)       AS subtype,
    co.collname                                      AS collation_name,
    co_ns.nspname                                    AS collation_schema,
    canon_n.nspname                                  AS canonical_schema,
    canon.proname                                    AS canonical_func,
    diff_n.nspname                                   AS diff_schema,
    diff.proname                                     AS diff_func
FROM pg_range r
JOIN pg_type t ON t.oid = r.rngtypid
JOIN pg_namespace n ON n.oid = t.typnamespace
LEFT JOIN pg_collation co ON co.oid = r.rngcollation
LEFT JOIN pg_namespace co_ns ON co_ns.oid = co.collnamespace
LEFT JOIN pg_proc canon ON canon.oid = r.rngcanonical
LEFT JOIN pg_namespace canon_n ON canon_n.oid = canon.pronamespace
LEFT JOIN pg_proc diff ON diff.oid = r.rngsubdiff
LEFT JOIN pg_namespace diff_n ON diff_n.oid = diff.pronamespace
WHERE n.nspname = ANY($1::text[])
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.classid = 'pg_type'::regclass
        AND d.objid = t.oid
        AND d.deptype = 'e'
  )
ORDER BY n.nspname, t.typname;
"""


async def get_range_types(conn: asyncpg.Connection, schemas: list[str]) -> list[dict]:
    """Return user-defined range types (``CREATE TYPE … AS RANGE``) in the
    given schemas.  ``SUBTYPE_OPCLASS`` is not reproduced (the default
    operator class for the subtype is used) — rarely customised in practice.
    """
    return [dict(r) for r in await conn.fetch(_RANGE_TYPES_SQL, schemas)]


def _range_type_ddl(r: dict) -> str:
    fqn = f"{qi(r['schema_name'])}.{qi(r['type_name'])}"
    opts = [f"SUBTYPE = {r['subtype']}"]
    if r.get("collation_name"):
        opts.append(
            f"COLLATION = {_qualified_collation(r.get('collation_schema'), r['collation_name'])}"
        )
    if r.get("canonical_func"):
        opts.append(f"CANONICAL = {qi(r['canonical_schema'])}.{qi(r['canonical_func'])}")
    if r.get("diff_func"):
        opts.append(f"SUBTYPE_DIFF = {qi(r['diff_schema'])}.{qi(r['diff_func'])}")
    return f"CREATE TYPE {fqn} AS RANGE (" + ", ".join(opts) + ");"


async def sync_range_types(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> None:
    """Create missing range types (``CREATE TYPE … AS RANGE``) on the target.

    A range's subtype, or its CANONICAL/SUBTYPE_DIFF functions, may reference
    another custom type or a not-yet-created function, so creation is retried
    up to three times — same approach as composite types and domains.
    """
    ranges = await get_range_types(source_conn, schemas)
    if not ranges:
        return

    pending = list(ranges)
    for _attempt in range(3):
        if not pending:
            break
        failed = []
        for r in pending:
            schema, type_name = r["schema_name"], r["type_name"]
            exists = await target_conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM pg_range rg "
                "JOIN pg_type t ON t.oid = rg.rngtypid "
                "JOIN pg_namespace n ON n.oid = t.typnamespace "
                "WHERE n.nspname = $1 AND t.typname = $2)",
                schema, type_name,
            )
            if exists:
                continue
            try:
                await target_conn.execute(_range_type_ddl(r))
                log.info("Created range type %s.%s", schema, type_name)
            except Exception as exc:
                log.debug(
                    "Could not create range type %s.%s (will retry): %s", schema, type_name, exc,
                )
                failed.append(r)
        pending = failed

    for r in pending:
        log.warning(
            "Could not sync range type %s.%s — check for unresolvable dependencies",
            r["schema_name"], r["type_name"],
        )


_AGGREGATES_SQL = """
SELECT
    n.nspname                       AS schema_name,
    p.proname                       AS agg_name,
    pg_get_function_arguments(p.oid) AS args,
    a.aggkind::text                  AS aggkind,
    tf_n.nspname AS transfn_schema,  tf.proname AS transfn_name,
    ff_n.nspname AS finalfn_schema,  ff.proname AS finalfn_name,
    cf_n.nspname AS combinefn_schema, cf.proname AS combinefn_name,
    sf_n.nspname AS serialfn_schema, sf.proname AS serialfn_name,
    df_n.nspname AS deserialfn_schema, df.proname AS deserialfn_name,
    mtf_n.nspname AS mtransfn_schema, mtf.proname AS mtransfn_name,
    mif_n.nspname AS minvtransfn_schema, mif.proname AS minvtransfn_name,
    mff_n.nspname AS mfinalfn_schema, mff.proname AS mfinalfn_name,
    format_type(a.aggtranstype, NULL)  AS transtype,
    format_type(a.aggmtranstype, NULL) AS mtranstype,
    a.agginitval, a.aggminitval,
    a.aggfinalextra, a.aggmfinalextra,
    a.aggfinalmodify::text  AS finalmodify,
    a.aggmfinalmodify::text AS mfinalmodify,
    (a.aggsortop <> 0)      AS has_sortop,
    p.proparallel::text     AS parallel
FROM pg_aggregate a
JOIN pg_proc p ON p.oid = a.aggfnoid
JOIN pg_namespace n ON n.oid = p.pronamespace
LEFT JOIN pg_proc tf  ON tf.oid  = a.aggtransfn
LEFT JOIN pg_namespace tf_n  ON tf_n.oid  = tf.pronamespace
LEFT JOIN pg_proc ff  ON ff.oid  = a.aggfinalfn
LEFT JOIN pg_namespace ff_n  ON ff_n.oid  = ff.pronamespace
LEFT JOIN pg_proc cf  ON cf.oid  = a.aggcombinefn
LEFT JOIN pg_namespace cf_n  ON cf_n.oid  = cf.pronamespace
LEFT JOIN pg_proc sf  ON sf.oid  = a.aggserialfn
LEFT JOIN pg_namespace sf_n  ON sf_n.oid  = sf.pronamespace
LEFT JOIN pg_proc df  ON df.oid  = a.aggdeserialfn
LEFT JOIN pg_namespace df_n  ON df_n.oid  = df.pronamespace
LEFT JOIN pg_proc mtf ON mtf.oid = a.aggmtransfn
LEFT JOIN pg_namespace mtf_n ON mtf_n.oid = mtf.pronamespace
LEFT JOIN pg_proc mif ON mif.oid = a.aggminvtransfn
LEFT JOIN pg_namespace mif_n ON mif_n.oid = mif.pronamespace
LEFT JOIN pg_proc mff ON mff.oid = a.aggmfinalfn
LEFT JOIN pg_namespace mff_n ON mff_n.oid = mff.pronamespace
WHERE n.nspname = ANY($1::text[])
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.classid = 'pg_proc'::regclass
        AND d.objid = p.oid
        AND d.deptype = 'e'
  )
ORDER BY n.nspname, p.proname;
"""

_FINALMODIFY_KEYWORD = {"r": "READ_ONLY", "s": "SHAREABLE", "w": "READ_WRITE"}


async def get_aggregates(conn: asyncpg.Connection, schemas: list[str]) -> list[dict]:
    """Return user-defined aggregate functions (``CREATE AGGREGATE``) in the
    given schemas.  Ordered-set/hypothetical-set aggregates (``aggkind`` in
    ``'o'``, ``'h'``) are returned too but are not reproducible by
    :func:`sync_aggregates` — their very different ``WITHIN GROUP`` calling
    syntax is out of scope; callers should warn about them instead.
    """
    return [dict(r) for r in await conn.fetch(_AGGREGATES_SQL, schemas)]


def _aggregate_ddl(a: dict) -> str:
    fqn = f"{qi(a['schema_name'])}.{qi(a['agg_name'])}"
    opts = [
        f"SFUNC = {qi(a['transfn_schema'])}.{qi(a['transfn_name'])}",
        f"STYPE = {a['transtype']}",
    ]
    if a.get("finalfn_name"):
        opts.append(f"FINALFUNC = {qi(a['finalfn_schema'])}.{qi(a['finalfn_name'])}")
        if a["aggfinalextra"]:
            opts.append("FINALFUNC_EXTRA")
        opts.append(f"FINALFUNC_MODIFY = {_FINALMODIFY_KEYWORD.get(a['finalmodify'], 'READ_ONLY')}")
    if a.get("combinefn_name"):
        opts.append(f"COMBINEFUNC = {qi(a['combinefn_schema'])}.{qi(a['combinefn_name'])}")
    if a.get("serialfn_name"):
        opts.append(f"SERIALFUNC = {qi(a['serialfn_schema'])}.{qi(a['serialfn_name'])}")
    if a.get("deserialfn_name"):
        opts.append(f"DESERIALFUNC = {qi(a['deserialfn_schema'])}.{qi(a['deserialfn_name'])}")
    if a["agginitval"] is not None:
        opts.append(f"INITCOND = {ql(a['agginitval'])}")
    if a.get("mtransfn_name"):
        opts.append(f"MSFUNC = {qi(a['mtransfn_schema'])}.{qi(a['mtransfn_name'])}")
        opts.append(f"MINVFUNC = {qi(a['minvtransfn_schema'])}.{qi(a['minvtransfn_name'])}")
        opts.append(f"MSTYPE = {a['mtranstype']}")
        if a.get("mfinalfn_name"):
            opts.append(f"MFINALFUNC = {qi(a['mfinalfn_schema'])}.{qi(a['mfinalfn_name'])}")
            if a["aggmfinalextra"]:
                opts.append("MFINALFUNC_EXTRA")
            opts.append(
                f"MFINALFUNC_MODIFY = {_FINALMODIFY_KEYWORD.get(a['mfinalmodify'], 'READ_ONLY')}"
            )
        if a["aggminitval"] is not None:
            opts.append(f"MINITCOND = {ql(a['aggminitval'])}")
    if a["parallel"] == "s":
        opts.append("PARALLEL = SAFE")
    elif a["parallel"] == "r":
        opts.append("PARALLEL = RESTRICTED")
    return f"CREATE AGGREGATE {fqn}({a['args']}) (\n  " + ",\n  ".join(opts) + "\n);"


async def sync_aggregates(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> None:
    """Create missing user-defined aggregates (``CREATE AGGREGATE``) on the
    target — normal (non-ordered-set) aggregates only.

    ``pg_get_functiondef()`` does not support aggregates, so the DDL is
    hand-built from ``pg_aggregate``: SFUNC/STYPE/FINALFUNC/COMBINEFUNC/
    SERIALFUNC/DESERIALFUNC/INITCOND, the moving-aggregate (``M…``) variants,
    and PARALLEL are covered.  ``SORTOP`` (used by e.g. hand-rolled MIN/MAX
    replacements) is not reproduced.  Ordered-set and hypothetical-set
    aggregates (``WITHIN GROUP``) use different, unsupported syntax and are
    skipped with a warning rather than emitting DDL that would fail.

    Must run after the transition/final/combine functions it references
    already exist on the target.
    """
    aggs = await get_aggregates(source_conn, schemas)
    for a in aggs:
        schema, name, args = a["schema_name"], a["agg_name"], a["args"]
        if a["aggkind"] != "n":
            log.warning(
                "Skipping ordered-set/hypothetical-set aggregate %s.%s(%s) — "
                "not reproducible by pg_emigrant (unsupported WITHIN GROUP syntax); "
                "recreate it manually on the target if needed.",
                schema, name, args,
            )
            continue
        exists = await target_conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_aggregate a "
            "JOIN pg_proc p ON p.oid = a.aggfnoid "
            "JOIN pg_namespace n ON n.oid = p.pronamespace "
            "WHERE n.nspname = $1 AND p.proname = $2 "
            "AND pg_get_function_arguments(p.oid) = $3)",
            schema, name, args,
        )
        if exists:
            continue
        try:
            await target_conn.execute(_aggregate_ddl(a))
            log.info("Created aggregate %s.%s(%s)", schema, name, args)
        except Exception as exc:
            log.warning(
                "Could not create aggregate %s.%s(%s): %s", schema, name, args, exc,
            )


async def _exec_with_lock_timeout(
    conn: asyncpg.Connection,
    stmt: str,
    *,
    timeout: str = "5s",
    attempts: int = 5,
    base_delay: float = 2.0,
) -> None:
    """Execute DDL that takes a strong lock, bounded by lock_timeout and retried.

    ACCESS EXCLUSIVE DDL issued without a lock_timeout queues behind any
    long-running query — and every later statement on that table then queues
    behind *it*.  On a live production source that is an outage, not a wait.
    With the timeout the ALTER gives up after *timeout*, releases the queue,
    and is retried with exponential backoff instead.
    """
    await conn.execute(f"SET lock_timeout = {ql(timeout)};")
    try:
        for attempt in range(attempts):
            try:
                await conn.execute(stmt)
                return
            except asyncpg.LockNotAvailableError:
                if attempt == attempts - 1:
                    raise
                delay = base_delay * (2 ** attempt)
                log.warning(
                    "Lock timeout (%s) on: %s — retrying in %.0fs (attempt %d/%d)",
                    timeout, stmt.strip(), delay, attempt + 1, attempts,
                )
                await asyncio.sleep(delay)
    finally:
        await conn.execute("RESET lock_timeout;")


async def sync_replica_identity(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> None:
    """Set REPLICA IDENTITY FULL on tables that have no primary key.

    Logical replication requires a way to identify updated/deleted rows.
    Without a PK (or a unique index marked as replica identity), PostgreSQL
    rejects UPDATE/DELETE on the *publisher* the moment the table is
    published.  REPLICA IDENTITY FULL makes PostgreSQL include all columns in
    the WAL record so rows can be matched by value.  This is set on BOTH
    source and target so the publication and subscription are consistent.

    Tables whose replica identity is already usable are left untouched:
    ``relreplident = 'f'`` (already FULL) and ``'i'`` (a deliberately
    configured ``USING INDEX`` identity, which works for logical replication
    — overwriting it with FULL would only inflate WAL).  Only ``'d'``
    (default, with no PK) and ``'n'`` (NOTHING) get FULL — both would
    otherwise make UPDATE/DELETE start failing on the source once published.

    The ALTER takes an ACCESS EXCLUSIVE lock, so it runs under a bounded
    ``lock_timeout`` with retries (see :func:`_exec_with_lock_timeout`) —
    on the production *source*, an unbounded lock wait is an outage.

    Must run on the SOURCE before the replication slot is created: REPLICA
    IDENTITY is evaluated at the moment each UPDATE/DELETE is written to WAL,
    not at publication/slot-creation time, so any such statement on a PK-less
    table decoded from a slot created before this ALTER would carry no old-row
    identity and the subscriber would reject it.  Requires the target tables
    to already exist, so it must run after (pre-copy) ``sync_schemas``.
    """
    no_pk_tables = await source_conn.fetch(
        """
        SELECT n.nspname AS schema_name, c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY($1::text[])
          AND c.relkind = 'r'
          AND c.relreplident IN ('d', 'n')
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
                await _exec_with_lock_timeout(conn, stmt)
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
) -> set[str]:
    """Ensure all extensions present on the source are installed on the target.

    Extensions like btree_gin or pg_trgm provide operator classes required by
    indexes.  Without them, creating GIN indexes on types like UUID fails with
    'no default operator class'.  Failures are logged as warnings because some
    extensions (e.g. timescaledb) may not be available on the target system.

    Returns the set of schema names that are owned by extensions on the source.
    These schemas must NOT be pre-created via ensure_schema_exists — the extension
    install is responsible for creating them.
    """
    rows = await source_conn.fetch(
        """
        SELECT e.extname, n.nspname AS extschema
        FROM pg_extension e
        LEFT JOIN pg_namespace n ON n.oid = e.extnamespace
        WHERE e.extname NOT IN ('plpgsql')
        ORDER BY e.extname
        """
    )
    ext_schemas: set[str] = set()
    for row in rows:
        extname = row["extname"]
        extschema: str | None = row["extschema"]
        if extschema:
            ext_schemas.add(extschema)
        try:
            if extschema:
                # WITH SCHEMA does NOT create the schema itself — unlike most
                # object-creation DDL, CREATE EXTENSION requires it to already
                # exist.  Pre-create it so relocatable extensions installed
                # into a non-default schema on the source (a common pattern
                # for keeping extension objects out of `public`) land in the
                # same schema on the target — without this, the extension
                # silently lands in the connection's default schema and any
                # source DDL qualified as `extschema.func(...)` fails on the
                # target.  For extensions that auto-create their own schema
                # via a _PG_init hook (e.g. `anon`), this pre-created schema
                # is empty and gets picked up by the orphaned-schema fallback
                # below instead of conflicting with it.
                await target_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {qi(extschema)};")
                await target_conn.execute(
                    f"CREATE EXTENSION IF NOT EXISTS {qi(extname)} WITH SCHEMA {qi(extschema)};"
                )
            else:
                await target_conn.execute(
                    f"CREATE EXTENSION IF NOT EXISTS {qi(extname)};"
                )
            log.debug("Ensured extension %s is installed", extname)
        except Exception as exc:
            # Some extensions (e.g. anon with shared_preload_libraries) pre-create
            # their schema via a _PG_init hook before CREATE EXTENSION is run.
            # PostgreSQL then refuses to let the extension adopt the pre-existing
            # schema.  Detect this case: if the schema exists on the target but is
            # NOT owned by any extension, drop it (only if empty) and retry.
            if extschema and "is not a member of extension" in str(exc):
                schema_orphaned = await target_conn.fetchval(
                    """
                    SELECT n.oid IS NOT NULL
                    FROM pg_namespace n
                    LEFT JOIN pg_extension e ON e.extnamespace = n.oid
                    WHERE n.nspname = $1 AND e.extname IS NULL
                    """,
                    extschema,
                )
                if schema_orphaned:
                    try:
                        await target_conn.execute(
                            f"DROP SCHEMA IF EXISTS {qi(extschema)};"
                        )
                        await target_conn.execute(
                            f"CREATE EXTENSION IF NOT EXISTS {qi(extname)};"
                        )
                        log.info(
                            "Installed extension %s after dropping orphaned schema %s "
                            "(pre-created by shared_preload_libraries)",
                            extname, extschema,
                        )
                        continue
                    except Exception as exc2:
                        log.warning(
                            "Could not install extension %s after dropping orphaned schema %s: %s",
                            extname, extschema, exc2,
                        )
                        continue
            log.warning("Could not install extension %s: %s", extname, exc)
    return ext_schemas


async def sync_schemas(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> None:
    """Pre-copy schema sync: extensions → types → sequences → tables (PK/UNIQUE/CHECK only).

    Deliberately excludes FK constraints, triggers, and views — those are applied
    after the initial data COPY by ``sync_post_copy_constraints``.  This mirrors
    the pg_dump pre-data / data / post-data split:

    * FK constraints created post-COPY let PostgreSQL validate referential
      integrity across the whole dataset, turning silent copy failures into
      hard errors.
    * Triggers created post-COPY avoid side-effects during bulk insert
      (triggers with ENABLE ALWAYS fire even under session_replication_role=replica).
    """
    # Sync extensions FIRST — extensions like timescaledb / anon create their own
    # schemas on installation; creating those schemas beforehand causes the install
    # to fail.  sync_extensions returns the set of extension-owned schema names so
    # we can skip them in the ensure_schema_exists loop below.
    ext_schemas = await sync_extensions(source_conn, target_conn)

    # Ensure remaining target schemas exist; skip extension-owned ones —
    # they were (or should have been) created by the extension install above.
    for s in schemas:
        if s in ext_schemas:
            log.debug("Skipping ensure_schema_exists for %s (owned by extension on source)", s)
            continue
        await ensure_schema_exists(target_conn, s)

    # User-defined collations BEFORE types and tables — columns, domains and
    # indexes may reference them.
    await sync_collations(source_conn, target_conn, schemas)

    # Sync enum types BEFORE tables so columns with enum types can be created
    await sync_enum_types(source_conn, target_conn, schemas)

    # Sync composite types BEFORE tables — table columns may use them as their data type
    await sync_composite_types(source_conn, target_conn, schemas)

    # Sync domains and range types BEFORE tables — same reason.  Domains may
    # in turn be built on composite/enum types, and ranges on domains, so
    # these run after (not interleaved with) the two calls above.
    await sync_domains(source_conn, target_conn, schemas)
    await sync_range_types(source_conn, target_conn, schemas)

    # Sync sequences FIRST so that table DDL with DEFAULT nextval(...) succeeds.
    # Identity-backed sequences are deliberately skipped: the table's identity
    # column creates its own sequence, and pre-creating the name here would make
    # CREATE TABLE silently pick a "…_seq1" name for the real identity sequence —
    # a phantom that sequence value sync (matching by name) never updates,
    # causing duplicate-key errors after cutover.  Their values are synced by the
    # final sequence pass at the end of bootstrap, once the tables exist.
    sequences = await get_sequences(source_conn, schemas)
    for seq in sequences:
        if seq.get("is_identity"):
            log.debug(
                "Skipping pre-creation of identity sequence %s.%s (created by its table)",
                seq["schema_name"], seq["sequence_name"],
            )
            continue
        await sync_sequence(
            source_conn, target_conn,
            seq["schema_name"], seq["sequence_name"],
        )

    # Silent pre-pass: try functions before tables so that generated columns
    # or DEFAULT expressions that call functions can be created.  Most functions
    # will fail here (they reference tables not yet created) — that's expected
    # and logged only at DEBUG level.
    await sync_functions(source_conn, target_conn, schemas, silent=True)

    # Classic (non-declarative) INHERITS hierarchies are reproduced as
    # independent tables — the inheritance link itself is NOT recreated on the
    # target, so parent-table SELECTs there will not include child rows.
    # (Rows are still copied correctly per table thanks to FROM ONLY.)
    inherits_rows = await source_conn.fetch(
        """
        SELECT DISTINCT n.nspname AS schema_name, c.relname AS table_name
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE NOT c.relispartition
          AND n.nspname = ANY($1::text[])
        ORDER BY 1, 2
        """,
        schemas,
    )
    if inherits_rows:
        log.warning(
            "Classic INHERITS child table(s) detected on source: %s. "
            "The inheritance link is NOT recreated on the target — SELECTs on "
            "the parent table there will not include child rows. Review before cutover.",
            ", ".join(f"{r['schema_name']}.{r['table_name']}" for r in inherits_rows),
        )

    # Create tables with PK + UNIQUE + CHECK constraints.
    # FK constraints are intentionally deferred to sync_post_copy_constraints.
    tables = await get_tables(source_conn, schemas)
    for t in tables:
        await _sync_table_structure(
            source_conn, target_conn, t["schema_name"], t["table_name"],
        )

    # Second function pass now that tables exist.
    await sync_functions(source_conn, target_conn, schemas, silent=True)

    # Aggregates reference functions (SFUNC/FINALFUNC/…), so they run after
    # both function passes; they in turn may be referenced by post-copy views.
    await sync_aggregates(source_conn, target_conn, schemas)

    log.info("Pre-copy schema sync complete for schemas: %s", schemas)


async def sync_post_copy_constraints(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
) -> dict[str, list[str]]:
    """Post-copy phase: FK constraints, functions, views, and triggers.

    Called after the initial data COPY and non-unique index build.  Adding FK
    constraints at this point causes PostgreSQL to validate referential integrity
    across the entire copied dataset — a failed or partial table copy surfaces as
    a hard error here rather than silently leaving corrupt data behind.

    Ordering matters: triggers are created LAST, after the final function pass,
    so a trigger never fails just because its function could not be created yet
    (functions may in turn reference views, hence functions → views → functions).

    Note: ``REPLICA IDENTITY FULL`` (for PK-less tables) is NOT handled here —
    it must be set on the source *before* the replication slot is created (see
    ``sync_replica_identity`` and ``bootstrap.py``), otherwise UPDATE/DELETE on
    a PK-less table decoded from WAL between slot creation and the ALTER would
    carry no old-row identity and the subscriber would reject it.

    Returns a dict of object-sync failures, keyed by object kind
    (``function`` / ``view`` / ``trigger``), each a list of
    ``"identifier: error"`` strings.  Empty lists mean full success.
    """
    tables = await get_tables(source_conn, schemas)

    # FK constraints — pg_get_constraintdef returns unqualified table names, so
    # set search_path to all migrated schemas before executing FK DDL.
    fk_failures: list[str] = []
    _sp = ", ".join(qi(s) for s in schemas) + ", public"
    await target_conn.execute(f"SET search_path TO {_sp};")
    try:
        for t in tables:
            fk_failures.extend(
                await _sync_foreign_keys(
                    source_conn, target_conn, t["schema_name"], t["table_name"]
                )
            )
    finally:
        await target_conn.execute("RESET search_path;")

    # Functions before views: with check_function_bodies=off nearly all succeed
    # here; silent because BEGIN ATOMIC bodies referencing views legitimately
    # need the retry after views exist.
    await sync_functions(source_conn, target_conn, schemas, silent=True)

    # Views depend on tables and functions; create them after both exist.
    view_failures = await sync_views(source_conn, target_conn, schemas)

    # Final function pass — functions referencing views or other post-copy objects.
    func_failures = await sync_functions(source_conn, target_conn, schemas)

    # Triggers — created after COPY to avoid side-effects during bulk
    # insert, and after every function pass so their functions already exist.
    # (Triggers with ENABLE ALWAYS fire even under session_replication_role=replica.)
    trigger_failures = await sync_triggers(source_conn, target_conn, schemas)

    # Row-level security LAST, after the copy for the same reason as triggers
    # (see sync_row_security's docstring: FORCE ROW LEVEL SECURITY enabled too
    # early could block a non-superuser migration role's own bulk COPY).
    policy_failures = await sync_row_security(source_conn, target_conn, schemas)

    log.info("Post-copy constraint sync complete for schemas: %s", schemas)
    return {
        "foreign_key": fk_failures,
        "function": func_failures,
        "view": view_failures,
        "trigger": trigger_failures,
        "policy": policy_failures,
    }


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

    part = await _get_partition_info(source_conn, schema, table)

    # Partition children inherit columns, PK, constraints and indexes from the
    # parent.  Attach them (the parent is created first because the table list
    # is ordered by partition depth) and skip the per-table constraint/index
    # work that would otherwise fail with duplicate-object errors.
    if part.get("is_partition"):
        if not exists:
            ddl = await _generate_create_table_ddl(source_conn, schema, table)
            log.info("Attaching partition %s", fqn)
            await target_conn.execute(ddl)
        return

    if not exists:
        ddl = await _generate_create_table_ddl(source_conn, schema, table)
        log.info("Creating table %s", fqn)
        await target_conn.execute(ddl)
    else:
        # Add missing columns (only for tables / partitioned tables, not views)
        obj_kind = await target_conn.fetchval(
            "SELECT c.relkind::text FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = $1 AND c.relname = $2",
            schema, table,
        )
        if obj_kind not in ("r", "p"):
            log.debug("Skipping column/constraint sync for non-table %s (kind=%s)", fqn, obj_kind)
            return

        src_cols = await get_columns(source_conn, fqn)
        tgt_cols = await get_columns(target_conn, fqn)
        tgt_col_names = {c["column_name"] for c in tgt_cols}

        for col in src_cols:
            if col["column_name"] not in tgt_col_names:
                if col.get("identity") in ("a", "d"):
                    identity_clause = (
                        "GENERATED ALWAYS AS IDENTITY"
                        if col["identity"] == "a"
                        else "GENERATED BY DEFAULT AS IDENTITY"
                    )
                    if col.get("identity_options"):
                        identity_clause += f" ({col['identity_options']})"
                    col_def = f"{col['data_type']} {identity_clause}"
                elif _is_nextval_default(col["column_default"]):
                    parts = [col["data_type"], _collate_clause(col).strip()]
                    if col["not_null"]:
                        parts.append("NOT NULL")
                    parts.append(f"DEFAULT {col['column_default']}")
                    col_def = " ".join(p for p in parts if p)
                elif col.get("is_generated"):
                    col_def = (
                        f"{col['data_type']} {_collate_clause(col)}"
                        f"GENERATED ALWAYS AS ({col['column_default']}) STORED"
                    )
                else:
                    parts = [col["data_type"], _collate_clause(col).strip()]
                    # The DEFAULT must be kept for nullable columns too —
                    # dropping it here would make post-cutover inserts get
                    # NULL where the source produced the default value.
                    if col["column_default"] is not None:
                        parts.append(f"DEFAULT {col['column_default']}")
                    if col["not_null"]:
                        parts.append("NOT NULL")
                    col_def = " ".join(p for p in parts if p)
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
        is_fk = c["constraint_type"] == "f"
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
    p.prokind::text                                  AS prokind,
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
    if kind in ("view", "materialized_view"):
        # ALTER TABLE works for both regular views and materialized views,
        # avoiding DDL failures when source/target disagree on the relkind.
        return f"ALTER TABLE {qi(schema)}.{qi(obj)} OWNER TO {qi(owner)};"
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
        if tgt_owner is None:
            # Object exists on source but not on target (e.g. a TimescaleDB
            # continuous aggregate filtered out by sync_views, or a view that
            # failed to be created).  Ownership cannot be set on a non-existent
            # object — skip silently.
            log.debug("Skipping ownership of %s %s.%s — not present on target", kind, schema, obj)
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


# ---------------------------------------------------------------------------
# Privileges (GRANT / ACL)
# ---------------------------------------------------------------------------
# The object owner's own privileges are excluded from every query below (they
# are implicit in PostgreSQL and already reconciled by sync_ownership above —
# explicitly re-granting them is redundant and, if a target role happens to
# reuse the source owner's name for something else, actively wrong).  Only
# ADDITIONAL grants beyond the owner's own are meaningful ACL customisation.

_TABLE_ACL_SQL = """
SELECT
    n.nspname                      AS schema_name,
    c.relname                      AS object_name,
    CASE c.relkind
        WHEN 'r' THEN 'table'
        WHEN 'p' THEN 'table'
        WHEN 'v' THEN 'view'
        WHEN 'm' THEN 'materialized_view'
        WHEN 'S' THEN 'sequence'
    END                             AS kind,
    COALESCE(gr.rolname, 'PUBLIC') AS grantee,
    a.privilege_type,
    a.is_grantable
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL aclexplode(c.relacl) AS a(grantor, grantee, privilege_type, is_grantable)
LEFT JOIN pg_roles gr ON gr.oid = a.grantee
WHERE n.nspname = ANY($1::text[])
  AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
  AND c.relacl IS NOT NULL
  AND a.grantee <> c.relowner
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.classid = 'pg_class'::regclass
        AND d.objid = c.oid
        AND d.deptype = 'e'
  )
ORDER BY 1, 2, 4, 5;
"""

_SCHEMA_ACL_SQL = """
SELECT
    n.nspname                      AS schema_name,
    COALESCE(gr.rolname, 'PUBLIC') AS grantee,
    a.privilege_type,
    a.is_grantable
FROM pg_namespace n
CROSS JOIN LATERAL aclexplode(n.nspacl) AS a(grantor, grantee, privilege_type, is_grantable)
LEFT JOIN pg_roles gr ON gr.oid = a.grantee
WHERE n.nspname = ANY($1::text[])
  AND n.nspacl IS NOT NULL
  AND a.grantee <> n.nspowner
ORDER BY 1, 2, 3;
"""

_FUNCTION_ACL_SQL = """
SELECT
    n.nspname                                        AS schema_name,
    p.proname                                        AS func_name,
    pg_get_function_identity_arguments(p.oid)        AS func_args,
    p.prokind::text                                  AS prokind,
    COALESCE(gr.rolname, 'PUBLIC')                   AS grantee,
    a.privilege_type,
    a.is_grantable
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
CROSS JOIN LATERAL aclexplode(p.proacl) AS a(grantor, grantee, privilege_type, is_grantable)
LEFT JOIN pg_roles gr ON gr.oid = a.grantee
WHERE n.nspname = ANY($1::text[])
  AND p.prokind IN ('f', 'p')
  AND p.proacl IS NOT NULL
  AND a.grantee <> p.proowner
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.classid = 'pg_proc'::regclass
        AND d.objid = p.oid
        AND d.deptype = 'e'
  )
ORDER BY 1, 2, 3, 5, 6;
"""

_TYPE_ACL_SQL = """
SELECT
    n.nspname                      AS schema_name,
    t.typname                      AS type_name,
    COALESCE(gr.rolname, 'PUBLIC') AS grantee,
    a.privilege_type,
    a.is_grantable
FROM pg_type t
JOIN pg_namespace n ON n.oid = t.typnamespace
CROSS JOIN LATERAL aclexplode(t.typacl) AS a(grantor, grantee, privilege_type, is_grantable)
LEFT JOIN pg_roles gr ON gr.oid = a.grantee
WHERE n.nspname = ANY($1::text[])
  AND t.typacl IS NOT NULL
  AND a.grantee <> t.typowner
  AND t.typname NOT LIKE '\\_%'
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.classid = 'pg_type'::regclass
        AND d.objid = t.oid
        AND d.deptype = 'e'
  )
ORDER BY 1, 2, 3, 4;
"""

_DATABASE_ACL_SQL = """
SELECT
    COALESCE(gr.rolname, 'PUBLIC') AS grantee,
    a.privilege_type,
    a.is_grantable
FROM pg_database d
CROSS JOIN LATERAL aclexplode(d.datacl) AS a(grantor, grantee, privilege_type, is_grantable)
LEFT JOIN pg_roles gr ON gr.oid = a.grantee
WHERE d.datname = $1
  AND d.datacl IS NOT NULL
  AND a.grantee <> d.datdba
ORDER BY 1, 2;
"""

# ALTER DEFAULT PRIVILEGES entries.  defaclnamespace = 0 means a global
# default (not scoped to any schema) — schema_name is NULL for those rows.
_DEFAULT_ACL_SQL = """
SELECT
    n.nspname                      AS schema_name,
    r.rolname                      AS for_role,
    d.defaclobjtype::text          AS objtype,
    COALESCE(gr.rolname, 'PUBLIC') AS grantee,
    a.privilege_type,
    a.is_grantable
FROM pg_default_acl d
JOIN pg_roles r ON r.oid = d.defaclrole
LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace
CROSS JOIN LATERAL aclexplode(d.defaclacl) AS a(grantor, grantee, privilege_type, is_grantable)
LEFT JOIN pg_roles gr ON gr.oid = a.grantee
WHERE d.defaclnamespace = 0 OR n.nspname = ANY($1::text[])
ORDER BY 1, 2, 3, 4, 5;
"""

_DEFAULT_ACL_OBJTYPE_KEYWORD = {
    "r": "TABLES",
    "S": "SEQUENCES",
    "f": "FUNCTIONS",
    "T": "TYPES",
    "n": "SCHEMAS",
}


async def get_privileges(
    conn: asyncpg.Connection,
    schemas: list[str],
    *,
    dbname: str | None = None,
) -> list[dict]:
    """Return non-owner ACL entries for all schema objects in *schemas*.

    Covers tables/views/materialized views/sequences, schemas, functions,
    procedures, user-defined types, ``ALTER DEFAULT PRIVILEGES`` entries, and
    (when *dbname* is given) the database itself.  The object owner's own
    privileges are excluded — see the module comment above.
    """
    rows: list[dict] = []

    for r in await conn.fetch(_TABLE_ACL_SQL, schemas):
        rows.append({
            "acl_kind": "relation", "kind": r["kind"],
            "schema_name": r["schema_name"], "object_name": r["object_name"],
            "grantee": r["grantee"], "privilege_type": r["privilege_type"],
            "is_grantable": r["is_grantable"],
        })

    for r in await conn.fetch(_SCHEMA_ACL_SQL, schemas):
        rows.append({
            "acl_kind": "schema", "kind": "schema",
            "schema_name": r["schema_name"], "object_name": r["schema_name"],
            "grantee": r["grantee"], "privilege_type": r["privilege_type"],
            "is_grantable": r["is_grantable"],
        })

    for r in await conn.fetch(_FUNCTION_ACL_SQL, schemas):
        rows.append({
            "acl_kind": "function",
            "kind": "procedure" if r["prokind"] == "p" else "function",
            "schema_name": r["schema_name"],
            "object_name": f"{r['func_name']}({r['func_args']})",
            "func_name": r["func_name"], "func_args": r["func_args"],
            "grantee": r["grantee"], "privilege_type": r["privilege_type"],
            "is_grantable": r["is_grantable"],
        })

    for r in await conn.fetch(_TYPE_ACL_SQL, schemas):
        rows.append({
            "acl_kind": "type", "kind": "type",
            "schema_name": r["schema_name"], "object_name": r["type_name"],
            "grantee": r["grantee"], "privilege_type": r["privilege_type"],
            "is_grantable": r["is_grantable"],
        })

    for r in await conn.fetch(_DEFAULT_ACL_SQL, schemas):
        schema_name = r["schema_name"] or ""
        rows.append({
            "acl_kind": "default", "kind": "default_privilege",
            "schema_name": schema_name,
            # Synthetic object key — (for_role, objtype) is unique per schema
            # in pg_default_acl, so this composes correctly with schema_name
            # for the diff key used by sync_privileges().
            "object_name": f"{r['for_role']}:{r['objtype']}",
            "for_role": r["for_role"], "objtype": r["objtype"],
            "grantee": r["grantee"], "privilege_type": r["privilege_type"],
            "is_grantable": r["is_grantable"],
        })

    if dbname:
        for r in await conn.fetch(_DATABASE_ACL_SQL, dbname):
            rows.append({
                "acl_kind": "database", "kind": "database",
                "schema_name": "", "object_name": dbname,
                "grantee": r["grantee"], "privilege_type": r["privilege_type"],
                "is_grantable": r["is_grantable"],
            })

    return rows


def make_grant_ddl(rec: dict) -> str:
    """Generate the GRANT (or ALTER DEFAULT PRIVILEGES … GRANT) statement for
    one non-owner ACL record from :func:`get_privileges`."""
    priv = rec["privilege_type"]
    grantee = "PUBLIC" if rec["grantee"] == "PUBLIC" else qi(rec["grantee"])
    grant_option = " WITH GRANT OPTION" if rec["is_grantable"] else ""
    kind = rec["acl_kind"]

    if kind == "relation":
        obj_kw = "SEQUENCE" if rec["kind"] == "sequence" else "TABLE"
        fqn = f"{qi(rec['schema_name'])}.{qi(rec['object_name'])}"
        return f"GRANT {priv} ON {obj_kw} {fqn} TO {grantee}{grant_option};"
    if kind == "schema":
        return f"GRANT {priv} ON SCHEMA {qi(rec['schema_name'])} TO {grantee}{grant_option};"
    if kind == "function":
        return (
            f"GRANT {priv} ON ROUTINE {qi(rec['schema_name'])}.{qi(rec['func_name'])}"
            f"({rec['func_args']}) TO {grantee}{grant_option};"
        )
    if kind == "type":
        fqn = f"{qi(rec['schema_name'])}.{qi(rec['object_name'])}"
        return f"GRANT {priv} ON TYPE {fqn} TO {grantee}{grant_option};"
    if kind == "database":
        return f"GRANT {priv} ON DATABASE {qi(rec['object_name'])} TO {grantee}{grant_option};"
    if kind == "default":
        obj_kw = _DEFAULT_ACL_OBJTYPE_KEYWORD.get(rec["objtype"])
        if not obj_kw:
            return ""
        scope = f"IN SCHEMA {qi(rec['schema_name'])} " if rec["schema_name"] else ""
        return (
            f"ALTER DEFAULT PRIVILEGES FOR ROLE {qi(rec['for_role'])} {scope}"
            f"GRANT {priv} ON {obj_kw} TO {grantee}{grant_option};"
        )
    return ""


def make_revoke_ddl(rec: dict) -> str:
    """Generate the REVOKE (or ALTER DEFAULT PRIVILEGES … REVOKE) statement
    that would remove one non-owner ACL record from :func:`get_privileges`.

    Used only by drift detection's ``missing_on_source`` direction (a grant
    present on the target but not the source) — never applied automatically
    by :func:`sync_privileges`, only via ``detect-ddl --apply --drop-extra``.
    """
    priv = rec["privilege_type"]
    grantee = "PUBLIC" if rec["grantee"] == "PUBLIC" else qi(rec["grantee"])
    kind = rec["acl_kind"]

    if kind == "relation":
        obj_kw = "SEQUENCE" if rec["kind"] == "sequence" else "TABLE"
        fqn = f"{qi(rec['schema_name'])}.{qi(rec['object_name'])}"
        return f"REVOKE {priv} ON {obj_kw} {fqn} FROM {grantee};"
    if kind == "schema":
        return f"REVOKE {priv} ON SCHEMA {qi(rec['schema_name'])} FROM {grantee};"
    if kind == "function":
        return (
            f"REVOKE {priv} ON ROUTINE {qi(rec['schema_name'])}.{qi(rec['func_name'])}"
            f"({rec['func_args']}) FROM {grantee};"
        )
    if kind == "type":
        fqn = f"{qi(rec['schema_name'])}.{qi(rec['object_name'])}"
        return f"REVOKE {priv} ON TYPE {fqn} FROM {grantee};"
    if kind == "database":
        return f"REVOKE {priv} ON DATABASE {qi(rec['object_name'])} FROM {grantee};"
    if kind == "default":
        obj_kw = _DEFAULT_ACL_OBJTYPE_KEYWORD.get(rec["objtype"])
        if not obj_kw:
            return ""
        scope = f"IN SCHEMA {qi(rec['schema_name'])} " if rec["schema_name"] else ""
        return (
            f"ALTER DEFAULT PRIVILEGES FOR ROLE {qi(rec['for_role'])} {scope}"
            f"REVOKE {priv} ON {obj_kw} FROM {grantee};"
        )
    return ""


async def sync_privileges(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    schemas: list[str],
    *,
    dbname: str | None = None,
) -> int:
    """Grant every non-owner privilege present on the source but missing on
    the target: tables/views/materialized views, sequences, schemas (e.g.
    ``USAGE``), functions/procedures (``EXECUTE``), user-defined types,
    ``ALTER DEFAULT PRIVILEGES`` entries, and — when *dbname* is given — the
    database itself.

    Only ever ADDS grants; it never REVOKEs a privilege the target has that
    the source doesn't — narrowing access automatically is judged too risky
    for an unattended tool.  ``detect-ddl --apply --drop-extra`` surfaces (and
    can apply) that direction explicitly instead, consistent with how every
    other "extra on target" case is already handled.

    Grantee roles that don't exist on the target are skipped with a warning —
    create them manually beforehand if needed (same policy as
    :func:`sync_ownership`).

    Returns the number of statements applied.
    """
    src_list = await get_privileges(source_conn, schemas, dbname=dbname)
    tgt_list = await get_privileges(target_conn, schemas, dbname=dbname)

    def _key(r: dict) -> tuple:
        return (
            r["acl_kind"], r["schema_name"], r["object_name"],
            r["grantee"], r["privilege_type"], r["is_grantable"],
        )

    tgt_keys = {_key(r) for r in tgt_list}

    existing_roles = {
        r["rolname"]
        for r in await target_conn.fetch("SELECT rolname FROM pg_roles")
    }
    existing_roles.add("PUBLIC")

    applied = 0
    for rec in src_list:
        if _key(rec) in tgt_keys:
            continue
        if rec["grantee"] not in existing_roles:
            log.warning(
                "Skipping grant of %s on %s %s — role '%s' does not exist on target",
                rec["privilege_type"], rec["kind"], rec["object_name"], rec["grantee"],
            )
            continue
        stmt = make_grant_ddl(rec)
        if not stmt:
            continue
        try:
            await target_conn.execute(stmt)
            log.info(
                "Granted %s on %s %s to %s",
                rec["privilege_type"], rec["kind"], rec["object_name"], rec["grantee"],
            )
            applied += 1
        except Exception as exc:
            log.warning(
                "Could not grant %s on %s %s to %s — %s",
                rec["privilege_type"], rec["kind"], rec["object_name"], rec["grantee"], exc,
            )

    return applied


# GUCs whose stored value is already a formatted, individually-quoted list
# (pg_dump's GUC_LIST_QUOTE set).  Their setconfig value must be emitted
# verbatim, NOT wrapped in a single string literal — quoting the whole thing
# would bind e.g. search_path to ONE schema literally named '"$user", public'.
_GUC_LIST_QUOTE = {
    "search_path",
    "session_preload_libraries",
    "shared_preload_libraries",
    "local_preload_libraries",
    "temp_tablespaces",
    "unix_socket_directories",
}


async def sync_db_settings(
    source_conn: asyncpg.Connection,
    target_conn: asyncpg.Connection,
    dbname: str,
) -> int:
    """Reproduce per-database configuration from ``pg_db_role_setting``:
    ``ALTER DATABASE … SET`` and ``ALTER ROLE … IN DATABASE … SET``.

    Logical replication never carries these, and pg_dump only emits them via
    pg_dumpall — yet applications routinely rely on them (a per-database
    ``search_path`` being the classic case); missing them surfaces only after
    cutover, as runtime misbehaviour rather than an error.

    Overwrite/add-only: source values are applied; settings that exist only
    on the target are left alone (same policy as privilege sync).  Role-scoped
    settings whose role is missing on the target are skipped with a warning.

    Returns the number of individual settings applied.
    """
    rows = await source_conn.fetch(
        """
        SELECT COALESCE(r.rolname, '') AS rolname, s.setconfig
        FROM pg_db_role_setting s
        JOIN pg_database d ON d.oid = s.setdatabase
        LEFT JOIN pg_roles r ON r.oid = s.setrole
        WHERE d.datname = $1
        ORDER BY 1
        """,
        dbname,
    )
    if not rows:
        return 0

    existing_roles = {
        r["rolname"] for r in await target_conn.fetch("SELECT rolname FROM pg_roles")
    }
    applied = 0
    for row in rows:
        rolname = row["rolname"]
        if rolname and rolname not in existing_roles:
            log.warning(
                "Skipping per-database settings for role '%s' in %s — role "
                "does not exist on target; create it and re-run bootstrap or "
                "apply the settings manually.",
                rolname, dbname,
            )
            continue
        scope = (
            f"ALTER ROLE {qi(rolname)} IN DATABASE {qi(dbname)}"
            if rolname else f"ALTER DATABASE {qi(dbname)}"
        )
        for entry in row["setconfig"] or []:
            name, sep, value = entry.partition("=")
            if not sep:
                continue
            # GUC names may be dotted (extension.setting) — emitting them
            # unquoted is both valid and required (a quoted "a.b" would be
            # taken as a single identifier containing a dot).
            rendered = value if name in _GUC_LIST_QUOTE else ql(value)
            stmt = f"{scope} SET {name} = {rendered};"
            try:
                await target_conn.execute(stmt)
                log.info("Applied per-database setting: %s SET %s = %s", scope, name, value)
                applied += 1
            except Exception as exc:
                log.warning(
                    "Could not apply per-database setting %s in %s — %s",
                    name, dbname, exc,
                )
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
        # Indexes on a partitioned parent propagate to its partitions, so the
        # children must be skipped to avoid duplicate-index errors.
        if t.get("is_partition"):
            continue
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
) -> list[str]:
    """Add missing foreign keys (second pass after all tables exist).

    Returns ``"fk_name on schema.table: error"`` entries for every FK that
    was skipped — a skipped FK is the strongest possible signal of a broken
    copy, so it must reach the caller's red end-of-run summary rather than
    only a log line that can scroll away.
    """
    fqn = f"{qi(schema)}.{qi(table)}"
    failures: list[str] = []

    # Foreign keys on a partitioned table are defined on the parent and
    # automatically propagated to every partition.  Adding them again on a
    # child would duplicate (or conflict with) the inherited constraint.
    part = await _get_partition_info(source_conn, schema, table)
    if part.get("is_partition"):
        return failures

    src_constraints = await get_constraints(source_conn, fqn)
    tgt_constraints = await get_constraints(target_conn, fqn)
    tgt_names = {c["constraint_name"] for c in tgt_constraints}

    for c in src_constraints:
        is_fk = c["constraint_type"] == "f"
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
            except (asyncpg.UndefinedTableError, asyncpg.UndefinedObjectError) as exc:
                log.warning(
                    "Skipping FK %s on %s — referenced object not found on target: %s",
                    c["constraint_name"], fqn, exc,
                )
                err = (str(exc) or repr(exc)).splitlines()[0]
                failures.append(
                    f"{c['constraint_name']} on {schema}.{table}: "
                    f"referenced object missing — {err}"
                )
            except asyncpg.ForeignKeyViolationError as exc:
                log.warning(
                    "Skipping FK %s on %s — referential integrity violation in the "
                    "copied data. Logical replication will NOT backfill missing "
                    "rows; investigate the inconsistency, then run "
                    "'detect-ddl --apply' to add this constraint. Detail: %s",
                    c["constraint_name"], fqn, exc,
                )
                err = (str(exc) or repr(exc)).splitlines()[0]
                failures.append(
                    f"{c['constraint_name']} on {schema}.{table}: "
                    f"FK violation in copied data — {err}"
                )
    return failures
