"""Source-vs-target consistency verification, computed by PostgreSQL itself.

Every comparison here is evaluated *server-side* and reduced to a single
scalar per table, so the assertion is about what the two databases actually
contain — not about what a Python client managed to fetch and coerce.  Row
values are hashed through ``md5(t::text)`` over the whole row, which makes the
check sensitive to every column, including the ones a naive Python comparison
tends to normalise away (numeric scale, timestamptz offsets, bytea escapes,
jsonb key order, array boundaries, NULL vs empty string).

Ordering is never left to the planner: checksums are order-independent
aggregates (``sum`` over per-row hashes), so a parallel or differently-ordered
scan on one side cannot produce a spurious mismatch.
"""

from __future__ import annotations

import asyncpg

from pg_emigrant.utils import qi, qt

# Order-independent whole-table checksum.
#
# Each row is rendered with the row-to-text cast (every column, in physical
# order), hashed, folded to a 64-bit signed integer and summed.  Summation is
# commutative, so scan order is irrelevant; ``count(*)`` is carried alongside
# so that a table of all-identical rows still detects a cardinality
# difference, which a pure sum of equal hashes would not.
_CHECKSUM_SQL = """
SELECT count(*)::bigint AS n_rows,
       COALESCE(sum(('x' || substr(md5(t.*::text), 1, 15))::bit(60)::bigint), 0)::numeric
           AS checksum
FROM ONLY {fqn} AS t
"""


async def table_row_count(conn: asyncpg.Connection, schema: str, table: str) -> int:
    return await conn.fetchval(f"SELECT count(*) FROM ONLY {qt(schema, table)}")


async def table_checksum(
    conn: asyncpg.Connection, schema: str, table: str
) -> tuple[int, int]:
    """Return ``(row_count, order-independent checksum)`` for one table."""
    row = await conn.fetchrow(_CHECKSUM_SQL.format(fqn=qt(schema, table)))
    return int(row["n_rows"]), int(row["checksum"])


async def primary_key_columns(
    conn: asyncpg.Connection, schema: str, table: str
) -> list[str]:
    rows = await conn.fetch(
        """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = (quote_ident($1) || '.' || quote_ident($2))::regclass
          AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum)
        """,
        schema, table,
    )
    return [r["attname"] for r in rows]


async def primary_key_set(
    conn: asyncpg.Connection, schema: str, table: str, pk_cols: list[str]
) -> set[tuple]:
    """Every primary-key tuple in the table, as a Python set.

    Used to report *which* rows differ once a checksum mismatch has already
    proved that they do — so the O(n) client-side materialisation only happens
    on the failure path.
    """
    cols = ", ".join(qi(c) for c in pk_cols)
    rows = await conn.fetch(f"SELECT {cols} FROM ONLY {qt(schema, table)}")
    return {tuple(r) for r in rows}


async def user_tables(conn: asyncpg.Connection, schemas: list[str]) -> list[tuple[str, str]]:
    """Ordinary tables holding rows of their own (partition leaves included,
    partitioned parents excluded — their rows live in the leaves)."""
    rows = await conn.fetch(
        """
        SELECT n.nspname, c.relname
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY($1::text[]) AND c.relkind = 'r'
        ORDER BY 1, 2
        """,
        schemas,
    )
    return [(r["nspname"], r["relname"]) for r in rows]


async def sequence_values(conn: asyncpg.Connection, schemas: list[str]) -> dict[str, int]:
    """``{"schema.sequence": last_value}`` for every readable sequence."""
    rows = await conn.fetch(
        """
        SELECT schemaname, sequencename, COALESCE(last_value, start_value) AS val
        FROM pg_sequences WHERE schemaname = ANY($1::text[])
        """,
        schemas,
    )
    return {f"{r['schemaname']}.{r['sequencename']}": int(r["val"]) for r in rows}


async def object_owners(conn: asyncpg.Connection, schemas: list[str]) -> dict[str, str]:
    rows = await conn.fetch(
        """
        SELECT n.nspname || '.' || c.relname AS obj,
               pg_get_userbyid(c.relowner) AS owner
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY($1::text[]) AND c.relkind IN ('r','p','v','m','S')
        """,
        schemas,
    )
    return {r["obj"]: r["owner"] for r in rows}


async def table_privileges(conn: asyncpg.Connection, schemas: list[str]) -> set[tuple]:
    rows = await conn.fetch(
        """
        SELECT table_schema, table_name, grantee, privilege_type
        FROM information_schema.table_privileges
        WHERE table_schema = ANY($1::text[])
        """,
        schemas,
    )
    return {(r["table_schema"], r["table_name"], r["grantee"], r["privilege_type"])
            for r in rows}


async def schema_objects(conn: asyncpg.Connection, schemas: list[str]) -> set[tuple[str, str, str]]:
    """``(kind, schema, name)`` for relations, routines, types and triggers."""
    out: set[tuple[str, str, str]] = set()
    rel = await conn.fetch(
        """
        SELECT c.relkind::text AS kind, n.nspname, c.relname
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY($1::text[]) AND c.relkind IN ('r','p','v','m','S','i')
        """,
        schemas,
    )
    out |= {(f"rel:{r['kind']}", r["nspname"], r["relname"]) for r in rel}
    proc = await conn.fetch(
        """
        SELECT n.nspname, p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')' AS sig
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = ANY($1::text[])
        """,
        schemas,
    )
    out |= {("proc", r["nspname"], r["sig"]) for r in proc}
    typ = await conn.fetch(
        """
        SELECT n.nspname, t.typname
        FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = ANY($1::text[]) AND t.typtype IN ('e','c','d','r')
          AND NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.oid = t.typrelid
                          AND c.relkind <> 'c')
        """,
        schemas,
    )
    out |= {("type", r["nspname"], r["typname"]) for r in typ}
    trg = await conn.fetch(
        """
        SELECT n.nspname, c.relname || '.' || t.tgname AS name
        FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY($1::text[]) AND NOT t.tgisinternal
        """,
        schemas,
    )
    out |= {("trigger", r["nspname"], r["name"]) for r in trg}
    return out


class ConsistencyError(AssertionError):
    """Source and target disagree about data that must be identical."""


async def assert_tables_identical(
    src: asyncpg.Connection,
    tgt: asyncpg.Connection,
    schemas: list[str],
    *,
    ignore: set[tuple[str, str]] | None = None,
) -> dict[str, tuple[int, int]]:
    """Assert every table in *schemas* matches by row count AND checksum.

    Returns ``{"schema.table": (row_count, checksum)}`` for the tables that
    were compared, so a passing test can print the evidence it verified.
    """
    ignore = ignore or set()
    src_tables = [t for t in await user_tables(src, schemas) if t not in ignore]
    tgt_tables = set(await user_tables(tgt, schemas))

    problems: list[str] = []
    evidence: dict[str, tuple[int, int]] = {}

    for schema, table in src_tables:
        key = f"{schema}.{table}"
        if (schema, table) not in tgt_tables:
            problems.append(f"{key}: MISSING on target")
            continue
        s_n, s_sum = await table_checksum(src, schema, table)
        t_n, t_sum = await table_checksum(tgt, schema, table)
        evidence[key] = (s_n, s_sum)
        if s_n != t_n or s_sum != t_sum:
            detail = f"{key}: source ({s_n} rows, checksum {s_sum}) != target ({t_n} rows, checksum {t_sum})"
            pk = await primary_key_columns(src, schema, table)
            if pk:
                s_keys = await primary_key_set(src, schema, table, pk)
                t_keys = await primary_key_set(tgt, schema, table, pk)
                missing = sorted(s_keys - t_keys)[:5]
                extra = sorted(t_keys - s_keys)[:5]
                detail += (
                    f"; pk({', '.join(pk)}) missing on target: {missing}"
                    f"; extra on target: {extra}"
                )
            problems.append(detail)

    if problems:
        raise ConsistencyError(
            f"{len(problems)} table(s) differ between source and target:\n  "
            + "\n  ".join(problems)
        )
    return evidence
