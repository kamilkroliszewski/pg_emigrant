"""Which tables a migration is actually about.

``exclude_tables`` removes tables from the migration entirely — they are not
created on the target, not copied, not published, not replicated, and not
reported as drift.  The value of the setting is precisely that it applies
*everywhere*: an exclusion honoured by the copy but not by the publication
produces a subscription whose tablesync worker retries forever against a table
that does not exist on the target, and an exclusion honoured by the copy but
not by drift detection produces a permanent "missing on target" report for a
table that was left out on purpose.

So every place that asks "what tables are in scope?" goes through here.

Excluding a table is a decision to make the target knowingly incomplete, which
is the one thing the rest of this tool exists to prevent — so it is opt-in,
never inferred, and one shape of it is refused outright: a table that a
migrated table references by foreign key cannot be excluded, because the
target's constraint could never be satisfied against a table that isn't there.
"""

from __future__ import annotations

from fnmatch import fnmatchcase
from typing import Iterable

import asyncpg

from pg_emigrant.utils import get_logger

log = get_logger(__name__)


def _split(pattern: str) -> tuple[str | None, str]:
    """Split ``"schema.table"`` into its parts; a bare name matches any schema.

    Only the first dot separates: a pattern is at most two parts, so a table
    whose own name contains a dot is still addressable as ``schema.odd.name``.
    """
    schema, sep, table = pattern.partition(".")
    if not sep:
        return None, schema
    return schema, table


def matches(pattern: str, schema: str, table: str) -> bool:
    """Does *pattern* select ``schema.table``?

    Both halves are fnmatch globs, so ``audit_*``, ``legacy.*`` and
    ``*.temp_*`` all work.  Matching is case-sensitive because PostgreSQL
    identifiers are: a table created as ``"Orders"`` is not ``orders``.
    """
    pat_schema, pat_table = _split(pattern.strip())
    if pat_schema is not None and not fnmatchcase(schema, pat_schema):
        return False
    return fnmatchcase(table, pat_table)


def is_excluded(schema: str, table: str, patterns: Iterable[str] | None) -> bool:
    if not patterns:
        return False
    return any(matches(p, schema, table) for p in patterns)


def filter_tables(rows: list[dict], patterns: Iterable[str] | None) -> list[dict]:
    """Drop excluded entries from a ``get_tables``-shaped list of dicts."""
    if not patterns:
        return rows
    return [
        r for r in rows
        if not is_excluded(r["schema_name"], r["table_name"], patterns)
    ]


def filter_pairs(
    pairs: Iterable[tuple[str, str]], patterns: Iterable[str] | None
) -> set[tuple[str, str]]:
    if not patterns:
        return set(pairs)
    return {(s, t) for s, t in pairs if not is_excluded(s, t, patterns)}


class ExcludedTableIsReferenced(Exception):
    """A migrated table has a foreign key to an excluded table.

    Raised on the mutating path (bootstrap), not only reported by the
    read-only preflight: preflight is skippable with ``--skip-preflight`` and
    is a CLI step the library entry points and the web GUI never run, and this
    is a condition no later stage can repair.  The target's foreign key can
    never be satisfied against a table whose rows are deliberately not copied,
    so the choice is between refusing before anything is cleared and producing
    a target whose constraint silently does not exist.
    """

    @classmethod
    def aggregate(cls, dbname: str, problems: list[str]) -> "ExcludedTableIsReferenced":
        return cls(
            f"exclude_tables leaves out {len(problems)} table(s) that a migrated "
            f"table references by foreign key, so the target could never hold "
            f"the constraint: " + "; ".join(problems) + f". Nothing in {dbname} "
            f"was cleared or copied. Either drop those tables from "
            f"'exclude_tables' (they are part of the same data set), or exclude "
            f"the referencing table(s) as well."
        )


async def resolve_excluded(
    conn: asyncpg.Connection, schemas: list[str], patterns: Iterable[str] | None
) -> list[tuple[str, str]]:
    """The tables in *schemas* that *patterns* actually select, on this database.

    Returned sorted, so callers can log exactly what a pattern resolved to —
    a pattern that matches nothing is worth seeing, since it usually means a
    typo in a setting whose whole purpose is to leave data behind.
    """
    if not patterns:
        return []
    rows = await conn.fetch(
        """
        SELECT n.nspname, c.relname
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY($1::text[]) AND c.relkind IN ('r', 'p')
        ORDER BY 1, 2
        """,
        schemas,
    )
    return sorted(
        (r["nspname"], r["relname"])
        for r in rows
        if is_excluded(r["nspname"], r["relname"], patterns)
    )


async def check_exclusions_are_safe(
    conn: asyncpg.Connection, schemas: list[str], patterns: Iterable[str] | None
) -> list[str]:
    """Foreign keys from a migrated table to an excluded one.

    Returns human-readable descriptions; an empty list means the exclusions can
    be honoured.  The target cannot hold such a constraint — the referenced
    rows are never copied — so the choice is between refusing and producing a
    target whose foreign key silently does not exist.
    """
    excluded = set(await resolve_excluded(conn, schemas, patterns))
    if not excluded:
        return []
    edges = await conn.fetch(
        """
        SELECT rn.nspname AS ref_schema, rc.relname AS ref_table,
               con.conname,
               fn.nspname AS tgt_schema, fc.relname AS tgt_table
        FROM pg_constraint con
        JOIN pg_class rc ON rc.oid = con.conrelid
        JOIN pg_namespace rn ON rn.oid = rc.relnamespace
        JOIN pg_class fc ON fc.oid = con.confrelid
        JOIN pg_namespace fn ON fn.oid = fc.relnamespace
        WHERE con.contype = 'f'
          AND rn.nspname = ANY($1::text[])
        """,
        schemas,
    )
    problems: list[str] = []
    for e in edges:
        referencing = (e["ref_schema"], e["ref_table"])
        referenced = (e["tgt_schema"], e["tgt_table"])
        if referenced in excluded and referencing not in excluded:
            problems.append(
                f"{e['ref_schema']}.{e['ref_table']} references the excluded table "
                f"{e['tgt_schema']}.{e['tgt_table']} (constraint {e['conname']})"
            )
    return sorted(problems)
