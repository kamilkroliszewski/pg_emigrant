"""P0: what a migration is allowed to change on the production source.

The source stays live and serving traffic throughout.  Logical replication
needs three things there and nothing else — a replica identity on tables that
lack a primary key, a publication, and a replication slot — so those are the
whitelist, and anything else changing is a bug worth failing the build over.

The audit is behavioural rather than a code review: the entire source is
photographed before and after a real bootstrap (row checksums, object
inventory, ownership, privileges, indexes, constraints, sequence values,
per-database settings) and the two photographs are compared.  A static scan for
dangerous SQL would miss anything reached indirectly; this cannot.
"""

from __future__ import annotations

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.db import connect
from tests.helpers.verify import (
    comparable_columns,
    object_owners,
    schema_objects,
    table_checksum,
    table_privileges,
    user_tables,
)

pytestmark = [pytest.mark.integration, pytest.mark.slow]

SCHEMAS = ["app", "reporting"]

# The complete set of source-side changes a migration is permitted to make.
ALLOWED_SOURCE_CHANGES = {
    "replica_identity",   # REPLICA IDENTITY FULL on PK-less published tables
    "publication",        # CREATE PUBLICATION
    "replication_slot",   # CREATE_REPLICATION_SLOT
}


async def _photograph(conn) -> dict:
    """Everything about the source that a migration must not change."""
    data: dict = {}
    for schema, table in await user_tables(conn, SCHEMAS):
        cols = await comparable_columns(conn, schema, table)
        data[f"rows:{schema}.{table}"] = await table_checksum(conn, schema, table, cols)
        data[f"cols:{schema}.{table}"] = cols
    data["objects"] = await schema_objects(conn, SCHEMAS)
    data["owners"] = await object_owners(conn, SCHEMAS)
    data["privileges"] = await table_privileges(conn, SCHEMAS)
    data["indexes"] = {
        (r["schemaname"], r["indexname"], r["indexdef"])
        for r in await conn.fetch(
            "SELECT schemaname, indexname, indexdef FROM pg_indexes"
            " WHERE schemaname = ANY($1::text[])",
            SCHEMAS,
        )
    }
    data["constraints"] = {
        (r["nspname"], r["conname"], r["def"])
        for r in await conn.fetch(
            "SELECT n.nspname, c.conname, pg_get_constraintdef(c.oid) AS def"
            " FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace"
            " WHERE n.nspname = ANY($1::text[])",
            SCHEMAS,
        )
    }
    data["sequences"] = {
        (r["schemaname"], r["sequencename"], r["last_value"])
        for r in await conn.fetch(
            "SELECT schemaname, sequencename, last_value FROM pg_sequences"
            " WHERE schemaname = ANY($1::text[])",
            SCHEMAS,
        )
    }
    data["db_settings"] = await conn.fetchval(
        "SELECT setconfig FROM pg_db_role_setting s JOIN pg_database d"
        " ON d.oid = s.setdatabase WHERE d.datname = current_database()"
        " AND s.setrole = 0"
    )
    return data


async def _replica_identities(conn) -> dict[str, str]:
    rows = await conn.fetch(
        "SELECT n.nspname || '.' || c.relname AS name, c.relreplident::text AS ident"
        " FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = ANY($1::text[]) AND c.relkind IN ('r','p')",
        SCHEMAS,
    )
    return {r["name"]: r["ident"] for r in rows}


async def test_bootstrap_changes_nothing_on_the_source_outside_the_whitelist(
    cfg, source_db
):
    async with connect(cfg.source, source_db) as src:
        before = await _photograph(src)
        identities_before = await _replica_identities(src)

    await bootstrap(cfg, database=source_db)

    async with connect(cfg.source, source_db) as src:
        after = await _photograph(src)
        identities_after = await _replica_identities(src)

    differences = {k for k in before if before[k] != after.get(k)}
    assert not differences, (
        "the migration changed source state it has no business changing: "
        + ", ".join(
            f"{k} (before={before[k]!r}, after={after.get(k)!r})"
            for k in sorted(differences)
        )
    )

    # The one permitted schema change, and only on the tables that need it.
    changed_identities = {
        name: (identities_before[name], ident)
        for name, ident in identities_after.items()
        if identities_before.get(name) != ident
    }
    for name, (was, now) in changed_identities.items():
        assert now == "f" and was in ("d", "n"), (
            f"{name}: replica identity changed from {was!r} to {now!r}, which is "
            f"not the PK-less → FULL transition the migration is allowed to make"
        )
        has_pk = None
        async with connect(cfg.source, source_db) as src:
            schema, table = name.split(".", 1)
            has_pk = await src.fetchval(
                "SELECT EXISTS (SELECT 1 FROM pg_index i WHERE i.indisprimary"
                " AND i.indrelid = (quote_ident($1) || '.' || quote_ident($2))::regclass)",
                schema, table,
            )
        assert not has_pk, f"{name} has a primary key; its replica identity was changed anyway"


async def test_bootstrap_creates_no_source_objects_beyond_publication_and_slot(
    cfg, source_db
):
    async with connect(cfg.source, source_db) as src:
        pubs_before = {r["pubname"] for r in await src.fetch("SELECT pubname FROM pg_publication")}
        slots_before = {r["slot_name"] for r in
                        await src.fetch("SELECT slot_name FROM pg_replication_slots")}
        subs_before = await src.fetchval("SELECT count(*) FROM pg_subscription")

    await bootstrap(cfg, database=source_db)

    async with connect(cfg.source, source_db) as src:
        pubs_after = {r["pubname"] for r in await src.fetch("SELECT pubname FROM pg_publication")}
        slots_after = {r["slot_name"] for r in
                       await src.fetch("SELECT slot_name FROM pg_replication_slots")}
        subs_after = await src.fetchval("SELECT count(*) FROM pg_subscription")

    assert len(pubs_after - pubs_before) == 1, (
        f"expected exactly one new publication, got {sorted(pubs_after - pubs_before)}"
    )
    assert len(slots_after - slots_before) == 1, (
        f"expected exactly one new replication slot, got {sorted(slots_after - slots_before)}"
    )
    assert subs_after == subs_before, (
        "a subscription was created on the SOURCE — replication must flow one way"
    )


async def test_teardown_removes_only_what_the_migration_created(cfg, source_db):
    from pg_emigrant.replication import drop_publication, drop_subscription

    async with connect(cfg.source, source_db) as src:
        before = await _photograph(src)

    await bootstrap(cfg, database=source_db)
    await drop_subscription(cfg, source_db)
    await drop_publication(cfg, source_db)

    async with connect(cfg.source, source_db) as src:
        after = await _photograph(src)
        assert await src.fetchval("SELECT count(*) FROM pg_replication_slots") == 0
        assert await src.fetchval("SELECT count(*) FROM pg_publication") == 0

    differences = {k for k in before if before[k] != after.get(k)}
    assert not differences, (
        "teardown left the source changed: " + ", ".join(sorted(differences))
    )
