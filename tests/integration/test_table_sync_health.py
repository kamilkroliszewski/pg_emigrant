"""P0: a table can be published, tracked, and still not replicated at all.

A subscription is not one stream.  It is a shared apply worker plus one state
machine *per table*, and they fail independently.  When a table joins an
existing subscription — which is exactly what ``sync-sequences --loop`` does
automatically for every table created on the source after bootstrap — a
``tablesync`` worker performs its initial copy, and only when that copy
finishes is the table handed over to the apply worker.  Until then no change
for it is ever applied.

If that copy can never succeed, the table stays in its initial state forever
and the target simply does not have its rows.  Nothing about the *rest* of the
system looks wrong while this happens: the slot is fine, the apply worker is
fine, ``confirmed_flush_lsn`` keeps advancing, and the lag figure reads zero,
because none of those measure the stuck table.  Before this was checked,
``status --health`` reported HEALTHY and ``cutover-check`` reported SAFE TO CUT
OVER on a database with a whole table missing — reproduced, and the regression
these tests exist for.
"""

from __future__ import annotations

import asyncio

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.cutover import check_cutover_readiness
from pg_emigrant.db import connect
from pg_emigrant.health import ReplicationState, replication_health
from pg_emigrant.replication import sub_name, sync_new_tables
from tests.helpers.replication import wait_for_catchup

pytestmark = [pytest.mark.integration, pytest.mark.slow]


async def _table_states(cfg, dbname) -> dict[str, str]:
    """``schema.table`` → ``srsubstate``, straight from the target's catalog."""
    async with connect(cfg.target, dbname) as tgt:
        rows = await tgt.fetch(
            """
            SELECT n.nspname, c.relname, sr.srsubstate::text AS st
            FROM pg_subscription_rel sr
            JOIN pg_subscription s ON s.oid = sr.srsubid
            JOIN pg_class c ON c.oid = sr.srrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE s.subname = $1
            """,
            sub_name(cfg, dbname),
        )
    return {f"{r['nspname']}.{r['relname']}": r["st"] for r in rows}


async def _add_source_table(cfg, dbname, *, rows: int = 50) -> None:
    async with connect(cfg.source, dbname) as src:
        await src.execute(
            "CREATE TABLE app.late_arrival (id int PRIMARY KEY, val text NOT NULL)"
        )
        await src.execute(
            "INSERT INTO app.late_arrival SELECT g, 'row ' || g"
            " FROM generate_series(1, $1::int) g",
            rows,
        )


async def _wait_for_tracking(cfg, dbname, table: str, timeout: float = 30.0) -> None:
    """Wait until the subscription knows about *table* at all."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if table in await _table_states(cfg, dbname):
            return
        await asyncio.sleep(0.3)
    raise AssertionError(
        f"{table} never appeared in pg_subscription_rel — the test is not "
        f"exercising the tablesync path it claims to"
    )


async def test_bootstrap_leaves_every_table_ready(cfg, source_db):
    """The baseline: nothing about the normal path may look like a stuck sync.

    Bootstrap subscribes with ``copy_data = false`` because it has already
    copied the data itself, so PostgreSQL marks every table ready immediately.
    If that ever stopped being true, every health check below would fire on a
    perfectly good migration — so it is pinned down here rather than assumed.
    """
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    states = await _table_states(cfg, source_db)
    assert states, "bootstrap tracked no tables at all on the subscription"
    assert set(states.values()) == {"r"}, (
        f"bootstrap left tables in a non-ready state: "
        f"{ {k: v for k, v in states.items() if v != 'r'} }"
    )

    health = await replication_health(cfg, source_db)
    assert health.state is ReplicationState.HEALTHY, health.reasons
    assert health.tables_not_ready == []
    assert health.tables_total == len(states)


async def test_a_stuck_tablesync_is_not_healthy_and_blocks_the_cutover(
    cfg, source_db
):
    """The reproduced failure: a whole table missing, everything else green.

    ``app.late_arrival`` is created on the source and picked up by
    ``sync_new_tables`` the way the steady-state loop picks it up in
    production.  A CHECK constraint on the target that the source rows all
    violate makes its initial copy fail permanently — the shape of a type
    mismatch, a stricter constraint, or a partially applied schema change on a
    real target.

    Everything else keeps working, which is the point: the slot streams, the
    apply worker applies, the lag is zero.  What must NOT happen is a green
    light over a target that is permanently missing 50 rows.
    """
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    await _add_source_table(cfg, source_db)
    async with connect(cfg.target, source_db) as tgt:
        await tgt.execute(
            "CREATE TABLE app.late_arrival (id int PRIMARY KEY, val text NOT NULL"
            " CHECK (val = 'nothing matches this'))"
        )

    await sync_new_tables(cfg, source_db)
    await _wait_for_tracking(cfg, source_db, "app.late_arrival")

    # Give the tablesync worker time to fail, be restarted, and fail again, so
    # the assertion is about a permanently stuck sync rather than a slow one.
    for _ in range(60):
        health = await replication_health(cfg, source_db)
        if health.sync_error_count:
            break
        await asyncio.sleep(0.5)

    states = await _table_states(cfg, source_db)
    assert states.get("app.late_arrival") != "r", (
        "the test did not actually produce a stuck tablesync — it asserts nothing"
    )
    async with connect(cfg.target, source_db) as tgt:
        assert await tgt.fetchval("SELECT count(*) FROM app.late_arrival") == 0, (
            "the tablesync unexpectedly succeeded"
        )

    health = await replication_health(cfg, source_db)
    assert health.state is not ReplicationState.HEALTHY, (
        f"a database with a permanently un-synced table reported "
        f"{health.state.value.upper()} — the slot, the apply worker and the lag "
        f"are all fine, which is exactly why this needs its own signal"
    )
    assert any("late_arrival" in r for r in health.reasons), (
        f"health did not name the table that is not streaming: {health.reasons}"
    )
    assert "app.late_arrival (state=d)" in health.tables_not_ready or any(
        "late_arrival" in t for t in health.tables_not_ready
    ), health.tables_not_ready

    report = await check_cutover_readiness(cfg, database=source_db)
    assert not report.ready, (
        "cutover-check said SAFE TO CUT OVER while a published table had none "
        "of its rows on the target"
    )
    blockers = {c.name for c in report.databases[0].blockers}
    assert "all_tables_streaming" in blockers, (
        f"the un-synced table was not the reason given: {blockers}"
    )
    detail = " ".join(
        c.detail for c in report.databases[0].blockers
        if c.name == "all_tables_streaming"
    )
    assert "late_arrival" in detail, detail


async def test_a_sync_that_completes_returns_to_healthy(cfg, source_db):
    """Recovery, so the new signal is a state and not a one-way trapdoor.

    The same stuck table, then the obstruction removed.  PostgreSQL retries the
    tablesync on its own; health has to come back to HEALTHY and the cutover
    check has to stop blocking, or the check would be unusable in practice —
    an operator who fixed the cause would have no way to tell.
    """
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    await _add_source_table(cfg, source_db)
    async with connect(cfg.target, source_db) as tgt:
        await tgt.execute(
            "CREATE TABLE app.late_arrival (id int PRIMARY KEY, val text NOT NULL"
            " CONSTRAINT impossible CHECK (val = 'nothing matches this'))"
        )
    await sync_new_tables(cfg, source_db)
    await _wait_for_tracking(cfg, source_db, "app.late_arrival")

    for _ in range(60):
        if (await replication_health(cfg, source_db)).sync_error_count:
            break
        await asyncio.sleep(0.5)
    assert (await replication_health(cfg, source_db)).state is not ReplicationState.HEALTHY

    async with connect(cfg.target, source_db) as tgt:
        await tgt.execute("ALTER TABLE app.late_arrival DROP CONSTRAINT impossible")

    for _ in range(120):
        health = await replication_health(cfg, source_db)
        if not health.tables_not_ready:
            break
        await asyncio.sleep(0.5)

    assert health.tables_not_ready == [], (
        f"the tablesync did not recover after the obstruction was removed: "
        f"{health.tables_not_ready}"
    )
    async with connect(cfg.target, source_db) as tgt:
        assert await tgt.fetchval("SELECT count(*) FROM app.late_arrival") == 50

    await wait_for_catchup(cfg, source_db)
    health = await replication_health(cfg, source_db)
    assert health.state is ReplicationState.HEALTHY, health.reasons
    report = await check_cutover_readiness(cfg, database=source_db)
    assert "all_tables_streaming" not in {
        c.name for c in report.databases[0].blockers
    }, "a recovered tablesync still blocks the cutover"


async def test_a_table_no_subscription_knows_about_is_not_healthy(cfg, source_db):
    """The invisible failure, end to end: nothing anywhere is in an error state.

    A table that exists on the source *and* on the target but is in no
    publication is replicated by nothing at all. Every signal reads clean —
    the drift scan sees it on both sides and reports nothing, the slot and the
    apply worker are fine, the lag is zero — because none of them has ever
    heard of it. Its copy on the target is simply empty, forever.

    This is not hypothetical: reproduced against a PostgreSQL 14 source, where
    ``detect-ddl --apply`` created the table, reported ``applied: 1,
    failures: []``, and left it permanently empty while the next drift scan
    said "No drift detected", health said HEALTHY and cutover-check said SAFE
    TO CUT OVER. Here the state is built directly, so the guarantee is pinned
    independently of which code path produced it.
    """
    from pg_emigrant.ddl_detector import detect_drift

    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)
    assert (await replication_health(cfg, source_db)).state is ReplicationState.HEALTHY

    async with connect(cfg.source, source_db) as src:
        await src.execute("CREATE TABLE app.ghost (id int PRIMARY KEY, v text)")
        await src.execute(
            "INSERT INTO app.ghost SELECT g, 'v' || g"
            " FROM generate_series(1, 9::int) g"
        )
    async with connect(cfg.target, source_db) as tgt:
        await tgt.execute("CREATE TABLE app.ghost (id int PRIMARY KEY, v text)")

    # Both sides have the table, so the drift scan is satisfied — which is
    # precisely why something else has to notice.
    drift = await detect_drift(cfg, source_db)
    assert not any(i.table == "ghost" for i in drift.items), (
        "the premise of this test no longer holds: drift detection now reports "
        "the table, so it is not the invisible case any more"
    )

    health = await replication_health(cfg, source_db)
    assert health.state is not ReplicationState.HEALTHY, (
        "a table on both servers that nothing replicates reported HEALTHY"
    )
    assert any("app.ghost" in t for t in health.tables_not_replicated), (
        health.tables_not_replicated
    )

    report = await check_cutover_readiness(cfg, database=source_db)
    assert not report.ready, "cutover-check approved a target with an empty ghost table"
    assert "all_tables_streaming" in {c.name for c in report.databases[0].blockers}

    async with connect(cfg.target, source_db) as tgt:
        assert await tgt.fetchval("SELECT count(*) FROM app.ghost") == 0, (
            "the table replicated after all — the test is not exercising its case"
        )

    # And the documented remedy closes it.
    await sync_new_tables(cfg, source_db)
    await wait_for_catchup(cfg, source_db, timeout=120)
    health = await replication_health(cfg, source_db)
    assert health.tables_not_replicated == [], health.tables_not_replicated
    assert health.state is ReplicationState.HEALTHY, health.reasons
    async with connect(cfg.target, source_db) as tgt:
        assert await tgt.fetchval("SELECT count(*) FROM app.ghost") == 9
