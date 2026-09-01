"""P1: WAL retention is a source-side risk, and it has to be visible.

A logical replication slot is the only thing keeping WAL the target has not
replayed yet, and it keeps it on the **production source**. A slot whose
consumer has stopped retains WAL indefinitely; the symptom is a full disk on
the primary, days after everyone stopped watching the migration.

So retention is measured explicitly rather than inferred from a lag figure, and
— the part that matters most — pg_emigrant never relieves it by dropping the
slot. That would trade a recoverable disk-space problem for permanent,
unrecoverable data loss, and it is the operator's decision, not the tool's.
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.config import ReplicatorConfig
from pg_emigrant.db import connect
from pg_emigrant.health import ReplicationState, replication_health
from pg_emigrant.replication import disable_subscription, reinit_sync, sub_name
from tests.helpers.pg import start_pg
from tests.helpers.replication import all_slots, wait_for_catchup
from tests.helpers.verify import assert_tables_identical

pytestmark = [pytest.mark.integration, pytest.mark.slow]

SCHEMAS = ["app", "reporting"]


async def _write_wal(cfg, dbname, rows: int = 20000) -> None:
    async with connect(cfg.source, dbname) as src:
        await src.execute(
            "INSERT INTO app.documents (title, body)"
            " SELECT 'retain ' || i, repeat('x', 500) FROM generate_series(1, $1) i",
            rows,
        )


async def test_retention_is_reported_and_grows_while_the_target_is_stopped(
    cfg, source_db
):
    """The number an operator has to watch, and that did not exist before."""
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    caught_up = await replication_health(cfg, source_db)
    assert caught_up.retained_wal_bytes is not None

    # Stop the consumer and keep writing: exactly what a paused or broken
    # migration does to a production primary.
    await disable_subscription(cfg, source_db)
    await _write_wal(cfg, source_db)

    stalled = await replication_health(cfg, source_db)
    assert stalled.retained_wal_bytes > caught_up.retained_wal_bytes, (
        f"retained WAL did not grow while the subscription was disabled "
        f"({caught_up.retained_wal_bytes} → {stalled.retained_wal_bytes}) — "
        f"the figure an operator would rely on to notice a stalled migration"
    )
    assert stalled.state is ReplicationState.BROKEN
    assert any("DISABLED" in r for r in stalled.reasons)


async def test_a_stalled_slot_is_never_dropped_to_reclaim_space(cfg, source_db):
    """The one automation that must not exist.

    Dropping the slot would free the WAL and destroy the migration: everything
    the target had not replayed becomes unreachable, permanently. No read-only
    command, and no health check, may take that decision.
    """
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)
    await disable_subscription(cfg, source_db)
    await _write_wal(cfg, source_db)

    slot = sub_name(cfg, source_db)
    for _ in range(3):
        await replication_health(cfg, source_db)
        from pg_emigrant.cutover import check_cutover_readiness
        from pg_emigrant.monitor import _ALL_SECTIONS, collect_all_status

        await check_cutover_readiness(cfg, database=source_db)
        await collect_all_status(cfg, [source_db], _ALL_SECTIONS)

    async with connect(cfg.source, source_db) as src:
        assert await src.fetchval(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1", slot
        ), "a read-only command dropped the replication slot to relieve retention"


async def test_cutover_check_refuses_while_wal_is_being_discarded(cfg, source_db):
    """`wal_status` past 'reserved' means the gap is becoming permanent."""
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    from pg_emigrant.cutover import _check_wal_retention
    from pg_emigrant.cutover import DatabaseReadiness

    health = await replication_health(cfg, source_db)
    assert health.slot_wal_status == "reserved"

    # Drive the classification directly for the states a test cannot force
    # cheaply: filling a real max_slot_wal_keep_size means generating gigabytes.
    for status in ("unreserved", "lost"):
        readiness = DatabaseReadiness(database=source_db)
        health.slot_wal_status = status
        _check_wal_retention(health, readiness)
        assert not readiness.ready, f"wal_status={status!r} did not block a cutover"
        assert "wal_retention" in {c.name for c in readiness.blockers}

    readiness = DatabaseReadiness(database=source_db)
    health.slot_wal_status = "reserved"
    health.retained_wal_bytes = 900
    health.max_slot_wal_keep_size_bytes = 1000
    _check_wal_retention(health, readiness)
    assert not readiness.ready, (
        "retention within 20% of max_slot_wal_keep_size did not block a cutover — "
        "past that limit PostgreSQL discards the WAL and the slot is unusable"
    )


async def test_teardown_is_the_documented_way_to_release_retention(cfg, source_db):
    """Explicit, and the only way."""
    from pg_emigrant.replication import drop_publication, drop_subscription

    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)
    await disable_subscription(cfg, source_db)
    await _write_wal(cfg, source_db, rows=5000)

    await drop_subscription(cfg, source_db)
    await drop_publication(cfg, source_db)

    async with connect(cfg.source, source_db) as src:
        assert await src.fetchval("SELECT count(*) FROM pg_replication_slots") == 0

    health = await replication_health(cfg, source_db)
    assert health.state is ReplicationState.ABSENT


# ── The real thing: WAL that is actually gone ────────────────────────────────
#
# Every assertion above about ``wal_status`` drives the classifier directly,
# because reaching 'lost' on a default cluster means generating gigabytes.  A
# classifier that is right about a value PostgreSQL never actually produced
# would prove nothing, so the tests below use a source configured with a tiny
# ``max_slot_wal_keep_size`` and let PostgreSQL invalidate the slot for real.
#
# This is the failure that arrives days late: an interrupted or paused
# migration leaves a slot behind, the source keeps writing, PostgreSQL
# eventually discards the WAL the slot was holding, and every transaction
# committed since the slot's last confirmed LSN becomes unreachable.  A tool
# that quietly recreates the slot at the current position here produces a
# target that is permanently missing those rows and looks perfectly healthy
# doing it.


@pytest.fixture(scope="module")
def tiny_wal_source(pg_version_pair):
    """A source cluster that discards slot WAL almost immediately.

    ``max_slot_wal_keep_size = 1MB`` is far below one 16 MB WAL segment, so the
    first checkpoint after a segment switch invalidates any slot that is still
    holding it.  Its own container, because the setting is cluster-wide and
    every other test in the suite depends on WAL *not* disappearing — and on
    the matrix's own source version, because slot invalidation is exactly the
    kind of behaviour that has moved between releases.
    """
    container = start_pg(pg_version_pair[0],
                         extra_args=["-c", "max_slot_wal_keep_size=1MB"])
    container.psql(
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_owner')"
        " THEN CREATE ROLE app_owner NOLOGIN; END IF; END $$;"
    )
    container.psql(
        "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_reader')"
        " THEN CREATE ROLE app_reader NOLOGIN; END IF; END $$;"
    )
    yield container
    container.stop()


@pytest_asyncio.fixture
async def tiny_wal_cfg(tiny_wal_source, target_pg):
    """A migration from the WAL-starved source into the ordinary target."""
    from tests.integration.conftest import (
        drop_database,
        drop_orphan_slots,
        drop_subscription_if_present,
        load_fixture,
    )

    db = f"walloss_{uuid.uuid4().hex[:8]}"
    load_fixture(tiny_wal_source, db)
    cfg = ReplicatorConfig(
        source=tiny_wal_source.config,
        target=target_pg.config,
        databases=[db],
        parallel_workers=2,
        table_parallel_workers=1,
        sequence_sync_interval=1,
    )
    yield cfg, db
    drop_subscription_if_present(target_pg, db)
    drop_database(target_pg, db)
    drop_orphan_slots(tiny_wal_source, db)
    drop_database(tiny_wal_source, db)


async def _burn_wal_until_slot_is_lost(cfg, dbname, *, rounds: int = 15) -> None:
    """Write, switch segments and checkpoint until PostgreSQL discards the WAL."""
    async with connect(cfg.source, dbname) as src:
        for i in range(rounds):
            await src.execute(
                "INSERT INTO app.documents (title, body)"
                " SELECT 'burn ' || g, repeat('x', 400)"
                " FROM generate_series(1, 4000::int) g"
            )
            await src.execute("SELECT pg_switch_wal()")
            await src.execute("CHECKPOINT")
            row = await src.fetchrow(
                "SELECT wal_status FROM pg_replication_slots WHERE slot_name = $1",
                sub_name(cfg, dbname),
            )
            if row is not None and row["wal_status"] == "lost":
                return
    raise AssertionError(
        f"the slot was never invalidated after {rounds} rounds of WAL churn — "
        f"this test is not exercising WAL loss"
    )


async def test_wal_loss_is_reported_as_broken_not_as_lag(tiny_wal_cfg):
    """PostgreSQL discards the WAL for real; the tool must say so, not guess.

    Scenario: the target stops consuming, the source keeps writing, and the
    WAL the slot was holding is recycled past ``max_slot_wal_keep_size``.  The
    gap is now permanent — nothing the target can ask for will bring those
    transactions back — so the state has to be BROKEN, not a large lag figure
    that looks like it will clear on its own.
    """
    cfg, dbname = tiny_wal_cfg
    await bootstrap(cfg, database=dbname)
    await wait_for_catchup(cfg, dbname)
    assert (await replication_health(cfg, dbname)).state is ReplicationState.HEALTHY

    await disable_subscription(cfg, dbname)
    await _burn_wal_until_slot_is_lost(cfg, dbname)

    async with connect(cfg.source, dbname) as src:
        row = await src.fetchrow(
            "SELECT wal_status, restart_lsn FROM pg_replication_slots"
            " WHERE slot_name = $1",
            sub_name(cfg, dbname),
        )
    assert row["wal_status"] == "lost", dict(row)
    assert row["restart_lsn"] is None, (
        "an invalidated slot should have released its restart_lsn"
    )

    health = await replication_health(cfg, dbname)
    assert health.state is ReplicationState.BROKEN, (
        f"a slot whose WAL PostgreSQL has actually discarded reported "
        f"{health.state.value.upper()}"
    )
    assert any("recycled" in r or "lost" in r for r in health.reasons), health.reasons


async def test_wal_loss_makes_recovery_refuse_and_change_nothing(tiny_wal_cfg):
    """``reinit-sync`` must fail closed on a real invalidation, not a simulated one.

    Recreating the subscription here attaches it to a fresh slot starting at
    the current LSN: replication resumes, the dashboard goes green, and every
    row written between the old slot's last confirmed LSN and now is silently
    absent from the target forever.  The refusal has to happen *before*
    anything is dropped, so a refused repair leaves the broken-but-inspectable
    state exactly as it found it.
    """
    cfg, dbname = tiny_wal_cfg
    await bootstrap(cfg, database=dbname)
    await wait_for_catchup(cfg, dbname)
    await disable_subscription(cfg, dbname)
    await _burn_wal_until_slot_is_lost(cfg, dbname)

    slots_before = await all_slots(cfg)
    async with connect(cfg.target, dbname) as tgt:
        subs_before = [
            r["subname"] for r in await tgt.fetch(
                "SELECT subname FROM pg_subscription WHERE subdbid ="
                " (SELECT oid FROM pg_database WHERE datname = current_database())"
            )
        ]

    result = await reinit_sync(cfg, dbname)

    assert result["blocked"] is True, result
    assert result["data_gap"] is False, (
        "a refused repair must not report having accepted a data gap"
    )
    assert any("DATA-GAP REFUSED" in i for i in result["issues_found"]), result
    assert result["actions_taken"] == [] or all(
        "publication" in a for a in result["actions_taken"]
    ), f"the refusal still changed replication state: {result['actions_taken']}"

    assert await all_slots(cfg) == slots_before, (
        "the refused repair disturbed the slot it refused to repair"
    )
    async with connect(cfg.target, dbname) as tgt:
        subs_after = [
            r["subname"] for r in await tgt.fetch(
                "SELECT subname FROM pg_subscription WHERE subdbid ="
                " (SELECT oid FROM pg_database WHERE datname = current_database())"
            )
        ]
    assert subs_after == subs_before, (
        "the refused repair dropped the subscription, leaving the database "
        "strictly worse off than before it was asked"
    )


async def test_cutover_is_refused_after_real_wal_loss(tiny_wal_cfg):
    """The readiness check is the last gate before an irreversible step."""
    from pg_emigrant.cutover import check_cutover_readiness

    cfg, dbname = tiny_wal_cfg
    await bootstrap(cfg, database=dbname)
    await wait_for_catchup(cfg, dbname)
    await disable_subscription(cfg, dbname)
    await _burn_wal_until_slot_is_lost(cfg, dbname)

    report = await check_cutover_readiness(cfg, database=dbname)
    assert not report.ready, "cutover-check approved a target with a lost slot"
    blockers = {c.name for c in report.databases[0].blockers}
    assert {"replication_healthy", "wal_retention"} <= blockers, blockers


async def test_a_recopy_is_the_documented_repair_and_it_works(tiny_wal_cfg):
    """The refusal names one repair; that repair has to actually converge.

    ``teardown`` then ``bootstrap`` re-copies from a fresh snapshot taken at a
    new slot's own consistent point, so the gap the lost WAL created is closed
    by the copy rather than by streaming.  A refusal that pointed at a repair
    which did not work would be no better than the silent recreation it
    refuses.
    """
    from pg_emigrant.replication import drop_publication, drop_subscription

    cfg, dbname = tiny_wal_cfg
    await bootstrap(cfg, database=dbname)
    await wait_for_catchup(cfg, dbname)
    await disable_subscription(cfg, dbname)
    await _burn_wal_until_slot_is_lost(cfg, dbname)

    await drop_subscription(cfg, dbname)
    await drop_publication(cfg, dbname)

    report = await bootstrap(cfg, database=dbname)
    assert report.passed, report.summary
    await wait_for_catchup(cfg, dbname)

    async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
        await assert_tables_identical(src, tgt, SCHEMAS)


async def test_an_orphaned_slot_whose_wal_is_gone_still_recovers_by_recopy(
    tiny_wal_cfg
):
    """The SIGKILL aftermath, taken to its worst end.

    A killed run leaves a slot with nothing attached to it.  The source keeps
    writing for long enough that PostgreSQL invalidates that slot.  The next
    ``bootstrap`` run then finds an orphan whose WAL is gone — and because
    bootstrap re-copies rather than resumes, adopting it is safe: the new slot
    and the new snapshot are a single fresh consistent point, so nothing
    depends on the WAL that was lost.  What must not happen is the run
    reporting success while relying on that WAL.
    """
    cfg, dbname = tiny_wal_cfg
    slot = sub_name(cfg, dbname)

    async with connect(cfg.source, dbname) as src:
        await src.execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slot
        )
    await _burn_wal_until_slot_is_lost(cfg, dbname)
    async with connect(cfg.source, dbname) as src:
        assert await src.fetchval(
            "SELECT wal_status FROM pg_replication_slots WHERE slot_name = $1", slot
        ) == "lost"

    report = await bootstrap(cfg, database=dbname)
    assert report.passed, report.summary

    await wait_for_catchup(cfg, dbname)
    health = await replication_health(cfg, dbname)
    assert health.state is ReplicationState.HEALTHY, health.reasons
    assert health.slot_wal_status == "reserved", (
        "the run adopted the invalidated slot instead of replacing it"
    )
    async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
        await assert_tables_identical(src, tgt, SCHEMAS)
