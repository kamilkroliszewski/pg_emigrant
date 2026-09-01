"""P1: replication health, WAL retention, and cutover readiness.

"A subscription row exists" was the closest this tool came to a health signal,
and it is not one: a subscription can exist while its apply worker is dead,
while its slot has been dropped, or while it is far enough behind that cutting
over would lose recent writes.  Each of those states is produced here against a
real cluster and the reported state is checked.
"""

from __future__ import annotations

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.cutover import check_cutover_readiness
from pg_emigrant.db import connect
from pg_emigrant.health import ReplicationState, replication_health
from pg_emigrant.replication import (
    disable_subscription,
    drop_publication,
    drop_subscription,
    sub_name,
)
from tests.helpers.replication import wait_for_catchup

pytestmark = [pytest.mark.integration, pytest.mark.slow]


async def test_healthy_replication_reports_healthy_with_real_numbers(cfg, source_db):
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    h = await replication_health(cfg, source_db)
    assert h.state is ReplicationState.HEALTHY, h.reasons
    assert h.slot_exists and h.slot_active
    assert h.subscription_exists and h.subscription_enabled and h.apply_worker_running
    assert h.publication_exists
    assert h.slot_wal_status == "reserved"

    # The metrics the operator actually needs, and which did not exist before.
    assert h.lag_bytes is not None and h.lag_bytes >= 0
    assert h.retained_wal_bytes is not None and h.retained_wal_bytes >= 0
    assert h.confirmed_flush_lsn and h.source_lsn


async def test_a_missing_slot_is_broken_not_merely_lagging(cfg, source_db):
    """The state that means data is already unrecoverable."""
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    slot = sub_name(cfg, source_db)
    async with connect(cfg.source, source_db) as src:
        await src.execute(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots"
            " WHERE slot_name = $1 AND active_pid IS NOT NULL", slot
        )
        import asyncio

        for _ in range(40):
            if not await src.fetchval(
                "SELECT active FROM pg_replication_slots WHERE slot_name = $1", slot
            ):
                break
            await asyncio.sleep(0.25)
        await src.execute("SELECT pg_drop_replication_slot($1)", slot)

    h = await replication_health(cfg, source_db)
    assert h.state is ReplicationState.BROKEN, h.reasons
    assert any("slot is GONE" in r for r in h.reasons)


async def test_a_disabled_subscription_is_broken(cfg, source_db):
    """Nothing is being applied, so nothing about the target is current."""
    await bootstrap(cfg, database=source_db)
    await disable_subscription(cfg, source_db)

    h = await replication_health(cfg, source_db)
    assert h.state is ReplicationState.BROKEN
    assert any("DISABLED" in r for r in h.reasons)


async def test_lag_thresholds_move_the_state(cfg, source_db):
    """The boundaries are real thresholds, not decoration."""
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    healthy = await replication_health(cfg, source_db)
    assert healthy.state is ReplicationState.HEALTHY

    # A threshold below the (small but non-zero) live lag must reclassify it.
    lagging = await replication_health(
        cfg, source_db, lag_warn_bytes=0, lag_critical_bytes=10 ** 12
    )
    assert lagging.state is ReplicationState.LAGGING, lagging.reasons

    critical = await replication_health(
        cfg, source_db, lag_warn_bytes=0, lag_critical_bytes=0
    )
    assert critical.state is ReplicationState.CRITICAL, critical.reasons


async def test_replication_that_was_never_set_up_is_absent_not_broken(cfg, source_db):
    """Nothing to report is not the same as something being wrong."""
    from pg_emigrant.bootstrap import ensure_database_exists

    await ensure_database_exists(cfg, source_db)
    h = await replication_health(cfg, source_db)
    assert h.state is ReplicationState.ABSENT


async def test_cutover_check_says_safe_only_when_everything_is_verified(cfg, source_db):
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)
    from pg_emigrant.sequence_sync import sync_sequences_once

    await sync_sequences_once(cfg, source_db)

    report = await check_cutover_readiness(cfg, database=source_db)
    assert report.ready, (
        "a healthy, caught-up migration was not considered ready: "
        + "; ".join(f"{c.name}: {c.summary}" for c in report.databases[0].blockers)
    )
    # The complete set, pinned: a check silently disappearing is how a
    # readiness report starts approving something it no longer verifies.
    names = {c.name for c in report.databases[0].checks}
    assert names == {
        "cluster_identity", "target_writable", "replication_healthy",
        "replication_caught_up", "all_tables_streaming",
        "sequences_synchronised", "no_schema_drift", "wal_retention",
    }


async def test_cutover_check_refuses_when_replication_is_broken(cfg, source_db):
    await bootstrap(cfg, database=source_db)
    await drop_subscription(cfg, source_db)

    report = await check_cutover_readiness(cfg, database=source_db)
    assert not report.ready
    blockers = {c.name for c in report.databases[0].blockers}
    assert "replication_healthy" in blockers


async def test_cutover_check_refuses_on_behind_sequences(cfg, source_db):
    """A sequence behind the source is a duplicate-key outage at cutover."""
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    async with connect(cfg.target, source_db) as tgt:
        await tgt.execute("SELECT setval('app.ticket_seq', 1, false)")

    report = await check_cutover_readiness(cfg, database=source_db)
    assert not report.ready
    blockers = {c.name for c in report.databases[0].blockers}
    assert "sequences_synchronised" in blockers


async def test_cutover_check_refuses_on_schema_drift_unless_accepted(cfg, source_db):
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)
    from pg_emigrant.sequence_sync import sync_sequences_once

    await sync_sequences_once(cfg, source_db)

    async with connect(cfg.source, source_db) as src:
        await src.execute("CREATE TABLE app.added_after (id int primary key)")

    report = await check_cutover_readiness(cfg, database=source_db)
    assert not report.ready
    assert "no_schema_drift" in {c.name for c in report.databases[0].blockers}

    accepted = await check_cutover_readiness(cfg, database=source_db, accept_drift=True)
    assert "no_schema_drift" not in {c.name for c in accepted.databases[0].blockers}


async def test_cutover_check_changes_nothing(cfg, source_db):
    """It answers a question; it must not act on the answer."""
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    async def _snapshot():
        async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
            return (
                await src.fetchval("SELECT count(*) FROM pg_replication_slots"),
                await src.fetchval("SELECT count(*) FROM pg_publication"),
                await tgt.fetchval("SELECT count(*) FROM pg_subscription"),
                await tgt.fetchval("SELECT count(*) FROM app.customers"),
                await tgt.fetchval("SELECT last_value FROM app.ticket_seq"),
                await tgt.fetchval("SELECT subenabled FROM pg_subscription LIMIT 1"),
            )

    before = await _snapshot()
    await check_cutover_readiness(cfg, database=source_db)
    assert await _snapshot() == before


async def test_cutover_check_refuses_when_the_target_is_unreachable(cfg, source_db):
    """An unverifiable condition must count against readiness, never for it."""
    await bootstrap(cfg, database=source_db)

    broken = cfg.model_copy(deep=True)
    broken.target.port = 1  # nothing listens here

    report = await check_cutover_readiness(broken, database=source_db)
    assert not report.ready
    blockers = {c.name for c in report.databases[0].blockers}
    assert "target_writable" in blockers


async def test_a_publication_dropped_under_a_live_subscription_is_broken(cfg, source_db):
    await bootstrap(cfg, database=source_db)
    await drop_publication(cfg, source_db)

    h = await replication_health(cfg, source_db)
    assert h.state is ReplicationState.BROKEN
    assert any("publication is missing" in r for r in h.reasons)
