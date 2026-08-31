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

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.db import connect
from pg_emigrant.health import ReplicationState, replication_health
from pg_emigrant.replication import disable_subscription, sub_name
from tests.helpers.replication import wait_for_catchup

pytestmark = [pytest.mark.integration, pytest.mark.slow]


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
