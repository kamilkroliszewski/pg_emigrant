"""P0: what a source failover does to a migration in flight.

The intended production source may be managed by Patroni, so a switchover or
failover during the migration window is a normal event, not an exotic one.  The
consequence is specific and severe: a logical replication slot is *local* to
the instance that created it, and before PostgreSQL 17's failover slots it is
not carried to a physical replica.  A promoted node therefore has no slot —
which means every transaction the old primary had not yet streamed is
unreachable, permanently.

That is exactly the situation in which a tool is most tempted to "repair"
replication and hand back a target that looks healthy and is missing rows.
These tests promote a real streaming standby and check that it does not.

The simulation is faithful in the ways that matter (a real base backup, real
streaming, a real promotion, a genuinely absent slot) and does not include
Patroni itself: what Patroni contributes — leader election, DCS state, and
moving an endpoint — changes which node the tool connects to, not what
PostgreSQL does about the slot. See README, 'Patroni / failover behaviour'.
"""

from __future__ import annotations

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.db import connect
from pg_emigrant.health import ReplicationState, replication_health
from pg_emigrant.replication import reinit_sync, sub_name
from tests.helpers.pg import promote, start_physical_standby
from tests.helpers.replication import wait_for_catchup

pytestmark = [pytest.mark.integration, pytest.mark.slow]


@pytest.fixture
def promoted_source(source_pg, cfg, source_db):
    """A streaming replica of the source, promoted mid-migration.

    Yields a config whose ``source`` points at the new primary — the state an
    operator is left in after Patroni moves the leader endpoint.
    """
    standby = start_physical_standby(source_pg)
    try:
        yield standby
    finally:
        standby.stop()


async def test_promoted_node_has_no_logical_slot_and_recovery_is_refused(
    cfg, source_db, source_pg, promoted_source
):
    """The headline failover case, end to end."""
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    # Writes the old primary streamed, and writes it did not.
    async with connect(cfg.source, source_db) as src:
        await src.execute(
            "INSERT INTO app.nasty_strings (id, val, note)"
            " SELECT 400 + i, 'before failover ' || i, 'pre' FROM generate_series(1, 10) i"
        )
    await wait_for_catchup(cfg, source_db)

    promote(promoted_source)
    # The new primary inherits the data but NOT the logical slot.
    assert promoted_source.psql(
        "SELECT count(*) FROM pg_replication_slots WHERE slot_name = "
        + "'" + sub_name(cfg, source_db).replace("'", "''") + "'"
    ) == "0", "the promoted node unexpectedly has the logical slot"

    failed_over = cfg.model_copy(deep=True)
    failed_over.source = promoted_source.config

    # Writes that only the old primary's slot could ever have delivered.
    async with connect(failed_over.source, source_db) as src:
        await src.execute(
            "INSERT INTO app.nasty_strings (id, val, note)"
            " SELECT 450 + i, 'after failover ' || i, 'post' FROM generate_series(1, 10) i"
        )

    health = await replication_health(failed_over, source_db)
    assert health.state is ReplicationState.BROKEN, health.reasons
    assert any("slot is GONE" in r for r in health.reasons)

    async with connect(failed_over.target, source_db) as tgt:
        subs_before = await tgt.fetchval("SELECT count(*) FROM pg_subscription")

    result = await reinit_sync(failed_over, source_db)

    assert result["blocked"] is True, (
        "recovery proceeded against a promoted node with no slot — the target "
        "would silently and permanently be missing every write the old primary "
        f"had not streamed. issues={result['issues_found']}"
    )
    assert result["data_gap"] is False
    assert any("DATA-GAP REFUSED" in i for i in result["issues_found"])

    # A refusal changes nothing: the operator still has the evidence.
    async with connect(failed_over.target, source_db) as tgt:
        assert await tgt.fetchval("SELECT count(*) FROM pg_subscription") == subs_before


async def test_a_migration_does_not_silently_continue_against_a_standby(
    cfg, source_db, promoted_source
):
    """Pointing 'source' at a node still in recovery must not look fine.

    A standby is read-only and cannot host a logical slot, so a migration
    started against one produces nothing usable — the failure has to be loud
    and early rather than a half-built target.
    """
    from pg_emigrant.preflight import ERROR, run_preflight

    standby_cfg = cfg.model_copy(deep=True)
    standby_cfg.source = promoted_source.config  # still in recovery

    report = await run_preflight(standby_cfg, database=source_db)
    failed = {c.name for c in report.checks if c.status == ERROR}
    assert "source_is_primary" in failed, (
        f"preflight accepted a standby as the migration source: "
        f"{[(c.name, c.status, c.summary) for c in report.checks if c.status == ERROR]}"
    )
    assert not report.passed


async def test_the_target_is_not_mistaken_for_an_independent_cluster_after_promotion(
    cfg, source_db, source_pg, promoted_source
):
    """A promoted replica keeps the source's system_identifier.

    Which is what makes it recognisable as the same cluster — and what would
    make migrating "into" it destroy the data it just inherited.
    """
    from pg_emigrant.guards import UnsafeOperation, assert_distinct_clusters

    promote(promoted_source)

    same_cluster = cfg.model_copy(deep=True)
    same_cluster.target = promoted_source.config

    with pytest.raises(UnsafeOperation) as excinfo:
        await assert_distinct_clusters(same_cluster)
    assert "system_identifier" in str(excinfo.value)


async def test_replication_survives_the_old_primary_becoming_unreachable(
    cfg, source_db, source_pg
):
    """A source outage must not be mistaken for data loss.

    When the source comes back with its slot intact there is no gap: the WAL
    was retained the whole time.  Reporting this as unrecoverable would push an
    operator into an unnecessary re-copy of a production dataset.
    """
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    source_pg.pause()
    try:
        health = await _health_tolerating_outage(cfg, source_db)
    finally:
        source_pg.unpause()

    # However the outage is reported, the slot is intact underneath, so once
    # the source is back the repair path must NOT claim a data gap.
    result = await reinit_sync(cfg, source_db)
    assert result["blocked"] is False, result["issues_found"]
    assert result["data_gap"] is False

    async with connect(cfg.source, source_db) as src:
        await src.execute(
            "INSERT INTO app.nasty_strings (id, val, note) VALUES (999, 'v', 'after outage')"
        )
    await wait_for_catchup(cfg, source_db)
    async with connect(cfg.target, source_db) as tgt:
        assert await tgt.fetchval(
            "SELECT count(*) FROM app.nasty_strings WHERE note = 'after outage'"
        ) == 1
    del health


async def _health_tolerating_outage(cfg, dbname):
    """Health of an unreachable source: an error is an acceptable answer here,
    a confident 'healthy' is not."""
    try:
        health = await replication_health(cfg, dbname)
    except Exception:
        return None
    assert health.state is not ReplicationState.HEALTHY, (
        "replication was reported healthy while the source was unreachable"
    )
    return health
