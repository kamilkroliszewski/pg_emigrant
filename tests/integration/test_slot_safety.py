"""P0: replication-slot lifecycle, and what happens when the slot is gone.

A logical replication slot is the only thing holding WAL the target has not
replayed yet.  Once it is gone — the usual outcome of a pre-PG17 Patroni
promotion — everything committed since its last confirmed LSN exists nowhere
the target can still reach.  A fresh slot streams only from the moment it is
created, so "repairing" replication by recreating the subscription produces a
target that is permanently missing those rows *and reports itself healthy*.

That is the single worst failure mode in this tool, so the recovery path is
tested from the outside: the slot is really dropped on a real cluster and the
repair is asked to run.
"""

from __future__ import annotations

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.db import connect
from pg_emigrant.report import BootstrapIncomplete
from pg_emigrant.replication import reinit_sync, sub_name
from tests.helpers.replication import all_slots, slot_row, wait_for_catchup

pytestmark = [pytest.mark.integration, pytest.mark.slow]


async def _drop_slot(cfg, dbname):
    slot = sub_name(cfg, dbname)
    async with connect(cfg.source, dbname) as src:
        await src.execute(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots"
            " WHERE slot_name = $1 AND active_pid IS NOT NULL",
            slot,
        )
        for _ in range(40):
            still = await src.fetchval(
                "SELECT active FROM pg_replication_slots WHERE slot_name = $1", slot
            )
            if not still:
                break
            import asyncio

            await asyncio.sleep(0.25)
        await src.execute("SELECT pg_drop_replication_slot($1)", slot)


async def test_bootstrap_creates_exactly_one_slot_named_after_the_subscription(
    cfg, source_db
):
    await bootstrap(cfg, database=source_db)

    slots = await all_slots(cfg)
    assert slots == [sub_name(cfg, source_db)], (
        f"expected exactly one slot named after the subscription, got {slots}"
    )
    row = await slot_row(cfg, source_db)
    assert row["slot_type"] if "slot_type" in row else True
    assert row["active"], "the slot exists but nothing is streaming from it"
    assert row["wal_status"] == "reserved"
    assert row["database"] == source_db, (
        f"the slot was created in {row['database']!r}, not {source_db!r} — a "
        f"logical slot only decodes the database it was created in"
    )


async def test_lost_slot_makes_recovery_refuse_and_change_nothing(cfg, source_db):
    """The headline invariant: no slot means no provable continuity."""
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    # Writes that only the slot could still deliver.
    async with connect(cfg.source, source_db) as src:
        await src.execute(
            "INSERT INTO app.nasty_strings (id, val, note)"
            " SELECT 500 + i, 'after slot loss ' || i, 'gap' FROM generate_series(1, 10) i"
        )
    await _drop_slot(cfg, source_db)
    async with connect(cfg.source, source_db) as src:
        await src.execute(
            "INSERT INTO app.nasty_strings (id, val, note)"
            " SELECT 600 + i, 'unreachable ' || i, 'gap' FROM generate_series(1, 10) i"
        )

    async with connect(cfg.target, source_db) as tgt:
        subs_before = await tgt.fetchval(
            "SELECT count(*) FROM pg_subscription WHERE subname = $1",
            sub_name(cfg, source_db),
        )

    result = await reinit_sync(cfg, source_db)

    assert result["blocked"] is True, (
        f"recovery proceeded without the slot — the target would be silently "
        f"missing every write since it was lost. issues={result['issues_found']} "
        f"actions={result['actions_taken']}"
    )
    assert result["data_gap"] is False
    assert any("DATA-GAP REFUSED" in i for i in result["issues_found"])

    # A refusal must leave the (broken but inspectable) state exactly as found:
    # dropping the subscription first would make things strictly worse.
    async with connect(cfg.target, source_db) as tgt:
        assert await tgt.fetchval(
            "SELECT count(*) FROM pg_subscription WHERE subname = $1",
            sub_name(cfg, source_db),
        ) == subs_before, "the refused repair destroyed the existing subscription"
    assert await all_slots(cfg) == [], "a slot reappeared during a refused repair"


async def test_recovery_with_a_surviving_slot_resumes_without_a_gap(cfg, source_db):
    """Subscription lost, slot intact: the WAL is still there, so resume from it."""
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    sub = sub_name(cfg, source_db)
    async with connect(cfg.target, source_db) as tgt:
        # Detach before dropping, so the source-side slot survives.
        await tgt.execute(f'ALTER SUBSCRIPTION "{sub}" DISABLE')
        await tgt.execute(f'ALTER SUBSCRIPTION "{sub}" SET (slot_name = NONE)')
        await tgt.execute(f'DROP SUBSCRIPTION "{sub}"')

    async with connect(cfg.source, source_db) as src:
        await src.execute(
            "INSERT INTO app.nasty_strings (id, val, note)"
            " SELECT 700 + i, 'while detached ' || i, 'resume' FROM generate_series(1, 10) i"
        )

    result = await reinit_sync(cfg, source_db)
    assert result["blocked"] is False
    assert result["data_gap"] is False

    await wait_for_catchup(cfg, source_db)
    async with connect(cfg.target, source_db) as tgt:
        assert await tgt.fetchval(
            "SELECT count(*) FROM app.nasty_strings WHERE note = 'resume'"
        ) == 10, "the surviving slot's retained WAL was not replayed"


async def test_allow_data_gap_reports_the_loss_it_accepted(cfg, source_db):
    """The escape hatch must never look like a clean repair."""
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)
    await _drop_slot(cfg, source_db)

    result = await reinit_sync(cfg, source_db, allow_data_gap=True)

    assert result["blocked"] is False
    assert result["data_gap"] is True, (
        "a repair that recreated the slot from the current LSN did not record "
        "the data gap it created"
    )
    assert result["was_healthy"] is False
    assert any("DATA LOSS ACCEPTED" in i for i in result["issues_found"])


async def test_a_healthy_replication_setup_reports_healthy_and_changes_nothing(
    cfg, source_db
):
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    result = await reinit_sync(cfg, source_db)
    assert result["was_healthy"] is True, result["issues_found"]
    assert result["actions_taken"] == []
    assert result["blocked"] is False


async def test_a_second_migration_cannot_take_over_a_live_slot(
    cfg, source_db, source_pg, target_pg, pg_version_pair
):
    """Two migrations from one source, same configured names, different targets.

    Slots are cluster-wide and named from the configuration, so two migrations
    that were never told about each other can collide on one.  Taking the slot
    over terminates the first migration's walsender and drops the slot: its
    apply worker can never reconnect, its target silently stops receiving
    changes, and — before this was fixed — the second run reported success
    while doing it.
    """
    from tests.helpers.pg import start_pg
    from tests.integration.conftest import (
        FIXTURE_ROLES,
        drop_database,
        drop_subscription_if_present,
    )

    second_target = start_pg(pg_version_pair[1])
    try:
        for role in FIXTURE_ROLES:
            second_target.psql(
                f"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE "
                f"rolname = '{role}') THEN CREATE ROLE {role} NOLOGIN; END IF; END $$;"
            )

        await bootstrap(cfg, database=source_db)
        await wait_for_catchup(cfg, source_db)

        rival = cfg.model_copy(deep=True)
        rival.target = second_target.config

        with pytest.raises(BootstrapIncomplete) as excinfo:
            await bootstrap(rival, database=source_db)

        problems = " ".join(excinfo.value.report.databases[0].problems)
        assert "ACTIVE" in problems, problems
        assert "subscription_name" in problems, (
            "the refusal does not tell the operator how to run both migrations"
        )

        # The first migration must be completely unharmed.
        row = await slot_row(cfg, source_db)
        assert row is not None and row["active"], (
            "the refused run still disturbed the live slot"
        )

        async with connect(cfg.source, source_db) as src:
            await src.execute(
                "INSERT INTO app.nasty_strings (id, val, note)"
                " VALUES (4242, 'still replicating', 'rival')"
            )
        await wait_for_catchup(cfg, source_db)
        async with connect(cfg.target, source_db) as tgt:
            assert await tgt.fetchval(
                "SELECT count(*) FROM app.nasty_strings WHERE note = 'rival'"
            ) == 1, "the first migration stopped replicating after the second run"
    finally:
        drop_subscription_if_present(second_target, source_db)
        drop_database(second_target, source_db)
        second_target.stop()


async def test_a_slot_for_a_different_database_is_never_reused(cfg, source_db, source_pg):
    """A logical slot decodes only the database it was created in.

    A same-named slot on another database is definitively not this migration's,
    whatever its state, and dropping it would break whatever is using it.
    """
    from pg_emigrant.replication import SlotInUse, _drop_slot_if_present

    other = f"{source_db}_other"
    source_pg.psql(f'CREATE DATABASE "{other}"')
    try:
        slot = sub_name(cfg, source_db)
        async with connect(cfg.source, other) as conn:
            await conn.execute(
                "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slot
            )
            # Inactive, but in the wrong database: still not ours.
            async with connect(cfg.source, source_db) as here:
                with pytest.raises(SlotInUse) as excinfo:
                    await _drop_slot_if_present(here, slot)
            assert "belongs to database" in str(excinfo.value)

            await conn.execute("SELECT pg_drop_replication_slot($1)", slot)
    finally:
        from tests.integration.conftest import drop_database

        drop_database(source_pg, other)


async def test_an_inactive_orphan_is_still_cleaned_up(cfg, source_db):
    """The guard must not block the case it was always there to handle.

    An interrupted run leaves a slot with nothing streaming from it; that one
    is safe to reclaim, and refusing would make every crash need manual repair.
    """
    from pg_emigrant.replication import _drop_slot_if_present

    slot = sub_name(cfg, source_db)
    async with connect(cfg.source, source_db) as conn:
        await conn.execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slot
        )
        assert await _drop_slot_if_present(conn, slot) is True
        assert not await conn.fetchval(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1", slot
        )
