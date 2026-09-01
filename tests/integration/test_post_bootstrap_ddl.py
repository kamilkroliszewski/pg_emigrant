"""P0/P1: what happens to DDL executed on the source AFTER bootstrap.

Logical replication carries rows, never schema.  Everything an application team
does to a live database during a migration window — a new table, a new column,
an index — is invisible to the WAL stream, so pg_emigrant has to close each gap
itself or refuse to call the target ready.

Two mechanisms do that, and they have different jobs.  ``sync_new_tables`` runs
on every tick of ``sync-sequences --loop`` (and on every one-shot run) and is
the *only* thing that brings a table created after bootstrap into replication:
publication membership, the table on the target, and the subscription refresh
that starts its tablesync.  ``detect-ddl`` finds everything else and reports it;
``cutover-check`` refuses while any of it is outstanding.

The tests here run against every source version in the matrix on purpose.  The
publication built on a pre-15 source is a frozen ``FOR TABLE`` list, so a table
created afterwards is invisible to it until something explicitly adds it —
which makes the older versions the ones where this path matters most and the
ones where a version-specific bug hides longest.
"""

from __future__ import annotations

import asyncio

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.cutover import check_cutover_readiness
from pg_emigrant.db import connect
from pg_emigrant.ddl_detector import detect_drift
from pg_emigrant.health import ReplicationState, replication_health
from pg_emigrant.replication import sub_name, sync_new_tables
from tests.helpers.cli import run_cli, write_config
from tests.helpers.replication import wait_for_catchup
from tests.helpers.verify import table_checksum, table_row_count

pytestmark = [pytest.mark.integration, pytest.mark.slow]


async def _wait_until_ready(cfg, dbname, table: str, timeout: float = 60.0) -> None:
    """Wait for *table* to reach ``srsubstate = 'r'`` on the subscription."""
    deadline = asyncio.get_running_loop().time() + timeout
    last: list[str] = []
    while asyncio.get_running_loop().time() < deadline:
        health = await replication_health(cfg, dbname)
        last = health.tables_not_ready
        if not last and health.tables_total:
            return
        await asyncio.sleep(0.3)
    raise AssertionError(
        f"{table} never became ready on the subscription within {timeout}s "
        f"(not ready: {last})"
    )


async def test_a_table_created_after_bootstrap_is_actually_replicated(cfg, source_db):
    """The whole documented steady-state promise, end to end.

    A table created on the source after bootstrap must end up on the target
    with its existing rows *and* with the writes that follow — that is what
    ``sync-sequences --loop`` claims to do automatically, and it is the claim
    that had no test.  On a pre-15 source it exercised an unguarded
    PostgreSQL 15 catalog (``pg_publication_namespace``) and raised, taking
    the whole ``sync-sequences`` command down with it: no table picked up, and
    no sequence advanced either.
    """
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    async with connect(cfg.source, source_db) as src:
        await src.execute(
            "CREATE TABLE app.after_bootstrap ("
            "  id bigserial PRIMARY KEY, label text NOT NULL, made_at timestamptz"
            ")"
        )
        await src.execute(
            "INSERT INTO app.after_bootstrap (label, made_at)"
            " SELECT 'pre-existing ' || g, now() FROM generate_series(1, 40::int) g"
        )

    actions = await sync_new_tables(cfg, source_db)
    assert actions, (
        "sync_new_tables reported nothing to do for a table that did not exist "
        "at bootstrap"
    )
    await _wait_until_ready(cfg, source_db, "app.after_bootstrap")

    # Rows written AFTER the pickup have to stream too — the initial sync and
    # the ongoing stream are separate mechanisms and a table can get one
    # without the other.
    async with connect(cfg.source, source_db) as src:
        await src.execute(
            "INSERT INTO app.after_bootstrap (label, made_at)"
            " SELECT 'streamed ' || g, now() FROM generate_series(1, 10::int) g"
        )
        await src.execute(
            "UPDATE app.after_bootstrap SET label = label || '!' WHERE id <= 5"
        )
        await src.execute("DELETE FROM app.after_bootstrap WHERE id = 40")
    await wait_for_catchup(cfg, source_db)

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        assert await table_row_count(tgt, "app", "after_bootstrap") == 49
        assert (
            await table_checksum(src, "app", "after_bootstrap", ["id", "label"])
            == await table_checksum(tgt, "app", "after_bootstrap", ["id", "label"])
        ), "the table was picked up but its contents diverged"

    health = await replication_health(cfg, source_db)
    assert health.state is ReplicationState.HEALTHY, health.reasons


async def test_a_new_table_blocks_the_cutover_until_it_is_picked_up(cfg, source_db):
    """Between the CREATE TABLE and the next loop tick, the target is not ready."""
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)
    assert (await check_cutover_readiness(cfg, database=source_db)).ready

    async with connect(cfg.source, source_db) as src:
        await src.execute("CREATE TABLE app.not_yet (id int PRIMARY KEY)")

    report = await check_cutover_readiness(cfg, database=source_db)
    assert not report.ready, (
        "cutover-check approved a target that is missing a table the source has"
    )
    assert "no_schema_drift" in {c.name for c in report.databases[0].blockers}

    drift = await detect_drift(cfg, source_db)
    assert any(i.name == "not_yet" or "not_yet" in i.detail for i in drift.items), (
        f"the new table was not reported as drift: {drift.summary}"
    )

    await sync_new_tables(cfg, source_db)
    await _wait_until_ready(cfg, source_db, "app.not_yet")
    await wait_for_catchup(cfg, source_db)
    assert (await check_cutover_readiness(cfg, database=source_db)).ready, (
        [c.summary for c in
         (await check_cutover_readiness(cfg, database=source_db)).databases[0].blockers]
    )


async def test_sync_sequences_advances_sequences_on_every_supported_source(
    tmp_path, cfg, source_db
):
    """The final cutover step must work on every version in the matrix.

    ``sync-sequences --margin`` is the last thing a runbook runs before moving
    traffic; a target whose sequences were never advanced hands out already-used
    values on the first insert afterwards.  Driven through the CLI because the
    failure this pins down was an unhandled exception at the command level, not
    inside the sequence logic — the sequences were fine, nothing ever reached
    them.
    """
    await bootstrap(cfg, database=source_db)
    async with connect(cfg.source, source_db) as src:
        for _ in range(20):
            await src.fetchval("SELECT nextval('app.ticket_seq')")
        source_value = await src.fetchval(
            "SELECT last_value FROM pg_sequences"
            " WHERE schemaname = 'app' AND sequencename = 'ticket_seq'"
        )

    path = write_config(tmp_path / "config.yaml", cfg)
    result = run_cli("sync-sequences", "-c", str(path), "--format", "json")
    assert result.returncode == 0, (
        f"sync-sequences exited {result.returncode}; stderr={result.stderr[-2000:]}"
    )
    payload = result.json()
    tickets = [
        r for db in payload for r in db["sequences"] if r["sequence"] == "ticket_seq"
    ]
    assert tickets, f"ticket_seq was not in the report at all: {payload}"

    async with connect(cfg.target, source_db) as tgt:
        target_value = await tgt.fetchval(
            "SELECT last_value FROM pg_sequences"
            " WHERE schemaname = 'app' AND sequencename = 'ticket_seq'"
        )
    assert target_value >= source_value, (
        f"the target sequence was left behind the source ({target_value} < "
        f"{source_value}) — the first insert after cutover would collide"
    )


async def test_a_column_added_on_the_source_is_drift_and_survives_being_applied(
    cfg, source_db
):
    """Concurrent DDL that logical replication cannot carry.

    A column added on the source is not in the WAL stream, and the subscriber
    then receives rows carrying a column its own copy of the table does not
    have — PostgreSQL rejects those and the apply worker stops making progress.
    Both halves are asserted: the drift is reported and blocks a cutover, and
    once ``detect-ddl --apply`` has added the column the stream converges
    again.  What must not happen is the run continuing to look healthy while
    the target quietly stops matching.
    """
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    async with connect(cfg.source, source_db) as src:
        await src.execute("ALTER TABLE app.customers ADD COLUMN loyalty_tier text")
        await src.execute(
            "INSERT INTO app.customers (email, display_name, profile, balance,"
            " loyalty_tier) VALUES ('ddl@example.com', 'DDL', '{}', 0, 'gold')"
        )

    drift = await detect_drift(cfg, source_db)
    assert any("loyalty_tier" in (i.name or "") or "loyalty_tier" in i.detail
               for i in drift.items), f"the added column was not detected: {drift.summary}"

    report = await check_cutover_readiness(cfg, database=source_db)
    assert not report.ready, "cutover-check approved a target missing a source column"

    from pg_emigrant.ddl_detector import apply_drift_fixes

    await apply_drift_fixes(cfg, source_db, drift)

    async with connect(cfg.target, source_db) as tgt:
        assert await tgt.fetchval(
            "SELECT 1 FROM pg_attribute WHERE attrelid = 'app.customers'::regclass"
            " AND attname = 'loyalty_tier' AND NOT attisdropped"
        ), "detect-ddl --apply did not add the column"

    # The apply worker retries on its own once the target can hold the row.
    await wait_for_catchup(cfg, source_db, timeout=120)
    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        assert (
            await table_checksum(src, "app", "customers", ["id", "email", "loyalty_tier"])
            == await table_checksum(tgt, "app", "customers", ["id", "email", "loyalty_tier"])
        ), "the stream did not converge after the column was reconciled"


async def test_a_table_created_after_bootstrap_never_races_the_slot(cfg, source_db):
    """Picking up a new table must not disturb the existing replication.

    ``ALTER SUBSCRIPTION … REFRESH PUBLICATION`` and the tablesync workers it
    starts run against the same subscription that is streaming everything else.
    A refresh that dropped or reset the main slot would re-open the data-loss
    window bootstrap exists to close, so the slot's identity and its retained
    position are checked across the operation.
    """
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    slot = sub_name(cfg, source_db)
    async with connect(cfg.source, source_db) as src:
        before = dict(await src.fetchrow(
            "SELECT slot_name, plugin, database, restart_lsn FROM"
            " pg_replication_slots WHERE slot_name = $1", slot))
        await src.execute("CREATE TABLE app.side_car (id int PRIMARY KEY, v text)")
        await src.execute(
            "INSERT INTO app.side_car SELECT g, 'v' || g"
            " FROM generate_series(1, 25::int) g"
        )

    await sync_new_tables(cfg, source_db)
    await _wait_until_ready(cfg, source_db, "app.side_car")
    await wait_for_catchup(cfg, source_db)

    async with connect(cfg.source, source_db) as src:
        after = dict(await src.fetchrow(
            "SELECT slot_name, plugin, database, restart_lsn FROM"
            " pg_replication_slots WHERE slot_name = $1", slot))
        # Compared server-side: pg_lsn comes back as text, and '0/9FFFFFF' vs
        # '0/10000000' orders the wrong way as a string.
        moved_forward = await src.fetchval(
            "SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn) >= 0",
            after["restart_lsn"], before["restart_lsn"],
        )
    assert after["slot_name"] == before["slot_name"]
    assert after["plugin"] == before["plugin"]
    assert after["database"] == before["database"]
    assert moved_forward, (
        f"the slot went backwards while a new table was picked up "
        f"({before['restart_lsn']} → {after['restart_lsn']})"
    )

    async with connect(cfg.target, source_db) as tgt:
        assert await table_row_count(tgt, "app", "side_car") == 25


async def test_detect_ddl_apply_can_create_a_table_with_a_serial_column(
    tmp_path, cfg, source_db
):
    """The remedy the tool points at has to work on the commonest table shape.

    When the automatic pickup cannot create a table it tells the operator to
    run ``detect-ddl --apply``.  That path builds its DDL from the same
    generator, so a table with a ``serial`` column defeated both: the CREATE
    TABLE names ``nextval('…_id_seq')`` and the sequence is created — if at all
    — later in the same run, after the statement that needed it has already
    failed.  Driven through the CLI so the exit code is part of the assertion:
    a fix that did not land must not report success.
    """
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    async with connect(cfg.source, source_db) as src:
        await src.execute(
            "CREATE TABLE app.serial_latecomer ("
            "  id serial PRIMARY KEY,"
            "  big bigserial,"
            "  name text NOT NULL UNIQUE"
            ")"
        )
        await src.execute(
            "INSERT INTO app.serial_latecomer (name)"
            " SELECT 'n' || g FROM generate_series(1, 12::int) g"
        )

    path = write_config(tmp_path / "config.yaml", cfg)
    result = run_cli("detect-ddl", "-c", str(path), "--apply", "--format", "json")
    assert result.returncode == 0, (
        f"detect-ddl --apply exited {result.returncode}; "
        f"stdout={result.stdout[-1500:]} stderr={result.stderr[-1500:]}"
    )

    async with connect(cfg.target, source_db) as tgt:
        assert await tgt.fetchval("SELECT to_regclass('app.serial_latecomer')"), (
            "the table with a serial column was never created on the target"
        )
        # The sequence has to be OWNED BY its column, not left standalone:
        # that link is what keeps it in scope for sequence-sync and makes it
        # disappear with the table.
        owned = await tgt.fetchval(
            "SELECT pg_get_serial_sequence('app.serial_latecomer', 'id')"
        )
        assert owned is not None, (
            "the sequence was created but never linked to its column"
        )

    await _wait_until_ready(cfg, source_db, "app.serial_latecomer")
    await wait_for_catchup(cfg, source_db)
    async with connect(cfg.target, source_db) as tgt:
        assert await table_row_count(tgt, "app", "serial_latecomer") == 12

    # The sequence was created at START WITH 1 while the copied rows already
    # occupy 1..12, so the target is NOT ready yet and must not say it is —
    # the first insert after a cutover here would collide on the primary key.
    report = await check_cutover_readiness(cfg, database=source_db)
    assert not report.ready, (
        "cutover-check approved a target whose newly created sequence would "
        "hand out values that already exist"
    )
    assert "sequences_synchronised" in {c.name for c in report.databases[0].blockers}

    # …and the documented next step clears it.
    seq_result = run_cli("sync-sequences", "-c", str(path), "--format", "json")
    assert seq_result.returncode == 0, seq_result.stderr[-1500:]

    report = await check_cutover_readiness(cfg, database=source_db)
    assert report.ready, [c.summary for c in report.databases[0].blockers]

    async with connect(cfg.target, source_db) as tgt:
        new_id = await tgt.fetchval(
            "INSERT INTO app.serial_latecomer (name) VALUES ('post-cutover')"
            " RETURNING id"
        )
    assert new_id > 12, (
        f"the first insert after cutover reused id {new_id}, which the copied "
        f"rows already hold"
    )
