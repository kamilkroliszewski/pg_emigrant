"""P1: re-running, and being killed part-way through.

A migration is not a single command run once under ideal conditions.  It gets
re-run after a fix, interrupted by an operator who changed their mind, and
occasionally killed outright.  What must never happen is that the debris of
one attempt makes the next one fail — or, worse, silently produce a different
result.

SIGKILL deserves particular attention: the process cannot clean up after
itself, so anything it was holding is left on the *source*.  An orphaned
logical replication slot is the dangerous one, because it goes on retaining
WAL on a production primary until somebody notices the disk filling.
"""

from __future__ import annotations

import asyncio
import signal
import time

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.db import connect
from pg_emigrant.replication import drop_publication, drop_subscription, sub_name
from pg_emigrant.report import BootstrapIncomplete
from tests.helpers.cli import spawn_cli, write_config
from tests.helpers.replication import all_slots, wait_for_catchup
from tests.helpers.verify import assert_tables_identical

pytestmark = [pytest.mark.integration, pytest.mark.slow]

SCHEMAS = ["app", "reporting"]


async def test_rerunning_a_successful_bootstrap_is_refused_not_silently_destructive(
    cfg, source_db
):
    """A second bootstrap over live replication must not tear it down.

    Without the guard, the re-run treats the live slot as orphaned, terminates
    its walsender, recreates it at a new LSN and TRUNCATEs the target while the
    apply worker is still running — losing everything committed in between and
    then failing on duplicate-key conflicts.
    """
    await bootstrap(cfg, database=source_db)
    slots_before = await all_slots(cfg)

    with pytest.raises(BootstrapIncomplete) as excinfo:
        await bootstrap(cfg, database=source_db)

    problems = " ".join(excinfo.value.report.databases[0].problems)
    assert "already replicating" in problems, problems
    assert "teardown" in problems, "the refusal does not say how to proceed"

    assert await all_slots(cfg) == slots_before, (
        "the refused re-run disturbed the live replication slot"
    )
    async with connect(cfg.target, source_db) as tgt:
        assert await tgt.fetchval("SELECT count(*) FROM app.customers") == 504, (
            "the refused re-run truncated the target"
        )


async def test_teardown_then_bootstrap_converges_again(cfg, source_db):
    """The documented way to redo a migration must actually work."""
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    async with connect(cfg.source, source_db) as src:
        await src.execute(
            "INSERT INTO app.nasty_strings (id, val, note) VALUES (321, 'v', 'between runs')"
        )

    await drop_subscription(cfg, source_db)
    await drop_publication(cfg, source_db)
    assert await all_slots(cfg) == [], "teardown left a replication slot behind"

    report = await bootstrap(cfg, database=source_db)
    assert report.passed, report.summary

    await wait_for_catchup(cfg, source_db)
    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        await assert_tables_identical(src, tgt, SCHEMAS)


async def test_bootstrap_after_a_crash_cleans_up_the_orphaned_slot(cfg, source_db):
    """An orphaned slot from an interrupted run must not block the next one.

    This is the state SIGKILL leaves behind: a slot on the source with no
    subscription attached to it.
    """
    slot = sub_name(cfg, source_db)
    async with connect(cfg.source, source_db) as src:
        await src.execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slot
        )
    assert await all_slots(cfg) == [slot]

    report = await bootstrap(cfg, database=source_db)
    assert report.passed, report.summary

    await wait_for_catchup(cfg, source_db)
    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        await assert_tables_identical(src, tgt, SCHEMAS)
    assert await all_slots(cfg) == [slot], (
        "the run should have reused the slot name, not accumulated another"
    )


@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM])
def test_signalled_bootstrap_leaves_no_orphaned_slot(tmp_path, cfg, source_db, sig):
    """SIGINT/SIGTERM part-way through must not strand a slot on the source.

    A slot with nothing consuming it retains WAL on the production primary
    indefinitely — the failure mode that fills a disk days after everyone
    stopped thinking about the migration.
    """
    path = write_config(tmp_path / "config.yaml", cfg)
    proc = spawn_cli("bootstrap", "-c", str(path), "--skip-preflight")
    try:
        _wait_for_slot_or_exit(cfg, source_db, proc)
        proc.send_signal(sig)
        proc.wait(timeout=60)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=30)

    assert proc.returncode != 0, "an interrupted migration reported success"
    leftover = asyncio.run(all_slots(cfg))
    assert leftover == [], (
        f"{sig.name} during bootstrap stranded replication slot(s) {leftover} on "
        f"the source, where they retain WAL until dropped by hand"
    )


@pytest.mark.parametrize(
    "phase",
    ["data_copy", "index_create", "foreign_key", "sequence_sync", "subscription_create"],
)
def test_signalled_at_each_later_phase_leaves_no_orphaned_slot(
    tmp_path, cfg, source_db, phase
):
    """The same invariant, at each phase after the slot exists.

    Signalling "somewhere during the run" only ever lands in whichever phase
    happens to be slowest — the index build and the sequence sync take
    milliseconds on a fixture this size, so a timing-based test would never
    reach them and would pass while asserting nothing. The pause hook stops the
    run *at* the named phase so the signal is delivered exactly there.
    """
    path = write_config(tmp_path / "config.yaml", cfg)
    proc = spawn_cli(
        "bootstrap", "-c", str(path), "--skip-preflight",
        env={"PG_EMIGRANT_TEST_HOOKS_ENABLED": "1", "PG_EMIGRANT_TEST_PAUSE_AT": phase},
    )
    try:
        _wait_for_pause(cfg, source_db, proc, phase)
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=90)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=30)

    assert proc.returncode != 0, f"interrupting at {phase} reported success"
    leftover = asyncio.run(all_slots(cfg))
    assert leftover == [], (
        f"SIGTERM at phase {phase} stranded replication slot(s) {leftover}"
    )


def _wait_for_pause(cfg, dbname, proc, phase, timeout: float = 90.0) -> None:
    """Block until the run reaches the paused phase.

    The slot is created before every phase this is used with, so its existence
    plus a moment's settling is a sufficient signal that the run has got at
    least that far; the pause itself then holds it there.
    """
    slot = sub_name(cfg, dbname)
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise AssertionError(
                f"bootstrap exited ({proc.returncode}) before pausing at {phase}; "
                f"stderr: {proc.stderr.read()[-2000:]}"
            )
        if slot in asyncio.run(all_slots(cfg)):
            # The phases tested here all follow slot creation, and the pause is
            # a hard block, so once the slot exists the run is either at the
            # pause already or on its way there within a copy's worth of time.
            time.sleep(3)
            return
        time.sleep(0.2)
    raise AssertionError(f"bootstrap never reached the paused phase {phase}")


def test_sigkilled_bootstrap_is_recoverable_by_rerunning(tmp_path, cfg, source_db):
    """SIGKILL cannot clean up after itself; the *next* run must.

    The process gets no chance to run any handler, so this is the one case
    where debris on the source is expected — and the requirement moves to
    recovery: a plain re-run has to detect the orphan, take it over, and
    converge.
    """
    path = write_config(tmp_path / "config.yaml", cfg)
    proc = spawn_cli("bootstrap", "-c", str(path), "--skip-preflight")
    try:
        _wait_for_slot_or_exit(cfg, source_db, proc)
        proc.kill()
        proc.wait(timeout=60)
    finally:
        if proc.poll() is None:
            proc.kill()

    # Whatever it left behind, a re-run must handle it without manual repair.
    report = asyncio.run(bootstrap(cfg, database=source_db))
    assert report.passed, report.summary

    async def _check():
        await wait_for_catchup(cfg, source_db)
        async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
            await assert_tables_identical(src, tgt, SCHEMAS)

    asyncio.run(_check())


def _wait_for_slot_or_exit(cfg, dbname, proc, timeout: float = 60.0) -> None:
    """Block until the run has created its slot — the interesting moment.

    Signalling earlier would only test the argument parser.
    """
    slot = sub_name(cfg, dbname)
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise AssertionError(
                f"bootstrap exited ({proc.returncode}) before creating a slot; "
                f"stderr: {proc.stderr.read()[-2000:]}"
            )
        if slot in asyncio.run(all_slots(cfg)):
            return
        time.sleep(0.2)
    raise AssertionError("bootstrap never created its replication slot")
