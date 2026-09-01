"""P1: behaviour at size, and behaviour over time.

Two things a fixture-sized migration cannot tell you.

**Size.**  The intra-table ``ctid`` slicing only engages once a table has more
pages than workers, the streaming pipeline only proves its flat memory profile
on something that would not fit in it, and a copy long enough for the source to
move underneath it is the only one that exercises the snapshot boundary
properly.  ``PG_EMIGRANT_STRESS_ROWS`` sets the size; the default is small
enough that an ordinary run and every CI job still pay only a few seconds for
it, and a real stress run is one environment variable away.

**Time.**  A migration is not a command, it is a window — hours of steady-state
replication with an application writing the whole time, ending in a cutover
decision.  The soak test compresses that shape into
``PG_EMIGRANT_SOAK_SECONDS``: continuous INSERT/UPDATE/DELETE and sequence
traffic against a live subscription, then writes stop, replication converges,
and the same checks an operator would run at 3am have to agree that source and
target are identical.  The default is short so it always runs — a soak test
that is skipped by default is a soak test nobody notices has rotted — and the
real duration is the same variable.

    PG_EMIGRANT_STRESS_ROWS=5000000 pytest tests/integration/test_stress_and_soak.py
    PG_EMIGRANT_SOAK_SECONDS=3600   pytest tests/integration/test_stress_and_soak.py
"""

from __future__ import annotations

import asyncio
import os
import time

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.cutover import check_cutover_readiness
from pg_emigrant.db import connect
from pg_emigrant.health import ReplicationState, replication_health
from pg_emigrant.sequence_sync import sync_sequences_once
from tests.helpers.replication import wait_for_catchup
from tests.helpers.verify import (
    assert_tables_identical,
    table_checksum,
    table_row_count,
)
from tests.helpers.workload import ConcurrentWriter

pytestmark = [pytest.mark.integration, pytest.mark.slow]

SCHEMAS = ["app", "reporting"]

# Big enough that the table spans many more pages than table_parallel_workers
# (so the ctid slicing is genuinely exercised) and small enough to cost a few
# seconds.  Raise it for a real stress run; see the module docstring.
STRESS_ROWS = int(os.environ.get("PG_EMIGRANT_STRESS_ROWS", "60000"))
SOAK_SECONDS = float(os.environ.get("PG_EMIGRANT_SOAK_SECONDS", "15"))


async def _make_wide_table(cfg, dbname: str, rows: int) -> None:
    async with connect(cfg.source, dbname) as src:
        await src.execute(
            "CREATE TABLE app.bulk ("
            "  id bigint PRIMARY KEY,"
            "  payload text NOT NULL,"
            "  tag text,"
            "  amount numeric(12,2),"
            "  created timestamptz NOT NULL DEFAULT now()"
            ")"
        )
        await src.execute(
            "INSERT INTO app.bulk (id, payload, tag, amount)"
            " SELECT g, repeat('p', 120) || g, 'tag' || (g % 97),"
            "        (g % 100000)::numeric / 100"
            " FROM generate_series(1, $1::bigint) g",
            rows,
        )
        await src.execute("ANALYZE app.bulk")


async def test_a_large_table_copies_completely_and_in_parallel_slices(cfg, source_db):
    """Every row, once, across the ``ctid`` slice boundaries.

    Slicing a table by page range is where an off-by-one loses or duplicates
    rows, and it is invisible at fixture size because the split never happens.
    The last slice is deliberately unbounded so rows added since the last
    ANALYZE are not dropped off the end; a checksum over the whole table is
    what proves both halves of that.
    """
    cfg.table_parallel_workers = 4
    await _make_wide_table(cfg, source_db, STRESS_ROWS)

    started = time.monotonic()
    report = await bootstrap(cfg, database=source_db)
    elapsed = time.monotonic() - started
    assert report.passed, report.summary

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        pages = await src.fetchval(
            "SELECT pg_relation_size('app.bulk'::regclass)"
            " / current_setting('block_size')::int"
        )
        assert pages >= cfg.table_parallel_workers, (
            f"app.bulk is only {pages} pages, so the ctid slicing never engaged "
            f"and this test asserts nothing about it — raise "
            f"PG_EMIGRANT_STRESS_ROWS"
        )
        assert await table_row_count(tgt, "app", "bulk") == STRESS_ROWS
        assert (
            await table_checksum(src, "app", "bulk")
            == await table_checksum(tgt, "app", "bulk")
        ), "the sliced copy did not reproduce the table exactly"

    # Not an assertion about speed — a threshold here would be a flaky test on
    # a busy machine.  Recorded so a run that suddenly takes an order of
    # magnitude longer is visible in the log.
    print(
        f"\n[stress] copied {STRESS_ROWS:,} rows ({pages:,} pages) in "
        f"{elapsed:.1f}s with table_parallel_workers={cfg.table_parallel_workers}"
    )


async def test_a_large_table_converges_while_it_is_being_written_to(cfg, source_db):
    """The snapshot boundary, at a size where the copy actually takes time.

    With a fixture-sized table the copy finishes before the workload writes
    anything interesting, so the window in which a committed transaction could
    fall between the snapshot and the stream is never open long enough to lose
    anything.  Here it is.
    """
    cfg.table_parallel_workers = 4
    await _make_wide_table(cfg, source_db, STRESS_ROWS)

    async with ConcurrentWriter(cfg.source, source_db, delay=0.0) as writer:
        report = await bootstrap(cfg, database=source_db)
        assert report.passed, report.summary
        # Keep writing after the copy so the stream is exercised too.
        await asyncio.sleep(1.0)

    assert writer.inserted_customers > 0 and writer.deleted > 0, (
        "the workload wrote nothing meaningful during the migration"
    )

    await wait_for_catchup(cfg, source_db, timeout=180)
    await sync_sequences_once(cfg, source_db)
    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        await assert_tables_identical(src, tgt, SCHEMAS)
        assert (
            await table_checksum(src, "app", "bulk")
            == await table_checksum(tgt, "app", "bulk")
        )


async def test_soak_a_live_migration_then_decide_to_cut_over(cfg, source_db):
    """The shape of a real migration window, compressed.

    Bootstrap, then a continuous workload against a live subscription for
    ``PG_EMIGRANT_SOAK_SECONDS``, then the cutover sequence exactly as the
    runbook gives it: stop writing, converge, final sequence sync, and ask
    ``cutover-check``.  It has to say yes — and the data has to actually be
    identical, which is the assertion ``cutover-check`` itself cannot make.
    """
    report = await bootstrap(cfg, database=source_db)
    assert report.passed, report.summary
    await wait_for_catchup(cfg, source_db)

    deadline = time.monotonic() + SOAK_SECONDS
    async with ConcurrentWriter(cfg.source, source_db, delay=0.0) as writer:
        while time.monotonic() < deadline:
            await asyncio.sleep(0.5)
            # Sequence sync is a steady-state job, not a cutover-only one, so
            # it runs throughout the window the way --loop would run it.
            await sync_sequences_once(cfg, source_db)
            health = await replication_health(cfg, source_db)
            assert health.state in (
                ReplicationState.HEALTHY, ReplicationState.LAGGING
            ), (
                f"replication went {health.state.value.upper()} during a "
                f"perfectly ordinary write workload: {health.reasons}"
            )

    assert writer.inserted_customers > 0, "the soak workload never wrote anything"

    # ── the cutover sequence, in the documented order ─────────────────────
    await wait_for_catchup(cfg, source_db, timeout=180)
    await sync_sequences_once(cfg, source_db, margin=1000)

    readiness = await check_cutover_readiness(cfg, database=source_db)
    assert readiness.ready, (
        f"cutover-check refused after a clean soak: "
        f"{[(c.name, c.summary) for c in readiness.databases[0].blockers]}"
    )

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        await assert_tables_identical(src, tgt, SCHEMAS)

    print(
        f"\n[soak] {SOAK_SECONDS:.0f}s: {writer.inserted_customers} inserts, "
        f"{writer.updated} updates, {writer.deleted} deletes converged exactly"
    )
