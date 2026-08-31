"""P0: every bootstrap phase, failed on purpose, checked for the same invariants.

A migration tool is judged by what it does when a step fails, not by what it
does when everything works.  For each phase this asserts the four things that
must hold no matter where the failure lands:

1. the run reports failure — never a false success;
2. no replication slot is left behind on the source (an abandoned logical slot
   retains WAL on a production primary until someone notices);
3. no publication and no subscription are left behind;
4. re-running bootstrap afterwards succeeds and produces a consistent target.

Invariant 4 is the one that is easy to get wrong and expensive to discover in
production: a cleanup that is *almost* complete leaves a re-run failing on the
debris of the first attempt.
"""

from __future__ import annotations

import os
from contextlib import contextmanager

import pytest

from pg_emigrant._testhooks import ARM_VAR, PHASE_VAR, PHASES
from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.report import BootstrapIncomplete, Outcome
from pg_emigrant.db import connect
from tests.helpers.replication import all_publications, all_slots, all_subscriptions
from tests.helpers.verify import assert_tables_identical

pytestmark = [pytest.mark.integration, pytest.mark.slow]

SCHEMAS = ["app", "reporting"]

# Phases reached during an ordinary bootstrap of the fixture, in order.
# 'index_create' and the rest run after the copy; injecting there is what
# proves the post-copy half also cleans up after itself.
INJECTABLE = [p for p in PHASES if p not in ("replica_identity",)]


@contextmanager
def inject(phase: str):
    os.environ[ARM_VAR] = "1"
    os.environ[PHASE_VAR] = phase
    try:
        yield
    finally:
        os.environ.pop(ARM_VAR, None)
        os.environ.pop(PHASE_VAR, None)


async def _assert_no_leaked_replication_state(cfg, dbname):
    slots = await all_slots(cfg)
    assert slots == [], (
        f"replication slot(s) {slots} survived a failed bootstrap — an abandoned "
        f"logical slot retains WAL on the source until it is dropped by hand"
    )
    pubs = [p for p in await all_publications(cfg, dbname) if p.startswith("pg_emigrant")]
    assert pubs == [], f"publication(s) {pubs} survived a failed bootstrap"
    async with connect(cfg.target) as probe:
        has_db = await probe.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", dbname)
    if has_db:
        subs = await all_subscriptions(cfg, dbname)
        assert subs == [], f"subscription(s) {subs} survived a failed bootstrap"


@pytest.mark.parametrize("phase", INJECTABLE)
async def test_failure_at_phase_fails_closed_and_leaves_no_replication_state(
    cfg, source_db, phase
):
    with inject(phase):
        with pytest.raises(BootstrapIncomplete) as excinfo:
            await bootstrap(cfg, database=source_db)

    report = excinfo.value.report
    assert report.outcome is Outcome.FAILED, (
        f"phase {phase}: the run ended as {report.outcome.value}, not 'failed' — "
        f"{report.summary}"
    )
    assert report.exit_code != 0
    # The report must carry a reason, not just a status: an operator reading
    # only the exit code and the summary has to be able to act on it.
    problems = report.databases[0].problems
    assert problems and all(p.strip() for p in problems), (
        f"phase {phase}: the run failed without recording why"
    )
    await _assert_no_leaked_replication_state(cfg, source_db)


@pytest.mark.parametrize("phase", ["table_create", "data_copy", "index_create",
                                   "foreign_key", "sequence_sync", "subscription_create"])
async def test_rerun_after_failure_succeeds_and_converges(cfg, source_db, phase):
    """Cleanup must be complete enough that a plain re-run works.

    These six phases straddle every kind of partially-created state: half a
    schema, a truncated target, missing indexes, missing constraints, an
    un-synced sequence, and a slot with no subscription attached.
    """
    with inject(phase):
        with pytest.raises(BootstrapIncomplete):
            await bootstrap(cfg, database=source_db)

    report = await bootstrap(cfg, database=source_db)
    assert report.passed, f"the re-run did not fully succeed: {report.summary}"

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        await assert_tables_identical(src, tgt, SCHEMAS)


async def test_hooks_are_inert_without_the_arming_variable(cfg, source_db, monkeypatch):
    """The phase variable alone must never fire — production safety.

    A failure injector that one stray environment variable can switch on is a
    production hazard, so arming deliberately takes two.
    """
    monkeypatch.setenv(PHASE_VAR, "data_copy")
    monkeypatch.delenv(ARM_VAR, raising=False)

    await bootstrap(cfg, database=source_db)  # must complete normally

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        await assert_tables_identical(src, tgt, SCHEMAS)
