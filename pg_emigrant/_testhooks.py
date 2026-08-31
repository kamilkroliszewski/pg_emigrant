"""Deterministic failure injection for the integration suite.

The failure-injection tests need pg_emigrant to fail *at a precise phase* of a
real bootstrap — after a replication slot exists but before the copy finishes,
between the copy and the subscription, and so on — so that the cleanup,
exit-code and re-run behaviour around each phase can be asserted against a
real PostgreSQL cluster.  Simulating that from the outside (killing the
process, breaking the network) is both flaky and unable to reach most of the
interesting points.

Arming is deliberately awkward, because a failure injector that can be
switched on by one stray environment variable is a production hazard:

* ``PG_EMIGRANT_TEST_HOOKS_ENABLED=1`` must be set — the arming switch, which
  exists for no purpose other than this and would never be set by accident on
  a migration host.
* ``PG_EMIGRANT_TEST_FAIL_AT=<phase>`` raises at that phase.
* ``PG_EMIGRANT_TEST_PAUSE_AT=<phase>`` instead *blocks* there, which is what
  the signal tests need: without it, "send SIGTERM during the index build"
  means racing a phase that lasts milliseconds, and the test either misses the
  window or lands somewhere else entirely — asserting nothing while appearing
  to pass.

The arming switch is required for all of them.  With only a phase variable
set, hooks stay inert and a loud warning is logged once, because that
combination means someone believes injection is on when it is not.  Nothing
here reads the YAML configuration: failure injection is never a migration
setting and must not be reachable from a config file that gets copied between
environments.
"""

from __future__ import annotations

import os
import time

from pg_emigrant.utils import get_logger

log = get_logger(__name__)

ARM_VAR = "PG_EMIGRANT_TEST_HOOKS_ENABLED"
PHASE_VAR = "PG_EMIGRANT_TEST_FAIL_AT"
PAUSE_VAR = "PG_EMIGRANT_TEST_PAUSE_AT"

# How long a paused phase blocks before giving up.  Bounded so a test that
# forgets to signal the process fails on its own timeout rather than hanging
# the suite.
PAUSE_SECONDS = 120

# Every phase the bootstrap sequence can be interrupted at.  Kept as an
# explicit tuple so a typo in a test names a phase that does not exist and is
# rejected, rather than silently never firing.
PHASES = (
    "database_create",
    "schema_create",
    "type_create",
    "table_create",
    "replica_identity",
    "publication_create",
    "slot_create",
    "data_copy",
    "index_create",
    "foreign_key",
    "function_create",
    "view_create",
    "trigger_create",
    "ownership_sync",
    "privilege_sync",
    "sequence_sync",
    "subscription_create",
)

_warned = False


class InjectedFailure(RuntimeError):
    """Raised by :func:`maybe_fail` when the armed phase is reached."""


def _armed(var: str = PHASE_VAR) -> bool:
    global _warned
    phase = os.environ.get(var)
    if not phase:
        return False
    if os.environ.get(ARM_VAR) != "1":
        if not _warned:
            _warned = True
            log.warning(
                "%s=%r is set but %s is not '1' — failure injection is NOT active. "
                "This is the safe default; set both only in the test suite.",
                var, phase, ARM_VAR,
            )
        return False
    return True


def active_phase(var: str = PHASE_VAR) -> str | None:
    """The armed phase name for *var*, or None when injection is off."""
    if not _armed(var):
        return None
    phase = os.environ[var]
    if phase not in PHASES:
        raise InjectedFailure(
            f"{var}={phase!r} is not a known phase; expected one of {', '.join(PHASES)}"
        )
    return phase


def maybe_fail(phase: str) -> None:
    """Raise, or block, when *phase* is armed.

    Call sites are no-ops (one or two environment lookups) whenever injection
    is off, which is every run that is not this repository's own test suite.

    The blocking form is deliberately a plain ``time.sleep`` rather than an
    ``await``: it holds the whole event loop, so the run really is stopped at
    this phase and nothing else advances past it while the test does its work.
    A signal still lands, because Python delivers it between bytecodes.
    """
    if active_phase(PAUSE_VAR) == phase:
        log.error(
            "TEST HOOK: pausing at phase %r for up to %ds", phase, PAUSE_SECONDS
        )
        time.sleep(PAUSE_SECONDS)
        return
    if active_phase() == phase:
        log.error("TEST HOOK: injecting a failure at phase %r", phase)
        raise InjectedFailure(f"injected test failure at phase {phase!r}")
