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
* ``PG_EMIGRANT_TEST_FAIL_AT=<phase>`` selects the phase.

Both are required.  With only the phase set, hooks stay inert and a loud
warning is logged once, because that combination means someone believes
injection is on when it is not.  Nothing here reads the YAML configuration:
failure injection is never a migration setting and must not be reachable from
a config file that gets copied between environments.
"""

from __future__ import annotations

import os

from pg_emigrant.utils import get_logger

log = get_logger(__name__)

ARM_VAR = "PG_EMIGRANT_TEST_HOOKS_ENABLED"
PHASE_VAR = "PG_EMIGRANT_TEST_FAIL_AT"

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


def _armed() -> bool:
    global _warned
    phase = os.environ.get(PHASE_VAR)
    if not phase:
        return False
    if os.environ.get(ARM_VAR) != "1":
        if not _warned:
            _warned = True
            log.warning(
                "%s=%r is set but %s is not '1' — failure injection is NOT active. "
                "This is the safe default; set both only in the test suite.",
                PHASE_VAR, phase, ARM_VAR,
            )
        return False
    return True


def active_phase() -> str | None:
    """The armed phase name, or None when injection is off."""
    if not _armed():
        return None
    phase = os.environ[PHASE_VAR]
    if phase not in PHASES:
        raise InjectedFailure(
            f"{PHASE_VAR}={phase!r} is not a known phase; expected one of {', '.join(PHASES)}"
        )
    return phase


def maybe_fail(phase: str) -> None:
    """Raise :class:`InjectedFailure` when *phase* is the armed phase.

    Call sites are no-ops (one environment lookup) whenever injection is off,
    which is every run that is not this repository's own test suite.
    """
    if active_phase() == phase:
        log.error("TEST HOOK: injecting a failure at phase %r", phase)
        raise InjectedFailure(f"injected test failure at phase {phase!r}")
