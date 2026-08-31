"""Stable exit codes for the CLI.

A migration runbook is usually driven by something that reads the exit status,
not the terminal.  "Non-zero" is not enough for that: refusing to do something
unsafe, failing halfway, and finding replication unhealthy all call for
different automated responses, and collapsing them into ``1`` forces the
caller to scrape human-readable text to tell them apart.

These values are part of the tool's contract; existing meanings are additive
only.  ``0`` and ``1`` keep the meaning they always had, so a runbook that only
distinguishes success from failure is unaffected.
"""

from __future__ import annotations

# Everything asked for completed, and completed fully.
SUCCESS = 0
# Something went wrong that has no more specific code.
GENERIC_FAILURE = 1
# The configuration is wrong or unusable — nothing was attempted.
CONFIG_ERROR = 2
# A read-only preflight check failed; nothing was modified.
PREFLIGHT_FAILED = 3
# A migration ran and did not complete: aborted, or finished with objects or
# data outstanding.  The target is not fit to cut over to.
MIGRATION_FAILED = 4
# Replication exists but is not healthy (broken, or lagging past a threshold).
REPLICATION_UNHEALTHY = 5
# A repair cannot be performed without losing data, and was refused.
RECOVERY_IMPOSSIBLE = 6
# An operation was refused up front because performing it would be unsafe.
UNSAFE_REFUSED = 7

_NAMES = {
    SUCCESS: "success",
    GENERIC_FAILURE: "failure",
    CONFIG_ERROR: "configuration error",
    PREFLIGHT_FAILED: "preflight failed",
    MIGRATION_FAILED: "migration failed",
    REPLICATION_UNHEALTHY: "replication unhealthy",
    RECOVERY_IMPOSSIBLE: "recovery impossible",
    UNSAFE_REFUSED: "unsafe operation refused",
}


def name(code: int) -> str:
    return _NAMES.get(code, f"exit {code}")
