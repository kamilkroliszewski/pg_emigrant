"""Terminal outcomes for a bootstrap run.

Every database a bootstrap touches ends in exactly one of four states, and the
run as a whole reports the worst of them.  The distinction that matters most is
between ``FAILED`` and ``INCOMPLETE``:

``FAILED``
    The run aborted before replication was configured, and the replication
    objects it had created on the source were rolled back.  Nothing is
    streaming; re-running bootstrap is the repair.

``INCOMPLETE``
    The data copy and replication succeeded, but something the migration was
    asked to reproduce did not arrive — a view that would not compile, a
    trigger whose function is missing, a sequence that could not be read,
    residual schema drift.  No data is at risk *yet*, and tearing the
    subscription down would force a needless full re-copy, so the stream is
    deliberately left running.  What must not happen is the run reporting
    success: the target is not a faithful copy, and cutting over to it would
    turn a missing view into a production incident.

Both are non-zero exits.  Neither is ever downgraded to a warning.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from pg_emigrant import exits


class Outcome(str, Enum):
    SUCCESS = "success"
    INCOMPLETE = "incomplete"
    FAILED = "failed"
    REFUSED = "refused"

    @property
    def is_success(self) -> bool:
        return self is Outcome.SUCCESS


# Worst-first, so max() over the ranks picks the outcome the run is reported as.
_RANK = {Outcome.SUCCESS: 0, Outcome.INCOMPLETE: 1, Outcome.FAILED: 2, Outcome.REFUSED: 3}

_EXIT = {
    Outcome.SUCCESS: exits.SUCCESS,
    Outcome.INCOMPLETE: exits.MIGRATION_FAILED,
    Outcome.FAILED: exits.MIGRATION_FAILED,
    Outcome.REFUSED: exits.UNSAFE_REFUSED,
}


@dataclass
class DatabaseResult:
    """What happened to one database."""

    database: str
    outcome: Outcome = Outcome.SUCCESS
    # Why the run did not fully succeed, in the operator's terms.  Every entry
    # is something that must be resolved before this database is cut over to.
    problems: list[str] = field(default_factory=list)
    # Things worth knowing that do not block a cutover.
    notes: list[str] = field(default_factory=list)
    rows_copied: int = 0
    tables_copied: int = 0

    def fail(self, reason: str) -> None:
        self.outcome = Outcome.FAILED
        self.problems.append(reason)

    def refuse(self, reason: str) -> None:
        self.outcome = Outcome.REFUSED
        self.problems.append(reason)

    def incomplete(self, reason: str) -> None:
        """Record something outstanding, without downgrading a harder outcome."""
        if self.outcome is Outcome.SUCCESS:
            self.outcome = Outcome.INCOMPLETE
        self.problems.append(reason)

    def to_dict(self) -> dict[str, Any]:
        return {
            "database": self.database,
            "outcome": self.outcome.value,
            "problems": self.problems,
            "notes": self.notes,
            "rows_copied": self.rows_copied,
            "tables_copied": self.tables_copied,
        }


@dataclass
class BootstrapReport:
    databases: list[DatabaseResult] = field(default_factory=list)

    def add(self, database: str) -> DatabaseResult:
        result = DatabaseResult(database=database)
        self.databases.append(result)
        return result

    @property
    def outcome(self) -> Outcome:
        if not self.databases:
            return Outcome.SUCCESS
        return max((d.outcome for d in self.databases), key=lambda o: _RANK[o])

    @property
    def passed(self) -> bool:
        return self.outcome.is_success

    @property
    def exit_code(self) -> int:
        return _EXIT[self.outcome]

    def by_outcome(self, outcome: Outcome) -> list[DatabaseResult]:
        return [d for d in self.databases if d.outcome is outcome]

    @property
    def summary(self) -> str:
        counts = {o: len(self.by_outcome(o)) for o in Outcome}
        parts = [f"{n} {o.value}" for o, n in counts.items() if n]
        return f"{len(self.databases)} database(s): " + ", ".join(parts)

    def to_dict(self) -> dict[str, Any]:
        return {
            "outcome": self.outcome.value,
            "passed": self.passed,
            "exit_code": self.exit_code,
            "summary": self.summary,
            "databases": [d.to_dict() for d in self.databases],
        }


class BootstrapIncomplete(RuntimeError):
    """Raised when a bootstrap run did not fully succeed.

    Carries the full :class:`BootstrapReport` so callers that want structure
    have it, while remaining a ``RuntimeError`` for the callers (the web job
    runner, existing scripts) that only need "this raised".
    """

    def __init__(self, report: BootstrapReport):
        self.report = report
        detail = "; ".join(
            f"{d.database} [{d.outcome.value}]: " + " | ".join(d.problems)
            for d in report.databases
            if not d.outcome.is_success
        )
        super().__init__(
            f"Bootstrap did not complete — {report.summary}. {detail}"
        )
