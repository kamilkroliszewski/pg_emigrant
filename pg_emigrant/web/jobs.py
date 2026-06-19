"""In-memory background job manager for long-running / mutating operations.

Bootstrap, teardown, start/stop, sequence sync, reinit and drift-apply are run
off the request thread so the browser stays responsive and the UI can stream
their progress.  Each job captures the ``pg_emigrant`` logger output produced by
*its own* worker thread (filtered by thread id, so concurrent jobs don't mix),
and exposes ``status`` / ``logs`` / ``error`` for polling.

Note: decorative Rich ``console.print`` output (e.g. the bootstrap progress
spinner and per-table row counts) goes to the server's stdout, not to a job's
log buffer.  The substantive orchestration messages use the standard ``logging``
module and *are* captured here.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

CoroFactory = Callable[[], Awaitable[Any]]

# Logger that all orchestration modules are children of (e.g.
# ``pg_emigrant.bootstrap``).  A handler attached here receives their records
# through standard logging propagation.
_ROOT_LOGGER_NAME = "pg_emigrant"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class Job:
    """A single background operation and its captured output."""

    id: str
    name: str
    database: Optional[str]
    status: str = "pending"  # pending | running | success | error
    created_at: str = field(default_factory=_now)
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    logs: list[dict[str, str]] = field(default_factory=list)
    error: Optional[str] = None
    result: Any = None

    def add_log(self, level: str, message: str) -> None:
        self.logs.append({"time": _now(), "level": level, "message": message})

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "database": self.database,
            "status": self.status,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "logs": list(self.logs),
            "error": self.error,
            "result": self.result,
        }


class _JobLogHandler(logging.Handler):
    """Logging handler that appends records emitted by one worker thread."""

    def __init__(self, job: Job, thread_ident: int) -> None:
        super().__init__()
        self.job = job
        self.thread_ident = thread_ident

    def emit(self, record: logging.LogRecord) -> None:
        if record.thread != self.thread_ident:
            return
        try:
            msg = self.format(record)
        except Exception:  # never let logging break the job
            msg = record.getMessage()
        self.job.add_log(record.levelname, msg)


class JobManager:
    """Runs coroutine factories in daemon threads and tracks their state."""

    def __init__(self, max_history: int = 200) -> None:
        self._jobs: dict[str, Job] = {}
        self._order: list[str] = []
        self._lock = threading.Lock()
        self._max_history = max_history

    # ── public API ────────────────────────────────────────────────────────────
    def submit(
        self,
        name: str,
        make_coro: CoroFactory,
        database: Optional[str] = None,
    ) -> Job:
        """Create a job and start running *make_coro* in a background thread."""
        job = Job(id=uuid.uuid4().hex[:12], name=name, database=database)
        with self._lock:
            self._jobs[job.id] = job
            self._order.append(job.id)
            self._prune_locked()
        thread = threading.Thread(
            target=self._run, args=(job, make_coro), name=f"job-{job.id}", daemon=True
        )
        thread.start()
        return job

    def get(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def list(self) -> list[Job]:
        with self._lock:
            return [self._jobs[i] for i in reversed(self._order) if i in self._jobs]

    # ── internals ─────────────────────────────────────────────────────────────
    def _prune_locked(self) -> None:
        while len(self._order) > self._max_history:
            oldest = self._order.pop(0)
            self._jobs.pop(oldest, None)

    def _run(self, job: Job, make_coro: CoroFactory) -> None:
        ident = threading.get_ident()
        handler = _JobLogHandler(job, ident)
        handler.setFormatter(logging.Formatter("%(name)s: %(message)s"))
        logger = logging.getLogger(_ROOT_LOGGER_NAME)
        # Ensure INFO records from pg_emigrant.* reach our handler regardless of
        # the global root configuration (don't override a DEBUG/verbose level).
        if logger.level == logging.NOTSET or logger.level > logging.INFO:
            logger.setLevel(logging.INFO)
        logger.addHandler(handler)

        job.started_at = _now()
        job.add_log("INFO", f"Started: {job.name}")
        job.status = "running"
        # ``status`` is assigned *last* in each branch so a poller never observes
        # a terminal state before ``result`` / ``error`` / logs are populated.
        try:
            result = asyncio.run(make_coro())
            job.result = _jsonable(result)
            done_msg = ""
            if isinstance(result, dict) and result.get("message"):
                done_msg = f" — {result['message']}"
            job.add_log("INFO", f"Completed successfully{done_msg}")
            job.finished_at = _now()
            job.status = "success"
        except Exception as exc:  # noqa: BLE001 — surface everything to the UI
            job.error = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            job.add_log("ERROR", f"{type(exc).__name__}: {exc}")
            job.finished_at = _now()
            job.status = "error"
        finally:
            logger.removeHandler(handler)


def _jsonable(value: Any) -> Any:
    """Best-effort conversion of a job result into a JSON-serialisable value."""
    if value is None or isinstance(value, (str, int, float, bool, list, dict)):
        return value
    return str(value)
