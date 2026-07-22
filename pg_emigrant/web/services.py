"""Synchronous bridges from Flask to pg_emigrant's async orchestration layer.

Flask request handlers are synchronous while the orchestration functions are
``async``.  These helpers run a coroutine to completion with ``asyncio.run`` and
reshape the results into plain, JSON-serialisable structures for the templates
and the JSON API.

No orchestration logic is duplicated here — every function delegates to the
existing modules:

* :func:`pg_emigrant.db.discover_databases`
* :func:`pg_emigrant.monitor._collect_db_status` (returns a plain dict already)
* :func:`pg_emigrant.ddl_detector.detect_drift` / ``apply_drift_fixes``
* :func:`pg_emigrant.replication.enable_subscription` / ``disable_subscription`` /
  ``drop_subscription`` / ``drop_publication`` / ``reinit_sync``
* :func:`pg_emigrant.sequence_sync.sync_sequences_once`
* :func:`pg_emigrant.bootstrap.bootstrap`
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Optional, TypeVar

from pg_emigrant.config import ReplicatorConfig
from pg_emigrant.db import discover_databases
from pg_emigrant.monitor import _ALL_SECTIONS, _collect_db_status

T = TypeVar("T")

CoroFactory = Callable[[], Awaitable[Any]]


def run_sync(coro: Awaitable[T]) -> T:
    """Run an async coroutine to completion from synchronous Flask code."""
    return asyncio.run(coro)


# ──────────────────────────────────────────────────────────────────────────────
# Read-only helpers (called inline inside request handlers)
# ──────────────────────────────────────────────────────────────────────────────

def list_databases(cfg: ReplicatorConfig) -> list[str]:
    """Discover the databases pg_emigrant would operate on."""
    return run_sync(discover_databases(cfg))


def collect_status(
    cfg: ReplicatorConfig,
    dbname: str,
    sections: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Gather the monitoring status for one database as a plain dict.

    Reuses :func:`pg_emigrant.monitor._collect_db_status`, which already returns
    a JSON-serialisable structure and records per-section errors instead of
    raising — so the dashboard renders even when a database is unreachable or
    not yet bootstrapped.
    """
    secs = frozenset(sections) if sections else _ALL_SECTIONS
    return run_sync(_collect_db_status(cfg, dbname, secs))


def collect_all_status(
    cfg: ReplicatorConfig,
    sections: Optional[list[str]] = None,
) -> list[dict[str, Any]]:
    """Gather monitoring status for every database in a single batched pass.

    Discovers the databases and delegates to
    :func:`pg_emigrant.monitor.collect_all_status`, which fetches the
    cluster-wide sections with one query each and fans the per-database sections
    out concurrently — far cheaper than the browser issuing one request per
    database.
    """
    from pg_emigrant.monitor import _ALL_SECTIONS, collect_all_status as _collect_all

    secs = frozenset(sections) if sections else _ALL_SECTIONS

    async def _run() -> list[dict[str, Any]]:
        databases = await discover_databases(cfg)
        return await _collect_all(cfg, databases, secs)

    return run_sync(_run())


def drift_report(cfg: ReplicatorConfig, dbname: str) -> dict[str, Any]:
    """Run a full drift scan and serialise the report for the API."""
    from pg_emigrant.ddl_detector import detect_drift

    report = run_sync(detect_drift(cfg, dbname))
    return {
        "database": dbname,
        "has_drift": report.has_drift,
        "summary": report.summary,
        "items": [
            {
                "object_type": item.object_type,
                "schema": item.schema,
                "table": item.table,
                "name": item.name,
                "drift_type": item.drift_type,
                "detail": item.detail,
                "fix_ddl": item.fix_ddl,
            }
            for item in report.items
        ],
    }


# ──────────────────────────────────────────────────────────────────────────────
# Config view (read-only) — password is never sent to the browser
# ──────────────────────────────────────────────────────────────────────────────

_MASK = "••••••••"


def _mask_endpoint(ep: Any) -> dict[str, Any]:
    return {
        "host": ep.host,
        "port": ep.port,
        "user": ep.user,
        "password": _MASK if ep.password else "(empty)",
        "dbname": ep.dbname,
        "sslmode": ep.sslmode,
    }


def masked_config(cfg: ReplicatorConfig) -> dict[str, Any]:
    """Render the loaded config for display, with passwords masked."""
    return {
        "source": _mask_endpoint(cfg.source),
        "target": _mask_endpoint(cfg.target),
        "schemas": cfg.schemas,
        "exclude_schemas": cfg.exclude_schemas,
        "databases": cfg.databases,
        "exclude_databases": cfg.exclude_databases,
        "exclude_tables": cfg.exclude_tables,
        "publication_name": cfg.publication_name,
        "subscription_name": cfg.subscription_name,
        "replication_slot_name": cfg.replication_slot_name,
        "parallel_workers": cfg.parallel_workers,
        "table_parallel_workers": cfg.table_parallel_workers,
        "sequence_sync_interval": cfg.sequence_sync_interval,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Action registry — coroutine factories run by the JobManager in a background
# thread.  Each entry describes how to build the work and whether the UI should
# require an explicit confirmation.
# ──────────────────────────────────────────────────────────────────────────────

# action -> {label, destructive, needs_database, build(cfg, db, options) -> CoroFactory}
def _build_bootstrap(cfg: ReplicatorConfig, db: Optional[str], _opts: dict) -> CoroFactory:
    from pg_emigrant.bootstrap import bootstrap

    async def _coro() -> Any:
        await bootstrap(cfg, database=db)
        return {"message": f"Bootstrap finished for {db or 'all databases'}"}

    return _coro


def _build_teardown(cfg: ReplicatorConfig, db: str, _opts: dict) -> CoroFactory:
    from pg_emigrant.replication import drop_publication, drop_subscription
    from pg_emigrant.utils import get_logger

    log = get_logger("pg_emigrant.web")

    async def _coro() -> Any:
        await drop_subscription(cfg, db)
        await drop_publication(cfg, db)
        log.info("Torn down replication for %s", db)
        return {"message": f"Replication torn down for {db}"}

    return _coro


def _build_start(cfg: ReplicatorConfig, db: str, _opts: dict) -> CoroFactory:
    from pg_emigrant.replication import enable_subscription
    from pg_emigrant.utils import get_logger

    log = get_logger("pg_emigrant.web")

    async def _coro() -> Any:
        await enable_subscription(cfg, db)
        log.info("Enabled replication for %s", db)
        return {"message": f"Replication enabled for {db}"}

    return _coro


def _build_stop(cfg: ReplicatorConfig, db: str, _opts: dict) -> CoroFactory:
    from pg_emigrant.replication import disable_subscription
    from pg_emigrant.utils import get_logger

    log = get_logger("pg_emigrant.web")

    async def _coro() -> Any:
        await disable_subscription(cfg, db)
        log.info("Disabled replication for %s", db)
        return {"message": f"Replication disabled for {db}"}

    return _coro


def _build_sync_sequences(cfg: ReplicatorConfig, db: str, _opts: dict) -> CoroFactory:
    from pg_emigrant.sequence_sync import sync_sequences_once
    from pg_emigrant.utils import get_logger

    log = get_logger("pg_emigrant.web")

    async def _coro() -> Any:
        report = await sync_sequences_once(cfg, db)
        updated = sum(1 for r in report if r["status"] == "updated")
        log.info("Sequence sync for %s: %d sequence(s), %d updated", db, len(report), updated)
        return {"message": f"Synced {len(report)} sequence(s) for {db} ({updated} updated)"}

    return _coro


def _build_reinit_sync(cfg: ReplicatorConfig, db: str, _opts: dict) -> CoroFactory:
    from pg_emigrant.replication import reinit_sync
    from pg_emigrant.utils import get_logger

    log = get_logger("pg_emigrant.web")

    async def _coro() -> Any:
        result = await reinit_sync(cfg, db)
        for issue in result.get("issues_found", []):
            log.warning("⚠ %s", issue)
        for action in result.get("actions_taken", []):
            log.info("✓ %s", action)
        if result.get("was_healthy"):
            log.info("Replication for %s is healthy — nothing to do", db)
        return {
            "message": "healthy" if result.get("was_healthy") else "repaired",
            "issues_found": result.get("issues_found", []),
            "actions_taken": result.get("actions_taken", []),
        }

    return _coro


def _build_detect_ddl_apply(cfg: ReplicatorConfig, db: str, opts: dict) -> CoroFactory:
    from pg_emigrant.ddl_detector import apply_drift_fixes, detect_drift
    from pg_emigrant.utils import get_logger

    log = get_logger("pg_emigrant.web")
    drop_extra = bool(opts.get("drop_extra"))

    async def _coro() -> Any:
        report = await detect_drift(cfg, db)
        if not report.has_drift:
            log.info("No drift detected for %s — nothing to apply", db)
            return {"message": f"No drift detected for {db}", "applied": 0}
        log.info("Applying drift fixes for %s (%s)", db, report.summary)
        applied = await apply_drift_fixes(cfg, db, report, drop_extra=drop_extra)
        log.info("Applied %d fix(es) for %s", applied, db)
        return {"message": f"Applied {applied} fix(es) for {db}", "applied": applied}

    return _coro


ACTIONS: dict[str, dict[str, Any]] = {
    "bootstrap": {
        "label": "Bootstrap",
        "destructive": True,
        "needs_database": False,
        "build": _build_bootstrap,
    },
    "teardown": {
        "label": "Teardown",
        "destructive": True,
        "needs_database": True,
        "build": _build_teardown,
    },
    "start": {
        "label": "Start replication",
        "destructive": False,
        "needs_database": True,
        "build": _build_start,
    },
    "stop": {
        "label": "Stop replication",
        "destructive": False,
        "needs_database": True,
        "build": _build_stop,
    },
    "sync-sequences": {
        "label": "Sync sequences",
        "destructive": False,
        "needs_database": True,
        "build": _build_sync_sequences,
    },
    "reinit-sync": {
        "label": "Reinit sync",
        "destructive": False,
        "needs_database": True,
        "build": _build_reinit_sync,
    },
    "detect-ddl-apply": {
        "label": "Apply schema drift fixes",
        "destructive": True,
        "needs_database": True,
        "build": _build_detect_ddl_apply,
    },
}


def build_action_coro(
    cfg: ReplicatorConfig,
    action: str,
    database: Optional[str],
    options: Optional[dict] = None,
) -> CoroFactory:
    """Return a no-arg coroutine factory for *action*, ready for the JobManager.

    Raises ``ValueError`` for unknown actions or a missing required database.
    """
    spec = ACTIONS.get(action)
    if spec is None:
        raise ValueError(f"Unknown action: {action!r}")
    if spec["needs_database"] and not database:
        raise ValueError(f"Action {action!r} requires a database")
    return spec["build"](cfg, database, options or {})
