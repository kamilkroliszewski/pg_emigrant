"""Read-only cutover readiness: SAFE TO CUT OVER, or DO NOT CUT OVER.

The cutover is the irreversible step.  Everything before it can be redone by
tearing down and re-bootstrapping; once the application is writing to the
target, the source and target have diverged and there is no going back to a
single source of truth without downtime and reconciliation.

So this answers one question and takes no action.  It does not stop the
application, disable the subscription, or promote anything: deciding *when* to
move traffic is an operational judgement involving load balancers, DNS,
connection pools and people, none of which this tool can see.  What it can do
is refuse to let that decision be made on an assumption.

Every check defaults to "not ready".  A check that cannot be evaluated — an
unreachable target, an unreadable catalog — counts against readiness rather
than being skipped, because the alternative is a green light based on missing
evidence.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pg_emigrant.config import ReplicatorConfig
from pg_emigrant.db import connect, discover_databases
from pg_emigrant.ddl_detector import detect_drift
from pg_emigrant.guards import UnsafeOperation, assert_distinct_clusters
from pg_emigrant.health import ReplicationState, human_bytes, replication_health
from pg_emigrant.sequence_sync import get_sequence_status
from pg_emigrant.utils import get_logger

log = get_logger(__name__)

# How close the target has to be before "caught up" is a fair description.
# Not zero: a live source is still committing, so the gap is never exactly
# nothing while the application is running.  8 MiB is well inside what a
# healthy stream clears in under a second.
DEFAULT_MAX_LAG_BYTES = 8 * 1024 * 1024


@dataclass
class Check:
    name: str
    ready: bool
    summary: str
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "ready": self.ready,
            "summary": self.summary,
            "detail": self.detail,
        }


@dataclass
class DatabaseReadiness:
    database: str
    checks: list[Check] = field(default_factory=list)

    def add(self, name: str, ready: bool, summary: str, detail: str = "") -> None:
        self.checks.append(Check(name, ready, summary, detail))

    @property
    def ready(self) -> bool:
        return all(c.ready for c in self.checks)

    @property
    def blockers(self) -> list[Check]:
        return [c for c in self.checks if not c.ready]

    def to_dict(self) -> dict[str, Any]:
        return {
            "database": self.database,
            "ready": self.ready,
            "checks": [c.to_dict() for c in self.checks],
        }


@dataclass
class CutoverReport:
    databases: list[DatabaseReadiness] = field(default_factory=list)

    @property
    def ready(self) -> bool:
        return bool(self.databases) and all(d.ready for d in self.databases)

    @property
    def summary(self) -> str:
        n_ready = sum(1 for d in self.databases if d.ready)
        verdict = "SAFE TO CUT OVER" if self.ready else "DO NOT CUT OVER"
        return f"{verdict} — {n_ready}/{len(self.databases)} database(s) ready"

    def to_dict(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "verdict": "safe_to_cutover" if self.ready else "do_not_cutover",
            "summary": self.summary,
            "databases": [d.to_dict() for d in self.databases],
        }


async def check_cutover_readiness(
    cfg: ReplicatorConfig,
    database: str | None = None,
    *,
    max_lag_bytes: int = DEFAULT_MAX_LAG_BYTES,
    accept_drift: bool = False,
) -> CutoverReport:
    """Assess every database, or one, without changing anything."""
    report = CutoverReport()

    # Cluster identity first: if this passes on the wrong pair of endpoints,
    # every check below is measuring the wrong thing.
    identity_error: str | None = None
    try:
        await assert_distinct_clusters(cfg)
    except UnsafeOperation as exc:
        identity_error = str(exc)
    except Exception as exc:
        identity_error = f"could not verify cluster identity: {exc}"

    databases = [database] if database else await discover_databases(cfg)
    for dbname in databases:
        readiness = DatabaseReadiness(database=dbname)
        report.databases.append(readiness)

        if identity_error:
            readiness.add(
                "cluster_identity", False,
                "source and target could not be confirmed as distinct clusters",
                identity_error,
            )
        else:
            readiness.add("cluster_identity", True,
                          "source and target are independent clusters")

        await _check_target_writable(cfg, dbname, readiness)
        health = await _check_replication(cfg, dbname, readiness, max_lag_bytes)
        _check_tables_streaming(health, readiness)
        await _check_sequences(cfg, dbname, readiness)
        await _check_drift(cfg, dbname, readiness, accept_drift)
        _check_wal_retention(health, readiness)

    return report


async def _check_target_writable(cfg, dbname, readiness) -> None:
    """The target must exist, be reachable, and not be a read-only standby."""
    try:
        async with connect(cfg.target, dbname) as tgt:
            in_recovery = await tgt.fetchval("SELECT pg_is_in_recovery()")
            if in_recovery:
                readiness.add(
                    "target_writable", False,
                    "the target is a STANDBY and cannot accept writes",
                    "Point the application (and 'target' in the config) at the "
                    "target cluster's primary.",
                )
                return
            readiness.add("target_writable", True, "target is reachable and writable")
    except Exception as exc:
        readiness.add(
            "target_writable", False,
            f"the target database {dbname!r} could not be reached",
            f"{exc}. Nothing else about this database could be verified.",
        )


async def _check_replication(cfg, dbname, readiness, max_lag_bytes):
    try:
        health = await replication_health(cfg, dbname)
    except Exception as exc:
        readiness.add(
            "replication_healthy", False,
            "replication health could not be determined",
            f"{exc}. An unverifiable state counts against readiness.",
        )
        return None

    if health.state is ReplicationState.HEALTHY:
        readiness.add(
            "replication_healthy", True,
            f"replication is healthy (lag {human_bytes(health.lag_bytes)})",
        )
    else:
        readiness.add(
            "replication_healthy", False,
            f"replication is {health.state.value.upper()}",
            "; ".join(health.reasons),
        )

    # Lag is checked separately and more strictly than health: a stream that
    # is merely "not lagging" by monitoring standards can still be far enough
    # behind that cutting over loses recent writes.
    if health.lag_bytes is None:
        readiness.add(
            "replication_caught_up", False,
            "replication lag could not be measured",
            "Without a lag figure there is no evidence the target is current.",
        )
    elif health.lag_bytes <= max_lag_bytes:
        readiness.add(
            "replication_caught_up", True,
            f"target is caught up (lag {human_bytes(health.lag_bytes)} "
            f"≤ {human_bytes(max_lag_bytes)})",
        )
    else:
        readiness.add(
            "replication_caught_up", False,
            f"target is {human_bytes(health.lag_bytes)} behind",
            f"More than the {human_bytes(max_lag_bytes)} threshold. Stop writes "
            f"to the source and wait for the lag to fall before cutting over — "
            f"anything still in flight is lost the moment the application "
            f"starts writing to the target instead.",
        )
    return health


def _check_tables_streaming(health, readiness) -> None:
    """Every published table must actually be streaming, not merely published.

    Lag is a property of the subscription as a whole; being *in* the stream is
    a property of each table separately.  A table whose initial sync never
    completed is not in the stream at all — the apply worker skips it — so its
    rows are missing on the target while the slot, the apply worker and the lag
    figure all read as perfectly healthy.  That combination is the one shape of
    silent data loss a lag-based check cannot see, which is why it is a check
    of its own and an outright blocker rather than a note: cutting over now
    means switching the application onto a table that has no data.
    """
    if health is None or not health.subscription_exists:
        readiness.add(
            "all_tables_streaming", False,
            "per-table replication state could not be established",
            "Without it there is no evidence that every table is actually being "
            "replicated, only that a subscription exists.",
        )
        return
    if health.tables_not_replicated:
        readiness.add(
            "all_tables_streaming", False,
            f"{len(health.tables_not_replicated)} table(s) on the source are "
            f"not part of this subscription at all",
            f"{human_list(health.tables_not_replicated)}. Nothing is "
            f"replicating them: they are in no publication, or in one this "
            f"subscription has never refreshed into. Their copy on the target "
            f"is whatever happens to be there — most likely empty — and the "
            f"drift scan will not say so, because the table does exist on both "
            f"sides. Run 'pg_emigrant sync-sequences' to bring them in, or add "
            f"them to exclude_tables if they are meant to be left behind.",
        )
        return
    if health.tables_not_ready:
        readiness.add(
            "all_tables_streaming", False,
            f"{len(health.tables_not_ready)} of {health.tables_total} published "
            f"table(s) are NOT streaming",
            f"{human_list(health.tables_not_ready)}. A table is only replicated "
            f"once its state reaches 'r' (ready); until then its rows are not on "
            f"the target and the lag figure says nothing about it. Check the "
            f"TARGET's log for the tablesync worker's error "
            f"(pg_stat_subscription_stats.sync_error_count = "
            f"{health.sync_error_count}), fix the cause, and re-check.",
        )
        return
    readiness.add(
        "all_tables_streaming", True,
        f"all {health.tables_total} published table(s) are streaming",
    )


def human_list(items: list[str], limit: int = 10) -> str:
    shown = ", ".join(items[:limit])
    return shown + (f" (+{len(items) - limit} more)" if len(items) > limit else "")


async def _check_sequences(cfg, dbname, readiness) -> None:
    try:
        status = await get_sequence_status(cfg, dbname)
    except Exception as exc:
        readiness.add("sequences_synchronised", False,
                      "sequence state could not be read", str(exc))
        return

    behind = [r for r in status if r["status"] in ("behind", "missing_on_target",
                                                   "permission_denied")]
    if behind:
        names = ", ".join(f"{r['schema']}.{r['sequence']} ({r['status']})"
                          for r in behind[:10])
        more = f" (+{len(behind) - 10} more)" if len(behind) > 10 else ""
        readiness.add(
            "sequences_synchronised", False,
            f"{len(behind)} sequence(s) are not caught up on the target",
            f"{names}{more}. The first insert after cutover would hand out a "
            f"value that already exists. Run 'pg_emigrant sync-sequences "
            f"--margin 1000' after stopping writes to the source.",
        )
    else:
        readiness.add("sequences_synchronised", True,
                      f"all {len(status)} sequence(s) are at or ahead of the source")


async def _check_drift(cfg, dbname, readiness, accept_drift: bool) -> None:
    try:
        drift = await detect_drift(cfg, dbname)
    except Exception as exc:
        readiness.add("no_schema_drift", False,
                      "schema drift could not be checked", str(exc))
        return

    if not drift.has_drift:
        readiness.add("no_schema_drift", True, "no schema drift")
    elif accept_drift:
        readiness.add(
            "no_schema_drift", True,
            f"schema drift present but explicitly accepted ({drift.summary})",
            "Re-run without --accept-drift to make this a blocker again.",
        )
    else:
        readiness.add(
            "no_schema_drift", False,
            f"schema drift between source and target ({drift.summary})",
            f"Run 'pg_emigrant detect-ddl --database {dbname}' for the itemised "
            f"report and '--apply' to fix it. If the difference is deliberate, "
            f"re-run this check with --accept-drift.",
        )


def _check_wal_retention(health, readiness) -> None:
    """Retention is a source-side risk, not a target-side one.

    It does not block a cutover by itself — a slot holding a lot of WAL is
    still delivering it — but a slot on the edge of losing its WAL is one
    interruption away from turning this migration into a re-copy, and that is
    worth surfacing at exactly the moment someone is deciding to proceed.
    """
    if health is None:
        # The health probe itself failed, so retention is unknown.  Recorded as
        # a blocker rather than omitted: a caller reading the checks list — the
        # JSON output exists for exactly that — must not find this check simply
        # absent and take silence for a pass.
        readiness.add(
            "wal_retention", False,
            "WAL retention could not be measured",
            "The replication health probe failed, so how much WAL the slot is "
            "holding on the source is unknown.",
        )
        return
    if health.slot_wal_status in ("unreserved", "lost"):
        readiness.add(
            "wal_retention", False,
            f"the replication slot's WAL is {health.slot_wal_status}",
            "Beyond max_slot_wal_keep_size on the source: the WAL the target "
            "still needs is being (or has been) discarded. Cutting over now "
            "risks a target that is permanently missing recent writes.",
        )
        return
    retained = health.retained_wal_bytes
    limit = health.max_slot_wal_keep_size_bytes
    if retained is not None and limit and retained > limit * 0.8:
        readiness.add(
            "wal_retention", False,
            f"the slot is retaining {human_bytes(retained)} of WAL, close to the "
            f"{human_bytes(limit)} max_slot_wal_keep_size limit",
            "Past that limit PostgreSQL discards the WAL and the slot becomes "
            "unusable, making the gap permanent. Let the target catch up, or "
            "raise max_slot_wal_keep_size on the source. Do NOT drop the slot "
            "to reclaim the space — that is the data loss, not the fix.",
        )
        return
    readiness.add(
        "wal_retention", True,
        f"the slot is retaining {human_bytes(retained)} of WAL on the source",
    )
