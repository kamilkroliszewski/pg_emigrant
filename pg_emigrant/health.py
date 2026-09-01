"""Replication health, WAL retention, and whether it is safe to cut over.

Three questions this tool could not previously answer, all of which an
operator has to answer before switching an application over:

**Is replication actually healthy?**  "A subscription row exists" is not the
same thing, and it is what the status display used to imply.  A subscription
can exist while its apply worker is dead, while its slot has been dropped, or
while it is thirty gigabytes behind.  Health here is a state with defined
boundaries — HEALTHY / LAGGING / CRITICAL / BROKEN — computed from the slot
and the apply worker together.

**How much WAL is the slot holding?**  A logical slot is the only thing
keeping WAL the target has not replayed, and it holds it on the *production
source*.  Retention is the failure mode that shows up as a full disk on the
primary days after everyone stopped watching, so it is measured explicitly
rather than left to be inferred from a lag figure.  Nothing here drops a slot
to relieve retention: that trades a disk-space problem for permanent,
unrecoverable data loss, and it is the operator's call, not the tool's.

**Is it safe to cut over?**  Answered read-only, and answered conservatively:
anything unproven counts against readiness.

**Is every table actually streaming?**  A subscription is not one stream but
one per-table state machine plus a shared apply worker, and the two fail
independently.  A table whose initial sync (``tablesync``) never completes is
never handed over to the apply worker: no change for it is ever applied, its
rows simply are not on the target — and the slot, the apply worker and the lag
figure all stay perfectly healthy, because nothing about them is wrong.  So
``pg_subscription_rel.srsubstate`` is read alongside them; a table that is not
``r`` (ready) is a table the target does not have.

The same question has a second, earlier answer.  A table that exists on both
servers but is in **no publication** is replicated by nothing at all, and is in
an error state on neither server — the drift scan sees it on both sides and
reports nothing, precisely because it *is* on both sides.  So the migration's
in-scope source tables are compared against what the subscription actually
tracks, and anything missing from that comparison is named.

A note on measurement.  ``pg_stat_subscription.latest_end_lsn`` looks like the
obvious lag signal and is not one — it is the position the *sender* last
reported, which keepalives advance regardless of what the apply worker has
done, so a lag computed from it reads as zero on a subscription that is
badly behind.  The authoritative signal is the slot's ``confirmed_flush_lsn``
on the source: it moves only on the subscriber's own confirmation.  Likewise
``pg_current_wal_lsn()`` is the write pointer and can sit behind a committed
transaction under ``synchronous_commit = off``; ``pg_current_wal_insert_lsn()``
is the one to measure against.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from pg_emigrant.config import ReplicatorConfig
from pg_emigrant.db import connect, discover_schemas
from pg_emigrant.replication import pub_name, publishable_tables, sub_name
from pg_emigrant.utils import get_logger

log = get_logger(__name__)


class ReplicationState(str, Enum):
    """How replication for one database is doing, worst-first when aggregated."""

    HEALTHY = "healthy"
    LAGGING = "lagging"
    CRITICAL = "critical"
    BROKEN = "broken"
    ABSENT = "absent"     # never set up — not a failure, just nothing to report

    @property
    def is_ok(self) -> bool:
        return self is ReplicationState.HEALTHY


_RANK = {
    ReplicationState.HEALTHY: 0,
    ReplicationState.ABSENT: 1,
    ReplicationState.LAGGING: 2,
    ReplicationState.CRITICAL: 3,
    ReplicationState.BROKEN: 4,
}

# Defaults chosen to be useful rather than precise: 64 MiB of unreplayed WAL is
# more than a healthy stream keeps up with but small enough to be caught up in
# seconds, and a gigabyte means something is genuinely wrong.  Both are
# configurable, because "a lot of WAL" depends entirely on the write rate of
# the system being migrated.
DEFAULT_LAG_WARN_BYTES = 64 * 1024 * 1024
DEFAULT_LAG_CRITICAL_BYTES = 1024 * 1024 * 1024


@dataclass
class ReplicationHealth:
    """Everything measurable about one database's replication."""

    database: str
    state: ReplicationState = ReplicationState.ABSENT
    reasons: list[str] = field(default_factory=list)

    publication_exists: bool = False
    slot_exists: bool = False
    slot_active: bool = False
    slot_wal_status: str | None = None
    subscription_exists: bool = False
    subscription_enabled: bool = False
    apply_worker_running: bool = False
    apply_error_count: int | None = None
    sync_error_count: int | None = None

    # Per-table replication state, from pg_subscription_rel.  A table that is
    # not 'r' (ready) is not being applied by the apply worker: it is either
    # still in its initial sync or stuck in one, and either way the target
    # does not have its rows.
    tables_total: int = 0
    tables_not_ready: list[str] = field(default_factory=list)
    # In-scope tables on the SOURCE that the subscription does not know about
    # at all — not published, or published and never refreshed into.  Distinct
    # from tables_not_ready, which is about tables it does know about.
    tables_not_replicated: list[str] = field(default_factory=list)

    # Bytes of WAL the source still has to send before the target is current.
    lag_bytes: int | None = None
    # Bytes of WAL the slot is forcing the source to retain on disk.
    retained_wal_bytes: int | None = None
    # Time since the target last confirmed anything, in seconds.
    seconds_since_last_confirmation: float | None = None

    received_lsn: str | None = None
    confirmed_flush_lsn: str | None = None
    source_lsn: str | None = None

    max_slot_wal_keep_size_bytes: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "database": self.database,
            "state": self.state.value,
            "reasons": self.reasons,
            "publication_exists": self.publication_exists,
            "slot_exists": self.slot_exists,
            "slot_active": self.slot_active,
            "slot_wal_status": self.slot_wal_status,
            "subscription_exists": self.subscription_exists,
            "subscription_enabled": self.subscription_enabled,
            "apply_worker_running": self.apply_worker_running,
            "apply_error_count": self.apply_error_count,
            "sync_error_count": self.sync_error_count,
            "tables_total": self.tables_total,
            "tables_not_ready": self.tables_not_ready,
            "tables_not_replicated": self.tables_not_replicated,
            "lag_bytes": self.lag_bytes,
            "retained_wal_bytes": self.retained_wal_bytes,
            "seconds_since_last_confirmation": self.seconds_since_last_confirmation,
            "received_lsn": self.received_lsn,
            "confirmed_flush_lsn": self.confirmed_flush_lsn,
            "source_lsn": self.source_lsn,
            "max_slot_wal_keep_size_bytes": self.max_slot_wal_keep_size_bytes,
        }


async def replication_health(
    cfg: ReplicatorConfig,
    dbname: str,
    *,
    lag_warn_bytes: int | None = None,
    lag_critical_bytes: int | None = None,
) -> ReplicationHealth:
    """Measure, then classify.  Read-only on both clusters."""
    warn = lag_warn_bytes if lag_warn_bytes is not None else DEFAULT_LAG_WARN_BYTES
    critical = (
        lag_critical_bytes if lag_critical_bytes is not None
        else DEFAULT_LAG_CRITICAL_BYTES
    )

    h = ReplicationHealth(database=dbname)
    slot = sub_name(cfg, dbname)

    async with connect(cfg.source, dbname) as src:
        h.publication_exists = bool(await src.fetchval(
            "SELECT 1 FROM pg_publication WHERE pubname = $1", pub_name(cfg, dbname)
        ))
        row = await src.fetchrow(
            """
            SELECT s.active,
                   s.wal_status,
                   s.restart_lsn::text        AS restart_lsn,
                   s.confirmed_flush_lsn::text AS confirmed_flush_lsn,
                   pg_current_wal_insert_lsn()::text AS source_lsn,
                   pg_wal_lsn_diff(pg_current_wal_insert_lsn(),
                                   s.confirmed_flush_lsn)::bigint AS lag_bytes,
                   pg_wal_lsn_diff(pg_current_wal_insert_lsn(),
                                   s.restart_lsn)::bigint AS retained_bytes
            FROM pg_replication_slots s
            WHERE s.slot_name = $1
            """,
            slot,
        )
        h.max_slot_wal_keep_size_bytes = await _max_slot_wal_keep_size(src)
        # The tables this migration is supposed to be carrying, from the same
        # scope resolution every other stage uses.  Partition-parent shaped, to
        # match the roll-up applied to the target's tracked set below.
        in_scope = await publishable_tables(
            src, await discover_schemas(src, cfg), cfg.exclude_tables
        )

    if row is not None:
        h.slot_exists = True
        h.slot_active = bool(row["active"])
        h.slot_wal_status = row["wal_status"]
        h.confirmed_flush_lsn = row["confirmed_flush_lsn"]
        h.source_lsn = row["source_lsn"]
        h.lag_bytes = int(row["lag_bytes"]) if row["lag_bytes"] is not None else None
        h.retained_wal_bytes = (
            int(row["retained_bytes"]) if row["retained_bytes"] is not None else None
        )

    async with connect(cfg.target, dbname) as tgt:
        sub_row = await tgt.fetchrow(
            "SELECT subenabled FROM pg_subscription WHERE subname = $1"
            " AND subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())",
            slot,
        )
        if sub_row is not None:
            h.subscription_exists = True
            h.subscription_enabled = bool(sub_row["subenabled"])
            worker = await tgt.fetchrow(
                "SELECT pid, received_lsn::text AS received_lsn,"
                "       EXTRACT(EPOCH FROM (now() - last_msg_receipt_time)) AS since_msg"
                " FROM pg_stat_subscription WHERE subname = $1 AND relid IS NULL",
                slot,
            )
            if worker is not None:
                h.apply_worker_running = worker["pid"] is not None
                h.received_lsn = worker["received_lsn"]
                if worker["since_msg"] is not None:
                    h.seconds_since_last_confirmation = float(worker["since_msg"])
            if tgt.get_server_version().major >= 15:
                stats = await tgt.fetchrow(
                    "SELECT apply_error_count, sync_error_count"
                    " FROM pg_stat_subscription_stats WHERE subname = $1",
                    slot,
                )
                if stats is not None:
                    h.apply_error_count = stats["apply_error_count"]
                    h.sync_error_count = stats["sync_error_count"]
            # Per-table state.  ``srsubstate`` is a "char" column, cast to text
            # so every driver/version returns the same thing.  Only 'r' (ready)
            # means the main apply worker is streaming that table; 'i'/'d'/'f'/
            # 's' all mean its initial sync has not been handed over yet, and
            # the target does not have its rows.
            rel_rows = await tgt.fetch(
                """
                SELECT n.nspname, c.relname, sr.srsubstate::text AS srsubstate
                FROM pg_subscription_rel sr
                JOIN pg_subscription s ON s.oid = sr.srsubid
                JOIN pg_class c ON c.oid = sr.srrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE s.subname = $1
                  AND s.subdbid = (SELECT oid FROM pg_database
                                   WHERE datname = current_database())
                ORDER BY 1, 2
                """,
                slot,
            )
            h.tables_total = len(rel_rows)
            h.tables_not_ready = [
                f"{r['nspname']}.{r['relname']} (state={r['srsubstate']})"
                for r in rel_rows
                if r["srsubstate"] != "r"
            ]
            # Which in-scope tables the subscription knows about *at all*.
            # Rolled up to the partition root, because a published partitioned
            # parent is tracked here as its leaves and would otherwise read as
            # missing.  A table absent from this set is not being replicated by
            # any mechanism: it is in no publication, or in one the
            # subscription has never refreshed into.  That is the shape a
            # target table created by hand — or by a repair that forgot to
            # publish it — takes, and it is invisible to every other signal:
            # the drift scan sees the table on both sides and reports nothing,
            # while the target's copy stays permanently empty.
            root_rows = await tgt.fetch(
                """
                SELECT DISTINCT n.nspname, c.relname
                FROM pg_subscription_rel sr
                JOIN pg_subscription s ON s.oid = sr.srsubid
                JOIN pg_class c
                  ON c.oid = COALESCE(pg_partition_root(sr.srrelid), sr.srrelid)
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE s.subname = $1
                  AND s.subdbid = (SELECT oid FROM pg_database
                                   WHERE datname = current_database())
                """,
                slot,
            )
            tracked_roots = {(r["nspname"], r["relname"]) for r in root_rows}
            h.tables_not_replicated = [
                f"{schema}.{table}"
                for schema, table in sorted(in_scope - tracked_roots)
            ]

    _classify(h, warn=warn, critical=critical)
    return h


def _classify(h: ReplicationHealth, *, warn: int, critical: int) -> None:
    """Turn the measurements into a state, worst condition wins."""
    if not h.subscription_exists and not h.slot_exists and not h.publication_exists:
        h.state = ReplicationState.ABSENT
        h.reasons.append("replication has not been set up for this database")
        return

    broken: list[str] = []
    if not h.slot_exists:
        broken.append(
            "the replication slot is GONE — every transaction committed since it "
            "was lost can no longer reach the target, and no amount of streaming "
            "will bring it back"
        )
    elif h.slot_wal_status == "lost":
        broken.append(
            "the slot's WAL has been recycled (wal_status='lost') — the "
            "un-replayed transactions it held are unrecoverable"
        )
    if not h.subscription_exists:
        broken.append("no subscription exists on the target")
    elif not h.subscription_enabled:
        broken.append("the subscription is DISABLED, so nothing is being applied")
    elif not h.apply_worker_running:
        broken.append(
            "the subscription is enabled but its apply worker is not running — "
            "check the TARGET's log; the usual cause is a connection string that "
            "does not reach the source from the target machine"
        )
    if not h.publication_exists:
        broken.append("the publication is missing on the source")

    # A stuck initial table sync is the failure that looks exactly like health:
    # the slot is fine, the apply worker is fine, the lag is zero — and one
    # table's rows are simply not on the target and never will be.  Errors
    # recorded against the sync workers are what separate "still copying" from
    # "will never finish"; the plain in-progress case is handled as LAGGING
    # further down, because a table mid-sync genuinely IS behind.
    if h.tables_not_ready and h.sync_error_count:
        broken.append(
            f"the initial table sync is FAILING on the target "
            f"({h.sync_error_count} sync error(s) recorded) and "
            f"{len(h.tables_not_ready)} of {h.tables_total} published table(s) "
            f"are still not streaming: {_names(h.tables_not_ready)}. Those "
            f"tables' rows are NOT on the target and no amount of waiting will "
            f"put them there — check the TARGET's log for the tablesync worker's "
            f"error, fix the cause, and the sync retries on its own"
        )

    if broken:
        h.state = ReplicationState.BROKEN
        h.reasons.extend(broken)
        return

    if h.slot_wal_status == "unreserved":
        h.state = ReplicationState.CRITICAL
        h.reasons.append(
            "the slot's WAL is beyond max_slot_wal_keep_size (wal_status="
            "'unreserved') — it is about to be discarded, which would make the "
            "gap permanent. Catch the target up now, or raise "
            "max_slot_wal_keep_size on the source"
        )
        return

    if h.lag_bytes is not None and h.lag_bytes >= critical:
        h.state = ReplicationState.CRITICAL
        h.reasons.append(
            f"{_human(h.lag_bytes)} of WAL is not yet applied on the target "
            f"(critical threshold {_human(critical)})"
        )
        return
    if h.lag_bytes is not None and h.lag_bytes >= warn:
        h.state = ReplicationState.LAGGING
        h.reasons.append(
            f"{_human(h.lag_bytes)} of WAL is not yet applied on the target "
            f"(warning threshold {_human(warn)})"
        )
        return
    # A table the migration is supposed to carry that the subscription has
    # never heard of.  Not an error state on the server anywhere — which is the
    # problem: nothing else reports it.  LAGGING rather than BROKEN because the
    # steady-state 'sync-sequences --loop' process is expected to close exactly
    # this gap on its next tick, and a state that flipped to BROKEN for the
    # seconds between a CREATE TABLE and that tick would be noise.  It is never
    # HEALTHY, and cutover-check blocks on it outright.
    if h.tables_not_replicated:
        h.state = ReplicationState.LAGGING
        h.reasons.append(
            f"{len(h.tables_not_replicated)} table(s) in scope on the source are "
            f"not part of this subscription at all, so nothing is replicating "
            f"them: {_names(h.tables_not_replicated)}. Run 'pg_emigrant "
            f"sync-sequences' (or keep '--loop' running) to bring them in; if "
            f"they are meant to be left behind, add them to exclude_tables"
        )
        return

    # No recorded sync errors, so this is (or looks like) an initial sync still
    # in progress — normal for a few seconds after a table is picked up, and
    # not health: until it reaches 'r' the target does not have those rows.
    if h.tables_not_ready:
        h.state = ReplicationState.LAGGING
        h.reasons.append(
            f"{len(h.tables_not_ready)} of {h.tables_total} published table(s) "
            f"are not streaming yet — their initial sync is still in progress, "
            f"so the target does not have their rows: "
            f"{_names(h.tables_not_ready)}. If this does not clear, check the "
            f"TARGET's log for the tablesync worker"
        )
        return

    if not h.slot_active:
        h.state = ReplicationState.LAGGING
        h.reasons.append(
            "the slot has no walsender attached right now — briefly normal "
            "during a reconnect, a problem if it persists"
        )
        return

    if h.lag_bytes is None:
        # Every check above passed, but the one number that says whether the
        # target is current could not be read — the slot has no
        # confirmed_flush_lsn, so the subscriber has never confirmed anything.
        # Reporting HEALTHY here would be a green light resting on a missing
        # measurement, which is the one thing this module exists not to do.
        h.state = ReplicationState.LAGGING
        h.reasons.append(
            "the replication lag could not be measured: the slot has no "
            "confirmed_flush_lsn, so there is no evidence the target has "
            "applied anything at all"
        )
        return

    h.state = ReplicationState.HEALTHY


def _names(items: list[str], limit: int = 5) -> str:
    """Name the first few offenders, and say how many were left out."""
    shown = ", ".join(items[:limit])
    return shown + (f" (+{len(items) - limit} more)" if len(items) > limit else "")


async def _max_slot_wal_keep_size(conn) -> int | None:
    """``max_slot_wal_keep_size`` in bytes, or None where it does not apply.

    The setting is PostgreSQL 13+; -1 means unlimited, which is reported as
    None because "no limit" is not a number of bytes.
    """
    try:
        row = await conn.fetchrow(
            "SELECT setting::bigint AS setting, unit FROM pg_settings"
            " WHERE name = 'max_slot_wal_keep_size'"
        )
    except Exception:
        return None
    if row is None or row["setting"] < 0:
        return None
    multiplier = {"B": 1, "kB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3}.get(
        row["unit"] or "B", 1
    )
    return int(row["setting"]) * multiplier


def _human(n: int | None) -> str:
    if n is None:
        return "unknown"
    step = 1024.0
    value = float(n)
    for unit in ("B", "kB", "MB", "GB", "TB"):
        if abs(value) < step or unit == "TB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= step
    return f"{value:.1f} TB"


human_bytes = _human
