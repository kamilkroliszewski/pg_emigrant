"""Read-only production preflight: verify a migration will work BEFORE running it.

``bootstrap`` is the point of no return — it creates a replication slot on the
production source, copies data, and creates a subscription.  Most of the ways
it can fail are knowable in advance from the catalogs alone: a missing
extension on the target, a role that doesn't exist there, an exhausted
``max_replication_slots``, ``wal_level`` that isn't ``logical``, a name
collision with an existing slot, a source that is actually a standby, or
source/target pointing at the *same* cluster.

This module answers all of those questions **without writing anything**.
Every statement it issues is a ``SELECT`` against ``pg_catalog`` — no DDL, no
``CREATE``, no ``ALTER``, no slot creation, no temp objects.  It is safe to run
against production at any time, including while a migration is already
running.

The output is a list of :class:`CheckResult` records with a severity, so the
same report can drive the CLI's coloured table, the ``--format json`` output
for CI pipelines, and (eventually) a GUI panel.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import asyncpg

from pg_emigrant.config import ReplicatorConfig
from pg_emigrant.db import _SYSTEM_SCHEMAS, connect, discover_databases, discover_schemas
from pg_emigrant.replication import _UNSTABLE_HOSTS, pub_name, sub_name
from pg_emigrant.schema_sync import get_columns, get_tables
from pg_emigrant.utils import get_logger, qt

log = get_logger(__name__)

# Severity ordering, worst first — drives sorting and the exit code.
ERROR = "error"
WARN = "warn"
OK = "ok"
SKIP = "skip"

_SEVERITY_RANK = {ERROR: 0, WARN: 1, SKIP: 2, OK: 3}


@dataclass
class CheckResult:
    """One preflight check outcome.

    *status* is one of ``error`` (migration will fail or corrupt), ``warn``
    (works, but needs a human decision), ``ok``, or ``skip`` (could not be
    evaluated — e.g. insufficient privileges to read a catalog).
    """

    name: str
    category: str
    status: str
    summary: str
    detail: str = ""
    database: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "category": self.category,
            "status": self.status,
            "summary": self.summary,
            "detail": self.detail,
            "database": self.database,
        }


@dataclass
class PreflightReport:
    """Aggregate result of a preflight run."""

    checks: list[CheckResult] = field(default_factory=list)

    def add(self, *args, **kwargs) -> CheckResult:
        result = CheckResult(*args, **kwargs)
        self.checks.append(result)
        return result

    @property
    def errors(self) -> list[CheckResult]:
        return [c for c in self.checks if c.status == ERROR]

    @property
    def warnings(self) -> list[CheckResult]:
        return [c for c in self.checks if c.status == WARN]

    @property
    def skipped(self) -> list[CheckResult]:
        return [c for c in self.checks if c.status == SKIP]

    @property
    def passed(self) -> bool:
        """True when nothing would block a migration (warnings are allowed)."""
        return not self.errors

    @property
    def summary(self) -> str:
        n_err, n_warn = len(self.errors), len(self.warnings)
        n_skip, n_ok = len(self.skipped), len([c for c in self.checks if c.status == OK])
        return (
            f"{len(self.checks)} checks: {n_ok} ok, {n_warn} warning(s), "
            f"{n_err} error(s), {n_skip} skipped"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "summary": self.summary,
            "checks": [c.to_dict() for c in self.checks],
        }


# ──────────────────────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────────────────────

async def _scalar(conn: asyncpg.Connection, sql: str, *args) -> Any:
    """fetchval that returns None instead of raising, for optional catalogs."""
    try:
        return await conn.fetchval(sql, *args)
    except Exception as exc:  # pragma: no cover - depends on server config
        log.debug("preflight: query failed (%s): %s", sql.strip()[:60], exc)
        return None


async def _setting(conn: asyncpg.Connection, name: str) -> str | None:
    return await _scalar(conn, "SELECT current_setting($1, true)", name)


async def _int_setting(conn: asyncpg.Connection, name: str) -> int | None:
    raw = await _setting(conn, name)
    try:
        return int(raw) if raw is not None else None
    except ValueError:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Cluster-level checks
# ──────────────────────────────────────────────────────────────────────────────

async def _check_distinct_clusters(
    report: PreflightReport,
    cfg: ReplicatorConfig,
    src: asyncpg.Connection,
    tgt: asyncpg.Connection,
) -> None:
    """Source and target must not be the same cluster — nor a physical pair.

    ``system_identifier`` is stamped into the cluster at initdb time and is
    inherited by every physical replica, so a match means either literally the
    same server or that one is a streaming standby of the other.  Both are
    fatal: logical replication cannot target the cluster it reads from, and a
    physical standby is read-only.  This catches the classic copy-paste
    accident where ``target`` still points at the source's host.
    """
    src_id = await _scalar(src, "SELECT system_identifier FROM pg_control_system()")
    tgt_id = await _scalar(tgt, "SELECT system_identifier FROM pg_control_system()")

    same_endpoint = (
        cfg.source.host.strip().lower() == cfg.target.host.strip().lower()
        and int(cfg.source.port) == int(cfg.target.port)
    )
    if same_endpoint:
        report.add(
            "distinct_clusters", "cluster", ERROR,
            "source and target point at the SAME host:port",
            f"Both are {cfg.source.host}:{cfg.source.port}. Logical replication "
            f"cannot migrate a cluster into itself — fix 'target' in the config.",
        )
        return

    if src_id is None or tgt_id is None:
        report.add(
            "distinct_clusters", "cluster", SKIP,
            "could not read system_identifier (needs superuser or EXECUTE on pg_control_system)",
            "Falling back to the host:port comparison only, which passed. Grant "
            "EXECUTE ON FUNCTION pg_control_system() to the migration role for a "
            "definitive same-cluster/physical-replica check.",
        )
        return

    if src_id == tgt_id:
        report.add(
            "distinct_clusters", "cluster", ERROR,
            "source and target are the same cluster (or a physical primary/standby pair)",
            f"Both report system_identifier={src_id}. Even though the host:port "
            f"differ, one is a physical replica of the other (or the same server "
            f"reached by a second address). A logical migration needs an "
            f"independent target cluster — a physical standby is read-only and "
            f"shares the source's slots and data.",
        )
    else:
        report.add(
            "distinct_clusters", "cluster", OK,
            "source and target are independent clusters",
            f"system_identifier {src_id} ≠ {tgt_id}",
        )


async def _check_roles_topology(
    report: PreflightReport,
    src: asyncpg.Connection,
    tgt: asyncpg.Connection,
) -> None:
    """The source must be a primary and the target must be writable."""
    src_recovery = await _scalar(src, "SELECT pg_is_in_recovery()")
    tgt_recovery = await _scalar(tgt, "SELECT pg_is_in_recovery()")

    if src_recovery:
        report.add(
            "source_is_primary", "cluster", ERROR,
            "source is a STANDBY (pg_is_in_recovery() = true)",
            "pg_emigrant creates the replication slot on the source itself. "
            "Point 'source' at the cluster's current primary — on Patroni, its "
            "leader-only endpoint.",
        )
    else:
        report.add("source_is_primary", "cluster", OK, "source is a primary")

    if tgt_recovery:
        report.add(
            "target_writable", "cluster", ERROR,
            "target is a STANDBY (read-only)",
            "CREATE DATABASE / CREATE SUBSCRIPTION cannot run on a standby. "
            "Point 'target' at its cluster's primary.",
        )
    else:
        report.add("target_writable", "cluster", OK, "target is writable")


async def _check_versions(
    report: PreflightReport,
    src: asyncpg.Connection,
    tgt: asyncpg.Connection,
) -> None:
    src_major = src.get_server_version().major
    tgt_major = tgt.get_server_version().major

    if src_major < 13:
        report.add(
            "version_support", "cluster", ERROR,
            f"source is PostgreSQL {src_major} — pg_emigrant requires 13+",
            "Logical replication of a whole schema is not practical below 13.",
        )
    elif src_major > tgt_major:
        report.add(
            "version_support", "cluster", ERROR,
            f"source (PG {src_major}) is NEWER than target (PG {tgt_major})",
            "Schema captured from a newer server can use syntax and catalog "
            "features the older target cannot create. Migrate to a target that "
            "is the same major version or newer.",
        )
    else:
        status = OK if src_major >= 15 else WARN
        detail = ""
        if src_major < 15:
            detail = (
                f"Source PG {src_major} predates 'CREATE PUBLICATION ... FOR TABLES "
                f"IN SCHEMA', so the publication is a static table list. "
                f"pg_emigrant reconciles tables created later automatically, but "
                f"only while 'sync-sequences --loop' is running."
            )
        report.add(
            "version_support", "cluster", status,
            f"source PG {src_major} → target PG {tgt_major}",
            detail,
        )


async def _check_source_config(
    report: PreflightReport,
    src: asyncpg.Connection,
    n_databases: int,
) -> None:
    """wal_level and slot/walsender headroom on the source."""
    wal_level = await _setting(src, "wal_level")
    if wal_level != "logical":
        report.add(
            "wal_level", "config", ERROR,
            f"source wal_level = {wal_level!r}, must be 'logical'",
            "Set wal_level = logical in postgresql.conf and RESTART the source "
            "(it is not reloadable). Without it no logical slot can be created.",
        )
    else:
        report.add("wal_level", "config", OK, "source wal_level = logical")

    max_slots = await _int_setting(src, "max_replication_slots")
    used_slots = await _scalar(src, "SELECT count(*) FROM pg_replication_slots")
    if max_slots is not None and used_slots is not None:
        free = max_slots - used_slots
        if free < n_databases:
            report.add(
                "slot_headroom", "config", ERROR,
                f"not enough replication slots: need {n_databases}, {free} free "
                f"({used_slots}/{max_slots} in use)",
                "Raise max_replication_slots on the SOURCE and restart it, or drop "
                "unused slots. pg_emigrant needs one slot per migrated database.",
            )
        else:
            report.add(
                "slot_headroom", "config", OK,
                f"replication slots: {free} free, need {n_databases} "
                f"({used_slots}/{max_slots} in use)",
            )

    max_senders = await _int_setting(src, "max_wal_senders")
    used_senders = await _scalar(src, "SELECT count(*) FROM pg_stat_replication")
    if max_senders is not None and used_senders is not None:
        free = max_senders - used_senders
        if free < n_databases:
            report.add(
                "walsender_headroom", "config", ERROR,
                f"not enough WAL senders: need {n_databases}, {free} free "
                f"({used_senders}/{max_senders} in use)",
                "Raise max_wal_senders on the SOURCE and restart it. Each "
                "subscription holds one walsender for as long as it streams.",
            )
        else:
            report.add(
                "walsender_headroom", "config", OK,
                f"WAL senders: {free} free, need {n_databases}",
            )

    keep_size = await _setting(src, "max_slot_wal_keep_size")
    if keep_size in ("-1", None):
        report.add(
            "slot_wal_retention", "config", WARN,
            "max_slot_wal_keep_size is unlimited (-1) on the source",
            "A stalled bootstrap or a dead subscription will retain WAL forever "
            "and can fill the source's disk. Consider a generous bound (e.g. "
            "100GB) — big enough to cover the whole copy + index build, small "
            "enough to protect the disk.",
        )
    else:
        report.add(
            "slot_wal_retention", "config", OK,
            f"max_slot_wal_keep_size = {keep_size}",
        )


async def _check_target_config(
    report: PreflightReport,
    tgt: asyncpg.Connection,
    n_databases: int,
) -> None:
    """Apply-worker and origin-tracking capacity on the target."""
    max_workers = await _int_setting(tgt, "max_logical_replication_workers")
    if max_workers is not None and max_workers < n_databases:
        report.add(
            "apply_worker_capacity", "config", ERROR,
            f"max_logical_replication_workers = {max_workers}, need ≥ {n_databases}",
            "Each subscription needs its own apply worker (plus extra workers "
            "during table sync). Raise it on the TARGET and restart.",
        )
    elif max_workers is not None:
        report.add(
            "apply_worker_capacity", "config", OK,
            f"max_logical_replication_workers = {max_workers} for {n_databases} database(s)",
        )

    max_procs = await _int_setting(tgt, "max_worker_processes")
    if max_procs is not None and max_workers is not None and max_procs < max_workers + 2:
        report.add(
            "worker_process_capacity", "config", WARN,
            f"max_worker_processes = {max_procs} leaves little room above "
            f"max_logical_replication_workers = {max_workers}",
            "Logical replication workers come out of the same pool as autovacuum "
            "and parallel query workers. Keep a margin above the replication "
            "workers or subscriptions may fail to start.",
        )
    elif max_procs is not None:
        report.add("worker_process_capacity", "config", OK, f"max_worker_processes = {max_procs}")

    # The SUBSCRIBER uses max_replication_slots for replication-origin tracking.
    tgt_slots = await _int_setting(tgt, "max_replication_slots")
    if tgt_slots is not None and tgt_slots < n_databases:
        report.add(
            "origin_tracking_capacity", "config", ERROR,
            f"target max_replication_slots = {tgt_slots}, need ≥ {n_databases}",
            "On the SUBSCRIBER this setting caps replication origins, not slots — "
            "one per subscription. Raise it on the target and restart.",
        )
    elif tgt_slots is not None:
        report.add(
            "origin_tracking_capacity", "config", OK,
            f"target max_replication_slots = {tgt_slots} (replication origins)",
        )


async def _check_privileges(
    report: PreflightReport,
    cfg: ReplicatorConfig,
    src: asyncpg.Connection,
    tgt: asyncpg.Connection,
) -> None:
    """The migration role's attributes on both sides."""
    src_role = await src.fetchrow(
        "SELECT rolsuper, rolreplication, rolcreatedb FROM pg_roles WHERE rolname = current_user"
    )
    if src_role is None:
        report.add("source_privileges", "privileges", SKIP, "could not read current_user in pg_roles")
    elif src_role["rolsuper"] or src_role["rolreplication"]:
        how = "superuser" if src_role["rolsuper"] else "REPLICATION attribute"
        report.add(
            "source_privileges", "privileges", OK,
            f"source role '{cfg.source.user}' can create replication slots ({how})",
        )
    else:
        report.add(
            "source_privileges", "privileges", ERROR,
            f"source role '{cfg.source.user}' lacks REPLICATION (and is not superuser)",
            f"CREATE_REPLICATION_SLOT and the subscriber's WAL stream both require "
            f"it. Fix with:  ALTER ROLE {cfg.source.user} REPLICATION;",
        )

    tgt_major = tgt.get_server_version().major
    tgt_role = await tgt.fetchrow(
        "SELECT rolsuper, rolcreatedb FROM pg_roles WHERE rolname = current_user"
    )
    if tgt_role is None:
        report.add("target_privileges", "privileges", SKIP, "could not read current_user in pg_roles")
        return

    if tgt_role["rolsuper"]:
        report.add(
            "target_privileges", "privileges", OK,
            f"target role '{cfg.target.user}' is superuser",
        )
    else:
        can_subscribe = False
        if tgt_major >= 16:
            can_subscribe = bool(await _scalar(
                tgt, "SELECT pg_has_role(current_user, 'pg_create_subscription', 'USAGE')"
            ))
        if can_subscribe and tgt_role["rolcreatedb"]:
            report.add(
                "target_privileges", "privileges", OK,
                f"target role '{cfg.target.user}' has pg_create_subscription + CREATEDB",
            )
        else:
            missing = []
            if not tgt_role["rolcreatedb"]:
                missing.append("CREATEDB (to create the target databases)")
            if not can_subscribe:
                missing.append(
                    "the ability to CREATE SUBSCRIPTION (superuser, or membership in "
                    "pg_create_subscription on PG16+)"
                )
            report.add(
                "target_privileges", "privileges", ERROR,
                f"target role '{cfg.target.user}' is missing: {', '.join(missing)}",
                f"Simplest fix:  ALTER ROLE {cfg.target.user} SUPERUSER;",
            )


def _check_source_host(report: PreflightReport, cfg: ReplicatorConfig) -> None:
    """``source.host`` is resolved by the TARGET's apply worker, not by us.

    pg_emigrant stores ``source.host`` verbatim in the subscription's CONNECTION
    string; the target's apply worker dials it *later*, from the target machine.
    A loopback address therefore means "the target itself" at that point — which
    is fatal when the target lives on a different host, and is exactly the
    production incident this project has already hit.

    It is NOT automatically wrong, though: when the target runs on the same
    machine, loopback resolves back to the same source and everything works.
    So the severity depends on whether the target is co-located.
    """
    src_loopback = cfg.source.host.strip().lower() in _UNSTABLE_HOSTS
    tgt_loopback = cfg.target.host.strip().lower() in _UNSTABLE_HOSTS

    if not src_loopback:
        report.add(
            "source_host_reachable", "cluster", OK,
            f"source.host = {cfg.source.host} (routable from the target)",
        )
        return

    if tgt_loopback:
        # Both loopback → source, target and pg_emigrant are all on one machine.
        # The apply worker's loopback lands back on the source, so this works —
        # but it silently breaks the moment either side moves to another host.
        report.add(
            "source_host_reachable", "cluster", WARN,
            f"source.host is {cfg.source.host!r} (loopback), and so is target.host",
            "Everything is on one machine, so the target's apply worker resolves "
            "the loopback back to this same source and replication works. Be aware "
            "this breaks the instant either side moves to a different host: the "
            "CONNECTION string is stored verbatim and dialled from the TARGET, so "
            "loopback would then mean the target itself. It also pins connections "
            "to this one node regardless of its Patroni role. Fine for a local "
            "test; use routable addresses for anything real.",
        )
        return

    report.add(
        "source_host_reachable", "cluster", ERROR,
        f"source.host is {cfg.source.host!r} (loopback) but the target is remote "
        f"({cfg.target.host})",
        "pg_emigrant can reach the source at that address, but the target cannot: "
        "the CONNECTION string is stored verbatim in the subscription and dialled "
        "LATER by the target's apply worker, from the target machine — where "
        "loopback means the TARGET itself, not the source. CREATE SUBSCRIPTION "
        "still 'succeeds', then the apply worker fails forever in the background "
        "with 'could not connect to the publisher' or a missing-slot error, visible "
        "only in the target's own log. Set source.host to an address the TARGET can "
        "reach — the cluster's routable VIP / leader endpoint.",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Per-database checks
# ──────────────────────────────────────────────────────────────────────────────

async def _check_naming_collisions(
    report: PreflightReport,
    cfg: ReplicatorConfig,
    src: asyncpg.Connection,
    tgt: asyncpg.Connection,
    databases: list[str],
) -> None:
    """Publication / slot / subscription names must be free.

    Slots and subscriptions live in *cluster-wide* catalogs, so one query each
    covers every database.
    """
    slot_rows = await src.fetch("SELECT slot_name FROM pg_replication_slots")
    existing_slots = {r["slot_name"] for r in slot_rows}
    sub_rows = await tgt.fetch("SELECT subname FROM pg_subscription")
    existing_subs = {r["subname"] for r in sub_rows}

    for db in databases:
        slot = sub_name(cfg, db)
        collisions = []
        if slot in existing_slots:
            collisions.append(f"replication slot '{slot}' already exists on the source")
        if slot in existing_subs:
            collisions.append(f"subscription '{slot}' already exists on the target")

        if collisions:
            report.add(
                "naming_collision", "naming", ERROR,
                f"name already in use for '{db}'",
                "; ".join(collisions) + ". This database looks already bootstrapped "
                "— bootstrap refuses to re-run over a live subscription. Use "
                f"'pg_emigrant status --database {db}' to inspect it, or "
                f"'pg_emigrant teardown --database {db}' to start over.",
                database=db,
            )
        else:
            report.add(
                "naming_collision", "naming", OK,
                f"publication/slot/subscription names are free for '{db}'",
                database=db,
            )


async def _check_database_presence(
    report: PreflightReport,
    src: asyncpg.Connection,
    tgt: asyncpg.Connection,
    databases: list[str],
) -> list[str]:
    """Verify each database exists on the source; note whether it exists on target.

    Returns the databases that actually exist on the source, so later per-database
    checks can skip the ones that don't.
    """
    src_rows = await src.fetch("SELECT datname FROM pg_database")
    src_dbs = {r["datname"] for r in src_rows}
    tgt_rows = await tgt.fetch("SELECT datname FROM pg_database")
    tgt_dbs = {r["datname"] for r in tgt_rows}

    present: list[str] = []
    for db in databases:
        if db not in src_dbs:
            report.add(
                "database_exists", "schema", ERROR,
                f"database '{db}' does not exist on the source",
                "It is listed in the config's 'databases' but is not in pg_database "
                "on the source. Fix the config or the connection target.",
                database=db,
            )
            continue
        present.append(db)
        if db in tgt_dbs:
            report.add(
                "database_exists", "schema", OK,
                f"'{db}' exists on both sides (target database will be reused)",
                database=db,
            )
        else:
            report.add(
                "database_exists", "schema", OK,
                f"'{db}' exists on the source; bootstrap will CREATE it on the target",
                database=db,
            )
    return present


async def _check_db_local(
    cfg: ReplicatorConfig,
    dbname: str,
    tgt_available_ext: dict[str, str],
    tgt_roles: set[str],
    tgt_has_db: bool,
) -> list[CheckResult]:
    """All checks that need a connection into a specific database."""
    out: list[CheckResult] = []

    def add(name, category, status, summary, detail=""):
        out.append(CheckResult(name, category, status, summary, detail, database=dbname))

    try:
        async with connect(cfg.source, dbname) as src:
            schemas = await discover_schemas(src, cfg)

            # -- publication creatability: CREATE on the database ---------------
            can_create = await _scalar(
                src, "SELECT has_database_privilege(current_user, current_database(), 'CREATE')"
            )
            if can_create is False:
                add("publication_creatable", "privileges", ERROR,
                    f"no CREATE privilege on source database '{dbname}'",
                    f"CREATE PUBLICATION requires it. Fix with:  "
                    f"GRANT CREATE ON DATABASE {dbname} TO {cfg.source.user};")
            elif can_create:
                add("publication_creatable", "privileges", OK,
                    f"can CREATE PUBLICATION in '{dbname}'")

            # -- extensions -----------------------------------------------------
            ext_rows = await src.fetch(
                "SELECT e.extname, e.extversion, n.nspname AS schema"
                " FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace"
                " WHERE e.extname <> 'plpgsql'"
            )
            missing_ext, older_ext = [], []
            for r in ext_rows:
                name, version = r["extname"], r["extversion"]
                if name not in tgt_available_ext:
                    missing_ext.append(f"{name} {version}")
                else:
                    avail = tgt_available_ext[name]
                    if avail != version:
                        older_ext.append(f"{name}: source {version}, target offers {avail}")
            if missing_ext:
                add("extensions_available", "extensions", ERROR,
                    f"{len(missing_ext)} extension(s) not installable on the target",
                    "Not in the target's pg_available_extensions: "
                    + ", ".join(sorted(missing_ext))
                    + ". Install the matching packages on the target host — objects "
                      "depending on them cannot be created otherwise.")
            elif older_ext:
                add("extensions_available", "extensions", WARN,
                    f"{len(older_ext)} extension version mismatch(es)",
                    "; ".join(sorted(older_ext))
                    + ". Available, but at a different version — check for behaviour "
                      "or catalog differences.")
            elif ext_rows:
                add("extensions_available", "extensions", OK,
                    f"all {len(ext_rows)} extension(s) available on the target")
            else:
                add("extensions_available", "extensions", OK, "no non-default extensions in use")

            # -- object-owner roles must exist on the target --------------------
            owner_rows = await src.fetch(
                """
                SELECT DISTINCT pg_get_userbyid(c.relowner) AS role
                FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ANY($1::text[])
                UNION
                SELECT DISTINCT pg_get_userbyid(n.nspowner) FROM pg_namespace n
                WHERE n.nspname = ANY($1::text[])
                UNION
                SELECT DISTINCT pg_get_userbyid(p.proowner)
                FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = ANY($1::text[])
                """,
                schemas,
            )
            src_owners = {r["role"] for r in owner_rows if r["role"]}
            missing_roles = sorted(src_owners - tgt_roles)
            if missing_roles:
                add("roles_exist", "privileges", ERROR,
                    f"{len(missing_roles)} object-owner role(s) missing on the target",
                    "Missing: " + ", ".join(missing_roles)
                    + ". Ownership and GRANTs referencing them cannot be reproduced "
                      "(pg_emigrant does not create roles — they are cluster-wide and "
                      "may carry passwords/attributes you control). Create them on the "
                      "target first, e.g.  CREATE ROLE "
                    + missing_roles[0] + ";")
            else:
                add("roles_exist", "privileges", OK,
                    f"all {len(src_owners)} object-owner role(s) exist on the target")

            # -- readability of every table pg_emigrant will COPY ---------------
            unreadable = await src.fetch(
                """
                SELECT n.nspname, c.relname
                FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ANY($1::text[])
                  AND c.relkind IN ('r', 'p')
                  AND NOT has_table_privilege(current_user, c.oid, 'SELECT')
                ORDER BY 1, 2
                """,
                schemas,
            )
            if unreadable:
                names = ", ".join(f"{r['nspname']}.{r['relname']}" for r in unreadable[:10])
                more = f" (+{len(unreadable) - 10} more)" if len(unreadable) > 10 else ""
                add("table_readable", "privileges", ERROR,
                    f"{len(unreadable)} table(s) cannot be read by '{cfg.source.user}'",
                    f"No SELECT on: {names}{more}. The initial COPY would fail. "
                    f"Grant SELECT (or use a superuser migration role).")
            else:
                add("table_readable", "privileges", OK,
                    "all tables in scope are readable on the source")

            # -- unlogged tables are silently NOT replicated --------------------
            unlogged = await src.fetch(
                """
                SELECT n.nspname, c.relname
                FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ANY($1::text[])
                  AND c.relkind IN ('r', 'p') AND c.relpersistence = 'u'
                ORDER BY 1, 2
                """,
                schemas,
            )
            if unlogged:
                names = ", ".join(f"{r['nspname']}.{r['relname']}" for r in unlogged[:10])
                more = f" (+{len(unlogged) - 10} more)" if len(unlogged) > 10 else ""
                add("unlogged_tables", "schema", WARN,
                    f"{len(unlogged)} UNLOGGED table(s) will not replicate",
                    f"{names}{more}. UNLOGGED tables produce no WAL, so logical "
                    f"replication never carries their changes. Their initial copy "
                    f"still happens, but later writes will silently diverge.")
            else:
                add("unlogged_tables", "schema", OK, "no unlogged tables in scope")

            # -- tables with no replica identity --------------------------------
            no_identity = await src.fetch(
                """
                SELECT n.nspname, c.relname
                FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = ANY($1::text[])
                  AND c.relkind = 'r'
                  AND c.relreplident IN ('d', 'n')
                  AND NOT EXISTS (
                      SELECT 1 FROM pg_index i
                      WHERE i.indrelid = c.oid AND i.indisprimary
                  )
                ORDER BY 1, 2
                """,
                schemas,
            )
            if no_identity:
                names = ", ".join(f"{r['nspname']}.{r['relname']}" for r in no_identity[:10])
                more = f" (+{len(no_identity) - 10} more)" if len(no_identity) > 10 else ""
                add("replica_identity", "schema", WARN,
                    f"{len(no_identity)} table(s) have no PRIMARY KEY",
                    f"{names}{more}. Bootstrap will set REPLICA IDENTITY FULL on them "
                    f"— note this is a WRITE (ACCESS EXCLUSIVE DDL) on the production "
                    f"SOURCE, and it makes every UPDATE/DELETE log the full old row. "
                    f"Adding a real primary key first is cheaper and safer.")
            else:
                add("replica_identity", "schema", OK,
                    "every table has a primary key or an explicit replica identity")

            # -- column/type compatibility for tables that already exist there --
            if tgt_has_db:
                await _compare_columns(cfg, dbname, src, schemas, add)
            else:
                add("column_compatibility", "schema", OK,
                    "target database does not exist yet — schema will be created fresh")

    except Exception as exc:
        out.append(CheckResult(
            "database_reachable", "cluster", ERROR,
            f"cannot inspect source database '{dbname}': {exc}",
            "Every other check for this database was skipped.",
            database=dbname,
        ))
    return out


async def _compare_columns(cfg, dbname, src, schemas, add) -> None:
    """Compare columns/types for tables present on BOTH sides.

    Only meaningful when the target database already exists (a pre-created
    schema, or a re-run).  A type mismatch is fatal: the binary/CSV COPY and
    later the apply worker both bind by position and type.
    """
    try:
        async with connect(cfg.target, dbname) as tgt:
            src_tables = {(t["schema_name"], t["table_name"]) for t in await get_tables(src, schemas)}
            tgt_tables = {(t["schema_name"], t["table_name"]) for t in await get_tables(tgt, schemas)}
            common = sorted(src_tables & tgt_tables)
            if not common:
                add("column_compatibility", "schema", OK,
                    "no tables exist on both sides yet — nothing to compare")
                return

            problems: list[str] = []
            for schema, table in common:
                fqn = qt(schema, table)
                src_cols = {c["column_name"]: c for c in await get_columns(src, fqn)}
                tgt_cols = {c["column_name"]: c for c in await get_columns(tgt, fqn)}
                missing = sorted(set(src_cols) - set(tgt_cols))
                if missing:
                    problems.append(f"{schema}.{table}: target missing column(s) {', '.join(missing)}")
                for col in sorted(set(src_cols) & set(tgt_cols)):
                    s_type, t_type = src_cols[col]["data_type"], tgt_cols[col]["data_type"]
                    if s_type != t_type:
                        problems.append(
                            f"{schema}.{table}.{col}: source {s_type} vs target {t_type}"
                        )
            if problems:
                shown = "; ".join(problems[:10])
                more = f" (+{len(problems) - 10} more)" if len(problems) > 10 else ""
                add("column_compatibility", "schema", ERROR,
                    f"{len(problems)} column mismatch(es) between existing tables",
                    f"{shown}{more}. The initial COPY and the apply worker both bind "
                    f"by column name and type — a mismatch fails the copy or corrupts "
                    f"the target. Align the target schema (or drop the target tables "
                    f"and let bootstrap recreate them).")
            else:
                add("column_compatibility", "schema", OK,
                    f"{len(common)} table(s) exist on both sides with matching columns/types")
    except Exception as exc:
        add("column_compatibility", "schema", SKIP,
            f"could not compare columns against the target: {exc}")


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

async def run_preflight(
    cfg: ReplicatorConfig,
    database: str | None = None,
) -> PreflightReport:
    """Run every preflight check and return the aggregate report.

    Strictly read-only: only ``SELECT``s against ``pg_catalog`` are issued, so
    this is safe to run against production at any time — including while a
    migration is already in progress.
    """
    report = PreflightReport()

    # -- connectivity first: everything else depends on it ---------------------
    try:
        async with connect(cfg.source) as probe:
            await probe.fetchval("SELECT 1")
        report.add("source_reachable", "cluster", OK,
                   f"connected to source {cfg.source.host}:{cfg.source.port}")
    except Exception as exc:
        report.add("source_reachable", "cluster", ERROR,
                   f"cannot connect to the source: {exc}",
                   "Check host/port/user/password/sslmode in the config, the "
                   "source's pg_hba.conf, and network reachability.")
        return report

    try:
        async with connect(cfg.target) as probe:
            await probe.fetchval("SELECT 1")
        report.add("target_reachable", "cluster", OK,
                   f"connected to target {cfg.target.host}:{cfg.target.port}")
    except Exception as exc:
        report.add("target_reachable", "cluster", ERROR,
                   f"cannot connect to the target: {exc}",
                   "Check host/port/user/password/sslmode in the config, the "
                   "target's pg_hba.conf, and network reachability.")
        return report

    _check_source_host(report, cfg)

    async with connect(cfg.source) as src, connect(cfg.target) as tgt:
        await _check_distinct_clusters(report, cfg, src, tgt)
        await _check_roles_topology(report, src, tgt)
        await _check_versions(report, src, tgt)

        databases = [database] if database else await discover_databases(cfg)
        if not databases:
            report.add("databases_discovered", "schema", ERROR,
                       "no databases to migrate",
                       "Auto-discovery returned nothing (everything filtered by "
                       "exclude_databases?), and no explicit 'databases' list is set.")
            return report
        report.add("databases_discovered", "schema", OK,
                   f"{len(databases)} database(s) in scope: {', '.join(databases)}")

        present = await _check_database_presence(report, src, tgt, databases)

        await _check_source_config(report, src, len(present))
        await _check_target_config(report, tgt, len(present))
        await _check_privileges(report, cfg, src, tgt)
        await _check_naming_collisions(report, cfg, src, tgt, present)

        # Shared target-side facts, fetched once for all databases.
        ext_rows = await tgt.fetch("SELECT name, default_version FROM pg_available_extensions")
        tgt_available_ext = {r["name"]: r["default_version"] for r in ext_rows}
        role_rows = await tgt.fetch("SELECT rolname FROM pg_roles")
        tgt_roles = {r["rolname"] for r in role_rows}
        tgt_db_rows = await tgt.fetch("SELECT datname FROM pg_database")
        tgt_dbs = {r["datname"] for r in tgt_db_rows}

    # -- per-database work, bounded concurrency --------------------------------
    sem = asyncio.Semaphore(max(1, cfg.parallel_workers))

    async def _one(db: str) -> list[CheckResult]:
        async with sem:
            return await _check_db_local(
                cfg, db, tgt_available_ext, tgt_roles, tgt_has_db=db in tgt_dbs
            )

    for results in await asyncio.gather(*[_one(db) for db in present]):
        report.checks.extend(results)

    return report
