"""PostgreSQL logical replication management.

Creates publications on the source and subscriptions on the target.
Provides start / stop / status operations.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass

import asyncpg
import psycopg2
import psycopg2.extras

from pg_emigrant.config import DatabaseConfig, ReplicatorConfig
from pg_emigrant.db import connect
from pg_emigrant.utils import get_logger, qi, ql

log = get_logger(__name__)


def _safe_dbname(dbname: str) -> str:
    """Sanitize a database name for use as part of a PG identifier."""
    return re.sub(r"[^a-zA-Z0-9]", "_", dbname)


def pub_name(cfg: ReplicatorConfig, dbname: str) -> str:
    """Per-database publication name."""
    return f"{cfg.publication_name}_{_safe_dbname(dbname)}"


def sub_name(cfg: ReplicatorConfig, dbname: str) -> str:
    """Per-database subscription / slot name."""
    return f"{cfg.subscription_name}_{_safe_dbname(dbname)}"


def _libpq_conninfo(cfg: DatabaseConfig, dbname: str, *, replication: bool = False) -> str:
    """Build a keyword/value libpq connection string with proper escaping.

    Unlike a ``postgresql://`` URL, this format has no character (``@``, ``:``,
    ``/``, whitespace, ``'``) that needs percent-encoding in the password — each
    value is single-quoted with backslashes and quotes escaped per libpq rules.
    Used for the one raw ``CONNECTION '...'`` string embedded in subscription
    DDL, and for opening the replication-protocol connection (``replication=database``,
    required by ``CREATE_REPLICATION_SLOT`` — a command asyncpg cannot send since
    it does not implement the replication protocol).
    """
    def esc(v: object) -> str:
        return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"

    parts = [
        f"host={esc(cfg.host)}",
        f"port={esc(cfg.port)}",
        f"user={esc(cfg.user)}",
        f"password={esc(cfg.password)}",
        f"dbname={esc(dbname)}",
        f"sslmode={esc(cfg.sslmode)}",
    ]
    if replication:
        parts.append("replication=database")
    return " ".join(parts)


async def _drop_slot_if_present(conn: asyncpg.Connection, slot_name: str) -> bool:
    """Terminate the holding backend (if any) and drop *slot_name* if it exists.

    Shared by every place that needs to clean up a stale/orphaned replication
    slot before creating a fresh one (a previous bootstrap or subscription
    attempt may have been interrupted before teardown).  Returns True if a
    slot was found.
    """
    slot_row = await conn.fetchrow(
        "SELECT active, active_pid FROM pg_replication_slots WHERE slot_name = $1",
        slot_name,
    )
    if not slot_row:
        return False

    if slot_row["active"] and slot_row["active_pid"]:
        await conn.execute("SELECT pg_terminate_backend($1);", slot_row["active_pid"])
        log.info("Terminated backend PID %s holding slot %s", slot_row["active_pid"], slot_name)

        # pg_terminate_backend is asynchronous — the slot may still show as
        # active for a short period after the signal is sent.
        for _attempt in range(20):
            row = await conn.fetchrow(
                "SELECT active FROM pg_replication_slots WHERE slot_name = $1", slot_name
            )
            if row is None or not row["active"]:
                break
            await asyncio.sleep(0.5)
        else:
            log.warning("Slot %s still active after waiting; attempting drop anyway", slot_name)

    # Re-check existence before dropping — slot may have been cleaned up already.
    still_exists = await conn.fetchval(
        "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1", slot_name
    )
    if still_exists:
        await conn.execute("SELECT pg_drop_replication_slot($1);", slot_name)
        log.info("Dropped replication slot %s", slot_name)
    return True


@dataclass
class SlotSnapshot:
    """A freshly created logical replication slot and its exported snapshot.

    The exported snapshot (``snapshot_name``) is only valid while ``_conn`` —
    the replication-protocol connection that created the slot — stays open and
    idle.  The caller must finish every ``SET TRANSACTION SNAPSHOT
    '<snapshot_name>'`` copy before calling :meth:`aclose`.  Closing it does
    NOT drop the slot: the slot is a durable server-side object from this
    point on, ready to be attached to a subscription with ``create_slot =
    false``.
    """

    slot_name: str
    snapshot_name: str
    consistent_point: str
    _conn: "psycopg2.extensions.connection"

    async def aclose(self) -> None:
        await asyncio.to_thread(self._conn.close)


async def create_replication_slot_with_snapshot(
    cfg: ReplicatorConfig,
    dbname: str,
) -> SlotSnapshot:
    """Create the logical replication slot for *dbname* up front, exporting a
    snapshot consistent with the exact LSN the slot starts streaming from.

    This closes the data-loss window that exists if the slot were created only
    *after* the initial data copy: any transaction committed on the source in
    that window would be visible in neither the copy (already taken from an
    earlier, unrelated snapshot) nor the WAL stream (which only starts at slot
    creation) — it would be silently lost forever.  By creating the slot FIRST
    and copying data with the snapshot it exports, the copy and the start of
    WAL streaming are the exact same consistent point: nothing in between is
    missed, and nothing is duplicated.

    asyncpg cannot run ``CREATE_REPLICATION_SLOT`` — it does not implement the
    PostgreSQL replication protocol (no ``replication=database`` connection
    mode, no ``CopyBoth``).  This function uses ``psycopg2`` for that one
    command, run in a worker thread so the event loop is not blocked.

    The caller MUST keep the returned handle open until every worker copying
    data with ``snapshot_name`` has finished, then call ``await
    slot.aclose()``.
    """
    from pg_emigrant.db import connect as _connect  # local import: avoid cycle at module load

    slot = sub_name(cfg, dbname)

    async with _connect(cfg.source, dbname) as probe:
        if await _drop_slot_if_present(probe, slot):
            log.info("Dropped orphaned replication slot %s before recreating it", slot)
        # Pre-flight: CREATE_REPLICATION_SLOT for a logical slot must wait for
        # a consistent snapshot across the *entire* cluster — the same
        # ShareLock-on-every-active-XID wait that CREATE SUBSCRIPTION's
        # built-in slot creation is subject to.  Warn about long-running
        # transactions before attempting it so a hang is diagnosable.
        await _warn_replication_slot_blockers(probe)

    conninfo = _libpq_conninfo(cfg.source, dbname, replication=True)
    conn = await asyncio.to_thread(
        psycopg2.connect,
        conninfo,
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
    )
    try:
        cur = conn.cursor()
        # asyncio.shield: on timeout we still want to await the (now-cancelled)
        # call below so it actually unblocks and finishes before we touch the
        # connection again — psycopg2 connections are not safe to use from two
        # threads concurrently.
        execute_call = asyncio.to_thread(
            cur.execute, f'CREATE_REPLICATION_SLOT "{slot}" LOGICAL pgoutput'
        )
        try:
            await asyncio.wait_for(asyncio.shield(execute_call), timeout=60)
        except asyncio.TimeoutError:
            log.error(
                "CREATE_REPLICATION_SLOT for %s timed out after 60s — see the blocker "
                "warnings above; unblock the offending transaction(s) and retry",
                slot,
            )
            await asyncio.to_thread(conn.cancel)
            try:
                await execute_call
            except Exception:
                pass  # expected — the cancelled call raises once unblocked
            raise RuntimeError(
                f"CREATE_REPLICATION_SLOT for {slot} timed out — a long-running "
                f"transaction somewhere in the cluster is blocking slot creation "
                f"(see the warnings logged above for the exact pid/query)"
            )
        row = cur.fetchone()
    except BaseException:
        await asyncio.to_thread(conn.close)
        raise

    slot_name, consistent_point, snapshot_name = row[0], row[1], row[2]
    log.info(
        "Created replication slot %s in %s at %s (snapshot %s)",
        slot_name, dbname, consistent_point, snapshot_name,
    )
    return SlotSnapshot(
        slot_name=slot_name,
        snapshot_name=snapshot_name,
        consistent_point=consistent_point,
        _conn=conn,
    )


async def drop_replication_slot(cfg: ReplicatorConfig, dbname: str, slot_name: str) -> None:
    """Drop a replication slot on the source by name, with no subscription involved.

    Used to clean up a slot created by :func:`create_replication_slot_with_snapshot`
    when bootstrap aborts before a subscription is ever created for it.
    """
    async with connect(cfg.source, dbname) as conn:
        await _drop_slot_if_present(conn, slot_name)


async def _warn_replication_slot_blockers(conn: asyncpg.Connection) -> None:
    """Warn about open transactions that will block CREATE_REPLICATION_SLOT.

    CREATE_REPLICATION_SLOT must acquire a ShareLock on every active XID in the
    entire cluster — including transactions in *other* databases.  Any long-running
    or leaked 'idle in transaction' session will cause the slot creation to hang
    indefinitely until that transaction ends.

    Only transactions older than 30 seconds are reported to avoid noise from
    short-lived in-flight transactions.
    """
    rows = await conn.fetch(
        """
        SELECT pid,
               usename,
               datname,
               application_name,
               backend_type,
               state,
               backend_xid,
               EXTRACT(EPOCH FROM (now() - xact_start))::bigint AS xact_age_seconds,
               query
        FROM   pg_stat_activity
        WHERE  backend_xid IS NOT NULL
          AND  xact_start IS NOT NULL
          AND  pid <> pg_backend_pid()
          AND  EXTRACT(EPOCH FROM (now() - xact_start)) > 30
        ORDER  BY xact_start
        """
    )
    if not rows:
        return

    log.warning(
        "Found %d open transaction(s) on the source that will block CREATE_REPLICATION_SLOT. "
        "The slot creation must acquire a ShareLock on every active XID across the *entire* "
        "cluster — transactions in other databases also count. "
        "If any of these are leaked / idle-in-transaction sessions, terminate them before retrying.",
        len(rows),
    )
    for row in rows:
        age_s = int(row["xact_age_seconds"])
        age_str = f"{age_s // 3600:02d}:{(age_s % 3600) // 60:02d}:{age_s % 60:02d}"
        last_query = (row["query"] or "").strip().replace("\n", " ")[:120]
        log.warning(
            "  Blocker  pid=%-7s  user=%-20s  db=%-30s  state=%-22s  age=%s  query=%s",
            row["pid"],
            row["usename"] or "",
            row["datname"] or "",
            row["state"] or "",
            age_str,
            last_query,
        )
        log.warning(
            "  → To unblock run on source:  SELECT pg_terminate_backend(%s);",
            row["pid"],
        )


async def create_publication(
    cfg: ReplicatorConfig,
    dbname: str,
    schemas: list[str] | None = None,
) -> None:
    """Create a publication for all tables in *schemas*.

    *schemas* defaults to ``cfg.schemas`` when not provided.  Pass an explicit
    list when schemas were auto-discovered per-database during bootstrap.
    """
    pub = pub_name(cfg, dbname)
    resolved_schemas = schemas if schemas is not None else cfg.schemas
    async with connect(cfg.source, dbname) as conn:
        exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", pub
        )
        if exists:
            log.info("Publication %s already exists in %s", pub, dbname)
            return

        # Publication for all tables in the listed schemas
        schema_list = ", ".join(qi(s) for s in resolved_schemas)
        await conn.execute(
            f"CREATE PUBLICATION {qi(pub)} FOR TABLES IN SCHEMA {schema_list};"
        )
        log.info("Created publication %s in %s for schemas %s", pub, dbname, resolved_schemas)


async def drop_publication(
    cfg: ReplicatorConfig,
    dbname: str,
) -> None:
    """Drop the publication if it exists."""
    pub = pub_name(cfg, dbname)
    async with connect(cfg.source, dbname) as conn:
        await conn.execute(f"DROP PUBLICATION IF EXISTS {qi(pub)};")
        log.info("Dropped publication %s in %s", pub, dbname)


async def create_subscription(
    cfg: ReplicatorConfig,
    dbname: str,
    *,
    create_slot: bool = True,
) -> None:
    """Create a subscription on the target pointing to the source publication.

    The subscription is created with ``copy_data = false`` because we
    already performed an initial data copy via COPY.

    Args:
        create_slot: When True (default — used by ``reinit-sync``'s from-scratch
            recovery path, which never re-copies data), PostgreSQL creates a
            fresh replication slot as part of ``CREATE SUBSCRIPTION`` itself.
            When False, the slot named ``sub_name(cfg, dbname)`` MUST already
            exist (created by :func:`create_replication_slot_with_snapshot` —
            this is what ``bootstrap`` uses, so the data copy's snapshot and
            the slot's start LSN are the exact same consistent point).
    """
    sub = sub_name(cfg, dbname)
    pub = pub_name(cfg, dbname)

    async with connect(cfg.target, dbname) as conn:
        exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_subscription
                WHERE subname = $1
                  AND subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            )
            """,
            sub,
        )
        if exists:
            log.info("Subscription %s already exists in %s", sub, dbname)
            return

        conninfo = _libpq_conninfo(cfg.source, dbname)

        if create_slot:
            # Drop orphaned replication slot on source if it already exists
            # (can happen when a previous attempt was interrupted before teardown).
            async with connect(cfg.source, dbname) as src_conn:
                if await _drop_slot_if_present(src_conn, sub):
                    log.info("Dropped orphaned replication slot %s on source", sub)

            # Pre-flight: warn about open transactions that will block slot creation.
            # CREATE_REPLICATION_SLOT acquires a ShareLock on every active XID in the
            # entire cluster, so even a leaked 'idle in transaction' session in an
            # unrelated database will cause this to hang indefinitely.
            async with connect(cfg.source, dbname) as src_conn:
                await _warn_replication_slot_blockers(src_conn)

            with_clause = "copy_data = false, create_slot = true"
        else:
            # The slot already exists (created explicitly up front so its
            # exported snapshot could be used for the data copy) — attach to
            # it by name instead of creating a new one.
            with_clause = f'copy_data = false, create_slot = false, slot_name = {qi(sub)}'

        # conninfo is already libpq-escaped by _libpq_conninfo() (each value is
        # single-quoted, with embedded backslashes/quotes backslash-escaped
        # per libpq rules).  It is now embedded inside a SQL string literal for
        # CONNECTION, which is a SEPARATE grammar: every single quote in the
        # whole conninfo text (both libpq's own quotes and any escaped one)
        # must ALSO be doubled for SQL, or it terminates the literal early —
        # ql() does exactly that, on top of (not instead of) the libpq escaping.
        sql = (
            f"CREATE SUBSCRIPTION {qi(sub)} "
            f"CONNECTION {ql(conninfo)} "
            f"PUBLICATION {qi(pub)} "
            f"WITH ({with_clause});"
        )
        try:
            # Timeout guards against CREATE SUBSCRIPTION hanging indefinitely when
            # the WAL receiver cannot reach the source (e.g. pg_hba.conf replication
            # entry missing for the target host, max_wal_senders exhausted, or network
            # change).
            await conn.execute(sql, timeout=60)
        except (asyncio.TimeoutError, asyncpg.exceptions.QueryCanceledError) as exc:
            log.error(
                "CREATE SUBSCRIPTION %s timed out after 60 s — "
                "check pg_hba.conf (replication entry for the target host) and "
                "max_wal_senders on the source",
                sub,
            )
            if create_slot:
                # PostgreSQL creates the replication slot on the source first,
                # then opens the WAL receiver connection — if that second step
                # hangs we must clean up the orphaned slot it already made.
                async with connect(cfg.source, dbname) as src_conn:
                    if await _drop_slot_if_present(src_conn, sub):
                        log.info("Dropped orphaned slot %s after subscription timeout", sub)
            raise RuntimeError(
                f"CREATE SUBSCRIPTION {sub} timed out — verify that the source "
                f"pg_hba.conf has a 'replication' entry for the target host "
                f"and that max_wal_senders is not exhausted"
            ) from exc
        log.info("Created subscription %s in %s", sub, dbname)


async def drop_subscription(
    cfg: ReplicatorConfig,
    dbname: str,
) -> None:
    """Disable and drop the subscription if it exists."""
    sub = sub_name(cfg, dbname)
    async with connect(cfg.target, dbname) as conn:
        exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_subscription
                WHERE subname = $1
                  AND subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            )
            """,
            sub,
        )
        if not exists:
            log.info("Subscription %s does not exist in %s", sub, dbname)
            return

        await conn.execute(f"ALTER SUBSCRIPTION {qi(sub)} DISABLE;")
        await conn.execute(
            f"ALTER SUBSCRIPTION {qi(sub)} SET (slot_name = NONE);"
        )
        await conn.execute(f"DROP SUBSCRIPTION IF EXISTS {qi(sub)};")
        log.info("Dropped subscription %s in %s", sub, dbname)

    # Clean up the replication slot on the source
    async with connect(cfg.source, dbname) as conn:
        await _drop_slot_if_present(conn, sub)


async def enable_subscription(
    cfg: ReplicatorConfig,
    dbname: str,
) -> None:
    """Enable a disabled subscription."""
    sub = sub_name(cfg, dbname)
    async with connect(cfg.target, dbname) as conn:
        await conn.execute(f"ALTER SUBSCRIPTION {qi(sub)} ENABLE;")
        log.info("Enabled subscription %s in %s", sub, dbname)


async def disable_subscription(
    cfg: ReplicatorConfig,
    dbname: str,
) -> None:
    """Disable (pause) a subscription."""
    sub = sub_name(cfg, dbname)
    async with connect(cfg.target, dbname) as conn:
        await conn.execute(f"ALTER SUBSCRIPTION {qi(sub)} DISABLE;")
        log.info("Disabled subscription %s in %s", sub, dbname)


async def get_subscription_status(
    cfg: ReplicatorConfig,
    dbname: str,
) -> list[dict]:
    """Query pg_stat_subscription on the target."""
    sub = sub_name(cfg, dbname)
    async with connect(cfg.target, dbname) as conn:
        rows = await conn.fetch(
            """
            SELECT
                subname,
                pid,
                leader_pid,
                relid,
                received_lsn,
                last_msg_send_time,
                last_msg_receipt_time,
                latest_end_lsn,
                latest_end_time,
                pg_size_pretty(
                    pg_wal_lsn_diff(latest_end_lsn, received_lsn)
                ) AS lag
            FROM pg_stat_subscription
            WHERE subname = $1;
            """,
            sub,
        )
        return [dict(r) for r in rows]


async def get_all_subscription_status(cfg: ReplicatorConfig) -> list[dict]:
    """Query pg_stat_subscription for *every* subscription in one round-trip.

    ``pg_stat_subscription`` is a cluster-wide view, so a single connection to
    the target (any database) returns the status of all per-database
    subscriptions at once — no need to connect to each database separately.
    Callers match rows back to a database via ``sub_name(cfg, dbname)``.
    """
    async with connect(cfg.target) as conn:
        rows = await conn.fetch(
            """
            SELECT
                subname,
                pid,
                leader_pid,
                relid,
                received_lsn,
                last_msg_send_time,
                last_msg_receipt_time,
                latest_end_lsn,
                latest_end_time,
                pg_size_pretty(
                    pg_wal_lsn_diff(latest_end_lsn, received_lsn)
                ) AS lag
            FROM pg_stat_subscription;
            """
        )
        return [dict(r) for r in rows]


async def refresh_subscription(
    cfg: ReplicatorConfig,
    dbname: str,
    copy_data: bool = False,
) -> None:
    """Refresh the subscription so newly added tables start being replicated.

    Args:
        copy_data: When True, PostgreSQL will use its tablesync mechanism to
            perform an initial copy of any newly discovered table data.  This
            is the safe default when adding a new table to the publication
            because it avoids WAL-replay conflicts that arise from manually
            copying data before re-enabling the subscription.
    """
    sub = sub_name(cfg, dbname)
    async with connect(cfg.target, dbname) as conn:
        exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_subscription
                WHERE subname = $1
                  AND subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            )
            """,
            sub,
        )
        if not exists:
            log.info("No subscription %s in %s — skipping refresh", sub, dbname)
            return
        copy_data_val = "true" if copy_data else "false"
        await conn.execute(
            f"ALTER SUBSCRIPTION {qi(sub)} REFRESH PUBLICATION WITH (copy_data = {copy_data_val});"
        )
        log.info("Refreshed subscription %s in %s (copy_data=%s)", sub, dbname, copy_data_val)


async def get_replication_slots(
    cfg: ReplicatorConfig,
    dbname: str,
) -> list[dict]:
    """Query pg_replication_slots on the source, filtered to this database's slot."""
    slot = sub_name(cfg, dbname)
    async with connect(cfg.source, dbname) as conn:
        rows = await conn.fetch(
            """
            SELECT
                slot_name,
                slot_type,
                active,
                active_pid,
                restart_lsn,
                confirmed_flush_lsn,
                wal_status
            FROM pg_replication_slots
            WHERE slot_name = $1;
            """,
            slot,
        )
        return [dict(r) for r in rows]


async def get_all_replication_slots(cfg: ReplicatorConfig) -> list[dict]:
    """Query pg_replication_slots for *every* slot in one round-trip.

    ``pg_replication_slots`` is cluster-wide, so a single source connection
    returns all per-database slots at once.  Callers match a slot to its
    database via ``sub_name(cfg, dbname)`` (the slot name).
    """
    async with connect(cfg.source) as conn:
        rows = await conn.fetch(
            """
            SELECT
                slot_name,
                slot_type,
                active,
                active_pid,
                restart_lsn,
                confirmed_flush_lsn,
                wal_status
            FROM pg_replication_slots;
            """
        )
        return [dict(r) for r in rows]


async def reinit_sync(
    cfg: ReplicatorConfig,
    dbname: str,
) -> dict:
    """Verify and restore replication components for *dbname* after a Patroni switchover/failover.

    Checks performed (in order):
    1. Publication exists on source   → recreate if missing.
    2. Replication slot exists on source and is healthy (wal_status != 'lost').
    3. Subscription exists on target and matches slot health:
       - Slot OK, subscription disabled  → re-enable.
       - Slot OK, subscription enabled but apply worker not running → refresh publication.
       - Slot missing/lost OR subscription missing → drop subscription (if present) + drop
         orphaned slot (if present) + recreate subscription (which creates a fresh slot).

    Returns a dict with:
        database      – the database name processed
        issues_found  – list of problems detected
        actions_taken – list of corrective actions executed
        was_healthy   – True when no issues were found (nothing needed fixing)
    """
    pub = pub_name(cfg, dbname)
    sub = sub_name(cfg, dbname)
    issues: list[str] = []
    actions: list[str] = []

    # ── 1. Publication ────────────────────────────────────────────────────────
    async with connect(cfg.source, dbname) as conn:
        pub_exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", pub
        )

    if not pub_exists:
        issues.append(f"Publication '{pub}' not found on source")
        schema_list = ", ".join(qi(s) for s in cfg.schemas)
        async with connect(cfg.source, dbname) as conn:
            await conn.execute(
                f"CREATE PUBLICATION {qi(pub)} FOR TABLES IN SCHEMA {schema_list};"
            )
        actions.append(f"Recreated publication '{pub}' on source")
        log.info("reinit_sync [%s]: recreated publication %s", dbname, pub)
    else:
        log.debug("reinit_sync [%s]: publication %s OK", dbname, pub)

    # ── 2. Replication slot ───────────────────────────────────────────────────
    async with connect(cfg.source, dbname) as conn:
        slot_row = await conn.fetchrow(
            """
            SELECT slot_name, active, active_pid, wal_status
            FROM pg_replication_slots
            WHERE slot_name = $1
            """,
            sub,
        )

    if slot_row is None:
        issues.append(f"Replication slot '{sub}' not found on source")
        slot_ok = False
    elif slot_row["wal_status"] == "lost":
        issues.append(
            f"Replication slot '{sub}' has wal_status='lost' — WAL was already recycled"
        )
        slot_ok = False
    else:
        log.debug(
            "reinit_sync [%s]: slot %s OK (wal_status=%s, active=%s)",
            dbname, sub, slot_row["wal_status"], slot_row["active"],
        )
        slot_ok = True

    # ── 3. Subscription ───────────────────────────────────────────────────────
    async with connect(cfg.target, dbname) as conn:
        sub_row = await conn.fetchrow(
            """
            SELECT subname, subenabled
            FROM pg_subscription
            WHERE subname = $1
              AND subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            """,
            sub,
        )

    need_recreation = sub_row is None or not slot_ok

    if not need_recreation:
        # Slot is healthy and subscription row exists — check operational state.
        if not sub_row["subenabled"]:
            issues.append(f"Subscription '{sub}' exists but is disabled")
            async with connect(cfg.target, dbname) as conn:
                await conn.execute(f"ALTER SUBSCRIPTION {qi(sub)} ENABLE;")
            actions.append(f"Re-enabled subscription '{sub}'")
            log.info("reinit_sync [%s]: re-enabled subscription %s", dbname, sub)
        else:
            # Check whether the apply worker is actually running.
            stats = await get_subscription_status(cfg, dbname)
            apply_running = any(
                s["pid"] is not None and s["relid"] is None for s in stats
            )
            if not apply_running:
                issues.append(
                    f"Subscription '{sub}' is enabled but apply worker is not running"
                )
                async with connect(cfg.target, dbname) as conn:
                    await conn.execute(
                        f"ALTER SUBSCRIPTION {qi(sub)} REFRESH PUBLICATION "
                        f"WITH (copy_data = false);"
                    )
                actions.append(f"Refreshed publication list for subscription '{sub}'")
                log.info("reinit_sync [%s]: refreshed subscription %s", dbname, sub)
            else:
                log.debug("reinit_sync [%s]: subscription %s fully healthy", dbname, sub)
    else:
        # Need to tear down and recreate from scratch.
        if sub_row is None:
            issues.append(f"Subscription '{sub}' not found on target")
        else:
            issues.append(
                f"Subscription '{sub}' exists but replication slot is missing/unhealthy — will recreate"
            )

        # Gracefully drop the subscription if it still exists on target.
        if sub_row is not None:
            async with connect(cfg.target, dbname) as conn:
                await conn.execute(f"ALTER SUBSCRIPTION {qi(sub)} DISABLE;")
                await conn.execute(f"ALTER SUBSCRIPTION {qi(sub)} SET (slot_name = NONE);")
                await conn.execute(f"DROP SUBSCRIPTION IF EXISTS {qi(sub)};")
            log.info("reinit_sync [%s]: dropped broken subscription %s", dbname, sub)

        # Drop any orphaned slot that might still linger on source.
        async with connect(cfg.source, dbname) as conn:
            if await _drop_slot_if_present(conn, sub):
                log.info("reinit_sync [%s]: dropped orphaned slot %s", dbname, sub)

        # Create a fresh subscription — PostgreSQL will also create the slot.
        # (reinit-sync never re-copies data, so there is no snapshot/slot
        # consistency window to worry about here — unlike bootstrap.)
        await create_subscription(cfg, dbname, create_slot=True)
        actions.append(f"Recreated subscription '{sub}' with a fresh replication slot")
        log.info("reinit_sync [%s]: recreated subscription %s", dbname, sub)

    return {
        "database": dbname,
        "issues_found": issues,
        "actions_taken": actions,
        "was_healthy": len(issues) == 0,
    }
