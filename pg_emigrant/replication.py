"""PostgreSQL logical replication management.

Creates publications on the source and subscriptions on the target.
Provides start / stop / status operations.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
from dataclasses import dataclass

import asyncpg
import psycopg2
import psycopg2.extras

from pg_emigrant.config import DatabaseConfig, ReplicatorConfig
from pg_emigrant.db import connect, discover_schemas
from pg_emigrant.utils import get_logger, qi, ql

log = get_logger(__name__)


def _safe_dbname(dbname: str) -> str:
    """Sanitize a database name for use in publication/subscription/slot names.

    Replication slot names may only contain lower-case letters, digits and
    underscores, and slots are CLUSTER-WIDE on the source — so the mapping
    must also be collision-free: "my-app" and "my_app" (or "MyApp" and
    "myapp") must not produce the same slot name, otherwise bootstrapping one
    database would terminate and drop the other's live slot.  When the
    sanitisation is lossy, a short stable hash of the original name is
    appended so distinct databases stay distinct.
    """
    safe = re.sub(r"[^a-z0-9]", "_", dbname.lower())
    if safe != dbname:
        safe += "_" + hashlib.sha1(dbname.encode()).hexdigest()[:6]
    return safe


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


async def _node_fingerprint(conn: asyncpg.Connection) -> str:
    """One-line diagnostic identifying WHICH physical node a connection
    actually reached: recovery state (primary vs. standby) and the address
    the server itself reports for this connection.

    Logical replication slots are local storage on a single instance — if
    ``cfg.source``/``cfg.target`` resolve through a load balancer, VIP, or
    connection pooler that can route different TCP connections to different
    physical nodes (a health-checked HAProxy tier, DNS round-robin, a
    Patroni VIP mid-failover, …), two connections opened moments apart can
    silently land on different machines. This string is logged at every
    slot-sensitive step so that if it ever DOES happen, the evidence is
    already in the log instead of requiring guesswork after the fact.
    """
    row = await conn.fetchrow(
        "SELECT pg_is_in_recovery() AS in_recovery, "
        "inet_server_addr()::text AS addr, inet_server_port() AS port"
    )
    role = "STANDBY/replica" if row["in_recovery"] else "primary"
    return f"{role} at {row['addr'] or 'local socket'}:{row['port']}"


_UNSTABLE_HOSTS = {"localhost", "127.0.0.1", "::1"}


def warn_if_unstable_host(cfg: ReplicatorConfig) -> None:
    """Warn when ``source``/``target`` is configured as ``localhost`` (or an
    equivalent loopback address) — root-caused and reproduced directly
    against a real production incident.

    The trap: running pg_emigrant directly on the source's own database
    host, for network locality, with ``host: localhost`` in config.yaml.
    pg_emigrant's OWN connections (slot creation, schema sync, data copy,
    even a naive post-creation check) correctly resolve "localhost" to the
    real source, because pg_emigrant's process shares that machine's network
    namespace — everything LOOKS like it works. But ``CREATE SUBSCRIPTION``'s
    ``CONNECTION`` string is stored VERBATIM in ``pg_subscription.subconninfo``
    and is resolved LATER by the TARGET's own background apply worker
    process, using the TARGET MACHINE's own network stack — a completely
    different machine. "localhost" there means "connect to myself". The
    result, reproduced directly: `subconninfo` shows `host='localhost'`, the
    target's apply worker tries to stream from itself, and fails forever
    with a bare ``replication slot "..." does not exist`` — logged only on
    the target, invisible to pg_emigrant, and NOT fixed by repeated
    ``reinit-sync`` runs (each one bakes in the same wrong literal string
    again). A single-node, non-HA setup where source/target genuinely both
    run wherever "localhost" is evaluated is fine; this is a warning, not a
    hard error, because pg_emigrant cannot tell the two situations apart
    from the config alone.
    """
    for label, side in (("source", cfg.source), ("target", cfg.target)):
        if side.host.strip().lower() in _UNSTABLE_HOSTS:
            log.warning(
                "%s.host is %r. This string is stored verbatim in the "
                "subscription's CONNECTION info and later resolved by the "
                "TARGET's own apply worker process, on the TARGET machine — "
                "NOT by wherever pg_emigrant itself runs. If pg_emigrant runs "
                "on the source's own database host, its OWN connections "
                "resolve '%s' correctly (looks fine!) but the target's apply "
                "worker resolves the same literal string to ITSELF, silently "
                "breaking replication forever (a confirmed, reproduced "
                "production failure mode — see README 'Special handling & "
                "edge cases'). Point '%s' at a fixed address that means the "
                "SAME thing from every machine that might evaluate it — the "
                "cluster's real (VIP/HAProxy leader-only) address — never "
                "'localhost'/'127.0.0.1'. Harmless to ignore only if '%s' "
                "truly has no other machine ever resolving this string.",
                label, side.host, label, label, label,
            )


async def _verify_apply_worker_streaming(
    cfg: ReplicatorConfig,
    dbname: str,
    sub: str,
    *,
    attempts: int = 15,
    delay: float = 1.0,
) -> tuple[bool, str]:
    """Confirm the subscription's apply worker is ACTUALLY connected and
    streaming, by polling the TARGET's own view of it — the only check that
    is immune to the ``CONNECTION`` string embedded in the subscription
    being resolved differently by the target machine than by whatever
    machine pg_emigrant itself runs on.

    This distinction is not academic — it was root-caused against a real
    production incident. ``CREATE SUBSCRIPTION``'s ``CONNECTION`` string is
    stored verbatim in ``pg_subscription.subconninfo`` and is later resolved
    by the TARGET's own background apply worker process, using the TARGET
    MACHINE's own network stack — NOT by whatever process issued the DDL.
    If pg_emigrant runs directly on the source's own database host with
    ``source.host: localhost`` in config.yaml (tempting for network
    locality), pg_emigrant's OWN connections correctly resolve "localhost"
    to the real source (since pg_emigrant's process shares that machine's
    network namespace) — creating the slot and copying data all work fine,
    and a naive verification that also connects via `cfg.source` from
    pg_emigrant's own process would ALSO see the slot and report success.
    But the literal string "localhost" is what ends up in `subconninfo` —
    and when the TARGET's apply worker (running on the completely different
    target machine) resolves it, "localhost" means "connect to myself".
    Reproduced directly: `subconninfo` showed `host='localhost'`, the
    target's own apply worker tried to stream from itself, and failed with
    exactly the observed `replication slot "..." does not exist` (the target
    has no such slot — slots are per-instance local storage, and this one
    only ever existed on the real, remote source).

    Returns ``(ok, detail)``: `ok=True` once the main apply worker
    (``relid IS NULL`` in ``pg_stat_subscription``) is running and has
    advanced `received_lsn` past NULL; `ok=False` with a diagnostic message
    after exhausting ``attempts`` polls without that ever happening.
    """
    last_err_count: int | None = None
    detail = "no data collected"
    for attempt in range(attempts):
        async with connect(cfg.target, dbname) as conn:
            worker = await conn.fetchrow(
                "SELECT pid, received_lsn FROM pg_stat_subscription"
                " WHERE subname = $1 AND relid IS NULL",
                sub,
            )
            target_major = conn.get_server_version().major
            err_count = None
            if target_major >= 15:
                err_count = await conn.fetchval(
                    "SELECT apply_error_count FROM pg_stat_subscription_stats"
                    " WHERE subname = $1",
                    sub,
                )
        if worker is not None and worker["received_lsn"] is not None:
            return True, (
                f"apply worker running (pid={worker['pid']}, "
                f"received_lsn={worker['received_lsn']})"
            )
        if err_count is not None and last_err_count is not None and err_count > last_err_count:
            return False, (
                f"apply worker is failing repeatedly on the TARGET "
                f"(apply_error_count rising: {last_err_count} → {err_count} "
                f"over {attempt} attempt(s))"
            )
        last_err_count = err_count
        detail = (
            f"no successful stream after {attempt + 1}/{attempts} attempts"
            + (f" (apply_error_count={err_count})" if err_count else "")
        )
        if attempt < attempts - 1:
            await asyncio.sleep(delay)
    return False, detail


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
        # Log which physical node this connection actually reached — see
        # _node_fingerprint. If "source" resolves through a VIP/load
        # balancer/pooler, this is the only record of where the slot is
        # about to be created, in case a later step lands somewhere else.
        fp = await _node_fingerprint(probe)
        log.info("create_replication_slot_with_snapshot [%s]: source connection is %s", dbname, fp)
        if "STANDBY" in fp:
            log.warning(
                "create_replication_slot_with_snapshot [%s]: 'source' resolved to a "
                "STANDBY, not a primary. Creating a logical replication slot there "
                "only works with PostgreSQL 16+ standby logical decoding, explicitly "
                "enabled (hot_standby_feedback on the standby, wal_level=logical on "
                "the PRIMARY) — if that isn't deliberately configured, this will "
                "fail outright, or (worse) succeed on a node that a later connection "
                "through the same load-balanced endpoint won't reach again.",
                dbname,
            )
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

    Prepared (two-phase) transactions are checked separately: they hold their
    XID until COMMIT/ROLLBACK PREPARED, block slot creation indefinitely, and
    do NOT appear in pg_stat_activity at all.
    """
    prepared = await conn.fetch(
        """
        SELECT gid, prepared, owner, database
        FROM   pg_prepared_xacts
        WHERE  prepared < now() - interval '30 seconds'
        ORDER  BY prepared
        """
    )
    if prepared:
        log.warning(
            "Found %d prepared transaction(s) (two-phase commit) on the source. "
            "They hold their XID until COMMIT/ROLLBACK PREPARED and will block "
            "CREATE_REPLICATION_SLOT indefinitely — and they never show up in "
            "pg_stat_activity, so no backend list below can include them.",
            len(prepared),
        )
        for row in prepared:
            log.warning(
                "  Prepared xact  gid=%-30s  db=%-20s  owner=%-15s  prepared=%s",
                row["gid"], row["database"], row["owner"], row["prepared"],
            )
            log.warning(
                "  → To unblock run in database %s:  ROLLBACK PREPARED %s;  (or COMMIT PREPARED)",
                row["database"], ql(row["gid"]),
            )

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


async def _publishable_tables(
    conn: asyncpg.Connection, schemas: list[str]
) -> set[tuple[str, str]]:
    """(schema, table) pairs in *schemas* eligible for direct publication
    membership: ordinary tables and partitioned parents.  Partition children
    are excluded — they replicate through their root and would duplicate
    (or conflict with) it if published individually."""
    rows = await conn.fetch(
        """
        SELECT n.nspname, c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY($1::text[])
          AND c.relkind IN ('r', 'p')
          AND NOT c.relispartition
        """,
        schemas,
    )
    return {(r["nspname"], r["relname"]) for r in rows}


async def _create_publication_on(
    conn, pub: str, schemas: list[str], dbname: str
) -> None:
    """CREATE PUBLICATION for all tables in *schemas*, source-version-aware.

    PostgreSQL 15+ supports ``FOR TABLES IN SCHEMA`` (which auto-includes
    tables created later).  PG 13/14 predate that syntax, so there the
    current tables are enumerated with ``FOR TABLE`` instead. Either way,
    tables (and, on <15, schemas) created after this point are picked up
    automatically by :func:`sync_new_tables`, which runs on every tick of
    ``sync-sequences --loop`` — no manual ``ALTER PUBLICATION`` needed.
    """
    major = conn.get_server_version().major
    if major >= 15:
        schema_list = ", ".join(qi(s) for s in schemas)
        await conn.execute(
            f"CREATE PUBLICATION {qi(pub)} FOR TABLES IN SCHEMA {schema_list};"
        )
        return

    table_set = await _publishable_tables(conn, schemas)
    if table_set:
        table_list = ", ".join(f"{qi(s)}.{qi(t)}" for s, t in sorted(table_set))
        await conn.execute(f"CREATE PUBLICATION {qi(pub)} FOR TABLE {table_list};")
    else:
        await conn.execute(f"CREATE PUBLICATION {qi(pub)};")
    log.info(
        "Source database %s is PostgreSQL %d (< 15): publication %s enumerates "
        "the %d current top-level tables (FOR TABLES IN SCHEMA is unavailable "
        "before PG15). Tables and schemas created later are picked up "
        "automatically by the 'sync-sequences --loop' process — no manual "
        "action needed as long as that loop is running.",
        dbname, major, pub, len(table_set),
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

        await _create_publication_on(conn, pub, resolved_schemas, dbname)
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

        # CREATE SUBSCRIPTION succeeding proves NOTHING about whether
        # replication actually works: PostgreSQL does not validate slot
        # existence as part of this DDL (verified directly — the statement
        # returns success even when the referenced slot has already been
        # dropped), and — root-caused against a real production incident —
        # a naive check from pg_emigrant's OWN connection to "cfg.source" is
        # NOT a reliable proxy for what the target's apply worker actually
        # experiences: the CONNECTION string embedded in CREATE SUBSCRIPTION
        # is stored in pg_subscription.subconninfo and resolved LATER by the
        # target's own background apply worker, using the TARGET MACHINE's
        # own network stack — not by whatever process (pg_emigrant) issued
        # the DDL. If "cfg.source" is e.g. "localhost" because pg_emigrant
        # runs directly on the source's own database host, pg_emigrant's own
        # connections (including a naive post-creation check) correctly
        # resolve "localhost" to the real source — but the target's apply
        # worker, running on a DIFFERENT machine, resolves the same literal
        # string to ITSELF, and fails with a bare "replication slot ... does
        # not exist" that is only ever logged on the target, invisible here.
        # (The same failure mode also results from a load-balanced/failover
        # source endpoint routing different connections to different nodes.)
        #
        # The only check immune to all of the above: ask the TARGET what its
        # OWN apply worker is actually doing, via pg_stat_subscription —
        # this reflects reality regardless of which machine misresolved
        # what. Runs for both create_slot=True (reinit-sync's from-scratch
        # path) and create_slot=False (bootstrap's attach-to-existing-slot
        # path) — this class of problem can hit either.
        ok, worker_detail = await _verify_apply_worker_streaming(cfg, dbname, sub)
        if not ok:
            async with connect(cfg.target, dbname) as conn2:
                subconninfo = await conn2.fetchval(
                    "SELECT subconninfo FROM pg_subscription WHERE subname = $1", sub
                )
            raise RuntimeError(
                f"Subscription {sub} was created in {dbname}, but its apply worker "
                f"on the TARGET is not actually streaming ({worker_detail}). This "
                f"almost always means the CONNECTION string stored in the "
                f"subscription cannot reach the real source FROM THE TARGET "
                f"MACHINE's OWN point of view — even though pg_emigrant's own "
                f"connections to 'source' work fine. Two confirmed causes: (1) "
                f"'source.host' is 'localhost'/'127.0.0.1' — this resolves "
                f"correctly ONLY when pg_emigrant runs on the exact same machine "
                f"as the source, but the TARGET's apply worker resolves the same "
                f"literal string to ITSELF, not to the source; (2) 'source' is a "
                f"load-balanced endpoint that routed pg_emigrant's own "
                f"connections and the apply worker's connection to different "
                f"physical nodes. The stored connection string is: {subconninfo!r} "
                f"— log into the TARGET machine itself and confirm THIS EXACT "
                f"string, from there, reaches the real source (not the target "
                f"itself, not a different node). 'source.host' must be a fixed "
                f"address that means the same thing from every machine that "
                f"might evaluate it — never 'localhost', never a load-balanced "
                f"endpoint. Fix config.yaml, then run "
                f"'pg_emigrant teardown --database {dbname}' and re-run bootstrap."
            )
        log.info(
            "Verified subscription %s is actually streaming in %s (%s)",
            sub, dbname, worker_detail,
        )


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


async def sync_new_tables(cfg: ReplicatorConfig, dbname: str) -> list[str]:
    """Bring tables created on the source AFTER bootstrap into replication —
    fully automatically, with no manual ``ALTER PUBLICATION`` / ``detect-ddl
    --apply`` step required.

    PostgreSQL never auto-publishes a new table in two situations, and never
    auto-streams a newly published table to an existing subscriber at all:

    * On a **source older than PostgreSQL 15**, ``FOR TABLES IN SCHEMA``
      doesn't exist, so the publication created at bootstrap is a frozen
      snapshot of the tables that existed then (see
      :func:`_create_publication_on`) — a table created afterwards is
      invisible to the publication until explicitly ``ADD TABLE``'d.
    * On a **15+ source running in auto-discover mode** (empty
      ``cfg.schemas``), a schema created after bootstrap is likewise outside
      the publication's schema list until explicitly ``ADD TABLES IN
      SCHEMA``'d — ``FOR TABLES IN SCHEMA`` only auto-covers *new tables in
      an already-published schema*.
    * On **every** version, even once a table is a publication member,
      logical replication does not start streaming it to an already-created
      subscription until ``ALTER SUBSCRIPTION … REFRESH PUBLICATION`` runs —
      and that table must physically exist on the target first, or the
      subscription's tablesync worker fails and retries forever.

    This function closes all three gaps in one pass: it adds any new tables
    (<15) or schemas (auto-discover), creates the physical table on the
    target for anything the publication now covers that the target
    subscription doesn't know about yet, and refreshes the subscription with
    ``copy_data = true`` so PostgreSQL's own tablesync mechanism performs the
    initial copy — no manually-triggered command needed as long as this runs
    on a recurring basis, which is why it is folded into
    ``sync-sequences --loop`` (the process already documented as the one to
    keep running for the whole replication window).

    A no-op (returns ``[]``) when this database has no publication yet
    (replication not set up) — callers loop over every database and not all
    of them may be bootstrapped.

    Returns a list of human-readable action descriptions (empty when there
    was nothing to do); callers log/display these.
    """
    from pg_emigrant.ddl_detector import generate_full_table_ddl

    pub = pub_name(cfg, dbname)
    sub = sub_name(cfg, dbname)
    actions: list[str] = []

    async with connect(cfg.source, dbname) as src:
        pub_exists = await src.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", pub
        )
        if not pub_exists:
            return actions

        schemas = await discover_schemas(src, cfg)
        major = src.get_server_version().major

        published_rows = await src.fetch(
            "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = $1",
            pub,
        )
        published_tables = {(r["schemaname"], r["tablename"]) for r in published_rows}

        if major < 15:
            current_tables = await _publishable_tables(src, schemas)
            new_tables = sorted(current_tables - published_tables)
            if new_tables:
                table_list = ", ".join(f"{qi(s)}.{qi(t)}" for s, t in new_tables)
                await src.execute(f"ALTER PUBLICATION {qi(pub)} ADD TABLE {table_list};")
                actions.append(
                    f"Added {len(new_tables)} new table(s) to publication {pub}: "
                    + ", ".join(f"{s}.{t}" for s, t in new_tables)
                )
                log.info(
                    "sync_new_tables [%s]: added %d new table(s) to publication %s "
                    "(PostgreSQL %d source has no FOR TABLES IN SCHEMA auto-inclusion): %s",
                    dbname, len(new_tables), pub, major, new_tables,
                )
                published_tables |= set(new_tables)
        else:
            pub_schema_rows = await src.fetch(
                """
                SELECT n.nspname FROM pg_publication_namespace pn
                JOIN pg_namespace n ON n.oid = pn.pnnspid
                JOIN pg_publication p ON p.oid = pn.pnpubid
                WHERE p.pubname = $1
                """,
                pub,
            )
            published_schemas = {r["nspname"] for r in pub_schema_rows}
            new_schemas = sorted(set(schemas) - published_schemas)
            if new_schemas:
                schema_list = ", ".join(qi(s) for s in new_schemas)
                await src.execute(
                    f"ALTER PUBLICATION {qi(pub)} ADD TABLES IN SCHEMA {schema_list};"
                )
                actions.append(f"Added new schema(s) to publication {pub}: {', '.join(new_schemas)}")
                log.info(
                    "sync_new_tables [%s]: added schema(s) %s to publication %s (auto-discover mode)",
                    dbname, new_schemas, pub,
                )
                published_rows = await src.fetch(
                    "SELECT schemaname, tablename FROM pg_publication_tables"
                    " WHERE pubname = $1",
                    pub,
                )
                published_tables = {(r["schemaname"], r["tablename"]) for r in published_rows}
            # Tables created later in an already-published schema join the
            # publication automatically (FOR TABLES IN SCHEMA) — no action.

    # Whatever is now published but not yet known to the target subscription
    # needs its table created there (if missing) and the subscription
    # refreshed so PostgreSQL starts its tablesync.
    async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
        tracked_rows = await tgt.fetch(
            """
            SELECT n.nspname, c.relname
            FROM pg_subscription_rel sr
            JOIN pg_subscription s ON s.oid = sr.srsubid
            JOIN pg_class c ON c.oid = sr.srrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE s.subname = $1
            """,
            sub,
        )
        tracked_tables = {(r["nspname"], r["relname"]) for r in tracked_rows}
        untracked = sorted(published_tables - tracked_tables)
        if not untracked:
            return actions

        tgt_schema_rows = await tgt.fetch(
            "SELECT nspname FROM pg_namespace WHERE nspname = ANY($1::text[])",
            list({s for s, _ in untracked}),
        )
        tgt_schemas = {r["nspname"] for r in tgt_schema_rows}
        tgt_table_rows = await tgt.fetch(
            """
            SELECT n.nspname, c.relname
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = ANY($1::text[]) AND c.relkind IN ('r', 'p')
            """,
            list({s for s, _ in untracked}),
        )
        tgt_existing = {(r["nspname"], r["relname"]) for r in tgt_table_rows}

        created: list[str] = []
        for schema, table in untracked:
            if schema not in tgt_schemas:
                await tgt.execute(f"CREATE SCHEMA IF NOT EXISTS {qi(schema)};")
                tgt_schemas.add(schema)
            if (schema, table) not in tgt_existing:
                try:
                    table_ddl = await generate_full_table_ddl(src, schema, table)
                    await tgt.execute(table_ddl)
                    created.append(f"{schema}.{table}")
                except Exception as exc:
                    log.warning(
                        "sync_new_tables [%s]: could not create %s.%s on target — %s. "
                        "Its tablesync will keep failing until this is fixed — inspect "
                        "and run 'detect-ddl --apply' once resolved.",
                        dbname, schema, table, exc,
                    )

        if created:
            actions.append(f"Created {len(created)} new table(s) on target: {', '.join(created)}")
            log.info("sync_new_tables [%s]: created new table(s) on target: %s", dbname, created)

    try:
        await refresh_subscription(cfg, dbname, copy_data=True)
        actions.append(
            f"Refreshed subscription {sub} — PostgreSQL will tablesync "
            f"{len(untracked)} newly published table(s)"
        )
        log.info(
            "sync_new_tables [%s]: refreshed subscription %s for %d new table(s): %s",
            dbname, sub, len(untracked), untracked,
        )
    except Exception as exc:
        log.warning(
            "sync_new_tables [%s]: could not refresh subscription %s — %s. Will "
            "retry on the next loop iteration.",
            dbname, sub, exc,
        )

    return actions


async def run_new_table_sync_loop(cfg: ReplicatorConfig, dbname: str) -> None:
    """Continuously pick up newly created source tables at the configured
    interval — the counterpart of :func:`sync_new_tables` for ``--loop``."""
    interval = cfg.sequence_sync_interval
    log.info("Starting new-table sync loop for %s (every %ds)", dbname, interval)
    while True:
        try:
            actions = await sync_new_tables(cfg, dbname)
            for action in actions:
                log.info("[%s] %s", dbname, action)
        except Exception as exc:
            log.error("New-table sync error for %s: %s", dbname, exc)
        await asyncio.sleep(interval)


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
        async with connect(cfg.source, dbname) as conn:
            # cfg.schemas may be empty (auto-discover mode) — an empty list
            # would render "FOR TABLES IN SCHEMA ;", a syntax error, exactly
            # in the disaster-recovery path.  Resolve the actual schema list
            # the same way bootstrap does.
            schemas = await discover_schemas(conn, cfg)
            await _create_publication_on(conn, pub, schemas, dbname)
        actions.append(f"Recreated publication '{pub}' on source")
        log.info("reinit_sync [%s]: recreated publication %s", dbname, pub)
    else:
        log.debug("reinit_sync [%s]: publication %s OK", dbname, pub)

    # ── 2. Replication slot ───────────────────────────────────────────────────
    async with connect(cfg.source, dbname) as conn:
        slot_row = await conn.fetchrow(
            """
            SELECT slot_name, active, active_pid, wal_status, confirmed_flush_lsn
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
        # (slot_name = NONE detaches it first, so a surviving slot on the
        # source is NOT dropped along with it.)
        if sub_row is not None:
            async with connect(cfg.target, dbname) as conn:
                await conn.execute(f"ALTER SUBSCRIPTION {qi(sub)} DISABLE;")
                await conn.execute(f"ALTER SUBSCRIPTION {qi(sub)} SET (slot_name = NONE);")
                await conn.execute(f"DROP SUBSCRIPTION IF EXISTS {qi(sub)};")
            log.info("reinit_sync [%s]: dropped broken subscription %s", dbname, sub)

        if slot_ok and slot_row is not None:
            # The slot survived (only the subscription is gone/broken).
            # Attach a new subscription to it instead of dropping it: the
            # slot has retained WAL since its last confirmed LSN, so
            # replication resumes from there with NO data gap.  (A small
            # overlap right at the boundary is possible — the old
            # subscription may have applied slightly past the last
            # *confirmed* flush — which surfaces as visible duplicate-key
            # apply errors rather than silent loss; resolve those with
            # ALTER SUBSCRIPTION ... SKIP.)
            await create_subscription(cfg, dbname, create_slot=False)
            actions.append(
                f"Recreated subscription '{sub}' attached to the surviving "
                f"replication slot (resumes from {slot_row['confirmed_flush_lsn']} "
                f"— no data gap)"
            )
            log.info(
                "reinit_sync [%s]: recreated subscription %s attached to the "
                "surviving slot (resumes at %s)",
                dbname, sub, slot_row["confirmed_flush_lsn"],
            )
        else:
            # ── DATA-LOSS WINDOW ──────────────────────────────────────────
            # The old slot is gone or its WAL was already recycled — the
            # usual outcome of a Patroni promotion, since logical slots do
            # not survive failover before PostgreSQL 17 failover slots.  A
            # fresh slot only streams changes from the moment it is created:
            # everything committed on the source between the old slot's last
            # confirmed LSN and now was never streamed and will NOT be
            # replayed.  That gap cannot be repaired here without a re-copy
            # — make it impossible to miss.
            old_flush = slot_row["confirmed_flush_lsn"] if slot_row else None
            gap_start = (
                f"the old slot's last confirmed LSN ({old_flush})"
                if old_flush is not None
                else "an unknown point (the old slot is gone entirely)"
            )
            log.warning(
                "reinit_sync [%s]: recreating subscription '%s' with a FRESH "
                "replication slot. The new slot starts at the CURRENT WAL "
                "position — any write committed on the source between %s and "
                "now was never streamed and is PERMANENTLY MISSING on the "
                "target. Verify data consistency (row counts / checksums on "
                "recently-written tables) before trusting this target for "
                "cutover, and re-copy affected tables if needed. On "
                "PostgreSQL 17+ consider failover slots "
                "(sync_replication_slots = on) so a switchover no longer "
                "loses the slot.",
                dbname, sub, gap_start,
            )
            issues.append(
                "DATA-LOSS WINDOW: the fresh slot starts at the current LSN — "
                f"writes since {gap_start} were never streamed to the target. "
                "Verify affected tables (row counts / checksums) and re-copy "
                "them if needed before cutover."
            )

            # Drop the dead/lost slot that might still linger on source.
            async with connect(cfg.source, dbname) as conn:
                if await _drop_slot_if_present(conn, sub):
                    log.info("reinit_sync [%s]: dropped orphaned slot %s", dbname, sub)

            # Create a fresh subscription — PostgreSQL will also create the
            # slot.  (reinit-sync never re-copies data, so there is no
            # snapshot/slot consistency window to worry about here — unlike
            # bootstrap — but see the data-gap warning above.)
            await create_subscription(cfg, dbname, create_slot=True)
            actions.append(
                f"Recreated subscription '{sub}' with a fresh replication slot "
                f"(⚠ data gap — see issues)"
            )
            log.info("reinit_sync [%s]: recreated subscription %s", dbname, sub)

    return {
        "database": dbname,
        "issues_found": issues,
        "actions_taken": actions,
        "was_healthy": len(issues) == 0,
    }
