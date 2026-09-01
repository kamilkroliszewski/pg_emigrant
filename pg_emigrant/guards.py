"""Checks that must hold before anything is written, on every path.

The read-only ``preflight`` command already covers these and more, but it is
skippable (``--skip-preflight``) and it is a *CLI* step — the library entry
points, and the web GUI that calls them, do not run it.  The invariants here
are the ones whose violation is unrecoverable, so they are enforced where the
mutation happens rather than where the command line is parsed.
"""

from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from typing import AsyncIterator

from pg_emigrant.config import ReplicatorConfig
from pg_emigrant.db import connect
from pg_emigrant.utils import get_logger

log = get_logger(__name__)


class UnsafeOperation(Exception):
    """A precondition that makes the operation unsafe to attempt at all."""


class ConcurrentMigration(UnsafeOperation):
    """Another pg_emigrant run is already migrating this database."""


async def assert_distinct_clusters(cfg: ReplicatorConfig) -> None:
    """Refuse to migrate a cluster into itself, or into its own replica.

    ``system_identifier`` is stamped in at initdb time and inherited by every
    physical replica, so a match means either literally the same server or a
    primary/standby pair.  Comparing host and port cannot establish this:
    the same cluster is reachable under a VIP, a pooler, a DNS alias, a second
    listen address, or simply a second port — every one of which looks like a
    different endpoint and is not.

    Getting this wrong is not a failed migration but a destroyed source.  The
    initial copy TRUNCATEs its target tables; if "target" is the source, that
    truncate lands on production data, and it happens before anything else
    would have had a chance to notice.  So this runs on every mutating path,
    is not skippable, and treats an unreadable identifier as a reason to stop
    rather than a reason to assume the best.
    """
    async with connect(cfg.source) as src, connect(cfg.target) as tgt:
        src_id = await _system_identifier(src)
        tgt_id = await _system_identifier(tgt)

    if src_id is None or tgt_id is None:
        # A host:port comparison is a weak substitute, but it is the only
        # thing left, and the common copy-paste mistake it does catch is worth
        # catching.  The refusal below is what keeps this fail-closed.
        same_endpoint = (
            cfg.source.host.strip().lower() == cfg.target.host.strip().lower()
            and int(cfg.source.port) == int(cfg.target.port)
        )
        if same_endpoint:
            raise UnsafeOperation(
                f"source and target are the same endpoint "
                f"({cfg.source.host}:{cfg.source.port}). A migration cannot "
                f"read from and write to the same cluster."
            )
        raise UnsafeOperation(
            "cannot read system_identifier from "
            + ("the source" if src_id is None else "the target")
            + ", so it is impossible to prove that source and target are "
              "different clusters. Migrating into the source itself would "
              "TRUNCATE production tables during the initial copy, so this is "
              "refused rather than assumed safe. Grant the migration role "
              "EXECUTE ON FUNCTION pg_control_system() on both sides (or use a "
              "superuser role) and retry."
        )

    if src_id == tgt_id:
        raise UnsafeOperation(
            f"source and target are the SAME PostgreSQL cluster "
            f"(system_identifier={src_id}), or one is a physical replica of the "
            f"other — host:port differing means nothing here, since the same "
            f"cluster is reachable under a VIP, a pooler, an alias or a second "
            f"port. The initial copy TRUNCATEs its target tables, so continuing "
            f"would destroy production data. Point 'target' at an independent "
            f"cluster."
        )

    log.debug("Cluster identity check passed: source %s != target %s", src_id, tgt_id)


async def _system_identifier(conn) -> int | None:
    try:
        return await conn.fetchval("SELECT system_identifier FROM pg_control_system()")
    except Exception as exc:  # insufficient privilege, mostly
        log.debug("Could not read system_identifier: %s", exc)
        return None


def _lock_key(slot_name: str) -> tuple[int, int]:
    """A stable ``(classid, objid)`` advisory-lock key for *slot_name*.

    The two-``int4`` form of ``pg_try_advisory_lock`` rather than the ``bigint``
    one, so the key lands in ``pg_locks.classid``/``objid`` as two plain
    columns and the "who holds it?" lookup below is an equality test instead of
    a bit-shift reconstruction that gets the sign wrong half the time.

    Derived in Python, not with the server's ``hashtext()``: that function is an
    implementation detail whose result has changed between major versions, and
    a key that differed between a PostgreSQL 14 source and a 17 one would
    silently stop two runs from excluding each other.
    """
    digest = hashlib.sha256(f"pg_emigrant:{slot_name}".encode()).digest()
    hi = int.from_bytes(digest[0:4], "big", signed=True)
    lo = int.from_bytes(digest[4:8], "big", signed=True)
    return hi, lo


@asynccontextmanager
async def migration_lock(
    cfg: ReplicatorConfig, dbname: str, slot_name: str
) -> AsyncIterator[None]:
    """Hold an exclusive claim on this database's migration for the whole run.

    Two ``bootstrap`` processes started against the same configuration used to
    silently destroy each other's work, and the loser reported success.  The
    window is the entire post-copy half of the pipeline — deferred indexes,
    foreign keys, views, triggers, ownership, privileges, sequences — which is
    minutes on a real database.  During it the first run's replication slot
    exists but is *inactive*: its snapshot connection has been released and its
    subscription does not exist yet.

    A second run arriving then sees no subscription (so the already-replicating
    guard does not fire) and an inactive slot (so the "never steal a live slot"
    guard does not fire either).  It drops that slot as an orphan, creates a
    fresh one at a later LSN, and TRUNCATEs the target.  The first run then
    attaches its subscription to a slot that starts *after* the snapshot its
    own copy used — so every transaction committed in between is in neither the
    copy nor the WAL stream — and it exits 0.  Reproduced directly.

    Neither existing guard can close this: both ask about server state, and the
    server state during that window is genuinely indistinguishable from the
    debris of a killed run — which the next run is *supposed* to adopt.  The
    missing fact is whether a live process still owns it, and that is what a
    session advisory lock records.

    Taken on the **source**, in *dbname*, keyed by the slot name: the slot is
    the contended object, and its name is what two runs configured alike
    collide on.  Session-scoped, and released by closing the connection rather
    than by an explicit ``pg_advisory_unlock`` — the unlock would be one more
    ``await`` to get through while unwinding from a cancellation, for a lock
    the closing connection drops anyway.  That also means the claim dies with
    the process: a SIGKILLed run's debris stays adoptable by the next run
    instead of being locked out forever.
    """
    hi, lo = _lock_key(slot_name)
    async with connect(cfg.source, dbname) as conn:
        if not await conn.fetchval(
            "SELECT pg_try_advisory_lock($1::int, $2::int)", hi, lo
        ):
            # Best-effort diagnostic only.  pg_locks stores the two halves as
            # oid, i.e. unsigned, so they are compared as bigints against the
            # unsigned reinterpretation of the signed int4s above — and the
            # whole lookup is guarded, because a refusal that turned itself
            # into a generic failure would send the caller down the rollback
            # path and drop the very slot this lock exists to protect.
            where = ""
            try:
                holder = await conn.fetchrow(
                    "SELECT a.pid, a.client_addr::text AS client_addr,"
                    "       EXTRACT(EPOCH FROM (now() - a.backend_start))::bigint AS age"
                    " FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid"
                    " WHERE l.locktype = 'advisory' AND l.granted"
                    "   AND l.classid::bigint = $1 AND l.objid::bigint = $2"
                    " LIMIT 1",
                    hi & 0xFFFFFFFF, lo & 0xFFFFFFFF,
                )
                if holder is not None:
                    where = (
                        f" (source backend pid {holder['pid']}, from "
                        f"{holder['client_addr'] or 'a local socket'}, "
                        f"{holder['age']}s old)"
                    )
            except Exception as exc:
                log.debug("Could not identify the lock holder: %s", exc)
            raise ConcurrentMigration(
                f"another pg_emigrant run is already migrating {dbname!r}"
                f"{where}. Two runs of the same configuration destroy each "
                f"other's work: the second drops the first's replication slot "
                f"while it is briefly inactive, and the first then replicates "
                f"from a slot that starts after its own copy — losing every "
                f"transaction in between while reporting success. Nothing was "
                f"changed. Wait for that run to finish, or stop it and re-run."
            )
        log.debug("Holding the migration lock for %s (key %d/%d)", dbname, hi, lo)
        yield
