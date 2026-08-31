"""Waiting for logical replication to actually converge.

Every "is the target consistent?" assertion in the suite has to be made at a
point where the answer is meaningful.  Polling a row count until it looks
right is the tempting shortcut and the wrong one: it turns a real data-loss
bug into a flaky test, because the count can be right by coincidence while the
stream is still behind.

The wait here is LSN-based instead.  Writes stop, the source's current WAL
position is captured, and the target is polled until its apply worker reports
having flushed *past that position*.  At that point PostgreSQL itself is
asserting that everything committed before the mark has been applied — so a
subsequent mismatch is a genuine inconsistency, not a race.
"""

from __future__ import annotations

import asyncio

from pg_emigrant.config import ReplicatorConfig
from pg_emigrant.db import connect
from pg_emigrant.replication import sub_name


class ReplicationTimeout(AssertionError):
    """Replication did not reach the requested LSN in time."""


async def wait_for_catchup(
    cfg: ReplicatorConfig, dbname: str, *, timeout: float = 60.0
) -> str:
    """Block until the target has applied everything committed on the source.

    Returns the source LSN that was reached.
    """
    sub = sub_name(cfg, dbname)
    async with connect(cfg.source, dbname) as src:
        # The mark is a non-transactional logical message, and its own LSN is
        # the return value.  Two cheaper-looking marks are both wrong here:
        # pg_current_wal_lsn() is the *write* pointer, which under
        # synchronous_commit=off can still be behind a transaction that has
        # already committed — waiting on it returns before the change is even
        # decodable.  pg_current_wal_insert_lsn() fixes that but still needs
        # decodable WAL to follow it.  A logical message is written after every
        # preceding commit and is itself decodable, so "the slot confirmed
        # past this message" means exactly "everything committed before it has
        # been applied".
        mark = await src.fetchval(
            "SELECT pg_logical_emit_message(false, 'pgem_test', 'catchup-mark')"
        )

    deadline = asyncio.get_running_loop().time() + timeout
    last_seen = None
    while asyncio.get_running_loop().time() < deadline:
        # confirmed_flush_lsn is the authoritative signal, and the only one
        # that means "the subscriber has durably applied everything up to
        # here": the source advances it solely on the subscriber's own
        # feedback.  pg_stat_subscription.latest_end_lsn is NOT a substitute —
        # it is the position the sender last reported, which keepalives push
        # forward independently of what the apply worker has actually done, so
        # waiting on it returns while changes are still pending and turns a
        # real divergence into a passing test.
        async with connect(cfg.source, dbname) as src:
            row = await src.fetchrow(
                "SELECT confirmed_flush_lsn,"
                "       confirmed_flush_lsn >= $2::pg_lsn AS caught_up"
                " FROM pg_replication_slots WHERE slot_name = $1",
                sub, mark,
            )
            if row is not None:
                last_seen = row["confirmed_flush_lsn"]
            # Logical decoding only advances while there is something to
            # decode; an idle source would otherwise park the slot short of
            # the mark forever and time out a perfectly healthy system.
            await src.fetchval("SELECT pg_logical_emit_message(true, 'pgem_test', 'tick')")

        async with connect(cfg.target, dbname) as tgt:
            tablesync_pending = await tgt.fetchval(
                "SELECT count(*) FROM pg_subscription_rel sr"
                " JOIN pg_subscription s ON s.oid = sr.srsubid"
                " WHERE s.subname = $1 AND sr.srsubstate <> 'r'",
                sub,
            )

        if row is not None and row["caught_up"] and not tablesync_pending:
            return str(mark)
        await asyncio.sleep(0.2)

    raise ReplicationTimeout(
        f"subscription {sub} did not reach source LSN {mark} within {timeout}s "
        f"(slot confirmed_flush_lsn={last_seen})"
    )


async def subscription_is_streaming(cfg: ReplicatorConfig, dbname: str) -> bool:
    async with connect(cfg.target, dbname) as tgt:
        return bool(await tgt.fetchval(
            "SELECT 1 FROM pg_stat_subscription WHERE subname = $1"
            " AND relid IS NULL AND pid IS NOT NULL",
            sub_name(cfg, dbname),
        ))


async def slot_row(cfg: ReplicatorConfig, dbname: str, slot: str | None = None) -> dict | None:
    async with connect(cfg.source, dbname) as src:
        row = await src.fetchrow(
            "SELECT slot_name, active, wal_status, confirmed_flush_lsn, restart_lsn,"
            " plugin, database FROM pg_replication_slots WHERE slot_name = $1",
            slot or sub_name(cfg, dbname),
        )
    return dict(row) if row else None


async def all_slots(cfg: ReplicatorConfig) -> list[str]:
    async with connect(cfg.source) as src:
        return [r["slot_name"] for r in
                await src.fetch("SELECT slot_name FROM pg_replication_slots ORDER BY 1")]


async def all_publications(cfg: ReplicatorConfig, dbname: str) -> list[str]:
    async with connect(cfg.source, dbname) as src:
        return [r["pubname"] for r in
                await src.fetch("SELECT pubname FROM pg_publication ORDER BY 1")]


async def all_subscriptions(cfg: ReplicatorConfig, dbname: str) -> list[str]:
    async with connect(cfg.target, dbname) as tgt:
        return [r["subname"] for r in await tgt.fetch(
            "SELECT subname FROM pg_subscription WHERE subdbid ="
            " (SELECT oid FROM pg_database WHERE datname = current_database()) ORDER BY 1"
        )]
