"""PostgreSQL logical replication management.

Creates publications on the source and subscriptions on the target.
Provides start / stop / status operations.
"""

from __future__ import annotations

import re

import asyncpg

from replicator.config import ReplicatorConfig
from replicator.db import connect
from replicator.utils import get_logger, qi

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


async def create_publication(
    cfg: ReplicatorConfig,
    dbname: str,
) -> None:
    """Create a publication for all tables in the configured schemas."""
    pub = pub_name(cfg, dbname)
    async with connect(cfg.source, dbname) as conn:
        exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", pub
        )
        if exists:
            log.info("Publication %s already exists in %s", pub, dbname)
            return

        # Publication for all tables in the listed schemas
        schema_list = ", ".join(qi(s) for s in cfg.schemas)
        await conn.execute(
            f"CREATE PUBLICATION {qi(pub)} FOR TABLES IN SCHEMA {schema_list};"
        )
        log.info("Created publication %s in %s for schemas %s", pub, dbname, cfg.schemas)


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
) -> None:
    """Create a subscription on the target pointing to the source publication.

    The subscription is created with ``copy_data = false`` because we
    already performed an initial data copy via COPY.

    ``create_slot = true`` tells PostgreSQL to create the replication slot
    automatically on the source.
    """
    sub = sub_name(cfg, dbname)
    pub = pub_name(cfg, dbname)
    src = cfg.source

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

        conninfo = (
            f"host={src.host} port={src.port} "
            f"user={src.user} password={src.password} "
            f"dbname={dbname}"
        )

        # Drop orphaned replication slot on source if it already exists
        # (can happen when a previous bootstrap was interrupted before teardown).
        async with connect(cfg.source, dbname) as src_conn:
            slot_row = await src_conn.fetchrow(
                "SELECT active, active_pid FROM pg_replication_slots WHERE slot_name = $1",
                sub,
            )
            if slot_row:
                if slot_row["active"] and slot_row["active_pid"]:
                    await src_conn.execute(
                        "SELECT pg_terminate_backend($1);", slot_row["active_pid"]
                    )
                    log.info("Terminated backend PID %s holding slot %s", slot_row["active_pid"], sub)
                await src_conn.execute("SELECT pg_drop_replication_slot($1);", sub)
                log.info("Dropped orphaned replication slot %s on source", sub)

        # create_slot and copy_data are intentionally set this way:
        # - copy_data = false: we already copied data
        # - create_slot = true: let PG manage the slot
        sql = (
            f"CREATE SUBSCRIPTION {qi(sub)} "
            f"CONNECTION '{conninfo}' "
            f"PUBLICATION {qi(pub)} "
            f"WITH (copy_data = false, create_slot = true);"
        )
        await conn.execute(sql)
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
        slot_row = await conn.fetchrow(
            "SELECT active, active_pid FROM pg_replication_slots WHERE slot_name = $1",
            sub,
        )
        if slot_row:
            if slot_row["active"] and slot_row["active_pid"]:
                await conn.execute(
                    "SELECT pg_terminate_backend($1);", slot_row["active_pid"]
                )
            await conn.execute("SELECT pg_drop_replication_slot($1);", sub)
            log.info("Dropped replication slot %s on source", sub)


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
                latest_end_time
            FROM pg_stat_subscription
            WHERE subname = $1;
            """,
            sub,
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
