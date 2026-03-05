"""Sequence synchronization — keeps target sequences ahead of source.

Logical replication does not replicate sequence advances.  This module
periodically reads sequence values from the source and applies them to
the target, ensuring values only move forward.
"""

from __future__ import annotations

import asyncio

from replicator.config import ReplicatorConfig
from replicator.db import connect
from replicator.schema_sync import get_sequences, get_sequence_value
from replicator.utils import get_logger, qt

log = get_logger(__name__)


async def sync_sequences_once(
    cfg: ReplicatorConfig,
    dbname: str,
) -> list[dict]:
    """Synchronize all sequences once.  Returns a report of changes."""
    report: list[dict] = []

    async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
        sequences = await get_sequences(src, cfg.schemas)

        for seq in sequences:
            schema = seq["schema_name"]
            name = seq["sequence_name"]
            fqn = qt(schema, name)

            try:
                src_val = await get_sequence_value(src, schema, name)
            except Exception as exc:
                log.warning("Cannot read source sequence %s: %s", fqn, exc)
                continue

            try:
                tgt_val = await get_sequence_value(tgt, schema, name)
            except Exception:
                # Sequence might not exist on target yet
                log.warning("Sequence %s not found on target, skipping", fqn)
                continue

            src_last = src_val["last_value"]
            tgt_last = tgt_val["last_value"]

            status = "ok"
            if src_last > tgt_last:
                await tgt.execute(
                    f"SELECT setval('{schema}.{name}', $1, $2);",
                    src_last,
                    src_val["is_called"],
                )
                status = "updated"
                log.info("Sequence %s: %d → %d", fqn, tgt_last, src_last)
            elif src_last < tgt_last:
                # Target is ahead — do not move backwards
                status = "target_ahead"
                log.debug("Sequence %s: target %d > source %d, skipping", fqn, tgt_last, src_last)

            report.append({
                "schema": schema,
                "sequence": name,
                "source_value": src_last,
                "target_value": tgt_last,
                "status": status,
            })

    return report


async def run_sequence_sync_loop(
    cfg: ReplicatorConfig,
    dbname: str,
) -> None:
    """Continuously synchronize sequences at the configured interval."""
    interval = cfg.sequence_sync_interval
    log.info(
        "Starting sequence sync loop for %s (every %ds)", dbname, interval
    )
    while True:
        try:
            await sync_sequences_once(cfg, dbname)
        except Exception as exc:
            log.error("Sequence sync error for %s: %s", dbname, exc)
        await asyncio.sleep(interval)
