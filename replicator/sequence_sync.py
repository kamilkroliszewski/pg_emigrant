"""Sequence synchronization — keeps target sequences ahead of source.

Logical replication does not replicate sequence advances.  This module
periodically reads sequence values from the source and applies them to
the target, ensuring values only move forward.
"""

from __future__ import annotations

import asyncio

from replicator.config import ReplicatorConfig
from replicator.db import connect
from replicator.schema_sync import get_sequences
from replicator.utils import get_logger, qt

log = get_logger(__name__)

# Batch query — reads last_value + start_value for all sequences in given schemas.
# pg_sequences.last_value is NULL when the sequence has never been called
# (equivalent to is_called=false).  start_value is needed in that case.
_BATCH_SEQ_VALUES_SQL = """
SELECT schemaname, sequencename, last_value, start_value
FROM pg_sequences
WHERE schemaname = ANY($1::text[])
ORDER BY schemaname, sequencename;
"""


def _effective_value(row) -> tuple[int, bool]:
    """Return (last_value, is_called) from a pg_sequences row."""
    if row["last_value"] is None:
        # Never called — next nextval() will return start_value
        return row["start_value"], False
    return row["last_value"], True


async def sync_sequences_once(
    cfg: ReplicatorConfig,
    dbname: str,
) -> list[dict]:
    """Synchronize all sequences once.  Returns a report of changes.

    Uses two batch SELECTs (one per server) and a single DO-block to apply
    all setval() calls, reducing round-trips from O(N) to O(1) per server.
    """
    async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
        sequences = await get_sequences(src, cfg.schemas)
        if not sequences:
            return []

        schemas = list({s["schema_name"] for s in sequences})

        # --- batch read ---------------------------------------------------
        src_rows, tgt_rows = await asyncio.gather(
            src.fetch(_BATCH_SEQ_VALUES_SQL, schemas),
            tgt.fetch(_BATCH_SEQ_VALUES_SQL, schemas),
        )
        src_map = {(r["schemaname"], r["sequencename"]): r for r in src_rows}
        tgt_map = {(r["schemaname"], r["sequencename"]): r for r in tgt_rows}

        # --- compute updates ----------------------------------------------
        report: list[dict] = []
        updates: list[tuple[str, str, int, bool]] = []  # (schema, name, value, is_called)

        for seq in sequences:
            schema = seq["schema_name"]
            name = seq["sequence_name"]
            fqn = qt(schema, name)

            src_row = src_map.get((schema, name))
            tgt_row = tgt_map.get((schema, name))

            if src_row is None:
                log.warning("Cannot read source sequence %s: not found in pg_sequences", fqn)
                continue
            if tgt_row is None:
                log.warning("Sequence %s not found on target, skipping", fqn)
                continue

            src_last, src_is_called = _effective_value(src_row)
            tgt_last, _ = _effective_value(tgt_row)

            status = "ok"
            if src_last > tgt_last:
                updates.append((schema, name, src_last, src_is_called))
                status = "updated"
                log.info("Sequence %s [db: %s]: %d → %d", fqn, dbname, tgt_last, src_last)
            elif src_last < tgt_last:
                status = "target_ahead"
                log.debug("Sequence %s: target %d > source %d, skipping", fqn, tgt_last, src_last)

            report.append({
                "schema": schema,
                "sequence": name,
                "source_value": src_last,
                "target_value": tgt_last,
                "status": status,
            })

        # --- batch write (single round-trip) ------------------------------
        if updates:
            calls = "\n  ".join(
                f"PERFORM setval({repr(f'{schema}.{name}')}, {value}, {str(is_called).lower()});"
                for schema, name, value, is_called in updates
            )
            do_block = f"DO $$\nBEGIN\n  {calls}\nEND;\n$$;"
            await tgt.execute(do_block)

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
