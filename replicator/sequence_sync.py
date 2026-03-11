"""Sequence synchronization — keeps target sequences ahead of source.

Logical replication does not replicate sequence advances.  This module
periodically reads sequence values from the source and applies them to
the target, ensuring values only move forward.
"""

from __future__ import annotations

import asyncio

from replicator.config import ReplicatorConfig
from replicator.db import connect, discover_schemas
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
    
    Also handles orphaned sequences (exist on target but not on source):
    - If a sequence exists only on target, it attempts to determine its proper
      value by finding the maximum ID in tables that use it as DEFAULT.
    - If unable to find the table, logs a warning for manual intervention.
    """
    async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
        schemas = await discover_schemas(src, cfg)
        sequences = await get_sequences(src, schemas)
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

        # --- guard: skip if target has no sequences yet -------------------
        # An empty tgt_map means the database exists but hasn't been bootstrapped
        # (schemas/tables/sequences not yet created). Writing setval() here would
        # create sequences on a bare database and later cause _seq1 duplicates
        # when bootstrap runs CREATE TABLE with IDENTITY/nextval columns.
        if not tgt_map:
            log.info(
                "[db: %s] Target has no sequences in schemas %s — "
                "database not bootstrapped yet, skipping sync.",
                dbname, schemas,
            )
            return []

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

        # --- Handle orphaned sequences (target-only) ---------------------------
        # These sequences may have been created during migration/bootstrap but
        # don't exist on the source. We need to find their proper values to
        # avoid conflicts with newly inserted rows.
        src_keys = set(src_map.keys())
        tgt_only_keys = [k for k in tgt_map.keys() if k not in src_keys]
        
        if tgt_only_keys:
            log.warning(
                "[db: %s] Found %d sequences on target not found on source: %s. "
                "These should be either removed or have their values synchronized.",
                dbname,
                len(tgt_only_keys),
                ", ".join(f"{k[0]}.{k[1]}" for k in tgt_only_keys),
            )
            
            # Try to find proper values for orphaned sequences
            for schema, seq_name in tgt_only_keys:
                fqn = qt(schema, seq_name)
                tgt_row = tgt_map[(schema, seq_name)]
                tgt_last, _ = _effective_value(tgt_row)
                
                # Try to find which table uses this sequence
                # Query: find all columns that use this sequence in DEFAULT
                find_users_sql = f"""
                SELECT t.tablename, c.attname
                FROM pg_tables t
                JOIN pg_class pc ON pc.relname = t.tablename AND pc.relnamespace = 
                    (SELECT oid FROM pg_namespace WHERE nspname = t.schemaname)
                JOIN pg_attribute c ON c.attrelid = pc.oid
                WHERE pg_get_serial_sequence(
                    quote_ident(t.schemaname) || '.' || quote_ident(t.tablename), 
                    c.attname
                ) = {repr(fqn)}
                LIMIT 1;
                """
                
                try:
                    user_rows = await tgt.fetch(find_users_sql)
                    if user_rows:
                        table_name = user_rows[0]["tablename"]
                        column_name = user_rows[0]["attname"]
                        # Find max value in that column
                        max_id_sql = f"SELECT COALESCE(MAX({qt(column_name)}), 0) as max_id FROM {qt(schema, table_name)}"
                        max_id_row = await tgt.fetchrow(max_id_sql)
                        max_id = max_id_row["max_id"]
                        new_val = max(max_id + 1, tgt_last)
                        
                        if new_val > tgt_last:
                            updates.append((schema, seq_name, new_val, True))
                            log.info(
                                "Sequence %s [db: %s] (orphaned): %d → %d (based on MAX(%s.%s) = %d)",
                                fqn, dbname, tgt_last, new_val, table_name, column_name, max_id
                            )
                            report.append({
                                "schema": schema,
                                "sequence": seq_name,
                                "source_value": None,
                                "target_value": new_val,
                                "status": "orphaned_fixed",
                            })
                        else:
                            log.debug(
                                "Sequence %s (orphaned) on target already at %d, table max id is %d",
                                fqn, tgt_last, max_id
                            )
                    else:
                        # Can't find which table uses this sequence
                        log.warning(
                            "Cannot determine which table uses orphaned sequence %s. "
                            "Please check manually and consider dropping it or recreating the column DEFAULT.",
                            fqn
                        )
                        report.append({
                            "schema": schema,
                            "sequence": seq_name,
                            "source_value": None,
                            "target_value": tgt_last,
                            "status": "orphaned_unknown",
                        })
                except Exception as e:
                    log.error("Error handling orphaned sequence %s: %s", fqn, e)
                    report.append({
                        "schema": schema,
                        "sequence": seq_name,
                        "source_value": None,
                        "target_value": tgt_last,
                        "status": "orphaned_error",
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


async def get_sequence_status(
    cfg: ReplicatorConfig,
    dbname: str,
) -> list[dict]:
    """Read-only comparison of source vs target sequences.  Never writes to target."""
    async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
        schemas = await discover_schemas(src, cfg)
        sequences = await get_sequences(src, schemas)
        if not sequences:
            return []

        schemas = list({s["schema_name"] for s in sequences})

        src_rows, tgt_rows = await asyncio.gather(
            src.fetch(_BATCH_SEQ_VALUES_SQL, schemas),
            tgt.fetch(_BATCH_SEQ_VALUES_SQL, schemas),
        )
        src_map = {(r["schemaname"], r["sequencename"]): r for r in src_rows}
        tgt_map = {(r["schemaname"], r["sequencename"]): r for r in tgt_rows}

        report: list[dict] = []
        for seq in sequences:
            schema = seq["schema_name"]
            name = seq["sequence_name"]

            src_row = src_map.get((schema, name))
            tgt_row = tgt_map.get((schema, name))

            if src_row is None:
                continue

            src_last, _ = _effective_value(src_row)

            if tgt_row is None:
                status = "missing_on_target"
                tgt_last = None
            else:
                tgt_last, _ = _effective_value(tgt_row)
                if src_last > tgt_last:
                    status = "behind"
                elif src_last < tgt_last:
                    status = "target_ahead"
                else:
                    status = "ok"

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
