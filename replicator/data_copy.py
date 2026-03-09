"""Efficient initial data copy using PostgreSQL COPY protocol.

Uses asyncpg's copy_from_table / copy_to_table for streaming binary copies.
Supports parallel workers and snapshot-consistent reads.
"""

from __future__ import annotations

import asyncio
import io
from typing import Sequence

import asyncpg

from replicator.config import ReplicatorConfig
from replicator.db import connect
from replicator.utils import get_logger, qi, qt

log = get_logger(__name__)


async def copy_table_data(
    source_cfg,
    target_cfg,
    dbname: str,
    schema: str,
    table: str,
    snapshot_id: str | None = None,
) -> int:
    """Copy all rows from a single table using COPY protocol.

    If *snapshot_id* is given the source transaction is set to use that
    snapshot for a consistent read across tables.

    Returns the number of rows copied.
    """
    fqn = qt(schema, table)

    async with connect(source_cfg, dbname) as src, connect(target_cfg, dbname) as tgt:
        # Set up snapshot isolation on source
        await src.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
        if snapshot_id:
            await src.execute(f"SET TRANSACTION SNAPSHOT '{snapshot_id}';")

        # Truncate target table to ensure idempotency
        await tgt.execute(f"TRUNCATE TABLE {fqn} CASCADE;")

        # Stream data via COPY
        buf = io.BytesIO()
        await src.copy_from_table(
            table, schema_name=schema, output=buf, format="binary"
        )
        data = buf.getvalue()
        if data:
            await tgt.copy_to_table(
                table, schema_name=schema, source=io.BytesIO(data), format="binary"
            )

        row_count = await tgt.fetchval(f"SELECT count(*) FROM {fqn};")
        await src.execute("COMMIT;")

    log.info("Copied %s rows to %s", row_count, fqn)
    return row_count


async def copy_table_data_pipe(
    source_cfg,
    target_cfg,
    dbname: str,
    schema: str,
    table: str,
    snapshot_id: str | None = None,
) -> int:
    """Copy data using an intermediate bytes buffer (for cross-version compat).

    This approach serializes via CSV text format to avoid binary format
    incompatibilities between different major PostgreSQL versions.
    """
    fqn = qt(schema, table)

    async with connect(source_cfg, dbname) as src, connect(target_cfg, dbname) as tgt:
        await src.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
        if snapshot_id:
            await src.execute(f"SET TRANSACTION SNAPSHOT '{snapshot_id}';")

        # Disable FK trigger checks for this session so parallel workers
        # don't fail due to parent tables not yet being loaded.
        await tgt.execute("SET session_replication_role = 'replica';")

        # Resolve the column intersection between source and target so that
        # extra columns present only on the source (e.g. due to schema version
        # drift) don't cause "extra data after last expected column" errors.
        # Generated columns (attgenerated = 's') are excluded from both sides
        # because they are computed by the server and cannot be written via COPY.
        _col_query = """
            SELECT a.attname AS column_name
            FROM pg_attribute a
            WHERE a.attrelid = (quote_ident($1) || '.' || quote_ident($2))::regclass
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND a.attgenerated = ''
            ORDER BY a.attnum
        """
        src_cols = {r["column_name"] for r in await src.fetch(_col_query, schema, table)}
        tgt_col_rows = await tgt.fetch(_col_query, schema, table)
        # Preserve target column order; skip columns absent from source.
        common_columns = [r["column_name"] for r in tgt_col_rows if r["column_name"] in src_cols]

        cols_select = ", ".join(qi(c) for c in common_columns)

        # Use CSV text format for cross-version safety
        buf = io.BytesIO()
        await src.copy_from_query(
            f"SELECT {cols_select} FROM {fqn}",
            output=buf,
            format="csv",
        )

        data = buf.getvalue()
        if data:
            await tgt.copy_to_table(
                table,
                schema_name=schema,
                source=io.BytesIO(data),
                format="csv",
                columns=common_columns,
            )

        row_count = await tgt.fetchval(f"SELECT count(*) FROM {fqn};")
        await src.execute("COMMIT;")

    log.info("Copied %s rows to %s", row_count, fqn)
    return row_count


async def export_snapshot(source_cfg, dbname: str) -> tuple[asyncpg.Connection, str]:
    """Open a REPEATABLE READ transaction and export its snapshot ID.

    The caller must keep the returned connection open until all workers
    have finished using the snapshot, then COMMIT and close it.
    """
    from replicator.db import _dsn

    conn = await asyncpg.connect(_dsn(source_cfg, dbname))
    await conn.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
    snapshot_id = await conn.fetchval("SELECT pg_export_snapshot();")
    log.info("Exported snapshot %s for database %s", snapshot_id, dbname)
    return conn, snapshot_id


async def copy_all_tables(
    cfg: ReplicatorConfig,
    dbname: str,
    tables: list[dict],
    parallel: int | None = None,
) -> dict[str, int]:
    """Copy data for all tables with parallelism, using a shared snapshot.

    *tables* is a list of dicts with 'schema_name' and 'table_name' keys
    (as returned by ``schema_sync.get_tables``).

    Returns a dict mapping ``schema.table`` to row count.
    """
    workers = parallel or cfg.parallel_workers
    results: dict[str, int] = {}

    if not tables:
        log.info("No tables to copy for database %s", dbname)
        return results

    # Truncate all target tables in one shot with CASCADE to avoid
    # deadlocks and FK-reference errors from parallel per-table TRUNCATEs.
    async with connect(cfg.target, dbname) as tgt_conn:
        table_list = ", ".join(
            f"{qi(t['schema_name'])}.{qi(t['table_name'])}" for t in tables
        )
        await tgt_conn.execute(f"TRUNCATE {table_list} CASCADE;")

    # Export a snapshot so all workers see the same data
    holder_conn, snapshot_id = await export_snapshot(cfg.source, dbname)

    sem = asyncio.Semaphore(workers)

    async def _copy_one(schema: str, table: str) -> tuple[str, int]:
        key = f"{schema}.{table}"
        async with sem:
            try:
                count = await copy_table_data_pipe(
                    cfg.source, cfg.target, dbname, schema, table, snapshot_id
                )
                return key, count
            except Exception as exc:
                log.error("Failed to copy %s: %s", key, exc)
                return key, -1

    tasks = [
        _copy_one(t["schema_name"], t["table_name"])
        for t in tables
    ]
    done = await asyncio.gather(*tasks)

    for key, count in done:
        results[key] = count

    # Release the snapshot holder
    await holder_conn.execute("COMMIT;")
    await holder_conn.close()

    total = sum(c for c in results.values() if c >= 0)
    log.info(
        "Data copy complete for %s: %d tables, %d total rows",
        dbname, len(results), total,
    )
    return results
