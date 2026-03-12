"""Efficient initial data copy using PostgreSQL COPY protocol.

Uses asyncpg's copy_from_table / copy_to_table for streaming binary copies.
Supports parallel workers and snapshot-consistent reads.
"""

from __future__ import annotations

import asyncio
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

        # Stream data via COPY using a queue — no in-memory accumulation
        queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=128)

        async def _produce() -> None:
            async def _chunk(data: bytes) -> None:
                await queue.put(data)
            try:
                await src.copy_from_table(
                    table, schema_name=schema, output=_chunk, format="binary"
                )
            finally:
                await queue.put(None)  # sentinel — always sent, even on error

        async def _consume() -> None:
            async def _reader():
                while True:
                    chunk = await queue.get()
                    if chunk is None:
                        return
                    yield chunk
            await tgt.copy_to_table(
                table, schema_name=schema, source=_reader(), format="binary"
            )

        await asyncio.gather(_produce(), _consume())

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
    table_workers: int = 1,
) -> int:
    """Copy data via streaming CSV COPY, optionally splitting into parallel ctid chunks.

    When *table_workers* > 1 the table is divided into page-range slices that
    are streamed concurrently, keeping memory usage flat regardless of table size.
    Cross-version compatibility is maintained by using CSV format.
    """
    fqn = qt(schema, table)

    _col_query = """
        SELECT a.attname AS column_name
        FROM pg_attribute a
        WHERE a.attrelid = (quote_ident($1) || '.' || quote_ident($2))::regclass
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND a.attgenerated = ''
        ORDER BY a.attnum
    """

    # Resolve column intersection and physical page count (catalog queries,
    # no snapshot needed).
    async with connect(source_cfg, dbname) as src, connect(target_cfg, dbname) as tgt:
        src_cols = {r["column_name"] for r in await src.fetch(_col_query, schema, table)}
        relpages: int = (
            await src.fetchval(
                "SELECT relpages FROM pg_class"
                " WHERE oid = (quote_ident($1) || '.' || quote_ident($2))::regclass",
                schema,
                table,
            )
            or 0
        )
        tgt_col_rows = await tgt.fetch(_col_query, schema, table)

    # Preserve target column order; skip columns absent from source.
    common_columns = [r["column_name"] for r in tgt_col_rows if r["column_name"] in src_cols]
    cols_select = ", ".join(qi(c) for c in common_columns)

    async def _stream_query(src_conn, tgt_conn, query: str) -> None:
        """Pipe SELECT query to target COPY via asyncio.Queue — zero RAM accumulation."""
        queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=128)

        async def _produce() -> None:
            async def _cb(data: bytes) -> None:
                await queue.put(data)
            try:
                await src_conn.copy_from_query(query, output=_cb, format="csv")
            finally:
                await queue.put(None)  # sentinel — always sent, even on error

        async def _consume() -> None:
            async def _reader():
                while True:
                    chunk = await queue.get()
                    if chunk is None:
                        return
                    yield chunk
            await tgt_conn.copy_to_table(
                table,
                schema_name=schema,
                source=_reader(),
                format="csv",
                columns=common_columns,
            )

        await asyncio.gather(_produce(), _consume())

    # Use ctid-based chunking when multiple workers are requested and the
    # table has enough pages to make the split worthwhile.
    use_chunks = table_workers > 1 and relpages >= table_workers

    if use_chunks:
        # Divide pages evenly; the last slice has no upper bound so that rows
        # added after the last ANALYZE are not silently skipped.
        chunk_size = max(1, -(-relpages // table_workers))  # ceil division
        ranges = [
            (i * chunk_size, (i + 1) * chunk_size if i < table_workers - 1 else None)
            for i in range(table_workers)
        ]
        log.info(
            "Copying %s in %d chunks (~%d pages each, relpages=%d)",
            fqn, len(ranges), chunk_size, relpages,
        )

        async def _copy_chunk(page_start: int, page_end: int | None) -> None:
            where = f"ctid >= '({page_start},0)'::tid"
            if page_end is not None:
                where += f" AND ctid < '({page_end},0)'::tid"
            query = f"SELECT {cols_select} FROM {fqn} WHERE {where}"

            async with (
                connect(source_cfg, dbname) as src,
                connect(target_cfg, dbname) as tgt,
            ):
                await src.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
                if snapshot_id:
                    await src.execute(f"SET TRANSACTION SNAPSHOT '{snapshot_id}';")
                await tgt.execute("SET session_replication_role = 'replica';")
                await _stream_query(src, tgt, query)
                await src.execute("COMMIT;")

        await asyncio.gather(*[_copy_chunk(s, e) for s, e in ranges])

    else:
        query = f"SELECT {cols_select} FROM {fqn}"
        async with (
            connect(source_cfg, dbname) as src,
            connect(target_cfg, dbname) as tgt,
        ):
            await src.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
            if snapshot_id:
                await src.execute(f"SET TRANSACTION SNAPSHOT '{snapshot_id}';")
            await tgt.execute("SET session_replication_role = 'replica';")
            await _stream_query(src, tgt, query)
            await src.execute("COMMIT;")

    async with connect(target_cfg, dbname) as tgt:
        row_count: int = await tgt.fetchval(f"SELECT count(*) FROM {fqn};")

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
                    cfg.source, cfg.target, dbname, schema, table, snapshot_id,
                    table_workers=cfg.table_parallel_workers,
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
