"""Efficient initial data copy using PostgreSQL COPY protocol.

Uses asyncpg's copy_from_table / copy_to_table for streaming binary copies.
Supports parallel workers and snapshot-consistent reads.
"""

from __future__ import annotations

import asyncio
from typing import Callable, Sequence

from pg_emigrant.config import ReplicatorConfig
from pg_emigrant.db import connect
from pg_emigrant.utils import get_logger, qi, qt

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
        """Pipe SELECT query to target COPY via asyncio.Queue — zero RAM accumulation.

        When the producer fails mid-stream the consumer raises the same exception
        inside the COPY source generator.  asyncpg forwards it as a CopyFail to
        PostgreSQL, which rolls back the entire COPY — no partial data is committed.
        """
        queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=128)
        _produce_exc: BaseException | None = None

        async def _produce() -> None:
            nonlocal _produce_exc
            async def _cb(data: bytes) -> None:
                await queue.put(data)
            try:
                await src_conn.copy_from_query(query, output=_cb, format="csv")
            except BaseException as exc:
                _produce_exc = exc
                raise
            finally:
                await queue.put(None)  # sentinel — always sent, even on error

        async def _consume() -> None:
            async def _reader():
                while True:
                    chunk = await queue.get()
                    if chunk is None:
                        if _produce_exc is not None:
                            raise _produce_exc  # abort COPY — rolls back partial data
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
            # ONLY: a classic-inheritance parent would otherwise also return its
            # children's rows, duplicating them (children are copied separately).
            query = f"SELECT {cols_select} FROM ONLY {fqn} WHERE {where}"

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
        # ONLY: see the chunked variant above — prevents duplicating rows of
        # classic-inheritance children through their parent.
        query = f"SELECT {cols_select} FROM ONLY {fqn}"
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
        row_count: int = await tgt.fetchval(f"SELECT count(*) FROM ONLY {fqn};")

    log.info("Copied %s rows to %s", row_count, fqn)
    return row_count


async def copy_all_tables(
    cfg: ReplicatorConfig,
    dbname: str,
    tables: list[dict],
    snapshot_id: str,
    parallel: int | None = None,
    on_table_start: Callable[[str], None] | None = None,
    on_table_done: Callable[[str, int], None] | None = None,
) -> dict[str, int]:
    """Copy data for all tables with parallelism, using a shared snapshot.

    *tables* is a list of dicts with 'schema_name' and 'table_name' keys
    (as returned by ``schema_sync.get_tables``).

    *snapshot_id* MUST be the snapshot exported by
    ``replication.create_replication_slot_with_snapshot`` — copying with the
    exact snapshot the replication slot started from is what makes the data
    copy and the start of WAL streaming a single consistent point, with no gap
    in which a concurrent write could be lost.  The caller is responsible for
    keeping that snapshot's holder connection open for the duration of this
    call (this function only *uses* the snapshot id; it does not export or
    manage its lifetime).

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

    sem = asyncio.Semaphore(workers)

    async def _copy_one(schema: str, table: str) -> tuple[str, int]:
        key = f"{schema}.{table}"
        async with sem:
            if on_table_start:
                on_table_start(key)
            try:
                count = await copy_table_data_pipe(
                    cfg.source, cfg.target, dbname, schema, table, snapshot_id,
                    table_workers=cfg.table_parallel_workers,
                )
                if on_table_done:
                    on_table_done(key, count)
                return key, count
            except Exception as exc:
                log.error("Failed to copy %s: %s", key, exc)
                if on_table_done:
                    on_table_done(key, -1)
                return key, -1

    tasks = [
        _copy_one(t["schema_name"], t["table_name"])
        for t in tables
    ]
    done = await asyncio.gather(*tasks)

    for key, count in done:
        results[key] = count

    failed = [key for key, count in results.items() if count < 0]
    total = sum(c for c in results.values() if c >= 0)
    if failed:
        log.error(
            "Data copy INCOMPLETE for %s — %d table(s) failed to copy: %s. "
            "Logical replication will NOT backfill the missing rows. "
            "Bootstrap aborts for this database before any replication objects "
            "are created — fix the cause and re-run bootstrap.",
            dbname, len(failed), ", ".join(sorted(failed)),
        )
    log.info(
        "Data copy complete for %s: %d tables, %d total rows",
        dbname, len(results), total,
    )
    return results


async def verify_copy_counts(
    cfg: ReplicatorConfig,
    dbname: str,
    tables: list[dict],
    snapshot_id: str,
    target_counts: dict[str, int],
) -> dict[str, tuple[int, int]]:
    """Cross-check source row counts against what COPY reported on the target.

    Source counts are read under *snapshot_id* — the SAME snapshot the copy
    itself used — so both sides are counted at the identical, frozen point in
    time. This makes the comparison exact, not a race with concurrent writes
    (those are handled separately: writes committed after the slot/snapshot
    was taken arrive later over the replication stream). Any mismatch found
    here is therefore a genuine bug in the copy path itself, not a timing
    artifact — e.g. a driver quirk silently dropping or duplicating rows.

    Call this BEFORE releasing the snapshot (before closing the
    ``SlotSnapshot`` returned by ``create_replication_slot_with_snapshot``).

    Returns only the tables that don't match, as
    ``{"schema.table": (source_count, target_count)}``; an empty dict means
    every table matched exactly.
    """
    mismatches: dict[str, tuple[int, int]] = {}
    async with connect(cfg.source, dbname) as src:
        await src.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
        await src.execute(f"SET TRANSACTION SNAPSHOT '{snapshot_id}';")
        try:
            for t in tables:
                key = f"{t['schema_name']}.{t['table_name']}"
                tgt_count = target_counts.get(key)
                if tgt_count is None or tgt_count < 0:
                    continue  # already-failed table — reported separately
                fqn = qt(t["schema_name"], t["table_name"])
                src_count = await src.fetchval(f"SELECT count(*) FROM ONLY {fqn};")
                if src_count != tgt_count:
                    mismatches[key] = (src_count, tgt_count)
        finally:
            await src.execute("COMMIT;")
    return mismatches
