"""Efficient initial data copy using PostgreSQL COPY protocol.

Streams each table (or ctid page-range slice of it) source→target through a
bounded asyncio.Queue as CSV COPY — flat memory regardless of table size,
parallel across (and within) tables, snapshot-consistent reads.
"""

from __future__ import annotations

import asyncio
from typing import Callable

from pg_emigrant.config import ReplicatorConfig
from pg_emigrant.db import connect
from pg_emigrant._testhooks import maybe_fail
from pg_emigrant.utils import get_logger, qi, ql, qt

log = get_logger(__name__)


class IncompatibleTargetColumns(Exception):
    """A target table cannot faithfully hold the source table's rows.

    Raised before any data moves, for two shapes of mismatch:

    * a source column with no counterpart on the target — copying only the
      columns the two sides have in common produces a target that passes every
      row-count check while permanently missing a column's worth of data;
    * a column whose type differs — CSV COPY happily loads a bigint into a
      text column and an enum into text, so the copy "succeeds" and the target
      quietly stops being the same data.
    """

    def __init__(self, message: str):
        super().__init__(message)

    @classmethod
    def aggregate(cls, problems: list[str]) -> "IncompatibleTargetColumns":
        return cls(
            f"{len(problems)} target column mismatch(es) would make the copy "
            f"lose or silently alter data while reporting a clean migration: "
            + "; ".join(problems)
            + ". Align the target schema — or drop those target tables and let "
              "bootstrap recreate them — and re-run."
        )


class UnsafeTruncate(Exception):
    """Clearing the target tables would destroy data outside the migration."""


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
        # relpages is a statistic — 0 for a never-analyzed table no matter how
        # large it really is, which would silently disable intra-table
        # parallelism.  Floor it with the actual physical size.
        relpages: int = (
            await src.fetchval(
                "SELECT GREATEST(relpages,"
                " (pg_relation_size(oid) / current_setting('block_size')::int)::int)"
                " FROM pg_class"
                " WHERE oid = (quote_ident($1) || '.' || quote_ident($2))::regclass",
                schema,
                table,
            )
            or 0
        )
        tgt_col_rows = await tgt.fetch(_col_query, schema, table)

    tgt_cols = {r["column_name"] for r in tgt_col_rows}
    # A source column with nowhere to land is silent data loss: every row
    # copies, the row counts match, the run reports success, and one column's
    # worth of data is simply gone.  Refuse instead.  (Generated columns are
    # already excluded on both sides by _col_query — the target recomputes
    # them, so they are not expected to be copied.)
    dropped = sorted(src_cols - tgt_cols)
    if dropped:
        raise IncompatibleTargetColumns.aggregate(
            [f"{schema}.{table}: target is missing column(s) {', '.join(dropped)}"]
        )

    # Preserve target column order; skip columns absent from source.
    common_columns = [r["column_name"] for r in tgt_col_rows if r["column_name"] in src_cols]
    cols_select = ", ".join(qi(c) for c in common_columns)

    async def _stream_query(src_conn, tgt_conn, query: str) -> int:
        """Pipe SELECT query to target COPY via asyncio.Queue — zero RAM accumulation.

        Returns the row count the target reported in its ``COPY <n>`` command
        status — no extra ``count(*)`` scan needed.

        Failure handling:
        * Producer fails mid-stream → the consumer raises the same exception
          inside the COPY source generator.  asyncpg forwards it as a CopyFail
          to PostgreSQL, which rolls back the entire COPY — no partial data is
          committed.
        * CONSUMER fails first (target-side error: type mismatch, disk full,
          …) → the producer is cancelled and the queue drained.  Without
          that, the producer would block forever on the full queue, and the
          graceful connection close would never return — hanging the whole
          bootstrap instead of failing this one table.
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

        async def _consume() -> int:
            async def _reader():
                while True:
                    chunk = await queue.get()
                    if chunk is None:
                        if _produce_exc is not None:
                            raise _produce_exc  # abort COPY — rolls back partial data
                        return
                    yield chunk
            status = await tgt_conn.copy_to_table(
                table,
                schema_name=schema,
                source=_reader(),
                format="csv",
                columns=common_columns,
            )
            return int(status.split()[-1])  # command status is 'COPY <n>'

        producer = asyncio.create_task(_produce())
        try:
            copied = await _consume()
        except BaseException:
            # Unblock and stop the producer before anything touches src_conn
            # again (COMMIT / connection close) — see docstring.
            producer.cancel()
            while not queue.empty():
                queue.get_nowait()
            raise
        finally:
            try:
                await producer
            except (asyncio.CancelledError, Exception):
                pass  # its failure was already propagated via _produce_exc
        return copied

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

        async def _copy_chunk(page_start: int, page_end: int | None) -> int:
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
                    await src.execute(f"SET TRANSACTION SNAPSHOT {ql(snapshot_id)};")
                await tgt.execute("SET session_replication_role = 'replica';")
                rows = await _stream_query(src, tgt, query)
                await src.execute("COMMIT;")
                return rows

        chunk_counts = await asyncio.gather(*[_copy_chunk(s, e) for s, e in ranges])
        row_count: int = sum(chunk_counts)

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
                await src.execute(f"SET TRANSACTION SNAPSHOT {ql(snapshot_id)};")
            await tgt.execute("SET session_replication_role = 'replica';")
            row_count = await _stream_query(src, tgt, query)
            await src.execute("COMMIT;")

    log.info("Copied %s rows to %s", row_count, fqn)
    return row_count


async def _assert_target_columns_compatible(cfg, dbname: str, tables: list[dict]) -> None:
    """Verify each target table can faithfully hold its source rows.

    Checked up front rather than per table so the operator sees the complete
    list in one message, and — more importantly — so it is discovered *before*
    the target tables are cleared.  Finding it half way through would leave an
    emptied target behind for a problem that was knowable beforehand.
    """
    # format_type resolves to a fully schema-qualified name because every
    # connection runs with an empty search_path (see db.connect), so 'app.email'
    # on one side never compares equal to a bare 'email' on the other.
    _q = """
        SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) AS coltype
        FROM pg_attribute a
        WHERE a.attrelid = (quote_ident($1) || '.' || quote_ident($2))::regclass
          AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
    """
    problems: list[str] = []
    async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
        for t in tables:
            schema, table = t["schema_name"], t["table_name"]
            src_cols = {r["attname"]: r["coltype"] for r in await src.fetch(_q, schema, table)}
            try:
                tgt_cols = {r["attname"]: r["coltype"] for r in await tgt.fetch(_q, schema, table)}
            except Exception as exc:
                problems.append(f"{schema}.{table}: cannot inspect on target ({exc})")
                continue
            missing = sorted(set(src_cols) - set(tgt_cols))
            if missing:
                problems.append(
                    f"{schema}.{table}: target is missing column(s) {', '.join(missing)}"
                )
            for col in sorted(set(src_cols) & set(tgt_cols)):
                if src_cols[col] != tgt_cols[col]:
                    problems.append(
                        f"{schema}.{table}.{col}: source is {src_cols[col]}, "
                        f"target is {tgt_cols[col]}"
                    )
    if problems:
        raise IncompatibleTargetColumns.aggregate(problems)


async def _outside_scope_referencing_rows(
    conn, tables: list[dict]
) -> list[tuple[str, int]]:
    """Tables OUTSIDE the copy set that hold rows and reference a table inside it.

    ``TRUNCATE ... CASCADE`` empties these too.  When they belong to the
    migration they are in the set already and CASCADE changes nothing; when
    they do not — an application co-located on the same target cluster, a
    schema this run was told to exclude — CASCADE destroys live data that the
    migration's own consistency checks never look at, so nothing would report
    the loss.
    """
    in_scope = {(t["schema_name"], t["table_name"]) for t in tables}
    edges = await conn.fetch(
        """
        SELECT rn.nspname AS ref_schema, rc.relname AS ref_table,   -- referencing
               fn.nspname AS tgt_schema, fc.relname AS tgt_table    -- referenced
        FROM pg_constraint con
        JOIN pg_class rc ON rc.oid = con.conrelid
        JOIN pg_namespace rn ON rn.oid = rc.relnamespace
        JOIN pg_class fc ON fc.oid = con.confrelid
        JOIN pg_namespace fn ON fn.oid = fc.relnamespace
        WHERE con.contype = 'f'
        """
    )
    outside = sorted({
        (e["ref_schema"], e["ref_table"])
        for e in edges
        if (e["tgt_schema"], e["tgt_table"]) in in_scope
        and (e["ref_schema"], e["ref_table"]) not in in_scope
    })

    populated: list[tuple[str, int]] = []
    for schema, table in outside:
        n = await conn.fetchval(f"SELECT count(*) FROM ONLY {qt(schema, table)}")
        if n:
            populated.append((f"{schema}.{table}", n))
    return populated


async def _clear_target_tables(conn, tables: list[dict], dbname: str) -> None:
    """Empty every target table about to be loaded — and nothing else.

    One statement for the whole set, so PostgreSQL resolves the foreign keys
    *among* those tables itself instead of the caller having to order them (and
    without the deadlocks that parallel per-table TRUNCATEs produce).

    CASCADE is still needed for the set's own dependency closure, so the
    dangerous case — a populated table outside the set referencing one inside
    it — is checked first and refused.  Cascading into it would silently empty
    data this migration does not own and never looks at again.
    """
    collateral = await _outside_scope_referencing_rows(conn, tables)
    if collateral:
        detail = ", ".join(f"{name} ({n:,} rows)" for name, n in collateral)
        raise UnsafeTruncate(
            f"refusing to clear the target tables in {dbname}: {len(collateral)} "
            f"table(s) outside this migration's scope hold data and reference "
            f"tables inside it, so emptying those tables would destroy them "
            f"too: {detail}. Either bring those tables into scope (they are "
            f"part of the same data set), or drop/empty them on the target "
            f"first if they are obsolete."
        )
    table_list = ", ".join(
        f"{qi(t['schema_name'])}.{qi(t['table_name'])}" for t in tables
    )
    await conn.execute(f"TRUNCATE {table_list} CASCADE;")


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

    # Both of these run BEFORE anything is cleared or copied, so a target that
    # cannot faithfully hold the data is refused while it is still untouched.
    await _assert_target_columns_compatible(cfg, dbname, tables)
    async with connect(cfg.target, dbname) as tgt_conn:
        await _clear_target_tables(tgt_conn, tables, dbname)

    sem = asyncio.Semaphore(workers)

    async def _copy_one(schema: str, table: str) -> tuple[str, int]:
        key = f"{schema}.{table}"
        async with sem:
            if on_table_start:
                on_table_start(key)
            try:
                await maybe_fail("data_copy")
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
        await src.execute(f"SET TRANSACTION SNAPSHOT {ql(snapshot_id)};")
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
