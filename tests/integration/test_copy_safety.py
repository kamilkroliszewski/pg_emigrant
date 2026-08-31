"""P0: the initial copy, at its edges.

The copy is the only part of a migration that moves every byte, so its failure
modes are the expensive ones.  These tests cover the shapes that break a naive
CSV pipeline (empty tables, single rows, NULL vs empty string, the text-format
COPY sentinels as literal data, TOASTed values, generated and identity
columns), the parallelism (multiple tables at once, one table split into ctid
slices), and what happens when a copy is interrupted mid-stream.
"""

from __future__ import annotations

import asyncio

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.data_copy import copy_all_tables, verify_copy_counts
from pg_emigrant.db import connect
from pg_emigrant.replication import create_replication_slot_with_snapshot
from pg_emigrant.report import BootstrapIncomplete
from pg_emigrant.schema_sync import get_tables, sync_schemas
from tests.helpers.verify import assert_tables_identical, table_checksum

pytestmark = [pytest.mark.integration, pytest.mark.slow]

EDGE_DDL = """
CREATE SCHEMA edge;
CREATE TABLE edge.empty_table (id int primary key, v text);
CREATE TABLE edge.one_row (id int primary key, v text);
CREATE TABLE edge.wide (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    txt          text,
    blob         bytea,
    js           jsonb,
    arr          text[],
    nums         numeric(30, 10)[],
    ts           timestamptz,
    ts_plain     timestamp,
    iv           interval,
    flag         boolean,
    tiny         numeric,
    doubled      bigint GENERATED ALWAYS AS (id * 2) STORED
);
INSERT INTO edge.one_row VALUES (1, 'only');
"""

# Values chosen to break something specific: the CSV quoting rules, the text
# COPY sentinels, numeric scale preservation, timestamp infinities, array
# NULL-vs-empty-string, and the TOAST threshold.
EDGE_ROWS = r"""
INSERT INTO edge.wide (txt, blob, js, arr, nums, ts, ts_plain, iv, flag, tiny) VALUES
 (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 ('', '\x'::bytea, '{}'::jsonb, '{}'::text[], '{}'::numeric[], 'epoch', 'epoch', '0', false, 0),
 (E'a,b"c\\d', '\x00010203ff'::bytea, '{"a":[1,2,{"b":null}]}'::jsonb,
  ARRAY['x','','y',NULL], ARRAY[1.0000000001, -0.0000000001]::numeric(30,10)[],
  'infinity', '-infinity', '1 year 2 mons 3 days 04:05:06.789', true, 0.000000000000001),
 (E'line1\nline2\r\nline3', decode(repeat('de', 6000), 'hex'),
  jsonb_build_object('big', repeat('z', 5000)), ARRAY[repeat('q', 3000)],
  ARRAY[99999999999999999999.9999999999]::numeric(30,10)[],
  '294276-12-31 23:59:59+00', '4713-01-01 00:00:00 BC', '-178000000 years', true,
  -12345678901234567890.123456789),
 (E'\\N', NULL, 'null'::jsonb, ARRAY[E'\\.'], NULL, now(), now(), interval '1 microsecond',
  NULL, 'NaN'::numeric),
 (repeat('unicode ünïcödé 日本語 🐘 ', 500), NULL, '[]'::jsonb, NULL, NULL,
  '1970-01-01 00:00:00.000001+00', NULL, NULL, false, 1e-16);
"""


@pytest.fixture
def edge_db(source_pg, target_pg, dbname):
    source_pg.psql(f'CREATE DATABASE "{dbname}"')
    source_pg.psql_script(EDGE_DDL + EDGE_ROWS, dbname)
    source_pg.psql("ANALYZE", dbname=dbname)
    yield dbname
    from tests.integration.conftest import (
        drop_database,
        drop_orphan_slots,
        drop_subscription_if_present,
    )

    drop_subscription_if_present(target_pg, dbname)
    drop_database(target_pg, dbname)
    drop_orphan_slots(source_pg, dbname)
    drop_database(source_pg, dbname)


@pytest.fixture
def edge_cfg(source_pg, target_pg, edge_db):
    from pg_emigrant.config import ReplicatorConfig

    return ReplicatorConfig(
        source=source_pg.config, target=target_pg.config, databases=[edge_db],
        schemas=["edge"], parallel_workers=4, table_parallel_workers=4,
    )


async def test_every_value_shape_round_trips_exactly(edge_cfg, edge_db):
    report = await bootstrap(edge_cfg, database=edge_db)
    assert report.passed, report.summary

    async with connect(edge_cfg.source, edge_db) as src, connect(edge_cfg.target, edge_db) as tgt:
        await assert_tables_identical(src, tgt, ["edge"])

        # Spot-check the values a checksum could in principle agree on for the
        # wrong reason, and the ones a CSV round-trip most often mangles.
        for conn in (src, tgt):
            assert await conn.fetchval(
                "SELECT count(*) FROM edge.wide WHERE txt = '\\N'"
            ) == 1, "the literal text-COPY NULL marker did not survive"
            assert await conn.fetchval(
                "SELECT count(*) FROM edge.wide WHERE txt = '' AND txt IS NOT NULL"
            ) == 1, "an empty string became NULL"
            assert await conn.fetchval(
                "SELECT count(*) FROM edge.wide WHERE tiny = 'NaN'::numeric"
            ) == 1
            assert await conn.fetchval(
                "SELECT count(*) FROM edge.wide WHERE ts = 'infinity'"
            ) == 1
            assert await conn.fetchval(
                "SELECT length(blob) FROM edge.wide WHERE length(blob) > 5000"
            ) == 6000, "a TOASTed bytea was truncated"


async def test_generated_and_identity_columns(edge_cfg, edge_db):
    """GENERATED ALWAYS columns must be recomputed, identity values preserved."""
    await bootstrap(edge_cfg, database=edge_db)

    async with connect(edge_cfg.source, edge_db) as src, connect(edge_cfg.target, edge_db) as tgt:
        src_rows = await src.fetch("SELECT id, doubled FROM edge.wide ORDER BY id")
        tgt_rows = await tgt.fetch("SELECT id, doubled FROM edge.wide ORDER BY id")
        assert [tuple(r) for r in src_rows] == [tuple(r) for r in tgt_rows], (
            "GENERATED ALWAYS AS IDENTITY values were not preserved through COPY"
        )
        assert all(r["doubled"] == r["id"] * 2 for r in tgt_rows)


async def test_empty_and_single_row_tables(edge_cfg, edge_db):
    await bootstrap(edge_cfg, database=edge_db)
    async with connect(edge_cfg.target, edge_db) as tgt:
        assert await tgt.fetchval("SELECT count(*) FROM edge.empty_table") == 0
        assert await tgt.fetchval("SELECT count(*) FROM edge.one_row") == 1


async def test_intra_table_parallel_slices_cover_every_row(cfg, source_db):
    """A table split into ctid page ranges must be copied exactly once.

    Slices are computed from the physical page count, so an off-by-one at a
    boundary either drops rows (a gap between two ranges) or duplicates them
    (an overlap) — and both look like a successful copy from the outside.
    """
    async with connect(cfg.source, source_db) as src:
        # Enough pages that the split is real rather than degenerate.
        await src.execute(
            "INSERT INTO app.documents (title, body)"
            " SELECT 'bulk ' || i, repeat('x', 200) FROM generate_series(1, 20000) i"
        )
        await src.execute("ANALYZE app.documents")
        expected = await src.fetchval("SELECT count(*) FROM app.documents")
        pages = await src.fetchval("SELECT relpages FROM pg_class WHERE relname = 'documents'")
    assert pages > 8, f"the fixture is not large enough to be sliced ({pages} pages)"

    cfg.table_parallel_workers = 8
    report = await bootstrap(cfg, database=source_db)
    assert report.passed, report.summary

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        assert await tgt.fetchval("SELECT count(*) FROM app.documents") == expected
        s = await table_checksum(src, "app", "documents")
        t = await table_checksum(tgt, "app", "documents")
        assert s == t, "parallel ctid slices produced a different set of rows"


async def test_a_failed_table_copy_aborts_the_database(cfg, source_db, target_pg):
    """One table failing must not leave the other nine replicating.

    Logical replication only carries new changes; it would never backfill the
    rows the failed copy missed, so a target with nine good tables and one
    empty one would replicate happily forever while being wrong.
    """
    await _prepare_target_schema(cfg, source_db)
    # Make one target table impossible to load: a CHECK no source row satisfies.
    target_pg.psql(
        "ALTER TABLE app.nasty_strings ADD CONSTRAINT impossible CHECK (id < 0)",
        dbname=source_db,
    )

    with pytest.raises(BootstrapIncomplete) as excinfo:
        await bootstrap(cfg, database=source_db)

    problems = " ".join(excinfo.value.report.databases[0].problems)
    assert "nasty_strings" in problems, problems

    from tests.helpers.replication import all_slots, all_subscriptions

    assert await all_slots(cfg) == [], "a failed copy left its replication slot behind"
    assert await all_subscriptions(cfg, source_db) == [], (
        "replication was configured despite an incomplete copy"
    )


async def test_copy_count_verification_runs_under_the_copy_snapshot(cfg, source_db):
    """The post-copy count check must compare like with like.

    Counting the source outside the snapshot would race every concurrent
    write, so the check would either flap or be quietly useless.
    """
    await _prepare_target_schema(cfg, source_db)
    schemas = ["app", "reporting"]
    async with connect(cfg.source, source_db) as src:
        tables = [t for t in await get_tables(src, schemas) if t["relkind"] != "p"]

    slot = await create_replication_slot_with_snapshot(cfg, source_db)
    try:
        results = await copy_all_tables(cfg, source_db, tables, slot.snapshot_name)
        # Write to the source *after* the snapshot: the check must not see it.
        async with connect(cfg.source, source_db) as src:
            await src.execute(
                "INSERT INTO app.nasty_strings (id, val, note)"
                " SELECT 800 + i, 'after snapshot', 'race' FROM generate_series(1, 50) i"
            )
        mismatches = await verify_copy_counts(
            cfg, source_db, tables, slot.snapshot_name, results
        )
    finally:
        await slot.aclose()
        from pg_emigrant.replication import drop_replication_slot

        await drop_replication_slot(cfg, source_db, slot.slot_name)

    assert mismatches == {}, (
        f"the count check reported a mismatch for rows committed after the "
        f"snapshot it was supposed to be frozen at: {mismatches}"
    )


async def test_a_lost_source_connection_mid_copy_fails_the_table(cfg, source_db, source_pg):
    """A killed backend during COPY must fail that table, not truncate it.

    A half-streamed COPY that reported success would be the purest form of
    silent data loss: fewer rows, no error, replication started anyway.
    """
    await _prepare_target_schema(cfg, source_db)
    async with connect(cfg.source, source_db) as src:
        await src.execute(
            "INSERT INTO app.documents (title, body)"
            " SELECT 'bulk ' || i, repeat('y', 400) FROM generate_series(1, 40000) i"
        )

    async with connect(cfg.source, source_db) as src:
        tables = [t for t in await get_tables(src, ["app"]) if t["table_name"] == "documents"]

    async def _kill_copy_backends():
        for _ in range(200):
            killed = source_pg.psql(
                "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity"
                # asyncpg streams a query as COPY (SELECT …) TO STDOUT, so the
                # backend's query text starts with COPY, not SELECT.
                " WHERE query LIKE '%FROM ONLY %documents%'"
                "   AND pid <> pg_backend_pid()"
            )
            if killed and int(killed) > 0:
                return True
            await asyncio.sleep(0.02)
        return False

    killer = asyncio.create_task(_kill_copy_backends())
    results = await copy_all_tables(cfg, source_db, tables, None)
    did_kill = await killer

    if not did_kill:
        pytest.skip("the copy finished before a backend could be terminated")
    assert results["app.documents"] == -1, (
        "a COPY whose source connection was killed reported success"
    )


async def _prepare_target_schema(cfg, dbname):
    """Create the target database and schema without copying any data."""
    from pg_emigrant.bootstrap import ensure_database_exists

    await ensure_database_exists(cfg, dbname)
    async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
        await sync_schemas(src, tgt, ["app", "reporting"])
