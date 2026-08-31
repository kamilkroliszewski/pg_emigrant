"""End-to-end: a bootstrap must leave the target byte-identical to the source.

This is the baseline every other integration test builds on.  It runs the real
``bootstrap`` against two real clusters loaded with the full migration fixture
and then asks PostgreSQL itself whether the two databases agree — row counts,
whole-row checksums, primary-key sets, sequence values and the schema object
inventory.
"""

from __future__ import annotations

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.db import connect
from tests.helpers.verify import (
    assert_tables_identical,
    schema_objects,
    sequence_values,
    table_checksum,
)

pytestmark = [pytest.mark.integration, pytest.mark.slow]

SCHEMAS = ["app", "reporting"]


async def test_bootstrap_copies_every_row_exactly(cfg, source_db):
    await bootstrap(cfg, database=source_db)

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        evidence = await assert_tables_identical(src, tgt, SCHEMAS)

    assert evidence, "no tables were compared — the fixture did not load"
    # The fixture's headline tables must actually contain rows, otherwise an
    # empty-vs-empty comparison would pass while proving nothing.
    assert evidence["app.customers"][0] == 504
    assert evidence["app.order_lines"][0] == 1512
    assert evidence["app.nasty_strings"][0] == 15


async def test_bootstrap_preserves_adversarial_text_exactly(cfg, source_db):
    """CSV COPY must round-trip quotes, delimiters, newlines and NULL markers.

    ``\\N`` and ``\\.`` are the *text*-format COPY NULL marker and end-of-data
    terminator; a table containing them as literal data is the classic way a
    copy path that silently switches format corrupts rows.
    """
    await bootstrap(cfg, database=source_db)

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        for conn_name, conn in (("source", src), ("target", tgt)):
            rows = await conn.fetch(
                "SELECT id, val, note FROM app.nasty_strings ORDER BY id"
            )
            by_id = {r["id"]: r["val"] for r in rows}
            assert by_id[2] is None, f"{conn_name}: NULL became something else"
            assert by_id[3] == "", f"{conn_name}: empty string became NULL"
            assert by_id[11] == "\\N", f"{conn_name}: literal backslash-N corrupted"
            assert by_id[13] == "\\.", f"{conn_name}: literal backslash-dot corrupted"
            assert by_id[8] == "line1\nline2"
            assert len(by_id[15]) == 100_000

        s_n, s_sum = await table_checksum(src, "app", "nasty_strings")
        t_n, t_sum = await table_checksum(tgt, "app", "nasty_strings")
        assert (s_n, s_sum) == (t_n, t_sum)


async def test_bootstrap_syncs_sequences_and_schema_inventory(cfg, source_db):
    await bootstrap(cfg, database=source_db)

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        src_seq = await sequence_values(src, SCHEMAS)
        tgt_seq = await sequence_values(tgt, SCHEMAS)

        assert set(src_seq) == set(tgt_seq), (
            f"sequence inventory differs: only on source "
            f"{sorted(set(src_seq) - set(tgt_seq))}, only on target "
            f"{sorted(set(tgt_seq) - set(src_seq))}"
        )
        behind = {k: (v, tgt_seq[k]) for k, v in src_seq.items() if tgt_seq[k] < v}
        assert not behind, f"target sequences behind source (duplicate-key risk): {behind}"

        src_objs = await schema_objects(src, SCHEMAS)
        tgt_objs = await schema_objects(tgt, SCHEMAS)
        missing = src_objs - tgt_objs
        assert not missing, f"objects missing on target: {sorted(missing)}"


async def test_partitioned_parent_rows_are_not_duplicated(cfg, source_db):
    """Rows live in the leaves; copying the parent too would double them."""
    await bootstrap(cfg, database=source_db)

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        src_total = await src.fetchval("SELECT count(*) FROM app.events")
        tgt_total = await tgt.fetchval("SELECT count(*) FROM app.events")
        assert src_total == tgt_total == 400
        for leaf in ("events_2024", "events_2025"):
            s = await src.fetchval(f"SELECT count(*) FROM ONLY app.{leaf}")
            t = await tgt.fetchval(f"SELECT count(*) FROM ONLY app.{leaf}")
            assert s == t, f"{leaf}: {s} on source vs {t} on target"
