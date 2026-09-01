"""P1: partitions, once the copy is over and the stream takes over.

The initial copy's partition behaviour is pinned elsewhere
(``test_bootstrap_consistency.py::test_partitioned_parent_rows_are_not_duplicated``):
rows live in the leaves, and copying the parent as well would double every one
of them.  What that does not cover is what happens *afterwards*, and the
after is where partitions behave unlike ordinary tables:

* A publication built from a partitioned parent covers its leaves implicitly,
  and on a pre-15 source the publication is a frozen ``FOR TABLE`` list naming
  only the parent — so whether leaf changes actually stream is a property of
  PostgreSQL's implicit membership, not of anything pg_emigrant enumerated.
* With the default ``publish_via_partition_root = false``, a change is
  published under the *leaf's* identity, so the subscriber needs that leaf to
  exist under the same name.
* An UPDATE that moves a row across the partition boundary is decoded as a
  DELETE on one leaf and an INSERT on another — two changes, on two relations,
  that have to arrive and be applied as a pair or the row is duplicated (both
  applied out of order) or lost (only the DELETE applied).

Every one of those is a silent-divergence shape: the row counts on the parent
are what an operator would check, and they are exactly what stays plausible
while a leaf is wrong.
"""

from __future__ import annotations

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.db import connect
from pg_emigrant.health import ReplicationState, replication_health
from tests.helpers.replication import wait_for_catchup
from tests.helpers.verify import table_checksum, table_row_count

pytestmark = [pytest.mark.integration, pytest.mark.slow]


async def _leaf_counts(conn) -> dict[str, int]:
    return {
        leaf: await table_row_count(conn, "app", leaf)
        for leaf in ("events_2024", "events_2025")
    }


async def test_every_shape_of_partition_write_replicates_exactly(cfg, source_db):
    """Routed inserts, direct-leaf inserts, updates, deletes — and a row moved
    across the partition boundary, which is the one that is not an UPDATE at
    all on the wire."""
    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    async with connect(cfg.source, source_db) as src:
        # 1. INSERT routed through the parent.
        await src.execute(
            "INSERT INTO app.events (occurred, kind)"
            " SELECT DATE '2024-06-01', 'routed-' || g"
            " FROM generate_series(1, 20::int) g"
        )
        # 2. INSERT straight into a leaf, bypassing the parent's routing.
        #    The id is taken from the parent's identity sequence explicitly:
        #    before PostgreSQL 17 a partition does not inherit the parent's
        #    identity default, so a direct-leaf insert that omitted it would
        #    fail on the older sources in the matrix — a fact about
        #    PostgreSQL, not about replication, and not what this asserts.
        await src.execute(
            "INSERT INTO app.events_2025 (id, occurred, kind)"
            " SELECT nextval(pg_get_serial_sequence('app.events', 'id')),"
            "        DATE '2025-06-01', 'direct-' || g"
            " FROM generate_series(1, 15::int) g"
        )
        # 3. UPDATE within a partition.
        await src.execute(
            "UPDATE app.events SET kind = kind || '!'"
            " WHERE kind LIKE 'routed-%' AND occurred < DATE '2025-01-01'"
        )
        # 4. DELETE.
        await src.execute("DELETE FROM app.events WHERE kind = 'direct-1'")
        # 5. The interesting one: move rows ACROSS the boundary.  PostgreSQL
        #    decodes this as a DELETE on events_2024 and an INSERT on
        #    events_2025 — two changes on two relations that must arrive as a
        #    pair, or the row is duplicated or lost.
        moved = await src.fetchval(
            "WITH m AS ("
            "  UPDATE app.events SET occurred = DATE '2025-03-03'"
            "  WHERE kind LIKE 'routed-1%' AND occurred < DATE '2025-01-01'"
            "  RETURNING 1) SELECT count(*) FROM m"
        )
    assert moved > 0, "no row actually crossed the partition boundary"

    await wait_for_catchup(cfg, source_db)

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        src_leaves = await _leaf_counts(src)
        tgt_leaves = await _leaf_counts(tgt)
        assert src_leaves == tgt_leaves, (
            f"the partitions diverged: source {src_leaves} vs target "
            f"{tgt_leaves}. The parent's total can stay right while a leaf is "
            f"wrong, which is why this compares leaves"
        )
        # The parent's total, and then the actual rows — a count that matches
        # while a row moved to the wrong leaf would still be wrong.
        assert (
            await table_checksum(src, "app", "events", ["id", "occurred", "kind"])
            == await table_checksum(tgt, "app", "events", ["id", "occurred", "kind"])
        ), "app.events did not converge"
        for leaf in ("events_2024", "events_2025"):
            assert (
                await table_checksum(src, "app", leaf, ["id", "occurred", "kind"])
                == await table_checksum(tgt, "app", leaf, ["id", "occurred", "kind"])
            ), f"app.{leaf} did not converge"

    health = await replication_health(cfg, source_db)
    assert health.state is ReplicationState.HEALTHY, health.reasons


async def test_a_partition_added_after_bootstrap_is_picked_up(cfg, source_db):
    """A new partition is a new table, and the steady-state loop has to see it.

    Adding a range partition for the next period is routine maintenance, not an
    unusual event — a partitioned table that nobody extends stops accepting
    inserts. If the new leaf never reaches the target, every write routed into
    it is silently absent while the parent's older partitions keep replicating
    perfectly.
    """
    from pg_emigrant.cutover import check_cutover_readiness
    from pg_emigrant.replication import sync_new_tables

    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    async with connect(cfg.source, source_db) as src:
        await src.execute(
            "CREATE TABLE app.events_2026 PARTITION OF app.events"
            " FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"
        )
        await src.execute(
            "INSERT INTO app.events (occurred, kind)"
            " SELECT DATE '2026-02-02', 'next-year-' || g"
            " FROM generate_series(1, 12::int) g"
        )

    # Until it is picked up, the target is missing a table the source has, and
    # the readiness check has to say so rather than approve on the parent's
    # older partitions still looking fine.
    assert not (await check_cutover_readiness(cfg, database=source_db)).ready, (
        "cutover-check approved a target missing a whole partition"
    )

    await sync_new_tables(cfg, source_db)
    await wait_for_catchup(cfg, source_db, timeout=120)

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        assert await table_row_count(tgt, "app", "events_2026") == 12, (
            "the new partition's rows never reached the target"
        )
        assert (
            await table_checksum(src, "app", "events", ["id", "occurred", "kind"])
            == await table_checksum(tgt, "app", "events", ["id", "occurred", "kind"])
        )

    # And writes that follow keep streaming into it.
    async with connect(cfg.source, source_db) as src:
        await src.execute(
            "INSERT INTO app.events (occurred, kind) VALUES (DATE '2026-05-05', 'later')"
        )
    await wait_for_catchup(cfg, source_db)
    async with connect(cfg.target, source_db) as tgt:
        assert await table_row_count(tgt, "app", "events_2026") == 13
