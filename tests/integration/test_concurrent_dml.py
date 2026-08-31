"""P0: initial COPY concurrent with a live write workload.

The whole reason bootstrap creates the replication slot *before* the copy and
copies with the slot's exported snapshot is to close the window in which a
committed transaction lands in neither the copy nor the WAL stream.  That
claim is untestable without concurrent writes, so these tests keep a workload
running across the entire bootstrap and then verify convergence with
PostgreSQL-computed checksums.
"""

from __future__ import annotations

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.db import connect
from pg_emigrant.sequence_sync import sync_sequences_once
from tests.helpers.replication import wait_for_catchup
from tests.helpers.verify import assert_tables_identical, sequence_values
from tests.helpers.workload import ConcurrentWriter

pytestmark = [pytest.mark.integration, pytest.mark.slow]

SCHEMAS = ["app", "reporting"]


async def test_no_rows_lost_between_snapshot_and_stream(cfg, source_db):
    """Writes committed during bootstrap must arrive exactly once."""
    async with ConcurrentWriter(cfg.source, source_db) as writer:
        await bootstrap(cfg, database=source_db)
        # Keep writing after the subscription exists too, so the handover from
        # snapshot copy to WAL streaming is covered from both sides.
        for _ in range(50):
            if writer.inserted_customers > 20:
                break
            await _sleep()

    assert writer.inserted_customers > 5, (
        f"the workload only committed {writer.inserted_customers} batches — "
        "too few to prove anything about the copy/stream handover"
    )
    assert writer.updated > 0 and writer.deleted > 0

    await wait_for_catchup(cfg, source_db)

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        await assert_tables_identical(src, tgt, SCHEMAS)


async def test_sequences_converge_after_concurrent_inserts(cfg, source_db):
    """Sequences advanced during the copy must not leave the target behind.

    Logical replication never carries a sequence advance, so a target that is
    behind here hands out already-used values after cutover — a duplicate-key
    outage rather than a slow query.
    """
    async with ConcurrentWriter(cfg.source, source_db):
        await bootstrap(cfg, database=source_db)

    await wait_for_catchup(cfg, source_db)
    await sync_sequences_once(cfg, source_db)

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        src_seq = await sequence_values(src, SCHEMAS)
        tgt_seq = await sequence_values(tgt, SCHEMAS)
        behind = {k: (v, tgt_seq.get(k)) for k, v in src_seq.items()
                  if tgt_seq.get(k) is None or tgt_seq[k] < v}
        assert not behind, f"target sequence(s) behind the source: {behind}"


async def test_pk_less_table_updates_replicate(cfg, source_db):
    """UPDATE/DELETE on a table with no PK need REPLICA IDENTITY FULL.

    Without it the source logs no old-row identity, the apply worker rejects
    every UPDATE, and the subscription stalls — visibly on the target, silently
    from the source's point of view.
    """
    await bootstrap(cfg, database=source_db)

    async with connect(cfg.source, source_db) as src:
        identity = await src.fetchval(
            "SELECT relreplident FROM pg_class c JOIN pg_namespace n"
            " ON n.oid = c.relnamespace WHERE n.nspname='app' AND c.relname='audit_log'"
        )
        assert identity in ("f", b"f"), (
            f"expected REPLICA IDENTITY FULL on the source, got {identity!r}"
        )

        await src.execute(
            "UPDATE app.audit_log SET actor = 'rewritten' WHERE action LIKE 'action-1%'"
        )
        await src.execute("DELETE FROM app.audit_log WHERE action = 'action-7'")
        await src.execute(
            "INSERT INTO app.audit_log (actor, action) VALUES ('post', 'after-bootstrap')"
        )

    await wait_for_catchup(cfg, source_db)

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        await assert_tables_identical(src, tgt, ["app"])
        assert await tgt.fetchval(
            "SELECT count(*) FROM app.audit_log WHERE actor = 'rewritten'"
        ) > 0


async def test_long_running_write_transaction_blocks_slot_creation_and_fails_closed(
    cfg, source_db
):
    """A write transaction open across slot creation must not be papered over.

    ``CREATE_REPLICATION_SLOT`` for a logical slot waits for a ShareLock on
    every XID running anywhere in the cluster, so an open write transaction
    blocks it outright — PostgreSQL's behaviour, not the tool's.  What matters
    is what pg_emigrant does about it: time out with a diagnosable error, leave
    no slot behind, and above all not report success.
    """
    async with connect(cfg.source, source_db) as writer:
        tx = writer.transaction()
        await tx.start()
        await writer.execute(
            "INSERT INTO app.nasty_strings (id, val, note)"
            " SELECT 900 + i, 'uncommitted ' || i, 'long tx'"
            " FROM generate_series(1, 20) AS i"
        )

        with pytest.raises(RuntimeError) as excinfo:
            await bootstrap(cfg, database=source_db)
        assert "Bootstrap failed" in str(excinfo.value)

        await tx.rollback()

    # No orphaned slot may survive the refusal — an abandoned logical slot
    # retains WAL on the production source indefinitely.
    from tests.helpers.replication import all_slots

    assert await all_slots(cfg) == []


async def test_transaction_committing_mid_migration_lands_exactly_once(cfg, source_db):
    """A transaction that commits while the copy is running.

    Its rows are invisible to the slot's exported snapshot (so the copy must
    not contain them) and its commit is after the slot's start LSN (so the
    stream must deliver them).  Getting this wrong in either direction shows up
    as missing or duplicated rows.
    """
    import asyncio

    async with connect(cfg.source, source_db) as writer:
        boot = asyncio.create_task(bootstrap(cfg, database=source_db))

        # Open and commit the transaction *while* the bootstrap is in flight,
        # after the slot exists.  Opening it earlier would block slot creation
        # instead (see the test above).
        await _wait_for_slot(cfg, source_db)
        tx = writer.transaction()
        await tx.start()
        await writer.execute(
            "INSERT INTO app.nasty_strings (id, val, note)"
            " SELECT 900 + i, 'midflight ' || i, 'mid tx'"
            " FROM generate_series(1, 20) AS i"
        )
        await tx.commit()

        await boot

    await wait_for_catchup(cfg, source_db)

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        assert await tgt.fetchval(
            "SELECT count(*) FROM app.nasty_strings WHERE note = 'mid tx'"
        ) == 20, "rows committed after slot creation arrived neither in the copy nor the stream"
        await assert_tables_identical(src, tgt, ["app"])


async def _wait_for_slot(cfg, dbname, timeout: float = 30.0):
    """Block until bootstrap has created this database's replication slot."""
    import asyncio

    from pg_emigrant.replication import sub_name

    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        async with connect(cfg.source, dbname) as src:
            if await src.fetchval(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
                sub_name(cfg, dbname),
            ):
                return
        await asyncio.sleep(0.1)
    raise AssertionError("bootstrap never created a replication slot")


async def test_long_running_read_transaction_does_not_break_bootstrap(cfg, source_db):
    """A long READ transaction holds an XID and can block slot creation.

    It must not silently corrupt the migration; the tool either proceeds or
    fails loudly, never reports success on an incomplete copy.
    """
    async with connect(cfg.source, source_db) as reader:
        tx = reader.transaction(isolation="repeatable_read")
        await tx.start()
        await reader.fetchval("SELECT count(*) FROM app.customers")

        await bootstrap(cfg, database=source_db)
        await tx.rollback()

    await wait_for_catchup(cfg, source_db)
    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        await assert_tables_identical(src, tgt, SCHEMAS)


async def _sleep():
    import asyncio
    await asyncio.sleep(0.05)
