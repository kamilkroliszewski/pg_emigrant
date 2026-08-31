"""P0: a migration must never be pointed at the cluster it reads from.

The initial copy clears its target tables before loading them.  If "target"
resolves to the source, that clearing lands on production data, and it happens
early — before the copy, before replication, before anything that might have
noticed.  There is no recovery from it, so the check runs on every mutating
path and is not skippable.

Host and port cannot answer the question: one cluster is reachable under a
VIP, a pooler, a DNS alias, a second listen address, or simply a second port.
``system_identifier`` can, because it is stamped in at initdb time.
"""

from __future__ import annotations

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.config import DatabaseConfig, ReplicatorConfig
from pg_emigrant.db import connect
from pg_emigrant.guards import UnsafeOperation, assert_distinct_clusters

pytestmark = [pytest.mark.integration]


def _same_cluster_cfg(pg, dbname, *, host=None, port=None) -> ReplicatorConfig:
    target = DatabaseConfig(**{
        **pg.config.model_dump(),
        **({"host": host} if host else {}),
        **({"port": port} if port else {}),
    })
    return ReplicatorConfig(source=pg.config, target=target, databases=[dbname])


async def test_identical_endpoints_are_refused(source_pg, source_db):
    cfg = _same_cluster_cfg(source_pg, source_db)
    with pytest.raises(UnsafeOperation) as excinfo:
        await bootstrap(cfg, database=source_db)
    assert "SAME PostgreSQL cluster" in str(excinfo.value)


async def test_an_alias_for_the_same_cluster_is_refused(source_pg, source_db):
    """A different-looking address for the same server must not pass.

    'localhost' and '127.0.0.1' are the same machine, so nothing about the
    endpoint strings gives this away — only the system identifier does.
    """
    cfg = _same_cluster_cfg(source_pg, source_db, host="localhost")
    assert cfg.source.host != cfg.target.host  # the check has something to beat

    with pytest.raises(UnsafeOperation) as excinfo:
        await bootstrap(cfg, database=source_db)
    assert "system_identifier" in str(excinfo.value)


async def test_nothing_is_created_or_truncated_before_the_refusal(source_pg, source_db):
    """The refusal must precede every write, including the target truncate."""
    async with connect(source_pg.config, source_db) as conn:
        before = await conn.fetchval("SELECT count(*) FROM app.customers")
        slots_before = await conn.fetchval("SELECT count(*) FROM pg_replication_slots")
        pubs_before = await conn.fetchval("SELECT count(*) FROM pg_publication")

    cfg = _same_cluster_cfg(source_pg, source_db)
    with pytest.raises(UnsafeOperation):
        await bootstrap(cfg, database=source_db)

    async with connect(source_pg.config, source_db) as conn:
        assert await conn.fetchval("SELECT count(*) FROM app.customers") == before, (
            "production rows were destroyed before the same-cluster check ran"
        )
        assert await conn.fetchval("SELECT count(*) FROM pg_replication_slots") == slots_before
        assert await conn.fetchval("SELECT count(*) FROM pg_publication") == pubs_before


async def test_two_independent_clusters_pass(cfg):
    await assert_distinct_clusters(cfg)  # must not raise
