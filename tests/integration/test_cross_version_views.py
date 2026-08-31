"""P0: a view must not be reported as drift just for crossing major versions.

PostgreSQL 16 changed how ``pg_get_viewdef`` renders column references — it
stopped table-qualifying them in single-table queries — so the *text* of a
14-or-15 source's view definition never matches the same view's text on a 16+
target, no matter how identical the two views are.

That matters more than it looks. Drift is not a warning any more: a bootstrap
that leaves drift behind ends `incomplete` and `cutover-check` refuses on it.
So a false positive here does not just add noise, it blocks every cross-version
migration that contains a view — which is essentially all of them.

The fix is to make both sides go through the *same* deparser: the source's
definition is instantiated as a temporary view on the target and re-read there.
The subtlety this pins down is that the re-read has to use the same ``pretty``
flag as the comparison, because non-pretty output parenthesises expressions
that pretty leaves bare.
"""

from __future__ import annotations

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.db import connect
from pg_emigrant.ddl_detector import detect_drift

pytestmark = [pytest.mark.integration, pytest.mark.slow]

VIEW_DDL = """
CREATE SCHEMA v;
CREATE TABLE v.accounts (id int PRIMARY KEY, login text, tier int, props jsonb);
CREATE TABLE v.orders (id int PRIMARY KEY, account_id int REFERENCES v.accounts(id), cents bigint);

-- Single-table: the shape whose deparse changed in PG16.
CREATE VIEW v.simple AS SELECT id, login, tier FROM v.accounts;

-- An expression that non-pretty deparsing parenthesises and pretty does not.
CREATE VIEW v.expressions AS
    SELECT id, tier * 2 AS doubled, props -> 'k' AS k, upper(login) AS up
    FROM v.accounts;

-- Multi-table: qualification is required here on every version, so this one
-- must keep matching for the ordinary reason.
CREATE VIEW v.joined AS
    SELECT a.id, a.login, sum(o.cents) AS total
    FROM v.accounts a LEFT JOIN v.orders o ON o.account_id = a.id
    GROUP BY a.id, a.login;

CREATE VIEW v.layered AS SELECT * FROM v.simple WHERE tier > 0;

CREATE MATERIALIZED VIEW v.matview AS SELECT tier, count(*) AS n FROM v.accounts GROUP BY tier;

INSERT INTO v.accounts SELECT i, 'user' || i, i % 3, jsonb_build_object('k', i)
FROM generate_series(1, 50) i;
INSERT INTO v.orders SELECT i, 1 + (i % 50), i * 100 FROM generate_series(1, 50) i;
REFRESH MATERIALIZED VIEW v.matview;
"""


@pytest.fixture
def view_db(source_pg, target_pg, dbname):
    source_pg.psql(f'CREATE DATABASE "{dbname}"')
    source_pg.psql_script(VIEW_DDL, dbname)
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
def view_cfg(source_pg, target_pg, view_db):
    from pg_emigrant.config import ReplicatorConfig

    return ReplicatorConfig(
        source=source_pg.config, target=target_pg.config,
        databases=[view_db], schemas=["v"],
    )


async def test_views_do_not_register_as_drift_across_major_versions(view_cfg, view_db):
    report = await bootstrap(view_cfg, database=view_db)
    assert report.passed, (
        "a clean migration was reported incomplete: "
        + "; ".join(report.databases[0].problems)
    )

    drift = await detect_drift(view_cfg, view_db)
    view_drift = [i for i in drift.items if i.object_type in ("view", "materialized_view")]
    assert not view_drift, (
        "views reported as drift despite being identical: "
        + "; ".join(f"{i.schema}.{i.name} ({i.drift_type}): {i.detail}" for i in view_drift)
    )


async def test_the_views_actually_work_on_the_target(view_cfg, view_db):
    """Equivalence has to mean equivalence, not just a passing text compare."""
    await bootstrap(view_cfg, database=view_db)

    async with connect(view_cfg.source, view_db) as src, connect(view_cfg.target, view_db) as tgt:
        for query in (
            "SELECT count(*), sum(tier) FROM v.simple",
            "SELECT count(*), sum(doubled) FROM v.expressions",
            "SELECT count(*), sum(total) FROM v.joined",
            "SELECT count(*) FROM v.layered",
        ):
            assert await src.fetchrow(query) == await tgt.fetchrow(query), query

        # A materialized view is copied as a relation, so it holds rows of its
        # own rather than being recomputed.
        assert await tgt.fetchval("SELECT count(*) FROM v.matview") == await src.fetchval(
            "SELECT count(*) FROM v.matview"
        )


async def test_a_genuinely_changed_view_is_still_reported(view_cfg, view_db):
    """The normalisation must not swallow real differences."""
    await bootstrap(view_cfg, database=view_db)

    async with connect(view_cfg.target, view_db) as tgt:
        await tgt.execute(
            "CREATE OR REPLACE VIEW v.simple AS SELECT id, login, 0 AS tier FROM v.accounts"
        )

    drift = await detect_drift(view_cfg, view_db)
    changed = [i for i in drift.items if i.object_type == "view" and i.name == "simple"]
    assert changed, (
        "a view whose definition really differs was normalised away: "
        f"{drift.summary}"
    )
    assert changed[0].drift_type == "different"
