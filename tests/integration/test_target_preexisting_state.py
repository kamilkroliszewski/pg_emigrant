"""P0: the target is not assumed to be empty.

The intended production shape is a freshly provisioned cluster whose roles,
databases and sometimes schemas have already been created by configuration
management.  So "the target already contains things" is the normal case, not
an edge case, and the rule is that pg_emigrant may create what is missing and
must never silently destroy what it did not create.
"""

from __future__ import annotations

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.db import connect
from pg_emigrant.report import BootstrapIncomplete
from tests.helpers.verify import assert_tables_identical

pytestmark = [pytest.mark.integration, pytest.mark.slow]

SCHEMAS = ["app", "reporting"]


async def test_precreated_database_and_schemas_are_reused(cfg, source_db, target_pg):
    """A target pre-provisioned by Ansible must migrate exactly like an empty one."""
    target_pg.psql(f'CREATE DATABASE "{source_db}"')
    target_pg.psql("CREATE SCHEMA app", dbname=source_db)
    target_pg.psql("CREATE SCHEMA reporting", dbname=source_db)

    await bootstrap(cfg, database=source_db)

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        await assert_tables_identical(src, tgt, SCHEMAS)


async def test_unrelated_target_data_survives_the_migration(cfg, source_db, target_pg):
    """Tables outside the migration's scope must not be touched.

    The initial copy clears the target tables it is about to load.  Doing that
    with an unqualified CASCADE also empties every table that references them
    by foreign key — including tables in schemas the migration was never asked
    to touch.  Those hold real data belonging to something else on the same
    cluster, and losing them would be silent: nothing in the migration's own
    consistency checks looks outside its scope.
    """
    target_pg.psql(f'CREATE DATABASE "{source_db}"')
    target_pg.psql("CREATE SCHEMA app", dbname=source_db)
    target_pg.psql("CREATE SCHEMA other", dbname=source_db)
    target_pg.psql(
        "CREATE TABLE app.customers (id bigint PRIMARY KEY, email text, "
        "display_name text)",
        dbname=source_db,
    )
    target_pg.psql(
        "CREATE TABLE other.local_notes (id int PRIMARY KEY, "
        "customer_id bigint REFERENCES app.customers(id), body text)",
        dbname=source_db,
    )
    target_pg.psql("INSERT INTO app.customers VALUES (1, 'a@b.c', 'pre-existing')",
                   dbname=source_db)
    target_pg.psql("INSERT INTO other.local_notes VALUES (1, 1, 'not part of this migration')",
                   dbname=source_db)

    cfg.exclude_schemas = ["other"]
    try:
        await bootstrap(cfg, database=source_db)
    except RuntimeError:  # BootstrapIncomplete is one
        # Whether this particular target shape is migratable at all is a
        # separate question; what must hold either way is the invariant below.
        pass

    async with connect(cfg.target, source_db) as tgt:
        survivors = await tgt.fetchval("SELECT count(*) FROM other.local_notes")
    assert survivors == 1, (
        "a row in an out-of-scope schema was destroyed by the migration — "
        "TRUNCATE ... CASCADE reached beyond the tables being migrated"
    )


async def test_target_table_missing_a_column_is_reconciled(cfg, source_db, target_pg):
    """A pre-existing target table narrower than the source gains the columns.

    This is a reconcilable difference — the columns are simply added — and the
    result must be a faithful copy, not a copy of the column intersection.
    """
    target_pg.psql(f'CREATE DATABASE "{source_db}"')
    target_pg.psql("CREATE SCHEMA app", dbname=source_db)
    # 'note' and 'placed_at' exist on the source but not here.
    target_pg.psql(
        "CREATE TABLE app.orders (id integer PRIMARY KEY, customer_id bigint, "
        "total_cents bigint)",
        dbname=source_db,
    )

    await bootstrap(cfg, database=source_db)

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        await assert_tables_identical(src, tgt, ["app"])


async def test_target_column_type_mismatch_is_refused(cfg, source_db, target_pg):
    """A same-named column of a different type must fail closed.

    Nothing reconciles this: the column is not missing, so it is never added,
    and CSV COPY loads a bigint into a text column without complaint.  The run
    would report success while the target quietly stopped being the same data —
    and every row-count check would agree with it.
    """
    target_pg.psql(f'CREATE DATABASE "{source_db}"')
    target_pg.psql("CREATE SCHEMA app", dbname=source_db)
    target_pg.psql(
        "CREATE TABLE app.orders (id integer PRIMARY KEY, customer_id bigint, "
        "total_cents text, note text, placed_at timestamptz)",
        dbname=source_db,
    )

    with pytest.raises(BootstrapIncomplete) as excinfo:
        await bootstrap(cfg, database=source_db)

    problems = " ".join(excinfo.value.report.databases[0].problems)
    assert "orders" in problems and "total_cents" in problems, (
        f"the failure did not name the offending column: {problems}"
    )


async def test_a_refused_copy_leaves_the_target_untouched(cfg, source_db, target_pg):
    """The refusal must happen before anything is cleared.

    Discovering an unmigratable target half way through would leave it emptied
    for a problem that was knowable up front.
    """
    target_pg.psql(f'CREATE DATABASE "{source_db}"')
    target_pg.psql("CREATE SCHEMA app", dbname=source_db)
    target_pg.psql(
        "CREATE TABLE app.orders (id integer PRIMARY KEY, customer_id bigint, "
        "total_cents text, note text, placed_at timestamptz)",
        dbname=source_db,
    )
    target_pg.psql("INSERT INTO app.orders VALUES (1, 1, 'x', 'keep me', now())",
                   dbname=source_db)

    with pytest.raises(BootstrapIncomplete):
        await bootstrap(cfg, database=source_db)

    async with connect(cfg.target, source_db) as tgt:
        assert await tgt.fetchval("SELECT count(*) FROM app.orders") == 1
