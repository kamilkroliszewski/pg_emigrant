"""P0: ``exclude_tables`` is honoured everywhere, or not offered at all.

The setting existed in the config model and in config.yaml.example and was
read by nothing — the worst shape a configuration option can have, because an
operator who excluded a huge audit table to shorten a cutover window got it
migrated anyway and had no way to tell.

An exclusion is only meaningful if every stage agrees on it.  Honouring it in
the copy but not in the publication leaves the subscriber retrying a tablesync
against a table that does not exist on the target, forever; honouring it in the
copy but not in drift detection makes every ``detect-ddl`` report a permanent
difference that ``--apply`` then tries to "fix".
"""

from __future__ import annotations

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.db import connect
from pg_emigrant.ddl_detector import detect_drift
from pg_emigrant.preflight import ERROR, run_preflight
from pg_emigrant.report import BootstrapIncomplete
from tests.helpers.replication import wait_for_catchup
from tests.helpers.verify import assert_tables_identical

pytestmark = [pytest.mark.integration, pytest.mark.slow]


async def test_excluded_table_is_absent_from_every_stage(cfg, source_db):
    """Not created, not copied, not published, not replicated, not drift."""
    cfg.exclude_tables = ["app.audit_log", "app.documents"]

    report = await bootstrap(cfg, database=source_db)
    assert report.passed, report.summary

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        # Not created on the target.
        for table in ("audit_log", "documents"):
            assert not await tgt.fetchval(
                "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
                " WHERE n.nspname = 'app' AND c.relname = $1",
                table,
            ), f"excluded table app.{table} was created on the target"

        # Not a publication member — otherwise the subscriber would receive
        # changes for a table it does not have.
        published = {
            r["tablename"]
            for r in await src.fetch(
                "SELECT tablename FROM pg_publication_tables WHERE schemaname = 'app'"
            )
        }
        assert "audit_log" not in published and "documents" not in published, (
            f"excluded tables are still published: {sorted(published)}"
        )

        # Everything else migrated faithfully.
        await assert_tables_identical(
            src, tgt, ["app", "reporting"],
            ignore={("app", "audit_log"), ("app", "documents")},
        )

    # Not reported as drift, and therefore not "fixed" by detect-ddl --apply.
    drift = await detect_drift(cfg, source_db)
    names = {(i.schema, i.table or i.name) for i in drift.items}
    assert ("app", "audit_log") not in names and ("app", "documents") not in names, (
        f"an excluded table is reported as drift: {drift.summary} {names}"
    )


async def test_writes_to_an_excluded_table_are_not_replicated(cfg, source_db):
    """The exclusion has to hold for the streaming half too, not just the copy."""
    cfg.exclude_tables = ["app.audit_log"]
    await bootstrap(cfg, database=source_db)

    async with connect(cfg.source, source_db) as src:
        await src.execute(
            "INSERT INTO app.audit_log (actor, action) VALUES ('post', 'excluded')"
        )
        await src.execute(
            "INSERT INTO app.nasty_strings (id, val, note) VALUES (777, 'v', 'included')"
        )

    await wait_for_catchup(cfg, source_db)

    async with connect(cfg.target, source_db) as tgt:
        assert await tgt.fetchval(
            "SELECT count(*) FROM app.nasty_strings WHERE id = 777"
        ) == 1, "a non-excluded table stopped replicating"
        assert not await tgt.fetchval(
            "SELECT to_regclass('app.audit_log')"
        ), "the excluded table appeared on the target"


async def test_excluding_a_referenced_table_is_refused(cfg, source_db):
    """A migrated table cannot reference a table that is never copied."""
    cfg.exclude_tables = ["app.customers"]  # app.orders has an FK to it

    report = await run_preflight(cfg, database=source_db)
    matching = [c for c in report.checks
                if c.name == "exclude_tables" and c.status == ERROR]
    assert matching, (
        "preflight accepted an exclusion whose foreign key could never be "
        f"satisfied on the target: {[c.summary for c in report.checks if c.name == 'exclude_tables']}"
    )
    assert "orders" in matching[0].detail

    # And the migration itself must not quietly produce a target missing the
    # constraint either.
    with pytest.raises(BootstrapIncomplete):
        await bootstrap(cfg, database=source_db)


async def test_glob_patterns_and_bare_names(cfg, source_db):
    """``events_*`` and a bare table name both select what they claim to."""
    cfg.exclude_tables = ["app.events*", "nasty_strings"]

    report = await bootstrap(cfg, database=source_db)
    assert report.passed, report.summary

    async with connect(cfg.target, source_db) as tgt:
        for name in ("events", "events_2024", "events_2025", "nasty_strings"):
            assert not await tgt.fetchval(f"SELECT to_regclass('app.{name}')"), (
                f"app.{name} should have been excluded"
            )
        assert await tgt.fetchval("SELECT to_regclass('app.customers')"), (
            "the glob excluded more than it should have"
        )


async def test_no_exclusions_configured_changes_nothing(cfg, source_db):
    """The default (empty) must behave exactly as before the feature existed."""
    assert cfg.exclude_tables == []
    report = await bootstrap(cfg, database=source_db)
    assert report.passed

    async with connect(cfg.source, source_db) as src, connect(cfg.target, source_db) as tgt:
        await assert_tables_identical(src, tgt, ["app", "reporting"])
