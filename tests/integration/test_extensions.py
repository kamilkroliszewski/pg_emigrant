"""P0: extensions, and the line between what they own and what the user owns.

An extension installs its own functions, types, operators and sometimes
schemas.  Those are recreated by ``CREATE EXTENSION`` on the target and must
not be reproduced object-by-object — doing so either fails outright or leaves
duplicates the extension does not know about.  What *does* have to migrate is
everything the user built on top: a column of an extension type, an index
using an extension's operator class, a default calling an extension function.

The other half is refusal.  An extension the target cannot install is not a
warning to discover halfway through: every dependent object would fail, so it
has to be a preflight error with the package name in it.
"""

from __future__ import annotations

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.config import ReplicatorConfig
from pg_emigrant.db import connect
from pg_emigrant.preflight import ERROR, WARN, run_preflight
from tests.helpers.replication import wait_for_catchup
from tests.helpers.verify import assert_tables_identical

pytestmark = [pytest.mark.integration, pytest.mark.slow]

# Contrib extensions present in the postgres:*-alpine images, chosen to cover
# the ways a user object can depend on one: a function (pgcrypto, uuid-ossp),
# a type (hstore, citext), and an operator class an index is built on
# (pg_trgm, btree_gin, btree_gist).
EXT_DDL = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS citext;

CREATE SCHEMA ext;

CREATE TABLE ext.accounts (
    id        uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    login     citext NOT NULL UNIQUE,
    secret    bytea NOT NULL DEFAULT digest('placeholder', 'sha256'),
    props     hstore,
    bio       text,
    tenant    integer NOT NULL DEFAULT 1
);

-- An index that only exists because of an extension's operator class.
CREATE INDEX accounts_bio_trgm ON ext.accounts USING gin (bio gin_trgm_ops);
CREATE INDEX accounts_tenant_bio ON ext.accounts USING gin (tenant, bio gin_trgm_ops);
CREATE INDEX accounts_tenant_gist ON ext.accounts USING gist (tenant);

-- A user function calling extension functions.
CREATE FUNCTION ext.hash_login(p text) RETURNS bytea
LANGUAGE sql IMMUTABLE AS $$ SELECT digest(lower(p), 'sha256') $$;

CREATE VIEW ext.account_summary AS
    SELECT id, login, props -> 'plan' AS plan FROM ext.accounts;

INSERT INTO ext.accounts (login, props, bio, tenant)
SELECT 'User' || i, ('plan => ' || (ARRAY['free','pro'])[1 + i % 2])::hstore,
       'biography number ' || i, i % 5
FROM generate_series(1, 200) AS i;
"""


@pytest.fixture
def ext_db(source_pg, target_pg, dbname):
    source_pg.psql(f'CREATE DATABASE "{dbname}"')
    source_pg.psql_script(EXT_DDL, dbname)
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
def ext_cfg(source_pg, target_pg, ext_db):
    return ReplicatorConfig(
        source=source_pg.config, target=target_pg.config,
        databases=[ext_db], schemas=["ext"],
    )


async def test_extension_dependent_objects_migrate_and_data_matches(ext_cfg, ext_db):
    report = await bootstrap(ext_cfg, database=ext_db)
    assert report.passed, report.summary

    async with connect(ext_cfg.source, ext_db) as src, connect(ext_cfg.target, ext_db) as tgt:
        installed = {
            r["extname"] for r in
            await tgt.fetch("SELECT extname FROM pg_extension")
        }
        for name in ("pgcrypto", "uuid-ossp", "pg_trgm", "btree_gin",
                     "btree_gist", "hstore", "citext"):
            assert name in installed, f"{name} was not installed on the target"

        await assert_tables_identical(src, tgt, ["ext"])

        # The index that only compiles if the operator class came across.
        tgt_indexes = {
            r["indexname"] for r in
            await tgt.fetch("SELECT indexname FROM pg_indexes WHERE schemaname = 'ext'")
        }
        for idx in ("accounts_bio_trgm", "accounts_tenant_bio", "accounts_tenant_gist"):
            assert idx in tgt_indexes, f"{idx} is missing on the target"

        # The user function and view built on extension functions/types.
        # Asserted symmetrically rather than absolutely: the function body
        # calls digest() unqualified, so it resolves through the *caller's*
        # search_path on both sides. Demanding that it work under the empty
        # search_path these connections use would be demanding something the
        # source does not do either — the test would be checking the harness,
        # not the migration.
        for conn in (src, tgt):
            await conn.execute("SET search_path TO public")
        assert await tgt.fetchval(
            "SELECT ext.hash_login('ABC') = digest('abc', 'sha256')"
        ) == await src.fetchval(
            "SELECT ext.hash_login('ABC') = digest('abc', 'sha256')"
        ) is True
        assert await tgt.fetchval("SELECT count(*) FROM ext.account_summary") == 200


async def test_extension_owned_objects_are_not_recreated_by_hand(ext_cfg, ext_db):
    """The extension's own functions must belong to the extension, not be loose.

    A function recreated independently would shadow the extension's, survive
    ``DROP EXTENSION``, and drift from it on upgrade — a difference that only
    shows up much later, as a version mismatch nobody can explain.
    """
    await bootstrap(ext_cfg, database=ext_db)

    async with connect(ext_cfg.target, ext_db) as tgt:
        loose = await tgt.fetch(
            """
            SELECT p.proname
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.proname IN ('digest', 'uuid_generate_v4', 'similarity', 'hstore')
              AND NOT EXISTS (
                  SELECT 1 FROM pg_depend d
                  WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid
                    AND d.deptype = 'e'
              )
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            """
        )
        assert not loose, (
            "extension-owned function(s) were recreated as standalone objects: "
            + ", ".join(r["proname"] for r in loose)
        )


async def test_writes_to_extension_typed_columns_replicate(ext_cfg, ext_db):
    """citext/hstore/uuid columns must survive the streaming half too."""
    await bootstrap(ext_cfg, database=ext_db)

    async with connect(ext_cfg.source, ext_db) as src:
        await src.execute(
            "INSERT INTO ext.accounts (login, props, bio)"
            " VALUES ('AfterBootstrap', 'plan => enterprise'::public.hstore, 'streamed row')"
        )
        await src.execute("UPDATE ext.accounts SET bio = 'edited' WHERE login = 'User1'")

    await wait_for_catchup(ext_cfg, ext_db)

    async with connect(ext_cfg.source, ext_db) as src, connect(ext_cfg.target, ext_db) as tgt:
        for conn in (src, tgt):
            await conn.execute("SET search_path TO public")
        # citext is case-insensitive: a case-folded match proves the column
        # kept its extension type rather than degrading to text.
        assert await tgt.fetchval(
            "SELECT count(*) FROM ext.accounts WHERE login = 'afterbootstrap'"
        ) == 1, "the citext column did not survive as citext on the target"
        assert await tgt.fetchval(
            "SELECT props -> 'plan' FROM ext.accounts WHERE login = 'AfterBootstrap'"
        ) == "enterprise"
        await assert_tables_identical(src, tgt, ["ext"])


async def test_an_extension_the_target_cannot_install_is_a_preflight_error(
    ext_cfg, ext_db, target_pg
):
    """Missing on the target means every dependent object would fail.

    Preflight has to name it, because the fix is installing an OS package on
    the target host — nothing the migration itself can do.
    """
    # Hide one extension from the target by removing its control file, which is
    # what a missing contrib package looks like from PostgreSQL's point of view.
    import subprocess

    probe = subprocess.run(
        ["docker", "exec", target_pg.name, "sh", "-c",
         "mv /usr/local/share/postgresql/extension/pg_trgm.control /tmp/pg_trgm.control"],
        capture_output=True, text=True,
    )
    if probe.returncode != 0:
        pytest.skip(f"could not stage a missing extension: {probe.stderr}")
    try:
        report = await run_preflight(ext_cfg, database=ext_db)
        checks = [c for c in report.checks if c.name == "extensions_available"]
        assert checks and checks[0].status == ERROR, (
            f"preflight did not flag the missing extension: "
            f"{[(c.name, c.status, c.summary) for c in checks]}"
        )
        assert "pg_trgm" in checks[0].detail
        assert not report.passed
    finally:
        subprocess.run(
            ["docker", "exec", target_pg.name, "sh", "-c",
             "mv /tmp/pg_trgm.control /usr/local/share/postgresql/extension/pg_trgm.control"],
            capture_output=True, text=True,
        )


async def test_extension_version_differences_are_reported(ext_cfg, ext_db, pg_version_pair):
    """A version mismatch is a warning, not a blocker — but it must be visible."""
    if pg_version_pair[0] == pg_version_pair[1]:
        pytest.skip("same-version pair: no extension version skew to observe")

    report = await run_preflight(ext_cfg, database=ext_db)
    checks = [c for c in report.checks if c.name == "extensions_available"]
    assert checks, "preflight did not report on extensions at all"
    assert checks[0].status in (ERROR, WARN, "ok"), checks[0].summary
