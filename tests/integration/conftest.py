"""Fixtures that provision real PostgreSQL clusters and fixture databases."""

from __future__ import annotations

import subprocess
import uuid
from pathlib import Path

import pytest
import pytest_asyncio

from pg_emigrant.config import ReplicatorConfig
from pg_emigrant.db import connect
from tests.helpers.pg import PgContainer, start_pg

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "fixtures"

# One container per version, reused for the whole session.  Starting a
# PostgreSQL container costs seconds; creating a database inside a running one
# costs milliseconds, so per-test isolation is done with fresh database names.
_CONTAINERS: dict[str, PgContainer] = {}


# Roles the fixture's objects are owned by / granted to.  A production target
# is normally pre-provisioned with its roles by configuration management
# before pg_emigrant ever runs (roles are cluster-wide and carry passwords and
# attributes the tool deliberately does not invent), so the test clusters are
# provisioned the same way.
FIXTURE_ROLES = ("app_owner", "app_reader")


def _container(version: str, key: str | None = None) -> PgContainer:
    key = key or version
    if key not in _CONTAINERS:
        container = start_pg(version)
        for role in FIXTURE_ROLES:
            container.psql(
                f"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles "
                f"WHERE rolname = '{role}') THEN CREATE ROLE {role} NOLOGIN; END IF; END $$;"
            )
        _CONTAINERS[key] = container
    return _CONTAINERS[key]


@pytest.fixture(scope="session", autouse=True)
def _cleanup_containers():
    yield
    for c in _CONTAINERS.values():
        c.stop()
    _CONTAINERS.clear()


@pytest.fixture(scope="session")
def source_pg(pg_version_pair) -> PgContainer:
    return _container(pg_version_pair[0])


@pytest.fixture(scope="session")
def target_pg(pg_version_pair) -> PgContainer:
    src_v, tgt_v = pg_version_pair
    if src_v == tgt_v:
        # Same version on both sides still needs two independent clusters —
        # a migration into the cluster it reads from is exactly what the
        # same-cluster guard exists to refuse.
        return _container(tgt_v, key=f"{tgt_v}#target")
    return _container(tgt_v)


@pytest.fixture
def dbname() -> str:
    """A database name unique to this test.

    Includes a hyphen and an upper-case letter on purpose: those are exactly
    the characters that must survive identifier quoting and the slot-name
    sanitisation, and a name that never exercises them would hide a whole
    class of quoting bug.
    """
    return f"Test-db_{uuid.uuid4().hex[:8]}"


def load_fixture(container: PgContainer, db: str, *, with_data: bool = True) -> None:
    """Create *db* on *container* and load the migration fixture into it."""
    container.psql(f'CREATE DATABASE "{db}"')
    for name in ("schema.sql",) + (("data.sql",) if with_data else ()):
        # :"dbname" is psql's own identifier-substitution syntax; passing the
        # name with -v keeps the quoting server-side.
        container.psql_script((FIXTURE_DIR / name).read_text(), db, dbname=db)


@pytest.fixture
def source_db(source_pg, dbname) -> str:
    """A source database with the full migration fixture loaded."""
    load_fixture(source_pg, dbname)
    yield dbname
    drop_database(source_pg, dbname)


def _lit(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def drop_database(container: PgContainer, db: str) -> None:
    """Force-drop *db*, terminating whatever still holds a connection to it."""
    try:
        container.psql(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            f"WHERE datname = {_lit(db)} AND pid <> pg_backend_pid()"
        )
        container.psql(f'DROP DATABASE IF EXISTS "{db}"')
    except subprocess.CalledProcessError:
        pass  # best-effort teardown; the container dies with the session anyway


@pytest.fixture
def cfg(source_pg, target_pg, source_db) -> ReplicatorConfig:
    """A ReplicatorConfig wired to the two throw-away clusters."""
    return ReplicatorConfig(
        source=source_pg.config,
        target=target_pg.config,
        databases=[source_db],
        parallel_workers=4,
        table_parallel_workers=2,
        sequence_sync_interval=1,
    )


@pytest_asyncio.fixture
async def src_conn(cfg, source_db):
    async with connect(cfg.source, source_db) as conn:
        yield conn


@pytest_asyncio.fixture
async def tgt_conn(cfg, source_db):
    async with connect(cfg.target, source_db) as conn:
        yield conn
