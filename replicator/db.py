"""asyncpg connection pool management for source and target databases."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

import asyncpg

from replicator.config import DatabaseConfig, ReplicatorConfig
from replicator.utils import get_logger

log = get_logger(__name__)


def _dsn(cfg: DatabaseConfig, dbname: str | None = None) -> str:
    """Build a PostgreSQL DSN string from config."""
    db = dbname or cfg.dbname
    return (
        f"postgresql://{cfg.user}:{cfg.password}"
        f"@{cfg.host}:{cfg.port}/{db}"
        f"?sslmode={cfg.sslmode}"
    )


@asynccontextmanager
async def connect(
    cfg: DatabaseConfig, dbname: str | None = None
) -> AsyncIterator[asyncpg.Connection]:
    """Open a single connection and yield it, closing on exit."""
    dsn = _dsn(cfg, dbname)
    conn = await asyncpg.connect(dsn)
    try:
        yield conn
    finally:
        await conn.close()


@asynccontextmanager
async def create_pool(
    cfg: DatabaseConfig,
    dbname: str | None = None,
    min_size: int = 2,
    max_size: int = 10,
) -> AsyncIterator[asyncpg.Pool]:
    """Create a connection pool and yield it, closing on exit."""
    dsn = _dsn(cfg, dbname)
    pool = await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size)
    try:
        yield pool
    finally:
        await pool.close()


# System schemas that are never migrated regardless of configuration.
# Includes PostgreSQL built-ins and TimescaleDB internal schemas — those are
# managed exclusively by the extension and must not be manually reproduced.
_SYSTEM_SCHEMAS = {
    "information_schema",
    "pg_catalog",
    "pg_toast",
    # TimescaleDB-managed internal schemas
    "_timescaledb_catalog",
    "_timescaledb_config",
    "_timescaledb_internal",
    "_timescaledb_cache",
    "timescaledb_information",
    "timescaledb_experimental",
    "toolkit_experimental",
}


async def discover_schemas(
    conn: asyncpg.Connection,
    cfg: ReplicatorConfig,
) -> list[str]:
    """Return user-defined schemas present in the connected database.

    If ``cfg.schemas`` is non-empty those are returned directly (explicit list
    always wins).  Otherwise every schema that is not a PostgreSQL internal
    schema (``pg_catalog``, ``information_schema``, ``pg_toast``,
    ``pg_temp_*``, ``pg_toast_temp_*``) is included.
    """
    if cfg.schemas:
        # Warn about schemas that exist in the source database but are not in
        # the explicit list — they will be silently skipped during migration
        # and drift detection, which can lead to surprises like a missing
        # pgboss (or any other extension-managed) schema on the target.
        all_rows = await conn.fetch(
            """
            SELECT nspname
            FROM pg_namespace
            WHERE nspname NOT LIKE 'pg_temp_%'
              AND nspname NOT LIKE 'pg_toast_temp_%'
              AND nspname != ALL($1::text[])
            ORDER BY nspname
            """,
            list(_SYSTEM_SCHEMAS),
        )
        all_source_schemas = {r["nspname"] for r in all_rows}
        uncovered = all_source_schemas - set(cfg.schemas)
        if uncovered:
            log.warning(
                "Explicit schemas list is set but the following schemas exist on "
                "the source and will be SKIPPED (not checked/migrated): %s",
                sorted(uncovered),
            )
        return list(cfg.schemas)

    rows = await conn.fetch(
        """
        SELECT nspname
        FROM pg_namespace
        WHERE nspname NOT LIKE 'pg_temp_%'
          AND nspname NOT LIKE 'pg_toast_temp_%'
          AND nspname != ALL($1::text[])
        ORDER BY nspname
        """,
        list(_SYSTEM_SCHEMAS),
    )
    schemas = [r["nspname"] for r in rows]
    log.info("Discovered schemas: %s", schemas)
    return schemas


async def discover_databases(cfg: ReplicatorConfig) -> list[str]:
    """Return the list of user databases on the source server.

    Databases listed in ``exclude_databases`` are filtered out.
    If ``cfg.databases`` is non-empty, those are returned directly.
    """
    if cfg.databases:
        return list(cfg.databases)

    async with connect(cfg.source) as conn:
        rows = await conn.fetch(
            "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"
        )

    all_dbs = [r["datname"] for r in rows]
    filtered = [d for d in all_dbs if d not in cfg.exclude_databases]
    log.info("Discovered databases: %s", filtered)
    return filtered
