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
_SYSTEM_SCHEMAS = {
    "information_schema",
    "pg_catalog",
    "pg_toast",
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
        return list(cfg.schemas)

    rows = await conn.fetch(
        """
        SELECT nspname
        FROM pg_namespace
        WHERE nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
          AND nspname NOT LIKE 'pg_temp_%'
          AND nspname NOT LIKE 'pg_toast_temp_%'
        ORDER BY nspname
        """
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
