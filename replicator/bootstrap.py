"""Bootstrap orchestrator — full initial migration lifecycle.

Sequence:
  1. Discover databases on source
  2. Create databases on target
  3. For each database:
     a. Synchronize schemas (tables, columns, indexes, constraints, sequences)
     b. Copy initial data (parallel COPY with snapshot isolation)
     c. Create publication on source
     d. Create subscription on target
"""

from __future__ import annotations

from rich.progress import Progress, SpinnerColumn, TextColumn

from replicator.config import ReplicatorConfig
from replicator.data_copy import copy_all_tables
from replicator.db import connect, discover_databases
from replicator.replication import create_publication, create_subscription
from replicator.schema_sync import get_tables, sync_deferred_indexes, sync_schemas
from replicator.utils import console, get_logger

log = get_logger(__name__)


async def ensure_database_exists(cfg: ReplicatorConfig, dbname: str) -> None:
    """Create the database on the target if it doesn't already exist."""
    async with connect(cfg.target) as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", dbname
        )
        if not exists:
            # CREATE DATABASE cannot run inside a transaction
            await conn.execute(f'CREATE DATABASE "{dbname}";')
            log.info("Created database %s on target", dbname)
        else:
            log.debug("Database %s already exists on target", dbname)


async def bootstrap(cfg: ReplicatorConfig, database: str | None = None) -> None:
    """Run the full bootstrap migration."""
    console.rule("[bold green]pg_emigrant bootstrap")

    # Step 1: discover databases
    if database:
        databases = [database]
    else:
        databases = await discover_databases(cfg)
    console.print(f"Databases to migrate: {databases}")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        for dbname in databases:
            task = progress.add_task(f"Migrating {dbname}…", total=None)

            # Step 2: ensure database exists on target
            progress.update(task, description=f"[{dbname}] Creating database…")
            await ensure_database_exists(cfg, dbname)

            # Step 3: synchronize schemas
            progress.update(task, description=f"[{dbname}] Syncing schemas…")
            async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
                await sync_schemas(src, tgt, cfg.schemas)

            # Step 4: copy initial data
            progress.update(task, description=f"[{dbname}] Copying data…")
            async with connect(cfg.source, dbname) as src:
                tables = await get_tables(src, cfg.schemas)

            if tables:
                results = await copy_all_tables(cfg, dbname, tables)
                total_rows = sum(c for c in results.values() if c >= 0)
                console.print(
                    f"  [{dbname}] Copied {total_rows} rows across {len(results)} tables"
                )
            else:
                console.print(f"  [{dbname}] No tables to copy")

            # Step 4b: create non-unique indexes after COPY (faster than during insert)
            progress.update(task, description=f"[{dbname}] Creating indexes…")
            async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
                await sync_deferred_indexes(src, tgt, cfg.schemas)

            # Step 5: create publication + subscription
            progress.update(task, description=f"[{dbname}] Setting up replication…")
            await create_publication(cfg, dbname)
            await create_subscription(cfg, dbname)

            progress.update(task, description=f"[{dbname}] ✓ Done")
            progress.remove_task(task)

    console.rule("[bold green]Bootstrap complete")
