"""CLI entry point for pg_emigrant — built with typer + rich."""

from __future__ import annotations

import asyncio
from typing import Optional

import typer
from rich.table import Table

from replicator.config import load_config
from replicator.utils import console, setup_logging

app = typer.Typer(
    name="replicator",
    help="pg_emigrant — PostgreSQL migration & replication orchestrator",
    add_completion=False,
)


def _run(coro):
    """Run an async coroutine from the synchronous CLI layer."""
    return asyncio.run(coro)


@app.callback()
def main(verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging")):
    """pg_emigrant: migrate and replicate PostgreSQL databases."""
    setup_logging(verbose)


@app.command()
def bootstrap(
    config: str = typer.Option("config.yaml", "--config", "-c", help="Path to config file"),
    database: Optional[str] = typer.Option(None, "--database", "-d", help="Bootstrap only this database (default: all discovered)"),
):
    """Run full bootstrap migration: discover → schema sync → data copy → replication setup."""
    from replicator.bootstrap import bootstrap as do_bootstrap

    cfg = load_config(config)
    _run(do_bootstrap(cfg, database=database))


@app.command()
def start(
    config: str = typer.Option("config.yaml", "--config", "-c"),
    database: Optional[str] = typer.Option(None, "--database", "-d", help="Specific database"),
):
    """Start (enable) logical replication subscriptions."""
    from replicator.db import discover_databases
    from replicator.replication import enable_subscription

    cfg = load_config(config)

    async def _start():
        dbs = [database] if database else await discover_databases(cfg)
        for db in dbs:
            await enable_subscription(cfg, db)
            console.print(f"[green]Enabled replication for {db}")

    _run(_start())


@app.command()
def stop(
    config: str = typer.Option("config.yaml", "--config", "-c"),
    database: Optional[str] = typer.Option(None, "--database", "-d"),
):
    """Stop (disable) logical replication subscriptions."""
    from replicator.db import discover_databases
    from replicator.replication import disable_subscription

    cfg = load_config(config)

    async def _stop():
        dbs = [database] if database else await discover_databases(cfg)
        for db in dbs:
            await disable_subscription(cfg, db)
            console.print(f"[yellow]Disabled replication for {db}")

    _run(_stop())


@app.command()
def teardown(
    config: str = typer.Option("config.yaml", "--config", "-c"),
    database: Optional[str] = typer.Option(None, "--database", "-d"),
):
    """Remove subscriptions, publications, and replication slots."""
    from replicator.db import discover_databases
    from replicator.replication import drop_publication, drop_subscription

    cfg = load_config(config)

    async def _teardown():
        dbs = [database] if database else await discover_databases(cfg)
        for db in dbs:
            await drop_subscription(cfg, db)
            await drop_publication(cfg, db)
            console.print(f"[red]Torn down replication for {db}")

    _run(_teardown())


@app.command()
def status(
    config: str = typer.Option("config.yaml", "--config", "-c"),
    database: Optional[str] = typer.Option(None, "--database", "-d", help="Show status for a specific database only"),
):
    """Display replication status, lag, sequence sync, and drift for all databases."""
    from replicator.monitor import build_status

    cfg = load_config(config)
    _run(build_status(cfg, database=database))


@app.command(name="sync-sequences")
def sync_sequences(
    config: str = typer.Option("config.yaml", "--config", "-c"),
    database: Optional[str] = typer.Option(None, "--database", "-d"),
    loop: bool = typer.Option(False, "--loop", help="Run continuously"),
):
    """Synchronize sequences from source to target."""
    from replicator.db import discover_databases
    from replicator.sequence_sync import run_sequence_sync_loop, sync_sequences_once

    cfg = load_config(config)

    async def _sync():
        dbs = [database] if database else await discover_databases(cfg)

        if loop:
            tasks = [run_sequence_sync_loop(cfg, db) for db in dbs]
            await asyncio.gather(*tasks)
        else:
            for db in dbs:
                report = await sync_sequences_once(cfg, db)
                tbl = Table(title=f"Sequence Sync — {db}", show_lines=True)
                tbl.add_column("Schema")
                tbl.add_column("Sequence")
                tbl.add_column("Source")
                tbl.add_column("Target")
                tbl.add_column("Status")
                for r in report:
                    tbl.add_row(
                        r["schema"], r["sequence"],
                        str(r["source_value"]), str(r["target_value"]),
                        r["status"],
                    )
                console.print(tbl)

    _run(_sync())


@app.command(name="detect-ddl")
def detect_ddl(
    config: str = typer.Option("config.yaml", "--config", "-c"),
    database: Optional[str] = typer.Option(None, "--database", "-d"),
    apply: bool = typer.Option(False, "--apply", help="Apply fixes for missing objects"),
    drop_extra: bool = typer.Option(
        False, "--drop-extra",
        help="Also DROP tables/objects on target that no longer exist on source (destructive!)",
    ),
):
    """Detect schema drift between source and target."""
    from replicator.db import discover_databases
    from replicator.ddl_detector import apply_drift_fixes, detect_drift

    cfg = load_config(config)

    async def _detect():
        dbs = [database] if database else await discover_databases(cfg)
        for db in dbs:
            report = await detect_drift(cfg, db)
            console.rule(f"[bold]Drift Report — {db}")

            if not report.has_drift:
                console.print("[green]No schema drift detected")
                continue

            tbl = Table(title=f"Schema Drift — {db}", show_lines=True)
            tbl.add_column("Type")
            tbl.add_column("Schema")
            tbl.add_column("Table")
            tbl.add_column("Name")
            tbl.add_column("Drift")
            tbl.add_column("Detail")
            tbl.add_column("Fix DDL")

            for item in report.items:
                style = ""
                if item.drift_type == "missing_on_target":
                    style = "yellow"
                elif item.drift_type == "missing_on_source":
                    style = "red"
                elif item.drift_type == "different":
                    style = "red"
                # Truncate long DDL for readability in the table
                ddl_preview = item.fix_ddl
                if ddl_preview and len(ddl_preview) > 80:
                    ddl_preview = ddl_preview[:77] + "..."
                tbl.add_row(
                    item.object_type,
                    item.schema,
                    item.table,
                    item.name,
                    item.drift_type,
                    item.detail,
                    ddl_preview or "—",
                    style=style,
                )
            console.print(tbl)

            if apply:
                if drop_extra:
                    console.print(
                        "[bold red]WARNING:[/bold red] --drop-extra will DROP tables on target "
                        "that do not exist on source. This is destructive!"
                    )
                applied = await apply_drift_fixes(cfg, db, report, drop_extra=drop_extra)
                console.print(f"[green]Applied {applied} fix(es) for {db}")
            elif report.has_drift:
                console.print(
                    "[dim]Run with [bold]--apply[/bold] to fix missing objects, "
                    "or [bold]--apply --drop-extra[/bold] to also drop extra tables.[/dim]"
                )

    _run(_detect())


if __name__ == "__main__":
    app()
