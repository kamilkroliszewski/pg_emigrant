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
    format: str = typer.Option("rich", "--format", "-f", help="Output format: rich (default), simple (grep-friendly), json"),
    show_subscription: bool = typer.Option(False, "--subscription", help="Show subscription status"),
    show_slots: bool = typer.Option(False, "--slots", help="Show replication slots"),
    show_lag: bool = typer.Option(False, "--lag", help="Show replication lag"),
    show_tables: bool = typer.Option(False, "--tables", help="Show table counts per schema"),
    show_sequences: bool = typer.Option(False, "--sequences", help="Show sequence sync status"),
    show_drift: bool = typer.Option(False, "--drift", help="Show schema drift summary"),
):
    """Display replication status, lag, sequence sync, and drift for all databases."""
    from replicator.monitor import _ALL_SECTIONS, build_status

    cfg = load_config(config)

    selected: set[str] = set()
    if show_subscription:
        selected.add("subscription")
    if show_slots:
        selected.add("slots")
    if show_lag:
        selected.add("lag")
    if show_tables:
        selected.add("tables")
    if show_sequences:
        selected.add("sequences")
    if show_drift:
        selected.add("drift")

    sections = frozenset(selected) if selected else None  # None → all
    _run(build_status(cfg, database=database, fmt=format, sections=sections))


@app.command(name="sync-sequences")
def sync_sequences(
    config: str = typer.Option("config.yaml", "--config", "-c"),
    database: Optional[str] = typer.Option(None, "--database", "-d"),
    loop: bool = typer.Option(False, "--loop", help="Run continuously"),
    format: str = typer.Option("rich", "--format", "-f", help="Output format: rich (default), simple, json"),
):
    """Synchronize sequences from source to target."""
    import json as _json

    from replicator.db import discover_databases
    from replicator.sequence_sync import run_sequence_sync_loop, sync_sequences_once

    cfg = load_config(config)

    def _kv_quote(s: object) -> str:
        v = str(s) if s is not None else ""
        if not v:
            return '""'
        if any(c in v for c in ' \t\n"='):
            return '"' + v.replace('"', '\\"') + '"'
        return v

    async def _sync():
        dbs = [database] if database else await discover_databases(cfg)

        if loop:
            tasks = [run_sequence_sync_loop(cfg, db) for db in dbs]
            await asyncio.gather(*tasks)
            return

        reports = await asyncio.gather(*[sync_sequences_once(cfg, db) for db in dbs])
        all_data = []
        for db, report in zip(dbs, reports):
            if format == "json":
                all_data.append({"database": db, "sequences": report})
            elif format == "simple":
                p = f"db={_kv_quote(db)}"
                if not report:
                    print(f"{p} section=sequence status=no_sequences")
                for r in report:
                    print(
                        f"{p} section=sequence"
                        f" schema={r['schema']}"
                        f" sequence={r['sequence']}"
                        f" source={r['source_value']}"
                        f" target={r['target_value']}"
                        f" status={r['status']}"
                    )
            else:  # rich
                _STATUS_STYLE = {"ok": "green", "updated": "yellow", "target_ahead": "cyan"}
                tbl = Table(title=f"Sequence Sync — {db}", show_lines=True)
                tbl.add_column("Schema")
                tbl.add_column("Sequence")
                tbl.add_column("Source")
                tbl.add_column("Target")
                tbl.add_column("Status")
                for r in report:
                    s = r["status"]
                    style = _STATUS_STYLE.get(s, "")
                    tbl.add_row(
                        r["schema"], r["sequence"],
                        str(r["source_value"]), str(r["target_value"]),
                        f"[{style}]{s}[/{style}]" if style else s,
                    )
                console.print(tbl)

        if format == "json":
            print(_json.dumps(all_data, indent=2, default=str))

    _run(_sync())


@app.command(name="detect-ddl")
def detect_ddl(
    config: str = typer.Option("config.yaml", "--config", "-c"),
    database: Optional[str] = typer.Option(None, "--database", "-d"),
    apply: bool = typer.Option(False, "--apply", help="Apply fixes for missing objects and ownership drift"),
    drop_extra: bool = typer.Option(
        False, "--drop-extra",
        help="Also DROP tables/objects on target that no longer exist on source (destructive!)",
    ),
    format: str = typer.Option("rich", "--format", "-f", help="Output format: rich (default), simple, json"),
):
    """Detect schema drift between source and target (including ownership)."""
    import json as _json

    from replicator.db import discover_databases
    from replicator.ddl_detector import apply_drift_fixes, detect_drift

    cfg = load_config(config)

    def _kv_quote(s: object) -> str:
        v = str(s) if s is not None else ""
        if not v:
            return '""'
        if any(c in v for c in ' \t\n"='):
            return '"' + v.replace('"', '\\"') + '"'
        return v

    async def _detect():
        dbs = [database] if database else await discover_databases(cfg)
        all_data = []

        for db in dbs:
            report = await detect_drift(cfg, db)

            if format == "json":
                all_data.append({
                    "database": db,
                    "has_drift": report.has_drift,
                    "summary": report.summary,
                    "items": [
                        {
                            "object_type": item.object_type,
                            "schema": item.schema,
                            "table": item.table,
                            "name": item.name,
                            "drift_type": item.drift_type,
                            "detail": item.detail,
                            "fix_ddl": item.fix_ddl,
                        }
                        for item in report.items
                    ],
                })
            elif format == "simple":
                p = f"db={_kv_quote(db)}"
                if not report.has_drift:
                    print(f"{p} section=drift status=ok")
                else:
                    for item in report.items:
                        print(
                            f"{p} section=drift"
                            f" type={_kv_quote(item.object_type)}"
                            f" schema={item.schema}"
                            f" table={item.table}"
                            f" name={_kv_quote(item.name)}"
                            f" drift={item.drift_type}"
                            f" detail={_kv_quote(item.detail)}"
                        )
            else:  # rich
                console.rule(f"[bold]Drift Report — {db}")
                if not report.has_drift:
                    console.print("[green]No drift detected")
                else:
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
                            style = "yellow" if item.fix_ddl else "red"
                        ddl_preview = item.fix_ddl
                        if ddl_preview and len(ddl_preview) > 80:
                            ddl_preview = ddl_preview[:77] + "..."
                        tbl.add_row(
                            item.object_type, item.schema, item.table,
                            item.name, item.drift_type, item.detail,
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
                if format == "simple":
                    print(f"db={_kv_quote(db)} section=apply applied={applied}")
                elif format != "json":
                    console.print(f"[green]Applied {applied} fix(es) for {db}")
            elif format == "rich":
                console.print(
                    "[dim]Run with [bold]--apply[/bold] to fix missing objects and ownership drift, "
                    "or [bold]--apply --drop-extra[/bold] to also drop extra tables.[/dim]"
                )

        if format == "json":
            print(_json.dumps(all_data, indent=2))

    _run(_detect())


@app.command(name="reinit-sync")
def reinit_sync(
    config: str = typer.Option("config.yaml", "--config", "-c", help="Path to config file"),
    database: Optional[str] = typer.Option(
        None, "--database", "-d",
        help="Reinit only this database (default: all discovered)",
    ),
):
    """Re-initialize replication after a Patroni switchover/failover.

    Checks each database for missing or broken publications, replication slots,
    and subscriptions, then repairs them without re-copying data.

    Safe to run at any time — it only creates/enables/refreshes components
    that are missing or not working.
    """
    from replicator.db import discover_databases
    from replicator.replication import reinit_sync as do_reinit

    cfg = load_config(config)

    async def _reinit():
        dbs = [database] if database else await discover_databases(cfg)
        all_healthy = True

        for db in dbs:
            console.rule(f"[bold cyan]Reinit Sync — {db}")
            result = await do_reinit(cfg, db)

            if result["issues_found"]:
                all_healthy = False
                for issue in result["issues_found"]:
                    console.print(f"  [yellow]⚠  {issue}")
            if result["actions_taken"]:
                for action in result["actions_taken"]:
                    console.print(f"  [green]✓  {action}")
            if result["was_healthy"]:
                console.print(f"  [green]Replication for '{db}' is healthy — nothing to do")

        if all_healthy:
            console.rule("[bold green]All databases are healthy")
        else:
            console.rule("[bold yellow]Reinit complete — issues were detected and repaired")

    _run(_reinit())


if __name__ == "__main__":
    app()
