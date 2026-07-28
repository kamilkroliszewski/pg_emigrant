"""CLI entry point for pg_emigrant — built with typer + rich."""

from __future__ import annotations

import asyncio
from typing import Optional

import typer
from rich.table import Table

from pg_emigrant.config import load_config
from pg_emigrant.utils import console, setup_logging

app = typer.Typer(
    name="pg_emigrant",
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
def preflight(
    config: str = typer.Option("config.yaml", "--config", "-c", help="Path to config file"),
    database: Optional[str] = typer.Option(None, "--database", "-d", help="Check only this database (default: all discovered)"),
    format: str = typer.Option("rich", "--format", "-f", help="Output format: rich (default), simple, json"),
    strict: bool = typer.Option(False, "--strict", help="Exit non-zero on warnings too, not just errors"),
):
    """Verify a migration will work — WITHOUT changing anything.

    Runs read-only catalog checks against both clusters: same-cluster/standby
    detection, wal_level and slot/worker headroom, role privileges, name
    collisions, extension availability, missing roles, unreadable or unlogged
    tables, and column/type compatibility with any pre-existing target schema.

    Only SELECTs against pg_catalog are issued — nothing is created, altered or
    dropped — so it is safe to run against production at any time, including
    during an in-flight migration.

    Exit code is 1 when any check fails (or, with --strict, also on warnings),
    which makes it usable as a CI / runbook gate before 'bootstrap'.
    """
    import json as _json

    from pg_emigrant.preflight import ERROR, OK, SKIP, WARN, run_preflight

    cfg = load_config(config)
    report = _run(run_preflight(cfg, database=database))

    if format == "json":
        print(_json.dumps(report.to_dict(), indent=2))
    elif format == "simple":
        for c in report.checks:
            db = c.database or "-"
            print(f"db={db} check={c.name} category={c.category} status={c.status} summary={c.summary!r}")
        print(f"passed={report.passed} summary={report.summary!r}")
    else:
        _STYLE = {ERROR: "bold red", WARN: "yellow", OK: "green", SKIP: "dim"}
        _ICON = {ERROR: "✗", WARN: "⚠", OK: "✓", SKIP: "–"}

        console.rule("[bold green]pg_emigrant preflight")
        console.print(
            f"[dim]Source[/dim] {cfg.source.host}:{cfg.source.port}   "
            f"[dim]Target[/dim] {cfg.target.host}:{cfg.target.port}   "
            f"[dim](read-only — nothing is modified)[/dim]\n"
        )

        tbl = Table(show_lines=False, expand=True)
        tbl.add_column("", width=1, no_wrap=True)
        tbl.add_column("Database", style="cyan", no_wrap=True)
        tbl.add_column("Check", no_wrap=True)
        tbl.add_column("Result")
        for c in report.checks:
            tbl.add_row(
                f"[{_STYLE[c.status]}]{_ICON[c.status]}[/{_STYLE[c.status]}]",
                c.database or "—",
                c.name,
                f"[{_STYLE[c.status]}]{c.summary}[/{_STYLE[c.status]}]",
            )
        console.print(tbl)

        # Only failures/warnings get their remediation text printed — a clean
        # run stays short enough to read at a glance.
        for c in report.checks:
            if c.status in (ERROR, WARN, SKIP) and c.detail:
                label = f"{c.database}: " if c.database else ""
                console.print(
                    f"\n[{_STYLE[c.status]}]{_ICON[c.status]} {label}{c.name}[/{_STYLE[c.status]}] — {c.summary}"
                )
                console.print(f"  [dim]{c.detail}[/dim]")

        console.print()
        if report.passed and not report.warnings:
            console.rule(f"[bold green]Preflight PASSED — {report.summary}")
        elif report.passed:
            console.rule(f"[bold yellow]Preflight passed with warnings — {report.summary}")
        else:
            console.rule(f"[bold red]Preflight FAILED — {report.summary}")

    if not report.passed or (strict and report.warnings):
        raise typer.Exit(code=1)


@app.command()
def bootstrap(
    config: str = typer.Option("config.yaml", "--config", "-c", help="Path to config file"),
    database: Optional[str] = typer.Option(None, "--database", "-d", help="Bootstrap only this database (default: all discovered)"),
    skip_preflight: bool = typer.Option(
        False, "--skip-preflight",
        help="Do not run the read-only preflight checks before migrating (not recommended)",
    ),
):
    """Run full bootstrap migration: discover → schema sync → data copy → replication setup."""
    from pg_emigrant.bootstrap import bootstrap as do_bootstrap
    from pg_emigrant.preflight import run_preflight

    cfg = load_config(config)

    # Gate the irreversible part behind the read-only checks: almost everything
    # that makes a bootstrap fail halfway (missing extension/role on the target,
    # exhausted slots, wrong wal_level, a name collision, source and target being
    # the same cluster) is knowable up front — and far cheaper to fix before a
    # slot exists on the production source and data has been copied.
    if not skip_preflight:
        report = _run(run_preflight(cfg, database=database))
        if not report.passed:
            console.rule("[bold red]Preflight FAILED — bootstrap not started")
            for c in report.errors:
                label = f"{c.database}: " if c.database else ""
                console.print(f"  [bold red]✗ {label}{c.summary}[/bold red]")
                if c.detail:
                    console.print(f"    [dim]{c.detail}[/dim]")
            console.print(
                "\n[dim]Nothing was modified. Run 'pg_emigrant preflight' for the full "
                "report, or re-run with --skip-preflight to override.[/dim]"
            )
            raise typer.Exit(code=1)
        if report.warnings:
            console.print(
                f"[yellow]⚠ Preflight passed with {len(report.warnings)} warning(s)[/yellow] "
                f"[dim]— run 'pg_emigrant preflight' for details[/dim]"
            )

    try:
        _run(do_bootstrap(cfg, database=database))
    except RuntimeError as exc:
        console.print(f"[bold red]{exc}[/bold red]")
        raise typer.Exit(code=1)


@app.command()
def start(
    config: str = typer.Option("config.yaml", "--config", "-c"),
    database: Optional[str] = typer.Option(None, "--database", "-d", help="Specific database"),
):
    """Start (enable) logical replication subscriptions."""
    from pg_emigrant.db import discover_databases
    from pg_emigrant.replication import enable_subscription

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
    from pg_emigrant.db import discover_databases
    from pg_emigrant.replication import disable_subscription

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
    from pg_emigrant.db import discover_databases
    from pg_emigrant.replication import drop_publication, drop_subscription

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
    from pg_emigrant.monitor import _ALL_SECTIONS, build_status

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
    margin: int = typer.Option(
        0, "--margin",
        help=(
            "Advance sequences to source_value + MARGIN instead of exactly "
            "source_value. Use a small positive value (e.g. 1000) on the "
            "LAST sync-sequences run at cutover, as a safety buffer against "
            "nextval() calls on the source between reading it and the app "
            "actually stopping. Do not use with --loop — it would compound "
            "on every iteration."
        ),
    ),
    format: str = typer.Option("rich", "--format", "-f", help="Output format: rich (default), simple, json"),
):
    """Synchronize sequences from source to target."""
    if margin and loop:
        console.print(
            "[bold red]--margin cannot be used with --loop[/bold red] — it applies on "
            "every advancing write and would keep inflating the target ahead of the "
            "source with each iteration. Use --margin only for the final, one-shot "
            "sync-sequences run at cutover."
        )
        raise typer.Exit(1)
    import json as _json

    from pg_emigrant.db import discover_databases
    from pg_emigrant.replication import run_new_table_sync_loop
    from pg_emigrant.sequence_sync import run_sequence_sync_loop, sync_sequences_once

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
            # --loop is the documented "keep this running for the whole
            # replication window" process, so it also picks up tables
            # created on the source after bootstrap (ALTER PUBLICATION /
            # new-table creation on target / subscription refresh) — see
            # sync_new_tables(). This needs no separate command or manual
            # step from the user.
            tasks = [run_sequence_sync_loop(cfg, db) for db in dbs]
            tasks += [run_new_table_sync_loop(cfg, db) for db in dbs]
            await asyncio.gather(*tasks)
            return

        # One-shot mode also reconciles newly created tables — including
        # right before a final cutover sync, where leaving a just-created
        # table unreplicated would be worse than catching it up.
        from pg_emigrant.replication import sync_new_tables
        new_table_actions = await asyncio.gather(
            *[sync_new_tables(cfg, db) for db in dbs]
        )
        for db, actions in zip(dbs, new_table_actions):
            for action in actions:
                console.print(f"  [{db}] {action}")

        reports = await asyncio.gather(
            *[sync_sequences_once(cfg, db, margin=margin) for db in dbs]
        )
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
                _STATUS_STYLE = {
                    "ok": "green", "updated": "yellow", "target_ahead": "cyan",
                    "permission_denied": "red", "orphaned_unknown": "red",
                    "orphaned_error": "red",
                }
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

    from pg_emigrant.db import discover_databases
    from pg_emigrant.ddl_detector import apply_drift_fixes, detect_drift

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
    from pg_emigrant.db import discover_databases
    from pg_emigrant.replication import reinit_sync as do_reinit
    from pg_emigrant.replication import warn_if_unstable_host

    cfg = load_config(config)
    warn_if_unstable_host(cfg)

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


@app.command()
def web(
    config: str = typer.Option("config.yaml", "--config", "-c", help="Path to config file"),
    host: str = typer.Option("127.0.0.1", "--host", help="Interface to bind (default: localhost)"),
    port: int = typer.Option(8000, "--port", "-p", help="Port to listen on"),
    debug: bool = typer.Option(False, "--debug", help="Enable Flask debug/reloader"),
):
    """Launch the pg_emigrant web GUI (Flask).

    Serves a Material Design dashboard to configure-view and monitor migrations,
    plus background-job execution of bootstrap/teardown/start/stop/sync-sequences/
    reinit-sync/detect-ddl. Reuses the same orchestration functions as the CLI.

    Binds to 127.0.0.1 by default and ships without authentication — do not
    expose it publicly without a reverse proxy + auth (it can run destructive
    operations and displays the configuration).
    """
    try:
        from pg_emigrant.web.app import create_app
    except ImportError:
        console.print(
            '[red]Flask is not installed.[/red] Install the web extra:\n'
            '  [bold]pip install -e ".[web]"[/bold]'
        )
        raise typer.Exit(1)

    app_ = create_app(config_path=config)
    console.print(f"[green]pg_emigrant GUI →[/green] http://{host}:{port}")
    app_.run(host=host, port=port, threaded=True, debug=debug)


if __name__ == "__main__":
    app()
