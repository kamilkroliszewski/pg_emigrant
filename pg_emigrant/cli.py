"""CLI entry point for pg_emigrant — built with typer + rich."""

from __future__ import annotations

import asyncio
import signal
from typing import Optional

import typer
from rich.table import Table

from pg_emigrant import exits
from pg_emigrant.config import load_config
from pg_emigrant.utils import console, route_console_to_stderr, setup_logging

app = typer.Typer(
    name="pg_emigrant",
    help="pg_emigrant — PostgreSQL migration & replication orchestrator",
    add_completion=False,
)


def _run(coro):
    """Run an async coroutine from the synchronous CLI layer.

    SIGINT and SIGTERM cancel the running task rather than tearing the process
    down where it stands.  That matters because the dangerous moment to be
    killed is the one where a replication slot exists on the production source
    and nothing is attached to it yet: an abandoned logical slot retains WAL
    until somebody drops it, which is how an interrupted migration fills a
    primary's disk days later.  Cancellation gives the orchestrators a chance
    to roll that back (see bootstrap's handler); SIGKILL by definition does
    not, so the recovery path there is the next run adopting the orphan.
    """
    async def _cancellable():
        task = asyncio.ensure_future(coro)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, task.cancel)
            except (NotImplementedError, RuntimeError):
                # Windows, or a non-main thread: fall back to the default
                # behaviour rather than failing to run at all.
                pass
        return await task

    return asyncio.run(_cancellable())


_FORMATS = ("rich", "simple", "json")


def _resolve_format(fmt: str) -> str:
    """Validate ``--format`` and, for JSON, get everything else off stdout.

    An unrecognised value used to fall through to the rich renderer, so a
    typo in a CI pipeline's ``--format jsom`` produced a coloured table that
    the pipeline then failed to parse, blaming the data.
    """
    fmt = fmt.strip().lower()
    if fmt not in _FORMATS:
        console.print(
            f"[bold red]Configuration error:[/bold red] unknown --format "
            f"{fmt!r}; expected one of {', '.join(_FORMATS)}"
        )
        raise typer.Exit(code=exits.CONFIG_ERROR)
    if fmt == "json":
        route_console_to_stderr()
    return fmt


def _load(config: str):
    """Load the configuration, turning any problem with it into exit 2.

    A missing file, invalid YAML or a rejected setting is a *configuration*
    error, not a migration failure — a runbook needs to tell "fix your config"
    apart from "the migration went wrong", and an unhandled traceback tells it
    neither.
    """
    from pydantic import ValidationError

    try:
        return load_config(config)
    except FileNotFoundError as exc:
        console.print(f"[bold red]Configuration error:[/bold red] {exc}")
        raise typer.Exit(code=exits.CONFIG_ERROR)
    except ValidationError as exc:
        console.print("[bold red]Configuration error:[/bold red]")
        for err in exc.errors():
            location = ".".join(str(p) for p in err["loc"]) or "(root)"
            console.print(f"  [red]{location}: {err['msg']}[/red]")
        raise typer.Exit(code=exits.CONFIG_ERROR)
    except Exception as exc:  # malformed YAML, unreadable file, …
        console.print(f"[bold red]Configuration error:[/bold red] {exc!s}")
        raise typer.Exit(code=exits.CONFIG_ERROR)


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
    using_pg_tde: bool = typer.Option(
        False, "--using-pg-tde", "--using_pg_tde",
        help=(
            "Also check the pg_tde prerequisites a 'bootstrap --using-pg-tde' "
            "needs on the target: extension availability, shared_preload_libraries, "
            "the tde_heap access method, and a principal key per database."
        ),
    ),
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

    format = _resolve_format(format)
    cfg = _load(config)
    report = _run(run_preflight(cfg, database=database, use_pg_tde=using_pg_tde))

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
        raise typer.Exit(code=exits.PREFLIGHT_FAILED)


@app.command()
def bootstrap(
    config: str = typer.Option("config.yaml", "--config", "-c", help="Path to config file"),
    database: Optional[str] = typer.Option(None, "--database", "-d", help="Bootstrap only this database (default: all discovered)"),
    skip_preflight: bool = typer.Option(
        False, "--skip-preflight",
        help="Do not run the read-only preflight checks before migrating (not recommended)",
    ),
    using_pg_tde: bool = typer.Option(
        False, "--using-pg-tde", "--using_pg_tde",
        help=(
            "Migrate into pg_tde-encrypted storage: verify the target can encrypt, "
            "create every table USING tde_heap, set the database's "
            "default_table_access_method, and convert any pre-existing target "
            "table with ALTER TABLE … SET ACCESS METHOD tde_heap."
        ),
    ),
    format: str = typer.Option(
        "rich", "--format", "-f",
        help="Output format: rich (default) or json (machine-readable result on stdout)",
    ),
):
    """Run full bootstrap migration: discover → schema sync → data copy → replication setup.

    With --using-pg-tde the target must have pg_tde in shared_preload_libraries
    and a principal key configured for each target database; pg_emigrant
    installs the extension itself but never creates a key provider or key —
    those are security decisions (file / Vault / KMIP, and where the secrets
    live) that belong to you. The readiness check runs before anything is
    created on the source, so a target that cannot encrypt costs nothing.
    """
    import json as _json

    from pg_emigrant.bootstrap import bootstrap as do_bootstrap
    from pg_emigrant.guards import UnsafeOperation
    from pg_emigrant.preflight import run_preflight
    from pg_emigrant.report import BootstrapIncomplete

    format = _resolve_format(format)
    cfg = _load(config)

    # Gate the irreversible part behind the read-only checks: almost everything
    # that makes a bootstrap fail halfway (missing extension/role on the target,
    # exhausted slots, wrong wal_level, a name collision, source and target being
    # the same cluster) is knowable up front — and far cheaper to fix before a
    # slot exists on the production source and data has been copied.
    if not skip_preflight:
        report = _run(run_preflight(cfg, database=database, use_pg_tde=using_pg_tde))
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
            raise typer.Exit(code=exits.PREFLIGHT_FAILED)
        if report.warnings:
            console.print(
                f"[yellow]⚠ Preflight passed with {len(report.warnings)} warning(s)[/yellow] "
                f"[dim]— run 'pg_emigrant preflight' for details[/dim]"
            )

    try:
        result = _run(do_bootstrap(cfg, database=database, use_pg_tde=using_pg_tde))
    except UnsafeOperation as exc:
        console.print(f"[bold red]Refusing to start the migration:[/bold red] {exc}")
        raise typer.Exit(code=exits.UNSAFE_REFUSED)
    except BootstrapIncomplete as exc:
        if format == "json":
            print(_json.dumps(exc.report.to_dict(), indent=2))
        raise typer.Exit(code=exc.report.exit_code)
    except asyncio.CancelledError:
        # An interrupt that reached here without a report: the run was
        # cancelled before it had per-database state to summarise.
        console.print("[bold yellow]Interrupted — nothing further was changed.[/bold yellow]")
        raise typer.Exit(code=exits.MIGRATION_FAILED)
    except RuntimeError as exc:
        console.print(f"[bold red]{exc}[/bold red]")
        raise typer.Exit(code=exits.MIGRATION_FAILED)

    if format == "json":
        print(_json.dumps(result.to_dict(), indent=2))


@app.command()
def start(
    config: str = typer.Option("config.yaml", "--config", "-c"),
    database: Optional[str] = typer.Option(None, "--database", "-d", help="Specific database"),
):
    """Start (enable) logical replication subscriptions."""
    from pg_emigrant.db import discover_databases
    from pg_emigrant.replication import enable_subscription

    cfg = _load(config)

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

    cfg = _load(config)

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
    from pg_emigrant.guards import UnsafeOperation, assert_distinct_clusters
    from pg_emigrant.replication import drop_publication, drop_subscription

    cfg = _load(config)

    async def _teardown():
        # Teardown drops publications and replication slots on the SOURCE; if
        # 'target' is really the source, the subscription lookup and the drop
        # both land on production.
        await assert_distinct_clusters(cfg)
        dbs = [database] if database else await discover_databases(cfg)
        for db in dbs:
            await drop_subscription(cfg, db)
            await drop_publication(cfg, db)
            console.print(f"[red]Torn down replication for {db}")

    try:
        _run(_teardown())
    except UnsafeOperation as exc:
        console.print(f"[bold red]Refusing to tear down:[/bold red] {exc}")
        raise typer.Exit(code=exits.UNSAFE_REFUSED)


@app.command()
def status(
    config: str = typer.Option("config.yaml", "--config", "-c"),
    database: Optional[str] = typer.Option(None, "--database", "-d", help="Show status for a specific database only"),
    format: str = typer.Option("rich", "--format", "-f", help="Output format: rich (default), simple (grep-friendly), json"),
    show_health: bool = typer.Option(
        False, "--health",
        help=(
            "Show the replication health state (HEALTHY/LAGGING/CRITICAL/BROKEN), "
            "unapplied-WAL lag, and how much WAL the slot is retaining on the source"
        ),
    ),
    show_subscription: bool = typer.Option(False, "--subscription", help="Show subscription status"),
    show_slots: bool = typer.Option(False, "--slots", help="Show replication slots"),
    show_lag: bool = typer.Option(False, "--lag", help="Show replication lag"),
    show_tables: bool = typer.Option(False, "--tables", help="Show table counts per schema"),
    show_sequences: bool = typer.Option(False, "--sequences", help="Show sequence sync status"),
    show_drift: bool = typer.Option(False, "--drift", help="Show schema drift summary"),
):
    """Display replication status, lag, sequence sync, and drift for all databases."""
    from pg_emigrant.monitor import build_status

    format = _resolve_format(format)
    cfg = _load(config)

    selected: set[str] = set()
    if show_health:
        selected.add("health")
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

    format = _resolve_format(format)
    cfg = _load(config)

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

    format = _resolve_format(format)
    cfg = _load(config)

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
    allow_data_gap: bool = typer.Option(
        False, "--allow-data-gap",
        help=(
            "Recreate the subscription even when the replication slot is gone, "
            "ACCEPTING PERMANENT DATA LOSS for everything committed since the old "
            "slot's last confirmed LSN. Without this, such a repair is refused and "
            "nothing is changed."
        ),
    ),
    format: str = typer.Option(
        "rich", "--format", "-f",
        help="Output format: rich (default) or json (machine-readable result on stdout)",
    ),
):
    """Re-initialize replication after a Patroni switchover/failover.

    Repairs what can be repaired without re-copying data: a missing publication,
    a disabled subscription, a stalled apply worker, or a subscription that was
    lost while its replication slot survived (which resumes from the slot's
    confirmed LSN — no gap).

    NOT always safe to complete: if the replication slot itself is gone or its
    WAL was recycled, the transactions it still held can no longer reach the
    target, and no amount of streaming will bring them back. That repair is
    REFUSED by default (nothing is changed) because completing it would leave a
    silently incomplete target that reports itself healthy. Restore consistency
    with 'teardown --database X' + 'bootstrap --database X', or override with
    --allow-data-gap if you have verified the gap is acceptable.

    Exits non-zero if any database was refused or repaired with data loss.
    """
    from pg_emigrant.db import discover_databases
    from pg_emigrant.replication import reinit_sync as do_reinit
    from pg_emigrant.replication import warn_if_unstable_host

    import json as _json

    format = _resolve_format(format)
    cfg = _load(config)
    warn_if_unstable_host(cfg)
    results: list[dict] = []

    async def _reinit() -> int:
        dbs = [database] if database else await discover_databases(cfg)
        all_healthy = True
        blocked: list[str] = []
        lossy: list[str] = []

        for db in dbs:
            console.rule(f"[bold cyan]Reinit Sync — {db}")
            result = await do_reinit(cfg, db, allow_data_gap=allow_data_gap)
            results.append(result)

            if result["issues_found"]:
                all_healthy = False
                for issue in result["issues_found"]:
                    console.print(f"  [yellow]⚠  {issue}")
            if result["actions_taken"]:
                for action in result["actions_taken"]:
                    console.print(f"  [green]✓  {action}")
            if result["was_healthy"]:
                console.print(f"  [green]Replication for '{db}' is healthy — nothing to do")

            if result.get("blocked"):
                blocked.append(db)
                console.print(
                    f"  [bold red]✗  REFUSED for '{db}' — nothing was changed.[/bold red]\n"
                    f"     [dim]The slot is gone, so the un-replayed transactions it held "
                    f"cannot reach the target. Re-copy to restore consistency:[/dim]\n"
                    f"     [bold]pg_emigrant teardown --database {db} && "
                    f"pg_emigrant bootstrap --database {db}[/bold]\n"
                    f"     [dim]Or re-run with --allow-data-gap to accept permanent loss.[/dim]"
                )
            if result.get("data_gap"):
                lossy.append(db)
                console.print(
                    f"  [bold red]⚠  DATA WAS LOST for '{db}'[/bold red] "
                    f"[dim]— replication runs again, but the target is missing rows "
                    f"until you re-copy the affected tables. Do not cut over on it.[/dim]"
                )

        if blocked or lossy:
            if blocked:
                console.rule(
                    f"[bold red]Reinit REFUSED for {len(blocked)} database(s): "
                    f"{', '.join(blocked)} — nothing was changed"
                )
            if lossy:
                console.rule(
                    f"[bold red]Reinit completed WITH DATA LOSS for {len(lossy)} "
                    f"database(s): {', '.join(lossy)} — re-copy before cutover"
                )
            return 1
        if all_healthy:
            console.rule("[bold green]All databases are healthy")
        else:
            console.rule("[bold green]Reinit complete — issues repaired with no data loss")
        return 0

    code = _run(_reinit())

    if format == "json":
        blocked = [r for r in results if r.get("blocked")]
        lossy = [r for r in results if r.get("data_gap")]
        print(_json.dumps({
            "repaired": code == 0,
            # 'blocked' is the one an automated caller must never treat as a
            # transient failure to retry: the repair is impossible, not slow.
            "blocked": [r["database"] for r in blocked],
            "data_gap": [r["database"] for r in lossy],
            "databases": results,
        }, indent=2, default=str))

    if code != 0:
        # A refusal is not the same failure as a completed-but-lossy repair:
        # the first means 'this cannot be fixed by streaming', the second
        # means 'it was fixed and the target is now incomplete'.
        if any(r.get("blocked") for r in results):
            raise typer.Exit(code=exits.RECOVERY_IMPOSSIBLE)
        raise typer.Exit(code=exits.MIGRATION_FAILED)


@app.command(name="cutover-check")
def cutover_check(
    config: str = typer.Option("config.yaml", "--config", "-c", help="Path to config file"),
    database: Optional[str] = typer.Option(
        None, "--database", "-d", help="Check only this database (default: all discovered)"
    ),
    max_lag_bytes: int = typer.Option(
        None, "--max-lag-bytes",
        help=(
            "How far behind the target may be and still count as caught up "
            "(default 8 MiB). Not zero: a live source keeps committing, so the "
            "gap is never exactly nothing while the application is running."
        ),
    ),
    accept_drift: bool = typer.Option(
        False, "--accept-drift",
        help=(
            "Treat existing schema drift as a deliberate decision rather than a "
            "blocker. Use only after reviewing 'detect-ddl' output."
        ),
    ),
    format: str = typer.Option("rich", "--format", "-f", help="Output format: rich (default), simple, json"),
):
    """Answer one question, read-only: is it safe to cut over yet?

    Checks that replication is healthy and caught up, that sequences are at or
    ahead of the source, that there is no unresolved schema drift, that the
    target is reachable and writable, and that source and target really are
    different clusters. Anything that cannot be verified counts against
    readiness — a green light on missing evidence is worse than a red one.

    Changes nothing. It will not stop the application, disable the
    subscription, or move any traffic: when to move traffic involves load
    balancers, DNS, connection pools and people, none of which this tool can
    see. Exit code 0 means SAFE TO CUT OVER, 5 means DO NOT CUT OVER.
    """
    import json as _json

    from pg_emigrant.cutover import DEFAULT_MAX_LAG_BYTES, check_cutover_readiness

    format = _resolve_format(format)
    cfg = _load(config)
    report = _run(check_cutover_readiness(
        cfg, database=database,
        max_lag_bytes=max_lag_bytes if max_lag_bytes is not None else DEFAULT_MAX_LAG_BYTES,
        accept_drift=accept_drift,
    ))

    if format == "json":
        print(_json.dumps(report.to_dict(), indent=2))
    elif format == "simple":
        for db in report.databases:
            for c in db.checks:
                print(
                    f"db={db.database} check={c.name} "
                    f"ready={'yes' if c.ready else 'no'} summary={c.summary!r}"
                )
        print(f"ready={'yes' if report.ready else 'no'} summary={report.summary!r}")
    else:
        console.rule("[bold]pg_emigrant cutover readiness")
        for db in report.databases:
            tbl = Table(title=f"Database: {db.database}", show_lines=False, expand=True)
            tbl.add_column("", width=1, no_wrap=True)
            tbl.add_column("Check", no_wrap=True)
            tbl.add_column("Result")
            for c in db.checks:
                mark = "[green]✓[/green]" if c.ready else "[bold red]✗[/bold red]"
                style = "" if c.ready else "red"
                tbl.add_row(mark, c.name,
                            f"[{style}]{c.summary}[/{style}]" if style else c.summary)
            console.print(tbl)
            for c in db.blockers:
                if c.detail:
                    console.print(f"  [red]✗ {c.name}[/red] — {c.detail}")
        console.print()
        if report.ready:
            console.rule(f"[bold green]{report.summary}")
        else:
            console.rule(f"[bold red]{report.summary}")

    if not report.ready:
        raise typer.Exit(code=exits.REPLICATION_UNHEALTHY)


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

    Binds to 127.0.0.1 by default. Require a login by setting 'web.auth' in the
    config file (generate the password hash with 'pg_emigrant hash-password');
    without one the GUI is open to anyone who can reach the port, and it can run
    destructive operations and displays the configuration. Even with a login,
    put a TLS-terminating reverse proxy in front of it before exposing it beyond
    localhost — the password and session cookie travel in clear over plain HTTP.
    """
    try:
        from pg_emigrant.web.app import create_app
        from pg_emigrant.web.auth import AuthConfigError
    except ImportError:
        console.print(
            '[red]Flask is not installed.[/red] Install the web extra:\n'
            '  [bold]pip install -e ".[web]"[/bold]'
        )
        raise typer.Exit(1)

    try:
        app_ = create_app(config_path=config)
    except AuthConfigError as exc:
        console.print(f"[bold red]Refusing to start the GUI:[/bold red] {exc}")
        raise typer.Exit(1)

    if app_.config.get("EMIGRANT_AUTH_ACTIVE"):
        console.print("[green]Authentication:[/green] enabled (web.auth)")
    else:
        console.print(
            "[bold yellow]⚠ Authentication is DISABLED[/bold yellow] — anyone who can "
            "reach this port can read the configuration and run destructive "
            "operations.\n[dim]  Set web.auth.password_hash in the config file; "
            "generate one with 'pg_emigrant hash-password'.[/dim]"
        )
        # Binding beyond loopback without a login publishes those operations to
        # the network, which is a different order of mistake from leaving the
        # localhost default open.
        if host not in ("127.0.0.1", "localhost", "::1"):
            console.print(
                f"[bold red]  You are binding to {host}, not loopback — the GUI will "
                f"be reachable from the network with NO authentication.[/bold red]"
            )

    console.print(f"[green]pg_emigrant GUI →[/green] http://{host}:{port}")
    app_.run(host=host, port=port, threaded=True, debug=debug)


@app.command(name="hash-password")
def hash_password(
    password: Optional[str] = typer.Option(
        None, "--password",
        help=(
            "The password to hash. Omit it to be prompted instead — which keeps "
            "the password out of your shell history and process list."
        ),
    ),
):
    """Generate a password hash for the web GUI's 'web.auth.password_hash'.

    Prints only the hash, so it can be piped or copied straight into the config
    file. The hash is salted, so running this twice on the same password gives
    two different (both valid) results.
    """
    try:
        from pg_emigrant.web.auth import hash_password as _hash
    except ImportError:
        console.print(
            '[red]Flask is not installed.[/red] The password hasher lives in the web extra:\n'
            '  [bold]pip install -e ".[web]"[/bold]'
        )
        raise typer.Exit(1)

    if password is None:
        password = typer.prompt("Password", hide_input=True, confirmation_prompt=True)
    if not password:
        console.print("[bold red]Refusing to hash an empty password.[/bold red]")
        raise typer.Exit(1)

    # The hash is the only thing on stdout, so `pg_emigrant hash-password > h`
    # yields exactly the hash and nothing else; the guidance goes to stderr.
    from rich.console import Console

    print(_hash(password))
    Console(stderr=True).print(
        "\n[dim]Add it to your config file as:[/dim]\n"
        "[bold]web:\n  auth:\n    username: admin\n    password_hash: \"<the line above>\"[/bold]"
    )


if __name__ == "__main__":
    app()
