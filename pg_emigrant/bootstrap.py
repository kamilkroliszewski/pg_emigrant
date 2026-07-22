"""Bootstrap orchestrator — full initial migration lifecycle.

Sequence:
  1. Discover databases on source
  2. Create databases on target
  3. For each database:
     a. Synchronize schemas (tables, columns, indexes, constraints, sequences)
     b. Set REPLICA IDENTITY FULL on PK-less tables (source + target) — must
        happen before the slot exists, see the note on ordering below
     c. Create the publication on the source
     d. Create the replication slot on the source UP FRONT, exporting a
        snapshot consistent with the slot's exact start LSN
     e. Copy initial data using THAT snapshot (parallel COPY); a failed table
        copy ABORTS this database — the slot and publication are dropped and
        replication is never set up for it, because logical replication would
        never backfill the missing rows
     f. Deferred indexes, FKs, functions, views, triggers, ownership
     g. Final sequence value sync (covers identity-backed sequences)
     h. Create the subscription on the target, attached to the slot created
        in (d) — NOT creating a new one

Ordering rationale (steps c/d before e): if the slot were created only AFTER
the data copy (the naive order), the copy's snapshot and the slot's start LSN
would be two different points in time. Any transaction committed on the
source in between would be in neither the copy (already taken) nor the WAL
stream (starts later) — a silent, permanent data loss window. Creating the
slot first and copying data with ITS exported snapshot makes the copy and the
start of replication the exact same consistent point.

Object-sync failures (functions/views/triggers) are collected and summarized
in red at the end of the run; databases with an incomplete data copy make the
whole command fail with a non-zero exit code.
"""

from __future__ import annotations

from rich.progress import Progress, SpinnerColumn, TextColumn

from pg_emigrant.config import ReplicatorConfig
from pg_emigrant.data_copy import copy_all_tables, verify_copy_counts
from pg_emigrant.db import connect, discover_databases, discover_schemas
from pg_emigrant.ddl_detector import detect_drift
from pg_emigrant.replication import (
    create_publication,
    create_replication_slot_with_snapshot,
    create_subscription,
    drop_publication,
    drop_replication_slot,
    sub_name,
)
from pg_emigrant.schema_sync import (
    get_tables,
    sync_db_settings,
    sync_deferred_indexes,
    sync_ownership,
    sync_post_copy_constraints,
    sync_privileges,
    sync_replica_identity,
    sync_schemas,
)
from pg_emigrant.sequence_sync import sync_sequences_once
from pg_emigrant.utils import console, get_logger, ql

log = get_logger(__name__)


class _DatabaseBootstrapFailed(Exception):
    """Raised to abort one database's bootstrap; caught by the per-database loop."""


async def ensure_database_exists(cfg: ReplicatorConfig, dbname: str) -> None:
    """Create the database on the target if it doesn't already exist.

    Reproduces the source's encoding, collation and ctype (or ICU locale, on
    PostgreSQL 15+ ICU-provider databases) instead of falling back to the
    target cluster's defaults.  A locale mismatch silently changes sort order
    for every ``ORDER BY``, index range scan, and text comparison beyond
    plain ASCII — a correctness issue, not a cosmetic one.  ``TEMPLATE
    template0`` is required: template1 already has an encoding/locale baked
    in and refuses a different one.

    If the target OS doesn't have the requested locale installed,
    ``CREATE DATABASE`` fails outright (PostgreSQL does not silently
    substitute a close match) — that failure is caught and this falls back
    to the target's own defaults, with a loud warning: the OS-level locale
    package is a platform prerequisite completely outside this tool's
    control, so treat it as a real signal, not noise.
    """
    async with connect(cfg.target) as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", dbname
        )
        if not exists:
            async with connect(cfg.source, dbname) as src:
                # pg_database's locale columns are version-dependent:
                # PG ≤14 has neither datlocprovider nor an ICU-locale column
                # (libc only), PG15 added datlocprovider + daticulocale, and
                # PG17 renamed daticulocale to datlocale (plus the 'b'
                # builtin provider).  Querying the wrong shape is an
                # UndefinedColumnError.
                src_major = src.get_server_version().major
                if src_major >= 17:
                    prov_cols = ("datlocprovider::text AS locprovider,"
                                 " datlocale AS provider_locale")
                elif src_major >= 15:
                    prov_cols = ("datlocprovider::text AS locprovider,"
                                 " daticulocale AS provider_locale")
                else:
                    prov_cols = "'c' AS locprovider, NULL::text AS provider_locale"
                meta = await src.fetchrow(
                    "SELECT pg_encoding_to_char(encoding) AS encoding,"
                    f" datcollate, datctype, {prov_cols}"
                    " FROM pg_database WHERE datname = current_database()"
                )
            opts = [f"ENCODING {ql(meta['encoding'])}"]
            if meta["locprovider"] == "i":
                opts.append("LOCALE_PROVIDER icu")
                if meta["provider_locale"]:
                    opts.append(f"ICU_LOCALE {ql(meta['provider_locale'])}")
            elif meta["locprovider"] == "b":
                # PostgreSQL 17+ builtin provider ("C", "C.UTF-8", …).
                # Requires a 17+ target too; an older target fails the CREATE
                # and lands in the locale-fallback path below, with its loud
                # warning.
                opts.append("LOCALE_PROVIDER builtin")
                if meta["provider_locale"]:
                    opts.append(f"BUILTIN_LOCALE {ql(meta['provider_locale'])}")
            else:
                opts.append(f"LC_COLLATE {ql(meta['datcollate'])}")
                opts.append(f"LC_CTYPE {ql(meta['datctype'])}")
            # CREATE DATABASE cannot run inside a transaction
            try:
                await conn.execute(
                    f'CREATE DATABASE "{dbname}" TEMPLATE template0 {" ".join(opts)};'
                )
                log.info(
                    "Created database %s on target (encoding=%s, collate=%s, ctype=%s)",
                    dbname, meta["encoding"], meta["datcollate"], meta["datctype"],
                )
            except Exception as exc:
                log.warning(
                    "Could not create database %s with the source's locale (%s) — "
                    "falling back to the target cluster's default locale. Sort "
                    "order and text comparisons may now differ from the source "
                    "for non-ASCII data — install the matching OS locale on the "
                    "target and recreate the database before cutover if this "
                    "matters for your data.",
                    dbname, exc,
                )
                await conn.execute(f'CREATE DATABASE "{dbname}";')
                log.info("Created database %s on target (target cluster defaults)", dbname)
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

    # Databases whose initial data copy failed — replication is NOT set up for
    # them and the whole command exits with an error at the end.
    failed_dbs: list[str] = []
    # Per-database object-sync failures (functions/views/triggers) — summarized
    # loudly at the end so they cannot drown in the scrollback.
    object_warnings: dict[str, dict[str, list[str]]] = {}
    # Per-database schema drift found by the built-in post-bootstrap check.
    drift_warnings: dict[str, str] = {}

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        for dbname in databases:
            task = progress.add_task(f"Migrating {dbname}…", total=None)
            # Tracks whether the publication/slot for this database have been
            # created yet, so the except-handler below knows what it needs to
            # clean up on any failure (from here on, this database owns
            # server-side replication state that must not be left orphaned).
            slot = None
            pub_created = False

            try:
                # Step 2: ensure database exists on target
                progress.update(task, description=f"[{dbname}] Creating database…")
                await ensure_database_exists(cfg, dbname)

                # Step 2b: refuse to re-bootstrap a database that is already
                # replicating.  Without this guard, a re-run would treat the
                # LIVE slot as orphaned (terminating its walsender and
                # recreating the slot at a new LSN) and then TRUNCATE the
                # target while the still-enabled apply worker is running —
                # guaranteeing duplicate-apply conflicts.  Tearing down must
                # be an explicit, separate decision.
                async with connect(cfg.target, dbname) as probe:
                    already_subscribed = bool(await probe.fetchval(
                        "SELECT 1 FROM pg_subscription WHERE subname = $1"
                        " AND subdbid = (SELECT oid FROM pg_database"
                        " WHERE datname = current_database())",
                        sub_name(cfg, dbname),
                    ))
                if already_subscribed:
                    raise _DatabaseBootstrapFailed(
                        f"subscription {sub_name(cfg, dbname)!r} already exists — "
                        f"this database is already replicating. Re-running "
                        f"bootstrap would drop its live replication slot and "
                        f"truncate the target mid-replication. Run "
                        f"'pg_emigrant teardown --database {dbname}' first if "
                        f"you really want to re-bootstrap it."
                    )

                # Step 3: discover schemas for this database, then synchronize them
                progress.update(task, description=f"[{dbname}] Syncing schemas…")
                async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
                    schemas = await discover_schemas(src, cfg)
                    console.print(f"  [{dbname}] Schemas: {schemas}")
                    await sync_schemas(src, tgt, schemas)

                # Step 3b: REPLICA IDENTITY FULL for PK-less tables, on the SOURCE
                # before the slot exists — see the module docstring for why this
                # ordering is required (REPLICA IDENTITY is evaluated at WAL-write
                # time, not at slot-creation time).
                progress.update(task, description=f"[{dbname}] Setting replica identity…")
                async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
                    await sync_replica_identity(src, tgt, schemas)

                # Step 3c/3d: publication, then the replication slot — BEFORE the
                # data copy, so the copy can use the slot's own exported snapshot.
                progress.update(task, description=f"[{dbname}] Creating publication…")
                await create_publication(cfg, dbname, schemas=schemas)
                pub_created = True

                progress.update(task, description=f"[{dbname}] Creating replication slot…")
                slot = await create_replication_slot_with_snapshot(cfg, dbname)

                # Step 4: copy initial data, using the slot's exported snapshot
                progress.update(task, description=f"[{dbname}] Copying data…")
                async with connect(cfg.source, dbname) as src:
                    all_tables = await get_tables(src, schemas)
                # Partitioned parents (relkind 'p') hold no rows of their own —
                # the data physically lives in the leaf partitions, which are
                # copied individually.  Copying the parent too would duplicate
                # every row.
                tables = [t for t in all_tables if t["relkind"] != "p"]

                if tables:
                    n_total = len(tables)
                    n_done = 0
                    active: set[str] = set()

                    def _on_start(key: str) -> None:
                        nonlocal active
                        active.add(key)
                        _active_str = ", ".join(sorted(active))
                        progress.update(
                            task,
                            description=f"[{dbname}] Copying data… [{n_done}/{n_total}] → {_active_str}",
                        )

                    def _on_done(key: str, rows: int) -> None:
                        nonlocal n_done, active
                        n_done += 1
                        active.discard(key)
                        if rows >= 0:
                            console.print(f"    [{dbname}] ✓ {key} ({rows:,} rows)")
                        else:
                            console.print(f"    [{dbname}] ✗ {key} (failed)")
                        _active_str = ", ".join(sorted(active)) if active else "…"
                        progress.update(
                            task,
                            description=f"[{dbname}] Copying data… [{n_done}/{n_total}] → {_active_str}",
                        )

                    try:
                        results = await copy_all_tables(
                            cfg, dbname, tables, slot.snapshot_name,
                            on_table_start=_on_start,
                            on_table_done=_on_done,
                        )
                        # Cross-check row counts while the snapshot is still
                        # valid: source counted under the EXACT snapshot the
                        # copy used vs. what COPY reported landed on the
                        # target.  Both sides are the same frozen point in
                        # time, so any mismatch is a genuine copy bug, not a
                        # race with concurrent writes.
                        count_mismatches = await verify_copy_counts(
                            cfg, dbname, tables, slot.snapshot_name, results,
                        )
                    finally:
                        # The snapshot is only needed for the copy (and the
                        # count check) above — release it (and the connection
                        # holding it) as soon as both are done, success or
                        # not.  This does NOT drop the slot itself.
                        await slot.aclose()
                    total_rows = sum(c for c in results.values() if c >= 0)
                    console.print(
                        f"  [{dbname}] Copied {total_rows:,} rows across {len(results)} tables"
                    )

                    # A failed table copy is fatal for this database: logical
                    # replication only streams NEW changes and would never
                    # backfill the missing rows.
                    failed_tables = sorted(k for k, c in results.items() if c < 0)
                    if failed_tables:
                        raise _DatabaseBootstrapFailed(
                            f"initial data copy FAILED for {len(failed_tables)} "
                            f"table(s): {', '.join(failed_tables)}"
                        )
                    if count_mismatches:
                        detail = ", ".join(
                            f"{k} (source={s:,}, target={t:,})"
                            for k, (s, t) in sorted(count_mismatches.items())
                        )
                        raise _DatabaseBootstrapFailed(
                            f"row count MISMATCH after copy for "
                            f"{len(count_mismatches)} table(s): {detail}"
                        )
                else:
                    console.print(f"  [{dbname}] No tables to copy")
                    await slot.aclose()

                # Step 4b: create non-unique indexes after COPY (faster than during insert)
                progress.update(task, description=f"[{dbname}] Creating indexes…")
                async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
                    await sync_deferred_indexes(src, tgt, schemas)

                # Step 4c: FK constraints, functions, views, triggers — post-COPY
                # so that PostgreSQL validates referential integrity across the
                # fully-loaded dataset.  Triggers are created last, after the
                # final function pass, so they never fail on a not-yet-created
                # function.
                progress.update(task, description=f"[{dbname}] Applying constraints…")
                async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
                    obj_failures = await sync_post_copy_constraints(src, tgt, schemas)
                obj_failures = {k: v for k, v in obj_failures.items() if v}
                if obj_failures:
                    object_warnings[dbname] = obj_failures
                    console.print(
                        f"  [bold red]⚠ [{dbname}] Some schema objects could NOT be created:[/bold red]"
                    )
                    for kind, entries in obj_failures.items():
                        for entry in entries:
                            console.print(f"    [red]✗ {kind} {entry}[/red]")

                # Step 4d: synchronize ownership (tables, sequences, views, functions, types, database)
                progress.update(task, description=f"[{dbname}] Syncing ownership…")
                async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
                    own_count = await sync_ownership(src, tgt, schemas, dbname=dbname)
                    if own_count:
                        console.print(f"  [{dbname}] Applied {own_count} ownership change(s)")

                # Step 4d-2: synchronize GRANTs (tables, sequences, schemas,
                # functions, types, default privileges, database) — additive
                # only, never revokes; see sync_privileges() docstring.
                progress.update(task, description=f"[{dbname}] Syncing privileges…")
                async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
                    priv_count = await sync_privileges(src, tgt, schemas, dbname=dbname)
                    if priv_count:
                        console.print(f"  [{dbname}] Applied {priv_count} privilege grant(s)")

                # Step 4d-3: per-database configuration (ALTER DATABASE … SET
                # / ALTER ROLE … IN DATABASE … SET) — never carried by
                # logical replication, and missing settings (a per-database
                # search_path being the classic case) only surface after
                # cutover as runtime misbehaviour.
                progress.update(task, description=f"[{dbname}] Syncing database settings…")
                async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
                    set_count = await sync_db_settings(src, tgt, dbname)
                    if set_count:
                        console.print(f"  [{dbname}] Applied {set_count} per-database setting(s)")

                # Step 4e: final sequence value sync.  Identity-backed sequences
                # are not pre-created (their tables create them), so their
                # values can only be applied now — and every other sequence may
                # have advanced on the source while the data was being copied.
                progress.update(task, description=f"[{dbname}] Syncing sequence values…")
                seq_report = await sync_sequences_once(cfg, dbname)
                n_seq = sum(
                    1 for r in seq_report if r["status"] in ("updated", "orphaned_fixed")
                )
                if n_seq:
                    console.print(f"  [{dbname}] Advanced {n_seq} sequence value(s)")

                # Step 5: create the subscription, attached to the slot created
                # in step 3d — NOT creating a new one (create_slot=False).
                progress.update(task, description=f"[{dbname}] Setting up replication…")
                await create_subscription(cfg, dbname, create_slot=False)

                # Step 6: built-in post-bootstrap verification — a full
                # schema-drift scan, the same one 'detect-ddl' runs on demand,
                # so any gap left by the migration is visible immediately
                # rather than discovered later at cutover.  Report-only: never
                # applies fixes automatically.
                progress.update(task, description=f"[{dbname}] Verifying (detect-ddl)…")
                drift_report = await detect_drift(cfg, dbname)
                if drift_report.has_drift:
                    drift_warnings[dbname] = drift_report.summary
                    console.print(
                        f"  [bold yellow]⚠ [{dbname}] Post-bootstrap drift check: "
                        f"{drift_report.summary}[/bold yellow]"
                    )
                else:
                    console.print(f"  [{dbname}] Post-bootstrap drift check: clean")

                progress.update(task, description=f"[{dbname}] ✓ Done")

            except Exception as exc:
                reason = str(exc) if isinstance(exc, _DatabaseBootstrapFailed) else repr(exc)
                console.print(
                    f"  [bold red]✗ [{dbname}] Bootstrap FAILED: {reason}[/bold red]"
                )
                console.print(
                    f"  [bold red]  Cleaning up and aborting {dbname} before replication "
                    f"setup — fix the cause and re-run bootstrap for this database.[/bold red]"
                )
                log.error("Bootstrap failed for %s: %s", dbname, reason)

                # Only safe to roll back the slot/publication we created if no
                # subscription ended up depending on them.  Check the actual
                # server state rather than "did we reach that line" — a late
                # error (e.g. a timeout) could in principle fire right after
                # CREATE SUBSCRIPTION actually succeeded server-side, and
                # dropping the slot out from under a working subscription
                # would be worse than the original error.
                sub_exists = False
                try:
                    async with connect(cfg.target, dbname) as probe:
                        sub_exists = bool(await probe.fetchval(
                            "SELECT 1 FROM pg_subscription WHERE subname = $1"
                            " AND subdbid = (SELECT oid FROM pg_database"
                            " WHERE datname = current_database())",
                            sub_name(cfg, dbname),
                        ))
                except Exception:
                    pass  # can't verify — err on the side of NOT auto-dropping

                if sub_exists:
                    log.warning(
                        "[%s] A subscription already exists despite the error above — "
                        "NOT auto-dropping its slot/publication. Investigate with "
                        "'pg_emigrant status --database %s' and 'reinit-sync' if needed.",
                        dbname, dbname,
                    )
                else:
                    if slot is not None:
                        try:
                            await drop_replication_slot(cfg, dbname, slot.slot_name)
                        except Exception as cleanup_exc:
                            log.warning(
                                "Could not clean up slot %s for %s: %s",
                                slot.slot_name, dbname, cleanup_exc,
                            )
                    if pub_created:
                        try:
                            await drop_publication(cfg, dbname)
                        except Exception as cleanup_exc:
                            log.warning(
                                "Could not clean up publication for %s: %s", dbname, cleanup_exc,
                            )
                failed_dbs.append(dbname)
            finally:
                progress.remove_task(task)

    if object_warnings:
        console.rule("[bold yellow]Object sync warnings")
        for db, kinds in object_warnings.items():
            for kind, entries in kinds.items():
                for entry in entries:
                    console.print(f"  [yellow]{db}: {kind} {entry}[/yellow]")
        console.print(
            "[yellow]These objects are missing or stale on the target. "
            "Fix the causes (see warnings above) and re-run "
            "'pg_emigrant detect-ddl --apply' before cutover.[/yellow]"
        )

    if drift_warnings:
        console.rule("[bold yellow]Post-bootstrap drift check")
        for db, summary in drift_warnings.items():
            console.print(f"  [yellow]{db}: {summary}[/yellow]")
        console.print(
            "[yellow]Run 'pg_emigrant detect-ddl --database <db>' for the full "
            "itemised report, and 'detect-ddl --apply' to fix it, before "
            "cutover.[/yellow]"
        )

    if failed_dbs:
        console.rule("[bold red]Bootstrap FAILED")
        raise RuntimeError(
            "Bootstrap failed for database(s): "
            + ", ".join(failed_dbs)
            + " — initial data copy incomplete; replication was NOT configured "
            "for them. Fix the cause and re-run bootstrap."
        )

    if object_warnings or drift_warnings:
        console.rule("[bold yellow]Bootstrap complete — with warnings")
    else:
        console.rule("[bold green]Bootstrap complete")
