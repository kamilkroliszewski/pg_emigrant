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

Every database ends in exactly one terminal state, and the run reports the
worst of them (see :mod:`pg_emigrant.report`):

  * ``success``    — copied, replicating, nothing outstanding.
  * ``incomplete`` — copied and replicating, but something the migration was
    asked to reproduce is missing (a view that would not compile, a trigger,
    a sequence that could not be read, residual drift).  The subscription is
    deliberately LEFT RUNNING — tearing it down would force a needless full
    re-copy — but the run exits non-zero and the target must not be cut over
    to until the listed problems are resolved.
  * ``failed``     — aborted before replication; the slot and publication this
    run created on the source were rolled back.
  * ``refused``    — refused up front because proceeding would be unsafe.

None of these is ever downgraded to a warning on a zero exit: a bootstrap that
did not fully reproduce the source is not a successful bootstrap.

``use_pg_tde`` (``--using-pg-tde``) adds three steps to the sequence above,
and changes nothing when it is off:

  * 2c — verify the target database can encrypt at all (extension installed,
    ``tde_heap`` registered, principal key configured) and set the database's
    ``default_table_access_method``.  Deliberately BEFORE (c)/(d): a target
    that cannot encrypt should be rejected before a replication slot exists on
    the production source.
  * 3a-tde — convert any pre-existing target table to ``tde_heap``, while it
    is still empty (the data copy TRUNCATEs it moments later, so the rewrite
    costs nothing here and would cost a full second rewrite afterwards).
  * 6b — verify, report-only, that nothing in scope was left unencrypted.

Tables created by the run itself carry an explicit ``USING tde_heap`` from
the CREATE TABLE generator, so encryption never depends on the database
default alone.  See :mod:`pg_emigrant.tde`.
"""

from __future__ import annotations

import asyncio

from rich.progress import Progress, SpinnerColumn, TextColumn

from pg_emigrant.config import ReplicatorConfig
from pg_emigrant.data_copy import (
    IncompatibleTargetColumns,
    UnsafeTruncate,
    copy_all_tables,
    verify_copy_counts,
)
from pg_emigrant.db import connect, discover_databases, discover_schemas
from pg_emigrant.ddl_detector import detect_drift
from pg_emigrant.guards import assert_distinct_clusters
from pg_emigrant.report import BootstrapIncomplete, BootstrapReport, DatabaseResult
from pg_emigrant.replication import (
    create_publication,
    create_replication_slot_with_snapshot,
    create_subscription,
    drop_publication,
    drop_replication_slot,
    sub_name,
    warn_if_unstable_host,
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
from pg_emigrant._testhooks import maybe_fail
from pg_emigrant.sequence_sync import sync_sequences_once
from pg_emigrant.tde import (
    TDE_ACCESS_METHOD,
    TdeNotAvailable,
    enforce_access_method,
    ensure_tde_ready,
    set_database_default_access_method,
    verify_encrypted,
)
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


async def bootstrap(
    cfg: ReplicatorConfig,
    database: str | None = None,
    *,
    use_pg_tde: bool = False,
) -> BootstrapReport:
    """Run the full bootstrap migration.

    ``use_pg_tde`` migrates into pg_tde-encrypted storage on the target: every
    table is created ``USING tde_heap``, pre-existing target tables are
    converted, and a target that cannot encrypt aborts the database before
    anything is created on the source.  See the module docstring.

    Returns the :class:`~pg_emigrant.report.BootstrapReport` on full success,
    and raises :class:`~pg_emigrant.report.BootstrapIncomplete` (carrying the
    same report) otherwise — so a caller that only checks for an exception
    still cannot mistake a partial migration for a complete one.
    """
    console.rule("[bold green]pg_emigrant bootstrap")
    if use_pg_tde:
        console.print(
            f"[cyan]pg_tde enabled[/cyan] — target tables will be created "
            f"[bold]USING {TDE_ACCESS_METHOD}[/bold]"
        )

    # Passed to the CREATE TABLE generator; None leaves the emitted DDL exactly
    # as it is for an ordinary migration.
    access_method = TDE_ACCESS_METHOD if use_pg_tde else None

    warn_if_unstable_host(cfg)

    # Before anything is discovered, created or truncated.  Deliberately here
    # and not only in the (skippable, CLI-only) preflight: the initial copy
    # TRUNCATEs its target tables, so a target that is really the source
    # destroys production data — and the web GUI and library callers never run
    # preflight at all.
    await assert_distinct_clusters(cfg)

    # Step 1: discover databases
    if database:
        databases = [database]
    else:
        databases = await discover_databases(cfg)
    console.print(f"Databases to migrate: {databases}")

    report = BootstrapReport()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        for dbname in databases:
            result = report.add(dbname)
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
                maybe_fail("database_create")
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

                # Step 2c: pg_tde readiness — deliberately BEFORE the publication
                # and the replication slot.  A target that cannot encrypt must be
                # rejected while nothing has been created on the production source
                # yet; discovering it at the first CREATE TABLE would mean tearing
                # a live slot back down.  Raises TdeNotAvailable, which the
                # per-database handler below reports and cleans up after.
                if use_pg_tde:
                    progress.update(task, description=f"[{dbname}] Checking pg_tde…")
                    tde_status = await ensure_tde_ready(cfg, dbname)
                    console.print(
                        f"  [{dbname}] pg_tde {tde_status.version} ready — "
                        f"tables will use {TDE_ACCESS_METHOD}"
                    )
                    # Database-level default, so that relation-creating paths
                    # that never see `access_method` (materialized views,
                    # detect-ddl --apply, post-cutover application DDL) encrypt
                    # too.  Re-asserted after sync_db_settings in step 4d-3.
                    await set_database_default_access_method(cfg, dbname)

                # Step 3: discover schemas for this database, then synchronize them
                progress.update(task, description=f"[{dbname}] Syncing schemas…")
                async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
                    schemas = await discover_schemas(src, cfg)
                    console.print(f"  [{dbname}] Schemas: {schemas}")
                    maybe_fail("schema_create")
                    await sync_schemas(
                        src, tgt, schemas,
                        access_method=access_method,
                        exclude_tables=cfg.exclude_tables,
                    )

                # Step 3a-tde: convert relations that already existed on the
                # target (a pre-created schema, or a re-run) — CREATE TABLE IF
                # NOT EXISTS leaves those with whatever storage they had.  Here
                # they are still empty, so SET ACCESS METHOD's rewrite is free;
                # after the copy it would rewrite the loaded table all over again.
                if use_pg_tde:
                    progress.update(task, description=f"[{dbname}] Applying {TDE_ACCESS_METHOD}…")
                    converted, conv_failed = await enforce_access_method(cfg, dbname, schemas)
                    if converted:
                        console.print(
                            f"  [{dbname}] Converted {len(converted)} pre-existing "
                            f"relation(s) to {TDE_ACCESS_METHOD}"
                        )
                    if conv_failed:
                        raise _DatabaseBootstrapFailed(
                            f"could not convert {len(conv_failed)} relation(s) to "
                            f"{TDE_ACCESS_METHOD}: {'; '.join(conv_failed)}"
                        )

                # Step 3b: REPLICA IDENTITY FULL for PK-less tables, on the SOURCE
                # before the slot exists — see the module docstring for why this
                # ordering is required (REPLICA IDENTITY is evaluated at WAL-write
                # time, not at slot-creation time).
                progress.update(task, description=f"[{dbname}] Setting replica identity…")
                maybe_fail("replica_identity")
                async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
                    ri_failures = await sync_replica_identity(
                        src, tgt, schemas, exclude_tables=cfg.exclude_tables
                    )
                if ri_failures:
                    # Deliberately fatal, and deliberately here — before the
                    # publication exists.  See sync_replica_identity: a PK-less
                    # published table with no usable replica identity makes
                    # PostgreSQL reject every UPDATE/DELETE against it on the
                    # production SOURCE.  Stopping now costs nothing; carrying
                    # on would take the source down.
                    raise _DatabaseBootstrapFailed(
                        "could not set REPLICA IDENTITY FULL on "
                        f"{len(ri_failures)} PK-less table(s): "
                        + "; ".join(ri_failures)
                        + ". Publishing them would make PostgreSQL reject every "
                        "UPDATE/DELETE against them on the SOURCE, so nothing "
                        "was published. Give those tables a primary key, or "
                        "grant the migration role the privilege to ALTER them."
                    )

                # Step 3c/3d: publication, then the replication slot — BEFORE the
                # data copy, so the copy can use the slot's own exported snapshot.
                progress.update(task, description=f"[{dbname}] Creating publication…")
                maybe_fail("publication_create")
                await create_publication(cfg, dbname, schemas=schemas)
                pub_created = True

                progress.update(task, description=f"[{dbname}] Creating replication slot…")
                maybe_fail("slot_create")
                slot = await create_replication_slot_with_snapshot(cfg, dbname)

                # Step 4: copy initial data, using the slot's exported snapshot
                progress.update(task, description=f"[{dbname}] Copying data…")
                async with connect(cfg.source, dbname) as src:
                    all_tables = await get_tables(src, schemas, cfg.exclude_tables)
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
                    result.rows_copied = total_rows
                    result.tables_copied = len(results)
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
                maybe_fail("index_create")
                async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
                    await sync_deferred_indexes(
                        src, tgt, schemas, exclude_tables=cfg.exclude_tables
                    )

                # Step 4c: FK constraints, functions, views, triggers — post-COPY
                # so that PostgreSQL validates referential integrity across the
                # fully-loaded dataset.  Triggers are created last, after the
                # final function pass, so they never fail on a not-yet-created
                # function.
                progress.update(task, description=f"[{dbname}] Applying constraints…")
                maybe_fail("foreign_key")
                async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
                    obj_failures = await sync_post_copy_constraints(
                        src, tgt, schemas, exclude_tables=cfg.exclude_tables
                    )
                obj_failures = {k: v for k, v in obj_failures.items() if v}
                if obj_failures:
                    # NOT a warning.  A missing foreign key, function, view,
                    # trigger or RLS policy means the target is not the source,
                    # and the failure shows up after cutover as an application
                    # error rather than as anything a row count would catch.
                    console.print(
                        f"  [bold red]✗ [{dbname}] Some schema objects could NOT be created:[/bold red]"
                    )
                    for kind, entries in obj_failures.items():
                        for entry in entries:
                            console.print(f"    [red]✗ {kind} {entry}[/red]")
                            result.incomplete(f"{kind} not created: {entry}")

                # Step 4d: synchronize ownership (tables, sequences, views, functions, types, database)
                progress.update(task, description=f"[{dbname}] Syncing ownership…")
                async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
                    maybe_fail("ownership_sync")
                    own_count = await sync_ownership(
                        src, tgt, schemas, dbname=dbname,
                        exclude_tables=cfg.exclude_tables,
                    )
                    if own_count:
                        console.print(f"  [{dbname}] Applied {own_count} ownership change(s)")

                # Step 4d-2: synchronize GRANTs (tables, sequences, schemas,
                # functions, types, default privileges, database) — additive
                # only, never revokes; see sync_privileges() docstring.
                progress.update(task, description=f"[{dbname}] Syncing privileges…")
                async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
                    maybe_fail("privilege_sync")
                    priv_count = await sync_privileges(
                        src, tgt, schemas, dbname=dbname,
                        exclude_tables=cfg.exclude_tables,
                    )
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
                # sync_db_settings copies the SOURCE's per-database settings, so a
                # source that pins default_table_access_method (to plain 'heap',
                # typically) has just overwritten the value set in step 2c.  The
                # encrypted target's own storage default has to win.
                if use_pg_tde:
                    await set_database_default_access_method(cfg, dbname)

                # Step 4e: final sequence value sync.  Identity-backed sequences
                # are not pre-created (their tables create them), so their
                # values can only be applied now — and every other sequence may
                # have advanced on the source while the data was being copied.
                progress.update(task, description=f"[{dbname}] Syncing sequence values…")
                maybe_fail("sequence_sync")
                seq_report = await sync_sequences_once(cfg, dbname)
                n_seq = sum(
                    1 for r in seq_report if r["status"] in ("updated", "orphaned_fixed")
                )
                if n_seq:
                    console.print(f"  [{dbname}] Advanced {n_seq} sequence value(s)")
                # A sequence left behind on the target hands out already-used
                # values the moment the application starts writing there, so an
                # unsynchronised one is a duplicate-key outage waiting for the
                # cutover — never a warning.
                seq_bad = [
                    r for r in seq_report
                    if r["status"] in ("permission_denied", "missing_on_target",
                                       "orphaned_unknown", "orphaned_error")
                ]
                for r in seq_bad:
                    console.print(
                        f"  [bold red]✗ [{dbname}] sequence {r['schema']}.{r['sequence']}: "
                        f"{r['status']}[/bold red]"
                    )
                    result.incomplete(
                        f"sequence {r['schema']}.{r['sequence']} not synchronised "
                        f"({r['status']}) — duplicate-key risk at cutover"
                    )

                # Step 5: create the subscription, attached to the slot created
                # in step 3d — NOT creating a new one (create_slot=False).
                progress.update(task, description=f"[{dbname}] Setting up replication…")
                maybe_fail("subscription_create")
                await create_subscription(cfg, dbname, create_slot=False)

                # Step 6: built-in post-bootstrap verification — a full
                # schema-drift scan, the same one 'detect-ddl' runs on demand,
                # so any gap left by the migration is visible immediately
                # rather than discovered later at cutover.  Report-only: never
                # applies fixes automatically.
                progress.update(task, description=f"[{dbname}] Verifying (detect-ddl)…")
                drift_report = await detect_drift(cfg, dbname)
                if drift_report.has_drift:
                    console.print(
                        f"  [bold red]✗ [{dbname}] Post-bootstrap drift check: "
                        f"{drift_report.summary}[/bold red]"
                    )
                    result.incomplete(
                        f"schema drift remains after bootstrap ({drift_report.summary}) "
                        f"— run 'detect-ddl --database {dbname}' for the itemised report"
                    )
                else:
                    console.print(f"  [{dbname}] Post-bootstrap drift check: clean")

                # Step 6b: report-only encryption check.  Report-only because
                # converting here would rewrite fully-loaded tables at the worst
                # possible moment — a migration that asked for encryption and
                # did not fully get it has to say so, not quietly paper over it.
                if use_pg_tde:
                    progress.update(task, description=f"[{dbname}] Verifying encryption…")
                    unencrypted = await verify_encrypted(cfg, dbname, schemas)
                    if unencrypted:
                        console.print(
                            f"  [bold red]✗ [{dbname}] {len(unencrypted)} relation(s) "
                            f"are NOT stored as {TDE_ACCESS_METHOD}[/bold red]"
                        )
                        result.incomplete(
                            f"{len(unencrypted)} relation(s) are readable on disk "
                            f"without the pg_tde key despite --using-pg-tde: "
                            + ", ".join(unencrypted)
                        )
                    else:
                        console.print(
                            f"  [{dbname}] Encryption check: all relations use "
                            f"{TDE_ACCESS_METHOD}"
                        )

                progress.update(task, description=f"[{dbname}] ✓ Done")

            except BaseException as exc:
                # BaseException, not Exception: Ctrl-C (KeyboardInterrupt) and
                # a cancelled task (which is how SIGTERM arrives — see
                # cli._run) are exactly the moments a half-created replication
                # slot is most likely to exist, and they are not Exceptions.
                # Letting them skip this handler is what left an orphaned
                # logical slot retaining WAL on the production source after
                # every interrupted run.
                interrupted = isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError))
                if interrupted:
                    reason = "interrupted before replication was configured"
                    console.print(
                        f"\n  [bold yellow]Interrupted — rolling back {dbname}'s "
                        f"replication objects on the source. Do not kill this "
                        f"process; an abandoned slot retains WAL.[/bold yellow]"
                    )
                else:
                    # Both of these carry a message written to be read by a human;
                    # anything else is an unexpected exception whose repr() (type
                    # included) is the more useful thing to show.
                    reason = (
                        str(exc)
                        if isinstance(exc, (
                            _DatabaseBootstrapFailed,
                            TdeNotAvailable,
                            IncompatibleTargetColumns,
                            UnsafeTruncate,
                        ))
                        else repr(exc)
                    )
                    console.print(
                        f"  [bold red]✗ [{dbname}] Bootstrap FAILED: {reason}[/bold red]"
                    )
                    console.print(
                        f"  [bold red]  Cleaning up and aborting {dbname} before replication "
                        f"setup — fix the cause and re-run bootstrap for this database.[/bold red]"
                    )
                log.error("Bootstrap failed for %s: %s", dbname, reason)

                try:
                    # Bounded: an unreachable source must not turn an interrupt
                    # into a hang.  If it does time out, the orphan is left
                    # behind deliberately and named in the message below — the
                    # next bootstrap run adopts it (and 'teardown' removes it).
                    await asyncio.wait_for(
                        _rollback_replication_state(cfg, dbname, slot, pub_created),
                        timeout=60,
                    )
                except (asyncio.TimeoutError, Exception) as cleanup_exc:
                    log.error(
                        "[%s] Could not roll back replication state: %s. If a "
                        "replication slot named %r still exists on the source it "
                        "is retaining WAL — re-run bootstrap for this database "
                        "(which adopts it) or drop it with 'pg_emigrant teardown "
                        "--database %s'.",
                        dbname, cleanup_exc, sub_name(cfg, dbname), dbname,
                    )

                result.fail(reason)
                if interrupted:
                    # The operator asked for this to stop, so stop — but only
                    # after the rollback above, and with the report intact so
                    # the caller still reports a non-zero, explained outcome.
                    _print_summary(report)
                    raise BootstrapIncomplete(report) from exc
            finally:
                progress.remove_task(task)

    _print_summary(report)
    if not report.passed:
        raise BootstrapIncomplete(report)
    return report


async def _rollback_replication_state(
    cfg: ReplicatorConfig, dbname: str, slot, pub_created: bool
) -> None:
    """Undo the source-side objects this database's run created.

    Only safe when no subscription ended up depending on them, so the actual
    server state is checked rather than "did we reach that line" — a late error
    (a timeout, an interrupt) can fire right after CREATE SUBSCRIPTION
    succeeded server-side, and dropping the slot out from under a working
    subscription would be worse than the error that got us here.
    """
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
        return

    # The slot may exist even when this run has no handle on it: an interrupt
    # can land between CREATE_REPLICATION_SLOT returning on the server and the
    # handle being assigned here, and that orphan is the one that quietly
    # retains WAL.  Drop by name, which covers both cases.
    slot_name = slot.slot_name if slot is not None else sub_name(cfg, dbname)
    try:
        await drop_replication_slot(cfg, dbname, slot_name)
    except Exception as cleanup_exc:
        log.warning("Could not clean up slot %s for %s: %s", slot_name, dbname, cleanup_exc)
    if pub_created:
        try:
            await drop_publication(cfg, dbname)
        except Exception as cleanup_exc:
            log.warning("Could not clean up publication for %s: %s", dbname, cleanup_exc)


def _print_summary(report: BootstrapReport) -> None:
    """Final, unmissable statement of what each database ended up as.

    Printed after the per-database detail so that the last thing on screen is
    the conclusion, not the scrollback of a long run.
    """
    from pg_emigrant.report import Outcome

    refused = report.by_outcome(Outcome.REFUSED)
    failed = report.by_outcome(Outcome.FAILED)
    incomplete = report.by_outcome(Outcome.INCOMPLETE)

    for group, title, advice in (
        (refused, "REFUSED — nothing was changed",
         "Resolve the condition above and re-run bootstrap for these databases."),
        (failed, "FAILED — replication was NOT configured",
         "The replication objects this run created on the source were rolled "
         "back. Fix the cause and re-run bootstrap for these databases."),
        (incomplete, "INCOMPLETE — replicating, but NOT ready to cut over",
         "The data copy and replication succeeded and are deliberately left "
         "running, but the items above are missing on the target. Resolve them "
         "(often 'pg_emigrant detect-ddl --apply') and confirm with "
         "'pg_emigrant cutover-check' before cutting over."),
    ):
        if not group:
            continue
        console.rule(f"[bold red]{title}")
        for db in group:
            console.print(f"  [bold red]{db.database}[/bold red]")
            for problem in db.problems:
                console.print(f"    [red]• {problem}[/red]")
        console.print(f"[red]{advice}[/red]")

    if report.passed:
        console.rule(f"[bold green]Bootstrap complete — {report.summary}")
    else:
        console.rule(f"[bold red]Bootstrap did NOT complete — {report.summary}")
