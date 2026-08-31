"""pg_tde (Percona Transparent Data Encryption) support for the TARGET cluster.

Opt-in via ``pg_emigrant bootstrap --using-pg-tde``.  Nothing in this module
runs unless that flag is set — an ordinary migration is byte-for-byte
unaffected.

What "using pg_tde" means in practice
-------------------------------------
pg_tde stores encrypted relations through its own table access method,
``tde_heap``.  Encryption is therefore a property of *each relation*, decided
at CREATE TABLE time — not a cluster or database switch that retro-encrypts
what already exists.  So making a migrated database encrypted has three
distinct parts, and this module covers all three:

1. **The target database must be able to encrypt at all.**  ``pg_tde`` has to
   be in ``shared_preload_libraries``, the extension has to be installed *in
   that database* (extensions are per-database), and a principal key has to be
   configured for it.  Without the key, the very first
   ``CREATE TABLE … USING tde_heap`` fails — so :func:`ensure_tde_ready`
   verifies all of it up front, before bootstrap creates a replication slot on
   the production source and starts copying data.

2. **Every table pg_emigrant creates must be created encrypted.**  Two
   mechanisms, deliberately overlapping:

   * an explicit ``USING tde_heap`` on the generated ``CREATE TABLE`` — the
     statement says what it means, and it is visible in the logs;
   * ``ALTER DATABASE … SET default_table_access_method = 'tde_heap'`` on the
     target database, which catches every *other* relation-creating path that
     does not go through the CREATE TABLE generator: materialized views,
     ``detect-ddl --apply``, ``sync-sequences``' new-table reconciliation, and
     any DDL the application itself runs after cutover.  Relying on the
     explicit clause alone would silently leave those unencrypted.

3. **Relations that already exist on the target must be converted.**  A
   re-bootstrap over a pre-created schema finds plain ``heap`` tables that
   ``CREATE TABLE IF NOT EXISTS`` will not touch.  :func:`enforce_access_method`
   rewrites them with ``ALTER TABLE … SET ACCESS METHOD tde_heap``.  Bootstrap
   calls it *before* the data copy, while those tables are still empty (the
   copy TRUNCATEs them anyway), so the rewrite is free; running it afterwards
   would rewrite the fully-loaded table a second time.

Version requirements
--------------------
``ALTER TABLE … SET ACCESS METHOD`` needs PostgreSQL 15+.  Naming an access
method on a *partitioned parent* (which has no storage of its own — it only
sets the default for future partitions) needs PostgreSQL 17+; on older targets
the clause is omitted for parents and applied to the leaf partitions, which is
where the rows actually live.  Both are checked against the target's real
server version rather than assumed.
"""

from __future__ import annotations

from dataclasses import dataclass

import asyncpg

from pg_emigrant.config import ReplicatorConfig
from pg_emigrant.db import connect
from pg_emigrant.utils import get_logger, qi, ql, qt

log = get_logger(__name__)

#: Extension and table access method names, as installed by Percona's pg_tde.
TDE_EXTENSION = "pg_tde"
TDE_ACCESS_METHOD = "tde_heap"

#: ``ALTER TABLE … SET ACCESS METHOD`` was introduced in PostgreSQL 15.
MIN_SET_ACCESS_METHOD_MAJOR = 15
#: Specifying an access method on a partitioned parent needs PostgreSQL 17.
MIN_PARTITIONED_AM_MAJOR = 17

# Zero-argument principal-key introspection functions, across pg_tde versions:
# 1.0 shipped pg_tde_principal_key_info(), later releases renamed it to
# pg_tde_key_info().  Probing by name (instead of hard-coding one) keeps this
# working on both without pinning pg_emigrant to a single pg_tde release.
_KEY_INFO_FUNCTIONS = ("pg_tde_key_info", "pg_tde_principal_key_info")

KEY_SETUP_HINT = (
    "Configure a key provider and a principal key for this database, e.g. "
    "(pg_tde 1.0 names):  SELECT pg_tde_add_database_key_provider_file"
    "('file-provider', '/secure/path/keyring.per');  SELECT "
    "pg_tde_set_key_using_database_key_provider('principal-key', "
    "'file-provider');  — older builds spell these pg_tde_add_key_provider_file"
    "() / pg_tde_set_principal_key(). A global provider plus a default "
    "principal key works too, and is what you want when the target database "
    "does not exist yet. Consult the pg_tde docs for your version: a key "
    "provider is a security decision (file / Vault / KMIP, and where its "
    "secrets live), so pg_emigrant deliberately does not create one for you."
)


class TdeNotAvailable(RuntimeError):
    """The target cannot create ``tde_heap`` relations — bootstrap must not start.

    Raised only for conditions that are *certain* to make the first encrypted
    ``CREATE TABLE`` fail.  Anything merely unverifiable is logged as a warning
    instead, so an unfamiliar pg_tde build cannot block a legitimate migration.
    """


@dataclass
class TdeStatus:
    """Read-only snapshot of pg_tde's state in one target database."""

    available: bool = False
    """``pg_tde`` appears in the target's ``pg_available_extensions``."""

    preloaded: bool | None = None
    """``pg_tde`` is in ``shared_preload_libraries`` (None = unreadable)."""

    installed: bool = False
    """``CREATE EXTENSION pg_tde`` has been run *in this database*."""

    version: str | None = None

    access_method: bool = False
    """``tde_heap`` is registered as a table access method in ``pg_am``."""

    key_configured: bool | None = None
    """True / False / None when this pg_tde build exposes no way to tell."""

    key_detail: str = ""

    @property
    def usable(self) -> bool:
        """True when nothing *known* prevents creating a ``tde_heap`` table."""
        return self.installed and self.access_method and self.key_configured is not False


# ──────────────────────────────────────────────────────────────────────────────
# Read-only probing — safe for preflight
# ──────────────────────────────────────────────────────────────────────────────

async def _probe_principal_key(conn: asyncpg.Connection) -> tuple[bool | None, str]:
    """Return ``(configured, detail)`` for this database's principal key.

    ``None`` means "could not be determined" — this pg_tde build exposes none
    of the known key-info functions.  That is reported as a warning by callers,
    never as a hard failure: guessing wrong would block a migration that would
    actually have worked.
    """
    rows = await conn.fetch(
        "SELECT n.nspname, p.proname"
        " FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace"
        " WHERE p.proname = ANY($1::text[]) AND p.pronargs = 0",
        list(_KEY_INFO_FUNCTIONS),
    )
    if not rows:
        return None, (
            "this pg_tde build exposes none of "
            + "(), ".join(_KEY_INFO_FUNCTIONS)
            + "(), so the principal key could not be verified from the catalog"
        )

    last_err = ""
    for r in rows:
        fn = f"{qi(r['nspname'])}.{qi(r['proname'])}"
        try:
            row = await conn.fetchrow(f"SELECT * FROM {fn}()")
        except Exception as exc:
            # pg_tde raises rather than returning NULLs when no key is set.
            last_err = (str(exc) or repr(exc)).splitlines()[0]
            continue
        if row is not None and any(v is not None for v in row.values()):
            return True, f"{r['proname']}() reports a principal key for this database"

    return False, last_err or (
        "the pg_tde key-info function reports no principal key for this database"
    )


async def probe_tde(conn: asyncpg.Connection) -> TdeStatus:
    """Inspect pg_tde in the connected database. Issues only ``SELECT``s."""
    st = TdeStatus()

    st.available = bool(await conn.fetchval(
        "SELECT 1 FROM pg_available_extensions WHERE name = $1", TDE_EXTENSION
    ))

    spl = await conn.fetchval("SELECT current_setting('shared_preload_libraries', true)")
    if spl is not None:
        st.preloaded = TDE_EXTENSION in {part.strip() for part in spl.split(",")}

    st.version = await conn.fetchval(
        "SELECT extversion FROM pg_extension WHERE extname = $1", TDE_EXTENSION
    )
    st.installed = st.version is not None

    # amtype 't' = table access method (as opposed to 'i', index).
    st.access_method = bool(await conn.fetchval(
        "SELECT 1 FROM pg_am WHERE amname = $1 AND amtype = 't'", TDE_ACCESS_METHOD
    ))

    if st.installed:
        st.key_configured, st.key_detail = await _probe_principal_key(conn)

    return st


# ──────────────────────────────────────────────────────────────────────────────
# Setup — called by bootstrap before anything irreversible happens
# ──────────────────────────────────────────────────────────────────────────────

async def _create_extension(conn: asyncpg.Connection) -> None:
    """``CREATE EXTENSION pg_tde`` with a usable search_path.

    :func:`pg_emigrant.db.connect` deliberately sets ``search_path = ''`` so
    every server-side deparse comes out schema-qualified.  ``CREATE EXTENSION``
    without an explicit schema resolves the target schema *through* search_path
    and fails with "no schema has been selected to create in" under that
    setting, so it is restored for the duration of this one statement.  Setting
    it here rather than passing ``WITH SCHEMA public`` also keeps working if a
    pg_tde build declares itself non-relocatable with a fixed schema.
    """
    await conn.execute("SET search_path TO public, pg_catalog;")
    try:
        await conn.execute(f"CREATE EXTENSION IF NOT EXISTS {qi(TDE_EXTENSION)};")
    finally:
        await conn.execute("SELECT pg_catalog.set_config('search_path', '', false);")


async def ensure_tde_ready(cfg: ReplicatorConfig, dbname: str) -> TdeStatus:
    """Make *dbname* on the target able to create ``tde_heap`` relations.

    Installs the extension if it is available but not yet present in this
    database — unavoidable for a freshly created target database, which is
    cloned from ``template0`` and therefore has no extensions at all, leaving
    the user no window in which to install it themselves.

    Raises :class:`TdeNotAvailable` when encryption is impossible: the package
    is not installed on the target host, ``pg_tde`` is missing from
    ``shared_preload_libraries``, ``tde_heap`` is not registered, or the
    database has no principal key.  Bootstrap calls this *before* creating the
    publication and replication slot, so a failure here costs nothing on the
    production source.
    """
    async with connect(cfg.target, dbname) as tgt:
        st = await probe_tde(tgt)

        if not st.installed:
            if not st.available:
                raise TdeNotAvailable(
                    f"--using-pg-tde was requested but the '{TDE_EXTENSION}' extension "
                    f"is not available on the target ({cfg.target.host}:{cfg.target.port}) "
                    f"— it is not in pg_available_extensions. Install the pg_tde package "
                    f"on the target host (it ships with Percona Server for PostgreSQL) "
                    f"and restart the server."
                )
            if st.preloaded is False:
                raise TdeNotAvailable(
                    f"--using-pg-tde was requested but '{TDE_EXTENSION}' is not in the "
                    f"target's shared_preload_libraries. pg_tde hooks the storage "
                    f"manager at startup, so CREATE EXTENSION refuses to run without "
                    f"it. Add it (e.g. shared_preload_libraries = 'pg_tde') and RESTART "
                    f"the target — a reload is not enough."
                )
            log.info(
                "pg_tde is not installed in target database %s — installing it "
                "(a freshly created database is cloned from template0 and has no "
                "extensions)", dbname,
            )
            await _create_extension(tgt)
            st = await probe_tde(tgt)

        if not st.installed:
            raise TdeNotAvailable(
                f"could not install the '{TDE_EXTENSION}' extension in target database "
                f"'{dbname}' — see the log above for the server's error."
            )

        if not st.access_method:
            raise TdeNotAvailable(
                f"the '{TDE_EXTENSION}' extension is installed in '{dbname}' (version "
                f"{st.version}) but it does not register the '{TDE_ACCESS_METHOD}' table "
                f"access method. Early pg_tde builds shipped only 'tde_heap_basic', and "
                f"'{TDE_ACCESS_METHOD}' additionally requires a PostgreSQL build with "
                f"Percona's storage-manager patches (Percona Server for PostgreSQL). "
                f"Upgrade the target, or migrate without --using-pg-tde."
            )

        if st.key_configured is False:
            raise TdeNotAvailable(
                f"the '{TDE_EXTENSION}' extension is ready in '{dbname}' but no principal "
                f"key is configured for it ({st.key_detail}). Every "
                f"CREATE TABLE … USING {TDE_ACCESS_METHOD} would fail. " + KEY_SETUP_HINT
            )
        if st.key_configured is None:
            log.warning(
                "[%s] Could not verify pg_tde's principal key — %s. Proceeding; if no "
                "key is set, the first encrypted CREATE TABLE will fail and bootstrap "
                "will abort this database before replication is configured.",
                dbname, st.key_detail,
            )

        log.info(
            "pg_tde ready in target database %s (extension %s, access method %s)",
            dbname, st.version, TDE_ACCESS_METHOD,
        )
        return st


async def set_database_default_access_method(cfg: ReplicatorConfig, dbname: str) -> bool:
    """``ALTER DATABASE … SET default_table_access_method = 'tde_heap'``.

    This is what makes encryption hold for relation-creating paths that do not
    go through pg_emigrant's CREATE TABLE generator — materialized views,
    ``detect-ddl --apply``, the new-table reconciliation inside
    ``sync-sequences``, and the application's own post-cutover DDL.  Being a
    database-level default it applies to every *new* session, so bootstrap sets
    it before schema sync (each step opens fresh connections) and re-asserts it
    after ``sync_db_settings``, which copies the source's per-database settings
    and would otherwise reinstate a source-side ``default_table_access_method``
    of plain ``heap`` over it.

    Returns False (with a warning) rather than raising if the migration role
    may not alter the database: the explicit ``USING tde_heap`` on generated
    CREATE TABLE statements still encrypts the tables themselves, and the
    post-bootstrap check reports whatever is left unencrypted.
    """
    stmt = (
        f"ALTER DATABASE {qi(dbname)} SET default_table_access_method = "
        f"{ql(TDE_ACCESS_METHOD)};"
    )
    try:
        async with connect(cfg.target, dbname) as tgt:
            await tgt.execute(stmt)
        log.info("Set default_table_access_method = %s on target database %s",
                 TDE_ACCESS_METHOD, dbname)
        return True
    except Exception as exc:
        log.warning(
            "Could not set default_table_access_method on target database %s: %s. "
            "Tables created by pg_emigrant are still encrypted via an explicit "
            "USING %s, but materialized views and later DDL (detect-ddl --apply, "
            "your application) will default to plain heap. Run manually as a "
            "database owner or superuser:  %s",
            dbname, exc, TDE_ACCESS_METHOD, stmt,
        )
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Conversion and verification of existing relations
# ──────────────────────────────────────────────────────────────────────────────

# Relations in scope whose access method is not tde_heap.  Extension-owned
# relations are excluded on purpose: they belong to an extension's own upgrade
# machinery (TimescaleDB's internal hypertables being the obvious case), and
# rewriting them under the extension is far more likely to break it than to
# protect anything the user cares about.
_WRONG_AM_SQL = """
SELECT
    n.nspname                  AS schema_name,
    c.relname                  AS table_name,
    c.relkind::text            AS relkind,
    COALESCE(am.amname, '?')   AS current_am,
    c.relpages                 AS relpages
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_am am  ON am.oid = c.relam
WHERE n.nspname = ANY($1::text[])
  AND c.relkind IN ('r', 'p', 'm')
  AND COALESCE(am.amname, '') <> $2
  AND NOT EXISTS (
      SELECT 1 FROM pg_depend d
      WHERE d.classid = 'pg_class'::regclass
        AND d.objid = c.oid
        AND d.deptype = 'e'
  )
ORDER BY n.nspname, c.relname;
"""


async def find_unencrypted(
    conn: asyncpg.Connection, schemas: list[str], *, target_major: int | None = None
) -> list[dict]:
    """Return relations in *schemas* that are not stored as ``tde_heap``.

    Partitioned parents are only reported on PostgreSQL 17+, where they can
    actually carry an access method; on older versions their ``relam`` is
    always 0 and flagging them would be pure noise — the rows live in the leaf
    partitions, which are checked on their own.
    """
    rows = await conn.fetch(_WRONG_AM_SQL, schemas, TDE_ACCESS_METHOD)
    major = target_major if target_major is not None else conn.get_server_version().major
    return [
        dict(r) for r in rows
        if not (r["relkind"] == "p" and major < MIN_PARTITIONED_AM_MAJOR)
    ]


async def enforce_access_method(
    cfg: ReplicatorConfig, dbname: str, schemas: list[str]
) -> tuple[list[str], list[str]]:
    """Convert every non-``tde_heap`` relation in *schemas* to ``tde_heap``.

    Returns ``(converted, failed)`` as ``"schema.name"`` / ``"schema.name: error"``
    strings.

    ``ALTER TABLE … SET ACCESS METHOD`` **rewrites the whole relation**, which
    is why bootstrap runs this before the initial data copy: at that point the
    tables it finds are either brand-new-and-empty or about to be TRUNCATEd by
    the copy, so the rewrite is free.  A relation that already holds pages is
    still converted — that is the entire point of asking for encryption — but
    it is logged at warning level, because the rewrite takes an ACCESS
    EXCLUSIVE lock and doubles the relation's disk usage while it runs.
    """
    converted: list[str] = []
    failed: list[str] = []

    async with connect(cfg.target, dbname) as tgt:
        major = tgt.get_server_version().major
        if major < MIN_SET_ACCESS_METHOD_MAJOR:
            log.warning(
                "[%s] Target is PostgreSQL %s — ALTER TABLE … SET ACCESS METHOD needs "
                "%s+. Pre-existing relations cannot be converted to %s; only tables "
                "created by this run are encrypted.",
                dbname, major, MIN_SET_ACCESS_METHOD_MAJOR, TDE_ACCESS_METHOD,
            )
            return converted, failed

        for rel in await find_unencrypted(tgt, schemas, target_major=major):
            fqn = qt(rel["schema_name"], rel["table_name"])
            # A materialized view is not an ALTER TABLE target for this
            # subcommand; it needs its own statement.
            verb = "ALTER MATERIALIZED VIEW" if rel["relkind"] == "m" else "ALTER TABLE"
            if rel["relpages"] and rel["relkind"] != "p":
                log.warning(
                    "[%s] Rewriting non-empty relation %s (%s pages) from %s to %s — "
                    "this holds an ACCESS EXCLUSIVE lock and needs room for a second "
                    "copy of the relation.",
                    dbname, fqn, rel["relpages"], rel["current_am"], TDE_ACCESS_METHOD,
                )
            try:
                await tgt.execute(
                    f"{verb} {fqn} SET ACCESS METHOD {qi(TDE_ACCESS_METHOD)};"
                )
                log.info("[%s] %s: access method %s → %s",
                         dbname, fqn, rel["current_am"], TDE_ACCESS_METHOD)
                converted.append(f"{rel['schema_name']}.{rel['table_name']}")
            except Exception as exc:
                msg = (str(exc) or repr(exc)).splitlines()[0]
                log.warning("[%s] Could not convert %s to %s: %s",
                            dbname, fqn, TDE_ACCESS_METHOD, msg)
                failed.append(f"{rel['schema_name']}.{rel['table_name']}: {msg}")

    return converted, failed


async def verify_encrypted(
    cfg: ReplicatorConfig, dbname: str, schemas: list[str]
) -> list[str]:
    """Report-only: relations in *schemas* still not stored as ``tde_heap``.

    Report-only on purpose.  It runs at the very end of a database's bootstrap,
    where converting would mean rewriting fully-loaded tables at the least
    convenient moment; a migration that asked for encryption and did not fully
    get it needs to say so loudly, not fix it silently behind the user's back.
    """
    async with connect(cfg.target, dbname) as tgt:
        return [
            f"{r['schema_name']}.{r['table_name']} ({r['relkind']}, am={r['current_am']})"
            for r in await find_unencrypted(tgt, schemas)
        ]
