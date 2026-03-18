# pg_emigrant

**PostgreSQL migration & replication orchestrator** — native logical replication with sequence sync, DDL drift detection, parallel initial copy, and automatic recovery from Patroni failover/switchover events.

---

## What is pg_emigrant?

Migrating a PostgreSQL database between servers is usually a headache: taking the application offline, a lengthy dump/restore, manual sequence synchronisation, and ultimately — the fear of the final cutover. **pg_emigrant** eliminates that pain.

The tool is built on top of PostgreSQL's native logical replication, but goes a step further — it resolves its well-known limitations and delivers a set of capabilities you won't find in any other open-source solution of this kind.

### Why pg_emigrant?

**Full failover and switchover support for Patroni clusters**
Patroni clusters can promote a new primary at any moment. Typical replication solutions lose track after such an event — pg_emigrant deliberately manages connections and subscriptions to minimise the risk of data loss when cutting traffic over to a new target server.

**Sequence synchronisation without the risk of primary key conflicts**
Native PostgreSQL logical replication does not replicate sequence advances. This means that after the cutover, `SERIAL` / `BIGSERIAL` columns may generate primary keys that collide with existing records. pg_emigrant tracks sequences on the source and periodically updates them on the target — always moving forward, never backward.

**Schema drift detection and automatic repair (DDL drift)**
DDL changes applied on the source server (new tables, columns, indexes, constraints) are not replicated by PostgreSQL. pg_emigrant compares schemas on both servers and can automatically generate and apply corrective DDL — with no downtime and no manual work.

**Lightning-fast initial copy using parallel workers**
Instead of a serial `pg_dump | pg_restore`, pg_emigrant exports data using multiple parallel workers leveraging the COPY protocol and a consistent transaction snapshot. Many tables are copied simultaneously — reducing migration time by an order of magnitude on large databases.

**Automatic database and schema discovery**
You don't need to enumerate every database or schema to migrate. pg_emigrant automatically discovers databases on the source server (skipping system ones), and for each database independently discovers all user-defined schemas — skipping PostgreSQL internals (`pg_catalog`, `information_schema`, `pg_toast`, `pg_temp_*`). Everything starts replicating immediately — zero per-database, zero per-schema configuration.

**Index creation after data copy for maximum throughput**
pg_emigrant creates non-unique indexes **after** the initial data COPY, not before. PostgreSQL can then build each index in a single sequential scan instead of updating it row-by-row during the bulk insert — significantly reducing migration time for large, heavily-indexed databases. Primary keys and unique indexes are always created first (required by the replication engine).

**Clean YAML configuration with Pydantic validation**
The entire configuration fits in a single YAML file. Pydantic enforces correctness and reports errors before anything runs. No hidden parameters, no magic defaults.

**Real-time monitoring**
The built-in status dashboard shows subscription state, replication slot activity, WAL lag, and sequence drift for every database — all in one place, without having to dig through `pg_stat_replication`.

---

**pg_emigrant** combines both approaches:

| Feature | Native replication | Bucardo | pg_emigrant |
|---|---|---|---|
| WAL (logical) replication | ✅ | ❌ | ✅ |
| Automatic sequence synchronisation | ❌ | ✅ | ✅ |
| Schema drift detection (DDL drift) | ❌ | ❌ | ✅ |
| Parallel initial copy (COPY protocol) | ❌ | ❌ | ✅ |
| Auto-discovery of databases | ❌ | ❌ | ✅ |
| Auto-discovery of schemas per database | ❌ | ❌ | ✅ |
| Deferred index build (post-COPY) | ❌ | ❌ | ✅ |
| Table/sequence/schema ownership sync | ❌ | ❌ | ✅ |
| Pydantic config + YAML | ❌ | ❌ | ✅ |
| Patroni failover/switchover recovery | ❌ | ❌ | ✅ |
| Tables without PK (REPLICA IDENTITY FULL) | ❌ | ✅ | ✅ |
| Extension sync (btree_gin, pg_trgm, …) | ❌ | ❌ | ✅ |
| User-defined function sync | ❌ | ❌ | ✅ |
| Stored generated columns | ❌ | ❌ | ✅ |
| Case-sensitive table/schema names | ✅ | ✅ | ✅ |
---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         pg_emigrant CLI                         │
│  bootstrap │ start │ stop │ teardown │ status │ sync-sequences  │
│                  detect-ddl │ reinit-sync                       │
└──────────────────────────┬──────────────────────────────────────┘
                           │
         ┌─────────────────▼────────────────────┐
         │           bootstrap.py               │
         │  1. discover databases               │
         │  2. create DBs on target             │
         │  3. discover schemas (per DB)        │
         │  4. sync schemas (no plain indexes)  │
         │  5. parallel COPY (snapshot safe)    │
         │  6. create plain indexes (post-COPY  │
         │  7. sync ownership (tables/seq/sch)  │
         │  8. create publication               │
         │  9. create subscription              │
         └─────────────────┬────────────────────┘
                           │
    ┌──────────────────────┼──────────────────────┐
    │                      │                      │
┌───▼───┐            ┌─────▼──────┐        ┌──────▼──────┐
│schema │            │replication │        │sequence_sync│
│_sync  │            │    .py     │        │    .py      │
│       │            │            │        │             │
│tables │            │publication │        │ periodic    │
│columns│            │subscription│        │ setval()    │
│indexes│            │  slots     │        │ source→tgt  │
│constr.│            └────────────┘        └─────────────┘
│seq.   │
└───────┘
    │
┌───▼──────────┐    ┌──────────────┐
│ddl_detector  │    │  monitor.py  │
│              │    │              │
│ detect drift │    │ sub status   │
│ apply fixes  │    │ slot lag     │
│              │    │ seq drift    │
└──────────────┘    └──────────────┘
```

### Bootstrap flow (full migration)

```
Source DB ──────────────────────────────────────────► Target DB
    │                                                     │
    │  1. pg_export_snapshot()                            │
    │  2. COPY TO (CSV format, parallel workers) ────────►│
    │                                                     │
    │  CREATE PUBLICATION pg_emigrant_pub                 │
    │         FOR TABLES IN SCHEMA public     ◄───────────┤
    │                                                     │ CREATE SUBSCRIPTION pg_emigrant_sub
    │  WAL stream (logical replication) ─────────────────►│   copy_data = false
    │                                                     │   create_slot = true
    │                                                     │
    │  sequence values (polled every N sec.) ────────────►│ setval(seq, src_val)
```

---

## Requirements

- Python **3.11+**
- PostgreSQL **14+** on both servers (source and target)
- A database user with logical replication privileges:
  - `REPLICATION` on source (for creating publications and slots)
  - Superuser or `CREATE SUBSCRIPTION` on target
- `wal_level = logical` on the source server

### PostgreSQL — minimum source configuration

```ini
wal_level = logical
max_replication_slots = 10   # at least 1 per database
max_wal_senders = 10
```

### PostgreSQL — minimum target configuration

```ini
max_active_replication_origins = 10  # min. one per source database)
max_logical_replication_workers = 10 # min. one per source database)
max_replication_slots = 10   # at least 1 per database
```

---

## Installation

```bash
# Clone the repository
git clone https://github.com/yourorg/pg_emigrant.git
cd pg_emigrant

# Install (preferably inside a venv)
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

After installation the `replicator` command is available:

```bash
replicator --help
```

---

## Configuration

Create a `config.yaml` file (the default location) or point to a custom one with `--config`:

```yaml
# Source server
source:
  host: localhost
  port: 5433
  user: migrator
  password: secret
  dbname: postgres        # admin DB — used for auto-discovery
  sslmode: prefer

# Target server
target:
  host: localhost
  port: 5434
  user: migrator
  password: secret
  dbname: postgres
  sslmode: prefer

# Schemas to replicate — empty = auto-discover all non-system schemas per database
# (excludes pg_catalog, information_schema, pg_toast, pg_temp_*).
# Explicit list example: [public, analytics, reporting]
schemas: []

# Databases to migrate — empty = auto-discovery (all except excluded)
databases: []

# System databases to skip during auto-discovery
exclude_databases:
  - template0
  - template1
  - postgres

# Tables to skip (format: schema.table)
exclude_tables: []

# Prefix for replication object names (the database name is appended to each).
# E.g. for database "myapp": publication = pg_emigrant_pub_myapp, subscription = pg_emigrant_sub_myapp.
# The replication slot is created automatically by PostgreSQL with the same name as the subscription.
publication_name: pg_emigrant_pub
subscription_name: pg_emigrant_sub

# Number of parallel workers for data copy
parallel_workers: 4

# Sequence synchronisation interval (seconds)
sequence_sync_interval: 10
```

### Configuration parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `source.*` | `DatabaseConfig` | — | Source connection details |
| `target.*` | `DatabaseConfig` | — | Target connection details |
| `schemas` | `list[str]` | `[]` | Schemas to replicate per database; empty = auto-discover all non-system schemas (`pg_catalog`, `information_schema`, `pg_toast`, `pg_temp_*` are always excluded) |
| `databases` | `list[str]` | `[]` | Databases to migrate; empty = auto-discover |
| `exclude_databases` | `list[str]` | `[template0,template1,postgres]` | Databases skipped during discovery |
| `exclude_tables` | `list[str]` | `[]` | Tables to skip |
| `publication_name` | `str` | `pg_emigrant_pub` | Publication name prefix; `_{dbname}` is appended per database |
| `subscription_name` | `str` | `pg_emigrant_sub` | Subscription name prefix; the replication slot gets the same name as the subscription |
| `parallel_workers` | `int` | `4` | Parallel workers for COPY — how many tables are copied simultaneously |
| `table_parallel_workers` | `int` | `4` | Parallel workers **per table** — each worker streams an independent ctid page-range slice; increase this for very large tables |
| `sequence_sync_interval` | `int` | `10` | Sequence synchronisation interval [s] |

---

## CLI Commands

### `replicator bootstrap`

Full automated initial migration:

1. Auto-discovery of databases on source
2. Create databases on target (if they do not exist)
3. Per-database auto-discovery of user schemas (skips `pg_catalog`, `information_schema`, `pg_toast`, `pg_temp_*`)
4. Schema synchronisation (tables, columns, PK/unique indexes, constraints, sequences); `SERIAL`/`BIGSERIAL` columns are reproduced with the original `DEFAULT nextval(...)` — the named sequence is created explicitly beforehand so PostgreSQL does not auto-generate a shadow `_seq1` sequence; **stored generated columns** (`GENERATED ALWAYS AS … STORED`) are reproduced correctly
5. Extension synchronisation — extensions present on the source (e.g. `btree_gin`, `pg_trgm`) are installed on the target before any tables or indexes are created
6. User-defined function synchronisation — functions and procedures in migrated schemas are applied to the target before tables, so that generated columns and DEFAULT expressions that call them work correctly
7. Tables without a PRIMARY KEY are automatically detected; `REPLICA IDENTITY FULL` is set on **both** source and target and the table is highlighted in yellow in the bootstrap output
8. Parallel initial COPY (snapshot-consistent, CSV format for cross-version PG compatibility); correctly handles tables with mixed-case names (e.g. `__EFMigrationsHistory`)
9. Non-unique index creation **after** COPY — faster than incremental updates during bulk insert
10. Ownership synchronization for tables, sequences, and schemas (`ALTER ... OWNER TO`)
11. Create publication on source
12. Create subscription on target (without re-copying data)

```bash
replicator bootstrap
replicator bootstrap --config /path/to/config.yaml
replicator bootstrap --database mydb
replicator --verbose bootstrap
```

---

### `replicator start` / `replicator stop`

Enable and disable (pause) replication subscriptions:

```bash
# All databases
replicator start
replicator stop

# A specific database
replicator start --database mydb
replicator stop  --database mydb
```

---

### `replicator teardown`

Removes the subscription, publication, and replication slot:

```bash
replicator teardown
replicator teardown --database mydb
```

> **Warning:** this operation is irreversible. After teardown you must run `bootstrap` again to resume replication.

---

### `replicator status`

Displays a rich, **read-only** status dashboard for all databases. The command never writes anything to the target — it is safe to run at any point, including before `bootstrap`.

- **Subscription status** — worker PID, Received LSN, Last Message timestamps
- **Replication slots (source)** — slot state, WAL lag, Restart LSN
- **Replication lag** — WAL delay between source and target
- **Tables per schema** — source vs target table count broken down by schema; rows with a mismatch are highlighted in yellow
- **Sequence sync status** — read-only comparison of sequence values on source and target (no `setval()` calls)
- **Schema drift summary** — indication of detected DDL drift

```bash
replicator status
replicator status --config /path/to/config.yaml
replicator status --database mydb
```

---

### `replicator sync-sequences`

Sequence synchronisation — the key feature that sets pg_emigrant apart from native logical replication.

Sequences are propagated one-way (source → target), and the value on target is **never decreased** (guard against rollback).

```bash
# One-off synchronisation (displays a results table)
replicator sync-sequences

# A specific database
replicator sync-sequences --database mydb

# Continuous loop (recommended during a live migration)
replicator sync-sequences --loop
replicator sync-sequences --database mydb --loop
```

One-off synchronisation output:

```
┌───────────────────────────────────────────────────────────┐
│                 Sequence Sync — myapp                     │
├────────┬────────────────┬────────┬────────┬───────────────┤
│ Schema │ Sequence       │ Source │ Target │ Status        │
├────────┼────────────────┼────────┼────────┼───────────────┤
│ public │ users_id_seq   │ 10053  │ 9821   │ updated       │
│ public │ orders_id_seq  │ 45210  │ 45210  │ ok            │
│ public │ items_id_seq   │ 1200   │ 1350   │ target_ahead  │
└────────┴────────────────┴────────┴────────┴───────────────┘
```

| Status | Meaning |
|---|---|
| `ok` | Values are equal |
| `updated` | Target updated to the source value |
| `target_ahead` | Target is ahead of source — no change made (safe) |
| `orphaned_fixed` | Sequence exists only on target; reset to `MAX(id)+1` from its owning table |
| `orphaned_unknown` | Sequence exists only on target but no owning table could be found — manual action required |

> **Guard against premature sync:** `sync-sequences` checks whether the target has any sequences in the relevant schemas before writing. If none are found (database exists but has not been bootstrapped yet), the command exits without making any changes. This prevents phantom sequences from being created on a bare database before `bootstrap` runs.

---

### `replicator detect-ddl`

Detects schema drift (DDL drift) between source and target — useful when the source schema evolves during an ongoing replication.

```bash
# Detection only (report)
replicator detect-ddl
replicator detect-ddl --database mydb

# Detection + automatic application of fixes (missing objects)
replicator detect-ddl --apply
replicator detect-ddl --database mydb --apply

# Apply fixes + drop objects that exist on target but are missing on source
replicator detect-ddl --apply --drop-extra

# Detect and fix ownership drift (tables, sequences, schemas)
replicator detect-ddl --fix-roles
replicator detect-ddl --fix-roles --apply
replicator detect-ddl --fix-roles --apply --database mydb
```

Objects checked:
- **Tables** — missing on target or on source
- **Columns** — missing, extra, type mismatches
- **Indexes** — missing on target
- **Constraints** — missing on target
- **Ownership** (`--fix-roles`) — `ALTER TABLE/SEQUENCE/SCHEMA ... OWNER TO` for every object whose owner differs between source and target

> **Note:** ownership sync requires that the target roles already exist. pg_emigrant skips objects whose source owner is absent on the target and logs a warning — create the missing roles manually first.

Sample ownership drift report:

```
━━━━━━━━━━━━━━━━━━━━━━ Ownership Drift — myapp ━━━━━━━━━━━━━━━━━━━━━━
┌──────────────────┬────────┬───────────┬─────────────────────────────────────┬────────────────────────────────────────────────┐
│ Kind             │ Schema │ Object    │ Detail                              │ Fix DDL                                        │
├──────────────────┼────────┼───────────┼─────────────────────────────────────┼────────────────────────────────────────────────┤
│ ownership(table) │ public │ orders    │ Owner mismatch: src=app, tgt=postgres│ ALTER TABLE "public"."orders" OWNER TO "app"; │
│ ownership(schema)│ public │ public    │ Owner mismatch: src=app, tgt=postgres│ ALTER SCHEMA "public" OWNER TO "app";         │
└──────────────────┴────────┴───────────┴─────────────────────────────────────┴────────────────────────────────────────────────┘
```

Sample report:

```
━━━━━━━━━━━━━━━━━━━━━  Drift Report — myapp  ━━━━━━━━━━━━━━━━━━━━━
┌────────────┬────────┬───────────┬─────────────────┬───────────────────┬──────────────────────────┬────────────────────────────────────────┐
│ Type       │ Schema │ Table     │ Name            │ Drift             │ Detail                   │ Fix DDL                                │
├────────────┼────────┼───────────┼─────────────────┼───────────────────┼──────────────────────────┼────────────────────────────────────────┤
│ column     │ public │ users     │ phone_number    │ missing_on_target │ Column phone_number (...)│ ALTER TABLE "public"."users" ADD ...   │
│ index      │ public │ orders    │ idx_orders_date │ missing_on_target │ CREATE INDEX ...         │ CREATE INDEX idx_orders_date ON ...    │
└────────────┴────────┴───────────┴─────────────────┴───────────────────┴──────────────────────────┴────────────────────────────────────────┘
```

---

### `replicator reinit-sync`

Re-initializes replication after a **Patroni switchover or failover** — the command that makes pg_emigrant production-safe in HA environments.

When Patroni promotes a new primary, the old replication slot and subscription can become broken or stale. `reinit-sync` performs a full health check and repairs whatever is needed — **without re-copying any data**.

Checks performed (in order):
1. **Publication** exists on source → recreates it if missing.
2. **Replication slot** exists on source and has a healthy `wal_status` (not `lost`).
3. **Subscription** on target matches the slot state:
   - Slot healthy, subscription disabled → re-enables the subscription.
   - Slot healthy, subscription enabled but apply worker not running → refreshes publication list.
   - Slot missing/unhealthy or subscription missing → drops broken subscription and orphaned slot, then creates a fresh subscription with a new slot.

Safe to run at any time — it only touches components that are missing or broken.

```bash
# Check and repair all databases
replicator reinit-sync

# Repair a specific database only
replicator reinit-sync --database mydb

# With a custom config
replicator reinit-sync --config /path/to/config.yaml
```

Sample output after a Patroni switchover:

```
──────────────────── Reinit Sync — myapp ────────────────────
  ⚠  Replication slot 'pg_emigrant_sub_myapp' not found on source
  ⚠  Subscription 'pg_emigrant_sub_myapp' not found on target
  ✓  Recreated subscription 'pg_emigrant_sub_myapp' with a fresh replication slot
──────────────────── Reinit complete — issues were detected and repaired ────────────────────
```

Sample output when everything is healthy:

```
──────────────────── Reinit Sync — myapp ────────────────────
  Replication for 'myapp' is healthy — nothing to do
──────────────────────── All databases are healthy ──────────────────────────
```

---

## Typical Migration Scenario

### 1. Preparation

```bash
# Configure config.yaml with source and target details
cp config.yaml.example config.yaml
# Edit config.yaml...
```

### 2. Bootstrap (one-time run)

```bash
replicator bootstrap --config config.yaml --verbose
```

Progress is displayed live with a Rich spinner.

### 3. Monitoring and ongoing sequence synchronisation

Run sequence synchronisation in the background in continuous mode:

```bash
# In a separate terminal / as a service
replicator sync-sequences --loop --config config.yaml
```

### 4. Verify status

```bash
replicator status --config config.yaml
```

### 5. Check for DDL drift (optional)

```bash
replicator detect-ddl --config config.yaml
replicator detect-ddl --config config.yaml --apply   # automatically applies fixes
```

### 6. Cutover

```bash
# Stop replication
replicator stop --config config.yaml

# Run one final sequence synchronisation
replicator sync-sequences --config config.yaml

# Redirect the application to the target
# ...

# Optionally: remove replication objects
replicator teardown --config config.yaml
```

---

## Multiple Databases

pg_emigrant can automatically discover and migrate all databases on the source server at once:

```yaml
# config.yaml
databases: []   # auto-discover all databases (except exclude_databases)
exclude_databases:
  - template0
  - template1
  - postgres
```

Or explicitly specify which databases to use:

```yaml
databases:
  - app_production
  - analytics
  - reporting
```

---

## Multiple Schemas

By default (`schemas: []`) pg_emigrant discovers all user-defined schemas **independently for each database**. This means that `myapp` can have `[public, analytics]` while `legacy` has only `[public]` — without any manual configuration.

The following schemas are always excluded:

| Schema pattern | Reason |
|---|---|
| `pg_catalog` | PostgreSQL system catalog |
| `information_schema` | SQL standard metadata views |
| `pg_toast` | TOAST storage (internal) |
| `pg_temp_*` | Temporary tables (session-scoped, not persistent) |
| `pg_toast_temp_*` | TOAST for temporary tables |

To replicate only specific schemas, list them explicitly:

```yaml
schemas:
  - public
  - analytics
```

The same list applies to all databases in that case. For per-database schema control, leave `schemas: []` and use `databases:` to migrate them in separate runs with different configs.

---

## Project Structure

```
pg_emigrant/
├── pyproject.toml              # package metadata and dependencies
├── fix_identity_sequences.sh   # one-off remediation for _seq1 sequences
├── replicator/
│   ├── __init__.py             # package version
│   ├── cli.py                  # CLI (Typer) — entry point
│   ├── config.py               # Pydantic models + YAML loader
│   ├── bootstrap.py            # full migration orchestrator
│   ├── db.py                   # asyncpg — connection pools, DB auto-discovery
│   ├── schema_sync.py          # introspection + schema synchronisation
│   ├── data_copy.py            # parallel COPY with snapshot isolation
│   ├── replication.py          # publication and subscription management
│   ├── sequence_sync.py        # sequence synchronisation source → target
│   ├── ddl_detector.py         # DDL drift detection and repair
│   ├── monitor.py              # replication status dashboard (read-only)
│   └── utils.py                # logging (Rich), SQL identifier quoting
```

---

## Dependencies

| Package | Version | Role |
|---|---|---|
| `asyncpg` | ≥0.29 | Async PostgreSQL driver (COPY, replication) |
| `pydantic` | ≥2.0 | Configuration validation and typing |
| `pyyaml` | ≥6.0 | Configuration file parsing |
| `typer` | ≥0.9 | CLI framework |
| `rich` | ≥13.0 | Coloured tables, progress bar, logging |

---

## Known Limitations

### Not yet implemented (planned or possible)

- **Batch migration** — when migrating many databases at once every database gets its own WAL sender process on the source (~1.3% CPU each). There is no built-in `batch_size` setting yet; the recommended workaround is to specify explicit `databases:` lists and run `bootstrap` in multiple rounds.
- **Partitioned tables** — logical replication of declaratively partitioned tables has quirks in PostgreSQL (child partitions must be replicated individually). pg_emigrant does not yet handle partition hierarchies automatically.
- **Tablespaces** — tables and indexes are always created in the default tablespace on the target; non-default tablespace definitions from the source are ignored.
- **Row-level filtering** — there is no per-table `WHERE` filter for the initial COPY or for publications; entire tables are always replicated.
- **Resumable bootstrap** — if `bootstrap` is interrupted mid-copy, it restarts from scratch for the affected databases. Progress is not checkpointed.
- **Observability integrations** — there is no built-in Prometheus exporter, Grafana dashboard, or external alerting. Monitoring relies on the `replicator status` CLI command.
- **Automated cutover** — switching application traffic to the target (DNS change, connection pooler reconfiguration, etc.) must be done manually. pg_emigrant stops at `replicator stop` + `sync-sequences`.

### Will never be possible (PostgreSQL architectural constraints)

- **One WAL sender per database** — PostgreSQL logical replication is scoped to a single database. There is no way to multiplex multiple databases over a single WAL sender connection. This is a hard limit of the PostgreSQL replication protocol; `max_wal_senders` on the source must be ≥ the number of concurrently replicated databases.
- **Real-time DDL replication** — PostgreSQL logical replication transmits DML events only (`INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`). DDL statements (`ALTER TABLE`, `CREATE INDEX`, etc.) are never included in the WAL stream. pg_emigrant detects and repairs drift via `detect-ddl`, but changes must be applied explicitly — they cannot be streamed automatically.
- **Unlogged and temporary tables** — PostgreSQL intentionally excludes `UNLOGGED` and `TEMPORARY` tables from logical replication. They will not be replicated.
- **Large Objects (`pg_largeobject`)** — the `lo` subsystem is not replicated via logical replication. Applications relying on server-side large objects must migrate them separately (e.g. with `pg_dump -Fc --section=data`).
- **Tables without PRIMARY KEY or REPLICA IDENTITY** — for tables without a primary key, pg_emigrant automatically sets `REPLICA IDENTITY FULL` on **both** source and target during bootstrap. This allows `UPDATE` and `DELETE` to be replicated correctly by including all column values in the WAL record. Such tables are highlighted in yellow in the bootstrap output. Note: `REPLICA IDENTITY FULL` generates more WAL than a PK-based identity; adding a PK to the table remains the preferred solution where possible.
- **Sequence values in real time** — the WAL stream does not contain sequence advances. pg_emigrant works around this by polling `last_value` on the source and calling `setval()` on the target periodically (configurable via `sequence_sync_interval`), but there is always a small window where target sequences may lag behind the source. This is unavoidable with native logical replication.

---

## Security

- Database passwords are stored exclusively in the local `config.yaml` file — ensure appropriate file permissions (`chmod 600 config.yaml`).
- The DSN containing the password is built in process memory only and is never logged.
- The database user should have **the minimum necessary privileges** (see the Requirements section).

---

## License

MIT License — see the `LICENSE` file for details.
