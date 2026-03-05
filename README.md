# pg_emigrant

**PostgreSQL migration & replication orchestrator** — combines the reliability of native PostgreSQL logical replication with the automatic sequence synchronisation found in Bucardo.

---

## What is pg_emigrant?

Native PostgreSQL logical replication is efficient and stable, but it has one well-known limitation: **it does not replicate sequence value changes**. This means that after cutting traffic over to the target server, `SERIAL` / `BIGSERIAL` columns may generate primary keys that conflict with existing data.

Bucardo addresses this problem but is complex to configure and does not use PostgreSQL's native replication mechanisms.

**pg_emigrant** combines both approaches:

| Feature | Native replication | Bucardo | pg_emigrant |
|---|---|---|---|
| WAL (logical) replication | ✅ | ❌ | ✅ |
| Automatic sequence synchronisation | ❌ | ✅ | ✅ |
| Schema drift detection (DDL drift) | ❌ | ❌ | ✅ |
| Parallel initial copy (COPY protocol) | ❌ | ❌ | ✅ |
| Auto-discovery of databases | ❌ | ❌ | ✅ |
| Pydantic config + YAML | ❌ | ❌ | ✅ |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         pg_emigrant CLI                         │
│  bootstrap │ start │ stop │ teardown │ status │ sync-sequences  │
│                         detect-ddl                              │
└──────────────────────────┬──────────────────────────────────────┘
                           │
         ┌─────────────────▼──────────────────┐
         │           bootstrap.py             │
         │  1. discover databases             │
         │  2. create DBs on target           │
         │  3. sync schemas                   │
         │  4. parallel COPY (snapshot safe)  │
         │  5. create publication             │
         │  6. create subscription            │
         └─────────────────┬──────────────────┘
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
    │  2. COPY TO (CSV format, parallel workers) ─────────►│
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

# Schemas to replicate
schemas:
  - public

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
| `schemas` | `list[str]` | `["public"]` | Schemas to replicate |
| `databases` | `list[str]` | `[]` | Databases to migrate; empty = auto-discover |
| `exclude_databases` | `list[str]` | `[template0,template1,postgres]` | Databases skipped during discovery |
| `exclude_tables` | `list[str]` | `[]` | Tables to skip |
| `publication_name` | `str` | `pg_emigrant_pub` | Publication name prefix; `_{dbname}` is appended per database |
| `subscription_name` | `str` | `pg_emigrant_sub` | Subscription name prefix; the replication slot gets the same name as the subscription |
| `parallel_workers` | `int` | `4` | Parallel workers for COPY |
| `sequence_sync_interval` | `int` | `10` | Sequence synchronisation interval [s] |

---

## CLI Commands

### `replicator bootstrap`

Full automated initial migration:

1. Auto-discovery of databases on source
2. Create databases on target (if they do not exist)
3. Schema synchronisation (tables, columns, indexes, constraints, sequences); `SERIAL`/`BIGSERIAL` columns are converted to `GENERATED BY DEFAULT AS IDENTITY`
4. Parallel initial COPY (snapshot-consistent, CSV format for cross-version PG compatibility)
5. Create publication on source
6. Create subscription on target (without re-copying data)

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

Displays a rich status dashboard for all databases:

- **Subscription status** — worker PID, Received LSN, Last Message timestamps
- **Replication slots (source)** — slot state, WAL lag, Restart LSN
- **Replication lag** — WAL delay between source and target
- **Table counts** — comparison of table counts on source vs target
- **Sequence sync status** — current sequence values on source and target
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
┌──────────────────────────────────────────────────────────┐
│                 Sequence Sync — myapp                    │
├────────┬────────────────┬────────┬────────┬─────────────┤
│ Schema │ Sequence       │ Source │ Target │ Status      │
├────────┼────────────────┼────────┼────────┼─────────────┤
│ public │ users_id_seq   │ 10053  │ 9821   │ updated     │
│ public │ orders_id_seq  │ 45210  │ 45210  │ ok          │
│ public │ items_id_seq   │ 1200   │ 1350   │ target_ahead│
└────────┴────────────────┴────────┴────────┴─────────────┘
```

| Status | Meaning |
|---|---|
| `ok` | Values are equal |
| `updated` | Target updated to the source value |
| `target_ahead` | Target is ahead of source — no change made (safe) |

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
```

Objects checked:
- **Tables** — missing on target or on source
- **Columns** — missing, extra, type mismatches
- **Indexes** — missing on target
- **Constraints** — missing on target

Sample report:

```
━━━━━━━━━━━━━━━━━━━━━  Drift Report — myapp  ━━━━━━━━━━━━━━━━━━━━━
┌────────────┬────────┬───────────┬─────────────────┬───────────────────┬──────────────────────────┬──────────────────────────────────────┐
│ Type       │ Schema │ Table     │ Name            │ Drift             │ Detail                   │ Fix DDL                              │
├────────────┼────────┼───────────┼─────────────────┼───────────────────┼──────────────────────────┼──────────────────────────────────────┤
│ column     │ public │ users     │ phone_number    │ missing_on_target │ Column phone_number (...)│ ALTER TABLE "public"."users" ADD ...  │
│ index      │ public │ orders    │ idx_orders_date │ missing_on_target │ CREATE INDEX ...         │ CREATE INDEX idx_orders_date ON ...  │
└────────────┴────────┴───────────┴─────────────────┴───────────────────┴──────────────────────────┴──────────────────────────────────────┘
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

## Project Structure

```
pg_emigrant/
├── pyproject.toml              # package metadata and dependencies
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
│   ├── monitor.py              # replication status dashboard
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

## Security

- Database passwords are stored exclusively in the local `config.yaml` file — ensure appropriate file permissions (`chmod 600 config.yaml`).
- The DSN containing the password is built in process memory only and is never logged.
- The database user should have **the minimum necessary privileges** (see the Requirements section).

---

## License

MIT License — see the `LICENSE` file for details.
