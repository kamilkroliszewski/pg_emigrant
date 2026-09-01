# pg_emigrant — production readiness

What this tool is safe to be used for, what it is not, and — for every claim
below — the executable evidence behind it.

The organising question is not "does the migration work?" but:

> **If pg_emigrant migrates a real production PostgreSQL cluster, what are the
> realistic ways it could silently lose, duplicate, corrupt or misrepresent
> data — and has each of those paths been eliminated, or made to refuse?**

Nothing here is marked PASS on the strength of a code review. PASS means a
test in this repository exercises the behaviour against real PostgreSQL
servers and fails if the behaviour regresses.

---

## Verdict

**PRODUCTION READY WITH EXPLICIT LIMITATIONS** — for *controlled, attended*
migrations, of the shape described under [Supported use cases](#supported-use-cases),
by an operator who runs `cutover-check` and treats its answer as binding.

The limitations are not footnotes. Read
[Explicitly unsupported](#explicitly-unsupported-scenarios) and
[Remaining risks](#remaining-risks) before planning a migration; several of
them will decide whether this tool fits your database at all.

**One caveat on the verdict itself, which belongs here rather than buried.**
This pass found **six P0 defects** (plus three P1s), four of the P0s capable of
producing a target that was silently missing data while every check reported
success — in code
that had already been through several hardening passes. Every one is now fixed
and pinned by a regression test against real PostgreSQL. But they clustered:
all four of the silent ones lived in *post-bootstrap table reconciliation*, an
area that had **no test at all** before this pass, and three of them were only
reachable with a pre-15 source. The lesson generalises. Where this document
says NOT TESTED, read it as "unknown", not "probably fine" — the areas with no
coverage are exactly where the defects were. Rehearse on a clone
([procedure below](#recommended-production-rehearsal)) before trusting a real
migration, and compare the data yourself rather than only asking the tool.

---

## Supported use cases

* A **one-off, low-downtime migration** of one or many databases from one
  PostgreSQL cluster to another, with an operator watching.
* **Source PostgreSQL 14–18 → target PostgreSQL 17 or 18**, same-version or
  older→newer. Be precise about what that rests on: CI runs the pairs
  `14→18`, `15→18`, `16→18`, `17→18`, `17→17`, `18→18`. **A target older than
  17 is exercised by nothing**, and a downgrade (newer source → older target)
  is neither tested nor supported.
* A **freshly provisioned target**, possibly already carrying its roles,
  databases and schemas from configuration management. Pre-existing *data*
  outside the migration's scope survives; pre-existing data *inside* it is
  cleared and reloaded.
* **Patroni-managed sources**, with the caveats in
  [Patroni considerations](#patroni-considerations).
* A migration window measured in hours or days, with the application writing
  to the source throughout.

## Explicitly unsupported scenarios

Each of these produces a target that differs from the source. Where
pg_emigrant can detect it, it refuses or reports; where it cannot, it is
listed here because that is the only mitigation there is.

| Scenario | What happens | Detected? |
|---|---|---|
| **`UNLOGGED` tables** | Initial copy lands; later writes produce no WAL and never replicate. Target silently diverges. | `preflight` **warns**. Not blocked, not re-checked at cutover. |
| **Large objects (`pg_largeobject`)** | Never migrated. | No. Migrate separately. |
| **Excluded tables (`exclude_tables`)** | Not created, not copied, not published, not replicated. That is the point of the setting. | By definition. An FK *into* an excluded table is now **refused** before anything is cleared. |
| **Tablespaces** | Ignored; everything lands in the target's default tablespace. | No. |
| **`COMMENT ON`, event triggers, FDWs/foreign tables, `CREATE RULE`, `CREATE STATISTICS`** | Not migrated. | No. |
| **Ordered-set / hypothetical-set aggregates** (`WITHIN GROUP`) | Not reproduced. | Reported as a warning during schema sync. |
| **Range type `SUBTYPE_OPCLASS`, aggregate `SORTOP`** | Defaults used instead. | No. |
| **Row-level filtering** | No per-table `WHERE`; whole tables only. | N/A |
| **Resumable bootstrap** | An interrupted bootstrap re-copies that database from scratch. Safe, not cheap. | N/A |
| **Automated cutover** | Moving application traffic is manual, deliberately. | N/A |
| **Multi-writer / bidirectional replication** | Not attempted. The target must take no writes before cutover. | **Partly.** A target write that *collides* with a replicated change stalls the apply worker, which health reports as lag and then CRITICAL. A non-colliding one (an INSERT on an unused key) is invisible to every check. |

---

## Guarantee matrix

Statuses: **PASS** (executable evidence, real PostgreSQL) · **PARTIAL**
(covered in part; the gap is stated) · **NOT TESTED** · **N/A**.

| Guarantee | Status | Test / evidence | Notes |
|---|---|---|---|
| No silent data loss during initial COPY | PASS | `test_bootstrap_consistency.py`, `test_copy_safety.py::test_copy_count_verification_runs_under_the_copy_snapshot` | Source counted under the *same* snapshot the copy used; a mismatch aborts the database. |
| No silent data loss during concurrent DML | PASS | `test_concurrent_dml.py`, `test_stress_and_soak.py::test_a_large_table_converges_while_it_is_being_written_to` | Slot created before the copy; the copy uses the slot's own exported snapshot, so copy and stream start at one point. |
| No duplicate rows from partitioned tables | PASS | `test_bootstrap_consistency.py::test_partitioned_parent_rows_are_not_duplicated` | Parents (`relkind='p'`) excluded from the copy; children copied individually; publication excludes `relispartition` members. |
| No duplicate rows from inheritance parents | PASS | Copy uses `SELECT … FROM ONLY`; covered by the consistency checksums | Inheritance *links* are not recreated — warned about during schema sync. |
| Sequence convergence | PASS | `test_sequences.py` (8 tests), `test_post_bootstrap_ddl.py::test_sync_sequences_advances_sequences_on_every_supported_source` | Includes cached sequences, identity, serial, `--margin`, and target-ahead. |
| SIGTERM / SIGINT safety | PASS | `test_idempotency_and_interrupts.py::test_signalled_bootstrap_leaves_no_orphaned_slot`, `…_at_each_later_phase_…` | The per-phase test now **proves** the interrupt landed in the named phase (marker file), instead of inferring it from a sleep. |
| SIGKILL recovery | PASS | `test_idempotency_and_interrupts.py::test_sigkilled_bootstrap_is_recoverable_by_rerunning`, `test_wal_retention.py::test_an_orphaned_slot_whose_wal_is_gone_still_recovers_by_recopy` | SIGKILL leaves an orphan by definition; the next run adopts it — even after its WAL has been invalidated. |
| Replication slot safety (no takeover) | PASS | `test_slot_safety.py` (9 tests) | An **active** slot is never reclaimed implicitly; a slot belonging to another database never is at all. |
| WAL-loss detection | PASS | `test_wal_retention.py::test_wal_loss_is_reported_as_broken_not_as_lag` and the three tests after it | Driven by a real `max_slot_wal_keep_size=1MB` source until PostgreSQL actually invalidates the slot — not a simulated `wal_status`. |
| Recovery refuses when WAL is gone | PASS | `test_wal_retention.py::test_wal_loss_makes_recovery_refuse_and_change_nothing`, `test_slot_safety.py::test_lost_slot_makes_recovery_refuse_and_change_nothing` | Refuses **before** dropping anything, so the refused state is no worse than the broken one. |
| Orphan slot recovery | PASS | `test_idempotency_and_interrupts.py::test_bootstrap_after_a_crash_cleans_up_the_orphaned_slot` | An *inactive* orphan is adopted; an active one is not. |
| Failure cleanup at every phase | PASS | `test_failure_injection.py` — every injectable phase × (no slot, no publication, no subscription, re-run converges) | |
| Concurrent runs do not corrupt each other | PASS | `test_idempotency_and_interrupts.py::test_a_second_concurrent_bootstrap_is_refused_and_steals_nothing` | A source-side session advisory lock held for the whole per-database run; the second run refuses (exit 7) and touches nothing. |
| Idempotent rerun | PASS | `test_idempotency_and_interrupts.py`, `test_failure_injection.py::test_rerun_after_failure_succeeds_and_converges` | A re-run over *live* replication is refused, not silently destructive. |
| Existing target protection | PASS | `test_target_preexisting_state.py` (5 tests) | Out-of-scope data survives; a cascading truncate is refused; a column-type mismatch is refused before anything is cleared. |
| Same-cluster protection | PASS | `test_same_cluster_guard.py` (4 tests) | `system_identifier`, not host:port. Not skippable, enforced on every mutating path. |
| Excluded table safety | PASS | `test_exclude_tables.py` (7 tests) | An FK from a migrated table into an excluded one is now **refused on the bootstrap path**, exit 7, before the target is cleared. |
| Schema drift detection | PASS | `test_cross_version_views.py`, `test_post_bootstrap_ddl.py`, `test_health_and_cutover.py::test_cutover_check_refuses_on_schema_drift_unless_accepted` | Any drift blocks a cutover unless `--accept-drift`. |
| Tables created after bootstrap are replicated | PASS | `test_post_bootstrap_ddl.py` (6 tests) | **Previously untested, and broken four different ways** — see [Bugs found](#bugs-found-and-fixed). |
| Per-table replication state (stuck tablesync) | PASS | `test_table_sync_health.py` (4 tests), `test_health_classification.py` (14 unit tests) | **Previously the tool's headline false-positive** — see below. |
| A table nothing replicates is detected | PASS | `test_table_sync_health.py::test_a_table_no_subscription_knows_about_is_not_healthy` | A table present on both servers but in no publication is in an error state on neither — the drift scan sees it on both sides and says nothing. Health now compares the in-scope source tables against what the subscription tracks. |
| Partitions under live replication | PASS | `test_partition_replication.py` (2 tests) | Routed inserts, direct-leaf inserts, updates, deletes, a row moved **across** the partition boundary (decoded as DELETE + INSERT on two relations), and a partition added after bootstrap. Leaf-level checksums, not just the parent's total. |
| Subscription health | PASS | `test_health_and_cutover.py` (12 tests), `test_health_classification.py` (14 unit tests) | Slot + apply worker + per-table state together; no single statistic is the source of truth. |
| WAL lag detection | PASS | `test_health_and_cutover.py::test_lag_thresholds_move_the_state`, `test_wal_retention.py` | Measured from the slot's `confirmed_flush_lsn` against `pg_current_wal_insert_lsn()`. |
| Cutover safety | PASS | `test_health_and_cutover.py`, `test_wal_retention.py::test_cutover_is_refused_after_real_wal_loss` | 8 independent checks, every one defaulting to "not ready". |
| Source is barely touched | PASS | `test_source_mutation.py` (3 tests) | The whole source photographed before/after; only replica identity, one publication, one slot may differ. |
| GUI reports the same state as the CLI | PASS | Verified against a real stuck tablesync via `web.services.collect_all_status` | The dashboard pill now shows the computed replication state, not a separate heuristic that read green over a missing table. |
| Credentials never printed | PASS | `test_security.py` (4 tests) | Including the diagnostic that deliberately quotes the connection string. |
| PostgreSQL compatibility, source 14–18 → target 17/18 | PASS | Whole suite, `--pg-matrix "14->18,15->18,16->18,17->18,17->17,18->18"` in CI, one job per pair | Four of the bugs below were reachable **only** with a pre-15 source, and none of the affected code had any test. Older sources are where the version assumptions hide. |
| PostgreSQL target older than 17 | NOT TESTED | — | No pair in the matrix targets 14, 15 or 16. `pg_stat_subscription_stats` (PG15+) is already version-gated, but nothing else about a PG14 target is exercised. |
| Downgrade (newer source → older target) | NOT TESTED | — | Not in the matrix, not supported. |
| Long **read** transaction during migration | PASS | `test_concurrent_dml.py::test_long_running_read_transaction_does_not_break_bootstrap` | |
| Long **write** transaction during migration | PASS | `test_concurrent_dml.py::test_long_running_write_transaction_blocks_slot_creation_and_fails_closed` | PostgreSQL semantics: slot creation waits on every active XID cluster-wide. pg_emigrant names the blocking pid and times out rather than hanging forever. |
| Large-table behaviour | PARTIAL | `test_stress_and_soak.py` (2 tests), default `PG_EMIGRANT_STRESS_ROWS=60000`; run here at **2,000,000 rows / 46,512 pages, copied in 5.7 s** across 4 `ctid` slices, checksum-identical, and again under a concurrent write workload | The slicing and streaming paths are genuinely exercised at a size where a page-range off-by-one would show. A *multi-hundred-GB* migration has still not been run. |
| Long-running soak | PARTIAL | `test_stress_and_soak.py::test_soak_a_live_migration_then_decide_to_cut_over`, default `PG_EMIGRANT_SOAK_SECONDS=15`; observed 7,670 inserts / 7,670 updates / 7,669 deletes converging exactly, then `cutover-check` approving | The shape is asserted on every run. Multi-hour soaks are opt-in and have not been run. |
| Physical failover / promotion semantics | PASS | `test_failover.py` (4 tests) | A real `pg_basebackup` standby, really promoted. |
| **Patroni integration** | **NOT TESTED** | — | See [Patroni considerations](#patroni-considerations). The failover tests cover PostgreSQL promotion semantics, **not** Patroni, its DCS, or its leader election. |
| Target receiving writes before cutover | **NOT TESTED** | — | Only *colliding* writes are detected, indirectly (they stall the apply worker). See [Remaining risks](#remaining-risks). |
| `UNLOGGED` table divergence | **NOT TESTED** | `preflight` warns | The warning is not re-asserted at cutover. |

---

## Safety guarantees, and what each one rests on

**The rule: if pg_emigrant cannot prove source and target are consistent, it
fails closed.**

1. **It never reports success on an incomplete migration.** Four terminal
   outcomes (`success` / `incomplete` / `failed` / `refused`), the run reports
   the worst, and none is downgraded to a warning on a zero exit.
2. **It never migrates a cluster into itself.** Compared by
   `system_identifier`, which host and port cannot establish. Not skippable.
3. **It never destroys target data it does not own.** A populated
   out-of-scope table referencing an in-scope one makes the truncate refuse.
4. **It never silently drops or retypes a column.**
5. **It never takes over another migration's replication objects** — nor
   another *live run of its own*, which server state alone cannot distinguish
   from a killed run's debris (see the migration lock).
6. **It never calls a database healthy while one of its tables is not being
   replicated** — whether the table's initial sync is stuck, or the table is in
   no publication at all. Both look exactly like health to a lag figure.
7. **It never claims replication was repaired when it was not.**
   `reinit-sync` decides *before* dropping anything.
8. **It never drops a slot to relieve WAL retention.** That trades a disk
   problem for permanent data loss; it is the operator's call.
9. **It never calls a database ready to cut over on missing evidence.** Every
   `cutover-check` check defaults to not-ready.

---

## Failure behaviour

| Exit | Meaning | Correct response |
|---|---|---|
| `0` | Everything asked for completed, fully. | Proceed. |
| `1` | Generic failure. | Read the message. |
| `2` | Configuration error. Nothing attempted. | Fix `config.yaml`. |
| `3` | Preflight failed. Nothing modified. | Fix the reported conditions. |
| `4` | Migration failed or finished incomplete; or a `detect-ddl --apply` fix failed; or `sync-sequences` could not bring a new table into replication. | Fix the cause, re-run. |
| `5` | Replication unhealthy — also `cutover-check`'s "do not cut over". | Do not cut over. Diagnose. |
| `6` | Recovery impossible; `reinit-sync` refused. | **Do not retry.** Re-copy: `teardown` + `bootstrap`. |
| `7` | Refused up front as unsafe. | **Re-running unchanged will refuse again.** Change the configuration or the state. |

Exit **7** is now reachable per database, not only for the whole run: an
unsatisfiable `exclude_tables`, a live subscription already in place, a
column-type mismatch, an unsafe cascading truncate, a slot already in use, and
another `pg_emigrant` run already migrating that database all report `refused`
rather than `failed`. A runbook that retries on 4 would
previously have retried these forever.

---

## WAL requirements

* `wal_level = logical` on the source.
* `max_replication_slots` and `max_wal_senders` ≥ the number of databases
  replicated at once, on the source.
* `max_logical_replication_workers` / `max_worker_processes` on the **target**
  ≥ databases + concurrent tablesync workers. Exhausting these is one of the
  ways a tablesync gets stuck — see below.
* **`max_slot_wal_keep_size` is the setting that decides whether an
  interruption is recoverable.** While a slot is not being consumed it holds
  WAL on the production source. Past this limit PostgreSQL discards it, the
  slot is invalidated (`wal_status = 'lost'`), and everything committed since
  the slot's last confirmed LSN can never reach the target. `status --health`
  reports retention explicitly; `cutover-check` blocks at 80% of the limit.
  **Never drop the slot to reclaim disk** — that *is* the data loss.

## Replication slot lifecycle

```
bootstrap
  ├─ migration lock taken on the source (session advisory lock, whole run)
  ├─ publication created on source
  ├─ slot created on source, exporting a snapshot at its own start LSN
  ├─ initial COPY reads THAT snapshot          ← no gap, no overlap
  ├─ … deferred indexes, FKs, views, triggers, ownership, sequences …
  └─ subscription created on target, attached to that slot (create_slot=false)

steady state
  └─ slot retains WAL on the SOURCE until the target confirms it

teardown
  └─ subscription detached (slot_name = NONE), dropped, then slot dropped
```

* An **interrupted** run (SIGINT/SIGTERM) rolls its own slot and publication
  back, within a bounded 60 seconds.
* A **SIGKILLed** run leaves the slot behind. The next `bootstrap` adopts it
  if it is inactive; `teardown` removes it explicitly.
* An **active** slot is never reclaimed implicitly, at any point.
* A slot belonging to a **run that is still alive** is never reclaimed either,
  even while it is momentarily inactive — that is what the migration lock adds,
  and it is the difference between a live run's slot and a killed run's debris,
  which server state alone cannot tell apart.
* An `incomplete` outcome deliberately **leaves the subscription running** —
  the data is intact and tearing it down would force a needless re-copy.

## Interrupt / restart behaviour

| State when interrupted | Re-running `bootstrap` |
|---|---|
| Before validation | Runs normally. |
| After the same-cluster check | Runs normally. |
| After the publication exists | Reuses it; does not roll back a publication it did not create. |
| After the slot exists, inactive | Adopts the orphan, re-copies. |
| After the slot exists, **active** | **Refuses** — something is streaming from it. |
| While another `bootstrap` of the same database is still running | **Refuses** (exit 7), and touches nothing — the migration lock, not server state, is what tells a live run apart from a killed one's debris. |
| During COPY | Re-copies that database from scratch (TRUNCATE + reload). |
| After COPY, during indexes/FKs/sequences | Re-copies from scratch; no per-table checkpointing. |
| After the subscription exists | **Refuses** — `teardown` first. |
| Orphan slot whose WAL was since invalidated | Adopts and replaces it; the re-copy takes a fresh snapshot, so nothing depends on the lost WAL. |

---

## Cutover procedure

`cutover-check` is read-only and takes no action. It runs eight checks per
database, each defaulting to not-ready:

1. `cluster_identity` — source and target are provably distinct clusters.
2. `target_writable` — reachable, and not a standby.
3. `replication_healthy` — the state machine says HEALTHY.
4. `replication_caught_up` — lag ≤ 8 MiB, measured from the slot's
   `confirmed_flush_lsn`.
5. `all_tables_streaming` — **every** published table is at `srsubstate = 'r'`.
6. `sequences_synchronised` — no sequence behind, missing, or unreadable.
7. `no_schema_drift` — unless `--accept-drift`.
8. `wal_retention` — the slot's WAL is not being discarded, and is not within
   20% of `max_slot_wal_keep_size`.

Run it **after** stopping writes to the source: the lag check only means
something once the source has stopped moving.

```bash
# … stop the application writing to the source …
pg_emigrant sync-sequences -c config.yaml --margin 1000
pg_emigrant cutover-check  -c config.yaml            # exit 0 = safe
# … repoint the application at the target …
pg_emigrant teardown       -c config.yaml            # releases WAL retention
```

## Patroni considerations

**Read this before relying on the Patroni claims.**

What the test suite actually proves is *PostgreSQL promotion semantics*: a
real `pg_basebackup` streaming standby, really promoted, against which
`reinit-sync` must refuse because the promoted node genuinely has no logical
slot. That is faithful for the property that matters most — logical slots are
local to the instance that created them and are not carried by physical
replication before PostgreSQL 17's failover slots — and it is **not** a test of
Patroni.

Not covered by any test in this repository:

* Patroni's DCS (etcd/Consul/ZooKeeper) and leader election.
* Patroni's own promotion path, callbacks and `pg_rewind` usage.
* A Patroni VIP or HAProxy leader-only endpoint moving mid-migration.
* PostgreSQL 17+ failover slots (`sync_replication_slots = on`) under Patroni.

What pg_emigrant relies on, and what you must therefore guarantee:

1. **`source.host` must mean the same physical node to pg_emigrant and to the
   target's apply worker, on every connection.** A logical slot lives on one
   instance. A load-balanced or round-robin endpoint can create the slot on
   one node and stream from another — the tool logs which node each
   slot-sensitive connection reached (`_node_fingerprint`) so the evidence
   exists after the fact, but it cannot prevent it.
2. **Never `localhost`/`127.0.0.1`.** `CREATE SUBSCRIPTION`'s `CONNECTION`
   string is stored verbatim and resolved later by the *target's* apply worker
   on the *target* machine. This is a reproduced production failure. The tool
   warns, and verifies streaming from the target's own point of view.
3. **A failover loses the slot on PostgreSQL < 17.** `reinit-sync` then
   refuses (exit 6) and the repair is a re-copy. On 17+, enable
   `sync_replication_slots` so a switchover stops losing the slot at all —
   **untested here**.

**Rehearse a failover on a clone before trusting this path in production.**

## Recommended production rehearsal

Do this against a clone, not production. It takes an afternoon and is the only
way to find out what your data does.

```bash
# 1. Clone the source (pg_basebackup / snapshot restore) and provision a target
#    cluster with the same roles.

# 2. Verify before touching anything. Read every WARN, not just the ERRORs.
pg_emigrant preflight -c config.yaml --strict

# 3. Bootstrap.
pg_emigrant bootstrap -c config.yaml --format json | tee bootstrap.json
#    Exit 0 only if every database came out `success`.

# 4. Start a representative write workload against the source clone.

# 5. Keep the steady-state process running for the whole window.
pg_emigrant sync-sequences -c config.yaml --loop

# 6. Watch it. Watch the retained-WAL figure especially.
watch -n30 'pg_emigrant status -c config.yaml --health'

# 7. Kill pg_emigrant with SIGKILL mid-bootstrap and restart it.
#    Confirm the re-run adopts the orphan slot and converges.

# 8. If the source is Patroni: force a switchover. Then
pg_emigrant reinit-sync -c config.yaml
#    Expect exit 6 (refused) on PostgreSQL < 17 without failover slots.
#    That refusal is correct. The repair is teardown + bootstrap.

# 9. Let the target fall behind on purpose (stop the subscription, keep
#    writing) and watch retained WAL grow. Confirm it never self-heals by
#    dropping the slot.
pg_emigrant stop -c config.yaml

# 10. Stop writes. Final sequence sync with a margin.
pg_emigrant sync-sequences -c config.yaml --margin 1000

# 11. Ask, and believe the answer.
pg_emigrant cutover-check -c config.yaml --format json

# 12. Compare the data yourself. Do not skip this on the rehearsal:
#     row counts and checksums per table, on both sides.

# 13. Validate sequences: on the target, INSERT one row into each hot table
#     and confirm no primary-key collision.

# 14. Perform the cutover, then:
pg_emigrant teardown -c config.yaml
```

---

## Bugs found and fixed

Each was reproduced first, has a regression test, and the test fails against
the previous implementation.

### 1. Two concurrent `bootstrap` runs silently destroyed each other's work (P0)

**Impact.** Two `pg_emigrant bootstrap` processes started against the same
configuration. The second **exited 0** while dropping the first run's
replication slot and creating a fresh one at a later LSN, and TRUNCATEd the
target underneath it. The first run then attached its subscription to a slot
that starts *after* the snapshot its own copy used — so every transaction
committed in between is in neither the copy nor the WAL stream — and reported
success. Reproduced directly: the slot's `restart_lsn` moved while run #1 was
mid-flight, and run #2 exited 0.

**Root cause.** The dangerous window is the entire post-copy half of the
pipeline — deferred indexes, foreign keys, views, triggers, ownership,
privileges, sequences — which is minutes on a real database. Throughout it the
first run's slot exists but is **inactive**: its snapshot connection has been
released and its subscription does not exist yet. A second run then sees no
subscription (so the already-replicating guard does not fire) and an inactive
slot (so the never-steal-a-live-slot guard does not fire either), and treats
the slot as the orphan a killed run leaves behind — which, from server state
alone, it is genuinely indistinguishable from.

Neither existing guard could close this, because both ask about *server state*,
and the missing fact is whether a live process still owns it.

**Fix.** `guards.migration_lock` — a session-scoped PostgreSQL advisory lock
taken on the source, in that database, keyed by a stable hash of the slot name,
held for the whole per-database run. A second run refuses with
`ConcurrentMigration` → outcome `refused`, exit 7, naming the holding backend.
It deliberately performs **no rollback**: the slot it would find belongs to the
other run. Session-scoped is the point — the lock dies with the process, so a
SIGKILLed run's debris stays adoptable instead of being locked out forever.

**Regression test.** `test_idempotency_and_interrupts.py::test_a_second_concurrent_bootstrap_is_refused_and_steals_nothing`
— which also asserts that a subsequent run succeeds after the first process is
signalled, so the guard cannot become a permanent outage.

### 2. `detect-ddl --apply` created an empty target table and reported success (P0, PostgreSQL 13/14 sources)

**Impact.** On a pre-15 source, `detect-ddl --apply` "fixed" a table created
after bootstrap by creating it on the target — and never adding it to the
publication. It reported `applied: 1, failures: []`. The table was then
**permanently empty and completely invisible**: the next drift scan saw it on
both sides and reported *"No drift detected"*, `status --health` reported
*HEALTHY*, and `cutover-check` reported *SAFE TO CUT OVER*. Reproduced against
a PostgreSQL 14 source.

**Root cause.** `apply_drift_fixes` finished by calling
`ALTER SUBSCRIPTION … REFRESH PUBLICATION`. On PostgreSQL 15+ that is enough,
because `FOR TABLES IN SCHEMA` auto-publishes the new table. On 13/14 the
publication is a frozen `FOR TABLE` list, so the refresh finds nothing new and
the table is published nowhere.

**Fix.** `apply_drift_fixes` now calls `replication.sync_new_tables`, the one
place that gets publication membership right on every version and refreshes
with `copy_data = true` itself.

**Also fixed generally.** A table that exists on both servers but is in no
publication is in an error state on neither of them, which is why nothing
reported it. `replication_health` now compares the migration's in-scope source
tables (same scope resolution as every other stage, rolled up to partition
roots) against what the subscription actually tracks, and reports
`tables_not_replicated`; `cutover-check` blocks on it outright. So the class is
closed, not only this one path into it.

**Regression tests.** `test_table_sync_health.py::test_a_table_no_subscription_knows_about_is_not_healthy`
(which asserts its own premise — that drift detection stays silent — so it
cannot quietly stop testing the invisible case),
`test_post_bootstrap_ddl.py::test_detect_ddl_apply_can_create_a_table_with_a_serial_column`,
and two unit tests in `test_health_classification.py`.

### 3. `sync_new_tables` raised on every run against a partitioned pre-15 source (P0)

**Impact.** On a PostgreSQL 13/14 source containing **any** partitioned table,
every call raised `DuplicateObjectError: relation "…" is already member of
publication`. Combined with bug 5, that took the whole `sync-sequences` command
down: no table created after bootstrap was ever picked up, and no sequence was
ever advanced.

**Root cause.** `pg_publication_tables` is the *expanded* view — a published
partitioned parent appears there as its leaf partitions, never as itself —
while `_publishable_tables()` returns parents and never their children.
Differencing one against the other left every partitioned parent looking
unpublished forever, so each pass re-issued `ALTER PUBLICATION … ADD TABLE` for
it.

**Fix.** The two questions now use the two different catalogs they need:
`pg_publication_rel` (membership) for "what still needs adding",
`pg_publication_tables` (expanded) for "what does the subscriber track".

**Regression tests.** `test_partition_replication.py` (2 tests), which run
against every source version in the matrix.

### 4. A stuck table sync reported HEALTHY and SAFE TO CUT OVER (P0)

**Impact.** A published table whose initial sync could never complete had
**none of its rows on the target**, permanently — while `status --health`
reported HEALTHY and `cutover-check` reported SAFE TO CUT OVER. Reproduced
directly: 50 source rows, 0 target rows, `sync_error_count` rising, lag 0 B,
verdict "SAFE TO CUT OVER".

**Root cause.** Health was computed from the replication slot and the *main
apply worker* only. A subscription is one apply worker plus one state machine
per table (`pg_subscription_rel.srsubstate`), and they fail independently: a
table stuck at `d` is skipped by the apply worker entirely, so the slot,
`confirmed_flush_lsn` and the lag figure all stay genuinely perfect. This is
reachable in normal operation — every table created on the source after
bootstrap goes through a tablesync, and any target-side obstruction (a
stricter constraint, a type mismatch, exhausted
`max_logical_replication_workers`) makes that sync fail forever.

**Fix.** `health.replication_health` now reads `pg_subscription_rel` and
`sync_error_count`; not-ready tables with recorded sync errors are **BROKEN**,
not-ready tables without them are **LAGGING** (a table mid-sync *is* behind).
`cutover-check` gained `all_tables_streaming` as an outright blocker.

**Regression tests.** `test_table_sync_health.py` (3), `test_health_classification.py` (12).

### 5. `sync-sequences` crashed outright against a PostgreSQL 13/14 source (P0)

**Impact.** On a PG14 source, `pg_emigrant sync-sequences` — the documented
steady-state *and* final-cutover command — raised an unhandled
`UndefinedTableError` and synchronised **nothing**. Sequences never advanced;
tables created after bootstrap were never picked up. `cutover-check` would
still have refused on the behind sequences, so it is not silent at the final
gate — but the remedy it names was the command that crashed.

**Root cause.** `replication.sync_new_tables` queried
`pg_publication_namespace` unconditionally. That catalog arrived with
`FOR TABLES IN SCHEMA` in PostgreSQL 15. On older sources the query raised —
on exactly the versions where the publication is a frozen `FOR TABLE` list and
this pass is the only thing that could ever add to it.

**Fix.** The lookup is gated on `major >= 15`. Separately, the one-shot
`sync-sequences` path no longer lets a new-table failure abort the sequence
sync: the two jobs are independent and the sequence sync is the one with a
cutover deadline. It reports the failure and exits 4.

**Regression tests.** `test_post_bootstrap_ddl.py::test_a_table_created_after_bootstrap_is_actually_replicated`
and `::test_sync_sequences_advances_sequences_on_every_supported_source`, both
run against every source version in the CI matrix.

### 6. A table with a `serial` column created after bootstrap could never be created on the target (P0)

**Impact.** `serial`/`bigserial` is the commonest shape a PostgreSQL table
has. `generate_full_table_ddl` emitted
`DEFAULT nextval('app.t_id_seq'::regclass)` without ever creating the
sequence, so both the automatic pickup and `detect-ddl --apply` failed with
`relation "app.t_id_seq" does not exist` and retried forever. The table stayed
missing on the target and its rows were never replicated.

**Root cause.** The generator emitted the table's columns, constraints and
indexes but not the sequences its own defaults depend on.

**Fix.** `schema_sync._generate_owned_sequence_ddl` emits
`CREATE SEQUENCE IF NOT EXISTS` before the table and `ALTER SEQUENCE … OWNED BY`
after it, for `pg_depend.deptype = 'a'` links only — identity columns create
their own sequence and pre-creating it would make PostgreSQL attach a phantom
`…_seq1` instead.

**Regression test.** `test_post_bootstrap_ddl.py::test_detect_ddl_apply_can_create_a_table_with_a_serial_column`.

### 7. A table with a UNIQUE constraint created after bootstrap could never be created on the target (P1)

**Impact.** The same two paths, for a different reason and a very common table
shape.

**Root cause.** `generate_full_table_ddl` emitted both
`ALTER TABLE … ADD CONSTRAINT x UNIQUE (…)` *and* `CREATE UNIQUE INDEX x …`.
The constraint builds the index under that same name, so the second statement
failed with `relation "x" already exists` — and because the whole block is
applied as one statement, the `CREATE TABLE` rolled back with it. The live
schema-sync path never saw this: it creates constraints first and then skips
index names that already exist.

**Fix.** `_generate_index_ddl` skips any index backing a constraint
(`pg_constraint.conindid`), not only the primary key's.

**Regression test.** Same test as #3 (the table carries both a `serial` column
and a `UNIQUE` constraint).

### 8. The documented `refused` outcome was unreachable per database (P1)

**Impact.** `DatabaseResult.refuse()` existed and was never called. Every
per-database refusal — a live subscription already present, a column-type
mismatch, an unsafe cascading truncate, a slot in use — reported `failed`
(exit 4, "fix the cause and re-run") instead of `refused` (exit 7, "re-running
unchanged will refuse again"). A runbook branching on exit 4 retries these
forever.

**Fix.** Those aborts now produce `Outcome.REFUSED` and exit 7.

**Regression tests.** `test_cli_contract.py::test_an_unsafe_bootstrap_exits_refused_not_merely_failed`,
`test_exclude_tables.py::test_the_exclusion_refusal_happens_before_the_target_is_touched`.

### 9. `exclude_tables` FK safety was enforced only by the skippable preflight (P1)

**Impact.** `scope.check_exclusions_are_safe` was called from `preflight` and
nowhere else — and `preflight` is skippable with `--skip-preflight`, is a CLI
step the library entry points never run, and is not run by the web GUI. A
migration whose `exclude_tables` left out a table that an in-scope table
references by foreign key therefore cleared the target, reloaded it, *then*
failed to create the constraint, and reported `incomplete`. `ExcludedTableIsReferenced`
was declared and never raised.

**Fix.** Enforced in `bootstrap`, before any target table is created or
cleared, as a refusal (exit 7).

**Regression tests.** `test_exclude_tables.py::test_the_exclusion_refusal_happens_before_the_target_is_touched`,
`::test_no_replication_object_survives_the_exclusion_refusal`.

### 10. The per-phase interrupt tests could not prove which phase they interrupted (test quality, P0)

**Impact.** No production bug, but five parametrisations that all asserted the
same thing about whichever phase the run happened to be in three seconds after
the slot appeared. A regression in four of the five phases would not have been
caught, while the suite reported five passing tests.

**Fix.** The pause hook writes the phase name atomically to
`PG_EMIGRANT_TEST_PAUSE_MARKER` before blocking; the test waits for that file
to name the phase it asked for, and fails on its own timeout if the run never
arrives.

### 11. An unmeasurable replication lag read as HEALTHY (hardening)

Every other signal can be clean while the one number that says whether the
target is current is absent — a slot with no `confirmed_flush_lsn` has never
had anything confirmed by the subscriber. `cutover-check` already refused on
it; `_classify` did not, so `status --health` could report HEALTHY on a
measurement that was never taken. It is now `LAGGING` with that as the stated
reason. Regression test: `test_health_classification.py::test_an_unmeasurable_lag_is_not_healthy`.

### 12. The web dashboard's health pill was a separate, weaker computation (P1)

**Impact.** The GUI derived its per-database pill from the subscription row,
slot activity, the lag string and per-schema table *counts* — none of which
change when a table's initial sync is stuck or when a table is in no
publication. Measured directly against a stuck sync: table counts 2/2, slot
active, lag "0 bytes" — a green "ok" pill over a table with none of its rows on
the target, at the same moment the CLI reported `BROKEN`.

**Fix.** The dashboard now requests the `health` section and shows the state
the CLI computes, with the reasons as the pill's tooltip. It costs one more
read-only section per database per refresh; a cheap wrong answer refreshed
every fifteen seconds is worse than a correct one.

**Verified** by driving `web.services.collect_all_status` over a real stuck
tablesync and confirming the state it now returns is `broken` while every
signal the old pill used still read clean.

### 13. `standard_conforming_strings` was assumed, not asserted (hardening)

Every SQL literal this tool builds goes through `utils.ql()`, which doubles
single quotes and leaves backslashes alone — correct **only** under
`standard_conforming_strings = on`. That has been the default since PostgreSQL
9.1, but it is a setting a legacy application can pin off per database. It is
now set explicitly on every connection, alongside the existing `search_path`
and timeout neutralisation, so the escaping assumption is true rather than
merely usual. Snapshot identifiers are also passed through `ql()` rather than
interpolated raw.

---

## Remaining risks

Stated honestly, and separated by what is actually known.

### KNOWN LIMITATION — inside the supported contract, mitigate operationally

* **`UNLOGGED` tables silently diverge after the initial copy.** `preflight`
  warns; nothing re-checks at cutover. If you have unlogged tables that matter,
  convert them or accept the divergence knowingly.
* **A tablesync stuck for a reason that records no error reads as LAGGING
  rather than BROKEN.** It is never HEALTHY and `cutover-check` always blocks,
  so nothing unsafe follows from it — but the severity understates a table that
  will never finish syncing. Treat a `LAGGING` that does not clear as a stuck
  sync, not a slow one.
* **A migration window long enough to fill `max_slot_wal_keep_size` while the
  target is not consuming becomes a re-copy.** Detected, refused, never
  silently papered over — but it is a real operational cliff. Size the setting
  for the window, and watch the retention figure.
* **Concurrent DDL on the source is not replicated.** `detect-ddl` finds it and
  `cutover-check` refuses while it is outstanding, but an `ALTER TABLE ADD
  COLUMN` followed by writes will stall the apply worker until the column is
  reconciled. Prefer a DDL freeze during the window.
* **An interrupted bootstrap re-copies that database from scratch.** For a
  multi-hundred-GB database that is hours.
* **`replication_health` is now a heavier read.** It adds two source-side
  catalog queries (schema discovery and the in-scope table set) and two
  target-side ones per call, on top of what it already did. It is still
  strictly read-only and still safe to run against production at any time, but
  a web dashboard auto-refreshing many databases every fifteen seconds does
  proportionally more catalog work than it used to. Raise the refresh interval
  if that matters more to you than a fifteen-second-fresh answer.
* **The migration lock covers `bootstrap` against `bootstrap`, and nothing
  else.** `teardown` deliberately does not take it — it is the escape hatch for
  a wedged run, and a teardown that refused while a bootstrap was stuck would
  be worse than one that works. `reinit-sync` does not take it either: run
  concurrently with a bootstrap it can attach a subscription to that run's
  slot, which is untidy but preserves the slot/snapshot alignment rather than
  breaking it. Run one command against one database at a time.

### NOT YET TESTED — no evidence either way

* **Patroni.** See above. Physical promotion semantics are tested; Patroni is
  not. Rehearse it.
* **PostgreSQL 17+ failover slots** (`sync_replication_slots = on`) — the
  documented mitigation for losing a slot at switchover is untested here.
* **Writes made directly to the target before cutover.** Only the colliding
  ones are detected, and only indirectly: they stall the apply worker, which
  surfaces as growing lag and then CRITICAL. A non-colliding write — an INSERT
  on a key the source has not used — leaves the target quietly divergent and no
  check looks for it. `cutover-check` compares no row counts, because the source
  is still moving while it runs. Keep the target closed to everything but the
  apply worker.
* **A PostgreSQL 14, 15 or 16 target.** Every pair in the matrix targets 17 or
  18. The one place the code branches on target version
  (`pg_stat_subscription_stats`, PG15+) is gated, but that gating is reasoned,
  not tested — and on a PG14 target `sync_error_count` does not exist, so a
  permanently stuck table sync reads as LAGGING rather than BROKEN. It still
  blocks the cutover.
* **Databases at real production scale** — hundreds of GB, tens of thousands of
  tables, or a source under heavy sustained write load. The largest run
  performed here is 2,000,000 rows / 46,512 pages (checksum-identical, and
  again under a concurrent write workload) and a 15-second soak; the knobs go
  further (`PG_EMIGRANT_STRESS_ROWS`, `PG_EMIGRANT_SOAK_SECONDS`,
  `PG_EMIGRANT_TEST_TMPFS_SIZE`) but larger runs have not been performed. The
  count of *tables* is the dimension with no evidence at all: nothing here has
  more than a couple of dozen.
* **Two-phase commit (prepared transactions) on the source during the window.**
  `preflight`/slot creation warns about them because they block slot creation,
  but their interaction with logical decoding over a long window is untested.
* **A source or target with a non-UTF8 encoding, or an ICU collation mismatch
  the target's OS cannot satisfy.** The fallback path exists and warns loudly;
  it is not covered by a test.
* **Very wide rows / heavy TOAST under concurrent update.** TOAST round-trips
  are covered in `test_copy_safety.py`; sustained TOAST churn during
  replication is not.

### PROVEN SAFE — evidence in the matrix above

Everything marked PASS. In particular the paths most likely to lose data
silently — the snapshot/stream boundary, the per-table replication state, WAL
invalidation, slot takeover, cascading truncate, and column mismatch — all
have real-PostgreSQL tests that fail if the behaviour regresses.

---

## Test matrix and exact commands

```bash
pip install -e ".[web,test]"

# Unit — pure logic, no database, <1s
pytest tests/unit -q

# Integration — real PostgreSQL in Docker, default 18->18 pair
pytest tests/integration -q

# Everything
pytest -q

# The full supported version matrix (what CI runs, one job per pair)
pytest tests/integration -q --pg-matrix "14->18,15->18,16->18,17->18,17->17,18->18"
pytest tests/integration -q --pg-matrix full      # the same set

# Skip reasons made visible (§ "no silently skipped tests")
pytest tests/integration -q -rs

# Stress: a genuinely large initial copy, ctid slicing under load.
# Raise the throw-away containers' tmpfs datadir alongside the row count — one
# that runs out of space surfaces as a bare "connection was closed in the
# middle of operation", which looks like a pg_emigrant bug and is not one.
PG_EMIGRANT_STRESS_ROWS=5000000 PG_EMIGRANT_TEST_TMPFS_SIZE=8g \
  pytest tests/integration/test_stress_and_soak.py -q -s

# Soak: a live migration under continuous write load, then a cutover decision
PG_EMIGRANT_SOAK_SECONDS=3600 pytest \
  tests/integration/test_stress_and_soak.py::test_soak_a_live_migration_then_decide_to_cut_over -q -s

# The P0 regressions specifically
pytest tests/integration/test_table_sync_health.py \
       tests/integration/test_wal_retention.py \
       tests/integration/test_post_bootstrap_ddl.py \
       tests/integration/test_partition_replication.py \
       tests/integration/test_idempotency_and_interrupts.py -q

# Several of them are version-specific — four were only reachable with a
# pre-15 source, and none of them had any test at all before. Run the P0 set
# against the oldest supported source too:
pytest tests/integration/test_post_bootstrap_ddl.py \
       tests/integration/test_partition_replication.py \
       tests/integration/test_table_sync_health.py -q --pg-matrix "14->18"

# Patroni: no automated suite exists. See "Patroni considerations".

# Lint (the CI correctness gate) and full lint
ruff check pg_emigrant tests --select F,E9
ruff check pg_emigrant tests

# Leave no containers behind after an interrupted run
docker rm -f $(docker ps -aq --filter label=pg_emigrant_test=1)
```

There is no `mypy` configuration in this repository, so no type-check step is
claimed here.

## Benchmark methodology

The stress test is the benchmark; it prints, rather than asserts, its figures —
a timing threshold would only be a flaky test on a busy machine.

```bash
PG_EMIGRANT_STRESS_ROWS=5000000 PG_EMIGRANT_TEST_TMPFS_SIZE=8g pytest \
  tests/integration/test_stress_and_soak.py::test_a_large_table_copies_completely_and_in_parallel_slices \
  -q -s
```

It reports rows, physical pages and elapsed seconds at the configured
`table_parallel_workers`. To characterise a real migration, run it at your own
row count and row width, then watch, on the source, `pg_replication_slots`
retention and, on the target, `pg_stat_subscription` — those two are what
determine whether a window is survivable, not throughput.
