"""Schema drift detection between source and target databases.

Compares tables, columns, indexes, and constraints, and reports
differences.  Optionally generates and applies corrective DDL.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import asyncpg

from replicator.config import ReplicatorConfig
from replicator.db import connect, discover_schemas
from replicator.schema_sync import (
    generate_full_table_ddl,
    get_columns,
    get_constraints,
    get_enum_types,
    get_functions,
    get_indexes,
    get_object_owners,
    get_tables,
    get_triggers,
)
from replicator.utils import get_logger, qi, qt

log = get_logger(__name__)


@dataclass
class DriftItem:
    """A single schema difference."""

    object_type: str  # table, column, index, constraint
    schema: str
    table: str
    name: str
    drift_type: str  # missing_on_target, missing_on_source, different
    detail: str = ""
    fix_ddl: str = ""


@dataclass
class DriftReport:
    """Aggregated drift report for a database."""

    database: str
    items: list[DriftItem] = field(default_factory=list)

    @property
    def has_drift(self) -> bool:
        return len(self.items) > 0

    @property
    def summary(self) -> str:
        if not self.items:
            return "No drift detected"
        counts: dict[str, int] = {}
        for item in self.items:
            counts[item.drift_type] = counts.get(item.drift_type, 0) + 1
        parts = [f"{v} {k}" for k, v in counts.items()]
        return f"{len(self.items)} drifts: {', '.join(parts)}"


async def detect_drift(
    cfg: ReplicatorConfig,
    dbname: str,
) -> DriftReport:
    """Compare schema objects between source and target for one database."""
    report = DriftReport(database=dbname)

    async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
        schemas = await discover_schemas(src, cfg)

        # Detect schemas missing on target (must run before table comparison so
        # fix_ddl for missing tables can safely assume the schema exists).
        tgt_schema_rows = await tgt.fetch(
            "SELECT nspname FROM pg_namespace WHERE nspname = ANY($1::text[])",
            schemas,
        )
        tgt_schemas: set[str] = {r["nspname"] for r in tgt_schema_rows}
        missing_schemas: set[str] = set(schemas) - tgt_schemas

        for schema in sorted(missing_schemas):
            report.items.append(DriftItem(
                object_type="schema",
                schema=schema,
                table="",
                name=schema,
                drift_type="missing_on_target",
                detail=f"Schema {schema!r} exists on source but not on target",
                fix_ddl=f"CREATE SCHEMA IF NOT EXISTS {qi(schema)};",
            ))

        src_tables = await get_tables(src, schemas)
        tgt_tables = await get_tables(tgt, schemas)

        src_table_set = {(t["schema_name"], t["table_name"]) for t in src_tables}
        tgt_table_set = {(t["schema_name"], t["table_name"]) for t in tgt_tables}

        # Tables missing on target
        for schema, table in sorted(src_table_set - tgt_table_set):
            table_ddl = await generate_full_table_ddl(src, schema, table)
            # Prepend schema creation when the schema itself is also missing so
            # the fix_ddl is self-contained and can be applied as-is.
            if schema in missing_schemas:
                fix_ddl = f"CREATE SCHEMA IF NOT EXISTS {qi(schema)};\n{table_ddl}"
            else:
                fix_ddl = table_ddl
            report.items.append(DriftItem(
                object_type="table",
                schema=schema,
                table=table,
                name=table,
                drift_type="missing_on_target",
                detail=f"Table {schema}.{table} exists on source but not target",
                fix_ddl=fix_ddl,
            ))

        # Tables missing on source (extra on target)
        for schema, table in sorted(tgt_table_set - src_table_set):
            fqn = qt(schema, table)
            report.items.append(DriftItem(
                object_type="table",
                schema=schema,
                table=table,
                name=table,
                drift_type="missing_on_source",
                detail=f"Table {schema}.{table} exists on target but not source",
                fix_ddl=f"DROP TABLE IF EXISTS {fqn};",
            ))

        # Compare columns, indexes, constraints for common tables
        common = src_table_set & tgt_table_set
        for schema, table in sorted(common):
            fqn = qt(schema, table)

            # Columns
            src_cols = await get_columns(src, fqn)
            tgt_cols = await get_columns(tgt, fqn)
            src_col_map = {c["column_name"]: c for c in src_cols}
            tgt_col_map = {c["column_name"]: c for c in tgt_cols}

            for col_name in set(src_col_map) - set(tgt_col_map):
                col = src_col_map[col_name]
                fix = (
                    f"ALTER TABLE {fqn} ADD COLUMN {qi(col_name)} {col['data_type']}"
                )
                if col["not_null"]:
                    fix += " NOT NULL"
                if col["column_default"]:
                    fix += f" DEFAULT {col['column_default']}"
                fix += ";"
                report.items.append(DriftItem(
                    object_type="column",
                    schema=schema,
                    table=table,
                    name=col_name,
                    drift_type="missing_on_target",
                    detail=f"Column {col_name} ({col['data_type']})",
                    fix_ddl=fix,
                ))

            for col_name in set(tgt_col_map) - set(src_col_map):
                report.items.append(DriftItem(
                    object_type="column",
                    schema=schema,
                    table=table,
                    name=col_name,
                    drift_type="missing_on_source",
                    detail=f"Column {col_name} exists on target but not source",
                ))

            # Type mismatches
            for col_name in set(src_col_map) & set(tgt_col_map):
                sc = src_col_map[col_name]
                tc = tgt_col_map[col_name]
                if sc["data_type"] != tc["data_type"]:
                    report.items.append(DriftItem(
                        object_type="column",
                        schema=schema,
                        table=table,
                        name=col_name,
                        drift_type="different",
                        detail=(
                            f"Type mismatch: source={sc['data_type']}, "
                            f"target={tc['data_type']}"
                        ),
                    ))

            # Indexes
            src_idx = await get_indexes(src, fqn)
            tgt_idx = await get_indexes(tgt, fqn)
            src_idx_map = {i["index_name"]: i for i in src_idx}
            tgt_idx_map = {i["index_name"]: i for i in tgt_idx}

            for idx_name in set(src_idx_map) - set(tgt_idx_map):
                idx = src_idx_map[idx_name]
                if not idx["is_primary"]:
                    report.items.append(DriftItem(
                        object_type="index",
                        schema=schema,
                        table=table,
                        name=idx_name,
                        drift_type="missing_on_target",
                        detail=idx["index_def"],
                        fix_ddl=idx["index_def"] + ";",
                    ))

            # Constraints
            src_con = await get_constraints(src, fqn)
            tgt_con = await get_constraints(tgt, fqn)
            src_con_map = {c["constraint_name"]: c for c in src_con}
            tgt_con_map = {c["constraint_name"]: c for c in tgt_con}

            for con_name in set(src_con_map) - set(tgt_con_map):
                c = src_con_map[con_name]
                fix = (
                    f"ALTER TABLE {fqn} ADD CONSTRAINT {qi(con_name)} "
                    f"{c['constraint_def']};"
                )
                report.items.append(DriftItem(
                    object_type="constraint",
                    schema=schema,
                    table=table,
                    name=con_name,
                    drift_type="missing_on_target",
                    detail=c["constraint_def"],
                    fix_ddl=fix,
                ))

        # Enum types
        src_enums = await get_enum_types(src, schemas)
        tgt_enums = await get_enum_types(tgt, schemas)
        tgt_enum_map = {
            (e["schema_name"], e["type_name"]): list(e["labels"]) for e in tgt_enums
        }

        for src_enum in src_enums:
            schema = src_enum["schema_name"]
            type_name = src_enum["type_name"]
            src_labels: list[str] = list(src_enum["labels"])
            key = (schema, type_name)

            if key not in tgt_enum_map:
                labels_sql = ", ".join(f"'{lbl}'" for lbl in src_labels)
                fix = (
                    f"CREATE TYPE {qi(schema)}.{qi(type_name)} "
                    f"AS ENUM ({labels_sql});"
                )
                report.items.append(DriftItem(
                    object_type="enum",
                    schema=schema,
                    table="",
                    name=type_name,
                    drift_type="missing_on_target",
                    detail=f"Enum type {schema}.{type_name} missing on target",
                    fix_ddl=fix,
                ))
            else:
                tgt_label_set = set(tgt_enum_map[key])
                for label in src_labels:
                    if label not in tgt_label_set:
                        fix = (
                            f"ALTER TYPE {qi(schema)}.{qi(type_name)} "
                            f"ADD VALUE IF NOT EXISTS '{label}';"
                        )
                        report.items.append(DriftItem(
                            object_type="enum",
                            schema=schema,
                            table="",
                            name=type_name,
                            drift_type="different",
                            detail=f"Enum value '{label}' missing on target",
                            fix_ddl=fix,
                        ))

        # Functions
        src_funcs = await get_functions(src, schemas)
        tgt_funcs = await get_functions(tgt, schemas)
        tgt_func_map = {
            (f["schema_name"], f["func_name"]): f["func_def"] for f in tgt_funcs
        }

        for src_func in src_funcs:
            func_schema = src_func["schema_name"]
            func_name = src_func["func_name"]
            key = (func_schema, func_name)
            src_def: str = src_func["func_def"]
            tgt_def = tgt_func_map.get(key)
            if tgt_def is None:
                report.items.append(DriftItem(
                    object_type="function",
                    schema=func_schema,
                    table="",
                    name=func_name,
                    drift_type="missing_on_target",
                    detail=f"Function {func_schema}.{func_name} missing on target",
                    fix_ddl=src_def + ";",
                ))
            elif src_def != tgt_def:
                report.items.append(DriftItem(
                    object_type="function",
                    schema=func_schema,
                    table="",
                    name=func_name,
                    drift_type="different",
                    detail=f"Function {func_schema}.{func_name} body differs",
                    fix_ddl=src_def + ";",
                ))

        # Triggers
        src_trigs = await get_triggers(src, schemas)
        tgt_trigs = await get_triggers(tgt, schemas)
        tgt_trig_map = {
            (t["schema_name"], t["table_name"], t["trigger_name"]): t["trigger_def"]
            for t in tgt_trigs
        }

        for src_trig in src_trigs:
            trig_schema = src_trig["schema_name"]
            trig_table = src_trig["table_name"]
            trig_name = src_trig["trigger_name"]
            src_def = src_trig["trigger_def"]
            key = (trig_schema, trig_table, trig_name)
            tgt_def = tgt_trig_map.get(key)
            fqn = f"{qi(trig_schema)}.{qi(trig_table)}"
            if tgt_def is None:
                report.items.append(DriftItem(
                    object_type="trigger",
                    schema=trig_schema,
                    table=trig_table,
                    name=trig_name,
                    drift_type="missing_on_target",
                    detail=f"Trigger {trig_name} on {trig_schema}.{trig_table} missing on target",
                    fix_ddl=src_def + ";",
                ))
            elif src_def != tgt_def:
                report.items.append(DriftItem(
                    object_type="trigger",
                    schema=trig_schema,
                    table=trig_table,
                    name=trig_name,
                    drift_type="different",
                    detail=f"Trigger {trig_name} on {trig_schema}.{trig_table} body differs",
                    fix_ddl=(
                        f"DROP TRIGGER IF EXISTS {qi(trig_name)} ON {fqn};\n"
                        + src_def + ";"
                    ),
                ))

        # Ownership drift — tables, sequences, schemas
        src_owners_map = {
            (r["schema_name"], r["object_name"], r["kind"]): r["owner"]
            for r in await get_object_owners(src, schemas)
        }
        tgt_owners_map = {
            (r["schema_name"], r["object_name"], r["kind"]): r["owner"]
            for r in await get_object_owners(tgt, schemas)
        }
        existing_roles = {
            r["rolname"]
            for r in await tgt.fetch("SELECT rolname FROM pg_roles")
        }

        for (own_schema, obj, kind), src_owner in src_owners_map.items():
            tgt_owner = tgt_owners_map.get((own_schema, obj, kind))
            if tgt_owner == src_owner:
                continue
            if src_owner not in existing_roles:
                fix = ""
                detail = (
                    f"Owner mismatch: source={src_owner!r}, target={tgt_owner!r}; "
                    f"role '{src_owner}' does not exist on target — create it first"
                )
            else:
                if kind == "table":
                    fix = f"ALTER TABLE {qi(own_schema)}.{qi(obj)} OWNER TO {qi(src_owner)};"
                elif kind == "sequence":
                    fix = f"ALTER SEQUENCE {qi(own_schema)}.{qi(obj)} OWNER TO {qi(src_owner)};"
                else:  # schema
                    fix = f"ALTER SCHEMA {qi(obj)} OWNER TO {qi(src_owner)};"
                detail = f"Owner mismatch: source={src_owner!r}, target={tgt_owner!r}"
            report.items.append(DriftItem(
                object_type=f"ownership ({kind})",
                schema=own_schema,
                table=obj if kind == "table" else "",
                name=obj,
                drift_type="different",
                detail=detail,
                fix_ddl=fix,
            ))

    return report


async def detect_ownership_drift(
    cfg: ReplicatorConfig,
    dbname: str,
) -> list[DriftItem]:
    """Return DriftItems for every object whose owner differs between source and target.

    Covers tables, sequences, and user schemas.  Objects whose source owner
    does not exist on the target are flagged but marked with an empty fix_ddl
    so that callers can warn the user without attempting to apply a broken
    ALTER statement.
    """
    items: list[DriftItem] = []
    async with connect(cfg.source, dbname) as src, connect(cfg.target, dbname) as tgt:
        schemas = await discover_schemas(src, cfg)
        src_owners = {
            (r["schema_name"], r["object_name"], r["kind"]): r["owner"]
            for r in await get_object_owners(src, schemas)
        }
        tgt_owners = {
            (r["schema_name"], r["object_name"], r["kind"]): r["owner"]
            for r in await get_object_owners(tgt, schemas)
        }
        existing_roles = {
            r["rolname"]
            for r in await tgt.fetch("SELECT rolname FROM pg_roles")
        }

    for (schema, obj, kind), src_owner in src_owners.items():
        tgt_owner = tgt_owners.get((schema, obj, kind))
        if tgt_owner == src_owner:
            continue
        if src_owner not in existing_roles:
            fix = ""
            detail = (
                f"Owner mismatch: source={src_owner!r}, target={tgt_owner!r}; "
                f"role '{src_owner}' does not exist on target — create it first"
            )
        else:
            if kind == "table":
                fix = f"ALTER TABLE {qi(schema)}.{qi(obj)} OWNER TO {qi(src_owner)};"
            elif kind == "sequence":
                fix = f"ALTER SEQUENCE {qi(schema)}.{qi(obj)} OWNER TO {qi(src_owner)};"
            else:  # schema
                fix = f"ALTER SCHEMA {qi(obj)} OWNER TO {qi(src_owner)};"
            detail = f"Owner mismatch: source={src_owner!r}, target={tgt_owner!r}"
        items.append(DriftItem(
            object_type=f"ownership ({kind})",
            schema=schema,
            table=obj if kind == "table" else "",
            name=obj,
            drift_type="different",
            detail=detail,
            fix_ddl=fix,
        ))
    return items


async def apply_drift_fixes(
    cfg: ReplicatorConfig,
    dbname: str,
    report: DriftReport,
    drop_extra: bool = False,
) -> int:
    """Apply corrective DDL for fixable drift items.  Returns count of applied fixes.

    For newly created tables (missing_on_target) the subscription is refreshed
    with ``copy_data = true`` so that PostgreSQL's built-in tablesync mechanism
    handles the initial data copy.  This avoids the WAL-replay conflict that
    arises when data is manually pre-copied and then the subscription worker
    tries to replay the same changes from the replication slot.

    Args:
        drop_extra: When True, also DROP tables that exist on target but not on source.
    """
    from replicator.replication import refresh_subscription

    applied = 0
    new_tables: list[tuple[str, str]] = []  # (schema, table) of tables we just created

    async with connect(cfg.target, dbname) as tgt:
        for item in report.items:
            if not item.fix_ddl:
                continue
            if item.drift_type == "missing_on_target":
                try:
                    await tgt.execute(item.fix_ddl)
                    log.info("Applied fix for %s %s.%s", item.object_type, item.schema, item.name)
                    applied += 1
                    if item.object_type == "table":
                        new_tables.append((item.schema, item.table))
                except Exception as exc:
                    log.error(
                        "Failed to apply DDL for %s %s.%s — %s",
                        item.object_type, item.schema, item.name, exc,
                    )
            elif item.drift_type == "missing_on_source" and drop_extra:
                try:
                    await tgt.execute(item.fix_ddl)
                    log.info("Dropped extra %s %s.%s from target", item.object_type, item.schema, item.name)
                    applied += 1
                except Exception as exc:
                    log.error(
                        "Failed to drop %s %s.%s — %s",
                        item.object_type, item.schema, item.name, exc,
                    )
            elif item.drift_type == "different":
                try:
                    await tgt.execute(item.fix_ddl)
                    log.info("Applied fix for %s %s.%s", item.object_type, item.schema, item.name)
                    applied += 1
                except Exception as exc:
                    log.error(
                        "Failed to apply DDL for %s %s.%s — %s",
                        item.object_type, item.schema, item.name, exc,
                    )

    # Refresh the subscription with copy_data=true so PostgreSQL's tablesync
    # worker handles the initial data copy for newly added tables.  This
    # coordinates the snapshot and WAL position internally, preventing the
    # duplicate-key / WAL conflict that a manual COPY would cause.
    if new_tables:
        try:
            await refresh_subscription(cfg, dbname, copy_data=True)
            log.info(
                "Scheduled tablesync for new tables in %s: %s",
                dbname,
                ", ".join(f"{s}.{t}" for s, t in new_tables),
            )
        except Exception as exc:
            log.warning("Could not refresh subscription for %s — %s", dbname, exc)

    return applied
