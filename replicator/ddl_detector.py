"""Schema drift detection between source and target databases.

Compares tables, columns, indexes, and constraints, and reports
differences.  Optionally generates and applies corrective DDL.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import asyncpg

from replicator.config import ReplicatorConfig
from replicator.db import connect
from replicator.schema_sync import (
    generate_full_table_ddl,
    get_columns,
    get_constraints,
    get_enum_types,
    get_indexes,
    get_tables,
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
        src_tables = await get_tables(src, cfg.schemas)
        tgt_tables = await get_tables(tgt, cfg.schemas)

        src_table_set = {(t["schema_name"], t["table_name"]) for t in src_tables}
        tgt_table_set = {(t["schema_name"], t["table_name"]) for t in tgt_tables}

        # Tables missing on target
        for schema, table in sorted(src_table_set - tgt_table_set):
            fix_ddl = await generate_full_table_ddl(src, schema, table)
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
        src_enums = await get_enum_types(src, cfg.schemas)
        tgt_enums = await get_enum_types(tgt, cfg.schemas)
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

    return report


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
