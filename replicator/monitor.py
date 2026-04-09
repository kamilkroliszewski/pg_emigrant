"""Monitoring dashboard — replication lag, table counts, status."""

from __future__ import annotations

import json as _json
from typing import Any

from rich.table import Table

from replicator.config import ReplicatorConfig
from replicator.db import connect, discover_databases, discover_schemas
from replicator.ddl_detector import detect_drift
from replicator.replication import get_replication_slots, get_subscription_status, sub_name
from replicator.schema_sync import get_tables
from replicator.sequence_sync import get_sequence_status
from replicator.utils import console, get_logger

log = get_logger(__name__)

_ALL_SECTIONS = frozenset({"subscription", "slots", "lag", "tables", "sequences", "drift"})

_LAG_SQL = """
SELECT
    application_name,
    state,
    sent_lsn,
    write_lsn,
    flush_lsn,
    replay_lsn,
    write_lag,
    flush_lag,
    replay_lag
FROM pg_stat_replication
WHERE application_name = $1;
"""


def _kv_quote(s: object) -> str:
    """Wrap a value in double-quotes if it contains whitespace or '=' signs."""
    v = str(s) if s is not None else ""
    if not v:
        return '""'
    if any(c in v for c in ' \t\n"='):
        return '"' + v.replace('"', '\\"') + '"'
    return v


async def _collect_db_status(
    cfg: ReplicatorConfig,
    dbname: str,
    sections: frozenset[str],
) -> dict[str, Any]:
    """Gather raw status data for *dbname* into a plain-Python dict."""
    data: dict[str, Any] = {"database": dbname, "errors": {}}

    if "subscription" in sections:
        try:
            rows = await get_subscription_status(cfg, dbname)
            data["subscription"] = [dict(r) for r in rows]
        except Exception as exc:
            data["subscription"] = []
            data["errors"]["subscription"] = str(exc)

    if "slots" in sections:
        try:
            rows = await get_replication_slots(cfg, dbname)
            data["slots"] = [dict(r) for r in rows]
        except Exception as exc:
            data["slots"] = []
            data["errors"]["slots"] = str(exc)

    if "lag" in sections:
        try:
            async with connect(cfg.source, dbname) as src:
                rows = await src.fetch(_LAG_SQL, sub_name(cfg, dbname))
            data["lag"] = [dict(r) for r in rows]
        except Exception as exc:
            data["lag"] = []
            data["errors"]["lag"] = str(exc)

    if "tables" in sections:
        try:
            async with connect(cfg.source, dbname) as src:
                schemas = await discover_schemas(src, cfg)
                src_tables = await get_tables(src, schemas)
            async with connect(cfg.target, dbname) as tgt:
                tgt_tables = await get_tables(tgt, schemas)
            src_by: dict[str, int] = {}
            for t in src_tables:
                src_by[t["schema_name"]] = src_by.get(t["schema_name"], 0) + 1
            tgt_by: dict[str, int] = {}
            for t in tgt_tables:
                tgt_by[t["schema_name"]] = tgt_by.get(t["schema_name"], 0) + 1
            data["tables"] = [
                {"schema": s, "source": src_by.get(s, 0), "target": tgt_by.get(s, 0)}
                for s in sorted(set(list(src_by) + list(tgt_by)))
            ]
        except Exception as exc:
            data["tables"] = []
            data["errors"]["tables"] = str(exc)

    if "sequences" in sections:
        try:
            data["sequences"] = await get_sequence_status(cfg, dbname)
        except Exception as exc:
            data["sequences"] = []
            data["errors"]["sequences"] = str(exc)

    if "drift" in sections:
        try:
            drift = await detect_drift(cfg, dbname)
            data["drift"] = {
                "has_drift": drift.has_drift,
                "summary": drift.summary,
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
                    for item in drift.items
                ],
            }
        except Exception as exc:
            data["drift"] = {"has_drift": False, "summary": "", "items": []}
            data["errors"]["drift"] = str(exc)

    return data


def _render_rich(data: dict[str, Any], sections: frozenset[str]) -> None:
    """Render status for one database using rich tables (default format)."""
    dbname = data["database"]
    console.rule(f"[bold blue]Database: {dbname}")

    for section_name, err in data.get("errors", {}).items():
        console.print(f"[red]Cannot query {section_name}: {err}")

    if "subscription" in sections:
        sub_table = Table(title="Subscription Status", show_lines=True)
        sub_table.add_column("Name")
        sub_table.add_column("PID")
        sub_table.add_column("Received LSN")
        sub_table.add_column("Latest End LSN")
        sub_table.add_column("Lag", style="bold")
        sub_table.add_column("Last Msg Sent")
        sub_table.add_column("Last Msg Received")
        for r in data.get("subscription", []):
            lag = str(r.get("lag", ""))
            lag_style = "red" if lag and lag != "0 bytes" else "green"
            sub_table.add_row(
                str(r.get("subname", "")),
                str(r.get("pid", "")),
                str(r.get("received_lsn", "")),
                str(r.get("latest_end_lsn", "")),
                f"[{lag_style}]{lag}[/{lag_style}]",
                str(r.get("last_msg_send_time", "")),
                str(r.get("last_msg_receipt_time", "")),
            )
        console.print(sub_table)

    if "slots" in sections:
        slot_table = Table(title="Replication Slots (source)", show_lines=True)
        slot_table.add_column("Slot")
        slot_table.add_column("Type")
        slot_table.add_column("Active")
        slot_table.add_column("Active PID")
        slot_table.add_column("Restart LSN")
        slot_table.add_column("Confirmed Flush LSN")
        slot_table.add_column("WAL Status")
        slot_rows = data.get("slots", [])
        if not slot_rows:
            console.print(f"[bold red]⚠ No replication slot found for {dbname} — replication may be broken!")
        else:
            for r in slot_rows:
                active = r.get("active", False)
                active_str = "[green]✓ active[/green]" if active else "[red]✗ inactive[/red]"
                slot_table.add_row(
                    str(r.get("slot_name", "")),
                    str(r.get("slot_type", "")),
                    active_str,
                    str(r.get("active_pid", "") or ""),
                    str(r.get("restart_lsn", "")),
                    str(r.get("confirmed_flush_lsn", "")),
                    str(r.get("wal_status", "")),
                )
            console.print(slot_table)

    if "lag" in sections:
        lag_table = Table(title="Replication Lag (source → target)", show_lines=True)
        lag_table.add_column("Application")
        lag_table.add_column("State")
        lag_table.add_column("Sent LSN")
        lag_table.add_column("Write Lag")
        lag_table.add_column("Flush Lag")
        lag_table.add_column("Replay Lag")
        for r in data.get("lag", []):
            lag_table.add_row(
                str(r.get("application_name", "")),
                str(r.get("state", "")),
                str(r.get("sent_lsn", "")),
                str(r.get("write_lag", "")),
                str(r.get("flush_lag", "")),
                str(r.get("replay_lag", "")),
            )
        console.print(lag_table)

    if "tables" in sections:
        count_table = Table(title="Tables per Schema", show_lines=True)
        count_table.add_column("Schema")
        count_table.add_column("Source", justify="right")
        count_table.add_column("Target", justify="right")
        for row in data.get("tables", []):
            style = "" if row["source"] == row["target"] else "yellow"
            count_table.add_row(row["schema"], str(row["source"]), str(row["target"]), style=style)
        console.print(count_table)

    if "sequences" in sections:
        seq_table = Table(title="Sequence Sync", show_lines=True)
        seq_table.add_column("Schema")
        seq_table.add_column("Sequence")
        seq_table.add_column("Source Value")
        seq_table.add_column("Target Value")
        seq_table.add_column("Status")
        for s in data.get("sequences", []):
            style = {"behind": "yellow", "target_ahead": "cyan", "missing_on_target": "red"}.get(s["status"], "")
            seq_table.add_row(
                s["schema"], s["sequence"],
                str(s["source_value"]), str(s["target_value"]),
                s["status"],
                style=style,
            )
        console.print(seq_table)

    if "drift" in sections:
        drift_data = data.get("drift", {})
        if drift_data.get("has_drift"):
            console.print(f"  [yellow]Schema drift: {drift_data.get('summary', '')}")
        else:
            console.print("  [green]No schema drift detected")

    console.print()


def _render_simple(data: dict[str, Any], sections: frozenset[str]) -> None:
    """Render status as grep-friendly key=value lines — one record per line."""
    dbname = data["database"]
    p = f"db={_kv_quote(dbname)}"

    for section_name, err in data.get("errors", {}).items():
        print(f"{p} section={section_name} error={_kv_quote(err)}")

    if "subscription" in sections:
        for r in data.get("subscription", []):
            lag = str(r.get("lag", ""))
            print(
                f"{p} section=subscription"
                f" subname={_kv_quote(r.get('subname', ''))}"
                f" pid={r.get('pid', '')}"
                f" received_lsn={r.get('received_lsn', '')}"
                f" latest_end_lsn={r.get('latest_end_lsn', '')}"
                f" lag={_kv_quote(lag)}"
                f" last_msg_send={r.get('last_msg_send_time', '')}"
                f" last_msg_recv={r.get('last_msg_receipt_time', '')}"
            )

    if "slots" in sections:
        slot_rows = data.get("slots", [])
        if not slot_rows:
            print(f"{p} section=slot warning=no_slot_found")
        for r in slot_rows:
            active = "true" if r.get("active") else "false"
            print(
                f"{p} section=slot"
                f" slot_name={r.get('slot_name', '')}"
                f" slot_type={r.get('slot_type', '')}"
                f" active={active}"
                f" active_pid={r.get('active_pid', '') or ''}"
                f" restart_lsn={r.get('restart_lsn', '')}"
                f" confirmed_flush_lsn={r.get('confirmed_flush_lsn', '')}"
                f" wal_status={r.get('wal_status', '')}"
            )

    if "lag" in sections:
        for r in data.get("lag", []):
            print(
                f"{p} section=lag"
                f" application={r.get('application_name', '')}"
                f" state={r.get('state', '')}"
                f" sent_lsn={r.get('sent_lsn', '')}"
                f" write_lag={r.get('write_lag', '')}"
                f" flush_lag={r.get('flush_lag', '')}"
                f" replay_lag={r.get('replay_lag', '')}"
            )

    if "tables" in sections:
        for row in data.get("tables", []):
            match = "true" if row["source"] == row["target"] else "false"
            print(
                f"{p} section=table"
                f" schema={row['schema']}"
                f" source={row['source']}"
                f" target={row['target']}"
                f" match={match}"
            )

    if "sequences" in sections:
        for s in data.get("sequences", []):
            print(
                f"{p} section=sequence"
                f" schema={s['schema']}"
                f" sequence={s['sequence']}"
                f" source={s['source_value']}"
                f" target={s['target_value']}"
                f" status={s['status']}"
            )

    if "drift" in sections:
        drift_data = data.get("drift", {})
        if not drift_data.get("has_drift"):
            print(f"{p} section=drift status=ok")
        else:
            for item in drift_data.get("items", []):
                print(
                    f"{p} section=drift"
                    f" type={_kv_quote(item['object_type'])}"
                    f" schema={item['schema']}"
                    f" table={item['table']}"
                    f" name={_kv_quote(item['name'])}"
                    f" drift={item['drift_type']}"
                    f" detail={_kv_quote(item['detail'])}"
                )


async def build_status(
    cfg: ReplicatorConfig,
    database: str | None = None,
    fmt: str = "rich",
    sections: frozenset[str] | None = None,
) -> None:
    """Gather status from all configured databases and render in *fmt* format.

    Args:
        fmt: ``rich`` (default, coloured tables), ``simple`` (grep-friendly
             key=value lines), or ``json`` (single JSON array to stdout).
        sections: Which sections to include.  ``None`` means all sections.
    """
    if sections is None:
        sections = _ALL_SECTIONS

    databases = [database] if database else await discover_databases(cfg)
    all_data: list[dict[str, Any]] = []

    for dbname in databases:
        data = await _collect_db_status(cfg, dbname, sections)
        all_data.append(data)
        if fmt == "rich":
            _render_rich(data, sections)
        elif fmt == "simple":
            _render_simple(data, sections)

    if fmt == "json":
        print(_json.dumps(all_data, indent=2, default=str))
