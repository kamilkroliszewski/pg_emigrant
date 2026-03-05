"""Monitoring dashboard — replication lag, table counts, status."""

from __future__ import annotations

from rich.table import Table

from replicator.config import ReplicatorConfig
from replicator.db import connect, discover_databases
from replicator.ddl_detector import detect_drift
from replicator.replication import get_replication_slots, get_subscription_status, sub_name
from replicator.schema_sync import get_sequences, get_tables
from replicator.sequence_sync import sync_sequences_once
from replicator.utils import console, get_logger

log = get_logger(__name__)


async def build_status(cfg: ReplicatorConfig, database: str | None = None) -> None:
    """Gather status from all configured databases and print rich tables."""
    databases = [database] if database else await discover_databases(cfg)

    for dbname in databases:
        console.rule(f"[bold blue]Database: {dbname}")

        # --- Subscription status ---
        try:
            sub_rows = await get_subscription_status(cfg, dbname)
        except Exception as exc:
            console.print(f"[red]Cannot query subscription status: {exc}")
            sub_rows = []

        sub_table = Table(title="Subscription Status", show_lines=True)
        sub_table.add_column("Name")
        sub_table.add_column("PID")
        sub_table.add_column("Received LSN")
        sub_table.add_column("Latest End LSN")
        sub_table.add_column("Last Msg Sent")
        sub_table.add_column("Last Msg Received")

        for r in sub_rows:
            sub_table.add_row(
                str(r.get("subname", "")),
                str(r.get("pid", "")),
                str(r.get("received_lsn", "")),
                str(r.get("latest_end_lsn", "")),
                str(r.get("last_msg_send_time", "")),
                str(r.get("last_msg_receipt_time", "")),
            )
        console.print(sub_table)

        # --- Replication slots (on source) ---
        try:
            slot_rows = await get_replication_slots(cfg, dbname)
        except Exception as exc:
            console.print(f"[red]Cannot query replication slots: {exc}")
            slot_rows = []

        slot_table = Table(title="Replication Slots (source)", show_lines=True)
        slot_table.add_column("Slot")
        slot_table.add_column("Type")
        slot_table.add_column("Active")
        slot_table.add_column("Active PID")
        slot_table.add_column("Restart LSN")
        slot_table.add_column("Confirmed Flush LSN")
        slot_table.add_column("WAL Status")

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

        # --- Replication lag ---
        try:
            async with connect(cfg.source, dbname) as src:
                lag_rows = await src.fetch(
                    """
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
                    """,
                    sub_name(cfg, dbname),
                )
        except Exception as exc:
            console.print(f"[red]Cannot query replication lag: {exc}")
            lag_rows = []

        lag_table = Table(title="Replication Lag (source → target)", show_lines=True)
        lag_table.add_column("Application")
        lag_table.add_column("State")
        lag_table.add_column("Sent LSN")
        lag_table.add_column("Write Lag")
        lag_table.add_column("Flush Lag")
        lag_table.add_column("Replay Lag")

        for r in lag_rows:
            lag_table.add_row(
                str(r.get("application_name", "")),
                str(r.get("state", "")),
                str(r.get("sent_lsn", "")),
                str(r.get("write_lag", "")),
                str(r.get("flush_lag", "")),
                str(r.get("replay_lag", "")),
            )
        console.print(lag_table)

        # --- Table count ---
        try:
            async with connect(cfg.source, dbname) as src:
                src_tables = await get_tables(src, cfg.schemas)
            async with connect(cfg.target, dbname) as tgt:
                tgt_tables = await get_tables(tgt, cfg.schemas)
            console.print(
                f"  Tables: source={len(src_tables)}, target={len(tgt_tables)}"
            )
        except Exception as exc:
            console.print(f"[red]Cannot count tables: {exc}")

        # --- Sequence sync status ---
        try:
            seq_report = await sync_sequences_once(cfg, dbname)
            seq_table = Table(title="Sequence Sync", show_lines=True)
            seq_table.add_column("Schema")
            seq_table.add_column("Sequence")
            seq_table.add_column("Source Value")
            seq_table.add_column("Target Value")
            seq_table.add_column("Status")

            for s in seq_report:
                style = ""
                if s["status"] == "updated":
                    style = "yellow"
                elif s["status"] == "target_ahead":
                    style = "cyan"
                seq_table.add_row(
                    s["schema"],
                    s["sequence"],
                    str(s["source_value"]),
                    str(s["target_value"]),
                    s["status"],
                    style=style,
                )
            console.print(seq_table)
        except Exception as exc:
            console.print(f"[red]Cannot check sequences: {exc}")

        # --- Schema drift summary ---
        try:
            drift = await detect_drift(cfg, dbname)
            if drift.has_drift:
                console.print(f"  [yellow]Schema drift: {drift.summary}")
            else:
                console.print("  [green]No schema drift detected")
        except Exception as exc:
            console.print(f"[red]Cannot check drift: {exc}")

        console.print()
