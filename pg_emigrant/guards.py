"""Checks that must hold before anything is written, on every path.

The read-only ``preflight`` command already covers these and more, but it is
skippable (``--skip-preflight``) and it is a *CLI* step — the library entry
points, and the web GUI that calls them, do not run it.  The invariants here
are the ones whose violation is unrecoverable, so they are enforced where the
mutation happens rather than where the command line is parsed.
"""

from __future__ import annotations

from pg_emigrant.config import ReplicatorConfig
from pg_emigrant.db import connect
from pg_emigrant.utils import get_logger

log = get_logger(__name__)


class UnsafeOperation(Exception):
    """A precondition that makes the operation unsafe to attempt at all."""


async def assert_distinct_clusters(cfg: ReplicatorConfig) -> None:
    """Refuse to migrate a cluster into itself, or into its own replica.

    ``system_identifier`` is stamped in at initdb time and inherited by every
    physical replica, so a match means either literally the same server or a
    primary/standby pair.  Comparing host and port cannot establish this:
    the same cluster is reachable under a VIP, a pooler, a DNS alias, a second
    listen address, or simply a second port — every one of which looks like a
    different endpoint and is not.

    Getting this wrong is not a failed migration but a destroyed source.  The
    initial copy TRUNCATEs its target tables; if "target" is the source, that
    truncate lands on production data, and it happens before anything else
    would have had a chance to notice.  So this runs on every mutating path,
    is not skippable, and treats an unreadable identifier as a reason to stop
    rather than a reason to assume the best.
    """
    async with connect(cfg.source) as src, connect(cfg.target) as tgt:
        src_id = await _system_identifier(src)
        tgt_id = await _system_identifier(tgt)

    if src_id is None or tgt_id is None:
        # A host:port comparison is a weak substitute, but it is the only
        # thing left, and the common copy-paste mistake it does catch is worth
        # catching.  The refusal below is what keeps this fail-closed.
        same_endpoint = (
            cfg.source.host.strip().lower() == cfg.target.host.strip().lower()
            and int(cfg.source.port) == int(cfg.target.port)
        )
        if same_endpoint:
            raise UnsafeOperation(
                f"source and target are the same endpoint "
                f"({cfg.source.host}:{cfg.source.port}). A migration cannot "
                f"read from and write to the same cluster."
            )
        raise UnsafeOperation(
            "cannot read system_identifier from "
            + ("the source" if src_id is None else "the target")
            + ", so it is impossible to prove that source and target are "
              "different clusters. Migrating into the source itself would "
              "TRUNCATE production tables during the initial copy, so this is "
              "refused rather than assumed safe. Grant the migration role "
              "EXECUTE ON FUNCTION pg_control_system() on both sides (or use a "
              "superuser role) and retry."
        )

    if src_id == tgt_id:
        raise UnsafeOperation(
            f"source and target are the SAME PostgreSQL cluster "
            f"(system_identifier={src_id}), or one is a physical replica of the "
            f"other — host:port differing means nothing here, since the same "
            f"cluster is reachable under a VIP, a pooler, an alias or a second "
            f"port. The initial copy TRUNCATEs its target tables, so continuing "
            f"would destroy production data. Point 'target' at an independent "
            f"cluster."
        )

    log.debug("Cluster identity check passed: source %s != target %s", src_id, tgt_id)


async def _system_identifier(conn) -> int | None:
    try:
        return await conn.fetchval("SELECT system_identifier FROM pg_control_system()")
    except Exception as exc:  # insufficient privilege, mostly
        log.debug("Could not read system_identifier: %s", exc)
        return None
