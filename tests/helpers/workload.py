"""A concurrent write workload to run *while* a migration is in flight.

The point of the initial-copy design (create the slot first, copy with its
exported snapshot) is that there is no window in which a committed write lands
in neither the copy nor the WAL stream.  That claim is only worth anything if
something is actually writing during the copy, on more than one table, in more
than one shape — including the shapes that behave differently under logical
replication: a table with no primary key (REPLICA IDENTITY FULL), an UPDATE
that moves a row, a DELETE, and sequence advancement.
"""

from __future__ import annotations

import asyncio

import asyncpg

from pg_emigrant.config import DatabaseConfig
from pg_emigrant.db import connect


class ConcurrentWriter:
    """Drives INSERT/UPDATE/DELETE against the fixture while a test runs."""

    def __init__(self, cfg: DatabaseConfig, dbname: str, *, delay: float = 0.01):
        self.cfg = cfg
        self.dbname = dbname
        self.delay = delay
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self.inserted_customers = 0
        self.inserted_audit = 0
        self.updated = 0
        self.deleted = 0
        self.error: BaseException | None = None

    async def __aenter__(self) -> "ConcurrentWriter":
        self._task = asyncio.create_task(self._run())
        # Do not return until at least one batch has committed, so a fast
        # bootstrap cannot finish before the workload has written anything —
        # which would make the test silently vacuous.
        for _ in range(200):
            if self.inserted_customers or self.error:
                break
            await asyncio.sleep(0.02)
        return self

    async def __aexit__(self, *exc) -> None:
        await self.stop()

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await self._task
        if self.error:
            raise self.error

    async def _run(self) -> None:
        try:
            async with connect(self.cfg, self.dbname) as conn:
                n = 0
                while not self._stop.is_set():
                    await self._batch(conn, n)
                    n += 1
                    await asyncio.sleep(self.delay)
                # One final batch after the stop signal, so the last write is
                # known to be recent relative to the catch-up mark.
                await self._batch(conn, n)
        except BaseException as exc:  # noqa: BLE001 - surfaced by stop()
            self.error = exc

    async def _batch(self, conn: asyncpg.Connection, n: int) -> None:
        async with conn.transaction():
            cid = await conn.fetchval(
                "INSERT INTO app.customers (email, display_name, profile, balance)"
                " VALUES ($1, $2, $3, $4) RETURNING id",
                f"live{n}@example.com", f"Live writer {n}",
                f'{{"batch": {n}}}', n % 100,
            )
            self.inserted_customers += 1

            await conn.execute(
                "INSERT INTO app.orders (customer_id, state, note) VALUES ($1, 'placed', $2)",
                cid, f"live order {n}",
            )

            # PK-less table: only reachable through REPLICA IDENTITY FULL.
            await conn.execute(
                "INSERT INTO app.audit_log (actor, action, payload)"
                " VALUES ($1, $2, $3)",
                f"live{n}", "created", f'{{"batch": {n}}}',
            )
            self.inserted_audit += 1

            # UPDATE and DELETE on pre-existing rows, so the workload exercises
            # more than append-only traffic.
            # balance is numeric(18,4) and the fixture deliberately seeds one
            # row at the type's boundary (99999999999999.9999) to exercise
            # COPY's handling of it.  Incrementing that row overflows — which
            # only ever happens once the workload has run long enough to walk
            # the id order down to it, i.e. in the soak test and not in the
            # short ones.  The increment is guarded rather than dropped: the
            # UPDATE still has to change a numeric column, which is the shape
            # being replicated.
            updated = await conn.fetchval(
                "UPDATE app.customers SET display_name = display_name || '*',"
                " balance = CASE WHEN balance < 1e13 THEN balance + 1 ELSE balance END"
                " WHERE id = ("
                "  SELECT id FROM app.customers WHERE display_name NOT LIKE '%*'"
                "  ORDER BY id LIMIT 1) RETURNING 1"
            )
            if updated:
                self.updated += 1

            # UPDATE on the PK-less table — the case that fails outright
            # without REPLICA IDENTITY FULL on the source.
            await conn.execute(
                "UPDATE app.audit_log SET action = 'amended'"
                " WHERE ctid = (SELECT ctid FROM app.audit_log"
                "               WHERE action = 'created' LIMIT 1)"
            )

            await conn.execute(
                "INSERT INTO app.nasty_strings (id, val, note) VALUES ($1, $2, 'live')",
                100 + n, f"live value {n}",
            )
            # Delete the row the PREVIOUS batch inserted, so every batch after
            # the first performs a real DELETE of an already-committed row —
            # the case an initial copy plus stream has to get right.
            if n > 0:
                deleted = await conn.fetchval(
                    "DELETE FROM app.nasty_strings WHERE id = $1 RETURNING 1", 100 + n - 1
                )
                if deleted:
                    self.deleted += 1

            # Advance a standalone sequence that no INSERT touches.
            await conn.fetchval("SELECT nextval('app.ticket_seq')")
