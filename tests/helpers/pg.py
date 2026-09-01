"""Throw-away PostgreSQL clusters in Docker, for the integration suite.

Every integration test in this repository runs against a *real* PostgreSQL
server.  Logical replication, replication slots, exported snapshots, COPY,
``session_replication_role``, sequence semantics and transaction visibility
are precisely the behaviours these tests exist to pin down — mocking any of
them would only assert that the mock behaves like the mock.

A cluster is started with ``docker run``, configured for logical replication,
and torn down at the end of the session.  Containers are labelled so that a
crashed run can be cleaned up with::

    docker rm -f $(docker ps -aq --filter label=pg_emigrant_test=1)

Containers share the host network namespace (``--network host``, each on its
own port) rather than being port-mapped.  That is not a convenience: the
subscription's ``CONNECTION`` string is stored verbatim and resolved later by
the *target's own apply worker*, inside the target container.  With published
ports, ``127.0.0.1:<source port>`` means "the source" to the test process and
"myself" to the target container — which is precisely the production failure
mode ``warn_if_unstable_host`` exists to warn about, and it would make every
replication test fail for a reason unrelated to what it is testing.  Sharing
one network namespace makes the address mean the same thing everywhere, the
property real deployments are told to guarantee.
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
import uuid
from dataclasses import dataclass

from pg_emigrant.config import DatabaseConfig

# Password for the throw-away containers.  Overridable so that CI can inject a
# secret; the default is only ever used by a container that lives for the
# duration of one test session, is bound to 127.0.0.1, and holds nothing but
# generated fixture data.
TEST_PG_PASSWORD = os.environ.get("PG_EMIGRANT_TEST_PG_PASSWORD", "pgemigrant-test-pw")
TEST_PG_USER = os.environ.get("PG_EMIGRANT_TEST_PG_USER", "postgres")

LABEL = "pg_emigrant_test=1"

# Size of the tmpfs each throw-away cluster keeps its data directory on.  1 GiB
# is ample for the fixture and keeps a full version-matrix run off the
# developer's disk, but a real stress run (PG_EMIGRANT_STRESS_ROWS in the
# millions) needs room for the table, its indexes and the WAL the load
# generates — and a datadir that runs out of space surfaces as a bare
# "connection was closed in the middle of operation", which looks like a
# pg_emigrant bug and is not one.  Raise it alongside the row count:
#
#   PG_EMIGRANT_STRESS_ROWS=5000000 PG_EMIGRANT_TEST_TMPFS_SIZE=8g pytest …
TMPFS_SIZE = os.environ.get("PG_EMIGRANT_TEST_TMPFS_SIZE", "1g")

# Server settings every test cluster needs: logical decoding on the source
# side, and enough slots/workers that a multi-database test does not hit a
# limit unrelated to what it is testing.
_SERVER_ARGS = [
    "-c", "wal_level=logical",
    "-c", "max_replication_slots=20",
    "-c", "max_wal_senders=20",
    "-c", "max_worker_processes=20",
    "-c", "max_logical_replication_workers=16",
    "-c", "fsync=off",
    "-c", "full_page_writes=off",
    "-c", "synchronous_commit=off",
    "-c", "log_min_messages=warning",
    # Subscriber-side feedback interval.  The default 10s makes the source
    # slot's confirmed_flush_lsn — the authoritative "the target has this"
    # signal the catch-up helper waits on — lag by up to ten seconds, which
    # would turn every convergence assertion into a timeout race.
    "-c", "wal_receiver_status_interval=1s",
]

SUPPORTED_VERSIONS = ("14", "15", "16", "17", "18")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@dataclass
class PgContainer:
    """A running throw-away PostgreSQL container."""

    name: str
    version: str
    port: int
    image: str

    @property
    def config(self) -> DatabaseConfig:
        return DatabaseConfig(
            host="127.0.0.1",
            port=self.port,
            user=TEST_PG_USER,
            password=TEST_PG_PASSWORD,
            dbname="postgres",
            sslmode="disable",
        )

    def psql(self, sql: str, dbname: str = "postgres") -> str:
        """Run one statement through the container's own psql. Raises on error."""
        return subprocess.run(
            [
                "docker", "exec", "-e", f"PGPASSWORD={TEST_PG_PASSWORD}", self.name,
                "psql", "-v", "ON_ERROR_STOP=1", "-U", TEST_PG_USER,
                "-h", "127.0.0.1", "-p", str(self.port),
                "-d", dbname, "-tAc", sql,
            ],
            check=True, capture_output=True, text=True,
        ).stdout.strip()

    def psql_script(self, sql: str, db: str = "postgres", **psql_vars: str) -> str:
        """Feed a whole script to psql on stdin, stopping at the first error."""
        cmd = [
            "docker", "exec", "-i", "-e", f"PGPASSWORD={TEST_PG_PASSWORD}", self.name,
            "psql", "-v", "ON_ERROR_STOP=1", "-U", TEST_PG_USER,
            "-h", "127.0.0.1", "-p", str(self.port), "-d", db,
        ]
        for k, v in psql_vars.items():
            cmd += ["-v", f"{k}={v}"]
        return subprocess.run(
            cmd, input=sql, check=True, capture_output=True, text=True
        ).stdout

    def stop(self) -> None:
        subprocess.run(["docker", "rm", "-f", self.name],
                       capture_output=True, check=False)

    def pause(self) -> None:
        """Freeze the server process — simulates an unreachable node."""
        subprocess.run(["docker", "pause", self.name], capture_output=True, check=True)

    def unpause(self) -> None:
        subprocess.run(["docker", "unpause", self.name], capture_output=True, check=True)


def start_pg(version: str, *, extra_args: list[str] | None = None) -> PgContainer:
    """Start a PostgreSQL *version* container and block until it accepts queries."""
    name = f"pgem-test-{version}-{uuid.uuid4().hex[:10]}"
    port = _free_port()
    image = f"postgres:{version}-alpine"

    subprocess.run(
        [
            "docker", "run", "-d", "--name", name,
            "--label", LABEL,
            "-e", f"POSTGRES_PASSWORD={TEST_PG_PASSWORD}",
            "-e", f"POSTGRES_USER={TEST_PG_USER}",
            "-e", "POSTGRES_INITDB_ARGS=--encoding=UTF8 --locale=C",
            # See the module docstring: one shared network namespace, so an
            # address means the same thing to the test process and to a
            # target-side apply worker.
            "--network", "host",
            # tmpfs datadir: these clusters are disposable and this keeps a
            # full version-matrix run off the developer's disk.  Mounted at
            # the parent, not at PGDATA, because PGDATA moved between the
            # supported images (14-17: /var/lib/postgresql/data, 18:
            # /var/lib/postgresql/18/docker).  Size: see TMPFS_SIZE.
            "--tmpfs", f"/var/lib/postgresql:rw,size={TMPFS_SIZE},mode=1777",
            image, "postgres", "-c", f"port={port}",
            *_SERVER_ARGS, *(extra_args or []),
        ],
        check=True, capture_output=True, text=True,
    )
    container = PgContainer(name=name, version=version, port=port, image=image)
    try:
        _wait_ready(container)
    except Exception:
        logs = subprocess.run(["docker", "logs", "--tail", "40", name],
                              capture_output=True, text=True).stdout
        container.stop()
        raise RuntimeError(f"PostgreSQL {version} container did not become ready:\n{logs}")
    return container


def _wait_ready(container: PgContainer, timeout: float = 90.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        probe = subprocess.run(
            ["docker", "exec", container.name, "pg_isready", "-U", TEST_PG_USER,
             "-h", "127.0.0.1", "-p", str(container.port), "-q"],
            capture_output=True,
        )
        if probe.returncode == 0:
            # pg_isready goes green during initdb's own bootstrap phase, before
            # the real server is listening on the mapped port.  Confirm through
            # the port the tests will actually use.
            try:
                container.psql("SELECT 1")
                return
            except subprocess.CalledProcessError:
                pass
        time.sleep(0.4)
    raise TimeoutError(f"{container.name} not ready after {timeout}s")


def start_physical_standby(primary: PgContainer) -> PgContainer:
    """Stream a physical replica of *primary* and start it in recovery.

    This is the closest reproduction of a Patroni failover that does not
    require running Patroni: a real streaming standby, promoted for real.  What
    makes it faithful is the part that matters — logical replication slots are
    *local* to the instance that created them and are not carried by physical
    replication before PostgreSQL 17's failover slots, so the promoted node
    genuinely has no slot, exactly as a promoted Patroni leader would not.
    """
    name = f"pgem-standby-{primary.version}-{uuid.uuid4().hex[:10]}"
    port = _free_port()
    datadir = "/var/lib/postgresql/standby"

    # -R writes the primary_conninfo/standby.signal that put it in recovery;
    # -X stream keeps the WAL flowing so the base backup is self-consistent.
    script = (
        f"set -e; "
        f"mkdir -p {datadir}; chmod 0700 {datadir}; "
        f"pg_basebackup -h 127.0.0.1 -p {primary.port} -U {TEST_PG_USER} "
        f"  -D {datadir} -R -X stream -c fast; "
        f"exec postgres -D {datadir} -c port={port} "
        + " ".join(f"-c {a}" for a in _SERVER_ARGS if a != "-c")
    )
    subprocess.run(
        [
            "docker", "run", "-d", "--name", name, "--label", LABEL,
            "--network", "host", "--user", "postgres",
            "-e", f"PGPASSWORD={TEST_PG_PASSWORD}",
            f"postgres:{primary.version}-alpine",
            "sh", "-c", script,
        ],
        check=True, capture_output=True, text=True,
    )
    standby = PgContainer(
        name=name, version=primary.version, port=port,
        image=f"postgres:{primary.version}-alpine",
    )
    try:
        _wait_ready(standby)
    except Exception:
        logs = subprocess.run(["docker", "logs", "--tail", "40", name],
                              capture_output=True, text=True)
        standby.stop()
        raise RuntimeError(
            f"physical standby did not come up:\n{logs.stdout}\n{logs.stderr}"
        )
    return standby


def promote(standby: PgContainer, timeout: float = 60.0) -> None:
    """Promote a standby to primary and wait for it to leave recovery."""
    subprocess.run(
        ["docker", "exec", standby.name, "pg_ctl", "-D", "/var/lib/postgresql/standby",
         "promote"],
        check=True, capture_output=True, text=True,
    )
    deadline = time.time() + timeout
    while time.time() < deadline:
        if standby.psql("SELECT pg_is_in_recovery()") == "f":
            return
        time.sleep(0.3)
    raise TimeoutError(f"{standby.name} did not leave recovery within {timeout}s")


def docker_available() -> bool:
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=20).returncode == 0
    except Exception:
        return False
