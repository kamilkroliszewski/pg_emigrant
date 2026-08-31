"""Running the real CLI in a subprocess.

Some behaviour only exists at the process boundary: exit codes, what lands on
stdout versus stderr, and what happens when the process is killed part-way
through.  None of that can be observed by calling the library functions
directly, so these tests drive ``pg_emigrant`` the way a runbook does.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import yaml

from pg_emigrant.config import ReplicatorConfig


def write_config(path: Path, cfg: ReplicatorConfig, **overrides) -> Path:
    """Serialise *cfg* to a YAML file the CLI can load.

    Written under the test's own tmp_path, never into the repository: it
    carries the throw-away containers' credentials, and a config file with
    credentials in it has no business being somewhere a commit could pick up.
    """
    data = {
        "source": cfg.source.model_dump(),
        "target": cfg.target.model_dump(),
        "databases": list(cfg.databases),
        "schemas": list(cfg.schemas),
        "exclude_databases": list(cfg.exclude_databases),
        "exclude_schemas": list(cfg.exclude_schemas),
        "exclude_tables": list(cfg.exclude_tables),
        "publication_name": cfg.publication_name,
        "subscription_name": cfg.subscription_name,
        "parallel_workers": cfg.parallel_workers,
        "table_parallel_workers": cfg.table_parallel_workers,
        "sequence_sync_interval": cfg.sequence_sync_interval,
    }
    data.update(overrides)
    path.write_text(yaml.safe_dump(data))
    path.chmod(0o600)
    return path


@dataclass
class CliResult:
    returncode: int
    stdout: str
    stderr: str

    def json(self):
        """Parse stdout as JSON.

        Fails loudly rather than falling back: the point of ``--format json`` is
        that stdout is machine-readable, so a log line leaking into it is the
        bug being tested for, not something to tolerate.
        """
        return json.loads(self.stdout)


def run_cli(*args: str, timeout: float = 300.0, env: dict | None = None) -> CliResult:
    proc = subprocess.run(
        [sys.executable, "-m", "pg_emigrant.cli", *args],
        capture_output=True, text=True, timeout=timeout,
        env={**os.environ, **(env or {}), "TERM": "dumb", "NO_COLOR": "1",
             "COLUMNS": "200"},
        cwd=Path(__file__).resolve().parents[2],
    )
    return CliResult(proc.returncode, proc.stdout, proc.stderr)


def spawn_cli(*args: str, env: dict | None = None) -> subprocess.Popen:
    """Start the CLI without waiting — for the interrupt tests."""
    return subprocess.Popen(
        [sys.executable, "-m", "pg_emigrant.cli", *args],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        env={**os.environ, **(env or {}), "TERM": "dumb", "NO_COLOR": "1"},
        cwd=Path(__file__).resolve().parents[2],
    )
