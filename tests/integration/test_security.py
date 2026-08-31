"""Credentials must not reach stdout, stderr, or a job log.

The source password is embedded in the ``CONNECTION`` clause of
``CREATE SUBSCRIPTION`` and stored in ``pg_subscription.subconninfo``, where a
superuser can read it back.  The diagnostics that quote that string — the ones
that make a broken subscription diagnosable — are therefore the exact place a
password is most likely to be printed into a log that outlives the migration.
"""

from __future__ import annotations

import pytest

from pg_emigrant.bootstrap import bootstrap
from pg_emigrant.db import connect
from tests.helpers.cli import run_cli, write_config
from tests.helpers.pg import TEST_PG_PASSWORD

pytestmark = [pytest.mark.integration, pytest.mark.slow]


def test_no_command_prints_the_database_password(tmp_path, cfg, source_db):
    """Across every read-only command, on both streams."""
    path = write_config(tmp_path / "config.yaml", cfg)

    for args in (
        ("--verbose", "preflight", "-c", str(path)),
        ("--verbose", "preflight", "-c", str(path), "--format", "json"),
        ("--verbose", "bootstrap", "-c", str(path)),
        ("--verbose", "status", "-c", str(path)),
        ("--verbose", "status", "-c", str(path), "--format", "json"),
        ("--verbose", "cutover-check", "-c", str(path)),
        ("--verbose", "detect-ddl", "-c", str(path)),
        ("--verbose", "sync-sequences", "-c", str(path)),
        ("--verbose", "teardown", "-c", str(path)),
    ):
        result = run_cli(*args)
        combined = result.stdout + result.stderr
        assert TEST_PG_PASSWORD not in combined, (
            f"'{' '.join(args)}' printed the database password"
        )


async def test_a_broken_subscription_diagnostic_redacts_the_connection_string(
    cfg, source_db
):
    """The message that quotes subconninfo is the highest-risk one.

    It exists precisely to show the operator the exact stored string, and it
    is raised in the situation where someone will paste it into a ticket.
    """
    await bootstrap(cfg, database=source_db)

    async with connect(cfg.target, source_db) as tgt:
        stored = await tgt.fetchval(
            "SELECT subconninfo FROM pg_subscription LIMIT 1"
        )
    assert TEST_PG_PASSWORD in stored, (
        "the test is not exercising anything: PostgreSQL already hid the "
        "password, so there is nothing for redaction to remove"
    )

    from pg_emigrant.utils import redact_conninfo

    assert TEST_PG_PASSWORD not in redact_conninfo(stored)
    assert cfg.source.host in redact_conninfo(stored), (
        "redaction removed the host too, which is what the message is for"
    )


def test_a_failing_subscription_error_does_not_leak_the_password(tmp_path, cfg, source_db):
    """Point the subscription at an unreachable source and read the error.

    The CONNECTION string is part of the failing statement, so a server error
    that echoes the statement would carry the password with it.
    """
    broken = cfg.model_copy(deep=True)
    # A host the target's apply worker cannot reach, so CREATE SUBSCRIPTION
    # fails with the connection string in play.
    broken.source.host = "203.0.113.1"  # TEST-NET-3, guaranteed unroutable
    path = write_config(tmp_path / "broken.yaml", broken)

    result = run_cli("--verbose", "bootstrap", "-c", str(path), "--skip-preflight",
                     timeout=420)
    combined = result.stdout + result.stderr
    assert result.returncode != 0
    assert TEST_PG_PASSWORD not in combined, (
        "the password leaked through a failing CREATE SUBSCRIPTION"
    )


def test_a_written_config_is_not_world_readable(tmp_path, cfg):
    """Config files hold database passwords in plaintext."""
    import stat

    path = write_config(tmp_path / "config.yaml", cfg)
    mode = stat.S_IMODE(path.stat().st_mode)
    assert not mode & (stat.S_IRGRP | stat.S_IROTH), oct(mode)
