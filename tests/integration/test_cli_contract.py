"""P1: the contract a runbook actually depends on — exit codes and stdout.

Automation reads the exit status and, increasingly, parses ``--format json``.
Both are part of the tool's interface: a script that gates a cutover on
``preflight`` needs to tell "this cannot work" apart from "the config file is
missing", and one that parses the JSON needs stdout to contain JSON and
nothing else.
"""

from __future__ import annotations

import pytest

from pg_emigrant import exits
from pg_emigrant.bootstrap import bootstrap
from tests.helpers.cli import run_cli, write_config

pytestmark = [pytest.mark.integration, pytest.mark.slow]


@pytest.fixture
def config_file(tmp_path, cfg):
    return write_config(tmp_path / "config.yaml", cfg)


def test_missing_config_file_is_a_configuration_error(tmp_path):
    result = run_cli("preflight", "-c", str(tmp_path / "nope.yaml"))
    assert result.returncode == exits.CONFIG_ERROR, (
        f"expected a configuration-error exit, got {result.returncode}: "
        f"{result.stderr[-800:]}"
    )


def test_removed_option_is_a_configuration_error_with_the_fix_in_it(tmp_path, cfg):
    path = write_config(tmp_path / "config.yaml", cfg,
                        replication_slot_name="pg_emigrant_slot")
    result = run_cli("preflight", "-c", str(path))
    assert result.returncode == exits.CONFIG_ERROR
    combined = result.stdout + result.stderr
    assert "replication_slot_name" in combined
    assert "Delete the 'replication_slot_name:' line" in combined


def test_preflight_json_is_the_only_thing_on_stdout(config_file):
    """Logs go to stderr; stdout stays parseable.

    A single INFO line interleaved into stdout makes the payload unparseable
    for the caller that asked for JSON precisely so it would not have to parse
    prose.
    """
    # --verbose is a global option, so it precedes the subcommand.
    result = run_cli("--verbose", "preflight", "-c", str(config_file),
                     "--format", "json")
    payload = result.json()
    assert "checks" in payload and "passed" in payload
    assert result.stderr, "verbose logging produced nothing on stderr"


def test_preflight_failure_exits_with_the_preflight_code(tmp_path, cfg):
    """Point the target at the source: preflight must fail, and say why."""
    broken = cfg.model_copy(deep=True)
    broken.target = cfg.source.model_copy()
    path = write_config(tmp_path / "same.yaml", broken)

    result = run_cli("preflight", "-c", str(path), "--format", "json")
    assert result.returncode == exits.PREFLIGHT_FAILED
    payload = result.json()
    assert payload["passed"] is False
    failed = [c["name"] for c in payload["checks"] if c["status"] == "error"]
    assert "distinct_clusters" in failed


def test_bootstrap_into_the_same_cluster_is_refused_not_merely_failed(tmp_path, cfg):
    """The refusal has its own exit code, because it means 'do not retry'."""
    broken = cfg.model_copy(deep=True)
    broken.target = cfg.source.model_copy()
    path = write_config(tmp_path / "same.yaml", broken)

    result = run_cli("bootstrap", "-c", str(path), "--skip-preflight")
    assert result.returncode == exits.UNSAFE_REFUSED, (
        f"expected the 'unsafe, refused' exit code; got {result.returncode}. "
        f"stderr: {result.stderr[-500:]}"
    )


def test_successful_bootstrap_exits_zero_and_reports_json(config_file):
    result = run_cli("bootstrap", "-c", str(config_file), "--format", "json")
    assert result.returncode == exits.SUCCESS, result.stderr[-2000:]
    payload = result.json()
    assert payload["outcome"] == "success"
    assert payload["databases"][0]["rows_copied"] > 0


async def test_incomplete_bootstrap_exits_non_zero_with_the_migration_code(
    tmp_path, cfg, source_db, source_pg
):
    """A view that cannot be created is not a warning.

    The data is all there and replication is running, so nothing a row count
    checks would notice — which is exactly why this used to exit 0.
    """
    # A view whose definition cannot be reproduced on the target: it depends on
    # a function in a schema this migration is not carrying.
    source_pg.psql("CREATE SCHEMA helper", dbname=source_db)
    source_pg.psql(
        "CREATE FUNCTION helper.secret() RETURNS int LANGUAGE sql AS 'SELECT 42'",
        dbname=source_db,
    )
    source_pg.psql(
        "CREATE VIEW app.needs_helper AS SELECT helper.secret() AS n",
        dbname=source_db,
    )
    cfg.exclude_schemas = ["helper"]
    cfg.schemas = ["app", "reporting"]
    path = write_config(tmp_path / "config.yaml", cfg)

    result = run_cli("bootstrap", "-c", str(path), "--format", "json",
                     "--skip-preflight")
    assert result.returncode == exits.MIGRATION_FAILED, (
        f"a bootstrap that could not create a view exited {result.returncode}; "
        f"stdout={result.stdout[-1500:]}"
    )
    payload = result.json()
    assert payload["outcome"] == "incomplete"
    problems = " ".join(payload["databases"][0]["problems"])
    assert "needs_helper" in problems, problems


async def test_reinit_sync_refusal_has_its_own_exit_code_and_json(
    tmp_path, cfg, source_db
):
    """An impossible repair must not look like a retryable failure.

    A caller that retries on exit 1 would loop forever against a slot that is
    gone; exit 6 says 'streaming cannot fix this — re-copy'.
    """
    from pg_emigrant.db import connect
    from pg_emigrant.replication import sub_name
    from tests.helpers.replication import wait_for_catchup

    await bootstrap(cfg, database=source_db)
    await wait_for_catchup(cfg, source_db)

    slot = sub_name(cfg, source_db)
    async with connect(cfg.source, source_db) as src:
        await src.execute(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots"
            " WHERE slot_name = $1 AND active_pid IS NOT NULL", slot
        )
        import asyncio

        for _ in range(40):
            if not await src.fetchval(
                "SELECT active FROM pg_replication_slots WHERE slot_name = $1", slot
            ):
                break
            await asyncio.sleep(0.25)
        await src.execute("SELECT pg_drop_replication_slot($1)", slot)

    path = write_config(tmp_path / "config.yaml", cfg)
    result = run_cli("reinit-sync", "-c", str(path), "--format", "json")

    assert result.returncode == exits.RECOVERY_IMPOSSIBLE, (
        f"a refused repair exited {result.returncode}; stderr {result.stderr[-500:]}"
    )
    payload = result.json()
    assert payload["repaired"] is False
    assert payload["blocked"] == [source_db]
    assert payload["data_gap"] == []


async def test_detect_ddl_apply_exits_non_zero_when_a_fix_fails(
    tmp_path, cfg, source_db, source_pg, target_pg
):
    """'Applied 4 fixes' must not be the whole story when two of them failed.

    A repair pass that could not apply half its DDL and reported only a count
    leaves the operator believing the target is now correct.
    """
    from pg_emigrant.bootstrap import bootstrap

    await bootstrap(cfg, database=source_db)

    # Drift that cannot be repaired: a view on the source whose definition
    # depends on a function the target does not have.
    source_pg.psql("CREATE SCHEMA hidden", dbname=source_db)
    source_pg.psql(
        "CREATE FUNCTION hidden.f() RETURNS int LANGUAGE sql AS 'SELECT 1'",
        dbname=source_db,
    )
    source_pg.psql(
        "CREATE VIEW app.broken AS SELECT hidden.f() AS n", dbname=source_db
    )
    cfg.schemas = ["app", "reporting"]
    path = write_config(tmp_path / "config.yaml", cfg)

    result = run_cli("detect-ddl", "-c", str(path), "--apply", "--format", "json")

    assert result.returncode == exits.MIGRATION_FAILED, (
        f"a failed drift fix exited {result.returncode}; stdout {result.stdout[-800:]}"
    )
    payload = result.json()
    apply_result = payload[0]["apply"]
    assert apply_result["failed"] >= 1, apply_result
    assert any("broken" in f for f in apply_result["failures"]), apply_result


async def test_detect_ddl_apply_exits_zero_when_every_fix_lands(
    tmp_path, cfg, source_db, source_pg
):
    """The success path must stay a success path."""
    from pg_emigrant.bootstrap import bootstrap

    await bootstrap(cfg, database=source_db)
    source_pg.psql(
        "CREATE TABLE app.added_later (id int PRIMARY KEY, v text)", dbname=source_db
    )
    path = write_config(tmp_path / "config.yaml", cfg)

    result = run_cli("detect-ddl", "-c", str(path), "--apply", "--format", "json")
    assert result.returncode == exits.SUCCESS, result.stdout[-800:]
    payload = result.json()
    assert payload[0]["apply"]["failed"] == 0
    assert payload[0]["apply"]["applied"] >= 1
