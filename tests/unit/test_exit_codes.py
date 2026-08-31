"""Exit codes are part of the interface a runbook depends on."""

from __future__ import annotations

from pg_emigrant import exits
from pg_emigrant.report import BootstrapReport, Outcome


def test_codes_are_distinct_and_stable():
    codes = {
        exits.SUCCESS: 0, exits.GENERIC_FAILURE: 1, exits.CONFIG_ERROR: 2,
        exits.PREFLIGHT_FAILED: 3, exits.MIGRATION_FAILED: 4,
        exits.REPLICATION_UNHEALTHY: 5, exits.RECOVERY_IMPOSSIBLE: 6,
        exits.UNSAFE_REFUSED: 7,
    }
    assert list(codes.keys()) == list(codes.values()), (
        "an exit code changed value; existing runbooks depend on these"
    )
    assert len(set(codes)) == 8


def test_every_code_has_a_name():
    for code in range(8):
        assert exits.name(code) and not exits.name(code).startswith("exit ")


def test_a_report_with_no_databases_is_success():
    assert BootstrapReport().exit_code == exits.SUCCESS


def test_the_worst_outcome_decides_the_exit_code():
    report = BootstrapReport()
    report.add("a")
    assert report.exit_code == exits.SUCCESS and report.passed

    report.add("b").incomplete("a view is missing")
    assert report.outcome is Outcome.INCOMPLETE
    assert report.exit_code == exits.MIGRATION_FAILED
    assert not report.passed

    report.add("c").refuse("source and target are the same cluster")
    assert report.outcome is Outcome.REFUSED
    assert report.exit_code == exits.UNSAFE_REFUSED


def test_incomplete_never_collapses_to_success():
    """The specific regression: partial success used to exit 0."""
    report = BootstrapReport()
    result = report.add("db")
    result.rows_copied = 1_000_000
    result.incomplete("trigger not created: t on app.orders")
    assert report.exit_code != 0
    assert "incomplete" in report.summary


def test_a_harder_outcome_is_not_downgraded_by_a_later_soft_problem():
    report = BootstrapReport()
    result = report.add("db")
    result.fail("initial copy failed")
    result.incomplete("and a view is missing")
    assert result.outcome is Outcome.FAILED
