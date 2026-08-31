"""The failure injector must be impossible to arm by accident.

It exists only for this repository's own tests.  A hook that one stray
environment variable could switch on would be a production hazard, so arming
takes two variables and neither of them is anything a migration host would
have set.
"""

from __future__ import annotations

import pytest

from pg_emigrant._testhooks import (
    ARM_VAR,
    PHASE_VAR,
    PHASES,
    InjectedFailure,
    active_phase,
    maybe_fail,
)


def test_inert_with_no_environment(monkeypatch):
    monkeypatch.delenv(ARM_VAR, raising=False)
    monkeypatch.delenv(PHASE_VAR, raising=False)
    assert active_phase() is None
    for phase in PHASES:
        maybe_fail(phase)  # must not raise


def test_the_phase_variable_alone_does_not_arm_it(monkeypatch):
    monkeypatch.delenv(ARM_VAR, raising=False)
    monkeypatch.setenv(PHASE_VAR, "data_copy")
    assert active_phase() is None
    maybe_fail("data_copy")


def test_the_arming_variable_alone_does_not_arm_it(monkeypatch):
    monkeypatch.setenv(ARM_VAR, "1")
    monkeypatch.delenv(PHASE_VAR, raising=False)
    assert active_phase() is None
    maybe_fail("data_copy")


def test_both_variables_arm_exactly_one_phase(monkeypatch):
    monkeypatch.setenv(ARM_VAR, "1")
    monkeypatch.setenv(PHASE_VAR, "data_copy")
    assert active_phase() == "data_copy"
    with pytest.raises(InjectedFailure):
        maybe_fail("data_copy")
    maybe_fail("table_create")  # a different phase is unaffected


def test_an_unknown_phase_is_rejected_rather_than_never_firing(monkeypatch):
    """A typo must be loud: a phase that silently never fires would make a
    failure-injection test pass by testing nothing."""
    monkeypatch.setenv(ARM_VAR, "1")
    monkeypatch.setenv(PHASE_VAR, "not_a_phase")
    with pytest.raises(InjectedFailure):
        active_phase()


def test_arming_is_not_reachable_from_configuration():
    """Failure injection must never be settable from a config file.

    Config files get copied between environments; an environment variable
    named for testing does not.
    """
    from pg_emigrant.config import ReplicatorConfig

    fields = set(ReplicatorConfig.model_fields)
    assert not {f for f in fields if "fail" in f or "inject" in f or "hook" in f}


def test_the_pause_variable_alone_does_not_arm_it(monkeypatch):
    from pg_emigrant._testhooks import PAUSE_VAR

    monkeypatch.delenv(ARM_VAR, raising=False)
    monkeypatch.setenv(PAUSE_VAR, "index_create")
    assert active_phase(PAUSE_VAR) is None
    maybe_fail("index_create")  # must return immediately, not block


def test_pausing_and_failing_are_independent_phases(monkeypatch):
    from pg_emigrant._testhooks import PAUSE_VAR

    monkeypatch.setenv(ARM_VAR, "1")
    monkeypatch.setenv(PAUSE_VAR, "index_create")
    monkeypatch.setenv(PHASE_VAR, "data_copy")
    assert active_phase(PAUSE_VAR) == "index_create"
    assert active_phase(PHASE_VAR) == "data_copy"
    with pytest.raises(InjectedFailure):
        maybe_fail("data_copy")
    maybe_fail("table_create")  # neither armed phase: no block, no raise


def test_the_pause_is_bounded(monkeypatch):
    """A test that forgets to signal must fail on its own timeout, not hang."""
    from pg_emigrant._testhooks import PAUSE_SECONDS

    assert 0 < PAUSE_SECONDS <= 300
