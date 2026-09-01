"""The health state machine, pinned down without a database.

Every transition here is reachable against a real cluster and several are
covered that way in ``tests/integration``.  What a real cluster is bad at is
*combinations*: proving that a stuck table sync outranks a zero lag figure
takes one dataclass and no containers, and pins the precedence rules that the
integration tests can only observe one at a time.

Nothing here mocks PostgreSQL.  ``ReplicationHealth`` is pg_emigrant's own
value object; the measurements that fill it come from real catalog queries in
``replication_health`` and are asserted against real servers elsewhere.
"""

from __future__ import annotations

import pytest

from pg_emigrant.health import (
    DEFAULT_LAG_CRITICAL_BYTES,
    DEFAULT_LAG_WARN_BYTES,
    ReplicationHealth,
    ReplicationState,
    _classify,
)


def _measured(**overrides) -> ReplicationHealth:
    """A health record in the shape a working migration produces."""
    base = {
        "publication_exists": True,
        "slot_exists": True,
        "slot_active": True,
        "slot_wal_status": "reserved",
        "subscription_exists": True,
        "subscription_enabled": True,
        "apply_worker_running": True,
        "apply_error_count": 0,
        "sync_error_count": 0,
        "tables_total": 3,
        "tables_not_ready": [],
        "tables_not_replicated": [],
        "lag_bytes": 0,
        "retained_wal_bytes": 1024,
    }
    base.update(overrides)
    return ReplicationHealth(database="db", **base)


def _state(h: ReplicationHealth) -> ReplicationState:
    _classify(h, warn=DEFAULT_LAG_WARN_BYTES, critical=DEFAULT_LAG_CRITICAL_BYTES)
    return h.state


def test_a_fully_working_migration_is_healthy():
    assert _state(_measured()) is ReplicationState.HEALTHY


def test_nothing_set_up_is_absent_not_broken():
    h = ReplicationHealth(database="db")
    assert _state(h) is ReplicationState.ABSENT


@pytest.mark.parametrize(
    "override, expected_word",
    [
        ({"slot_exists": False}, "GONE"),
        ({"slot_wal_status": "lost"}, "recycled"),
        ({"subscription_exists": False}, "no subscription"),
        ({"subscription_enabled": False}, "DISABLED"),
        ({"apply_worker_running": False}, "apply worker is not running"),
        ({"publication_exists": False}, "publication is missing"),
    ],
)
def test_each_broken_condition_is_broken_and_says_why(override, expected_word):
    h = _measured(**override)
    assert _state(h) is ReplicationState.BROKEN
    assert any(expected_word in r for r in h.reasons), h.reasons


def test_a_stuck_table_sync_outranks_a_perfect_lag_figure():
    """The regression: everything measurable was fine except the missing table.

    A tablesync that cannot finish leaves the slot, the apply worker and the
    lag untouched — they are all genuinely healthy, because none of them is
    what is broken.  Reading only those reported HEALTHY over a table with none
    of its rows on the target, and cutover-check then said SAFE TO CUT OVER.
    """
    h = _measured(
        lag_bytes=0,
        sync_error_count=4,
        tables_not_ready=["app.late_arrival (state=d)"],
    )
    assert _state(h) is ReplicationState.BROKEN
    assert any("late_arrival" in r for r in h.reasons), h.reasons
    assert any("NOT on the target" in r for r in h.reasons), h.reasons


def test_a_table_sync_in_progress_is_lagging_not_healthy_and_not_broken():
    """No errors yet, so this is an initial sync doing its job — but the
    target still does not have those rows, and saying HEALTHY would be a
    green light over a table that is not there yet."""
    h = _measured(tables_not_ready=["app.late_arrival (state=d)"], sync_error_count=0)
    assert _state(h) is ReplicationState.LAGGING
    assert any("initial sync is still in progress" in r for r in h.reasons), h.reasons


def test_a_table_sync_in_progress_on_a_broken_slot_is_still_broken():
    """Severity ordering: a lost slot is not softened by anything below it."""
    h = _measured(slot_wal_status="lost", tables_not_ready=["app.t (state=i)"])
    assert _state(h) is ReplicationState.BROKEN


def test_unreserved_wal_is_critical_before_it_becomes_loss():
    h = _measured(slot_wal_status="unreserved")
    assert _state(h) is ReplicationState.CRITICAL
    assert any("max_slot_wal_keep_size" in r for r in h.reasons)


def test_lag_thresholds_are_inclusive_at_the_boundary():
    assert _state(_measured(lag_bytes=DEFAULT_LAG_WARN_BYTES - 1)) is ReplicationState.HEALTHY
    assert _state(_measured(lag_bytes=DEFAULT_LAG_WARN_BYTES)) is ReplicationState.LAGGING
    assert _state(_measured(lag_bytes=DEFAULT_LAG_CRITICAL_BYTES)) is ReplicationState.CRITICAL


def test_an_idle_walsender_is_lagging_not_healthy():
    assert _state(_measured(slot_active=False)) is ReplicationState.LAGGING


def test_many_unready_tables_are_summarised_not_dumped():
    h = _measured(
        sync_error_count=1,
        tables_not_ready=[f"app.t{i} (state=d)" for i in range(12)],
    )
    _state(h)
    reason = " ".join(h.reasons)
    assert "+7 more" in reason, reason
    assert "12 of 3" in reason or "12 of" in reason, reason


def test_to_dict_carries_the_new_signals_for_json_consumers():
    """``status --health --format json`` is the documented integration point
    for an exporter, so a signal that only exists in the terminal output is a
    signal a monitoring system cannot alert on."""
    h = _measured(sync_error_count=2, tables_not_ready=["app.t (state=d)"])
    _state(h)
    payload = h.to_dict()
    assert payload["tables_not_ready"] == ["app.t (state=d)"]
    assert payload["tables_total"] == 3
    assert payload["sync_error_count"] == 2


def test_an_unmeasurable_lag_is_not_healthy():
    """The rule the whole module is built on: unproven counts against.

    Every other signal can read clean while the one number that says whether
    the target is current is simply absent — a slot with no
    ``confirmed_flush_lsn`` has never had anything confirmed by the subscriber.
    Reporting HEALTHY there is a green light resting on a missing measurement.
    """
    h = _measured(lag_bytes=None)
    assert _state(h) is ReplicationState.LAGGING
    assert any("could not be measured" in r for r in h.reasons), h.reasons


def test_a_table_no_subscription_knows_about_is_not_healthy():
    """The invisible failure: nothing on either server is in an error state.

    A table that exists on the source and on the target but is in no
    publication is replicated by nothing. The drift scan sees it on both sides
    and reports nothing; the slot, the apply worker and the lag are all fine
    because none of them has ever heard of it. Reproduced against a PostgreSQL
    14 source, where ``detect-ddl --apply`` created the table, reported success,
    and left it permanently empty while `cutover-check` said SAFE TO CUT OVER.
    """
    h = _measured(tables_not_replicated=["app.late"])
    assert _state(h) is ReplicationState.LAGGING
    assert any("not part of this subscription" in r for r in h.reasons), h.reasons
    assert any("app.late" in r for r in h.reasons), h.reasons


def test_an_unreplicated_table_does_not_mask_a_broken_slot():
    h = _measured(slot_exists=False, tables_not_replicated=["app.late"])
    assert _state(h) is ReplicationState.BROKEN
