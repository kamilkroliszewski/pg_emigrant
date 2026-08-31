"""Pattern matching for ``exclude_tables`` — pure logic, no database needed.

Exclusion decides what data is left behind, so the matching rules are worth
pinning precisely: a pattern that matches one table too many silently drops
that table's data, and one that matches nothing silently drops nothing while
looking like it did.
"""

from __future__ import annotations

import pytest

from pg_emigrant.scope import filter_pairs, filter_tables, is_excluded, matches


@pytest.mark.parametrize(
    "pattern, schema, table, expected",
    [
        # Qualified: both halves must match.
        ("app.audit_log", "app", "audit_log", True),
        ("app.audit_log", "other", "audit_log", False),
        ("app.audit_log", "app", "orders", False),
        # Bare name: any schema.
        ("audit_log", "app", "audit_log", True),
        ("audit_log", "reporting", "audit_log", True),
        ("audit_log", "app", "orders", False),
        # Globs in either half.
        ("app.*", "app", "anything", True),
        ("app.audit_*", "app", "audit_log", True),
        ("app.audit_*", "app", "auditlog", False),
        ("*.temp_*", "anything", "temp_x", True),
        ("*_log", "app", "audit_log", True),
        # Identifiers are case-sensitive in PostgreSQL, so matching is too:
        # a table created as "Orders" is a different table from orders.
        ("Orders", "app", "orders", False),
        ("orders", "app", "Orders", False),
        ("APP.orders", "app", "orders", False),
        # A table name containing a dot is still addressable: only the first
        # dot separates schema from table.
        ("legacy.odd.name", "legacy", "odd.name", True),
        # Surrounding whitespace in a hand-edited config is forgiven.
        ("  app.orders  ", "app", "orders", True),
    ],
)
def test_matches(pattern, schema, table, expected):
    assert matches(pattern, schema, table) is expected


def test_no_patterns_excludes_nothing():
    for empty in (None, [], ()):
        assert is_excluded("app", "orders", empty) is False


def test_any_pattern_matching_is_enough():
    patterns = ["reporting.*", "app.audit_log"]
    assert is_excluded("app", "audit_log", patterns)
    assert is_excluded("reporting", "anything", patterns)
    assert not is_excluded("app", "orders", patterns)


def test_filter_tables_preserves_order_and_shape():
    rows = [
        {"schema_name": "app", "table_name": "orders", "relkind": "r"},
        {"schema_name": "app", "table_name": "audit_log", "relkind": "r"},
        {"schema_name": "reporting", "table_name": "totals", "relkind": "r"},
    ]
    kept = filter_tables(rows, ["app.audit_log"])
    assert [r["table_name"] for r in kept] == ["orders", "totals"]
    assert kept[0] is rows[0], "filtering should not rebuild the row dicts"
    assert filter_tables(rows, None) is rows


def test_filter_pairs():
    pairs = [("app", "orders"), ("app", "audit_log")]
    assert filter_pairs(pairs, ["audit_log"]) == {("app", "orders")}
    assert filter_pairs(pairs, None) == set(pairs)
