"""The GUI's configuration page must describe the tool that exists.

It is read by someone deciding whether a migration is set up correctly, so a
row describing a setting that was removed is worse than no row at all.
"""

from __future__ import annotations

import pytest

from pg_emigrant.config import ReplicatorConfig

flask = pytest.importorskip("flask", reason="the web extra is not installed")

from pg_emigrant.web.services import masked_config  # noqa: E402

CFG = ReplicatorConfig(
    source={"host": "src", "password": "sourcepw"},
    target={"host": "tgt", "password": "targetpw"},
    subscription_name="my_sub",
)


def test_no_removed_option_is_displayed():
    view = masked_config(CFG)
    assert "replication_slot_name" not in view


def test_the_slot_name_is_shown_as_derived():
    view = masked_config(CFG)
    assert view["slot_name_pattern"] == "my_sub_<database>"


def test_passwords_never_reach_the_browser():
    view = masked_config(CFG)
    rendered = repr(view)
    assert "sourcepw" not in rendered
    assert "targetpw" not in rendered


def test_exclude_tables_is_shown_because_it_now_does_something():
    cfg = CFG.model_copy(deep=True)
    cfg.exclude_tables = ["app.audit_log"]
    assert masked_config(cfg)["exclude_tables"] == ["app.audit_log"]
