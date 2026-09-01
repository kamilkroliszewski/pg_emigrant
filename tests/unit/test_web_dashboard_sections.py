"""The dashboard must actually fetch the section its health pill depends on.

The pill shows the replication state computed by :mod:`pg_emigrant.health`, and
falls back to a much weaker heuristic — subscription row, slot activity, lag
string, per-schema table counts — whenever that state is absent from the
payload. Every one of those fallback signals reads perfectly normal while a
table's initial sync is stuck or a table is in no publication at all, which is
the exact case measured before this was changed: table counts 2/2, slot active,
lag "0 bytes", and a green pill over a table with none of its rows on the
target.

So dropping ``health`` from the dashboard's section list — a tempting way to
make the dashboard cheaper — silently reverts the pill to that heuristic
without any other visible change. This pins the two halves that have to agree:
the section is requested, and its name is one the status collector knows.
"""

from __future__ import annotations

import re
from pathlib import Path

from pg_emigrant.monitor import _ALL_SECTIONS

APP_JS = Path(__file__).resolve().parents[2] / "pg_emigrant" / "web" / "static" / "js" / "app.js"


def _dashboard_sections() -> list[str]:
    match = re.search(
        r"^const DASH_LIGHT_SECTIONS\s*=\s*'([^']*)';", APP_JS.read_text(), re.M
    )
    assert match, "DASH_LIGHT_SECTIONS is no longer a plain string constant in app.js"
    return [s.strip() for s in match.group(1).split(",") if s.strip()]


def test_the_dashboard_requests_the_health_section():
    sections = _dashboard_sections()
    assert "health" in sections, (
        f"the dashboard fetches {sections} — without 'health' its pill falls "
        f"back to subscription/slot/lag/table-count heuristics, every one of "
        f"which reads green over a table that is not being replicated"
    )


def test_every_section_the_dashboard_asks_for_actually_exists():
    """A typo'd section name is silently returned empty, not rejected."""
    unknown = set(_dashboard_sections()) - set(_ALL_SECTIONS)
    assert not unknown, (
        f"app.js asks for section(s) the status collector does not produce: "
        f"{sorted(unknown)}; known sections are {sorted(_ALL_SECTIONS)}"
    )


def test_the_health_state_mapping_covers_every_state():
    """Each ReplicationState needs a colour, or it silently renders as a
    warning — including BROKEN, which must never be anything but an error."""
    from pg_emigrant.health import ReplicationState

    body = APP_JS.read_text()
    match = re.search(r"const HEALTH_STATE_CLASS\s*=\s*\{(.*?)\};", body, re.S)
    assert match, "HEALTH_STATE_CLASS is no longer a plain object literal in app.js"
    mapped = dict(re.findall(r"(\w+)\s*:\s*'(\w+)'", match.group(1)))

    for state in ReplicationState:
        assert state.value in mapped, (
            f"{state.value!r} has no colour in app.js's HEALTH_STATE_CLASS"
        )
    assert mapped["healthy"] == "ok"
    assert mapped["broken"] == "err", "BROKEN must render as an error, not a warning"
    assert mapped["critical"] == "err"
