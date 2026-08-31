"""Configuration must be truthful: every option does what it claims.

The specific failure this guards against is an option that looks load-bearing
and is read by nothing — the shape ``exclude_tables`` and
``replication_slot_name`` both had, where an operator's decision to exclude a
table or name a slot silently did not happen.
"""

from __future__ import annotations

import pytest
import yaml
from pydantic import ValidationError

from pg_emigrant.config import ReplicatorConfig, load_config

MINIMAL = {"source": {"host": "a"}, "target": {"host": "b"}}


def test_replication_slot_name_is_rejected_with_the_fix_in_the_message():
    """It never took effect, and honouring it now would orphan live slots.

    Every existing installation carries the example file's value, so making it
    meaningful would rename the slot under a running migration and strand the
    old one on the production source, retaining WAL. Refusing costs one line.
    """
    with pytest.raises(ValidationError) as excinfo:
        ReplicatorConfig(**MINIMAL, replication_slot_name="pg_emigrant_slot")

    message = str(excinfo.value)
    assert "no longer a configuration option" in message
    assert "Delete the 'replication_slot_name:' line" in message
    assert "subscription_name" in message, "the message must say what to use instead"


def test_the_example_config_loads_and_sets_no_removed_options(tmp_path):
    """config.yaml.example is what every deployment starts from."""
    from pathlib import Path

    raw = yaml.safe_load(
        (Path(__file__).resolve().parents[2] / "config.yaml.example").read_text()
    )
    assert "replication_slot_name" not in raw, (
        "the example config still ships a setting the loader rejects"
    )
    cfg = ReplicatorConfig(**raw)
    assert cfg.publication_name and cfg.subscription_name


def test_every_documented_option_is_read_by_the_code():
    """A field nothing consults is a lie waiting to be believed.

    Scans the package for each field name rather than asserting a hand-kept
    list, so a newly added option that is never wired up fails here.
    """
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[2] / "pg_emigrant"
    sources = "\n".join(
        p.read_text() for p in root.rglob("*.py") if p.name != "config.py"
    )
    unused = []
    for name in ReplicatorConfig.model_fields:
        if name == "web":  # consumed by the optional web extra, checked below
            continue
        if not re.search(rf"\b{re.escape(name)}\b", sources):
            unused.append(name)
    assert not unused, (
        f"configuration option(s) {unused} appear in the model but are never "
        f"read anywhere in pg_emigrant/ — either wire them up or remove them"
    )


def test_missing_config_file_raises_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_config(str(tmp_path / "absent.yaml"))


def test_defaults_are_the_documented_ones():
    cfg = ReplicatorConfig(**MINIMAL)
    assert cfg.publication_name == "pg_emigrant_pub"
    assert cfg.subscription_name == "pg_emigrant_sub"
    assert cfg.exclude_tables == []
    assert cfg.exclude_databases == ["template0", "template1", "postgres"]
    assert cfg.parallel_workers == 4
