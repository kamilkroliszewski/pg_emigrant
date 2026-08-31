"""Shared pytest configuration.

The suite is split in two:

``tests/unit``
    Pure logic — identifier quoting, config validation, name derivation,
    severity classification.  No database, no Docker, milliseconds to run.

``tests/integration``
    Real PostgreSQL in Docker.  These are the tests that matter for the
    safety claims: replication slots, exported snapshots, COPY, sequence
    semantics and transaction visibility are only meaningfully verified
    against a real server.

Version matrix: integration tests run source→target pairs.  ``--pg-matrix``
(or ``PG_EMIGRANT_TEST_MATRIX``) selects them, e.g.
``--pg-matrix 14->18,17->17``.  The default is a single ``18->18`` pair, so an
ordinary ``pytest`` run is fast; the cross-version matrix is opt-in.
"""

from __future__ import annotations

import os

import pytest

from tests.helpers.pg import SUPPORTED_VERSIONS, docker_available

DEFAULT_MATRIX = "18->18"


def pytest_addoption(parser):
    parser.addoption(
        "--pg-matrix",
        action="store",
        default=os.environ.get("PG_EMIGRANT_TEST_MATRIX", DEFAULT_MATRIX),
        help=(
            "Comma-separated source->target PostgreSQL version pairs to run the "
            "integration suite against, e.g. '14->18,15->18,18->18'. "
            "Use 'full' for the whole supported matrix."
        ),
    )


FULL_MATRIX = [f"{v}->18" for v in SUPPORTED_VERSIONS] + ["17->17"]


def _parse_matrix(raw: str) -> list[tuple[str, str]]:
    if raw.strip().lower() == "full":
        raw = ",".join(FULL_MATRIX)
    pairs = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        src, _, tgt = chunk.partition("->")
        src, tgt = src.strip(), tgt.strip()
        if src not in SUPPORTED_VERSIONS or tgt not in SUPPORTED_VERSIONS:
            raise pytest.UsageError(
                f"--pg-matrix entry {chunk!r} names an unsupported version; "
                f"supported: {', '.join(SUPPORTED_VERSIONS)}"
            )
        pairs.append((src, tgt))
    if not pairs:
        raise pytest.UsageError("--pg-matrix selected no version pairs")
    return pairs


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: needs a real PostgreSQL in Docker")
    config.addinivalue_line("markers", "slow: takes more than a few seconds")


def pytest_generate_tests(metafunc):
    """Parameterise anything that asks for ``pg_pair`` over the version matrix."""
    if "pg_version_pair" in metafunc.fixturenames:
        pairs = _parse_matrix(metafunc.config.getoption("--pg-matrix"))
        metafunc.parametrize(
            "pg_version_pair",
            pairs,
            ids=[f"pg{s}-to-pg{t}" for s, t in pairs],
            scope="session",
        )


def pytest_collection_modifyitems(config, items):
    if docker_available():
        return
    skip = pytest.mark.skip(reason="Docker is not available — integration tests skipped")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
