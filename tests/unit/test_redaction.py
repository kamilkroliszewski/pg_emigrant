"""Credentials must not travel with diagnostics.

A connection string is the one piece of migration state that contains a
password, and the places it naturally ends up — an exception message, a log
line, the web GUI's job output — are all places it must not.
"""

from __future__ import annotations

import pytest

from pg_emigrant.utils import redact_conninfo

SECRET = "s3cr3t!pa ss'word"


@pytest.mark.parametrize(
    "conninfo",
    [
        f"host='db' port='5432' user='m' password='{SECRET}' dbname='app'",
        "host=db port=5432 password=hunter2 dbname=app",
        "password=hunter2",
        "password='hunter2'",
        "password = 'hunter2' host=x",
        r"password='pa\'ss' host=x",
        "host=x password=hunter2 replication=database",
        "sslpassword='keysecret' host=x",
    ],
)
def test_secrets_are_removed(conninfo):
    out = redact_conninfo(conninfo)
    assert "REDACTED" in out
    for secret in ("hunter2", "keysecret", "s3cr3t", "pa\\'ss"):
        assert secret not in out, f"{secret!r} survived redaction of {conninfo!r}"


def test_the_rest_of_the_string_survives():
    """Redaction must not destroy the diagnostic value of the string.

    The reason a connection string is printed at all is that seeing the exact
    host it names is what makes the failure diagnosable.
    """
    out = redact_conninfo(
        "host='db.internal' port='5432' user='migrator' "
        f"password='{SECRET}' dbname='app' sslmode='require'"
    )
    assert "db.internal" in out
    assert "5432" in out
    assert "migrator" in out
    assert "sslmode='require'" in out


@pytest.mark.parametrize(
    "text",
    [
        "",
        "host=x sslmode=prefer",
        "user=nopassword_person host=x",
        "the password could not be verified",  # prose, not a conninfo
    ],
)
def test_strings_without_a_credential_are_left_alone(text):
    assert "REDACTED" not in redact_conninfo(text)


def test_none_is_tolerated():
    assert redact_conninfo(None) == ""
