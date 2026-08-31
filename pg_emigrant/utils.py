"""Shared helpers: logging, SQL quoting, async utilities."""

from __future__ import annotations

import logging
import sys

from rich.console import Console
from rich.logging import RichHandler

console = Console()

# Log records go to STDERR, not stdout.  stdout is reserved for command output
# a caller may want to parse — `--format json` / `--format simple` are designed
# to be piped into jq or a CI gate, and an INFO line interleaved into them makes
# the payload unparseable.  Keeping diagnostics on stderr is also just the Unix
# convention, and it leaves them visible on an interactive terminal either way.
_log_console = Console(stderr=True)


def route_console_to_stderr() -> None:
    """Send every human-facing console write to stderr.

    Called whenever a command is asked for machine-readable output.  Log
    records already go to stderr, but the Rich ``console`` — progress spinners,
    rules, per-table copy lines, the final summary — writes to stdout, and a
    single one of those interleaved into a JSON document makes it unparseable
    for the caller that asked for JSON precisely so it would not have to parse
    prose.  Keeping the diagnostics is the point: they still appear on the
    terminal, just not in the payload.
    """
    console.file = sys.stderr


def setup_logging(verbose: bool = False) -> None:
    """Configure structured logging with rich output (on stderr)."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=_log_console, rich_tracebacks=True)],
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


_SECRET_KEYWORDS = ("password", "passfile", "sslpassword", "sslkey")


def redact_conninfo(text: str | None) -> str:
    """Blank out secrets in a libpq connection string before it is shown.

    Connection strings end up in three places a password must not: an
    exception message, a log line, and the web GUI's job output.  The most
    important of those is the diagnostic raised when a subscription's stored
    ``CONNECTION`` cannot reach the source — it deliberately quotes the string
    back, because seeing the exact literal is what makes that failure
    diagnosable, and it is read by a superuser, for whom
    ``pg_subscription.subconninfo`` is not redacted by PostgreSQL.

    Handles the two spellings libpq accepts (``password=secret`` and
    ``password='se cret'``, backslash escapes included) and leaves everything
    else untouched, so the string stays useful for the thing it was printed
    for.
    """
    if not text:
        return ""

    out: list[str] = []
    i = 0
    while i < len(text):
        match = None
        for keyword in _SECRET_KEYWORDS:
            if text.startswith(keyword, i) and _at_word_start(text, i):
                j = i + len(keyword)
                while j < len(text) and text[j] in " \t":
                    j += 1
                if j < len(text) and text[j] == "=":
                    match = (keyword, j + 1)
                    break
        if match is None:
            out.append(text[i])
            i += 1
            continue

        keyword, value_start = match
        j = value_start
        while j < len(text) and text[j] in " \t":
            j += 1
        if j < len(text) and text[j] == "'":
            j += 1
            while j < len(text):
                if text[j] == "\\":
                    j += 2
                    continue
                if text[j] == "'":
                    j += 1
                    break
                j += 1
        else:
            while j < len(text) and text[j] not in " \t":
                j += 1
        out.append(f"{keyword}=***REDACTED***")
        i = j
    return "".join(out)


def _at_word_start(text: str, i: int) -> bool:
    return i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")


def qi(identifier: str) -> str:
    """Quote a SQL identifier (schema, table, column name)."""
    # Double any embedded double-quotes, then wrap in double-quotes
    return '"' + identifier.replace('"', '""') + '"'


def qt(schema: str, table: str) -> str:
    """Return a fully qualified ``"schema"."table"`` reference."""
    return f"{qi(schema)}.{qi(table)}"


def ql(text: str) -> str:
    """Quote a SQL string literal (double any embedded single quotes)."""
    return "'" + text.replace("'", "''") + "'"
