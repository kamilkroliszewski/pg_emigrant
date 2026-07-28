"""Shared helpers: logging, SQL quoting, async utilities."""

from __future__ import annotations

import logging

from rich.console import Console
from rich.logging import RichHandler

console = Console()

# Log records go to STDERR, not stdout.  stdout is reserved for command output
# a caller may want to parse — `--format json` / `--format simple` are designed
# to be piped into jq or a CI gate, and an INFO line interleaved into them makes
# the payload unparseable.  Keeping diagnostics on stderr is also just the Unix
# convention, and it leaves them visible on an interactive terminal either way.
_log_console = Console(stderr=True)


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
