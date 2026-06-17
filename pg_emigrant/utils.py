"""Shared helpers: logging, SQL quoting, async utilities."""

from __future__ import annotations

import logging

from rich.console import Console
from rich.logging import RichHandler

console = Console()


def setup_logging(verbose: bool = False) -> None:
    """Configure structured logging with rich output."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
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
