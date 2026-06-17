"""Configuration models and YAML loader for pg_emigrant."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field


class DatabaseConfig(BaseModel):
    """Connection parameters for a single PostgreSQL server."""

    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = ""
    dbname: str = "postgres"  # admin database for discovery
    sslmode: str = "prefer"


class ReplicatorConfig(BaseModel):
    """Top-level configuration for pg_emigrant."""

    source: DatabaseConfig
    target: DatabaseConfig
    schemas: list[str] = Field(default_factory=list)  # empty = auto-discover (all non-system schemas)
    databases: list[str] = Field(default_factory=list)  # empty = auto-discover
    publication_name: str = "pg_emigrant_pub"
    subscription_name: str = "pg_emigrant_sub"
    replication_slot_name: str = "pg_emigrant_slot"
    parallel_workers: int = 4
    table_parallel_workers: int = 4
    sequence_sync_interval: int = 10  # seconds
    exclude_databases: list[str] = Field(
        default_factory=lambda: ["template0", "template1", "postgres"]
    )
    exclude_tables: list[str] = Field(default_factory=list)


def load_config(path: Optional[str] = None) -> ReplicatorConfig:
    """Load configuration from a YAML file.

    Falls back to ``config.yaml`` in the current directory.
    """
    config_path = Path(path) if path else Path("config.yaml")
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path) as fh:
        raw = yaml.safe_load(fh)

    return ReplicatorConfig(**raw)
