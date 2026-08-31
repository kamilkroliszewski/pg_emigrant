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


class WebAuthConfig(BaseModel):
    """Login credentials for the ``pg_emigrant web`` GUI.

    ``enabled`` is deliberately tri-state.  Left unset (the default) it means
    "on when a credential is configured": adding a password switches
    authentication on, and an existing config that has none keeps working
    exactly as before — but says so loudly at startup.  Setting it explicitly
    makes the intent unambiguous in both directions, and ``enabled: true`` with
    no credential is a configuration error the app refuses to start with,
    rather than quietly serving an unprotected GUI.

    Supply exactly one of ``password_hash`` (preferred — generate it with
    ``pg_emigrant hash-password``) or ``password``.  Plaintext is accepted
    because this file already holds database passwords, so a hash here is not
    the only thing standing between an attacker and the databases; it is still
    the weaker option, and a hash costs one command.
    """

    enabled: Optional[bool] = None
    username: str = "admin"
    password: str = ""
    password_hash: str = ""
    # Signs the session cookie.  Left empty, a random key is generated at
    # startup — safe, but every restart invalidates existing sessions.  Set it
    # to keep people logged in across restarts (and across several workers).
    secret_key: str = ""
    # How long a session stays valid, refreshed on each request.
    session_timeout_minutes: int = 720
    # Set when the GUI is served over HTTPS (behind a TLS-terminating proxy):
    # marks the session cookie Secure so it is never sent over plain HTTP.
    cookie_secure: bool = False
    # Failed logins per client address before that address is refused for
    # lockout_seconds.  0 disables the throttle.
    max_attempts: int = 5
    lockout_seconds: int = 300

    @property
    def has_credential(self) -> bool:
        return bool(self.password_hash or self.password)

    @property
    def is_enabled(self) -> bool:
        """Resolve the tri-state ``enabled`` against the configured credential."""
        if self.enabled is None:
            return self.has_credential
        return self.enabled


class WebConfig(BaseModel):
    """Settings that apply only to the optional web GUI."""

    auth: WebAuthConfig = Field(default_factory=WebAuthConfig)


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
    # Only applies in auto-discover mode (schemas: []) — same relationship as
    # exclude_databases has to databases: an explicit "schemas" list already
    # says exactly what to migrate and always wins outright.
    exclude_schemas: list[str] = Field(default_factory=list)
    exclude_tables: list[str] = Field(default_factory=list)
    # Web GUI settings; ignored by every CLI command except `pg_emigrant web`.
    web: WebConfig = Field(default_factory=WebConfig)


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
