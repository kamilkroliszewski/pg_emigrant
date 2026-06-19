"""Web GUI for pg_emigrant — a thin Flask layer over the existing async
orchestration modules.

The web package never reimplements migration logic; every page and API call
delegates to the same functions the CLI uses (``config``, ``db``, ``monitor``,
``replication``, ``sequence_sync``, ``ddl_detector``, ``bootstrap``).
"""
