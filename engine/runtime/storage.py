"""Public runtime storage facade backed by Postgres.

Production and production-like modes must use configured Postgres storage.
Local safe mode reports degraded storage when Postgres is unavailable; it does
not silently fall back to SQLite from this facade.
"""

from __future__ import annotations

from engine.runtime.storage_pg import *  # noqa: F401,F403
from engine.runtime.storage_pg import init_db as _pg_init_db


def init_rl_portfolio_tables(con=None) -> None:
    """Compatibility shim; RL tables are owned by schema migrations."""
    del con
    _pg_init_db()


def init_db(schema: str | None = None):
    return _pg_init_db(schema)
