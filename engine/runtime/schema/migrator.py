"""Numbered Python migration runner for runtime Postgres storage."""

from __future__ import annotations

import importlib
import logging
import pkgutil
import threading
from types import ModuleType
from typing import Iterable


MIGRATION_LOCK_KEY = 0x54535F534348454D
_APPLY_LOCK = threading.Lock()
LOG = logging.getLogger(__name__)


def _migration_modules(package: str) -> list[ModuleType]:
    pkg = importlib.import_module(package)
    modules: list[ModuleType] = []
    for info in pkgutil.iter_modules(pkg.__path__, prefix=f"{package}."):
        leaf = info.name.rsplit(".", 1)[-1]
        if not leaf[:4].isdigit():
            continue
        module = importlib.import_module(info.name)
        modules.append(module)
    return sorted(modules, key=lambda module: int(getattr(module, "id")))


def _ensure_schema_table(conn) -> None:
    from engine.runtime.storage_pool import quote_ident, schema_name

    schema = quote_ident(schema_name())
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f"SET search_path = {schema}, public")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
          id INTEGER PRIMARY KEY,
          description TEXT,
          applied_at TIMESTAMPTZ DEFAULT now()
        )
        """
    )


def _acquire_migration_lock(conn) -> None:
    conn.execute("SELECT pg_advisory_xact_lock(?)", (int(MIGRATION_LOCK_KEY),))


def _applied_ids(conn) -> set[int]:
    rows = conn.execute("SELECT id FROM schema_migrations").fetchall()
    return {int(row[0]) for row in rows or []}


def apply_migrations(
    *,
    package: str = "engine.runtime.schema.migrations",
    migrations: Iterable[ModuleType] | None = None,
) -> list[int]:
    from engine.runtime import storage_pg

    with _APPLY_LOCK:
        modules = list(migrations) if migrations is not None else _migration_modules(package)
        applied: list[int] = []

        with storage_pg.connect_rw_direct() as conn:
            with conn.transaction():
                _acquire_migration_lock(conn)
                _ensure_schema_table(conn)

        for module in modules:
            migration_id = int(getattr(module, "id"))
            description = str(getattr(module, "description", ""))
            with storage_pg.connect_rw_direct() as conn:
                with conn.transaction():
                    _acquire_migration_lock(conn)
                    _ensure_schema_table(conn)
                    if migration_id in _applied_ids(conn):
                        continue
                    up = getattr(module, "up")
                    up(conn)
                    conn.execute(
                        """
                        INSERT INTO schema_migrations(id, description)
                        VALUES (?, ?)
                        ON CONFLICT(id) DO NOTHING
                        """,
                        (int(migration_id), str(description)),
                    )
                    applied.append(int(migration_id))
        return applied


def main() -> None:
    applied = apply_migrations()
    LOG.info("schema_migrations_applied applied=%s", applied)


if __name__ == "__main__":
    main()
