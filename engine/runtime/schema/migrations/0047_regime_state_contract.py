"""Repair regime_state symbol/time persistence contract."""

from __future__ import annotations

from importlib import import_module

id = 47
description = "regime state symbol time contract"


def up(conn) -> None:
    migration_0015 = import_module("engine.runtime.schema.migrations.0015_inference_runtime_tables")
    migration_0015._ensure_regime_state_schema(conn)
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_regime_state_symbol_time_desc
          ON regime_state(symbol, time DESC)
        """
    )
