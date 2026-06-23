"""Align existing Timescale hypertable chunk intervals with table policy."""

from __future__ import annotations

import importlib
import os

from engine.runtime.schema.table_classification import Hypertable, TABLE_CLASS

id = 65
description = "policy-driven hypertable chunk intervals"


def _env_truthy(name: str) -> bool:
    return str(os.environ.get(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def up(conn) -> None:
    if _env_truthy("TRADING_UNIT_TEST_SCHEMA_FAST"):
        return
    hypertables = importlib.import_module("engine.runtime.schema.migrations.0002_hypertables")
    conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
    hypertables._create_integer_now_func(conn)
    for table_name, spec in sorted(TABLE_CLASS.items()):
        if isinstance(spec, Hypertable):
            hypertables._create_hypertable(conn, table_name, spec)
