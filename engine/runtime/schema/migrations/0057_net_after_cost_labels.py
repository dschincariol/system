"""Add durable net-after-cost label artifacts."""

from __future__ import annotations

import importlib
import os

from engine.runtime.schema.table_classification import TABLE_CLASS, Hypertable

id = 57
description = "net-after-cost labels for training evaluation and promotion"


def _env_truthy(name: str) -> bool:
    return str(os.environ.get(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def _looks_like_sqlite(conn) -> bool:
    module_name = str(getattr(conn, "__class__", type(conn)).__module__ or "").lower()
    class_name = str(getattr(conn, "__class__", type(conn)).__name__ or "").lower()
    return "sqlite" in module_name or "sqlite" in class_name


def up(conn) -> None:
    from engine.strategy.net_after_cost_labels import ensure_net_after_cost_labels_schema

    ensure_net_after_cost_labels_schema(conn)
    if _env_truthy("TRADING_UNIT_TEST_SCHEMA_FAST") or _looks_like_sqlite(conn):
        return
    spec = TABLE_CLASS.get("net_after_cost_labels")
    if not isinstance(spec, Hypertable):
        return
    hypertables = importlib.import_module("engine.runtime.schema.migrations.0002_hypertables")
    indexes = importlib.import_module("engine.runtime.schema.migrations.0003_indexes")
    hypertables._create_integer_now_func(conn)
    hypertables._create_hypertable(conn, "net_after_cost_labels", spec)
    hypertables._enable_compression(conn, "net_after_cost_labels", spec)
    hypertables._enable_retention(conn, "net_after_cost_labels", spec)
    indexes._create_hypertable_indexes(conn, "net_after_cost_labels", spec)
