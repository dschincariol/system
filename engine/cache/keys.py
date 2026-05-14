"""Centralized Redis keyspace builder."""

from __future__ import annotations

PREFIX = "trading"

HOT_PATH_TABLES = frozenset(
    {
        "kill_switch_state",
        "execution_mode",
        "execution_health_state",
        "broker_order_state",
        "position_reconcile_baseline",
        "strategy_allocations",
        "model_feature_snapshots",
    }
)


def _part(value: object) -> str:
    text = str(value if value is not None else "").strip()
    if not text:
        raise ValueError("cache_key_part_required")
    if any(ch.isspace() for ch in text):
        text = "_".join(text.split())
    return text


def table_key(table: str, *parts: object) -> str:
    table_name = _part(table)
    if table_name not in HOT_PATH_TABLES:
        raise ValueError(f"unsupported_cache_table:{table_name}")
    key_parts = [_part(part) for part in parts]
    if not key_parts:
        raise ValueError("cache_key_id_required")
    return ":".join([PREFIX, table_name, *key_parts])


def kill_switch(scope: object = "snapshot", key: object | None = None) -> str:
    if key is None:
        return table_key("kill_switch_state", scope)
    return table_key("kill_switch_state", scope, key)


def execution_mode() -> str:
    return table_key("execution_mode", "singleton")


def execution_health() -> str:
    return table_key("execution_health_state", "latest")


def broker_order_state(identifier: object) -> str:
    return table_key("broker_order_state", identifier)


def position_baseline(broker: object) -> str:
    return table_key("position_reconcile_baseline", broker)


def strategy_allocations(window_days: object = 0) -> str:
    return table_key("strategy_allocations", window_days)


def feature_snapshot(symbol: object, feature_group: object) -> str:
    return table_key(
        "model_feature_snapshots",
        str(symbol or "").upper().strip(),
        feature_group,
    )
