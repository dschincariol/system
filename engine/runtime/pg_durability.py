"""Postgres durability policy for refetchable ingestion writes."""

from __future__ import annotations

import os
from typing import Any


REFETCHABLE_PG_DURABILITY_TIER_ENV = "TRADING_REFETCHABLE_PG_DURABILITY_TIER"
REFETCHABLE_PG_DURABILITY_DEFAULT = "default"
REFETCHABLE_PG_DURABILITY_RELAXED = "relaxed"
SET_LOCAL_SYNCHRONOUS_COMMIT_OFF_SQL = "SET LOCAL synchronous_commit = off"

_RUNTIME_REFETCHABLE_TELEMETRY_WRITES: frozenset[tuple[str, str]] = frozenset(
    {
        ("price_quotes_raw", "flush_price_quotes_raw_buffer"),
        ("price_provider_health", "flush_price_provider_health_buffer"),
        ("weather_provider_health", "flush_weather_provider_health_buffer"),
        ("ingestion_pipeline_health", "flush_ingestion_pipeline_health_buffer"),
        ("ingest_slippage", "flush_ingest_slippage_buffer"),
    }
)

_TIMESCALE_REFETCHABLE_PRICE_TELEMETRY_TABLES: frozenset[str] = frozenset(
    {
        "data_source_logs",
        "ingestion_pipeline_health",
        "price_data",
        "price_provider_health",
        "runtime_metrics",
        "weather_provider_health",
    }
)

_POSTGRES_PRICE_STORAGE_SCOPE = "storage_pg_prices.write_batch"


def refetchable_pg_durability_tier() -> str:
    """Return the configured durability tier for approved refetchable writes."""
    raw = str(os.environ.get(REFETCHABLE_PG_DURABILITY_TIER_ENV) or "").strip().lower()
    if raw == REFETCHABLE_PG_DURABILITY_RELAXED:
        return REFETCHABLE_PG_DURABILITY_RELAXED
    return REFETCHABLE_PG_DURABILITY_DEFAULT


def relaxed_refetchable_pg_durability_enabled() -> bool:
    return refetchable_pg_durability_tier() == REFETCHABLE_PG_DURABILITY_RELAXED


def refetchable_pg_durability_snapshot() -> dict[str, Any]:
    tier = refetchable_pg_durability_tier()
    return {
        "env": REFETCHABLE_PG_DURABILITY_TIER_ENV,
        "tier": tier,
        "relaxed": tier == REFETCHABLE_PG_DURABILITY_RELAXED,
        "approved_runtime_telemetry_writes": sorted(
            f"{table}:{operation}"
            for table, operation in _RUNTIME_REFETCHABLE_TELEMETRY_WRITES
        ),
        "approved_timescale_tables": sorted(_TIMESCALE_REFETCHABLE_PRICE_TELEMETRY_TABLES),
        "approved_price_storage_scope": _POSTGRES_PRICE_STORAGE_SCOPE,
    }


def _normalize(value: Any) -> str:
    return str(value or "").strip().lower()


def is_runtime_refetchable_ingestion_telemetry_write(
    *,
    table: str | None,
    operation: str | None,
) -> bool:
    return (_normalize(table), _normalize(operation)) in _RUNTIME_REFETCHABLE_TELEMETRY_WRITES


def validate_runtime_refetchable_ingestion_telemetry_write(
    *,
    table: str | None,
    operation: str | None,
) -> None:
    if is_runtime_refetchable_ingestion_telemetry_write(table=table, operation=operation):
        return
    raise ValueError(
        "unapproved_refetchable_ingestion_telemetry_write:"
        f"table={table or '<unset>'}:operation={operation or '<unset>'}"
    )


def is_timescale_refetchable_price_telemetry_table(table: str | None) -> bool:
    return _normalize(table) in _TIMESCALE_REFETCHABLE_PRICE_TELEMETRY_TABLES


def should_relax_runtime_refetchable_ingestion_telemetry_write(
    *,
    table: str | None,
    operation: str | None,
) -> bool:
    return bool(
        relaxed_refetchable_pg_durability_enabled()
        and is_runtime_refetchable_ingestion_telemetry_write(table=table, operation=operation)
    )


def should_relax_timescale_price_telemetry_write(*, table: str | None) -> bool:
    return bool(
        relaxed_refetchable_pg_durability_enabled()
        and is_timescale_refetchable_price_telemetry_table(table)
    )


def should_relax_postgres_price_storage_write(*, scope: str | None) -> bool:
    return bool(
        relaxed_refetchable_pg_durability_enabled()
        and _normalize(scope) == _POSTGRES_PRICE_STORAGE_SCOPE
    )


def maybe_apply_sync_refetchable_pg_durability(
    executor: Any,
    *,
    scope: str,
    table: str | None = None,
    operation: str | None = None,
) -> bool:
    """Apply SET LOCAL for an approved synchronous psycopg-style transaction."""
    scope_name = _normalize(scope)
    should_apply = False
    if scope_name == "runtime_refetchable_ingestion_telemetry":
        should_apply = should_relax_runtime_refetchable_ingestion_telemetry_write(
            table=table,
            operation=operation,
        )
    elif scope_name == _POSTGRES_PRICE_STORAGE_SCOPE:
        should_apply = should_relax_postgres_price_storage_write(scope=scope_name)
    else:
        should_apply = False
    if not should_apply:
        return False
    executor.execute(SET_LOCAL_SYNCHRONOUS_COMMIT_OFF_SQL)
    return True


async def maybe_apply_async_refetchable_pg_durability(
    executor: Any,
    *,
    scope: str,
    table: str | None = None,
) -> bool:
    """Apply SET LOCAL for an approved asyncpg-style transaction."""
    if _normalize(scope) != "timescale_price_telemetry":
        return False
    if not should_relax_timescale_price_telemetry_write(table=table):
        return False
    await executor.execute(SET_LOCAL_SYNCHRONOUS_COMMIT_OFF_SQL)
    return True
