"""Postgres durability policy for refetchable ingestion writes."""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from typing import Any


REFETCHABLE_PG_DURABILITY_TIER_ENV = "TRADING_REFETCHABLE_PG_DURABILITY_TIER"
REFETCHABLE_PG_DURABILITY_DEFAULT = "default"
REFETCHABLE_PG_DURABILITY_RELAXED = "relaxed"
SUPPORTED_REFETCHABLE_PG_DURABILITY_TIERS: frozenset[str] = frozenset(
    {
        REFETCHABLE_PG_DURABILITY_DEFAULT,
        REFETCHABLE_PG_DURABILITY_RELAXED,
    }
)
SET_LOCAL_SYNCHRONOUS_COMMIT_OFF_SQL = "SET LOCAL synchronous_commit = off"

_RUNTIME_REFETCHABLE_SCOPE = "runtime_refetchable_ingestion_telemetry"
_TIMESCALE_REFETCHABLE_SCOPE = "timescale_price_telemetry"
_POSTGRES_PRICE_STORAGE_SCOPE = "storage_pg_prices.write_batch"

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

_POSTGRES_PRICE_STORAGE_TABLES: frozenset[str] = frozenset(
    {
        "price_ticks",
        "price_quotes",
        "price_quotes_raw",
    }
)

_PROTECTED_REFETCHABLE_PG_DURABILITY_TABLES: frozenset[str] = frozenset(
    {
        "broker_order_state",
        "capital_preservation_audit",
        "equity_history",
        "event_log",
        "execution_ledger",
        "execution_mode",
        "execution_mode_audit",
        "execution_orders",
        "experiment_ledger",
        "kill_switch_audit",
        "kill_switch_state",
        "model_predictions",
        "model_registry",
        "portfolio_orders",
        "portfolio_state",
        "predictions",
        "risk_events",
        "risk_state",
        "shadow_capital_scores",
        "trade_attribution_ledger",
        "trade_outcomes",
    }
)


class RefetchablePgDurabilityConfigError(ValueError):
    """Raised when relaxed-durability configuration is malformed."""


class RefetchablePgDurabilityScopeError(ValueError):
    """Raised when an unapproved write surface requests relaxed durability."""


def _normalize(value: Any) -> str:
    return str(value or "").strip().lower()


def parse_refetchable_pg_durability_tier(
    env: Mapping[str, str] | None = None,
) -> str:
    """Parse the configured durability tier.

    Empty/unset values intentionally mean ``default``. Any other unknown value
    fails closed so an operator typo cannot silently disable or enable the tier.
    """
    source = os.environ if env is None else env
    raw = str(source.get(REFETCHABLE_PG_DURABILITY_TIER_ENV) or "").strip().lower()
    if not raw:
        return REFETCHABLE_PG_DURABILITY_DEFAULT
    if raw in SUPPORTED_REFETCHABLE_PG_DURABILITY_TIERS:
        return raw
    raise RefetchablePgDurabilityConfigError(
        f"invalid {REFETCHABLE_PG_DURABILITY_TIER_ENV}={raw!r}; "
        "expected one of: "
        + ",".join(sorted(SUPPORTED_REFETCHABLE_PG_DURABILITY_TIERS))
    )


def refetchable_pg_durability_tier() -> str:
    """Return the configured durability tier for approved refetchable writes."""
    return parse_refetchable_pg_durability_tier(os.environ)


def relaxed_refetchable_pg_durability_enabled() -> bool:
    return refetchable_pg_durability_tier() == REFETCHABLE_PG_DURABILITY_RELAXED


def refetchable_pg_durability_snapshot() -> dict[str, Any]:
    tier = refetchable_pg_durability_tier()
    return {
        "env": REFETCHABLE_PG_DURABILITY_TIER_ENV,
        "raw": str(os.environ.get(REFETCHABLE_PG_DURABILITY_TIER_ENV) or ""),
        "tier": tier,
        "relaxed": tier == REFETCHABLE_PG_DURABILITY_RELAXED,
        "supported_tiers": sorted(SUPPORTED_REFETCHABLE_PG_DURABILITY_TIERS),
        "approved_scopes": [
            _POSTGRES_PRICE_STORAGE_SCOPE,
            _RUNTIME_REFETCHABLE_SCOPE,
            _TIMESCALE_REFETCHABLE_SCOPE,
        ],
        "approved_runtime_telemetry_writes": sorted(
            f"{table}:{operation}"
            for table, operation in _RUNTIME_REFETCHABLE_TELEMETRY_WRITES
        ),
        "approved_timescale_tables": sorted(_TIMESCALE_REFETCHABLE_PRICE_TELEMETRY_TABLES),
        "approved_price_storage_scope": _POSTGRES_PRICE_STORAGE_SCOPE,
        "approved_price_storage_tables": sorted(_POSTGRES_PRICE_STORAGE_TABLES),
        "protected_tables": sorted(_PROTECTED_REFETCHABLE_PG_DURABILITY_TABLES),
    }


def protected_refetchable_pg_durability_tables() -> frozenset[str]:
    return _PROTECTED_REFETCHABLE_PG_DURABILITY_TABLES


def _reject_if_protected_table(table: str | None) -> None:
    normalized = _normalize(table)
    if normalized in _PROTECTED_REFETCHABLE_PG_DURABILITY_TABLES:
        raise RefetchablePgDurabilityScopeError(
            "protected_refetchable_pg_durability_table:"
            f"table={table or '<unset>'}"
        )


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
    _reject_if_protected_table(table)
    raise RefetchablePgDurabilityScopeError(
        "unapproved_refetchable_ingestion_telemetry_write:"
        f"table={table or '<unset>'}:operation={operation or '<unset>'}"
    )


def is_timescale_refetchable_price_telemetry_table(table: str | None) -> bool:
    return _normalize(table) in _TIMESCALE_REFETCHABLE_PRICE_TELEMETRY_TABLES


def validate_timescale_refetchable_price_telemetry_table(table: str | None) -> None:
    if is_timescale_refetchable_price_telemetry_table(table):
        return
    _reject_if_protected_table(table)
    raise RefetchablePgDurabilityScopeError(
        "unapproved_refetchable_timescale_price_telemetry_write:"
        f"table={table or '<unset>'}"
    )


def is_postgres_price_storage_table(table: str | None) -> bool:
    return _normalize(table) in _POSTGRES_PRICE_STORAGE_TABLES


def _normalize_table_set(tables: Iterable[str | None] | None) -> frozenset[str]:
    return frozenset(
        table
        for table in (_normalize(value) for value in (tables or ()))
        if table
    )


def validate_postgres_price_storage_tables(tables: Iterable[str | None] | None) -> None:
    table_set = _normalize_table_set(tables)
    if table_set and table_set.issubset(_POSTGRES_PRICE_STORAGE_TABLES):
        return
    for table in sorted(table_set):
        _reject_if_protected_table(table)
    raise RefetchablePgDurabilityScopeError(
        "unapproved_refetchable_postgres_price_write:"
        f"tables={','.join(sorted(table_set)) if table_set else '<unset>'}"
    )


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


def should_relax_postgres_price_storage_write(
    *,
    scope: str | None,
    target_tables: Iterable[str | None] | None,
) -> bool:
    if _normalize(scope) == _POSTGRES_PRICE_STORAGE_SCOPE:
        validate_postgres_price_storage_tables(target_tables)
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
    target_tables: Iterable[str | None] | None = None,
) -> bool:
    """Apply SET LOCAL for an approved synchronous psycopg-style transaction."""
    scope_name = _normalize(scope)
    should_apply = False
    if scope_name == _RUNTIME_REFETCHABLE_SCOPE:
        if not relaxed_refetchable_pg_durability_enabled():
            return False
        validate_runtime_refetchable_ingestion_telemetry_write(
            table=table,
            operation=operation,
        )
        should_apply = True
    elif scope_name == _POSTGRES_PRICE_STORAGE_SCOPE:
        should_apply = should_relax_postgres_price_storage_write(
            scope=scope_name,
            target_tables=target_tables,
        )
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
    if _normalize(scope) != _TIMESCALE_REFETCHABLE_SCOPE:
        return False
    if not relaxed_refetchable_pg_durability_enabled():
        return False
    validate_timescale_refetchable_price_telemetry_table(table)
    await executor.execute(SET_LOCAL_SYNCHRONOUS_COMMIT_OFF_SQL)
    return True
