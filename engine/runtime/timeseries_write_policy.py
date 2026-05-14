from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict


_TRUTHY_VALUES = {"1", "true", "yes", "on"}


def _env_flag(name: str, default: str) -> bool:
    return str(os.environ.get(name, default)).strip().lower() in _TRUTHY_VALUES


@dataclass(frozen=True)
class PriceRouterWritePlan:
    sqlite_write_prices: bool
    sqlite_write_quotes: bool
    sqlite_write_raw: bool
    async_required: bool


@dataclass(frozen=True)
class TimeseriesWritePolicy:
    sqlite_write_enabled: bool
    sqlite_prices_enabled: bool
    sqlite_quotes_enabled: bool
    sqlite_raw_enabled: bool
    require_async_during_cutover: bool
    sync_provider_aux_sqlite_write_enabled: bool
    block_sync_sqlite_in_live: bool
    allow_sync_sqlite_in_live: bool

    def price_persistence_mode(self, *, async_price_writer_enabled: bool) -> Dict[str, bool]:
        return {
            "sqlite_write_enabled": bool(self.sqlite_write_enabled),
            "sqlite_prices_enabled": bool(self.sqlite_prices_enabled),
            "sqlite_quotes_enabled": bool(self.sqlite_quotes_enabled),
            "sqlite_raw_enabled": bool(self.sqlite_raw_enabled),
            "async_price_writer_enabled": bool(async_price_writer_enabled),
            "block_sync_sqlite_in_live": bool(self.block_sync_sqlite_in_live),
            "allow_sync_sqlite_in_live": bool(self.allow_sync_sqlite_in_live),
        }

    def plan_price_router_writes(
        self,
        *,
        write_prices: bool,
        write_quotes: bool,
        write_raw: bool,
    ) -> PriceRouterWritePlan:
        sqlite_write_prices = bool(write_prices and self.sqlite_prices_enabled)
        sqlite_write_quotes = bool(write_quotes and self.sqlite_quotes_enabled)
        sqlite_write_raw = bool(write_raw and self.sqlite_raw_enabled)
        async_required = bool(
            (write_prices and not sqlite_write_prices)
            or (write_quotes and not sqlite_write_quotes)
        )
        return PriceRouterWritePlan(
            sqlite_write_prices=bool(sqlite_write_prices),
            sqlite_write_quotes=bool(sqlite_write_quotes),
            sqlite_write_raw=bool(sqlite_write_raw),
            async_required=bool(async_required),
        )

    def should_buffer_pipeline_health(self, *, best_effort: bool, min_interval_ms: int) -> bool:
        return bool(best_effort) and int(min_interval_ms) <= 0

    def validate_high_volume_runtime(self, *, engine_mode: str | None = None) -> Dict[str, object]:
        mode = str(engine_mode or os.environ.get("ENGINE_MODE") or "safe").strip().lower()
        sqlite_surfaces = {
            "prices": bool(self.sqlite_prices_enabled),
            "quotes": bool(self.sqlite_quotes_enabled),
            "raw": bool(self.sqlite_raw_enabled),
        }
        active_sqlite_surfaces = sorted(k for k, enabled in sqlite_surfaces.items() if enabled)
        blocked = bool(
            mode == "live"
            and self.block_sync_sqlite_in_live
            and not self.allow_sync_sqlite_in_live
            and active_sqlite_surfaces
        )
        return {
            "ok": not blocked,
            "mode": mode,
            "reason": "ok" if not blocked else "live_high_volume_sqlite_sync_write_blocked",
            "active_sqlite_surfaces": active_sqlite_surfaces,
            "block_sync_sqlite_in_live": bool(self.block_sync_sqlite_in_live),
            "allow_sync_sqlite_in_live": bool(self.allow_sync_sqlite_in_live),
        }


def get_timeseries_write_policy() -> TimeseriesWritePolicy:
    sqlite_write_enabled = _env_flag("PRICE_ROUTER_SQLITE_WRITE_ENABLED", "1")
    return TimeseriesWritePolicy(
        sqlite_write_enabled=bool(sqlite_write_enabled),
        sqlite_prices_enabled=_env_flag(
            "PRICE_ROUTER_SQLITE_PRICES_ENABLED",
            "1" if sqlite_write_enabled else "0",
        ),
        sqlite_quotes_enabled=_env_flag(
            "PRICE_ROUTER_SQLITE_QUOTES_ENABLED",
            "1" if sqlite_write_enabled else "0",
        ),
        sqlite_raw_enabled=_env_flag("PRICE_ROUTER_SQLITE_RAW_ENABLED", "0"),
        require_async_during_cutover=_env_flag(
            "PRICE_ROUTER_REQUIRE_ASYNC_DURING_CUTOVER",
            "1",
        ),
        sync_provider_aux_sqlite_write_enabled=_env_flag(
            "POLL_PRICES_SYNC_PROVIDER_AUX_SQLITE_WRITE_ENABLED",
            "0",
        ),
        block_sync_sqlite_in_live=_env_flag(
            "PRICE_ROUTER_BLOCK_SYNC_SQLITE_IN_LIVE",
            "1",
        ),
        allow_sync_sqlite_in_live=_env_flag(
            "PRICE_ROUTER_ALLOW_SYNC_SQLITE_IN_LIVE",
            "0",
        ),
    )


__all__ = [
    "PriceRouterWritePlan",
    "TimeseriesWritePolicy",
    "get_timeseries_write_policy",
]
