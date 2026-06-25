"""Short-equity borrow cost helpers.

The model is pure and deterministic: it reads only environment overrides and
function inputs. Borrow cost is enabled by default so short-equity labels and
CPCV intervals pay a realistic financing floor in default deployments. With
`EQUITY_BORROW_COST_ENABLED=0` callers must leave legacy numbers unchanged.
"""

from __future__ import annotations

import json
import logging
import math
import os
from typing import Mapping

from engine.runtime.failure_diagnostics import log_failure

LOG = logging.getLogger("engine.strategy.borrow_cost_model")

# Annualized bps floors by borrow difficulty. GC is a realistic large-cap
# general-collateral floor; hard/special buckets model scarce borrow inventory.
_DEFAULT_BORROW_BPS_PER_YEAR = {
    "GC": 30.0,
    "MODERATE": 75.0,
    "HARD": 300.0,
    "SPECIAL": 1000.0,
}

# Upper bounds for days-to-cover buckets. Values are exclusive.
_DEFAULT_DTC_THRESHOLDS = {
    "GC": 2.0,
    "MODERATE": 5.0,
    "HARD": 10.0,
}

_EQUITY_ASSET_CLASSES = {"EQUITY", "US_EQUITY"}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name))
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _json_object_env(name: str) -> Mapping[str, object]:
    raw = str(os.environ.get(str(name), "") or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception as e:
        log_failure(
            LOG,
            event="borrow_cost_json_env_parse_failed",
            code="BORROW_COST_JSON_ENV_PARSE_FAILED",
            message=str(name),
            error=e,
            level=logging.WARNING,
            component="engine.strategy.borrow_cost_model",
            extra={"env_name": str(name)},
            persist=False,
        )
        return {}
    return parsed if isinstance(parsed, dict) else {}


def borrow_cost_enabled() -> bool:
    return _env_bool("EQUITY_BORROW_COST_ENABLED", True)


def cpcv_borrow_cost_enabled() -> bool:
    raw = os.environ.get("CPCV_BORROW_COST_ENABLED")
    if raw is None or str(raw).strip() == "":
        return borrow_cost_enabled()
    return _env_bool("CPCV_BORROW_COST_ENABLED", False)


def _borrow_bps_table() -> dict[str, float]:
    table = dict(_DEFAULT_BORROW_BPS_PER_YEAR)
    for key, value in _json_object_env("EQUITY_BORROW_BPS_PER_YEAR_JSON").items():
        bucket = str(key or "").upper().strip()
        if not bucket:
            continue
        table[bucket] = max(0.0, _safe_float(value, table.get(bucket, 0.0)))
    return table


def _dtc_thresholds() -> dict[str, float]:
    thresholds = dict(_DEFAULT_DTC_THRESHOLDS)
    for key, value in _json_object_env("EQUITY_BORROW_DTC_THRESHOLDS_JSON").items():
        bucket = str(key or "").upper().strip()
        if bucket not in thresholds:
            continue
        thresholds[bucket] = max(0.0, _safe_float(value, thresholds[bucket]))
    return thresholds


def _default_bucket() -> str:
    bucket = str(os.environ.get("EQUITY_BORROW_DEFAULT_BUCKET", "GC") or "GC").upper().strip()
    return bucket if bucket in _borrow_bps_table() else "GC"


def borrow_difficulty_bucket(
    *,
    days_to_cover: float | None = None,
    short_interest_shares: float | None = None,
    float_shares: float | None = None,
) -> str:
    dtc = _safe_float(days_to_cover, -1.0)
    if dtc >= 0.0:
        thresholds = _dtc_thresholds()
        if dtc < float(thresholds.get("GC", 2.0)):
            return "GC"
        if dtc < float(thresholds.get("MODERATE", 5.0)):
            return "MODERATE"
        if dtc < float(thresholds.get("HARD", 10.0)):
            return "HARD"
        return "SPECIAL"

    short_interest = _safe_float(short_interest_shares, 0.0)
    float_base = _safe_float(float_shares, 0.0)
    if short_interest > 0.0 and float_base > 0.0:
        ratio = short_interest / float_base
        if ratio >= 0.25:
            return "SPECIAL"
        if ratio >= 0.15:
            return "HARD"
        if ratio >= 0.05:
            return "MODERATE"
        return "GC"

    return _default_bucket()


def annual_borrow_bps(symbol: str | None = None, *, bucket: str | None = None, **difficulty: object) -> float:
    del symbol
    resolved_bucket = str(bucket or "").upper().strip()
    if not resolved_bucket:
        resolved_bucket = borrow_difficulty_bucket(
            days_to_cover=difficulty.get("days_to_cover"),  # type: ignore[arg-type]
            short_interest_shares=difficulty.get("short_interest_shares"),  # type: ignore[arg-type]
            float_shares=difficulty.get("float_shares"),  # type: ignore[arg-type]
        )
    table = _borrow_bps_table()
    return max(0.0, float(table.get(resolved_bucket, table.get(_default_bucket(), table["GC"]))))


def borrow_bps_for_period(symbol: str | None = None, *, holding_days: float, **difficulty: object) -> float:
    days = max(0.0, _safe_float(holding_days, 0.0))
    if days <= 0.0:
        return 0.0
    annual_bps = annual_borrow_bps(symbol, **difficulty)
    return max(0.0, float(annual_bps) * float(days) / 365.0)


def is_borrowable_short_equity(*, side: int | float, asset_class: str | None) -> bool:
    side_value = _safe_float(side, 0.0)
    cls = str(asset_class or "").upper().strip()
    return side_value < 0.0 and cls in _EQUITY_ASSET_CLASSES
