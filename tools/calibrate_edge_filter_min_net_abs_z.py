#!/usr/bin/env python3
"""Calibrate ALERT_MIN_NET_ABS_Z from realized execution-cost fills.

The tool is intentionally read-only. It reads trade_attribution_ledger rows,
converts realized cost bps into z-units using the same horizon-volatility basis
as engine.strategy.edge_filter, and prints JSON diagnostics to stdout.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import os
import sys
import time
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.data.asset_map import asset_class_for_symbol
from engine.runtime.storage import connect
from engine.strategy.edge_filter import PRICE_STEP_S
from engine.strategy.risk import realized_vol_from_prices

DEFAULT_ASSET_CLASS = "EQUITY"
DEFAULT_HORIZON_S = 300
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_MIN_FILLS = 50
DEFAULT_PERCENTILE = 90.0
DEFAULT_MAX_ROWS = 10_000
_NOTIONAL_KEYS = {
    "notional",
    "order_notional",
    "filled_notional",
    "fill_notional",
    "trade_notional",
    "gross_notional",
    "market_value",
}


def _insufficient_result(
    *,
    asset_class: str,
    include_unknown: bool,
    lookback_days: int,
    horizon_s: int,
    min_fills: int,
    percentile: float,
    source_error: str | None = None,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "status": "insufficient_data",
        "asset_class": str(asset_class or "").upper().strip() or "ALL",
        "include_unknown": bool(include_unknown),
        "lookback_days": int(lookback_days),
        "horizon_s": int(horizon_s),
        "price_step_s": int(PRICE_STEP_S),
        "min_fills": int(min_fills),
        "percentile_used": float(percentile),
        "n_rows": 0,
        "n_fills": 0,
        "n_usable": 0,
        "recommended_min_net_abs_z": None,
        "cost_z_percentiles": {"p50": None, "p75": None, "p90": None, "p95": None, "p99": None},
        "cost_bps_percentiles": {"p50": None, "p90": None, "p95": None},
        "skipped": {},
        "asset_classes": {},
    }
    if source_error:
        out["source_error"] = str(source_error)
    return out


def _default_postgres_credentials_unavailable() -> str | None:
    backend = str(os.environ.get("TS_STORAGE_BACKEND") or "").strip().lower()
    if backend in {"sqlite", "sqlite-test", "test"}:
        return None
    if str(os.environ.get("TS_TESTING") or "").strip().lower() in {"1", "true", "yes", "on"}:
        return None
    credential_keys = {
        "TS_PG_DSN",
        "TS_PG_PASSWORD",
        "TIMESCALE_PASSWORD",
        "PGPASSWORD",
        "TS_PG_PASSWORD_FILE",
        "TIMESCALE_PASSWORD_FILE",
        "TS_SECRETS_PROVIDER",
        "TS_DEV_SECRETS_DIR",
        "CREDENTIALS_DIRECTORY",
    }
    if any(str(os.environ.get(key) or "").strip() for key in credential_keys):
        return None
    for key in os.environ:
        if key.startswith("TS_PG_PASSWORD_"):
            return None
        if key.startswith("TS_PG_") and key.endswith("_PASSWORD"):
            return None
    if backend in {"", "postgres", "pg"}:
        return "default Postgres storage selected but no Postgres credential input is configured"
    return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        text = str(value).strip()
        if not text:
            return None
        out = float(text)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return out


def _row_value(row: Any, key: str, index: int) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    if hasattr(row, "keys"):
        try:
            keys = tuple(row.keys())
        except Exception:
            keys = ()
        if key in keys:
            try:
                return row[key]
            except Exception:
                return None
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        try:
            return row[index]
        except Exception:
            return None
    return None


def _json_obj(raw: Any) -> dict[str, Any]:
    if isinstance(raw, Mapping):
        return dict(raw)
    if raw is None:
        return {}
    try:
        parsed = json.loads(str(raw))
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _find_first_number(obj: Any, keys: set[str], depth: int = 0) -> float | None:
    if depth > 6:
        return None
    if isinstance(obj, Mapping):
        for key, value in obj.items():
            if str(key).strip().lower() in keys:
                number = _safe_float(value)
                if number is not None and abs(number) > 0.0:
                    return abs(number)
        for value in obj.values():
            found = _find_first_number(value, keys, depth + 1)
            if found is not None:
                return found
    elif isinstance(obj, Sequence) and not isinstance(obj, (str, bytes, bytearray)):
        for item in obj:
            found = _find_first_number(item, keys, depth + 1)
            if found is not None:
                return found
    return None


def _percentile(values: Sequence[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    p = min(100.0, max(0.0, float(percentile)))
    if len(ordered) == 1:
        return float(ordered[0])
    pos = (len(ordered) - 1) * p / 100.0
    lower = int(math.floor(pos))
    upper = int(math.ceil(pos))
    if lower == upper:
        return float(ordered[lower])
    weight = pos - lower
    return float(ordered[lower] * (1.0 - weight) + ordered[upper] * weight)


def _table_columns(con: Any, table: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall() or []
    except Exception:
        return set()
    out: set[str] = set()
    for row in rows:
        name = _row_value(row, "name", 1)
        if name is not None:
            out.add(str(name))
    return out


def _fetch_ledger_rows(
    con: Any,
    *,
    since_ms: int,
    max_rows: int,
) -> tuple[list[Any], str | None]:
    columns = _table_columns(con, "trade_attribution_ledger")
    if columns and not {"symbol", "ts_ms", "slippage_bps", "fees"}.issubset(columns):
        return [], "trade_attribution_ledger missing required calibration columns"

    signal_expr = "signal_json" if not columns or "signal_json" in columns else "NULL AS signal_json"
    decision_expr = "decision_json" if not columns or "decision_json" in columns else "NULL AS decision_json"
    sql = (
        "SELECT symbol, ts_ms, slippage_bps, fees, "
        f"{signal_expr}, {decision_expr} "
        "FROM trade_attribution_ledger "
        "WHERE ts_ms >= ? AND symbol IS NOT NULL "
        "ORDER BY ts_ms ASC "
        "LIMIT ?"
    )
    try:
        return list(con.execute(sql, (int(since_ms), int(max_rows))).fetchall() or []), None
    except Exception as exc:
        return [], f"{type(exc).__name__}: {exc}"


def _class_for_symbol(symbol: str) -> str:
    try:
        return str(asset_class_for_symbol(symbol) or "UNKNOWN").upper().strip()
    except Exception:
        return "UNKNOWN"


def _asset_class_allowed(
    symbol: str,
    *,
    asset_class: str,
    include_unknown: bool,
) -> tuple[bool, str]:
    requested = str(asset_class or "").upper().strip()
    actual = _class_for_symbol(symbol)
    if requested in {"", "ALL", "*", "ANY"}:
        return True, actual
    if actual == requested:
        return True, actual
    if include_unknown and actual == "UNKNOWN":
        return True, actual
    return False, actual


def _fee_bps(
    fees: Any,
    payloads: Iterable[Mapping[str, Any]],
    *,
    fallback_notional_usd: float | None,
) -> tuple[float | None, str | None]:
    fee_value = _safe_float(fees)
    if fee_value is None or abs(fee_value) <= 0.0:
        return 0.0, None

    notional = None
    if fallback_notional_usd is not None and fallback_notional_usd > 0.0:
        notional = float(fallback_notional_usd)
    else:
        for payload in payloads:
            notional = _find_first_number(payload, _NOTIONAL_KEYS)
            if notional is not None:
                break

    if notional is None or notional <= 0.0:
        return None, "fees_without_notional"
    return float(abs(fee_value) / float(notional) * 1e4), None


def _cost_z_for_row(
    con: Any,
    row: Any,
    *,
    horizon_s: int,
    asset_class: str,
    include_unknown: bool,
    fallback_notional_usd: float | None,
) -> tuple[float | None, dict[str, Any]]:
    symbol = str(_row_value(row, "symbol", 0) or "").strip().upper()
    if not symbol:
        return None, {"skip": "missing_symbol"}

    allowed, symbol_asset_class = _asset_class_allowed(
        symbol,
        asset_class=asset_class,
        include_unknown=include_unknown,
    )
    if not allowed:
        return None, {"skip": "out_of_scope", "asset_class": symbol_asset_class}

    slippage_bps = _safe_float(_row_value(row, "slippage_bps", 2))
    if slippage_bps is None:
        return None, {"skip": "missing_slippage_bps", "asset_class": symbol_asset_class}

    signal_json = _json_obj(_row_value(row, "signal_json", 4))
    decision_json = _json_obj(_row_value(row, "decision_json", 5))
    fees_bps, fee_skip = _fee_bps(
        _row_value(row, "fees", 3),
        (signal_json, decision_json),
        fallback_notional_usd=fallback_notional_usd,
    )
    if fee_skip:
        return None, {"skip": fee_skip, "asset_class": symbol_asset_class}

    try:
        vol_step = realized_vol_from_prices(con, symbol)
    except Exception:
        vol_step = None
    vol_value = _safe_float(vol_step)
    if vol_value is None or vol_value <= 0.0:
        return None, {"skip": "missing_vol", "asset_class": symbol_asset_class}

    steps = max(1.0, float(horizon_s) / max(1.0, float(PRICE_STEP_S)))
    vol_horizon = float(vol_value) * math.sqrt(steps)
    if vol_horizon <= 1e-12:
        return None, {"skip": "missing_vol", "asset_class": symbol_asset_class}

    cost_bps = abs(float(slippage_bps)) + float(fees_bps or 0.0)
    return float((cost_bps / 1e4) / vol_horizon), {
        "skip": None,
        "asset_class": symbol_asset_class,
        "cost_bps": float(cost_bps),
    }


def calibrate(
    *,
    con: Any | None = None,
    rows: Iterable[Any] | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    horizon_s: int = DEFAULT_HORIZON_S,
    asset_class: str = DEFAULT_ASSET_CLASS,
    include_unknown: bool = False,
    min_fills: int = DEFAULT_MIN_FILLS,
    percentile: float = DEFAULT_PERCENTILE,
    max_rows: int = DEFAULT_MAX_ROWS,
    fee_notional_usd: float | None = None,
    now_ms: int | None = None,
) -> dict[str, Any]:
    opened = False
    source_error = None
    if con is None:
        preflight_error = _default_postgres_credentials_unavailable()
        if preflight_error:
            return _insufficient_result(
                asset_class=asset_class,
                include_unknown=include_unknown,
                lookback_days=lookback_days,
                horizon_s=horizon_s,
                min_fills=min_fills,
                percentile=percentile,
                source_error=preflight_error,
            )
        try:
            with contextlib.redirect_stdout(sys.stderr):
                con = connect(readonly=True)
            opened = True
        except Exception as exc:
            return _insufficient_result(
                asset_class=asset_class,
                include_unknown=include_unknown,
                lookback_days=lookback_days,
                horizon_s=horizon_s,
                min_fills=min_fills,
                percentile=percentile,
                source_error=f"{type(exc).__name__}: {exc}",
            )

    try:
        effective_now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
        since_ms = int(effective_now_ms - max(0, int(lookback_days)) * 86_400_000)
        if rows is None:
            row_list, source_error = _fetch_ledger_rows(con, since_ms=since_ms, max_rows=max_rows)
        else:
            row_list = list(rows)

        cost_zs: list[float] = []
        cost_bps_values: list[float] = []
        skipped: Counter[str] = Counter()
        asset_classes: Counter[str] = Counter()
        scoped_fills = 0

        for row in row_list:
            cost_z, meta = _cost_z_for_row(
                con,
                row,
                horizon_s=int(horizon_s),
                asset_class=str(asset_class),
                include_unknown=bool(include_unknown),
                fallback_notional_usd=fee_notional_usd,
            )
            if meta.get("asset_class"):
                asset_classes[str(meta["asset_class"])] += 1
            if meta.get("skip") == "out_of_scope":
                skipped["out_of_scope"] += 1
                continue
            scoped_fills += 1
            if meta.get("skip"):
                skipped[str(meta["skip"])] += 1
                continue
            if cost_z is None:
                skipped["invalid_cost_z"] += 1
                continue
            cost_zs.append(float(cost_z))
            cost_bps_values.append(float(meta.get("cost_bps") or 0.0))

        percentiles = {
            "p50": _percentile(cost_zs, 50),
            "p75": _percentile(cost_zs, 75),
            "p90": _percentile(cost_zs, 90),
            "p95": _percentile(cost_zs, 95),
            "p99": _percentile(cost_zs, 99),
        }
        cost_bps_percentiles = {
            "p50": _percentile(cost_bps_values, 50),
            "p90": _percentile(cost_bps_values, 90),
            "p95": _percentile(cost_bps_values, 95),
        }
        recommendation = _percentile(cost_zs, float(percentile))
        status = "ok" if len(cost_zs) >= int(min_fills) else "insufficient_data"
        if status != "ok":
            recommendation = None

        out: dict[str, Any] = {
            "status": status,
            "asset_class": str(asset_class or "").upper().strip() or "ALL",
            "include_unknown": bool(include_unknown),
            "lookback_days": int(lookback_days),
            "horizon_s": int(horizon_s),
            "price_step_s": int(PRICE_STEP_S),
            "min_fills": int(min_fills),
            "percentile_used": float(percentile),
            "n_rows": int(len(row_list)),
            "n_fills": int(scoped_fills),
            "n_usable": int(len(cost_zs)),
            "recommended_min_net_abs_z": recommendation,
            "cost_z_percentiles": percentiles,
            "cost_bps_percentiles": cost_bps_percentiles,
            "skipped": dict(sorted(skipped.items())),
            "asset_classes": dict(sorted(asset_classes.items())),
        }
        if source_error:
            out["source_error"] = source_error
        return out
    finally:
        if opened:
            con.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--horizon-s", type=int, default=DEFAULT_HORIZON_S)
    parser.add_argument("--asset-class", default=DEFAULT_ASSET_CLASS)
    parser.add_argument("--include-unknown", action="store_true")
    parser.add_argument("--min-fills", type=int, default=DEFAULT_MIN_FILLS)
    parser.add_argument("--percentile", type=float, default=DEFAULT_PERCENTILE)
    parser.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS)
    parser.add_argument(
        "--fee-notional-usd",
        type=float,
        default=None,
        help="Fallback notional for converting monetary fees to bps when ledger JSON has no notional.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    payload = calibrate(
        lookback_days=int(args.lookback_days),
        horizon_s=int(args.horizon_s),
        asset_class=str(args.asset_class),
        include_unknown=bool(args.include_unknown),
        min_fills=int(args.min_fills),
        percentile=float(args.percentile),
        max_rows=int(args.max_rows),
        fee_notional_usd=args.fee_notional_usd,
    )
    print(json.dumps(payload, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
