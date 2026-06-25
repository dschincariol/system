"""Read-only options-chain data-quality checks.

``compute_options_data_quality`` returns a JSON-serializable report with this
stable top-level shape:

``ok``
    True only when the report is available, coverage meets the configured
    threshold, expected fields are sufficiently complete, and IV sanity checks
    pass.
``available``
    True when at least one configured-underlying has a fresh chain snapshot.
    Empty universes or no fresh chains are unavailable, not green.
``degraded``
    True when coverage, field completeness, or IV sanity falls below the
    env-tunable thresholds.
``coverage_fraction``
    Fresh-underlying count divided by the configured options universe.
``providers``
    Per-provider row counts and completeness fractions. Rows sourced from the
    legacy ``options_chain`` table explicitly report bid/ask and greeks
    completeness as ``0.0`` because those columns do not exist there.
``iv_sanity``
    Counts for negative IV, IV above ``OPTIONS_IV_SANITY_MAX`` (an operator
    sanity ceiling, not a market fact), and zero-greeks rows where Polygon
    greeks are expected.
``freshness``
    Per-underlying state and latest-chain timestamps used to compute coverage.
``thresholds``
    The effective stale-window, coverage, completeness, and IV sanity settings.

The module never writes to options chain/state tables. Metric and degradation
event helpers are best-effort wrappers around existing runtime observability
paths and are intentionally separate from the pure compute function.
"""

from __future__ import annotations

import logging
import math
import os
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("data.options_data_quality")
_WARNED_NONFATAL_KEYS: set[str] = set()
_LAST_DQ_DEGRADATION_EVENT_TS_MS = 0

_DEFAULT_SNAPSHOT_STALE_MS = 15 * 60 * 1000
_DEFAULT_MIN_COVERAGE = 0.80
_DEFAULT_MIN_COMPLETENESS = 0.50
_DEFAULT_IV_SANITY_MAX = 5.0
_DEFAULT_EVENT_THROTTLE_MS = 15 * 60 * 1000

_CHAIN_V2_TABLE = "options_chain_v2"
_CHAIN_LEGACY_TABLE = "options_chain"
_STATE_TABLE = "options_symbol_ingestion_state"


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="data_options_data_quality_nonfatal",
        code=str(code),
        message=str(code),
        error=error,
        level=logging.WARNING,
        component="engine.data.options_data_quality",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(str(os.environ.get(name, str(default)) or str(default)).strip()))
    except Exception as exc:
        _warn_nonfatal(f"{name}_PARSE_FAILED", exc, once_key=f"env_int:{name}", value=os.environ.get(name))
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        value = float(str(os.environ.get(name, str(default)) or str(default)).strip())
    except Exception as exc:
        _warn_nonfatal(f"{name}_PARSE_FAILED", exc, once_key=f"env_float:{name}", value=os.environ.get(name))
        return float(default)
    if not math.isfinite(value):
        return float(default)
    return float(value)


def _snapshot_stale_ms() -> int:
    return max(1, _env_int("OPTIONS_FEATURE_STALE_MS", _DEFAULT_SNAPSHOT_STALE_MS))


def _min_coverage() -> float:
    return max(0.0, min(1.0, _env_float("OPTIONS_DQ_MIN_COVERAGE", _DEFAULT_MIN_COVERAGE)))


def _min_completeness() -> float:
    return max(0.0, min(1.0, _env_float("OPTIONS_DQ_MIN_COMPLETENESS", _DEFAULT_MIN_COMPLETENESS)))


def _iv_sanity_max() -> float:
    return max(0.0, _env_float("OPTIONS_IV_SANITY_MAX", _DEFAULT_IV_SANITY_MAX))


def _event_throttle_ms() -> int:
    return max(1, _env_int("OPTIONS_DQ_EVENT_THROTTLE_MS", _DEFAULT_EVENT_THROTTLE_MS))


def _normal_symbol(value: Any) -> str:
    return str(value or "").upper().strip()


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return float(out)


def _is_nonzero(value: Any) -> bool:
    out = _safe_float(value)
    return out is not None and abs(float(out)) > 1e-12


def _fraction(numerator: int, denominator: int) -> float:
    if int(denominator) <= 0:
        return 0.0
    return float(int(numerator)) / float(int(denominator))


def _row_value(row: Any, index: int, name: str) -> Any:
    if row is None:
        return None
    try:
        return row[name]
    except Exception:
        pass
    try:
        return row[index]
    except Exception:
        return None


def _table_columns(con, table: str) -> set[str]:
    table_name = str(table)
    if table_name not in {_CHAIN_V2_TABLE, _CHAIN_LEGACY_TABLE, _STATE_TABLE, "symbols"}:
        return set()
    module_name = str(type(con).__module__ or "")
    raw_module_name = str(type(getattr(con, "raw", None)).__module__ or "")
    looks_postgres = "storage_pg" in module_name or raw_module_name.startswith("psycopg")
    if not looks_postgres:
        try:
            rows = con.execute(f"PRAGMA table_info({table_name})").fetchall() or []
            cols = {str(_row_value(row, 1, "name") or "").strip() for row in rows}
            return {col for col in cols if col}
        except Exception:
            pass
    try:
        rows = con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = ?
            """,
            (table_name,),
        ).fetchall() or []
        return {str(_row_value(row, 0, "column_name") or "").strip() for row in rows if _row_value(row, 0, "column_name")}
    except Exception:
        return set()


def _table_exists(con, table: str) -> bool:
    return bool(_table_columns(con, table))


def _active_universe(con) -> List[str]:
    try:
        from engine.data.default_symbols import parse_symbol_limit
        from engine.data.universe import get_active_symbols
        from engine.runtime.ingestion_shards import current_ingestion_shard, filter_symbols_for_shard

        limit = parse_symbol_limit(
            os.environ.get("OPTIONS_SYMBOL_LIMIT", os.environ.get("OPTIONS_UNDERLYING_LIMIT")),
            600,
        )
        symbols = [_normal_symbol(symbol) for symbol in get_active_symbols(con, limit=limit) if _normal_symbol(symbol)]
        return list(dict.fromkeys(filter_symbols_for_shard(symbols, current_ingestion_shard())))
    except Exception as exc:
        _warn_nonfatal("OPTIONS_DQ_ACTIVE_UNIVERSE_FAILED", exc, once_key="active_universe")
        return []


def _load_symbol_states(con, symbols: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    ordered = list(dict.fromkeys(_normal_symbol(symbol) for symbol in symbols if _normal_symbol(symbol)))
    states = {
        symbol: {
            "provider": "",
            "last_fresh_snapshot_ts_ms": None,
            "last_cached_snapshot_ts_ms": None,
            "disabled_until_ts_ms": 0,
            "updated_ts_ms": 0,
        }
        for symbol in ordered
    }
    if not ordered or not _table_exists(con, _STATE_TABLE):
        return states

    for idx in range(0, len(ordered), 250):
        chunk = ordered[idx : idx + 250]
        placeholders = ",".join("?" for _ in chunk)
        try:
            rows = con.execute(
                f"""
                SELECT
                  symbol,
                  provider,
                  last_fresh_snapshot_ts_ms,
                  last_cached_snapshot_ts_ms,
                  disabled_until_ts_ms,
                  updated_ts_ms
                FROM options_symbol_ingestion_state
                WHERE symbol IN ({placeholders})
                """,
                tuple(chunk),
            ).fetchall() or []
        except Exception as exc:
            _warn_nonfatal("OPTIONS_DQ_SYMBOL_STATE_QUERY_FAILED", exc, once_key="symbol_state_query")
            return states
        for row in rows:
            symbol = _normal_symbol(_row_value(row, 0, "symbol"))
            if not symbol:
                continue
            states[symbol] = {
                "provider": str(_row_value(row, 1, "provider") or ""),
                "last_fresh_snapshot_ts_ms": (
                    int(_row_value(row, 2, "last_fresh_snapshot_ts_ms"))
                    if _row_value(row, 2, "last_fresh_snapshot_ts_ms") is not None
                    else None
                ),
                "last_cached_snapshot_ts_ms": (
                    int(_row_value(row, 3, "last_cached_snapshot_ts_ms"))
                    if _row_value(row, 3, "last_cached_snapshot_ts_ms") is not None
                    else None
                ),
                "disabled_until_ts_ms": int(_row_value(row, 4, "disabled_until_ts_ms") or 0),
                "updated_ts_ms": int(_row_value(row, 5, "updated_ts_ms") or 0),
            }
    return states


def _provider_name(source: Any, fallback: str) -> str:
    text = str(source or "").strip().lower()
    if "polygon" in text:
        return "polygon"
    if "tradier" in text:
        return "tradier"
    if text:
        return text
    return str(fallback or "unknown").strip().lower() or "unknown"


def _latest_ts(con, table: str, symbol_column: str, symbol: str) -> Optional[int]:
    if not _table_exists(con, table):
        return None
    try:
        row = con.execute(
            f"SELECT MAX(ts_ms) FROM {table} WHERE {symbol_column}=?",
            (str(symbol),),
        ).fetchone()
    except Exception as exc:
        _warn_nonfatal(f"OPTIONS_DQ_{table.upper()}_MAX_TS_FAILED", exc, once_key=f"max_ts:{table}")
        return None
    value = _row_value(row, 0, "max")
    return int(value) if value is not None else None


def _load_v2_rows(con, symbol: str, latest_ts_ms: int, stale_ms: int) -> List[Dict[str, Any]]:
    try:
        rows = con.execute(
            """
            SELECT
              ts_ms, underlying, contract, expiration, contract_type, strike,
              iv, open_interest, volume, bid, ask, delta, gamma, theta, vega, source
            FROM options_chain_v2
            WHERE underlying=?
              AND ts_ms >= ?
              AND ts_ms <= ?
            ORDER BY contract ASC, ts_ms DESC
            """,
            (str(symbol), int(latest_ts_ms) - int(stale_ms), int(latest_ts_ms)),
        ).fetchall() or []
    except Exception as exc:
        _warn_nonfatal("OPTIONS_DQ_CHAIN_V2_QUERY_FAILED", exc, once_key="chain_v2_query")
        return []
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        contract = str(_row_value(row, 2, "contract") or "").strip()
        if not contract or contract in seen:
            continue
        seen.add(contract)
        source = _row_value(row, 15, "source")
        out.append(
            {
                "table": _CHAIN_V2_TABLE,
                "ts_ms": int(_row_value(row, 0, "ts_ms") or 0),
                "underlying": _normal_symbol(_row_value(row, 1, "underlying")),
                "contract_key": contract,
                "iv": _row_value(row, 6, "iv"),
                "open_interest": _row_value(row, 7, "open_interest"),
                "volume": _row_value(row, 8, "volume"),
                "bid": _row_value(row, 9, "bid"),
                "ask": _row_value(row, 10, "ask"),
                "delta": _row_value(row, 11, "delta"),
                "gamma": _row_value(row, 12, "gamma"),
                "theta": _row_value(row, 13, "theta"),
                "vega": _row_value(row, 14, "vega"),
                "source": str(source or "polygon"),
                "provider": _provider_name(source, "polygon"),
                "has_bid_ask_columns": True,
                "has_greeks_columns": True,
            }
        )
    return out


def _load_legacy_rows(con, symbol: str, latest_ts_ms: int, stale_ms: int) -> List[Dict[str, Any]]:
    try:
        rows = con.execute(
            """
            SELECT ts_ms, symbol, expiry, call_put, strike, iv, open_interest, volume, source
            FROM options_chain
            WHERE symbol=?
              AND ts_ms >= ?
              AND ts_ms <= ?
            ORDER BY expiry ASC, strike ASC, call_put ASC, ts_ms DESC
            """,
            (str(symbol), int(latest_ts_ms) - int(stale_ms), int(latest_ts_ms)),
        ).fetchall() or []
    except Exception as exc:
        _warn_nonfatal("OPTIONS_DQ_CHAIN_LEGACY_QUERY_FAILED", exc, once_key="chain_legacy_query")
        return []
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        key = f"{_row_value(row, 2, 'expiry')}:{_row_value(row, 4, 'strike')}:{_row_value(row, 3, 'call_put')}"
        if not key.strip() or key in seen:
            continue
        seen.add(key)
        source = _row_value(row, 8, "source")
        out.append(
            {
                "table": _CHAIN_LEGACY_TABLE,
                "ts_ms": int(_row_value(row, 0, "ts_ms") or 0),
                "underlying": _normal_symbol(_row_value(row, 1, "symbol")),
                "contract_key": key,
                "iv": _row_value(row, 5, "iv"),
                "open_interest": _row_value(row, 6, "open_interest"),
                "volume": _row_value(row, 7, "volume"),
                "bid": None,
                "ask": None,
                "delta": None,
                "gamma": None,
                "theta": None,
                "vega": None,
                "source": str(source or "legacy"),
                "provider": _provider_name(source, "legacy"),
                "has_bid_ask_columns": False,
                "has_greeks_columns": False,
            }
        )
    return out


def _latest_chain_rows_for_symbol(con, symbol: str, stale_ms: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    v2_ts = _latest_ts(con, _CHAIN_V2_TABLE, "underlying", symbol)
    legacy_ts = _latest_ts(con, _CHAIN_LEGACY_TABLE, "symbol", symbol)
    rows: List[Dict[str, Any]] = []
    if v2_ts is not None:
        rows.extend(_load_v2_rows(con, symbol, int(v2_ts), int(stale_ms)))
    if legacy_ts is not None:
        rows.extend(_load_legacy_rows(con, symbol, int(legacy_ts), int(stale_ms)))
    latest_ts = max([int(ts) for ts in (v2_ts, legacy_ts) if ts is not None], default=0)
    source = "none"
    if v2_ts is not None and (legacy_ts is None or int(v2_ts) >= int(legacy_ts)):
        source = "options_chain_v2"
    elif legacy_ts is not None:
        source = "options_chain"
    return rows, {
        "latest_chain_ts_ms": int(latest_ts) if latest_ts > 0 else None,
        "latest_chain_source": source,
        "latest_v2_ts_ms": int(v2_ts) if v2_ts is not None else None,
        "latest_legacy_ts_ms": int(legacy_ts) if legacy_ts is not None else None,
    }


def _provider_template(provider: str) -> Dict[str, Any]:
    return {
        "provider": str(provider),
        "rows": 0,
        "v2_rows": 0,
        "legacy_rows": 0,
        "iv_complete_rows": 0,
        "open_interest_complete_rows": 0,
        "volume_complete_rows": 0,
        "bid_complete_rows": 0,
        "ask_complete_rows": 0,
        "bid_ask_complete_rows": 0,
        "delta_complete_rows": 0,
        "gamma_complete_rows": 0,
        "theta_complete_rows": 0,
        "vega_complete_rows": 0,
        "greeks_complete_rows": 0,
        "zero_greeks_rows": 0,
        "legacy_missing_bid_ask_greeks": False,
        "sources": [],
    }


def _provider_stats(rows: Iterable[Dict[str, Any]], *, iv_max: float) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, int]]:
    providers: Dict[str, Dict[str, Any]] = {}
    iv_negative_rows = 0
    iv_absurd_rows = 0
    zero_greeks_rows = 0
    for row in rows:
        provider = _provider_name(row.get("provider") or row.get("source"), "unknown")
        stats = providers.setdefault(provider, _provider_template(provider))
        stats["rows"] += 1
        if row.get("table") == _CHAIN_V2_TABLE:
            stats["v2_rows"] += 1
        if row.get("table") == _CHAIN_LEGACY_TABLE:
            stats["legacy_rows"] += 1
            stats["legacy_missing_bid_ask_greeks"] = True
        source = str(row.get("source") or "")
        if source and source not in stats["sources"]:
            stats["sources"].append(source)

        iv = _safe_float(row.get("iv"))
        if _is_nonzero(row.get("iv")):
            stats["iv_complete_rows"] += 1
        if _is_nonzero(row.get("open_interest")):
            stats["open_interest_complete_rows"] += 1
        if _is_nonzero(row.get("volume")):
            stats["volume_complete_rows"] += 1
        if iv is not None and iv < 0.0:
            iv_negative_rows += 1
        if iv is not None and iv > float(iv_max):
            iv_absurd_rows += 1

        bid_complete = bool(row.get("has_bid_ask_columns")) and _is_nonzero(row.get("bid"))
        ask_complete = bool(row.get("has_bid_ask_columns")) and _is_nonzero(row.get("ask"))
        if bid_complete:
            stats["bid_complete_rows"] += 1
        if ask_complete:
            stats["ask_complete_rows"] += 1
        if bid_complete and ask_complete:
            stats["bid_ask_complete_rows"] += 1

        delta_complete = bool(row.get("has_greeks_columns")) and _is_nonzero(row.get("delta"))
        gamma_complete = bool(row.get("has_greeks_columns")) and _is_nonzero(row.get("gamma"))
        theta_complete = bool(row.get("has_greeks_columns")) and _is_nonzero(row.get("theta"))
        vega_complete = bool(row.get("has_greeks_columns")) and _is_nonzero(row.get("vega"))
        if delta_complete:
            stats["delta_complete_rows"] += 1
        if gamma_complete:
            stats["gamma_complete_rows"] += 1
        if theta_complete:
            stats["theta_complete_rows"] += 1
        if vega_complete:
            stats["vega_complete_rows"] += 1
        if delta_complete and gamma_complete and theta_complete and vega_complete:
            stats["greeks_complete_rows"] += 1

        expects_polygon_greeks = "polygon" in provider or "polygon" in str(row.get("source") or "").lower()
        if expects_polygon_greeks:
            greeks = [row.get("delta"), row.get("gamma"), row.get("theta"), row.get("vega")]
            if all(not _is_nonzero(value) for value in greeks):
                stats["zero_greeks_rows"] += 1
                zero_greeks_rows += 1

    for stats in providers.values():
        rows_count = int(stats["rows"])
        for key in (
            "iv",
            "open_interest",
            "volume",
            "bid",
            "ask",
            "bid_ask",
            "delta",
            "gamma",
            "theta",
            "vega",
            "greeks",
        ):
            stats[f"{key}_complete_fraction"] = _fraction(int(stats.get(f"{key}_complete_rows") or 0), rows_count)
        stats["sources"] = sorted(str(src) for src in stats.get("sources") or [])
    return providers, {
        "iv_negative_rows": int(iv_negative_rows),
        "iv_absurd_rows": int(iv_absurd_rows),
        "zero_greeks_rows": int(zero_greeks_rows),
    }


def compute_options_data_quality(
    con,
    *,
    now_ms: int,
    symbols: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Compute a read-only data-quality report from options chain/state tables."""

    now = int(now_ms)
    stale_ms = _snapshot_stale_ms()
    min_coverage = _min_coverage()
    min_completeness = _min_completeness()
    iv_max = _iv_sanity_max()
    universe = list(dict.fromkeys(_normal_symbol(symbol) for symbol in (symbols or []) if _normal_symbol(symbol)))
    universe_source = "explicit"
    if symbols is None:
        universe = _active_universe(con)
        universe_source = "active_symbols"

    states = _load_symbol_states(con, universe)
    all_rows: List[Dict[str, Any]] = []
    freshness: Dict[str, Dict[str, Any]] = {}
    fresh_symbols: List[str] = []

    for symbol in universe:
        symbol_rows, chain_meta = _latest_chain_rows_for_symbol(con, symbol, stale_ms)
        all_rows.extend(symbol_rows)
        state = states.get(symbol) or {}
        last_fresh = state.get("last_fresh_snapshot_ts_ms")
        last_fresh_int = int(last_fresh) if last_fresh is not None else 0
        latest_chain_ts = int(chain_meta.get("latest_chain_ts_ms") or 0)
        state_age_ms = max(0, now - last_fresh_int) if last_fresh_int > 0 else None
        chain_age_ms = max(0, now - latest_chain_ts) if latest_chain_ts > 0 else None
        state_fresh = bool(last_fresh_int > 0 and state_age_ms is not None and state_age_ms <= int(stale_ms))
        chain_fresh = bool(latest_chain_ts > 0 and chain_age_ms is not None and chain_age_ms <= int(stale_ms))
        is_fresh = bool(state_fresh and chain_fresh and symbol_rows)
        if is_fresh:
            fresh_symbols.append(symbol)
        freshness[symbol] = {
            "symbol": symbol,
            "provider": str(state.get("provider") or ""),
            "fresh": bool(is_fresh),
            "state_fresh": bool(state_fresh),
            "chain_fresh": bool(chain_fresh),
            "last_fresh_snapshot_ts_ms": int(last_fresh_int) if last_fresh_int > 0 else None,
            "state_age_ms": int(state_age_ms) if state_age_ms is not None else None,
            "latest_chain_ts_ms": int(latest_chain_ts) if latest_chain_ts > 0 else None,
            "chain_age_ms": int(chain_age_ms) if chain_age_ms is not None else None,
            "latest_chain_source": str(chain_meta.get("latest_chain_source") or "none"),
            "latest_v2_ts_ms": chain_meta.get("latest_v2_ts_ms"),
            "latest_legacy_ts_ms": chain_meta.get("latest_legacy_ts_ms"),
            "rows": int(len(symbol_rows)),
        }

    providers, sanity_counts = _provider_stats(all_rows, iv_max=iv_max)
    universe_count = int(len(universe))
    fresh_count = int(len(fresh_symbols))
    coverage = _fraction(fresh_count, universe_count)
    available = bool(universe_count > 0 and fresh_count > 0 and all_rows)

    reason_codes: List[str] = []
    completeness_failures: List[str] = []
    if universe_count <= 0:
        reason_codes.append("no_options_universe")
    if universe_count > 0 and fresh_count <= 0:
        reason_codes.append("no_fresh_options_chains")
    if universe_count > 0 and coverage < min_coverage:
        reason_codes.append("coverage_below_min")
    for provider, stats in sorted(providers.items()):
        if int(stats.get("rows") or 0) <= 0:
            continue
        if float(stats.get("greeks_complete_fraction") or 0.0) < min_completeness:
            completeness_failures.append(f"{provider}:greeks")
        if float(stats.get("bid_ask_complete_fraction") or 0.0) < min_completeness:
            completeness_failures.append(f"{provider}:bid_ask")
    if completeness_failures:
        reason_codes.append("field_completeness_below_min")
    if int(sanity_counts["iv_negative_rows"]) > 0:
        reason_codes.append("iv_negative_rows")
    if int(sanity_counts["iv_absurd_rows"]) > 0:
        reason_codes.append("iv_absurd_rows")
    if int(sanity_counts["zero_greeks_rows"]) > 0:
        reason_codes.append("zero_greeks_rows")

    degraded = bool(
        not available
        or (universe_count > 0 and coverage < min_coverage)
        or completeness_failures
        or int(sanity_counts["iv_negative_rows"]) > 0
        or int(sanity_counts["iv_absurd_rows"]) > 0
        or int(sanity_counts["zero_greeks_rows"]) > 0
    )
    ok = bool(available and not degraded)
    detail = "ok" if ok else (reason_codes[0] if reason_codes else "options_data_quality_degraded")
    return {
        "ok": bool(ok),
        "available": bool(available),
        "degraded": bool(degraded),
        "detail": str(detail),
        "reason_codes": list(reason_codes),
        "coverage_fraction": float(coverage),
        "fresh_underlyings": int(fresh_count),
        "universe_underlyings": int(universe_count),
        "universe_source": str(universe_source),
        "symbols": list(universe),
        "fresh_symbols": sorted(fresh_symbols),
        "chain_rows": int(len(all_rows)),
        "providers": providers,
        "completeness_failures": list(completeness_failures),
        "iv_sanity": {
            "iv_negative_rows": int(sanity_counts["iv_negative_rows"]),
            "iv_absurd_rows": int(sanity_counts["iv_absurd_rows"]),
            "zero_greeks_rows": int(sanity_counts["zero_greeks_rows"]),
            "iv_sanity_max": float(iv_max),
        },
        "freshness": freshness,
        "thresholds": {
            "stale_ms": int(stale_ms),
            "min_coverage": float(min_coverage),
            "min_completeness": float(min_completeness),
            "iv_sanity_max": float(iv_max),
        },
        "ts_ms": int(now),
    }


def options_data_quality_ok(report: Dict[str, Any]) -> bool:
    """Return the cheap consumer trust flag for an existing DQ report."""

    return bool(
        isinstance(report, dict)
        and bool(report.get("available"))
        and bool(report.get("ok"))
        and not bool(report.get("degraded"))
    )


def _write_metric(metric: str, value: Any, *, ts_ms: int, tags: Optional[Dict[str, Any]] = None) -> None:
    try:
        from engine.runtime.metrics_store import write_runtime_metric

        write_runtime_metric(
            str(metric),
            value_num=value,
            tags={str(k): str(v) for k, v in dict(tags or {}).items() if v is not None},
            ts_ms=int(ts_ms),
        )
    except Exception as exc:
        _warn_nonfatal(
            "OPTIONS_DQ_RUNTIME_METRIC_FAILED",
            exc,
            once_key=f"options_dq_metric:{metric}",
            metric=str(metric),
        )


def write_options_data_quality_metrics(report: Dict[str, Any], *, ts_ms: Optional[int] = None) -> None:
    """Best-effort runtime metric emission for a computed DQ report."""

    if not isinstance(report, dict):
        return
    now = int(ts_ms or report.get("ts_ms") or time.time() * 1000)
    scalars = {
        "options.dq.available": 1 if report.get("available") else 0,
        "options.dq.ok": 1 if report.get("ok") else 0,
        "options.dq.degraded": 1 if report.get("degraded") else 0,
        "options.dq.coverage_fraction": float(report.get("coverage_fraction") or 0.0),
        "options.dq.fresh_underlyings": int(report.get("fresh_underlyings") or 0),
        "options.dq.universe_underlyings": int(report.get("universe_underlyings") or 0),
        "options.dq.chain_rows": int(report.get("chain_rows") or 0),
        "options.dq.iv_negative_rows": int(((report.get("iv_sanity") or {}).get("iv_negative_rows")) or 0),
        "options.dq.iv_absurd_rows": int(((report.get("iv_sanity") or {}).get("iv_absurd_rows")) or 0),
        "options.dq.zero_greeks_rows": int(((report.get("iv_sanity") or {}).get("zero_greeks_rows")) or 0),
    }
    for metric, value in scalars.items():
        _write_metric(metric, value, ts_ms=now)

    for provider, stats in sorted(dict(report.get("providers") or {}).items()):
        tags = {"provider": str(provider)}
        for metric, value in {
            "options.dq.provider.rows": int(stats.get("rows") or 0),
            "options.dq.provider.v2_rows": int(stats.get("v2_rows") or 0),
            "options.dq.provider.legacy_rows": int(stats.get("legacy_rows") or 0),
            "options.dq.provider.iv_complete_fraction": float(stats.get("iv_complete_fraction") or 0.0),
            "options.dq.provider.bid_ask_complete_fraction": float(stats.get("bid_ask_complete_fraction") or 0.0),
            "options.dq.provider.greeks_complete_fraction": float(stats.get("greeks_complete_fraction") or 0.0),
        }.items():
            _write_metric(metric, value, ts_ms=now, tags=tags)


def _dq_degradation_event_needed(report: Dict[str, Any]) -> bool:
    if options_data_quality_ok(report):
        return False
    if int(report.get("universe_underlyings") or 0) <= 0:
        return False
    reasons = set(str(v) for v in (report.get("reason_codes") or []))
    return bool(
        reasons.intersection(
            {
                "no_fresh_options_chains",
                "coverage_below_min",
                "field_completeness_below_min",
                "iv_negative_rows",
                "iv_absurd_rows",
                "zero_greeks_rows",
            }
        )
    )


def emit_options_data_quality_degradation_event(
    report: Dict[str, Any],
    *,
    now_ms: Optional[int] = None,
) -> Dict[str, Any]:
    """Emit one throttled ``options_data_quality_degraded`` event if needed."""

    global _LAST_DQ_DEGRADATION_EVENT_TS_MS
    if not isinstance(report, dict) or not _dq_degradation_event_needed(report):
        return {"events": 0, "throttled": False, "needed": False}
    now = int(now_ms or report.get("ts_ms") or time.time() * 1000)
    throttle_ms = _event_throttle_ms()
    if _LAST_DQ_DEGRADATION_EVENT_TS_MS > 0 and now - int(_LAST_DQ_DEGRADATION_EVENT_TS_MS) < int(throttle_ms):
        return {"events": 0, "throttled": True, "needed": True}

    kind = "options_data_quality_degraded"
    bucket = int(now // max(1, int(throttle_ms)))
    reasons = [str(v) for v in (report.get("reason_codes") or [])]
    title = "Options data quality degraded"
    body = (
        f"Coverage {float(report.get('coverage_fraction') or 0.0):.3f}; "
        f"reasons={','.join(reasons[:6]) or 'degraded'}."
    )
    payload = {
        "ts_ms": int(now),
        "timestamp": int(now),
        "event_type": "options",
        "symbol": "*",
        "source": "options_data_quality",
        "title": title,
        "body": body,
        "source_id": f"options_data_quality:{kind}:{bucket}",
        "event_key": f"options:{kind}:{bucket}",
        "raw_payload": {
            "event_kind": kind,
            "options_event_kind": kind,
            "coverage_fraction": float(report.get("coverage_fraction") or 0.0),
            "fresh_underlyings": int(report.get("fresh_underlyings") or 0),
            "universe_underlyings": int(report.get("universe_underlyings") or 0),
            "reason_codes": reasons,
            "thresholds": dict(report.get("thresholds") or {}),
            "iv_sanity": dict(report.get("iv_sanity") or {}),
            "completeness_failures": list(report.get("completeness_failures") or []),
        },
        "derived_features": {
            "options_event_kind": kind,
            "coverage_fraction": float(report.get("coverage_fraction") or 0.0),
            "fresh_underlyings": int(report.get("fresh_underlyings") or 0),
            "universe_underlyings": int(report.get("universe_underlyings") or 0),
            "iv_negative_rows": int(((report.get("iv_sanity") or {}).get("iv_negative_rows")) or 0),
            "iv_absurd_rows": int(((report.get("iv_sanity") or {}).get("iv_absurd_rows")) or 0),
            "zero_greeks_rows": int(((report.get("iv_sanity") or {}).get("zero_greeks_rows")) or 0),
            "source_reliability": 0.70,
        },
    }

    try:
        from engine.data import options_features

        def _write(con):
            return int(options_features.put_normalized_event(payload, con=con) or 0)

        event_id = int(
            options_features.run_write_txn(
                _write,
                table="events",
                operation="emit_options_data_quality_degraded",
                context={
                    "coverage_fraction": float(report.get("coverage_fraction") or 0.0),
                    "reason_codes": ",".join(reasons[:8]),
                },
            )
            or 0
        )
    except Exception as exc:
        _warn_nonfatal("OPTIONS_DQ_DEGRADATION_EVENT_FAILED", exc, once_key="dq_degradation_event")
        return {"events": 0, "throttled": False, "needed": True, "error": type(exc).__name__}

    if event_id > 0:
        _LAST_DQ_DEGRADATION_EVENT_TS_MS = int(now)
    return {"events": 1 if event_id > 0 else 0, "event_id": int(event_id), "throttled": False, "needed": True}


def record_options_data_quality_observability(report: Dict[str, Any], *, now_ms: Optional[int] = None) -> Dict[str, Any]:
    """Best-effort metrics plus throttled degradation event for a DQ report."""

    ts_ms = int(now_ms or (report or {}).get("ts_ms") or time.time() * 1000)
    write_options_data_quality_metrics(report, ts_ms=ts_ms)
    return emit_options_data_quality_degradation_event(report, now_ms=ts_ms)


__all__ = [
    "compute_options_data_quality",
    "emit_options_data_quality_degradation_event",
    "options_data_quality_ok",
    "record_options_data_quality_observability",
    "write_options_data_quality_metrics",
]
