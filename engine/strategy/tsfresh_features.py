"""
Optional tsfresh-derived feature extraction with persisted snapshot support.

This module keeps tsfresh imports lazy so baseline startup is unaffected when
the feature group is disabled.
"""

from __future__ import annotations

import importlib
import json
import logging
import math
import os
from typing import Any, Dict, Iterable, List, Optional, Sequence

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect
from engine.runtime.workload_profiles import (
    tsfresh_n_jobs,
    tsfresh_snapshot_batch_size,
    tsfresh_snapshot_symbol_limit,
)

LOG = get_logger("engine.strategy.tsfresh_features")
_WARNED_NONFATAL_KEYS: set[str] = set()

TSFRESH_FEATURE_PREFIX = "tsfresh."
TSFRESH_WINDOW_S = max(60, int(os.environ.get("TSFRESH_WINDOW_S", "3600")))
TSFRESH_FC_PROFILE = str(os.environ.get("TSFRESH_FC_PROFILE", "minimal") or "minimal").strip().lower() or "minimal"
TSFRESH_MAX_FEATURES = max(1, int(os.environ.get("TSFRESH_MAX_FEATURES", "200")))
TSFRESH_N_JOBS = tsfresh_n_jobs()
TSFRESH_SNAPSHOT_SYMBOL_LIMIT = tsfresh_snapshot_symbol_limit()
TSFRESH_SNAPSHOT_BATCH_SIZE = tsfresh_snapshot_batch_size()
TSFRESH_USE_PERSISTED_SNAPSHOTS = os.environ.get("TSFRESH_USE_PERSISTED_SNAPSHOTS", "1") == "1"
TSFRESH_LIVE_COMPUTE_ENABLED = os.environ.get("TSFRESH_LIVE_COMPUTE_ENABLED", "0") == "1"
TSFRESH_SNAPSHOT_BUCKET_SEC = max(
    60,
    int(
        os.environ.get(
            "TSFRESH_SNAPSHOT_BUCKET_SEC",
            os.environ.get("MODEL_FEATURE_SNAPSHOT_BUCKET_SEC", "300"),
        )
    ),
)

_MINIMAL_FEATURE_NAMES = [
    "abs_energy",
    "absolute_maximum",
    "absolute_sum_of_changes",
    "count_above_mean",
    "count_below_mean",
    "first_location_of_maximum",
    "first_location_of_minimum",
    "kurtosis",
    "last_location_of_maximum",
    "last_location_of_minimum",
    "length",
    "longest_strike_above_mean",
    "longest_strike_below_mean",
    "maximum",
    "mean",
    "mean_abs_change",
    "mean_change",
    "median",
    "minimum",
    "root_mean_square",
    "skewness",
    "standard_deviation",
    "sum_values",
    "variance",
]

_EXTENDED_FEATURE_NAMES = list(_MINIMAL_FEATURE_NAMES) + [
    "has_duplicate",
    "has_duplicate_max",
    "has_duplicate_min",
    "ratio_value_number_to_time_series_length",
    "sample_entropy",
    "variance_larger_than_standard_deviation",
]

_PROFILE_FEATURE_NAMES = {
    "minimal": list(_MINIMAL_FEATURE_NAMES),
    "efficient": list(_EXTENDED_FEATURE_NAMES),
    "balanced": list(_EXTENDED_FEATURE_NAMES),
}

_CANONICAL_FEATURE_NAMES = list(dict.fromkeys(_EXTENDED_FEATURE_NAMES))
_CANONICAL_FEATURE_IDS = [f"{TSFRESH_FEATURE_PREFIX}{name}" for name in _CANONICAL_FEATURE_NAMES]


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.tsfresh_features",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _bucket_start(ts_ms: int, bucket_sec: int) -> int:
    size_ms = max(1, int(bucket_sec)) * 1000
    return int(ts_ms // size_ms) * size_ms


def _canonical_zero_feature_map() -> Dict[str, float]:
    return {fid: 0.0 for fid in get_tsfresh_feature_ids()}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _normalize_symbol(symbol: str) -> str:
    return str(symbol or "").upper().strip()


def _bounded_symbols(symbols: Sequence[str], *, symbol_limit: Optional[int] = None) -> List[str]:
    limit = max(1, int(symbol_limit if symbol_limit is not None else TSFRESH_SNAPSHOT_SYMBOL_LIMIT))
    out: List[str] = []
    seen: set[str] = set()
    for raw in symbols or []:
        symbol = _normalize_symbol(str(raw))
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
        if len(out) >= int(limit):
            break
    return out


def _chunks(values: Sequence[str], size: int) -> Iterable[List[str]]:
    batch_size = max(1, int(size))
    current: List[str] = []
    for value in values:
        current.append(str(value))
        if len(current) >= batch_size:
            yield list(current)
            current = []
    if current:
        yield list(current)


def _profile_feature_names(profile: str | None = None) -> List[str]:
    key = str(profile or TSFRESH_FC_PROFILE or "minimal").strip().lower() or "minimal"
    return list(_PROFILE_FEATURE_NAMES.get(key) or _PROFILE_FEATURE_NAMES["minimal"])


def _default_feature_names() -> List[str]:
    names = _profile_feature_names(TSFRESH_FC_PROFILE)
    limit = int(max(1, TSFRESH_MAX_FEATURES))
    return list(names[:limit])


def get_default_tsfresh_feature_ids() -> List[str]:
    """Return the default TSFresh feature ids exposed to model pipelines."""
    return [f"{TSFRESH_FEATURE_PREFIX}{name}" for name in _default_feature_names()]


def get_tsfresh_feature_ids() -> List[str]:
    """Return the canonical TSFresh feature ids supported by stored snapshots."""
    return list(_CANONICAL_FEATURE_IDS)


def _fc_parameters_for_extract() -> Dict[str, None]:
    # Persisted snapshots compute the supported superset so previously persisted
    # model feature schemas keep resolving even if defaults change later.
    return {name: None for name in _CANONICAL_FEATURE_NAMES}


def _normalize_feature_map(raw: Optional[Dict[str, Any]]) -> Dict[str, float]:
    out = _canonical_zero_feature_map()
    for key, value in dict(raw or {}).items():
        text = str(key or "").strip()
        if not text:
            continue
        if not text.startswith(TSFRESH_FEATURE_PREFIX):
            text = f"{TSFRESH_FEATURE_PREFIX}{text}"
        if text not in out:
            continue
        out[text] = _safe_float(value, 0.0)
    return out


def _load_optional_dependencies():
    pd = importlib.import_module("pandas")
    tsfresh = importlib.import_module("tsfresh")
    return pd, tsfresh


def _build_tsfresh_window_with_con(con, symbol: str, end_ts: int, window_s: int):
    pd = importlib.import_module("pandas")
    symbol_key = _normalize_symbol(symbol)
    end_ts_ms = int(end_ts)
    start_ts_ms = int(end_ts_ms) - (max(1, int(window_s)) * 1000)
    rows = con.execute(
        """
        SELECT ts_ms, COALESCE(price, px) AS value
        FROM prices
        WHERE symbol = ?
          AND ts_ms BETWEEN ? AND ?
          AND COALESCE(price, px) IS NOT NULL
        ORDER BY ts_ms ASC
        """,
        (symbol_key, int(start_ts_ms), int(end_ts_ms)),
    ).fetchall()
    if not rows:
        return pd.DataFrame(columns=["id", "sort", "value"])
    return pd.DataFrame(
        [
            {
                "id": str(symbol_key),
                "sort": int(row[0]),
                "value": _safe_float(row[1], 0.0),
            }
            for row in (rows or [])
            if row and row[1] is not None
        ],
        columns=["id", "sort", "value"],
    )


def build_tsfresh_window(symbol, end_ts, window_s):
    """Load the historical price window used to compute TSFresh features for one symbol."""
    con = connect(readonly=True)
    try:
        return _build_tsfresh_window_with_con(con, str(symbol), int(end_ts), int(window_s))
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal(
                "TSFRESH_WINDOW_CLOSE_FAILED",
                exc,
                once_key="tsfresh_window_close_failed",
            )


def compute_tsfresh_features(window_df) -> Dict[str, float]:
    """Compute a normalized TSFresh feature map from one extracted price window."""
    base = _canonical_zero_feature_map()
    if window_df is None:
        return base

    try:
        pd, tsfresh = _load_optional_dependencies()
    except Exception as exc:
        raise RuntimeError(f"tsfresh_dependency_missing:{type(exc).__name__}:{exc}") from exc

    try:
        df = pd.DataFrame(window_df).copy()
    except Exception:
        return base

    required = {"id", "sort", "value"}
    if not required.issubset(set(df.columns)):
        return base

    try:
        df = df.loc[:, ["id", "sort", "value"]].copy()
        df["id"] = df["id"].astype(str)
        df["sort"] = pd.to_numeric(df["sort"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["sort", "value"])
    except Exception as exc:
        _warn_nonfatal(
            "TSFRESH_WINDOW_NORMALIZE_FAILED",
            exc,
            once_key="tsfresh_window_normalize_failed",
        )
        return base

    if int(len(df.index)) < 2:
        return base

    try:
        extracted = tsfresh.extract_features(
            df,
            column_id="id",
            column_sort="sort",
            column_value="value",
            default_fc_parameters=_fc_parameters_for_extract(),
            disable_progressbar=True,
            n_jobs=int(TSFRESH_N_JOBS),
        )
    except Exception as exc:
        _warn_nonfatal(
            "TSFRESH_EXTRACT_FAILED",
            exc,
            once_key="tsfresh_extract_failed",
            rows=int(len(df.index)),
        )
        return base

    if extracted is None or getattr(extracted, "empty", True):
        return base

    try:
        row = dict(extracted.iloc[0].to_dict())
    except Exception as exc:
        _warn_nonfatal(
            "TSFRESH_RESULT_PARSE_FAILED",
            exc,
            once_key="tsfresh_result_parse_failed",
        )
        return base

    for name in _CANONICAL_FEATURE_NAMES:
        fid = f"{TSFRESH_FEATURE_PREFIX}{name}"
        base[fid] = _safe_float(row.get(f"value__{name}"), 0.0)
    return base


def build_tsfresh_feature_snapshot(
    *,
    symbol: str,
    ts_ms: int,
    window_s: Optional[int] = None,
    con=None,
) -> Dict[str, Any]:
    """Build one TSFresh snapshot payload for a symbol and anchor timestamp."""
    symbol_key = _normalize_symbol(symbol)
    anchor_ts_ms = _bucket_start(int(ts_ms), int(TSFRESH_SNAPSHOT_BUCKET_SEC))
    window_seconds = max(60, int(window_s or TSFRESH_WINDOW_S))
    owns = False
    if con is None:
        con = connect(readonly=True)
        owns = True
    try:
        window_df = _build_tsfresh_window_with_con(
            con,
            str(symbol_key),
            int(anchor_ts_ms),
            int(window_seconds),
        )
        features = compute_tsfresh_features(window_df)
        return {
            "symbol": str(symbol_key),
            "ts": int(anchor_ts_ms),
            "window_s": int(window_seconds),
            "features": dict(features),
            "row_count": int(len(getattr(window_df, "index", []))),
        }
    finally:
        if owns:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal(
                    "TSFRESH_BUILD_CLOSE_FAILED",
                    exc,
                    once_key="tsfresh_build_close_failed",
                )


def store_tsfresh_feature_snapshots(
    snapshots: Iterable[Dict[str, Any]],
    *,
    con=None,
) -> int:
    """Upsert TSFresh feature snapshots into the persisted snapshot table."""
    snapshot_list = list(snapshots or [])
    rows = []
    for snap in snapshot_list:
        symbol = _normalize_symbol(str((snap or {}).get("symbol") or ""))
        if not symbol:
            continue
        rows.append(
            (
                str(symbol),
                int((snap or {}).get("ts") or 0),
                int((snap or {}).get("window_s") or TSFRESH_WINDOW_S),
                _json_dumps(_normalize_feature_map((snap or {}).get("features"))),
            )
        )
    if not rows:
        return 0

    owns = False
    if con is None:
        con = connect(readonly=False)
        owns = True
    try:
        con.executemany(
            """
            INSERT INTO tsfresh_feature_snapshots(symbol, ts, window_s, features_json)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(symbol, ts, window_s) DO UPDATE SET
              features_json=excluded.features_json
            """,
            rows,
        )
        if owns:
            con.commit()
        return int(len(rows))
    finally:
        if owns:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal(
                    "TSFRESH_STORE_CLOSE_FAILED",
                    exc,
                    once_key="tsfresh_store_close_failed",
                )


def load_tsfresh_feature_snapshot(
    *,
    symbol: str,
    ts_ms: int,
    window_s: Optional[int] = None,
    exact: bool = False,
    con=None,
) -> Optional[Dict[str, Any]]:
    """Load the latest TSFresh snapshot at or before the requested timestamp."""
    symbol_key = _normalize_symbol(symbol)
    if not symbol_key:
        return None

    comparator = "=" if bool(exact) else "<="
    window_seconds = max(60, int(window_s or TSFRESH_WINDOW_S))
    owns = False
    if con is None:
        con = connect(readonly=True)
        owns = True
    try:
        row = con.execute(
            f"""
            SELECT symbol, ts, window_s, features_json
            FROM tsfresh_feature_snapshots
            WHERE symbol = ?
              AND window_s = ?
              AND ts {comparator} ?
            ORDER BY ts DESC
            LIMIT 1
            """,
            (str(symbol_key), int(window_seconds), int(ts_ms)),
        ).fetchone()
        if not row:
            return None
        try:
            parsed = json.loads(str(row[3] or "{}"))
        except Exception as exc:
            _warn_nonfatal(
                "TSFRESH_SNAPSHOT_PARSE_FAILED",
                exc,
                once_key="tsfresh_snapshot_parse_failed",
                symbol=str(symbol_key),
            )
            parsed = {}
        return {
            "symbol": str(row[0] or ""),
            "ts": int(row[1] or 0),
            "window_s": int(row[2] or window_seconds),
            "features": _normalize_feature_map(parsed),
        }
    finally:
        if owns:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal(
                    "TSFRESH_LOAD_CLOSE_FAILED",
                    exc,
                    once_key="tsfresh_load_close_failed",
                )


def resolve_tsfresh_features(*, symbol: str, ts_ms: int) -> Dict[str, float]:
    """Resolve TSFresh features from persisted snapshots or live computation."""
    anchor_ts_ms = _bucket_start(int(ts_ms), int(TSFRESH_SNAPSHOT_BUCKET_SEC))
    if int(anchor_ts_ms) <= 0:
        return _canonical_zero_feature_map()

    if TSFRESH_USE_PERSISTED_SNAPSHOTS:
        try:
            snap = load_tsfresh_feature_snapshot(
                symbol=str(symbol),
                ts_ms=int(anchor_ts_ms),
                window_s=int(TSFRESH_WINDOW_S),
                exact=False,
            )
        except Exception as exc:
            _warn_nonfatal(
                "TSFRESH_PERSISTED_LOAD_FAILED",
                exc,
                once_key="tsfresh_persisted_load_failed",
                symbol=str(symbol),
                ts_ms=int(anchor_ts_ms),
            )
            snap = None
        if isinstance(snap, dict):
            return _normalize_feature_map((snap or {}).get("features"))

    if not TSFRESH_LIVE_COMPUTE_ENABLED:
        return _canonical_zero_feature_map()

    try:
        snap = build_tsfresh_feature_snapshot(
            symbol=str(symbol),
            ts_ms=int(anchor_ts_ms),
            window_s=int(TSFRESH_WINDOW_S),
        )
        if TSFRESH_USE_PERSISTED_SNAPSHOTS:
            try:
                store_tsfresh_feature_snapshots([snap])
            except Exception as exc:
                _warn_nonfatal(
                    "TSFRESH_LIVE_STORE_FAILED",
                    exc,
                    once_key="tsfresh_live_store_failed",
                    symbol=str(symbol),
                    ts_ms=int(anchor_ts_ms),
                )
        return _normalize_feature_map((snap or {}).get("features"))
    except Exception as exc:
        _warn_nonfatal(
            "TSFRESH_LIVE_COMPUTE_FAILED",
            exc,
            once_key="tsfresh_live_compute_failed",
            symbol=str(symbol),
            ts_ms=int(anchor_ts_ms),
        )
        return _canonical_zero_feature_map()


def materialize_tsfresh_feature_snapshots(
    *,
    symbols: Sequence[str],
    ts_ms: int,
    window_s: Optional[int] = None,
    symbol_limit: Optional[int] = None,
    batch_size: Optional[int] = None,
    con=None,
) -> Dict[str, Any]:
    """Compute and persist TSFresh snapshots for a batch of symbols."""
    anchor_ts_ms = _bucket_start(int(ts_ms), int(TSFRESH_SNAPSHOT_BUCKET_SEC))
    window_seconds = max(60, int(window_s or TSFRESH_WINDOW_S))
    bounded_symbols = _bounded_symbols(list(symbols or []), symbol_limit=symbol_limit)
    effective_batch_size = max(1, int(batch_size if batch_size is not None else TSFRESH_SNAPSHOT_BATCH_SIZE))
    written = 0
    built = 0
    for chunk in _chunks(bounded_symbols, effective_batch_size):
        snapshot_list = [
            build_tsfresh_feature_snapshot(
                symbol=str(symbol),
                ts_ms=int(anchor_ts_ms),
                window_s=int(window_seconds),
                con=con,
            )
            for symbol in chunk
        ]
        built += int(len(snapshot_list))
        written += int(store_tsfresh_feature_snapshots(snapshot_list, con=con))
    return {
        "snapshots": int(written),
        "symbols": int(built),
        "feature_dim": int(len(get_tsfresh_feature_ids())),
        "symbol_limit": int(TSFRESH_SNAPSHOT_SYMBOL_LIMIT if symbol_limit is None else max(1, int(symbol_limit))),
        "batch_size": int(effective_batch_size),
        "window_s": int(window_seconds),
        "ts_ms": int(anchor_ts_ms),
    }
