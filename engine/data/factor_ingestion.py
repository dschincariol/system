"""
FILE: factor_ingestion.py

Data subsystem module for `factor_ingestion`.

Leakage-safe external factor ingestion utilities.

This module standardizes how external factors enter the system:
- factor_registry: metadata about factors
- factor_observations: raw observed values with (asof_ts, effective_ts, version)
- factor_features: derived/transformed features for model consumption

Key concepts:
- asof_ts: the timestamp when you learned/ingested the data (decision-time safe)
- effective_ts: the timestamp the value applies to (event time / target time)
- version: allows revisions (macro revisions, forecast model reruns, restatements)
"""

from __future__ import annotations

import csv
import io
import json
import math
import os
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import run_write_txn

try:
    from zoneinfo import ZoneInfo

    _EASTERN = ZoneInfo("America/New_York")
except Exception:
    _EASTERN = None


_ALFRED_DOWNLOAD_URL = "https://alfred.stlouisfed.org/series/downloaddata?seid={series_id}"
_ALFRED_VINTAGE_RE = re.compile(r'<option value="(\d{4}-\d{2}-\d{2})">')
_ALFRED_OBS_END_RE = re.compile(r'id="form_obs_end_date"[^>]*value="(\d{4}-\d{2}-\d{2})"')
_FRED_GRAPH_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}&cosd={obs_start}&coed={obs_end}"

_DEFAULT_MACRO_SYMBOL_MAP = {
    "rates": ["XLF", "KRE", "IAT", "JPM", "BAC", "GS", "MS", "SPY", "TLT", "IEF"],
    "cpi": ["XLP", "XLY", "XRT", "SPY", "IWM"],
    "unemployment": ["XLY", "XLI", "IWM", "SPY"],
    "gdp": ["XLI", "XLB", "XLY", "IWM", "SPY"],
    "oil": ["XLE", "USO", "OIL", "XOM", "CVX", "COP", "SLB", "HAL", "SPY"],
    "commodities": ["DBC", "GLD", "GDX", "XLB", "SPY"],
}
LOG = get_logger("engine.data.factor_ingestion")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="factor_ingestion_nonfatal",
        code=code,
        message=code,
        error=error,
        level=30,
        component="engine.data.factor_ingestion",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


@dataclass(frozen=True)
class MacroSeriesSpec:
    factor_id: str
    source_series_id: str
    family: str
    name: str
    cadence: str
    applies_to: str
    units: str
    transform: str
    release_hour_et: int
    release_minute_et: int
    is_revisioned: bool
    history_start: str
    z_window: int
    delta_lag: int
    download_mode: str = "alfred_initial"
    emit_release_event: Optional[str] = None
    symbol_topic: Optional[str] = None


MACRO_SERIES_SPECS: List[MacroSeriesSpec] = [
    MacroSeriesSpec(
        factor_id="macro.cpi_yoy",
        source_series_id="CPIAUCSL",
        family="macro",
        name="US CPI YoY",
        cadence="monthly",
        applies_to="cpi",
        units="pct",
        transform="initial_release_yoy",
        release_hour_et=8,
        release_minute_et=30,
        is_revisioned=True,
        history_start="2010-01-01",
        z_window=24,
        delta_lag=1,
        emit_release_event="CPI_RELEASE",
        symbol_topic="cpi",
    ),
    MacroSeriesSpec(
        factor_id="macro.policy_rate_upper",
        source_series_id="DFEDTARU",
        family="macro",
        name="US Fed Target Upper Bound",
        cadence="daily",
        applies_to="rates",
        units="pct",
        transform="initial_release_level",
        release_hour_et=14,
        release_minute_et=0,
        is_revisioned=False,
        history_start="2025-01-01",
        z_window=60,
        delta_lag=5,
        emit_release_event="RATE_DECISION",
        symbol_topic="rates",
    ),
    MacroSeriesSpec(
        factor_id="macro.unemployment_rate",
        source_series_id="UNRATE",
        family="macro",
        name="US Unemployment Rate",
        cadence="monthly",
        applies_to="unemployment",
        units="pct",
        transform="initial_release_level",
        release_hour_et=8,
        release_minute_et=30,
        is_revisioned=True,
        history_start="2010-01-01",
        z_window=24,
        delta_lag=1,
        symbol_topic="unemployment",
    ),
    MacroSeriesSpec(
        factor_id="macro.gdp_real_qoq_ann",
        source_series_id="GDPC1",
        family="macro",
        name="US Real GDP QoQ Annualized",
        cadence="quarterly",
        applies_to="gdp",
        units="pct",
        transform="initial_release_qoq_annualized",
        release_hour_et=8,
        release_minute_et=30,
        is_revisioned=True,
        history_start="2010-01-01",
        z_window=12,
        delta_lag=1,
        symbol_topic="gdp",
    ),
    MacroSeriesSpec(
        factor_id="macro.oil_wti_spot",
        source_series_id="DCOILWTICO",
        family="macro",
        name="WTI Spot Oil",
        cadence="daily",
        applies_to="oil",
        units="usd_per_bbl",
        transform="initial_release_level",
        release_hour_et=23,
        release_minute_et=59,
        is_revisioned=False,
        history_start="2023-01-01",
        z_window=60,
        delta_lag=5,
        download_mode="fred_graph",
        symbol_topic="oil",
    ),
    MacroSeriesSpec(
        factor_id="macro.natgas_spot",
        source_series_id="DHHNGSP",
        family="macro",
        name="Henry Hub Natural Gas Spot",
        cadence="daily",
        applies_to="commodities",
        units="usd_per_mmbtu",
        transform="initial_release_level",
        release_hour_et=23,
        release_minute_et=59,
        is_revisioned=False,
        history_start="2023-01-01",
        z_window=60,
        delta_lag=5,
        download_mode="fred_graph",
        symbol_topic="oil",
    ),
]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except Exception as e:
        _warn_nonfatal(
            "FACTOR_INGESTION_SAFE_FLOAT_FAILED",
            e,
            once_key="safe_float",
            value=repr(x)[:120],
        )
        return None


def _json_dumps(data: Dict[str, Any]) -> str:
    return json.dumps(data or {}, separators=(",", ":"), sort_keys=True)


def _rolling_zscore(values: List[float], win: int) -> float:
    if len(values) < max(3, int(win)):
        return 0.0
    window = values[-int(win):]
    mu = sum(window) / float(len(window))
    if len(window) < 2:
        return 0.0
    var = sum((x - mu) ** 2 for x in window) / float(len(window) - 1)
    sd = math.sqrt(max(0.0, var))
    if sd <= 1e-12:
        return 0.0
    z = (window[-1] - mu) / sd
    return float(z) if math.isfinite(z) else 0.0


def _parse_date(value: str) -> date:
    return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()


def _et_ts_ms(d: date, hour: int, minute: int, second: int = 0) -> int:
    if _EASTERN is not None:
        dt = datetime.combine(d, dt_time(hour=hour, minute=minute, second=second), tzinfo=_EASTERN)
    else:
        dt = datetime.combine(d, dt_time(hour=hour, minute=minute, second=second))
    return int(dt.timestamp() * 1000)


def _effective_ts_ms(d: date) -> int:
    return _et_ts_ms(d, 0, 0, 0)


def _extract_alfred_vintage_dates(html: str) -> List[str]:
    dates = _ALFRED_VINTAGE_RE.findall(str(html or ""))
    out: List[str] = []
    seen = set()
    for value in dates:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _extract_alfred_error(html: str) -> str:
    m = re.search(r'<p class="error">\s*(.*?)\s*</p>', str(html or ""), flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return "alfred_download_failed"
    text = re.sub(r"<[^>]+>", " ", m.group(1))
    text = re.sub(r"\s+", " ", text).strip()
    return text or "alfred_download_failed"


def _extract_alfred_obs_end(html: str) -> Optional[str]:
    m = _ALFRED_OBS_END_RE.search(str(html or ""))
    if not m:
        return None
    return str(m.group(1))


def _read_first_csv_from_zip(payload: bytes) -> List[Dict[str, str]]:
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        csv_names = [n for n in zf.namelist() if str(n).lower().endswith(".csv")]
        if not csv_names:
            return []
        raw = zf.read(csv_names[0]).decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(raw))
    return [dict(row or {}) for row in reader]


def _download_alfred_initial_release_rows(
    *,
    series_id: str,
    obs_start: str,
    obs_end: str,
    max_vintages: int = 1500,
    timeout_s: float = 60.0,
) -> List[Dict[str, str]]:
    url = _ALFRED_DOWNLOAD_URL.format(series_id=str(series_id))
    last_error = "alfred_unknown_error"

    for attempt in range(3):
        sess = requests.Session()
        try:
            html = sess.get(url, timeout=timeout_s).text
            vintage_dates = _extract_alfred_vintage_dates(html)
            if not vintage_dates:
                raise RuntimeError(f"alfred_vintage_dates_missing:{series_id}")
            available_obs_end = _extract_alfred_obs_end(html) or str(obs_end)
            effective_obs_end = min(str(obs_end), str(available_obs_end))

            selected = [v for v in vintage_dates if v >= str(obs_start)]
            if not selected:
                selected = vintage_dates[-240:]
            else:
                selected = selected[-max(1, int(max_vintages)):]

            form_data = {
                "form[units]": "lin",
                "form[obs_start_date]": str(obs_start),
                "form[obs_end_date]": str(effective_obs_end),
                "form[entered_vintage_dates]": " ".join(selected),
                "form[file_type]": "4",
                "form[file_format]": "csv",
                "form[download_data]": "Download data",
            }
            resp = sess.post(url, data=form_data, timeout=timeout_s)
            payload = bytes(resp.content or b"")
            if payload[:2] == b"PK":
                return _read_first_csv_from_zip(payload)
            last_error = _extract_alfred_error(resp.text)
        except Exception as e:
            last_error = f"{type(e).__name__}:{e}"
        finally:
            try:
                sess.close()
            except Exception as e:
                _warn_nonfatal("FACTOR_INGESTION_SESSION_CLOSE_FAILED", e, series_id=str(series_id))

        time.sleep(1.0 + float(attempt))

    raise RuntimeError(f"alfred_zip_missing:{series_id}:{last_error}")


def _download_fred_graph_rows(
    *,
    series_id: str,
    obs_start: str,
    obs_end: str,
    timeout_s: float = 60.0,
) -> List[Dict[str, Any]]:
    url = _FRED_GRAPH_URL.format(series_id=str(series_id), obs_start=str(obs_start), obs_end=str(obs_end))
    last_error = "fred_graph_unknown_error"
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=timeout_s)
            resp.raise_for_status()
            reader = csv.DictReader(io.StringIO(resp.text))
            out: List[Dict[str, Any]] = []
            for row in reader:
                observation_date = str(row.get("observation_date") or "").strip()
                value = _safe_float(row.get(str(series_id)))
                if not observation_date:
                    continue
                out.append(
                    {
                        "observation_date": observation_date,
                        "release_date": observation_date,
                        "value": value,
                    }
                )
            out.sort(key=lambda r: str(r["observation_date"]))
            return out
        except Exception as e:
            last_error = f"{type(e).__name__}:{e}"
            time.sleep(1.0 + float(attempt))
    raise RuntimeError(f"fred_graph_failed:{series_id}:{last_error}")


def _normalize_initial_release_rows(rows: Iterable[Dict[str, str]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        observation_date = str(row.get("period_start_date") or row.get("observation_date") or "").strip()
        release_date = str(row.get("realtime_start_date") or row.get("realtime_start") or "").strip()
        value_key = next(
            (
                k
                for k in row.keys()
                if k not in {"period_start_date", "observation_date", "realtime_start_date", "realtime_start"}
            ),
            None,
        )
        value = _safe_float(row.get(value_key)) if value_key else None
        if not observation_date or not release_date:
            continue
        out.append(
            {
                "observation_date": observation_date,
                "release_date": release_date,
                "value": value,
            }
        )
    out.sort(key=lambda r: (str(r["release_date"]), str(r["observation_date"])))
    return out


def _load_source_rows_for_spec(spec: MacroSeriesSpec, *, obs_end: str) -> List[Dict[str, Any]]:
    if str(spec.download_mode).strip().lower() == "fred_graph":
        return _download_fred_graph_rows(
            series_id=spec.source_series_id,
            obs_start=str(spec.history_start),
            obs_end=str(obs_end),
        )
    return _normalize_initial_release_rows(
        _download_alfred_initial_release_rows(
            series_id=spec.source_series_id,
            obs_start=str(spec.history_start),
            obs_end=str(obs_end),
            max_vintages=(450 if str(spec.cadence).strip().lower() == "daily" else 1500),
        )
    )


def _build_factor_rows_from_source(spec: MacroSeriesSpec, source_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    prior_release_value: Optional[float] = None

    for idx, row in enumerate(source_rows):
        base_value = _safe_float(row.get("value"))
        observation_date = _parse_date(str(row["observation_date"]))
        release_date = _parse_date(str(row["release_date"]))
        transformed: Optional[float] = None

        if spec.transform == "initial_release_level":
            transformed = base_value
        elif spec.transform == "initial_release_yoy":
            prev = _safe_float(source_rows[idx - 12].get("value")) if idx >= 12 else None
            if base_value is not None and prev is not None and abs(prev) > 1e-12:
                transformed = ((base_value / prev) - 1.0) * 100.0
        elif spec.transform == "initial_release_qoq_annualized":
            prev = prior_release_value
            if base_value is not None and prev is not None and abs(prev) > 1e-12:
                transformed = ((base_value / prev) ** 4.0 - 1.0) * 100.0
        else:
            transformed = base_value

        if base_value is not None:
            prior_release_value = base_value

        if transformed is None:
            continue

        out.append(
            {
                "factor_id": spec.factor_id,
                "asof_ts": _et_ts_ms(release_date, spec.release_hour_et, spec.release_minute_et),
                "effective_ts": _effective_ts_ms(observation_date),
                "value": transformed,
                "version": 1,
                "meta": {
                    "source_series_id": spec.source_series_id,
                    "observation_date": row["observation_date"],
                    "release_date": row["release_date"],
                    "raw_value": base_value,
                    "transform": spec.transform,
                    "initial_release_only": True,
                },
            }
        )
    return out


def _materialize_release_features(
    con,
    *,
    factor_id: str,
    rows: List[Dict[str, Any]],
    z_window: int,
    delta_lag: int,
) -> int:
    level_feature_id = str(factor_id)
    z_feature_id = f"{factor_id}_z"
    delta_feature_id = f"{factor_id}_d{int(delta_lag)}"
    hist: List[float] = []
    written = 0

    for row in sorted(rows or [], key=lambda r: (int(r["asof_ts"]), int(r["effective_ts"]), int(r.get("version") or 1))):
        value = _safe_float(row.get("value"))
        if value is None:
            continue

        hist.append(float(value))
        delta = 0.0
        if len(hist) > int(delta_lag):
            delta = float(hist[-1] - hist[-(int(delta_lag) + 1)])
        z = _rolling_zscore(hist, int(z_window))
        feature_meta = {
            "src_factor_id": str(factor_id),
            "observation_date": (row.get("meta") or {}).get("observation_date"),
            "release_date": (row.get("meta") or {}).get("release_date"),
        }

        put_factor_feature(
            con,
            feature_id=level_feature_id,
            asof_ts=int(row["asof_ts"]),
            effective_ts=int(row["effective_ts"]),
            value=float(value),
            meta=dict(feature_meta, feature_kind="level"),
        )
        put_factor_feature(
            con,
            feature_id=z_feature_id,
            asof_ts=int(row["asof_ts"]),
            effective_ts=int(row["effective_ts"]),
            value=float(z),
            meta=dict(feature_meta, feature_kind="zscore", z_window=int(z_window)),
        )
        put_factor_feature(
            con,
            feature_id=delta_feature_id,
            asof_ts=int(row["asof_ts"]),
            effective_ts=int(row["effective_ts"]),
            value=float(delta),
            meta=dict(feature_meta, feature_kind="delta", delta_lag=int(delta_lag)),
        )
        written += 3

    return int(written)


def _load_macro_symbol_map() -> Dict[str, List[str]]:
    mapping = {str(k): list(v) for k, v in _DEFAULT_MACRO_SYMBOL_MAP.items()}
    raw = str(os.environ.get("MACRO_SYMBOL_MAP_JSON") or "").strip()
    if not raw:
        return mapping
    try:
        parsed = json.loads(raw)
    except Exception as e:
        _warn_nonfatal(
            "FACTOR_INGESTION_MACRO_SYMBOL_MAP_PARSE_FAILED",
            e,
            once_key="macro_symbol_map_parse",
        )
        return mapping
    if not isinstance(parsed, dict):
        return mapping

    for key, values in parsed.items():
        if isinstance(values, list):
            cleaned = []
            seen = set()
            for value in values:
                sym = str(value or "").upper().strip()
                if not sym or sym in seen:
                    continue
                seen.add(sym)
                cleaned.append(sym)
            if cleaned:
                mapping[str(key)] = cleaned
    return mapping


def macro_target_symbols(con, *, topic: Optional[str], limit: int = 12) -> List[str]:
    defaults = list((_load_macro_symbol_map().get(str(topic or "").strip().lower()) or []))
    if not defaults:
        return []
    try:
        from engine.data.universe import get_active_symbols

        active = {str(s).upper().strip() for s in get_active_symbols(con, limit=5000)}
    except Exception:
        active = set()

    eligible = [sym for sym in defaults if not active or sym in active]
    if not eligible:
        eligible = defaults
    return eligible[: max(1, int(limit))]


def _format_pct(value: Optional[float]) -> str:
    v = _safe_float(value)
    if v is None:
        return "n/a"
    return f"{v:.2f}%"


def _build_macro_release_payloads(
    con,
    *,
    event_type: str,
    spec: MacroSeriesSpec,
    row: Dict[str, Any],
    title: str,
    body: str,
    extra_features: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    meta = dict(row.get("meta") or {})
    symbols = macro_target_symbols(con, topic=spec.symbol_topic or spec.applies_to)
    if not symbols:
        symbols = [None]

    out: List[Dict[str, Any]] = []
    for symbol in symbols:
        source_id = ":".join(
            [
                str(event_type),
                str(spec.factor_id),
                str(meta.get("release_date") or ""),
                str(meta.get("observation_date") or ""),
                str(symbol or "GLOBAL"),
            ]
        )
        out.append(
            {
                "event_type": str(event_type),
                "ts_ms": int(row["asof_ts"]),
                "timestamp": int(row["asof_ts"]),
                "source": "macro",
                "symbol": symbol,
                "title": str(title),
                "body": str(body),
                "source_id": source_id,
                "event_key": source_id,
                "provider": "alfred",
                "topic": spec.applies_to,
                "factor_id": spec.factor_id,
                "release_date": meta.get("release_date"),
                "observation_date": meta.get("observation_date"),
                "value": row.get("value"),
                "raw_value": meta.get("raw_value"),
                "initial_release_only": True,
                "mapped_symbols": symbols,
                "derived_features": dict(extra_features or {}),
            }
        )
    return out


def _macro_event_payloads_for_spec(con, *, spec: MacroSeriesSpec, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not spec.emit_release_event:
        return []

    payloads: List[Dict[str, Any]] = []
    if spec.emit_release_event == "CPI_RELEASE":
        for row in rows:
            title = f"CPI release: {_format_pct(row.get('value'))} YoY"
            meta = dict(row.get("meta") or {})
            body = (
                f"Initial CPI release for {meta.get('observation_date')}. "
                f"YoY inflation printed {_format_pct(row.get('value'))}."
            )
            payloads.extend(
                _build_macro_release_payloads(
                    con,
                    event_type="CPI_RELEASE",
                    spec=spec,
                    row=row,
                    title=title,
                    body=body,
                    extra_features={"macro_topic": "cpi", "release_value": row.get("value")},
                )
            )
        return payloads

    if spec.emit_release_event == "RATE_DECISION":
        prev_value: Optional[float] = None
        for row in rows:
            value = _safe_float(row.get("value"))
            if value is None:
                continue
            if prev_value is not None and abs(value - prev_value) > 1e-12:
                delta = float(value - prev_value)
                direction = "hike" if delta > 0 else "cut"
                title = f"Rate decision: {_format_pct(value)} target upper bound"
                meta = dict(row.get("meta") or {})
                body = (
                    f"Fed target upper bound changed to {_format_pct(value)} "
                    f"for {meta.get('observation_date')} ({direction} {_format_pct(abs(delta))})."
                )
                payloads.extend(
                    _build_macro_release_payloads(
                        con,
                        event_type="RATE_DECISION",
                        spec=spec,
                        row=row,
                        title=title,
                        body=body,
                        extra_features={
                            "macro_topic": "rates",
                            "release_value": value,
                            "previous_value": prev_value,
                            "delta": delta,
                            "direction": direction,
                        },
                    )
                )
            prev_value = value
        return payloads

    return payloads


def emit_macro_release_events(con, *, spec: MacroSeriesSpec, rows: List[Dict[str, Any]]) -> int:
    payloads = _macro_event_payloads_for_spec(con, spec=spec, rows=rows)
    if not payloads:
        return 0

    from engine.data.event_normalization import normalize_macro_event
    from engine.runtime.storage import put_normalized_event

    emitted = 0
    for payload in payloads:
        try:
            put_normalized_event(normalize_macro_event(payload), con=con)
            emitted += 1
        except Exception as e:
            _warn_nonfatal(
                "FACTOR_INGESTION_MACRO_EVENT_EMIT_FAILED",
                e,
                once_key="macro_event_emit",
                factor_id=str(spec.factor_id),
                payload_symbol=str(payload.get("symbol") or ""),
            )
            continue
    return int(emitted)


def ensure_factor_registry(
    con,
    *,
    factor_id: str,
    family: str,
    name: str,
    cadence: str,
    release_lag_sec: int = 0,
    applies_to: Optional[str] = None,
    units: Optional[str] = None,
    transform: Optional[str] = None,
    is_revisioned: bool = False,
    source: Optional[str] = None,
    enabled: bool = True,
) -> None:
    """
    Idempotent upsert into factor_registry.
    """
    con.execute(
        """
        INSERT OR REPLACE INTO factor_registry(
          factor_id, family, name, cadence, release_lag_sec,
          applies_to, units, transform, is_revisioned, source, enabled
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            str(factor_id),
            str(family),
            str(name),
            str(cadence),
            int(release_lag_sec or 0),
            (str(applies_to) if applies_to else None),
            (str(units) if units else None),
            (str(transform) if transform else None),
            (1 if bool(is_revisioned) else 0),
            (str(source) if source else None),
            (1 if bool(enabled) else 0),
        ),
    )


def put_factor_observation(
    con,
    *,
    factor_id: str,
    asof_ts: int,
    effective_ts: int,
    value: Optional[float],
    version: int = 1,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Inserts a raw observation into factor_observations (revision-safe).
    """
    con.execute(
        """
        INSERT OR REPLACE INTO factor_observations(
          factor_id, asof_ts, effective_ts, value, version, meta_json
        )
        VALUES (?,?,?,?,?,?)
        """,
        (
            str(factor_id),
            int(asof_ts),
            int(effective_ts),
            _safe_float(value),
            int(version or 1),
            _json_dumps(meta or {}),
        ),
    )


def get_factor_value_asof(
    con,
    *,
    factor_id: str,
    ts_ms: int,
) -> Optional[float]:
    """
    Leakage-safe as-of lookup:
    - eligible rows have asof_ts <= ts_ms AND effective_ts <= ts_ms
    - picks latest by asof_ts, then effective_ts, then version
    """
    row = con.execute(
        """
        SELECT value
        FROM factor_observations
        WHERE factor_id=?
          AND asof_ts <= ?
          AND effective_ts <= ?
        ORDER BY asof_ts DESC, effective_ts DESC, version DESC
        LIMIT 1
        """,
        (str(factor_id), int(ts_ms), int(ts_ms)),
    ).fetchone()
    if not row:
        return None
    return _safe_float(row[0])


def put_factor_feature(
    con,
    *,
    feature_id: str,
    asof_ts: int,
    effective_ts: int,
    value: Optional[float],
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Inserts a derived feature value into factor_features.
    """
    con.execute(
        """
        INSERT OR REPLACE INTO factor_features(
          feature_id, asof_ts, effective_ts, value, meta_json
        )
        VALUES (?,?,?,?,?)
        """,
        (
            str(feature_id),
            int(asof_ts),
            int(effective_ts),
            _safe_float(value) or 0.0,
            _json_dumps(meta or {}),
        ),
    )


def materialize_simple_feature(
    con,
    *,
    factor_id: str,
    feature_id: str,
    ts_ms: int,
    default: float = 0.0,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Convenience: read factor value as-of ts_ms and write to factor_features.
    """
    v = get_factor_value_asof(con, factor_id=str(factor_id), ts_ms=int(ts_ms))
    put_factor_feature(
        con,
        feature_id=str(feature_id),
        asof_ts=int(ts_ms),
        effective_ts=int(ts_ms),
        value=(v if v is not None else float(default)),
        meta=meta or {"src_factor_id": str(factor_id)},
    )


def ingest_batch_observations(
    *,
    factor_id: str,
    rows: List[Tuple[int, int, Optional[float], int, Optional[Dict[str, Any]]]],
    registry: Optional[Dict[str, Any]] = None,
) -> int:
    """
    High-throughput helper:
    rows = [(asof_ts, effective_ts, value, version, meta_dict), ...]

    If registry is provided, it is used to ensure factor_registry is populated.
    Returns number of inserted rows (best-effort).
    """
    def _write(con) -> int:
        inserted = 0
        if registry:
            ensure_factor_registry(con, factor_id=str(factor_id), **registry)

        for asof_ts, effective_ts, value, version, meta in (rows or []):
            put_factor_observation(
                con,
                factor_id=str(factor_id),
                asof_ts=int(asof_ts),
                effective_ts=int(effective_ts),
                value=value,
                version=int(version or 1),
                meta=meta,
            )
            inserted += 1

        return int(inserted)
    return int(
        run_write_txn(
            _write,
            table="factor_observations",
            operation="ingest_batch_observations",
            context={"factor_id": str(factor_id), "rows": int(len(rows or []))},
        )
        or 0
    )


def ensure_macro_factor_registry(con) -> None:
    for spec in MACRO_SERIES_SPECS:
        ensure_factor_registry(
            con,
            factor_id=spec.factor_id,
            family=spec.family,
            name=spec.name,
            cadence=spec.cadence,
            release_lag_sec=0,
            applies_to=spec.applies_to,
            units=spec.units,
            transform=spec.transform,
            is_revisioned=spec.is_revisioned,
            source="alfred",
            enabled=True,
        )


def sync_macro_factors(*, now_ms: Optional[int] = None) -> Dict[str, Any]:
    """
    Fetches point-in-time macro series, writes observations and derived features,
    and emits targeted macro release events.
    """
    _ = int(now_ms or _now_ms())
    obs_end = date.today().isoformat()
    summary: Dict[str, Any] = {
        "series": 0,
        "observation_rows": 0,
        "feature_rows": 0,
        "event_rows": 0,
        "errors": [],
        "series_status": {},
    }
    def _write(con) -> Dict[str, Any]:
        ensure_macro_factor_registry(con)

        for spec in MACRO_SERIES_SPECS:
            series_info: Dict[str, Any] = {"ok": False, "factor_id": spec.factor_id, "source_series_id": spec.source_series_id}
            try:
                source_rows = _load_source_rows_for_spec(spec, obs_end=str(obs_end))
                factor_rows = _build_factor_rows_from_source(spec, source_rows)

                for row in factor_rows:
                    put_factor_observation(
                        con,
                        factor_id=str(spec.factor_id),
                        asof_ts=int(row["asof_ts"]),
                        effective_ts=int(row["effective_ts"]),
                        value=row.get("value"),
                        version=int(row.get("version") or 1),
                        meta=dict(row.get("meta") or {}),
                    )

                feature_rows = _materialize_release_features(
                    con,
                    factor_id=spec.factor_id,
                    rows=factor_rows,
                    z_window=int(spec.z_window),
                    delta_lag=int(spec.delta_lag),
                )
                event_rows = emit_macro_release_events(con, spec=spec, rows=factor_rows)

                summary["series"] += 1
                summary["observation_rows"] += len(factor_rows)
                summary["feature_rows"] += int(feature_rows)
                summary["event_rows"] += int(event_rows)
                series_info.update(
                    {
                        "ok": True,
                        "observations": int(len(factor_rows)),
                        "features": int(feature_rows),
                        "events": int(event_rows),
                    }
                )
            except Exception as e:
                err = f"{spec.factor_id}:{type(e).__name__}:{e}"
                summary["errors"].append(err)
                series_info["error"] = str(err)
            summary["series_status"][spec.factor_id] = series_info

        return summary
    return run_write_txn(
        _write,
        table="events",
        operation="sync_macro_factors",
        context={"series": int(len(MACRO_SERIES_SPECS))},
    )


def example_register_and_ingest_skeleton() -> None:
    """
    Non-executing example function you can call manually.
    Keeps module self-contained for future ingest jobs.
    """
    ts = _now_ms()
    def _write(con) -> None:
        ensure_factor_registry(
            con,
            factor_id="macro.cpi_yoy",
            family="macro",
            name="US CPI YoY",
            cadence="monthly",
            release_lag_sec=3600,
            units="pct",
            transform="zscore_24m",
            is_revisioned=True,
            source="bls",
            enabled=True,
        )
        put_factor_observation(
            con,
            factor_id="macro.cpi_yoy",
            asof_ts=ts,
            effective_ts=ts,
            value=3.1,
            version=1,
            meta={"note": "example"},
        )
    run_write_txn(
        _write,
        table="factor_observations",
        operation="example_register_and_ingest_skeleton",
        context={"factor_id": "macro.cpi_yoy"},
    )
