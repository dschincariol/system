"""Governed benchmark runner for time-series foundation model challengers."""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

import numpy as np

from engine.strategy.ensemble.oos_store import ensure_schema as ensure_oos_schema
from engine.strategy.ensemble.oos_store import upsert_oos_predictions
from engine.strategy.model_competition.repository import CompetitionRepository
from engine.strategy.tsfm_adapters import (
    TSFMAdapterConfig,
    TSFMAdapterUnavailable,
    TSFMSeriesContext,
    adapter_config_from_env,
    create_tsfm_adapter,
    normalize_tsfm_backend,
    tsfm_feature_ids_for_backend,
)


TSFM_BENCHMARK_ARTIFACT_KIND = "tsfm_benchmark_manifest"
TSFM_BENCHMARK_STAGE = "shadow"
DEFAULT_BASELINES = ("trailing", "har", "garch", "lightgbm", "patchtst", "itransformer")
DEFAULT_TASKS = ("forecast", "realized_volatility")
MODEL_BASELINE_FAMILIES: dict[str, tuple[str, ...]] = {
    "lightgbm": ("lgbm_regressor", "lightgbm", "lgbm_ranker"),
    "patchtst": ("patchtst", "PatchTST", "patchtst_regressor"),
    "itransformer": ("itransformer", "iTransformer", "itransformer_regressor"),
}


@dataclass(frozen=True)
class TSFMBenchmarkConfig:
    run_id: str = ""
    symbols: tuple[str, ...] = ("SPY",)
    adapters: tuple[str, ...] = ("chronos",)
    baselines: tuple[str, ...] = DEFAULT_BASELINES
    tasks: tuple[str, ...] = DEFAULT_TASKS
    horizon: int = 1
    context_length: int = 64
    min_context: int = 16
    step: int = 1
    max_eval_points: int = 100
    embedding_dim: int = 16
    quantiles: tuple[float, ...] = (0.1, 0.5, 0.9)
    device: str = "cpu"
    local_files_only: bool = True
    fallback: str = "skip"
    require_artifact_persistence: bool = True
    risk_register: bool = True
    model_ids: Mapping[str, str] = field(default_factory=dict)
    asset_class_by_symbol: Mapping[str, str] = field(default_factory=dict)
    series_by_symbol: Mapping[str, Sequence[tuple[int, float]]] = field(default_factory=dict)

    def normalized_run_id(self) -> str:
        return str(self.run_id or f"tsfm-benchmark-{uuid.uuid4()}").strip()


@dataclass(frozen=True)
class PITWindow:
    symbol: str
    asset_class: str
    task: str
    context: TSFMSeriesContext
    target_ts_ms: int
    target_value: float
    future_values: tuple[float, ...]


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _json_dumps(value: Any) -> str:
    return json.dumps(_json_sanitize(value), separators=(",", ":"), sort_keys=True, allow_nan=False)


def _json_sanitize(value: Any) -> Any:
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Mapping):
        return {str(key): _json_sanitize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_sanitize(item) for item in value]
    return value


def _json_param(con: Any, value: Any) -> Any:
    payload = _json_sanitize(value)
    if "sqlite" in type(con).__module__.lower():
        return _json_dumps(payload)
    try:
        from psycopg.types.json import Jsonb

        return Jsonb(payload)
    except Exception:
        return _json_dumps(payload)


def _commit_if_possible(con: Any) -> None:
    commit = getattr(con, "commit", None)
    if callable(commit):
        commit()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _series_digest(series: Sequence[tuple[int, float]]) -> str:
    payload = _json_dumps([(int(ts), float(value)) for ts, value in series]).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _normalize_price_series(rows: Sequence[tuple[Any, Any]]) -> tuple[tuple[int, float], ...]:
    out: list[tuple[int, float]] = []
    prev = -1
    for raw_ts, raw_value in rows:
        ts_ms = _safe_int(raw_ts, 0)
        value = _safe_float(raw_value, math.nan)
        if ts_ms <= 0 or ts_ms <= prev or not math.isfinite(value):
            continue
        prev = ts_ms
        out.append((int(ts_ms), float(value)))
    return tuple(out)


def _load_price_series(con: Any, symbol: str) -> tuple[tuple[int, float], ...]:
    rows = con.execute(
        """
        SELECT ts_ms, COALESCE(price, px) AS value
        FROM prices
        WHERE symbol=?
          AND COALESCE(price, px) IS NOT NULL
        ORDER BY ts_ms ASC
        """,
        (str(symbol).upper().strip(),),
    ).fetchall()
    return _normalize_price_series([(row[0], row[1]) for row in rows or []])


def _asset_class_for_symbol(symbol: str, overrides: Mapping[str, str]) -> str:
    override = str(overrides.get(str(symbol).upper().strip()) or "").strip().lower()
    if override:
        return override
    try:
        from engine.data.asset_map import asset_class_for_symbol

        return str(asset_class_for_symbol(str(symbol))).strip().lower()
    except Exception:
        return "equity"


def _returns(values: Sequence[float]) -> np.ndarray:
    arr = np.asarray([float(value) for value in values], dtype=np.float64)
    if arr.size < 2:
        return np.asarray([], dtype=np.float64)
    if np.all(arr > 0.0):
        return np.diff(np.log(arr))
    return np.diff(arr)


def _realized_volatility(values: Sequence[float]) -> float:
    rets = _returns(values)
    if rets.size == 0:
        return 0.0
    vol = float(np.nanstd(rets))
    return float(vol if math.isfinite(vol) else 0.0)


def _target_for_task(task: str, context_values: Sequence[float], future_values: Sequence[float]) -> float:
    task_key = str(task or "forecast").strip().lower()
    if task_key in {"realized_volatility", "rv", "volatility"}:
        return _realized_volatility([*(float(v) for v in context_values[-1:]), *(float(v) for v in future_values)])
    return float(future_values[-1])


def build_pit_window(
    *,
    symbol: str,
    asset_class: str,
    task: str,
    series: Sequence[tuple[int, float]],
    context_end_index: int,
    context_length: int,
    horizon: int,
) -> PITWindow:
    ordered = tuple(series)
    end = int(context_end_index)
    h = max(1, int(horizon))
    if end < 0 or end >= len(ordered):
        raise ValueError("tsfm_pit_context_index_out_of_range")
    start = max(0, end - max(1, int(context_length)) + 1)
    context_points = ordered[start : end + 1]
    future_points = ordered[end + 1 : end + 1 + h]
    if len(future_points) < h:
        raise ValueError("tsfm_pit_target_missing")
    asof_ts_ms = int(context_points[-1][0])
    target_ts_ms = int(future_points[-1][0])
    if target_ts_ms <= asof_ts_ms:
        raise ValueError("tsfm_pit_target_not_after_asof")
    if any(int(ts) > asof_ts_ms for ts, _value in context_points):
        raise ValueError("tsfm_pit_context_after_asof")
    context = TSFMSeriesContext(
        symbol=str(symbol).upper().strip(),
        timestamps_ms=tuple(int(ts) for ts, _value in context_points),
        values=tuple(float(value) for _ts, value in context_points),
        asof_ts_ms=int(asof_ts_ms),
        asset_class=str(asset_class or ""),
        task=str(task or "forecast"),
    )
    context.validate_pit()
    future_values = tuple(float(value) for _ts, value in future_points)
    return PITWindow(
        symbol=str(symbol).upper().strip(),
        asset_class=str(asset_class or ""),
        task=str(task or "forecast"),
        context=context,
        target_ts_ms=int(target_ts_ms),
        target_value=_target_for_task(str(task), context.values, future_values),
        future_values=future_values,
    )


def walk_forward_windows(
    *,
    symbol: str,
    asset_class: str,
    task: str,
    series: Sequence[tuple[int, float]],
    config: TSFMBenchmarkConfig,
) -> list[PITWindow]:
    min_context = max(1, int(config.min_context))
    context_length = max(min_context, int(config.context_length))
    horizon = max(1, int(config.horizon))
    step = max(1, int(config.step))
    max_points = max(1, int(config.max_eval_points))
    n = len(series)
    first_end = min_context - 1
    last_end = n - horizon - 1
    if last_end < first_end:
        return []
    candidates = list(range(first_end, last_end + 1, step))
    if len(candidates) > max_points:
        candidates = candidates[-max_points:]
    return [
        build_pit_window(
            symbol=symbol,
            asset_class=asset_class,
            task=task,
            series=series,
            context_end_index=idx,
            context_length=context_length,
            horizon=horizon,
        )
        for idx in candidates
    ]


def ensure_tsfm_benchmark_schema(con: Any) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS tsfm_benchmark_runs (
            run_id TEXT PRIMARY KEY,
            created_ts_ms BIGINT NOT NULL,
            updated_ts_ms BIGINT NOT NULL,
            status TEXT NOT NULL,
            stage TEXT NOT NULL DEFAULT 'shadow',
            config_json JSONB NOT NULL,
            artifact_alias TEXT,
            artifact_sha256 TEXT,
            summary_json JSONB NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS tsfm_benchmark_rows (
            run_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            task TEXT NOT NULL,
            family TEXT NOT NULL,
            row_kind TEXT NOT NULL,
            ts_ms BIGINT NOT NULL,
            target_ts_ms BIGINT NOT NULL,
            horizon_s BIGINT NOT NULL,
            prediction DOUBLE PRECISION,
            target DOUBLE PRECISION,
            abs_error DOUBLE PRECISION,
            squared_error DOUBLE PRECISION,
            quantiles_json JSONB,
            horizon_path_json JSONB,
            feature_snapshot_json JSONB,
            latency_ms DOUBLE PRECISION,
            resource_json JSONB,
            provenance_json JSONB,
            status TEXT NOT NULL,
            created_ts_ms BIGINT NOT NULL,
            PRIMARY KEY(run_id, symbol, task, family, row_kind, ts_ms, horizon_s)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tsfm_benchmark_rows_symbol_ts
          ON tsfm_benchmark_rows(symbol, task, ts_ms)
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS tsfm_risk_inputs (
            run_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            ts_ms BIGINT NOT NULL,
            target_ts_ms BIGINT NOT NULL,
            horizon_s BIGINT NOT NULL,
            adapter TEXT NOT NULL,
            risk_input_kind TEXT NOT NULL,
            value DOUBLE PRECISION NOT NULL,
            stage TEXT NOT NULL DEFAULT 'shadow',
            provenance_json JSONB NOT NULL,
            created_ts_ms BIGINT NOT NULL,
            PRIMARY KEY(run_id, symbol, ts_ms, horizon_s, adapter, risk_input_kind)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tsfm_risk_inputs_symbol_ts
          ON tsfm_risk_inputs(symbol, risk_input_kind, ts_ms)
        """
    )
    _ensure_model_versions_schema(con)
    _ensure_marketplace_schema(con)
    _ensure_governance_log_schema(con)
    ensure_oos_schema(con)


def _ensure_model_versions_schema(con: Any) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS model_versions (
            model_name TEXT NOT NULL,
            model_version TEXT NOT NULL,
            model_kind TEXT NOT NULL,
            parent_version TEXT,
            mutation_kind TEXT NOT NULL DEFAULT 'baseline_retrain',
            stage TEXT NOT NULL DEFAULT 'shadow',
            status TEXT NOT NULL DEFAULT 'candidate',
            live_ready BIGINT NOT NULL DEFAULT 0,
            training_job_name TEXT,
            train_scope_json JSONB,
            meta_json JSONB,
            created_ts_ms BIGINT NOT NULL,
            updated_ts_ms BIGINT NOT NULL,
            PRIMARY KEY(model_name, model_version)
        )
        """
    )


def _ensure_marketplace_schema(con: Any) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS model_marketplace_scores (
            model_id TEXT NOT NULL DEFAULT 'baseline',
            model_name TEXT NOT NULL,
            symbol TEXT NOT NULL,
            horizon_s BIGINT NOT NULL DEFAULT 0,
            regime TEXT NOT NULL DEFAULT 'global',
            stage TEXT NOT NULL DEFAULT 'challenger',
            score DOUBLE PRECISION NOT NULL DEFAULT 0,
            trades BIGINT NOT NULL DEFAULT 0,
            wins BIGINT NOT NULL DEFAULT 0,
            losses BIGINT NOT NULL DEFAULT 0,
            gross_pnl DOUBLE PRECISION NOT NULL DEFAULT 0,
            net_pnl DOUBLE PRECISION NOT NULL DEFAULT 0,
            avg_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
            last_signal_ts_ms BIGINT,
            updated_ts_ms BIGINT NOT NULL,
            meta_json JSONB,
            PRIMARY KEY(model_id, model_name, symbol, horizon_s, regime)
        )
        """
    )


def _ensure_governance_log_schema(con: Any) -> None:
    id_ddl = "INTEGER PRIMARY KEY AUTOINCREMENT" if "sqlite" in type(con).__module__.lower() else "BIGSERIAL PRIMARY KEY"
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS model_governance_log (
            id {id_ddl},
            ts_ms BIGINT NOT NULL,
            source TEXT NOT NULL,
            regime TEXT NOT NULL DEFAULT 'global',
            champion_name TEXT,
            challenger_name TEXT,
            status TEXT,
            summary_json JSONB NOT NULL
        )
        """
    )


def _persist_manifest(
    *,
    config: TSFMBenchmarkConfig,
    run_id: str,
    adapter_descriptions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    manifest = {
        "artifact_schema_version": 1,
        "artifact_kind": TSFM_BENCHMARK_ARTIFACT_KIND,
        "run_id": str(run_id),
        "stage": TSFM_BENCHMARK_STAGE,
        "direct_trading_authority": False,
        "config": _config_to_json(config, include_series=False),
        "adapters": [dict(item) for item in adapter_descriptions],
    }
    payload = _json_dumps(manifest).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    out = {
        "artifact_alias": f"benchmark:tsfm:{run_id}:manifest",
        "artifact_sha256": digest,
        "artifact_persisted": False,
        "artifact_kind": TSFM_BENCHMARK_ARTIFACT_KIND,
        "artifact_manifest": manifest,
    }
    try:
        from engine.artifacts.store import LocalArtifactStore

        ref = LocalArtifactStore().put(
            payload,
            content_type="application/json",
            kind=TSFM_BENCHMARK_ARTIFACT_KIND,
            alias=str(out["artifact_alias"]),
            metadata=manifest,
        )
        out.update(
            {
                "artifact_sha256": str(ref.sha256),
                "artifact_created_ts_ms": int(ref.created_ts.timestamp() * 1000),
                "artifact_persisted": True,
            }
        )
    except Exception as exc:
        if bool(config.require_artifact_persistence):
            raise
        out["artifact_error"] = f"{type(exc).__name__}: {exc}"
    return out


def _config_to_json(config: TSFMBenchmarkConfig, *, include_series: bool = False) -> dict[str, Any]:
    payload = {
        "symbols": list(config.symbols),
        "adapters": list(config.adapters),
        "baselines": list(config.baselines),
        "tasks": list(config.tasks),
        "horizon": int(config.horizon),
        "context_length": int(config.context_length),
        "min_context": int(config.min_context),
        "step": int(config.step),
        "max_eval_points": int(config.max_eval_points),
        "embedding_dim": int(config.embedding_dim),
        "quantiles": list(config.quantiles),
        "device": str(config.device),
        "local_files_only": bool(config.local_files_only),
        "fallback": str(config.fallback),
        "require_artifact_persistence": bool(config.require_artifact_persistence),
        "risk_register": bool(config.risk_register),
        "model_ids": dict(config.model_ids),
        "asset_class_by_symbol": dict(config.asset_class_by_symbol),
    }
    if include_series:
        payload["series_by_symbol"] = {
            str(symbol): [(int(ts), float(value)) for ts, value in rows]
            for symbol, rows in dict(config.series_by_symbol).items()
        }
    else:
        payload["series_by_symbol"] = {
            str(symbol): {"rows": len(rows), "sha256": _series_digest(rows)}
            for symbol, rows in dict(config.series_by_symbol).items()
        }
    return payload


def _row_metrics(prediction: float | None, target: float | None) -> tuple[float | None, float | None]:
    if prediction is None or target is None:
        return None, None
    pred = _safe_float(prediction, math.nan)
    tgt = _safe_float(target, math.nan)
    if not math.isfinite(pred) or not math.isfinite(tgt):
        return None, None
    error = float(pred - tgt)
    return abs(error), error * error


def _insert_benchmark_row(
    con: Any,
    *,
    run_id: str,
    window: PITWindow,
    family: str,
    row_kind: str,
    prediction: float | None,
    quantiles: Mapping[str, Sequence[float]] | None,
    horizon_path: Sequence[float] | None,
    feature_snapshot: Mapping[str, Any] | None,
    latency_ms: float | None,
    resource: Mapping[str, Any] | None,
    provenance: Mapping[str, Any] | None,
    status: str,
) -> None:
    abs_error, squared_error = _row_metrics(prediction, window.target_value)
    con.execute(
        """
        INSERT INTO tsfm_benchmark_rows(
          run_id, symbol, asset_class, task, family, row_kind, ts_ms, target_ts_ms,
          horizon_s, prediction, target, abs_error, squared_error, quantiles_json,
          horizon_path_json, feature_snapshot_json, latency_ms, resource_json,
          provenance_json, status, created_ts_ms
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(run_id, symbol, task, family, row_kind, ts_ms, horizon_s) DO UPDATE SET
          prediction=excluded.prediction,
          target=excluded.target,
          abs_error=excluded.abs_error,
          squared_error=excluded.squared_error,
          quantiles_json=excluded.quantiles_json,
          horizon_path_json=excluded.horizon_path_json,
          feature_snapshot_json=excluded.feature_snapshot_json,
          latency_ms=excluded.latency_ms,
          resource_json=excluded.resource_json,
          provenance_json=excluded.provenance_json,
          status=excluded.status
        """,
        (
            str(run_id),
            str(window.symbol),
            str(window.asset_class),
            str(window.task),
            str(family),
            str(row_kind),
            int(window.context.asof_ts_ms),
            int(window.target_ts_ms),
            int(window.context.asof_ts_ms - window.context.asof_ts_ms + window.context.asof_ts_ms) * 0
            + int(len(window.future_values)),
            prediction,
            float(window.target_value),
            abs_error,
            squared_error,
            _json_param(con, dict(quantiles or {})),
            _json_param(con, list(horizon_path or [])),
            _json_param(con, dict(feature_snapshot or {})),
            latency_ms,
            _json_param(con, dict(resource or {})),
            _json_param(con, dict(provenance or {})),
            str(status),
            _now_ms(),
        ),
    )


def _oos_family(family: str) -> str:
    if str(family).startswith("tsfm."):
        return str(family)
    return f"baseline.{family}"


def _persist_oos_row(
    con: Any,
    *,
    run_id: str,
    window: PITWindow,
    family: str,
    prediction: float,
) -> None:
    upsert_oos_predictions(
        [
            {
                "symbol": str(window.symbol),
                "horizon": int(len(window.future_values)),
                "family": _oos_family(family),
                "ts": int(window.context.asof_ts_ms),
                "run_id": str(run_id),
                "prediction": float(prediction),
                "target": float(window.target_value),
            }
        ],
        con=con,
        ensure=False,
    )


def _lookup_model_oos_prediction(con: Any, *, window: PITWindow, baseline: str) -> tuple[float | None, dict[str, Any]]:
    families = MODEL_BASELINE_FAMILIES.get(str(baseline).lower(), ())
    if not families:
        return None, {}
    placeholders = ",".join("?" for _ in families)
    row = con.execute(
        f"""
        SELECT family, prediction, target, run_id
        FROM model_oos_predictions
        WHERE symbol=?
          AND horizon=?
          AND ts=?
          AND family IN ({placeholders})
        ORDER BY run_id DESC
        LIMIT 1
        """,
        (str(window.symbol), int(len(window.future_values)), int(window.context.asof_ts_ms), *families),
    ).fetchone()
    if not row:
        return None, {"available": False, "reason": "model_oos_prediction_missing", "families": list(families)}
    return _safe_float(row[1], math.nan), {
        "available": True,
        "source_family": str(row[0] or ""),
        "run_id": str(row[3] or ""),
        "target": _safe_float(row[2], math.nan),
    }


def _baseline_prediction(con: Any, *, window: PITWindow, baseline: str) -> tuple[float | None, str, dict[str, Any]]:
    name = str(baseline or "").strip().lower()
    values = tuple(float(v) for v in window.context.values)
    last = float(values[-1])
    task = str(window.task).strip().lower()
    if name == "trailing":
        prediction = _realized_volatility(values[-min(len(values), 8) :]) if task == "realized_volatility" else last
        return float(prediction), "ok", {"baseline_source": "trailing"}
    if name == "har":
        windows = (5, 22, 66)
        vols = [_realized_volatility(values[-min(len(values), width) :]) for width in windows]
        prediction = float(np.mean(vols)) if task == "realized_volatility" else float(last + np.mean(np.diff(values[-min(len(values), 22) :])))
        return prediction, "ok", {"baseline_source": "har_proxy", "windows": list(windows)}
    if name == "garch":
        rets = _returns(values)
        if rets.size == 0:
            prediction = 0.0 if task == "realized_volatility" else last
        else:
            alpha = 0.94
            var = float(rets[0] * rets[0])
            for ret in rets[1:]:
                var = alpha * var + (1.0 - alpha) * float(ret * ret)
            prediction = math.sqrt(max(var, 0.0)) if task == "realized_volatility" else float(last + np.mean(rets[-min(5, rets.size) :]))
        return float(prediction), "ok", {"baseline_source": "ewma_garch_proxy", "lambda": 0.94}
    model_prediction, meta = _lookup_model_oos_prediction(con, window=window, baseline=name)
    if model_prediction is None or not math.isfinite(float(model_prediction)):
        return None, "unavailable", meta
    return float(model_prediction), "ok", meta


def _resource_snapshot() -> dict[str, Any]:
    out: dict[str, Any] = {}
    try:
        import resource

        usage = resource.getrusage(resource.RUSAGE_SELF)
        out["maxrss_kb"] = int(getattr(usage, "ru_maxrss", 0) or 0)
        out["user_cpu_s"] = float(getattr(usage, "ru_utime", 0.0) or 0.0)
        out["system_cpu_s"] = float(getattr(usage, "ru_stime", 0.0) or 0.0)
    except Exception:  # no-op-guard: allow - resource metrics are best-effort.
        pass
    return out


def _register_risk_input(
    con: Any,
    *,
    run_id: str,
    window: PITWindow,
    adapter: str,
    value: float,
    provenance: Mapping[str, Any],
) -> None:
    con.execute(
        """
        INSERT INTO tsfm_risk_inputs(
          run_id, symbol, ts_ms, target_ts_ms, horizon_s, adapter, risk_input_kind,
          value, stage, provenance_json, created_ts_ms
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(run_id, symbol, ts_ms, horizon_s, adapter, risk_input_kind) DO UPDATE SET
          value=excluded.value,
          stage=excluded.stage,
          provenance_json=excluded.provenance_json
        """,
        (
            str(run_id),
            str(window.symbol),
            int(window.context.asof_ts_ms),
            int(window.target_ts_ms),
            int(len(window.future_values)),
            str(adapter),
            "volatility_forecast",
            float(value),
            TSFM_BENCHMARK_STAGE,
            _json_param(con, dict(provenance)),
            _now_ms(),
        ),
    )


def _metric_bucket(summary: dict[str, Any], family: str) -> dict[str, Any]:
    bucket = summary.setdefault(
        str(family),
        {
            "n": 0,
            "mae_sum": 0.0,
            "mse_sum": 0.0,
            "direction_correct": 0,
            "direction_n": 0,
            "status_counts": {},
        },
    )
    return dict(bucket)


def _record_metric(
    summary: dict[str, Any],
    *,
    family: str,
    status: str,
    prediction: float | None,
    target: float | None,
    last_value: float,
) -> None:
    bucket = summary.setdefault(
        str(family),
        {
            "n": 0,
            "mae_sum": 0.0,
            "mse_sum": 0.0,
            "direction_correct": 0,
            "direction_n": 0,
            "status_counts": {},
        },
    )
    counts = dict(bucket.get("status_counts") or {})
    counts[str(status)] = int(counts.get(str(status), 0)) + 1
    bucket["status_counts"] = counts
    if prediction is None or target is None:
        return
    pred = _safe_float(prediction, math.nan)
    tgt = _safe_float(target, math.nan)
    if not math.isfinite(pred) or not math.isfinite(tgt):
        return
    error = float(pred - tgt)
    bucket["n"] = int(bucket.get("n") or 0) + 1
    bucket["mae_sum"] = float(bucket.get("mae_sum") or 0.0) + abs(error)
    bucket["mse_sum"] = float(bucket.get("mse_sum") or 0.0) + error * error
    if tgt != last_value:
        bucket["direction_n"] = int(bucket.get("direction_n") or 0) + 1
        if (pred - last_value) * (tgt - last_value) >= 0.0:
            bucket["direction_correct"] = int(bucket.get("direction_correct") or 0) + 1


def _finalize_metrics(summary: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for family, raw in summary.items():
        bucket = dict(raw or {})
        n = int(bucket.get("n") or 0)
        direction_n = int(bucket.get("direction_n") or 0)
        out[str(family)] = {
            "n": n,
            "mae": float(bucket.get("mae_sum") or 0.0) / n if n > 0 else None,
            "rmse": math.sqrt(float(bucket.get("mse_sum") or 0.0) / n) if n > 0 else None,
            "direction_accuracy": float(bucket.get("direction_correct") or 0) / direction_n if direction_n > 0 else None,
            "status_counts": dict(bucket.get("status_counts") or {}),
        }
    return out


def _persist_model_metadata(
    con: Any,
    *,
    run_id: str,
    symbol: str,
    horizon: int,
    family: str,
    metrics: Mapping[str, Any],
    artifact_meta: Mapping[str, Any],
    adapter_meta: Mapping[str, Any],
) -> None:
    now = _now_ms()
    model_name = str(family)
    model_version = f"{run_id}:{symbol}:{horizon}"
    meta = {
        "benchmark_run_id": str(run_id),
        "score_source": "model_oos_predictions",
        "promotion_authority": "shadow_only_oos_no_execution_authority",
        "zero_shot": True,
        "net_cost_evidence_available": False,
        "net_cost_label_count": 0,
        "direct_trading_authority": False,
        "artifact_alias": str(artifact_meta.get("artifact_alias") or ""),
        "artifact_sha256": str(artifact_meta.get("artifact_sha256") or ""),
        "metrics": dict(metrics),
        "adapter": dict(adapter_meta),
    }
    con.execute(
        """
        INSERT INTO model_versions(
          model_name, model_version, model_kind, parent_version, mutation_kind,
          stage, status, live_ready, training_job_name, train_scope_json,
          meta_json, created_ts_ms, updated_ts_ms
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(model_name, model_version) DO UPDATE SET
          model_kind=excluded.model_kind,
          mutation_kind=excluded.mutation_kind,
          stage=excluded.stage,
          status=excluded.status,
          live_ready=excluded.live_ready,
          training_job_name=excluded.training_job_name,
          train_scope_json=excluded.train_scope_json,
          meta_json=excluded.meta_json,
          updated_ts_ms=excluded.updated_ts_ms
        """,
        (
            model_name,
            model_version,
            "tsfm_foundation_challenger",
            None,
            "tsfm_zero_shot_benchmark",
            TSFM_BENCHMARK_STAGE,
            "benchmarked",
            0,
            "tsfm_benchmark",
            _json_param(con, {"symbol": symbol, "horizon": int(horizon), "run_id": run_id}),
            _json_param(con, meta),
            now,
            now,
        ),
    )
    score = -float(metrics.get("rmse") or 0.0) if metrics.get("rmse") is not None else 0.0
    CompetitionRepository(con).upsert_marketplace_score(
        {
            "model_id": f"{model_name}:{model_version}",
            "model_name": model_name,
            "symbol": symbol,
            "horizon_s": int(horizon),
            "regime": "global",
            "stage": TSFM_BENCHMARK_STAGE,
            "score": float(score),
            "trades": int(metrics.get("n") or 0),
            "wins": 0,
            "losses": 0,
            "gross_pnl": 0.0,
            "net_pnl": 0.0,
            "avg_confidence": 0.0,
            "last_signal_ts_ms": now,
        },
        meta=meta,
        updated_ts_ms=now,
        update_pnl_on_conflict=False,
    )


def _insert_governance_log(con: Any, *, run_id: str, summary: Mapping[str, Any]) -> None:
    con.execute(
        """
        INSERT INTO model_governance_log(ts_ms, source, regime, champion_name, challenger_name, status, summary_json)
        VALUES(?,?,?,?,?,?,?)
        """,
        (
            _now_ms(),
            "tsfm_benchmark",
            "global",
            None,
            str(run_id),
            "shadow_only",
            _json_param(con, dict(summary)),
        ),
    )


def run_tsfm_benchmark(config: TSFMBenchmarkConfig, *, con: Any | None = None) -> dict[str, Any]:
    own = con is None
    if con is None:
        from engine.runtime.storage import connect

        con = connect()
    run_id = config.normalized_run_id()
    ensure_tsfm_benchmark_schema(con)
    created_ts_ms = _now_ms()
    adapter_descriptions: list[dict[str, Any]] = []
    adapters: dict[str, Any] = {}
    for backend in config.adapters:
        backend_key = normalize_tsfm_backend(backend)
        base_adapter_cfg = adapter_config_from_env(backend_key)
        adapter_cfg = TSFMAdapterConfig(
            **{
                **base_adapter_cfg.__dict__,
                "backend": backend_key,
                "model_id": str(dict(config.model_ids).get(backend_key) or base_adapter_cfg.model_id or ""),
                "context_length": int(config.context_length),
                "horizon": int(config.horizon),
                "embedding_dim": int(config.embedding_dim),
                "device": str(config.device),
                "local_files_only": bool(config.local_files_only),
                "quantiles": tuple(config.quantiles),
                "fallback": str(config.fallback),
            }
        )
        adapter = create_tsfm_adapter(adapter_cfg)
        adapters[backend_key] = adapter
        adapter_descriptions.append(adapter.describe())
    artifact_meta = _persist_manifest(config=config, run_id=run_id, adapter_descriptions=adapter_descriptions)
    con.execute(
        """
        INSERT INTO tsfm_benchmark_runs(
          run_id, created_ts_ms, updated_ts_ms, status, stage, config_json,
          artifact_alias, artifact_sha256, summary_json
        )
        VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(run_id) DO UPDATE SET
          updated_ts_ms=excluded.updated_ts_ms,
          status=excluded.status,
          config_json=excluded.config_json,
          artifact_alias=excluded.artifact_alias,
          artifact_sha256=excluded.artifact_sha256,
          summary_json=excluded.summary_json
        """,
        (
            run_id,
            created_ts_ms,
            created_ts_ms,
            "running",
            TSFM_BENCHMARK_STAGE,
            _json_param(con, _config_to_json(config)),
            str(artifact_meta.get("artifact_alias") or ""),
            str(artifact_meta.get("artifact_sha256") or ""),
            _json_param(con, {}),
        ),
    )

    metric_accumulator: dict[str, Any] = {}
    rows_written = 0
    oos_rows_written = 0
    risk_inputs_written = 0
    series_meta: dict[str, Any] = {}
    adapter_meta_by_family = {f"tsfm.{backend}": dict(desc) for backend, desc in zip(adapters, adapter_descriptions)}

    for symbol in config.symbols:
        symbol_key = str(symbol).upper().strip()
        configured_series = dict(config.series_by_symbol).get(symbol_key) or dict(config.series_by_symbol).get(str(symbol))
        series = (
            _normalize_price_series(configured_series or [])
            if configured_series is not None
            else _load_price_series(con, symbol_key)
        )
        asset_class = _asset_class_for_symbol(symbol_key, config.asset_class_by_symbol)
        series_meta[symbol_key] = {"rows": len(series), "sha256": _series_digest(series), "asset_class": asset_class}
        for task in config.tasks:
            windows = walk_forward_windows(
                symbol=symbol_key,
                asset_class=asset_class,
                task=str(task),
                series=series,
                config=config,
            )
            for window in windows:
                last_value = float(window.context.values[-1])
                for backend_key, adapter in adapters.items():
                    family = f"tsfm.{backend_key}"
                    start = time.perf_counter()
                    status = "ok"
                    prediction: float | None = None
                    quantiles: Mapping[str, Sequence[float]] = {}
                    path: Sequence[float] = ()
                    feature_snapshot: dict[str, Any] = {}
                    provenance: dict[str, Any] = {"adapter": adapter.describe(), "artifact": dict(artifact_meta)}
                    try:
                        forecast = adapter.forecast(window.context, horizon=int(config.horizon))
                        if str(task).strip().lower() == "realized_volatility":
                            prediction = float(forecast.volatility_proxy)
                        else:
                            prediction = float(forecast.point)
                        quantiles = dict(forecast.quantiles)
                        path = tuple(forecast.horizon_path)
                        provenance["forecast"] = dict(forecast.metadata)
                        try:
                            embedding = adapter.embed(window.context, dim=int(config.embedding_dim))
                            feature_snapshot = {
                                "stage": TSFM_BENCHMARK_STAGE,
                                "feature_ids": list(embedding.feature_ids),
                                "features": {
                                    str(fid): float(value)
                                    for fid, value in zip(embedding.feature_ids, embedding.values)
                                },
                                "metadata": dict(embedding.metadata),
                            }
                        except Exception as embed_exc:
                            feature_snapshot = {
                                "stage": TSFM_BENCHMARK_STAGE,
                                "feature_ids": tsfm_feature_ids_for_backend(
                                    backend_key,
                                    embedding_dim=int(config.embedding_dim),
                                ),
                                "features": {},
                                "metadata": {"embedding_status": "unavailable", "error": str(embed_exc)},
                            }
                    except TSFMAdapterUnavailable as exc:
                        status = "unavailable"
                        provenance["error"] = str(exc)
                    except Exception as exc:
                        status = "failed"
                        provenance["error"] = f"{type(exc).__name__}: {exc}"
                    latency_ms = (time.perf_counter() - start) * 1000.0
                    _insert_benchmark_row(
                        con,
                        run_id=run_id,
                        window=window,
                        family=family,
                        row_kind="adapter",
                        prediction=prediction,
                        quantiles=quantiles,
                        horizon_path=path,
                        feature_snapshot=feature_snapshot,
                        latency_ms=latency_ms,
                        resource=_resource_snapshot(),
                        provenance=provenance,
                        status=status,
                    )
                    rows_written += 1
                    _record_metric(
                        metric_accumulator,
                        family=family,
                        status=status,
                        prediction=prediction,
                        target=window.target_value,
                        last_value=last_value,
                    )
                    if prediction is not None and math.isfinite(float(prediction)):
                        _persist_oos_row(
                            con,
                            run_id=run_id,
                            window=window,
                            family=family,
                            prediction=float(prediction),
                        )
                        oos_rows_written += 1
                        if (
                            bool(config.risk_register)
                            and str(task).strip().lower() in {"realized_volatility", "rv", "volatility"}
                        ):
                            _register_risk_input(
                                con,
                                run_id=run_id,
                                window=window,
                                adapter=family,
                                value=float(prediction),
                                provenance=provenance,
                            )
                            risk_inputs_written += 1

                for baseline in config.baselines:
                    prediction, status, provenance = _baseline_prediction(con, window=window, baseline=str(baseline))
                    _insert_benchmark_row(
                        con,
                        run_id=run_id,
                        window=window,
                        family=str(baseline),
                        row_kind="baseline",
                        prediction=prediction,
                        quantiles={},
                        horizon_path=[],
                        feature_snapshot={},
                        latency_ms=None,
                        resource={},
                        provenance=provenance,
                        status=status,
                    )
                    rows_written += 1
                    _record_metric(
                        metric_accumulator,
                        family=str(baseline),
                        status=status,
                        prediction=prediction,
                        target=window.target_value,
                        last_value=last_value,
                    )
                    if prediction is not None and math.isfinite(float(prediction)) and status == "ok":
                        if str(baseline).lower() in {"trailing", "har", "garch"}:
                            _persist_oos_row(
                                con,
                                run_id=run_id,
                                window=window,
                                family=str(baseline),
                                prediction=float(prediction),
                            )
                            oos_rows_written += 1

    metrics = _finalize_metrics(metric_accumulator)
    for symbol in config.symbols:
        symbol_key = str(symbol).upper().strip()
        for family, family_metrics in metrics.items():
            if not str(family).startswith("tsfm."):
                continue
            _persist_model_metadata(
                con,
                run_id=run_id,
                symbol=symbol_key,
                horizon=int(config.horizon),
                family=str(family),
                metrics=dict(family_metrics),
                artifact_meta=artifact_meta,
                adapter_meta=dict(adapter_meta_by_family.get(str(family), {})),
            )

    summary = {
        "run_id": run_id,
        "stage": TSFM_BENCHMARK_STAGE,
        "status": "completed",
        "direct_trading_authority": False,
        "promotion_authority": "shadow_only_oos_no_execution_authority",
        "zero_shot_promotable": False,
        "promotion_blockers": [
            "score_source_model_oos_predictions_only",
            "net_cost_evidence_missing",
            "replay_and_champion_challenger_gate_required",
        ],
        "rows_written": int(rows_written),
        "oos_rows_written": int(oos_rows_written),
        "risk_inputs_written": int(risk_inputs_written),
        "artifact": dict(artifact_meta),
        "series": series_meta,
        "metrics": metrics,
        "baselines": {name: metrics.get(name, {"n": 0}) for name in config.baselines},
    }
    _insert_governance_log(con, run_id=run_id, summary=summary)
    con.execute(
        """
        UPDATE tsfm_benchmark_runs
        SET updated_ts_ms=?, status=?, summary_json=?
        WHERE run_id=?
        """,
        (_now_ms(), "completed", _json_param(con, summary), run_id),
    )
    _commit_if_possible(con)
    if own:
        close = getattr(con, "close", None)
        if callable(close):
            close()
    return summary


def config_from_env() -> TSFMBenchmarkConfig:
    def _csv(name: str, default: str) -> tuple[str, ...]:
        return tuple(part.strip() for part in str(os.environ.get(name) or default).split(",") if part.strip())

    quantiles = tuple(
        _safe_float(part, 0.5)
        for part in str(os.environ.get("TSFM_BENCHMARK_QUANTILES") or "0.1,0.5,0.9").split(",")
        if part.strip()
    )
    return TSFMBenchmarkConfig(
        symbols=_csv("TSFM_BENCHMARK_SYMBOLS", "SPY"),
        adapters=_csv("TSFM_BENCHMARK_BACKENDS", os.environ.get("TS_FOUNDATION_BACKEND") or "chronos"),
        baselines=_csv("TSFM_BENCHMARK_BASELINES", ",".join(DEFAULT_BASELINES)),
        tasks=_csv("TSFM_BENCHMARK_TASKS", ",".join(DEFAULT_TASKS)),
        horizon=max(1, _safe_int(os.environ.get("TSFM_BENCHMARK_HORIZON_ROWS") or os.environ.get("TS_FOUNDATION_HORIZON_ROWS"), 1)),
        context_length=max(1, _safe_int(os.environ.get("TSFM_BENCHMARK_CONTEXT_ROWS") or os.environ.get("TS_FOUNDATION_CONTEXT_ROWS"), 64)),
        min_context=max(1, _safe_int(os.environ.get("TSFM_BENCHMARK_MIN_CONTEXT_ROWS"), 16)),
        step=max(1, _safe_int(os.environ.get("TSFM_BENCHMARK_STEP_ROWS"), 1)),
        max_eval_points=max(1, _safe_int(os.environ.get("TSFM_BENCHMARK_MAX_EVAL_POINTS"), 100)),
        embedding_dim=max(1, _safe_int(os.environ.get("TSFM_BENCHMARK_EMBEDDING_DIM") or os.environ.get("TS_FOUNDATION_EMBEDDING_DIM"), 16)),
        quantiles=quantiles or (0.1, 0.5, 0.9),
        device=str(os.environ.get("TSFM_BENCHMARK_DEVICE") or os.environ.get("TS_FOUNDATION_DEVICE") or "cpu"),
        local_files_only=str(os.environ.get("TS_FOUNDATION_LOCAL_FILES_ONLY") or "1").strip().lower()
        not in {"0", "false", "no", "off"},
        fallback=str(os.environ.get("TSFM_BENCHMARK_FALLBACK") or "skip"),
        require_artifact_persistence=str(os.environ.get("TS_FOUNDATION_REQUIRE_ARTIFACT_PERSISTENCE") or "1").strip().lower()
        not in {"0", "false", "no", "off"},
        risk_register=str(os.environ.get("TSFM_BENCHMARK_REGISTER_RISK_INPUTS") or "1").strip().lower()
        not in {"0", "false", "no", "off"},
    )


__all__ = [
    "DEFAULT_BASELINES",
    "DEFAULT_TASKS",
    "PITWindow",
    "TSFMBenchmarkConfig",
    "build_pit_window",
    "config_from_env",
    "ensure_tsfm_benchmark_schema",
    "run_tsfm_benchmark",
    "walk_forward_windows",
]
