"""Asynchronous prediction outcome scoring and online model feedback."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import threading
import time
from bisect import bisect_left
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping

from engine.artifacts.serialization import dump_pickle_artifact
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db, run_write_txn
from engine.regime_detector import get_latest_regime_snapshot, has_known_regime, normalize_regime_state
from engine.runtime.artifact_store import get_artifact_manifest, resolve_artifact_write_path
from engine.strategy.models.base_model import BaseModel

LOG = get_logger("engine.model_scoring")
_WARNED_NONFATAL_KEYS: set[str] = set()
_SERVICE_LOCK = threading.Lock()
_SERVICE: "ModelScoringService | None" = None


def _is_postgres_connection(con: Any) -> bool:
    module_name = str(type(con).__module__)
    return module_name.endswith("storage_pg") or ".storage_pg" in module_name


def _unresolved_predictions_sql(*, postgres: bool) -> str:
    if not postgres:
        return """
                SELECT
                  p.id,
                  p.ts_ms,
                  p.symbol,
                  p.model_name,
                  p.model_version,
                  p.predicted_z,
                  p.confidence,
                  p.event_id,
                  p.horizon_s,
                  p.model_id,
                  tp.id,
                  tp.features_version,
                  tp.source_alert_id,
                  tp.tracking_source,
                  tp.metadata_json
                FROM predictions p
                LEFT JOIN tracked_predictions tp
                  ON tp.id = (
                    SELECT tp2.id
                    FROM tracked_predictions tp2
                    WHERE tp2.prediction_id = p.id
                    ORDER BY tp2.ts_ms DESC, tp2.id DESC
                    LIMIT 1
                  )
                WHERE NOT EXISTS (
                  SELECT 1
                  FROM model_performance mp
                  WHERE mp.prediction_id = p.id
                )
                ORDER BY p.ts_ms ASC, p.id ASC
                LIMIT ?
                """

    # Expected Postgres plan: materialize at most MODEL_SCORING_BATCH_LIMIT
    # unresolved prediction rows, anti-probe model_performance by
    # idx_model_performance_prediction_id, then nested-loop into
    # idx_tracked_predictions_prediction_id_ts_id for a LIMIT 1 latest tracker
    # lookup with no sort.
    return """
                WITH unresolved_predictions AS MATERIALIZED (
                  SELECT
                    p.id,
                    p.ts_ms,
                    p.symbol,
                    p.model_name,
                    p.model_version,
                    p.predicted_z,
                    p.confidence,
                    p.event_id,
                    p.horizon_s,
                    p.model_id
                  FROM predictions p
                  WHERE NOT EXISTS (
                    SELECT 1
                    FROM model_performance mp
                    WHERE mp.prediction_id = p.id
                  )
                  ORDER BY p.ts_ms ASC, p.id ASC
                  LIMIT ?
                )
                SELECT
                  p.id,
                  p.ts_ms,
                  p.symbol,
                  p.model_name,
                  p.model_version,
                  p.predicted_z,
                  p.confidence,
                  p.event_id,
                  p.horizon_s,
                  p.model_id,
                  tp.id,
                  tp.features_version,
                  tp.source_alert_id,
                  tp.tracking_source,
                  tp.metadata_json
                FROM unresolved_predictions p
                LEFT JOIN LATERAL (
                  SELECT
                    tp2.id,
                    tp2.features_version,
                    tp2.source_alert_id,
                    tp2.tracking_source,
                    tp2.metadata_json
                  FROM tracked_predictions tp2
                  WHERE tp2.prediction_id = p.id
                  ORDER BY tp2.ts_ms DESC, tp2.id DESC
                  LIMIT 1
                ) tp ON TRUE
                ORDER BY p.ts_ms ASC, p.id ASC
                """


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if raw == "":
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="model_scoring_nonfatal",
        code=str(code),
        message=str(code),
        error=error,
        level=logging.WARNING,
        component="engine.model_scoring",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            out = json.loads(value)
        except Exception:
            return {}
        return dict(out) if isinstance(out, dict) else {}
    return {}


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_timestamp(value: Any) -> int:
    if value in (None, ""):
        return _now_ms()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return int(value.replace(tzinfo=timezone.utc).timestamp() * 1000)
        return int(value.astimezone(timezone.utc).timestamp() * 1000)
    if isinstance(value, (int, float)):
        ts = float(value)
        if abs(ts) < 10_000_000_000:
            ts *= 1000.0
        return int(ts)
    text = str(value).strip()
    if not text:
        return _now_ms()
    if text.isdigit() or (text.startswith("-") and text[1:].isdigit()):
        return _normalize_timestamp(int(text))
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return int(parsed.timestamp() * 1000)


def _sign(value: Any) -> int:
    numeric = _safe_float(value, 0.0)
    if numeric > 0.0:
        return 1
    if numeric < 0.0:
        return -1
    return 0


def _resolve_row_regime(row: Mapping[str, Any]) -> dict[str, Any]:
    metadata = dict(row.get("metadata") or {})
    symbol_key = _normalize_symbol(row.get("symbol"))
    feature_ts_ms = int(_safe_int(metadata.get("feature_ts_ms"), _safe_int(row.get("prediction_time"), _now_ms())))
    loaded = normalize_regime_state(
        get_latest_regime_snapshot(symbol_key, target_time_ms=feature_ts_ms),
        symbol=symbol_key,
        ts_ms=feature_ts_ms,
    )
    if has_known_regime(loaded):
        return loaded
    return normalize_regime_state(
        metadata.get("regime"),
        symbol=symbol_key,
        ts_ms=feature_ts_ms,
    )


class ModelScorer:
    """Match predictions to realized outcomes and maintain model-performance state."""

    def __init__(
        self,
        *,
        batch_limit: int | None = None,
        rolling_alpha: float | None = None,
        online_updates_enabled: bool | None = None,
    ) -> None:
        self.batch_limit = max(1, int(batch_limit or os.environ.get("MODEL_SCORING_BATCH_LIMIT", "512")))
        self.rolling_alpha = float(
            max(
                0.01,
                min(
                    1.0,
                    _safe_float(
                        rolling_alpha
                        if rolling_alpha is not None
                        else os.environ.get("MODEL_SCORING_ROLLING_ALPHA", "0.2"),
                        0.2,
                    ),
                ),
            )
        )
        self.online_updates_enabled = bool(
            _env_bool("MODEL_SCORING_ONLINE_UPDATES", True)
            if online_updates_enabled is None
            else online_updates_enabled
        )
        self._score_lock = threading.Lock()

    async def record_outcome(self, symbol: Any, timestamp: Any, realized_return: Any) -> Dict[str, Any]:
        """Persist one realized outcome for later prediction-to-outcome matching."""
        return await asyncio.to_thread(
            self._record_outcome_blocking,
            symbol=symbol,
            timestamp=timestamp,
            realized_return=realized_return,
        )

    async def score_models(self) -> Dict[str, Any]:
        """Score unresolved predictions against realized outcomes in a worker thread."""
        return await asyncio.to_thread(self._score_models_blocking_locked)

    def _record_outcome_blocking(self, *, symbol: Any, timestamp: Any, realized_return: Any) -> Dict[str, Any]:
        symbol_key = _normalize_symbol(symbol)
        if not symbol_key:
            raise ValueError("record_outcome requires symbol")
        ts_ms = int(_normalize_timestamp(timestamp))
        realized_value = float(_safe_float(realized_return))
        init_db()
        now_ms = _now_ms()

        def _write(con) -> None:
            con.execute(
                """
                INSERT INTO realized_outcomes(
                  symbol, ts_ms, realized_return, metadata_json, created_ts_ms, updated_ts_ms
                )
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(symbol, ts_ms) DO UPDATE SET
                  realized_return=excluded.realized_return,
                  updated_ts_ms=excluded.updated_ts_ms
                """,
                (
                    str(symbol_key),
                    int(ts_ms),
                    float(realized_value),
                    "{}",
                    int(now_ms),
                    int(now_ms),
                ),
            )

        run_write_txn(_write, table="realized_outcomes", operation="record_model_outcome")
        return {
            "ok": True,
            "symbol": str(symbol_key),
            "timestamp": int(ts_ms),
            "realized_return": float(realized_value),
            "ts_ms": int(_now_ms()),
        }

    def _score_models_blocking_locked(self) -> Dict[str, Any]:
        if not self._score_lock.acquire(blocking=False):
            return {
                "ok": True,
                "skipped": True,
                "reason": "score_in_progress",
                "ts_ms": int(_now_ms()),
            }
        try:
            return self._score_models_blocking()
        finally:
            self._score_lock.release()

    def _score_models_blocking(self) -> Dict[str, Any]:
        init_db()
        unresolved = self._load_unresolved_predictions(limit=self.batch_limit)
        result: Dict[str, Any] = {
            "ok": True,
            "pending_predictions": int(len(unresolved)),
            "matched_predictions": 0,
            "scored_predictions": 0,
            "online_updates": 0,
            "skipped_online_updates": 0,
            "ts_ms": int(_now_ms()),
        }
        if not unresolved:
            result["status"] = "idle"
            return result

        matched_rows = self._match_predictions_to_outcomes(unresolved)
        result["matched_predictions"] = int(len(matched_rows))
        if not matched_rows:
            result["status"] = "awaiting_outcomes"
            return result

        touched_models = {
            (
                str(row["model_name"]),
                str(row["model_version"]),
                str(row["volatility_regime"]),
                str(row["trend_regime"]),
                str(row["liquidity_regime"]),
            )
            for row in matched_rows
            if str(row.get("model_name") or "").strip() and str(row.get("model_version") or "").strip()
        }
        now_ms = _now_ms()

        def _write(con) -> None:
            for row in matched_rows:
                metadata_json = json.dumps(
                    dict(row.get("metadata") or {}),
                    separators=(",", ":"),
                    sort_keys=True,
                    default=str,
                )
                con.execute(
                    """
                    INSERT INTO model_performance(
                      tracked_prediction_id, prediction_id, outcome_id, "time", prediction_time,
                      symbol, model_id, model_name, model_version, horizon_s,
                      prediction, realized_return, error, directional_accuracy,
                      pnl_impact, rolling_score,
                      regime_time_ms, volatility_regime, trend_regime, liquidity_regime,
                      metadata_json, created_ts_ms, updated_ts_ms
                    )
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(tracked_prediction_id) DO UPDATE SET
                      prediction_id=excluded.prediction_id,
                      outcome_id=excluded.outcome_id,
                      "time"=excluded."time",
                      prediction_time=excluded.prediction_time,
                      symbol=excluded.symbol,
                      model_id=excluded.model_id,
                      model_name=excluded.model_name,
                      model_version=excluded.model_version,
                      horizon_s=excluded.horizon_s,
                      prediction=excluded.prediction,
                      realized_return=excluded.realized_return,
                      error=excluded.error,
                      directional_accuracy=excluded.directional_accuracy,
                      pnl_impact=excluded.pnl_impact,
                      regime_time_ms=excluded.regime_time_ms,
                      volatility_regime=excluded.volatility_regime,
                      trend_regime=excluded.trend_regime,
                      liquidity_regime=excluded.liquidity_regime,
                      metadata_json=excluded.metadata_json,
                      updated_ts_ms=excluded.updated_ts_ms
                    """,
                    (
                        int(row["tracked_prediction_id"]),
                        (
                            int(row["prediction_id"])
                            if _safe_int(row.get("prediction_id"), 0) > 0
                            else None
                        ),
                        int(row["outcome_id"]),
                        int(row["time"]),
                        int(row["prediction_time"]),
                        str(row["symbol"]),
                        (str(row["model_id"]) if row.get("model_id") not in (None, "") else None),
                        str(row["model_name"]),
                        str(row["model_version"]),
                        (
                            int(row["horizon_s"])
                            if row.get("horizon_s") not in (None, "")
                            else None
                        ),
                        float(row["prediction"]),
                        float(row["realized_return"]),
                        float(row["error"]),
                        int(row["directional_accuracy"]),
                        float(row["pnl_impact"]),
                        None,
                        int(row["regime_time_ms"]),
                        str(row["volatility_regime"]),
                        str(row["trend_regime"]),
                        str(row["liquidity_regime"]),
                        str(metadata_json),
                        int(now_ms),
                        int(now_ms),
                    ),
                )
            self._recompute_rolling_scores(con, touched_models=touched_models)

        run_write_txn(_write, table="model_performance", operation="score_models")
        result["scored_predictions"] = int(len(matched_rows))
        result["status"] = "ok"

        if self.online_updates_enabled:
            update_counts = self._apply_online_updates(matched_rows)
            result["online_updates"] = int(update_counts.get("updated", 0))
            result["skipped_online_updates"] = int(update_counts.get("skipped", 0))

        return result

    def _load_unresolved_predictions(self, *, limit: int) -> list[Dict[str, Any]]:
        con = connect(readonly=True)
        try:
            prediction_rows = con.execute(
                _unresolved_predictions_sql(postgres=_is_postgres_connection(con)),
                (int(limit),),
            ).fetchall()
            tracked_only_rows = con.execute(
                """
                SELECT
                  tp.id,
                  tp.prediction_id,
                  tp.ts_ms,
                  tp.symbol,
                  tp.model_name,
                  tp.model_version,
                  tp.prediction,
                  tp.confidence,
                  tp.features_version,
                  tp.event_id,
                  tp.horizon_s,
                  tp.source_alert_id,
                  tp.model_id,
                  tp.tracking_source,
                  tp.metadata_json
                FROM tracked_predictions tp
                WHERE tp.prediction_id IS NULL
                  AND NOT EXISTS (
                    SELECT 1
                    FROM model_performance mp
                    WHERE mp.tracked_prediction_id = tp.id
                  )
                ORDER BY tp.ts_ms ASC, tp.id ASC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        finally:
            con.close()

        out: list[Dict[str, Any]] = []
        for row in prediction_rows or []:
            out.append(
                {
                    "tracked_prediction_id": (
                        int(row[10]) if _safe_int(row[10], 0) > 0 else -1 * int(row[0])
                    ),
                    "prediction_id": int(row[0]),
                    "prediction_time": int(row[1]),
                    "symbol": _normalize_symbol(row[2]),
                    "model_name": str(row[3] or "").strip() or "baseline",
                    "model_version": str(row[4] or "").strip() or "unversioned",
                    "prediction": float(_safe_float(row[5], 0.0)),
                    "confidence": float(_safe_float(row[6], 0.0)),
                    "event_id": (_safe_int(row[7], 0) or None),
                    "horizon_s": (_safe_int(row[8], 0) or None),
                    "model_id": (str(row[9]).strip() if row[9] not in (None, "") else None),
                    "features_version": str(row[11] or "").strip() or "unknown",
                    "source_alert_id": (_safe_int(row[12], 0) or None),
                    "tracking_source": (str(row[13]).strip() if row[13] not in (None, "") else "predictions_table"),
                    "metadata": _safe_json_dict(row[14]),
                }
            )
        for row in tracked_only_rows or []:
            out.append(
                {
                    "tracked_prediction_id": int(row[0]),
                    "prediction_id": (_safe_int(row[1], 0) or None),
                    "prediction_time": int(row[2]),
                    "symbol": _normalize_symbol(row[3]),
                    "model_name": str(row[4] or "").strip() or "baseline",
                    "model_version": str(row[5] or "").strip() or "unversioned",
                    "prediction": float(_safe_float(row[6], 0.0)),
                    "confidence": float(_safe_float(row[7], 0.0)),
                    "features_version": str(row[8] or "").strip() or "unknown",
                    "event_id": (_safe_int(row[9], 0) or None),
                    "horizon_s": (_safe_int(row[10], 0) or None),
                    "source_alert_id": (_safe_int(row[11], 0) or None),
                    "model_id": (str(row[12]).strip() if row[12] not in (None, "") else None),
                    "tracking_source": (str(row[13]).strip() if row[13] not in (None, "") else None),
                    "metadata": _safe_json_dict(row[14]),
                }
            )
        out.sort(
            key=lambda item: (
                int(item.get("prediction_time") or 0),
                int(item.get("prediction_id") or 0),
                int(item.get("tracked_prediction_id") or 0),
            )
        )
        return out[: int(limit)]

    def _match_predictions_to_outcomes(self, predictions: Iterable[Mapping[str, Any]]) -> list[Dict[str, Any]]:
        prediction_rows = [dict(row) for row in predictions or [] if _normalize_symbol((row or {}).get("symbol"))]
        if not prediction_rows:
            return []

        symbols = sorted({_normalize_symbol(row.get("symbol")) for row in prediction_rows if _normalize_symbol(row.get("symbol"))})
        min_prediction_ts_ms = min(int(row.get("prediction_time") or 0) for row in prediction_rows)
        placeholders = ",".join("?" for _ in symbols)
        con = connect(readonly=True)
        try:
            rows = con.execute(
                f"""
                SELECT id, symbol, ts_ms, realized_return, metadata_json
                FROM realized_outcomes
                WHERE symbol IN ({placeholders})
                  AND ts_ms >= ?
                ORDER BY symbol ASC, ts_ms ASC, id ASC
                """,
                tuple([*symbols, int(min_prediction_ts_ms)]),
            ).fetchall()
        finally:
            con.close()

        outcomes_by_symbol: dict[str, list[Dict[str, Any]]] = {}
        outcome_times: dict[str, list[int]] = {}
        for row in rows or []:
            symbol_key = _normalize_symbol(row[1])
            item = {
                "outcome_id": int(row[0]),
                "symbol": str(symbol_key),
                "time": int(row[2]),
                "realized_return": float(_safe_float(row[3], 0.0)),
                "metadata": _safe_json_dict(row[4]),
            }
            outcomes_by_symbol.setdefault(symbol_key, []).append(item)
            outcome_times.setdefault(symbol_key, []).append(int(item["time"]))

        matched: list[Dict[str, Any]] = []
        for row in prediction_rows:
            symbol_key = _normalize_symbol(row.get("symbol"))
            series = outcomes_by_symbol.get(symbol_key) or []
            if not series:
                continue
            regime_state = _resolve_row_regime(row)
            horizon_s = _safe_int(row.get("horizon_s"), 0)
            due_ts_ms = int(row.get("prediction_time") or 0) + (
                int(horizon_s) * 1000 if int(horizon_s) > 0 else 0
            )
            ts_values = outcome_times.get(symbol_key) or []
            idx = bisect_left(ts_values, int(due_ts_ms))
            if idx >= len(series):
                continue
            outcome = series[idx]
            prediction = float(_safe_float(row.get("prediction"), 0.0))
            realized_return = float(_safe_float(outcome.get("realized_return"), 0.0))
            direction_hit = int(_sign(prediction) == _sign(realized_return))
            pnl_impact = float(_sign(prediction) * realized_return)
            meta = {
                "confidence": float(_safe_float(row.get("confidence"), 0.0)),
                "features_version": str(row.get("features_version") or "unknown"),
                "feature_ts_ms": int(
                    _safe_int(
                        dict(row.get("metadata") or {}).get("feature_ts_ms"),
                        _safe_int(row.get("prediction_time"), 0),
                    )
                ),
                "tracking_source": str(row.get("tracking_source") or ""),
                "matched_due_ts_ms": int(due_ts_ms),
            }
            meta["regime"] = {
                "time": int(regime_state["time"]),
                "symbol": str(regime_state["symbol"]),
                "volatility_regime": str(regime_state["volatility_regime"]),
                "trend_regime": str(regime_state["trend_regime"]),
                "liquidity_regime": str(regime_state["liquidity_regime"]),
            }
            outcome_meta = dict(outcome.get("metadata") or {})
            if outcome_meta:
                meta["outcome"] = outcome_meta
            matched.append(
                {
                    "tracked_prediction_id": int(row["tracked_prediction_id"]),
                    "prediction_id": row.get("prediction_id"),
                    "outcome_id": int(outcome["outcome_id"]),
                    "time": int(outcome["time"]),
                    "prediction_time": int(row["prediction_time"]),
                    "symbol": str(symbol_key),
                    "model_id": row.get("model_id"),
                    "model_name": str(row["model_name"]),
                    "model_version": str(row["model_version"]),
                    "horizon_s": row.get("horizon_s"),
                    "prediction": float(prediction),
                    "realized_return": float(realized_return),
                    # Store absolute error so lower is always better when inspecting history.
                    "error": float(abs(realized_return - prediction)),
                    "directional_accuracy": int(direction_hit),
                    "pnl_impact": float(pnl_impact),
                    "regime_time_ms": int(regime_state["time"]),
                    "volatility_regime": str(regime_state["volatility_regime"]),
                    "trend_regime": str(regime_state["trend_regime"]),
                    "liquidity_regime": str(regime_state["liquidity_regime"]),
                    "metadata": meta,
                }
            )
        return matched

    def _recompute_rolling_scores(
        self,
        con,
        *,
        touched_models: Iterable[tuple[str, str, str, str, str]],
    ) -> None:
        now_ms = _now_ms()
        for model_name, model_version, volatility_regime, trend_regime, liquidity_regime in touched_models:
            rows = con.execute(
                """
                SELECT id, error, directional_accuracy, pnl_impact
                FROM model_performance
                WHERE model_name=? AND model_version=?
                  AND volatility_regime=? AND trend_regime=? AND liquidity_regime=?
                ORDER BY "time" ASC, id ASC
                """,
                (
                    str(model_name),
                    str(model_version),
                    str(volatility_regime),
                    str(trend_regime),
                    str(liquidity_regime),
                ),
            ).fetchall()
            rolling_score: float | None = None
            for row in rows or []:
                component = self._score_component(
                    error=float(_safe_float(row[1], 0.0)),
                    directional_accuracy=int(_safe_int(row[2], 0)),
                    pnl_impact=float(_safe_float(row[3], 0.0)),
                )
                if rolling_score is None:
                    rolling_score = float(component)
                else:
                    rolling_score = ((1.0 - float(self.rolling_alpha)) * float(rolling_score)) + (
                        float(self.rolling_alpha) * float(component)
                    )
                con.execute(
                    """
                    UPDATE model_performance
                    SET rolling_score=?, updated_ts_ms=?
                    WHERE id=?
                    """,
                    (float(rolling_score), int(now_ms), int(row[0])),
                )

    def _score_component(self, *, error: float, directional_accuracy: int, pnl_impact: float) -> float:
        error_component = 1.0 / (1.0 + max(0.0, float(error)))
        direction_component = 1.0 if int(directional_accuracy) else 0.0
        pnl_component = math.tanh(float(pnl_impact))
        return float((0.45 * direction_component) + (0.35 * error_component) + (0.20 * pnl_component))

    def _apply_online_updates(self, rows: Iterable[Mapping[str, Any]]) -> Dict[str, int]:
        updated = 0
        skipped = 0
        for row in rows or []:
            try:
                record = self._load_registry_record(row)
                if not record:
                    skipped += 1
                    continue
                artifact = self._load_artifact(record)
                if artifact is None or not self._supports_online_update(artifact):
                    skipped += 1
                    continue
                features = self._resolve_feature_payload(row)
                artifact.update(features, row.get("realized_return"))
                if not self._persist_artifact(record, artifact):
                    skipped += 1
                    continue
                updated += 1
            except Exception as exc:
                skipped += 1
                _warn_nonfatal(
                    "MODEL_SCORING_ONLINE_UPDATE_FAILED",
                    exc,
                    once_key=None,
                    symbol=str((row or {}).get("symbol") or ""),
                    model_name=str((row or {}).get("model_name") or ""),
                    model_version=str((row or {}).get("model_version") or ""),
                )
        return {"updated": int(updated), "skipped": int(skipped)}

    def _load_registry_record(self, row: Mapping[str, Any]) -> Dict[str, Any] | None:
        try:
            from engine.model_registry import load_model

            record = load_model(
                str(row.get("symbol") or ""),
                model_name=str(row.get("model_name") or ""),
                version=str(row.get("model_version") or ""),
            )
        except Exception as exc:
            _warn_nonfatal(
                "MODEL_SCORING_MODEL_LOOKUP_FAILED",
                exc,
                once_key=None,
                symbol=str(row.get("symbol") or ""),
                model_name=str(row.get("model_name") or ""),
                model_version=str(row.get("model_version") or ""),
            )
            return None
        return dict(record) if isinstance(record, dict) else None

    def _load_artifact(self, record: Mapping[str, Any]) -> Any | None:
        try:
            from engine.inference_engine import _load_model_artifact

            return _load_model_artifact(record)
        except Exception as exc:
            _warn_nonfatal(
                "MODEL_SCORING_ARTIFACT_LOAD_FAILED",
                exc,
                once_key=None,
                model_name=str(record.get("model_name") or ""),
                model_version=str(record.get("version") or ""),
                artifact_uri=str(record.get("artifact_uri") or ""),
            )
            return None

    def _supports_online_update(self, artifact: Any) -> bool:
        update = getattr(artifact, "update", None)
        if not callable(update):
            return False
        if bool(getattr(artifact, "supports_online_update", False)):
            return True
        try:
            return getattr(update, "__func__", update) is not BaseModel.update
        except Exception:
            return True

    def _resolve_feature_payload(self, row: Mapping[str, Any]) -> Any:
        feature_ts_ms = int(
            _safe_int(
                dict((row or {}).get("metadata") or {}).get("feature_ts_ms"),
                _safe_int((row or {}).get("prediction_time"), 0),
            )
        )
        symbol = str(row.get("symbol") or "")
        if feature_ts_ms <= 0 or not symbol:
            return symbol
        try:
            from engine.data.feature_store import get_features_asof

            snapshot = get_features_asof(symbol, feature_ts_ms)
        except Exception as exc:
            _warn_nonfatal(
                "MODEL_SCORING_FEATURE_SNAPSHOT_FAILED",
                exc,
                once_key=None,
                symbol=str(symbol),
                feature_ts_ms=int(feature_ts_ms),
            )
            return symbol
        if isinstance(snapshot, Mapping) and (
            dict(snapshot).get("features")
            or dict(snapshot).get("vector")
            or int(_safe_int(dict(snapshot).get("ts_ms"), 0)) > 0
        ):
            return dict(snapshot)
        return symbol

    def _persist_artifact(self, record: Mapping[str, Any], artifact: Any) -> bool:
        manifest = get_artifact_manifest(record) or {}
        artifact_uri = str(manifest.get("artifact_uri") or record.get("artifact_uri") or "").strip()
        if not artifact_uri:
            return False
        target = resolve_artifact_write_path(record)
        if target is None:
            _warn_nonfatal(
                "MODEL_SCORING_IMMUTABLE_ARTIFACT",
                RuntimeError("immutable_artifact_store_target"),
                once_key=None,
                model_name=str(record.get("model_name") or ""),
                model_version=str(record.get("version") or ""),
                artifact_uri=str(artifact_uri),
            )
            return False
        _, path = target
        saver = getattr(artifact, "save_artifact", None)
        if callable(saver):
            saver(path)
            return True
        dump_pickle_artifact(
            artifact,
            path,
            prefer_joblib=path.suffix.lower() == ".joblib",
        )
        return True


class ModelScoringService:
    """Background loop that periodically runs the model scoring pass."""

    def __init__(
        self,
        *,
        scorer: ModelScorer | None = None,
        interval_s: float | None = None,
        enabled: bool | None = None,
    ) -> None:
        self.scorer = scorer or DEFAULT_MODEL_SCORER
        self.interval_s = max(0.1, _safe_float(interval_s or os.environ.get("MODEL_SCORING_INTERVAL_S", "5"), 5.0))
        self.enabled = bool(_env_bool("MODEL_SCORING_ENABLED", True) if enabled is None else enabled)
        self._thread: threading.Thread | None = None
        self._started_event = threading.Event()
        self._state_lock = threading.Lock()
        self._last_run: Dict[str, Any] = {}
        self._last_error: str | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task[None] | None = None
        self._async_stop: asyncio.Event | None = None

    def start(self) -> Dict[str, Any]:
        """Start the background scoring loop if it is enabled."""
        if not self.enabled:
            return self.get_snapshot()
        with self._state_lock:
            if self._thread is not None and self._thread.is_alive():
                return self.get_snapshot()
            self._started_event.clear()
            self._thread = threading.Thread(target=self._thread_main, name="model-scoring-loop", daemon=True)
            self._thread.start()
        self._started_event.wait(timeout=max(0.5, min(10.0, self.interval_s + 0.5)))
        return self.get_snapshot()

    def close(self, timeout_s: float | None = None) -> Dict[str, Any]:
        """Stop the scoring loop and return the final service snapshot."""
        thread = None
        loop = None
        async_stop = None
        with self._state_lock:
            thread = self._thread
            loop = self._loop
            async_stop = self._async_stop
        if loop is not None and async_stop is not None:
            try:
                loop.call_soon_threadsafe(async_stop.set)
            except Exception as exc:
                _warn_nonfatal("MODEL_SCORING_SERVICE_STOP_SIGNAL_FAILED", exc, once_key=None)
        if thread is not None:
            thread.join(timeout=float(timeout_s or max(1.0, self.interval_s)))
        return self.get_snapshot()

    def get_snapshot(self) -> Dict[str, Any]:
        """Return the current background-service state for diagnostics."""
        thread = None
        with self._state_lock:
            thread = self._thread
        return {
            "ok": (not self.enabled) or bool(thread is not None and thread.is_alive()),
            "enabled": bool(self.enabled),
            "started": bool(thread is not None and thread.is_alive()),
            "interval_s": float(self.interval_s),
            "last_run": dict(self._last_run or {}),
            "last_error": self._last_error,
            "ts_ms": int(_now_ms()),
        }

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            self._loop = loop
            self._async_stop = asyncio.Event()
            self._task = loop.create_task(self._run_loop())
            self._started_event.set()
            loop.run_until_complete(self._task)
        except Exception as exc:
            self._last_error = f"{type(exc).__name__}: {exc}"
            _warn_nonfatal("MODEL_SCORING_SERVICE_CRASHED", exc, once_key=None)
        finally:
            try:
                pending = asyncio.all_tasks(loop=loop)
                for task in pending:
                    task.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass  # no-op-guard: allow best-effort async shutdown
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception:
                pass  # no-op-guard: allow best-effort async shutdown
            with self._state_lock:
                self._thread = None
                self._task = None
                self._loop = None
                self._async_stop = None
            asyncio.set_event_loop(None)
            loop.close()

    async def _run_loop(self) -> None:
        stop_event = self._async_stop
        if stop_event is None:
            raise RuntimeError("model_scoring_async_stop_uninitialized")
        while not stop_event.is_set():
            try:
                self._last_run = dict(await self.scorer.score_models())
                self._last_error = None
            except Exception as exc:
                self._last_error = f"{type(exc).__name__}: {exc}"
                _warn_nonfatal("MODEL_SCORING_SERVICE_ITERATION_FAILED", exc, once_key=None)
            remaining = float(self.interval_s)
            while remaining > 0.0 and not stop_event.is_set():
                sleep_s = min(0.5, remaining)
                await asyncio.sleep(float(sleep_s))
                remaining -= float(sleep_s)


DEFAULT_MODEL_SCORER = ModelScorer()


def get_model_scoring_service() -> ModelScoringService:
    """Return the process-wide model scoring service singleton."""
    global _SERVICE
    service = _SERVICE
    if service is not None:
        return service
    with _SERVICE_LOCK:
        service = _SERVICE
        if service is None:
            service = ModelScoringService()
            _SERVICE = service
        return service


def start_model_scoring_service(
    *,
    interval_s: float | None = None,
    enabled: bool | None = None,
) -> Dict[str, Any]:
    """Start or replace the process-wide model scoring service."""
    global _SERVICE
    previous_service: ModelScoringService | None = None
    with _SERVICE_LOCK:
        service = _SERVICE
        if service is None or (
            interval_s is not None
            or enabled is not None
        ):
            previous_service = service
            service = ModelScoringService(
                interval_s=interval_s,
                enabled=enabled,
            )
            _SERVICE = service
    if previous_service is not None and previous_service is not service:
        try:
            previous_service.close(timeout_s=1.0)
        except Exception as exc:
            _warn_nonfatal("MODEL_SCORING_SERVICE_REPLACE_CLOSE_FAILED", exc, once_key=None)
    return service.start()


def stop_model_scoring_service(timeout_s: float | None = None) -> Dict[str, Any]:
    """Stop the process-wide model scoring service."""
    service = get_model_scoring_service()
    return service.close(timeout_s=timeout_s)


def get_model_scoring_snapshot() -> Dict[str, Any]:
    """Return a diagnostic snapshot of the process-wide scoring service."""
    return get_model_scoring_service().get_snapshot()


__all__ = [
    "DEFAULT_MODEL_SCORER",
    "ModelScorer",
    "ModelScoringService",
    "get_model_scoring_service",
    "get_model_scoring_snapshot",
    "start_model_scoring_service",
    "stop_model_scoring_service",
]
