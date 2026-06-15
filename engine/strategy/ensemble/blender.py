"""Serve-time weighted blending for stacked ridge ensembles."""

from __future__ import annotations
import logging

import json
import math
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect


ENSEMBLE_WEIGHTS_TABLE = "ensemble_weights"
ELIGIBLE_STAGES = {"champion", "challenger"}
LOG = logging.getLogger(__name__)


def _warn_nonfatal(code: str, error: BaseException | None = None, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error or code),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.ensemble.blender",
        extra=extra or None,
        persist=False,
    )


def ensemble_mode() -> str:
    mode = str(os.environ.get("ENSEMBLE_MODE", "blend") or "blend").strip().lower()
    if mode not in {"blend", "single_champion"}:
        return "blend"
    return mode


def _own_connection(con):
    if con is not None:
        return con, False
    return connect(), True


def _commit_if_possible(con) -> None:
    commit = getattr(con, "commit", None)
    if callable(commit):
        commit()


def ensure_schema(con=None) -> None:
    con, own = _own_connection(con)
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ensemble_weights (
                symbol TEXT NOT NULL,
                horizon INTEGER NOT NULL,
                ts INTEGER NOT NULL,
                weights_json TEXT NOT NULL,
                intercept REAL NOT NULL DEFAULT 0,
                alpha REAL NOT NULL DEFAULT 0,
                n_train_obs INTEGER NOT NULL DEFAULT 0,
                val_metric REAL NULL,
                PRIMARY KEY(symbol, horizon, ts)
            )
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ensemble_weights_lookup
              ON ensemble_weights(symbol, horizon, ts DESC)
            """
        )
        if own:
            _commit_if_possible(con)
    finally:
        if own:
            try:
                con.close()
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


def _rows_to_dicts(cursor) -> list[dict[str, Any]]:
    rows = cursor.fetchall()
    columns = [desc[0] for desc in (cursor.description or [])]
    return [dict(zip(columns, row)) for row in rows]


def _decode_weights(payload: Any) -> dict[str, float]:
    if payload is None:
        return {}
    if isinstance(payload, Mapping):
        raw = dict(payload)
    else:
        raw = json.loads(str(payload or "{}"))
    return {
        str(family): float(weight)
        for family, weight in raw.items()
        if str(family or "").strip()
    }


def persist_weights(
    *,
    symbol: str,
    horizon: int,
    weights: Mapping[str, float],
    intercept: float = 0.0,
    alpha: float = 0.0,
    n_train_obs: int = 0,
    val_metric: float | None = None,
    ts: int | None = None,
    con=None,
    ensure: bool = True,
) -> int:
    con, own = _own_connection(con)
    try:
        if ensure:
            ensure_schema(con)
        ts_value = int(ts if ts is not None else time.time() * 1000)
        clean_weights = {
            str(family): float(weight)
            for family, weight in dict(weights or {}).items()
            if str(family or "").strip() and abs(float(weight)) > 0.0
        }
        con.execute(
            """
            INSERT INTO ensemble_weights(
                symbol, horizon, ts, weights_json, intercept, alpha, n_train_obs, val_metric
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, horizon, ts) DO UPDATE SET
              weights_json = excluded.weights_json,
              intercept = excluded.intercept,
              alpha = excluded.alpha,
              n_train_obs = excluded.n_train_obs,
              val_metric = excluded.val_metric
            """,
            (
                str(symbol),
                int(horizon),
                int(ts_value),
                json.dumps(clean_weights, sort_keys=True),
                float(intercept),
                float(alpha),
                int(n_train_obs),
                (float(val_metric) if val_metric is not None else None),
            ),
        )
        if own:
            _commit_if_possible(con)
        return ts_value
    finally:
        if own:
            try:
                con.close()
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


def load_latest_weights(
    symbol: str,
    horizon: int,
    *,
    con=None,
    ensure: bool = True,
) -> dict[str, Any] | None:
    con, own = _own_connection(con)
    try:
        if ensure:
            ensure_schema(con)
        rows = _rows_to_dicts(
            con.execute(
                """
                SELECT symbol, horizon, ts, weights_json, intercept, alpha, n_train_obs, val_metric
                  FROM ensemble_weights
                 WHERE symbol = ?
                   AND horizon = ?
                 ORDER BY ts DESC
                 LIMIT 1
                """,
                (str(symbol), int(horizon)),
            )
        )
        if not rows and str(symbol) != "*":
            rows = _rows_to_dicts(
                con.execute(
                    """
                    SELECT symbol, horizon, ts, weights_json, intercept, alpha, n_train_obs, val_metric
                      FROM ensemble_weights
                     WHERE symbol = '*'
                       AND horizon = ?
                     ORDER BY ts DESC
                     LIMIT 1
                    """,
                    (int(horizon),),
                )
            )
        if not rows:
            return None
        row = dict(rows[0])
        row["weights"] = _decode_weights(row.get("weights_json"))
        row["intercept"] = float(row.get("intercept") or 0.0)
        row["alpha"] = float(row.get("alpha") or 0.0)
        row["n_train_obs"] = int(row.get("n_train_obs") or 0)
        row["val_metric"] = float(row["val_metric"]) if row.get("val_metric") is not None else None
        row["ts"] = int(row.get("ts") or 0)
        return row
    finally:
        if own:
            try:
                con.close()
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


def _family_from_model_name(model_name: str) -> str:
    name = str(model_name or "").strip().lower()
    if not name:
        return ""
    if "lgbm" in name or "lightgbm" in name:
        return "lgbm_regressor"
    if "xgb" in name or "xgboost" in name:
        return "xgb_regressor"
    if "patchtst" in name:
        return "patchtst"
    if "temporal" in name:
        return "temporal_predictor"
    if "gbm" in name:
        return "gbm_regressor"
    if "regime_stats" in name:
        return "regime_stats"
    if "embed" in name:
        return "embed_regressor"
    return name


def _safe_stage_rows(con, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    try:
        return _rows_to_dicts(con.execute(sql, params))
    except Exception as e:
        _warn_nonfatal(
            "ENSEMBLE_STAGE_ROWS_QUERY_FAILED",
            e,
            sql=str(sql),
            params=repr(params),
        )
        return []


def family_stage_map(
    symbol: str,
    horizon: int,
    *,
    con=None,
) -> dict[str, str]:
    con, own = _own_connection(con)
    try:
        stages: dict[str, str] = {}
        marketplace_rows = _safe_stage_rows(
            con,
            """
            SELECT model_name, stage, updated_ts_ms
              FROM model_marketplace_scores
             WHERE symbol IN (?, '*')
               AND horizon_s IN (?, 0)
             ORDER BY updated_ts_ms DESC
            """,
            (str(symbol), int(horizon)),
        )
        for row in marketplace_rows:
            family = _family_from_model_name(str(row.get("model_name") or ""))
            stage = str(row.get("stage") or "").strip().lower()
            if family and stage and family not in stages:
                stages[family] = stage
        registry_rows = _safe_stage_rows(
            con,
            """
            SELECT model_name, stage, updated_ts_ms
              FROM model_registry
             WHERE stage IN ('champion', 'challenger', 'shadow')
             ORDER BY updated_ts_ms DESC
            """,
            (),
        )
        for row in registry_rows:
            family = _family_from_model_name(str(row.get("model_name") or ""))
            stage = str(row.get("stage") or "").strip().lower()
            if family and stage and family not in stages:
                stages[family] = stage
        return stages
    finally:
        if own:
            try:
                con.close()
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


def filter_weights_by_eligibility(
    weights: Mapping[str, float],
    *,
    symbol: str,
    horizon: int,
    con=None,
) -> tuple[dict[str, float], dict[str, str]]:
    stages = family_stage_map(str(symbol), int(horizon), con=con)
    kept: dict[str, float] = {}
    excluded: dict[str, str] = {}
    for family, weight in dict(weights or {}).items():
        family_key = str(family or "").strip()
        if not family_key or abs(float(weight)) <= 0.0:
            continue
        stage = str(stages.get(family_key) or "").strip().lower()
        if stage not in ELIGIBLE_STAGES:
            excluded[family_key] = f"{stage or 'missing'}_stage"
            continue
        kept[family_key] = float(weight)
    return kept, excluded


@dataclass(frozen=True)
class BlendResult:
    prediction: float
    confidence: float
    applied: bool
    diagnostics: dict[str, Any]


def _parse_component_result(family: str, value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        if "prediction" not in value:
            return None
        out = dict(value)
        out.setdefault("family", str(family))
        out["prediction"] = float(out["prediction"])
        if out.get("confidence") is not None:
            out["confidence"] = float(out["confidence"])
        return out
    if isinstance(value, tuple) or isinstance(value, list):
        if not value:
            return None
        out = {
            "family": str(family),
            "prediction": float(value[0]),
        }
        if len(value) > 1 and value[1] is not None:
            out["confidence"] = float(value[1])
        if len(value) > 2 and isinstance(value[2], Mapping):
            out["explain"] = dict(value[2])
        return out
    return {"family": str(family), "prediction": float(value)}


def _safe_finite_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        LOG.debug("ensemble_float_parse_failed value=%r", value, exc_info=True)
        return None
    return float(out) if math.isfinite(out) else None


def _clamped_confidence(value: Any, default: float = 0.0) -> float:
    parsed = _safe_finite_float(value)
    if parsed is None:
        parsed = float(default)
    return float(max(0.0, min(1.0, parsed)))


def _renormalize_available_weights(
    eligible_weights: Mapping[str, float],
    available_families: set[str],
) -> dict[str, float]:
    available = {
        str(family): float(weight)
        for family, weight in dict(eligible_weights or {}).items()
        if str(family) in available_families
    }
    if not available:
        return {}
    original_abs = sum(abs(float(weight)) for weight in dict(eligible_weights or {}).values())
    available_abs = sum(abs(float(weight)) for weight in available.values())
    if original_abs <= 0.0 or available_abs <= 0.0:
        return dict(available)
    scale = float(original_abs / available_abs)
    return {str(family): float(weight) * scale for family, weight in available.items()}


def _emit_fallback_depth_metric(*, symbol: str, horizon: int, base_family: str, depth: int, reason: str) -> None:
    try:
        from engine.runtime.metrics import emit_gauge

        emit_gauge(
            "ensemble_fallback_depth",
            int(depth),
            component="engine.strategy.ensemble.blender",
            symbol=str(symbol),
            extra_tags={
                "horizon": int(horizon),
                "base_family": str(base_family or ""),
                "fallback_reason": str(reason or ""),
            },
        )
    except Exception:
        LOG.debug("Ignored recoverable exception.", exc_info=True)


class EnsembleBlender:
    def __init__(self, *, con=None, mode: str | None = None) -> None:
        self.con = con
        self.mode = str(mode or ensemble_mode()).strip().lower()
        self._last_good_by_key: dict[tuple[str, int, str], tuple[float, float]] = {}

    def _last_good_key(self, symbol: str, horizon: int, base_family: str) -> tuple[str, int, str]:
        return (str(symbol), int(horizon), str(base_family or ""))

    def _record_last_good(
        self,
        *,
        symbol: str,
        horizon: int,
        base_family: str,
        prediction: float,
        confidence: float,
    ) -> None:
        if math.isfinite(float(prediction)):
            self._last_good_by_key[self._last_good_key(symbol, horizon, base_family)] = (
                float(prediction),
                _clamped_confidence(confidence),
            )

    def _fallback_to_champion_or_last_good(
        self,
        *,
        symbol: str,
        horizon: int,
        base_family: str,
        base_prediction: float | None,
        base_confidence: float,
        diagnostics: dict[str, Any],
        reason: str,
    ) -> BlendResult:
        if base_prediction is not None:
            diagnostics.update({"applied": False, "fallback": True, "fallback_reason": str(reason)})
            return self._finish(
                symbol=str(symbol),
                horizon=int(horizon),
                base_family=str(base_family),
                prediction=float(base_prediction),
                confidence=float(base_confidence),
                applied=False,
                diagnostics=diagnostics,
                cascade_depth=2,
            )

        key = self._last_good_key(symbol, horizon, base_family)
        last_prediction, last_confidence = self._last_good_by_key.get(key, (0.0, 0.0))
        diagnostics.update(
            {
                "applied": False,
                "fallback": True,
                "fallback_reason": f"{str(reason)}_champion_missing",
                "last_good_prediction": float(last_prediction),
            }
        )
        return self._finish(
            symbol=str(symbol),
            horizon=int(horizon),
            base_family=str(base_family),
            prediction=float(last_prediction),
            confidence=float(last_confidence),
            applied=False,
            diagnostics=diagnostics,
            cascade_depth=3,
        )

    def _finish(
        self,
        *,
        symbol: str,
        horizon: int,
        base_family: str,
        prediction: float,
        confidence: float,
        applied: bool,
        diagnostics: dict[str, Any],
        cascade_depth: int,
    ) -> BlendResult:
        depth = int(cascade_depth)
        diagnostics["fallback_cascade_depth"] = int(depth)
        reason = str(diagnostics.get("fallback_reason") or "")
        if depth >= 1:
            _warn_nonfatal(
                "ENSEMBLE_FALLBACK_DEPTH",
                None,
                symbol=str(symbol),
                horizon=int(horizon),
                depth=int(depth),
                reason=reason,
            )
        if depth >= 2:
            LOG.error(
                "ensemble_fallback_depth symbol=%s horizon=%s depth=%s reason=%s",
                str(symbol),
                int(horizon),
                int(depth),
                reason,
            )
        _emit_fallback_depth_metric(
            symbol=str(symbol),
            horizon=int(horizon),
            base_family=str(base_family),
            depth=int(depth),
            reason=reason,
        )
        confidence_value = _clamped_confidence(confidence)
        self._record_last_good(
            symbol=str(symbol),
            horizon=int(horizon),
            base_family=str(base_family),
            prediction=float(prediction),
            confidence=float(confidence_value),
        )
        return BlendResult(float(prediction), float(confidence_value), bool(applied), diagnostics)

    def blend(
        self,
        *,
        symbol: str,
        horizon: int,
        ts: int | None,
        base_prediction: float,
        base_confidence: float,
        base_family: str,
        predict_family: Callable[[str], Any],
    ) -> BlendResult:
        diagnostics: dict[str, Any] = {
            "mode": self.mode,
            "base_family": str(base_family or ""),
            "requested_ts_ms": int(ts if ts is not None else time.time() * 1000),
            "components": {},
            "weights": {},
            "excluded_families": {},
            "missing_families": [],
        }
        base_prediction_value = _safe_finite_float(base_prediction)
        base_confidence_value = _clamped_confidence(base_confidence)
        if self.mode == "single_champion":
            return self._fallback_to_champion_or_last_good(
                symbol=str(symbol),
                horizon=int(horizon),
                base_family=str(base_family),
                base_prediction=base_prediction_value,
                base_confidence=float(base_confidence_value),
                diagnostics=diagnostics,
                reason="single_champion_mode",
            )

        weight_row = load_latest_weights(str(symbol), int(horizon), con=self.con)
        if not weight_row:
            return self._fallback_to_champion_or_last_good(
                symbol=str(symbol),
                horizon=int(horizon),
                base_family=str(base_family),
                base_prediction=base_prediction_value,
                base_confidence=float(base_confidence_value),
                diagnostics=diagnostics,
                reason="no_ensemble_weights",
            )

        raw_weights = dict(weight_row.get("weights") or {})
        eligible_weights, excluded = filter_weights_by_eligibility(
            raw_weights,
            symbol=str(symbol),
            horizon=int(horizon),
            con=self.con,
        )
        diagnostics["weight_ts"] = int(weight_row.get("ts") or 0)
        diagnostics["intercept"] = float(weight_row.get("intercept") or 0.0)
        diagnostics["alpha"] = float(weight_row.get("alpha") or 0.0)
        diagnostics["n_train_obs"] = int(weight_row.get("n_train_obs") or 0)
        diagnostics["val_metric"] = weight_row.get("val_metric")
        diagnostics["excluded_families"] = dict(excluded)
        if not eligible_weights:
            return self._fallback_to_champion_or_last_good(
                symbol=str(symbol),
                horizon=int(horizon),
                base_family=str(base_family),
                base_prediction=base_prediction_value,
                base_confidence=float(base_confidence_value),
                diagnostics=diagnostics,
                reason="no_eligible_weights",
            )

        components: dict[str, dict[str, Any]] = {}
        missing: list[str] = []
        for family in sorted(eligible_weights):
            try:
                parsed = _parse_component_result(str(family), predict_family(str(family)))
            except Exception as e:
                _warn_nonfatal(
                    "ENSEMBLE_COMPONENT_PREDICTION_FAILED",
                    e,
                    family=str(family),
                    symbol=str(symbol),
                    horizon=int(horizon),
                )
                parsed = None
            if parsed is None:
                missing.append(str(family))
                continue
            components[str(family)] = parsed
        diagnostics["components"] = dict(components)
        diagnostics["weights"] = {str(family): float(eligible_weights[family]) for family in sorted(eligible_weights)}
        diagnostics["missing_families"] = list(missing)
        if missing and not components:
            return self._fallback_to_champion_or_last_good(
                symbol=str(symbol),
                horizon=int(horizon),
                base_family=str(base_family),
                base_prediction=base_prediction_value,
                base_confidence=float(base_confidence_value),
                diagnostics=diagnostics,
                reason="missing_component_predictions",
            )

        cascade_depth = 1 if missing else 0
        blend_weights = dict(eligible_weights)
        if missing:
            blend_weights = _renormalize_available_weights(eligible_weights, set(components))
            diagnostics["weights"] = {str(family): float(blend_weights[family]) for family in sorted(blend_weights)}
            diagnostics["weight_renormalized"] = True
        else:
            diagnostics["weight_renormalized"] = False

        prediction = float(weight_row.get("intercept") or 0.0)
        confidence_num = 0.0
        confidence_den = 0.0
        for family, weight in blend_weights.items():
            component = components[str(family)]
            prediction += float(weight) * float(component["prediction"])
            if component.get("confidence") is not None:
                confidence_num += abs(float(weight)) * float(component["confidence"])
                confidence_den += abs(float(weight))
        confidence = float(confidence_num / confidence_den) if confidence_den > 0 else float(base_confidence_value)
        diagnostics.update(
            {
                "applied": True,
                "fallback": bool(cascade_depth > 0),
                "fallback_reason": "partial_component_predictions" if cascade_depth > 0 else "",
                "final_prediction": float(prediction),
                "aggregated_confidence": float(max(0.0, min(1.0, confidence))),
            }
        )
        return self._finish(
            symbol=str(symbol),
            horizon=int(horizon),
            base_family=str(base_family),
            prediction=float(prediction),
            confidence=float(confidence),
            applied=True,
            diagnostics=diagnostics,
            cascade_depth=int(cascade_depth),
        )
