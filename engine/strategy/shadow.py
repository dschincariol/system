"""
FILE: shadow.py

Runs shadow-model predictions alongside the champion path without placing
trades. This lets the system collect out-of-sample model telemetry before a
promotion decision.
"""

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect
from engine.execution.kill_switch import execution_allowed
from engine.model_registry import get_stage_latest
from engine.strategy.model_config import configured_model_horizons, configured_model_names
from engine.strategy.model_v2 import get_current_regime, get_regime_prior
from engine.strategy.temporal_predictor import predict_temporal_shadow
from engine.strategy.predictor import _track_prediction_output, predict_forced_model
from engine.strategy.champion_manager import get_champion_assignment, get_live_competition_champion_name
from engine.strategy.model_marketplace import record_shadow_order

DEFAULT_HORIZONS = [
    int(x.strip())
    for x in os.environ.get("CHALLENGER_HORIZONS_S", "300,3600,86400").split(",")
    if str(x).strip()
]
for _cfg_horizon in configured_model_horizons(default=[]):
    if int(_cfg_horizon) > 0 and int(_cfg_horizon) not in DEFAULT_HORIZONS:
        DEFAULT_HORIZONS.append(int(_cfg_horizon))
LOG = get_logger("engine.strategy.shadow")
_WARNED_NONFATAL_KEYS: set[str] = set()


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
        component="engine.strategy.shadow",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _competition_horizons(horizon_s: int) -> list[int]:
    hs = []
    for x in list(DEFAULT_HORIZONS) + [int(horizon_s or 0)]:
        try:
            xi = int(x)
            if xi > 0 and xi not in hs:
                hs.append(xi)
        except Exception as e:
            _warn_nonfatal(
                "SHADOW_COMPETITION_HORIZON_PARSE_FAILED",
                e,
                once_key="competition_horizon_parse",
                horizon_value=repr(x),
            )
            continue
    return hs


def _confidence_from_sample(n: int) -> float:
    try:
        nn = max(0, int(n))
    except Exception:
        nn = 0
    return float(max(0.0, min(1.0, nn / 100.0)))


def _shadow_side(predicted_z: float) -> str:
    if float(predicted_z) > 0.0:
        return "buy"
    if float(predicted_z) < 0.0:
        return "sell"
    return "hold"


def _load_competition_models(symbol: str, horizon_s: int) -> List[Tuple[str, Optional[str], Optional[int]]]:
    sym = str(symbol or "").upper().strip()
    hs = int(horizon_s or 0)
    names: Dict[str, Tuple[Optional[str], Optional[int]]] = {}
    con = connect()
    try:
        try:
            rows = con.execute(
                """
                SELECT DISTINCT model_name,
                       json_extract(meta_json, '$.model_kind'),
                       json_extract(meta_json, '$.model_ts_ms')
                FROM model_marketplace_scores
                WHERE symbol=?
                  AND horizon_s IN (?, 0)
                ORDER BY updated_ts_ms DESC
                """,
                (sym, int(hs)),
            ).fetchall() or []
        except Exception:
            rows = []
        for model_name, model_kind, model_ts_ms in rows:
            name = str(model_name or "").strip()
            if name and name not in names:
                names[name] = (
                    (str(model_kind).strip() if model_kind not in (None, "") else None),
                    (int(model_ts_ms) if model_ts_ms not in (None, "") else None),
                )

        try:
            reg_rows = con.execute(
                """
                SELECT DISTINCT model_name, model_kind, model_ts_ms
                FROM model_registry
                WHERE COALESCE(status, CASE
                  WHEN stage='champion' THEN 'champion'
                  WHEN stage='challenger' THEN 'challenger'
                  ELSE 'inactive'
                END) IN ('champion','challenger')
                ORDER BY updated_ts_ms DESC, created_ts_ms DESC
                """
            ).fetchall() or []
        except Exception:
            reg_rows = []
        for model_name, model_kind, model_ts_ms in reg_rows:
            name = str(model_name or "").strip()
            if name and name not in names:
                names[name] = (
                    (str(model_kind).strip() if model_kind not in (None, "") else None),
                    (int(model_ts_ms) if model_ts_ms not in (None, "") else None),
                )
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "SHADOW_COMPETITION_MODELS_CLOSE_FAILED",
                e,
                once_key="competition_models_close",
                symbol=str(symbol),
                horizon_s=int(horizon_s),
            )

    try:
        live_name = str(get_live_competition_champion_name(sym, hs) or "").strip()
        if live_name and live_name not in names:
            names[live_name] = (None, None)
    except Exception as e:
        _warn_nonfatal(
            "SHADOW_LIVE_CHAMPION_LOOKUP_FAILED",
            e,
            once_key="live_competition_champion_name",
            symbol=str(sym),
            horizon_s=int(hs),
        )

    try:
        champ = get_champion_assignment("global", sym, hs)
        champ_name = str((champ or {}).get("model_name") or "").strip()
        champ_meta = dict((champ or {}).get("meta") or {})
        if champ_name and champ_name not in names:
            names[champ_name] = (
                (str(champ_meta.get("model_kind") or "").strip() or None),
                (_safe_int(champ_meta.get("model_ts_ms")) if champ_meta.get("model_ts_ms") not in (None, "") else None),
            )
    except Exception as e:
        _warn_nonfatal(
            "SHADOW_CHAMPION_ASSIGNMENT_LOOKUP_FAILED",
            e,
            once_key="champion_assignment_lookup",
            symbol=str(sym),
            horizon_s=int(hs),
        )

    for configured_name in configured_model_names(symbol=sym, horizon_s=hs):
        name = str(configured_name or "").strip()
        if name and name not in names:
            names[name] = (None, None)

    return [(name, meta[0], meta[1]) for name, meta in names.items() if name]


def list_competition_models(symbol: str, horizon_s: int) -> List[Tuple[str, Optional[str], Optional[int]]]:
    return _load_competition_models(symbol, horizon_s)

def log_shadow_prediction(
    *,
    event_id: int,
    symbol: str,
    horizon_s: int,
    predicted_z: float,
    confidence: float,
    model_name: str,
    model_id: Optional[str] = None,
    model_kind: Optional[str] = None,
    model_ts_ms: Optional[int] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    con = connect()
    try:
        regime = None
        try:
            regime = str(get_current_regime(str(symbol)) or "").strip()
        except Exception:
            regime = None

        # Reuse the live execution gate so shadow telemetry reflects the same
        # tradability constraints as the champion path.
        allow, _, _ = execution_allowed(con=con, symbol=symbol, regime=regime)
        if not allow:
            return

        cost = None
        net = float(predicted_z)

        con.execute(
            """
            INSERT INTO shadow_predictions
              (ts_ms, event_id, symbol, regime, horizon_s,
               model_name, model_kind, model_ts_ms,
               predicted_z, confidence, cost_est, net_pred_z, extra_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                _now_ms(),
                int(event_id),
                str(symbol),
                regime,
                int(horizon_s),
                str(model_name),
                model_kind,
                model_ts_ms,
                float(predicted_z),
                float(confidence),
                cost,
                net,
                json.dumps(extra or {}, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.commit()
    finally:
        con.close()


def persist_shadow_model_signal(
    *,
    event_id: int,
    symbol: str,
    horizon_s: int,
    predicted_z: float,
    confidence: float,
    model_name: str,
    model_id: Optional[str] = None,
    model_kind: Optional[str] = None,
    model_ts_ms: Optional[int] = None,
    regime: Optional[str] = None,
    explain: Optional[Dict[str, Any]] = None,
) -> None:
    explain_dict = dict(explain or {})
    final_regime = str(regime or explain_dict.get("regime") or explain_dict.get("regime_at_trade") or "global")
    final_model_id = str(model_id or explain_dict.get("model_id") or model_name).strip() or str(model_name)
    final_model_kind = str(model_kind or explain_dict.get("model_kind") or "").strip() or None
    final_model_ts_ms = int(model_ts_ms or explain_dict.get("model_ts_ms") or _now_ms())
    model_intent = dict(explain_dict.get("model_intent") or {}) if isinstance(explain_dict.get("model_intent"), dict) else {}
    log_shadow_prediction(
        event_id=int(event_id),
        symbol=str(symbol),
        horizon_s=int(horizon_s),
        predicted_z=float(predicted_z),
        confidence=float(confidence),
        model_name=str(model_name),
        model_id=str(final_model_id),
        model_kind=final_model_kind,
        model_ts_ms=int(final_model_ts_ms),
        extra={"model_id": str(final_model_id), "meta": explain_dict},
    )
    record_shadow_order(
        model_name=str(model_name),
        symbol=str(symbol),
        side=_shadow_side(float(predicted_z)),
        qty=max(1.0, round(float(confidence) * 10.0, 4)),
        ref_price=None,
        confidence=float(confidence),
        horizon_s=int(horizon_s),
        regime=str(final_regime or "global"),
        meta={
            "event_id": int(event_id),
            "model_id": str(final_model_id),
            "model_kind": final_model_kind,
            "model_ts_ms": _safe_int(final_model_ts_ms),
            "model_version": (
                str(explain_dict.get("model_version")).strip()
                if explain_dict.get("model_version") not in (None, "")
                else None
            ),
            "predicted_z": float(predicted_z),
            "confidence": float(confidence),
            "regime": str(final_regime or "global"),
            "signal_ts_ms": _safe_int(explain_dict.get("signal_ts_ms") or explain_dict.get("ts_ms") or _now_ms()),
            "alpha_ttl_ms": (
                _safe_int(explain_dict.get("alpha_ttl_ms"))
                if explain_dict.get("alpha_ttl_ms") not in (None, "")
                else None
            ),
            "alpha_half_life_ms": (
                _safe_int(explain_dict.get("alpha_half_life_ms"))
                if explain_dict.get("alpha_half_life_ms") not in (None, "")
                else None
            ),
            "source_alert_id": (
                _safe_int(explain_dict.get("source_alert_id"))
                if explain_dict.get("source_alert_id") not in (None, "")
                else None
            ),
            "model_intent": model_intent,
            "explain": dict(explain_dict),
        },
    )

def shadow_predict(
    *,
    event_id: int,
    symbol: str,
    horizon_s: int,
    features: Any,
    temporal_predictions: Optional[Dict[tuple[str, int], tuple[float, float, Dict[str, Any]]]] = None,
) -> None:
    """
    Runs shadow model prediction in parallel to champion.
    NEVER returns a value. NEVER executes trades.
    """
    horizons = _competition_horizons(int(horizon_s or 0))
    now = _now_ms()

    con = connect()
    try:
        forced_models = _load_competition_models(symbol, int(horizon_s or 0))
        for hi in horizons:
            for forced_name, forced_kind, forced_ts_ms in forced_models:
                family = str(forced_name or "").strip().lower()
                if not forced_name or family.startswith("temporal_predictor"):
                    continue
                try:
                    pred_z, conf, explain = predict_forced_model(
                        features,
                        symbol=str(symbol),
                        horizon_s=int(hi),
                        model_name=str(forced_name),
                        top_k=8,
                        event={
                            "id": int(event_id),
                            "event_id": int(event_id),
                            "ts_ms": int(now),
                            "symbol": str(symbol),
                        },
                    )
                except Exception as e:
                    _warn_nonfatal(
                        "SHADOW_FORCED_MODEL_PREDICT_FAILED",
                        e,
                        once_key=f"forced_model_predict_{forced_name}_{hi}",
                        symbol=str(symbol),
                        horizon_s=int(hi),
                        model_name=str(forced_name),
                    )
                    continue
                explain = dict(explain or {})
                model_name = str(explain.get("model_name") or forced_name).strip()
                model_kind = str(explain.get("model_kind") or forced_kind or "").strip() or None
                model_ts_ms = int(explain.get("model_ts_ms") or forced_ts_ms or now)
                model_id = str(explain.get("model_id") or model_name).strip() or model_name
                regime = str(explain.get("regime") or explain.get("regime_at_trade") or get_current_regime(symbol) or "global")
                extra_meta = {
                    **explain,
                    "event_id": int(event_id),
                    "features_present": features is not None,
                    "model_id": str(model_id),
                }
                persist_shadow_model_signal(
                    event_id=int(event_id),
                    symbol=symbol,
                    horizon_s=int(hi),
                    predicted_z=float(pred_z),
                    confidence=float(conf),
                    model_name=model_name,
                    model_id=str(model_id),
                    model_kind=model_kind,
                    model_ts_ms=int(model_ts_ms),
                    regime=str(regime or "global"),
                    explain=extra_meta,
                )

        lifecycle_candidate = None
        try:
            lifecycle_candidate = get_stage_latest(
                model_name="regime_stats_v2",
                stage="challenger",
                regime="global",
            )
            if not lifecycle_candidate:
                lifecycle_candidate = get_stage_latest(
                    model_name="regime_stats_v2",
                    stage="shadow",
                    regime="global",
                )
        except Exception:
            lifecycle_candidate = None

        for hi in horizons:
            candidate_version = str((lifecycle_candidate or {}).get("model_version") or "").strip()
            candidate_name = str((lifecycle_candidate or {}).get("model_name") or "regime_stats_v2").strip()
            candidate_kind = str((lifecycle_candidate or {}).get("model_kind") or "shadow_regime_stats").strip()
            candidate_ts_ms = int((lifecycle_candidate or {}).get("model_ts_ms") or now)
            try:
                pred_z, n, regime = get_regime_prior(
                    str(symbol),
                    int(hi),
                    model_version=(candidate_version or None),
                    model_name=(candidate_name or "regime_stats_v2"),
                )
            except Exception:
                pred_z, n, regime = 0.0, 0, "global"
            if int(n or 0) <= 0:
                continue

            model_name = candidate_name or f"regime_stats_{int(hi)}"
            model_kind = candidate_kind or "shadow_regime_stats"
            model_ts_ms = int(candidate_ts_ms or now)

            persist_shadow_model_signal(
                event_id=event_id,
                symbol=symbol,
                horizon_s=int(hi),
                predicted_z=float(pred_z),
                confidence=float(_confidence_from_sample(int(n))),
                model_name=str(model_name),
                model_id=str(model_name),
                model_kind=str(model_kind),
                model_ts_ms=int(model_ts_ms),
                regime=str(regime),
                explain={
                    "model_id": str(model_name),
                    "regime": str(regime),
                    "train_rows": int(n),
                    "model_version": (candidate_version or None),
                    "features_present": features is not None,
                },
            )
            _track_prediction_output(
                symbol=str(symbol),
                horizon_s=int(hi),
                prediction=float(pred_z),
                confidence=float(_confidence_from_sample(int(n))),
                explain={
                    "model_name": str(model_name),
                    "model_id": str(model_name),
                    "model_kind": str(model_kind),
                    "model_version": (candidate_version or ""),
                    "feature_set_tag": "shadow_regime_prior",
                    "regime": str(regime),
                },
                source="shadow_predict",
            )

        temporal = temporal_predictions if isinstance(temporal_predictions, dict) else None
        if temporal is None:
            try:
                temporal = predict_temporal_shadow(
                    con,
                    ts_ms=int(now),
                    symbols=[str(symbol)],
                    horizons=horizons,
                )
            except Exception:
                temporal = None

        for (sym, hi), (pred_z, conf, explain) in (temporal or {}).items():
            explain = dict(explain or {})
            model_name = str(
                explain.get("model_key")
                or f"temporal_{str(explain.get('model_key_type') or 'global')}_{int(hi)}"
            )
            persist_shadow_model_signal(
                event_id=event_id,
                symbol=str(sym),
                horizon_s=int(hi),
                predicted_z=float(pred_z),
                confidence=float(conf),
                model_name=model_name,
                model_id=str(explain.get("model_id") or model_name),
                model_kind=str(explain.get("model_kind") or "temporal_mlp"),
                model_ts_ms=(int(explain.get("model_ts_ms") or now)),
                regime=str(explain.get("regime") or explain.get("regime_at_trade") or "global"),
                explain={**explain, "model_id": str(explain.get("model_id") or model_name)},
            )
            _track_prediction_output(
                symbol=str(sym),
                horizon_s=int(hi),
                prediction=float(pred_z),
                confidence=float(conf),
                explain={
                    **dict(explain or {}),
                    "model_name": str(model_name),
                    "model_id": str(explain.get("model_id") or model_name),
                    "model_kind": str(explain.get("model_kind") or "temporal_mlp"),
                    "model_version": str(explain.get("model_version") or ""),
                    "feature_set_tag": str(
                        explain.get("feature_set_tag")
                        or explain.get("features_version")
                        or "temporal_shadow"
                    ),
                },
                source="shadow_predict",
            )
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "SHADOW_PREDICT_CLOSE_FAILED",
                e,
                once_key="shadow_predict_close",
                symbol=str(symbol),
                horizon_s=int(horizon_s),
                event_id=int(event_id),
            )
