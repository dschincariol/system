"""Main live prediction orchestrator for the repo's trading-model families.

The predictor resolves which model should serve a symbol and horizon, restores
the feature contract recorded at training time, runs the appropriate model
adapter, and falls back to safer baseline logic when a newer family is missing
or not ready.
"""

import math
import os
import time
import json
import logging
import threading
from typing import Any, Callable, Dict, List, Mapping, Tuple, Optional

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

from engine.data.asset_map import asset_class_for_symbol
from engine.model_registry import DEFAULT_MODEL_REGISTRY, get_active_model_name, get_active_model_spec, get_model_spec
from engine.prediction_logger import DEFAULT_PREDICTION_LOGGER
from engine.runtime.storage import connect
from engine.strategy.ensemble.blender import (
    EnsembleBlender as RidgeStackBlender,
    ensemble_mode as ridge_stack_ensemble_mode,
)
from engine.strategy.ensemble import hedge as hedge_ensemble
from engine.strategy.ensemble_blender import (
    blend_predictions,
    clear_prediction_context,
    collect_family_predictions,
    compute_blend_weights,
    ensemble_blend_enabled,
    ensemble_blend_mode,
    ensemble_min_agreement,
    persist_blend_weights,
    persist_ensemble_prediction,
    set_prediction_context,
)
from engine.strategy.champion_manager import get_champion_assignment, get_live_competition_champion_name
from engine.strategy.confidence_engine import (
    apply_confidence_payload,
    calibrate_confidence_score,
    describe_signal_confidence,
)
from engine.strategy.conformal import apply_conformal_to_explain
from engine.strategy.learning import (
    confidence_from_weight,
    confidence_from_n,
    get_global_prior,
    learn_relevance_stats,
)
from engine.strategy.model_v2 import get_regime_prior, get_spillover_betas, get_current_regime, get_live_model_version

# ------            -- ------------------------------------------------------
# Option A: supervised embedding regressor (OPT-IN)
# ------            -- ------------------------------------------------------
from engine.strategy.embed_regressor import predict_with_embed_model
from engine.strategy.gbm_regressor import load_gbm_model_record, predict_with_gbm_model
from engine.strategy.models.lgbm_ranker import (
    load_model_from_artifact as load_lgbm_ranker_model_from_artifact,
    ranker_scores_to_signals,
)
from engine.strategy.models.itransformer import load_model_from_artifact as load_itransformer_model_from_artifact
from engine.strategy.models.lgbm_regressor import load_model_from_artifact as load_lgbm_model_from_artifact
from engine.strategy.models.patchtst import load_model_from_artifact as load_patchtst_model_from_artifact
from engine.strategy.models.xgb_regressor import load_model_from_artifact as load_xgb_model_from_artifact
from engine.strategy.feature_expansion import build_feature_vector, feature_set_tag
from engine.strategy.feature_registry import (
    assert_no_shadow_features,
    build_feature_snapshot,
    feature_set_tag_from_ids,
    resolve_feature_ids,
)
from engine.strategy.feature_neutralization import neutralize_mode, neutralize_predictions
from engine.strategy.ood import score_ood
from engine.strategy.model_config import (
    active_model_names,
    get_model_config,
    is_active_model_name,
    primary_active_model_name,
    resolve_active_model_name,
)
from engine.strategy.shap_explainer import (
    explain_prediction,
    normalize_explanation_payload,
    shap_explanations_enabled,
    shap_live_compute_enabled,
    shap_persist_explanations_enabled,
    shap_top_k,
)
from engine.strategy.temporal_predictor import predict_temporal_live
from engine.runtime.failure_diagnostics import log_failure

_USE_EMBED_REGRESSOR = os.environ.get("USE_EMBED_REGRESSOR", "0") == "1"
_USE_GBM_REGRESSOR = os.environ.get("USE_GBM_REGRESSOR", "0") == "1"
MODEL_NAME = (
    primary_active_model_name()
    or os.environ.get("MODEL_NAME", "embed_regressor").strip()
    or "embed_regressor"
)
_EMBED_REGRESSOR_CONF_K = float(os.environ.get("EMBED_REGRESSOR_CONF_K", "75.0"))
_EMBED_CONF_CALIB = os.environ.get("EMBED_CONF_CALIB", "1") == "1"

# Small in-process cache so repeated predictions do not hit SQLite for the same
# calibration curves on every call.
_CALIB_CACHE_TTL_S = 60.0
_calib_cache = {
    "ts_s": 0.0,
    "curves": {},  # (horizon_s, model_kind) -> (xs, ys)
}
_CALIB_LOCK = threading.Lock()
_FEATURE_SNAPSHOT_PREFETCH = threading.local()
_PREFETCH_UNSET = object()
LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: Optional[str] = None, **extra: Any) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOGGER,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component=__name__,
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


def _registry_feature_set_tag(feature_ids: Any, *, model_name: str = "", model_spec: Any = None) -> str:
    try:
        resolved = resolve_feature_ids(feature_ids, model_name=str(model_name or ""), model_spec=model_spec)
    except Exception:
        if isinstance(feature_ids, (list, tuple)):
            resolved = [str(value) for value in feature_ids if str(value or "").strip()]
        else:
            resolved = []
    try:
        return str(feature_set_tag_from_ids(resolved) or "").strip()
    except Exception as e:
        _warn_nonfatal(
            "predictor_feature_set_tag_from_registry_failed",
            "PREDICTOR_FEATURE_SET_TAG_FROM_REGISTRY_FAILED",
            e,
            warn_key="predictor_feature_set_tag_from_registry_failed",
        )
        return str(feature_set_tag(resolved) or "").strip()


def _attach_ood_diagnostics(
    explain: Mapping[str, Any] | None,
    model: Any,
    feature_map: Mapping[str, Any] | None,
    *,
    warn_key: str,
) -> Dict[str, Any]:
    explain_dict = dict(explain or {})
    try:
        features_payload = {"features": dict(feature_map or {})}
        if hasattr(model, "score_ood") and callable(getattr(model, "score_ood")):
            ood_payload = dict(model.score_ood(features_payload) or {})
        else:
            ood_payload = dict(score_ood(getattr(model, "ood_profile", None), features_payload) or {})
    except Exception as e:
        _warn_nonfatal(
            "predictor_ood_score_failed",
            "PREDICTOR_OOD_SCORE_FAILED",
            e,
            warn_key=str(warn_key or "predictor_ood_score_failed"),
        )
        return explain_dict
    if not ood_payload:
        return explain_dict
    explain_dict["ood"] = dict(ood_payload)
    if bool(ood_payload.get("available")):
        score = _safe_float(ood_payload.get("ood_score", ood_payload.get("ood_distance")), 0.0)
        explain_dict["ood_score"] = float(score)
        explain_dict["ood_distance"] = float(score)
        explain_dict["feature_ood_distance"] = float(score)
        explain_dict["ood_threshold"] = _safe_float(ood_payload.get("threshold"), 0.0)
        explain_dict["ood_hard_threshold"] = _safe_float(ood_payload.get("hard_threshold"), 0.0)
        explain_dict["ood_range_violation_count"] = int(_safe_int(ood_payload.get("range_violation_count"), 0))
    return explain_dict


def _track_prediction_output(
    *,
    symbol: str,
    horizon_s: int,
    prediction: float,
    confidence: float,
    explain: Dict[str, Any],
    source: str,
) -> None:
    explain_dict = dict(explain or {})
    model_name = str(explain_dict.get("model_name") or explain_dict.get("model") or "").strip()
    if not model_name:
        return
    model_version = str(explain_dict.get("model_version") or "").strip() or "legacy"
    feature_ids = list(explain_dict.get("feature_ids") or [])
    features_version = str(
        explain_dict.get("feature_set_tag")
        or dict(explain_dict.get("feature_schema") or {}).get("feature_set_tag")
        or _registry_feature_set_tag(feature_ids, model_name=model_name)
        or "unknown"
    ).strip() or "unknown"
    tracking_metadata = {
        "source": str(source),
        "symbol": str(symbol or "").upper().strip(),
        "horizon_s": int(horizon_s),
        "model_id": str(explain_dict.get("model_id") or model_name),
        "model_kind": str(explain_dict.get("model_kind") or ""),
        "model_family": str(explain_dict.get("model_family") or ""),
        "requested_model_family": str(explain_dict.get("requested_model_family") or ""),
        "requested_model_name": str(explain_dict.get("requested_model_name") or model_name),
        "resolved_model_name": str(explain_dict.get("resolved_model_name") or model_name),
        "resolution_source": str(explain_dict.get("resolution_source") or ""),
        "served_model_family": str(
            explain_dict.get("served_model_family")
            or explain_dict.get("model_family")
            or ""
        ),
        "serve_fallback_active": bool(explain_dict.get("serve_fallback_active")),
        "fallback_reason": str(explain_dict.get("fallback_reason") or ""),
        "candidate_names": [
            str(name)
            for name in (explain_dict.get("candidate_names") or [])
            if str(name or "").strip()
        ],
        "features_version": str(features_version),
        "feature_ids": feature_ids,
        "regime": str(explain_dict.get("regime") or explain_dict.get("regime_at_trade") or ""),
    }
    try:
        DEFAULT_MODEL_REGISTRY.register_model(
            name=str(model_name),
            version=str(model_version),
            metadata=tracking_metadata,
        )
    except Exception as e:
        _warn_nonfatal(
            "predictor_tracking_register_failed",
            "PREDICTOR_TRACKING_REGISTER_FAILED",
            e,
            warn_key=None,
            symbol=str(symbol),
            horizon_s=int(horizon_s),
            model_name=str(model_name),
            model_version=str(model_version),
        )
    event_id = _safe_int(explain_dict.get("event_id"), 0)
    if str(source) != "shadow_predict" and int(event_id) > 0:
        return
    try:
        DEFAULT_PREDICTION_LOGGER.log_prediction_nowait(
            model_name=str(model_name),
            model_version=str(model_version),
            symbol=str(symbol or "").upper().strip(),
            timestamp=int(explain_dict.get("signal_ts_ms") or explain_dict.get("ts_ms") or int(time.time() * 1000)),
            prediction=float(prediction),
            confidence=float(confidence),
            features_version=str(features_version),
            event_id=(int(event_id) if int(event_id) > 0 else None),
            horizon_s=int(horizon_s),
            model_id=str(explain_dict.get("model_id") or model_name),
            tracking_source=str(source),
            metadata=dict(tracking_metadata),
        )
    except Exception as e:
        _warn_nonfatal(
            "predictor_tracking_prediction_failed",
            "PREDICTOR_TRACKING_PREDICTION_FAILED",
            e,
            warn_key=None,
            symbol=str(symbol),
            horizon_s=int(horizon_s),
            model_name=str(model_name),
            model_version=str(model_version),
        )


def _prediction_explanation_top_k() -> int:
    try:
        return max(1, int(shap_top_k()))
    except Exception as e:
        _warn_nonfatal(
            "predictor_shap_top_k_failed",
            "PREDICTOR_SHAP_TOP_K_FAILED",
            e,
            warn_key="prediction_explanation_top_k",
        )
        return 10


def _queue_prediction_explanation(
    *,
    symbol: str,
    horizon_s: int,
    event: Optional[Dict[str, Any]],
    explain: Dict[str, Any],
) -> None:
    if not shap_explanations_enabled() or not shap_persist_explanations_enabled():
        return
    payload = dict(explain.get("prediction_explanation") or {})
    if not payload:
        return
    diagnostics = dict(payload.get("diagnostics") or {})
    diagnostics.setdefault("horizon_s", int(horizon_s))
    diagnostics.setdefault("model_id", str(explain.get("model_id") or ""))
    diagnostics.setdefault("model_kind", str(explain.get("model_kind") or ""))
    diagnostics.setdefault("feature_set_tag", str(explain.get("feature_set_tag") or ""))
    if event is not None:
        event_id = int(_safe_int((event or {}).get("event_id"), 0))
        if event_id > 0:
            diagnostics.setdefault("event_id", int(event_id))
    try:
        DEFAULT_PREDICTION_LOGGER.log_prediction_explanation_nowait(
            symbol=str(symbol or "").upper().strip(),
            timestamp=int((event or {}).get("ts_ms") or explain.get("signal_ts_ms") or int(time.time() * 1000)),
            model_family=str(explain.get("model_family") or explain.get("model") or ""),
            model_name=(str(explain.get("model_name") or "") or None),
            version=(str(explain.get("model_version") or "") or None),
            explanation_type=str(payload.get("explanation_type") or "unsupported"),
            top_features=list(payload.get("top_features") or []),
            base_value=payload.get("base_value"),
            diagnostics=diagnostics,
        )
    except Exception as e:
        _warn_nonfatal(
            "predictor_prediction_explanation_queue_failed",
            "PREDICTOR_PREDICTION_EXPLANATION_QUEUE_FAILED",
            e,
            warn_key=None,
            symbol=str(symbol),
            horizon_s=int(horizon_s),
        )


def _maybe_attach_prediction_explanation(
    *,
    symbol: str,
    horizon_s: int,
    event: Optional[Dict[str, Any]],
    explain: Dict[str, Any],
    feature_snapshot: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    explain_dict = dict(explain or {})
    if not shap_explanations_enabled():
        return explain_dict

    try:
        feature_ids = resolve_feature_ids(
            explain_dict.get("feature_ids"),
            model_name=str(explain_dict.get("model_name") or ""),
        )
    except Exception:
        feature_ids = list(explain_dict.get("feature_ids") or [])

    existing = explain_dict.get("prediction_explanation")
    if isinstance(existing, dict):
        normalized = normalize_explanation_payload(existing, feature_ids=feature_ids)
    else:
        feature_map = dict(feature_snapshot or {})
        raw_payload = explain_prediction(
            str(explain_dict.get("model_family") or explain_dict.get("model") or _model_family(explain_dict.get("model_name") or "")),
            None,
            {
                "symbol": str(symbol or "").upper().strip(),
                "ts_ms": int((event or {}).get("ts_ms") or explain_dict.get("signal_ts_ms") or int(time.time() * 1000)),
                "feature_ids": list(feature_ids or []),
                "feature_set_tag": str(
                    explain_dict.get("feature_set_tag")
                    or _registry_feature_set_tag(feature_ids, model_name=str(explain_dict.get("model_name") or ""))
                ),
                "feature_schema": dict(explain_dict.get("feature_schema") or {}),
                "features": dict(feature_map),
                "explain_context": dict(explain_dict),
            },
            top_k=_prediction_explanation_top_k(),
        )
        normalized = normalize_explanation_payload(raw_payload, feature_ids=feature_ids)

    explain_dict["prediction_explanation"] = dict(normalized or {})
    _queue_prediction_explanation(
        symbol=str(symbol),
        horizon_s=int(horizon_s),
        event=event,
        explain=dict(explain_dict),
    )
    return explain_dict


def _dedupe_model_names(names: List[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw_name in names or []:
        name = str(raw_name or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def _live_model_resolution(symbol: Optional[str] = None, horizon_s: Optional[int] = None) -> Dict[str, Any]:
    sym = str(symbol or "").upper().strip()
    hi = int(horizon_s or 0)
    candidate_records: List[Dict[str, str]] = []

    def _append_candidate(name: Any, source: str) -> None:
        model_name = str(name or "").strip()
        if not model_name:
            return
        candidate_records.append({"name": str(model_name), "source": str(source)})

    try:
        _append_candidate(get_live_competition_champion_name(sym, hi), "competition")
    except Exception as e:
        _warn_nonfatal(
            "predictor_live_model_competition_lookup_failed",
            "PREDICTOR_LIVE_MODEL_COMPETITION_LOOKUP_FAILED",
            e,
            warn_key=f"predictor_live_model_competition_lookup_failed:{sym}:{hi}",
            symbol=sym,
            horizon_s=int(hi),
        )
    if sym:
        try:
            row = get_champion_assignment("global", sym, hi)
            if not row and hi != 0:
                row = get_champion_assignment("global", sym, 0)
            _append_candidate((row or {}).get("model_name"), "assignment")
        except Exception as e:
            _warn_nonfatal(
                "predictor_live_model_assignment_lookup_failed",
                "PREDICTOR_LIVE_MODEL_ASSIGNMENT_LOOKUP_FAILED",
                e,
                warn_key=f"predictor_live_model_assignment_lookup_failed:{sym}:{hi}",
                symbol=sym,
                horizon_s=int(hi),
            )
    try:
        _append_candidate(get_active_model_name(regime="global"), "registry")
    except Exception as e:
        _warn_nonfatal(
            "predictor_live_model_registry_lookup_failed",
            "PREDICTOR_LIVE_MODEL_REGISTRY_LOOKUP_FAILED",
            e,
            warn_key="predictor_live_model_registry_lookup_failed",
        )
    _append_candidate(MODEL_NAME, "env_default")

    candidate_names = _dedupe_model_names([row.get("name") for row in candidate_records])
    requested_model_name = str(candidate_names[0] if candidate_names else MODEL_NAME).strip() or MODEL_NAME
    resolved_model_name = str(
        resolve_active_model_name(
            symbol=sym,
            horizon_s=hi,
            preferred_names=candidate_names,
        )
        or ""
    ).strip()
    if not resolved_model_name:
        resolved_model_name = str(MODEL_NAME)

    resolution_source = ""
    for row in candidate_records:
        if str(row.get("name") or "").strip() == resolved_model_name:
            resolution_source = str(row.get("source") or "").strip()
            break
    if not resolution_source:
        resolution_source = "env_default" if resolved_model_name == str(MODEL_NAME) else "registry"

    fallback_reason = ""
    serve_fallback_active = bool(
        requested_model_name
        and resolved_model_name
        and requested_model_name != resolved_model_name
    )
    if serve_fallback_active:
        fallback_reason = (
            f"resolved_to_{resolution_source}"
            if resolution_source
            else "live_model_resolution_fallback"
        )

    return {
        "requested_model_name": str(requested_model_name),
        "resolved_model_name": str(resolved_model_name),
        "requested_model_family": str(_model_family(requested_model_name)),
        "resolution_source": str(resolution_source or "env_default"),
        "candidate_names": list(candidate_names),
        "serve_fallback_active": bool(serve_fallback_active),
        "fallback_reason": str(fallback_reason),
    }


def _live_model_name(symbol: Optional[str] = None, horizon_s: Optional[int] = None) -> str:
    resolution = _live_model_resolution(symbol, horizon_s)
    return str(resolution.get("resolved_model_name") or MODEL_NAME).strip() or str(MODEL_NAME)


def _model_family(model_name: str) -> str:
    name = str(model_name or "").strip().lower()
    if name == "lgbm_regressor" or name.startswith("lgbm_regressor"):
        return "lgbm_regressor"
    if name == "lgbm_ranker" or name.startswith("lgbm_ranker"):
        return "lgbm_ranker"
    if name == "xgb_regressor" or name.startswith("xgb_regressor"):
        return "xgb_regressor"
    if name == "patchtst" or name.startswith("patchtst"):
        return "patchtst"
    if name == "itransformer" or name.startswith("itransformer"):
        return "itransformer"
    if name == "gbm_regressor" or name.startswith("gbm_regressor"):
        return "gbm_regressor"
    if name == "temporal_predictor" or name.startswith("temporal_predictor"):
        return "temporal_predictor"
    if name.startswith("regime_stats_") or name == "regime_stats":
        return "regime_stats"
    return "embed_regressor"


def _live_feature_contract_required() -> bool:
    try:
        from engine.runtime.live_ai_safety import live_ai_required

        return bool(live_ai_required())
    except Exception as e:
        _warn_nonfatal(
            "predictor_live_feature_contract_required_fallback",
            "PREDICTOR_LIVE_FEATURE_CONTRACT_REQUIRED_FAILED",
            e,
            warn_key="predictor_live_feature_contract_required_fallback",
        )
        modes = {
            str(os.environ.get("ENGINE_MODE") or "").strip().lower(),
            str(os.environ.get("EXECUTION_MODE") or "").strip().lower(),
        }
        return "live" in modes


def _resolve_active_model(symbol: str, horizon_s: int, forced_model_name: Optional[str] = None) -> Dict[str, Any]:
    # Resolution order matters:
    # 1. explicit override
    # 2. current champion/challenger assignment
    # 3. configured default
    # 4. registry active model
    #
    # That keeps live inference aligned with governance decisions while still
    # leaving a safe fallback when newer families are absent.
    forced_name = str(forced_model_name or "").strip()
    if forced_name:
        if not is_active_model_name(forced_name):
            raise ValueError(f"inactive_model:{forced_name}")
        resolution_meta = {
            "requested_model_name": str(forced_name),
            "resolved_model_name": str(forced_name),
            "requested_model_family": str(_model_family(forced_name)),
            "resolution_source": "forced",
            "candidate_names": [str(forced_name)],
            "serve_fallback_active": False,
            "fallback_reason": "",
        }
        model_name = str(forced_name)
    else:
        resolution_meta = _live_model_resolution(symbol, int(horizon_s))
        model_name = str(resolution_meta.get("resolved_model_name") or "").strip() or MODEL_NAME
    family = _model_family(model_name)
    spec = {}
    config = {}
    try:
        spec = get_model_spec(model_name, regime="global") or {}
    except Exception:
        spec = {}
    try:
        config = get_model_config(model_name) or {}
    except Exception:
        config = {}
    if (not spec.get("feature_ids")) and (not spec.get("feature_schema")):
        try:
            active_spec = get_active_model_spec(regime="global") or {}
        except Exception:
            active_spec = {}
        if str(active_spec.get("model_name") or "").strip() == model_name:
            spec = dict(active_spec)
    feature_ids = resolve_feature_ids(
        (spec.get("feature_ids") or config.get("feature_ids")),
        model_name=model_name,
        model_spec=(spec or config),
    )
    if _live_feature_contract_required():
        assert_no_shadow_features(
            list(feature_ids),
            context="live_model_serving",
            model_name=str(model_name),
        )
    model_version = ""
    try:
        model_version = str(spec.get("model_version") or "").strip()
    except Exception:
        model_version = ""
    if not model_version:
        try:
            model_version = str(get_live_model_version(model_name) or "").strip()
        except Exception:
            model_version = ""
    model_id = str(spec.get("model_id") or config.get("model_id") or model_name).strip() or model_name
    model_family = str(spec.get("model_family") or config.get("family") or family).strip() or family
    feature_schema = dict(spec.get("feature_schema") or config.get("feature_schema") or {})
    if feature_ids and not isinstance(feature_schema.get("feature_ids"), list):
        feature_schema["feature_ids"] = list(feature_ids)
    metadata = {}
    try:
        metadata = {
            **dict(config.get("metadata") or {}),
            **dict(spec.get("metadata") or {}),
            **dict(config.get("meta") or {}),
            **dict(spec.get("meta") or {}),
        }
    except Exception:
        metadata = {}
    learning_scope = str(
        spec.get("learning_scope")
        or config.get("learning_scope")
        or metadata.get("learning_scope")
        or feature_schema.get("learning_scope")
        or ""
    ).strip()
    asset_scope = str(
        spec.get("asset_scope")
        or spec.get("ranker_asset_scope")
        or spec.get("training_asset_scope")
        or config.get("asset_scope")
        or config.get("ranker_asset_scope")
        or config.get("training_asset_scope")
        or metadata.get("asset_scope")
        or metadata.get("ranker_asset_scope")
        or metadata.get("training_asset_scope")
        or feature_schema.get("asset_scope")
        or ""
    ).strip()
    feature_set = str(
        spec.get("feature_set_tag")
        or config.get("feature_set_tag")
        or _registry_feature_set_tag(feature_ids, model_name=model_name, model_spec=(spec or config))
    )
    if feature_set and not str(feature_schema.get("feature_set_tag") or "").strip():
        feature_schema["feature_set_tag"] = str(feature_set)
    return {
        "model_name": str(model_name),
        "model_id": str(model_id),
        "model_family": str(model_family),
        "family": str(family),
        "requested_model_name": str(resolution_meta.get("requested_model_name") or model_name),
        "resolved_model_name": str(resolution_meta.get("resolved_model_name") or model_name),
        "requested_model_family": str(resolution_meta.get("requested_model_family") or family),
        "resolution_source": str(resolution_meta.get("resolution_source") or ""),
        "candidate_names": list(resolution_meta.get("candidate_names") or [model_name]),
        "serve_fallback_active": bool(resolution_meta.get("serve_fallback_active")),
        "fallback_reason": str(resolution_meta.get("fallback_reason") or ""),
        "feature_ids": list(feature_ids),
        "feature_set_tag": str(feature_set),
        "feature_schema": dict(feature_schema),
        "model_kind": str(spec.get("model_kind") or config.get("model_kind") or ""),
        "model_ts_ms": int(spec.get("model_ts_ms") or 0),
        "model_version": str(model_version),
        "artifact_alias": str(spec.get("artifact_alias") or config.get("artifact_alias") or ""),
        "artifact_sha256": str(spec.get("artifact_sha256") or config.get("artifact_sha256") or ""),
        "artifact_path": str(spec.get("artifact_path") or config.get("artifact_path") or ""),
        "spec_source_stage": str(spec.get("source_stage") or ""),
        "asset_scope": str(asset_scope),
        "ranker_asset_scope": str(asset_scope),
        "learning_scope": str(learning_scope),
        "risk_profile": str(spec.get("risk_profile") or config.get("risk_profile") or ""),
        "symbol_universe": list(spec.get("symbol_universe") or config.get("symbol_universe") or []),
        "horizon_s": int(spec.get("horizon_s") or config.get("horizon_s") or 0),
        "horizons_s": list(spec.get("horizons_s") or config.get("horizons_s") or []),
        "training_window_days": int(spec.get("training_window_days") or config.get("training_window_days") or 0),
    }


def _apply_model_serving_diagnostics(
    explain: Dict[str, Any],
    active_model: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    explain_dict = dict(explain or {})
    active = dict(active_model or {})
    serve_fallback = dict(explain_dict.get("serve_fallback") or {})

    requested_model_name = str(
        active.get("requested_model_name")
        or explain_dict.get("requested_model_name")
        or active.get("model_name")
        or explain_dict.get("model_name")
        or explain_dict.get("model")
        or ""
    ).strip()
    resolved_model_name = str(
        active.get("resolved_model_name")
        or active.get("model_name")
        or explain_dict.get("resolved_model_name")
        or explain_dict.get("model_name")
        or explain_dict.get("model")
        or requested_model_name
        or ""
    ).strip()
    requested_model_family = str(
        active.get("requested_model_family")
        or explain_dict.get("requested_model_family")
        or _model_family(requested_model_name or resolved_model_name)
    ).strip()
    served_model_family = str(
        explain_dict.get("served_model_family")
        or serve_fallback.get("served_family")
        or explain_dict.get("model_family")
        or active.get("model_family")
        or active.get("family")
        or requested_model_family
    ).strip()
    resolution_source = str(
        active.get("resolution_source")
        or explain_dict.get("resolution_source")
        or ""
    ).strip()
    candidate_names = _dedupe_model_names(
        list(active.get("candidate_names") or explain_dict.get("candidate_names") or [])
    )
    fallback_reason = str(
        explain_dict.get("fallback_reason")
        or serve_fallback.get("reason")
        or active.get("fallback_reason")
        or ""
    ).strip()
    serve_fallback_active = bool(
        explain_dict.get("serve_fallback_active")
        or serve_fallback
        or active.get("serve_fallback_active")
        or (
            requested_model_name
            and resolved_model_name
            and requested_model_name != resolved_model_name
        )
        or (
            requested_model_family
            and served_model_family
            and requested_model_family != served_model_family
        )
    )
    if serve_fallback_active and not fallback_reason:
        if requested_model_name and resolved_model_name and requested_model_name != resolved_model_name:
            fallback_reason = str(active.get("fallback_reason") or "live_model_resolution_fallback")
        elif requested_model_family and served_model_family and requested_model_family != served_model_family:
            fallback_reason = str(serve_fallback.get("reason") or "requested_live_model_unavailable")

    explain_dict["requested_model_name"] = str(requested_model_name or resolved_model_name)
    explain_dict["resolved_model_name"] = str(resolved_model_name or requested_model_name)
    explain_dict["resolution_source"] = str(resolution_source)
    explain_dict["requested_model_family"] = str(
        requested_model_family or _model_family(requested_model_name or resolved_model_name)
    )
    explain_dict["served_model_family"] = str(served_model_family or explain_dict.get("model_family") or "")
    explain_dict["serve_fallback_active"] = bool(serve_fallback_active)
    explain_dict["fallback_reason"] = str(fallback_reason)
    explain_dict["candidate_names"] = list(candidate_names)
    if serve_fallback:
        serve_fallback.setdefault("requested_model_name", str(requested_model_name or resolved_model_name))
        serve_fallback.setdefault("requested_family", str(explain_dict.get("requested_model_family") or ""))
        serve_fallback.setdefault("served_family", str(explain_dict.get("served_model_family") or ""))
        if fallback_reason:
            serve_fallback.setdefault("reason", str(fallback_reason))
        explain_dict["serve_fallback"] = dict(serve_fallback)
    return explain_dict


def _resolve_active_model_for_family(
    symbol: str,
    horizon_s: int,
    family: str,
    *,
    primary_active_model: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    requested_family = str(family or "").strip()
    if not requested_family:
        return {}
    primary = dict(primary_active_model or {})
    primary_name = str(primary.get("model_name") or "").strip()
    primary_family = str(primary.get("family") or _model_family(primary_name)).strip()
    if primary and primary_name and primary_family == requested_family:
        return primary

    preferred: List[str] = []
    try:
        live_name = str(_live_model_name(symbol, int(horizon_s)) or "").strip()
        if live_name and _model_family(live_name) == requested_family:
            preferred.append(live_name)
    except Exception as e:
        _warn_nonfatal(
            "predictor_family_live_model_lookup_failed",
            "PREDICTOR_FAMILY_LIVE_MODEL_LOOKUP_FAILED",
            e,
            warn_key=f"predictor_family_live_model_lookup_failed:{symbol}:{horizon_s}:{requested_family}",
            symbol=str(symbol),
            horizon_s=int(horizon_s),
            family=str(requested_family),
        )

    resolved_name = str(
        resolve_active_model_name(
            symbol=str(symbol),
            horizon_s=int(horizon_s),
            preferred_names=preferred,
            family=str(requested_family),
        )
        or ""
    ).strip()
    if not resolved_name:
        return {}
    try:
        return _resolve_active_model(symbol, int(horizon_s), forced_model_name=resolved_name)
    except Exception as e:
        _warn_nonfatal(
            "predictor_family_model_resolution_failed",
            "PREDICTOR_FAMILY_MODEL_RESOLUTION_FAILED",
            e,
            warn_key=f"predictor_family_model_resolution_failed:{symbol}:{horizon_s}:{requested_family}:{resolved_name}",
            symbol=str(symbol),
            horizon_s=int(horizon_s),
            family=str(requested_family),
            model_name=str(resolved_name),
        )
        return {}


def _predict_via_temporal_adapter(
    query_vec: np.ndarray,
    sym: str,
    h: int,
    *,
    event: Optional[Dict] = None,
    active_model_name: str,
    active_model_version: str,
    active_family: str,
    feature_ids: List[str],
    knn_z: float,
    wsum: float,
    knn_ex: Dict[str, Any],
    regime_at_trade: str,
) -> Optional[Tuple[float, float, Dict[str, Any]]]:
    temporal_pred = None
    con_temporal = None
    try:
        con_temporal = connect()
        event_ts_ms = _safe_int((event or {}).get("ts_ms"), 0)
        if event_ts_ms <= 0:
            event_ts_ms = int(time.time() * 1000)
        tl = predict_temporal_live(
            con_temporal,
            ts_ms=int(event_ts_ms),
            symbols=[str(sym)],
            horizons=[int(h)],
        )
        if isinstance(tl, dict):
            temporal_pred = tl.get((str(sym), int(h)))
    except Exception:
        temporal_pred = None
    finally:
        try:
            if con_temporal is not None:
                con_temporal.close()
        except Exception as e:
            _warn_nonfatal(
                "predictor_temporal_connection_close_failed",
                "PREDICTOR_TEMPORAL_CONNECTION_CLOSE_FAILED",
                e,
                warn_key="predictor_temporal_connection_close_failed",
                symbol=str(sym),
                horizon_s=int(h),
            )

    if temporal_pred is None:
        return None

    pred_z, conf, explain = temporal_pred
    explain = dict(explain or {})
    explain["model_name"] = str(active_model_name)
    if str(active_family or "").strip():
        explain["requested_model_family"] = str(active_family)
    explain["regime_at_trade"] = str(regime_at_trade)
    explain["feature_ids"] = list(feature_ids or [])
    explain["feature_set_tag"] = _registry_feature_set_tag(feature_ids)
    explain["fallback_knn"] = {
        "knn_z": float(knn_z),
        "weight_sum": float(wsum),
        "knn": knn_ex,
    }
    z2, conf2, prior_ex = _blend_with_priors(sym, int(h), float(pred_z), 100.0 * float(conf))
    explain["prior"] = prior_ex
    return float(z2), float(conf2), explain


def _latest_feature_snapshot_features(
    symbol: str,
    feature_ids: List[str],
    *,
    decision_ts_ms: int | None = None,
) -> Optional[Dict[str, Any]]:
    group = _registry_feature_set_tag(feature_ids)
    if not group:
        return None
    prefetched = _prefetched_feature_snapshot_features(
        str(symbol),
        str(group),
        decision_ts_ms=decision_ts_ms,
    )
    if prefetched is not _PREFETCH_UNSET:
        return dict(prefetched) if isinstance(prefetched, dict) else None
    try:
        from engine.cache.wrappers.feature_snapshots import latest

        snap = latest(str(symbol), str(group))
    except Exception as e:
        _warn_nonfatal(
            "predictor_feature_snapshot_cache_read_failed",
            "PREDICTOR_FEATURE_SNAPSHOT_CACHE_READ_FAILED",
            e,
            warn_key=f"predictor_feature_snapshot_cache_read_failed:{symbol}:{group}",
            symbol=str(symbol),
            feature_set_tag=str(group),
        )
        return None
    return _features_from_cached_snapshot(
        str(symbol),
        str(group),
        snap,
        decision_ts_ms=decision_ts_ms,
    )


def _features_from_cached_snapshot(
    symbol: str,
    group: str,
    snap: Any,
    *,
    decision_ts_ms: int | None = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(snap, dict):
        return None
    decision_ts = int(decision_ts_ms or 0)
    if decision_ts > 0 and int(snap.get("ts_ms") or 0) > int(decision_ts):
        return None
    if decision_ts > 0:
        try:
            from engine.strategy.model_feature_snapshots import summarize_model_feature_snapshots

            check_snap = dict(snap)
            check_snap["ts_ms"] = int(decision_ts)
            validation = summarize_model_feature_snapshots([check_snap])
            if not bool(validation.get("ok", True)):
                return None
        except Exception as e:
            _warn_nonfatal(
                "predictor_feature_snapshot_pit_validation_failed",
                "PREDICTOR_FEATURE_SNAPSHOT_PIT_VALIDATION_FAILED",
                e,
                warn_key=f"predictor_feature_snapshot_pit_validation_failed:{symbol}:{group}",
                symbol=str(symbol),
                feature_set_tag=str(group),
            )
            return None
    features = snap.get("features")
    if isinstance(features, dict):
        return dict(features)
    return dict(snap)


def _prefetched_feature_snapshot_features(
    symbol: str,
    group: str,
    *,
    decision_ts_ms: int | None = None,
) -> object:
    state = getattr(_FEATURE_SNAPSHOT_PREFETCH, "state", None)
    if not isinstance(state, dict):
        return _PREFETCH_UNSET
    expected_ts_ms = int(state.get("decision_ts_ms") or 0)
    if expected_ts_ms <= 0 or expected_ts_ms != int(decision_ts_ms or 0):
        return _PREFETCH_UNSET
    features_by_key = state.get("features_by_key")
    if not isinstance(features_by_key, dict):
        return _PREFETCH_UNSET
    cache_key = (str(symbol or "").upper().strip(), str(group or "").strip())
    if cache_key not in features_by_key:
        return _PREFETCH_UNSET
    features = features_by_key.get(cache_key)
    return dict(features) if isinstance(features, dict) else None


def _clear_feature_snapshot_prefetch() -> None:
    if hasattr(_FEATURE_SNAPSHOT_PREFETCH, "state"):
        delattr(_FEATURE_SNAPSHOT_PREFETCH, "state")


def _install_feature_snapshot_prefetch(state: Dict[str, Any]) -> None:
    _FEATURE_SNAPSHOT_PREFETCH.state = dict(state or {})


def _latest_feature_snapshot_features_many(
    symbols: List[str],
    feature_ids: List[str],
    *,
    decision_ts_ms: int | None = None,
) -> Dict[str, Dict[str, Any]]:
    group = _registry_feature_set_tag(feature_ids)
    if not group:
        return {}
    symbol_keys = list(
        dict.fromkeys(
            str(symbol or "").upper().strip()
            for symbol in list(symbols or [])
            if str(symbol or "").strip()
        )
    )
    if not symbol_keys:
        return {}
    try:
        from engine.cache.wrappers.feature_snapshots import latest_many

        snapshots = latest_many(symbol_keys, str(group))
    except Exception as e:
        _warn_nonfatal(
            "predictor_feature_snapshot_cache_batch_read_failed",
            "PREDICTOR_FEATURE_SNAPSHOT_CACHE_BATCH_READ_FAILED",
            e,
            warn_key=f"predictor_feature_snapshot_cache_batch_read_failed:{group}",
            feature_set_tag=str(group),
            symbol_count=int(len(symbol_keys)),
        )
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for symbol_key in symbol_keys:
        features = _features_from_cached_snapshot(
            symbol_key,
            str(group),
            (snapshots or {}).get(symbol_key),
            decision_ts_ms=decision_ts_ms,
        )
        if isinstance(features, dict) and features:
            out[symbol_key] = dict(features)
    return out


def _prefetch_feature_snapshot_features_for_event(
    symbols: List[str],
    horizons: List[int],
    event: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    if event is None:
        return {}
    decision_ts_ms = int((event or {}).get("ts_ms", 0) or time.time() * 1000)
    symbols_by_group: Dict[str, List[str]] = {}
    for h in list(horizons or []):
        for raw_symbol in list(symbols or []):
            symbol_key = str(raw_symbol or "").upper().strip()
            if not symbol_key:
                continue
            try:
                active = _resolve_active_model(str(raw_symbol), int(h))
                feature_ids = list(active.get("feature_ids") or [])
                model_name = str(active.get("model_name") or "")
                group = str(
                    active.get("feature_set_tag")
                    or _registry_feature_set_tag(
                        feature_ids,
                        model_name=model_name,
                        model_spec=dict(active or {}),
                    )
                ).strip()
            except Exception as e:
                _warn_nonfatal(
                    "predictor_feature_snapshot_prefetch_resolution_failed",
                    "PREDICTOR_FEATURE_SNAPSHOT_PREFETCH_RESOLUTION_FAILED",
                    e,
                    warn_key=f"predictor_feature_snapshot_prefetch_resolution_failed:{symbol_key}:{h}",
                    symbol=str(symbol_key),
                    horizon_s=int(h),
                )
                group = ""
            if not group:
                continue
            symbols_by_group.setdefault(group, []).append(symbol_key)

    if not symbols_by_group:
        return {}

    try:
        from engine.cache.wrappers.feature_snapshots import latest_many
    except Exception as e:
        _warn_nonfatal(
            "predictor_feature_snapshot_prefetch_import_failed",
            "PREDICTOR_FEATURE_SNAPSHOT_PREFETCH_IMPORT_FAILED",
            e,
            warn_key="predictor_feature_snapshot_prefetch_import_failed",
        )
        return {}

    features_by_key: Dict[Tuple[str, str], Optional[Dict[str, Any]]] = {}
    for group, group_symbols in symbols_by_group.items():
        symbol_keys = list(dict.fromkeys(group_symbols))
        try:
            snapshots = latest_many(symbol_keys, str(group))
        except Exception as e:
            _warn_nonfatal(
                "predictor_feature_snapshot_prefetch_failed",
                "PREDICTOR_FEATURE_SNAPSHOT_PREFETCH_FAILED",
                e,
                warn_key=f"predictor_feature_snapshot_prefetch_failed:{group}",
                feature_set_tag=str(group),
                symbol_count=int(len(symbol_keys)),
            )
            continue
        for symbol_key in symbol_keys:
            features = _features_from_cached_snapshot(
                symbol_key,
                str(group),
                (snapshots or {}).get(symbol_key),
                decision_ts_ms=int(decision_ts_ms),
            )
            features_by_key[(symbol_key, str(group))] = (
                dict(features) if isinstance(features, dict) and features else None
            )

    if not features_by_key:
        return {}
    return {"decision_ts_ms": int(decision_ts_ms), "features_by_key": features_by_key}


def _cached_or_build_feature_snapshot(
    *,
    event: Optional[Dict],
    symbol: str,
    feature_ids: List[str],
) -> Dict[str, Any]:
    decision_ts_ms = int((event or {}).get("ts_ms", 0) or time.time() * 1000) if isinstance(event, dict) else int(time.time() * 1000)
    cached = _latest_feature_snapshot_features(str(symbol), list(feature_ids or []), decision_ts_ms=int(decision_ts_ms))
    if isinstance(cached, dict) and cached:
        return cached
    return build_feature_snapshot(
        event=(dict(event or {}) if isinstance(event, dict) else {"ts_ms": int(time.time() * 1000), "title": "", "body": "", "source": ""}),
        symbol=str(symbol),
        feature_ids=list(feature_ids or []),
    )


def _predict_via_gbm_adapter(
    query_vec: np.ndarray,
    sym: str,
    h: int,
    *,
    event: Optional[Dict] = None,
    active_model_name: str,
    active_model_version: str,
    active_family: str,
    feature_ids: List[str],
    knn_z: float,
    wsum: float,
    knn_ex: Dict[str, Any],
    regime_at_trade: str,
) -> Optional[Tuple[float, float, Dict[str, Any]]]:
    del query_vec

    if not _USE_GBM_REGRESSOR:
        return None

    version = str(active_model_version or "").strip()
    if not version:
        try:
            version = str(get_live_model_version(active_model_name) or "").strip()
        except Exception:
            version = ""
    if not version:
        return None

    record = None
    try:
        record = load_gbm_model_record(str(active_model_name), str(version))
    except Exception:
        record = None
    if not record:
        return None

    event_payload = dict(event or {})
    if not event_payload:
        event_payload = {"ts_ms": int(time.time() * 1000), "title": "", "body": "", "source": ""}

    try:
        feature_map = _cached_or_build_feature_snapshot(
            event=event_payload,
            symbol=str(sym),
            feature_ids=list(feature_ids or []),
        )
    except Exception as e:
        _warn_nonfatal(
            "predictor_feature_snapshot_build_failed",
            "PREDICTOR_FEATURE_SNAPSHOT_BUILD_FAILED",
            e,
            warn_key="feature_snapshot_build",
            symbol=str(sym).upper().strip(),
            feature_count=int(len(feature_ids or [])),
        )
        return None

    feature_snapshot = {
        "symbol": str(sym).upper().strip(),
        "ts_ms": int(event_payload.get("ts_ms") or 0),
        "feature_ids": list(feature_ids or []),
        "feature_set_tag": _registry_feature_set_tag(feature_ids),
        "features": dict(feature_map or {}),
    }

    try:
        pred_z, diagnostics = predict_with_gbm_model(bytes(record.get("blob") or b""), feature_snapshot)
    except Exception as e:
        _warn_nonfatal(
            "predictor_gbm_predict_failed",
            "PREDICTOR_GBM_PREDICT_FAILED",
            e,
            warn_key="gbm_predict_live",
            symbol=str(sym).upper().strip(),
            model_name=str(record.get("model_name") or ""),
            model_version=str(record.get("model_version") or record.get("version") or ""),
        )
        return None

    training_metrics = dict(record.get("training_metrics") or {})
    n_support = max(0, int(training_metrics.get("n_train") or 0))
    conf = float(max(0.0, min(1.0, confidence_from_n(int(n_support)))))
    explain = {
        "model": "gbm_regressor",
        "model_name": str(active_model_name),
        "model_version": str(version),
        "model_kind": str(diagnostics.get("model_kind") or "lightgbm"),
        "regime_at_trade": str(regime_at_trade),
        "feature_ids": list(diagnostics.get("feature_ids") or feature_ids or []),
        "feature_set_tag": str(diagnostics.get("feature_set_tag") or _registry_feature_set_tag(feature_ids)),
        "feature_coverage": float(diagnostics.get("feature_coverage") or 0.0),
        "missing_feature_ids": list(diagnostics.get("missing_feature_ids") or []),
        "feature_schema": dict(record.get("feature_schema") or diagnostics.get("schema") or {}),
        "model_ts_ms": int(record.get("created_ts") or 0),
        "model_n": int(n_support),
        "training_metrics": dict(training_metrics),
        "fallback_knn": {
            "knn_z": float(knn_z),
            "weight_sum": float(wsum),
            "knn": knn_ex,
        },
    }
    if shap_explanations_enabled() and shap_live_compute_enabled():
        try:
            explain["prediction_explanation"] = explain_prediction(
                "gbm_regressor",
                bytes(record.get("blob") or b""),
                dict(feature_snapshot),
                top_k=_prediction_explanation_top_k(),
            )
        except Exception as e:
            _warn_nonfatal(
                "predictor_gbm_prediction_explanation_failed",
                "PREDICTOR_GBM_PREDICTION_EXPLANATION_FAILED",
                e,
                warn_key=f"predictor_gbm_prediction_explanation_failed:{sym}:{h}",
                symbol=str(sym),
                horizon_s=int(h),
                model_name=str(active_model_name),
            )
    if active_family != "gbm_regressor":
        explain["requested_model_family"] = str(active_family)
    z2, conf2, prior_ex = _blend_with_priors(sym, int(h), float(pred_z), float(n_support))
    explain["prior"] = prior_ex
    return float(z2), float(max(conf, conf2)), explain


def _artifact_location_for_model(model_name: str) -> Dict[str, str]:
    spec: Dict[str, Any] = {}
    config: Dict[str, Any] = {}
    try:
        spec = get_model_spec(str(model_name), regime="global") or {}
    except Exception:
        spec = {}
    try:
        config = get_model_config(str(model_name)) or {}
    except Exception:
        config = {}
    return {
        "alias": str(spec.get("artifact_alias") or config.get("artifact_alias") or ""),
        "sha256": str(spec.get("artifact_sha256") or config.get("artifact_sha256") or ""),
        "path": str(spec.get("artifact_path") or config.get("artifact_path") or ""),
    }


def _predict_via_tabular_artifact_adapter(
    family: str,
    load_fn: Callable[..., Any],
    sym: str,
    h: int,
    *,
    event: Optional[Dict],
    active_model_name: str,
    active_family: str,
    feature_ids: List[str],
    knn_z: float,
    wsum: float,
    knn_ex: Dict[str, Any],
    regime_at_trade: str,
) -> Optional[Tuple[float, float, Dict[str, Any]]]:
    location = _artifact_location_for_model(str(active_model_name))
    if not any(str(location.get(key) or "").strip() for key in ("alias", "sha256", "path")):
        return None
    event_payload = dict(event or {})
    if not event_payload:
        event_payload = {"ts_ms": int(time.time() * 1000), "title": "", "body": "", "source": ""}
    try:
        feature_map = _cached_or_build_feature_snapshot(
            event=event_payload,
            symbol=str(sym),
            feature_ids=list(feature_ids or []),
        )
        model = load_fn(
            alias=str(location.get("alias") or ""),
            sha256=str(location.get("sha256") or ""),
            path=(str(location.get("path") or "") or None),
        )
        pred_z = float(model.predict({"features": dict(feature_map or {})})[0])
    except Exception as e:
        _warn_nonfatal(
            "predictor_tabular_artifact_predict_failed",
            "PREDICTOR_TABULAR_ARTIFACT_PREDICT_FAILED",
            e,
            warn_key=f"predictor_tabular_artifact_predict_failed:{family}:{active_model_name}:{sym}:{h}",
            symbol=str(sym),
            horizon_s=int(h),
            model_name=str(active_model_name),
            family=str(family),
        )
        return None

    metrics = dict(getattr(model, "training_metrics", {}) or {})
    n_support = int(metrics.get("n_train") or 0)
    conf = float(max(0.0, min(1.0, confidence_from_n(max(0, n_support)))))
    explain = {
        "model": str(family),
        "model_name": str(active_model_name),
        "model_family": str(family),
        "model_kind": str(getattr(model, "model_kind", family) or family),
        "regime_at_trade": str(regime_at_trade),
        "feature_ids": list(getattr(model, "feature_ids", None) or feature_ids or []),
        "feature_set_tag": _registry_feature_set_tag(list(getattr(model, "feature_ids", None) or feature_ids or [])),
        "feature_schema": dict(getattr(model, "feature_schema", {}) or {}),
        "feature_snapshot": dict(feature_map or {}),
        "model_n": int(n_support),
        "training_metrics": dict(metrics),
        "artifact_alias": str(location.get("alias") or ""),
        "fallback_knn": {
            "knn_z": float(knn_z),
            "weight_sum": float(wsum),
            "knn": knn_ex,
        },
    }
    explain = _attach_ood_diagnostics(
        explain,
        model,
        dict(feature_map or {}),
        warn_key=f"predictor_ood_score_failed:{family}:{active_model_name}:{sym}:{h}",
    )
    if active_family != family:
        explain["requested_model_family"] = str(active_family)
    z2, conf2, prior_ex = _blend_with_priors(sym, int(h), float(pred_z), float(max(1, n_support)))
    explain["prior"] = prior_ex
    return float(z2), float(max(conf, conf2)), explain


def _predict_via_lgbm_adapter(
    query_vec: np.ndarray,
    sym: str,
    h: int,
    *,
    event: Optional[Dict] = None,
    active_model_name: str,
    active_model_version: str,
    active_family: str,
    feature_ids: List[str],
    knn_z: float,
    wsum: float,
    knn_ex: Dict[str, Any],
    regime_at_trade: str,
) -> Optional[Tuple[float, float, Dict[str, Any]]]:
    del query_vec, active_model_version
    return _predict_via_tabular_artifact_adapter(
        "lgbm_regressor",
        load_lgbm_model_from_artifact,
        sym,
        int(h),
        event=event,
        active_model_name=active_model_name,
        active_family=active_family,
        feature_ids=feature_ids,
        knn_z=knn_z,
        wsum=wsum,
        knn_ex=knn_ex,
        regime_at_trade=regime_at_trade,
    )


def _predict_via_lgbm_ranker_adapter(
    query_vec: np.ndarray,
    sym: str,
    h: int,
    *,
    event: Optional[Dict] = None,
    active_model_name: str,
    active_model_version: str,
    active_family: str,
    feature_ids: List[str],
    knn_z: float,
    wsum: float,
    knn_ex: Dict[str, Any],
    regime_at_trade: str,
) -> Optional[Tuple[float, float, Dict[str, Any]]]:
    del query_vec, sym, h, event, active_model_name, active_model_version
    del active_family, feature_ids, knn_z, wsum, knn_ex, regime_at_trade
    return None


def _predict_via_xgb_adapter(
    query_vec: np.ndarray,
    sym: str,
    h: int,
    *,
    event: Optional[Dict] = None,
    active_model_name: str,
    active_model_version: str,
    active_family: str,
    feature_ids: List[str],
    knn_z: float,
    wsum: float,
    knn_ex: Dict[str, Any],
    regime_at_trade: str,
) -> Optional[Tuple[float, float, Dict[str, Any]]]:
    del query_vec, active_model_version
    return _predict_via_tabular_artifact_adapter(
        "xgb_regressor",
        load_xgb_model_from_artifact,
        sym,
        int(h),
        event=event,
        active_model_name=active_model_name,
        active_family=active_family,
        feature_ids=feature_ids,
        knn_z=knn_z,
        wsum=wsum,
        knn_ex=knn_ex,
        regime_at_trade=regime_at_trade,
    )


def _predict_via_patchtst_adapter(
    query_vec: np.ndarray,
    sym: str,
    h: int,
    *,
    event: Optional[Dict] = None,
    active_model_name: str,
    active_model_version: str,
    active_family: str,
    feature_ids: List[str],
    knn_z: float,
    wsum: float,
    knn_ex: Dict[str, Any],
    regime_at_trade: str,
) -> Optional[Tuple[float, float, Dict[str, Any]]]:
    del query_vec, active_model_version
    location = _artifact_location_for_model(str(active_model_name))
    if not any(str(location.get(key) or "").strip() for key in ("alias", "sha256", "path")):
        return None
    event_payload = dict(event or {})
    if not event_payload:
        event_payload = {"ts_ms": int(time.time() * 1000), "title": "", "body": "", "source": ""}
    try:
        model = load_patchtst_model_from_artifact(
            alias=str(location.get("alias") or ""),
            sha256=str(location.get("sha256") or ""),
            path=(str(location.get("path") or "") or None),
        )
        model_feature_ids = list(getattr(model, "feature_ids", None) or feature_ids or [])
        feature_map = _cached_or_build_feature_snapshot(
            event=event_payload,
            symbol=str(sym),
            feature_ids=list(model_feature_ids),
        )
        vector = np.asarray([float(dict(feature_map or {}).get(fid, 0.0) or 0.0) for fid in model_feature_ids], dtype=np.float32)
        seq_len = int(getattr(model, "seq_len", 1) or 1)
        X = np.repeat(vector.reshape(1, 1, -1), seq_len, axis=1)
        horizon_pred = np.asarray(model.predict(X), dtype=np.float32).reshape(-1)
        pred_z = float(horizon_pred[0]) if horizon_pred.size else 0.0
    except Exception as e:
        _warn_nonfatal(
            "predictor_patchtst_predict_failed",
            "PREDICTOR_PATCHTST_PREDICT_FAILED",
            e,
            warn_key=f"predictor_patchtst_predict_failed:{active_model_name}:{sym}:{h}",
            symbol=str(sym),
            horizon_s=int(h),
            model_name=str(active_model_name),
        )
        return None

    metrics = dict(getattr(model, "training_metrics", {}) or {})
    n_support = int(metrics.get("n_train") or 0)
    conf = float(max(0.0, min(1.0, confidence_from_n(max(0, n_support)))))
    explain = {
        "model": "patchtst",
        "model_name": str(active_model_name),
        "model_family": "patchtst",
        "model_kind": "patchtst",
        "regime_at_trade": str(regime_at_trade),
        "feature_ids": list(getattr(model, "feature_ids", None) or feature_ids or []),
        "feature_set_tag": _registry_feature_set_tag(list(getattr(model, "feature_ids", None) or feature_ids or [])),
        "feature_schema": dict(getattr(model, "feature_schema", {}) or {}),
        "feature_snapshot": dict(feature_map or {}),
        "seq_len": int(getattr(model, "seq_len", 0) or 0),
        "n_horizons": int(getattr(model, "n_horizons", 0) or 0),
        "model_n": int(n_support),
        "training_metrics": dict(metrics),
        "artifact_alias": str(location.get("alias") or ""),
        "fallback_knn": {
            "knn_z": float(knn_z),
            "weight_sum": float(wsum),
            "knn": knn_ex,
        },
    }
    explain = _attach_ood_diagnostics(
        explain,
        model,
        dict(feature_map or {}),
        warn_key=f"predictor_ood_score_failed:patchtst:{active_model_name}:{sym}:{h}",
    )
    if active_family != "patchtst":
        explain["requested_model_family"] = str(active_family)
    z2, conf2, prior_ex = _blend_with_priors(sym, int(h), float(pred_z), float(max(1, n_support)))
    explain["prior"] = prior_ex
    return float(z2), float(max(conf, conf2)), explain


def _predict_via_itransformer_adapter(
    query_vec: np.ndarray,
    sym: str,
    h: int,
    *,
    event: Optional[Dict] = None,
    active_model_name: str,
    active_model_version: str,
    active_family: str,
    feature_ids: List[str],
    knn_z: float,
    wsum: float,
    knn_ex: Dict[str, Any],
    regime_at_trade: str,
) -> Optional[Tuple[float, float, Dict[str, Any]]]:
    del query_vec, active_model_version
    try:
        spec = get_model_spec(str(active_model_name), regime="global") or {}
    except Exception:
        spec = {}
    if str(spec.get("source_stage") or "").strip() != "champion":
        return None
    location = _artifact_location_for_model(str(active_model_name))
    if not any(str(location.get(key) or "").strip() for key in ("alias", "sha256", "path")):
        return None
    event_payload = dict(event or {})
    if not event_payload:
        event_payload = {"ts_ms": int(time.time() * 1000), "title": "", "body": "", "source": ""}
    try:
        model = load_itransformer_model_from_artifact(
            alias=str(location.get("alias") or ""),
            sha256=str(location.get("sha256") or ""),
            path=(str(location.get("path") or "") or None),
        )
        model_feature_ids = list(getattr(model, "feature_ids", None) or feature_ids or [])
        feature_map = _cached_or_build_feature_snapshot(
            event=event_payload,
            symbol=str(sym),
            feature_ids=list(model_feature_ids),
        )
        vector = np.asarray([float(dict(feature_map or {}).get(fid, 0.0) or 0.0) for fid in model_feature_ids], dtype=np.float32)
        seq_len = int(getattr(model, "seq_len", 1) or 1)
        X = np.repeat(vector.reshape(1, 1, -1), seq_len, axis=1)
        prediction_payload = model.predict_with_uncertainty(X)
        pred_z = float(prediction_payload.get("prediction") or 0.0)
    except Exception as e:
        _warn_nonfatal(
            "predictor_itransformer_predict_failed",
            "PREDICTOR_ITRANSFORMER_PREDICT_FAILED",
            e,
            warn_key=f"predictor_itransformer_predict_failed:{active_model_name}:{sym}:{h}",
            symbol=str(sym),
            horizon_s=int(h),
            model_name=str(active_model_name),
        )
        return None

    metrics = dict(getattr(model, "training_metrics", {}) or {})
    n_support = int(metrics.get("n_train") or 0)
    conf = float(max(0.0, min(1.0, confidence_from_n(max(0, n_support)))))
    explain = {
        "model": "itransformer",
        "model_name": str(active_model_name),
        "model_family": "itransformer",
        "model_kind": "itransformer",
        "regime_at_trade": str(regime_at_trade),
        "feature_ids": list(getattr(model, "feature_ids", None) or feature_ids or []),
        "feature_set_tag": _registry_feature_set_tag(list(getattr(model, "feature_ids", None) or feature_ids or [])),
        "feature_schema": dict(getattr(model, "feature_schema", {}) or {}),
        "feature_snapshot": dict(feature_map or {}),
        "seq_len": int(getattr(model, "seq_len", 0) or 0),
        "n_horizons": int(getattr(model, "n_horizons", 0) or 0),
        "model_n": int(n_support),
        "training_metrics": dict(metrics),
        "artifact_alias": str(location.get("alias") or ""),
        "epistemic_uncertainty": float(prediction_payload.get("epistemic_uncertainty") or 0.0),
        "uncertainty_ts_ms": int(prediction_payload.get("uncertainty_ts_ms") or int(time.time() * 1000)),
        "uncertainty_detail": dict(prediction_payload.get("uncertainty_detail") or {"method": "deterministic"}),
        "fallback_knn": {
            "knn_z": float(knn_z),
            "weight_sum": float(wsum),
            "knn": knn_ex,
        },
    }
    explain = _attach_ood_diagnostics(
        explain,
        model,
        dict(feature_map or {}),
        warn_key=f"predictor_ood_score_failed:itransformer:{active_model_name}:{sym}:{h}",
    )
    if active_family != "itransformer":
        explain["requested_model_family"] = str(active_family)
    z2, conf2, prior_ex = _blend_with_priors(sym, int(h), float(pred_z), float(max(1, n_support)))
    explain["prior"] = prior_ex
    return float(z2), float(max(conf, conf2)), explain


def _predict_via_embed_adapter(
    query_vec: np.ndarray,
    sym: str,
    h: int,
    *,
    event: Optional[Dict] = None,
    active_model_name: str,
    active_model_version: str,
    active_family: str,
    feature_ids: List[str],
    knn_z: float,
    wsum: float,
    knn_ex: Dict[str, Any],
    regime_at_trade: str,
) -> Optional[Tuple[float, float, Dict[str, Any]]]:
    qv_embed = query_vec
    if event is not None:
        feats = build_feature_vector(event=event, symbol=sym, feature_ids=feature_ids)
        qv_embed = np.concatenate([query_vec, np.asarray(feats, dtype=np.float32)])

    embed_pred = None
    if _USE_EMBED_REGRESSOR:
        try:
            embed_pred = predict_with_embed_model(
                sym,
                int(h),
                qv_embed,
                feature_ids=feature_ids,
                model_name=active_model_name,
            )
        except Exception:
            embed_pred = None

    if embed_pred is not None:
        pred_z, n_support, model_ts, model_key_type, model_key, model_kind = embed_pred
        try:
            n_support = max(0, int(n_support))
            conf_raw = float(1.0 - math.exp(-n_support / _EMBED_REGRESSOR_CONF_K))
        except Exception:
            conf_raw = 0.0

        conf_raw = float(max(0.0, min(1.0, conf_raw)))
        conf = float(conf_raw)

        if _EMBED_CONF_CALIB:
            try:
                con2 = connect()
                try:
                    curve = _load_embed_conf_calib(con2, int(h), str(model_kind))
                finally:
                    con2.close()
                if curve is not None:
                    conf = float(_apply_calib(conf_raw, curve))
            except Exception as e:
                _warn_nonfatal(
                    "predictor_embed_confidence_calibration_failed",
                    "PREDICTOR_EMBED_CONFIDENCE_CALIBRATION_FAILED",
                    e,
                    warn_key=f"predictor_embed_confidence_calibration_failed:{sym}:{h}:{model_kind}",
                    symbol=str(sym),
                    horizon_s=int(h),
                    model_kind=str(model_kind),
                )

        z = float(pred_z)
        conf = float(max(0.0, min(1.0, conf)))
        explain = {
            "model": "embed_regressor",
            "model_name": str(active_model_name),
            "regime_at_trade": str(regime_at_trade),
            "feature_ids": list(feature_ids or []),
            "feature_set_tag": _registry_feature_set_tag(feature_ids, model_name=str(active_model_name)),
            "model_kind": str(model_kind),
            "conf_raw": float(conf_raw),
            "conf_calibrated": float(conf),
            "model_key_type": str(model_key_type),
            "model_key": str(model_key),
            "model_ts_ms": int(model_ts),
            "model_n": int(n_support),
            "fallback_knn": {
                "knn_z": float(knn_z),
                "weight_sum": float(wsum),
                "knn": knn_ex,
            },
        }
        if active_family != "embed_regressor":
            explain["serve_fallback"] = {
                "requested_model_name": str(active_model_name),
                "requested_family": str(active_family),
                "served_family": "embed_regressor",
                "reason": "requested_live_model_unavailable",
            }
        z2, conf2, prior_ex = _blend_with_priors(sym, int(h), z, float(n_support))
        explain["prior"] = prior_ex
        return float(z2), float(conf2), explain

    return None


def _predict_via_regime_stats_adapter(
    query_vec: np.ndarray,
    sym: str,
    h: int,
    *,
    event: Optional[Dict] = None,
    active_model_name: str,
    active_model_version: str,
    active_family: str,
    feature_ids: List[str],
    knn_z: float,
    wsum: float,
    knn_ex: Dict[str, Any],
    regime_at_trade: str,
) -> Optional[Tuple[float, float, Dict[str, Any]]]:
    try:
        prior_z, prior_n, prior_regime = get_regime_prior(sym, int(h))
    except Exception:
        prior_z, prior_n, prior_regime = 0.0, 0, "MID"

    if int(prior_n or 0) <= 0:
        return None

    conf = float(max(0.0, min(1.0, confidence_from_n(int(prior_n)))))
    explain = {
        "model": "regime_stats",
        "model_name": str(active_model_name),
        "requested_model_family": str(active_family),
        "feature_ids": list(feature_ids or []),
        "feature_set_tag": _registry_feature_set_tag(feature_ids, model_name=str(active_model_name)),
        "model_kind": "shadow_regime_stats",
        "model_ts_ms": 0,
        "regime_at_trade": str(regime_at_trade),
        "regime_source": str(prior_regime or "MID"),
        "model_n": int(prior_n),
        "prior_only": True,
        "fallback_knn": {
            "knn_z": float(knn_z),
            "weight_sum": float(wsum),
            "knn": knn_ex,
        },
    }
    return float(prior_z), float(conf), explain


_MODEL_ADAPTERS: Dict[str, Callable[..., Optional[Tuple[float, float, Dict[str, Any]]]]] = {
    "temporal_predictor": _predict_via_temporal_adapter,
    "gbm_regressor": _predict_via_gbm_adapter,
    "lgbm_regressor": _predict_via_lgbm_adapter,
    "lgbm_ranker": _predict_via_lgbm_ranker_adapter,
    "xgb_regressor": _predict_via_xgb_adapter,
    "patchtst": _predict_via_patchtst_adapter,
    "itransformer": _predict_via_itransformer_adapter,
    "embed_regressor": _predict_via_embed_adapter,
    "regime_stats": _predict_via_regime_stats_adapter,
}
_REALTIME_INFERENCE_ENABLED = os.environ.get("REALTIME_INFERENCE_ENABLED", "1") == "1"
_REALTIME_INFERENCE_TIMEOUT_S = max(0.05, float(os.environ.get("REALTIME_INFERENCE_TIMEOUT_S", "1.0")))
_REALTIME_INFERENCE_LEGACY_FALLBACK = str(os.environ.get("REALTIME_INFERENCE_LEGACY_FALLBACK", "") or "").strip().lower()


def available_model_families() -> List[str]:
    return sorted(_MODEL_ADAPTERS.keys())


def _adapter_predict(
    family: str,
    query_vec: np.ndarray,
    sym: str,
    h: int,
    *,
    event: Optional[Dict],
    active_model_name: str,
    active_model_version: str,
    active_family: str,
    feature_ids: List[str],
    knn_z: float,
    wsum: float,
    knn_ex: Dict[str, Any],
    regime_at_trade: str,
) -> Optional[Tuple[float, float, Dict[str, Any]]]:
    fn = _MODEL_ADAPTERS.get(str(family or "").strip())
    if not callable(fn):
        return None
    return fn(
        query_vec,
        sym,
        h,
        event=event,
        active_model_name=active_model_name,
        active_model_version=active_model_version,
        active_family=active_family,
        feature_ids=feature_ids,
        knn_z=knn_z,
        wsum=wsum,
        knn_ex=knn_ex,
        regime_at_trade=regime_at_trade,
    )


def _load_embed_conf_calib(con, horizon_s: int, model_kind: str):
    key = (int(horizon_s), str(model_kind))
    now_s = time.time()
    with _CALIB_LOCK:
        if (now_s - float(_calib_cache["ts_s"])) < float(_CALIB_CACHE_TTL_S) and key in _calib_cache["curves"]:
            return _calib_cache["curves"][key]

    row = con.execute(
        """
        SELECT x_json, y_json
        FROM embed_conf_calib
        WHERE horizon_s=? AND model_kind=?
        """,
        (int(horizon_s), str(model_kind)),
    ).fetchone()
    if not row:
        return None

    try:
        xs = [float(x) for x in json.loads(row[0] or "[]")]
        ys = [float(y) for y in json.loads(row[1] or "[]")]
        if len(xs) < 2 or len(xs) != len(ys):
            return None
    except Exception as e:
        _warn_nonfatal(
            "predictor_calibration_curve_parse_failed",
            "PREDICTOR_CALIBRATION_CURVE_PARSE_FAILED",
            e,
            warn_key=f"predictor_calibration_curve_parse_failed:{horizon_s}:{model_kind}",
            horizon_s=int(horizon_s),
            model_kind=str(model_kind or ""),
        )
        return None

    curve = (xs, ys)
    with _CALIB_LOCK:
        if (now_s - float(_calib_cache["ts_s"])) < float(_CALIB_CACHE_TTL_S) and key in _calib_cache["curves"]:
            return _calib_cache["curves"][key]
        _calib_cache["ts_s"] = float(now_s)
        _calib_cache["curves"][key] = curve
    return curve


def _safe_int(v, default: int = 0) -> int:
    try:
        return int(v)
    except Exception as e:
        _warn_nonfatal(
            "predictor_safe_int_failed",
            "PREDICTOR_SAFE_INT_FAILED",
            e,
            warn_key="predictor_safe_int_failed",
            value_type=type(v).__name__,
        )
        return int(default)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        out = float(v)
    except Exception as e:
        _warn_nonfatal(
            "predictor_safe_float_failed",
            "PREDICTOR_SAFE_FLOAT_FAILED",
            e,
            warn_key="safe_float",
            value=repr(v)[:120],
        )
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _apply_calib(conf_raw: float, curve):
    try:
        xs, ys = curve
        x = float(conf_raw)
        # Piecewise-linear calibration keeps confidence monotonic and cheap.
        if x <= xs[0]:
            return float(max(0.0, min(1.0, ys[0])))
        if x >= xs[-1]:
            return float(max(0.0, min(1.0, ys[-1])))
        # linear interp
        return float(max(0.0, min(1.0, float(np.interp(x, np.asarray(xs), np.asarray(ys))))))
    except Exception as e:
        _warn_nonfatal(
            "predictor_apply_calibration_failed",
            "PREDICTOR_APPLY_CALIBRATION_FAILED",
            e,
            warn_key="predictor_apply_calibration_failed",
        )
        return float(conf_raw)


HALF_LIFE_DAYS = 7.0
MS_PER_DAY = 24 * 3600 * 1000

MIN_BETA_N = 10

# ------            -- ------------------------------------------------------
# Prediction core
# ------            -- ------------------------------------------------------

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [predictor] %(message)s",
)

# Confidence-collapse guardrails
CONF_COLLAPSE_MIN = float(os.environ.get("CONF_COLLAPSE_MIN", "0.15"))
CONF_COLLAPSE_FRAC = float(os.environ.get("CONF_COLLAPSE_FRAC", "0.7"))

# ------            -- ------------------------------------------------------
# Option 4: performance/scalability cache (behavior-preserving)
# ------            -- ------------------------------------------------------

_CACHE_TTL_S = 10.0  # short TTL; invalidation also uses label stamp

_cached = {
    "ts_s": 0.0,                # last refresh time (monotonic seconds)
    "stamp": None,              # (label_count, max_created_at_ms)
    "events": None,             # list[(event_id, ts_ms, vec)]
    "labels": None,             # dict[(event_id,symbol,horizon_s)] -> impact_z
    "label_created_at": None,   # dict[(event_id,symbol,horizon_s)] -> created_at_ms
    "vecs": None,               # np.ndarray shape [N, D]
    "event_ids": None,          # list[int]
    "event_ts": None,           # list[int]
}

# ------            -- ------------------------------------------------------
# Option 5.1: learned relevance → confidence scaling (OPT-IN)
# ------            -- ------------------------------------------------------

_USE_LEARNED_REL = os.environ.get("PREDICTOR_USE_LEARNED_RELEVANCE", "0") == "1"
_LEARNED_REL_ABS_Z = float(os.environ.get("PREDICTOR_LEARNED_REL_ABS_Z", "0.5"))
# multiplier = floor + (ceil-floor)*learned_relevance
_LEARNED_REL_CONF_FLOOR = float(os.environ.get("PREDICTOR_LEARNED_REL_CONF_FLOOR", "0.5"))
_LEARNED_REL_CONF_CEIL = float(os.environ.get("PREDICTOR_LEARNED_REL_CONF_CEIL", "1.0"))


def _labels_stamp(con) -> Tuple[int, int]:
    """
    Stamp for invalidation: (count, max_created_at_ms).
    Uses only existing columns.
    """
    try:
        row = con.execute(
            """
            SELECT COUNT(*), MAX(created_at_ms)
            FROM labels
            WHERE impact_z IS NOT NULL
            """
        ).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "predictor_impact_coverage_query_failed",
            "PREDICTOR_IMPACT_COVERAGE_QUERY_FAILED",
            e,
            warn_key="predictor_impact_coverage_query_failed",
        )
        return 0, 0

    if not row:
        return 0, 0

    try:
        n = int(row[0] or 0)
    except Exception:
        n = 0

    try:
        mx = int(row[1] or 0)
    except Exception:
        mx = 0

    return n, mx


def _load_labeled_event_vectors_internal(*, as_of_ts_ms: Optional[int] = None):
    """
    Returns:
      events: list of (event_id, ts_ms, vector)
      labels: dict[(event_id, symbol, horizon_s)] -> impact_z
    """
    con = connect()
    try:
        use_temporal = os.environ.get("USE_TEMPORAL_EMBED", "0") == "1"
        emb_table = "event_embeddings_seq" if use_temporal else "event_embeddings"

        params: list[int] = []
        event_where = ""
        label_where = ""
        if as_of_ts_ms is not None:
            as_of = int(as_of_ts_ms)
            event_where = "WHERE e.ts_ms < ?"
            label_where = "AND COALESCE(created_at_ms, 0) <= ?"
            params.append(int(as_of))

        evs = con.execute(
            f"""
            SELECT e.id, e.ts_ms, emb.vec
            FROM events e
            JOIN {emb_table} emb ON emb.event_id = e.id
            {event_where}
            """,
            tuple(params),
        ).fetchall()

        label_params = (int(as_of_ts_ms),) if as_of_ts_ms is not None else ()
        lbls = con.execute(
            f"""
            SELECT event_id, symbol, horizon_s, impact_z, created_at_ms
            FROM labels
            WHERE impact_z IS NOT NULL
            {label_where}
            """,
            label_params,
        ).fetchall()

        events = []
        for eid, ts_ms, blob in evs:
            vec = np.frombuffer(blob, dtype=np.float32)
            events.append((int(eid), int(ts_ms), vec))

        labels = {}
        label_created_at = {}
        for eid, sym, h, z, created_at_ms in lbls:
            labels[(int(eid), str(sym), int(h))] = float(z)
            label_created_at[(int(eid), str(sym), int(h))] = int(created_at_ms or 0)

        return events, labels, label_created_at
    finally:
        con.close()


def load_labeled_event_vectors(*, as_of_ts_ms: Optional[int] = None):
    events, labels, _ = _load_labeled_event_vectors_internal(as_of_ts_ms=as_of_ts_ms)
    return events, labels


def _load_labeled_event_vectors_cached():
    """
    Cached version of load_labeled_event_vectors().
    Builds vec matrix + metadata once.
    """
    now_s = time.monotonic()
    if (
        _cached["events"] is not None
        and _cached["labels"] is not None
        and _cached["label_created_at"] is not None
        and _cached["vecs"] is not None
        and (now_s - float(_cached["ts_s"] or 0.0)) < _CACHE_TTL_S
    ):
        return _cached["events"], _cached["labels"], _cached["vecs"], _cached["event_ids"], _cached["event_ts"]

    con = connect()
    try:
        stamp = _labels_stamp(con)
        if _cached["stamp"] == stamp and _cached["events"] is not None and _cached["vecs"] is not None:
            _cached["ts_s"] = now_s
            return _cached["events"], _cached["labels"], _cached["vecs"], _cached["event_ids"], _cached["event_ts"]
    finally:
        con.close()

    events, labels, label_created_at = _load_labeled_event_vectors_internal()
    if not events:
        _cached.update({
            "ts_s": now_s,
            "stamp": stamp,
            "events": [],
            "labels": labels,
            "label_created_at": label_created_at,
            "vecs": None,
            "event_ids": [],
            "event_ts": [],
        })
        return _cached["events"], _cached["labels"], _cached["vecs"], _cached["event_ids"], _cached["event_ts"]

    try:
        vecs = np.stack([v for _, _, v in events]).astype(np.float32, copy=False)
    except Exception:
        vecs = None

    event_ids = [int(eid) for (eid, _, _) in events]
    event_ts = [int(ts) for (_, ts, _) in events]

    _cached.update({
        "ts_s": now_s,
        "stamp": stamp,
        "events": events,
        "labels": labels,
        "label_created_at": label_created_at,
        "vecs": vecs,
        "event_ids": event_ids,
        "event_ts": event_ts,
    })

    return events, labels, vecs, event_ids, event_ts


def _time_decay_weight(event_ts_ms: int, now_ms: int) -> float:
    age_days = max(0.0, (now_ms - event_ts_ms) / MS_PER_DAY)
    return math.exp(-age_days / HALF_LIFE_DAYS)


def _knn_raw(
    query_vec: np.ndarray,
    symbol: str,
    horizon_s: int,
    top_k: int,
    *,
    as_of_ts_ms: Optional[int] = None,
):
    """
    Returns:
      knn_z, weight_sum, explain_knn
    """
    events, labels_raw, vecs, event_ids, event_ts = _load_labeled_event_vectors_cached()
    labels = dict(labels_raw or {})
    label_created_at = dict(_cached.get("label_created_at") or {})
    now_ms = int(as_of_ts_ms) if as_of_ts_ms is not None else int(time.time() * 1000)
    if not events or vecs is None:
        if not events:
            return 0.0, 0.0, {"fallback_reason": "no_cached_labeled_events"}

        events2, labels2_raw = load_labeled_event_vectors(as_of_ts_ms=as_of_ts_ms)
        labels2 = dict(labels2_raw or {})
        if not events2:
            return 0.0, 0.0, {"fallback_reason": "no_labeled_events_after_reload"}

        vecs2 = np.stack([v for _, _, v in events2])
        sims = cosine_similarity([query_vec], vecs2)[0]

        scored = []
        explain_neighbors = []

        for (eid, ts_ms, _), sim in zip(events2, sims):
            if int(ts_ms) >= int(now_ms):
                continue
            if sim <= 0:
                continue
            key = (eid, symbol, horizon_s)
            if key not in labels2:
                continue

            decay = _time_decay_weight(ts_ms, now_ms)
            w = float(sim) * float(decay)
            if w <= 0:
                continue

            z = labels2[key]
            age_days = (now_ms - ts_ms) / MS_PER_DAY

            scored.append((w, z))
            explain_neighbors.append({
                "event_id": int(eid),
                "sim": float(sim),
                "decay": float(decay),
                "age_days": float(age_days),
                "weight": float(w),
                "impact_z": float(z),
            })

        if not scored:
            return 0.0, 0.0, {"fallback_reason": "no_positive_labeled_neighbors_after_reload"}

        scored.sort(reverse=True, key=lambda x: x[0])
        explain_neighbors.sort(reverse=True, key=lambda x: x["weight"])

        top = scored[:top_k]
        neighbors = explain_neighbors[:top_k]

        weights = np.array([w for w, _ in top], dtype=float)
        impacts = np.array([z for _, z in top], dtype=float)

        wsum = float(weights.sum())
        if wsum <= 0:
            return 0.0, 0.0, {"fallback_reason": "zero_weight_sum_after_reload"}

        knn_z = float(np.dot(weights, impacts) / wsum)

        explain = {
            "top_k": int(top_k),
            "used": int(len(top)),
            "weight_sum": float(wsum),
            "neighbors": neighbors,
        }

        return knn_z, wsum, explain

    sims = cosine_similarity([query_vec], vecs)[0]

    scored = []
    explain_neighbors = []

    for eid, ts_ms, sim in zip(event_ids, event_ts, sims):
        if int(ts_ms) >= int(now_ms):
            continue
        if sim <= 0:
            continue
        key = (int(eid), str(symbol), int(horizon_s))
        if key not in labels:
            continue
        created_at_ms = label_created_at.get(key)
        if created_at_ms is not None and int(created_at_ms) > int(now_ms):
            continue

        decay = _time_decay_weight(int(ts_ms), now_ms)
        w = float(sim) * float(decay)
        if w <= 0:
            continue

        z = labels[key]
        age_days = (now_ms - int(ts_ms)) / MS_PER_DAY

        scored.append((w, z))
        explain_neighbors.append({
            "event_id": int(eid),
            "sim": float(sim),
            "decay": float(decay),
            "age_days": float(age_days),
            "weight": float(w),
            "impact_z": float(z),
        })

    if not scored:
        return 0.0, 0.0, {"fallback_reason": "no_positive_labeled_neighbors"}

    scored.sort(reverse=True, key=lambda x: x[0])
    explain_neighbors.sort(reverse=True, key=lambda x: x["weight"])

    top = scored[:top_k]
    neighbors = explain_neighbors[:top_k]

    weights = np.array([w for w, _ in top], dtype=float)
    impacts = np.array([z for _, z in top], dtype=float)

    wsum = float(weights.sum())
    if wsum <= 0:
        return 0.0, 0.0, {"fallback_reason": "zero_weight_sum"}

    knn_z = float(np.dot(weights, impacts) / wsum)

    explain = {
        "top_k": int(top_k),
        "used": int(len(top)),
        "weight_sum": float(wsum),
        "neighbors": neighbors,
    }

    return knn_z, wsum, explain


def _blend_with_priors(symbol: str, horizon_s: int, knn_z: float, wsum: float):
    reg_mean, reg_n, reg = get_regime_prior(symbol, horizon_s)
    glob_mean, glob_n = get_global_prior(symbol, horizon_s)

    prior_z = 0.0
    prior_n = 0
    if reg_n > 0:
        prior_z = float(reg_mean)
        prior_n = int(reg_n)
    elif glob_n > 0:
        prior_z = float(glob_mean)
        prior_n = int(glob_n)

    knn_conf = confidence_from_weight(wsum)

    if prior_n <= 0:
        return knn_z, knn_conf, {
            "prior": "none",
            "regime": reg,
            "prior_n": 0,
        }

    prior_conf = confidence_from_n(prior_n)
    prior_strength = 3.0
    alpha = float(wsum / (wsum + prior_strength))

    expected = float(alpha * knn_z + (1.0 - alpha) * prior_z)
    fused = float(
        1.0 - (1.0 - knn_conf) * (1.0 - (0.6 * prior_conf) * (1.0 - alpha))
    )

    explain = {
        "prior": "regime" if reg_n > 0 else "global",
        "regime": reg,
        "prior_n": int(prior_n),
        "alpha": float(alpha),
    }

    return expected, max(0.0, min(1.0, fused)), explain


def _confidence_collapse(confs: list[float]) -> bool:
    if not confs:
        return True
    low = [c for c in confs if c < CONF_COLLAPSE_MIN]
    return (len(low) / max(1, len(confs))) >= CONF_COLLAPSE_FRAC


def _prediction_asset_class(symbol: str) -> str:
    sym = str(symbol or "").upper().strip()
    try:
        asset_class = str(asset_class_for_symbol(sym) or "UNKNOWN").upper().strip()
    except Exception as e:
        _warn_nonfatal(
            "predictor_asset_class_lookup_failed",
            "PREDICTOR_ASSET_CLASS_LOOKUP_FAILED",
            e,
            warn_key=f"predictor_asset_class_lookup_failed:{symbol}",
            symbol=str(symbol),
        )
        asset_class = "UNKNOWN"
    if asset_class == "UNKNOWN":
        base = _crypto_base_symbol(sym)
        if base and base != sym:
            try:
                base_asset_class = str(asset_class_for_symbol(base) or "UNKNOWN").upper().strip()
            except Exception:
                base_asset_class = "UNKNOWN"
            if base_asset_class in _CRYPTO_ASSET_CLASSES:
                return base_asset_class
    return asset_class


_CRYPTO_ASSET_CLASSES = {"CRYPTO", "CRYPTOCURRENCY", "DIGITAL_ASSET", "DIGITAL_ASSETS"}
_RANKER_EQUITY_ASSET_CLASSES = {"EQUITY", "EQUITIES", "US_EQUITY", "STOCK", "STOCKS"}
_RANKER_DEFAULT_EXCLUDED_ASSET_CLASSES = {
    "CRYPTO",
    "CRYPTOCURRENCY",
    "DIGITAL_ASSET",
    "DIGITAL_ASSETS",
    "COMMODITY",
    "FX",
    "RATES",
    "OPTION",
    "OPTIONS",
    "FUTURES",
}
_CRYPTO_QUOTE_SUFFIXES = ("USDT", "USD", "USDC", "EUR", "GBP")


def _crypto_base_symbol(symbol: str) -> str:
    sym = str(symbol or "").upper().strip().replace("/", "").replace("-", "")
    if not sym:
        return ""
    for suffix in _CRYPTO_QUOTE_SUFFIXES:
        if sym.endswith(suffix) and len(sym) > len(suffix):
            return sym[: -len(suffix)]
    return sym


def _crypto_regime_anchor_symbol() -> str:
    anchor = str(os.environ.get("CRYPTO_REGIME_ANCHOR_SYMBOL", "BTCUSD") or "BTCUSD").upper().strip()
    return anchor or "BTCUSD"


def _regime_anchor_symbol(symbol: str) -> str:
    sym = str(symbol or "").upper().strip()
    asset_class = _prediction_asset_class(sym)
    if asset_class in _CRYPTO_ASSET_CLASSES:
        return _crypto_regime_anchor_symbol()
    if asset_class != "FX":
        return "SPY"
    try:
        from engine.data.fx_instrument import parse_fx_symbol

        meta = parse_fx_symbol(sym)
        if meta is not None and str(getattr(meta, "symbol", "") or "").strip():
            return str(meta.symbol).upper().strip()
    except Exception as e:
        _warn_nonfatal(
            "predictor_fx_regime_anchor_normalization_failed",
            "PREDICTOR_FX_REGIME_ANCHOR_NORMALIZATION_FAILED",
            e,
            warn_key=f"predictor_fx_regime_anchor_normalization_failed:{sym}",
            symbol=str(sym),
        )
    return sym or "DXY"


def _prediction_regime_context(symbol: str, event: Optional[Mapping[str, Any]]) -> Tuple[str, Dict[str, Any]]:
    sym = str(symbol or "").upper().strip()
    asset_class = _prediction_asset_class(sym)
    anchor = _regime_anchor_symbol(sym)
    if asset_class == "FX":
        default_regime = "FX_MID"
    elif asset_class in _CRYPTO_ASSET_CLASSES:
        default_regime = "CRYPTO_MID"
    else:
        default_regime = "MID"
    try:
        regime_at_trade = str(get_current_regime(anchor) or default_regime).upper()
    except Exception as e:
        _warn_nonfatal(
            "predictor_current_regime_lookup_failed",
            "PREDICTOR_CURRENT_REGIME_LOOKUP_FAILED",
            e,
            warn_key=f"predictor_current_regime_lookup_failed:{anchor}",
            symbol=str(sym),
            anchor_symbol=str(anchor),
        )
        regime_at_trade = default_regime

    if asset_class in _CRYPTO_ASSET_CLASSES:
        return regime_at_trade, {"anchor_symbol": str(anchor), "asset_class": "CRYPTO"}
    if asset_class != "FX":
        return regime_at_trade, {}

    context: Dict[str, Any] = {"anchor_symbol": str(anchor), "asset_class": "FX"}
    try:
        from engine.strategy.regime_stack import compute_regime_vector

        ts_ms = _safe_int((event or {}).get("ts_ms"), int(time.time() * 1000))
        vector = compute_regime_vector(symbol=str(anchor), ts_ms=int(ts_ms), con=None, include_hmm=False)
        macro = dict((vector or {}).get("macro") or {})
        fx_macro = {
            key: float(macro.get(key) or 0.0)
            for key in ("fx_usd_strength_z", "fx_usd_strength_dir", "fx_carry_pressure")
            if key in macro
        }
        if fx_macro:
            context["macro"] = fx_macro
    except Exception as e:
        _warn_nonfatal(
            "predictor_fx_regime_context_failed",
            "PREDICTOR_FX_REGIME_CONTEXT_FAILED",
            e,
            warn_key=f"predictor_fx_regime_context_failed:{sym}",
            symbol=str(sym),
            anchor_symbol=str(anchor),
        )
    return regime_at_trade, context


def _attach_prediction_regime_context(explain: Dict[str, Any], context: Mapping[str, Any]) -> Dict[str, Any]:
    context_dict = dict(context or {})
    if not context_dict:
        return explain
    explain["regime_anchor_symbol"] = str(context_dict.get("anchor_symbol") or "")
    asset_class = str(context_dict.get("asset_class") or "").upper().strip()
    if asset_class in _CRYPTO_ASSET_CLASSES:
        explain["crypto_regime_context"] = dict(context_dict)
    else:
        explain["fx_regime_context"] = dict(context_dict)
    return explain


def _predict_resolved_model(
    query_vec: np.ndarray,
    sym: str,
    h: int,
    *,
    top_k: int,
    active_model: Dict[str, Any],
    event: Optional[Dict] = None,
) -> Tuple[float, float, Dict]:
    # Live serving uses the feature contract attached to the selected model so
    # inference cannot silently drift away from the schema the model trained on.
    active_model = dict(active_model or {})
    active_model_name = str(active_model.get("model_name") or MODEL_NAME).strip() or MODEL_NAME
    active_model_id = str(active_model.get("model_id") or active_model_name).strip() or active_model_name
    active_family = str(active_model.get("family") or "embed_regressor").strip() or "embed_regressor"
    feature_ids = resolve_feature_ids(
        active_model.get("feature_ids"),
        model_name=active_model_name,
        model_spec=active_model,
    )
    feature_schema = dict(active_model.get("feature_schema") or {})

    qv_knn = query_vec
    as_of_ts_ms = _safe_int((event or {}).get("ts_ms"), int(time.time() * 1000))
    knn_z, wsum, knn_ex = _knn_raw(qv_knn, sym, int(h), top_k, as_of_ts_ms=int(as_of_ts_ms))

    regime_at_trade, regime_context = _prediction_regime_context(sym, event)

    served = _adapter_predict(
        active_family,
        query_vec,
        sym,
        int(h),
        event=event,
        active_model_name=active_model_name,
        active_model_version=str(active_model.get("model_version") or ""),
        active_family=active_family,
        feature_ids=feature_ids,
        knn_z=float(knn_z),
        wsum=float(wsum),
        knn_ex=dict(knn_ex or {}),
        regime_at_trade=str(regime_at_trade),
    )
    if served is None and active_family != "embed_regressor":
        served = _adapter_predict(
            "embed_regressor",
            query_vec,
            sym,
            int(h),
            event=event,
            active_model_name=active_model_name,
            active_model_version=str(active_model.get("model_version") or ""),
            active_family=active_family,
            feature_ids=feature_ids,
            knn_z=float(knn_z),
            wsum=float(wsum),
            knn_ex=dict(knn_ex or {}),
            regime_at_trade=str(regime_at_trade),
        )
    if served is not None:
        try:
            z_served, conf_served, explain_served = served
            explain_served = dict(explain_served or {})
            explain_served["model_id"] = str(explain_served.get("model_id") or active_model_id)
            explain_served["model_family"] = str(explain_served.get("model_family") or active_model.get("model_family") or active_family)
            explain_served["model_spec_source_stage"] = str(active_model.get("spec_source_stage") or "")
            if not explain_served.get("model_kind"):
                explain_served["model_kind"] = str(active_model.get("model_kind") or "")
            if not int(explain_served.get("model_ts_ms") or 0):
                explain_served["model_ts_ms"] = int(active_model.get("model_ts_ms") or 0)
            if not str(explain_served.get("model_version") or "").strip():
                explain_served["model_version"] = str(active_model.get("model_version") or "")
            if str(active_model.get("risk_profile") or "").strip():
                explain_served["risk_profile"] = str(active_model.get("risk_profile") or "")
            if list(active_model.get("symbol_universe") or []):
                explain_served["symbol_universe"] = list(active_model.get("symbol_universe") or [])
            if int(active_model.get("horizon_s") or 0) > 0:
                explain_served["configured_horizon_s"] = int(active_model.get("horizon_s") or 0)
            if list(active_model.get("horizons_s") or []):
                explain_served["configured_horizons_s"] = list(active_model.get("horizons_s") or [])
            if int(active_model.get("training_window_days") or 0) > 0:
                explain_served["training_window_days"] = int(active_model.get("training_window_days") or 0)
            if feature_schema:
                explain_served["feature_schema"] = dict(feature_schema)
            explain_served.setdefault("regime_at_trade", str(regime_at_trade))
            explain_served = _attach_prediction_regime_context(explain_served, regime_context)
            explain_served = _apply_model_serving_diagnostics(explain_served, active_model)
            return float(z_served), float(conf_served), explain_served
        except Exception as e:
            _warn_nonfatal(
                "predictor_served_explain_enrichment_failed",
                "PREDICTOR_SERVED_EXPLAIN_ENRICHMENT_FAILED",
                e,
                warn_key=f"predictor_served_explain_enrichment_failed:{sym}:{h}",
                symbol=str(sym),
                horizon_s=int(h),
            )
            z_served, conf_served, explain_served = served
            explain_dict = _apply_model_serving_diagnostics(dict(explain_served or {}), active_model)
            explain_dict.setdefault("regime_at_trade", str(regime_at_trade))
            explain_dict = _attach_prediction_regime_context(explain_dict, regime_context)
            return float(z_served), float(conf_served), explain_dict

    z, conf, prior_ex = _blend_with_priors(sym, int(h), knn_z, wsum)
    explain = {
        "model_name": str(active_model_name),
        "model_id": str(active_model_id),
        "model_family": str(active_model.get("model_family") or active_family),
        "model_kind": str(active_model.get("model_kind") or ""),
        "model_ts_ms": int(active_model.get("model_ts_ms") or 0),
        "model_version": str(active_model.get("model_version") or ""),
        "requested_model_family": str(active_family),
        "model_spec_source_stage": str(active_model.get("spec_source_stage") or ""),
        "feature_ids": list(feature_ids or []),
        "feature_set_tag": _registry_feature_set_tag(feature_ids, model_name=str(active_model_name)),
        "regime_at_trade": str(regime_at_trade),
        "knn": knn_ex,
        "prior": prior_ex,
    }
    if str(active_model.get("risk_profile") or "").strip():
        explain["risk_profile"] = str(active_model.get("risk_profile") or "")
    if list(active_model.get("symbol_universe") or []):
        explain["symbol_universe"] = list(active_model.get("symbol_universe") or [])
    if int(active_model.get("horizon_s") or 0) > 0:
        explain["configured_horizon_s"] = int(active_model.get("horizon_s") or 0)
    if list(active_model.get("horizons_s") or []):
        explain["configured_horizons_s"] = list(active_model.get("horizons_s") or [])
    if int(active_model.get("training_window_days") or 0) > 0:
        explain["training_window_days"] = int(active_model.get("training_window_days") or 0)
    if feature_schema:
        explain["feature_schema"] = feature_schema
    explain = _attach_prediction_regime_context(explain, regime_context)
    if active_family != "embed_regressor":
        explain["serve_fallback"] = {
            "requested_model_name": str(active_model_name),
            "requested_family": str(active_family),
            "served_family": "knn_prior",
            "reason": "requested_live_model_unavailable",
        }
    explain = _apply_model_serving_diagnostics(explain, active_model)

    drift_scale = 1.0
    con = None
    try:
        con = connect()
        row = con.execute(
            """
            SELECT drift_ratio FROM model_drift
            WHERE symbol=? AND horizon_s=?
            """,
            (str(sym), int(h)),
        ).fetchone()
        if row and float(row[0]) > 1.0:
            drift_scale = float(1.0 / min(3.0, float(row[0])))
    except Exception:
        drift_scale = 1.0
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal(
                "predictor_model_drift_connection_close_failed",
                "PREDICTOR_MODEL_DRIFT_CONNECTION_CLOSE_FAILED",
                e,
                warn_key="predictor_model_drift_connection_close_failed",
                symbol=str(sym),
                horizon_s=int(h),
            )

    explain["drift"] = {"applied": True, "scale": float(drift_scale)}
    conf = float(max(0.0, min(1.0, conf * drift_scale)))
    return float(z), float(conf), explain


def _build_family_prediction_payload(
    family: str,
    prediction: float,
    confidence: float,
    explain: Dict[str, Any],
) -> Dict[str, Any]:
    explain_dict = dict(explain or {})
    return {
        "family": str(family or "").strip(),
        "prediction": float(prediction),
        "confidence": float(max(0.0, min(1.0, confidence))),
        "model_name": str(explain_dict.get("model_name") or explain_dict.get("model") or ""),
        "model_id": str(explain_dict.get("model_id") or explain_dict.get("model_name") or ""),
        "model_kind": str(explain_dict.get("model_kind") or ""),
        "model_version": str(explain_dict.get("model_version") or ""),
        "explain": explain_dict,
    }


def _maybe_apply_stacked_ridge_ensemble(
    query_vec: np.ndarray,
    sym: str,
    h: int,
    *,
    top_k: int,
    event: Optional[Dict],
    active_model: Dict[str, Any],
    base_prediction: Tuple[float, float, Dict[str, Any]],
) -> Tuple[float, float, Dict[str, Any]]:
    primary_model = dict(active_model or {})
    primary_name = str(primary_model.get("model_name") or "").strip()
    primary_family = str(
        primary_model.get("family")
        or primary_model.get("model_family")
        or _model_family(primary_name)
    ).strip() or "embed_regressor"
    base_z, base_conf, base_explain = base_prediction
    base_explain_dict = dict(base_explain or {})
    ts_ms = _safe_int((event or {}).get("ts_ms"), int(time.time() * 1000))

    base_member = _build_family_prediction_payload(
        str(primary_family),
        float(base_z),
        float(base_conf),
        dict(base_explain_dict),
    )

    def _predict_family(family_name: str) -> Optional[Dict[str, Any]]:
        family_key = str(family_name or "").strip()
        if not family_key:
            return None
        if family_key == primary_family:
            return dict(base_member)
        family_model = _resolve_active_model_for_family(
            str(sym),
            int(h),
            str(family_key),
            primary_active_model=primary_model,
        )
        if not family_model:
            return None
        z_family, conf_family, explain_family = _predict_resolved_model(
            query_vec,
            str(sym),
            int(h),
            top_k=int(top_k),
            active_model=dict(family_model),
            event=event,
        )
        explain_family = dict(explain_family or {})
        serve_fallback = dict(explain_family.get("serve_fallback") or {})
        served_family = str(serve_fallback.get("served_family") or "").strip()
        requested_family = str(serve_fallback.get("requested_family") or family_key).strip()
        if requested_family == family_key and served_family and served_family != family_key:
            return None
        return _build_family_prediction_payload(
            str(family_key),
            float(z_family),
            float(conf_family),
            dict(explain_family),
        )

    result = RidgeStackBlender().blend(
        symbol=str(sym),
        horizon=int(h),
        ts=int(ts_ms),
        base_prediction=float(base_z),
        base_confidence=float(base_conf),
        base_family=str(primary_family),
        predict_family=_predict_family,
    )
    diagnostics = dict(result.diagnostics or {})
    diagnostics["method"] = "ridge_stack"
    diagnostics["base_model_name"] = str(base_explain_dict.get("model_name") or primary_name)

    explain_dict = dict(base_explain_dict)
    explain_dict["ensemble_blend"] = dict(diagnostics)
    explain_dict["ensemble_output"] = {
        "aggregated_confidence": float(result.confidence),
        "ensemble_size": int(len(diagnostics.get("components") or {})),
        "fallback": bool(not result.applied),
        "fallback_reason": str(diagnostics.get("fallback_reason") or ""),
        "final_prediction": float(result.prediction),
        "method": "ridge_stack",
        "weight_ts": int(diagnostics.get("weight_ts") or 0),
    }
    explain_dict["ensemble_components"] = dict(diagnostics.get("components") or {})
    explain_dict["ensemble_weights"] = dict(diagnostics.get("weights") or {})
    return float(result.prediction), float(result.confidence), explain_dict


def _maybe_apply_ensemble_blend(
    query_vec: np.ndarray,
    sym: str,
    h: int,
    *,
    top_k: int,
    event: Optional[Dict],
    active_model: Dict[str, Any],
    base_prediction: Tuple[float, float, Dict[str, Any]],
) -> Tuple[float, float, Dict[str, Any]]:
    if not ensemble_blend_enabled():
        return base_prediction

    mode = str(ensemble_blend_mode() or "equal").strip().lower() or "equal"
    primary_model = dict(active_model or {})
    primary_family = str(primary_model.get("family") or _model_family(primary_model.get("model_name") or "")).strip() or "embed_regressor"
    base_z, base_conf, base_explain = base_prediction
    base_explain_dict = dict(base_explain or {})
    ts_ms = _safe_int((event or {}).get("ts_ms"), int(time.time() * 1000))
    regime = str(base_explain_dict.get("regime_at_trade") or base_explain_dict.get("regime") or "global").strip() or "global"
    attempted_families: List[str] = []
    try:
        for model_name in active_model_names(symbol=str(sym), horizon_s=int(h)):
            family_name = str(_model_family(model_name)).strip()
            if family_name and family_name not in attempted_families:
                attempted_families.append(family_name)
    except Exception as e:
        _warn_nonfatal(
            "predictor_ensemble_active_family_resolution_failed",
            "PREDICTOR_ENSEMBLE_ACTIVE_FAMILY_RESOLUTION_FAILED",
            e,
            warn_key=f"predictor_ensemble_active_family_resolution_failed:{sym}:{h}",
            symbol=str(sym),
            horizon_s=int(h),
        )
    if primary_family and primary_family not in attempted_families:
        attempted_families.insert(0, str(primary_family))
    if not attempted_families:
        attempted_families = [str(primary_family)] if primary_family else []

    def _predict_family(family_name: str) -> Optional[Dict[str, Any]]:
        family_key = str(family_name or "").strip()
        if not family_key:
            return None
        family_model = _resolve_active_model_for_family(
            str(sym),
            int(h),
            str(family_key),
            primary_active_model=primary_model,
        )
        if not family_model:
            return None
        z_family, conf_family, explain_family = _predict_resolved_model(
            query_vec,
            str(sym),
            int(h),
            top_k=int(top_k),
            active_model=dict(family_model),
            event=event,
        )
        explain_family = dict(explain_family or {})
        serve_fallback = dict(explain_family.get("serve_fallback") or {})
        served_family = str(serve_fallback.get("served_family") or "").strip()
        requested_family = str(serve_fallback.get("requested_family") or family_key).strip()
        if requested_family == family_key and served_family and served_family != family_key:
            return None
        return _build_family_prediction_payload(
            str(family_key),
            float(z_family),
            float(conf_family),
            dict(explain_family or {}),
        )

    base_member = _build_family_prediction_payload(
        str(primary_family),
        float(base_z),
        float(base_conf),
        dict(base_explain_dict),
    )
    family_preds: Dict[str, Any] = {}
    try:
        set_prediction_context(
            families=list(attempted_families),
            base_family_pred=dict(base_member),
            predict_family=_predict_family,
        )
        family_preds = dict(collect_family_predictions(str(sym), int(ts_ms)) or {})
    finally:
        clear_prediction_context()

    available_families = [family for family in family_preds.keys() if not str(family).startswith("__")]
    weight_payload = compute_blend_weights(dict(family_preds), str(mode), regime=str(regime))
    hmm_signal: Dict[str, Any] = {}
    hmm_weight_adjustment: Dict[str, Any] = {}
    try:
        from engine.strategy.hmm_regime import (
            apply_hmm_uncertainty_to_weights,
            resolve_hmm_regime_snapshot,
        )

        hmm_signal = dict(
            resolve_hmm_regime_snapshot(
                symbol=str(sym or "SPY"),
                ts_ms=int(ts_ms),
            )
            or {}
        )
        weight_payload, hmm_weight_adjustment = apply_hmm_uncertainty_to_weights(
            dict(weight_payload or {}),
            available_families=list(available_families),
            signal=dict(hmm_signal or {}),
        )
    except Exception as e:
        _warn_nonfatal(
            "predictor_hmm_ensemble_adjust_failed",
            "PREDICTOR_HMM_ENSEMBLE_ADJUST_FAILED",
            e,
            warn_key=f"predictor_hmm_ensemble_adjust_failed:{sym}:{h}",
            symbol=str(sym),
            horizon_s=int(h),
        )
    persisted_weight_payload: Dict[str, Any] = {"mode": str(mode), "weights": {}}
    if isinstance(weight_payload, dict):
        persisted_weight_payload["weights"] = {
            str(key): float(value)
            for key, value in weight_payload.items()
            if str(key or "").strip() and not str(key).startswith("__")
        }
        if "__intercept__" in weight_payload:
            persisted_weight_payload["intercept"] = float(weight_payload.get("__intercept__") or 0.0)
        if "__trained_ts__" in weight_payload:
            persisted_weight_payload["trained_ts"] = int(weight_payload.get("__trained_ts__") or 0)
        if "__has_meta_blob__" in weight_payload:
            persisted_weight_payload["has_meta_blob"] = bool(weight_payload.get("__has_meta_blob__"))

    if str(mode) in {"equal", "inverse_variance"} and dict(persisted_weight_payload.get("weights") or {}):
        try:
            persist_blend_weights(
                mode=str(mode),
                regime=str(regime),
                weights=dict(persisted_weight_payload.get("weights") or {}),
            )
        except Exception as e:
            _warn_nonfatal(
                "predictor_ensemble_weight_persist_failed",
                "PREDICTOR_ENSEMBLE_WEIGHT_PERSIST_FAILED",
                e,
                warn_key=f"predictor_ensemble_weight_persist_failed:{mode}:{regime}",
                mode=str(mode),
                regime=str(regime),
            )

    blended_prediction, diagnostics = blend_predictions(dict(family_preds), dict(weight_payload or {}))
    diagnostics = dict(diagnostics or {})
    diagnostics["mode"] = str(mode)
    diagnostics["base_family"] = str(primary_family)
    diagnostics["base_model_name"] = str(base_explain_dict.get("model_name") or "")
    diagnostics["missing_families"] = list(
        diagnostics.get("missing_families")
        or family_preds.get("__missing_families__")
        or []
    )
    diagnostics["attempted_families"] = list(
        family_preds.get("__attempted_families__")
        or attempted_families
        or []
    )
    diagnostics["available_families"] = list(diagnostics.get("available_families") or available_families)
    diagnostics["min_agreement"] = float(ensemble_min_agreement())
    diagnostics["requested_ts_ms"] = int(ts_ms)
    if hmm_signal:
        diagnostics["hmm_regime"] = dict(hmm_signal)
    if hmm_weight_adjustment:
        diagnostics["hmm_weight_adjustment"] = dict(hmm_weight_adjustment)

    fallback_reason = ""
    if len(available_families) < 2:
        diagnostics["applied"] = False
        fallback_reason = "insufficient_family_predictions"
    elif float(diagnostics.get("agreement") or 0.0) < float(ensemble_min_agreement()):
        diagnostics["applied"] = False
        fallback_reason = "agreement_below_threshold"
    else:
        diagnostics["applied"] = True

    if fallback_reason:
        diagnostics["fallback_reason"] = str(fallback_reason)
        final_prediction = float(base_z)
        final_confidence = float(base_conf)
    else:
        final_prediction = float(blended_prediction)
        final_confidence = float(
            max(
                0.0,
                min(
                    1.0,
                    _safe_float(diagnostics.get("blended_confidence"), float(base_conf)),
                ),
            )
        )

    explain_dict = dict(base_explain_dict)
    explain_dict["ensemble_blend"] = dict(diagnostics)
    explain_dict["ensemble_output"] = {
        "agreement": float(diagnostics.get("agreement") or 0.0),
        "aggregated_confidence": float(final_confidence),
        "attempted_size": int(len(diagnostics.get("attempted_families") or attempted_families)),
        "ensemble_size": int(len(available_families)),
        "fallback": bool(not diagnostics.get("applied")),
        "fallback_reason": str(fallback_reason),
        "final_prediction": float(final_prediction),
        "method": str(mode),
    }

    persisted_weight_payload["applied"] = bool(diagnostics.get("applied"))
    persisted_weight_payload["missing_families"] = list(diagnostics.get("missing_families") or [])
    persisted_weight_payload["attempted_families"] = list(diagnostics.get("attempted_families") or [])
    persisted_weight_payload["agreement"] = float(diagnostics.get("agreement") or 0.0)
    try:
        persist_ensemble_prediction(
            symbol=str(sym),
            ts=int(ts_ms),
            blended_prediction=float(final_prediction),
            family_preds=dict(family_preds),
            weights=dict(persisted_weight_payload),
            agreement=float(diagnostics.get("agreement") or 0.0),
        )
    except Exception as e:
        _warn_nonfatal(
            "predictor_ensemble_prediction_persist_failed",
            "PREDICTOR_ENSEMBLE_PREDICTION_PERSIST_FAILED",
            e,
            warn_key=f"predictor_ensemble_prediction_persist_failed:{sym}:{h}",
            symbol=str(sym),
            horizon_s=int(h),
            mode=str(mode),
        )

    return float(final_prediction), float(final_confidence), explain_dict


def _hedge_component_vector(diagnostics: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "method": "hedge",
        "mode": "hedge",
        "components": dict(diagnostics.get("components") or {}),
        "weights": dict(diagnostics.get("weights") or {}),
        "qualified_models": list(diagnostics.get("qualified_models") or []),
        "excluded_models": dict(diagnostics.get("excluded_models") or {}),
        "missing_models": list(diagnostics.get("missing_models") or []),
        "weight_ts_ms": int(diagnostics.get("weight_ts_ms") or 0),
        "applied": bool(diagnostics.get("applied")),
        "fallback_reason": str(diagnostics.get("fallback_reason") or ""),
    }


def _with_hedge_diagnostics(
    base_prediction: Tuple[float, float, Dict[str, Any]],
    *,
    diagnostics: Dict[str, Any],
) -> Tuple[float, float, Dict[str, Any]]:
    z_base, conf_base, explain_base = base_prediction
    explain_dict = dict(explain_base or {})
    diagnostics = dict(diagnostics or {})
    component_vector = _hedge_component_vector(diagnostics)
    explain_dict["prediction_blend_mode"] = "hedge"
    explain_dict["ensemble_blend"] = dict(diagnostics)
    explain_dict["ensemble_output"] = {
        "aggregated_confidence": float(conf_base),
        "ensemble_size": int(len(diagnostics.get("components") or {})),
        "fallback": bool(not diagnostics.get("applied")),
        "fallback_reason": str(diagnostics.get("fallback_reason") or ""),
        "final_prediction": float(z_base),
        "method": "hedge",
        "weight_ts": int(diagnostics.get("weight_ts_ms") or 0),
    }
    explain_dict["ensemble_components"] = dict(diagnostics.get("components") or {})
    explain_dict["ensemble_weights"] = dict(diagnostics.get("weights") or {})
    explain_dict["component_vector"] = dict(component_vector)
    return float(z_base), float(conf_base), explain_dict


def _maybe_apply_hedge_blend(
    query_vec: np.ndarray,
    sym: str,
    h: int,
    *,
    top_k: int,
    event: Optional[Dict],
    active_model: Dict[str, Any],
    base_prediction: Tuple[float, float, Dict[str, Any]],
) -> Tuple[float, float, Dict[str, Any]]:
    primary_model = dict(active_model or {})
    primary_name = str(primary_model.get("model_name") or "").strip()
    base_z, base_conf, base_explain = base_prediction
    base_explain_dict = dict(base_explain or {})
    ts_ms = _safe_int((event or {}).get("ts_ms"), int(time.time() * 1000))
    diagnostics: Dict[str, Any] = {
        "mode": "hedge",
        "method": "hedge",
        "requested_ts_ms": int(ts_ms),
        "base_model_name": str(primary_name),
        "qualified_models": [],
        "components": {},
        "weights": {},
        "excluded_models": {},
        "missing_models": [],
        "applied": False,
    }

    con = None
    try:
        con = connect(readonly=True)
        qualified = hedge_ensemble.qualified_model_pool(
            con,
            symbol=str(sym),
            horizon=int(h),
            champion_name=str(primary_name),
            asof_ts_ms=int(ts_ms),
        )
        diagnostics["qualified_models"] = list(qualified)
        if len(qualified) < 2:
            diagnostics["fallback_reason"] = "insufficient_qualified_pool"
            return _with_hedge_diagnostics(base_prediction, diagnostics=diagnostics)
        weight_row = hedge_ensemble.load_hedge_weights(
            con,
            symbol=str(sym),
            horizon=int(h),
            qualified_models=list(qualified),
            ensure=False,
        )
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal(
                "predictor_hedge_connection_close_failed",
                "PREDICTOR_HEDGE_CONNECTION_CLOSE_FAILED",
                e,
                warn_key=f"predictor_hedge_connection_close_failed:{sym}:{h}",
                symbol=str(sym),
                horizon_s=int(h),
            )

    if not weight_row:
        diagnostics["fallback_reason"] = "no_hedge_weights"
        return _with_hedge_diagnostics(base_prediction, diagnostics=diagnostics)

    raw_weights = dict(weight_row.get("weights") or {})
    diagnostics["weight_ts_ms"] = int(weight_row.get("ts_ms") or 0)
    diagnostics["weight_regime"] = str(weight_row.get("regime") or "")
    if list(weight_row.get("excluded_models") or []):
        diagnostics["excluded_models"] = {
            str(model): "not_qualified"
            for model in list(weight_row.get("excluded_models") or [])
            if str(model or "").strip()
        }

    components: Dict[str, Dict[str, Any]] = {}
    missing: List[str] = []
    excluded = dict(diagnostics.get("excluded_models") or {})

    def _component_from_result(model_name: str, result: Tuple[float, float, Dict[str, Any]]) -> Dict[str, Any]:
        z_model, conf_model, explain_model = result
        explain_model = dict(explain_model or {})
        return {
            "prediction": float(z_model),
            "confidence": float(max(0.0, min(1.0, conf_model))),
            "model_name": str(explain_model.get("model_name") or model_name),
            "model_id": str(explain_model.get("model_id") or explain_model.get("model_name") or model_name),
            "model_kind": str(explain_model.get("model_kind") or ""),
            "model_version": str(explain_model.get("model_version") or ""),
        }

    for model_name in sorted(raw_weights.keys()):
        model_key = str(model_name or "").strip()
        if not model_key:
            continue
        if model_key == primary_name:
            components[model_key] = _component_from_result(model_key, base_prediction)
            continue
        try:
            if not is_active_model_name(model_key):
                excluded[model_key] = "inactive_model_config"
                continue
            model_spec = _resolve_active_model(str(sym), int(h), forced_model_name=model_key)
            z_model, conf_model, explain_model = _predict_resolved_model(
                query_vec,
                str(sym),
                int(h),
                top_k=int(top_k),
                active_model=dict(model_spec),
                event=event,
            )
        except Exception as e:
            _warn_nonfatal(
                "predictor_hedge_component_failed",
                "PREDICTOR_HEDGE_COMPONENT_FAILED",
                e,
                warn_key=f"predictor_hedge_component_failed:{sym}:{h}:{model_key}",
                symbol=str(sym),
                horizon_s=int(h),
                model_name=str(model_key),
            )
            missing.append(str(model_key))
            continue
        components[model_key] = _component_from_result(model_key, (z_model, conf_model, explain_model))

    diagnostics["components"] = dict(components)
    diagnostics["missing_models"] = list(missing)
    diagnostics["excluded_models"] = dict(excluded)
    if len(components) < 2:
        diagnostics["fallback_reason"] = "insufficient_component_predictions"
        return _with_hedge_diagnostics(base_prediction, diagnostics=diagnostics)

    available_weights = {
        model: float(raw_weights.get(model, 0.0))
        for model in components.keys()
        if float(raw_weights.get(model, 0.0) or 0.0) > 0.0
    }
    blend_weights = hedge_ensemble._apply_floor(available_weights, floor=0.0)
    if not blend_weights:
        diagnostics["fallback_reason"] = "empty_available_weights"
        return _with_hedge_diagnostics(base_prediction, diagnostics=diagnostics)

    final_prediction = 0.0
    final_confidence = 0.0
    for model_name, weight in blend_weights.items():
        component = dict(components.get(model_name) or {})
        final_prediction += float(weight) * float(component.get("prediction") or 0.0)
        final_confidence += float(weight) * float(component.get("confidence") or 0.0)

    diagnostics["weights"] = {str(model): float(blend_weights[model]) for model in sorted(blend_weights)}
    diagnostics["applied"] = True
    diagnostics["fallback_reason"] = "partial_component_predictions" if (missing or excluded) else ""
    diagnostics["final_prediction"] = float(final_prediction)
    diagnostics["aggregated_confidence"] = float(max(0.0, min(1.0, final_confidence)))

    explain_dict = dict(base_explain_dict)
    component_vector = _hedge_component_vector(diagnostics)
    explain_dict["prediction_blend_mode"] = "hedge"
    explain_dict["ensemble_blend"] = dict(diagnostics)
    explain_dict["ensemble_output"] = {
        "aggregated_confidence": float(diagnostics["aggregated_confidence"]),
        "ensemble_size": int(len(components)),
        "fallback": False,
        "fallback_reason": str(diagnostics.get("fallback_reason") or ""),
        "final_prediction": float(final_prediction),
        "method": "hedge",
        "weight_ts": int(diagnostics.get("weight_ts_ms") or 0),
    }
    explain_dict["ensemble_components"] = dict(components)
    explain_dict["ensemble_weights"] = dict(diagnostics["weights"])
    explain_dict["component_vector"] = dict(component_vector)
    return float(final_prediction), float(diagnostics["aggregated_confidence"]), explain_dict


def _predict_single_model(
    query_vec: np.ndarray,
    sym: str,
    h: int,
    *,
    top_k: int,
    event: Optional[Dict] = None,
    forced_model_name: Optional[str] = None,
) -> Tuple[float, float, Dict]:
    active_model = _resolve_active_model(sym, int(h), forced_model_name=forced_model_name)
    served = _predict_resolved_model(
        query_vec,
        str(sym),
        int(h),
        top_k=int(top_k),
        active_model=dict(active_model or {}),
        event=event,
    )
    if forced_model_name is None:
        prediction_blend_mode = hedge_ensemble.prediction_blend_mode()
        if prediction_blend_mode == "hedge":
            try:
                served = _maybe_apply_hedge_blend(
                    query_vec,
                    str(sym),
                    int(h),
                    top_k=int(top_k),
                    event=event,
                    active_model=dict(active_model or {}),
                    base_prediction=served,
                )
            except Exception as e:
                _warn_nonfatal(
                    "predictor_hedge_blend_failed",
                    "PREDICTOR_HEDGE_BLEND_FAILED",
                    e,
                    warn_key=f"predictor_hedge_blend_failed:{sym}:{h}",
                    symbol=str(sym),
                    horizon_s=int(h),
                )
        ridge_mode = str(ridge_stack_ensemble_mode() or "blend").strip().lower()
        ridge_applied = False
        ensemble_enabled = bool(ensemble_blend_enabled())
        if prediction_blend_mode != "hedge" and ensemble_enabled and ridge_mode != "single_champion":
            try:
                served = _maybe_apply_stacked_ridge_ensemble(
                    query_vec,
                    str(sym),
                    int(h),
                    top_k=int(top_k),
                    event=event,
                    active_model=dict(active_model or {}),
                    base_prediction=served,
                )
                ridge_applied = bool(
                    dict((served[2] or {}).get("ensemble_blend") or {}).get("applied")
                )
            except Exception as e:
                _warn_nonfatal(
                    "predictor_ridge_stack_ensemble_failed",
                    "PREDICTOR_RIDGE_STACK_ENSEMBLE_FAILED",
                    e,
                    warn_key=f"predictor_ridge_stack_ensemble_failed:{sym}:{h}",
                    symbol=str(sym),
                    horizon_s=int(h),
                )
        if prediction_blend_mode != "hedge" and ensemble_enabled and ridge_mode != "single_champion" and not ridge_applied:
            try:
                served = _maybe_apply_ensemble_blend(
                    query_vec,
                    str(sym),
                    int(h),
                    top_k=int(top_k),
                    event=event,
                    active_model=dict(active_model or {}),
                    base_prediction=served,
                )
            except Exception as e:
                _warn_nonfatal(
                    "predictor_ensemble_blend_failed",
                    "PREDICTOR_ENSEMBLE_BLEND_FAILED",
                    e,
                    warn_key=f"predictor_ensemble_blend_failed:{sym}:{h}",
                    symbol=str(sym),
                    horizon_s=int(h),
                )
    z_final, conf_final, explain_final = served
    explain_dict = dict(explain_final or {})
    if event is not None:
        try:
            feature_ids = resolve_feature_ids(
                explain_dict.get("feature_ids"),
                model_name=str(explain_dict.get("model_name") or ""),
            )
        except Exception:
            feature_ids = resolve_feature_ids(model_name=str(explain_dict.get("model_name") or ""))
        try:
            feature_snapshot = explain_dict.get("feature_snapshot")
            if not isinstance(feature_snapshot, dict):
                feature_snapshot = _cached_or_build_feature_snapshot(
                    event=event,
                    symbol=str(sym),
                    feature_ids=feature_ids,
            )
            explain_dict["feature_snapshot"] = dict(feature_snapshot or {})
            explain_dict["feature_ids"] = list(feature_ids)
            explain_dict["feature_set_tag"] = _registry_feature_set_tag(
                feature_ids,
                model_name=str(explain_dict.get("model_name") or ""),
            )
        except Exception as e:
            _warn_nonfatal(
                "predictor_feature_snapshot_build_failed",
                "PREDICTOR_FEATURE_SNAPSHOT_BUILD_FAILED",
                e,
                warn_key=f"predictor_feature_snapshot_build_failed:{sym}:{h}",
                symbol=str(sym),
                horizon_s=int(h),
            )
    try:
        explain_dict = _maybe_attach_prediction_explanation(
            symbol=str(sym),
            horizon_s=int(h),
            event=event,
            explain=dict(explain_dict),
            feature_snapshot=(dict(explain_dict.get("feature_snapshot") or {}) if isinstance(explain_dict.get("feature_snapshot"), dict) else None),
        )
    except Exception as e:
        _warn_nonfatal(
            "predictor_prediction_explanation_attach_failed",
            "PREDICTOR_PREDICTION_EXPLANATION_ATTACH_FAILED",
            e,
            warn_key=f"predictor_prediction_explanation_attach_failed:{sym}:{h}",
            symbol=str(sym),
            horizon_s=int(h),
        )
    explain_dict = _apply_model_serving_diagnostics(explain_dict, active_model)
    _track_prediction_output(
        symbol=str(sym),
        horizon_s=int(h),
        prediction=float(z_final),
        confidence=float(conf_final),
        explain=dict(explain_dict or {}),
        source="legacy_predictor",
    )
    return float(z_final), float(conf_final), dict(explain_dict or {})


def _maybe_apply_feature_neutralization(
    out: Dict[Tuple[str, int], Tuple[float, float, Dict]],
    *,
    symbols: List[str],
    horizon_s: int,
) -> Dict[Tuple[str, int], Tuple[float, float, Dict]]:
    mode = neutralize_mode()
    if mode == "off":
        return out

    predictions: Dict[str, float] = {}
    feature_snapshots: Dict[str, Dict[str, Any]] = {}
    key_by_symbol: Dict[str, Tuple[str, int]] = {}
    for raw_sym in symbols or []:
        key = (str(raw_sym), int(horizon_s))
        if key not in out:
            continue
        z0, _conf0, explain0 = out[key]
        symbol_key = str(raw_sym).upper().strip()
        if not symbol_key:
            continue
        predictions[symbol_key] = float(z0)
        explain_dict = dict(explain0 or {})
        snapshot = explain_dict.get("feature_snapshot")
        feature_snapshots[symbol_key] = dict(snapshot or {}) if isinstance(snapshot, dict) else {}
        key_by_symbol[symbol_key] = key

    if not predictions:
        return out

    result = neutralize_predictions(
        predictions,
        feature_snapshots,
        mode=str(mode),
    )
    diagnostics = result.diagnostics()
    for symbol_key, key in key_by_symbol.items():
        z0, conf0, explain0 = out[key]
        explain_dict = dict(explain0 or {})
        neutral_z = float(result.neutralized_predictions.get(symbol_key, z0))
        served_z = neutral_z if mode == "serve" and bool(result.applied) else float(z0)
        explain_dict["feature_neutralization"] = {
            **diagnostics,
            "symbol": str(symbol_key),
            "raw_prediction": float(z0),
            "neutralized_prediction": float(neutral_z),
            "served_prediction": float(served_z),
            "served": bool(mode == "serve" and bool(result.applied)),
        }
        if mode == "serve" and bool(result.applied):
            explain_dict["raw_prediction_before_feature_neutralization"] = float(z0)
        out[key] = (float(served_z), float(conf0), explain_dict)
    return out


def _ranker_equity_scope_symbol(symbol: str) -> bool:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return False
    try:
        asset_class = _prediction_asset_class(sym)
    except Exception:
        asset_class = "UNKNOWN"
    if asset_class in _RANKER_EQUITY_ASSET_CLASSES:
        return True
    if asset_class in _RANKER_DEFAULT_EXCLUDED_ASSET_CLASSES:
        return False
    return bool(sym.replace(".", "").isalnum()) and not sym.endswith(("USD", "USDT"))


def _ranker_active_asset_scope(active_model: Mapping[str, Any]) -> str:
    active = dict(active_model or {})
    metadata = {}
    try:
        metadata = {**dict(active.get("metadata") or {}), **dict(active.get("meta") or {})}
    except Exception:
        metadata = {}
    feature_schema = {}
    try:
        feature_schema = dict(active.get("feature_schema") or {})
    except Exception:
        feature_schema = {}
    raw = str(
        active.get("asset_scope")
        or active.get("ranker_asset_scope")
        or active.get("training_asset_scope")
        or metadata.get("asset_scope")
        or metadata.get("ranker_asset_scope")
        or metadata.get("training_asset_scope")
        or feature_schema.get("asset_scope")
        or active.get("learning_scope")
        or metadata.get("learning_scope")
        or feature_schema.get("learning_scope")
        or "EQUITY"
    ).upper().strip()
    if "CRYPTO" in raw or "DIGITAL_ASSET" in raw:
        return "CRYPTO"
    if raw in {
        "EQUITY",
        "EQUITIES",
        "US_EQUITY",
        "STOCK",
        "STOCKS",
        "CROSS_SECTIONAL_EQUITIES",
        "CROSS_SECTIONAL_EQUITY",
    }:
        return "EQUITY"
    return raw or "EQUITY"


def _ranker_symbol_in_asset_scope(symbol: str, active_model: Mapping[str, Any]) -> bool:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return False
    scope = _ranker_active_asset_scope(active_model)
    if scope == "CRYPTO":
        return _prediction_asset_class(sym) in _CRYPTO_ASSET_CLASSES
    if scope == "EQUITY":
        return _ranker_equity_scope_symbol(sym)
    if scope in _CRYPTO_ASSET_CLASSES:
        return _prediction_asset_class(sym) in _CRYPTO_ASSET_CLASSES
    if scope in _RANKER_EQUITY_ASSET_CLASSES:
        return _ranker_equity_scope_symbol(sym)
    return False


def _ranker_selection_count(env_name: str, default_value: int, universe_n: int) -> int:
    raw = os.environ.get(str(env_name))
    default = max(0, int(default_value or 0))
    value = _safe_int(raw, default) if raw is not None else default
    return max(0, min(int(value), int(max(0, universe_n))))


def _active_model_is_lgbm_ranker(active_model: Mapping[str, Any]) -> bool:
    active = dict(active_model or {})
    family = str(
        active.get("model_family")
        or active.get("family")
        or _model_family(str(active.get("model_name") or ""))
    ).strip()
    return family == "lgbm_ranker"


def _ranker_artifact_location(active_model: Mapping[str, Any]) -> Dict[str, str]:
    active = dict(active_model or {})
    loc = {
        "alias": str(active.get("artifact_alias") or ""),
        "sha256": str(active.get("artifact_sha256") or ""),
        "path": str(active.get("artifact_path") or ""),
    }
    if any(str(loc.get(k) or "").strip() for k in ("alias", "sha256", "path")):
        return loc
    return _artifact_location_for_model(str(active.get("model_name") or ""))


def _maybe_apply_lgbm_ranker_batch(
    out: Dict[Tuple[str, int], Tuple[float, float, Dict]],
    *,
    symbols: List[str],
    horizon_s: int,
    top_k: int,
    event: Optional[Dict],
) -> Dict[Tuple[str, int], Tuple[float, float, Dict]]:
    groups: Dict[Tuple[str, str, str, str, str, Tuple[str, ...]], Dict[str, Any]] = {}
    for sym_raw in list(symbols or []):
        sym = str(sym_raw or "").upper().strip()
        if not sym:
            continue
        try:
            active = _resolve_active_model(sym, int(horizon_s))
        except Exception as e:
            _warn_nonfatal(
                "predictor_lgbm_ranker_active_model_lookup_failed",
                "PREDICTOR_LGBM_RANKER_ACTIVE_MODEL_LOOKUP_FAILED",
                e,
                warn_key=f"predictor_lgbm_ranker_active_model_lookup_failed:{sym}:{horizon_s}",
                symbol=str(sym),
                horizon_s=int(horizon_s),
            )
            continue
        if not _active_model_is_lgbm_ranker(active):
            continue
        if not _ranker_symbol_in_asset_scope(sym, active):
            continue
        loc = _ranker_artifact_location(active)
        if not any(str(loc.get(k) or "").strip() for k in ("alias", "sha256", "path")):
            continue
        feature_ids = resolve_feature_ids(
            active.get("feature_ids"),
            model_name=str(active.get("model_name") or ""),
            model_spec=dict(active or {}),
        )
        key = (
            str(active.get("model_name") or ""),
            str(loc.get("alias") or ""),
            str(loc.get("sha256") or ""),
            str(loc.get("path") or ""),
            str(_ranker_active_asset_scope(active)),
            tuple(str(fid) for fid in list(feature_ids or [])),
        )
        bucket = groups.setdefault(key, {"active": dict(active), "location": dict(loc), "symbols": [], "feature_ids": list(feature_ids)})
        bucket["symbols"].append(str(sym))

    if not groups:
        return out

    event_payload = dict(event or {})
    if not event_payload:
        event_payload = {"ts_ms": int(time.time() * 1000), "title": "", "body": "", "source": ""}

    next_out = dict(out)
    for _key, bucket in groups.items():
        syms = [str(s) for s in list(bucket.get("symbols") or []) if str(s or "").strip()]
        if len(syms) < 2:
            continue
        active = dict(bucket.get("active") or {})
        asset_scope = _ranker_active_asset_scope(active)
        loc = dict(bucket.get("location") or {})
        try:
            model = load_lgbm_ranker_model_from_artifact(
                alias=str(loc.get("alias") or ""),
                sha256=str(loc.get("sha256") or ""),
                path=(str(loc.get("path") or "") or None),
            )
            feature_ids = [str(feature_id) for feature_id in list(getattr(model, "feature_ids", None) or bucket.get("feature_ids") or [])]
            decision_ts_ms = int(event_payload.get("ts_ms") or time.time() * 1000)
            cached_feature_maps = _latest_feature_snapshot_features_many(
                syms,
                list(feature_ids),
                decision_ts_ms=int(decision_ts_ms),
            )
            feature_maps = []
            for sym in syms:
                symbol_key = str(sym).upper().strip()
                feature_map = dict(cached_feature_maps.get(symbol_key) or {})
                missing_feature_ids = [str(feature_id) for feature_id in feature_ids if str(feature_id) not in feature_map]
                if not feature_map or missing_feature_ids:
                    feature_map = _cached_or_build_feature_snapshot(
                        event=event_payload,
                        symbol=str(sym),
                        feature_ids=list(feature_ids),
                    )
                    missing_feature_ids = [str(feature_id) for feature_id in feature_ids if str(feature_id) not in feature_map]
                if missing_feature_ids:
                    feature_map = build_feature_snapshot(
                        event=event_payload,
                        symbol=str(sym),
                        feature_ids=list(feature_ids),
                    )
                    missing_feature_ids = [str(feature_id) for feature_id in feature_ids if str(feature_id) not in feature_map]
                if missing_feature_ids:
                    _warn_nonfatal(
                        "predictor_lgbm_ranker_feature_snapshot_incomplete",
                        "PREDICTOR_LGBM_RANKER_FEATURE_SNAPSHOT_INCOMPLETE",
                        RuntimeError("lgbm_ranker_feature_snapshot_incomplete"),
                        warn_key=f"predictor_lgbm_ranker_feature_snapshot_incomplete:{active.get('model_name')}:{horizon_s}:{symbol_key}",
                        horizon_s=int(horizon_s),
                        model_name=str(active.get("model_name") or ""),
                        symbol=symbol_key,
                        missing_feature_ids=missing_feature_ids,
                    )
                    raise ValueError("lgbm_ranker_feature_snapshot_incomplete")
                feature_maps.append(dict(feature_map or {}))
            scores = model.predict(feature_maps)
            default_leg = max(1, min(3, int(top_k or 3), len(syms) // 2 if len(syms) > 2 else 1))
            top_n = _ranker_selection_count("LGBM_RANKER_TOP_K", default_leg, len(syms))
            bottom_n = _ranker_selection_count("LGBM_RANKER_BOTTOM_K", default_leg, max(0, len(syms) - top_n))
            signals = ranker_scores_to_signals(syms, list(scores), top_k=int(top_n), bottom_k=int(bottom_n))
        except Exception as e:
            _warn_nonfatal(
                "predictor_lgbm_ranker_batch_failed",
                "PREDICTOR_LGBM_RANKER_BATCH_FAILED",
                e,
                warn_key=f"predictor_lgbm_ranker_batch_failed:{active.get('model_name')}:{horizon_s}",
                horizon_s=int(horizon_s),
                model_name=str(active.get("model_name") or ""),
            )
            continue

        feature_schema = dict(getattr(model, "feature_schema", {}) or active.get("feature_schema") or {})
        training_metrics = dict(getattr(model, "training_metrics", {}) or {})
        for idx, sym in enumerate(syms):
            key = (str(sym), int(horizon_s))
            if key not in next_out:
                continue
            z0, conf0, explain0 = next_out[key]
            signal = dict(signals.get(str(sym)) or {})
            if not signal:
                continue
            feature_map = dict(feature_maps[idx] or {}) if idx < len(feature_maps) else {}
            served_z = float(signal.get("expected_z") or 0.0)
            served_conf = float(signal.get("confidence") or 0.0)
            explain = dict(explain0 or {})
            explain.update(
                {
                    "model": "lgbm_ranker",
                    "model_name": str(active.get("model_name") or getattr(model, "model_name", "") or "lgbm_ranker"),
                    "model_id": str(active.get("model_id") or active.get("model_name") or getattr(model, "model_name", "") or "lgbm_ranker"),
                    "model_family": "lgbm_ranker",
                    "served_model_family": "lgbm_ranker",
                    "model_kind": str(getattr(model, "model_kind", "lightgbm_ranker") or "lightgbm_ranker"),
                    "model_version": str(active.get("model_version") or ""),
                    "model_ts_ms": int(active.get("model_ts_ms") or 0),
                    "model_spec_source_stage": str(active.get("spec_source_stage") or ""),
                    "ranker_asset_scope": str(asset_scope),
                    "feature_ids": list(feature_ids),
                    "feature_set_tag": _registry_feature_set_tag(
                        list(feature_ids),
                        model_name=str(active.get("model_name") or getattr(model, "model_name", "") or "lgbm_ranker"),
                        model_spec=dict(feature_schema or {}),
                    ),
                    "feature_schema": dict(feature_schema),
                    "feature_snapshot": feature_map,
                    "prediction_strength": float(abs(served_z) * max(0.0, served_conf)),
                    "selection_score": float(abs(served_z) * max(0.0, served_conf)),
                    "rank_score": float(signal.get("rank_score") or 0.0),
                    "should_trade": bool(signal.get("selected")),
                    "ranker_selected": bool(signal.get("selected")),
                    "ranker_side": str(signal.get("side") or "FLAT"),
                    "ranker_rank": int(signal.get("rank") or 0),
                    "training_metrics": dict(training_metrics),
                    "artifact_alias": str(loc.get("alias") or ""),
                    "lgbm_ranker_batch": {
                        "applied": True,
                        "universe_n": int(len(syms)),
                        "top_k": int(top_n),
                        "bottom_k": int(bottom_n),
                        "raw_fallback_expected_z": float(z0),
                        "raw_fallback_confidence": float(conf0),
                        "asset_scope": str(asset_scope),
                    },
                }
            )
            explain = _attach_ood_diagnostics(
                explain,
                model,
                dict(feature_map or {}),
                warn_key=f"predictor_ood_score_failed:lgbm_ranker:{active.get('model_name')}:{sym}:{horizon_s}",
            )
            try:
                _track_prediction_output(
                    symbol=str(sym),
                    horizon_s=int(horizon_s),
                    prediction=float(served_z),
                    confidence=float(served_conf),
                    explain=dict(explain or {}),
                    source="lgbm_ranker_batch",
                )
            except Exception as e:
                _warn_nonfatal(
                    "predictor_lgbm_ranker_tracking_failed",
                    "PREDICTOR_LGBM_RANKER_TRACKING_FAILED",
                    e,
                    warn_key=f"predictor_lgbm_ranker_tracking_failed:{sym}:{horizon_s}",
                    symbol=str(sym),
                    horizon_s=int(horizon_s),
                )
            next_out[key] = (float(served_z), float(served_conf), explain)
    return next_out


def predict_event(
    query_vec: np.ndarray,
    symbols: List[str],
    horizons: List[int],
    top_k: int = 8,
    event: Optional[Dict] = None,
) -> Dict[Tuple[str, int], Tuple[float, float, Dict]]:
    """
    Returns:
      (symbol, horizon_s) -> (expected_z, confidence, explain_dict)
    """
    learned = None

    if _USE_LEARNED_REL:
        try:
            learned = learn_relevance_stats(abs_z_threshold=float(_LEARNED_REL_ABS_Z))
        except Exception:
            learned = None

    _clear_feature_snapshot_prefetch()
    if event is not None:
        prefetch_state = _prefetch_feature_snapshot_features_for_event(
            list(symbols or []),
            list(horizons or []),
            event,
        )
        if prefetch_state:
            _install_feature_snapshot_prefetch(prefetch_state)

    base: Dict[Tuple[str, int], Tuple[float, float, Dict]] = {}

    for h in horizons:
        for sym in symbols:
            z, conf, explain = _predict_single_model(
                query_vec,
                str(sym),
                int(h),
                top_k=int(top_k),
                event=event,
            )
            try:
                feature_ids = resolve_feature_ids(
                    explain.get("feature_ids"),
                    model_name=str(explain.get("model_name") or ""),
                )
            except Exception:
                feature_ids = resolve_feature_ids(model_name=str(explain.get("model_name") or ""))
            if event is not None and not isinstance(explain.get("feature_snapshot"), dict):
                try:
                    explain["feature_snapshot"] = _cached_or_build_feature_snapshot(
                        event=event,
                        symbol=str(sym),
                        feature_ids=feature_ids,
                    )
                    explain["feature_ids"] = list(feature_ids)
                    explain["feature_set_tag"] = _registry_feature_set_tag(
                        feature_ids,
                        model_name=str(explain.get("model_name") or ""),
                    )
                except Exception as e:
                    _warn_nonfatal(
                        "predictor_feature_snapshot_build_failed",
                        "PREDICTOR_FEATURE_SNAPSHOT_BUILD_FAILED",
                        e,
                        warn_key=f"predictor_feature_snapshot_build_failed:{sym}:{h}",
                        symbol=str(sym),
                        horizon_s=int(h),
                    )

            # Option 5.1 (opt-in): scale confidence by learned relevance
            if learned is not None:
                info = (learned.get(str(sym)) or {}).get(int(h)) or {}
                rel = float(info.get("relevance", 0.0))
                n = int(info.get("n", 0))

                m = float(_LEARNED_REL_CONF_FLOOR + (_LEARNED_REL_CONF_CEIL - _LEARNED_REL_CONF_FLOOR) * rel)
                m = max(0.0, min(1.0, m))

                explain["learned_relevance"] = {
                    "abs_z_threshold": float(_LEARNED_REL_ABS_Z),
                    "value": float(rel),
                    "n": int(n),
                    "conf_multiplier": float(m),
                    "applied": bool(_USE_LEARNED_REL),
                }

                if _USE_LEARNED_REL:
                    explain["confidence_base"] = float(conf)
                    conf = float(max(0.0, min(1.0, float(conf) * m)))
            base[(sym, int(h))] = (float(z), float(conf), explain)

    out: Dict[Tuple[str, int], Tuple[float, float, Dict]] = dict(base)

    for h in horizons:
        out = _maybe_apply_lgbm_ranker_batch(
            out,
            symbols=list(symbols or []),
            horizon_s=int(h),
            top_k=int(top_k),
            event=event,
        )

    for h in horizons:
        zmap = {sym: out[(sym, int(h))][0] for sym in symbols if (sym, int(h)) in out}
        cmap = {sym: out[(sym, int(h))][1] for sym in symbols if (sym, int(h)) in out}

        for target in symbols:
            betas = get_spillover_betas(target, int(h))
            if not betas:
                continue

            adj = 0.0
            used = 0
            contribs = []

            for driver, beta, n in betas:
                if driver not in zmap or int(n) < MIN_BETA_N:
                    continue
                contrib = float(beta) * float(zmap[driver]) * float(min(1.0, cmap[driver]))
                adj += contrib
                used += 1
                contribs.append({
                    "driver": driver,
                    "beta": float(beta),
                    "z_driver": float(zmap[driver]),
                    "conf_driver": float(cmap[driver]),
                    "contrib": float(contrib),
                })

            if used <= 0:
                continue

            z0, c0, ex0 = out[(target, int(h))]
            ex0["spillover"] = {
                "enabled": True,
                "used": int(used),
                "adj": float(adj),
                "contributions": contribs,
            }
            out[(target, int(h))] = (float(z0 + adj), float(c0), ex0)

    for h in horizons:
        out = _maybe_apply_feature_neutralization(
            out,
            symbols=list(symbols or []),
            horizon_s=int(h),
        )

    # ---- confidence collapse detection ----
    try:
        confs = [float(v[1]) for v in out.values()]
        if _confidence_collapse(confs):
            _warn_nonfatal(
                "predictor_confidence_collapse_detected",
                "PREDICTOR_CONFIDENCE_COLLAPSE_DETECTED",
                RuntimeError("confidence_collapse_detected"),
                warn_key="predictor_confidence_collapse_detected",
                symbol_count=int(len(symbols or [])),
                horizon_count=int(len(horizons or [])),
            )
            for k in list(out.keys()):
                expected_z, conf, explain = out[k]
                out[k] = (
                    float(expected_z),
                    min(1.0, max(0.0, float(conf))),
                    explain,
                )
                if isinstance(explain, dict):
                    explain["fallback"] = "preserve_expected_z"
    except Exception as e:
        _warn_nonfatal(
            "predictor_confidence_collapse_guard_failed",
            "PREDICTOR_CONFIDENCE_COLLAPSE_GUARD_FAILED",
            e,
            warn_key="predictor_confidence_collapse_guard_failed",
            symbol_count=int(len(symbols or [])),
            horizon_count=int(len(horizons or [])),
        )

    signal_ts_ms = _safe_int((event or {}).get("ts_ms"), int(time.time() * 1000))
    con_conf = None
    try:
        con_conf = connect()
        for sym, h in list(out.keys()):
            z0, conf0, explain0 = out[(sym, int(h))]
            calibrated_conf, calib_meta = calibrate_confidence_score(
                symbol=str(sym),
                horizon_s=int(h),
                confidence_raw=float(conf0),
                con=con_conf,
            )
            payload = describe_signal_confidence(
                expected_z=float(z0),
                confidence=float(calibrated_conf),
                raw_confidence=float(conf0),
                horizon_s=int(h),
                calibration=calib_meta,
                signal_ts_ms=int(signal_ts_ms),
            )
            explain0 = apply_confidence_payload(explain0, payload)
            final_conf, explain0, _conformal = apply_conformal_to_explain(
                con=con_conf,
                symbol=str(sym),
                horizon_s=int(h),
                prediction=float(z0),
                confidence=float(payload["confidence"]),
                explain=explain0,
                signal_ts_ms=int(signal_ts_ms),
            )
            out[(sym, int(h))] = (float(z0), float(final_conf), explain0)
    except Exception as e:
        _warn_nonfatal(
            "predictor_confidence_payload_batch_failed",
            "PREDICTOR_CONFIDENCE_PAYLOAD_BATCH_FAILED",
            e,
            warn_key="predictor_confidence_payload_batch_failed",
            symbol_count=int(len(symbols or [])),
            horizon_count=int(len(horizons or [])),
        )
    finally:
        try:
            if con_conf is not None:
                con_conf.close()
        except Exception as e:
            _warn_nonfatal(
                "predictor_confidence_connection_close_failed",
                "PREDICTOR_CONFIDENCE_CONNECTION_CLOSE_FAILED",
                e,
                warn_key="predictor_confidence_connection_close_failed",
            )

    _clear_feature_snapshot_prefetch()
    return out


def expected_impact(
    query_vec: np.ndarray,
    symbol: str,
    horizon_s: int,
    top_k: int = 8,
    as_of_ts_ms: Optional[int] = None,
):
    knn_z, wsum, _ = _knn_raw(query_vec, symbol, horizon_s, top_k, as_of_ts_ms=as_of_ts_ms)
    z, conf, _ = _blend_with_priors(symbol, horizon_s, knn_z, wsum)
    return float(z), float(conf)


def predict_forced_model(
    query_vec: np.ndarray,
    *,
    symbol: str,
    horizon_s: int,
    model_name: str,
    top_k: int = 8,
    event: Optional[Dict] = None,
) -> Tuple[float, float, Dict]:
    return _predict_single_model(
        query_vec,
        str(symbol),
        int(horizon_s),
        top_k=int(top_k),
        event=event,
        forced_model_name=str(model_name),
    )


def predict_live_symbol(
    symbol: str,
    *,
    model_name: Optional[str] = None,
    version: Optional[str] = None,
    horizon_s: Optional[int] = None,
    timeout_s: Optional[float] = None,
    persist: Optional[bool] = None,
) -> Dict[str, Any]:
    from engine.inference_engine import predict as realtime_predict

    return realtime_predict(
        str(symbol),
        model_name=model_name,
        version=version,
        horizon_s=horizon_s,
        timeout_s=timeout_s,
        persist=persist,
    )


def batch_predict_live_symbols(
    symbols: List[str],
    *,
    model_name: Optional[str] = None,
    version: Optional[str] = None,
    horizon_s: Optional[int] = None,
    timeout_s: Optional[float] = None,
    persist: Optional[bool] = None,
) -> Dict[str, Dict[str, Any]]:
    from engine.inference_engine import batch_predict as realtime_batch_predict

    return realtime_batch_predict(
        list(symbols or []),
        model_name=model_name,
        version=version,
        horizon_s=horizon_s,
        timeout_s=timeout_s,
        persist=persist,
    )


def realtime_inference_enabled() -> bool:
    return bool(_REALTIME_INFERENCE_ENABLED)


def _legacy_realtime_fallback_allowed() -> bool:
    raw = str(_REALTIME_INFERENCE_LEGACY_FALLBACK or "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    return False


def _safe_payload_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception as e:
        _warn_nonfatal(
            "predictor_safe_payload_float_failed",
            "PREDICTOR_SAFE_PAYLOAD_FLOAT_FAILED",
            e,
            warn_key="safe_payload_float",
            value=repr(value)[:120],
        )
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _explain_from_realtime_payload(
    symbol: str,
    horizon_s: int,
    payload: Dict[str, Any],
    *,
    event: Optional[Dict] = None,
) -> Dict[str, Any]:
    model_name = str(payload.get("model_name") or "safe_default").strip() or "safe_default"
    feature_snapshot = {
        "symbol": str(symbol),
        "ts_ms": int(payload.get("feature_ts_ms") or 0),
        "feature_set_tag": str(payload.get("feature_set_tag") or ""),
        "feature_ids": list(payload.get("feature_ids") or []),
        "feature_coverage": float(_safe_payload_float(payload.get("feature_coverage"), 0.0)),
    }
    explain = {
        "model": str(model_name),
        "model_name": str(model_name),
        "model_id": str(payload.get("model_id") or model_name),
        "model_version": str(payload.get("model_version") or ""),
        "model_kind": payload.get("model_kind"),
        "feature_ids": list(payload.get("feature_ids") or []),
        "feature_set_tag": str(payload.get("feature_set_tag") or ""),
        "feature_snapshot": feature_snapshot,
        "feature_coverage": float(_safe_payload_float(payload.get("feature_coverage"), 0.0)),
        "prediction_source": "realtime_inference_engine",
        "status": str(payload.get("status") or ""),
        "safe_output": bool(payload.get("safe_output")),
        "timed_out": bool(payload.get("timed_out")),
        "fallback_reason": payload.get("fallback_reason"),
        "horizon_s": int(horizon_s),
    }
    ensemble_output = payload.get("ensemble_output")
    if isinstance(ensemble_output, dict):
        explain["ensemble_output"] = dict(ensemble_output)
    ensemble_members = payload.get("ensemble_members")
    if isinstance(ensemble_members, list):
        explain["ensemble_members"] = [
            dict(member) if isinstance(member, dict) else member
            for member in ensemble_members
        ]
    if event is not None:
        explain["event_id"] = int(_safe_int((event or {}).get("event_id"), 0))
    return explain


def predict_runtime_event(
    query_vec: np.ndarray,
    symbols: List[str],
    horizons: List[int],
    top_k: int = 8,
    event: Optional[Dict] = None,
    *,
    prefer_realtime: Optional[bool] = None,
    timeout_s: Optional[float] = None,
) -> Dict[Tuple[str, int], Tuple[float, float, Dict]]:
    use_realtime = realtime_inference_enabled() if prefer_realtime is None else bool(prefer_realtime)
    if not use_realtime:
        return predict_event(query_vec, symbols, horizons, top_k=top_k, event=event)

    realtime_timeout_s = float(timeout_s if timeout_s is not None else _REALTIME_INFERENCE_TIMEOUT_S)
    out: Dict[Tuple[str, int], Tuple[float, float, Dict]] = {}
    try:
        for horizon in list(horizons or []):
            horizon_s = int(horizon)
            batch = batch_predict_live_symbols(
                list(symbols or []),
                horizon_s=int(horizon_s),
                timeout_s=float(realtime_timeout_s),
                persist=False,
            )
            for sym in list(symbols or []):
                symbol_key = str(sym)
                lookup_key = str(sym).upper().strip()
                payload = dict(batch.get(lookup_key) or batch.get(symbol_key) or {})
                if not payload:
                    _warn_nonfatal(
                        "predictor_realtime_payload_missing",
                        "PREDICTOR_REALTIME_PAYLOAD_MISSING",
                        KeyError(f"missing_realtime_payload:{lookup_key}:{horizon_s}"),
                        warn_key=f"predictor_realtime_payload_missing:{lookup_key}:{horizon_s}",
                        symbol=str(lookup_key or symbol_key),
                        horizon_s=int(horizon_s),
                    )
                    payload = {
                        "symbol": str(lookup_key or symbol_key),
                        "prediction": 0.0,
                        "confidence": 0.0,
                        "model_name": "safe_default",
                        "model_id": "safe_default",
                        "model_version": "",
                        "model_kind": None,
                        "feature_ts_ms": 0,
                        "feature_set_tag": "",
                        "feature_ids": [],
                        "feature_coverage": 0.0,
                        "status": "safe_default",
                        "safe_output": True,
                        "timed_out": False,
                        "fallback_reason": "missing_realtime_payload",
                    }
                explain = _explain_from_realtime_payload(lookup_key or symbol_key, int(horizon_s), payload, event=event)
                out[(str(symbol_key), int(horizon_s))] = (
                    float(_safe_payload_float(payload.get("prediction"), 0.0)),
                    float(max(0.0, min(1.0, _safe_payload_float(payload.get("confidence"), 0.0)))),
                    explain,
                )
        return out
    except Exception as e:
        _warn_nonfatal(
            "predictor_realtime_runtime_failed",
            "PREDICTOR_REALTIME_RUNTIME_FAILED",
            e,
            warn_key="predictor_realtime_runtime_failed",
            symbol_count=int(len(symbols or [])),
            horizon_count=int(len(horizons or [])),
            legacy_fallback_allowed=bool(_legacy_realtime_fallback_allowed()),
        )
        if _legacy_realtime_fallback_allowed():
            return predict_event(query_vec, symbols, horizons, top_k=top_k, event=event)

        safe: Dict[Tuple[str, int], Tuple[float, float, Dict]] = {}
        for horizon in list(horizons or []):
            horizon_s = int(horizon)
            for sym in list(symbols or []):
                symbol_key = str(sym).upper().strip() or str(sym)
                explain = {
                    "model": "safe_default",
                    "model_name": "safe_default",
                    "model_id": "safe_default",
                    "model_version": "",
                    "model_kind": None,
                    "feature_ids": [],
                    "feature_set_tag": "",
                    "feature_snapshot": {
                        "symbol": str(symbol_key),
                        "ts_ms": 0,
                        "feature_set_tag": "",
                        "feature_ids": [],
                        "feature_coverage": 0.0,
                    },
                    "feature_coverage": 0.0,
                    "prediction_source": "realtime_inference_safe_fallback",
                    "status": "safe_default",
                    "safe_output": True,
                    "timed_out": False,
                    "fallback_reason": "realtime_inference_failed",
                    "horizon_s": int(horizon_s),
                }
                if event is not None:
                    explain["event_id"] = int(_safe_int((event or {}).get("event_id"), 0))
                safe[(str(sym), int(horizon_s))] = (0.0, 0.0, explain)
        return safe
