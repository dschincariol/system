"""Bounded alpha-discovery loop that feeds the normal model-governance path."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from typing import Any, Dict, Iterable, List, Optional

import numpy as np

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.runtime_meta import meta_set
from engine.runtime.storage import (
    connect,
    fetch_recent_alpha_candidates,
    record_alpha_candidate,
    record_alpha_lifecycle,
    update_alpha_candidate,
)
from engine.strategy.feature_registry import (
    BASE_FEATURE_IDS,
    FEATURE_GROUPS,
    build_feature_snapshot,
    default_feature_ids,
    feature_set_tag_from_ids,
    registered_feature_ids,
    resolve_feature_ids,
)
from engine.strategy.gbm_regressor import load_gbm_model, persist_gbm_model_record, train_gbm_model
from engine.strategy.learning_loop import build_dataset_snapshot
from engine.strategy.model_config import build_model_registration_metadata, load_model_configs
from engine.strategy.model_lifecycle import (
    record_version_performance,
    register_model_version,
    update_model_version_status,
)
from engine.strategy.model_marketplace import refresh_replay_validation_snapshot, upsert_marketplace_candidate
from engine.strategy.promotion_guard import evaluate_statistical_promotion_gate
from engine.model_registry import register_model


LOG = get_logger("research.alpha_generator")
JOB_NAME = "alpha_discovery_loop"
_WARNED_NONFATAL_KEYS: set[str] = set()
_SUPPORTED_FAMILIES = {"gbm_regressor"}


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="alpha_generator_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.research.alpha_generator",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


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
    return float(out) if np.isfinite(out) else float(default)


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)


def alpha_discovery_config_from_env() -> Dict[str, Any]:
    """Build alpha-discovery configuration from the current environment."""
    allowed_raw = str(os.environ.get("ALPHA_DISCOVERY_ALLOWED_FAMILIES", "gbm_regressor") or "")
    allowed: List[str] = []
    seen = set()
    for item in allowed_raw.split(","):
        family = str(item or "").strip().lower()
        if not family or family in seen:
            continue
        seen.add(family)
        allowed.append(family)
    if not allowed:
        allowed = ["gbm_regressor"]
    return {
        "enabled": _safe_bool(os.environ.get("ALPHA_DISCOVERY_ENABLED", "0"), False),
        "max_candidates": max(0, _safe_int(os.environ.get("ALPHA_DISCOVERY_MAX_CANDIDATES", "4"), 4)),
        "allowed_families": allowed,
        "shadow_only": _safe_bool(os.environ.get("ALPHA_DISCOVERY_SHADOW_ONLY", "1"), True),
        "require_cpcv": _safe_bool(os.environ.get("ALPHA_DISCOVERY_REQUIRE_CPCV", "1"), True),
        "require_stat_gate": _safe_bool(os.environ.get("ALPHA_DISCOVERY_REQUIRE_STAT_GATE", "1"), True),
        "min_samples": max(4, _safe_int(os.environ.get("GBM_MIN_SAMPLES", "50"), 50)),
        "group_feature_limit": 4,
    }


def _family_priority(allowed_families: Iterable[str]) -> str:
    allowed = [str(item or "").strip().lower() for item in list(allowed_families or []) if str(item or "").strip()]
    for family in ("gbm_regressor",):
        if family in allowed and family in _SUPPORTED_FAMILIES:
            return family
    return ""


def _base_training_config(model_family: str) -> Dict[str, Any]:
    configs = list(load_model_configs(family=str(model_family), include_disabled=True) or [])
    configs.sort(
        key=lambda cfg: (
            0 if bool(cfg.get("enabled")) else 1,
            0 if not bool(cfg.get("experimental")) else 1,
            str(cfg.get("model_name") or ""),
        )
    )
    if configs:
        return dict(configs[0] or {})
    horizon_s = max(1, _safe_int(os.environ.get("MODEL_HORIZON_MEDIUM_S", "3600"), 3600))
    return {
        "model_name": str(model_family),
        "family": str(model_family),
        "feature_ids": list(default_feature_ids()),
        "symbol_universe": ["*"],
        "horizon_s": int(horizon_s),
        "horizons_s": [int(horizon_s)],
        "training_window_days": max(1, _safe_int(os.environ.get("GBM_LOOKBACK_DAYS", "365"), 365)),
        "risk_profile": "balanced",
        "model_kind": "lightgbm",
        "hyperparams": {},
        "enabled": False,
    }


def _dedupe_feature_ids(feature_ids: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    allowed = set(registered_feature_ids())
    for feature_id in list(feature_ids or []):
        key = str(feature_id or "").strip()
        if not key or key in seen or key not in allowed:
            continue
        seen.add(key)
        out.append(key)
    return out


def _ordered_group_features(group_name: str, features: Iterable[str], preferred_ids: Iterable[str]) -> List[str]:
    group_set = set(list(features or []))
    preferred = [fid for fid in list(preferred_ids or []) if fid in group_set]
    extras = [fid for fid in list(features or []) if fid not in set(preferred)]
    return _dedupe_feature_ids(preferred + extras)


def _candidate_name(model_family: str, index: int, feature_ids: Iterable[str]) -> str:
    digest = hashlib.sha1("|".join(list(feature_ids or [])).encode("utf-8")).hexdigest()[:10]
    family = str(model_family or "model").strip().lower().replace(".", "_")
    return f"alpha_{family}_{int(index):02d}_{digest}"


def _symbolic_candidate_name(model_family: str, feature_id: str) -> str:
    digest_source = str(feature_id or "").strip() or str(model_family or "model")
    digest = hashlib.sha1(digest_source.encode("utf-8")).hexdigest()[:10]
    family = str(model_family or "model").strip().lower().replace(".", "_")
    return f"alpha_{family}_sym_{digest}"


def _load_symbolic_alpha_support() -> Optional[Dict[str, Any]]:
    try:
        from engine.research.symbolic_alpha_generator import (
            generate_symbolic_alpha_candidates,
            symbolic_alpha_enabled,
            symbolic_alpha_require_shadow_only,
        )

        return {
            "generate_symbolic_alpha_candidates": generate_symbolic_alpha_candidates,
            "symbolic_alpha_enabled": symbolic_alpha_enabled,
            "symbolic_alpha_require_shadow_only": symbolic_alpha_require_shadow_only,
        }
    except Exception as e:
        _warn_nonfatal(
            "ALPHA_DISCOVERY_SYMBOLIC_IMPORT_FAILED",
            e,
            once_key="alpha_discovery_symbolic_import",
        )
        return None


def _generate_classic_candidate_specs(
    *,
    base_config: Optional[Dict[str, Any]] = None,
    max_candidates: Optional[int] = None,
) -> List[Dict[str, Any]]:
    base_cfg = dict(base_config or {})
    max_items = max(0, _safe_int(max_candidates, 0))
    if max_items <= 0:
        return []

    preferred_ids = list(resolve_feature_ids(base_cfg.get("feature_ids") or default_feature_ids()))
    registered = set(registered_feature_ids())
    core_features = [feature_id for feature_id in list(BASE_FEATURE_IDS) if feature_id in registered]
    group_limit = 4
    definitions: List[Dict[str, Any]] = []

    preferred_extras = [feature_id for feature_id in preferred_ids if feature_id not in core_features][:group_limit]
    if preferred_extras:
        definitions.append(
            {
                "generation_method": "preferred_seed_v1",
                "group_names": ["base", "preferred"],
                "feature_ids": _dedupe_feature_ids(core_features + preferred_extras),
            }
        )

    ordered_groups = []
    for group_name in sorted(FEATURE_GROUPS.keys()):
        if str(group_name) == "base":
            continue
        group_features = _ordered_group_features(group_name, FEATURE_GROUPS.get(group_name) or [], preferred_ids)[:group_limit]
        if not group_features:
            continue
        ordered_groups.append((str(group_name), list(group_features)))
        definitions.append(
            {
                "generation_method": "single_group_v1",
                "group_names": ["base", str(group_name)],
                "feature_ids": _dedupe_feature_ids(core_features + list(group_features)),
            }
        )

    for left, right in zip(ordered_groups, ordered_groups[1:]):
        left_name, left_features = left
        right_name, right_features = right
        definitions.append(
            {
                "generation_method": "paired_group_v1",
                "group_names": ["base", str(left_name), str(right_name)],
                "feature_ids": _dedupe_feature_ids(core_features + list(left_features) + list(right_features)),
            }
        )

    out: List[Dict[str, Any]] = []
    seen_signatures = set()
    model_family = str(base_cfg.get("family") or "gbm_regressor").strip().lower() or "gbm_regressor"
    for idx, definition in enumerate(definitions, start=1):
        feature_ids = _dedupe_feature_ids(definition.get("feature_ids") or [])
        if len(feature_ids) <= len(core_features):
            continue
        signature = tuple(feature_ids)
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        out.append(
            {
                "candidate_name": _candidate_name(model_family, idx, feature_ids),
                "model_family": model_family,
                "generation_method": str(definition.get("generation_method") or "group_subset_v1"),
                "group_names": list(definition.get("group_names") or []),
                "feature_ids": list(feature_ids),
                "feature_set_tag": str(feature_set_tag_from_ids(feature_ids)),
            }
        )
        if len(out) >= int(max_items):
            break
    return out


def _generate_symbolic_candidate_specs(
    *,
    base_config: Optional[Dict[str, Any]] = None,
    max_candidates: Optional[int] = None,
) -> List[Dict[str, Any]]:
    base_cfg = dict(base_config or {})
    max_items = max(0, _safe_int(max_candidates, 0))
    if max_items <= 0:
        return []

    symbolic_support = _load_symbolic_alpha_support()
    if not symbolic_support or not bool(symbolic_support["symbolic_alpha_enabled"]()):
        return []

    model_family = str(base_cfg.get("family") or "gbm_regressor").strip().lower() or "gbm_regressor"
    registered = set(registered_feature_ids())
    core_features = [feature_id for feature_id in list(BASE_FEATURE_IDS) if feature_id in registered]
    try:
        symbolic_rows = list(
            symbolic_support["generate_symbolic_alpha_candidates"](
                dict(base_cfg),
                max_expressions=int(max_items),
            )
            or []
        )
    except Exception as e:
        _warn_nonfatal(
            "ALPHA_DISCOVERY_SYMBOLIC_GENERATION_FAILED",
            e,
            once_key="alpha_discovery_symbolic_generation_failed",
            model_family=str(model_family),
        )
        return []

    out: List[Dict[str, Any]] = []
    seen_feature_ids = set()
    require_shadow_only = bool(symbolic_support["symbolic_alpha_require_shadow_only"]())
    for row in symbolic_rows:
        feature_id = str(dict(row or {}).get("feature_id") or "").strip()
        if not feature_id or feature_id in seen_feature_ids:
            continue
        seen_feature_ids.add(feature_id)
        feature_ids = list(core_features) + [str(feature_id)]
        symbolic_meta = {
            "candidate_id": int(dict(row or {}).get("id") or 0),
            "feature_id": str(feature_id),
            "expression_text": str(dict(row or {}).get("expression_text") or ""),
            "source_feature_ids": [
                str(fid)
                for fid in list(dict(row or {}).get("source_feature_ids") or [])
                if str(fid).strip()
            ],
            "complexity": int(dict(row or {}).get("complexity") or 0),
            "score": _safe_float(dict(row or {}).get("score"), 0.0),
            "status": str(dict(row or {}).get("status") or "accepted"),
            "shadow_only": bool(require_shadow_only),
        }
        out.append(
            {
                "candidate_name": _symbolic_candidate_name(model_family, str(feature_id)),
                "model_family": model_family,
                "generation_method": "symbolic_expression_v1",
                "group_names": ["base", "symbolic"],
                "feature_ids": list(feature_ids),
                "feature_set_tag": str(feature_set_tag_from_ids(feature_ids)),
                "symbolic_candidate": dict(symbolic_meta),
            }
        )
        if len(out) >= int(max_items):
            break
    return out


def generate_candidate_specs(
    *,
    base_config: Optional[Dict[str, Any]] = None,
    max_candidates: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Generate bounded candidate model specifications for discovery runs."""
    base_cfg = dict(base_config or {})
    max_items = max(0, _safe_int(max_candidates, 0))
    if max_items <= 0:
        return []

    out: List[Dict[str, Any]] = []
    seen_signatures = set()
    candidate_batches = [
        _generate_symbolic_candidate_specs(base_config=dict(base_cfg), max_candidates=int(max_items)),
        _generate_classic_candidate_specs(base_config=dict(base_cfg), max_candidates=int(max_items)),
    ]
    for batch in candidate_batches:
        for spec in list(batch or []):
            symbolic_candidate = dict(spec.get("symbolic_candidate") or {})
            signature = (
                str(spec.get("generation_method") or ""),
                str(symbolic_candidate.get("feature_id") or ""),
                tuple(list(spec.get("feature_ids") or [])),
            )
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            out.append(dict(spec))
            if len(out) >= int(max_items):
                return out
    return out


def _runtime_symbols(train_cfg: Dict[str, Any]) -> List[str]:
    out = [str(item).upper().strip() for item in list(train_cfg.get("symbol_universe") or []) if str(item).strip()]
    return out or ["*"]


def _load_labeled_feature_rows(
    *,
    cutoff_ms: int,
    symbol_filter: set[str],
    horizon_s: int,
    feature_ids: List[str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    con = connect(readonly=True)
    try:
        query = """
            SELECT
              l.event_id,
              l.symbol,
              l.horizon_s,
              COALESCE(le.net_z, l.impact_z) AS impact_z,
              e.ts_ms,
              e.title,
              e.body,
              e.source
            FROM labels l
            JOIN events e ON e.id = l.event_id
            LEFT JOIN labels_exec le
              ON le.event_id = l.event_id
             AND le.symbol = l.symbol
             AND le.horizon_s = l.horizon_s
             AND le.realized = 1
            WHERE e.ts_ms >= ?
              AND COALESCE(le.net_z, l.impact_z) IS NOT NULL
            ORDER BY e.ts_ms ASC, l.event_id ASC, l.symbol ASC
        """
        fallback_query = """
            SELECT
              l.event_id,
              l.symbol,
              l.horizon_s,
              l.impact_z AS impact_z,
              e.ts_ms,
              e.title,
              e.body,
              e.source
            FROM labels l
            JOIN events e ON e.id = l.event_id
            WHERE e.ts_ms >= ?
              AND l.impact_z IS NOT NULL
            ORDER BY e.ts_ms ASC, l.event_id ASC, l.symbol ASC
        """
        try:
            source_rows = con.execute(query, (int(cutoff_ms),)).fetchall() or []
        except Exception:
            source_rows = con.execute(fallback_query, (int(cutoff_ms),)).fetchall() or []

        for event_id, symbol, row_horizon_s, impact_z, ts_ms, title, body, source in source_rows:
            symbol_u = str(symbol or "").upper().strip()
            if not symbol_u:
                continue
            if symbol_filter and symbol_u not in symbol_filter:
                continue
            if int(row_horizon_s or 0) != int(horizon_s):
                continue

            event = {
                "id": int(event_id or 0),
                "ts_ms": int(ts_ms or 0),
                "title": str(title or ""),
                "body": str(body or ""),
                "source": str(source or ""),
            }
            try:
                snapshot = build_feature_snapshot(event=event, symbol=str(symbol_u), feature_ids=list(feature_ids))
            except Exception as exc:
                _warn_nonfatal(
                    "ALPHA_DISCOVERY_FEATURE_SNAPSHOT_FAILED",
                    exc,
                    once_key=f"alpha_discovery_feature_snapshot:{symbol_u}:{int(horizon_s)}",
                    symbol=str(symbol_u),
                    horizon_s=int(horizon_s),
                )
                continue

            rows.append(
                {
                    "event_id": int(event_id or 0),
                    "symbol": str(symbol_u),
                    "horizon_s": int(horizon_s),
                    "ts_ms": int(ts_ms or 0),
                    "label": _safe_float(impact_z, 0.0),
                    "feature_snapshot": dict(snapshot or {}),
                    "values": np.asarray(
                        [_safe_float(dict(snapshot or {}).get(feature_id), 0.0) for feature_id in list(feature_ids)],
                        dtype=np.float32,
                    ),
                }
            )
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("ALPHA_DISCOVERY_LOAD_ROWS_CLOSE_FAILED", e)
    return rows


def _spearman_like(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    if int(len(y_true)) < 2 or int(len(y_pred)) < 2:
        return 0.0
    try:
        rt = np.asarray(y_true, dtype=float).argsort().argsort()
        rp = np.asarray(y_pred, dtype=float).argsort().argsort()
        out = float(np.corrcoef(rt, rp)[0, 1])
        return float(out) if np.isfinite(out) else 0.0
    except Exception:
        return 0.0


def _prediction_confidence(prediction: float) -> float:
    magnitude = abs(_safe_float(prediction, 0.0))
    return float(max(0.05, min(0.99, magnitude / (magnitude + 1.0))))


def _numeric_metrics_only(metrics: Optional[Dict[str, Any]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for key, value in dict(metrics or {}).items():
        if isinstance(value, bool):
            out[str(key)] = 1.0 if bool(value) else 0.0
            continue
        if isinstance(value, (int, float, np.integer, np.floating)):
            number = float(value)
            if np.isfinite(number):
                out[str(key)] = float(number)
    return out


def _train_candidate_bundle(
    *,
    candidate_spec: Dict[str, Any],
    train_cfg: Dict[str, Any],
    loop_cfg: Dict[str, Any],
    rows: List[Dict[str, Any]],
    created_ts: Optional[int] = None,
    candidate_version: Optional[str] = None,
) -> Dict[str, Any]:
    feature_ids = list(candidate_spec.get("feature_ids") or [])
    train_rows = list(rows or [])
    min_samples = max(4, int(loop_cfg.get("min_samples") or 4))
    if len(train_rows) < min_samples:
        return {
            "ok": False,
            "status": "insufficient_samples",
            "reason": "insufficient_samples",
            "n_rows": int(len(train_rows)),
            "min_samples": int(min_samples),
        }

    split_idx = min(max(1, int(len(train_rows) * 0.8)), int(len(train_rows) - 1))
    if split_idx <= 0 or split_idx >= len(train_rows):
        return {
            "ok": False,
            "status": "invalid_split",
            "reason": "invalid_split",
            "n_rows": int(len(train_rows)),
            "split_idx": int(split_idx),
        }

    training_rows = list(train_rows[:split_idx])
    eval_rows = list(train_rows[split_idx:])
    Xtr = np.stack([row["values"] for row in training_rows]).astype(np.float32, copy=False)
    ytr = np.asarray([row["label"] for row in training_rows], dtype=np.float32)
    Xev = np.stack([row["values"] for row in eval_rows]).astype(np.float32, copy=False)
    yev = np.asarray([row["label"] for row in eval_rows], dtype=np.float32)

    blob = train_gbm_model(Xtr, ytr, feature_ids=list(feature_ids), hyperparams=dict(train_cfg.get("hyperparams") or {}))
    model, _schema = load_gbm_model(blob)
    predictions = np.asarray(model.predict(Xev), dtype=np.float32).reshape(-1)

    rmse = float(np.sqrt(np.mean((yev - predictions) ** 2))) if int(len(yev)) > 0 else 0.0
    directional_acc = float(np.mean((predictions >= 0.0) == (yev >= 0.0))) if int(len(yev)) > 0 else 0.0
    spearman = _spearman_like(yev, predictions)
    returns = [float((1.0 if float(pred) >= 0.0 else -1.0) * float(realized)) for pred, realized in zip(predictions.tolist(), yev.tolist())]
    created_ts_ms = int(created_ts if created_ts is not None else _now_ms())
    resolved_candidate_version = str(candidate_version or str(int(created_ts_ms)))
    feature_schema = {
        "feature_ids": list(feature_ids),
        "feature_set_tag": str(feature_set_tag_from_ids(feature_ids)),
        "feature_count": int(len(feature_ids)),
        "ts_ms": int(created_ts_ms),
    }
    evaluation = {
        "rows": list(eval_rows),
        "predictions": [float(item) for item in predictions.tolist()],
        "returns": list(returns),
        "mean_confidence": float(np.mean([_prediction_confidence(item) for item in predictions.tolist()]) if len(predictions) else 0.0),
        "metrics": {
            "rmse": float(rmse),
            "spearman": float(spearman),
            "directional_acc": float(directional_acc),
            "n_eval": int(len(eval_rows)),
            "signed_alpha": float(sum(returns)),
        },
    }
    training_metrics = {
        "model_name": str(candidate_spec.get("candidate_name") or ""),
        "model_kind": "lightgbm",
        "n_train": int(len(training_rows)),
        "n_eval": int(len(eval_rows)),
        "rmse": float(rmse),
        "spearman": float(spearman),
        "directional_acc": float(directional_acc),
        "quality_score": float(max(0.0, min(1.0, directional_acc))),
        "feature_ids": list(feature_ids),
        "feature_set_tag": str(feature_schema.get("feature_set_tag") or ""),
        "feature_schema": dict(feature_schema),
        "model_version": str(resolved_candidate_version),
        "model_family": str(candidate_spec.get("model_family") or "gbm_regressor"),
        "signed_alpha": float(sum(returns)),
    }
    return {
        "ok": True,
        "status": "trained",
        "candidate_version": str(resolved_candidate_version),
        "created_ts": int(created_ts_ms),
        "model_kind": "lightgbm",
        "blob": bytes(blob or b""),
        "feature_schema": dict(feature_schema),
        "training_metrics": dict(training_metrics),
        "evaluation": dict(evaluation),
    }


def _write_shadow_validation_rows(
    *,
    candidate_id: int,
    candidate_name: str,
    candidate_version: str,
    model_kind: str,
    generation_method: str,
    eval_rows: List[Dict[str, Any]],
    predictions: List[float],
) -> int:
    rows = list(zip(list(eval_rows or []), list(predictions or [])))
    if not rows:
        return 0
    con = connect(readonly=False)
    try:
        inserted = 0
        ts_ms = _now_ms()
        for row, prediction in rows:
            con.execute(
                """
                INSERT INTO shadow_predictions(
                  ts_ms, event_id, symbol, regime, horizon_s,
                  model_name, model_kind, model_ts_ms,
                  predicted_z, confidence, cost_est, net_pred_z, extra_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(ts_ms),
                    int(row.get("event_id") or 0),
                    str(row.get("symbol") or "").upper().strip(),
                    "global",
                    int(row.get("horizon_s") or 0),
                    str(candidate_name),
                    str(model_kind or ""),
                    int(candidate_version),
                    float(prediction),
                    float(_prediction_confidence(prediction)),
                    None,
                    float(prediction),
                    _json_dumps(
                        {
                            "alpha_candidate_id": int(candidate_id),
                            "candidate_version": str(candidate_version),
                            "generation_method": str(generation_method or ""),
                            "source": JOB_NAME,
                        }
                    ),
                ),
            )
            inserted += 1
        con.commit()
        return int(inserted)
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("ALPHA_DISCOVERY_SHADOW_WRITE_CLOSE_FAILED", e)


def _replay_rows_for_candidate(
    *,
    replay_snapshot: Dict[str, Any],
    candidate_name: str,
    candidate_version: str,
    runtime_symbols: List[str],
) -> List[Dict[str, Any]]:
    snapshot = dict(replay_snapshot.get("snapshot") or replay_snapshot)
    model_rows = [dict(value) for value in dict(snapshot.get("models") or {}).values() if isinstance(value, dict)]
    version_int = _safe_int(candidate_version, 0)
    matches = [
        row
        for row in model_rows
        if str(row.get("model_name") or "") == str(candidate_name)
        and (version_int <= 0 or int(row.get("model_ts_ms") or 0) == int(version_int))
    ]
    if not matches:
        return []
    specific = [row for row in matches if str(row.get("symbol") or "") != "*"]
    if specific:
        specific.sort(key=lambda row: (int(row.get("n") or 0), str(row.get("symbol") or "")), reverse=True)
        return specific
    if len(runtime_symbols) == 1 and str(runtime_symbols[0]) != "*":
        synthesized = dict(matches[0] or {})
        synthesized["symbol"] = str(runtime_symbols[0])
        return [synthesized]
    return list(matches)


def _validation_gate_config(loop_cfg: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "use_stat_gate": bool(loop_cfg.get("require_stat_gate")),
        "cpcv": {"enabled": bool(loop_cfg.get("require_cpcv"))},
    }


def _candidate_requires_shadow_only(candidate_spec: Dict[str, Any], loop_cfg: Dict[str, Any]) -> bool:
    symbolic_candidate = dict(candidate_spec.get("symbolic_candidate") or {})
    if symbolic_candidate:
        return bool(symbolic_candidate.get("shadow_only", loop_cfg.get("shadow_only")))
    return bool(loop_cfg.get("shadow_only"))


def _candidate_mutation_kind(candidate_spec: Dict[str, Any]) -> str:
    return "symbolic_alpha_discovery" if dict(candidate_spec.get("symbolic_candidate") or {}) else "alpha_discovery"


def _reject_candidate(
    *,
    candidate_id: int,
    candidate_name: str,
    candidate_version: str,
    reason: str,
    diagnostics: Dict[str, Any],
    candidate_spec: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    merged_diagnostics = dict(diagnostics or {})
    symbolic_candidate = dict((candidate_spec or {}).get("symbolic_candidate") or {})
    if symbolic_candidate:
        merged_diagnostics["symbolic_candidate"] = dict(symbolic_candidate)
    update_alpha_candidate(
        candidate_id=int(candidate_id),
        status="rejected",
        diagnostics={"reason": str(reason), **dict(merged_diagnostics or {})},
    )
    if str(candidate_name or "").strip() and str(candidate_version or "").strip():
        update_model_version_status(
            str(candidate_name),
            str(candidate_version),
            stage="retired",
            status="rejected",
            live_ready=False,
            meta_patch={"rejection_reason": str(reason), "validation": dict(merged_diagnostics or {})},
        )
    record_alpha_lifecycle(
        candidate_id=int(candidate_id),
        stage="validation",
        outcome="rejected",
        metrics={"reason": str(reason)},
        notes=dict(merged_diagnostics or {}),
    )
    return {"ok": True, "status": "rejected", "reason": str(reason)}


def _register_survivor(
    *,
    candidate_id: int,
    candidate_spec: Dict[str, Any],
    train_cfg: Dict[str, Any],
    train_result: Dict[str, Any],
    replay_rows: List[Dict[str, Any]],
    validation_diagnostics: Dict[str, Any],
    loop_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    candidate_name = str(candidate_spec.get("candidate_name") or "")
    candidate_version = str(train_result.get("candidate_version") or "")
    model_kind = str(train_result.get("model_kind") or "lightgbm")
    symbolic_candidate = dict(candidate_spec.get("symbolic_candidate") or {})
    final_stage = "shadow" if _candidate_requires_shadow_only(candidate_spec, loop_cfg) else "challenger"
    feature_schema = dict(train_result.get("feature_schema") or {})
    training_metrics = dict(train_result.get("training_metrics") or {})
    approved_rows = [row for row in list(replay_rows or []) if bool(row.get("approved"))]
    registration_cfg = {
        **dict(train_cfg or {}),
        "model_name": str(candidate_name),
        "model_id": str(candidate_name),
        "instance_name": str(candidate_name),
        "feature_ids": list(candidate_spec.get("feature_ids") or []),
        "model_kind": str(model_kind),
    }
    registration_meta = build_model_registration_metadata(registration_cfg)
    update_model_version_status(
        str(candidate_name),
        str(candidate_version),
        stage=str(final_stage),
        status="validated",
        live_ready=False,
        meta_patch={
            "alpha_candidate_id": int(candidate_id),
            "feature_schema": dict(feature_schema),
            "validation": dict(validation_diagnostics or {}),
            "approved_rows": list(approved_rows),
            "symbolic_candidate": dict(symbolic_candidate or {}),
        },
    )
    register_model(
        model_name=str(candidate_name),
        model_kind=str(model_kind),
        model_ts_ms=int(candidate_version),
        stage=str(final_stage),
        metrics={
            **dict(registration_meta or {}),
            **dict(training_metrics or {}),
            "model_version": str(candidate_version),
            "model_family": str(candidate_spec.get("model_family") or ""),
            "alpha_candidate_id": int(candidate_id),
            "validation": dict(validation_diagnostics or {}),
            "feature_schema": dict(feature_schema),
            "symbolic_candidate": dict(symbolic_candidate or {}),
        },
        note=JOB_NAME,
        regime="global",
    )
    record_version_performance(
        model_name=str(candidate_name),
        model_version=str(candidate_version),
        metric_scope="shadow_validation",
        metrics={
            **_numeric_metrics_only(training_metrics),
            "validated_models": int(len(approved_rows)),
            "validation_passed": 1.0,
        },
        sample_n=int(dict(train_result.get("evaluation") or {}).get("metrics", {}).get("n_eval") or 0),
        meta={
            "job_name": JOB_NAME,
            "approved_symbols": [str(row.get("symbol") or "") for row in approved_rows],
            "feature_schema": dict(feature_schema),
            "model_kind": str(model_kind),
            "model_family": str(candidate_spec.get("model_family") or ""),
            "symbolic_candidate": dict(symbolic_candidate or {}),
        },
    )

    evaluation = dict(train_result.get("evaluation") or {})
    mean_confidence = _safe_float(evaluation.get("mean_confidence"), 0.0)
    for replay_row in approved_rows:
        n_obs = int(replay_row.get("n") or 0)
        dir_acc = _safe_float(replay_row.get("dir_acc"), 0.0)
        wins = int(round(float(dir_acc) * float(max(0, n_obs))))
        upsert_marketplace_candidate(
            model_name=str(candidate_name),
            symbol=str(replay_row.get("symbol") or "").upper().strip(),
            horizon_s=int(replay_row.get("horizon_s") or 0),
            regime=str(replay_row.get("regime") or "global"),
            stage=str(final_stage),
            score=float(dir_acc),
            trades=int(n_obs),
            wins=int(max(0, wins)),
            losses=int(max(0, n_obs - wins)),
            gross_pnl=_safe_float(replay_row.get("signed_alpha"), 0.0),
            net_pnl=_safe_float(replay_row.get("signed_alpha"), 0.0),
            avg_confidence=float(mean_confidence),
            last_signal_ts_ms=int(train_result.get("created_ts") or _now_ms()),
            meta={
                "alpha_candidate_id": int(candidate_id),
                "candidate_version": str(candidate_version),
                "model_id": str(candidate_name),
                "model_kind": str(model_kind),
                "model_ts_ms": int(candidate_version),
                "score_source": "shadow_predictions",
                "feature_ids": list(candidate_spec.get("feature_ids") or []),
                "feature_schema": dict(feature_schema),
                "generation_method": str(candidate_spec.get("generation_method") or ""),
                "validation": dict(validation_diagnostics or {}),
                "replay_validation": dict(replay_row or {}),
                "symbolic_candidate": dict(symbolic_candidate or {}),
            },
        )

    status = "registered_shadow" if str(final_stage) == "shadow" else "registered_challenger"
    update_alpha_candidate(
        candidate_id=int(candidate_id),
        status=str(status),
        diagnostics={
            "candidate_name": str(candidate_name),
            "candidate_version": str(candidate_version),
            "feature_schema": dict(feature_schema),
            "validation": dict(validation_diagnostics or {}),
            "replay_rows": list(replay_rows or []),
            "registered_rows": int(len(approved_rows)),
            "stage": str(final_stage),
            "symbolic_candidate": dict(symbolic_candidate or {}),
        },
    )
    record_alpha_lifecycle(
        candidate_id=int(candidate_id),
        stage="registration",
        outcome=str(status),
        metrics={"registered_rows": int(len(approved_rows)), "stage": str(final_stage)},
        notes={"approved_rows": list(approved_rows or []), "symbolic_candidate": dict(symbolic_candidate or {})},
    )
    return {"ok": True, "status": str(status), "stage": str(final_stage), "registered_rows": int(len(approved_rows))}


def _process_candidate(
    *,
    candidate_spec: Dict[str, Any],
    train_cfg: Dict[str, Any],
    loop_cfg: Dict[str, Any],
    n_competing_trials: int,
) -> Dict[str, Any]:
    candidate_name = str(candidate_spec.get("candidate_name") or "").strip()
    feature_ids = list(candidate_spec.get("feature_ids") or [])
    symbolic_candidate = dict(candidate_spec.get("symbolic_candidate") or {})
    created_ts = _now_ms()
    candidate_version = str(int(created_ts))
    candidate_id = int(
        record_alpha_candidate(
            candidate_name=str(candidate_name),
            candidate_version=str(candidate_version),
            model_family=str(candidate_spec.get("model_family") or ""),
            feature_ids=list(feature_ids),
            generation_method=str(candidate_spec.get("generation_method") or ""),
            hyperparams=dict(train_cfg.get("hyperparams") or {}),
            status="generated",
            diagnostics={
                "feature_set_tag": str(candidate_spec.get("feature_set_tag") or ""),
                "group_names": list(candidate_spec.get("group_names") or []),
                "base_model_name": str(train_cfg.get("model_name") or ""),
                "candidate_version": str(candidate_version),
                "symbolic_candidate": dict(symbolic_candidate or {}),
            },
            created_ts=int(created_ts),
        )
        or 0
    )
    record_alpha_lifecycle(
        candidate_id=int(candidate_id),
        stage="generation",
        outcome="generated",
        metrics={"feature_count": int(len(feature_ids))},
        notes={
            "candidate_name": str(candidate_name),
            "generation_method": str(candidate_spec.get("generation_method") or ""),
            "group_names": list(candidate_spec.get("group_names") or []),
            "symbolic_candidate": dict(symbolic_candidate or {}),
        },
        created_ts=int(created_ts),
    )

    runtime_symbols = _runtime_symbols(train_cfg)
    dataset_symbols = [] if "*" in runtime_symbols else list(runtime_symbols)
    lookback_days = int(train_cfg.get("training_window_days") or 0)
    horizon_s = int(train_cfg.get("horizon_s") or 0)
    dataset_used = build_dataset_snapshot(
        model_name=str(candidate_spec.get("model_family") or ""),
        lookback_days=int(lookback_days),
        symbols=list(dataset_symbols),
        horizons=[int(horizon_s)],
        feature_ids=list(feature_ids),
        feature_schema={
            "feature_ids": list(feature_ids),
            "feature_set_tag": str(candidate_spec.get("feature_set_tag") or ""),
            "feature_count": int(len(list(feature_ids))),
        },
        training_window={
            "lookback_days": int(lookback_days),
            "end_ts_ms": int(created_ts),
            "start_ts_ms": int(created_ts - (int(lookback_days) * 24 * 60 * 60 * 1000)),
            "horizon_s": int(horizon_s),
        },
        extra={
            "job_name": JOB_NAME,
            "candidate_name": str(candidate_name),
            "generation_method": str(candidate_spec.get("generation_method") or ""),
        },
    )
    rows = _load_labeled_feature_rows(
        cutoff_ms=int(_now_ms() - (int(lookback_days) * 24 * 60 * 60 * 1000)),
        symbol_filter=set(dataset_symbols),
        horizon_s=int(horizon_s),
        feature_ids=list(feature_ids),
    )
    update_alpha_candidate(
        candidate_id=int(candidate_id),
        status="training",
        diagnostics={
            "dataset_used": dict(dataset_used or {}),
            "n_rows": int(len(rows)),
            "symbolic_candidate": dict(symbolic_candidate or {}),
        },
    )
    record_alpha_lifecycle(
        candidate_id=int(candidate_id),
        stage="training",
        outcome="started",
        metrics={"n_rows": int(len(rows)), "horizon_s": int(horizon_s)},
        notes={"dataset_used": dict(dataset_used or {}), "symbolic_candidate": dict(symbolic_candidate or {})},
    )

    train_result = _train_candidate_bundle(
        candidate_spec=dict(candidate_spec),
        train_cfg=dict(train_cfg),
        loop_cfg=dict(loop_cfg),
        rows=list(rows),
        created_ts=int(created_ts),
        candidate_version=str(candidate_version),
    )
    if not bool(train_result.get("ok")):
        failed_diagnostics = {**dict(train_result or {}), "symbolic_candidate": dict(symbolic_candidate or {})}
        update_alpha_candidate(candidate_id=int(candidate_id), status="rejected", diagnostics=failed_diagnostics)
        record_alpha_lifecycle(
            candidate_id=int(candidate_id),
            stage="training",
            outcome="failed",
            metrics={"n_rows": int(len(rows))},
            notes=failed_diagnostics,
        )
        return {"ok": True, "status": "rejected", "reason": str(train_result.get("reason") or train_result.get("status") or "")}

    candidate_version = str(train_result.get("candidate_version") or str(candidate_version))
    created_ts = int(train_result.get("created_ts") or created_ts or _now_ms())
    update_alpha_candidate(
        candidate_id=int(candidate_id),
        status="trained",
        diagnostics={
            "dataset_used": dict(dataset_used or {}),
            "training_metrics": dict(train_result.get("training_metrics") or {}),
            "feature_schema": dict(train_result.get("feature_schema") or {}),
            "candidate_version": str(candidate_version),
            "symbolic_candidate": dict(symbolic_candidate or {}),
        },
    )

    feature_schema = dict(train_result.get("feature_schema") or {})
    persist_con = connect(readonly=False)
    try:
        persist_gbm_model_record(
            persist_con,
            model_name=str(candidate_name),
            version=str(candidate_version),
            created_ts=int(created_ts),
            blob=bytes(train_result.get("blob") or b""),
            feature_schema=dict(feature_schema),
            training_metrics=dict(train_result.get("training_metrics") or {}),
        )
        persist_con.commit()
    finally:
        try:
            persist_con.close()
        except Exception as e:
            _warn_nonfatal("ALPHA_DISCOVERY_PERSIST_CLOSE_FAILED", e, candidate_name=str(candidate_name))

    registration_cfg = {
        **dict(train_cfg or {}),
        "model_name": str(candidate_name),
        "model_id": str(candidate_name),
        "instance_name": str(candidate_name),
        "feature_ids": list(feature_ids),
        "model_kind": str(train_result.get("model_kind") or "lightgbm"),
    }
    registration_meta = build_model_registration_metadata(registration_cfg)
    register_model_version(
        model_name=str(candidate_name),
        model_version=str(candidate_version),
        model_kind=str(train_result.get("model_kind") or "lightgbm"),
        mutation_kind=str(_candidate_mutation_kind(candidate_spec)),
        stage="shadow",
        status="trained",
        live_ready=False,
        training_job_name=JOB_NAME,
        train_scope={
            "symbols": list(runtime_symbols),
            "horizons": [int(horizon_s)],
            "lookback_days": int(lookback_days),
            "feature_ids": list(feature_ids),
            "dataset_used": dict(dataset_used or {}),
            "generation_method": str(candidate_spec.get("generation_method") or ""),
            "symbolic_candidate": dict(symbolic_candidate or {}),
        },
        meta={
            "alpha_candidate_id": int(candidate_id),
            "model_id": str(registration_meta.get("model_id") or candidate_name),
            "model_family": str(registration_meta.get("model_family") or candidate_spec.get("model_family") or ""),
            "instance_name": str(registration_meta.get("instance_name") or candidate_name),
            "risk_profile": str(registration_meta.get("risk_profile") or train_cfg.get("risk_profile") or "balanced"),
            "feature_schema": dict(feature_schema),
            "dataset_used": dict(dataset_used or {}),
            "training_started_ts_ms": int(created_ts),
            "training_completed_ts_ms": int(created_ts),
            "symbolic_candidate": dict(symbolic_candidate or {}),
        },
    )
    record_version_performance(
        model_name=str(candidate_name),
        model_version=str(candidate_version),
        metric_scope="training",
        metrics={
            **_numeric_metrics_only(train_result.get("training_metrics") or {}),
            "eval_ts_ms": int(created_ts),
            "alpha_candidate_id": int(candidate_id),
        },
        sample_n=int(dict(train_result.get("evaluation") or {}).get("metrics", {}).get("n_eval") or 0),
        meta={
            "job_name": JOB_NAME,
            "feature_ids": list(feature_ids),
            "feature_schema": dict(feature_schema),
            "model_kind": str(train_result.get("model_kind") or "lightgbm"),
            "model_family": str(candidate_spec.get("model_family") or ""),
            "symbolic_candidate": dict(symbolic_candidate or {}),
        },
    )
    record_alpha_lifecycle(
        candidate_id=int(candidate_id),
        stage="training",
        outcome="trained",
        metrics=dict(train_result.get("training_metrics") or {}),
        notes={
            "candidate_version": str(candidate_version),
            "dataset_used": dict(dataset_used or {}),
            "symbolic_candidate": dict(symbolic_candidate or {}),
        },
    )

    evaluation = dict(train_result.get("evaluation") or {})
    inserted_shadow = _write_shadow_validation_rows(
        candidate_id=int(candidate_id),
        candidate_name=str(candidate_name),
        candidate_version=str(candidate_version),
        model_kind=str(train_result.get("model_kind") or "lightgbm"),
        generation_method=str(candidate_spec.get("generation_method") or ""),
        eval_rows=list(evaluation.get("rows") or []),
        predictions=[float(item) for item in list(evaluation.get("predictions") or [])],
    )
    record_alpha_lifecycle(candidate_id=int(candidate_id), stage="shadow_validation", outcome="predictions_logged", metrics={"shadow_rows": int(inserted_shadow)}, notes={})

    replay_summary = dict(refresh_replay_validation_snapshot() or {})
    replay_rows = _replay_rows_for_candidate(
        replay_snapshot=dict(replay_summary.get("snapshot") or replay_summary),
        candidate_name=str(candidate_name),
        candidate_version=str(candidate_version),
        runtime_symbols=list(runtime_symbols),
    )
    validation_passed, validation_diagnostics = evaluate_statistical_promotion_gate(
        model_name=str(candidate_name),
        candidate_version=str(candidate_version),
        returns=list(evaluation.get("returns") or []),
        n_competing_trials=max(1, int(n_competing_trials or 1)),
        config=_validation_gate_config(loop_cfg),
        persist=True,
    )
    full_validation = {**dict(validation_diagnostics or {}), "replay_rows": list(replay_rows or []), "replay_refresh": dict(replay_summary or {})}
    if not replay_rows or not any(bool(row.get("approved")) for row in replay_rows):
        return _reject_candidate(
            candidate_id=int(candidate_id),
            candidate_name=str(candidate_name),
            candidate_version=str(candidate_version),
            reason="replay_validation_failed",
            diagnostics=dict(full_validation),
            candidate_spec=dict(candidate_spec),
        )
    if not bool(validation_passed):
        return _reject_candidate(
            candidate_id=int(candidate_id),
            candidate_name=str(candidate_name),
            candidate_version=str(candidate_version),
            reason=str(full_validation.get("status") or "validation_gate_failed"),
            diagnostics=dict(full_validation),
            candidate_spec=dict(candidate_spec),
        )
    return _register_survivor(
        candidate_id=int(candidate_id),
        candidate_spec=dict(candidate_spec),
        train_cfg=dict(train_cfg),
        train_result={**dict(train_result or {}), "dataset_used": dict(dataset_used or {})},
        replay_rows=list(replay_rows),
        validation_diagnostics=dict(full_validation),
        loop_cfg=dict(loop_cfg),
    )


def _publish_alpha_discovery_status(summary: Dict[str, Any]) -> None:
    payload = {**dict(summary or {}), "recent_candidates": fetch_recent_alpha_candidates(limit=10), "updated_ts_ms": int(_now_ms())}
    try:
        meta_set("alpha_discovery_status", _json_dumps(payload))
    except Exception as e:
        _warn_nonfatal("ALPHA_DISCOVERY_STATUS_META_SET_FAILED", e)


def run_alpha_discovery() -> Dict[str, Any]:
    """Run the bounded alpha-discovery loop and publish a summary."""
    loop_cfg = alpha_discovery_config_from_env()
    if not bool(loop_cfg.get("enabled")):
        summary = {"ok": True, "enabled": False, "status": "disabled", "generated": 0, "registered_shadow": 0, "registered_challenger": 0, "rejected": 0, "errors": 0}
        _publish_alpha_discovery_status(summary)
        return summary

    model_family = _family_priority(loop_cfg.get("allowed_families") or [])
    if not model_family:
        summary = {"ok": False, "enabled": True, "status": "unsupported_family", "generated": 0, "registered_shadow": 0, "registered_challenger": 0, "rejected": 0, "errors": 1}
        _publish_alpha_discovery_status(summary)
        return summary

    train_cfg = _base_training_config(str(model_family))
    candidates = generate_candidate_specs(base_config=dict(train_cfg), max_candidates=int(loop_cfg.get("max_candidates") or 0))
    if not candidates:
        summary = {"ok": True, "enabled": True, "status": "no_candidates", "generated": 0, "registered_shadow": 0, "registered_challenger": 0, "rejected": 0, "errors": 0}
        _publish_alpha_discovery_status(summary)
        return summary

    summary = {"ok": True, "enabled": True, "status": "completed", "generated": int(len(candidates)), "registered_shadow": 0, "registered_challenger": 0, "rejected": 0, "errors": 0, "candidates": []}
    for candidate_spec in candidates:
        try:
            result = _process_candidate(candidate_spec=dict(candidate_spec), train_cfg=dict(train_cfg), loop_cfg=dict(loop_cfg), n_competing_trials=int(len(candidates)))
        except Exception as e:
            summary["ok"] = False
            summary["errors"] = int(summary.get("errors") or 0) + 1
            _warn_nonfatal("ALPHA_DISCOVERY_CANDIDATE_PROCESS_FAILED", e, candidate_name=str(candidate_spec.get("candidate_name") or ""))
            result = {"ok": False, "status": "error", "reason": f"{type(e).__name__}:{e}"}
        summary["candidates"].append({"candidate_name": str(candidate_spec.get("candidate_name") or ""), "status": str(result.get("status") or ""), "reason": result.get("reason")})
        if str(result.get("status") or "") == "registered_shadow":
            summary["registered_shadow"] = int(summary.get("registered_shadow") or 0) + 1
        elif str(result.get("status") or "") == "registered_challenger":
            summary["registered_challenger"] = int(summary.get("registered_challenger") or 0) + 1
        elif str(result.get("status") or "") == "rejected":
            summary["rejected"] = int(summary.get("rejected") or 0) + 1
        elif not bool(result.get("ok")):
            summary["errors"] = int(summary.get("errors") or 0) + 1

    _publish_alpha_discovery_status(summary)
    return summary


def main() -> int:
    """Run the alpha-discovery loop from the command line."""
    summary = run_alpha_discovery()
    print(_json_dumps(summary))
    return 0 if bool(summary.get("ok", True)) else 1


__all__ = ["alpha_discovery_config_from_env", "generate_candidate_specs", "main", "run_alpha_discovery"]
