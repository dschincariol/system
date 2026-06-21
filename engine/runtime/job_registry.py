"""Canonical runtime job registry and launch-order metadata.

This module is the single source of truth for allowed jobs, execution modes,
pipeline ordering, and static validation used by runtime startup, supervision,
and operator-facing job management.
"""

from __future__ import annotations

import ast
import importlib
import logging
import os
import py_compile
from pathlib import Path
from typing import Any, Dict, List, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import default_ingestion_pipeline_jobs
from engine.runtime.logging import get_logger


LOG = get_logger("runtime.job_registry")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="runtime_job_registry_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.runtime.job_registry",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _source_allowed_job_duplicates() -> List[str]:
    try:
        source = Path(__file__).read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(__file__))
    except Exception as e:
        _warn_nonfatal(
            "RUNTIME_JOB_REGISTRY_ALLOWED_DUPLICATES_PARSE_FAILED",
            e,
            once_key="runtime_job_registry_allowed_duplicates_parse_failed",
        )
        return []

    duplicates: List[str] = []

    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue

        targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
        if "ALLOWED_JOBS" not in targets:
            continue
        if not isinstance(node.value, ast.Dict):
            continue

        seen = set()
        for key_node in node.value.keys:
            key = None
            if isinstance(key_node, ast.Constant):
                key = key_node.value
            elif isinstance(key_node, ast.Str):
                key = key_node.s
            if not isinstance(key, str):
                continue
            if key in seen:
                duplicates.append(key)
            else:
                seen.add(key)
        break

    return duplicates

def _resolve_job_path(path: str) -> str:
    # Keep older registry entries working even after provider-session files were
    # moved to their newer package location.
    root = Path(__file__).resolve().parents[2]
    p = (root / path).resolve()
    if p.exists():
        return path

    # fallback for moved provider session files
    alt = root / "engine" / "data" / "provider_sessions" / Path(path).name
    if alt.exists():
        return str(Path("engine/data/provider_sessions") / Path(path).name)

    return path


def _discover_repo_job_files(repo_root: str | Path | None = None) -> set[str]:
    base = Path(repo_root).resolve() if repo_root else Path(__file__).resolve().parents[2]
    engine_root = base / "engine"
    if not engine_root.exists():
        return set()
    return {
        path.relative_to(base).as_posix()
        for path in engine_root.glob("**/jobs/*.py")
        if path.is_file() and path.name != "__init__.py"
    }


def _registered_job_script_paths() -> set[str]:
    paths: set[str] = set()
    for spec in ALLOWED_JOBS.values():
        if not isinstance(spec, (tuple, list)) or not spec:
            continue
        script_rel = str(spec[0] or "").strip()
        if not script_rel:
            continue
        paths.add(Path(_resolve_job_path(script_rel)).as_posix())
    return paths


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0") or "").strip().lower()
    if not raw:
        return bool(default)
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _repo_relative_job_path(path: str | Path, repo_root: str | Path | None = None) -> str:
    base = Path(repo_root).resolve() if repo_root else Path(__file__).resolve().parents[2]
    raw = Path(str(path))
    candidate = raw if raw.is_absolute() else base / raw
    try:
        rel = candidate.resolve().relative_to(base.resolve()).as_posix()
    except Exception:
        rel = raw.as_posix()
    return Path(_resolve_job_path(rel)).as_posix()


def _allow_unregistered_jobs() -> bool:
    if str(os.environ.get("TS_ENV", "") or "").strip().lower() == "production":
        return False
    return _env_flag("TS_ALLOW_UNREGISTERED_JOBS", False)


def is_registered_job_path(path: str | Path, repo_root: str | Path | None = None) -> bool:
    return _repo_relative_job_path(path, repo_root) in _registered_job_script_paths()


def enforce_registered_job_path(path: str | Path, repo_root: str | Path | None = None) -> str:
    script_rel = _repo_relative_job_path(path, repo_root)
    if script_rel in _registered_job_script_paths():
        return script_rel
    if _allow_unregistered_jobs():
        return script_rel
    raise PermissionError(f"unregistered_job: {script_rel}")


def _tsfresh_pipeline_enabled() -> bool:
    return bool(_env_flag("USE_TSFRESH_FEATURES", False))


def _finbert_pipeline_enabled() -> bool:
    return bool(_env_flag("USE_FINBERT_SENTIMENT", False))


def _nlp_pipeline_enabled() -> bool:
    return bool(_env_flag("USE_NLP_FEATURES", False) or _env_flag("NLP_PIPELINE_ENABLED", False))


def _hmm_pipeline_enabled() -> bool:
    return bool(_env_flag("HMM_REGIME_ENABLED", False))


def _drift_retrain_pipeline_enabled() -> bool:
    return bool(_env_flag("DRIFT_RETRAIN_ENABLED", False))


def _cpcv_pipeline_enabled() -> bool:
    return bool(_env_flag("CPCV_ENABLED", False))


def _causal_scoring_enabled() -> bool:
    return bool(_env_flag("CAUSAL_SCORING_ENABLED", False))


def _gbm_pipeline_enabled() -> bool:
    if _env_flag("USE_GBM_REGRESSOR", False):
        return True
    try:
        from engine.strategy.model_config import load_model_configs

        configs = load_model_configs(family="gbm_regressor", include_disabled=True)
    except Exception as e:
        _warn_nonfatal(
            "RUNTIME_JOB_REGISTRY_GBM_CONFIG_LOAD_FAILED",
            e,
            once_key="runtime_job_registry_gbm_config_load_failed",
        )
        return False
    for cfg in list(configs or []):
        if bool(cfg.get("enabled")) or bool(cfg.get("active")) or bool(cfg.get("prediction_enabled")):
            return True
    return False


def _model_family_pipeline_enabled(family: str, env_flag: str) -> bool:
    if _env_flag(str(env_flag), False):
        return True
    try:
        from engine.strategy.model_config import load_model_configs

        configs = load_model_configs(family=str(family), include_disabled=True)
    except Exception as e:
        _warn_nonfatal(
            "RUNTIME_JOB_REGISTRY_MODEL_FAMILY_CONFIG_LOAD_FAILED",
            e,
            once_key=f"runtime_job_registry_model_family_config_load_failed:{family}",
            family=str(family),
        )
        return False
    for cfg in list(configs or []):
        if bool(cfg.get("enabled")) or bool(cfg.get("active")) or bool(cfg.get("prediction_enabled")):
            return True
    return False


def _pit_universe_backfill_enabled() -> bool:
    return bool(_env_flag("PIT_UNIVERSE_BACKFILL_ENABLED", False))


def _has_valid_script_entrypoint(script_abs: str) -> bool:
    # Registry validation is intentionally static: compile/import checks happen
    # elsewhere, while this only verifies the file looks launchable.
    try:
        with open(script_abs, "r", encoding="utf-8") as f:
            source = f.read()
    except Exception as e:
        _warn_nonfatal(
            "RUNTIME_JOB_REGISTRY_SCRIPT_READ_FAILED",
            e,
            once_key=f"runtime_job_registry_script_read:{script_abs}",
            script_abs=script_abs,
        )
        return False

    try:
        tree = ast.parse(source, filename=script_abs)
    except Exception as e:
        _warn_nonfatal(
            "RUNTIME_JOB_REGISTRY_SCRIPT_AST_PARSE_FAILED",
            e,
            once_key=f"runtime_job_registry_script_ast_parse:{script_abs}",
            script_abs=script_abs,
        )
        return False

    fn_names = {
        str(node.name)
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    if "main" in fn_names or "run" in fn_names:
        return True

    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue

        test = node.test
        if not isinstance(test, ast.Compare):
            continue
        if not isinstance(test.left, ast.Name) or test.left.id != "__name__":
            continue
        if len(test.ops) != 1 or not isinstance(test.ops[0], ast.Eq):
            continue
        if len(test.comparators) != 1:
            continue

        comp = test.comparators[0]
        comp_value = None
        if isinstance(comp, ast.Constant):
            comp_value = comp.value
        elif isinstance(comp, ast.Str):
            comp_value = comp.s

        if comp_value != "__main__":
            continue

        for child in ast.walk(node):
            if not isinstance(child, ast.Call):
                continue
            func = child.func
            if isinstance(func, ast.Name) and func.id in {"main", "run", "run_root_script"}:
                return True
            if isinstance(func, ast.Attribute) and func.attr in {"main", "run", "run_root_script"}:
                return True

    return False


ALLOWED_JOBS = {

# This registry is the canonical DAG input for JobsManager, the supervisor,
# startup orchestration, and UI/operator job status.

# ---------------------------
# Ingestion runtime + price feeds
# ---------------------------

"ingestion_runtime": (
    "engine/runtime/ingestion_runtime.py",
    "daemon",
    None,
    {"execution": False, "isolated_market_data": True},
),

"stream_prices_polygon_ws": (
    "engine/jobs/stream_prices_polygon_ws.py",
    "daemon",
    "price_feed",
    {"execution": False, "primary_feed": True},
),

"stream_prices_ibkr": (
    "engine/data/providers/ibkr/daemon_stream.py",
    "daemon",
    "price_feed",
    {"execution": False, "primary_feed": False, "gateway_feed": True},
),

"poll_prices": (
    "engine/data/poll_prices.py",
    "daemon",
    "price_feed",
    {"execution": False, "fallback_feed": True, "failover": True},
),

"options_poll": (
    "engine/data/options_poll.py",
    "daemon",
    None,
    {"execution": False, "schedule": "every 300s", "cadence_seconds": 300},
),

"poll_macro": (
    "engine/data/jobs/poll_macro.py",
    "daemon",
    None,
    {"execution": False, "schedule": "every 21600s", "cadence_seconds": 21600},
),

"poll_kalshi_prediction_markets": (
    "engine/data/jobs/poll_kalshi_prediction_markets.py",
    "daemon",
    None,
    {"execution": False, "schedule": "every 900s", "cadence_seconds": 900, "pipeline_stage": "prediction_market_macro_shadow"},
),

"poll_cme_fedwatch": (
    "engine/data/jobs/poll_cme_fedwatch.py",
    "daemon",
    None,
    {"execution": False, "schedule": "every 21600s", "cadence_seconds": 21600, "pipeline_stage": "prediction_market_macro_shadow"},
),

"poll_deribit_crypto_derivatives": (
    "engine/data/jobs/poll_deribit_crypto_derivatives.py",
    "daemon",
    None,
    {
        "execution": False,
        "schedule": "every 900s",
        "cadence_seconds": 900,
        "pipeline_stage": "deribit_crypto_derivatives_shadow",
        "direct_trading_authority": False,
    },
),

"poll_sportsbook_odds": (
    "engine/data/jobs/poll_sportsbook_odds.py",
    "daemon",
    None,
    {
        "execution": False,
        "schedule": "every 1800s",
        "cadence_seconds": 1800,
        "pipeline_stage": "sportsbook_odds_research_shadow",
        "direct_trading_authority": False,
    },
),

"poll_polymarket_prediction_markets": (
    "engine/data/jobs/poll_polymarket_prediction_markets.py",
    "daemon",
    None,
    {"execution": False, "schedule": "every 900s", "cadence_seconds": 900, "pipeline_stage": "prediction_market_event_shadow"},
),

"poll_forecastex_event_contracts": (
    "engine/data/jobs/poll_forecastex_event_contracts.py",
    "daemon",
    None,
    {
        "execution": False,
        "schedule": "every 600s",
        "cadence_seconds": 600,
        "pipeline_stage": "regulated_event_contract_shadow",
        "direct_trading_authority": False,
    },
),

"backfill_macro_vintages": (
    "engine/data/jobs/backfill_macro_vintages.py",
    "oneshot",
    None,
    {"execution": False, "schedule": "manual one-shot", "pipeline_stage": "macro_backfill"},
),

"backfill_prediction_market_macro": (
    "engine/data/jobs/backfill_prediction_market_macro.py",
    "oneshot",
    None,
    {"execution": False, "schedule": "manual one-shot", "pipeline_stage": "prediction_market_macro_backfill"},
),

"backfill_sportsbook_odds_event_study": (
    "engine/data/jobs/backfill_sportsbook_odds_event_study.py",
    "oneshot",
    None,
    {
        "execution": False,
        "schedule": "manual one-shot",
        "pipeline_stage": "sportsbook_odds_research_backfill",
        "direct_trading_authority": False,
    },
),

"backfill_features": (
    "engine/data/jobs/backfill_features.py",
    "oneshot",
    None,
    {"execution": False, "schedule": "manual one-shot", "pipeline_stage": "feature_backfill"},
),

"snapshot_model_features": (
    "engine/data/jobs/snapshot_model_features.py",
    "daemon",
    None,
    {"execution": False},
),

"inference_health_probe": (
    "engine/runtime/jobs/inference_health_probe.py",
    "daemon",
    None,
    {"execution": False, "resource_class": "inference"},
),

"provider_monitor": (
    "engine/runtime/jobs/provider_monitor_job.py",
    "daemon",
    None,
    {"execution": False},
),

"metrics_collector": (
    "engine/runtime/jobs/metrics_collector.py",
    "daemon",
    None,
    {"execution": False},
),

"kill_switch_cache_refresh": (
    "engine/runtime/jobs/kill_switch_cache_refresh.py",
    "daemon",
    None,
    {
        "execution": False,
        "schedule": "every 10s",
        "cadence_seconds": 10,
        "resource_class": "control_plane",
    },
),

"observability_snapshot": (
    "engine/strategy/jobs/observability_snapshot.py",
    "daemon",
    None,
    {"execution": False},
),

# ---------------------------
# OPTIONAL DAEMONS
# Registered, but not auto-booted in JOB_ORDER
# ---------------------------

"compute_weather_ingest": (
    "engine/data/jobs/compute_weather_ingest.py",
    "daemon",
    None,
    {"execution": False},
),

"compute_weather_alerts_ingest": (
    "engine/data/jobs/compute_weather_alerts_ingest.py",
    "daemon",
    None,
    {"execution": False},
),

"compute_weather_promotion_guard": (
    "engine/data/jobs/compute_weather_promotion_guard.py",
    "daemon",
    None,
    {"execution": False},
),

"poll_social_reddit": (
    "engine/data/jobs/poll_social_reddit.py",
    "daemon",
    None,
    {"execution": False},
),

"poll_social_stocktwits": (
    "engine/data/jobs/poll_social_stocktwits.py",
    "daemon",
    None,
    {"execution": False},
),

"build_social_features": (
    "ops/build_social_features.py",
    "daemon",
    None,
    {"execution": False},
),

"compute_tsfresh_snapshots": (
    "engine/data/jobs/compute_tsfresh_snapshots.py",
    "daemon",
    None,
    {
        "execution": False,
        "pipeline_stage": "feature_snapshot_training",
        "resource_class": "training",
        "resource_priority": 25,
        "slot_cost": 1,
    },
),

"poll_weather_alerts": (
    "engine/data/jobs/poll_weather_alerts.py",
    "daemon",
    None,
    {"execution": False},
),

"poll_weather_forecasts": (
    "engine/data/jobs/poll_weather_forecasts.py",
    "daemon",
    None,
    {"execution": False},
),

# ---------------------------
# Core data pipeline
# ---------------------------

"ingest_now": (
    "engine/data/jobs/ingest_now.py",
    "daemon",
    None,
    {"execution": False, "pipeline_stage": "ingest"},
),

"update_universe": (
    "engine/data/jobs/update_universe.py",
    "oneshot",
    None,
    {"execution": False, "pipeline_stage": "universe"},
),

"backfill_universe_pit": (
    "engine/data/jobs/backfill_universe_pit.py",
    "oneshot",
    None,
    {"execution": False, "pipeline_stage": "universe_pit"},
),

"process_events": (
    "engine/data/jobs/process_events.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "process",
        "resource_class": "inference",
        "resource_priority": 90,
        "slot_cost": 1,
    },
),

"label_due_events": (
    "engine/data/jobs/label_due_events.py",
    "oneshot",
    None,
    {"execution": False, "pipeline_stage": "label"},
),

"fill_ensemble_oos_targets": (
    "engine/strategy/jobs/fill_ensemble_oos_targets.py",
    "oneshot",
    None,
    {"execution": False, "pipeline_stage": "ensemble_target_fill"},
),

"adwin_residual_drift": (
    "engine/strategy/jobs/adwin_residual_drift.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "drift",
        "resource_class": "training",
        "resource_priority": 36,
        "slot_cost": 1,
    },
),

"triple_barrier_labels": (
    "engine/strategy/jobs/triple_barrier_labels.py",
    "oneshot",
    None,
    {"execution": False, "pipeline_stage": "meta_labeling"},
),

"train_meta_label_model": (
    "engine/strategy/jobs/train_meta_label_model.py",
    "oneshot",
    None,
    {"execution": False, "pipeline_stage": "model_training"},
),

"causal_scoring": (
    "engine/strategy/jobs/causal_scoring.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "causal_scoring",
        "resource_class": "training",
        "resource_priority": 32,
        "slot_cost": 1,
    },
),

"compute_drift": (
    "engine/data/jobs/compute_drift.py",
    "oneshot",
    None,
    {"execution": False, "pipeline_stage": "drift"},
),

"har_rv_forecast": (
    "engine/strategy/jobs/har_rv_forecast.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "risk_forecast",
        "resource_class": "training",
        "resource_priority": 33,
        "slot_cost": 1,
    },
),

"bocpd_regime_update": (
    "engine/strategy/jobs/bocpd_regime_update.py",
    "oneshot",
    None,
    {
        "execution": False,
        "schedule": "daily",
        "pipeline_stage": "regime",
        "resource_class": "training",
        "resource_priority": 37,
        "slot_cost": 1,
    },
),

"shadow_metrics": (
    "engine/strategy/jobs/shadow_metrics_job.py",
    "oneshot",
    None,
    {"execution": False, "pipeline_stage": "shadow_metrics"},
),

"train_ensemble": (
    "engine/strategy/jobs/train_ensemble.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "ensemble_train",
        "resource_class": "training",
        "resource_priority": 34,
        "slot_cost": 1,
    },
),

"train_ensemble_meta": (
    "engine/strategy/jobs/train_ensemble_meta.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "ensemble_meta_train",
        "resource_class": "training",
        "resource_priority": 35,
        "slot_cost": 1,
    },
),

"train_hmm_regime": (
    "engine/strategy/jobs/train_hmm_regime.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "hmm_regime_train",
        "resource_class": "training",
        "resource_priority": 38,
        "slot_cost": 1,
    },
),

"train_embed_models": (
    "engine/strategy/jobs/train_embed_models.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "embed_train",
        "resource_class": "training",
        "resource_priority": 40,
        "slot_cost": 1,
    },
),

"train_gbm_regressor": (
    "engine/strategy/jobs/train_gbm_regressor.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "gbm_train",
        "resource_class": "training",
        "resource_priority": 40,
        "slot_cost": 1,
    },
),

"train_lgbm_models": (
    "engine/strategy/jobs/train_lgbm_models.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "lgbm_train",
        "resource_class": "training",
        "resource_priority": 40,
        "slot_cost": 1,
    },
),

"incremental_lgbm_refresh": (
    "engine/strategy/jobs/incremental_lgbm_refresh.py",
    "oneshot",
    None,
    {
        "execution": False,
        "schedule": "nightly",
        "pipeline_stage": "lgbm_refresh",
        "resource_class": "training",
        "resource_priority": 39,
        "slot_cost": 1,
    },
),

"train_lgbm_ranker_models": (
    "engine/strategy/jobs/train_lgbm_ranker_models.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "lgbm_ranker_train",
        "resource_class": "training",
        "resource_priority": 40,
        "slot_cost": 1,
    },
),

"train_xgb_models": (
    "engine/strategy/jobs/train_xgb_models.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "xgb_train",
        "resource_class": "training",
        "resource_priority": 40,
        "slot_cost": 1,
    },
),

"train_patchtst_models": (
    "engine/strategy/jobs/train_patchtst_models.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "patchtst_train",
        "resource_class": "training",
        "resource_priority": 40,
        "slot_cost": 1,
    },
),

"train_itransformer_models": (
    "engine/strategy/jobs/train_itransformer_models.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "itransformer_train",
        "resource_class": "training",
        "resource_priority": 40,
        "slot_cost": 1,
        "default_stage": "shadow",
    },
),

"pretrain_patchtst_models": (
    "engine/strategy/jobs/pretrain_patchtst_models.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "patchtst_pretrain",
        "resource_class": "training",
        "resource_priority": 40,
        "slot_cost": 1,
        "default_stage": "shadow",
    },
),

"tune_gbm_regressor_optuna": (
    "engine/strategy/jobs/tune_gbm_regressor_optuna.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "gbm_tune",
        "resource_class": "training",
        "resource_priority": 38,
        "slot_cost": 1,
    },
),

"tune_models": (
    "engine/strategy/jobs/tune_models.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "model_tuning",
        "resource_class": "training",
        "resource_priority": 38,
        "slot_cost": 1,
    },
),

"train_model_v2": (
    "engine/strategy/jobs/train_model_v2.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "model_train",
        "resource_class": "training",
        "resource_priority": 40,
        "slot_cost": 1,
    },
),

"train_temporal_predictor": (
    "engine/strategy/jobs/train_temporal_predictor.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "temporal_train",
        "resource_class": "training",
        "resource_priority": 40,
        "slot_cost": 1,
    },
),

"eval_temporal_shadow": (
    "engine/strategy/jobs/eval_temporal_shadow.py",
    "oneshot",
    None,
    {"execution": False, "pipeline_stage": "temporal_shadow_eval"},
),

"promote_temporal_models": (
    "engine/strategy/jobs/promote_temporal_models.py",
    "oneshot",
    None,
    {"execution": False, "pipeline_stage": "temporal_promote"},
),

"model_lifecycle_manager": (
    "engine/strategy/jobs/model_lifecycle_manager.py",
    "oneshot",
    None,
    {"execution": False, "pipeline_stage": "model_lifecycle"},
),

"alpha_discovery_loop": (
    "engine/strategy/jobs/alpha_discovery_loop.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "alpha_discovery",
        "resource_class": "training",
        "resource_priority": 35,
        "slot_cost": 1,
    },
),

"discover_features": (
    "engine/strategy/jobs/discover_features.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "feature_discovery",
        "resource_class": "training",
        "resource_priority": 35,
        "slot_cost": 1,
    },
),

"llm_factor_discovery": (
    "engine/strategy/jobs/llm_factor_discovery.py",
    "oneshot",
    None,
    {
        "execution": False,
        "schedule": "weekly/manual",
        "cadence_seconds": 604800,
        "pipeline_stage": "feature_discovery",
        "resource_class": "training",
        "resource_priority": 30,
        "slot_cost": 1,
        "requires_secret": "ANTHROPIC_API_KEY",
    },
),

"drift_triggered_retrain": (
    "engine/strategy/jobs/drift_triggered_retrain.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "model_lifecycle",
        "resource_class": "training",
        "resource_priority": 35,
        "slot_cost": 1,
    },
),

"validate_now": (
    "engine/strategy/jobs/validate_now.py",
    "oneshot",
    None,
    {"execution": False, "pipeline_stage": "validate"},
),

"backtest_cpcv": (
    "engine/strategy/jobs/backtest_cpcv.py",
    "oneshot",
    None,
    {
        "execution": False,
        "resource_class": "replay",
        "resource_priority": 30,
        "slot_cost": 1,
    },
),

"backtest_walk_forward": (
    "engine/strategy/jobs/backtest_walk_forward.py",
    "oneshot",
    None,
    {
        "execution": False,
        "resource_class": "replay",
        "resource_priority": 30,
        "slot_cost": 1,
    },
),

"portfolio_backtest": (
    "engine/strategy/jobs/portfolio_backtest.py",
    "oneshot",
    None,
    {
        "execution": False,
        "resource_class": "replay",
        "resource_priority": 30,
        "slot_cost": 1,
    },
),

# ---------------------------
# Additional oneshot jobs
# Registered, but not auto-booted in JOB_ORDER
# ---------------------------

"repair_schema": (
    "engine/runtime/jobs/repair_schema.py",
    "oneshot",
    None,
    {"execution": False},
),

"artifacts_fsck": (
    "engine/strategy/jobs/artifacts_fsck.py",
    "oneshot",
    None,
    {"execution": False, "schedule": "daily", "pipeline_stage": "maintenance"},
),

"monthly_restore_drill": (
    "engine/runtime/jobs/monthly_restore_drill.py",
    "oneshot",
    None,
    {"execution": False, "schedule": "monthly", "pipeline_stage": "maintenance"},
),

"prod_preflight": (
    "engine/runtime/jobs/prod_preflight.py",
    "oneshot",
    None,
    {"execution": False},
),

"snapshot_equity": (
    "engine/runtime/jobs/snapshot_equity.py",
    "daemon",
    None,
    {"execution": False},
),

"backfill_labels_price": (
    "engine/data/jobs/backfill_labels_price_from_prices.py",
    "oneshot",
    None,
    {"execution": False},
),

"calibrate_price_confidence": (
    "engine/data/jobs/calibrate_price_confidence.py",
    "oneshot",
    None,
    {"execution": False},
),

"ingest_options": (
    "engine/data/jobs/ingest_options.py",
    "oneshot",
    None,
    {"execution": False, "schedule": "every 300s", "cadence_seconds": 300},
),

"poll_earnings": (
    "engine/data/jobs/earnings_poll.py",
    "daemon",
    None,
    {"execution": False},
),

"poll_gdelt": (
    "engine/data/jobs/gdelt_poll.py",
    "daemon",
    None,
    {"execution": False},
),

"poll_sec_filings": (
    "engine/data/jobs/sec_poll.py",
    "daemon",
    None,
    {"execution": False},
),

"ingest_form4": (
    "engine/data/jobs/ingest_form4.py",
    "daemon",
    None,
    {"execution": False, "schedule": "every 1800s", "cadence_seconds": 1800},
),

"ingest_finra_short_volume": (
    "engine/data/jobs/ingest_finra_short_volume.py",
    "daemon",
    None,
    {"execution": False, "schedule": "every 21600s", "cadence_seconds": 21600},
),

"ingest_finra_short_interest": (
    "engine/data/jobs/ingest_finra_short_interest.py",
    "daemon",
    None,
    {"execution": False, "schedule": "every 86400s", "cadence_seconds": 86400},
),

"ingest_crypto_funding": (
    "engine/data/jobs/ingest_crypto_funding.py",
    "daemon",
    None,
    {"execution": False, "schedule": "settlement-aligned 00/08/16 UTC", "cadence_seconds": 28800},
),

"ingest_etf_flows": (
    "engine/data/jobs/ingest_etf_flows.py",
    "daemon",
    None,
    {"execution": False, "schedule": "every 86400s", "cadence_seconds": 86400},
),

"ingest_cftc_cot": (
    "engine/data/jobs/ingest_cftc_cot.py",
    "daemon",
    None,
    {"execution": False, "schedule": "every 86400s", "cadence_seconds": 86400},
),

"ingest_13f": (
    "engine/data/jobs/ingest_13f.py",
    "daemon",
    None,
    {"execution": False, "schedule": "quarterly source; poll every 86400s", "cadence_seconds": 86400, "source_cadence": "quarterly"},
),

"ingest_quiver_gov": (
    "engine/data/jobs/ingest_quiver_gov.py",
    "daemon",
    None,
    {"execution": False, "schedule": "every 86400s", "cadence_seconds": 86400, "requires_secret": "QUIVER_API_KEY"},
),

"ingest_fundamentals_pit": (
    "engine/data/jobs/ingest_fundamentals_pit.py",
    "daemon",
    None,
    {
        "execution": False,
        "schedule": "every 86400s",
        "cadence_seconds": 86400,
        "requires_secret_any": ["SIMFIN_API_KEY", "SHARADAR_API_KEY"],
    },
),

"ingest_congressional_trades": (
    "engine/data/jobs/ingest_congressional_trades.py",
    "daemon",
    None,
    {"execution": False},
),

"process_events_enriched": (
    "engine/data/jobs/process_events_enriched.py",
    "oneshot",
    None,
    {
        "execution": False,
        "resource_class": "inference",
        "resource_priority": 90,
        "slot_cost": 1,
    },
),

"process_finbert_sentiment": (
    "engine/data/jobs/process_finbert_sentiment.py",
    "oneshot",
    None,
    {
        "execution": False,
        "resource_class": "inference",
        "resource_priority": 80,
        "slot_cost": 1,
    },
),

"embed_news": (
    "engine/strategy/jobs/embed_news.py",
    "oneshot",
    None,
    {
        "execution": False,
        "resource_class": "inference",
        "resource_priority": 78,
        "slot_cost": 1,
        "pipeline_stage": "nlp_news",
    },
),

"process_news_flow": (
    "engine/data/jobs/process_news_flow.py",
    "oneshot",
    None,
    {
        "execution": False,
        "schedule": "every 900s",
        "cadence_seconds": 900,
        "resource_class": "inference",
        "resource_priority": 77,
        "slot_cost": 1,
        "pipeline_stage": "news_flow",
    },
),

"embed_filings": (
    "engine/strategy/jobs/embed_filings.py",
    "oneshot",
    None,
    {
        "execution": False,
        "resource_class": "inference",
        "resource_priority": 76,
        "slot_cost": 1,
        "pipeline_stage": "nlp_filings",
    },
),

"embed_transcripts": (
    "engine/strategy/jobs/embed_transcripts.py",
    "oneshot",
    None,
    {
        "execution": False,
        "resource_class": "inference",
        "resource_priority": 76,
        "slot_cost": 1,
        "pipeline_stage": "nlp_transcripts",
    },
),

"process_events_live": (
    "engine/data/jobs/process_events_live.py",
    "oneshot",
    None,
    {
        "execution": False,
        "resource_class": "inference",
        "resource_priority": 90,
        "slot_cost": 1,
    },
),

"process_events_shadow": (
    "engine/data/jobs/process_events_shadow.py",
    "oneshot",
    None,
    {
        "execution": False,
        "resource_class": "inference",
        "resource_priority": 70,
        "slot_cost": 1,
    },
),

"backfill_news_features": (
    "engine/data/jobs/backfill_news_features.py",
    "oneshot",
    None,
    {"execution": False},
),

"check_alerts": (
    "engine/runtime/jobs/check_alerts.py",
    "oneshot",
    None,
    {"execution": False},
),

"check_events": (
    "engine/runtime/jobs/check_events.py",
    "oneshot",
    None,
    {"execution": False},
),

"check_labels": (
    "engine/runtime/jobs/check_labels.py",
    "oneshot",
    None,
    {"execution": False},
),

"check_predictions": (
    "engine/runtime/jobs/check_predictions.py",
    "oneshot",
    None,
    {"execution": False},
),

"kill_drift_monitor": (
    "engine/runtime/jobs/kill_drift_monitor.py",
    "oneshot",
    None,
    {"execution": False},
),

"strategy_kill_drift_monitor": (
    "engine/strategy/jobs/kill_drift_monitor.py",
    "oneshot",
    None,
    {"execution": False},
),

"kill_health_monitor": (
    "engine/runtime/jobs/kill_health_monitor.py",
    "oneshot",
    None,
    {"execution": False},
),

"strategy_kill_health_monitor": (
    "engine/strategy/jobs/kill_health_monitor.py",
    "oneshot",
    None,
    {"execution": False},
),

"kill_slippage_monitor": (
    "engine/runtime/jobs/kill_slippage_monitor.py",
    "oneshot",
    None,
    {"execution": False},
),

"strategy_kill_slippage_monitor": (
    "engine/strategy/jobs/kill_slippage_monitor.py",
    "oneshot",
    None,
    {"execution": False},
),

"post_promotion_monitor": (
    "engine/runtime/jobs/post_promotion_monitor.py",
    "oneshot",
    None,
    {"execution": False},
),

"blacklist_update_job": (
    "engine/runtime/jobs/blacklist_update_job.py",
    "oneshot",
    None,
    {"execution": False},
),

"train_size_policy": (
    "engine/strategy/jobs/train_size_policy.py",
    "oneshot",
    None,
    {"execution": False},
),

"train_learned_alpha_decay": (
    "engine/strategy/jobs/train_learned_alpha_decay.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "learned_alpha_decay",
        "resource_class": "training",
        "resource_priority": 24,
        "slot_cost": 1,
    },
),

"train_drawdown_policy": (
    "engine/strategy/jobs/train_drawdown_policy.py",
    "oneshot",
    None,
    {"execution": False},
),

"train_rl_portfolio": (
    "engine/strategy/jobs/train_rl_portfolio.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "rl_portfolio_train",
        "resource_class": "training",
        "resource_priority": 28,
        "slot_cost": 1,
    },
),

"run_rl_shadow": (
    "engine/strategy/jobs/run_rl_shadow.py",
    "oneshot",
    None,
    {"execution": False, "pipeline_stage": "rl_portfolio_shadow"},
),

"pipeline_train_and_eval": (
    "engine/strategy/jobs/pipeline_train_and_eval.py",
    "oneshot",
    None,
    {
        "execution": False,
        "resource_class": "training",
        "resource_priority": 40,
        "slot_cost": 1,
    },
),

"calibrate_confidence_from_prices": (
    "engine/strategy/jobs/calibrate_confidence_from_prices.py",
    "oneshot",
    None,
    {"execution": False},
),

"recalibrate_confidence": (
    "engine/strategy/jobs/recalibrate_confidence.py",
    "oneshot",
    None,
    {"execution": False},
),

"shadow_train": (
    "engine/strategy/jobs/shadow_train_job.py",
    "oneshot",
    None,
    {
        "execution": False,
        "resource_class": "training",
        "resource_priority": 35,
        "slot_cost": 1,
    },
),

"strategy_governance": (
    "engine/strategy/jobs/strategy_governance_job.py",
    "oneshot",
    None,
    {"execution": False},
),

"execution_quality_job": (
    "engine/strategy/jobs/execution_quality_job.py",
    "oneshot",
    None,
    {"execution": False},
),

"live_stability_guard_job": (
    "engine/strategy/jobs/live_stability_guard_job.py",
    "oneshot",
    None,
    {"execution": False},
),

"trade_attribution_audit_job": (
    "engine/strategy/jobs/trade_attribution_audit_job.py",
    "oneshot",
    None,
    {"execution": False},
),

"trade_lifecycle_audit_job": (
    "engine/strategy/jobs/trade_lifecycle_audit_job.py",
    "oneshot",
    None,
    {"execution": False},
),

"audit_chain_verify": (
    "engine/strategy/jobs/audit_chain_verify.py",
    "oneshot",
    None,
    {
        "execution": False,
        "pipeline_stage": "audit",
        "resource_class": "maintenance",
        "resource_priority": 25,
        "slot_cost": 1,
    },
),

"refresh_marketplace_replay": (
    "engine/strategy/jobs/refresh_marketplace_replay.py",
    "oneshot",
    None,
    {
        "execution": False,
        "resource_class": "replay",
        "resource_priority": 30,
        "slot_cost": 1,
    },
),

"universe_discovery": (
    "engine/strategy/jobs/universe_discovery_job.py",
    "oneshot",
    None,
    {"execution": False},
),

# ---------------------------
# Execution
# ---------------------------

"portfolio_rebalance": (
    "engine/execution/jobs/portfolio_rebalance.py",
    "oneshot",
    None,
    {
        "execution": True,
        "schedule": "every 300s (gap recovery)",
        "cadence_seconds": 300,
        "resource_class": "execution",
        "resource_priority": 100,
        "slot_cost": 1,
    },
),

"broker_apply_orders": (
    "engine/execution/jobs/broker_apply_orders.py",
    "oneshot",
    None,
    {
        "execution": True,
        "resource_class": "execution",
        "resource_priority": 100,
        "slot_cost": 1,
    },
),

"trade_pipeline": (
    "engine/strategy/jobs/trade_pipeline_job.py",
    "oneshot",
    None,
    {
        "execution": True,
        "resource_class": "execution",
        "resource_priority": 100,
        "slot_cost": 1,
    },
),

"execution_poll_and_attrib": (
    "engine/execution/jobs/execution_poll_and_attrib.py",
    "oneshot",
    None,
    {
        "execution": True,
        "resource_class": "execution",
        "resource_priority": 100,
        "slot_cost": 1,
    },
),

"alpaca_trade_updates_stream": (
    "engine/execution/jobs/alpaca_trade_updates_stream.py",
    "daemon",
    None,
    {
        "execution": True,
        "resource_class": "execution",
        "resource_priority": 100,
        "slot_cost": 1,
        "streaming": True,
        "gap_recovery_job": "execution_poll_and_attrib",
        "requires_secret": "ALPACA_KEY_ID,ALPACA_SECRET_KEY",
    },
),

"compute_exec_labels": (
    "engine/execution/jobs/compute_exec_labels.py",
    "oneshot",
    None,
    {"execution": False},
),

"compute_exec_labels_from_fills": (
    "engine/execution/jobs/compute_exec_labels_from_fills.py",
    "oneshot",
    None,
    {"execution": False},
),

"compute_exec_z": (
    "engine/execution/jobs/compute_exec_z.py",
    "oneshot",
    None,
    {"execution": False},
),

"model_competition": (
    "engine/execution/jobs/model_competition.py",
    "oneshot",
    None,
    {"execution": False},
),

"repair_trade_attribution_history": (
    "engine/execution/jobs/repair_trade_attribution_history.py",
    "oneshot",
    None,
    {"execution": False},
),

}


# The canonical target state is zero untracked engine/**/jobs files. Keep this
# set empty unless a job file is intentionally quarantined during an active
# migration.
QUARANTINED_JOB_FILES = frozenset()


def _build_pipeline_order() -> List[str]:
    order: List[str] = [
        "update_universe",
    ]
    if _pit_universe_backfill_enabled():
        order.append("backfill_universe_pit")
    order.extend(
        [
            "ingest_options",
            "process_events",
        ]
    )
    if _finbert_pipeline_enabled():
        order.append("process_finbert_sentiment")
    if _nlp_pipeline_enabled():
        order.extend(["embed_news", "process_news_flow", "embed_filings", "embed_transcripts"])
    order.extend(
        [
            "label_due_events",
            "fill_ensemble_oos_targets",
            "triple_barrier_labels",
        ]
    )
    if _causal_scoring_enabled():
        order.append("causal_scoring")
    order.append("compute_drift")
    if _hmm_pipeline_enabled():
        order.append("train_hmm_regime")
    order.append("har_rv_forecast")
    order.append("bocpd_regime_update")
    order.append("shadow_metrics")
    if _gbm_pipeline_enabled():
        order.append("train_gbm_regressor")
    if _model_family_pipeline_enabled("lgbm_regressor", "USE_LGBM_REGRESSOR"):
        order.append("train_lgbm_models")
    if _model_family_pipeline_enabled("lgbm_ranker", "USE_LGBM_RANKER"):
        order.append("train_lgbm_ranker_models")
    order.append("train_meta_label_model")
    if _model_family_pipeline_enabled("xgb_regressor", "USE_XGB_REGRESSOR"):
        order.append("train_xgb_models")
    if _model_family_pipeline_enabled("patchtst", "USE_PATCHTST"):
        order.append("pretrain_patchtst_models")
        order.append("train_patchtst_models")
    if _model_family_pipeline_enabled("itransformer", "USE_ITRANSFORMER"):
        order.append("train_itransformer_models")
    if _env_flag("TUNE_MODELS_ENABLED", False):
        order.append("tune_models")
    order.extend(
        [
            "train_ensemble",
            "train_ensemble_meta",
            "model_lifecycle_manager",
        ]
    )
    if _drift_retrain_pipeline_enabled():
        order.append("drift_triggered_retrain")
    order.append("alpha_discovery_loop")
    if _cpcv_pipeline_enabled():
        order.append("backtest_cpcv")
    order.extend(
        [
            "eval_temporal_shadow",
            "promote_temporal_models",
            "validate_now",
            "refresh_marketplace_replay",
            "portfolio_rebalance",
            "broker_apply_orders",
            "execution_poll_and_attrib",
            "model_competition",
        ]
    )
    return list(dict.fromkeys(order))


PIPELINE_ORDER = _build_pipeline_order()


JOB_ORDER = [

    # --- Runtime coordination (must start first) ---
    "ingestion_runtime",

    # Monitoring
    "provider_monitor",
    "metrics_collector",
    "kill_switch_cache_refresh",
    "inference_health_probe",
    "observability_snapshot",
    "snapshot_equity",
]

def _build_ingestion_daemon_jobs() -> List[str]:
    jobs = list(default_ingestion_pipeline_jobs())
    if _tsfresh_pipeline_enabled():
        jobs.append("compute_tsfresh_snapshots")
    return list(dict.fromkeys(jobs))


INGESTION_DAEMON_JOBS = _build_ingestion_daemon_jobs()


def validate_job_registry_paths(
    repo_root: str | Path | None = None,
    *,
    import_check: bool = False,
) -> Dict[str, Any]:
    base = Path(repo_root) if repo_root else Path(__file__).resolve().parents[2]
    root = os.path.abspath(str(base))

    errors: List[str] = []

    for name in _source_allowed_job_duplicates():
        errors.append(f"allowed_jobs_duplicate:{name}")

    for order_name, order_values in (
        ("PIPELINE_ORDER", PIPELINE_ORDER),
        ("JOB_ORDER", JOB_ORDER),
    ):
        seen = set()
        for name in order_values or []:
            if name in seen:
                errors.append(f"{order_name}_duplicate:{name}")
                continue
            seen.add(name)
            if name not in ALLOWED_JOBS:
                errors.append(f"{order_name}_unknown_job:{name}")

    discovered_job_files = _discover_repo_job_files(root)
    registered_job_files = _registered_job_script_paths()
    quarantined_job_files = {Path(path).as_posix() for path in QUARANTINED_JOB_FILES}

    for job_file in sorted(quarantined_job_files):
        if job_file in registered_job_files:
            errors.append(f"quarantined_job_registered:{job_file}")
        elif job_file not in discovered_job_files:
            errors.append(f"quarantined_job_missing_file:{job_file}")

    for job_file in sorted(discovered_job_files):
        if job_file not in registered_job_files and job_file not in quarantined_job_files:
            errors.append(f"untracked_job_file:{job_file}")

    price_daemons = [
        name
        for name, spec in ALLOWED_JOBS.items()
        if isinstance(spec, (list, tuple))
        and len(spec) >= 4
        and (
            spec[2] == "price_feed"
            or (isinstance(spec[3], dict) and spec[3].get("fallback_feed"))
        )
    ]

    # ensure at least one fallback exists
    if not price_daemons and "poll_prices" in ALLOWED_JOBS:
        price_daemons = ["poll_prices"]

    if not price_daemons:
        errors.append("no_price_daemon_registered")

    isolated_required = False

    for job_name, spec in ALLOWED_JOBS.items():
        if isinstance(spec, (tuple, list)) and len(spec) >= 4:
            if spec[3].get("isolated_market_data"):
                isolated_required = True

        if not isinstance(spec, (tuple, list)) or len(spec) < 2:
            errors.append(f"invalid_spec:{job_name}")
            continue

        script_rel = str(spec[0] or "").strip()
        mode = str(spec[1] or "").strip().lower()

        if not script_rel:
            errors.append(f"missing_script:{job_name}")
        else:
            script_abs = os.path.abspath(os.path.join(root, script_rel))
            script_abs = os.path.normpath(script_abs)

            if not script_abs.startswith(root):
                errors.append(f"script_outside_repo:{job_name}:{script_rel}")
            elif not os.path.exists(script_abs):
                errors.append(f"missing_script_path:{job_name}:{script_rel}")
            elif not os.path.isfile(script_abs):
                errors.append(f"invalid_script_type:{job_name}:{script_rel}")
            elif not script_abs.endswith(".py"):
                errors.append(f"invalid_script_extension:{job_name}:{script_rel}")
            else:
                try:
                    py_compile.compile(script_abs, doraise=True)
                except Exception as e:
                    errors.append(f"invalid_python_entry:{job_name}:{script_rel}:{type(e).__name__}:{e}")
                else:
                    if not _has_valid_script_entrypoint(script_abs):
                        errors.append(f"missing_callable_entrypoint:{job_name}:{script_rel}")
                    elif import_check and job_name in set(PIPELINE_ORDER or []).union(set(JOB_ORDER or [])):
                        try:
                            module_name = ".".join(Path(script_rel).with_suffix("").parts)
                            importlib.import_module(module_name)
                        except Exception as e:
                            errors.append(f"invalid_import_entry:{job_name}:{script_rel}:{type(e).__name__}:{e}")

        if mode not in ("daemon", "oneshot"):
            errors.append(f"invalid_mode:{job_name}:{mode}")

    if isolated_required and "ingestion_runtime" not in ALLOWED_JOBS:
        errors.append("isolated_market_data_requires_ingestion_runtime")

    return {
        "ok": len(errors) == 0,
        "errors": errors,
    }


def validate_runtime_architecture(
    repo_root: str | Path | None = None,
    *,
    import_check: bool = False,
) -> Dict[str, Any]:
    registry_check = validate_job_registry_paths(repo_root=repo_root, import_check=import_check)
    errors: List[str] = list(registry_check.get("errors") or [])

    pipeline_seen = set()

    if not PIPELINE_ORDER:
        errors.append("pipeline_order_empty")

    for job_name in PIPELINE_ORDER:
        if job_name in pipeline_seen:
            errors.append(f"pipeline_duplicate_job:{job_name}")
        pipeline_seen.add(job_name)

    boot_seen = set()
    for job_name in JOB_ORDER:
        if job_name in boot_seen:
            errors.append(f"boot_duplicate_job:{job_name}")
        boot_seen.add(job_name)

    for job_name in PIPELINE_ORDER:
        spec = ALLOWED_JOBS.get(job_name)
        if not isinstance(spec, (list, tuple)) or len(spec) < 2:
            errors.append(f"pipeline_invalid_spec:{job_name}")
            continue

        mode = str(spec[1] or "").strip().lower()
        if mode != "oneshot":
            errors.append(f"pipeline_non_oneshot_job:{job_name}:{mode}")

    for job_name in JOB_ORDER:
        spec = ALLOWED_JOBS.get(job_name)

        # execution jobs must never auto-boot
        if spec and len(spec) >= 4 and isinstance(spec[3], dict):
            if spec[3].get("execution") is True:
                errors.append(f"boot_execution_job_forbidden:{job_name}")
        if not isinstance(spec, (list, tuple)) or len(spec) < 2:
            errors.append(f"boot_invalid_spec:{job_name}")
            continue

        mode = str(spec[1] or "").strip().lower()
        if mode != "daemon":
            errors.append(f"boot_non_daemon_job:{job_name}:{mode}")

    return {
        "ok": len(errors) == 0,
        "errors": errors,
    }


def get_job_spec(job_name: str) -> Optional[tuple]:
    return ALLOWED_JOBS.get(str(job_name or "").strip())


def get_job_meta(job_name: str) -> Dict[str, Any]:
    spec = get_job_spec(job_name)
    if not isinstance(spec, (tuple, list)):
        return {}
    if len(spec) < 4:
        return {}
    if not isinstance(spec[3], dict):
        return {}
    return dict(spec[3])


def is_execution_job(job_name: str) -> bool:
    return bool(get_job_meta(job_name).get("execution") is True)


def is_price_feed_job(job_name: str) -> bool:
    spec = get_job_spec(job_name)
    if not isinstance(spec, (tuple, list)) or len(spec) < 3:
        return False
    return str(spec[2] or "").strip() == "price_feed"


def is_market_data_job(job_name: str) -> bool:
    return str(job_name or "").strip() == "ingestion_runtime" or is_price_feed_job(job_name)


def is_offline_workload_job(job_name: str) -> bool:
    name = str(job_name or "").strip().lower()
    meta = get_job_meta(str(job_name))
    resource_class = str(meta.get("resource_class") or "").strip().lower()
    stage = str(meta.get("pipeline_stage") or "").strip().lower()
    if resource_class in {"training", "replay"}:
        return True
    if stage and any(token in stage for token in ("train", "tune", "backtest", "discovery", "replay")):
        return True
    if name.startswith(("train_", "pretrain_", "tune_")):
        return True
    if name in {
        "pipeline_train_and_eval",
        "shadow_train",
        "drift_triggered_retrain",
        "alpha_discovery_loop",
        "discover_features",
        "llm_factor_discovery",
        "backtest_cpcv",
        "backtest_walk_forward",
        "portfolio_backtest",
    }:
        return True
    return False


def get_price_feed_jobs() -> List[str]:
    ranked = []

    for name, spec in ALLOWED_JOBS.items():
        if not isinstance(spec, (tuple, list)) or len(spec) < 4:
            continue
        if str(spec[1] or "").strip().lower() != "daemon":
            continue
        if not is_price_feed_job(name):
            continue

        meta = spec[3] if isinstance(spec[3], dict) else {}
        rank = (
            0 if bool(meta.get("primary_feed")) else 1,
            0 if bool(meta.get("gateway_feed")) else 1,
            1 if bool(meta.get("fallback_feed")) else 0,
            str(name),
        )
        ranked.append((rank, str(name)))

    ranked.sort(key=lambda row: row[0])
    return [name for _, name in ranked]


def get_boot_jobs() -> List[str]:
    out: List[str] = []
    for name in JOB_ORDER:
        spec = get_job_spec(name)
        if not isinstance(spec, (tuple, list)) or len(spec) < 2:
            continue
        if str(spec[1] or "").strip().lower() != "daemon":
            continue
        out.append(str(name))
    return out
