"""Train an offline, risk-sensitive shadow-only portfolio RL baseline."""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Sequence

from engine.rl.offline_dataset import (
    OfflineDatasetConfig,
    RiskSensitiveRewardConfig,
    build_offline_rl_dataset,
    save_offline_dataset,
)
from engine.rl.offline_policy import (
    OfflinePolicyConfig,
    default_policy_artifact_dir,
    evaluate_offline_policy_ope,
    normalize_offline_family,
    train_behavior_cloning_policy,
)
from engine.rl.offline_shadow import log_offline_shadow_decisions
from engine.runtime import storage
from engine.runtime.platform import default_local_models_dir


def _csv(value: str, default: Sequence[str]) -> list[str]:
    parts = [p.strip().upper() for p in str(value or "").split(",") if p.strip()]
    return parts or [str(x).upper() for x in default]


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on", "y"}


def _optional_int_env(name: str) -> int | None:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def _reward_config_from_env() -> RiskSensitiveRewardConfig:
    return RiskSensitiveRewardConfig(
        fallback_cost_bps=_env_float("RL_OFFLINE_FALLBACK_COST_BPS", 1.0),
        drawdown_penalty=_env_float("RL_OFFLINE_DRAWDOWN_PENALTY", 1.0),
        drawdown_threshold=_env_float("RL_OFFLINE_DRAWDOWN_THRESHOLD", 0.08),
        turnover_penalty=_env_float("RL_OFFLINE_TURNOVER_PENALTY", 0.01),
        slippage_penalty=_env_float("RL_OFFLINE_SLIPPAGE_PENALTY", 1.0),
        concentration_penalty=_env_float("RL_OFFLINE_CONCENTRATION_PENALTY", 0.01),
        cvar_penalty=_env_float("RL_OFFLINE_CVAR_PENALTY", 1.0),
        cvar_alpha=_env_float("RL_OFFLINE_CVAR_ALPHA", 0.10),
        cvar_window=_env_int("RL_OFFLINE_CVAR_WINDOW", 50),
    )


def _ope_overrides_from_env() -> dict[str, Any]:
    mapping = {
        "RL_OFFLINE_OPE_MIN_OBS": "min_obs",
        "RL_OFFLINE_OPE_MIN_EFFECTIVE_N": "min_effective_n",
        "RL_OFFLINE_OPE_MIN_SUPPORT": "min_support",
        "RL_OFFLINE_OPE_MAX_IMPORTANCE_WEIGHT": "max_importance_weight",
        "RL_OFFLINE_OPE_MIN_POLICY_VALUE_LOWER_BOUND": "min_policy_value_lower_bound",
        "RL_OFFLINE_OPE_MAX_STANDARD_ERROR": "max_standard_error",
        "RL_OFFLINE_OPE_MAX_CI_WIDTH": "max_ci_width",
        "RL_OFFLINE_OPE_MAX_MODEL_OPTIMISM": "max_model_optimism",
    }
    out: dict[str, Any] = {}
    for env_name, key in mapping.items():
        raw = str(os.environ.get(env_name, "") or "").strip()
        if not raw:
            continue
        if key == "min_obs":
            out[key] = _env_int(env_name, 50)
        else:
            out[key] = _env_float(env_name, 0.0)
    return out


def _dataset_config(args: argparse.Namespace) -> OfflineDatasetConfig:
    feature_default = "price.pct_ret_1d,price.momentum_1d,price.rv_20"
    artifact_root = str(
        args.artifact_root
        or os.environ.get("RL_OFFLINE_ARTIFACT_ROOT")
        or (default_local_models_dir() / "rl" / "offline").resolve()
    )
    return OfflineDatasetConfig(
        universe=_csv(str(args.symbols), []),
        feature_ids=[part.strip() for part in str(args.feature_ids or feature_default).split(",") if part.strip()],
        feature_set_tag=str(args.feature_set_tag),
        start_ts_ms=args.start_ts_ms,
        end_ts_ms=args.end_ts_ms,
        horizon_ms=int(args.horizon_ms),
        max_w=float(args.max_w),
        leverage_cap=float(args.leverage_cap),
        min_rows=int(args.min_rows),
        require_outcomes=not bool(args.allow_missing_outcomes),
        reward=_reward_config_from_env(),
        artifact_root=artifact_root,
        model_name=str(args.model_name),
        candidate_version=str(args.candidate_version),
    )


def _policy_config(args: argparse.Namespace, dataset_config: OfflineDatasetConfig) -> OfflinePolicyConfig:
    return OfflinePolicyConfig(
        family=normalize_offline_family(args.family),
        model_name=str(args.model_name),
        candidate_version=str(args.candidate_version),
        model_id=str(args.model_id or args.model_name),
        ridge_l2=float(args.ridge_l2),
        max_w=float(dataset_config.max_w),
        leverage_cap=float(dataset_config.leverage_cap),
        artifact_root=str(dataset_config.artifact_root) + "/policies",
    )


def main(argv: Sequence[str] | None = None) -> dict[str, Any]:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--family", default=os.environ.get("RL_OFFLINE_POLICY_FAMILY", "behavior_cloning"))
    parser.add_argument("--symbols", default=os.environ.get("RL_OFFLINE_SYMBOLS", os.environ.get("RL_PORTFOLIO_SYMBOLS", "")))
    parser.add_argument("--feature-ids", default=os.environ.get("RL_OFFLINE_FEATURE_IDS", os.environ.get("RL_PORTFOLIO_FEATURE_IDS", "")))
    parser.add_argument("--feature-set-tag", default=os.environ.get("RL_OFFLINE_FEATURE_SET_TAG", "default"))
    parser.add_argument("--start-ts-ms", type=int, default=_optional_int_env("RL_OFFLINE_START_TS_MS"))
    parser.add_argument("--end-ts-ms", type=int, default=_optional_int_env("RL_OFFLINE_END_TS_MS"))
    parser.add_argument("--horizon-ms", type=int, default=_env_int("RL_OFFLINE_HORIZON_MS", 86_400_000))
    parser.add_argument("--min-rows", type=int, default=_env_int("RL_OFFLINE_MIN_ROWS", 50))
    parser.add_argument("--max-w", type=float, default=_env_float("RL_OFFLINE_MAX_W", _env_float("RL_PORTFOLIO_MAX_W", 0.35)))
    parser.add_argument(
        "--leverage-cap",
        type=float,
        default=_env_float("RL_OFFLINE_LEVERAGE_CAP", _env_float("RL_PORTFOLIO_LEVERAGE_CAP", 1.0)),
    )
    parser.add_argument("--ridge-l2", type=float, default=_env_float("RL_OFFLINE_BC_RIDGE_L2", 1.0e-4))
    parser.add_argument("--model-name", default=os.environ.get("RL_OFFLINE_MODEL_NAME", "offline_rl_behavior_cloning"))
    parser.add_argument("--model-id", default=os.environ.get("RL_OFFLINE_MODEL_ID", ""))
    parser.add_argument("--candidate-version", default=os.environ.get("RL_OFFLINE_CANDIDATE_VERSION", ""))
    parser.add_argument("--artifact-root", default=os.environ.get("RL_OFFLINE_ARTIFACT_ROOT", ""))
    parser.add_argument("--allow-missing-outcomes", action="store_true", default=_env_bool("RL_OFFLINE_ALLOW_MISSING_OUTCOMES", False))
    parser.add_argument("--emit-shadow-from-last", action="store_true", default=_env_bool("RL_OFFLINE_EMIT_SHADOW_FROM_LAST", False))
    args = parser.parse_args(list(argv) if argv is not None else None)

    storage.init_db()
    con = storage.connect()
    try:
        dataset_config = _dataset_config(args)
        dataset = build_offline_rl_dataset(con, dataset_config)
        dataset_dir = save_offline_dataset(dataset)
        if int(dataset.diagnostics.get("rows") or 0) < int(dataset_config.min_rows):
            result = {
                "ok": False,
                "status": "insufficient_dataset",
                "dataset_hash": str(dataset.dataset_hash),
                "dataset_path": str(dataset_dir),
                "diagnostics": dict(dataset.diagnostics),
            }
            print(json.dumps(result, indent=2, sort_keys=True))
            return result

        policy_config = _policy_config(args, dataset_config)
        policy = train_behavior_cloning_policy(dataset, policy_config)
        policy_dir = policy.save(default_policy_artifact_dir(policy))
        ope_ok, ope_payload = evaluate_offline_policy_ope(
            dataset,
            con=con,
            policy=policy,
            config=_ope_overrides_from_env() or None,
            persist_inputs=True,
            persist_evidence=True,
        )
        shadow_result: dict[str, Any] | None = None
        if bool(args.emit_shadow_from_last) and dataset.transitions:
            last = dataset.transitions[-1]
            shadow_result = log_offline_shadow_decisions(
                con=con,
                policy=policy,
                universe=last.universe,
                observation=last.observation,
                live_weights={},
                ts_ms=int(last.ts_ms),
                evidence=ope_payload,
            )
        con.commit()
        result = {
            "ok": True,
            "status": "trained",
            "family": normalize_offline_family(args.family),
            "dataset_hash": str(dataset.dataset_hash),
            "dataset_path": str(dataset_dir),
            "policy_path": str(policy_dir),
            "policy_hash32": policy.policy_hash32(),
            "behavior_policy": dict(dataset.behavior_policy),
            "dataset_diagnostics": dict(dataset.diagnostics),
            "ope_ok": bool(ope_ok),
            "ope": dict(ope_payload),
            "shadow": shadow_result,
        }
        print(json.dumps(result, indent=2, sort_keys=True))
        return result
    finally:
        con.close()


if __name__ == "__main__":
    main()
