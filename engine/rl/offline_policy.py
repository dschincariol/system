"""Offline portfolio RL policy baselines and OPE persistence."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping

import numpy as np

from engine.rl.offline_dataset import OfflineRLDataset
from engine.rl.wrappers import clip_and_normalize_action
from engine.runtime.platform import default_local_models_dir
from engine.strategy.ope_gate import evaluate_policy_ope_gate, record_policy_ope_observation


BEHAVIOR_CLONING_FAMILIES = {"behavior_cloning", "bc"}
OPTIONAL_OFFLINE_FAMILY_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "iql": ("d3rlpy",),
    "cql": ("d3rlpy",),
    "decision_transformer": ("torch", "transformers"),
}


def _default_policy_root() -> str:
    return str((default_local_models_dir() / "rl" / "offline" / "policies").resolve())


@dataclass(frozen=True)
class OfflinePolicyConfig:
    family: str = "behavior_cloning"
    model_name: str = "offline_rl_behavior_cloning"
    candidate_version: str = ""
    model_id: str = "offline_rl_behavior_cloning"
    ridge_l2: float = 1.0e-4
    max_w: float = 0.35
    leverage_cap: float = 1.0
    artifact_root: str = field(default_factory=_default_policy_root)

    def to_dict(self) -> dict[str, Any]:
        return {
            "family": str(self.family),
            "model_name": str(self.model_name),
            "candidate_version": str(self.candidate_version),
            "model_id": str(self.model_id),
            "ridge_l2": float(self.ridge_l2),
            "max_w": float(self.max_w),
            "leverage_cap": float(self.leverage_cap),
            "artifact_root": str(self.artifact_root),
        }


@dataclass(frozen=True)
class OptionalFamilyStatus:
    family: str
    available: bool
    missing_dependencies: tuple[str, ...]
    dependencies: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "family": str(self.family),
            "available": bool(self.available),
            "missing_dependencies": list(self.missing_dependencies),
            "dependencies": list(self.dependencies),
        }


class OptionalOfflineFamilyUnavailable(RuntimeError):
    """Raised when an optional offline RL family is selected without deps."""


def normalize_offline_family(family: Any) -> str:
    value = str(family or "behavior_cloning").strip().lower().replace("-", "_")
    if value in BEHAVIOR_CLONING_FAMILIES:
        return "behavior_cloning"
    if value in {"dt", "decisiontransformer"}:
        return "decision_transformer"
    return value


def optional_family_status(
    family: Any,
    *,
    import_fn: Callable[[str], Any] | None = None,
) -> OptionalFamilyStatus:
    fam = normalize_offline_family(family)
    deps = OPTIONAL_OFFLINE_FAMILY_DEPENDENCIES.get(fam, tuple())
    if not deps:
        return OptionalFamilyStatus(family=fam, available=fam == "behavior_cloning", missing_dependencies=tuple(), dependencies=tuple())
    importer = import_fn or __import__
    missing: list[str] = []
    for dep in deps:
        try:
            importer(str(dep))
        except Exception:
            missing.append(str(dep))
    return OptionalFamilyStatus(family=fam, available=not missing, missing_dependencies=tuple(missing), dependencies=tuple(deps))


def ensure_optional_family_available(
    family: Any,
    *,
    import_fn: Callable[[str], Any] | None = None,
) -> OptionalFamilyStatus:
    status = optional_family_status(family, import_fn=import_fn)
    if status.family == "behavior_cloning":
        return status
    if not bool(status.available):
        missing = ",".join(status.missing_dependencies)
        raise OptionalOfflineFamilyUnavailable(
            f"offline RL family {status.family!r} requires optional dependencies: {missing}"
        )
    raise NotImplementedError(
        f"offline RL family {status.family!r} dependency adapter is available but not enabled as a production baseline"
    )


class BehaviorCloningPolicy:
    """Linear ridge behavior-cloning baseline for target portfolio weights."""

    def __init__(
        self,
        *,
        coefficients: np.ndarray,
        intercept: np.ndarray,
        config: OfflinePolicyConfig,
        dataset_hash: str,
        behavior_policy: Mapping[str, Any] | None = None,
        diagnostics: Mapping[str, Any] | None = None,
    ):
        self.coefficients = np.asarray(coefficients, dtype=np.float64)
        self.intercept = np.asarray(intercept, dtype=np.float64).reshape(-1)
        if self.coefficients.ndim != 2:
            raise ValueError("coefficients must be 2D")
        if self.intercept.shape[0] != self.coefficients.shape[1]:
            raise ValueError("intercept dimension must match action dimension")
        self.config = config
        self.dataset_hash = str(dataset_hash)
        self.behavior_policy = dict(behavior_policy or {})
        self.diagnostics = dict(diagnostics or {})

    @property
    def action_dim(self) -> int:
        return int(self.coefficients.shape[1])

    @property
    def observation_dim(self) -> int:
        return int(self.coefficients.shape[0])

    def predict(self, observation: Any, deterministic: bool = True) -> np.ndarray:
        del deterministic
        obs = np.asarray(observation, dtype=np.float64).reshape(-1)
        if obs.shape[0] < self.observation_dim:
            obs = np.pad(obs, (0, self.observation_dim - obs.shape[0]))
        elif obs.shape[0] > self.observation_dim:
            obs = obs[: self.observation_dim]
        raw = obs @ self.coefficients + self.intercept
        return clip_and_normalize_action(raw, max_w=float(self.config.max_w), leverage_cap=float(self.config.leverage_cap))

    def parameter_bytes(self) -> bytes:
        payload = {
            "coefficients": np.asarray(self.coefficients, dtype=np.float64).round(12).tolist(),
            "intercept": np.asarray(self.intercept, dtype=np.float64).round(12).tolist(),
            "config": self.config.to_dict(),
            "dataset_hash": str(self.dataset_hash),
        }
        return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")

    def policy_hash32(self) -> str:
        return hashlib.sha256(self.parameter_bytes()).hexdigest()[:8]

    def save(self, path: str | Path) -> Path:
        root = Path(path)
        root.mkdir(parents=True, exist_ok=True)
        model_payload = {
            "family": "behavior_cloning",
            "coefficients": np.asarray(self.coefficients, dtype=np.float64).tolist(),
            "intercept": np.asarray(self.intercept, dtype=np.float64).tolist(),
            "config": self.config.to_dict(),
            "dataset_hash": str(self.dataset_hash),
            "behavior_policy": dict(self.behavior_policy),
            "diagnostics": dict(self.diagnostics),
        }
        metadata = {
            "family": "behavior_cloning",
            "model_name": str(self.config.model_name),
            "model_id": str(self.config.model_id),
            "candidate_version": str(self.config.candidate_version),
            "dataset_hash": str(self.dataset_hash),
            "policy_hash32": self.policy_hash32(),
            "created_ts_ms": int(time.time() * 1000),
            "shadow_only": True,
            "live_order_authority": False,
            "optional_families": {
                family: optional_family_status(family).to_dict()
                for family in sorted(OPTIONAL_OFFLINE_FAMILY_DEPENDENCIES)
            },
        }
        (root / "model.json").write_text(json.dumps(model_payload, indent=2, sort_keys=True), encoding="utf-8")
        (root / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
        return root

    @classmethod
    def load(cls, path: str | Path) -> "BehaviorCloningPolicy":
        root = Path(path)
        payload = json.loads((root / "model.json").read_text(encoding="utf-8"))
        cfg = OfflinePolicyConfig(**dict(payload.get("config") or {}))
        return cls(
            coefficients=np.asarray(payload.get("coefficients") or [], dtype=np.float64),
            intercept=np.asarray(payload.get("intercept") or [], dtype=np.float64),
            config=cfg,
            dataset_hash=str(payload.get("dataset_hash") or ""),
            behavior_policy=dict(payload.get("behavior_policy") or {}),
            diagnostics=dict(payload.get("diagnostics") or {}),
        )


def train_behavior_cloning_policy(dataset: OfflineRLDataset, config: OfflinePolicyConfig | Mapping[str, Any] | None = None) -> BehaviorCloningPolicy:
    cfg = config if isinstance(config, OfflinePolicyConfig) else OfflinePolicyConfig(**dict(config or {}))
    family = normalize_offline_family(cfg.family)
    if family != "behavior_cloning":
        ensure_optional_family_available(family)
    if not dataset.transitions:
        raise ValueError("offline RL dataset has no transitions")
    x = np.asarray([row.observation for row in dataset.transitions], dtype=np.float64)
    y = np.asarray([row.action for row in dataset.transitions], dtype=np.float64)
    if x.ndim != 2 or y.ndim != 2:
        raise ValueError("offline RL dataset observations/actions must be 2D")
    x_aug = np.concatenate([x, np.ones((x.shape[0], 1), dtype=np.float64)], axis=1)
    reg = np.eye(x_aug.shape[1], dtype=np.float64) * max(0.0, float(cfg.ridge_l2))
    reg[-1, -1] = 0.0
    beta = np.linalg.pinv(x_aug.T @ x_aug + reg) @ x_aug.T @ y
    coef = beta[:-1, :]
    intercept = beta[-1, :]
    preds = x @ coef + intercept
    mse = float(np.mean(np.square(preds - y))) if y.size else 0.0
    diagnostics = {
        "rows": int(x.shape[0]),
        "observation_dim": int(x.shape[1]),
        "action_dim": int(y.shape[1]),
        "train_mse": float(mse),
        "dataset_diagnostics": dict(dataset.diagnostics),
    }
    return BehaviorCloningPolicy(
        coefficients=coef,
        intercept=intercept,
        config=cfg,
        dataset_hash=str(dataset.dataset_hash),
        behavior_policy=dict(dataset.behavior_policy),
        diagnostics=diagnostics,
    )


def default_policy_artifact_dir(policy: BehaviorCloningPolicy) -> Path:
    return Path(policy.config.artifact_root) / "behavior_cloning" / f"{int(time.time() * 1000)}_{policy.policy_hash32()}"


def persist_offline_ope_inputs(
    dataset: OfflineRLDataset,
    *,
    con: Any,
    policy: BehaviorCloningPolicy | None = None,
    model_id: str | None = None,
    model_name: str | None = None,
    candidate_version: str | None = None,
    source_table: str = "offline_rl_dataset",
) -> list[int]:
    """Persist dataset transitions as canonical policy OPE observations."""

    ids: list[int] = []
    policy_model_id = str(model_id or (policy.config.model_id if policy else dataset.config.model_name))
    policy_model_name = str(model_name or (policy.config.model_name if policy else dataset.config.model_name))
    version = str(candidate_version if candidate_version is not None else (policy.config.candidate_version if policy else dataset.config.candidate_version))
    for idx, transition in enumerate(dataset.transitions):
        target_action = str(transition.target_action)
        target_propensity = float(transition.target_propensity)
        target_model_estimate = float(transition.target_model_estimate)
        if policy is not None:
            predicted = policy.predict(transition.observation)
            predicted_action = "weights:" + ",".join(
                f"{sym}={float(predicted[pos]) if pos < len(predicted) else 0.0:.8f}"
                for pos, sym in enumerate(transition.universe)
            )
            target_action = predicted_action
            target_propensity = 1.0 if np.allclose(np.asarray(predicted), np.asarray(transition.action), atol=1e-4) else 0.0
            target_model_estimate = float(transition.target_model_estimate)
        row_id = record_policy_ope_observation(
            con=con,
            model_id=policy_model_id,
            model_name=policy_model_name,
            candidate_type="rl",
            candidate_version=version,
            symbol="",
            horizon_s=max(1, int(round(float(dataset.config.horizon_ms) / 1000.0))),
            regime="global",
            logged_action=str(transition.logged_action),
            target_action=str(target_action),
            behavior_propensity=float(transition.behavior_propensity),
            target_propensity=float(target_propensity),
            outcome=float(transition.outcome),
            logged_model_estimate=float(transition.logged_model_estimate),
            target_model_estimate=float(target_model_estimate),
            source_table=str(source_table),
            source_id=f"{dataset.dataset_hash}:{idx}",
            ts_ms=int(transition.ts_ms),
            meta={
                "offline_rl": True,
                "shadow_only": True,
                "dataset_hash": str(dataset.dataset_hash),
                "obs_hash": str(transition.obs_hash),
                "source_ids": list(transition.source_ids),
                "reward": dict((transition.meta or {}).get("reward") or {}),
            },
        )
        ids.append(int(row_id))
    return ids


def evaluate_offline_policy_ope(
    dataset: OfflineRLDataset,
    *,
    con: Any,
    policy: BehaviorCloningPolicy | None = None,
    config: Mapping[str, Any] | None = None,
    persist_inputs: bool = True,
    persist_evidence: bool = True,
) -> tuple[bool, dict[str, Any]]:
    if persist_inputs:
        persist_offline_ope_inputs(dataset, con=con, policy=policy)
    model_id = str(policy.config.model_id if policy else dataset.config.model_name)
    model_name = str(policy.config.model_name if policy else dataset.config.model_name)
    candidate_version = str(policy.config.candidate_version if policy else dataset.config.candidate_version)
    return evaluate_policy_ope_gate(
        con=con,
        model_id=model_id,
        model_name=model_name,
        candidate_type="rl",
        candidate_version=candidate_version,
        symbol="",
        horizon_s=max(1, int(round(float(dataset.config.horizon_ms) / 1000.0))),
        regime="global",
        metadata={"offline_rl": True, "dataset_hash": str(dataset.dataset_hash)},
        config=config,
        persist=bool(persist_evidence),
    )
