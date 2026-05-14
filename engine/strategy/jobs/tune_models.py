"""Generic Optuna hyperparameter tuning job."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)
from engine.strategy.tuning.catalog import catalog_for_family
from engine.strategy.tuning.objective import build_quadratic_smoke_objective
from engine.strategy.tuning.study import open_study, record_best_params

JOB_NAME = "tune_models"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))

ObjectiveFactory = Callable[[str, str], Callable[[Any], float]]


def _csv(value: str | None, default: Iterable[str]) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return [str(item) for item in default]
    return [part.strip() for part in text.split(",") if part.strip()]


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except Exception:
        return int(default)


def _objective_for(
    *,
    model_family: str,
    symbol: str,
    objective_factory: ObjectiveFactory | None,
    allow_smoke_objective: bool,
) -> Callable[[Any], float]:
    if objective_factory is not None:
        return objective_factory(str(model_family), str(symbol))
    if allow_smoke_objective:
        return build_quadratic_smoke_objective(str(model_family))
    raise RuntimeError(
        "No tuning objective factory was provided. Production tuning must pass a real "
        "training/validation objective; set TUNE_MODE=smoke only for local smoke tests."
    )


def run_tuning_job(
    *,
    model_families: Iterable[str] | None = None,
    symbols: Iterable[str] | None = None,
    n_trials: int | None = None,
    timeout_s: int | None = None,
    seed: int | None = None,
    objective_factory: ObjectiveFactory | None = None,
    allow_smoke_objective: bool | None = None,
) -> dict[str, Any]:
    families = [str(f).strip() for f in (model_families or _csv(os.environ.get("TUNE_MODEL_FAMILIES"), ("temporal_predictor", "embed_regressor"))) if str(f).strip()]
    syms = [str(s).strip().upper() for s in (symbols or _csv(os.environ.get("TUNE_SYMBOLS"), ("GLOBAL",))) if str(s).strip()]
    trials = max(1, int(n_trials if n_trials is not None else _env_int("TUNE_N_TRIALS", 50)))
    timeout = timeout_s if timeout_s is not None else _env_int("TUNE_TIMEOUT_S", 0)
    resolved_seed = int(seed if seed is not None else _env_int("TUNE_SEED", 7))
    smoke = bool(allow_smoke_objective) if allow_smoke_objective is not None else str(os.environ.get("TUNE_MODE", "")).strip().lower() == "smoke"

    results: list[dict[str, Any]] = []
    started = int(time.time() * 1000)
    for family in families:
        if not catalog_for_family(family):
            results.append({"model_family": family, "status": "skipped_no_catalog"})
            continue
        for symbol in syms:
            objective = _objective_for(
                model_family=family,
                symbol=symbol,
                objective_factory=objective_factory,
                allow_smoke_objective=smoke,
            )
            study = open_study(model_family=family, symbol=symbol, seed=resolved_seed)
            before = len(list(getattr(study, "trials", []) or []))
            study.optimize(objective, n_trials=trials, timeout=(None if not timeout else int(timeout)))
            after = len(list(getattr(study, "trials", []) or []))
            best = record_best_params(model_family=family, symbol=symbol, study=study, seed=resolved_seed)
            results.append(
                {
                    "model_family": family,
                    "symbol": symbol,
                    "status": "completed",
                    "study_name": best["study_name"],
                    "trials_before": int(before),
                    "trials_after": int(after),
                    "best_value": float(best["value"]),
                    "best_trial_number": int(best["trial_number"]),
                }
            )

    return {
        "ok": all(str(row.get("status")) in {"completed", "skipped_no_catalog"} for row in results),
        "job": JOB_NAME,
        "started_ts": started,
        "finished_ts": int(time.time() * 1000),
        "results": results,
    }


def main() -> int:
    result = run_tuning_job()
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    init_db()
    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        raise SystemExit(0)
    try:
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps({"phase": "start"}, separators=(",", ":"), sort_keys=True),
        )
        rc = int(main() or 0)
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps({"phase": "done", "rc": rc}, separators=(",", ":"), sort_keys=True),
        )
        raise SystemExit(rc)
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)
