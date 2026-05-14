"""Persistent Optuna study helpers."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

from engine.runtime.platform import default_data_root
from engine.strategy.tuning.catalog import catalog_defaults

_OPTUNA_STUDY_DB_FILENAME = ".".join(("optuna_studies", "db"))


def _import_optuna():
    try:
        import optuna  # type: ignore
    except ImportError as exc:
        raise RuntimeError("Optuna is required for hyperparameter tuning; install requirements.txt") from exc
    return optuna


def study_name_for(model_family: str, symbol: str = "GLOBAL") -> str:
    family = str(model_family or "").strip()
    sym = str(symbol or "GLOBAL").strip().upper() or "GLOBAL"
    return f"{family}_{sym}"


def study_storage_url(db_path: str | os.PathLike[str] | None = None) -> str:
    explicit = str(os.environ.get("TS_OPTUNA_STORAGE_URL", "") or "").strip()
    if explicit:
        return explicit
    path = Path(db_path or os.environ.get("OPTUNA_DB_PATH", "") or (default_data_root() / _OPTUNA_STUDY_DB_FILENAME))
    path.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{path.as_posix()}"


def open_study(
    *,
    model_family: str,
    symbol: str = "GLOBAL",
    direction: str = "maximize",
    db_path: str | os.PathLike[str] | None = None,
    seed: int | None = None,
):
    optuna = _import_optuna()
    sampler = None
    if seed is not None:
        sampler = optuna.samplers.TPESampler(seed=int(seed))
    return optuna.create_study(
        study_name=study_name_for(model_family, symbol),
        storage=study_storage_url(db_path),
        direction=str(direction or "maximize"),
        load_if_exists=True,
        sampler=sampler,
    )


def fetch_best_params(model_family: str, symbol: str = "GLOBAL", *, con=None) -> dict[str, Any]:
    from engine.runtime.storage import fetch_model_best_params

    try:
        row = fetch_model_best_params(model_family=str(model_family), symbol=str(symbol or "GLOBAL"), con=con)
    except Exception:
        return catalog_defaults(model_family)
    if not row:
        return catalog_defaults(model_family)
    params = row.get("params_json") or {}
    if not isinstance(params, dict):
        return catalog_defaults(model_family)
    merged = catalog_defaults(model_family)
    merged.update(params)
    return merged


def record_best_params(
    *,
    model_family: str,
    symbol: str,
    study,
    seed: int | None = None,
    con=None,
) -> dict[str, Any]:
    from engine.runtime.storage import upsert_model_best_params

    best = getattr(study, "best_trial", None)
    if best is None:
        raise ValueError("study has no best trial")
    params = dict(getattr(best, "params", {}) or {})
    value = float(getattr(best, "value", 0.0) or 0.0)
    trial_number = int(getattr(best, "number", 0) or 0)
    study_name = str(getattr(study, "study_name", "") or study_name_for(model_family, symbol))
    upsert_model_best_params(
        model_family=str(model_family),
        symbol=str(symbol or "GLOBAL"),
        study_name=study_name,
        params_json=params,
        value=value,
        ts=int(time.time() * 1000),
        trial_number=trial_number,
        seed=seed,
        con=con,
    )
    return {
        "model_family": str(model_family),
        "symbol": str(symbol or "GLOBAL").upper(),
        "study_name": study_name,
        "params": params,
        "value": value,
        "trial_number": trial_number,
        "seed": seed,
    }
