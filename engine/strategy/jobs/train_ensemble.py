"""Nightly stacked ridge ensemble refit job."""

from __future__ import annotations
import logging

import os
import time
from collections import defaultdict
from collections.abc import Mapping
from typing import Any

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect

LOG = logging.getLogger(__name__)


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.jobs.train_ensemble",
        extra=extra or None,
        persist=False,
    )


try:
    from engine.runtime.storage import fetch_latest_model_hyperparameters
except Exception as e:  # pragma: no cover - older storage facades may not expose it
    _warn_nonfatal(
        "ENSEMBLE_HYPERPARAMETER_FETCHER_IMPORT_FAILED",
        e,
    )
    fetch_latest_model_hyperparameters = None  # type: ignore[assignment]
from engine.strategy.ensemble.blender import filter_weights_by_eligibility, persist_weights
from engine.strategy.ensemble.oos_store import read_oos_predictions, trailing_start_ts
from engine.strategy.ensemble.ridge_meta import RidgeStackEnsemble

def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return float(default)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return int(default)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _mapping_or_none(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        try:
            import json

            parsed = json.loads(value)
        except Exception:
            LOG.debug("ensemble_mapping_json_parse_failed value=%r", value, exc_info=True)
            return None
        if isinstance(parsed, Mapping):
            return parsed
    return None


def _extract_alpha_from_catalog_row(row: Any) -> float | None:
    payload = _mapping_or_none(row)
    if not payload:
        return None
    for key in ("alpha", "ridge_alpha", "ensemble_ridge_alpha"):
        value = _float_or_none(payload.get(key))
        if value is not None:
            return value
    for key in ("params", "hyperparams", "best_params", "params_json"):
        nested = _mapping_or_none(payload.get(key))
        if not nested:
            continue
        for alpha_key in ("alpha", "ridge_alpha", "ensemble_ridge_alpha"):
            value = _float_or_none(nested.get(alpha_key))
            if value is not None:
                return value
    return None


def _extract_lambda_prior_from_catalog_row(row: Any) -> float | None:
    payload = _mapping_or_none(row)
    if not payload:
        return None
    for key in ("lambda_prior", "causal_lambda_prior", "ensemble_causal_lambda_prior"):
        value = _float_or_none(payload.get(key))
        if value is not None:
            return value
    for key in ("params", "hyperparams", "best_params", "params_json"):
        nested = _mapping_or_none(payload.get(key))
        if not nested:
            continue
        for lambda_key in ("lambda_prior", "causal_lambda_prior", "ensemble_causal_lambda_prior"):
            value = _float_or_none(nested.get(lambda_key))
            if value is not None:
                return value
    return None


def _catalog_alpha(symbol: str, horizon: int) -> float | None:
    fetcher = fetch_latest_model_hyperparameters
    if fetcher is None:
        return None
    candidates = [
        {
            "model_name": f"ridge_stack_ensemble:{str(symbol)}:{int(horizon)}",
            "model_family": "ridge_stack_ensemble",
            "tuner": "optuna_cpcv",
        },
        {
            "model_name": "ridge_stack_ensemble",
            "model_family": "ridge_stack_ensemble",
            "tuner": "optuna_cpcv",
        },
        {
            "model_name": "ridge_stack_ensemble",
            "model_family": "ensemble",
            "tuner": "optuna_cpcv",
        },
    ]
    for kwargs in candidates:
        try:
            row = fetcher(**kwargs)
        except TypeError:
            try:
                row = fetcher(
                    kwargs["model_name"],
                    kwargs["model_family"],
                    kwargs.get("tuner"),
                )
            except Exception as e:
                _warn_nonfatal("ENSEMBLE_CATALOG_ALPHA_POSITIONAL_FETCH_FAILED", e, kwargs=repr(kwargs))
                continue
        except Exception as e:
            _warn_nonfatal("ENSEMBLE_CATALOG_ALPHA_FETCH_FAILED", e, kwargs=repr(kwargs))
            continue
        alpha = _extract_alpha_from_catalog_row(row)
        if alpha is not None:
            return float(alpha)
    return None


def _catalog_lambda_prior(symbol: str, horizon: int) -> float | None:
    fetcher = fetch_latest_model_hyperparameters
    if fetcher is None:
        return None
    candidates = [
        {
            "model_name": f"ridge_stack_ensemble:{str(symbol)}:{int(horizon)}",
            "model_family": "ridge_stack_ensemble",
            "tuner": "optuna_cpcv",
        },
        {
            "model_name": "ridge_stack_ensemble",
            "model_family": "ridge_stack_ensemble",
            "tuner": "optuna_cpcv",
        },
        {
            "model_name": "ridge_stack_ensemble",
            "model_family": "ensemble",
            "tuner": "optuna_cpcv",
        },
    ]
    for kwargs in candidates:
        try:
            row = fetcher(**kwargs)
        except TypeError:
            try:
                row = fetcher(
                    kwargs["model_name"],
                    kwargs["model_family"],
                    kwargs.get("tuner"),
                )
            except Exception as e:
                _warn_nonfatal("ENSEMBLE_CATALOG_LAMBDA_POSITIONAL_FETCH_FAILED", e, kwargs=repr(kwargs))
                continue
        except Exception as e:
            _warn_nonfatal("ENSEMBLE_CATALOG_LAMBDA_FETCH_FAILED", e, kwargs=repr(kwargs))
            continue
        value = _extract_lambda_prior_from_catalog_row(row)
        if value is not None:
            return float(value)
    return None


def resolve_ridge_alpha(
    *,
    symbol: str = "",
    horizon: int = 0,
    default: float = 1.0,
    use_catalog: bool = True,
) -> float:
    env_value = os.environ.get("ENSEMBLE_RIDGE_ALPHA")
    if env_value is not None:
        return _env_float("ENSEMBLE_RIDGE_ALPHA", float(default))
    if bool(use_catalog):
        catalog_value = _catalog_alpha(str(symbol), int(horizon))
        if catalog_value is not None:
            return float(catalog_value)
    return float(default)


def resolve_causal_lambda_prior(
    *,
    symbol: str = "",
    horizon: int = 0,
    default: float = 0.0,
    use_catalog: bool = True,
) -> float:
    env_value = os.environ.get("ENSEMBLE_CAUSAL_PRIOR_LAMBDA")
    if env_value is not None:
        return max(0.0, _env_float("ENSEMBLE_CAUSAL_PRIOR_LAMBDA", float(default)))
    if bool(use_catalog):
        catalog_value = _catalog_lambda_prior(str(symbol), int(horizon))
        if catalog_value is not None:
            return max(0.0, float(catalog_value))
    return max(0.0, float(default))


def _table_exists(con, table: str) -> bool:
    try:
        row = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (str(table),)).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal("ENSEMBLE_TABLE_EXISTS_FAILED", e, table=str(table))
        return False


def _latest_causal_score_for_family(
    con,
    *,
    family: str,
    target: str | None,
    window: str | None,
) -> float | None:
    where = ["feature=?"]
    params: list[Any] = [str(family)]
    if target:
        where.append("target=?")
        params.append(str(target))
    if window:
        where.append("window=?")
        params.append(str(window))
    try:
        row = con.execute(
            f"""
            SELECT score
            FROM causal_scores
            WHERE {' AND '.join(where)}
            ORDER BY ts DESC
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "ENSEMBLE_CAUSAL_SCORE_LOOKUP_FAILED",
            e,
            family=str(family),
            target=str(target or ""),
            window=str(window or ""),
        )
        return None
    return _float_or_none(row[0]) if row else None


def causal_prior_target_for_horizon(horizon: int) -> str | None:
    raw = str(os.environ.get("ENSEMBLE_CAUSAL_PRIOR_TARGET", "") or "").strip()
    if raw:
        return raw.format(horizon=int(horizon))
    return f"impact_z_{int(horizon)}" if int(horizon) > 0 else None


def load_causal_prior_weights(
    families: set[str] | list[str] | tuple[str, ...],
    *,
    horizon: int,
    con,
    target: str | None = None,
    window: str | None = None,
    missing_score: float | None = None,
) -> dict[str, float]:
    names = sorted(str(family or "").strip() for family in families if str(family or "").strip())
    if not names or con is None or not _table_exists(con, "causal_scores"):
        return {}
    target_key = str(target or causal_prior_target_for_horizon(int(horizon)) or "").strip() or None
    window_key = str(window if window is not None else os.environ.get("ENSEMBLE_CAUSAL_PRIOR_WINDOW", "365d") or "").strip()
    missing_value = max(0.0, min(1.0, float(missing_score if missing_score is not None else _env_float("ENSEMBLE_CAUSAL_PRIOR_MISSING_SCORE", 0.5))))
    observed: dict[str, float] = {}
    for family in names:
        value = _latest_causal_score_for_family(
            con,
            family=family,
            target=target_key,
            window=window_key or None,
        )
        if value is not None:
            observed[family] = max(0.0, min(1.0, float(value)))
    if not observed:
        return {}
    raw = {family: float(observed.get(family, missing_value)) for family in names}
    total = float(sum(raw.values()))
    if total <= 0.0:
        return {}
    return {family: float(value / total) for family, value in raw.items()}


def fit_and_persist_group(
    rows: list[dict[str, Any]],
    *,
    symbol: str,
    horizon: int,
    alpha: float,
    nonneg: bool,
    prior_weights: Mapping[str, float] | None = None,
    lambda_prior: float = 0.0,
    con=None,
    ts: int | None = None,
) -> dict[str, Any] | None:
    families = {str(row.get("family") or "") for row in rows}
    families.discard("")
    if len(families) < 2:
        return None
    causal_prior = dict(prior_weights or {}) if float(lambda_prior or 0.0) > 0.0 else {}
    model = RidgeStackEnsemble(alpha=float(alpha), nonneg=bool(nonneg)).fit(
        rows,
        symbol=str(symbol),
        horizon=int(horizon),
        prior_weights=causal_prior or None,
        lambda_prior=float(lambda_prior or 0.0),
    )
    eligible_weights, excluded = filter_weights_by_eligibility(
        dict(model.weights_),
        symbol=str(symbol),
        horizon=int(horizon),
        con=con,
    )
    if len(eligible_weights) < 1:
        return None
    persisted_ts = persist_weights(
        symbol=str(symbol),
        horizon=int(horizon),
        weights=eligible_weights,
        intercept=float(model.intercept_),
        alpha=float(model.alpha),
        n_train_obs=int(model.n_train_obs_),
        val_metric=model.val_metric_,
        ts=ts,
        con=con,
    )
    return {
        "symbol": str(symbol),
        "horizon": int(horizon),
        "ts": int(persisted_ts),
        "families": list(model.families_),
        "weights": dict(eligible_weights),
        "excluded_families": dict(excluded),
        "intercept": float(model.intercept_),
        "alpha": float(model.alpha),
        "lambda_prior": float(lambda_prior or 0.0),
        "causal_prior_weights": dict(causal_prior),
        "n_train_obs": int(model.n_train_obs_),
        "val_metric": model.val_metric_,
    }


def run(*, con=None) -> dict[str, Any]:
    own = con is None
    con = connect() if con is None else con
    try:
        lookback_days = _env_int("ENSEMBLE_REFIT_LOOKBACK_DAYS", 252)
        default_alpha = _env_float("ENSEMBLE_RIDGE_ALPHA_DEFAULT", 1.0)
        nonneg = _env_bool("ENSEMBLE_RIDGE_NONNEG", True)
        now_ms = int(time.time() * 1000)
        rows = read_oos_predictions(
            start_ts=trailing_start_ts(lookback_days, now_ms=now_ms),
            require_target=True,
            con=con,
        )
        grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[(str(row.get("symbol") or ""), int(row.get("horizon") or 0))].append(row)
        persisted: list[dict[str, Any]] = []
        alpha_by_group: dict[str, float] = {}
        lambda_prior_by_group: dict[str, float] = {}
        causal_prior_by_group: dict[str, dict[str, float]] = {}
        skipped = 0
        for (symbol, horizon), group_rows in sorted(grouped.items()):
            if not symbol or int(horizon) <= 0:
                skipped += 1
                continue
            try:
                alpha = resolve_ridge_alpha(
                    symbol=str(symbol),
                    horizon=int(horizon),
                    default=float(default_alpha),
                    use_catalog=bool(own),
                )
                alpha_by_group[f"{symbol}:{int(horizon)}"] = float(alpha)
                lambda_prior = resolve_causal_lambda_prior(
                    symbol=str(symbol),
                    horizon=int(horizon),
                    default=0.0,
                    use_catalog=bool(own),
                )
                lambda_prior_by_group[f"{symbol}:{int(horizon)}"] = float(lambda_prior)
                families = {str(row.get("family") or "") for row in group_rows}
                causal_prior = (
                    load_causal_prior_weights(families, horizon=int(horizon), con=con)
                    if float(lambda_prior) > 0.0
                    else {}
                )
                if causal_prior:
                    causal_prior_by_group[f"{symbol}:{int(horizon)}"] = dict(causal_prior)
                result = fit_and_persist_group(
                    group_rows,
                    symbol=str(symbol),
                    horizon=int(horizon),
                    alpha=float(alpha),
                    nonneg=bool(nonneg),
                    prior_weights=causal_prior,
                    lambda_prior=float(lambda_prior),
                    con=con,
                    ts=now_ms,
                )
                if result is None:
                    skipped += 1
                else:
                    persisted.append(result)
            except Exception as e:
                _warn_nonfatal(
                    "ENSEMBLE_WEIGHT_PERSIST_FAILED",
                    e,
                    symbol=str(symbol),
                    horizon=int(horizon),
                    families=sorted(str(family) for family in weights),
                )
                skipped += 1
                continue
        con.commit()
        unique_alphas = sorted({float(value) for value in alpha_by_group.values()})
        summary_alpha = (
            float(unique_alphas[0])
            if len(unique_alphas) == 1
            else float(resolve_ridge_alpha(default=float(default_alpha)) if not unique_alphas else default_alpha)
        )
        return {
            "ok": True,
            "persisted": persisted,
            "persisted_count": len(persisted),
            "skipped_count": int(skipped),
            "lookback_days": int(lookback_days),
            "alpha": float(summary_alpha),
            "alpha_default": float(default_alpha),
            "alpha_by_group": dict(alpha_by_group),
            "lambda_prior_by_group": dict(lambda_prior_by_group),
            "causal_prior_by_group": dict(causal_prior_by_group),
            "nonneg": bool(nonneg),
        }
    finally:
        if own:
            try:
                con.close()
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


def main() -> None:
    LOG.info("train_ensemble_result result=%s", run())


if __name__ == "__main__":
    main()
