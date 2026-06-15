"""Exponentially weighted Hedge blending for governed model pools.

The Hedge layer only reweights models that are already qualified by the
champion/challenger governance path. It does not promote models and does not
create a second model-selection framework.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from collections.abc import Iterable, Mapping
from typing import Any

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect
from engine.strategy.adwin_drift import effective_window_after_adwin
from engine.strategy.bocpd import effective_hedge_window, log_ensemble_trigger

MODE = "hedge"
DEFAULT_WINDOW = int(os.environ.get("ENSEMBLE_HEDGE_WINDOW", "60") or 60)
DEFAULT_FLOOR = float(os.environ.get("ENSEMBLE_HEDGE_WEIGHT_FLOOR", "0.02") or 0.02)
LOG = get_logger("engine.strategy.ensemble.hedge")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.ensemble.hedge",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def prediction_blend_mode() -> str:
    raw = str(os.environ.get("PREDICTION_BLEND_MODE", "champion_only") or "champion_only").strip().lower()
    return raw if raw in {"champion_only", "hedge"} else "champion_only"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _json_loads(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if value in (None, "", b"", bytearray()):
        return default
    try:
        out = json.loads(value.decode("utf-8", "replace") if isinstance(value, (bytes, bytearray)) else str(value))
    except Exception:
        return default
    return out if isinstance(out, type(default)) else default


def _own_connection(con):
    if con is not None:
        return con, False
    return connect(), True


def _commit_if_possible(con) -> None:
    try:
        con.commit()
    except Exception as e:
        _warn_nonfatal("ENSEMBLE_HEDGE_COMMIT_FAILED", e, once_key="commit_failed")
        raise


def ensure_operational_weight_schema(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ensemble_blend_weights (
            id BIGSERIAL PRIMARY KEY,
            created_ts BIGINT NOT NULL,
            mode TEXT NOT NULL,
            regime TEXT,
            weights_json TEXT NOT NULL,
            meta_blob BYTEA,
            meta_artifact_sha256 TEXT,
            meta_artifact_alias TEXT
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ensemble_blend_weights_mode_created
          ON ensemble_blend_weights(mode, regime, created_ts DESC)
        """
    )


def _regime_key(symbol: str, horizon: int) -> str:
    return f"{str(symbol or '*').upper().strip() or '*'}:{int(horizon or 0)}"


def _dedupe(values: Iterable[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        item = str(value or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _deployable_marketplace_row(model_name: str, stage: str, meta: Mapping[str, Any]) -> bool:
    stage_key = str(stage or "").strip().lower()
    if stage_key not in {"champion", "challenger"}:
        return False
    score_source = str((meta or {}).get("score_source") or "").strip().lower()
    model_kind = str((meta or {}).get("model_kind") or "").strip().lower()
    model_key = str(model_name or "").strip().lower()
    if score_source == "shadow_predictions" and model_kind != "shadow_regime_stats" and not model_key.startswith("regime_stats_"):
        return False
    return True


def qualified_model_pool(
    con,
    *,
    symbol: str,
    horizon: int,
    champion_name: str = "",
    asof_ts_ms: int | None = None,
) -> list[str]:
    """Return currently governed champion/challenger model names for a group."""

    symbol_key = str(symbol or "").upper().strip()
    horizon_value = int(horizon or 0)
    cutoff = int(asof_ts_ms or _now_ms())
    names: list[str] = []
    if str(champion_name or "").strip():
        names.append(str(champion_name).strip())

    try:
        rows = con.execute(
            """
            SELECT model_name, stage, meta_json, updated_ts_ms
            FROM model_marketplace_scores
            WHERE symbol IN (?, '*')
              AND horizon_s IN (?, 0)
              AND stage IN ('champion', 'challenger')
              AND updated_ts_ms <= ?
            ORDER BY
              CASE WHEN symbol=? THEN 0 ELSE 1 END,
              CASE WHEN horizon_s=? THEN 0 ELSE 1 END,
              updated_ts_ms DESC
            """,
            (symbol_key, int(horizon_value), int(cutoff), symbol_key, int(horizon_value)),
        ).fetchall()
    except Exception:
        rows = []
    marketplace_names: set[str] = set()
    for model_name, stage, meta_json, _updated_ts_ms in rows or []:
        meta = _json_loads(meta_json, {})
        if _deployable_marketplace_row(str(model_name or ""), str(stage or ""), meta if isinstance(meta, Mapping) else {}):
            marketplace_name = str(model_name or "").strip()
            marketplace_names.add(marketplace_name)
            names.append(marketplace_name)

    try:
        assignment_rows = con.execute(
            """
            SELECT model_name, challenger_name, state, updated_ts_ms
            FROM champion_assignments
            WHERE scope='global'
              AND symbol=?
              AND horizon_s IN (?, 0)
              AND updated_ts_ms <= ?
            ORDER BY
              CASE WHEN horizon_s=? THEN 0 ELSE 1 END,
              updated_ts_ms DESC
            """,
            (symbol_key, int(horizon_value), int(cutoff), int(horizon_value)),
        ).fetchall()
    except Exception:
        assignment_rows = []
    for model_name, challenger_name, state, _updated_ts_ms in assignment_rows or []:
        if str(state or "").strip().lower() == "champion":
            names.append(str(model_name or "").strip())
        challenger_key = str(challenger_name or "").strip()
        if challenger_key and challenger_key in marketplace_names:
            names.append(challenger_key)

    return _dedupe(names)


def _apply_floor(weights: Mapping[str, float], *, floor: float = DEFAULT_FLOOR) -> dict[str, float]:
    cleaned = {
        str(model): max(0.0, _safe_float(weight, 0.0))
        for model, weight in dict(weights or {}).items()
        if str(model or "").strip()
    }
    if not cleaned:
        return {}
    names = list(cleaned.keys())
    n_models = len(names)
    floor_value = max(0.0, min(float(floor), 1.0 / float(n_models)))
    if n_models * floor_value >= 1.0 - 1e-12:
        equal = 1.0 / float(n_models)
        return {name: float(equal) for name in names}
    total = sum(cleaned.values())
    if total <= 0.0:
        raw = {name: 1.0 / float(n_models) for name in names}
    else:
        raw = {name: float(cleaned[name] / total) for name in names}
    remaining = 1.0 - (float(n_models) * floor_value)
    return {name: float(floor_value + remaining * raw[name]) for name in names}


def _eta_value(n_models: int, window: int, eta: float | None = None) -> float:
    if eta is not None and float(eta) > 0.0:
        return float(eta)
    raw = str(os.environ.get("ENSEMBLE_HEDGE_ETA", "") or "").strip()
    if raw:
        parsed = _safe_float(raw, 0.0)
        if parsed > 0.0:
            return float(parsed)
    n = max(2, int(n_models))
    t = max(1, int(window))
    return float(math.sqrt((8.0 * math.log(float(n))) / float(t)))


def compute_hedge_weights(
    losses_by_model: Mapping[str, Iterable[float]],
    *,
    window: int = DEFAULT_WINDOW,
    floor: float = DEFAULT_FLOOR,
    eta: float | None = None,
) -> dict[str, float]:
    """Compute floored Hedge weights from recent per-model losses."""

    model_losses = {
        str(model): [
            max(0.0, _safe_float(loss, 0.0))
            for loss in list(losses or [])[-int(max(1, window)):]
        ]
        for model, losses in dict(losses_by_model or {}).items()
        if str(model or "").strip()
    }
    if not model_losses:
        return {}
    names = list(model_losses.keys())
    eta_value = _eta_value(len(names), int(window), eta)
    observed_sums = [float(sum(values)) for values in model_losses.values() if values]
    missing_loss = max(observed_sums) if observed_sums else 0.0
    cumulative = {
        name: float(sum(model_losses.get(name) or []) if (model_losses.get(name) or []) else missing_loss)
        for name in names
    }
    min_loss = min(cumulative.values()) if cumulative else 0.0
    raw = {
        name: float(math.exp(-float(eta_value) * (float(cumulative[name]) - float(min_loss))))
        for name in names
    }
    return _apply_floor(raw, floor=float(floor))


def _append_loss(losses: dict[str, list[tuple[int, float]]], model_name: Any, ts_ms: Any, prediction: Any, target: Any) -> None:
    model_key = str(model_name or "").strip()
    if not model_key:
        return
    pred = _safe_float(prediction, float("nan"))
    actual = _safe_float(target, float("nan"))
    if not math.isfinite(pred) or not math.isfinite(actual):
        return
    losses.setdefault(model_key, []).append((int(_safe_int(ts_ms, 0)), float((pred - actual) ** 2)))


def _fetch_model_oos_losses(
    con,
    *,
    symbol: str,
    horizon: int,
    model_names: list[str],
    window: int,
) -> dict[str, list[tuple[int, float]]]:
    if not model_names:
        return {}
    placeholders = ",".join(["?"] * len(model_names))
    try:
        rows = con.execute(
            f"""
            SELECT family, ts, prediction, target
            FROM model_oos_predictions
            WHERE symbol=?
              AND horizon=?
              AND family IN ({placeholders})
              AND target IS NOT NULL
            ORDER BY ts DESC
            LIMIT ?
            """,
            (str(symbol).upper().strip(), int(horizon), *model_names, int(max(1, window) * len(model_names))),
        ).fetchall()
    except Exception:
        return {}
    losses: dict[str, list[tuple[int, float]]] = {}
    for family, ts, pred, target in rows or []:
        _append_loss(losses, family, ts, pred, target)
    return losses


def _fetch_joined_prediction_losses(
    con,
    *,
    table_name: str,
    prediction_col: str,
    ts_col: str,
    symbol_col: str,
    horizon_col: str,
    model_col: str,
    symbol: str,
    horizon: int,
    model_names: list[str],
    window: int,
) -> dict[str, list[tuple[int, float]]]:
    if not model_names:
        return {}
    placeholders = ",".join(["?"] * len(model_names))
    target_exprs = [
        "CASE WHEN le.realized=1 THEN le.net_z ELSE COALESCE(le.net_z, l.realized_z, l.impact_z) END",
        "COALESCE(le.net_z, l.realized_z, l.impact_z)",
        "COALESCE(l.realized_z, l.impact_z)",
        "l.impact_z",
    ]
    joins = [
        f"""
        FROM {table_name} p
        JOIN labels l
          ON l.event_id = p.event_id
         AND l.symbol = p.{symbol_col}
         AND l.horizon_s = p.{horizon_col}
        LEFT JOIN labels_exec le
          ON le.event_id = l.event_id
         AND le.symbol = l.symbol
         AND le.horizon_s = l.horizon_s
        """,
        f"""
        FROM {table_name} p
        JOIN labels l
          ON l.event_id = p.event_id
         AND l.symbol = p.{symbol_col}
         AND l.horizon_s = p.{horizon_col}
        """,
    ]
    for join_sql in joins:
        for target_expr in target_exprs:
            if "le." in target_expr and "labels_exec" not in join_sql:
                continue
            sql = f"""
                SELECT p.{model_col} AS model_name,
                       p.{ts_col} AS ts_ms,
                       p.{prediction_col} AS prediction,
                       {target_expr} AS target
                {join_sql}
                WHERE p.{symbol_col}=?
                  AND p.{horizon_col}=?
                  AND p.{model_col} IN ({placeholders})
                  AND ({target_expr}) IS NOT NULL
                ORDER BY p.{ts_col} DESC
                LIMIT ?
            """
            try:
                rows = con.execute(
                    sql,
                    (str(symbol).upper().strip(), int(horizon), *model_names, int(max(1, window) * len(model_names))),
                ).fetchall()
            except Exception:
                continue
            losses: dict[str, list[tuple[int, float]]] = {}
            for model_name, ts_ms, prediction, target in rows or []:
                _append_loss(losses, model_name, ts_ms, prediction, target)
            return losses
    return {}


def recent_losses_by_model(
    con,
    *,
    symbol: str,
    horizon: int,
    model_names: Iterable[str],
    window: int = DEFAULT_WINDOW,
) -> dict[str, list[float]]:
    names = _dedupe(model_names)
    combined: dict[str, list[tuple[int, float]]] = {name: [] for name in names}
    for source in (
        _fetch_model_oos_losses(
            con,
            symbol=str(symbol),
            horizon=int(horizon),
            model_names=names,
            window=int(window),
        ),
        _fetch_joined_prediction_losses(
            con,
            table_name="shadow_predictions",
            prediction_col="predicted_z",
            ts_col="ts_ms",
            symbol_col="symbol",
            horizon_col="horizon_s",
            model_col="model_name",
            symbol=str(symbol),
            horizon=int(horizon),
            model_names=names,
            window=int(window),
        ),
        _fetch_joined_prediction_losses(
            con,
            table_name="decision_log",
            prediction_col="predicted_z",
            ts_col="ts_ms",
            symbol_col="symbol",
            horizon_col="horizon_s",
            model_col="model_name",
            symbol=str(symbol),
            horizon=int(horizon),
            model_names=names,
            window=int(window),
        ),
    ):
        for model_name, rows in dict(source or {}).items():
            combined.setdefault(str(model_name), []).extend(list(rows or []))
    out: dict[str, list[float]] = {}
    for model_name in names:
        rows = sorted(combined.get(model_name) or [], key=lambda item: int(item[0]), reverse=True)
        out[str(model_name)] = [float(loss) for _ts, loss in rows[: int(max(1, window))]]
    return out


def persist_hedge_weights(
    con,
    *,
    symbol: str,
    horizon: int,
    weights: Mapping[str, float],
    ts_ms: int | None = None,
) -> int:
    ensure_operational_weight_schema(con)
    ts_value = int(ts_ms if ts_ms is not None else _now_ms())
    clean = {
        str(model): float(weight)
        for model, weight in dict(weights or {}).items()
        if str(model or "").strip() and float(weight) > 0.0
    }
    if not clean:
        return 0
    con.execute(
        """
        INSERT INTO ensemble_blend_weights(created_ts, mode, regime, weights_json, meta_blob, meta_artifact_sha256, meta_artifact_alias)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(ts_value),
            MODE,
            _regime_key(str(symbol), int(horizon)),
            json.dumps(clean, separators=(",", ":"), sort_keys=True),
            None,
            None,
            None,
        ),
    )
    return int(ts_value)


def load_hedge_weights(
    con,
    *,
    symbol: str,
    horizon: int,
    qualified_models: Iterable[str],
    floor: float = DEFAULT_FLOOR,
    ensure: bool = True,
) -> dict[str, Any] | None:
    qualified = set(_dedupe(qualified_models))
    if not qualified:
        return None
    if ensure:
        ensure_operational_weight_schema(con)
    regime_keys = [_regime_key(str(symbol), int(horizon))]
    fallback_key = _regime_key("*", int(horizon))
    if fallback_key not in regime_keys:
        regime_keys.append(fallback_key)
    for key in regime_keys:
        try:
            row = con.execute(
                """
                SELECT created_ts, weights_json
                FROM ensemble_blend_weights
                WHERE mode=?
                  AND regime=?
                ORDER BY created_ts DESC, id DESC
                LIMIT 1
                """,
                (MODE, str(key)),
            ).fetchone()
        except Exception:
            row = None
        if not row:
            continue
        raw = _json_loads(row[1], {})
        if not isinstance(raw, Mapping):
            continue
        filtered = {
            str(model): _safe_float(weight, 0.0)
            for model, weight in dict(raw).items()
            if str(model or "").strip() in qualified and _safe_float(weight, 0.0) > 0.0
        }
        if not filtered:
            continue
        weights = _apply_floor(filtered, floor=float(floor))
        return {
            "ts_ms": int(_safe_int(row[0], 0)),
            "regime": str(key),
            "weights": dict(weights),
            "raw_weights": dict(raw),
            "excluded_models": sorted(str(model) for model in dict(raw).keys() if str(model) not in qualified),
        }
    return None


def _candidate_groups(con) -> list[tuple[str, int]]:
    groups: set[tuple[str, int]] = set()
    queries = [
        "SELECT DISTINCT symbol, horizon FROM model_oos_predictions WHERE target IS NOT NULL",
        "SELECT DISTINCT symbol, horizon_s FROM shadow_predictions",
        "SELECT DISTINCT symbol, horizon_s FROM decision_log",
        "SELECT DISTINCT symbol, horizon_s FROM model_marketplace_scores WHERE stage IN ('champion','challenger')",
    ]
    for sql in queries:
        try:
            rows = con.execute(sql).fetchall()
        except Exception:
            continue
        for symbol, horizon in rows or []:
            symbol_key = str(symbol or "").upper().strip()
            horizon_value = int(_safe_int(horizon, 0))
            if symbol_key and horizon_value >= 0:
                groups.add((symbol_key, horizon_value))
    return sorted(groups)


def refresh_hedge_weights(
    *,
    con=None,
    symbols: Iterable[str] | None = None,
    horizons: Iterable[int] | None = None,
    now_ms: int | None = None,
    window: int = DEFAULT_WINDOW,
    floor: float = DEFAULT_FLOOR,
) -> dict[str, Any]:
    own = con is None
    con = connect() if con is None else con
    ts_value = int(now_ms if now_ms is not None else _now_ms())
    try:
        ensure_operational_weight_schema(con)
        if symbols is not None or horizons is not None:
            symbol_values = [str(s).upper().strip() for s in list(symbols or []) if str(s or "").strip()]
            horizon_values = [int(h) for h in list(horizons or [])]
            groups = [
                (symbol, horizon)
                for symbol in (symbol_values or ["*"])
                for horizon in (horizon_values or [0])
            ]
        else:
            groups = _candidate_groups(con)

        refreshed: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        trigger_log_failures: list[dict[str, Any]] = []
        for symbol, horizon in groups:
            pool = qualified_model_pool(con, symbol=str(symbol), horizon=int(horizon), asof_ts_ms=int(ts_value))
            if len(pool) < 2:
                skipped.append({"symbol": str(symbol), "horizon": int(horizon), "reason": "insufficient_qualified_pool", "qualified": pool})
                continue
            effective_window, bocpd_trigger = effective_hedge_window(
                con,
                symbol=str(symbol),
                horizon=int(horizon),
                base_window=int(window),
            )
            if bool((bocpd_trigger or {}).get("triggered")):
                try:
                    log_ensemble_trigger(con, bocpd_trigger)
                except Exception as e:
                    _warn_nonfatal(
                        "ENSEMBLE_HEDGE_TRIGGER_LOG_FAILED",
                        e,
                        once_key=f"trigger_log:{symbol}:{horizon}",
                        symbol=str(symbol),
                        horizon=int(horizon),
                        trigger_type=str((bocpd_trigger or {}).get("trigger_type") or ""),
                    )
                    trigger_log_failures.append(
                        {
                            "symbol": str(symbol),
                            "horizon": int(horizon),
                            "error": type(e).__name__,
                            "trigger_type": str((bocpd_trigger or {}).get("trigger_type") or ""),
                        }
                    )
            effective_window, adwin_trigger = effective_window_after_adwin(
                con,
                symbol=str(symbol),
                horizon=int(horizon),
                model_names=pool,
                base_window=int(effective_window),
                now_ms=int(ts_value),
            )
            losses = recent_losses_by_model(con, symbol=str(symbol), horizon=int(horizon), model_names=pool, window=int(effective_window))
            if bool((adwin_trigger or {}).get("triggered")):
                per_model_windows = dict((adwin_trigger or {}).get("per_model_windows") or {})
                for model_name, model_window in per_model_windows.items():
                    name = str(model_name or "").strip()
                    if name in losses:
                        losses[name] = list(losses.get(name) or [])[: max(1, int(model_window or effective_window))]
            if not any(losses.get(model) for model in pool):
                skipped.append({
                    "symbol": str(symbol),
                    "horizon": int(horizon),
                    "reason": "no_realized_losses",
                    "qualified": pool,
                    "bocpd_trigger": dict(bocpd_trigger or {}),
                    "adwin_trigger": dict(adwin_trigger or {}),
                })
                continue
            weights = compute_hedge_weights(losses, window=int(effective_window), floor=float(floor))
            if not weights:
                skipped.append({
                    "symbol": str(symbol),
                    "horizon": int(horizon),
                    "reason": "empty_weights",
                    "qualified": pool,
                    "bocpd_trigger": dict(bocpd_trigger or {}),
                    "adwin_trigger": dict(adwin_trigger or {}),
                })
                continue
            persist_hedge_weights(con, symbol=str(symbol), horizon=int(horizon), weights=weights, ts_ms=int(ts_value))
            refreshed.append(
                {
                    "symbol": str(symbol),
                    "horizon": int(horizon),
                    "qualified": list(pool),
                    "weights": dict(weights),
                    "n_obs": {model: int(len(losses.get(model) or [])) for model in pool},
                    "base_window": int(window),
                    "effective_window": int(effective_window),
                    "bocpd_trigger": dict(bocpd_trigger or {}),
                    "adwin_trigger": dict(adwin_trigger or {}),
                }
            )
        if own:
            _commit_if_possible(con)
        return {
            "ok": True,
            "mode": MODE,
            "ts_ms": int(ts_value),
            "refreshed": refreshed,
            "skipped": skipped,
            "refreshed_count": int(len(refreshed)),
            "skipped_count": int(len(skipped)),
            "trigger_log_failures": trigger_log_failures,
            "trigger_log_failure_count": int(len(trigger_log_failures)),
        }
    finally:
        if own:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("ENSEMBLE_HEDGE_CLOSE_FAILED", e, once_key="close_failed")


def replay_hedge_vs_champion(
    con,
    *,
    symbol: str,
    horizon: int,
    champion_name: str,
    challenger_names: Iterable[str],
    window: int = DEFAULT_WINDOW,
    floor: float = DEFAULT_FLOOR,
) -> dict[str, Any]:
    """Replay stored OOS rows and compare Hedge loss with champion-only loss."""

    models = _dedupe([champion_name, *list(challenger_names or [])])
    if len(models) < 2:
        return {"ok": False, "reason": "insufficient_models", "rows": 0}
    losses = recent_losses_by_model(con, symbol=str(symbol), horizon=int(horizon), model_names=models, window=10_000)
    rows_by_idx = min([len(losses.get(model) or []) for model in models] or [0])
    if rows_by_idx <= int(window):
        return {"ok": False, "reason": "insufficient_history", "rows": int(rows_by_idx)}
    # Loss-only replay cannot reconstruct blended signed return Sharpe, but it
    # gives the requested error comparison when only OOS prediction losses exist.
    hedge_loss = 0.0
    champion_loss = 0.0
    for idx in range(int(window), rows_by_idx):
        train = {model: list(reversed(losses[model]))[:idx] for model in models}
        weights = compute_hedge_weights(train, window=int(window), floor=float(floor))
        current = {model: list(reversed(losses[model]))[idx] for model in models}
        hedge_loss += sum(float(weights.get(model, 0.0)) * float(current[model]) for model in models)
        champion_loss += float(current.get(str(champion_name), 0.0))
    eval_n = max(1, rows_by_idx - int(window))
    return {
        "ok": True,
        "symbol": str(symbol).upper().strip(),
        "horizon": int(horizon),
        "models": models,
        "n_eval": int(eval_n),
        "hedge_mse": float(hedge_loss / eval_n),
        "champion_mse": float(champion_loss / eval_n),
        "improved": bool(hedge_loss <= champion_loss),
        "cost_adjusted_sharpe": None,
    }
