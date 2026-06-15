"""One-shot causal diagnostics job for registered features."""

from __future__ import annotations
import logging

import json
import math
import os
import re
import time
from typing import Any, Iterable, Mapping, Sequence

from engine.causal.dag import CausalDAG
from engine.causal.dowhy_runner import run_dowhy
from engine.causal.granger import granger_causality
from engine.causal.scores import (
    CausalScoreRecord,
    causal_score,
    ensure_causal_schema,
    load_causal_dags,
    upsert_causal_score,
)

DEFAULT_WINDOWS = ("30d", "90d", "365d")
DEFAULT_TARGETS = ("impact_z_300", "impact_z_3600")


def _env_list(name: str, default: Sequence[str]) -> tuple[str, ...]:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return tuple(default)
    return tuple(item.strip() for item in raw.split(",") if item.strip())


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return int(default)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _table_exists(con, table: str) -> bool:
    try:
        row = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (str(table),)).fetchone()
        return bool(row)
    except Exception:
        return False


def _columns(con, table: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall() or []
    except Exception:
        return set()
    return {str(row[1]) for row in rows if row and len(row) > 1}


def _finite(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _parse_json(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if value is None:
        return None
    try:
        return json.loads(str(value))
    except Exception:
        return None


def _window_ms(window: str) -> int | None:
    text = str(window or "").strip().lower()
    match = re.fullmatch(r"(\d+)\s*([dhm])", text)
    if not match:
        return None
    value = int(match.group(1))
    unit = match.group(2)
    if unit == "d":
        return value * 24 * 60 * 60 * 1000
    if unit == "h":
        return value * 60 * 60 * 1000
    return value * 60 * 1000


def _default_features() -> tuple[str, ...]:
    try:
        from engine.strategy.feature_registry import registered_feature_ids

        return tuple(registered_feature_ids(include_shadow=True))
    except Exception:
        return ()


def _pick_column(columns: set[str], candidates: Sequence[str]) -> str | None:
    lower_map = {col.lower(): col for col in columns}
    for candidate in candidates:
        if candidate in columns:
            return candidate
        mapped = lower_map.get(str(candidate).lower())
        if mapped:
            return mapped
    return None


def _rows_from_causal_observations(
    con,
    *,
    feature: str,
    target: str,
    window: str,
    limit: int,
    controls: Sequence[str] = (),
) -> list[dict[str, float]]:
    if not _table_exists(con, "causal_observations"):
        return []
    cols = _columns(con, "causal_observations")
    if not cols:
        return []
    ts_col = _pick_column(cols, ("ts", "ts_ms", "time", "timestamp"))
    x_col = _pick_column(cols, ("feature_value", "x", "treatment_value", "value"))
    y_col = _pick_column(cols, ("target_value", "y", "outcome_value", "return_value"))
    feature_col = _pick_column(cols, ("feature", "feature_id", "treatment"))
    target_col = _pick_column(cols, ("target", "target_id", "outcome"))
    window_col = _pick_column(cols, ("window", "lookback_window"))
    if not x_col and feature in cols:
        x_col = feature
    if not y_col and target in cols:
        y_col = target
    if not x_col or not y_col:
        return []
    control_cols: dict[str, str] = {}
    for control in controls or ():
        control_key = str(control or "").strip()
        if not control_key:
            continue
        picked = _pick_column(cols, (control_key,))
        if picked and picked not in {x_col, y_col}:
            control_cols[control_key] = picked
    order_col = ts_col or x_col
    where: list[str] = []
    params: list[Any] = []
    if feature_col:
        where.append(f"{feature_col}=?")
        params.append(str(feature))
    if target_col:
        where.append(f"{target_col}=?")
        params.append(str(target))
    if window_col:
        where.append(f"{window_col}=?")
        params.append(str(window))
    select_cols = [x_col, y_col, *control_cols.values()]
    sql = f"""
        SELECT {', '.join(select_cols)}
        FROM causal_observations
        {('WHERE ' + ' AND '.join(where)) if where else ''}
        ORDER BY {order_col} ASC
        LIMIT ?
    """
    params.append(int(limit))
    try:
        rows = con.execute(sql, tuple(params)).fetchall() or []
    except Exception:
        return []
    out: list[dict[str, float]] = []
    for row in rows:
        x_value = row[0]
        y_value = row[1]
        x_float = _finite(x_value)
        y_float = _finite(y_value)
        if x_float is None or y_float is None:
            continue
        payload = {str(feature): float(x_float), str(target): float(y_float)}
        for idx, control in enumerate(control_cols, start=2):
            control_value = _finite(row[idx] if idx < len(row) else None)
            if control_value is not None:
                payload[str(control)] = float(control_value)
        out.append(payload)
    return out


def _target_spec(target: str) -> tuple[str, int | None]:
    text = str(target or "").strip()
    match = re.fullmatch(r"(.+)_(\d+)", text)
    if match:
        return match.group(1), int(match.group(2))
    return text, None


def _feature_value_from_snapshot(row: Mapping[str, Any], feature: str) -> float | None:
    features = _parse_json(row.get("features_json"))
    if isinstance(features, Mapping):
        direct = _finite(features.get(feature))
        if direct is not None:
            return direct
        nested = features.get("features")
        if isinstance(nested, Mapping):
            direct = _finite(nested.get(feature))
            if direct is not None:
                return direct
    ids = _parse_json(row.get("feature_ids_json"))
    vector = _parse_json(row.get("vector_json"))
    if isinstance(ids, list) and isinstance(vector, list):
        for idx, fid in enumerate(ids):
            if str(fid) == str(feature) and idx < len(vector):
                return _finite(vector[idx])
    return None


def _rows_from_feature_snapshots(
    con,
    *,
    feature: str,
    target: str,
    window: str,
    now_ms: int,
    limit: int,
    controls: Sequence[str] = (),
) -> list[dict[str, float]]:
    if not _table_exists(con, "model_feature_snapshots") or not _table_exists(con, "labels_exec"):
        return []
    snapshot_cols = _columns(con, "model_feature_snapshots")
    labels_exec_cols = _columns(con, "labels_exec")
    required_snapshot = {"symbol", "ts_ms", "feature_ids_json", "vector_json", "features_json"}
    if not required_snapshot.issubset(snapshot_cols):
        return []
    target_col, horizon = _target_spec(target)
    if target_col == "impact_z":
        target_col = "net_z"
    if target_col == "realized_ret":
        target_col = "net_ret"
    if target_col not in labels_exec_cols:
        return []
    cutoff_ms = _window_ms(window)
    where = ["le.ts_ms = s.ts_ms", "le.symbol = s.symbol", f"le.{target_col} IS NOT NULL"]
    params: list[Any] = []
    if horizon is not None and "horizon_s" in labels_exec_cols:
        where.append("le.horizon_s=?")
        params.append(int(horizon))
    if cutoff_ms is not None:
        where.append("s.ts_ms>=?")
        params.append(int(now_ms - cutoff_ms))
    sql = f"""
        SELECT
          s.feature_ids_json,
          s.vector_json,
          s.features_json,
          le.{target_col} AS target_value
        FROM model_feature_snapshots s
        JOIN labels_exec le ON {' AND '.join(where)}
        ORDER BY s.ts_ms ASC
        LIMIT ?
    """
    params.append(int(limit))
    try:
        rows = con.execute(sql, tuple(params)).fetchall() or []
    except Exception:
        return []
    out: list[dict[str, float]] = []
    for feature_ids_json, vector_json, features_json, target_value in rows:
        x_value = _feature_value_from_snapshot(
            {
                "feature_ids_json": feature_ids_json,
                "vector_json": vector_json,
                "features_json": features_json,
            },
            feature,
        )
        y_value = _finite(target_value)
        if x_value is None or y_value is None:
            continue
        payload = {str(feature): float(x_value), str(target): float(y_value)}
        snapshot_payload = {
            "feature_ids_json": feature_ids_json,
            "vector_json": vector_json,
            "features_json": features_json,
        }
        for control in controls or ():
            control_key = str(control or "").strip()
            if not control_key:
                continue
            control_value = _feature_value_from_snapshot(snapshot_payload, control_key)
            if control_value is not None:
                payload[control_key] = float(control_value)
        out.append(payload)
    return out


def _load_observations(
    con,
    *,
    feature: str,
    target: str,
    window: str,
    now_ms: int,
    limit: int,
    controls: Sequence[str] = (),
) -> list[dict[str, float]]:
    rows = _rows_from_causal_observations(
        con,
        feature=feature,
        target=target,
        window=window,
        limit=limit,
        controls=controls,
    )
    if rows:
        return rows
    return _rows_from_feature_snapshots(
        con,
        feature=feature,
        target=target,
        window=window,
        now_ms=now_ms,
        limit=limit,
        controls=controls,
    )


def _as_series(
    rows: Sequence[Mapping[str, float]],
    *,
    feature: str,
    target: str,
    controls: Sequence[str] = (),
) -> tuple[dict[str, list[float]], list[str]]:
    out = {
        str(feature): [float(row[feature]) for row in rows],
        str(target): [float(row[target]) for row in rows],
    }
    available_controls: list[str] = []
    for control in controls or ():
        control_key = str(control or "").strip()
        if not control_key or control_key in out:
            continue
        values: list[float] = []
        for row in rows:
            value = _finite(row.get(control_key))
            if value is None:
                values = []
                break
            values.append(float(value))
        if values and len(values) == len(rows):
            out[control_key] = values
            available_controls.append(control_key)
    return out, available_controls


def _matching_dag(dags: Mapping[str, CausalDAG], *, feature: str, target: str) -> CausalDAG | None:
    for dag in dags.values():
        if str(dag.treatment) == str(feature) and str(dag.outcome) == str(target):
            return dag
    return None


def _dowhy_t(effect: float | None, effect_se: float | None) -> float | None:
    if effect is None or effect_se is None:
        return None
    if not math.isfinite(float(effect_se)) or abs(float(effect_se)) <= 1e-12:
        return None
    return float(effect) / float(effect_se)


def run_causal_scoring(
    *,
    con=None,
    features: Iterable[str] | None = None,
    targets: Iterable[str] | None = None,
    windows: Iterable[str] | None = None,
    now_ms: int | None = None,
    min_obs: int | None = None,
    max_lag: int | None = None,
    limit_per_combo: int | None = None,
) -> dict[str, Any]:
    own = con is None
    if con is None:
        from engine.runtime.storage import connect, init_db

        init_db()
        con = connect()
    now_value = int(now_ms if now_ms is not None else _now_ms())
    features_value = tuple(features if features is not None else _default_features())
    targets_value = tuple(targets if targets is not None else _env_list("CAUSAL_TARGETS", DEFAULT_TARGETS))
    windows_value = tuple(windows if windows is not None else _env_list("CAUSAL_WINDOWS", DEFAULT_WINDOWS))
    min_obs_value = max(5, int(min_obs if min_obs is not None else _env_int("CAUSAL_MIN_OBS", 80)))
    max_lag_value = max(1, int(max_lag if max_lag is not None else _env_int("CAUSAL_MAX_LAG", 10)))
    limit_value = max(min_obs_value, int(limit_per_combo if limit_per_combo is not None else _env_int("CAUSAL_LIMIT", 5000)))

    written = 0
    skipped = 0
    failed = 0
    decisions: dict[str, int] = {}
    try:
        ensure_causal_schema(con)
        dags = load_causal_dags(con)
        for feature in features_value:
            feature_key = str(feature or "").strip()
            if not feature_key:
                continue
            for target in targets_value:
                target_key = str(target or "").strip()
                if not target_key or target_key == feature_key:
                    continue
                for window in windows_value:
                    window_key = str(window or "").strip() or "all"
                    dag = _matching_dag(dags, feature=feature_key, target=target_key)
                    requested_controls = tuple(dag.confounders) if dag is not None else ()
                    rows = _load_observations(
                        con,
                        feature=feature_key,
                        target=target_key,
                        window=window_key,
                        now_ms=now_value,
                        limit=limit_value,
                        controls=requested_controls,
                    )
                    decision = "granger_only"
                    granger_p = 1.0
                    granger_lag = 0
                    dowhy_effect = None
                    dowhy_p = None
                    dowhy_t = None
                    if len(rows) < min_obs_value:
                        decision = "insufficient_data"
                        skipped += 1
                    else:
                        try:
                            series, granger_controls = _as_series(
                                rows,
                                feature=feature_key,
                                target=target_key,
                                controls=requested_controls,
                            )
                            result = granger_causality(
                                series,
                                cause=feature_key,
                                effect=target_key,
                                controls=granger_controls,
                                max_lag=max_lag_value,
                            )
                            granger_p = float(result.p_value)
                            granger_lag = int(result.lag)
                        except Exception:
                            decision = "failed_granger"
                            failed += 1

                        if decision != "failed_granger":
                            if dag is not None:
                                frame, _ = _as_series(
                                    rows,
                                    feature=feature_key,
                                    target=target_key,
                                    controls=dag.confounders,
                                )
                                dowhy_result = run_dowhy(frame, dag)
                                dowhy_effect = dowhy_result.effect
                                dowhy_p = dowhy_result.p_value
                                dowhy_t = _dowhy_t(dowhy_result.effect, dowhy_result.effect_se)
                                decision = str(dowhy_result.decision or "estimated")
                    score = causal_score(granger_p=granger_p, dowhy_t=dowhy_t)
                    upsert_causal_score(
                        con,
                        CausalScoreRecord(
                            feature=feature_key,
                            target=target_key,
                            window=window_key,
                            ts=now_value,
                            granger_p=float(granger_p),
                            granger_lag=int(granger_lag),
                            dowhy_effect=dowhy_effect,
                            dowhy_p=dowhy_p,
                            score=float(score),
                            decision=decision,
                        ),
                    )
                    written += 1
                    decisions[decision] = int(decisions.get(decision, 0) + 1)
        con.commit()
        return {
            "ok": True,
            "written": int(written),
            "skipped": int(skipped),
            "failed": int(failed),
            "decisions": decisions,
            "features": int(len(features_value)),
            "targets": int(len(targets_value)),
            "windows": int(len(windows_value)),
            "ts": int(now_value),
        }
    finally:
        if own:
            try:
                con.close()
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


def run(*, con=None) -> dict[str, Any]:
    return run_causal_scoring(con=con)


def main() -> None:
    print(run())


if __name__ == "__main__":
    main()
