"""ADWIN drift detection over champion residual streams.

This module extends the existing drift-retrain path with a statistically
grounded residual detector.  ADWIN observes absolute prediction residuals for
the currently assigned champion and emits into the existing
``drift_retrain_events`` / ``model_lifecycle_runs`` tables; it never promotes or
serves a replacement model directly.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

import numpy as np

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db

TRIGGER_TYPE = "adwin_residual"
TRIGGERED_BY = "adwin_residual_drift"
DEFAULT_DELTA = float(os.environ.get("ADWIN_DELTA", "0.002") or 0.002)
DEFAULT_MIN_WINDOW = int(os.environ.get("ADWIN_MIN_WINDOW", "16") or 16)
DEFAULT_MAX_WINDOW = int(os.environ.get("ADWIN_MAX_WINDOW", "2048") or 2048)
DEFAULT_HEDGE_LOOKBACK_MS = int(os.environ.get("ADWIN_HEDGE_TRIGGER_LOOKBACK_MS", str(7 * 24 * 60 * 60 * 1000)))
LOG = get_logger("engine.strategy.adwin_drift")
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
        component="engine.strategy.adwin_drift",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _json_loads(value: Any, default: Any) -> Any:
    if isinstance(value, type(default)):
        return value
    if value in (None, "", b"", bytearray()):
        return default
    try:
        raw = value.decode("utf-8", "replace") if isinstance(value, (bytes, bytearray)) else str(value)
        out = json.loads(raw)
    except Exception:
        return default
    return out if isinstance(out, type(default)) else default


def _json_param(con: Any, value: Any) -> Any:
    module = str(getattr(type(con), "__module__", "") or "")
    if module.startswith("sqlite3"):
        return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)
    return value


def _family_name(model_name: str) -> str:
    name = str(model_name or "").strip()
    if not name:
        return ""
    if name.startswith("lgbm_regressor") or name.startswith("gbm_regressor"):
        return "lgbm_regressor" if name.startswith("lgbm_regressor") else "gbm_regressor"
    return name.split(".", 1)[0]


@dataclass
class ADWIN:
    """Small ADWIN-style detector for bounded-memory numeric streams.

    The detector evaluates all admissible splits inside a capped recent window
    and compares mean differences with a variance-aware Hoeffding bound.  On a
    drift signal the older side of the split is discarded, which is the adaptive
    windowing step that makes subsequent updates focus on the new regime.
    """

    delta: float = DEFAULT_DELTA
    min_window: int = DEFAULT_MIN_WINDOW
    max_window: int = DEFAULT_MAX_WINDOW
    window: list[float] = field(default_factory=list)
    n_seen: int = 0
    n_detections: int = 0

    def __post_init__(self) -> None:
        self.delta = max(1e-9, min(0.5, float(self.delta)))
        self.min_window = max(4, int(self.min_window))
        self.max_window = max(2 * self.min_window, int(self.max_window))
        self.window = [float(x) for x in list(self.window or []) if math.isfinite(float(x))]
        if len(self.window) > self.max_window:
            self.window = self.window[-self.max_window :]
        self.n_seen = max(int(self.n_seen or 0), len(self.window))
        self.n_detections = max(0, int(self.n_detections or 0))

    @property
    def width(self) -> int:
        return int(len(self.window))

    @property
    def mean(self) -> float:
        if not self.window:
            return 0.0
        return float(np.mean(np.asarray(self.window, dtype=np.float64)))

    def to_json(self) -> dict[str, Any]:
        return {
            "delta": float(self.delta),
            "min_window": int(self.min_window),
            "max_window": int(self.max_window),
            "window": list(self.window),
            "n_seen": int(self.n_seen),
            "n_detections": int(self.n_detections),
        }

    @classmethod
    def from_json(cls, payload: Mapping[str, Any] | None, *, delta: float = DEFAULT_DELTA) -> "ADWIN":
        data = dict(payload or {})
        return cls(
            delta=_safe_float(data.get("delta"), delta),
            min_window=_safe_int(data.get("min_window"), DEFAULT_MIN_WINDOW),
            max_window=_safe_int(data.get("max_window"), DEFAULT_MAX_WINDOW),
            window=[_safe_float(x, 0.0) for x in list(data.get("window") or [])],
            n_seen=_safe_int(data.get("n_seen"), 0),
            n_detections=_safe_int(data.get("n_detections"), 0),
        )

    def update(self, value: float) -> dict[str, Any]:
        x = _safe_float(value, float("nan"))
        if not math.isfinite(x):
            return {"drift": False, "reason": "nonfinite_value"}
        self.window.append(float(x))
        self.n_seen += 1
        if len(self.window) > self.max_window:
            self.window = self.window[-self.max_window :]

        candidate = self._best_split()
        if not candidate:
            return {"drift": False, "width": self.width, "mean": self.mean}

        self.n_detections += 1
        cut = int(candidate["cut"])
        old_width = self.width
        self.window = self.window[cut:]
        return {
            "drift": True,
            "cut": int(cut),
            "old_width": int(old_width),
            "new_width": int(self.width),
            "mean_left": float(candidate["mean_left"]),
            "mean_right": float(candidate["mean_right"]),
            "diff": float(candidate["diff"]),
            "epsilon": float(candidate["epsilon"]),
            "n_seen": int(self.n_seen),
            "n_detections": int(self.n_detections),
        }

    def _best_split(self) -> dict[str, Any] | None:
        n = len(self.window)
        if n < 2 * self.min_window:
            return None
        arr = np.asarray(self.window, dtype=np.float64)
        prefix = np.concatenate(([0.0], np.cumsum(arr)))
        prefix_sq = np.concatenate(([0.0], np.cumsum(arr * arr)))
        total = float(prefix[-1])
        total_sq = float(prefix_sq[-1])
        mean_all = total / max(1, n)
        variance = max(1e-12, (total_sq / max(1, n)) - mean_all * mean_all)
        # ADWIN-style confidence term.  The log(log(n)) factor compensates for
        # scanning many cut points without making the detector trigger-happy.
        dd = math.log(max(math.e, 2.0 * math.log(max(math.e, float(n))) / self.delta))

        best: dict[str, Any] | None = None
        for cut in range(self.min_window, n - self.min_window + 1):
            n0 = cut
            n1 = n - cut
            sum0 = float(prefix[cut])
            sum1 = total - sum0
            mean0 = sum0 / n0
            mean1 = sum1 / n1
            inv_m = (1.0 / n0) + (1.0 / n1)
            epsilon = math.sqrt(2.0 * variance * inv_m * dd) + (2.0 / 3.0) * inv_m * dd
            diff = abs(mean0 - mean1)
            margin = diff - epsilon
            if margin > 0.0 and (best is None or margin > float(best["margin"])):
                best = {
                    "cut": int(cut),
                    "mean_left": float(mean0),
                    "mean_right": float(mean1),
                    "diff": float(diff),
                    "epsilon": float(epsilon),
                    "margin": float(margin),
                }
        return best


def ensure_adwin_schema(con: Any) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS champion_residual_adwin_state (
            model_name TEXT NOT NULL,
            family TEXT NOT NULL DEFAULT '',
            symbol TEXT NOT NULL,
            horizon_s BIGINT NOT NULL,
            delta DOUBLE PRECISION NOT NULL DEFAULT 0.002,
            window_json JSONB NOT NULL DEFAULT '[]',
            n_seen BIGINT NOT NULL DEFAULT 0,
            n_detections BIGINT NOT NULL DEFAULT 0,
            last_decision_ts_ms BIGINT NOT NULL DEFAULT 0,
            width BIGINT NOT NULL DEFAULT 0,
            mean DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            updated_ts_ms BIGINT NOT NULL,
            PRIMARY KEY(model_name, symbol, horizon_s)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_champion_residual_adwin_symbol
          ON champion_residual_adwin_state(symbol, horizon_s, updated_ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS drift_retrain_events (
            id BIGSERIAL PRIMARY KEY,
            created_ts BIGINT NOT NULL,
            model_name TEXT NOT NULL DEFAULT '',
            family TEXT,
            trigger_type TEXT,
            trigger_metrics JSONB NOT NULL DEFAULT '{}',
            action_taken TEXT,
            cooldown_applied BOOLEAN NOT NULL DEFAULT FALSE,
            candidate_version TEXT,
            outcome_status TEXT,
            diagnostics JSONB NOT NULL DEFAULT '{}'
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS model_lifecycle_runs (
            id BIGSERIAL PRIMARY KEY,
            model_name TEXT NOT NULL,
            model_version TEXT,
            parent_version TEXT,
            action TEXT NOT NULL,
            status TEXT NOT NULL,
            triggered_by TEXT,
            mutation_kind TEXT,
            details_json JSONB,
            created_ts_ms BIGINT NOT NULL,
            updated_ts_ms BIGINT NOT NULL
        )
        """
    )


def _table_columns(con: Any, table: str) -> set[str]:
    pragma_error: BaseException | None = None
    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall()
        cols = {str(row[1]) for row in rows or [] if len(row) > 1}
        if cols:
            return cols
    except Exception as e:
        pragma_error = e
    try:
        rows = con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name=?
            """,
            (str(table),),
        ).fetchall()
        return {str(row[0]) for row in rows or []}
    except Exception as e:
        _warn_nonfatal(
            "ADWIN_DRIFT_TABLE_COLUMNS_FAILED",
            e,
            once_key=f"table_columns:{table}",
            table=str(table),
            pragma_error=(
                f"{type(pragma_error).__name__}: {pragma_error}"
                if pragma_error is not None
                else ""
            ),
        )
        return set()


def _table_exists(con: Any, table: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def _champion_rows(con: Any) -> list[dict[str, Any]]:
    if not _table_exists(con, "champion_assignments"):
        return []
    rows = con.execute(
        """
        SELECT symbol, horizon_s, model_name, regime, state
        FROM champion_assignments
        WHERE lower(COALESCE(state, ''))='champion'
        ORDER BY symbol, horizon_s, model_name
        """
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows or []:
        model_name = str(row[2] or "").strip()
        if not model_name:
            continue
        out.append(
            {
                "symbol": str(row[0] or "*").upper().strip() or "*",
                "horizon_s": _safe_int(row[1], 0),
                "model_name": model_name,
                "family": _family_name(model_name),
                "regime": str(row[3] or "global"),
            }
        )
    return out


def _load_state(con: Any, champion: Mapping[str, Any], *, delta: float) -> tuple[ADWIN, int]:
    row = con.execute(
        """
        SELECT delta, window_json, n_seen, n_detections, last_decision_ts_ms
        FROM champion_residual_adwin_state
        WHERE model_name=? AND symbol=? AND horizon_s=?
        """,
        (
            str(champion.get("model_name") or ""),
            str(champion.get("symbol") or "*").upper(),
            int(champion.get("horizon_s") or 0),
        ),
    ).fetchone()
    if not row:
        return ADWIN(delta=float(delta)), 0
    payload = _json_loads(row[1], [])
    if isinstance(payload, list):
        payload = {
            "window": payload,
            "delta": _safe_float(row[0], delta),
            "n_seen": _safe_int(row[2], 0),
            "n_detections": _safe_int(row[3], 0),
        }
    state = ADWIN.from_json(payload if isinstance(payload, Mapping) else {}, delta=_safe_float(row[0], delta))
    return state, _safe_int(row[4], 0)


def _save_state(con: Any, champion: Mapping[str, Any], state: ADWIN, *, last_decision_ts_ms: int, now_ms: int) -> None:
    payload = state.to_json()
    con.execute(
        """
        INSERT INTO champion_residual_adwin_state(
          model_name, family, symbol, horizon_s, delta, window_json, n_seen,
          n_detections, last_decision_ts_ms, width, mean, updated_ts_ms
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(model_name, symbol, horizon_s) DO UPDATE SET
          family=excluded.family,
          delta=excluded.delta,
          window_json=excluded.window_json,
          n_seen=excluded.n_seen,
          n_detections=excluded.n_detections,
          last_decision_ts_ms=excluded.last_decision_ts_ms,
          width=excluded.width,
          mean=excluded.mean,
          updated_ts_ms=excluded.updated_ts_ms
        """,
        (
            str(champion.get("model_name") or ""),
            str(champion.get("family") or _family_name(str(champion.get("model_name") or ""))),
            str(champion.get("symbol") or "*").upper(),
            int(champion.get("horizon_s") or 0),
            float(state.delta),
            _json_param(con, payload),
            int(state.n_seen),
            int(state.n_detections),
            int(last_decision_ts_ms),
            int(state.width),
            float(state.mean),
            int(now_ms),
        ),
    )


def _target_expression(label_cols: set[str], exec_cols: set[str]) -> tuple[str, str]:
    label_target = "l.impact_z" if "impact_z" in label_cols else ""
    if "realized_z" in label_cols and label_target:
        label_target = "COALESCE(l.realized_z, l.impact_z)"
    elif "realized_z" in label_cols:
        label_target = "l.realized_z"
    elif "realized_ret" in label_cols:
        label_target = "l.realized_ret"
    if not label_target:
        return "", ""
    join_sql = ""
    target = label_target
    if exec_cols and "net_z" in exec_cols:
        join_sql = """
        LEFT JOIN labels_exec le
          ON le.event_id=d.event_id
         AND upper(le.symbol)=upper(d.symbol)
         AND le.horizon_s=d.horizon_s
        """
        if "realized" in exec_cols:
            target = f"COALESCE(CASE WHEN le.realized=1 THEN le.net_z ELSE NULL END, {label_target})"
        else:
            target = f"COALESCE(le.net_z, {label_target})"
    return target, join_sql


def champion_residual_rows(con: Any, champion: Mapping[str, Any], *, after_ts_ms: int = 0) -> list[dict[str, Any]]:
    if not _table_exists(con, "decision_log") or not _table_exists(con, "labels"):
        return []
    d_cols = _table_columns(con, "decision_log")
    l_cols = _table_columns(con, "labels")
    le_cols = _table_columns(con, "labels_exec") if _table_exists(con, "labels_exec") else set()
    required = {"event_id", "symbol", "horizon_s", "predicted_z", "model_name", "ts_ms"}
    if not required.issubset(d_cols) or not {"event_id", "symbol", "horizon_s"}.issubset(l_cols):
        return []
    target_expr, exec_join = _target_expression(l_cols, le_cols)
    if not target_expr:
        return []
    decision_id_expr = "d.id" if "id" in d_cols else "d.event_id"
    rows = con.execute(
        f"""
        SELECT {decision_id_expr} AS decision_id,
               d.ts_ms,
               d.event_id,
               d.symbol,
               d.horizon_s,
               d.model_name,
               d.predicted_z,
               {target_expr} AS realized_target
        FROM decision_log d
        JOIN labels l
          ON l.event_id=d.event_id
         AND upper(l.symbol)=upper(d.symbol)
         AND l.horizon_s=d.horizon_s
        {exec_join}
        WHERE upper(d.symbol)=?
          AND d.horizon_s=?
          AND d.model_name=?
          AND d.ts_ms>?
          AND d.predicted_z IS NOT NULL
          AND {target_expr} IS NOT NULL
        ORDER BY d.ts_ms ASC, decision_id ASC
        """,
        (
            str(champion.get("symbol") or "*").upper(),
            int(champion.get("horizon_s") or 0),
            str(champion.get("model_name") or ""),
            int(after_ts_ms),
        ),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows or []:
        pred = _safe_float(row[6], float("nan"))
        target = _safe_float(row[7], float("nan"))
        if not math.isfinite(pred) or not math.isfinite(target):
            continue
        out.append(
            {
                "decision_id": _safe_int(row[0], 0),
                "ts_ms": _safe_int(row[1], 0),
                "event_id": _safe_int(row[2], 0),
                "symbol": str(row[3] or "").upper(),
                "horizon_s": _safe_int(row[4], 0),
                "model_name": str(row[5] or ""),
                "predicted_z": float(pred),
                "realized_target": float(target),
                "abs_residual": float(abs(pred - target)),
            }
        )
    return out


def _insert_lifecycle_run(con: Any, *, champion: Mapping[str, Any], metrics: Mapping[str, Any], now_ms: int) -> int:
    version = f"adwin-{int(now_ms)}"
    details = {
        "trigger": dict(metrics or {}),
        "source": TRIGGERED_BY,
        "symbol": str(champion.get("symbol") or "*").upper(),
        "horizon_s": int(champion.get("horizon_s") or 0),
    }
    cur = con.execute(
        """
        INSERT INTO model_lifecycle_runs(
          model_name, model_version, parent_version, action, status, triggered_by,
          mutation_kind, details_json, created_ts_ms, updated_ts_ms
        )
        VALUES(?,?,?,?,?,?,?,?,?,?)
        """,
        (
            str(champion.get("model_name") or ""),
            version,
            None,
            "drift_triggered_retrain",
            "queued",
            TRIGGERED_BY,
            "drift_retrain",
            _json_param(con, details),
            int(now_ms),
            int(now_ms),
        ),
    )
    return _safe_int(getattr(cur, "lastrowid", 0), 0)


def _insert_drift_event(
    con: Any,
    *,
    champion: Mapping[str, Any],
    metrics: Mapping[str, Any],
    diagnostics: Mapping[str, Any],
    now_ms: int,
) -> int:
    cur = con.execute(
        """
        INSERT INTO drift_retrain_events(
          created_ts, model_name, family, trigger_type, trigger_metrics,
          action_taken, cooldown_applied, candidate_version, outcome_status, diagnostics
        )
        VALUES(?,?,?,?,?,?,?,?,?,?)
        """,
        (
            int(now_ms),
            str(champion.get("model_name") or ""),
            str(champion.get("family") or _family_name(str(champion.get("model_name") or ""))),
            TRIGGER_TYPE,
            _json_param(con, dict(metrics or {})),
            "queue_training",
            False,
            diagnostics.get("candidate_version"),
            "enqueued",
            _json_param(con, dict(diagnostics or {})),
        ),
    )
    return _safe_int(getattr(cur, "lastrowid", 0), 0)


def run_adwin_residual_drift(*, con: Any = None, now_ms: int | None = None, enqueue_retrain: bool = True) -> dict[str, Any]:
    """Update ADWIN states for all champion residual streams."""

    own = con is None
    if own:
        init_db()
        con = connect()
    ts_value = int(now_ms if now_ms is not None else _now_ms())
    delta = max(1e-9, min(0.5, _safe_float(os.environ.get("ADWIN_DELTA"), DEFAULT_DELTA)))
    try:
        ensure_adwin_schema(con)
        champions = _champion_rows(con)
        updated: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for champion in champions:
            state, last_ts = _load_state(con, champion, delta=delta)
            rows = champion_residual_rows(con, champion, after_ts_ms=int(last_ts))
            if not rows:
                skipped.append({**dict(champion), "reason": "no_new_matured_residuals"})
                continue
            local_last_ts = int(last_ts)
            stream_events: list[dict[str, Any]] = []
            for row in rows:
                local_last_ts = max(local_last_ts, int(row.get("ts_ms") or 0))
                result = state.update(float(row["abs_residual"]))
                if not bool(result.get("drift")):
                    continue
                metrics = {
                    **dict(result),
                    "trigger_types": [TRIGGER_TYPE],
                    "model_name": str(champion.get("model_name") or ""),
                    "family": str(champion.get("family") or ""),
                    "symbol": str(champion.get("symbol") or "*").upper(),
                    "horizon_s": int(champion.get("horizon_s") or 0),
                    "decision_ts_ms": int(row.get("ts_ms") or 0),
                    "decision_id": int(row.get("decision_id") or 0),
                    "residual": float(row.get("abs_residual") or 0.0),
                    "delta": float(state.delta),
                }
                diagnostics = {
                    "source": TRIGGERED_BY,
                    "candidate_version": f"adwin-{int(ts_value)}",
                    "enqueue_requested": bool(enqueue_retrain),
                }
                if enqueue_retrain:
                    lifecycle_id = _insert_lifecycle_run(con, champion=champion, metrics=metrics, now_ms=ts_value)
                    diagnostics["lifecycle_run_id"] = int(lifecycle_id)
                event_id = _insert_drift_event(
                    con,
                    champion=champion,
                    metrics=metrics,
                    diagnostics=diagnostics,
                    now_ms=ts_value,
                )
                event = {"event_id": int(event_id), "lifecycle_run_id": int(diagnostics.get("lifecycle_run_id") or 0), **metrics}
                stream_events.append(event)
                events.append(event)
            _save_state(con, champion, state, last_decision_ts_ms=int(local_last_ts), now_ms=ts_value)
            updated.append(
                {
                    **dict(champion),
                    "rows": int(len(rows)),
                    "width": int(state.width),
                    "mean": float(state.mean),
                    "n_seen": int(state.n_seen),
                    "n_detections": int(state.n_detections),
                    "events": stream_events,
                }
            )
        if own:
            con.commit()
        return {
            "ok": True,
            "ts_ms": int(ts_value),
            "delta": float(delta),
            "updated": updated,
            "events": events,
            "skipped": skipped,
            "event_count": int(len(events)),
        }
    finally:
        if own:
            con.close()


def _recent_adwin_events(
    con: Any,
    *,
    model_names: Iterable[str],
    symbol: str,
    horizon: int,
    now_ms: int,
    lookback_ms: int = DEFAULT_HEDGE_LOOKBACK_MS,
) -> list[dict[str, Any]]:
    names = {str(name or "").strip() for name in model_names if str(name or "").strip()}
    if not names or not _table_exists(con, "drift_retrain_events"):
        return []
    rows = con.execute(
        """
        SELECT created_ts, model_name, trigger_metrics, diagnostics
        FROM drift_retrain_events
        WHERE trigger_type=?
          AND created_ts>=?
        ORDER BY created_ts DESC
        LIMIT 100
        """,
        (TRIGGER_TYPE, int(now_ms) - int(max(0, lookback_ms))),
    ).fetchall()
    out: list[dict[str, Any]] = []
    sym = str(symbol or "*").upper().strip() or "*"
    hor = int(horizon or 0)
    for row in rows or []:
        model_name = str(row[1] or "").strip()
        if model_name not in names:
            continue
        metrics = _json_loads(row[2], {})
        if isinstance(metrics, Mapping):
            event_symbol = str(metrics.get("symbol") or "*").upper().strip() or "*"
            event_horizon = _safe_int(metrics.get("horizon_s"), 0)
            if event_symbol not in {sym, "*"} or event_horizon not in {hor, 0}:
                continue
        else:
            metrics = {}
        out.append(
            {
                "created_ts": _safe_int(row[0], 0),
                "model_name": model_name,
                "trigger_metrics": dict(metrics),
                "diagnostics": _json_loads(row[3], {}),
            }
        )
    return out


def effective_window_after_adwin(
    con: Any,
    *,
    symbol: str,
    horizon: int,
    model_names: Iterable[str],
    base_window: int,
    now_ms: int | None = None,
) -> tuple[int, dict[str, Any]]:
    """Return Hedge's residual-drift-aware effective window."""

    base = max(1, int(base_window or 1))
    ts_value = int(now_ms if now_ms is not None else _now_ms())
    lookback_ms = int(os.environ.get("ADWIN_HEDGE_TRIGGER_LOOKBACK_MS", str(DEFAULT_HEDGE_LOOKBACK_MS)) or DEFAULT_HEDGE_LOOKBACK_MS)
    events = _recent_adwin_events(
        con,
        model_names=list(model_names or []),
        symbol=str(symbol),
        horizon=int(horizon),
        now_ms=int(ts_value),
        lookback_ms=int(lookback_ms),
    )
    model_list = [str(name or "").strip() for name in list(model_names or []) if str(name or "").strip()]
    if not events:
        return base, {
            "triggered": False,
            "source": TRIGGERED_BY,
            "base_window": int(base),
            "effective_window": int(base),
            "per_model_windows": {name: int(base) for name in model_list},
        }
    drifted_models = {str(event.get("model_name") or "").strip() for event in events if str(event.get("model_name") or "").strip()}
    half_window = max(1, int(math.ceil(base / 2.0)))
    per_model_windows = {
        name: int(half_window if name in drifted_models else base)
        for name in model_list
    }
    return base, {
        "triggered": True,
        "source": TRIGGERED_BY,
        "symbol": str(symbol or "*").upper(),
        "horizon_s": int(horizon or 0),
        "base_window": int(base),
        "effective_window": int(base),
        "per_model_windows": per_model_windows,
        "drifted_models": sorted(drifted_models),
        "events": events[:10],
    }


__all__ = [
    "ADWIN",
    "DEFAULT_DELTA",
    "TRIGGER_TYPE",
    "TRIGGERED_BY",
    "champion_residual_rows",
    "effective_window_after_adwin",
    "ensure_adwin_schema",
    "run_adwin_residual_drift",
]
