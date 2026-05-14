"""
FILE: validation.py

Human-readable purpose:
Persists predictions and computes validation metrics by joining predictions to
realized labels. This is the bridge between model output and evidence about how
well those outputs performed historically.
"""

import json
import logging
import time
from typing import Any, Optional

from engine.prediction_logger import DEFAULT_PREDICTION_LOGGER
from engine.regime_detector import has_known_regime, normalize_regime_state, regime_signature
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, get_timescale_client, init_db, register_after_commit, run_write_txn

LOG = get_logger("strategy.validation")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_validation_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.validation",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

def init_validation_db():
    init_db()

def _safe_str(value: Any) -> Optional[str]:
    s = str(value or "").strip()
    return s or None


def _resolve_storage_regime(
    regime: Any,
    *,
    symbol: str,
    ts_ms: int,
    tracking_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidate = regime
    if candidate is None and isinstance(tracking_metadata, dict):
        candidate = tracking_metadata.get("regime")
    normalized = normalize_regime_state(candidate, symbol=symbol, ts_ms=ts_ms)
    if has_known_regime(normalized):
        return normalized
    persisted = _load_persisted_regime(symbol=symbol, ts_ms=int(normalized["time"] or ts_ms))
    if persisted is not None and has_known_regime(persisted):
        return persisted
    return normalized


def _load_persisted_regime(*, symbol: str, ts_ms: int) -> dict[str, Any] | None:
    con = None
    try:
        con = connect(readonly=True, timeout_s=0.50)
        row = con.execute(
            """
            SELECT time, symbol, volatility_regime, trend_regime, liquidity_regime
            FROM regime_state
            WHERE symbol=? AND time<=?
            ORDER BY time DESC
            LIMIT 1
            """,
            (str(symbol), int(ts_ms)),
        ).fetchone()
    except Exception as exc:
        _warn_nonfatal(
            "STRATEGY_VALIDATION_REGIME_LOOKUP_FAILED",
            exc,
            once_key=f"regime_lookup:{symbol}:{ts_ms}",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return None
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                LOG.debug("Ignored recoverable exception.", exc_info=True)
    if row is None:
        return None
    return normalize_regime_state(
        {
            "time": int(row[0] or 0),
            "symbol": str(row[1] or symbol),
            "volatility_regime": row[2],
            "trend_regime": row[3],
            "liquidity_regime": row[4],
            "source": "db",
        },
        symbol=symbol,
        ts_ms=ts_ms,
    )


def _register_timescale_prediction_after_commit(
    con,
    *,
    ts_ms: int,
    symbol: str,
    model_id: Optional[str],
    model_name: Optional[str],
    predicted_z: float,
    confidence: float,
) -> None:
    client = get_timescale_client()
    if client is None or not bool(getattr(client, "enabled", False)):
        return

    payload = {
        "model_id": str(model_id or model_name or "baseline"),
        "symbol": str(symbol),
        "timestamp": int(ts_ms),
        "prediction": float(predicted_z),
        "confidence": float(confidence),
    }

    def _enqueue() -> None:
        try:
            client.enqueue_model_predictions((payload,))
        except Exception as e:
            _warn_nonfatal(
                "VALIDATION_TIMESCALE_ENQUEUE_FAILED",
                e,
                once_key="validation_timescale_enqueue",
                model_id=str(payload["model_id"]),
                symbol=str(payload["symbol"]),
            )

    register_after_commit(con, _enqueue)


def _register_prediction_tracking_after_commit(
    con,
    *,
    prediction_id: int,
    event_id: int,
    symbol: str,
    horizon_s: int,
    ts_ms: int,
    predicted_z: float,
    confidence: float,
    model_name: Optional[str],
    model_id: Optional[str],
    model_version: Optional[str],
    features_version: Optional[str],
    tracking_source: Optional[str],
    metadata: Optional[dict[str, Any]] = None,
) -> None:
    payload = {
        "prediction_id": int(prediction_id),
        "event_id": int(event_id),
        "symbol": str(symbol),
        "horizon_s": int(horizon_s),
        "ts_ms": int(ts_ms),
        "prediction": float(predicted_z),
        "confidence": float(confidence),
        "model_name": str(model_name or model_id or "baseline"),
        "model_id": str(model_id or model_name or "baseline"),
        "model_version": str(model_version or "linked"),
        "features_version": str(features_version or "unknown"),
        "tracking_source": str(tracking_source or "validation_store_prediction"),
        "metadata": dict(metadata or {}),
    }

    def _enqueue() -> None:
        try:
            DEFAULT_PREDICTION_LOGGER.log_prediction_nowait(
                model_name=str(payload["model_name"]),
                model_version=str(payload["model_version"]),
                symbol=str(payload["symbol"]),
                timestamp=int(payload["ts_ms"]),
                prediction=float(payload["prediction"]),
                confidence=float(payload["confidence"]),
                features_version=str(payload["features_version"]),
                event_id=int(payload["event_id"]),
                horizon_s=int(payload["horizon_s"]),
                prediction_id=int(payload["prediction_id"]),
                model_id=str(payload["model_id"]),
                tracking_source=str(payload["tracking_source"]),
                metadata=dict(payload["metadata"]),
            )
        except Exception as e:
            _warn_nonfatal(
                "VALIDATION_PREDICTION_TRACKING_ENQUEUE_FAILED",
                e,
                once_key="validation_prediction_tracking_enqueue",
                prediction_id=int(payload["prediction_id"]),
                symbol=str(payload["symbol"]),
            )

    register_after_commit(con, _enqueue)


def store_prediction(
    event_id,
    symbol,
    horizon_s,
    predicted_z,
    confidence,
    *,
    confidence_raw=None,
    prediction_strength=None,
    model_name=None,
    model_id=None,
    model_version=None,
    features_version=None,
    tracking_source=None,
    tracking_metadata=None,
    regime=None,
    con=None,
):
    if con is None:
        init_validation_db()

    ts_ms = int(time.time() * 1000)
    symbol_key = str(symbol)
    storage_regime = _resolve_storage_regime(
        regime,
        symbol=symbol_key,
        ts_ms=ts_ms,
        tracking_metadata=dict(tracking_metadata or {}),
    )
    tracking_metadata_payload = dict(tracking_metadata or {})
    tracking_metadata_payload["regime"] = {
        "time": int(storage_regime["time"]),
        "symbol": str(storage_regime["symbol"]),
        "volatility_regime": str(storage_regime["volatility_regime"]),
        "trend_regime": str(storage_regime["trend_regime"]),
        "liquidity_regime": str(storage_regime["liquidity_regime"]),
    }
    tracking_metadata_payload["regime_key"] = regime_signature(storage_regime)

    params = (
        int(ts_ms),
        int(event_id),
        str(symbol_key),
        int(horizon_s),
        float(predicted_z),
        float(confidence),
        (float(confidence_raw) if confidence_raw is not None else float(confidence)),
        (float(prediction_strength) if prediction_strength is not None else None),
        _safe_str(model_name),
        _safe_str(model_id),
        _safe_str(model_version),
        int(storage_regime["time"]),
        str(storage_regime["volatility_regime"]),
        str(storage_regime["trend_regime"]),
        str(storage_regime["liquidity_regime"]),
    )

    def _write(con):
        con.execute(
            """
            INSERT INTO prediction_history(
              ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
              confidence_raw, prediction_strength,
              model_name, model_id, model_version,
              regime_time_ms, volatility_regime, trend_regime, liquidity_regime
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            params,
        )
        _register_timescale_prediction_after_commit(
            con,
            ts_ms=int(params[0]),
            symbol=str(params[2]),
            model_id=_safe_str(model_id),
            model_name=_safe_str(model_name),
            predicted_z=float(params[4]),
            confidence=float(params[5]),
        )
        con.execute(
            """
            DELETE FROM predictions
            WHERE event_id=? AND symbol=? AND horizon_s=?
            """,
            (int(params[1]), str(params[2]), int(params[3])),
        )
        con.execute(
            """
            INSERT INTO predictions(
              ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
              confidence_raw, prediction_strength,
              model_name, model_id, model_version,
              regime_time_ms, volatility_regime, trend_regime, liquidity_regime
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            params,
        )
        prediction_row = con.execute(
            """
            SELECT id
            FROM predictions
            WHERE event_id=? AND symbol=? AND horizon_s=?
            LIMIT 1
            """,
            (int(params[1]), str(params[2]), int(params[3])),
        ).fetchone()
        prediction_id = int((prediction_row or [0])[0] or 0)
        if prediction_id > 0:
            _register_prediction_tracking_after_commit(
                con,
                prediction_id=int(prediction_id),
                event_id=int(params[1]),
                symbol=str(params[2]),
                horizon_s=int(params[3]),
                ts_ms=int(params[0]),
                predicted_z=float(params[4]),
                confidence=float(params[5]),
                model_name=_safe_str(model_name),
                model_id=_safe_str(model_id),
                model_version=_safe_str(model_version),
                features_version=_safe_str(features_version),
                tracking_source=_safe_str(tracking_source),
                metadata=dict(tracking_metadata_payload),
            )

    if con is not None:
        _write(con)
        return

    run_write_txn(_write)

def compute_validation_scores():
    """
    Joins predictions with realized labels and computes MAE / RMSE.
    """
    init_validation_db()
    result = {"count": 0}

    def _write(con):
        # Validation is allowed to no-op early in bootstrap when the labels
        # table has not been produced yet.
        try:
            con.execute("SELECT 1 FROM labels LIMIT 1").fetchone()
        except Exception as e:
            _warn_nonfatal(
                "VALIDATION_LABELS_PROBE_FAILED",
                e,
                once_key="labels_probe_compute_validation_scores",
                scope="compute_validation_scores",
            )
            result["count"] = 0
            return

        rows = con.execute(
            """
            SELECT
              p.symbol,
              p.horizon_s,
              p.predicted_z,
              l.impact_z
            FROM predictions p
            JOIN labels l
              ON l.event_id = p.event_id
             AND l.symbol = p.symbol
             AND l.horizon_s = p.horizon_s
            """
        ).fetchall()

        if not rows:
            result["count"] = 0
            return

        from collections import defaultdict
        import math

        acc = defaultdict(list)
        for sym, h, pred, real in rows:
            acc[(sym, h)].append((float(pred), float(real)))

        now_ms = int(time.time() * 1000)
        cur = con.cursor()

        for (sym, h), vals in acc.items():
            n = len(vals)
            errs = [(p - r) for p, r in vals]
            mae = sum(abs(e) for e in errs) / n
            rmse = math.sqrt(sum(e * e for e in errs) / n)

            cur.execute(
                """
                INSERT INTO validation_scores(
                  ts_ms, symbol, horizon_s, mae, rmse, n
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, horizon_s) DO UPDATE SET
                  ts_ms=excluded.ts_ms,
                  mae=excluded.mae,
                  rmse=excluded.rmse,
                  n=excluded.n
                """,
                (now_ms, str(sym), int(h), float(mae), float(rmse), int(n)),
            )

        result["count"] = len(acc)

    run_write_txn(_write)
    return int(result["count"])

def _compute_metrics_for_group(vals, err_threshold=1.0, n_bins=10):
    import math

    n = len(vals)
    preds = [float(p) for p, _, _ in vals]
    reals = [float(r) for _, r, _ in vals]
    confs = [float(c) for _, _, c in vals]

    errs = [p - r for p, r in zip(preds, reals)]
    abs_errs = [abs(e) for e in errs]

    mae = sum(abs_errs) / n
    rmse = math.sqrt(sum(e * e for e in errs) / n)

    y_mean = sum(reals) / n
    ss_tot = sum((r - y_mean) ** 2 for r in reals)
    ss_res = sum((r - p) ** 2 for p, r in zip(preds, reals))
    r2 = 0.0 if ss_tot <= 1e-12 else float(1.0 - (ss_res / ss_tot))

    def sgn(x):
        if x > 0:
            return 1
        if x < 0:
            return -1
        return 0

    dir_hits = 0
    dir_n = 0
    for p, r in zip(preds, reals):
        sp = sgn(p)
        sr = sgn(r)
        if sp == 0 or sr == 0:
            continue
        dir_n += 1
        if sp == sr:
            dir_hits += 1
    dir_acc = float(dir_hits / dir_n) if dir_n else 0.0

    nb = max(2, min(50, int(n_bins)))
    bins = [{"n": 0, "avg_conf": 0.0, "acc": 0.0} for _ in range(nb)]
    for ae, c in zip(abs_errs, confs):
        cc = c
        if cc != cc:
            cc = 0.0
        cc = max(0.0, min(1.0, float(cc)))
        bi = min(nb - 1, int(cc * nb))
        bins[bi]["n"] += 1
        bins[bi]["avg_conf"] += cc
        bins[bi]["acc"] += 1.0 if ae <= float(err_threshold) else 0.0

    ece = 0.0
    for b in bins:
        if b["n"] <= 0:
            continue
        b["avg_conf"] = b["avg_conf"] / b["n"]
        b["acc"] = b["acc"] / b["n"]
        frac = b["n"] / n
        ece += frac * abs(b["acc"] - b["avg_conf"])

    return {
        "mae": float(mae),
        "rmse": float(rmse),
        "r2": float(r2),
        "direction_acc": float(dir_acc),
        "avg_conf": float(sum(confs) / n),
        "err_threshold": float(err_threshold),
        "ece": float(ece),
        "abs_err_p50": float(sorted(abs_errs)[int(0.50 * (n - 1))]),
        "abs_err_p90": float(sorted(abs_errs)[int(0.90 * (n - 1))]),
        "bins": bins,
    }

def compute_model_metrics(model_name="default", model_id=None, err_threshold=1.0, n_bins=10):
    init_validation_db()
    result = {"count": 0}

    def _write(con):
        try:
            con.execute("SELECT 1 FROM labels LIMIT 1").fetchone()
        except Exception as e:
            _warn_nonfatal(
                "VALIDATION_LABELS_PROBE_FAILED",
                e,
                once_key="labels_probe_compute_model_metrics",
                scope="compute_model_metrics",
            )
            result["count"] = 0
            return

        model_name_filter = _safe_str(model_name)
        model_id_filter = _safe_str(model_id)
        where_sql = ""
        params = []
        if model_id_filter:
            where_sql = "WHERE COALESCE(NULLIF(TRIM(p.model_id), ''), '') = ?"
            params.append(str(model_id_filter))
        elif model_name_filter:
            where_sql = "WHERE COALESCE(NULLIF(TRIM(p.model_name), ''), '') = ?"
            params.append(str(model_name_filter))

        rows = con.execute(
            f"""
            SELECT
              p.symbol,
              p.horizon_s,
              p.predicted_z,
              l.impact_z,
              p.confidence
            FROM predictions p
            JOIN labels l
              ON l.event_id = p.event_id
             AND l.symbol = p.symbol
             AND l.horizon_s = p.horizon_s
            {where_sql}
            """,
            tuple(params),
        ).fetchall()

        if not rows:
            result["count"] = 0
            return

        from collections import defaultdict
        acc = defaultdict(list)
        for sym, h, pred, real, conf in rows:
            try:
                acc[(str(sym), int(h))].append((float(pred), float(real), float(conf)))
            except Exception as e:
                _warn_nonfatal(
                    "VALIDATION_MODEL_METRICS_ROW_PARSE_FAILED",
                    e,
                    once_key="model_metrics_row_parse",
                    symbol=str(sym),
                    horizon=repr(h)[:120],
                )
                continue

        now_ms = int(time.time() * 1000)
        cur = con.cursor()

        for (sym, h), vals in acc.items():
            n = len(vals)
            metrics = _compute_metrics_for_group(vals, float(err_threshold), int(n_bins))
            metrics_json = json.dumps(metrics, separators=(",", ":"), sort_keys=True)

            cur.execute(
                """
                INSERT INTO model_metrics(
                  ts_ms, model_name, symbol, horizon_s, n, metrics_json
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(model_name, symbol, horizon_s) DO UPDATE SET
                  ts_ms=excluded.ts_ms,
                  n=excluded.n,
                  metrics_json=excluded.metrics_json
                """,
                (now_ms, str(model_name_filter or model_name or "default"), str(sym), int(h), int(n), metrics_json),
            )

        result["count"] = len(acc)

    run_write_txn(_write)
    return int(result["count"])

def get_validation_scores():
    con = connect(readonly=True)
    try:
        return con.execute(
            """
            SELECT symbol, horizon_s, mae, rmse, n, ts_ms
            FROM validation_scores
            ORDER BY symbol, horizon_s
            """
        ).fetchall()
    finally:
        con.close()

def get_model_metrics(model_name="default"):
    con = connect()
    try:
        try:
            con.execute("SELECT 1 FROM labels LIMIT 1").fetchone()
        except Exception as e:
            _warn_nonfatal(
                "VALIDATION_LABELS_PROBE_FAILED",
                e,
                once_key="labels_probe_get_model_metrics",
                scope="get_model_metrics",
            )
            return []

        rows = con.execute(
            """
            SELECT symbol, horizon_s, n, ts_ms, metrics_json
            FROM model_metrics
            WHERE model_name=?
            ORDER BY symbol, horizon_s
            """,
            (str(model_name),),
        ).fetchall()

        out = []
        for sym, h, n, ts_ms, mj in rows:
            try:
                metrics = json.loads(mj) if mj else {}
            except Exception:
                metrics = {}
            out.append({
                "model_name": str(model_name),
                "symbol": str(sym),
                "horizon_s": int(h),
                "n": int(n),
                "ts_ms": int(ts_ms),
                "metrics": metrics,
            })
        return out
    finally:
        con.close()
