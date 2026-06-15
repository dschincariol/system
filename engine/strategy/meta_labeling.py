"""Triple-barrier meta-labeling for primary model intents.

Meta-labeling is deliberately a sizing/suppression input only. The primary
model still proposes intent; execution policy and risk controls retain final
authority.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
import hashlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np

from engine.artifacts.serialization import dumps_pickle_artifact, loads_pickle_artifact
from engine.artifacts.store import LocalArtifactStore
from engine.model_registry import get_stage_latest, register_model, register_model_family
from engine.runtime.storage import connect, init_db
from engine.strategy.model_lifecycle import register_model_version
from engine.strategy.ood import extract_ood_payload

LOG = logging.getLogger(__name__)

FAMILY = "meta_label_classifier"
MODEL_KIND = "lightgbm_classifier"
META_LABEL_FEATURE_IDS = [
    "meta_label.primary_abs_z",
    "meta_label.primary_confidence",
    "meta_label.side_sign",
    "meta_label.vol_level",
    "meta_label.vol_ratio",
    "meta_label.rolling_hit_rate",
    "meta_label.regime_risk_off",
    "meta_label.regime_confidence",
    "meta_label.ood_distance",
]
DEFAULT_BARRIER_K = float(os.environ.get("META_LABEL_BARRIER_K", "1.5"))
DEFAULT_MIN_ABS_Z = float(os.environ.get("META_LABEL_ACTION_Z_THRESHOLD", os.environ.get("ACTION_Z_THRESHOLD", "0.0")))
DEFAULT_LOOKBACK = int(os.environ.get("META_LABEL_VOL_LOOKBACK", "240"))
DEFAULT_MIN_SAMPLES = int(os.environ.get("META_LABEL_MIN_SAMPLES", "80"))


def _register_family() -> None:
    try:
        register_model_family(
            FAMILY,
            training_entrypoint="engine.strategy.jobs.train_meta_label_model",
            inference_entrypoint="engine.strategy.meta_labeling.score_order_meta_label",
            default_stage="shadow",
            promotion_guard="engine.strategy.promotion_guard.assess_challenger",
            metadata={"feature_ids": list(META_LABEL_FEATURE_IDS)},
        )
    except Exception:
        LOG.debug("Ignored recoverable exception.", exc_info=True)


_register_family()


@dataclass(frozen=True)
class BarrierOutcome:
    outcome: str
    label: int
    timeout_sign: int
    exit_ts_ms: int
    realized_ret: float
    profit_take_ret: float
    stop_loss_ret: float
    max_favorable_ret: float
    max_adverse_ret: float


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


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def _json_load_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if value in (None, "", b"", bytearray()):
        return {}
    try:
        raw = value.decode("utf-8", "replace") if isinstance(value, (bytes, bytearray)) else str(value)
        out = json.loads(raw)
    except Exception:
        return {}
    return dict(out) if isinstance(out, Mapping) else {}


def _is_sqlite(con) -> bool:
    return "sqlite" in type(con).__module__.lower()


def _feature_schema() -> dict[str, Any]:
    feature_ids = list(META_LABEL_FEATURE_IDS)
    try:
        from engine.strategy.feature_registry import feature_set_tag_from_ids

        feature_set_tag = str(feature_set_tag_from_ids(feature_ids))
    except Exception:
        payload = "\n".join(feature_ids).encode("utf-8")
        feature_set_tag = hashlib.sha256(payload).hexdigest()[:16]
    return {
        "feature_ids": feature_ids,
        "feature_count": int(len(feature_ids)),
        "feature_set_tag": str(feature_set_tag),
        "target": "triple_barrier_label_profit",
    }


def ensure_schema(con) -> None:
    id_col = "INTEGER PRIMARY KEY AUTOINCREMENT" if _is_sqlite(con) else "BIGSERIAL PRIMARY KEY"
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS triple_barrier_labels (
            id {id_col},
            source_table TEXT NOT NULL,
            source_id BIGINT NOT NULL,
            event_id BIGINT,
            symbol TEXT NOT NULL,
            horizon_s BIGINT NOT NULL,
            ts_ms BIGINT NOT NULL,
            entry_ts_ms BIGINT NOT NULL,
            vertical_ts_ms BIGINT NOT NULL,
            exit_ts_ms BIGINT NOT NULL,
            side TEXT NOT NULL,
            side_sign BIGINT NOT NULL,
            model_name TEXT,
            model_id TEXT,
            model_family TEXT,
            primary_predicted_z DOUBLE PRECISION NOT NULL,
            primary_confidence DOUBLE PRECISION NOT NULL,
            sigma DOUBLE PRECISION NOT NULL,
            sigma_source TEXT NOT NULL,
            barrier_k DOUBLE PRECISION NOT NULL,
            profit_take_ret DOUBLE PRECISION NOT NULL,
            stop_loss_ret DOUBLE PRECISION NOT NULL,
            realized_ret DOUBLE PRECISION NOT NULL,
            outcome TEXT NOT NULL,
            label BIGINT NOT NULL,
            timeout_sign BIGINT NOT NULL DEFAULT 0,
            feature_ids_json TEXT,
            feature_schema_json TEXT,
            feature_values_json TEXT,
            meta_json TEXT,
            created_ts_ms BIGINT NOT NULL,
            UNIQUE(source_table, source_id)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_triple_barrier_labels_symbol_ts
          ON triple_barrier_labels(symbol, ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_triple_barrier_labels_family_ts
          ON triple_barrier_labels(model_family, ts_ms DESC)
        """
    )


def _side_from_prediction(predicted_z: float) -> tuple[str, int]:
    sign = -1 if float(predicted_z) < 0.0 else 1
    return ("SHORT" if sign < 0 else "LONG"), int(sign)


def triple_barrier_outcome(
    price_path: Sequence[tuple[int, float]],
    *,
    side_sign: int,
    sigma: float,
    barrier_k: float = DEFAULT_BARRIER_K,
) -> BarrierOutcome:
    """Return first-hit triple-barrier outcome for a single price path."""

    rows = [(int(ts), float(px)) for ts, px in list(price_path or []) if float(px) > 0.0]
    if not rows:
        raise ValueError("price_path_empty")
    entry_ts, entry_px = rows[0]
    if float(entry_px) <= 0.0:
        raise ValueError("entry_price_nonpositive")

    sign = -1 if int(side_sign) < 0 else 1
    pt = abs(float(barrier_k) * max(1.0e-12, float(sigma)))
    sl = -pt
    max_fav = 0.0
    max_adv = 0.0
    last_ts = int(entry_ts)
    last_ret = 0.0

    for ts_ms, px in rows[1:] if len(rows) > 1 else rows:
        signed_ret = sign * ((float(px) / float(entry_px)) - 1.0)
        last_ts = int(ts_ms)
        last_ret = float(signed_ret)
        max_fav = max(float(max_fav), float(signed_ret))
        max_adv = min(float(max_adv), float(signed_ret))
        if signed_ret >= pt:
            return BarrierOutcome(
                outcome="profit",
                label=1,
                timeout_sign=0,
                exit_ts_ms=int(ts_ms),
                realized_ret=float(signed_ret),
                profit_take_ret=float(pt),
                stop_loss_ret=float(sl),
                max_favorable_ret=float(max_fav),
                max_adverse_ret=float(max_adv),
            )
        if signed_ret <= sl:
            return BarrierOutcome(
                outcome="loss",
                label=0,
                timeout_sign=0,
                exit_ts_ms=int(ts_ms),
                realized_ret=float(signed_ret),
                profit_take_ret=float(pt),
                stop_loss_ret=float(sl),
                max_favorable_ret=float(max_fav),
                max_adverse_ret=float(max_adv),
            )

    timeout_sign = 1 if last_ret > 0.0 else (-1 if last_ret < 0.0 else 0)
    outcome = "timeout_profit" if timeout_sign > 0 else ("timeout_loss" if timeout_sign < 0 else "timeout_flat")
    return BarrierOutcome(
        outcome=str(outcome),
        label=0,
        timeout_sign=int(timeout_sign),
        exit_ts_ms=int(last_ts),
        realized_ret=float(last_ret),
        profit_take_ret=float(pt),
        stop_loss_ret=float(sl),
        max_favorable_ret=float(max_fav),
        max_adverse_ret=float(max_adv),
    )


def _price_path_for_intent(con, symbol: str, ts_ms: int, horizon_s: int) -> list[tuple[int, float]]:
    vertical = int(ts_ms) + int(horizon_s) * 1000
    entry = con.execute(
        """
        SELECT ts_ms, COALESCE(price, px)
        FROM prices
        WHERE symbol=?
          AND ts_ms>=?
          AND ts_ms<=?
        ORDER BY ts_ms ASC
        LIMIT 1
        """,
        (str(symbol), int(ts_ms), int(vertical)),
    ).fetchone()
    if not entry or entry[1] is None:
        return []
    entry_ts = int(entry[0])
    rows = con.execute(
        """
        SELECT ts_ms, COALESCE(price, px)
        FROM prices
        WHERE symbol=?
          AND ts_ms>=?
          AND ts_ms<=?
        ORDER BY ts_ms ASC
        """,
        (str(symbol), int(entry_ts), int(vertical)),
    ).fetchall()
    return [(int(row[0]), float(row[1])) for row in rows or [] if row and row[1] is not None and float(row[1]) > 0.0]


def _trailing_vol_asof(con, symbol: str, ts_ms: int, lookback: int = DEFAULT_LOOKBACK) -> float | None:
    rows = con.execute(
        """
        SELECT COALESCE(price, px)
        FROM prices
        WHERE symbol=?
          AND ts_ms<=?
        ORDER BY ts_ms DESC
        LIMIT ?
        """,
        (str(symbol), int(ts_ms), int(max(4, lookback))),
    ).fetchall()
    px = [float(row[0]) for row in rows or [] if row and row[0] is not None and float(row[0]) > 0.0]
    px.reverse()
    if len(px) < 4:
        return None
    rets = [math.log(px[idx] / px[idx - 1]) for idx in range(1, len(px)) if px[idx - 1] > 0.0 and px[idx] > 0.0]
    if len(rets) < 3:
        return None
    mean = sum(rets) / len(rets)
    var = sum((ret - mean) ** 2 for ret in rets) / max(1, len(rets) - 1)
    return float(max(1.0e-6, math.sqrt(max(0.0, var))))


def resolve_barrier_sigma(con, symbol: str, ts_ms: int) -> dict[str, Any]:
    try:
        from engine.strategy.har_rv import resolve_vol_forecast

        resolved = resolve_vol_forecast(con, str(symbol), ts_ms=int(ts_ms), source="har")
        vol = _safe_float(resolved.get("vol") or resolved.get("forecast_vol_1d"), 0.0)
        if vol > 0.0:
            return {
                "sigma": float(max(1.0e-6, vol)),
                "source": str(resolved.get("resolved_source") or resolved.get("source") or "har"),
                "vol_ratio": _safe_float(resolved.get("forecast_ratio"), 1.0),
            }
    except Exception:
        LOG.debug("Ignored recoverable exception.", exc_info=True)

    vol = _trailing_vol_asof(con, str(symbol), int(ts_ms))
    return {
        "sigma": float(max(1.0e-6, vol if vol is not None else 1.0e-6)),
        "source": "trailing",
        "vol_ratio": 1.0,
    }


def infer_model_family(model_name: str, model_id: str = "", model_kind: str = "") -> str:
    text = " ".join([str(model_name or ""), str(model_id or ""), str(model_kind or "")]).lower()
    for family in ("lgbm_regressor", "xgb_regressor", "patchtst", "gbm_regressor", "temporal_predictor", "embed_regressor"):
        if family in text:
            return family
    name = str(model_name or model_id or model_kind or "baseline").strip()
    return name.split(":", 1)[0] if name else "baseline"


def _rolling_hit_rate(con, *, model_family: str, symbol: str, ts_ms: int, lookback: int = 60) -> float:
    try:
        rows = con.execute(
            """
            SELECT label
            FROM triple_barrier_labels
            WHERE model_family=?
              AND symbol=?
              AND ts_ms<?
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(model_family), str(symbol), int(ts_ms), int(max(1, lookback))),
        ).fetchall()
    except Exception:
        rows = []
    labels = [int(row[0]) for row in rows or [] if row and row[0] is not None]
    if not labels:
        return 0.5
    return float(sum(labels) / max(1, len(labels)))


def _regime_features(regime_vec: Mapping[str, Any] | None) -> dict[str, float]:
    rv = dict(regime_vec or {})
    risk_off = (
        rv.get("risk_off")
        or rv.get("risk_off_score")
        or rv.get("macro_risk_off")
        or rv.get("stress_score")
        or 0.0
    )
    confidence = rv.get("confidence") if not isinstance(rv.get("confidence"), Mapping) else (rv.get("confidence") or {}).get("overall")
    return {
        "meta_label.regime_risk_off": max(0.0, min(1.0, _safe_float(risk_off, 0.0))),
        "meta_label.regime_confidence": max(0.0, min(1.0, _safe_float(confidence, 0.0))),
    }


def build_meta_label_features(
    *,
    predicted_z: float,
    confidence: float,
    side_sign: int,
    vol_level: float,
    vol_ratio: float = 1.0,
    rolling_hit_rate: float = 0.5,
    regime_vec: Mapping[str, Any] | None = None,
    ood_distance: float = 0.0,
) -> dict[str, float]:
    features = {
        "meta_label.primary_abs_z": abs(_safe_float(predicted_z, 0.0)),
        "meta_label.primary_confidence": max(0.0, min(1.0, _safe_float(confidence, 0.0))),
        "meta_label.side_sign": float(-1 if int(side_sign) < 0 else 1),
        "meta_label.vol_level": max(0.0, _safe_float(vol_level, 0.0)),
        "meta_label.vol_ratio": max(0.0, _safe_float(vol_ratio, 1.0)),
        "meta_label.rolling_hit_rate": max(0.0, min(1.0, _safe_float(rolling_hit_rate, 0.5))),
        "meta_label.ood_distance": max(0.0, _safe_float(ood_distance, 0.0)),
    }
    features.update(_regime_features(regime_vec))
    return {fid: float(features.get(fid, 0.0)) for fid in META_LABEL_FEATURE_IDS}


def feature_vector(features: Mapping[str, Any], feature_ids: Sequence[str] | None = None) -> np.ndarray:
    ids = list(feature_ids or META_LABEL_FEATURE_IDS)
    return np.asarray([_safe_float(dict(features or {}).get(fid), 0.0) for fid in ids], dtype=np.float32).reshape(1, -1)


def _candidate_rows(con, *, now_ms: int, limit: int, min_abs_z: float) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    queries = [
        (
            "decision_log",
            """
            SELECT id, event_id, symbol, horizon_s, ts_ms, predicted_z, confidence,
                   model_name, model_name AS model_id, model_kind, model_version, explain_json, extra_json
            FROM decision_log d
            WHERE ABS(predicted_z) >= ?
              AND ts_ms + horizon_s * 1000 <= ?
              AND NOT EXISTS (
                SELECT 1 FROM triple_barrier_labels t
                WHERE t.source_table='decision_log' AND t.source_id=d.id
              )
            ORDER BY ts_ms ASC
            LIMIT ?
            """,
        ),
        (
            "predictions",
            """
            SELECT id, event_id, symbol, horizon_s, ts_ms, predicted_z, confidence,
                   model_name, model_id, model_name AS model_kind, model_version, NULL, NULL
            FROM predictions p
            WHERE ABS(predicted_z) >= ?
              AND ts_ms + horizon_s * 1000 <= ?
              AND NOT EXISTS (
                SELECT 1 FROM triple_barrier_labels t
                WHERE t.source_table='predictions' AND t.source_id=p.id
              )
            ORDER BY ts_ms ASC
            LIMIT ?
            """,
        ),
    ]
    for source_table, sql in queries:
        try:
            fetched = con.execute(sql, (float(min_abs_z), int(now_ms), int(limit))).fetchall()
        except Exception:
            continue
        for row in fetched or []:
            rows.append(
                {
                    "source_table": source_table,
                    "source_id": int(row[0]),
                    "event_id": int(row[1] or 0) if row[1] is not None else None,
                    "symbol": str(row[2] or "").upper().strip(),
                    "horizon_s": int(row[3] or 0),
                    "ts_ms": int(row[4] or 0),
                    "predicted_z": _safe_float(row[5], 0.0),
                    "confidence": _safe_float(row[6], 0.0),
                    "model_name": str(row[7] or ""),
                    "model_id": str(row[8] or ""),
                    "model_kind": str(row[9] or ""),
                    "model_version": str(row[10] or ""),
                    "explain": _json_load_dict(row[11]),
                    "extra": _json_load_dict(row[12]),
                }
            )
        if len(rows) >= int(limit):
            break
    return rows[: int(limit)]


def _extract_ood_distance(candidate: Mapping[str, Any]) -> float:
    ood_payload = extract_ood_payload(candidate)
    if ood_payload:
        return max(0.0, _safe_float(ood_payload.get("ood_score", ood_payload.get("ood_distance")), 0.0))
    for payload_key in ("explain", "extra"):
        payload = dict(candidate.get(payload_key) or {})
        for key in ("ood_score", "ood_distance", "distance_to_train", "feature_ood_distance"):
            if key in payload:
                return max(0.0, _safe_float(payload.get(key), 0.0))
    return 0.0


def label_candidate(con, candidate: Mapping[str, Any], *, barrier_k: float = DEFAULT_BARRIER_K, created_ts_ms: int | None = None) -> dict[str, Any]:
    symbol = str(candidate.get("symbol") or "").upper().strip()
    horizon_s = int(candidate.get("horizon_s") or 0)
    ts_ms = int(candidate.get("ts_ms") or 0)
    predicted_z = _safe_float(candidate.get("predicted_z"), 0.0)
    confidence = _safe_float(candidate.get("confidence"), 0.0)
    if not symbol or horizon_s <= 0 or ts_ms <= 0:
        return {"ok": False, "reason": "invalid_candidate"}

    price_path = _price_path_for_intent(con, symbol, ts_ms, horizon_s)
    if not price_path:
        return {"ok": False, "reason": "missing_price_path"}

    side, side_sign = _side_from_prediction(predicted_z)
    sigma_payload = resolve_barrier_sigma(con, symbol, ts_ms)
    sigma = max(1.0e-6, _safe_float(sigma_payload.get("sigma"), 1.0e-6))
    outcome = triple_barrier_outcome(price_path, side_sign=side_sign, sigma=sigma, barrier_k=float(barrier_k))
    model_family = infer_model_family(
        str(candidate.get("model_name") or ""),
        str(candidate.get("model_id") or ""),
        str(candidate.get("model_kind") or ""),
    )
    features = build_meta_label_features(
        predicted_z=predicted_z,
        confidence=confidence,
        side_sign=side_sign,
        vol_level=sigma,
        vol_ratio=_safe_float(sigma_payload.get("vol_ratio"), 1.0),
        rolling_hit_rate=_rolling_hit_rate(con, model_family=model_family, symbol=symbol, ts_ms=ts_ms),
        regime_vec=dict((candidate.get("explain") or {}).get("regime_vector") or {}),
        ood_distance=_extract_ood_distance(candidate),
    )
    schema = _feature_schema()
    now_value = int(created_ts_ms if created_ts_ms is not None else _now_ms())
    con.execute(
        """
        INSERT INTO triple_barrier_labels(
          source_table, source_id, event_id, symbol, horizon_s, ts_ms,
          entry_ts_ms, vertical_ts_ms, exit_ts_ms, side, side_sign,
          model_name, model_id, model_family, primary_predicted_z,
          primary_confidence, sigma, sigma_source, barrier_k,
          profit_take_ret, stop_loss_ret, realized_ret, outcome, label,
          timeout_sign, feature_ids_json, feature_schema_json,
          feature_values_json, meta_json, created_ts_ms
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(source_table, source_id) DO UPDATE SET
          exit_ts_ms=excluded.exit_ts_ms,
          realized_ret=excluded.realized_ret,
          outcome=excluded.outcome,
          label=excluded.label,
          timeout_sign=excluded.timeout_sign,
          feature_values_json=excluded.feature_values_json,
          meta_json=excluded.meta_json
        """,
        (
            str(candidate.get("source_table") or ""),
            int(candidate.get("source_id") or 0),
            candidate.get("event_id"),
            symbol,
            int(horizon_s),
            int(ts_ms),
            int(price_path[0][0]),
            int(ts_ms + horizon_s * 1000),
            int(outcome.exit_ts_ms),
            str(side),
            int(side_sign),
            str(candidate.get("model_name") or ""),
            str(candidate.get("model_id") or ""),
            str(model_family),
            float(predicted_z),
            float(confidence),
            float(sigma),
            str(sigma_payload.get("source") or "trailing"),
            float(barrier_k),
            float(outcome.profit_take_ret),
            float(outcome.stop_loss_ret),
            float(outcome.realized_ret),
            str(outcome.outcome),
            int(outcome.label),
            int(outcome.timeout_sign),
            _json_dumps(list(META_LABEL_FEATURE_IDS)),
            _json_dumps(schema),
            _json_dumps(features),
            _json_dumps(
                {
                    "max_favorable_ret": float(outcome.max_favorable_ret),
                    "max_adverse_ret": float(outcome.max_adverse_ret),
                    "model_version": str(candidate.get("model_version") or ""),
                    "sigma": dict(sigma_payload),
                }
            ),
            int(now_value),
        ),
    )
    return {"ok": True, "outcome": str(outcome.outcome), "label": int(outcome.label), "features": features}


def generate_triple_barrier_labels(
    *,
    con=None,
    now_ms: int | None = None,
    limit: int | None = None,
    min_abs_z: float | None = None,
    barrier_k: float | None = None,
) -> dict[str, Any]:
    own = con is None
    con = connect() if con is None else con
    try:
        ensure_schema(con)
        now_value = int(now_ms if now_ms is not None else _now_ms())
        limit_value = int(limit if limit is not None else os.environ.get("META_LABEL_LABEL_LIMIT", "5000"))
        threshold = float(DEFAULT_MIN_ABS_Z if min_abs_z is None else min_abs_z)
        k_value = float(DEFAULT_BARRIER_K if barrier_k is None else barrier_k)
        candidates = _candidate_rows(con, now_ms=now_value, limit=limit_value, min_abs_z=threshold)
        wrote = 0
        skipped: dict[str, int] = {}
        for candidate in candidates:
            result = label_candidate(con, candidate, barrier_k=k_value, created_ts_ms=now_value)
            if bool(result.get("ok")):
                wrote += 1
            else:
                reason = str(result.get("reason") or "unknown")
                skipped[reason] = int(skipped.get(reason, 0)) + 1
        con.commit()
        return {
            "ok": True,
            "candidate_count": int(len(candidates)),
            "wrote_count": int(wrote),
            "skipped": skipped,
            "barrier_k": float(k_value),
            "min_abs_z": float(threshold),
        }
    finally:
        if own:
            try:
                con.close()
            except Exception:
                LOG.debug("Ignored recoverable exception.", exc_info=True)


def meta_label_multiplier(probability: float, *, lower: float | None = None, upper: float | None = None) -> float:
    lo = float(lower if lower is not None else os.environ.get("META_LABEL_PROB_FLOOR", "0.45"))
    hi = float(upper if upper is not None else os.environ.get("META_LABEL_PROB_FULL", "0.65"))
    p = max(0.0, min(1.0, _safe_float(probability, 0.0)))
    if p < lo:
        return 0.0
    if p >= hi:
        return 1.0
    if hi <= lo:
        return 1.0 if p >= hi else 0.0
    return float((p - lo) / (hi - lo))


def brier_score(probabilities: Iterable[float], labels: Iterable[int]) -> float:
    probs = np.asarray([max(0.0, min(1.0, _safe_float(p, 0.0))) for p in probabilities], dtype=np.float64)
    y = np.asarray([1 if int(v) else 0 for v in labels], dtype=np.float64)
    if probs.size == 0 or probs.size != y.size:
        return float("inf")
    return float(np.mean((probs - y) ** 2))


class IdentityCalibrator:
    def predict(self, values: Any) -> np.ndarray:
        return np.asarray(values, dtype=np.float64).reshape(-1)


def calibrate_probabilities(raw_probabilities: Sequence[float], labels: Sequence[int]) -> dict[str, Any]:
    raw = np.asarray([max(0.0, min(1.0, _safe_float(p, 0.0))) for p in raw_probabilities], dtype=np.float64)
    y = np.asarray([1 if int(v) else 0 for v in labels], dtype=np.int8)
    if raw.size == 0 or raw.size != y.size or len(set(int(v) for v in y.tolist())) < 2:
        return {"probabilities": raw, "calibrator": IdentityCalibrator(), "method": "identity", "brier": brier_score(raw, y), "raw_brier": brier_score(raw, y)}
    raw_brier = brier_score(raw, y)
    try:
        from sklearn.isotonic import IsotonicRegression

        calibrator = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0)
        calibrated = np.asarray(calibrator.fit_transform(raw, y), dtype=np.float64)
        calibrated = np.clip(calibrated, 0.0, 1.0)
        calibrated_brier = brier_score(calibrated, y)
        if calibrated_brier <= raw_brier + 1.0e-12:
            return {
                "probabilities": calibrated,
                "calibrator": calibrator,
                "method": "isotonic",
                "brier": float(calibrated_brier),
                "raw_brier": float(raw_brier),
            }
    except Exception:
        LOG.debug("Ignored recoverable exception.", exc_info=True)
    return {"probabilities": raw, "calibrator": IdentityCalibrator(), "method": "identity", "brier": float(raw_brier), "raw_brier": float(raw_brier)}


def reliability_bins(probabilities: Sequence[float], labels: Sequence[int], *, bins: int = 10) -> list[dict[str, float | int]]:
    pairs = sorted(
        (
            (max(0.0, min(1.0, _safe_float(prob, 0.0))), 1 if int(label) else 0)
            for prob, label in zip(probabilities, labels)
        ),
        key=lambda item: item[0],
    )
    if not pairs:
        return []
    n = len(pairs)
    out: list[dict[str, float | int]] = []
    for idx in range(max(1, int(bins))):
        lo = int(idx * n / max(1, int(bins)))
        hi = int((idx + 1) * n / max(1, int(bins)))
        chunk = pairs[lo:hi]
        if not chunk:
            continue
        probs = [p for p, _y in chunk]
        ys = [y for _p, y in chunk]
        out.append(
            {
                "prob_mean": float(sum(probs) / len(probs)),
                "observed_rate": float(sum(ys) / len(ys)),
                "n": int(len(chunk)),
            }
        )
    return out


def _training_rows(con, *, model_family: str | None = None, limit: int = 20_000) -> list[dict[str, Any]]:
    params: list[Any] = []
    where = ""
    if model_family:
        where = "WHERE model_family=?"
        params.append(str(model_family))
    rows = con.execute(
        f"""
        SELECT ts_ms, vertical_ts_ms, symbol, model_family, label, feature_values_json
        FROM triple_barrier_labels
        {where}
        ORDER BY ts_ms ASC
        LIMIT ?
        """,
        (*params, int(limit)),
    ).fetchall()
    out = []
    for ts_ms, vertical_ts_ms, symbol, family, label, features_json in rows or []:
        features = _json_load_dict(features_json)
        out.append(
            {
                "ts_ms": int(ts_ms or 0),
                "vertical_ts_ms": int(vertical_ts_ms or ts_ms or 0),
                "symbol": str(symbol or ""),
                "model_family": str(family or ""),
                "label": int(label or 0),
                "features": {fid: _safe_float(features.get(fid), 0.0) for fid in META_LABEL_FEATURE_IDS},
            }
        )
    return out


def _new_classifier() -> tuple[Any, str]:
    try:
        import lightgbm as lgb

        return (
            lgb.LGBMClassifier(
                objective="binary",
                n_estimators=int(os.environ.get("META_LABEL_LGBM_ESTIMATORS", "80")),
                learning_rate=float(os.environ.get("META_LABEL_LGBM_LEARNING_RATE", "0.05")),
                num_leaves=int(os.environ.get("META_LABEL_LGBM_NUM_LEAVES", "15")),
                min_child_samples=max(2, int(os.environ.get("META_LABEL_LGBM_MIN_CHILD_SAMPLES", "5"))),
                random_state=42,
                n_jobs=1,
                verbosity=-1,
                deterministic=True,
                force_col_wise=True,
            ),
            "lightgbm",
        )
    except Exception:
        from sklearn.ensemble import GradientBoostingClassifier

        return GradientBoostingClassifier(random_state=42), "sklearn_gradient_boosting_fallback"


def _predict_proba_positive(model: Any, X: np.ndarray) -> np.ndarray:
    if hasattr(model, "predict_proba"):
        probs = np.asarray(model.predict_proba(X), dtype=np.float64)
        if probs.ndim == 2 and probs.shape[1] >= 2:
            return probs[:, 1]
        return probs.reshape(-1)
    raw = np.asarray(model.predict(X), dtype=np.float64).reshape(-1)
    return 1.0 / (1.0 + np.exp(-raw))


def _cpcv_calibration(
    X: np.ndarray,
    y: np.ndarray,
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    total = int(len(y))
    n_splits = max(2, min(int(os.environ.get("META_LABEL_CPCV_N_SPLITS", os.environ.get("CPCV_N_SPLITS", "6"))), max(2, total // 2)))
    n_test_splits = max(1, min(int(os.environ.get("META_LABEL_CPCV_N_TEST_SPLITS", os.environ.get("CPCV_N_TEST_SPLITS", "2"))), n_splits - 1))
    embargo_pct = float(os.environ.get("META_LABEL_CPCV_EMBARGO_PCT", os.environ.get("CPCV_EMBARGO_PCT", "0.01")))
    try:
        from engine.backtest.cpcv import CombinatorialPurgedKFold

        starts = np.asarray([int(row.get("ts_ms") or 0) for row in rows], dtype=np.float64)
        ends = np.asarray([int(row.get("vertical_ts_ms") or row.get("ts_ms") or 0) for row in rows], dtype=np.float64)
        splitter = CombinatorialPurgedKFold(
            n_splits=int(n_splits),
            n_test_splits=int(n_test_splits),
            embargo=float(max(0.0, embargo_pct)),
            label_start_times=starts,
            label_end_times=ends,
        )
        prob_sum = np.zeros(total, dtype=np.float64)
        prob_count = np.zeros(total, dtype=np.float64)
        fold_metrics: list[dict[str, Any]] = []
        for fold_idx, (train_idx, test_idx) in enumerate(splitter.split(X)):
            if train_idx.size <= 0 or test_idx.size <= 0:
                continue
            y_train = y[train_idx]
            if len(set(int(v) for v in y_train.tolist())) < 2:
                continue
            classifier, backend = _new_classifier()
            classifier.fit(X[train_idx], y_train)
            fold_raw = np.clip(_predict_proba_positive(classifier, X[test_idx]), 0.0, 1.0)
            prob_sum[test_idx] += fold_raw
            prob_count[test_idx] += 1.0
            fold_metrics.append(
                {
                    "fold": int(fold_idx),
                    "backend": str(backend),
                    "train_n": int(train_idx.size),
                    "validation_n": int(test_idx.size),
                    "raw_brier": float(brier_score(fold_raw, y[test_idx])),
                }
            )
        mask = prob_count > 0.0
        if not np.any(mask):
            raise RuntimeError("cpcv_no_valid_folds")
        raw_oof = np.clip(prob_sum[mask] / prob_count[mask], 0.0, 1.0)
        y_oof = y[mask]
        calibration = calibrate_probabilities(raw_oof, y_oof)
        calibrated = np.asarray(calibration["probabilities"], dtype=np.float64)
        return {
            "calibration": calibration,
            "raw_probabilities": raw_oof,
            "calibrated_probabilities": calibrated,
            "labels": y_oof,
            "method": "cpcv",
            "fold_count": int(len(fold_metrics)),
            "coverage": float(np.mean(mask)),
            "n_splits": int(n_splits),
            "n_test_splits": int(n_test_splits),
            "embargo_pct": float(max(0.0, embargo_pct)),
            "folds": fold_metrics,
        }
    except Exception:
        LOG.debug("Ignored recoverable exception.", exc_info=True)

    split = max(1, min(total - 1, int(math.floor(total * 0.8))))
    X_train, y_train = X[:split], y[:split]
    X_val, y_val = X[split:], y[split:]
    classifier, backend = _new_classifier()
    classifier.fit(X_train, y_train)
    raw_val = np.clip(_predict_proba_positive(classifier, X_val), 0.0, 1.0)
    calibration = calibrate_probabilities(raw_val, y_val)
    calibrated = np.asarray(calibration["probabilities"], dtype=np.float64)
    return {
        "calibration": calibration,
        "raw_probabilities": raw_val,
        "calibrated_probabilities": calibrated,
        "labels": y_val,
        "method": "temporal_holdout_fallback",
        "fold_count": 1,
        "coverage": float(len(y_val) / max(1, total)),
        "n_splits": 0,
        "n_test_splits": 0,
        "embargo_pct": 0.0,
        "folds": [{"fold": 0, "backend": str(backend), "train_n": int(len(y_train)), "validation_n": int(len(y_val)), "raw_brier": float(brier_score(raw_val, y_val))}],
    }


def train_meta_label_model(
    *,
    con=None,
    model_family: str | None = None,
    min_samples: int = DEFAULT_MIN_SAMPLES,
    limit: int = 20_000,
    now_ms: int | None = None,
) -> dict[str, Any]:
    own = con is None
    con = connect() if con is None else con
    try:
        ensure_schema(con)
        rows = _training_rows(con, model_family=model_family, limit=int(limit))
        if len(rows) < int(min_samples):
            return {"ok": False, "reason": "insufficient_samples", "n": int(len(rows))}
        y = np.asarray([int(row["label"]) for row in rows], dtype=np.int8)
        if len(set(int(v) for v in y.tolist())) < 2:
            return {"ok": False, "reason": "single_class", "n": int(len(rows))}
        X = np.asarray([[float(row["features"].get(fid, 0.0)) for fid in META_LABEL_FEATURE_IDS] for row in rows], dtype=np.float32)
        validation = _cpcv_calibration(X, y, rows)
        calibration = validation["calibration"]
        classifier, backend = _new_classifier()
        classifier.fit(X, y)
        validation_labels = np.asarray(validation["labels"], dtype=np.int8)
        calibrated_val = np.asarray(validation["calibrated_probabilities"], dtype=np.float64)
        metrics = {
            "backend": str(backend),
            "n_train": int(len(y)),
            "n_validation": int(len(validation_labels)),
            "raw_brier": float(calibration["raw_brier"]),
            "brier": float(calibration["brier"]),
            "calibration_method": str(calibration["method"]),
            "validation_method": str(validation["method"]),
            "cpcv": {
                "fold_count": int(validation["fold_count"]),
                "coverage": float(validation["coverage"]),
                "n_splits": int(validation["n_splits"]),
                "n_test_splits": int(validation["n_test_splits"]),
                "embargo_pct": float(validation["embargo_pct"]),
                "folds": list(validation["folds"]),
            },
            "reliability_bins": reliability_bins(calibrated_val, validation_labels, bins=10),
            "feature_schema": _feature_schema(),
        }
        version_ts = int(now_ms if now_ms is not None else _now_ms())
        family_key = str(model_family or "global").strip() or "global"
        model_name = f"{FAMILY}:{family_key}"
        version = f"v{version_ts}"
        alias = f"model:{FAMILY}:{family_key}:candidate"
        bundle = {
            "model": classifier,
            "calibrator": calibration["calibrator"],
            "feature_ids": list(META_LABEL_FEATURE_IDS),
            "feature_schema": _feature_schema(),
            "model_family": family_key,
            "backend": str(backend),
            "metrics": dict(metrics),
        }
        ref = LocalArtifactStore().put(
            dumps_pickle_artifact(bundle, prefer_joblib=True),
            content_type="application/vnd.joblib",
            kind="model",
            alias=alias,
            metadata={"model_name": model_name, "model_version": version, "family": FAMILY},
        )
        metrics["artifact_alias"] = alias
        metrics["artifact_sha256"] = ref.sha256
        register_model_version(
            model_name=model_name,
            model_version=version,
            model_kind=MODEL_KIND,
            stage="challenger",
            status="candidate",
            live_ready=False,
            training_job_name="train_meta_label_model",
            train_scope={"model_family": family_key, "feature_schema": _feature_schema()},
            meta=dict(metrics),
        )
        register_model(
            model_name=model_name,
            model_kind=MODEL_KIND,
            model_ts_ms=version_ts,
            stage="challenger",
            metrics=dict(metrics),
            note="meta-label classifier candidate",
            regime=family_key,
        )
        if own:
            con.commit()
        return {"ok": True, "model_name": model_name, "model_version": version, "metrics": metrics}
    finally:
        if own:
            try:
                con.close()
            except Exception:
                LOG.debug("Ignored recoverable exception.", exc_info=True)


def _load_champion_bundle(model_family: str) -> dict[str, Any] | None:
    family_key = str(model_family or "global").strip() or "global"
    for key in (family_key, "global"):
        model_name = f"{FAMILY}:{key}"
        try:
            row = get_stage_latest(model_name, "champion", regime=key)
        except Exception:
            row = None
        if not row:
            continue
        metrics = dict(row.get("metrics") or {})
        alias = str(metrics.get("artifact_alias") or "").strip()
        sha = str(metrics.get("artifact_sha256") or "").strip()
        try:
            store = LocalArtifactStore(ensure_schema=False)
            ref = store.resolve(alias) if alias else None
            if ref is None and sha:
                from datetime import datetime, timezone

                from engine.artifacts.refs import ArtifactRef

                ref = ArtifactRef(
                    sha256=sha,
                    size=0,
                    content_type="application/vnd.joblib",
                    kind="model",
                    created_ts=datetime.now(timezone.utc),
                    metadata={},
                )
            if ref is not None:
                return dict(loads_pickle_artifact(store.get_bytes(ref), prefer_joblib=True))
        except Exception:
            LOG.debug("Ignored recoverable exception.", exc_info=True)
    return None


def score_order_meta_label(
    con,
    order: Mapping[str, Any],
    *,
    regime_vec: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    order_dict = dict(order or {})
    explicit_prob = order_dict.get("meta_label_probability", order_dict.get("meta_probability"))
    model_family = infer_model_family(
        str(order_dict.get("model_name") or ""),
        str(order_dict.get("model_id") or ""),
        str(order_dict.get("model_family") or ""),
    )
    if explicit_prob is not None:
        prob = max(0.0, min(1.0, _safe_float(explicit_prob, 0.0)))
        return {
            "enabled": True,
            "applied": True,
            "source": "order",
            "model_family": str(model_family),
            "probability": float(prob),
            "multiplier": float(meta_label_multiplier(prob)),
            "feature_schema": _feature_schema(),
        }

    if str(os.environ.get("META_LABEL_GATE_ENABLED", "0")).strip().lower() in {"0", "false", "no", "off"}:
        return {"enabled": False, "applied": False, "probability": None, "multiplier": 1.0, "reason": "disabled"}

    bundle = _load_champion_bundle(str(model_family))
    if not bundle:
        return {
            "enabled": True,
            "applied": False,
            "source": "none",
            "model_family": str(model_family),
            "probability": None,
            "multiplier": 1.0,
            "reason": "no_champion_meta_model",
        }

    predicted_z = _safe_float(order_dict.get("expected_z", order_dict.get("zscore", order_dict.get("predicted_z"))), 0.0)
    side_sign = -1 if str(order_dict.get("side") or order_dict.get("to_side") or "").upper() in {"SELL", "SHORT"} or predicted_z < 0.0 else 1
    symbol = str(order_dict.get("symbol") or "").upper().strip()
    ts_ms = _safe_int(order_dict.get("signal_ts_ms", order_dict.get("ts_ms")), _now_ms())
    sigma_payload = resolve_barrier_sigma(con, symbol, ts_ms) if symbol else {"sigma": 0.0, "vol_ratio": 1.0}
    ood_payload = extract_ood_payload(order_dict)
    features = build_meta_label_features(
        predicted_z=predicted_z,
        confidence=_safe_float(order_dict.get("confidence"), 0.0),
        side_sign=side_sign,
        vol_level=_safe_float(sigma_payload.get("sigma"), _safe_float(order_dict.get("volatility"), 0.0)),
        vol_ratio=_safe_float(sigma_payload.get("vol_ratio"), 1.0),
        rolling_hit_rate=_rolling_hit_rate(con, model_family=model_family, symbol=symbol, ts_ms=ts_ms) if symbol else 0.5,
        regime_vec=regime_vec,
        ood_distance=_safe_float((ood_payload or {}).get("ood_score", (ood_payload or {}).get("ood_distance", order_dict.get("ood_distance"))), 0.0),
    )
    ids = list(bundle.get("feature_ids") or META_LABEL_FEATURE_IDS)
    X = feature_vector(features, ids)
    prob = float(_predict_proba_positive(bundle["model"], X)[0])
    calibrator = bundle.get("calibrator")
    if calibrator is not None and hasattr(calibrator, "predict"):
        prob = float(np.asarray(calibrator.predict([prob]), dtype=np.float64).reshape(-1)[0])
    prob = max(0.0, min(1.0, prob))
    return {
        "enabled": True,
        "applied": True,
        "source": "champion_model",
        "model_family": str(model_family),
        "probability": float(prob),
        "multiplier": float(meta_label_multiplier(prob)),
        "features": features,
        "feature_schema": dict(bundle.get("feature_schema") or _feature_schema()),
    }


def backtest_meta_label_gate(con, *, model_family: str | None = None, threshold: float = 0.45) -> dict[str, Any]:
    rows = _training_rows(con, model_family=model_family, limit=100_000)
    if not rows:
        return {"ok": False, "reason": "no_labels"}
    kept = [row for row in rows if float(row["features"].get("meta_label.primary_confidence", 0.0)) >= float(threshold)]
    ungated_precision = float(sum(int(row["label"]) for row in rows) / max(1, len(rows)))
    gated_precision = float(sum(int(row["label"]) for row in kept) / max(1, len(kept))) if kept else 0.0
    return {
        "ok": True,
        "n_ungated": int(len(rows)),
        "n_gated": int(len(kept)),
        "ungated_precision": float(ungated_precision),
        "gated_precision": float(gated_precision),
        "total_return_proxy_ungated": float(sum(1 if int(row["label"]) else -1 for row in rows)),
        "total_return_proxy_gated": float(sum(1 if int(row["label"]) else -1 for row in kept)),
    }


def run_label_job() -> dict[str, Any]:
    init_db()
    return generate_triple_barrier_labels()


def run_train_job() -> dict[str, Any]:
    init_db()
    con = connect()
    try:
        ensure_schema(con)
        families = [
            str(row[0] or "")
            for row in con.execute(
                "SELECT DISTINCT model_family FROM triple_barrier_labels WHERE model_family IS NOT NULL ORDER BY model_family"
            ).fetchall()
            if row and str(row[0] or "").strip()
        ]
        if not families:
            families = ["global"]
        results = [train_meta_label_model(con=con, model_family=family) for family in families]
        return {"ok": True, "results": results}
    finally:
        try:
            con.close()
        except Exception:
            LOG.debug("Ignored recoverable exception.", exc_info=True)
