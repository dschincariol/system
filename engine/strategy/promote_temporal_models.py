"""Guarded promotion job for temporal models.

Promotion only happens after shadow evaluation, safety metrics, replay context,
and feature-schema metadata indicate that a trained temporal version is fit to
become a deployable challenger or live candidate.
"""

import os
import json
import time
import socket
import logging

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import (
    connect,
    init_db,
    acquire_job_lock,
    release_job_lock,
)
from engine.model_registry import register_model
from engine.strategy.model_lifecycle import (
    mark_version_live,
    record_version_performance,
    register_model_version,
    retire_underperforming_versions,
    version_from_ts,
)
from engine.strategy.model_marketplace import upsert_marketplace_candidate
from engine.strategy.promotion_audit import audit
from engine.strategy.temporal_predictor import load_temporal_model_feature_schema


DRY_RUN = os.environ.get("DRY_RUN", "1") == "1"
ALLOW_PROMOTE = os.environ.get("PROMOTE_TEMPORAL", "0") == "1"

MIN_N = int(os.environ.get("TEMPORAL_PROMOTE_MIN_N", "200"))
MAX_MODEL_AGE_DAYS = int(os.environ.get("TEMPORAL_PROMOTE_MAX_AGE_DAYS", "30"))

MIN_SAFETY_MARGIN = float(os.environ.get("TEMPORAL_PROMOTE_MIN_SAFETY_MARGIN", "0.05"))
MAX_SAFETY_REGRESSION = float(os.environ.get("TEMPORAL_PROMOTE_MAX_SAFETY_REGRESSION", "0.10"))

JOB_NAME = "promote_temporal_models"

OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", socket.gethostname())),
)

PID = os.getpid()
LOG = logging.getLogger("promote_temporal_models")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(event: str, error: BaseException, **extra) -> None:
    log_failure(
        LOG,
        event=event,
        code=event,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.promote_temporal_models",
        extra=extra,
        persist=False,
    )


def _safe_json_load(x):
    try:
        if x is None:
            return {}
        if isinstance(x, (bytes, bytearray)):
            x = x.decode("utf-8", errors="replace")
        return json.loads(str(x))
    except Exception as e:
        _warn_nonfatal("PROMOTE_TEMPORAL_MODELS_JSON_LOAD_FAILED", e)
        return {}


def _latest_promotion_reason(con, promote_key: str):
    row = con.execute(
        """
        SELECT reason_json
        FROM model_promotion_audit
        WHERE model_name='temporal_predictor'
          AND regime=?
          AND action='promote_temporal'
        ORDER BY ts_ms DESC
        LIMIT 1
        """,
        (str(promote_key),),
    ).fetchone()

    return _safe_json_load(row[0]) if row and row[0] else {}


def main() -> int:

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID):
        print("[BLOCKED] another promotion job is running")
        return 2

    now_ms = _now_ms()
    max_age_ms = MAX_MODEL_AGE_DAYS * 86400 * 1000

    promoted = []
    skipped = []

    try:

        con = connect()

        try:

            rows = con.execute(
                """
                SELECT
                  e.symbol,
                  e.horizon_s,
                  e.rmse,
                  e.baseline_rmse,
                  e.directional_acc,
                  e.baseline_directional_acc,
                  e.n,
                  json_extract(e.detail_json, '$.latest_model_ts_ms') AS model_ts_ms,
                  json_extract(e.detail_json, '$.latest_model_kind') AS model_kind,
                  json_extract(e.detail_json, '$.capital_efficiency') AS capital_efficiency,
                  json_extract(e.detail_json, '$.drawdown_contribution') AS drawdown_contribution,
                  json_extract(e.detail_json, '$.avg_slippage_impact') AS avg_slippage_impact,
                  json_extract(e.detail_json, '$.signed_alpha') AS signed_alpha,
                  json_extract(e.detail_json, '$.safety_score') AS safety_score,
                  json_extract(e.detail_json, '$.rmse_improvement') AS rmse_improvement,
                  json_extract(e.detail_json, '$.diracc_delta') AS diracc_delta,
                  tm.artifact_sha256,
                  tm.artifact_alias
                FROM temporal_shadow_eval e
                LEFT JOIN temporal_models tm
                  ON tm.key_type='symbol'
                 AND tm.key=e.symbol
                 AND tm.horizon_s=e.horizon_s
                 AND tm.ts_ms=json_extract(e.detail_json, '$.latest_model_ts_ms')
                WHERE e.pass_all = 1
                  AND e.n >= ?
                """,
                (int(MIN_N),),
            ).fetchall()

            for (
                symbol,
                horizon_s,
                rmse,
                baseline_rmse,
                da,
                b_da,
                n,
                model_ts_ms,
                model_kind,
                capital_efficiency,
                drawdown_contribution,
                avg_slippage_impact,
                signed_alpha,
                safety_score,
                rmse_improvement,
                diracc_delta,
                artifact_sha256,
                artifact_alias,
            ) in rows or []:

                symbol_u = str(symbol or "").upper().strip()
                promote_key = f"symbol:{symbol_u}:{int(horizon_s)}"

                if not model_ts_ms:
                    skipped.append({"key": promote_key, "reason": "missing_model_ts"})
                    continue

                age_ms = now_ms - int(model_ts_ms)
                model_version = version_from_ts("temporal_predictor", int(model_ts_ms), prefix="temporal")

                if age_ms > max_age_ms:
                    skipped.append({"key": promote_key, "reason": "model_too_old"})
                    continue

                if capital_efficiency is None:
                    skipped.append({"key": promote_key, "reason": "missing_capital_efficiency"})
                    continue

                if safety_score is None:
                    skipped.append({"key": promote_key, "reason": "missing_safety_score"})
                    continue

                safety_score = float(safety_score or 0.0)

                if safety_score <= 0:
                    skipped.append({"key": promote_key, "reason": "negative_safety_score"})
                    continue

                # Promotion is relative, not absolute. The candidate must clear
                # current gates and also avoid regressing against the previous
                # accepted safety profile for the same symbol/horizon key.
                prev = _latest_promotion_reason(con, promote_key)

                prev_safety = float(prev.get("safety_score", 0.0) or 0.0)
                prev_rmse = prev.get("rmse")
                prev_dd = prev.get("drawdown_contribution")
                prev_slip = prev.get("avg_slippage_impact")

                allow = False
                allow_reason = {}

                if not prev:

                    allow = True
                    allow_reason = {"rule": "first_promotion_for_key"}

                else:

                    safety_improved = safety_score >= (prev_safety + float(MIN_SAFETY_MARGIN))

                    dd_ok = True
                    if prev_dd is not None:
                        dd_ok = float(drawdown_contribution or 0.0) <= float(prev_dd) * (1.0 + float(MAX_SAFETY_REGRESSION))

                    slip_ok = True
                    if prev_slip is not None:
                        slip_ok = float(avg_slippage_impact or 0.0) <= float(prev_slip) * (1.0 + float(MAX_SAFETY_REGRESSION))

                    rmse_ok = True
                    if prev_rmse is not None:
                        rmse_ok = float(rmse or 0.0) <= float(prev_rmse) * (1.0 + float(MAX_SAFETY_REGRESSION))

                    allow = bool(
                        safety_improved
                        or (rmse_ok and dd_ok and slip_ok and safety_score >= prev_safety)
                    )

                    allow_reason = {
                        "rule": "safety_first_compare",
                        "prev_safety_score": prev_safety,
                        "safety_improved": bool(safety_improved),
                        "rmse_ok": bool(rmse_ok),
                        "dd_ok": bool(dd_ok),
                        "slip_ok": bool(slip_ok),
                    }

                if not allow:

                    skipped.append({"key": promote_key, "reason": "champion_not_beaten"})

                    audit(
                        actor="auto",
                        action="block_temporal",
                        model_name="temporal_predictor",
                        regime=str(promote_key),
                        reason={
                            "rmse": rmse,
                            "baseline_rmse": baseline_rmse,
                            "directional_acc": da,
                            "baseline_directional_acc": b_da,
                            "n": n,
                            "model_ts_ms": int(model_ts_ms),
                            "capital_efficiency": capital_efficiency,
                            "drawdown_contribution": drawdown_contribution,
                            "avg_slippage_impact": avg_slippage_impact,
                            "safety_score": safety_score,
                            "rmse_improvement": rmse_improvement,
                            "diracc_delta": diracc_delta,
                            "compare": allow_reason,
                        },
                    )

                    continue

                schema = {}
                try:
                    schema = load_temporal_model_feature_schema(int(model_ts_ms)) or {}
                except Exception:
                    schema = {}

                register_model_version(
                    model_name="temporal_predictor",
                    model_version=str(model_version),
                    model_kind=str(model_kind or "temporal_mlp"),
                    mutation_kind="validated_shadow",
                    stage="challenger",
                    status="validated",
                    live_ready=False,
                    training_job_name="promote_temporal_models",
                    train_scope={
                        "symbol": str(symbol_u),
                        "horizon_s": int(horizon_s),
                        "promote_key": str(promote_key),
                    },
                    meta={"feature_schema": schema},
                )
                record_version_performance(
                    model_name="temporal_predictor",
                    model_version=str(model_version),
                    metric_scope="shadow_validation",
                    metrics={
                        "rmse": float(rmse or 0.0),
                        "baseline_rmse": float(baseline_rmse or 0.0),
                        "directional_acc": float(da or 0.0),
                        "baseline_directional_acc": float(b_da or 0.0),
                        "capital_efficiency": float(capital_efficiency or 0.0),
                        "drawdown_contribution": float(drawdown_contribution or 0.0),
                        "avg_slippage_impact": float(avg_slippage_impact or 0.0),
                        "signed_alpha": float(signed_alpha or 0.0),
                        "safety_score": float(safety_score or 0.0),
                        "rmse_improvement": float(rmse_improvement or 0.0),
                        "diracc_delta": float(diracc_delta or 0.0),
                        "quality_score": float(max(0.0, min(1.0, float(safety_score or 0.0)))),
                    },
                    sample_n=int(n or 0),
                    meta={"promote_key": str(promote_key)},
                )

                promoted.append(promote_key)

                if DRY_RUN:
                    print("[DRY-RUN] would promote", promote_key)
                    continue

                if not ALLOW_PROMOTE:
                    continue

                register_model(
                    model_name="temporal_predictor",
                    model_kind=str(model_kind or "temporal_mlp"),
                    model_ts_ms=int(model_ts_ms),
                    stage="challenger",
                    metrics={
                        "symbol": symbol_u,
                        "horizon_s": int(horizon_s),
                        "rmse": float(rmse or 0.0),
                        "baseline_rmse": float(baseline_rmse or 0.0),
                        "directional_acc": float(da or 0.0),
                        "baseline_directional_acc": float(b_da or 0.0),
                        "n": int(n or 0),
                        "capital_efficiency": float(capital_efficiency or 0.0),
                        "drawdown_contribution": float(drawdown_contribution or 0.0),
                        "avg_slippage_impact": float(avg_slippage_impact or 0.0),
                        "signed_alpha": float(signed_alpha or 0.0),
                        "safety_score": float(safety_score or 0.0),
                        "rmse_improvement": float(rmse_improvement or 0.0),
                        "diracc_delta": float(diracc_delta or 0.0),
                        "model_version": str(model_version),
                        "artifact_sha256": str(artifact_sha256 or ""),
                        "artifact_alias": str(artifact_alias or ""),
                        "feature_ids": list(schema.get("feature_ids") or []),
                        "feature_set_tag": str(schema.get("feature_set_tag") or ""),
                        "feature_schema": {
                            "feature_ids": list(schema.get("feature_ids") or []),
                            "feature_set_tag": str(schema.get("feature_set_tag") or ""),
                            "sequence_schema": dict(schema.get("sequence_schema") or {}),
                            "ts_ms": int(model_ts_ms),
                        },
                    },
                    note="promote_temporal_models",
                    regime="global",
                )

                upsert_marketplace_candidate(
                    model_name="temporal_predictor",
                    symbol=symbol_u,
                    horizon_s=int(horizon_s),
                    regime="global",
                    stage="challenger",
                    score=float(safety_score or 0.0),
                    trades=int(n or 0),
                    wins=int(round(float(da or 0.0) * int(n or 0))),
                    losses=max(0, int(n or 0) - int(round(float(da or 0.0) * int(n or 0)))),
                    gross_pnl=float(signed_alpha or 0.0),
                    net_pnl=float(signed_alpha or 0.0),
                    avg_confidence=float(da or 0.0),
                    last_signal_ts_ms=int(now_ms),
                    meta={
                        "model_kind": str(model_kind or "temporal_mlp"),
                        "model_ts_ms": int(model_ts_ms),
                        "score_source": "temporal_shadow_eval",
                        "capital_efficiency": float(capital_efficiency or 0.0),
                        "drawdown_contribution": float(drawdown_contribution or 0.0),
                        "avg_slippage_impact": float(avg_slippage_impact or 0.0),
                        "safety_score": float(safety_score or 0.0),
                        "rmse": float(rmse or 0.0),
                        "baseline_rmse": float(baseline_rmse or 0.0),
                        "directional_acc": float(da or 0.0),
                        "baseline_directional_acc": float(b_da or 0.0),
                        "rmse_improvement": float(rmse_improvement or 0.0),
                        "diracc_delta": float(diracc_delta or 0.0),
                        "signed_alpha": float(signed_alpha or 0.0),
                    },
                )
                mark_version_live(
                    "temporal_predictor",
                    str(model_version),
                    stage="champion",
                    meta_patch={"promote_key": str(promote_key), "promoted_ts_ms": int(now_ms)},
                )
                retire_underperforming_versions("temporal_predictor", protect_versions=[str(model_version)])

                audit(
                    actor="auto",
                    action="promote_temporal",
                    model_name="temporal_predictor",
                    regime=str(promote_key),
                    reason={
                        "rmse": rmse,
                        "baseline_rmse": baseline_rmse,
                        "directional_acc": da,
                        "baseline_directional_acc": b_da,
                        "n": n,
                        "model_ts_ms": int(model_ts_ms),
                        "capital_efficiency": capital_efficiency,
                        "drawdown_contribution": drawdown_contribution,
                        "avg_slippage_impact": avg_slippage_impact,
                        "safety_score": safety_score,
                        "rmse_improvement": rmse_improvement,
                        "diracc_delta": diracc_delta,
                        "compare": allow_reason,
                    },
                    to_artifact_sha256=(str(artifact_sha256) if artifact_sha256 else None),
                )

            if not DRY_RUN and ALLOW_PROMOTE:
                con.commit()

        finally:
            con.close()

        print(
            json.dumps(
                {
                    "ok": True,
                    "dry_run": DRY_RUN,
                    "allow_promote": ALLOW_PROMOTE,
                    "promoted": promoted,
                    "skipped": skipped,
                },
                indent=2,
            )
        )

        return 0

    finally:

        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("promote_temporal_models_release_lock_failed", e)


if __name__ == "__main__":
    raise SystemExit(main())
