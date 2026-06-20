"""
FILE: pipeline_train_and_eval.py

Runs the end-to-end strategy training and evaluation pipeline.

This job coordinates retraining, challenger evaluation, and optional promotion
gates. It is effectively the orchestration layer that ties the strategy
subsystem's offline components together.
"""

import os
import time
import math
import subprocess
import sys
import json
import logging
import random
from typing import Dict, Any, List, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    connect,
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
)
# from dashboard_server import get_health_snapshot  # unused

from engine.model_registry import register_model, get_stage_latest
from engine.strategy.model_lifecycle import (
    mark_version_live,
    record_version_performance,
    register_model_version,
    retire_underperforming_versions,
    update_model_version_status,
    version_from_ts,
)
from engine.strategy.embed_regressor import load_feature_schema
from engine.strategy.model_config import (
    DEFAULT_FAMILY,
    MODEL_INSTANCE_CONFIG_JSON_ENV,
    build_model_registration_metadata,
    load_model_configs,
)
from engine.strategy.promotion_hardening import promote_with_snapshot_and_db_watch
from engine.strategy.promotion_guard import (
    metrics_have_net_cost_evidence,
    promotion_allowed,
    run_position_reconcile_before_promotion,
)
from engine.strategy.promotion_audit import audit
from engine.strategy.net_after_cost_labels import net_cost_evidence_summary
from engine.runtime.workload_profiles import assert_offline_work_allowed
from engine.training_guard import training_allowed


# ------            -- ------------------------------------------------------
# Constants
# ------            -- ------------------------------------------------------
MODEL_NAME = os.environ.get("MODEL_NAME", "embed_regressor")
ACTIVE_REGIMES = ["global", "low_vol", "high_vol", "trend", "shock"]

JOB_NAME = "pipeline_train_and_eval"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "20.0"))

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [pipeline_train_and_eval] %(message)s",
)
LOG = get_logger("engine.strategy.pipeline_train_and_eval")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="pipeline_train_and_eval_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.pipeline_train_and_eval",
        extra=extra or None,
        persist=False,
    )

# Promotion thresholds
PROMOTE_MIN_IMPROVEMENT = float(os.environ.get("PROMOTE_MIN_IMPROVEMENT", "0.01"))
PROMOTE_DIRACC_TOL = float(os.environ.get("PROMOTE_DIRACC_TOL", "0.00"))
EVAL_LIMIT_ROWS = int(os.environ.get("CHALLENGER_EVAL_LIMIT_ROWS", "5000"))


# ------            -- ------------------------------------------------------
# Small helpers
# ------            -- ------------------------------------------------------
def _sleep_with_jitter(seconds: float) -> None:
    if seconds <= 0:
        return
    j = seconds * 0.2
    time.sleep(max(0.05, seconds + random.uniform(-j, j)))


def _latest_age_s(con, table: str) -> Optional[float]:
    try:
        row = con.execute(f"SELECT MAX(ts_ms) FROM {table}").fetchone()
    except Exception as e:
        _warn_nonfatal(
            "PIPELINE_TRAIN_AND_EVAL_LATEST_AGE_FAILED",
            e,
            table=str(table),
        )
        return None
    if not row or not row[0]:
        return None
    return (int(time.time() * 1000) - int(row[0])) / 1000.0


def _count(con, table: str) -> int:
    try:
        row = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return int(row[0] or 0)
    except Exception as e:
        _warn_nonfatal(
            "PIPELINE_TRAIN_AND_EVAL_COUNT_FAILED",
            e,
            table=str(table),
        )
        return 0


def _run_python(script: str, *, env_overrides: Optional[Dict[str, str]] = None) -> int:
    timeout_s = int(os.environ.get("PIPELINE_CHILD_TIMEOUT_S", "1800"))
    try:
        env = dict(os.environ)
        env.update({str(k): str(v) for k, v in dict(env_overrides or {}).items()})
        p = subprocess.run(
            [sys.executable, script],
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
    except subprocess.TimeoutExpired as e:
        logging.error("child script timeout script=%s timeout_s=%s", script, timeout_s)
        _warn_nonfatal(
            "PIPELINE_TRAIN_AND_EVAL_CHILD_TIMEOUT",
            e,
            script=str(script),
            timeout_s=int(timeout_s),
        )
        if e.stdout:
            print(str(e.stdout).strip())
        if e.stderr:
            print(str(e.stderr).strip())
        return 124
    if p.stdout:
        print(p.stdout.strip())
    if p.returncode != 0 and p.stderr:
        print(p.stderr.strip())
    return int(p.returncode)


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception as e:
        _warn_nonfatal(
            "PIPELINE_TRAIN_AND_EVAL_AS_FLOAT_FAILED",
            e,
            value_type=type(value).__name__,
        )
        return None


# ------            -- ------------------------------------------------------
# Data-quality gates (fail-closed)
# ------            -- ------------------------------------------------------
def _data_gates_or_exit() -> None:
    # Training without fresh predictions and labels produces misleading metrics,
    # so the pipeline exits early rather than trying to recover implicitly.
    con = connect()
    try:
        pred_age_s = _latest_age_s(con, "predictions")
        lbl_age_s = _latest_age_s(con, "labels")
        lbl_n = _count(con, "labels")
    finally:
        con.close()

    MAX_PRED_AGE_S = float(os.environ.get("TRAIN_MAX_PREDICTIONS_AGE_S", "900"))
    MAX_LBL_AGE_S = float(os.environ.get("TRAIN_MAX_LABELS_AGE_S", "900"))
    MIN_LABELS = int(os.environ.get("TRAIN_MIN_LABELS", "50"))

    if pred_age_s is None or pred_age_s > MAX_PRED_AGE_S:
        logging.error("ABORT: predictions stale or missing age_s=%s limit=%s", pred_age_s, MAX_PRED_AGE_S)
        raise SystemExit(3)

    if lbl_age_s is None or lbl_age_s > MAX_LBL_AGE_S:
        logging.error("ABORT: labels stale or missing age_s=%s limit=%s", lbl_age_s, MAX_LBL_AGE_S)
        raise SystemExit(3)

    if lbl_n < MIN_LABELS:
        logging.error("ABORT: insufficient labels count=%s min=%s", lbl_n, MIN_LABELS)
        raise SystemExit(3)

    logging.info(
        "data gates OK predictions_age_s=%.1f labels_age_s=%.1f labels_n=%s",
        float(pred_age_s),
        float(lbl_age_s),
        int(lbl_n),
    )


# ------            -- ------------------------------------------------------
# Net-of-cost evaluation (execution-aware)
# ------            -- ------------------------------------------------------
def _net_eval_metrics(con, lookback_days: int = 90) -> Optional[Dict[str, Any]]:
    DAY_MS = 86400 * 1000
    now = int(time.time() * 1000)
    min_ts = now - int(lookback_days) * DAY_MS

    rows = con.execute(
        """
        SELECT p.predicted_z, le.net_z, le.gross_ret, le.net_ret, le.total_cost_bps, le.realized
        FROM predictions p
        JOIN labels_exec le
          ON le.event_id=p.event_id
         AND le.symbol=p.symbol
         AND le.horizon_s=p.horizon_s
        WHERE p.ts_ms >= ?
          AND le.net_z IS NOT NULL
        LIMIT 50000
        """,
        (int(min_ts),),
    ).fetchall()

    # Require a minimum sample size so promotion logic does not overreact to
    # thin or noisy evaluation windows.
    if not rows or len(rows) < 200:
        return None

    n = 0
    realized_n = 0
    se = 0.0
    hit = 0
    gross_sum = 0.0
    net_sum = 0.0
    cost_sum = 0.0
    cost_bps_sum = 0.0

    for pred, netz, gross_ret, net_ret, total_cost_bps, realized in rows:
        try:
            pr = float(pred)
            nz = float(netz)
        except Exception as e:
            _warn_nonfatal(
                "PIPELINE_TRAIN_AND_EVAL_NET_EVAL_PARSE_FAILED",
                e,
                pred=repr(pred),
                netz=repr(netz),
            )
            continue
        n += 1
        realized_n += 1 if int(realized or 0) == 1 else 0
        se += (pr - nz) ** 2
        if (pr >= 0 and nz >= 0) or (pr < 0 and nz < 0):
            hit += 1
        g = _as_float(gross_ret)
        nr = _as_float(net_ret)
        cbps = _as_float(total_cost_bps)
        if g is not None:
            gross_sum += float(g)
        if nr is not None:
            net_sum += float(nr)
        if g is not None and nr is not None:
            cost_sum += max(0.0, float(g) - float(nr))
        if cbps is not None:
            cost_bps_sum += max(0.0, float(cbps))

    if n <= 0:
        return None

    rmse = math.sqrt(se / n)
    diracc = float(hit) / float(n)

    artifact_evidence = net_cost_evidence_summary(con, lookback_days=int(lookback_days))
    return {
        "n_eval_net": n,
        "n_eval_net_realized": int(realized_n),
        "rmse_net": rmse,
        "directional_acc_net": diracc,
        "gross_edge": float(gross_sum / float(n)),
        "net_edge": float(net_sum / float(n)),
        "cost_drag": float(cost_sum / float(n)),
        "avg_total_cost_bps": float(cost_bps_sum / float(n)),
        "net_cost_label_count": int(artifact_evidence.get("n") or 0),
        "net_cost_evidence_available": bool(artifact_evidence.get("available")),
        "net_cost_evidence": dict(artifact_evidence),
    }


# ------            -- ------------------------------------------------------
# Embed model eval aggregation
# ------            -- ------------------------------------------------------
def _latest_embed_eval_snapshot(con) -> int:
    r = con.execute("SELECT MAX(ts_ms) FROM embed_model_eval").fetchone()
    return int(r[0] or 0)


def _load_embed_eval_rows(con, ts_ms: int):
    return con.execute(
        """
        SELECT model_kind, n_eval, rmse, directional_acc
        FROM embed_model_eval
        WHERE ts_ms=?
        LIMIT ?
        """,
        (int(ts_ms), int(EVAL_LIMIT_ROWS)),
    ).fetchall()


def _aggregate(rows) -> Tuple[str, Dict[str, Any]]:
    if not rows:
        return ("unknown", {"n_eval": 0, "rmse": None, "directional_acc": None})

    kinds = {}
    sum_w = 0.0
    sum_mse = 0.0
    sum_dir = 0.0

    for mk, n_eval, rmse, diracc in rows:
        w = float(n_eval or 0)
        if w <= 0:
            continue
        kinds[mk] = kinds.get(mk, 0) + w
        try:
            r = float(rmse)
            d = float(diracc)
        except Exception as e:
            _warn_nonfatal(
                "PIPELINE_TRAIN_AND_EVAL_EMBED_AGGREGATE_PARSE_FAILED",
                e,
                model_kind=str(mk),
                rmse=repr(rmse),
                directional_acc=repr(diracc),
            )
            continue
        sum_w += w
        sum_mse += w * (r * r)
        sum_dir += w * d

    kind = max(kinds.items(), key=lambda x: x[1])[0] if kinds else "unknown"
    if sum_w <= 0:
        return (kind, {"n_eval": 0, "rmse": None, "directional_acc": None})

    return (
        kind,
        {
            "n_eval": int(sum_w),
            "rmse": math.sqrt(sum_mse / sum_w),
            "directional_acc": sum_dir / sum_w,
        },
    )


# ------            -- ------------------------------------------------------
# Challenger vs champion comparison
# ------            -- ------------------------------------------------------
def _beats_champion(candidate: Dict[str, Any], champion: Optional[Dict[str, Any]]):
    if not metrics_have_net_cost_evidence(candidate):
        return False, {
            "missing_net_cost_evidence": True,
            "net_cost_label_count": int(candidate.get("net_cost_label_count") or 0),
        }
    if champion is None:
        return True, {"no_champion": True}

    c_rmse = _as_float(candidate.get("rmse_net", candidate.get("rmse")))
    c_dir = _as_float(candidate.get("directional_acc_net", candidate.get("directional_acc")))
    c_cap = _as_float(candidate.get("capital_efficiency"))
    c_dd = _as_float(candidate.get("drawdown_contribution"))
    c_slip = _as_float(candidate.get("avg_slippage_impact"))
    c_safe = _as_float(candidate.get("safety_score"))

    chm = champion.get("metrics") or {}
    ch_rmse = _as_float(chm.get("rmse_net", chm.get("rmse")))
    ch_dir = _as_float(chm.get("directional_acc_net", chm.get("directional_acc")))
    ch_cap = _as_float(chm.get("capital_efficiency"))
    ch_dd = _as_float(chm.get("drawdown_contribution"))
    ch_slip = _as_float(chm.get("avg_slippage_impact"))
    ch_safe = _as_float(chm.get("safety_score"))

    if c_rmse is None or c_dir is None or ch_rmse is None or ch_dir is None:
        return False, {"missing_metrics": True}

    c_rmse_f = float(c_rmse)
    c_dir_f = float(c_dir)
    ch_rmse_f = float(ch_rmse)
    ch_dir_f = float(ch_dir)

    rel_ok = c_rmse_f < ch_rmse_f * (1.0 - PROMOTE_MIN_IMPROVEMENT)
    dir_ok = c_dir_f >= ch_dir_f - PROMOTE_DIRACC_TOL

    safety_available = not any(
        metric is None for metric in (c_cap, c_dd, c_slip, c_safe, ch_cap, ch_dd, ch_slip, ch_safe)
    )
    safety_ok = True
    safer_override = False

    if safety_available:
        c_dd_f = float(c_dd) if c_dd is not None else 0.0
        ch_dd_f = float(ch_dd) if ch_dd is not None else 0.0
        c_slip_f = float(c_slip) if c_slip is not None else 0.0
        ch_slip_f = float(ch_slip) if ch_slip is not None else 0.0
        c_safe_f = float(c_safe) if c_safe is not None else 0.0
        ch_safe_f = float(ch_safe) if ch_safe is not None else 0.0
        safety_ok = (
            c_dd_f <= ch_dd_f * 1.10
            and c_slip_f <= ch_slip_f * 1.10
            and c_safe_f >= ch_safe_f - 0.05
        )
        safer_override = c_safe_f >= ch_safe_f + 0.10

    decision = bool((rel_ok and dir_ok and safety_ok) or safer_override)

    return decision, {
        "candidate_rmse": c_rmse_f,
        "champion_rmse": ch_rmse_f,
        "candidate_dir": c_dir_f,
        "champion_dir": ch_dir_f,
        "candidate_capital_efficiency": c_cap,
        "champion_capital_efficiency": ch_cap,
        "candidate_drawdown_contribution": c_dd,
        "champion_drawdown_contribution": ch_dd,
        "candidate_avg_slippage_impact": c_slip,
        "champion_avg_slippage_impact": ch_slip,
        "candidate_safety_score": c_safe,
        "champion_safety_score": ch_safe,
        "rel_ok": rel_ok,
        "dir_ok": dir_ok,
        "safety_ok": safety_ok,
        "safer_override": safer_override,
        "safety_available": safety_available,
    }


def _registered_variant_metrics(
    *,
    snap: int,
    started_ms: int,
    config: Dict[str, Any],
    feature_schema: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {
        "eval_ts_ms": int(snap),
        "pipeline_started_ms": int(started_ms),
        "pipeline_finished_ms": int(time.time() * 1000),
    }
    metrics.update(build_model_registration_metadata(config))
    metrics["model_version"] = str(version_from_ts(str(config.get("model_name") or MODEL_NAME), int(snap), prefix="embed"))
    if isinstance(feature_schema, dict) and feature_schema.get("feature_ids"):
        metrics["feature_ids"] = list(feature_schema.get("feature_ids") or [])
        metrics["feature_set_tag"] = str(feature_schema.get("feature_set_tag") or metrics.get("feature_set_tag") or "")
        metrics["feature_schema"] = {
            "feature_ids": list(feature_schema.get("feature_ids") or []),
            "feature_set_tag": str(feature_schema.get("feature_set_tag") or metrics.get("feature_set_tag") or ""),
            "ts_ms": int(feature_schema.get("ts_ms") or int(snap)),
        }
    return metrics


def _variant_requires_shadow_only(config: Dict[str, Any]) -> bool:
    candidate = dict(config.get("symbolic_candidate") or {})
    if not candidate:
        return bool(config.get("shadow_only"))
    return bool(candidate.get("shadow_only", config.get("shadow_only", True)))


def _variant_mutation_kind(config: Dict[str, Any]) -> str:
    if dict(config.get("symbolic_candidate") or {}):
        return "symbolic_alpha_discovery"
    return "baseline_retrain"


def _symbolic_pipeline_compat_enabled() -> bool:
    return str(os.environ.get("SYMBOLIC_ALPHA_PIPELINE_COMPAT_ENABLED", "0")).strip().lower() in {"1", "true", "yes", "on"}


def _extend_with_symbolic_model_configs(model_configs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not _symbolic_pipeline_compat_enabled():
        return list(model_configs or [])

    try:
        from engine.research.symbolic_alpha_generator import (
            build_symbolic_candidate_model_configs,
            symbolic_alpha_enabled,
        )
    except Exception as e:
        _warn_nonfatal("PIPELINE_TRAIN_AND_EVAL_SYMBOLIC_IMPORT_FAILED", e)
        return list(model_configs or [])

    if not symbolic_alpha_enabled():
        return list(model_configs or [])

    try:
        symbolic_configs = build_symbolic_candidate_model_configs(list(model_configs or []))
    except Exception as e:
        _warn_nonfatal("PIPELINE_TRAIN_AND_EVAL_SYMBOLIC_DISCOVERY_FAILED", e)
        symbolic_configs = []

    out = list(model_configs or [])
    out.extend(list(symbolic_configs or []))
    return out


# ------            -- ------------------------------------------------------
# Main pipeline
# ------            -- ------------------------------------------------------
def main() -> int:
    if not training_allowed():
        print("[training_guard] training disabled")
        raise SystemExit(0)
    try:
        assert_offline_work_allowed(job_name=JOB_NAME)
    except RuntimeError as exc:
        print(f"[workload_profile] {exc}")
        return 3

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        logging.error("another instance is holding the job lock; exiting")
        return 2

    started_ms = int(time.time() * 1000)
    last_hb_s = 0.0

    try:
        # 0) Data quality gates
        _data_gates_or_exit()

        # heartbeat
        now_s = time.time()
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps({"phase": "start"}))
        last_hb_s = now_s

        model_configs = load_model_configs(family=DEFAULT_FAMILY)
        if not model_configs:
            model_configs = [{"model_name": str(MODEL_NAME), "family": DEFAULT_FAMILY}]
        model_configs = _extend_with_symbolic_model_configs(list(model_configs or []))

        # 1) Train configured embed-model variants one at a time.
        trained_variants = 0
        for model_cfg in model_configs:
            variant_name = str(model_cfg.get("model_name") or MODEL_NAME).strip() or MODEL_NAME
            variant_stage = "shadow" if _variant_requires_shadow_only(model_cfg) else "challenger"
            registry_stage = "shadow" if str(variant_stage) == "shadow" else "challenger"
            mutation_kind = _variant_mutation_kind(model_cfg)
            env_overrides = {
                MODEL_INSTANCE_CONFIG_JSON_ENV: json.dumps(model_cfg, separators=(",", ":"), sort_keys=True),
            }

            con = connect()
            try:
                snap_before = _latest_embed_eval_snapshot(con)
            finally:
                con.close()

            rc = _run_python("train_embed_models.py", env_overrides=env_overrides)
            if rc != 0:
                audit(
                    actor="system",
                    action="block",
                    model_name=variant_name,
                    reason={"error": "train_embed_models_failed", "returncode": int(rc), "config": dict(model_cfg)},
                )
                continue

            con = connect()
            try:
                snap = _latest_embed_eval_snapshot(con)
                if int(snap or 0) <= int(snap_before or 0):
                    logging.info("variant skipped or no fresh eval snapshot model=%s snap_before=%s snap_after=%s", variant_name, snap_before, snap)
                    trained_variants += 1
                    continue
                rows = _load_embed_eval_rows(con, snap)
            finally:
                con.close()

            kind, metrics = _aggregate(rows)
            try:
                feature_schema = load_feature_schema(ts_ms=int(snap)) or {}
            except Exception:
                feature_schema = {}

            metrics.update(
                _registered_variant_metrics(
                    snap=int(snap),
                    started_ms=int(started_ms),
                    config=dict(model_cfg),
                    feature_schema=dict(feature_schema or {}),
                )
            )

            con = connect()
            try:
                nem = _net_eval_metrics(con, lookback_days=90)
            finally:
                con.close()
            if nem:
                metrics.update(nem)

            model_version = str(metrics.get("model_version") or version_from_ts(variant_name, int(snap), prefix="embed"))
            register_model_version(
                model_name=variant_name,
                model_version=str(model_version),
                model_kind=str(kind),
                mutation_kind=str(mutation_kind),
                stage=str(registry_stage),
                status="validated",
                live_ready=False,
                training_job_name=JOB_NAME,
                train_scope={
                    "eval_ts_ms": int(snap),
                    "active_regimes": list(ACTIVE_REGIMES),
                    "symbol_universe": list(metrics.get("symbol_universe") or []),
                    "horizons_s": list(metrics.get("horizons_s") or []),
                    "risk_profile": str(metrics.get("risk_profile") or ""),
                    "training_window_days": int(metrics.get("training_window_days") or 0),
                },
                meta={
                    "source": "pipeline_train_and_eval",
                    "model_id": str(metrics.get("model_id") or variant_name),
                    "symbolic_candidate": dict(model_cfg.get("symbolic_candidate") or {}),
                },
            )
            perf_metrics = {
                key: value
                for key, value in metrics.items()
                if isinstance(value, (int, float)) and not isinstance(value, bool)
            }
            record_version_performance(
                model_name=variant_name,
                model_version=str(model_version),
                metric_scope="evaluation",
                metrics=perf_metrics,
                sample_n=int(metrics.get("n_eval_net", metrics.get("n_eval", 0)) or 0),
                meta={"source": "pipeline_train_and_eval"},
            )

            register_model(
                model_name=variant_name,
                model_kind=kind,
                model_ts_ms=int(snap),
                stage=str(registry_stage),
                metrics=metrics,
                note="pipeline_train_and_eval:auto_config",
            )

            if variant_stage == "shadow":
                audit(
                    actor="auto",
                    action="shadow_only",
                    model_name=variant_name,
                    to_kind=kind,
                    to_ts_ms=int(snap),
                    reason={
                        "mutation_kind": str(mutation_kind),
                        "symbolic_candidate": dict(model_cfg.get("symbolic_candidate") or {}),
                        "metrics": metrics,
                    },
                )
                print(f"[SHADOW] model={variant_name} retained as shadow-only candidate")
                trained_variants += 1
                continue

            position_reconcile_gate = run_position_reconcile_before_promotion()
            if bool(position_reconcile_gate.get("required")) and not bool(position_reconcile_gate.get("ok")):
                audit(
                    actor="auto",
                    action="block",
                    model_name=variant_name,
                    to_kind=kind,
                    to_ts_ms=int(snap),
                    reason={"position_reconcile": position_reconcile_gate, "metrics": metrics},
                )
                print(
                    f"[BLOCKED] model={variant_name} position reconcile gate: "
                    f"{position_reconcile_gate.get('reason')}"
                )
                trained_variants += 1
                continue

            allowed, guard_reason = promotion_allowed()
            if not allowed:
                audit(
                    actor="auto",
                    action="block",
                    model_name=variant_name,
                    to_kind=kind,
                    to_ts_ms=int(snap),
                    reason={"guard": guard_reason, "metrics": metrics},
                )
                print(f"[BLOCKED] model={variant_name} promotion guards: {guard_reason}")
                trained_variants += 1
                continue

            champion = get_stage_latest(variant_name, "champion")
            ok, cmp_reason = _beats_champion(metrics, champion)
            if not ok:
                audit(
                    actor="auto",
                    action="reject",
                    model_name=variant_name,
                    from_kind=(champion.get("model_kind") if champion else None),
                    from_ts_ms=(champion.get("model_ts_ms") if champion else None),
                    to_kind=kind,
                    to_ts_ms=int(snap),
                    reason={"compare": cmp_reason, "guard": guard_reason, "metrics": metrics},
                )
                print(f"[REJECT] model={variant_name} challenger did not beat champion: {cmp_reason}")
                trained_variants += 1
                continue

            prev_kind = champion.get("model_kind") if champion else None
            prev_ts = champion.get("model_ts_ms") if champion else None
            promoted_regimes: List[str] = []

            for regime in ACTIVE_REGIMES:
                now_s = time.time()
                if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
                    touch_job_lock(JOB_NAME, OWNER, PID)

                put_job_heartbeat(
                    JOB_NAME,
                    OWNER,
                    PID,
                    extra_json=json.dumps({"phase": "promote", "regime": regime, "model_name": variant_name}),
                )
                last_hb_s = now_s

                try:
                    con = connect()
                    try:
                        row = con.execute(
                            """
                            SELECT COUNT(*)
                            FROM labels
                            WHERE impact_z IS NOT NULL
                            AND regime = ?
                            """,
                            (str(regime),),
                        ).fetchone()
                        n_reg = int(row[0] or 0)
                    finally:
                        con.close()
                except Exception:
                    n_reg = 0

                min_reg = int(os.environ.get("PROMOTE_MIN_REGIME_LABELS", "50"))
                if n_reg < min_reg:
                    print(f"[PROMOTE] model={variant_name} skip regime={regime} n_reg={n_reg} < min_reg={min_reg}")
                    continue

                promoted = promote_with_snapshot_and_db_watch(
                    variant_name,
                    kind,
                    int(snap),
                    key=str(regime),
                    actor="auto",
                    extra_reason={"compare": cmp_reason, "guard": guard_reason, "metrics": metrics},
                )

                if promoted:
                    promoted_regimes.append(str(regime))
                    print(f"[PROMOTE] model={variant_name} promoted challenger -> champion key={regime}")
                else:
                    print(f"[BLOCKED] model={variant_name} registry promotion failed key={regime}")

            if promoted_regimes:
                mark_version_live(
                    variant_name,
                    str(model_version),
                    stage="champion",
                    meta_patch={
                        "promoted_ts_ms": int(time.time() * 1000),
                        "model_id": str(metrics.get("model_id") or variant_name),
                        "promoted_regimes": list(promoted_regimes),
                    },
                )
                retire_underperforming_versions(variant_name, protect_versions=[str(model_version)])
                audit(
                    actor="auto",
                    action="promote",
                    model_name=variant_name,
                    from_kind=prev_kind,
                    from_ts_ms=prev_ts,
                    to_kind=kind,
                    to_ts_ms=int(snap),
                    reason={
                        "compare": cmp_reason,
                        "guard": guard_reason,
                        "metrics": metrics,
                        "promoted_regimes": list(promoted_regimes),
                    },
                )
                print(f"[PROMOTE] model={variant_name} champion <- {kind} @ {int(snap)}")
            else:
                update_model_version_status(
                    variant_name,
                    str(model_version),
                    stage="challenger",
                    status="candidate",
                    live_ready=False,
                    meta_patch={
                        "promotion_deferred_ts_ms": int(time.time() * 1000),
                        "model_id": str(metrics.get("model_id") or variant_name),
                        "deferred_reason": "registry_promotion_not_completed",
                    },
                )
                audit(
                    actor="auto",
                    action="block",
                    model_name=variant_name,
                    to_kind=kind,
                    to_ts_ms=int(snap),
                    reason={
                        "error": "registry_promotion_not_completed",
                        "compare": cmp_reason,
                        "guard": guard_reason,
                        "metrics": metrics,
                    },
                )
                print(f"[BLOCKED] model={variant_name} version remains challenger; no registry promotion completed")
            trained_variants += 1

        # 1.5) Evaluate temporal shadow models (A.7)
        _run_python("jobs/eval_temporal_shadow.py")

        if trained_variants <= 0:
            audit(
                actor="system",
                action="block",
                model_name=MODEL_NAME,
                reason={"error": "no_configured_variants_trained", "family": DEFAULT_FAMILY},
            )
            return 1

        return 0

    except SystemExit:
        raise
    except Exception as e:
        logging.exception("pipeline failed: %r", e)
        _sleep_with_jitter(5.0)
        raise
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("PIPELINE_TRAIN_AND_EVAL_LOCK_RELEASE_FAILED", e, job=JOB_NAME)


if __name__ == "__main__":
    raise SystemExit(main())
