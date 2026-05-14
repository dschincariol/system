"""
FILE: train_embed_models.py

One-shot retraining helper for supervised embedding models.

The job is intentionally idempotent: it records the last label watermark in the
database and skips work unless enough new labeled data has arrived.
"""

import os
import time
import socket
import os as _os
import json
from typing import Any, Dict

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db, acquire_job_lock, release_job_lock
from engine.data.universe_pit import resolve_training_window_universe
from engine.strategy.embed_regressor import train_embed_models
from engine.strategy.model_lifecycle import (
    finish_lifecycle_run,
    load_lifecycle_plan,
    publish_lifecycle_status,
    record_version_performance,
    register_model_version,
    start_lifecycle_run,
    update_model_version_status,
    version_from_ts,
)
from engine.strategy.learning_loop import build_dataset_snapshot
from engine.strategy.feature_registry import feature_set_tag_from_ids, resolve_feature_ids
from engine.strategy.model_config import (
    DEFAULT_FAMILY,
    build_model_registration_metadata,
    get_model_config,
    load_model_configs,
)
from engine.training_guard import training_allowed

# ----------------------------
# Job identity
# ----------------------------
OWNER = socket.gethostname()
PID = _os.getpid()


# ----------------------------
# Training config
# ----------------------------
SYMBOLS = ["SPY", "BTC", "OIL"]
HORIZONS = [300, 3600]

MIN_NEW_LABELS = int(os.environ.get("EMBED_MODEL_MIN_NEW_LABELS", "25"))
LOOKBACK_DAYS = int(os.environ.get("EMBED_MODEL_LOOKBACK_DAYS", "365"))
ALPHA = float(os.environ.get("EMBED_MODEL_ALPHA", "1.0"))
MIN_SAMPLES = int(os.environ.get("EMBED_MODEL_MIN_SAMPLES", "50"))
MODEL_KIND = os.environ.get("EMBED_MODEL_KIND", "ridge").strip().lower()  # ridge | mlp | auto
_FEATURE_IDS_RAW = str(os.environ.get("EMBED_MODEL_FEATURE_IDS", "") or "").strip()
FEATURE_IDS = (
    resolve_feature_ids(
        [s.strip() for s in _FEATURE_IDS_RAW.split(",") if s.strip()],
        model_name="embed_regressor",
    )
    if _FEATURE_IDS_RAW
    else resolve_feature_ids(model_name="embed_regressor")
)
LOG = get_logger("engine.strategy.train_embed_models")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.strategy.train_embed_models",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _resolve_training_config(plan: Dict[str, Any]) -> Dict[str, Any]:
    cfg = get_model_config(str(plan.get("model_name") or "").strip()) if plan else {}
    if not cfg:
        configs = load_model_configs(family=DEFAULT_FAMILY)
        cfg = dict(configs[0]) if configs else {}

    model_name = str(plan.get("model_name") or cfg.get("model_name") or DEFAULT_FAMILY).strip() or DEFAULT_FAMILY
    family = str(cfg.get("family") or DEFAULT_FAMILY).strip() or DEFAULT_FAMILY
    feature_ids = resolve_feature_ids(
        list(cfg.get("feature_ids") or FEATURE_IDS),
        model_name=str(model_name),
    )
    horizons = [int(h) for h in list(cfg.get("horizons_s") or cfg.get("horizons") or HORIZONS) if int(h) > 0]
    return {
        **cfg,
        "family": family,
        "model_name": str(cfg.get("model_name") or model_name).strip() or model_name,
        "model_id": str(cfg.get("model_id") or cfg.get("model_name") or model_name).strip() or model_name,
        "model_kind": str(cfg.get("model_kind") or MODEL_KIND).strip().lower() or MODEL_KIND,
        "symbol_universe": list(cfg.get("symbol_universe") or cfg.get("symbols") or SYMBOLS),
        "horizons_s": horizons or list(HORIZONS),
        "horizon_s": int(cfg.get("horizon_s") or (horizons[0] if horizons else 0) or 0),
        "feature_ids": list(feature_ids),
        "training_window_days": int(cfg.get("training_window_days") or cfg.get("lookback_days") or LOOKBACK_DAYS),
        "risk_profile": str(cfg.get("risk_profile") or "balanced").strip().lower() or "balanced",
        "instance_name": str(cfg.get("instance_name") or cfg.get("model_name") or model_name).strip() or model_name,
    }


# ----------------------------
# DB helpers
# ----------------------------
def _ensure_meta(con):
    # This small metadata table is the job's memory of the last successful
    # training watermark.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS model_runs (
          key TEXT PRIMARY KEY,
          last_count INTEGER NOT NULL,
          last_max_created_at_ms INTEGER NOT NULL,
          last_run_ms INTEGER NOT NULL
        )
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_model_runs_last_run ON model_runs(last_run_ms)"
    )


def _labels_stamp(con):
    row = con.execute(
        """
        SELECT COUNT(*), MAX(created_at_ms)
        FROM labels
        WHERE impact_z IS NOT NULL
        """
    ).fetchone()
    n = int((row[0] or 0) if row else 0)
    mx = int((row[1] or 0) if row else 0)
    return n, mx


# ----------------------------
# Main logic
# ----------------------------
def main() -> int:
    init_db()
    plan = load_lifecycle_plan()
    train_cfg = _resolve_training_config(plan)
    pit_universe = {
        "pit_enabled": False,
        "pit_applied": False,
        "symbols": list(train_cfg.get("symbol_universe") or SYMBOLS),
        "fallback_reason": "not_resolved",
    }
    model_name = str(train_cfg.get("model_name") or DEFAULT_FAMILY).strip() or DEFAULT_FAMILY
    model_run_key = f"embed_models:{model_name}"
    lifecycle_run_id = int(plan.get("lifecycle_run_id") or 0)
    version = ""

    if not training_allowed():
        print("embed_models: training disabled by training_guard")
        return 0

    # Only one retraining run should decide the watermark at a time.
    if not acquire_job_lock("train_embed_models", OWNER, PID):
        print("embed_models: another training job is running; exiting")
        return 0

    try:
        con = connect()
        try:
            _ensure_meta(con)

            cur_n, cur_mx = _labels_stamp(con)

            row = con.execute(
                """
                SELECT last_count, last_max_created_at_ms
                FROM model_runs
                WHERE key=?
                """,
                (str(model_run_key),),
            ).fetchone()

            last_n = int(row[0]) if row else 0
            last_mx = int(row[1]) if row else 0

            new_labels = max(0, cur_n - last_n)
            changed = (cur_mx != last_mx)

            if (not plan) and ((not changed) or (new_labels < MIN_NEW_LABELS)):
                print(
                    f"embed_models: SKIP cur_n={cur_n} last_n={last_n} "
                    f"new={new_labels} cur_mx={cur_mx} last_mx={last_mx} "
                    f"min_new={MIN_NEW_LABELS}"
                )

                con.execute(
                    """
                    INSERT INTO model_runs(key, last_count, last_max_created_at_ms, last_run_ms)
                    VALUES(?,?,?,?)
                    ON CONFLICT(key) DO UPDATE SET
                      last_count=excluded.last_count,
                      last_max_created_at_ms=excluded.last_max_created_at_ms,
                      last_run_ms=excluded.last_run_ms
                    """,
                    (str(model_run_key), last_n, last_mx, int(time.time() * 1000)),
                )
                con.commit()
                return 0

        finally:
            con.close()

        if lifecycle_run_id <= 0:
            lifecycle_run_id = int(
                start_lifecycle_run(
                    model_name=str(model_name),
                    model_version=str(plan.get("model_version") or ""),
                    parent_version=plan.get("parent_version"),
                    action="train_embed_models",
                    status="running",
                    triggered_by="train_embed_models",
                    mutation_kind=plan.get("mutation_kind"),
                    details={"variation": dict(plan or {})},
                )
                or 0
            )

        con_universe = connect(readonly=True)
        try:
            pit_universe = resolve_training_window_universe(
                con_universe,
                configured_symbols=list(train_cfg.get("symbol_universe") or SYMBOLS),
                lookback_days=int(train_cfg.get("training_window_days") or LOOKBACK_DAYS),
            )
        finally:
            con_universe.close()
        if list(pit_universe.get("symbols") or []):
            train_cfg["symbol_universe"] = list(pit_universe.get("symbols") or [])

        print(
            f"embed_models: TRAIN cur_n={cur_n} last_n={last_n} new={new_labels} "
            f"model_name={model_name} "
            f"lookback_days={int(train_cfg.get('training_window_days') or LOOKBACK_DAYS)} min_samples={MIN_SAMPLES} "
            f"alpha={ALPHA} kind={str(train_cfg.get('model_kind') or MODEL_KIND)} "
            f"symbols={json.dumps(list(train_cfg.get('symbol_universe') or SYMBOLS))} "
            f"horizons={json.dumps(list(train_cfg.get('horizons_s') or HORIZONS))} "
            f"feature_ids={json.dumps(list(train_cfg.get('feature_ids') or FEATURE_IDS))} "
            f"conf_calib={os.environ.get('EMBED_CONF_CALIB','1')} "
            f"conf_k={os.environ.get('EMBED_REGRESSOR_CONF_K','75.0')}"
        )

        training_started_ts_ms = int(time.time() * 1000)
        dataset_feature_schema = {
            "feature_ids": list(train_cfg.get("feature_ids") or FEATURE_IDS),
            "feature_set_tag": str(
                feature_set_tag_from_ids(list(train_cfg.get("feature_ids") or FEATURE_IDS))
            ),
            "feature_count": int(len(list(train_cfg.get("feature_ids") or FEATURE_IDS))),
            "ts_ms": int(training_started_ts_ms),
        }
        dataset_training_window = {
            "lookback_days": int(train_cfg.get("training_window_days") or LOOKBACK_DAYS),
            "end_ts_ms": int(training_started_ts_ms),
            "start_ts_ms": int(
                training_started_ts_ms
                - (int(train_cfg.get("training_window_days") or LOOKBACK_DAYS) * 24 * 60 * 60 * 1000)
            ),
            "horizons": list(train_cfg.get("horizons_s") or HORIZONS),
        }
        dataset_used = build_dataset_snapshot(
            model_name=str(model_name),
            lookback_days=int(train_cfg.get("training_window_days") or LOOKBACK_DAYS),
            symbols=list(train_cfg.get("symbol_universe") or SYMBOLS),
            horizons=list(train_cfg.get("horizons_s") or HORIZONS),
            feature_ids=list(train_cfg.get("feature_ids") or FEATURE_IDS),
            feature_schema=dict(dataset_feature_schema),
            training_window=dict(dataset_training_window),
            extra={
                "job_name": "train_embed_models",
                "model_kind": str(train_cfg.get("model_kind") or MODEL_KIND),
                "pit_universe": dict(pit_universe or {}),
            },
        )

        # Model training owns its own persistence; this wrapper only decides
        # whether the run is necessary and records the new watermark.
        _out = train_embed_models(
            symbols=list(train_cfg.get("symbol_universe") or SYMBOLS),
            horizons=list(train_cfg.get("horizons_s") or HORIZONS),
            min_samples=MIN_SAMPLES,
            alpha=ALPHA,
            lookback_days=int(train_cfg.get("training_window_days") or LOOKBACK_DAYS),
            feature_ids=list(train_cfg.get("feature_ids") or FEATURE_IDS),
            kind=str(train_cfg.get("model_kind") or MODEL_KIND),
            model_name=str(model_name),
        )

        eval_ts_ms = 0
        eval_rows = []
        con_eval = connect()
        try:
            row = con_eval.execute("SELECT MAX(ts_ms) FROM embed_model_eval").fetchone()
            eval_ts_ms = int((row[0] or 0) if row else 0)
            if eval_ts_ms > 0:
                eval_rows = con_eval.execute(
                    """
                    SELECT model_kind, n_train, n_eval, rmse, spearman, directional_acc
                    FROM embed_model_eval
                    WHERE ts_ms=?
                    ORDER BY n_eval DESC
                    LIMIT 250
                    """,
                    (int(eval_ts_ms),),
                ).fetchall()
        finally:
            con_eval.close()

        if eval_ts_ms > 0:
            version = str(plan.get("model_version") or version_from_ts(str(model_name), int(eval_ts_ms), prefix="embed"))
            total_eval = 0
            weighted_rmse = 0.0
            weighted_dir = 0.0
            chosen_kind = str(train_cfg.get("model_kind") or MODEL_KIND)
            for row in eval_rows or []:
                mk, _n_train, n_eval, rmse, _spearman, directional_acc = row
                weight = int(n_eval or 0)
                if weight <= 0:
                    continue
                if total_eval <= 0:
                    chosen_kind = str(mk or chosen_kind)
                total_eval += weight
                weighted_rmse += float(rmse or 0.0) * float(weight)
                weighted_dir += float(directional_acc or 0.0) * float(weight)
            avg_rmse = (weighted_rmse / float(total_eval)) if total_eval > 0 else 0.0
            avg_dir = (weighted_dir / float(total_eval)) if total_eval > 0 else 0.0
            registration_meta = build_model_registration_metadata(train_cfg)

            register_model_version(
                model_name=str(model_name),
                model_version=str(version),
                model_kind=str(chosen_kind or train_cfg.get("model_kind") or MODEL_KIND),
                parent_version=plan.get("parent_version"),
                mutation_kind=str(plan.get("mutation_kind") or "baseline_retrain"),
                stage="shadow",
                status="trained",
                live_ready=False,
                training_job_name="train_embed_models",
                train_scope={
                    **dict(plan.get("train_scope") or {
                    "symbols": list(train_cfg.get("symbol_universe") or SYMBOLS),
                    "horizons": list(train_cfg.get("horizons_s") or HORIZONS),
                    "lookback_days": int(train_cfg.get("training_window_days") or LOOKBACK_DAYS),
                    "feature_ids": list(train_cfg.get("feature_ids") or FEATURE_IDS),
                    "risk_profile": str(train_cfg.get("risk_profile") or "balanced"),
                    }),
                    "dataset_used": dataset_used,
                },
                meta={
                    "standalone_job": not bool(plan),
                    "trigger": plan.get("trigger") or {},
                    "model_id": str(registration_meta.get("model_id") or model_name),
                    "model_family": str(registration_meta.get("model_family") or DEFAULT_FAMILY),
                    "instance_name": str(registration_meta.get("instance_name") or model_name),
                    "risk_profile": str(registration_meta.get("risk_profile") or "balanced"),
                    "dataset_used": dataset_used,
                    "training_started_ts_ms": int(training_started_ts_ms),
                },
            )
            record_version_performance(
                model_name=str(model_name),
                model_version=str(version),
                metric_scope="training",
                metrics={
                    "avg_rmse": float(avg_rmse),
                    "avg_directional_acc": float(avg_dir),
                    "quality_score": float(max(0.0, min(1.0, avg_dir))),
                    "eval_ts_ms": int(eval_ts_ms),
                    "trained_models": int(len(_out or {})),
                },
                sample_n=int(total_eval),
                meta={"job_name": "train_embed_models"},
            )
            update_model_version_status(
                str(model_name),
                str(version),
                stage="shadow",
                status="trained",
                live_ready=False,
                meta_patch={
                    "dataset_used": dataset_used,
                    "training_started_ts_ms": int(training_started_ts_ms),
                    "training_completed_ts_ms": int(time.time() * 1000),
                },
            )
            if lifecycle_run_id > 0:
                finish_lifecycle_run(
                    int(lifecycle_run_id),
                    status="ok",
                    details={
                        "model_version": str(version),
                        "trained_models": int(len(_out or {})),
                        "total_eval": int(total_eval),
                        "dataset_used": dataset_used,
                    },
                )
            publish_lifecycle_status(
                {
                    "ok": True,
                    "model_name": str(model_name),
                    "active_job": "train_embed_models",
                    "version": str(version),
                    "mutation_kind": str(plan.get("mutation_kind") or "baseline_retrain"),
                    "trained_models": int(len(_out or {})),
                    "dataset_used": dataset_used,
                    "ts_ms": int(time.time() * 1000),
                }
            )

        # Update meta after training
        con2 = connect()
        try:
            _ensure_meta(con2)
            cur_n2, cur_mx2 = _labels_stamp(con2)
            con2.execute(
                """
                INSERT INTO model_runs(key, last_count, last_max_created_at_ms, last_run_ms)
                VALUES(?,?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                  last_count=excluded.last_count,
                  last_max_created_at_ms=excluded.last_max_created_at_ms,
                  last_run_ms=excluded.last_run_ms
                """,
                (str(model_run_key), int(cur_n2), int(cur_mx2), int(time.time() * 1000)),
            )
            con2.commit()
        finally:
            con2.close()

        return 0
    except Exception:
        if version:
            try:
                update_model_version_status(
                    str(model_name),
                    str(version),
                    stage="retired",
                    status="error",
                    live_ready=False,
                    meta_patch={"error_ts_ms": int(time.time() * 1000)},
                )
            except Exception:
                _warn_nonfatal("TRAIN_EMBED_MODELS_VERSION_STATUS_FAILED", RuntimeError("update_model_version_status failed"), once_key="version_status_error", version=str(version))
        if lifecycle_run_id > 0:
            try:
                finish_lifecycle_run(
                    int(lifecycle_run_id),
                    status="error",
                    details={"error_ts_ms": int(time.time() * 1000)},
                )
            except Exception:
                _warn_nonfatal("TRAIN_EMBED_MODELS_LIFECYCLE_STATUS_FAILED", RuntimeError("publish_lifecycle_status failed"), once_key="lifecycle_status_error", lifecycle_run_id=int(lifecycle_run_id))
        raise

    finally:
        try:
            release_job_lock("train_embed_models", OWNER, PID)
        except Exception as e:
            _warn_nonfatal("TRAIN_EMBED_MODELS_RELEASE_LOCK_FAILED", e, once_key="release_lock")


if __name__ == "__main__":
    raise SystemExit(main())
