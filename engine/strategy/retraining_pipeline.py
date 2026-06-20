"""Prepare a retraining dataset and dispatch existing training jobs.

This module is intentionally limited to pipeline hooks:
- extract point-in-time feature snapshots from ``engine.data.feature_store``
- join them to historical outcomes from ``labels`` / ``labels_exec``
- optionally trigger the repo's existing retraining jobs

It does not implement model fitting logic.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from engine.data import feature_store
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.job_registry import ALLOWED_JOBS
from engine.runtime.logging import get_logger
from engine.runtime.platform import default_local_artifacts_dir
from engine.runtime.storage import connect
from engine.training_guard import training_allowed

DAY_MS = 24 * 60 * 60 * 1000
DEFAULT_OUTPUT_DIR = default_local_artifacts_dir() / "retraining"
DEFAULT_LOOKBACK_DAYS = max(0, int(os.environ.get("RETRAINING_LOOKBACK_DAYS", "365")))
DEFAULT_LIMIT = max(0, int(os.environ.get("RETRAINING_SAMPLE_LIMIT", "0")))
DEFAULT_JOBS = (
    "train_embed_models",
    "train_model_v2",
    "train_temporal_predictor",
)
LOG = get_logger("retraining_pipeline")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        out = float(value)
    except Exception:
        return default
    if out != out or out in (float("inf"), float("-inf")):
        return default
    return float(out)


def _normalize_symbols(values: Iterable[str] | None) -> tuple[str, ...]:
    out: list[str] = []
    for value in values or ():
        symbol = str(value or "").strip().upper()
        if symbol and symbol not in out:
            out.append(symbol)
    return tuple(out)


def _normalize_horizons(values: Iterable[int] | None) -> tuple[int, ...]:
    out: list[int] = []
    for value in values or ():
        horizon_s = _safe_int(value, 0)
        if horizon_s > 0 and horizon_s not in out:
            out.append(horizon_s)
    return tuple(out)


def _timestamp_slug(ts_ms: int) -> str:
    return time.strftime("%Y%m%d_%H%M%S", time.localtime(int(ts_ms) / 1000.0))


@dataclass(frozen=True)
class RetrainingPipelineConfig:
    """Describe one retraining extraction run and its optional job dispatch."""

    output_dir: Path = DEFAULT_OUTPUT_DIR
    lookback_days: int = DEFAULT_LOOKBACK_DAYS
    limit: int = DEFAULT_LIMIT
    symbols: tuple[str, ...] = ()
    horizons: tuple[int, ...] = ()
    persist_feature_snapshots: bool = True
    trigger_jobs: bool = True
    jobs: tuple[str, ...] = DEFAULT_JOBS


def _table_exists(con: Any, table_name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table_name),),
    ).fetchone()
    return bool(row)


def _empty_feature_snapshot() -> dict[str, Any]:
    return {
        "ts_ms": 0,
        "schema_version": int(feature_store.FEATURE_SCHEMA_VERSION),
        "feature_set_tag": str(feature_store.FEATURE_SET_TAG),
        "feature_names": list(feature_store.FEATURE_NAMES),
        "point_count": 0,
        "source_timestamps": {},
        "vector": [0.0 for _ in feature_store.FEATURE_NAMES],
        "features": {str(name): 0.0 for name in feature_store.FEATURE_NAMES},
    }


def _build_outcome_query(config: RetrainingPipelineConfig) -> tuple[str, tuple[Any, ...]]:
    where = [
        "COALESCE(e.ts_ms, l.created_at_ms, le.ts_ms, 0) > 0",
        "COALESCE(CASE WHEN le.realized = 1 THEN le.net_z END, le.net_z, l.impact_z, le.net_ret, l.realized_ret) IS NOT NULL",
    ]
    params: list[Any] = []

    if int(config.lookback_days) > 0:
        where.append("COALESCE(e.ts_ms, l.created_at_ms, le.ts_ms) >= ?")
        params.append(_now_ms() - (int(config.lookback_days) * DAY_MS))

    if config.symbols:
        where.append("UPPER(l.symbol) IN (" + ",".join("?" for _ in config.symbols) + ")")
        params.extend(config.symbols)

    if config.horizons:
        where.append("l.horizon_s IN (" + ",".join("?" for _ in config.horizons) + ")")
        params.extend(int(h) for h in config.horizons)

    sql = f"""
        SELECT
          l.event_id,
          UPPER(l.symbol) AS symbol,
          l.horizon_s,
          e.ts_ms AS event_ts_ms,
          l.created_at_ms,
          le.ts_ms AS exec_ts_ms,
          CASE
            WHEN e.ts_ms IS NOT NULL THEN e.ts_ms
            WHEN l.created_at_ms IS NOT NULL THEN l.created_at_ms
            ELSE COALESCE(le.ts_ms, 0)
          END AS feature_anchor_ts_ms,
          CASE
            WHEN e.ts_ms IS NOT NULL THEN 'event_ts_ms'
            WHEN l.created_at_ms IS NOT NULL THEN 'label_created_at_ms'
            WHEN le.ts_ms IS NOT NULL THEN 'exec_label_ts_ms'
            ELSE 'unknown'
          END AS feature_anchor_source,
          CASE
            WHEN le.realized = 1 AND le.net_z IS NOT NULL THEN le.net_z
            WHEN le.net_z IS NOT NULL THEN le.net_z
            ELSE l.impact_z
          END AS target_z,
          CASE
            WHEN le.realized = 1 AND le.net_ret IS NOT NULL THEN le.net_ret
            WHEN le.net_ret IS NOT NULL THEN le.net_ret
            ELSE l.realized_ret
          END AS target_return,
          l.baseline_ret,
          l.realized_ret,
          l.impact_z,
          l.vol_proxy,
          l.regime,
          e.event_type,
          e.source AS event_source,
          le.source AS exec_source,
          le.realized AS exec_realized,
          le.gross_ret,
          le.net_ret,
          le.gross_z,
          le.net_z,
          le.total_cost_bps
        FROM labels l
        LEFT JOIN events e
          ON e.id = l.event_id
        LEFT JOIN labels_exec le
          ON le.event_id = l.event_id
         AND le.symbol = l.symbol
         AND le.horizon_s = l.horizon_s
        WHERE {" AND ".join(where)}
        ORDER BY feature_anchor_ts_ms ASC, l.event_id ASC, symbol ASC, l.horizon_s ASC
    """
    if int(config.limit) > 0:
        sql += " LIMIT ?"
        params.append(int(config.limit))
    return sql, tuple(params)


def _feature_snapshot_for_row(
    symbol: str,
    anchor_ts_ms: int,
    *,
    con: Any,
    persist: bool,
) -> dict[str, Any]:
    try:
        snapshot = feature_store.get_features_asof(
            str(symbol),
            int(anchor_ts_ms),
            con=con,
            persist=bool(persist),
        )
    except Exception as exc:
        log_failure(
            LOG,
            event="retraining_feature_snapshot_failed",
            code="RETRAINING_FEATURE_SNAPSHOT_FAILED",
            message=str(exc),
            error=exc,
            level=30,
            component="engine.strategy.retraining_pipeline",
            extra={"symbol": str(symbol), "anchor_ts_ms": int(anchor_ts_ms)},
            persist=False,
        )
        snapshot = _empty_feature_snapshot()
    return {
        "ts_ms": int(snapshot.get("ts_ms") or 0),
        "schema_version": int(snapshot.get("schema_version") or feature_store.FEATURE_SCHEMA_VERSION),
        "feature_set_tag": str(snapshot.get("feature_set_tag") or feature_store.FEATURE_SET_TAG),
        "feature_names": list(snapshot.get("feature_names") or feature_store.FEATURE_NAMES),
        "point_count": int(snapshot.get("point_count") or 0),
        "source_timestamps": dict(snapshot.get("source_timestamps") or {}),
        "vector": [float(v) for v in list(snapshot.get("vector") or [])],
        "features": dict(snapshot.get("features") or {}),
    }


def _outcome_source(exec_realized: int, exec_source: str | None) -> str:
    if int(exec_realized or 0) == 1:
        return str(exec_source or "labels_exec")
    if exec_source:
        return str(exec_source)
    return "labels"


def extract_training_data(config: RetrainingPipelineConfig) -> dict[str, Any]:
    """Build a point-in-time retraining dataset and write its manifest."""
    run_ts_ms = _now_ms()
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    slug = _timestamp_slug(run_ts_ms)
    dataset_path = output_dir / f"retraining_dataset_{slug}.jsonl"
    manifest_path = output_dir / f"retraining_manifest_{slug}.json"

    sql, params = _build_outcome_query(config)
    symbol_counts: Counter[str] = Counter()
    horizon_counts: Counter[str] = Counter()
    outcome_source_counts: Counter[str] = Counter()
    zero_feature_rows = 0
    extracted_rows = 0
    min_anchor_ts_ms = 0
    max_anchor_ts_ms = 0
    feature_names = list(feature_store.FEATURE_NAMES)
    feature_set_tags: set[str] = set()
    errors: list[str] = []

    con = connect(readonly=not bool(config.persist_feature_snapshots))
    try:
        can_persist_feature_snapshots = bool(config.persist_feature_snapshots) and _table_exists(con, "market_features")
        if not _table_exists(con, "labels"):
            errors.append("labels_table_missing")
            cursor = ()
        else:
            cursor = con.execute(sql, params)
        with dataset_path.open("w", encoding="utf-8", newline="\n") as handle:
            for row in cursor:
                (
                    event_id,
                    symbol,
                    horizon_s,
                    event_ts_ms,
                    label_created_at_ms,
                    exec_ts_ms,
                    feature_anchor_ts_ms,
                    feature_anchor_source,
                    target_z,
                    target_return,
                    baseline_ret,
                    realized_ret,
                    impact_z,
                    vol_proxy,
                    regime,
                    event_type,
                    event_source,
                    exec_source,
                    exec_realized,
                    gross_ret,
                    net_ret,
                    gross_z,
                    net_z,
                    total_cost_bps,
                ) = row

                anchor_ts_ms = _safe_int(feature_anchor_ts_ms, 0)
                if anchor_ts_ms <= 0:
                    continue

                snapshot = _feature_snapshot_for_row(
                    str(symbol or ""),
                    anchor_ts_ms,
                    con=con,
                    persist=bool(can_persist_feature_snapshots),
                )
                feature_names = list(snapshot.get("feature_names") or feature_names)
                feature_set_tags.add(str(snapshot.get("feature_set_tag") or ""))

                if int(snapshot.get("ts_ms") or 0) <= 0:
                    zero_feature_rows += 1

                sample = {
                    "event_id": _safe_int(event_id, 0),
                    "symbol": str(symbol or "").upper(),
                    "horizon_s": _safe_int(horizon_s, 0),
                    "feature_anchor": {
                        "ts_ms": anchor_ts_ms,
                        "source": str(feature_anchor_source or "unknown"),
                    },
                    "feature_snapshot": snapshot,
                    "historical_outcomes": {
                        "target_z": _safe_float(target_z),
                        "target_return": _safe_float(target_return),
                        "target_source": _outcome_source(_safe_int(exec_realized, 0), str(exec_source or "")),
                        "label": {
                            "baseline_ret": _safe_float(baseline_ret),
                            "realized_ret": _safe_float(realized_ret),
                            "impact_z": _safe_float(impact_z),
                            "created_at_ms": _safe_int(label_created_at_ms, 0),
                            "vol_proxy": _safe_float(vol_proxy),
                            "regime": str(regime or ""),
                        },
                        "execution": {
                            "ts_ms": _safe_int(exec_ts_ms, 0),
                            "source": str(exec_source or ""),
                            "realized": bool(_safe_int(exec_realized, 0)),
                            "gross_ret": _safe_float(gross_ret),
                            "net_ret": _safe_float(net_ret),
                            "gross_z": _safe_float(gross_z),
                            "net_z": _safe_float(net_z),
                            "total_cost_bps": _safe_float(total_cost_bps),
                        },
                    },
                    "context": {
                        "event_ts_ms": _safe_int(event_ts_ms, 0),
                        "event_type": str(event_type or ""),
                        "event_source": str(event_source or ""),
                    },
                }
                handle.write(json.dumps(sample, separators=(",", ":"), sort_keys=True) + "\n")

                extracted_rows += 1
                symbol_counts[str(sample["symbol"])] += 1
                horizon_counts[str(sample["horizon_s"])] += 1
                outcome_source_counts[str(sample["historical_outcomes"]["target_source"])] += 1
                min_anchor_ts_ms = anchor_ts_ms if min_anchor_ts_ms == 0 else min(min_anchor_ts_ms, anchor_ts_ms)
                max_anchor_ts_ms = max(max_anchor_ts_ms, anchor_ts_ms)

        if bool(can_persist_feature_snapshots):
            con.commit()
    finally:
        try:
            con.close()
        except Exception as exc:
            log_failure(
                LOG,
                event="retraining_pipeline_close_failed",
                code="RETRAINING_PIPELINE_CLOSE_FAILED",
                message=str(exc),
                error=exc,
                level=30,
                component="engine.strategy.retraining_pipeline",
                persist=False,
            )

    manifest = {
        "run_ts_ms": int(run_ts_ms),
        "dataset_path": str(dataset_path),
        "manifest_path": str(manifest_path),
        "lookback_days": int(config.lookback_days),
        "limit": int(config.limit),
        "symbols_filter": list(config.symbols),
        "horizons_filter": [int(h) for h in config.horizons],
        "row_count": int(extracted_rows),
        "feature_names": list(feature_names),
        "feature_set_tags": sorted(tag for tag in feature_set_tags if tag),
        "feature_snapshot_zero_rows": int(zero_feature_rows),
        "time_range": {
            "min_anchor_ts_ms": int(min_anchor_ts_ms),
            "max_anchor_ts_ms": int(max_anchor_ts_ms),
        },
        "counts": {
            "by_symbol": dict(symbol_counts),
            "by_horizon_s": {str(k): int(v) for k, v in horizon_counts.items()},
            "by_outcome_source": dict(outcome_source_counts),
        },
        "pipeline_scope": {
            "extract_training_data": True,
            "trigger_retraining_jobs": bool(config.trigger_jobs),
            "implements_model_training": False,
        },
        "errors": list(errors),
    }

    with manifest_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")

    return manifest


def trigger_retraining_jobs(job_names: Sequence[str]) -> list[dict[str, Any]]:
    """Dispatch allowed retraining jobs through the normal runtime job registry."""
    if not training_allowed():
        return [
            {
                "job": str(name),
                "ok": False,
                "error": "training_disabled_by_guard",
            }
            for name in job_names
        ]

    from engine.runtime.jobs_manager import JobManager

    manager = JobManager.get_instance()
    results: list[dict[str, Any]] = []
    for job_name in job_names:
        name = str(job_name or "").strip()
        if not name:
            continue
        if name not in ALLOWED_JOBS:
            results.append({"job": name, "ok": False, "error": "unknown_job"})
            continue
        try:
            result = manager.start(name)
        except Exception as exc:
            log_failure(
                LOG,
                event="retraining_job_start_failed",
                code="RETRAINING_JOB_START_FAILED",
                message=str(exc),
                error=exc,
                level=30,
                component="engine.strategy.retraining_pipeline",
                extra={"job": name},
                persist=False,
            )
            result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        results.append({"job": name, **dict(result or {})})
    return results


def run_retraining_pipeline(config: RetrainingPipelineConfig) -> dict[str, Any]:
    """Extract a dataset and optionally trigger the configured retraining jobs."""
    manifest = extract_training_data(config)
    job_results: list[dict[str, Any]] = []

    if bool(config.trigger_jobs) and int(manifest.get("row_count") or 0) > 0:
        job_results = trigger_retraining_jobs(config.jobs)
    elif bool(config.trigger_jobs):
        job_results = [
            {
                "job": str(name),
                "ok": False,
                "error": "no_training_rows_extracted",
            }
            for name in config.jobs
        ]

    manifest["job_results"] = job_results
    manifest["jobs_requested"] = list(config.jobs)
    manifest["jobs_triggered"] = [
        str(result.get("job") or "")
        for result in job_results
        if bool(result.get("ok"))
    ]

    manifest_path = Path(str(manifest.get("manifest_path") or ""))
    if manifest_path:
        with manifest_path.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)
            handle.write("\n")

    return manifest


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare retraining data and trigger retraining jobs.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory for JSONL dataset and manifest.")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS, help="Historical window to extract.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Maximum samples to extract. Use 0 for all rows.")
    parser.add_argument("--symbols", nargs="*", default=(), help="Optional symbol filter, e.g. SPY BTC AAPL.")
    parser.add_argument("--horizons", nargs="*", type=int, default=(), help="Optional horizon filter in seconds.")
    parser.add_argument(
        "--jobs",
        nargs="*",
        default=list(DEFAULT_JOBS),
        help="Registered training jobs to dispatch after extraction.",
    )
    parser.add_argument(
        "--no-persist-feature-snapshots",
        action="store_true",
        help="Do not write extracted feature snapshots back into market_features.",
    )
    parser.add_argument(
        "--dataset-only",
        action="store_true",
        help="Extract data and write artifacts, but do not start retraining jobs.",
    )
    return parser.parse_args()


def main() -> int:
    """Run the retraining pipeline from the command line."""
    args = _parse_args()
    config = RetrainingPipelineConfig(
        output_dir=Path(str(args.output_dir)),
        lookback_days=max(0, int(args.lookback_days)),
        limit=max(0, int(args.limit)),
        symbols=_normalize_symbols(args.symbols),
        horizons=_normalize_horizons(args.horizons),
        persist_feature_snapshots=not bool(args.no_persist_feature_snapshots),
        trigger_jobs=not bool(args.dataset_only),
        jobs=tuple(str(job).strip() for job in args.jobs if str(job).strip()),
    )
    result = run_retraining_pipeline(config)
    print(json.dumps(result, indent=2, sort_keys=True))

    failures = [item for item in list(result.get("job_results") or []) if not bool(item.get("ok"))]
    if bool(config.trigger_jobs) and failures:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
