"""
Synthetic load probe for the event->prediction->decision->execution path.

This runs the real SQLite-backed pipeline against a temporary DB and reports:
  - ingestion -> DB latency
  - DB -> prediction latency
  - prediction -> decision latency
  - decision -> execution latency
  - row counts and slow-write/query indicators

The probe intentionally stubs only the model edge and predictor edge so the
pipeline logic, DB access, alert persistence, execution-intent loading, and
broker-application control flow remain real.
"""

from __future__ import annotations

import argparse
import contextlib
import importlib
import json
import math
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Sequence
from unittest.mock import patch

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if not math.isfinite(out):
            return float(default)
        return float(out)
    except Exception as e:
        sys.stderr.write(
            f"[pipeline_latency_load_probe] safe_float_failed value={value!r}: {type(e).__name__}: {e}\n"
        )
        sys.stderr.flush()
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception as e:
        sys.stderr.write(
            f"[pipeline_latency_load_probe] safe_int_failed value={value!r}: {type(e).__name__}: {e}\n"
        )
        sys.stderr.flush()
        return int(default)


class _DummyModel:
    def __init__(self, dim: int = 16) -> None:
        self._dim = int(max(4, dim))

    def encode(
        self,
        texts: Sequence[str],
        *,
        batch_size: int = 32,
        show_progress_bar: bool = False,
        convert_to_numpy: bool = True,
        normalize_embeddings: bool = True,
        **_: Any,
    ) -> np.ndarray:
        out = np.zeros((len(list(texts or [])), self._dim), dtype=np.float32)
        for idx, text in enumerate(list(texts or [])):
            token = str(text or "")
            base = (sum(ord(ch) for ch in token) % 997) + 1
            row = np.asarray(
                [((base + (i * 17)) % 101) / 100.0 for i in range(self._dim)],
                dtype=np.float32,
            )
            if normalize_embeddings:
                norm = float(np.linalg.norm(row))
                if norm > 1e-9:
                    row = row / norm
            out[idx] = row
        return out


def _dummy_predict_event(
    vec: np.ndarray,
    symbols: Sequence[str],
    horizons: Sequence[int],
    *,
    top_k: int = 8,
    event: Dict[str, Any] | None = None,
) -> Dict[tuple[str, int], tuple[float, float, Dict[str, Any]]]:
    event_id = _safe_int((event or {}).get("event_id"), 0)
    out: Dict[tuple[str, int], tuple[float, float, Dict[str, Any]]] = {}
    for symbol in list(symbols or []):
        sym = str(symbol or "").strip().upper()
        if not sym:
            continue
        for horizon_s in list(horizons or []):
            hs = int(horizon_s or 0)
            sign = 1.0 if ((event_id + len(sym) + hs) % 2 == 0) else -1.0
            magnitude = 1.15 + (0.10 * ((event_id + hs) % 5))
            expected_z = float(sign * magnitude)
            conf = 0.88 + (0.02 * ((event_id + len(sym)) % 3))
            conf = float(min(0.97, max(0.80, conf)))
            explain = {
                "model_name": "load_probe_model",
                "model_id": "load_probe_model",
                "model_kind": "synthetic",
                "model_version": "v1",
                "model_ts_ms": int(_now_ms()),
                "feature_snapshot": {"vec_norm": float(np.linalg.norm(vec)), "top_k": int(top_k)},
            }
            out[(sym, hs)] = (float(expected_z), float(conf), explain)
    return out


def _seed_symbols_and_prices(storage, symbols: Sequence[str], *, price_ts_ms: int) -> None:
    con = storage.connect()
    try:
        now_ms = int(price_ts_ms)
        for idx, symbol in enumerate(list(symbols or [])):
            sym = str(symbol).upper().strip()
            con.execute(
                """
                INSERT INTO symbols(
                  symbol, asset_class, status, score, meta_json, created_ts_ms, updated_ts_ms
                )
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(symbol) DO UPDATE SET
                  status=excluded.status,
                  score=excluded.score,
                  updated_ts_ms=excluded.updated_ts_ms
                """,
                (
                    sym,
                    "EQUITY",
                    "ACTIVE",
                    float(100.0 - idx),
                    json.dumps({"seeded_by": "pipeline_latency_load_probe"}, separators=(",", ":"), sort_keys=True),
                    int(now_ms),
                    int(now_ms),
                ),
            )
            px = 100.0 + (idx * 7.5)
            con.execute(
                """
                INSERT INTO prices(ts_ms, symbol, price, px, source)
                VALUES (?,?,?,?,?)
                ON CONFLICT(symbol, ts_ms) DO UPDATE SET
                  price=excluded.price,
                  px=excluded.px,
                  source=excluded.source
                """,
                (int(now_ms), sym, float(px), float(px), "load_probe"),
            )
        con.commit()
    finally:
        con.close()


def _ensure_probe_aux_tables(storage) -> None:
    con = storage.connect()
    try:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS earnings_calendar(
              symbol TEXT,
              earnings_date TEXT,
              time_of_day TEXT,
              eps_est REAL,
              revenue_est REAL,
              source TEXT
            );
            CREATE TABLE IF NOT EXISTS sec_filings(
              symbol TEXT,
              form TEXT,
              filed_date TEXT,
              accession TEXT,
              primary_doc_url TEXT,
              source TEXT
            );
            CREATE TABLE IF NOT EXISTS price_quotes(
              symbol TEXT,
              last REAL,
              bid REAL,
              ask REAL,
              spread REAL,
              spread_bps REAL,
              source TEXT,
              ts_ms INTEGER
            );
            CREATE TABLE IF NOT EXISTS options_chain(
              symbol TEXT,
              iv REAL,
              open_interest REAL,
              ts_ms INTEGER
            );
            CREATE TABLE IF NOT EXISTS domain_blacklist(
              domain TEXT,
              symbol TEXT,
              status TEXT
            );
            CREATE TABLE IF NOT EXISTS domain_perf(
              domain TEXT,
              symbol TEXT,
              regime TEXT,
              horizon_s INTEGER,
              mean_edge REAL,
              win_rate REAL,
              n INTEGER
            );
            CREATE TABLE IF NOT EXISTS execution_analytics(
              symbol TEXT,
              broker TEXT,
              slippage_bps REAL,
              fill_latency_ms REAL
            );
            """
        )
        con.commit()
    finally:
        con.close()


def _insert_synthetic_events(storage, *, event_count: int, source_ts_ms: int) -> List[int]:
    event_ids: List[int] = []
    for idx in range(int(event_count)):
        ts_ms = int(source_ts_ms + idx)
        event_id = storage.put_normalized_event(
            {
                "timestamp": int(ts_ms),
                "source": "load_probe",
                "title": f"LOAD PROBE EVENT {idx}",
                "body": f"synthetic event body {idx}",
                "url": f"https://example.com/load-probe/{idx}",
                "event_key": f"load-probe-event-{ts_ms}-{idx}",
                "meta_json": {"probe_batch": int(idx // 10), "probe_seq": int(idx)},
            }
        )
        event_ids.append(int(event_id))
    return event_ids


def _synthesize_portfolio_orders(storage, *, max_orders: int, order_weight: float) -> Dict[str, Any]:
    con = storage.connect()
    try:
        rows = con.execute(
            """
            SELECT id, ts_ms, symbol, expected_z, explain_json
            FROM alerts
            ORDER BY ts_ms DESC, id DESC
            LIMIT ?
            """,
            (int(max_orders),),
        ).fetchall() or []

        now_ms = _now_ms()
        inserted = 0
        for alert_id, _alert_ts_ms, symbol, expected_z, explain_json in rows:
            sym = str(symbol or "").strip().upper()
            if not sym:
                continue
            side = "LONG" if float(expected_z or 0.0) >= 0.0 else "SHORT"
            explain_obj = {}
            try:
                explain_obj = json.loads(str(explain_json or "{}"))
            except Exception:
                explain_obj = {}
            if not isinstance(explain_obj, dict):
                explain_obj = {}
            model_id = str(explain_obj.get("model_id") or explain_obj.get("model_name") or "baseline").strip() or "baseline"
            to_weight = float(order_weight if side == "LONG" else -order_weight)
            con.execute(
                """
                INSERT INTO portfolio_orders(
                  ts_ms, model_id, symbol, action, from_side, to_side,
                  from_weight, to_weight, delta_weight, source_alert_id, explain_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    str(model_id),
                    str(sym),
                    "OPEN",
                    "FLAT",
                    str(side),
                    0.0,
                    float(to_weight),
                    float(to_weight),
                    int(alert_id),
                    json.dumps(explain_obj, separators=(",", ":"), sort_keys=True),
                ),
            )
            inserted += 1
        con.commit()
        return {"inserted_orders": int(inserted), "source_alerts": int(len(rows))}
    finally:
        con.close()


def _force_real_execution_batch(batch: Dict[str, Any] | None) -> Dict[str, Any]:
    out = dict(batch or {})
    intents = []
    for raw_intent in list(out.get("intents") or []):
        if not isinstance(raw_intent, dict):
            continue
        intent = dict(raw_intent)
        intent["execution_target"] = "real"
        competition = dict(intent.get("competition") or {})
        competition["allowed"] = True
        competition["blocked"] = False
        competition["reason"] = "load_probe_forced_real"
        if _safe_float(competition.get("capital_multiplier"), 0.0) <= 0.0:
            competition["capital_multiplier"] = 1.0
        if _safe_float(competition.get("effective_allocation_fraction"), 0.0) <= 0.0:
            competition["effective_allocation_fraction"] = 1.0
        if _safe_float(competition.get("model_weight"), 0.0) <= 0.0:
            competition["model_weight"] = 1.0
        intent["competition"] = competition
        intents.append(intent)
    out["intents"] = intents
    return out


def _latest_metric_map(storage) -> Dict[str, float]:
    con = storage.connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT metric, value_num
            FROM runtime_metrics
            WHERE metric LIKE 'pipeline_latency.%'
            ORDER BY ts_ms DESC, id DESC
            """
        ).fetchall() or []
    finally:
        con.close()

    out: Dict[str, float] = {}
    for metric, value_num in rows:
        key = str(metric or "")
        if key in out:
            continue
        out[key] = _safe_float(value_num)
    return out


def _slow_query_rows(storage) -> List[Dict[str, Any]]:
    con = storage.connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT ts_ms, value_num, tags_json
            FROM runtime_metrics
            WHERE metric='execution_intents_query_latency_ms'
            ORDER BY ts_ms DESC, id DESC
            LIMIT 25
            """
        ).fetchall() or []
    finally:
        con.close()

    out: List[Dict[str, Any]] = []
    for ts_ms, value_num, tags_json in rows:
        try:
            tags = json.loads(str(tags_json or "{}"))
        except Exception:
            tags = {}
        out.append(
            {
                "ts_ms": int(ts_ms or 0),
                "latency_ms": _safe_float(value_num),
                "tags": tags if isinstance(tags, dict) else {},
            }
        )
    return out


def _table_count(storage, table: str) -> int:
    con = storage.connect(readonly=True)
    try:
        row = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return _safe_int((row or [0])[0], 0)
    finally:
        con.close()


def _stage_metric(report: Dict[str, Any], stage_name: str, suffix: str = "p95_ms") -> float:
    return _safe_float((report.get("pipeline_metrics") or {}).get(f"pipeline_latency.{stage_name}.{suffix}"), 0.0)


def _stable(values: Sequence[float], *, max_cov: float) -> bool:
    cleaned = [float(v) for v in list(values or []) if _safe_float(v, float("nan")) == _safe_float(v, float("nan"))]
    if len(cleaned) <= 1:
        return True
    mean = statistics.fmean(cleaned)
    if abs(mean) <= 1e-9:
        return True
    cov = statistics.pstdev(cleaned) / abs(mean)
    return bool(cov <= float(max_cov))


def _processor_module_name(name: str) -> str:
    key = str(name or "").strip().lower()
    if key == "process_events_live":
        return "engine.data.jobs.process_events_live"
    return "engine.data.jobs.process_events"


def _prepare_env(temp_db_path: Path, args: argparse.Namespace) -> None:
    os.environ["DB_PATH"] = str(temp_db_path)
    os.environ["ENGINE_SUPERVISED"] = "1"
    os.environ["LOG_LEVEL"] = "WARNING"
    os.environ["PROCESS_EVENTS_TRACE_STEPS"] = "0"
    os.environ["ALERT_MAX_PER_WINDOW_GLOBAL"] = str(max(10000, int(args.events * 50)))
    os.environ["ALERT_MAX_PER_WINDOW_PER_SYMBOL"] = str(max(1000, int(args.events * 10)))
    os.environ["ALERT_COOLDOWN_WARN_S"] = "0"
    os.environ["ALERT_COOLDOWN_HIGH_S"] = "0"
    os.environ["ALERT_COOLDOWN_CRIT_S"] = "0"
    os.environ["BROKER_NAME"] = "sim"
    os.environ["BROKER_FAILOVER"] = "sim"
    os.environ["JOB_LOCK_STALE_AFTER_S"] = "120"


def _run_round(round_no: int, args: argparse.Namespace) -> Dict[str, Any]:
    symbols = [str(s).strip().upper() for s in str(args.symbols).split(",") if str(s).strip()]
    horizons = [int(x.strip()) for x in str(args.horizons).split(",") if str(x).strip()]
    with tempfile.TemporaryDirectory(prefix=f"pipeline_latency_round_{round_no}_", ignore_cleanup_errors=True) as tmpdir:
        db_path = Path(tmpdir) / "pipeline_latency.db"
        _prepare_env(db_path, args)

        (
            _db_guard,
            storage,
            validation,
            portfolio,
            execution_mode,
            state_cache,
        ) = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.validation",
            "engine.strategy.portfolio",
            "engine.execution.execution_mode",
            "engine.runtime.state_cache",
        )

        storage.init_db()
        validation.init_validation_db()
        portfolio.init_portfolio_db()
        _ensure_probe_aux_tables(storage)
        execution_mode.set_execution_mode("paper", actor="load_probe", reason=f"round_{round_no}")
        (process_module, broker_apply_orders) = _reload_modules(
            _processor_module_name(args.processor),
            "engine.execution.broker_apply_orders",
        )

        price_ts_ms = _now_ms()
        _seed_symbols_and_prices(storage, symbols, price_ts_ms=price_ts_ms)

        source_ts_ms = _now_ms()
        _insert_synthetic_events(storage, event_count=int(args.events), source_ts_ms=source_ts_ms)

        dummy_model = _DummyModel(dim=16)
        printed_payloads: List[Dict[str, Any]] = []

        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(process_module, "_get_model", return_value=dummy_model))
            stack.enter_context(patch.object(process_module, "predict_event", side_effect=_dummy_predict_event))
            stack.enter_context(patch.object(process_module, "get_active_symbols", return_value=list(symbols)))
            stack.enter_context(patch.object(process_module, "execution_allowed", side_effect=lambda *args, **kwargs: (True, None, None)))
            stack.enter_context(
                patch.object(
                    process_module,
                    "load_latest_microstructure_context",
                    side_effect=lambda *args, **kwargs: {},
                    create=True,
                )
            )
            stack.enter_context(
                patch.object(
                    process_module,
                    "apply_microstructure_confidence",
                    side_effect=lambda expected_z, base_conf, micro_ctx: (float(base_conf), {}),
                    create=True,
                )
            )
            if hasattr(process_module, "_require_fresh_prices_or_exit"):
                stack.enter_context(
                    patch.object(process_module, "_require_fresh_prices_or_exit", side_effect=lambda *args, **kwargs: None)
                )
            if hasattr(process_module, "evaluate_rules"):
                stack.enter_context(
                    patch.object(process_module, "evaluate_rules", side_effect=lambda *args, **kwargs: None)
                )
            if hasattr(process_module, "_detect_exec_cost_spike"):
                stack.enter_context(
                    patch.object(process_module, "_detect_exec_cost_spike", side_effect=lambda *args, **kwargs: {})
                )
            if hasattr(process_module, "get_current_regime"):
                stack.enter_context(patch.object(process_module, "get_current_regime", side_effect=lambda *_args, **_kwargs: "global"))
            if hasattr(process_module, "get_adjusted_confidence"):
                stack.enter_context(
                    patch.object(
                        process_module,
                        "get_adjusted_confidence",
                        side_effect=lambda conw, symbol, horizon_s, base_conf: (float(base_conf), {}),
                    )
                )
            if hasattr(process_module, "HORIZONS"):
                stack.enter_context(patch.object(process_module, "HORIZONS", list(horizons)))
            if hasattr(process_module, "DEFAULT_SYMBOLS"):
                stack.enter_context(patch.object(process_module, "DEFAULT_SYMBOLS", list(symbols)))

            process_started = time.perf_counter()
            process_module.main()
            process_duration_ms = (time.perf_counter() - process_started) * 1000.0

            synth = _synthesize_portfolio_orders(
                storage,
                max_orders=int(args.max_orders),
                order_weight=float(args.order_weight),
            )
            state_cache.cache_invalidate_namespace("portfolio_orders")
            state_cache.cache_invalidate_namespace("portfolio_snapshot")

            stack.enter_context(patch.object(broker_apply_orders, "_print", side_effect=lambda payload: printed_payloads.append(dict(payload or {}))))
            stack.enter_context(patch.object(broker_apply_orders, "evaluate_rules", side_effect=lambda: None))
            stack.enter_context(
                patch.object(
                    broker_apply_orders,
                    "execution_allowed",
                    side_effect=lambda *args, **kwargs: (True, None, None),
                )
            )
            stack.enter_context(
                patch.object(
                    broker_apply_orders,
                    "execution_gate_snapshot",
                    side_effect=lambda **kwargs: {
                        "ok": True,
                        "mode": "paper",
                        "allow_execution_pipeline": True,
                        "allow_execution": False,
                        "real_trading_allowed": False,
                        "reason": "paper_mode_allowed",
                    },
                )
            )
            stack.enter_context(
                patch.object(
                    broker_apply_orders,
                    "load_latest_execution_intents",
                    side_effect=lambda con, *a, **kw: _force_real_execution_batch(
                        importlib.import_module("engine.strategy.portfolio_execution_intents").load_latest_execution_intents(
                            con, *a, **kw
                        )
                    ),
                )
            )
            stack.enter_context(
                patch.object(
                    broker_apply_orders,
                    "_apply_epe_compat",
                    side_effect=lambda **kwargs: list(kwargs.get("raw_payload") or []),
                )
            )
            execution_started = time.perf_counter()
            broker_rc = broker_apply_orders.main()
            execution_duration_ms = (time.perf_counter() - execution_started) * 1000.0

        pipeline_metrics = _latest_metric_map(storage)
        slow_queries = _slow_query_rows(storage)
        storage_debug = storage.get_connection_debug_snapshot()

        return {
            "round": int(round_no),
            "db_path": str(db_path),
            "processor": str(args.processor),
            "events_requested": int(args.events),
            "symbols": list(symbols),
            "horizons": list(horizons),
            "process_duration_ms": round(float(process_duration_ms), 3),
            "execution_duration_ms": round(float(execution_duration_ms), 3),
            "broker_rc": int(broker_rc),
            "broker_output": list(printed_payloads),
            "portfolio_synthesis": dict(synth),
            "row_counts": {
                "events": _table_count(storage, "events"),
                "event_embeddings": _table_count(storage, "event_embeddings"),
                "predictions": _table_count(storage, "predictions"),
                "decision_log": _table_count(storage, "decision_log"),
                "alerts": _table_count(storage, "alerts"),
                "portfolio_orders": _table_count(storage, "portfolio_orders"),
                "execution_orders": _table_count(storage, "execution_orders"),
                "execution_fills": _table_count(storage, "execution_fills"),
            },
            "pipeline_metrics": dict(pipeline_metrics),
            "slow_queries": list(slow_queries),
            "storage_debug": {
                "txn_stats": dict(storage_debug.get("txn_stats") or {}),
                "wal_bytes": storage_debug.get("wal_bytes"),
                "connections": list(storage_debug.get("connections") or []),
            },
        }


def _build_summary(rounds: List[Dict[str, Any]], args: argparse.Namespace) -> Dict[str, Any]:
    stage_thresholds = {
        "ingestion_to_db": float(args.max_p95_ingestion_db_ms),
        "db_to_prediction": float(args.max_p95_db_prediction_ms),
        "prediction_to_decision": float(args.max_p95_prediction_decision_ms),
        "decision_to_execution": float(args.max_p95_decision_execution_ms),
    }

    stage_values: Dict[str, List[float]] = {k: [] for k in stage_thresholds}
    for report in list(rounds or []):
        for stage_name in stage_values:
            stage_values[stage_name].append(_stage_metric(report, stage_name, "p95_ms"))

    threshold_results = {
        stage_name: {
            "threshold_ms": float(threshold),
            "values_ms": [round(float(v), 3) for v in list(stage_values.get(stage_name) or [])],
            "max_ms": round(max(stage_values.get(stage_name) or [0.0]), 3),
            "stable": _stable(stage_values.get(stage_name) or [], max_cov=float(args.max_round_cov)),
            "within_threshold": max(stage_values.get(stage_name) or [0.0]) <= float(threshold),
        }
        for stage_name, threshold in stage_thresholds.items()
    }

    round_durations = {
        "process_duration_ms": [float(r.get("process_duration_ms") or 0.0) for r in list(rounds or [])],
        "execution_duration_ms": [float(r.get("execution_duration_ms") or 0.0) for r in list(rounds or [])],
    }

    ok = all(
        bool(item.get("stable")) and bool(item.get("within_threshold"))
        for item in threshold_results.values()
    )
    ok = ok and all(
        max(vals or [0.0]) <= limit
        for vals, limit in (
            (round_durations["process_duration_ms"], float(args.max_process_duration_ms)),
            (round_durations["execution_duration_ms"], float(args.max_execution_duration_ms)),
        )
    )

    return {
        "ok": bool(ok),
        "processor": str(args.processor),
        "rounds": int(len(rounds or [])),
        "stage_thresholds": threshold_results,
        "round_duration_thresholds": {
            "process_duration_ms": float(args.max_process_duration_ms),
            "execution_duration_ms": float(args.max_execution_duration_ms),
        },
        "round_durations": {
            name: {
                "values_ms": [round(float(v), 3) for v in vals],
                "max_ms": round(max(vals or [0.0]), 3),
                "stable": _stable(vals, max_cov=float(args.max_round_cov)),
            }
            for name, vals in round_durations.items()
        },
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Synthetic latency/load probe for the trading pipeline.")
    parser.add_argument("--processor", choices=["process_events", "process_events_live"], default="process_events_live")
    parser.add_argument("--warmup-rounds", type=int, default=1)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--events", type=int, default=40)
    parser.add_argument("--symbols", type=str, default="AAPL,MSFT,NVDA,TSLA")
    parser.add_argument("--horizons", type=str, default="300,900")
    parser.add_argument("--max-orders", type=int, default=200)
    parser.add_argument("--order-weight", type=float, default=0.02)
    parser.add_argument("--max-p95-ingestion-db-ms", type=float, default=500.0)
    parser.add_argument("--max-p95-db-prediction-ms", type=float, default=3000.0)
    parser.add_argument("--max-p95-prediction-decision-ms", type=float, default=750.0)
    parser.add_argument("--max-p95-decision-execution-ms", type=float, default=2500.0)
    parser.add_argument("--max-process-duration-ms", type=float, default=20000.0)
    parser.add_argument("--max-execution-duration-ms", type=float, default=10000.0)
    parser.add_argument("--max-round-cov", type=float, default=0.35)
    args = parser.parse_args(list(argv) if argv is not None else None)

    warmups: List[Dict[str, Any]] = []
    measured: List[Dict[str, Any]] = []

    for idx in range(int(max(0, args.warmup_rounds))):
        warmups.append(_run_round(idx + 1, args))

    for idx in range(int(max(1, args.rounds))):
        measured.append(_run_round(int(args.warmup_rounds) + idx + 1, args))

    summary = _build_summary(measured, args)
    payload = {
        "ok": bool(summary.get("ok")),
        "config": {
            "processor": str(args.processor),
            "warmup_rounds": int(args.warmup_rounds),
            "rounds": int(args.rounds),
            "events": int(args.events),
            "symbols": [str(s).strip().upper() for s in str(args.symbols).split(",") if str(s).strip()],
            "horizons": [int(x.strip()) for x in str(args.horizons).split(",") if str(x).strip()],
            "max_orders": int(args.max_orders),
            "order_weight": float(args.order_weight),
        },
        "summary": summary,
        "warmup_rounds": warmups,
        "measured_rounds": measured,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if bool(summary.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
