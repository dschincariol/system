"""Regression tests for structured failure diagnostics."""

from __future__ import annotations

import importlib
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

TRACE_MARKERS = (
    "log_failure(",
    "failure_response(",
    "_failure_out(",
    "_warn_nonfatal(",
    "_warn_nonfatal_once(",
    "_log_nonfatal(",
    "_record_nonfatal(",
    "_warn(",
    "_warn_failure(",
    "_log_swallowed(",
    "_stderr_nonfatal(",
    "return _fail(",
    "sys.stderr.write(",
    "stderr.write(",
    "__stderr__.write(",
    "print(",
    "_print(",
    "Write-Warning",
    "Write-Error",
    "log.exception(",
    "logger.exception(",
    "LOGGER.exception(",
    "LOG.exception(",
    "logging.error(",
    "logging.warning(",
    "LOG.error(",
    "LOG.warning(",
    "_record_startup_failure(",
    "traceback.print_exc(",
)

STRICT_STRUCTURED_TRACE_MARKERS = (
    "LOG.log(",
    "logging.log(",
    "log_event(",
    "log_failure(",
    "failure_response(",
    "_failure_out(",
    "_log_internal_nonfatal(",
    "_warn(",
    "_warn_nonfatal(",
    "_warn_nonfatal_once(",
    "_log_nonfatal(",
    "_record_nonfatal(",
    "_warn_failure(",
    "_log_swallowed(",
    "_record_startup_failure(",
)


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _collect_silent_except_fallbacks(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    offenders: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.lstrip()
        indent = len(line) - len(stripped)
        if stripped.startswith("except") and stripped.rstrip().endswith(":"):
            j = i + 1
            body: list[tuple[int, str]] = []
            while j < len(lines):
                body_line = lines[j]
                body_stripped = body_line.lstrip()
                if body_stripped and (len(body_line) - len(body_stripped)) <= indent:
                    break
                body.append((j + 1, body_line))
                j += 1

            seen_trace = False
            for line_no, body_line in body:
                body_stripped = body_line.lstrip()
                if not body_stripped:
                    continue
                if any(marker in body_line for marker in TRACE_MARKERS):
                    seen_trace = True
                if body_stripped.startswith(("return", "continue")) and not seen_trace:
                    rel = path.relative_to(REPO_ROOT).as_posix()
                    offenders.append(f"{rel}:{line_no}:{body_stripped}")
                    break
            i = j
            continue
        i += 1
    return offenders


def _collect_unstructured_except_fallbacks(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    offenders: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.lstrip()
        indent = len(line) - len(stripped)
        if stripped.startswith("except") and stripped.rstrip().endswith(":"):
            j = i + 1
            body: list[tuple[int, str]] = []
            while j < len(lines):
                body_line = lines[j]
                body_stripped = body_line.lstrip()
                if body_stripped and (len(body_line) - len(body_stripped)) <= indent:
                    break
                body.append((j + 1, body_line))
                j += 1

            seen_structured_trace = False
            for line_no, body_line in body:
                body_stripped = body_line.lstrip()
                if not body_stripped:
                    continue
                if any(marker in body_line for marker in STRICT_STRUCTURED_TRACE_MARKERS):
                    seen_structured_trace = True
                if body_stripped.startswith(("return", "continue")) and not seen_structured_trace:
                    rel = path.relative_to(REPO_ROOT).as_posix()
                    offenders.append(f"{rel}:{line_no}:{body_stripped}")
                    break
            i = j
            continue
        i += 1
    return offenders


class _BoomJobs:
    def start(self, _name: str):
        raise RuntimeError("start exploded")

    def stop(self, _name: str):
        raise RuntimeError("stop exploded")

    def list_jobs(self):
        raise RuntimeError("list exploded")

    def get_job_log(self, *, name: str, tail: int):
        raise RuntimeError(f"log exploded for {name}:{tail}")

    def get_job_history(self, *, name: str, limit: int):
        raise RuntimeError(f"history exploded for {name}:{limit}")


class _CaptureJobs:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def get_job_log(self, *, name: str, tail: int):
        self.calls.append((str(name), int(tail)))
        return {"ok": True, "job": str(name), "tail": int(tail), "data": []}


class FailureDiagnosticsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "failure_diag.db")
        os.environ["TRADING_LOGS"] = str(Path(self.tmp.name) / "logs")
        os.environ["TRADING_DATA"] = str(Path(self.tmp.name) / "data")
        os.environ["ENGINE_MODE"] = "safe"
        _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.runtime.failure_diagnostics",
            "engine.api.api_handlers",
            "engine.api.api_dashboard_reads",
            "engine.api.api_jobs",
            "engine.api.api_operator_handlers",
            "engine.api.http_parsing",
        )

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception as e:
            self.fail(f"storage.close_pooled_connections failed during tearDown: {type(e).__name__}: {e}")
        self.tmp.cleanup()

    def test_failure_response_includes_root_cause_code_and_snapshot(self) -> None:
        (diag,) = _reload_modules("engine.runtime.failure_diagnostics")

        out = diag.failure_response(
            None,
            event="unit_test_failure",
            code="UNIT_TEST_FAILURE",
            message="boom",
            error=RuntimeError("boom"),
            component="tests.failure",
        )

        self.assertFalse(out["ok"])
        self.assertEqual(out["root_cause_code"], "UNIT_TEST_FAILURE")
        self.assertEqual(out["failure_scope"], "unit_test_failure")
        self.assertIn("system_state_snapshot", out)
        self.assertEqual(out["system_state_snapshot"]["paths"]["db_path"], os.environ["DB_PATH"])

    def test_json_safe_compacts_large_strings_and_collections(self) -> None:
        (diag,) = _reload_modules("engine.runtime.failure_diagnostics")

        long_text = "x" * 5000
        out = diag._json_safe(
            {
                "message": long_text,
                "items": [{"detail": long_text} for _ in range(diag._JSON_SAFE_MAX_ITEMS + 5)],
            }
        )

        self.assertLessEqual(len(out["message"]), diag._JSON_SAFE_MAX_STRING)
        self.assertTrue(out["message"].endswith("...[truncated]"))
        self.assertEqual(len(out["items"]), diag._JSON_SAFE_MAX_ITEMS)
        self.assertLessEqual(len(out["items"][0]["detail"]), diag._JSON_SAFE_MAX_STRING)

    def test_capture_system_state_snapshot_compacts_large_health_and_storage_sections(self) -> None:
        _reload_modules("engine.runtime.storage", "engine.runtime.health")
        (diag,) = _reload_modules("engine.runtime.failure_diagnostics")

        long_text = "y" * 5000
        health_snapshot = {
            "ok": False,
            "reasons": [long_text for _ in range(30)],
            "providers": {
                "active_symbols": [long_text for _ in range(50)],
                "status": {"detail": long_text},
            },
        }
        debug_snapshot = {
            "db_bytes": 1,
            "wal_bytes": 2,
            "shm_bytes": 3,
            "reader_count": 4,
            "writer_count": 5,
            "long_lived_readers": list(range(30)),
            "db_validation": {"detail": long_text},
            "failure_classification": {"root_cause": long_text},
            "ingestion_state": {"stale_jobs": [long_text for _ in range(50)]},
            "supervisor_analysis": {"children": [long_text for _ in range(50)]},
            "startup_trace": {
                "phase": "boot",
                "first_failure": {"message": long_text},
                "import_errors": [long_text for _ in range(50)],
                "ts_ms": 123,
            },
            "import_smoke": {
                "ok": False,
                "failures": [long_text for _ in range(50)],
                "ts_ms": 456,
            },
        }

        with patch("engine.runtime.health.get_health_snapshot", return_value=health_snapshot), patch(
            "engine.runtime.storage.get_db_debug_snapshot",
            return_value=debug_snapshot,
        ):
            out = diag.capture_system_state_snapshot(include_health=True)

        self.assertLessEqual(len(out["health"]["reasons"]), diag._JSON_SAFE_MAX_ITEMS)
        self.assertLessEqual(
            len(out["health"]["providers"]["active_symbols"]),
            diag._JSON_SAFE_MAX_ITEMS,
        )
        self.assertLessEqual(
            len(out["health"]["providers"]["status"]["detail"]),
            diag._JSON_SAFE_MAX_STRING,
        )
        self.assertLessEqual(
            len(out["storage"]["db_validation"]["detail"]),
            diag._JSON_SAFE_MAX_STRING,
        )
        self.assertLessEqual(
            len(out["storage"]["ingestion_state"]["stale_jobs"]),
            diag._JSON_SAFE_MAX_ITEMS,
        )
        self.assertLessEqual(
            len(out["storage"]["startup_trace"]["first_failure"]["message"]),
            diag._JSON_SAFE_MAX_STRING,
        )
        self.assertLessEqual(
            len(out["storage"]["import_smoke"]["failures"]),
            diag._JSON_SAFE_MAX_ITEMS,
        )

    def test_api_job_log_failure_is_structured(self) -> None:
        (api_handlers,) = _reload_modules("engine.api.api_handlers")

        out = api_handlers.api_get_job_log(
            {"name": "ingestion_runtime", "tail": "25"},
            {"JOBS": _BoomJobs()},
        )

        self.assertFalse(out["ok"])
        self.assertEqual(out["root_cause_code"], "API_HANDLERS_JOB_LOG_FAILED")
        self.assertEqual(out["job"], "ingestion_runtime")
        self.assertIn("system_state_snapshot", out)
        self.assertIn("failure_scope", out)

    def test_api_job_log_clamps_tail_to_overload_limit(self) -> None:
        (api_handlers,) = _reload_modules("engine.api.api_handlers")
        jobs = _CaptureJobs()

        out = api_handlers.api_get_job_log(
            {"name": "ingestion_runtime", "tail": "999999"},
            {"JOBS": jobs},
        )

        self.assertTrue(out["ok"])
        self.assertEqual(out["tail"], 4000)
        self.assertEqual(jobs.calls, [("ingestion_runtime", 4000)])

    def test_dashboard_read_failure_is_structured(self) -> None:
        (api_dashboard_reads,) = _reload_modules("engine.api.api_dashboard_reads")

        with patch.object(api_dashboard_reads, "get_temporal_models", side_effect=RuntimeError("temporal boom")):
            out = api_dashboard_reads.api_get_temporal_models({"limit": "5"}, None)

        self.assertFalse(out["ok"])
        self.assertEqual(out["root_cause_code"], "API_DASHBOARD_TEMPORAL_MODELS_FAILED")
        self.assertIn("system_state_snapshot", out)
        self.assertEqual(out["failure_type"], "RuntimeError")

    def test_api_job_start_failure_is_structured(self) -> None:
        (api_jobs,) = _reload_modules("engine.api.api_jobs")

        out = api_jobs.api_post_job_start({"name": "poll_prices"}, None, {"JOBS": _BoomJobs()})

        self.assertFalse(out["ok"])
        self.assertEqual(out["root_cause_code"], "API_JOBS_START_FAILED")
        self.assertEqual(out["job"], "poll_prices")
        self.assertIn("system_state_snapshot", out)

    def test_operator_preflight_failure_is_structured(self) -> None:
        (api_operator_handlers,) = _reload_modules("engine.api.api_operator_handlers")

        out = api_operator_handlers.api_get_operator_preflight(
            None,
            {"_operator_preflight_steps": lambda: (_ for _ in ()).throw(RuntimeError("preflight exploded"))},
        )

        self.assertFalse(out["ok"])
        self.assertEqual(out["root_cause_code"], "API_OPERATOR_HANDLERS_PREFLIGHT_FAILED")
        self.assertIn("system_state_snapshot", out)

    def test_deny_if_shutdown_logs_and_degrades_open(self) -> None:
        (http_parsing,) = _reload_modules("engine.api.http_parsing")

        with patch("engine.api.http_parsing.log_failure") as mock_log_failure:
            with patch("engine.runtime.lifecycle.lifecycle_snapshot", side_effect=RuntimeError("lifecycle exploded")):
                out = http_parsing.deny_if_shutdown()

        self.assertIsNone(out)
        self.assertTrue(mock_log_failure.called)

    def test_control_plane_files_do_not_use_silent_except_pass(self) -> None:
        silent_pass = re.compile(
            r"except(?:\s+[A-Za-z_][A-Za-z0-9_\.]*(?:\s+as\s+[A-Za-z_][A-Za-z0-9_]*)?)?:\r?\n(?:\s*\r?\n)*\s+pass"
        )
        files = [
            REPO_ROOT / "dashboard_server.py",
            REPO_ROOT / "engine" / "api" / "api_handlers.py",
            REPO_ROOT / "engine" / "api" / "api_dashboard_reads.py",
            REPO_ROOT / "engine" / "api" / "api_governance.py",
            REPO_ROOT / "engine" / "api" / "api_jobs.py",
            REPO_ROOT / "engine" / "api" / "api_market.py",
            REPO_ROOT / "engine" / "api" / "api_operator_handlers.py",
            REPO_ROOT / "engine" / "api" / "api_ops_handlers.py",
            REPO_ROOT / "engine" / "api" / "api_read.py",
            REPO_ROOT / "engine" / "api" / "api_system.py",
            REPO_ROOT / "engine" / "api" / "http_parsing.py",
            REPO_ROOT / "engine" / "api" / "http_transport.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "process_events.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "process_events_enriched.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "process_events_live.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "process_events_shadow.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "backfill_labels_price_from_prices.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "gdelt_poll.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "ingest_options.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "poll_social_stocktwits.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "poll_weather_alerts.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "poll_weather_forecasts.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "snapshot_model_features.py",
            REPO_ROOT / "engine" / "data" / "monitor_calibration_health.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "update_universe.py",
            REPO_ROOT / "engine" / "data" / "asset_map.py",
            REPO_ROOT / "engine" / "data" / "factor_ingestion.py",
            REPO_ROOT / "engine" / "data" / "gdelt_macro.py",
            REPO_ROOT / "engine" / "data" / "weather_features.py",
            REPO_ROOT / "engine" / "data" / "poll_prices.py",
            REPO_ROOT / "engine" / "data" / "default_symbols.py",
            REPO_ROOT / "engine" / "data" / "ingest" / "gdelt_ingest.py",
            REPO_ROOT / "engine" / "data" / "ingest" / "rss_ingest.py",
            REPO_ROOT / "engine" / "data" / "live_prices" / "ibkr_live.py",
            REPO_ROOT / "engine" / "data" / "live_prices" / "polygon_live.py",
            REPO_ROOT / "engine" / "data" / "live_prices" / "yfinance_live.py",
            REPO_ROOT / "engine" / "data" / "live_prices" / "provider.py",
            REPO_ROOT / "engine" / "data" / "options" / "tradier_live.py",
            REPO_ROOT / "engine" / "data" / "provider_registry.py",
            REPO_ROOT / "engine" / "data" / "sec" / "edgar_live.py",
            REPO_ROOT / "engine" / "data" / "universe_discovery.py",
            REPO_ROOT / "engine" / "data" / "provider_sessions" / "ibkr_session.py",
            REPO_ROOT / "engine" / "data" / "provider_sessions" / "polygon_ws_session.py",
            REPO_ROOT / "engine" / "data" / "provider_sessions" / "session_manager.py",
            REPO_ROOT / "engine" / "data" / "providers" / "ibkr" / "daemon_stream.py",
            REPO_ROOT / "engine" / "execution" / "broker_apply_orders.py",
            REPO_ROOT / "engine" / "execution" / "broker_fill_utils.py",
            REPO_ROOT / "engine" / "execution" / "broker_alpaca_rest.py",
            REPO_ROOT / "engine" / "execution" / "broker_router.py",
            REPO_ROOT / "engine" / "execution" / "execution_broker_watchdog.py",
            REPO_ROOT / "engine" / "execution" / "execution_ai_advisor.py",
            REPO_ROOT / "engine" / "execution" / "broker_ibkr_gateway.py",
            REPO_ROOT / "engine" / "execution" / "broker_sim.py",
            REPO_ROOT / "engine" / "execution" / "dual_execution.py",
            REPO_ROOT / "engine" / "execution" / "execution_analytics_engine.py",
            REPO_ROOT / "engine" / "execution" / "execution_liquidity_model.py",
            REPO_ROOT / "engine" / "execution" / "execution_mode.py",
            REPO_ROOT / "engine" / "execution" / "execution_poll_and_attrib.py",
            REPO_ROOT / "engine" / "execution" / "execution_microstructure.py",
            REPO_ROOT / "engine" / "execution" / "execution_policy_engine.py",
            REPO_ROOT / "engine" / "execution" / "execution_slicing_engine.py",
            REPO_ROOT / "engine" / "execution" / "position_reconcile.py",
            REPO_ROOT / "engine" / "execution" / "trade_attribution_ledger.py",
            REPO_ROOT / "engine" / "execution" / "trade_suppression_engine.py",
            REPO_ROOT / "engine" / "execution" / "train_drawdown_policy.py",
            REPO_ROOT / "engine" / "execution" / "jobs" / "repair_trade_attribution_history.py",
            REPO_ROOT / "engine" / "execution" / "kill_switch.py",
            REPO_ROOT / "engine" / "jobs" / "stream_prices_polygon_ws.py",
            REPO_ROOT / "engine" / "execution" / "execution_ledger.py",
            REPO_ROOT / "engine" / "execution" / "execution_open_order_manager.py",
            REPO_ROOT / "engine" / "risk" / "portfolio_risk_engine.py",
            REPO_ROOT / "engine" / "runtime" / "failure_diagnostics.py",
            REPO_ROOT / "engine" / "runtime" / "health.py",
            REPO_ROOT / "engine" / "runtime" / "ingestion_runtime.py",
            REPO_ROOT / "engine" / "runtime" / "jobs_manager.py",
            REPO_ROOT / "engine" / "runtime" / "jobs" / "provider_monitor_job.py",
            REPO_ROOT / "engine" / "runtime" / "jobs" / "metrics_collector.py",
            REPO_ROOT / "engine" / "runtime" / "db_guard.py",
            REPO_ROOT / "engine" / "runtime" / "dashboard_weather_widgets.py",
            REPO_ROOT / "engine" / "runtime" / "event_replay.py",
            REPO_ROOT / "engine" / "runtime" / "factor_universe.py",
            REPO_ROOT / "engine" / "runtime" / "logging.py",
            REPO_ROOT / "engine" / "runtime" / "locks.py",
            REPO_ROOT / "engine" / "runtime" / "lifecycle_state.py",
            REPO_ROOT / "engine" / "runtime" / "orchestrator.py",
            REPO_ROOT / "engine" / "runtime" / "position_store.py",
            REPO_ROOT / "engine" / "runtime" / "runtime_bootstrap.py",
            REPO_ROOT / "engine" / "runtime" / "metrics_store.py",
            REPO_ROOT / "engine" / "runtime" / "alpha_decay_monitor.py",
            REPO_ROOT / "engine" / "runtime" / "cache_warm.py",
            REPO_ROOT / "engine" / "runtime" / "first_run.py",
            REPO_ROOT / "engine" / "runtime" / "hierarchical_allocator.py",
            REPO_ROOT / "engine" / "runtime" / "alerts.py",
            REPO_ROOT / "engine" / "runtime" / "alerts_notify.py",
            REPO_ROOT / "engine" / "runtime" / "allocator_status.py",
            REPO_ROOT / "engine" / "runtime" / "crash_recovery.py",
            REPO_ROOT / "engine" / "runtime" / "shadow_capital_allocator.py",
            REPO_ROOT / "engine" / "runtime" / "startup_orchestrator.py",
            REPO_ROOT / "engine" / "runtime" / "storage.py",
            REPO_ROOT / "engine" / "runtime" / "strategy_allocator.py",
            REPO_ROOT / "engine" / "runtime" / "supervisor.py",
            REPO_ROOT / "engine" / "runtime" / "system_state.py",
            REPO_ROOT / "engine" / "runtime" / "event_bus.py",
            REPO_ROOT / "engine" / "runtime" / "event_log.py",
            REPO_ROOT / "engine" / "runtime" / "ingestion_status.py",
            REPO_ROOT / "engine" / "runtime" / "ipc.py",
            REPO_ROOT / "engine" / "runtime" / "price_router.py",
            REPO_ROOT / "engine" / "runtime" / "prod_selftest.py",
            REPO_ROOT / "engine" / "runtime" / "risk_state.py",
            REPO_ROOT / "engine" / "runtime" / "runtime_meta.py",
            REPO_ROOT / "engine" / "runtime" / "trade_lifecycle.py",
            REPO_ROOT / "engine" / "runtime" / "tracing.py",
            REPO_ROOT / "engine" / "runtime" / "jobs" / "post_promotion_monitor.py",
            REPO_ROOT / "engine" / "strategy" / "capital_guard.py",
            REPO_ROOT / "engine" / "strategy" / "champion_manager.py",
            REPO_ROOT / "engine" / "strategy" / "alpha_lifecycle_engine.py",
            REPO_ROOT / "engine" / "strategy" / "compute_model_weather_effect.py",
            REPO_ROOT / "engine" / "strategy" / "compute_social_regime.py",
            REPO_ROOT / "engine" / "strategy" / "confidence_engine.py",
            REPO_ROOT / "engine" / "strategy" / "corr_opt.py",
            REPO_ROOT / "engine" / "strategy" / "model_marketplace.py",
            REPO_ROOT / "engine" / "strategy" / "model_governance_ext.py",
            REPO_ROOT / "engine" / "strategy" / "distribution_drift.py",
            REPO_ROOT / "engine" / "strategy" / "evaluate_strategies.py",
            REPO_ROOT / "engine" / "strategy" / "embed_regressor.py",
            REPO_ROOT / "engine" / "strategy" / "eval_temporal_shadow.py",
            REPO_ROOT / "engine" / "strategy" / "feature_registry.py",
            REPO_ROOT / "engine" / "strategy" / "learning_loop.py",
            REPO_ROOT / "engine" / "strategy" / "model_feature_snapshots.py",
            REPO_ROOT / "engine" / "strategy" / "market_stress.py",
            REPO_ROOT / "engine" / "strategy" / "model_lifecycle.py",
            REPO_ROOT / "engine" / "strategy" / "model_v2.py",
            REPO_ROOT / "engine" / "strategy" / "promotion_hardening.py",
            REPO_ROOT / "engine" / "strategy" / "news_domain.py",
            REPO_ROOT / "engine" / "strategy" / "options_context.py",
            REPO_ROOT / "engine" / "strategy" / "portfolio.py",
            REPO_ROOT / "engine" / "strategy" / "portfolio_backtest.py",
            REPO_ROOT / "engine" / "strategy" / "pnl_decomposition_engine.py",
            REPO_ROOT / "engine" / "strategy" / "portfolio_rebalance.py",
            REPO_ROOT / "engine" / "strategy" / "portfolio_execution_intents.py",
            REPO_ROOT / "engine" / "strategy" / "portfolio_risk_gate.py",
            REPO_ROOT / "engine" / "strategy" / "position_sizing.py",
            REPO_ROOT / "engine" / "strategy" / "predictor.py",
            REPO_ROOT / "engine" / "strategy" / "promote_temporal_models.py",
            REPO_ROOT / "engine" / "strategy" / "promotion_guard.py",
            REPO_ROOT / "engine" / "strategy" / "regime_size.py",
            REPO_ROOT / "engine" / "strategy" / "regime_stack.py",
            REPO_ROOT / "engine" / "strategy" / "rules_engine.py",
            REPO_ROOT / "engine" / "strategy" / "shadow.py",
            REPO_ROOT / "engine" / "strategy" / "shadow_trainer.py",
            REPO_ROOT / "engine" / "strategy" / "social_context.py",
            REPO_ROOT / "engine" / "strategy" / "social_regime.py",
            REPO_ROOT / "engine" / "strategy" / "tech_indicators.py",
            REPO_ROOT / "engine" / "strategy" / "temporal_predictor.py",
            REPO_ROOT / "engine" / "strategy" / "train_embed_models.py",
            REPO_ROOT / "engine" / "strategy" / "train_temporal_predictor.py",
            REPO_ROOT / "engine" / "strategy" / "universe_selector.py",
            REPO_ROOT / "engine" / "strategy" / "jobs" / "calibrate_confidence_from_prices.py",
            REPO_ROOT / "engine" / "strategy" / "jobs" / "confidence_calibration",
            REPO_ROOT / "engine" / "strategy" / "kill_health_monitor.py",
            REPO_ROOT / "engine" / "strategy" / "kill_drift_monitor.py",
            REPO_ROOT / "engine" / "runtime" / "jobs" / "repair_schema.py",
            REPO_ROOT / "engine" / "strategy" / "kill_slippage_monitor.py",
            REPO_ROOT / "engine" / "strategy" / "jobs" / "universe_discovery_job.py",
            REPO_ROOT / "engine" / "terminal" / "api" / "api_terminal.py",
            REPO_ROOT / "ops" / "alerts_service.py",
            REPO_ROOT / "ops" / "backtest_walk_forward.py",
            REPO_ROOT / "ops" / "compute_drift.py",
            REPO_ROOT / "ops" / "compute_gdelt_macro.py",
            REPO_ROOT / "ops" / "compute_exec_labels_from_fills.py",
            REPO_ROOT / "ops" / "compute_factor_features.py",
            REPO_ROOT / "ops" / "compute_factor_group_scores.py",
            REPO_ROOT / "ops" / "train_model_v2.py",
            REPO_ROOT / "services" / "data_source_manager.py",
            REPO_ROOT / "scripts" / "staging_e2e.ps1",
            REPO_ROOT / "start_all.py",
            REPO_ROOT / "start_ingestion.py",
            REPO_ROOT / "start_system.py",
            REPO_ROOT / "tests" / "test_audit_invariants.py",
            REPO_ROOT / "tests" / "test_ingestion_runtime_reliability.py",
            REPO_ROOT / "tests" / "test_model_activation.py",
            REPO_ROOT / "tests" / "test_model_competition_real_pnl.py",
            REPO_ROOT / "tests" / "test_non_production_model_barriers.py",
            REPO_ROOT / "tests" / "test_startup_health_validation.py",
        ]
        offenders = []
        for path in files:
            text = path.read_text(encoding="utf-8")
            if silent_pass.search(text):
                offenders.append(str(path))
        self.assertEqual(offenders, [])

    def test_portfolio_strategy_files_do_not_use_silent_except_return_or_continue(self) -> None:
        files = [
            REPO_ROOT / "start_system.py",
            REPO_ROOT / "engine" / "api" / "api_ops_handlers.py",
            REPO_ROOT / "engine" / "api" / "api_dashboard_reads.py",
            REPO_ROOT / "engine" / "api" / "api_system.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "process_events.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "update_universe.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "poll_social_stocktwits.py",
            REPO_ROOT / "engine" / "data" / "ingest" / "news_enrichment.py",
            REPO_ROOT / "engine" / "execution" / "broker_apply_orders.py",
            REPO_ROOT / "engine" / "execution" / "broker_ibkr_gateway.py",
            REPO_ROOT / "engine" / "execution" / "broker_router.py",
            REPO_ROOT / "engine" / "execution" / "execution_ai_advisor.py",
            REPO_ROOT / "engine" / "execution" / "execution_analytics_engine.py",
            REPO_ROOT / "engine" / "execution" / "execution_liquidity_model.py",
            REPO_ROOT / "engine" / "execution" / "execution_quality_supervisor.py",
            REPO_ROOT / "engine" / "execution" / "kill_switch.py",
            REPO_ROOT / "engine" / "execution" / "broker_sim.py",
            REPO_ROOT / "engine" / "execution" / "position_reconcile.py",
            REPO_ROOT / "engine" / "execution" / "trade_suppression_engine.py",
            REPO_ROOT / "engine" / "execution" / "execution_open_order_manager.py",
            REPO_ROOT / "engine" / "execution" / "broker_alpaca_rest.py",
            REPO_ROOT / "engine" / "execution" / "broker_fill_utils.py",
            REPO_ROOT / "engine" / "execution" / "execution_decision_engine.py",
            REPO_ROOT / "engine" / "execution" / "exec_stats.py",
            REPO_ROOT / "engine" / "execution" / "order_idempotency.py",
            REPO_ROOT / "engine" / "execution" / "train_size_policy.py",
            REPO_ROOT / "engine" / "risk" / "portfolio_risk_engine.py",
            REPO_ROOT / "engine" / "runtime" / "crash_recovery.py",
            REPO_ROOT / "engine" / "runtime" / "alpha_decay_monitor.py",
            REPO_ROOT / "engine" / "runtime" / "health.py",
            REPO_ROOT / "engine" / "runtime" / "ingestion_runtime.py",
            REPO_ROOT / "engine" / "runtime" / "jobs_manager.py",
            REPO_ROOT / "engine" / "runtime" / "event_replay.py",
            REPO_ROOT / "engine" / "runtime" / "gates.py",
            REPO_ROOT / "engine" / "runtime" / "global_risk_envelope.py",
            REPO_ROOT / "engine" / "runtime" / "job_registry.py",
            REPO_ROOT / "engine" / "runtime" / "prod_preflight.py",
            REPO_ROOT / "engine" / "runtime" / "position_store.py",
            REPO_ROOT / "engine" / "runtime" / "storage.py",
            REPO_ROOT / "engine" / "runtime" / "strategy_allocator.py",
            REPO_ROOT / "engine" / "runtime" / "supervisor.py",
            REPO_ROOT / "engine" / "runtime" / "jobs" / "post_promotion_monitor.py",
            REPO_ROOT / "dashboard_server.py",
            REPO_ROOT / "engine" / "data" / "factor_ingestion.py",
            REPO_ROOT / "engine" / "data" / "monitor_calibration_health.py",
            REPO_ROOT / "engine" / "data" / "options" / "tradier_live.py",
            REPO_ROOT / "engine" / "jobs" / "stream_prices_polygon_ws.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "process_events_enriched.py",
            REPO_ROOT / "engine" / "api" / "api_governance.py",
            REPO_ROOT / "engine" / "api" / "api_jobs.py",
            REPO_ROOT / "engine" / "api" / "api_read.py",
            REPO_ROOT / "engine" / "api" / "http_transport.py",
            REPO_ROOT / "engine" / "api" / "api_read_advanced.py",
            REPO_ROOT / "engine" / "strategy" / "portfolio.py",
            REPO_ROOT / "engine" / "strategy" / "portfolio_backtest.py",
            REPO_ROOT / "engine" / "strategy" / "portfolio_execution_intents.py",
            REPO_ROOT / "engine" / "strategy" / "pipeline_train_and_eval.py",
            REPO_ROOT / "engine" / "strategy" / "predictor.py",
            REPO_ROOT / "engine" / "strategy" / "challenger_runtime.py",
            REPO_ROOT / "engine" / "strategy" / "compute_model_weather_effect.py",
            REPO_ROOT / "engine" / "strategy" / "confidence_adjust.py",
            REPO_ROOT / "engine" / "strategy" / "confidence_engine.py",
            REPO_ROOT / "engine" / "strategy" / "embed_regressor.py",
            REPO_ROOT / "engine" / "strategy" / "evaluate_strategies.py",
            REPO_ROOT / "engine" / "strategy" / "learning.py",
            REPO_ROOT / "engine" / "strategy" / "learning_loop.py",
            REPO_ROOT / "engine" / "strategy" / "microstructure_signals.py",
            REPO_ROOT / "engine" / "strategy" / "model_governance_ext.py",
            REPO_ROOT / "engine" / "strategy" / "validation.py",
            REPO_ROOT / "engine" / "strategy" / "distribution_drift.py",
            REPO_ROOT / "ops" / "calibrate_price_confidence.py",
            REPO_ROOT / "ops" / "compute_exec_labels_from_fills.py",
            REPO_ROOT / "services" / "data_source_manager.py",
        ]
        offenders = []
        for path in files:
            offenders.extend(_collect_silent_except_fallbacks(path))
        self.assertEqual(offenders, [])

    def test_runtime_core_files_use_structured_failure_diagnostics(self) -> None:
        files = [
            REPO_ROOT / "engine" / "runtime" / "alerts.py",
            REPO_ROOT / "engine" / "runtime" / "allocator_status.py",
            REPO_ROOT / "engine" / "runtime" / "config.py",
            REPO_ROOT / "engine" / "runtime" / "crash_recovery.py",
            REPO_ROOT / "engine" / "runtime" / "execution_barrier.py",
            REPO_ROOT / "engine" / "runtime" / "event_log.py",
            REPO_ROOT / "engine" / "runtime" / "failure_diagnostics.py",
            REPO_ROOT / "engine" / "runtime" / "guards.py",
            REPO_ROOT / "engine" / "runtime" / "job_registry.py",
            REPO_ROOT / "engine" / "runtime" / "gates.py",
            REPO_ROOT / "engine" / "runtime" / "global_risk_envelope.py",
            REPO_ROOT / "engine" / "runtime" / "health.py",
            REPO_ROOT / "engine" / "runtime" / "ipc.py",
            REPO_ROOT / "engine" / "runtime" / "lifecycle.py",
            REPO_ROOT / "engine" / "runtime" / "metrics.py",
            REPO_ROOT / "engine" / "runtime" / "price_router.py",
            REPO_ROOT / "engine" / "runtime" / "runtime_bootstrap.py",
            REPO_ROOT / "engine" / "runtime" / "db_repair.py",
            REPO_ROOT / "engine" / "runtime" / "alerts_notify.py",
        ]
        offenders = []
        forbidden_markers = ("sys.stderr.write(", "stderr.write(", "logging.warning(", "LOG.warning(", "log.warning(")
        forbidden_usage = []
        for path in files:
            offenders.extend(_collect_unstructured_except_fallbacks(path))
            text = path.read_text(encoding="utf-8")
            if any(marker in text for marker in forbidden_markers):
                forbidden_usage.append(path.relative_to(REPO_ROOT).as_posix())
        self.assertEqual(offenders, [])
        self.assertEqual(forbidden_usage, [])

    def test_selected_production_files_use_structured_failure_diagnostics(self) -> None:
        files = [
            REPO_ROOT / "engine" / "api" / "api_relevance.py",
            REPO_ROOT / "engine" / "api" / "api_write.py",
            REPO_ROOT / "engine" / "data" / "asset_map.py",
            REPO_ROOT / "engine" / "data" / "default_symbols.py",
            REPO_ROOT / "engine" / "data" / "event_normalization.py",
            REPO_ROOT / "engine" / "data" / "options_features.py",
            REPO_ROOT / "engine" / "data" / "universe.py",
            REPO_ROOT / "engine" / "data" / "weather_mapping.py",
            REPO_ROOT / "engine" / "data" / "ingest" / "rss_ingest.py",
            REPO_ROOT / "engine" / "data" / "ingest" / "gdelt_ingest.py",
            REPO_ROOT / "engine" / "data" / "ingest" / "news_enrichment.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "poll_social_reddit.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "poll_weather_alerts.py",
            REPO_ROOT / "engine" / "data" / "jobs" / "poll_weather_forecasts.py",
            REPO_ROOT / "engine" / "data" / "live_prices" / "ccxt_live.py",
            REPO_ROOT / "engine" / "data" / "options_poll.py",
            REPO_ROOT / "engine" / "data" / "provider_sessions" / "session_manager.py",
            REPO_ROOT / "engine" / "execution" / "broker_fill_utils.py",
            REPO_ROOT / "engine" / "execution" / "execution_decision_engine.py",
            REPO_ROOT / "engine" / "execution" / "exec_stats.py",
            REPO_ROOT / "engine" / "execution" / "order_idempotency.py",
            REPO_ROOT / "engine" / "execution" / "train_size_policy.py",
            REPO_ROOT / "engine" / "strategy" / "allocation_risk_overlay.py",
            REPO_ROOT / "engine" / "strategy" / "challenger_runtime.py",
            REPO_ROOT / "engine" / "strategy" / "confidence_adjust.py",
            REPO_ROOT / "engine" / "strategy" / "decision_log.py",
            REPO_ROOT / "engine" / "strategy" / "drawdown_state.py",
            REPO_ROOT / "engine" / "strategy" / "drift.py",
            REPO_ROOT / "engine" / "strategy" / "drift_utils.py",
            REPO_ROOT / "engine" / "strategy" / "edge_filter.py",
            REPO_ROOT / "engine" / "strategy" / "learning.py",
            REPO_ROOT / "engine" / "strategy" / "microstructure_signals.py",
            REPO_ROOT / "engine" / "strategy" / "model_intent.py",
            REPO_ROOT / "engine" / "strategy" / "options_surface_intelligence.py",
            REPO_ROOT / "engine" / "strategy" / "promotion_guard.py",
            REPO_ROOT / "engine" / "strategy" / "relevance.py",
            REPO_ROOT / "engine" / "strategy" / "strategy_selector.py",
            REPO_ROOT / "engine" / "strategy" / "validation.py",
        ]
        offenders = []
        forbidden_markers = ("sys.stderr.write(", "stderr.write(", "logging.warning(", "LOG.warning(", "log.warning(")
        forbidden_usage = []
        for path in files:
            offenders.extend(_collect_unstructured_except_fallbacks(path))
            text = path.read_text(encoding="utf-8")
            if any(marker in text for marker in forbidden_markers):
                forbidden_usage.append(path.relative_to(REPO_ROOT).as_posix())
        self.assertEqual(offenders, [])
        self.assertEqual(forbidden_usage, [])

    def test_engine_production_tree_avoids_raw_failure_traces(self) -> None:
        roots = [
            REPO_ROOT / "engine" / "api",
            REPO_ROOT / "engine" / "execution",
            REPO_ROOT / "engine" / "data",
            REPO_ROOT / "engine" / "strategy",
        ]
        forbidden_markers = ("sys.stderr.write(", "stderr.write(", "logging.warning(", "LOG.warning(", "log.warning(")
        offenders = []
        for root in roots:
            for path in root.rglob("*"):
                if not path.is_file() or path.name == "__pycache__":
                    continue
                if path.suffix not in ("", ".py"):
                    continue
                text = path.read_text(encoding="utf-8")
                if any(marker in text for marker in forbidden_markers):
                    offenders.append(path.relative_to(REPO_ROOT).as_posix())
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
