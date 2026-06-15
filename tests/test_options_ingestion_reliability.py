"""Regression tests for hardened options ingestion reliability paths."""

from __future__ import annotations

import importlib
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _warn_cleanup_issue(scope: str, error: BaseException) -> None:
    sys.stderr.write(f"[{scope}] {type(error).__name__}: {error}\n")
    sys.stderr.flush()


class _FakeResponse:
    def __init__(self, status_code: int, payload=None, *, headers=None, text: str = "") -> None:
        self.status_code = int(status_code)
        self._payload = payload
        self.headers = dict(headers or {})
        self.text = str(text)

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses) -> None:
        self._responses = list(responses)

    def get(self, *_args, **_kwargs):
        if not self._responses:
            raise AssertionError("unexpected extra request")
        return self._responses.pop(0)

    def close(self) -> None:
        return None


class OptionsIngestionReliabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "options_reliability.db"
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["ENGINE_SUPERVISED"] = "1"
        os.environ["TRADIER_API_TOKEN"] = "token"
        os.environ["OPTIONS_PROVIDER_CHAIN"] = "tradier"
        os.environ["OPTIONS_CRITICAL_SYMBOLS"] = "SPY"
        os.environ["OPTIONS_SYMBOL_FAILURE_THRESHOLD"] = "2"
        os.environ["OPTIONS_SYMBOL_DISABLE_S"] = "300"
        os.environ["OPTIONS_CACHE_MAX_AGE_S"] = "3600"

        _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
        )

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception as e:
            _warn_cleanup_issue("test_options_ingestion_reliability.close_pooled_connections", e)
        try:
            self.tmp.cleanup()
        except PermissionError as e:
            _warn_cleanup_issue("test_options_ingestion_reliability.tempdir_cleanup", e)

    def test_tradier_retries_rate_limit_and_validates_payload(self) -> None:
        (tradier_live,) = _reload_modules("engine.data.options.tradier_live")
        session = _FakeSession(
            [
                _FakeResponse(429, {}, headers={"Retry-After": "0"}),
                _FakeResponse(200, {"expirations": {"date": ["2026-04-17"]}}),
                _FakeResponse(
                    200,
                    {
                        "options": {
                            "option": [
                                {
                                    "strike": "500",
                                    "option_type": "call",
                                    "implied_volatility": "0.2",
                                    "open_interest": "10",
                                    "volume": "5",
                                }
                            ]
                        }
                    },
                ),
            ]
        )

        with patch("engine.data.options.tradier_live.time.sleep", return_value=None):
            result = tradier_live.fetch_options_chain("SPY", session=session)

        self.assertEqual(result["rows"][0]["call_put"], "C")
        self.assertEqual(float(result["rows"][0]["strike"]), 500.0)

    def test_run_once_uses_cached_snapshot_and_then_disables_symbol(self) -> None:
        storage, options_poll = _reload_modules(
            "engine.runtime.storage",
            "engine.data.options_poll",
        )
        storage.init_db()

        cached_ts_ms = int(time.time() * 1000) - 60_000
        con = storage.connect()
        try:
            con.execute(
                """
                INSERT INTO options_chain(
                  ts_ms, symbol, expiry, strike, call_put, iv, open_interest, volume, source
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (int(cached_ts_ms), "SPY", "2026-04-17", 500.0, "C", 0.2, 10, 5, "tradier"),
            )
            con.commit()
        finally:
            con.close()

        with patch("engine.data.options_poll.get_active_symbols", return_value=["SPY"]):
            with patch(
                "engine.data.options_poll.fetch_options_chain",
                side_effect=options_poll.TradierFetchError("rate_limited", kind="rate_limit"),
            ):
                first = options_poll._run_once(["tradier"])
                second = options_poll._run_once(["tradier"])

        self.assertEqual(first["meta"]["cached_symbols"], ["SPY"])
        self.assertEqual(int(first["last_ingested_ts_ms"]), int(cached_ts_ms))
        self.assertEqual(second["meta"]["cached_symbols"], ["SPY"])

        con = storage.connect(readonly=True)
        try:
            state = con.execute(
                """
                SELECT consecutive_failures, disabled_until_ts_ms
                FROM options_symbol_ingestion_state
                WHERE symbol='SPY'
                """
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(state)
        self.assertEqual(int(state[0]), 2)
        self.assertGreater(int(state[1]), int(time.time() * 1000))

        with patch("engine.data.options_poll.get_active_symbols", return_value=["SPY"]):
            with patch("engine.data.options_poll.fetch_options_chain", side_effect=AssertionError("should not fetch disabled symbol")):
                third = options_poll._run_once(["tradier"])

        self.assertEqual(third["meta"]["cached_symbols"], ["SPY"])
        self.assertEqual(third["meta"]["symbol_status"]["SPY"]["status"], "disabled_cached")

    def test_run_once_short_circuits_provider_config_errors_across_symbols(self) -> None:
        storage, options_poll = _reload_modules(
            "engine.runtime.storage",
            "engine.data.options_poll",
        )
        storage.init_db()

        with patch("engine.data.options_poll.get_active_symbols", return_value=["SPY", "QQQ", "IWM"]):
            with patch(
                "engine.data.options_poll.fetch_options_chain",
                side_effect=options_poll.TradierFetchError("tradier_api_token_missing", kind="config_error"),
            ) as fetch_mock:
                result = options_poll._run_once(["tradier"])

        self.assertEqual(fetch_mock.call_count, 1)
        self.assertEqual(int(result["meta"]["symbols_attempted"]), 3)
        self.assertEqual(int(result["meta"]["symbols_succeeded"]), 0)
        self.assertEqual(int(result["meta"]["symbols_failed"]), 3)
        self.assertFalse(bool(result["pipeline_ok"]))
        self.assertEqual(int(result["provider_status"]["tradier"]["failed_symbols"]), 3)

    def test_run_once_suppresses_per_symbol_fetch_failed_after_provider_disable(self) -> None:
        storage, options_poll = _reload_modules(
            "engine.runtime.storage",
            "engine.data.options_poll",
        )
        storage.init_db()

        with patch("engine.data.options_poll.get_active_symbols", return_value=["SPY", "QQQ", "IWM"]):
            with patch(
                "engine.data.options_poll.fetch_options_chain",
                side_effect=options_poll.TradierFetchError("tradier_api_token_missing", kind="config_error"),
            ):
                with patch("engine.data.options_poll._warn_state") as warn_state:
                    options_poll._run_once(["tradier"])

        codes = [str(call.args[0]) for call in warn_state.call_args_list if call.args]
        self.assertEqual(codes.count("OPTIONS_POLL_PROVIDER_DISABLED"), 1)
        self.assertNotIn("OPTIONS_POLL_FETCH_FAILED", codes)

    def test_run_once_survives_symbol_failure_state_lock(self) -> None:
        storage, options_poll = _reload_modules(
            "engine.runtime.storage",
            "engine.data.options_poll",
        )
        storage.init_db()

        with patch("engine.data.options_poll.get_active_symbols", return_value=["SPY"]):
            with patch(
                "engine.data.options_poll.fetch_options_chain",
                side_effect=options_poll.TradierFetchError("tradier_api_token_missing", kind="config_error"),
            ):
                with patch(
                    "engine.data.options_poll._record_symbol_failure",
                    side_effect=sqlite3.OperationalError("database is locked"),
                ):
                    with patch("engine.data.options_poll._warn_nonfatal") as warn_nonfatal:
                        result = options_poll._run_once(["tradier"])

        self.assertFalse(bool(result["pipeline_ok"]))
        self.assertEqual(result["meta"]["symbol_status"]["SPY"]["status"], "failed")
        self.assertEqual(int(result["meta"]["symbol_status"]["SPY"]["disabled_until_ts_ms"] or 0), 0)
        codes = [str(call.args[0]) for call in warn_nonfatal.call_args_list if call.args]
        self.assertIn("OPTIONS_POLL_SYMBOL_FAILURE_RECORD_FAILED", codes)
        failure_record_calls = [
            call
            for call in warn_nonfatal.call_args_list
            if call.args and call.args[0] == "OPTIONS_POLL_SYMBOL_FAILURE_RECORD_FAILED"
        ]
        self.assertEqual(len(failure_record_calls), 1)
        self.assertNotIn("error", failure_record_calls[0].kwargs)
        self.assertIn("provider_error", failure_record_calls[0].kwargs)

    def test_lifecycle_degrades_when_options_ingestion_is_degraded(self) -> None:
        storage, runtime_meta, lifecycle_state, lifecycle = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
            "engine.runtime.lifecycle_state",
            "engine.runtime.lifecycle",
        )
        storage.init_db()
        runtime_meta.meta_set("first_price_ts_ms", "123")
        lifecycle_state.set_state(lifecycle_state.LIVE, "first_market_data_tick")

        stop_event = threading.Event()
        thread = lifecycle.start_lifecycle_monitor(
            get_health=lambda: {
                "prices": {"ok": True},
                "ingestion_runtime": {"running": True, "stale": False},
                "ingestion_freshness": {
                    "degraded": True,
                    "runtime_reason_codes": ["critical_source_stale:options"],
                },
            },
            get_jobs=lambda: [{"name": "ingestion_runtime", "running": True}],
            get_kill_switches=lambda: {},
            interval_s=0.05,
            stop_event=stop_event,
            claim_booting=False,
        )
        try:
            time.sleep(0.2)
            state = lifecycle_state.get_state()
        finally:
            stop_event.set()
            thread.join(timeout=1.0)

        self.assertEqual(state["state"], lifecycle_state.DEGRADED)
        self.assertEqual(state["detail"], "critical_source_stale:options")

    def test_options_source_not_critical_when_options_job_disabled(self) -> None:
        prev_tradier_enabled = os.environ.get("TRADIER_ENABLED")
        prev_critical_sources = os.environ.get("CRITICAL_INGESTION_SOURCES")
        try:
            os.environ["TRADIER_ENABLED"] = "0"
            os.environ["CRITICAL_INGESTION_SOURCES"] = "prices,options"
            (health,) = _reload_modules("engine.runtime.health")

            definitions = health._ingestion_source_definitions()

            self.assertFalse(bool((definitions.get("options") or {}).get("critical")))
        finally:
            if prev_tradier_enabled is None:
                os.environ.pop("TRADIER_ENABLED", None)
            else:
                os.environ["TRADIER_ENABLED"] = prev_tradier_enabled
            if prev_critical_sources is None:
                os.environ.pop("CRITICAL_INGESTION_SOURCES", None)
            else:
                os.environ["CRITICAL_INGESTION_SOURCES"] = prev_critical_sources

    def test_lifecycle_ignores_noncritical_source_staleness(self) -> None:
        storage, runtime_meta, lifecycle_state, lifecycle = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
            "engine.runtime.lifecycle_state",
            "engine.runtime.lifecycle",
        )
        storage.init_db()
        runtime_meta.meta_set("first_price_ts_ms", "123")
        lifecycle_state.set_state(lifecycle_state.LIVE, "first_market_data_tick")

        stop_event = threading.Event()
        thread = lifecycle.start_lifecycle_monitor(
            get_health=lambda: {
                "prices": {"ok": True},
                "ingestion_runtime": {"running": True, "stale": False},
                "ingestion_freshness": {
                    "degraded": False,
                    "runtime_reason_codes": [],
                    "advisory_reason_codes": ["source_stale:news", "source_stale:social"],
                    "stale_sources": ["news", "social"],
                    "stale_critical_sources": [],
                },
            },
            get_jobs=lambda: [{"name": "ingestion_runtime", "running": True}],
            get_kill_switches=lambda: {},
            interval_s=0.05,
            stop_event=stop_event,
            claim_booting=False,
        )
        try:
            time.sleep(0.2)
            state = lifecycle_state.get_state()
        finally:
            stop_event.set()
            thread.join(timeout=1.0)

        self.assertEqual(state["state"], lifecycle_state.LIVE)

    def test_lifecycle_does_not_degrade_for_nonfreshness_ingestion_flags(self) -> None:
        storage, runtime_meta, lifecycle_state, lifecycle = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
            "engine.runtime.lifecycle_state",
            "engine.runtime.lifecycle",
        )
        storage.init_db()
        runtime_meta.meta_set("first_price_ts_ms", "123")
        lifecycle_state.set_state(lifecycle_state.LIVE, "first_market_data_tick")

        stop_event = threading.Event()
        thread = lifecycle.start_lifecycle_monitor(
            get_health=lambda: {
                "prices": {"ok": True},
                "ingestion_runtime": {"running": False, "stale": True},
                "options_ingestion": {"degraded": True, "detail": "options_ingestion_failed"},
                "ingestion_freshness": {
                    "degraded": False,
                    "runtime_reason_codes": [],
                    "advisory_reason_codes": ["source_degraded:options"],
                },
            },
            get_jobs=lambda: [],
            get_kill_switches=lambda: {},
            interval_s=0.05,
            stop_event=stop_event,
            claim_booting=False,
        )
        try:
            time.sleep(0.2)
            state = lifecycle_state.get_state()
        finally:
            stop_event.set()
            thread.join(timeout=1.0)

        self.assertEqual(state["state"], lifecycle_state.LIVE)

    def test_lifecycle_recovers_when_critical_freshness_recovers(self) -> None:
        storage, runtime_meta, lifecycle_state, lifecycle = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
            "engine.runtime.lifecycle_state",
            "engine.runtime.lifecycle",
        )
        storage.init_db()
        runtime_meta.meta_set("first_price_ts_ms", "123")
        lifecycle_state.set_state(lifecycle_state.LIVE, "first_market_data_tick")

        health_state = {
            "prices": {"ok": True},
            "ingestion_runtime": {"running": True, "stale": False},
            "ingestion_freshness": {
                "degraded": True,
                "runtime_reason_codes": ["critical_source_stale:prices"],
            },
        }

        stop_event = threading.Event()
        thread = lifecycle.start_lifecycle_monitor(
            get_health=lambda: dict(health_state),
            get_jobs=lambda: [{"name": "ingestion_runtime", "running": True}],
            get_kill_switches=lambda: {},
            interval_s=0.05,
            stop_event=stop_event,
            claim_booting=False,
        )
        try:
            time.sleep(0.15)
            first_state = lifecycle_state.get_state()
            health_state["ingestion_freshness"] = {
                "degraded": False,
                "runtime_reason_codes": [],
            }
            time.sleep(1.3)
            recovered_state = lifecycle_state.get_state()
        finally:
            stop_event.set()
            thread.join(timeout=1.0)

        self.assertEqual(first_state["state"], lifecycle_state.DEGRADED)
        self.assertEqual(first_state["detail"], "critical_source_stale:prices")
        self.assertEqual(recovered_state["state"], lifecycle_state.LIVE)

    def test_health_freshness_tracker_keeps_runtime_stable_for_noncritical_stale_sources(self) -> None:
        (health,) = _reload_modules("engine.runtime.health")
        now_ms = int(time.time() * 1000)
        snapshot = health._build_ingestion_freshness_snapshot(
            now_ms=int(now_ms),
            prices_snapshot={"ok": True, "last_ts_ms": int(now_ms)},
            options_snapshot={"ok": True, "last_ingested_ts_ms": int(now_ms)},
            ingestion_runtime_snapshot={"last_publish_ts_ms": int(now_ms)},
            pipeline_statuses={
                "ingest_now": {"ok": True, "updated_ts_ms": int(now_ms - 1_000_000)},
                "poll_social_reddit": {"ok": True, "updated_ts_ms": int(now_ms - 1_000_000)},
                "poll_social_stocktwits": {"ok": True, "updated_ts_ms": int(now_ms)},
                "poll_macro": {"ok": True, "updated_ts_ms": int(now_ms)},
                "poll_weather_forecasts": {"ok": True, "updated_ts_ms": int(now_ms)},
                "poll_weather_alerts": {"ok": True, "updated_ts_ms": int(now_ms)},
            },
        )

        self.assertFalse(snapshot["degraded"])
        self.assertEqual(snapshot["stale_critical_sources"], [])
        self.assertIn("news", snapshot["stale_sources"])
        self.assertNotIn("critical_source_stale:news", list(snapshot.get("runtime_reason_codes") or []))

    def test_health_freshness_marks_fresh_failed_options_as_degraded(self) -> None:
        (health,) = _reload_modules("engine.runtime.health")
        now_ms = int(time.time() * 1000)

        snapshot = health._build_ingestion_freshness_snapshot(
            now_ms=int(now_ms),
            prices_snapshot={"ok": True, "last_ts_ms": int(now_ms)},
            options_snapshot={
                "ok": False,
                "available": True,
                "degraded": False,
                "failed": True,
                "stale": False,
                "status": "failed",
                "detail": "options_ingestion_failed",
                "last_ingested_ts_ms": None,
            },
            ingestion_runtime_snapshot={"last_publish_ts_ms": int(now_ms)},
            pipeline_statuses={
                "options_poll": {
                    "ok": False,
                    "updated_ts_ms": int(now_ms),
                    "last_error": "polygon 403",
                },
            },
        )

        options_source = dict(snapshot.get("sources", {}).get("options") or {})
        self.assertEqual(options_source.get("status"), "degraded")
        self.assertFalse(bool(options_source.get("ok")))
        self.assertFalse(bool(options_source.get("stale")))
        self.assertIn("source_degraded:options", list(snapshot.get("advisory_reason_codes") or []))

    def test_health_freshness_escalates_fresh_failed_critical_source(self) -> None:
        prev_critical_sources = os.environ.get("CRITICAL_INGESTION_SOURCES")
        try:
            os.environ["CRITICAL_INGESTION_SOURCES"] = "prices,macro"
            (health,) = _reload_modules("engine.runtime.health")
            now_ms = int(time.time() * 1000)

            snapshot = health._build_ingestion_freshness_snapshot(
                now_ms=int(now_ms),
                prices_snapshot={"ok": True, "last_ts_ms": int(now_ms)},
                options_snapshot={"ok": True, "last_ingested_ts_ms": int(now_ms)},
                ingestion_runtime_snapshot={"last_publish_ts_ms": int(now_ms)},
                pipeline_statuses={
                    "poll_macro": {
                        "ok": False,
                        "updated_ts_ms": int(now_ms),
                        "last_ingested_ts_ms": int(now_ms),
                        "last_error": "fred_http_403",
                    },
                },
            )
        finally:
            if prev_critical_sources is None:
                os.environ.pop("CRITICAL_INGESTION_SOURCES", None)
            else:
                os.environ["CRITICAL_INGESTION_SOURCES"] = prev_critical_sources

        macro_source = dict(snapshot.get("sources", {}).get("macro") or {})
        self.assertEqual(macro_source.get("status"), "degraded")
        self.assertTrue(bool(macro_source.get("critical")))
        self.assertFalse(bool(snapshot.get("critical_ok")))
        self.assertIn("macro", list(snapshot.get("failed_critical_sources") or []))
        self.assertIn("critical_source_failed:macro", list(snapshot.get("runtime_reason_codes") or []))
        self.assertIn("source_degraded:macro", list(snapshot.get("advisory_reason_codes") or []))

    def test_health_freshness_exposes_sec_and_form4_failures(self) -> None:
        prev_critical_sources = os.environ.get("CRITICAL_INGESTION_SOURCES")
        prev_child_jobs = os.environ.get("INGESTION_CHILD_JOBS")
        try:
            os.environ["CRITICAL_INGESTION_SOURCES"] = "sec,form4"
            os.environ["INGESTION_CHILD_JOBS"] = "poll_sec_filings,ingest_form4"
            (health,) = _reload_modules("engine.runtime.health")
            now_ms = int(time.time() * 1000)

            snapshot = health._build_ingestion_freshness_snapshot(
                now_ms=int(now_ms),
                prices_snapshot={"ok": True, "last_ts_ms": int(now_ms)},
                options_snapshot={"ok": True, "last_ingested_ts_ms": int(now_ms)},
                ingestion_runtime_snapshot={"last_publish_ts_ms": int(now_ms)},
                pipeline_statuses={
                    "poll_sec_filings": {
                        "ok": True,
                        "updated_ts_ms": int(now_ms),
                        "last_ingested_ts_ms": int(now_ms),
                    },
                    "ingest_form4": {
                        "ok": False,
                        "updated_ts_ms": int(now_ms),
                        "last_ingested_ts_ms": int(now_ms),
                        "last_error": "edgar_rate_limited",
                    },
                },
            )
        finally:
            if prev_critical_sources is None:
                os.environ.pop("CRITICAL_INGESTION_SOURCES", None)
            else:
                os.environ["CRITICAL_INGESTION_SOURCES"] = prev_critical_sources
            if prev_child_jobs is None:
                os.environ.pop("INGESTION_CHILD_JOBS", None)
            else:
                os.environ["INGESTION_CHILD_JOBS"] = prev_child_jobs

        sec_source = dict(snapshot.get("sources", {}).get("sec") or {})
        self.assertTrue(bool(sec_source.get("critical")))
        self.assertIn("poll_sec_filings", list(sec_source.get("pipeline_names") or []))
        self.assertIn("ingest_form4", list(sec_source.get("pipeline_names") or []))
        self.assertEqual(sec_source.get("status"), "degraded")
        self.assertIn("sec", list(snapshot.get("failed_critical_sources") or []))
        self.assertIn("critical_source_failed:sec", list(snapshot.get("runtime_reason_codes") or []))

    def test_health_freshness_exposes_alt_data_failures(self) -> None:
        prev_critical_sources = os.environ.get("CRITICAL_INGESTION_SOURCES")
        prev_child_jobs = os.environ.get("INGESTION_CHILD_JOBS")
        try:
            os.environ["CRITICAL_INGESTION_SOURCES"] = "etf_flows"
            os.environ["INGESTION_CHILD_JOBS"] = "ingest_etf_flows"
            data_source_manager, health = _reload_modules(
                "services.data_source_manager",
                "engine.runtime.health",
            )
            now_ms = int(time.time() * 1000)

            with patch.object(data_source_manager, "desired_ingestion_jobs", return_value=["ingest_etf_flows"]):
                snapshot = health._build_ingestion_freshness_snapshot(
                    now_ms=int(now_ms),
                    prices_snapshot={"ok": True, "last_ts_ms": int(now_ms)},
                    options_snapshot={"ok": True, "last_ingested_ts_ms": int(now_ms)},
                    ingestion_runtime_snapshot={"last_publish_ts_ms": int(now_ms)},
                    pipeline_statuses={
                        "ingest_etf_flows": {
                            "ok": False,
                            "updated_ts_ms": int(now_ms),
                            "last_ingested_ts_ms": int(now_ms),
                            "last_error": "etf_flows_provider_503",
                        },
                    },
                )
        finally:
            if prev_critical_sources is None:
                os.environ.pop("CRITICAL_INGESTION_SOURCES", None)
            else:
                os.environ["CRITICAL_INGESTION_SOURCES"] = prev_critical_sources
            if prev_child_jobs is None:
                os.environ.pop("INGESTION_CHILD_JOBS", None)
            else:
                os.environ["INGESTION_CHILD_JOBS"] = prev_child_jobs

        alt_source = dict(snapshot.get("sources", {}).get("alt_data") or {})
        self.assertTrue(bool(alt_source.get("critical")))
        self.assertEqual(alt_source.get("status"), "degraded")
        self.assertIn("ingest_etf_flows", list(alt_source.get("pipeline_names") or []))
        self.assertIn("alt_data", list(snapshot.get("failed_critical_sources") or []))
        self.assertIn("critical_source_failed:alt_data", list(snapshot.get("runtime_reason_codes") or []))

    def test_health_freshness_marks_fresh_failed_social_as_degraded(self) -> None:
        (health,) = _reload_modules("engine.runtime.health")
        now_ms = int(time.time() * 1000)

        snapshot = health._build_ingestion_freshness_snapshot(
            now_ms=int(now_ms),
            prices_snapshot={"ok": True, "last_ts_ms": int(now_ms)},
            options_snapshot={"ok": True, "last_ingested_ts_ms": int(now_ms)},
            ingestion_runtime_snapshot={"last_publish_ts_ms": int(now_ms)},
            pipeline_statuses={
                "poll_social_reddit": {
                    "ok": False,
                    "updated_ts_ms": int(now_ms - 1_000_000),
                    "last_error": "reddit_credentials_missing",
                },
                "poll_social_stocktwits": {
                    "ok": False,
                    "updated_ts_ms": int(now_ms),
                    "last_error": "stocktwits_http_403",
                },
            },
        )

        social_source = dict(snapshot.get("sources", {}).get("social") or {})
        self.assertEqual(social_source.get("status"), "degraded")
        self.assertFalse(bool(social_source.get("ok")))
        self.assertFalse(bool(social_source.get("stale")))
        self.assertIn("source_degraded:social", list(snapshot.get("advisory_reason_codes") or []))
