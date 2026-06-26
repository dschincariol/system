from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


def _json_lines(raw: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in str(raw or "").splitlines():
        text = line.strip()
        if not text.startswith("{"):
            continue
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows


def _postgres_paper_env(tmp_path: Path, schema: str) -> dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "ALLOW_TRAINING": "0",
            "APP_ENV": "test",
            "BROKER": "sim",
            "BROKER_FAILOVER": "sim",
            "BROKER_LATENCY_SLEEP": "0",
            "BROKER_NAME": "sim",
            "BROKER_START_CASH": "100000",
            "BROKER_START_EQUITY": "100000",
            "CAPITAL_AWARE_KILL_SWITCH": "0",
            "DATA_DIR": str(tmp_path / "data"),
            "DB_PATH": str(tmp_path / "postgres-concurrency.sqlite-placeholder"),
            "DISABLE_LIVE_EXECUTION": "1",
            "ENGINE_MODE": "paper",
            "ENGINE_SUPERVISED": "1",
            "ENV": "test",
            "EPE_EQUITY_SESSION_ENFORCE": "0",
            "EXECUTION_MAX_SIGNAL_AGE_S": "3600",
            "EXECUTION_MODE": "paper",
            "FEATURE_STORE_ENABLED": "0",
            "KILL_SWITCH": "0",
            "KILL_SWITCH_GLOBAL": "0",
            "KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES": "0",
            "KILL_SWITCH_MODEL_MAX_DRAWDOWN": "0",
            "KILL_SWITCH_REQUIRE_FRESH_DATA": "0",
            "KILL_SWITCH_REQUIRE_FRESH_JOBS": "0",
            "LIVE_BROKER": "sim",
            "LIVE_CACHE_BACKEND": "memory",
            "MODEL_AWARE_KILL_SWITCH": "0",
            "MODE": "paper",
            "OPERATOR_MODE": "paper",
            "PREFLIGHT_ENABLE": "0",
            "PYTHONPATH": str(REPO_ROOT),
            "REDIS_CACHE_URL": "",
            "REDIS_URL": "",
            "TS_ENV": "test",
            "TS_PG_SCHEMA": str(schema),
            "TS_PG_SCHEMA_PER_DB_PATH": "0",
            "TS_REDIS_KEY_PREFIX": f"broker_apply_pg_{schema}",
            "TS_STORAGE_BACKEND": "postgres",
            "TRADING_DATA": str(tmp_path / "data"),
            "TRADING_LOGS": str(tmp_path / "logs"),
            "TRADING_UNIT_TEST_SCHEMA_FAST": "1",
        }
    )
    (tmp_path / "data").mkdir(parents=True, exist_ok=True)
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)
    return env


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _seed_postgres_paper_order(env: dict[str, str]) -> None:
    for key, value in env.items():
        os.environ[key] = value
    (
        storage,
        broker_sim,
        execution_ledger,
        trade_attribution_ledger,
        lifecycle_state,
        execution_mode,
        runtime_meta,
    ) = _reload_modules(
        "engine.runtime.storage",
        "engine.execution.broker_sim",
        "engine.execution.execution_ledger",
        "engine.execution.trade_attribution_ledger",
        "engine.runtime.lifecycle_state",
        "engine.cache.wrappers.execution_mode",
        "engine.runtime.runtime_meta",
    )
    storage.init_db()
    broker_sim.init_broker_db()
    execution_ledger.init_execution_ledger()
    trade_attribution_ledger.ensure_trade_attribution_ready()
    execution_mode.set_execution_mode("paper", actor="postgres_concurrency_test", reason="broker_apply_concurrency", armed=0)
    now_ms = int(time.time() * 1000)
    runtime_meta.meta_set("first_price_ts_ms", str(now_ms))
    lifecycle_state.set_state(lifecycle_state.LIVE, "postgres_concurrency_test")

    con = storage.connect()
    try:
        con.execute(
            "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?,?,?,?,?) ON CONFLICT(symbol, ts_ms) DO NOTHING",
            (now_ms, "AAPL", 100.0, 100.0, "postgres_concurrency_test"),
        )
        con.execute(
            """
            INSERT INTO portfolio_orders(
              ts_ms, model_id, symbol, action, from_side, to_side,
              from_weight, to_weight, delta_weight, source_alert_id, explain_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                now_ms,
                "baseline",
                "AAPL",
                "rebalance",
                "FLAT",
                "LONG",
                0.0,
                0.10,
                0.10,
                1,
                json.dumps({"model_name": "baseline", "regime": "global"}, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.commit()
    finally:
        con.close()
        storage.close_pooled_connections()


def _fetch_fill_counts() -> dict[str, int]:
    (storage,) = _reload_modules("engine.runtime.storage")
    con = storage.connect(readonly=True)
    try:
        return {
            "broker_fills": int(con.execute("SELECT COUNT(*) FROM broker_fills WHERE symbol='AAPL'").fetchone()[0] or 0),
            "execution_fills": int(con.execute("SELECT COUNT(*) FROM execution_fills WHERE symbol='AAPL'").fetchone()[0] or 0),
        }
    finally:
        con.close()
        storage.close_pooled_connections()


def _drop_schema(schema: str) -> None:
    try:
        storage_pool = importlib.import_module("engine.runtime.storage_pool")
        storage = importlib.reload(importlib.import_module("engine.runtime.storage"))
        quote_ident = storage_pool.quote_ident
        con = storage.connect()
        try:
            con.execute(f"DROP SCHEMA IF EXISTS {quote_ident(schema)} CASCADE")
            con.commit()
        finally:
            con.close()
            storage.close_pooled_connections()
    except Exception:
        pass


@pytest.mark.requires_postgres
def test_two_broker_apply_orders_shared_postgres_complete_without_closed_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    schema = f"broker_apply_r12_{os.getpid()}_{uuid.uuid4().hex[:8]}"
    env = _postgres_paper_env(tmp_path, schema)
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    try:
        _seed_postgres_paper_order(env)
        procs = [
            subprocess.Popen(
                [sys.executable, "-m", "engine.execution.jobs.broker_apply_orders"],
                cwd=str(REPO_ROOT),
                env=dict(env),
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            for _ in range(2)
        ]
        results = [proc.communicate(timeout=90) for proc in procs]
        returncodes = [int(proc.returncode or 0) for proc in procs]
        combined = "\n".join(f"stdout:\n{stdout}\nstderr:\n{stderr}" for stdout, stderr in results)
        payloads = [payload for stdout, _stderr in results for payload in _json_lines(stdout)]

        assert returncodes == [0, 0], combined
        assert "cannot operate on a closed database" not in combined.lower()
        assert "connection already closed before commit" not in combined.lower()
        assert payloads, combined
        statuses = {str(payload.get("status") or "") for payload in payloads}
        assert statuses.issubset({"ok", "locked_out", "blocked"}), payloads
        assert "blocked" not in statuses, payloads

        counts = _fetch_fill_counts()
        assert counts["broker_fills"] >= 1
        assert counts["execution_fills"] >= 1
    finally:
        _drop_schema(schema)
