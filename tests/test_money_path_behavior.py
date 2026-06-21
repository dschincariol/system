from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from unittest.mock import Mock

import pytest


class _ManagedSqliteConnection(sqlite3.Connection):
    def close(self) -> None:
        return None

    def real_close(self) -> None:
        super().close()


_ManagedSqliteConnection.__module__ = "sqlite3"


class _CloseOnlyConnection:
    def close(self) -> None:
        return None


def _memory_db() -> _ManagedSqliteConnection:
    return sqlite3.connect(":memory:", factory=_ManagedSqliteConnection)


def _create_hierarchical_allocator_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE sleeve_metrics(
          sleeve_name TEXT NOT NULL,
          window_days INTEGER NOT NULL,
          ts_ms INTEGER NOT NULL,
          metrics_json TEXT NOT NULL,
          is_active INTEGER NOT NULL,
          PRIMARY KEY(sleeve_name, window_days)
        );
        CREATE TABLE sleeve_allocations(
          ts_ms INTEGER NOT NULL,
          window_days INTEGER NOT NULL,
          allocations_json TEXT NOT NULL,
          reason_json TEXT NOT NULL,
          PRIMARY KEY(ts_ms, window_days)
        );
        CREATE TABLE strategy_metrics(
          strategy_name TEXT NOT NULL,
          window_days INTEGER NOT NULL,
          ts_ms INTEGER NOT NULL,
          metrics_json TEXT NOT NULL,
          is_active INTEGER NOT NULL,
          PRIMARY KEY(strategy_name, window_days)
        );
        CREATE TABLE strategy_allocations(
          ts_ms INTEGER NOT NULL,
          window_days INTEGER NOT NULL,
          allocations_json TEXT NOT NULL,
          reason_json TEXT NOT NULL,
          PRIMARY KEY(ts_ms, window_days)
        );
        """
    )


def test_hierarchical_allocator_applies_sleeve_budget_and_orders_strategy_weights(monkeypatch):
    from engine.runtime import hierarchical_allocator as alloc

    con = _memory_db()
    _create_hierarchical_allocator_schema(con)
    now_ms = 10_000_000
    rows = [
        (now_ms - 240_000, "alpha", 10.0, 1.8, 0.01),
        (now_ms - 180_000, "alpha", 12.0, 2.0, 0.01),
        (now_ms - 120_000, "alpha", 15.0, 2.2, 0.01),
        (now_ms - 240_000, "beta", 1.0, 0.10, 0.01),
        (now_ms - 180_000, "beta", 1.0, 0.10, 0.01),
        (now_ms - 120_000, "beta", 1.0, 0.10, 0.01),
        (now_ms - 240_000, "gamma", 100.0, 5.0, 0.01),
        (now_ms - 180_000, "gamma", 100.0, 5.0, 0.01),
        (now_ms - 120_000, "gamma", 100.0, 5.0, 0.01),
    ]
    monkeypatch.setattr(alloc, "DEFAULT_WINDOW_S", 600)
    monkeypatch.setattr(alloc, "DEFAULT_BUCKET_S", 60)
    monkeypatch.setattr(alloc, "MIN_SHARE", 0.0)
    monkeypatch.setattr(alloc, "MAX_SHARE", 1.0)
    monkeypatch.setattr(alloc, "SCORE_FLOOR", 0.0)
    monkeypatch.setattr(alloc, "CORR_GAMMA_SLEEVE", 0.0)
    monkeypatch.setattr(alloc, "CORR_GAMMA_STRAT", 0.0)
    monkeypatch.setattr(alloc, "_SLEEVE_BUDGETS_RAW", '{"equities":1.0,"options":0.0}')
    monkeypatch.setattr(alloc, "_STRATEGY_BUDGETS_RAW", '{"alpha":1.0,"beta":0.25,"gamma":1.0}')
    monkeypatch.setattr(
        alloc,
        "_STRATEGY_SLEEVE_MAP_RAW",
        '{"alpha":"equities","beta":"equities","gamma":"options"}',
    )
    monkeypatch.setattr(alloc, "_load_strategy_registry_meta", lambda _con: {})
    monkeypatch.setattr(alloc, "_read_exec_cap_eff", lambda _con, since_ms, now_ms: list(rows))

    result = alloc.compute_and_persist_hier_allocations(con, now_ms=now_ms)

    assert result["ok"] is True
    sleeve_weights = result["sleeves"]["weights"]
    strategy_weights = result["strategies"]["weights"]
    assert sleeve_weights["equities"] == pytest.approx(1.0)
    assert sleeve_weights["options"] == pytest.approx(0.0)
    assert strategy_weights["alpha"] > strategy_weights["beta"] > 0.0
    assert strategy_weights["gamma"] == pytest.approx(0.0)
    assert sum(strategy_weights.values()) == pytest.approx(1.0)

    persisted = con.execute(
        "SELECT allocations_json FROM strategy_allocations WHERE ts_ms=? AND window_days=0",
        (now_ms,),
    ).fetchone()
    assert json.loads(persisted[0]) == pytest.approx(strategy_weights)
    con.real_close()


def _create_shadow_capital_schema(db_path: Path) -> None:
    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(
            """
            CREATE TABLE model_marketplace_scores(
              model_name TEXT NOT NULL,
              regime TEXT NOT NULL,
              trades INTEGER,
              score REAL,
              net_pnl REAL,
              meta_json TEXT,
              updated_ts_ms INTEGER NOT NULL
            );
            CREATE TABLE shadow_capital_scores(
              ts_ms INTEGER NOT NULL,
              window_s INTEGER NOT NULL,
              regime TEXT NOT NULL,
              model_name TEXT NOT NULL,
              model_kind TEXT,
              model_ts_ms INTEGER,
              n INTEGER,
              rmse REAL,
              dir_acc REAL,
              net_rmse REAL,
              slippage_bps_mean REAL,
              slippage_bps_std REAL,
              execution_latency_ms_mean REAL,
              execution_latency_ms_std REAL,
              drawdown_proxy REAL,
              cap_eff REAL,
              realized_pnl REAL,
              unrealized_pnl REAL,
              total_pnl REAL,
              score REAL,
              weights_json TEXT,
              components_json TEXT,
              PRIMARY KEY(model_name, window_s, regime)
            );
            """
        )
        rows = [
            (
                "model_a",
                "risk_on",
                2,
                0.0,
                0.0,
                json.dumps(
                    {
                        "score_source": "pnl_attribution",
                        "realized_pnl": 100.0,
                        "unrealized_pnl": -10.0,
                        "total_pnl": 90.0,
                    }
                ),
                3,
            ),
            (
                "model_a",
                "risk_on",
                3,
                0.0,
                0.0,
                json.dumps(
                    {
                        "score_source": "broker_fills",
                        "realized_pnl": 20.0,
                        "unrealized_pnl": 5.0,
                        "total_pnl": 25.0,
                    }
                ),
                2,
            ),
            (
                "model_bad",
                "risk_on",
                1,
                0.0,
                0.0,
                json.dumps({"score_source": "execution_fills", "realized_pnl": 1.0}),
                1,
            ),
            (
                "model_global",
                "global",
                100,
                0.0,
                0.0,
                json.dumps(
                    {
                        "score_source": "pnl_attribution",
                        "realized_pnl": 1_000.0,
                        "unrealized_pnl": 0.0,
                        "total_pnl": 1_000.0,
                    }
                ),
                4,
            ),
        ]
        con.executemany(
            """
            INSERT INTO model_marketplace_scores(
              model_name, regime, trades, score, net_pnl, meta_json, updated_ts_ms
            )
            VALUES (?,?,?,?,?,?,?)
            """,
            rows,
        )
        con.commit()
    finally:
        con.close()


def test_shadow_capital_scores_filter_regime_skip_malformed_and_persist_components(tmp_path, monkeypatch):
    from engine.runtime import shadow_capital_allocator as shadow

    db_path = tmp_path / "shadow.sqlite"
    _create_shadow_capital_schema(db_path)
    monkeypatch.setattr(shadow, "_db_connect", lambda: sqlite3.connect(str(db_path)))
    monkeypatch.setattr(
        shadow,
        "_table_exists",
        lambda _con, name: name in {"shadow_capital_scores", "model_marketplace_scores"},
    )
    monkeypatch.setattr(shadow, "_now_ms", lambda: 1_234_567)
    monkeypatch.setattr(shadow, "W_DD", 1.0)

    result = shadow.compute_and_persist_shadow_capital_scores(window_s=3600, regime="risk_on")

    assert result["ok"] is True
    assert result["upserts"] == 1
    assert result["skipped"] == 1
    con = sqlite3.connect(str(db_path))
    try:
        row = con.execute(
            """
            SELECT model_name, regime, n, realized_pnl, unrealized_pnl, total_pnl, score, components_json
            FROM shadow_capital_scores
            """
        ).fetchone()
    finally:
        con.close()
    assert row[:2] == ("model_a", "risk_on")
    assert int(row[2]) == 5
    assert float(row[3]) == pytest.approx(120.0)
    assert float(row[4]) == pytest.approx(-5.0)
    assert float(row[5]) == pytest.approx(115.0)
    assert float(row[6]) == pytest.approx(115.0)
    assert json.loads(row[7])["realized_pnl"] == pytest.approx(120.0)


def test_runtime_bootstrap_safe_mode_removes_credentials_and_disables_live_providers(monkeypatch):
    from engine.runtime import runtime_bootstrap as bootstrap

    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "paper")
    monkeypatch.setenv("BROKER", "sim")
    monkeypatch.setenv("BROKER_NAME", "sim")
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "1")
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "1")
    monkeypatch.setenv("POLYGON_API_KEY", "secret-token")
    monkeypatch.setenv("IBKR_USERNAME", "operator")
    monkeypatch.setenv("POLYGON_REST_ENABLED", "1")
    monkeypatch.setenv("IBKR_ENABLED", "1")

    bootstrap._apply_safe_no_credential_bootstrap_environment()

    assert "POLYGON_API_KEY" not in os.environ
    assert "IBKR_USERNAME" not in os.environ
    assert os.environ["POLYGON_REST_ENABLED"] == "0"
    assert os.environ["IBKR_ENABLED"] == "0"
    assert os.environ["YFINANCE_ENABLED"] == "1"
    assert os.environ["LIVE_PRICE_PROVIDER_CHAIN"] == "yfinance"


def test_runtime_bootstrap_init_failure_fails_fast_without_later_steps(monkeypatch):
    from engine.runtime import runtime_bootstrap as bootstrap

    init_db = Mock(side_effect=AssertionError("init_db should not run after first-run failure"))
    monkeypatch.setattr(bootstrap, "bootstrap_first_run", lambda mode: {"ok": False, "mode": mode})
    monkeypatch.setattr(bootstrap, "_init_db", init_db)
    monkeypatch.setattr(bootstrap, "ensure_db_ok", Mock(side_effect=AssertionError("db guard should not run")))
    monkeypatch.setattr(bootstrap, "_warn_nonfatal", lambda *args, **kwargs: None)

    result = bootstrap.bootstrap_runtime(log=Mock())

    assert result["ok"] is False
    assert any(str(err).startswith("init_db:") for err in result["errors"])
    assert result["init_db"] is False
    assert result["job_locks"] is False
    assert [step["name"] for step in result["steps"]] == ["init_db"]
    init_db.assert_not_called()


def test_liquidity_model_nbbo_rejects_stale_quotes_and_computes_true_spread(monkeypatch):
    from engine.execution import execution_liquidity_model as liq

    con = _memory_db()
    con.executescript(
        """
        CREATE TABLE price_quotes(
          symbol TEXT,
          ts_ms INTEGER,
          bid REAL,
          ask REAL,
          last REAL,
          spread REAL,
          source TEXT,
          volume REAL
        );
        """
    )
    con.executemany(
        "INSERT INTO price_quotes(symbol, ts_ms, bid, ask, last, spread, source, volume) VALUES (?,?,?,?,?,?,?,?)",
        [
            ("AAPL", 10_000, 99.0, 101.0, 100.0, 2.0, "sip", 1_000),
            ("MSFT", 10_100, None, None, 50.0, 0.5, "fallback", 1_000),
        ],
    )
    monkeypatch.setattr(liq, "connect", lambda readonly=True: con)

    fresh = liq.get_true_nbbo_snapshot("aapl", ts_ms=10_500, max_age_ms=1_000)
    stale = liq.get_true_nbbo_snapshot("AAPL", ts_ms=20_000, max_age_ms=1_000)
    fallback = liq.get_true_nbbo_snapshot("MSFT", ts_ms=10_500, max_age_ms=1_000)

    assert fresh["ok"] is True
    assert fresh["symbol"] == "AAPL"
    assert fresh["mid_px"] == pytest.approx(100.0)
    assert fresh["spread_px"] == pytest.approx(2.0)
    assert fresh["true_spread_bps"] == pytest.approx(200.0)
    assert fresh["age_ms"] == 500
    assert stale["ok"] is False
    assert stale["quote_ts_ms"] is None
    assert fallback["ok"] is True
    assert fallback["mid_px"] == pytest.approx(50.0)
    assert fallback["true_spread_bps"] == pytest.approx(100.0)
    con.real_close()


def test_liquidity_snapshot_escalates_for_wide_spread_high_participation_and_missing_price(monkeypatch):
    from engine.execution import execution_liquidity_model as liq

    monkeypatch.setattr(
        liq,
        "get_true_nbbo_snapshot",
        lambda symbol, ts_ms=None: {
            "ok": True,
            "symbol": symbol,
            "true_spread_bps": 20.0,
            "bid_px": 99.0,
            "ask_px": 101.0,
            "mid_px": 100.0,
        },
    )
    monkeypatch.setattr(liq, "connect", lambda readonly=True: _CloseOnlyConnection())
    monkeypatch.setattr(liq, "_rolling_adv", lambda _con, _symbol, lookback_ms: 100.0)
    monkeypatch.setattr(liq, "_intraday_vol_bps", lambda _con, _symbol, lookback_ms: 50.0)
    recent = iter([20.0, 50.0])
    monkeypatch.setattr(liq, "_recent_volume_delta", lambda _con, _symbol, lookback_ms: next(recent))

    snap = liq.get_execution_liquidity_snapshot(symbol="aapl", qty=-25, px=None, ts_ms=123)

    assert snap["ok"] is True
    assert snap["qty_abs"] == pytest.approx(25.0)
    assert snap["notional"] == pytest.approx(0.0)
    assert snap["spread_regime"] == "wide"
    assert snap["adv_participation"] == pytest.approx(0.25)
    assert snap["live_participation_rate"] == pytest.approx(1.25)
    assert snap["aggressiveness_bias"] >= 1.0
    assert snap["slice_size_mult"] < 1.0


def test_slicing_engine_preserves_parent_quantity_and_scales_interval(monkeypatch):
    from engine.execution import execution_slicing_engine as slicing

    monkeypatch.setattr(
        slicing,
        "get_execution_liquidity_snapshot",
        lambda **_kwargs: {
            "rolling_adv": 1_000.0,
            "slice_size_mult": 0.5,
            "interval_mult": 2.0,
            "aggressiveness_bias": 0.0,
            "recent_volume_1m": 0.0,
            "recent_volume_5m": 0.0,
            "spread_regime": "normal",
            "true_spread_bps": 5.0,
            "adv_participation": 0.0,
            "intraday_vol_bps": 0.0,
            "live_participation_rate": 0.0,
        },
    )

    unchanged_zero = {"symbol": "AAPL", "qty": 0, "slice_style": "twap"}
    unchanged_missing_symbol = {"qty": 10, "slice_style": "twap"}
    sell_order = {
        "symbol": "AAPL",
        "qty": -10,
        "slice_style": "twap",
        "slice_qty": 3,
        "slice_interval_ms": 100,
        "client_order_id": "parent-1",
    }

    assert slicing.build_order_slices(unchanged_zero) == [unchanged_zero]
    assert slicing.build_order_slices(unchanged_missing_symbol) == [unchanged_missing_symbol]
    slices = slicing.build_order_slices(sell_order, broker_name="sim")

    assert len(slices) == 4
    assert sum(float(item["qty"]) for item in slices) == pytest.approx(-10.0)
    assert all(float(item["qty"]) < 0.0 for item in slices)
    assert {item["slice_count"] for item in slices} == {4}
    assert {item["slice_interval_ms"] for item in slices} == {200}
    assert {item["slice_parent_id"] for item in slices} == {"parent-1"}
    assert {item["slice_broker"] for item in slices} == {"sim"}


def test_slicing_engine_pov_uses_recent_liquidity_without_oversizing(monkeypatch):
    from engine.execution import execution_slicing_engine as slicing

    monkeypatch.setenv("EXEC_MAX_SLICES_PER_ORDER", "64")
    monkeypatch.setattr(
        slicing,
        "get_execution_liquidity_snapshot",
        lambda **_kwargs: {
            "rolling_adv": 0.0,
            "slice_size_mult": 1.0,
            "interval_mult": 1.0,
            "aggressiveness_bias": 0.0,
            "recent_volume_1m": 10.0,
            "recent_volume_5m": 25.0,
            "spread_regime": "tight",
            "true_spread_bps": 1.0,
            "adv_participation": 0.0,
            "intraday_vol_bps": 0.0,
            "live_participation_rate": 0.0,
        },
    )

    slices = slicing.build_order_slices(
        {
            "symbol": "AAPL",
            "qty": 50,
            "slice_style": "pov",
            "target_participation": 0.10,
            "slice_interval_ms": 60_000,
        }
    )

    assert len(slices) == 50
    assert sum(float(item["qty"]) for item in slices) == pytest.approx(50.0)
    assert max(float(item["qty"]) for item in slices) <= 1.0
    assert all(item["slice_style"] == "pov" for item in slices)


def _create_dual_execution_schema(con: sqlite3.Connection) -> None:
    con.execute("CREATE TABLE broker_positions(symbol TEXT PRIMARY KEY, qty REAL)")
    con.executemany("INSERT INTO broker_positions(symbol, qty) VALUES (?,?)", [("AAPL", 100.0), ("MSFT", -50.0)])
    con.commit()


def test_dual_execution_divergence_persists_and_disarms_live_mode(monkeypatch):
    from engine.execution import dual_execution

    con = _memory_db()
    _create_dual_execution_schema(con)
    armed = Mock()
    mode = Mock()
    monkeypatch.setenv("EXECUTION_DIVERGENCE_ALERT_TH", "0.10")
    monkeypatch.setenv("EXECUTION_DIVERGENCE_DISABLE_TH", "0.25")
    monkeypatch.setattr(dual_execution.broker_ibkr_gateway, "get_positions_snapshot", lambda timeout_s: {"AAPL": 50.0})
    monkeypatch.setattr(dual_execution, "set_execution_armed", armed)
    monkeypatch.setattr(dual_execution, "set_execution_mode", mode)

    result = dual_execution.check_dual_divergence(con, ts_ms=42, exec_result={"ok": True})

    assert result["ok"] is True
    assert result["divergence"] >= 0.25
    assert result["actions"] == [
        {"action": "live_disabled", "reason": "divergence", "divergence": pytest.approx(result["divergence"])}
    ]
    armed.assert_called_once()
    mode.assert_called_once()
    row = con.execute("SELECT broker, divergence, details_json FROM execution_divergence WHERE ts_ms=42").fetchone()
    assert row[0] == "ibkr"
    assert float(row[1]) == pytest.approx(result["divergence"])
    assert json.loads(row[2])["exec_result"] == {"ok": True}
    con.real_close()


def test_dual_execution_dry_run_skips_live_broker_submit(monkeypatch):
    from engine.execution import dual_execution

    con = _memory_db()
    _create_dual_execution_schema(con)
    monkeypatch.setattr(dual_execution, "connect", lambda: con)
    monkeypatch.setattr(
        dual_execution.broker_sim,
        "apply_new_portfolio_orders",
        lambda **kwargs: {"ok": True, "orders": list(kwargs.get("override_orders") or [])},
    )
    live_submit = Mock(side_effect=AssertionError("dry_run_live must not call live broker"))
    monkeypatch.setattr(dual_execution.broker_ibkr_gateway, "apply_latest_portfolio_orders_live", live_submit)
    monkeypatch.setattr(
        dual_execution.broker_ibkr_gateway,
        "get_positions_snapshot",
        lambda timeout_s: {"AAPL": 100.0, "MSFT": -50.0},
    )

    result = dual_execution.apply_portfolio_orders_dual_ibkr(
        dry_run_live=True,
        override_orders=[{"symbol": "AAPL", "qty": 1}],
        override_order_id=7,
        override_ts_ms=8,
    )

    assert result["ok"] is True
    assert result["sim"]["orders"] == [{"symbol": "AAPL", "qty": 1}]
    assert result["live"] == {"ok": True, "status": "skipped"}
    assert result["divergence"] == pytest.approx(0.0)
    live_submit.assert_not_called()
    con.real_close()


def _create_execution_ai_advisor_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE execution_ai_advisory(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          batch_id INTEGER,
          portfolio_orders_id INTEGER,
          payload_source TEXT,
          execution_mode TEXT,
          broker TEXT,
          symbol TEXT,
          side TEXT,
          order_type TEXT,
          aggressiveness TEXT,
          urgency TEXT,
          recommendation TEXT,
          expected_slippage_bps REAL,
          confidence REAL,
          approved INTEGER NOT NULL DEFAULT 0,
          rejected INTEGER NOT NULL DEFAULT 0,
          rationale TEXT,
          features_json TEXT,
          advisory_json TEXT
        );
        CREATE TABLE execution_ai_advisory_actions(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          advisory_id INTEGER NOT NULL,
          action TEXT NOT NULL,
          actor TEXT,
          note TEXT,
          detail_json TEXT
        );
        """
    )


def test_execution_ai_advisor_persists_safe_high_risk_advice_and_records_action(monkeypatch):
    from engine.execution import execution_ai_advisor as advisor

    con = _memory_db()
    _create_execution_ai_advisor_schema(con)
    monkeypatch.setattr(advisor, "init_db", lambda: None)
    monkeypatch.setattr(advisor, "connect", lambda: con)
    monkeypatch.setattr(advisor, "run_write_txn", lambda fn: fn(con))
    monkeypatch.setattr(
        advisor,
        "_historical_execution_snapshot",
        lambda symbol, broker, lookback_n=120: {
            "sample_n": 12,
            "avg_slippage_bps": 2.0,
            "p95_slippage_bps": 8.0,
            "avg_latency_ms": 13_000.0,
            "source": "unit",
        },
    )

    advisories = advisor.persist_execution_advisories(
        shaped_payload=[
            {"symbol": "", "qty": 100},
            {
                "symbol": "aapl",
                "side": "BUY",
                "order_type": "MARKET",
                "aggressiveness": "AGGRESSIVE",
                "epe_alpha_remaining": "bad",
                "confidence": "nan",
            },
        ],
        batch_id=11,
        portfolio_orders_id=12,
        payload_source="unit",
        execution_mode="paper",
        broker="sim",
        ts_ms=123,
    )

    assert len(advisories) == 1
    advisory = advisories[0]
    assert advisory["symbol"] == "AAPL"
    assert advisory["urgency"] == "high"
    assert advisory["recommendation"] == "review_before_send"
    assert advisory["expected_slippage_bps"] >= 5.0
    assert advisory["confidence"] == 0.0
    assert "Expected slippage is elevated" in advisory["rationale"]

    action_result = advisor.record_execution_advisory_action(
        advisory_id=int(advisory["advisory_id"]),
        action="approve",
        actor="qa",
        note="reviewed",
        detail={"reason": "test"},
    )
    assert action_result["ok"] is True
    row = con.execute("SELECT approved, rejected FROM execution_ai_advisory WHERE id=?", (advisory["advisory_id"],)).fetchone()
    assert tuple(row) == (1, 0)
    with pytest.raises(ValueError, match="invalid_action"):
        advisor.record_execution_advisory_action(advisory_id=advisory["advisory_id"], action="send")
    con.real_close()


@pytest.mark.parametrize(
    ("open_qty", "broker_info", "expected"),
    [
        (10.0, {"remaining": 0, "side": "buy"}, 0.0),
        (10.0, {"qty": 10, "filled_qty": 4, "side": "buy"}, 6.0),
        (-10.0, {"qty": 10, "filled_qty": 4, "side": "sell"}, -6.0),
        (-10.0, {"remaining": 3, "side": "sell"}, -3.0),
    ],
)
def test_microstructure_remaining_qty_preserves_side_and_partial_fill_boundaries(open_qty, broker_info, expected):
    from engine.execution import execution_microstructure as micro

    assert micro._remaining_qty(open_qty, broker_info) == pytest.approx(expected)


def test_microstructure_native_replace_refuses_partial_or_market_escalation_without_broker_call(monkeypatch):
    from engine.execution import execution_microstructure as micro

    broker_call = Mock(side_effect=AssertionError("unsafe native replace should not call broker"))

    partial = micro.try_native_limit_replace(
        _CloseOnlyConnection(),
        open_id=1,
        now_ms=100,
        broker="alpaca",
        symbol="AAPL",
        current_qty=10.0,
        remaining_qty=6.0,
        limit_px=100.0,
        client_order_id="cid",
        broker_order_id="oid",
        attempts=0,
        max_attempts=3,
        aggressiveness="PASSIVE",
        next_action_ts_ms=200,
        replace_limit_fn=broker_call,
        meta={},
    )
    market = micro.try_native_limit_replace(
        _CloseOnlyConnection(),
        open_id=1,
        now_ms=100,
        broker="alpaca",
        symbol="AAPL",
        current_qty=10.0,
        remaining_qty=10.0,
        limit_px=100.0,
        client_order_id="cid",
        broker_order_id="oid",
        attempts=2,
        max_attempts=2,
        aggressiveness="AGGRESSIVE",
        next_action_ts_ms=200,
        replace_limit_fn=broker_call,
        meta={},
    )

    assert partial == {"attempted": False, "reason": "partial_fill_requires_cancel_verify"}
    assert market == {"attempted": False, "reason": "market_escalation_requires_cancel_verify"}
    broker_call.assert_not_called()


def test_open_order_manager_without_broker_ack_times_out_instead_of_resubmitting(monkeypatch):
    from engine.execution import broker_alpaca_rest
    from engine.execution import execution_open_order_manager as manager

    con = _memory_db()
    manager._ensure_tables(con)
    now_ms = 1_000_000
    con.execute(
        """
        INSERT INTO exec_open_orders(
          ts_ms, updated_ts_ms, broker, symbol, qty, side, order_type, aggressiveness,
          limit_px, client_order_id, broker_order_id, status, attempts, max_attempts,
          next_action_ts_ms, portfolio_orders_id, source_alert_id, meta_json
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            now_ms - 10_000,
            now_ms - 10_000,
            "alpaca",
            "AAPL",
            10.0,
            "BUY",
            "LIMIT",
            "PASSIVE",
            100.0,
            "cid-no-ack",
            "",
            "open",
            0,
            2,
            0,
            1,
            2,
            json.dumps({"broker_submit_ts_ms": now_ms - 10_000, "ack_timeout_ms": 1_000}),
        ),
    )
    con.commit()
    submit_limit = Mock(side_effect=AssertionError("ack timeout must not submit a replacement"))
    submit_market = Mock(side_effect=AssertionError("ack timeout must not submit a market order"))
    monkeypatch.setattr(manager, "connect", lambda: con)
    monkeypatch.setattr(manager, "_now_ms", lambda: now_ms)
    monkeypatch.setattr(broker_alpaca_rest, "get_order", lambda _oid: {})
    monkeypatch.setattr(broker_alpaca_rest, "cancel_order", lambda _oid: {"ok": True})
    monkeypatch.setattr(broker_alpaca_rest, "replace_limit_order", None)
    monkeypatch.setattr(broker_alpaca_rest, "submit_limit_order", submit_limit)
    monkeypatch.setattr(broker_alpaca_rest, "submit_market_order", submit_market)

    result = manager.manage_open_orders()

    assert result["ok"] is True
    assert result["updated"] == 1
    assert result["errors"] == 0
    row = con.execute("SELECT status, next_action_ts_ms FROM exec_open_orders WHERE client_order_id='cid-no-ack'").fetchone()
    assert row == ("ack_timeout", 0)
    event = con.execute("SELECT event, details_json FROM exec_order_events ORDER BY id DESC LIMIT 1").fetchone()
    assert event[0] == "broker_ack_timeout"
    assert json.loads(event[1])["ack_timeout_ms"] == 1_000
    submit_limit.assert_not_called()
    submit_market.assert_not_called()
    con.real_close()
