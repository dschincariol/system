from __future__ import annotations

from engine.api.api_ui_metrics import ROUTE_SPECS_UI_METRICS, build_ui_metrics_snapshot


def test_ui_metrics_route_spec_registers_canonical_read_endpoint() -> None:
    assert ("GET", "/api/ui/metrics", "api_get_ui_metrics") in ROUTE_SPECS_UI_METRICS


def test_ui_metrics_snapshot_full_sources_has_stable_shape() -> None:
    now_ms = 1_700_000_000_000

    payload = build_ui_metrics_snapshot(
        pnl={
            "ok": True,
            "data": {
                "day_pnl": 12.5,
                "total": 30.0,
                "realized": 10.0,
                "unrealized": 2.5,
                "ts_ms": now_ms - 1_000,
                "source": "canonical",
            },
        },
        pnl_summary={"ok": True, "day_pnl": 12.5, "total_pnl": 30.0, "ts_ms": now_ms - 1_000},
        portfolio={
            "ok": True,
            "meta": {"ready": True, "orders_batch_ts_ms": now_ms - 2_000},
            "state": [{"symbol": "MSFT", "updated_ts_ms": now_ms - 2_000}],
            "orders": [{"symbol": "MSFT", "ts_ms": now_ms - 2_000}],
        },
        risk_summary={
            "ok": True,
            "gross_exposure": 0.42,
            "net_exposure": -0.10,
            "max_drawdown_pct": 0.03,
            "execution_barrier": {"allowed": True, "reason": ""},
            "ts_ms": now_ms - 3_000,
        },
        portfolio_risk={
            "ok": True,
            "ready": True,
            "blocked": False,
            "status": "ok",
            "ts_ms": now_ms - 3_000,
            "history": [{"ts_ms": now_ms - 3_000, "gross": 0.4, "net": -0.1, "drawdown": 0.03}],
        },
        broker={
            "ok": True,
            "account": {"cash": 40_000.0, "equity": 100_000.0, "updated_ts_ms": now_ms - 1_500},
            "positions": [{"symbol": "MSFT", "qty": 5, "updated_ts_ms": now_ms - 1_500}],
        },
        terminal_positions={
            "ok": True,
            "rows": [{"symbol": "MSFT", "qty": 5, "updated_ts_ms": now_ms - 1_500}],
        },
        now_ms=now_ms,
    )

    assert payload["ok"] is True
    assert payload["schema_version"] == 1
    assert set(["pnl", "exposure", "positions", "account", "risk", "sources", "summary"]).issubset(payload)
    assert payload["pnl"]["today_pnl"] == 12.5
    assert payload["pnl"]["realized_pnl"] == 10.0
    assert payload["pnl"]["unrealized_pnl"] == 2.5
    assert payload["exposure"]["gross"] == 0.42
    assert payload["exposure"]["net"] == -0.10
    assert payload["account"]["cash"] == 40_000.0
    assert payload["account"]["equity"] == 100_000.0
    assert payload["positions"]["target_count"] == 1
    assert payload["positions"]["live_count"] == 1
    assert payload["risk"]["ready"] is True
    assert payload["sources"]["pnl"]["missing"] is False
    assert payload["summary"]["missing_sources"] == []


def test_ui_metrics_snapshot_missing_sources_keeps_shape_and_flags() -> None:
    payload = build_ui_metrics_snapshot(
        pnl={"ok": True, "data": {"source": "missing", "total": 0.0, "ts_ms": 0}},
        pnl_summary={"ok": False, "error": "missing"},
        portfolio={"ok": True, "meta": {"ready": False}, "state": [], "orders": []},
        risk_summary={"ok": False, "error": "risk_missing"},
        portfolio_risk={"ok": True, "ready": False, "status": "idle", "history": []},
        broker={"ok": False, "error": "broker_missing", "account": {}, "positions": []},
        terminal_positions={"ok": True, "rows": []},
        now_ms=1_700_000_000_000,
    )

    assert payload["ok"] is True
    assert payload["pnl"]["today_pnl"] == 0.0
    assert payload["exposure"]["gross"] is None
    assert payload["account"]["cash"] is None
    assert payload["risk"]["ready"] is False
    assert payload["sources"]["pnl"]["missing"] is True
    assert payload["sources"]["risk_summary"]["missing"] is True
    assert payload["sources"]["broker"]["missing"] is True
    assert payload["summary"]["degraded"] is True
    assert "pnl" in payload["summary"]["missing_sources"]
    assert "risk_summary" in payload["summary"]["missing_sources"]
