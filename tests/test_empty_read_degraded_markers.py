import importlib
import json
import sqlite3
from pathlib import Path


def _reload_dashboard(monkeypatch):
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)
    monkeypatch.delenv("TIMESCALE_URL", raising=False)
    monkeypatch.delenv("TIMESCALE_DATABASE_URL", raising=False)
    import dashboard_server

    return importlib.reload(dashboard_server)


def _set_dashboard_db(monkeypatch, dashboard_server, db_path: Path) -> None:
    monkeypatch.setattr(dashboard_server, "_dashboard_db_connect", lambda: sqlite3.connect(str(db_path)))


def _assert_marker(payload, *, reason: str, source: str, table_present: bool) -> None:
    assert isinstance(payload, dict)
    assert payload["ok"] is True
    assert payload["ready"] is False
    assert payload["reason"] == reason
    assert payload["source"] == source
    assert payload["table_present"] is table_present
    assert payload["meta"]["ready"] is False
    assert payload["meta"]["reason"] == reason
    assert payload["meta"]["source"] == source
    assert payload["meta"]["table_present"] is table_present


def test_dashboard_empty_read_markers_distinguish_missing_from_present_empty(tmp_path, monkeypatch):
    dashboard_server = _reload_dashboard(monkeypatch)

    missing_db = tmp_path / "missing.db"
    sqlite3.connect(str(missing_db)).close()
    _set_dashboard_db(monkeypatch, dashboard_server, missing_db)

    _assert_marker(
        dashboard_server.api_get_promotion_audit({"limit": "5"}),
        reason="model_promotion_audit_table_missing",
        source="model_promotion_audit",
        table_present=False,
    )
    _assert_marker(
        dashboard_server.api_get_strategy_metrics({"limit": "5"}),
        reason="strategy_metrics_table_missing",
        source="strategy_metrics",
        table_present=False,
    )
    _assert_marker(
        dashboard_server.api_get_causal_scores({"limit": "5"}),
        reason="causal_scores_table_missing",
        source="causal_scores",
        table_present=False,
    )
    _assert_marker(
        dashboard_server.api_get_market_stress_history({"limit": "10"}),
        reason="prices_table_missing",
        source="prices",
        table_present=False,
    )

    empty_db = tmp_path / "empty.db"
    with sqlite3.connect(str(empty_db)) as con:
        con.executescript(
            """
            CREATE TABLE model_promotion_audit (
              ts_ms INTEGER,
              actor TEXT,
              action TEXT,
              model_name TEXT,
              regime TEXT,
              reason_json TEXT
            );
            CREATE TABLE strategy_metrics (
              strategy_name TEXT,
              window_days INTEGER,
              ts_ms INTEGER,
              metrics_json TEXT
            );
            CREATE TABLE causal_scores (
              feature TEXT,
              target TEXT,
              window TEXT,
              ts INTEGER,
              granger_p REAL,
              granger_lag INTEGER,
              dowhy_effect REAL,
              dowhy_p REAL,
              score REAL,
              decision TEXT
            );
            CREATE TABLE prices (
              symbol TEXT,
              ts_ms INTEGER,
              price REAL
            );
            """
        )
    _set_dashboard_db(monkeypatch, dashboard_server, empty_db)

    _assert_marker(
        dashboard_server.api_get_promotion_audit({"limit": "5"}),
        reason="no_promotions_yet",
        source="model_promotion_audit",
        table_present=True,
    )
    _assert_marker(
        dashboard_server.api_get_strategy_metrics({"limit": "5"}),
        reason="no_strategy_metrics_yet",
        source="strategy_metrics",
        table_present=True,
    )
    _assert_marker(
        dashboard_server.api_get_causal_scores({"limit": "5"}),
        reason="no_causal_scores_yet",
        source="causal_scores",
        table_present=True,
    )
    _assert_marker(
        dashboard_server.api_get_market_stress_history({"limit": "10"}),
        reason="no_market_stress_history_yet",
        source="prices",
        table_present=True,
    )


def test_dashboard_populated_read_shapes_remain_legacy_lists(tmp_path, monkeypatch):
    dashboard_server = _reload_dashboard(monkeypatch)
    db_path = tmp_path / "populated.db"
    with sqlite3.connect(str(db_path)) as con:
        con.executescript(
            """
            CREATE TABLE model_promotion_audit (
              ts_ms INTEGER,
              actor TEXT,
              action TEXT,
              model_name TEXT,
              regime TEXT,
              reason_json TEXT
            );
            CREATE TABLE strategy_metrics (
              strategy_name TEXT,
              window_days INTEGER,
              ts_ms INTEGER,
              metrics_json TEXT
            );
            CREATE TABLE causal_scores (
              feature TEXT,
              target TEXT,
              window TEXT,
              ts INTEGER,
              granger_p REAL,
              granger_lag INTEGER,
              dowhy_effect REAL,
              dowhy_p REAL,
              score REAL,
              decision TEXT
            );
            CREATE TABLE prices (
              symbol TEXT,
              ts_ms INTEGER,
              price REAL
            );
            """
        )
        con.execute(
            "INSERT INTO model_promotion_audit(ts_ms, actor, action, model_name, regime, reason_json) VALUES (?, ?, ?, ?, ?, ?)",
            (100, "test", "promote", "model_a", "global", json.dumps({"why": "test"})),
        )
        con.execute(
            "INSERT INTO strategy_metrics(strategy_name, window_days, ts_ms, metrics_json) VALUES (?, ?, ?, ?)",
            ("strategy_a", 7, 101, json.dumps({"sharpe": 1.25, "net_calmar": 0.8, "turnover": 0.1})),
        )
        con.execute(
            """
            INSERT INTO causal_scores(feature, target, window, ts, granger_p, granger_lag, dowhy_effect, dowhy_p, score, decision)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("f1", "ret", "1d", 102, 0.03, 2, 0.4, 0.05, 0.7, "pass"),
        )
        con.executemany(
            "INSERT INTO prices(symbol, ts_ms, price) VALUES (?, ?, ?)",
            [("VIX", 100, 15.0), ("VIX", 101, 16.0)],
        )
    _set_dashboard_db(monkeypatch, dashboard_server, db_path)

    promotion = dashboard_server.api_get_promotion_audit({"limit": "5"})
    strategy = dashboard_server.api_get_strategy_metrics({"limit": "5"})
    causal = dashboard_server.api_get_causal_scores({"limit": "5"})
    stress = dashboard_server.api_get_market_stress_history({"limit": "10"})

    assert isinstance(promotion, list)
    assert promotion[0]["model_name"] == "model_a"
    assert isinstance(strategy, list)
    assert strategy[0]["strategy"] == "strategy_a"
    assert isinstance(causal, list)
    assert causal[0]["feature"] == "f1"
    assert stress["ok"] is True
    assert len(stress["series"]) == 2
    assert "reason" not in stress


def test_promotion_explain_and_governance_summary_include_audit_marker(tmp_path, monkeypatch):
    dashboard_server = _reload_dashboard(monkeypatch)
    import engine.api.api_governance as api_governance

    db_path = tmp_path / "audit_siblings.db"
    with sqlite3.connect(str(db_path)) as con:
        con.executescript(
            """
            CREATE TABLE model_promotion_audit (
              ts_ms INTEGER,
              actor TEXT,
              action TEXT,
              model_name TEXT,
              regime TEXT,
              reason_json TEXT
            );
            """
        )
    _set_dashboard_db(monkeypatch, dashboard_server, db_path)
    monkeypatch.setattr(api_governance, "get_promotion_explain", lambda: {"ok": True, "audit": []})
    monkeypatch.setattr(api_governance, "get_governance_summary", lambda: {"ok": True, "audit": []})

    promotion = dashboard_server.api_get_promotion_explain()
    governance = dashboard_server.api_get_governance_summary()

    assert promotion["audit_ready"] is False
    assert promotion["audit_reason"] == "no_promotions_yet"
    assert promotion["audit_meta"]["table_present"] is True
    assert governance["audit_ready"] is False
    assert governance["audit_reason"] == "no_promotions_yet"
    assert governance["audit_meta"]["source"] == "model_promotion_audit"


def test_relevance_stats_empty_markers_distinguish_labels_table_state(tmp_path, monkeypatch):
    import engine.api.internal_access as internal_access
    import engine.strategy.relevance as relevance

    relevance = importlib.reload(relevance)
    monkeypatch.setattr(relevance, "_compute_relevance_stats_with_timeout", lambda _timeout_s: {})

    missing_db = tmp_path / "relevance_missing.db"
    sqlite3.connect(str(missing_db)).close()
    monkeypatch.setattr(internal_access, "db_connect", lambda **_kwargs: sqlite3.connect(str(missing_db)))
    relevance._relevance_cache.update({"ts": 0.0, "value": None})
    missing = relevance.get_relevance_stats()
    _assert_marker(missing, reason="labels_table_missing", source="labels", table_present=False)
    assert missing["stats"] == {}

    labels_db = tmp_path / "relevance_empty.db"
    with sqlite3.connect(str(labels_db)) as con:
        con.execute("CREATE TABLE labels(symbol TEXT, horizon_s INTEGER, impact_z REAL)")
    monkeypatch.setattr(internal_access, "db_connect", lambda **_kwargs: sqlite3.connect(str(labels_db)))
    relevance._relevance_cache.update({"ts": 0.0, "value": None})
    empty = relevance.get_relevance_stats()
    _assert_marker(empty, reason="relevance_stats_no_labels_yet", source="labels", table_present=True)
    assert empty["stats"] == {}

    stats = {"AAPL:300": {"relevance": 1.0, "n": 3}}
    monkeypatch.setattr(relevance, "_compute_relevance_stats_with_timeout", lambda _timeout_s: stats)
    relevance._relevance_cache.update({"ts": 0.0, "value": None})
    populated = relevance.get_relevance_stats()
    assert populated == {"ok": True, "cached": False, "stats": stats}


def test_size_policy_empty_markers_distinguish_missing_from_untrained(tmp_path, monkeypatch):
    import engine.api.internal_access as internal_access
    import engine.api.api_read_advanced as api_read_advanced

    api_read_advanced = importlib.reload(api_read_advanced)

    missing_db = tmp_path / "size_policy_missing.db"
    sqlite3.connect(str(missing_db)).close()
    monkeypatch.setattr(internal_access, "db_connect", lambda **_kwargs: sqlite3.connect(str(missing_db)))
    missing = api_read_advanced.get_size_policy()
    _assert_marker(missing, reason="size_policy_table_missing", source="size_policy", table_present=False)
    assert missing["policy"] is None
    assert missing["points"] == []

    empty_db = tmp_path / "size_policy_empty.db"
    with sqlite3.connect(str(empty_db)) as con:
        con.executescript(
            """
            CREATE TABLE size_policy (
              id INTEGER PRIMARY KEY,
              ts_ms INTEGER,
              lookback_days INTEGER,
              buckets INTEGER,
              method TEXT,
              params_json TEXT,
              metrics_json TEXT
            );
            CREATE TABLE size_policy_points (
              policy_id INTEGER,
              bucket_idx INTEGER,
              conf_lo REAL,
              conf_hi REAL,
              n INTEGER,
              mean_net_ret REAL,
              std_net_ret REAL,
              factor REAL
            );
            """
        )
    monkeypatch.setattr(internal_access, "db_connect", lambda **_kwargs: sqlite3.connect(str(empty_db)))
    empty = api_read_advanced.get_size_policy()
    _assert_marker(empty, reason="size_policy_untrained", source="size_policy", table_present=True)
    assert empty["policy"] is None
    assert empty["points"] == []
