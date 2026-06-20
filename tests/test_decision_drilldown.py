import json
import shutil
import sqlite3
import subprocess
from pathlib import Path

import pytest

from engine.api import api_dashboard_reads, api_read_advanced


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_decision_json_decode_failure_warns_and_preserves_raw(monkeypatch) -> None:
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    monkeypatch.setattr(api_read_advanced, "_warn_nonfatal", lambda *args, **kwargs: calls.append((args, kwargs)))

    row = api_read_advanced._decision_decode_json_fields({"decision_json": "{bad-json", "symbol": "AAPL"})

    assert row["decision_json"] == "{bad-json"
    assert row["symbol"] == "AAPL"
    assert calls
    assert calls[0][0][0] == "API_READ_ADVANCED_DECISION_JSON_DECODE_FAILED"
    assert calls[0][1]["field"] == "decision_json"


def _init_decision_drilldown_db(db_path: Path) -> None:
    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(
            """
            CREATE TABLE alerts (
                id INTEGER PRIMARY KEY,
                event_id INTEGER,
                symbol TEXT,
                horizon_s INTEGER,
                ts_ms INTEGER,
                event_title TEXT,
                title TEXT,
                message TEXT
            );

            CREATE TABLE decision_log (
                id INTEGER PRIMARY KEY,
                event_id INTEGER,
                symbol TEXT,
                horizon_s INTEGER,
                ts_ms INTEGER,
                action TEXT,
                model_name TEXT,
                model_version TEXT,
                confidence REAL,
                why TEXT,
                risk_impact TEXT,
                feature_set_tag TEXT,
                explain_json TEXT,
                extra_json TEXT
            );

            CREATE TABLE portfolio_orders (
                id INTEGER PRIMARY KEY,
                source_alert_id INTEGER,
                ts_ms INTEGER,
                symbol TEXT,
                action TEXT,
                delta_weight REAL,
                client_order_id TEXT
            );

            CREATE TABLE execution_policy_audit (
                id INTEGER PRIMARY KEY,
                source_alert_id INTEGER,
                portfolio_orders_batch_id INTEGER,
                ts_ms INTEGER,
                decision TEXT,
                suppression_state TEXT,
                decision_json TEXT
            );

            CREATE TABLE execution_orders (
                id INTEGER PRIMARY KEY,
                source_alert_id INTEGER,
                portfolio_orders_id INTEGER,
                client_order_id TEXT,
                submit_ts_ms INTEGER,
                updated_ts_ms INTEGER,
                symbol TEXT,
                broker TEXT,
                status TEXT,
                side TEXT,
                qty REAL
            );

            CREATE TABLE execution_fills (
                id INTEGER PRIMARY KEY,
                source_alert_id INTEGER,
                client_order_id TEXT,
                fill_ts_ms INTEGER,
                fill_px REAL,
                fill_qty REAL,
                symbol TEXT
            );

            CREATE TABLE trade_attribution_ledger (
                id INTEGER PRIMARY KEY,
                source_alert_id INTEGER,
                portfolio_orders_id INTEGER,
                client_order_id TEXT,
                ts_ms INTEGER,
                symbol TEXT,
                suppression_reason TEXT,
                decision_json TEXT,
                pnl REAL
            );

            CREATE TABLE prediction_explanations (
                id INTEGER PRIMARY KEY,
                symbol TEXT,
                ts INTEGER,
                model_family TEXT,
                model_name TEXT,
                version TEXT,
                explanation_type TEXT,
                top_features TEXT,
                base_value REAL,
                diagnostics TEXT,
                created_ts INTEGER
            );

            INSERT INTO alerts (
                id, event_id, symbol, horizon_s, ts_ms, event_title, title, message
            ) VALUES (
                10, 9001, 'AAPL', 3600, 1700000000000,
                'AAPL source signal', 'AAPL alert', 'source alert body'
            );

            INSERT INTO decision_log (
                id, event_id, symbol, horizon_s, ts_ms, action, model_name,
                model_version, confidence, why, risk_impact, feature_set_tag,
                explain_json, extra_json
            ) VALUES (
                1, 9001, 'AAPL', 3600, 1700000000100, 'buy',
                'temporal_predictor', '2026.05.10', 0.72,
                'stored decision explanation', 'medium', 'features:v1',
                '{"top_feature":"news_score"}', '{"model_version":"2026.05.10"}'
            );

            INSERT INTO portfolio_orders (
                id, source_alert_id, ts_ms, symbol, action, delta_weight, client_order_id
            ) VALUES (
                301, 10, 1700000000200, 'AAPL', 'buy', 0.015, 'po-301'
            );

            INSERT INTO execution_policy_audit (
                id, source_alert_id, portfolio_orders_batch_id, ts_ms,
                decision, suppression_state, decision_json
            ) VALUES (
                201, 10, 301, 1700000000300,
                'blocked', 'suppressed', '{"blocked_by":"max_position"}'
            );

            INSERT INTO trade_attribution_ledger (
                id, source_alert_id, portfolio_orders_id, client_order_id, ts_ms,
                symbol, suppression_reason, decision_json, pnl
            ) VALUES (
                401, 10, 301, 'po-301', 1700000000400,
                'AAPL', 'max_position', '{"reason":"max_position"}', NULL
            );
            """
        )
        con.commit()
    finally:
        con.close()


def test_decision_detail_aggregates_existing_records(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "decision_drilldown.sqlite"
    _init_decision_drilldown_db(db_path)

    def connect():
        return sqlite3.connect(str(db_path))

    def fetch_decision_detail(decision_id: int):
        if int(decision_id) != 1:
            return None
        con = sqlite3.connect(str(db_path))
        try:
            cur = con.execute("SELECT * FROM decision_log WHERE id=?", (int(decision_id),))
            row = cur.fetchone()
            columns = [item[0] for item in cur.description]
            return dict(zip(columns, row)) if row else None
        finally:
            con.close()

    monkeypatch.setattr(api_read_advanced, "db_connect", connect)
    monkeypatch.setattr(api_read_advanced, "fetch_decision_detail", fetch_decision_detail)

    payload = api_read_advanced.get_decision_detail(1)

    assert payload["ok"] is True
    assert payload["decision"]["decision_id"] == 1
    assert payload["meta"]["detail_version"] == 1
    assert payload["meta"]["source_alert_id"] == 10
    assert payload["related"]["alert"]["id"] == 10
    assert payload["related"]["trade_attribution_ledger"][0]["suppression_reason"] == "max_position"

    stages = {stage["key"]: stage for stage in payload["stages"]}
    assert {"source", "model", "portfolio", "policy", "route", "outcome"} <= set(stages)
    assert stages["source"]["status"] == "available"
    assert stages["model"]["status"] == "available"
    assert "2026.05.10" in stages["model"]["summary"]
    assert "72%" in stages["model"]["summary"]
    assert stages["portfolio"]["status"] == "available"
    assert stages["policy"]["summary"] == "max_position"
    assert stages["route"]["status"] == "suppressed"
    assert stages["outcome"]["status"] == "suppressed"
    assert payload["attribution"]["available"] is False
    assert "No backend feature-contribution payload" in payload["attribution"]["unavailable_reason"]


def test_decision_detail_surfaces_prediction_explanation_contributions(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "decision_drilldown.sqlite"
    _init_decision_drilldown_db(db_path)

    explanation = {
        "prediction_explanation": {
            "available": True,
            "explanation_type": "shap",
            "is_shap": True,
            "model_family": "gbm_regressor",
            "top_features": [
                {"feature_id": "news_score", "attribution": 0.32, "value": 1.4},
                {"feature_id": "drawdown_pressure", "attribution": -0.12, "value": -0.6},
            ],
        }
    }
    con = sqlite3.connect(str(db_path))
    try:
        con.execute(
            "UPDATE decision_log SET model_name=?, explain_json=? WHERE id=1",
            ("gbm_regressor.live", json.dumps(explanation)),
        )
        con.commit()
    finally:
        con.close()

    def connect():
        return sqlite3.connect(str(db_path))

    def fetch_decision_detail(decision_id: int):
        con = sqlite3.connect(str(db_path))
        try:
            cur = con.execute("SELECT * FROM decision_log WHERE id=?", (int(decision_id),))
            row = cur.fetchone()
            columns = [item[0] for item in cur.description]
            return dict(zip(columns, row)) if row else None
        finally:
            con.close()

    monkeypatch.setattr(api_read_advanced, "db_connect", connect)
    monkeypatch.setattr(api_read_advanced, "fetch_decision_detail", fetch_decision_detail)

    payload = api_read_advanced.get_decision_detail(1)

    assert payload["ok"] is True
    attribution = payload["attribution"]
    assert attribution["available"] is True
    assert attribution["source"] == "decision.explain.prediction_explanation"
    assert attribution["explanation_type"] == "shap"
    assert attribution["is_shap"] is True
    assert [row["feature_id"] for row in attribution["top_features"]] == [
        "news_score",
        "drawdown_pressure",
    ]


def test_decision_detail_uses_persisted_prediction_explanation_fallback(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "decision_drilldown.sqlite"
    _init_decision_drilldown_db(db_path)
    con = sqlite3.connect(str(db_path))
    try:
        con.execute(
            """
            INSERT INTO prediction_explanations(
                symbol, ts, model_family, model_name, version, explanation_type,
                top_features, base_value, diagnostics, created_ts
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "AAPL",
                1700000000100,
                "gbm_regressor",
                "temporal_predictor",
                "2026.05.10",
                "feature_value_proxy",
                json.dumps([
                    {"feature_id": "momentum_5m", "attribution": 0.5, "value": 2.0},
                ]),
                0.0,
                json.dumps({"feature_set_tag": "features:v1"}),
                1700000000101,
            ),
        )
        con.commit()
    finally:
        con.close()

    def connect():
        return sqlite3.connect(str(db_path))

    def fetch_decision_detail(decision_id: int):
        con = sqlite3.connect(str(db_path))
        try:
            cur = con.execute("SELECT * FROM decision_log WHERE id=?", (int(decision_id),))
            row = cur.fetchone()
            columns = [item[0] for item in cur.description]
            return dict(zip(columns, row)) if row else None
        finally:
            con.close()

    monkeypatch.setattr(api_read_advanced, "db_connect", connect)
    monkeypatch.setattr(api_read_advanced, "fetch_decision_detail", fetch_decision_detail)

    payload = api_read_advanced.get_decision_detail(1)

    assert payload["ok"] is True
    assert payload["attribution"]["available"] is True
    assert payload["attribution"]["source"] == "prediction_explanations"
    assert payload["attribution"]["top_features"][0]["feature_id"] == "momentum_5m"


def test_decision_detail_enriches_structured_doc_and_graph_feature_lineage(monkeypatch, tmp_path: Path) -> None:
    from engine.data import structured_document_events as structured
    from engine.strategy.graph_relational import (
        GRAPH_RELATIONAL_FEATURE_IDS,
        GRAPH_RELATIONAL_GRAPH_ID,
        GRAPH_RELATIONAL_GROUP,
        GRAPH_RELATIONAL_SNAPSHOT_VERSION,
        ensure_graph_relational_schema,
        store_graph_relational_snapshots,
    )

    db_path = tmp_path / "decision_feature_visibility.sqlite"
    _init_decision_drilldown_db(db_path)
    con = sqlite3.connect(str(db_path))
    try:
        structured.ensure_structured_document_event_schema(con)
        structured.put_structured_document_events(
            con,
            [
                {
                    "source_document_id": "doc-decision",
                    "source_event_id": 10,
                    "symbol": "AAPL",
                    "document_type": "filing",
                    "source": "sec",
                    "event_type": "guidance_cut",
                    "event_ts_ms": 1700000000000,
                    "availability_ts_ms": 1700000000000,
                    "extraction_confidence": 0.71,
                    "polarity": -1.0,
                    "feature_id": structured.EVENT_FEATURE_ID["guidance_cut"],
                    "evidence": "lowered guidance",
                    "extractor_name": structured.EXTRACTOR_NAME,
                    "extractor_version": structured.EXTRACTOR_VERSION,
                    "created_ts_ms": 1700000000001,
                    "pit_metadata_json": {"availability_ts_ms": 1700000000000},
                }
            ],
        )
        ensure_graph_relational_schema(con)
        store_graph_relational_snapshots(
            [
                {
                    "symbol": "AAPL",
                    "ts_ms": 1700000000100,
                    "graph_id": GRAPH_RELATIONAL_GRAPH_ID,
                    "snapshot_version": GRAPH_RELATIONAL_SNAPSHOT_VERSION,
                    "feature_ids": list(GRAPH_RELATIONAL_FEATURE_IDS),
                    "features": {GRAPH_RELATIONAL_FEATURE_IDS[0]: 3.0},
                    "edge_counts": {"sector": 1},
                    "relationships": [],
                    "source_timestamps": {
                        "max_source_ts_ms": 1700000000000,
                        "max_availability_ts_ms": 1700000000000,
                        "relationship_hash": "decision-hash",
                    },
                    "availability": {GRAPH_RELATIONAL_GROUP: True},
                    "metadata": {
                        "graph_id": GRAPH_RELATIONAL_GRAPH_ID,
                        "snapshot_version": GRAPH_RELATIONAL_SNAPSHOT_VERSION,
                        "feature_ids": list(GRAPH_RELATIONAL_FEATURE_IDS),
                        "relationship_hash": "decision-hash",
                        "snapshot_available": True,
                        "pit_safe": True,
                        "max_source_ts_ms": 1700000000000,
                        "max_availability_ts_ms": 1700000000000,
                        "direct_trading_authority": False,
                        "stage": "shadow",
                    },
                }
            ],
            con=con,
        )
        explanation = {
            "prediction_explanation": {
                "available": True,
                "explanation_type": "feature_value_proxy",
                "model_family": "gbm_regressor",
                "top_features": [
                    {
                        "feature_id": structured.EVENT_FEATURE_ID["guidance_cut"],
                        "attribution": -0.22,
                        "value": 0.71,
                    },
                    {
                        "feature_id": GRAPH_RELATIONAL_FEATURE_IDS[0],
                        "attribution": 0.18,
                        "value": 3.0,
                    },
                ],
            }
        }
        con.execute("UPDATE decision_log SET model_name=?, explain_json=? WHERE id=1", ("gbm_regressor.live", json.dumps(explanation)))
        con.commit()
    finally:
        con.close()

    monkeypatch.setattr(api_read_advanced, "db_connect", lambda: sqlite3.connect(str(db_path)))

    def fetch_decision_detail(decision_id: int):
        local = sqlite3.connect(str(db_path))
        try:
            cur = local.execute("SELECT * FROM decision_log WHERE id=?", (int(decision_id),))
            row = cur.fetchone()
            columns = [item[0] for item in cur.description]
            return dict(zip(columns, row)) if row else None
        finally:
            local.close()

    monkeypatch.setattr(api_read_advanced, "fetch_decision_detail", fetch_decision_detail)

    payload = api_read_advanced.get_decision_detail(1)

    assert payload["ok"] is True
    rows = payload["attribution"]["top_features"]
    structured_row = next(row for row in rows if row["feature_id"].startswith("structured_doc_events_v1."))
    graph_row = next(row for row in rows if row["feature_id"].startswith("graph.relational_v1."))
    assert structured_row["feature_visibility"]["shadow_only"] is True
    assert structured_row["feature_visibility"]["point_in_time_valid"] is True
    assert structured_row["feature_visibility"]["lineage"][0]["source_artifact"] == "structured_document_events:doc-decision"
    assert graph_row["feature_visibility"]["status"] == "shadow_only"
    assert graph_row["feature_visibility"]["source_artifact"].startswith("graph_relational_snapshots:AAPL:")
    assert payload["attribution"]["feature_visibility_summary"]["annotated_feature_count"] == 2


def test_decision_detail_handles_missing_and_lineage_lookup(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "decision_drilldown.sqlite"
    _init_decision_drilldown_db(db_path)

    monkeypatch.setattr(api_read_advanced, "db_connect", lambda: sqlite3.connect(str(db_path)))
    monkeypatch.setattr(api_read_advanced, "fetch_decision_detail", lambda _decision_id: None)

    missing = api_read_advanced.get_decision_detail(404)
    assert missing == {"ok": False, "error": "decision_not_found", "decision": None}

    by_alert = api_read_advanced.get_decision_detail(0, source_alert_id=10)
    assert by_alert["ok"] is True
    assert by_alert["decision"]["decision_id"] == 1
    assert by_alert["meta"]["source_alert_id"] == 10


def test_dashboard_decision_handler_accepts_lineage_identifiers(monkeypatch) -> None:
    captured = {}

    def fake_get_decision_detail(decision_id: int, **kwargs):
        captured["decision_id"] = decision_id
        captured["kwargs"] = kwargs
        return {"ok": True, "decision": {"decision_id": decision_id or None}, "stages": []}

    monkeypatch.setattr(api_dashboard_reads, "get_decision_detail", fake_get_decision_detail)

    assert api_dashboard_reads.api_get_decision_detail({}) == {"ok": False, "error": "missing_id"}

    payload = api_dashboard_reads.api_get_decision_detail({
        "source_alert_id": "10",
        "portfolio_order_id": "301",
        "ledger_id": "401",
        "client_order_id": "po-301",
    })

    assert payload["ok"] is True
    assert captured == {
        "decision_id": 0,
        "kwargs": {
            "source_alert_id": 10,
            "portfolio_order_id": 301,
            "ledger_id": 401,
            "client_order_id": "po-301",
        },
    }


def test_frontend_decision_helper_renders_available_and_unavailable_stages() -> None:
    if not shutil.which("node"):
        pytest.skip("node executable is not available")

    script = """
        import {
          buildDecisionDetailUrl,
          buildDecisionStageRows,
          hasDecisionLookup
        } from './ui/decision_drilldown.mjs';

        const rows = buildDecisionStageRows({
          stages: [
            { key: 'model', label: 'Model decision', status: 'available', summary: 'model ok' },
            {
              key: 'route',
              label: 'Route',
              status: 'unavailable',
              unavailable_reason: 'execution_route_unavailable'
            }
          ]
        });
        const fallback = buildDecisionStageRows({});
        process.stdout.write(JSON.stringify({
          rows,
          fallback,
          lookup: hasDecisionLookup({ sourceAlertId: 10 }),
          url: buildDecisionDetailUrl({
            sourceAlertId: 10,
            portfolioOrderId: 301,
            ledgerId: 401,
            clientOrderId: 'po-301'
          })
        }));
    """
    result = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["rows"][0]["value"] == "available"
    assert payload["rows"][1]["value"] == "unavailable"
    assert payload["rows"][1]["meta"] == "execution_route_unavailable"
    assert payload["fallback"][0]["value"] == "unavailable"
    assert payload["lookup"] is True
    assert payload["url"] == (
        "/api/ui/decision?source_alert_id=10&portfolio_order_id=301"
        "&ledger_id=401&client_order_id=po-301"
    )
