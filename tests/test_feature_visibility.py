from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
from pathlib import Path

import pytest

from engine.api import api_dashboard_reads
from engine.api.feature_visibility import build_feature_visibility
from engine.data import structured_document_events as structured
from engine.strategy.graph_relational import (
    GRAPH_RELATIONAL_FEATURE_IDS,
    GRAPH_RELATIONAL_GRAPH_ID,
    GRAPH_RELATIONAL_GROUP,
    GRAPH_RELATIONAL_SNAPSHOT_VERSION,
    ensure_graph_relational_schema,
    store_graph_relational_snapshots,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
MS_DAY = 24 * 60 * 60 * 1000


def _conn() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    return con


def _seed_feature_visibility(con: sqlite3.Connection, *, anchor: int = 1_700_000_000_000) -> None:
    structured.ensure_structured_document_event_schema(con)
    structured.put_structured_document_events(
        con,
        [
            {
                "source_document_id": "doc-low",
                "source_event_id": 11,
                "symbol": "AAPL",
                "document_type": "filing",
                "source": "sec",
                "event_type": "guidance_cut",
                "event_ts_ms": anchor - 10_000,
                "availability_ts_ms": anchor - 9_000,
                "extraction_confidence": 0.55,
                "polarity": -1.0,
                "feature_id": structured.EVENT_FEATURE_ID["guidance_cut"],
                "evidence": "guidance lowered",
                "extractor_name": structured.EXTRACTOR_NAME,
                "extractor_version": structured.EXTRACTOR_VERSION,
                "created_ts_ms": anchor - 8_000,
                "pit_metadata_json": {"availability_ts_ms": anchor - 9_000},
            },
            {
                "source_document_id": "doc-high",
                "source_event_id": 12,
                "symbol": "MSFT",
                "document_type": "transcript",
                "source": "fmp_transcript",
                "event_type": "guidance_raise",
                "event_ts_ms": anchor - 7_000,
                "availability_ts_ms": anchor - 6_000,
                "extraction_confidence": 0.88,
                "polarity": 1.0,
                "feature_id": structured.EVENT_FEATURE_ID["guidance_raise"],
                "evidence": "guidance raised",
                "extractor_name": structured.EXTRACTOR_NAME,
                "extractor_version": structured.EXTRACTOR_VERSION,
                "created_ts_ms": anchor - 5_000,
                "pit_metadata_json": {"availability_ts_ms": anchor - 6_000},
            },
        ],
    )
    con.execute(
        """
        CREATE TABLE event_log(
          id INTEGER PRIMARY KEY,
          ts_ms INTEGER,
          event_type TEXT,
          event_source TEXT,
          event_version INTEGER,
          entity_type TEXT,
          entity_id TEXT,
          correlation_id TEXT,
          payload_json TEXT
        )
        """
    )
    con.execute(
        """
        INSERT INTO event_log(
          ts_ms, event_type, event_source, event_version, entity_type, entity_id, correlation_id, payload_json
        ) VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            anchor - 4_000,
            "runtime_failure",
            "structured_document_events",
            1,
            "failure",
            "STRUCTURED_DOCUMENT_EVENT_EXTRACTION_FAILED",
            None,
            "{}",
        ),
    )
    ensure_graph_relational_schema(con)
    store_graph_relational_snapshots(
        [
            {
                "symbol": "AAPL",
                "ts_ms": anchor,
                "graph_id": GRAPH_RELATIONAL_GRAPH_ID,
                "snapshot_version": GRAPH_RELATIONAL_SNAPSHOT_VERSION,
                "feature_ids": list(GRAPH_RELATIONAL_FEATURE_IDS),
                "features": {GRAPH_RELATIONAL_FEATURE_IDS[0]: 2.0},
                "edge_counts": {"sector": 1, "supply_chain": 1},
                "relationships": [
                    {
                        "source_symbol": "AAPL",
                        "target_symbol": "MSFT",
                        "relationship_type": "sector",
                        "weight": 1.0,
                        "source_ts_ms": anchor - 2_000,
                        "availability_ts_ms": anchor - 1_000,
                    }
                ],
                "source_timestamps": {
                    "max_source_ts_ms": anchor - 2_000,
                    "max_availability_ts_ms": anchor - 1_000,
                    "relationship_hash": "hash-aapl",
                },
                "availability": {GRAPH_RELATIONAL_GROUP: True},
                "metadata": {
                    "graph_id": GRAPH_RELATIONAL_GRAPH_ID,
                    "snapshot_version": GRAPH_RELATIONAL_SNAPSHOT_VERSION,
                    "feature_ids": list(GRAPH_RELATIONAL_FEATURE_IDS),
                    "relationship_hash": "hash-aapl",
                    "snapshot_available": True,
                    "pit_safe": True,
                    "max_source_ts_ms": anchor - 2_000,
                    "max_availability_ts_ms": anchor - 1_000,
                    "direct_trading_authority": False,
                    "stage": "shadow",
                },
            }
        ],
        con=con,
    )
    con.commit()


def test_feature_visibility_payload_reports_structured_and_graph_health(monkeypatch) -> None:
    monkeypatch.setenv("USE_GRAPH_RELATIONAL_FEATURES", "1")
    anchor = 1_700_000_000_000
    con = _conn()
    _seed_feature_visibility(con, anchor=anchor)

    payload = build_feature_visibility(con=con, now_ms=anchor + 60_000)

    assert payload["ok"] is True
    structured_payload = payload["structured_documents"]
    assert structured_payload["counts"]["events"] == 2
    assert structured_payload["counts"]["low_confidence"] == 1
    assert structured_payload["extraction_failures"]["count"] == 1
    assert structured_payload["lineage"]["source_documents"][0]["source_artifact"].startswith("structured_document_events:")
    assert {row["event_type"] for row in structured_payload["coverage"]["event_types"]} >= {"guidance_cut", "guidance_raise"}

    graph_payload = payload["graph_features"]
    assert graph_payload["status"] == "shadow_only"
    assert graph_payload["shadow_only"] is True
    assert graph_payload["direct_trading_authority"] is False
    assert graph_payload["pit_status"]["latest_snapshot_pit_safe"] is True
    assert graph_payload["feature_availability"]["observed_feature_count"] == len(GRAPH_RELATIONAL_FEATURE_IDS)
    assert graph_payload["snapshots"][0]["source_artifact"].startswith("graph_relational_snapshots:AAPL:")


def test_feature_visibility_surfaces_missing_and_stale_states() -> None:
    empty = build_feature_visibility(con=_conn(), now_ms=1_700_000_000_000)
    assert empty["structured_documents"]["status"] == "unavailable"
    assert "structured_document_events table unavailable" in empty["structured_documents"]["warnings"]
    assert empty["graph_features"]["status"] == "unavailable"
    assert "graph_relational_snapshots table unavailable" in empty["graph_features"]["warnings"]

    anchor = 1_700_000_000_000
    con = _conn()
    _seed_feature_visibility(con, anchor=anchor)
    stale = build_feature_visibility(con=con, now_ms=anchor + 181 * MS_DAY)

    assert stale["structured_documents"]["status"] == "stale"
    assert "feature_stale" in stale["structured_documents"]["pit_status"]["reason_codes"]
    assert stale["graph_features"]["status"] == "stale"
    assert "feature_stale" in stale["graph_features"]["pit_status"]["reason_codes"]


def test_feature_visibility_route_parses_query_options(monkeypatch) -> None:
    captured = {}

    def fake_build_feature_visibility(**kwargs):
        captured.update(kwargs)
        return {"ok": True, "meta": {"ready": True}}

    monkeypatch.setattr(api_dashboard_reads, "build_feature_visibility", fake_build_feature_visibility)

    payload = api_dashboard_reads.api_get_feature_visibility(
        {"symbol": "aapl", "limit": "7", "low_confidence_threshold": "0.72"}
    )

    assert payload["ok"] is True
    assert captured["symbol"] == "AAPL"
    assert captured["lineage_limit"] == 7
    assert captured["low_confidence_threshold"] == 0.72


def test_feature_visibility_documentation_examples_are_published() -> None:
    data_contracts = (REPO_ROOT / "docs" / "DATA_CONTRACTS.md").read_text()
    api_readme = (REPO_ROOT / "engine" / "api" / "README.md").read_text()

    assert "/api/data/feature_visibility" in data_contracts
    assert '"structured_documents"' in data_contracts
    assert '"graph_features"' in data_contracts
    assert "/api/data/feature_visibility" in api_readme


def test_frontend_feature_visibility_and_decision_rendering() -> None:
    if not shutil.which("node"):
        pytest.skip("node executable is not available")

    script = """
      import { buildFeatureVisibilityMarkup } from './ui/feature_visibility.js';
      import { renderDecisionAttribution } from './ui/decision_attribution.js';

      const payload = {
        structured_documents: {
          status: 'available',
          shadow_only: true,
          latest_availability_age_ms: 120000,
          counts: { events: 2, source_documents: 1, symbols: 1, low_confidence: 1 },
          latest_extraction_ts_ms: 1700000000000,
          latest_availability_ts_ms: 1700000000000,
          confidence: {
            low_confidence_threshold: 0.6,
            buckets: [{ label: 'low', count: 1 }, { label: 'high', count: 1 }]
          },
          pit_status: { ok: true, reason_codes: [] },
          extraction_failures: { available: true, count: 0 },
          coverage: {
            event_types: [{ event_type: 'guidance_cut', count: 1, latest_ts_ms: 1700000000000 }],
            symbols: [{ symbol: 'AAPL', count: 1, latest_ts_ms: 1700000000000 }]
          },
          lineage: {
            source_documents: [{
              source_artifact: 'structured_document_events:doc-1',
              symbol: 'AAPL',
              event_type: 'guidance_cut',
              extraction_confidence: 0.71,
              availability_ts_ms: 1700000000000
            }]
          },
          warnings: []
        },
        graph_features: {
          status: 'shadow_only',
          enabled: false,
          shadow_only: true,
          graph_id: 'graph_relational_v1',
          snapshot_version: 1,
          latest_snapshot_ts_ms: 1700000000000,
          latest_snapshot_age_ms: 120000,
          counts: { snapshots: 1, symbols: 1 },
          feature_availability: { observed_feature_count: 12, expected_feature_count: 12 },
          snapshot_freshness: { age_ms: 120000 },
          pit_status: { ok: true, pit_valid_snapshot_count: 1, pit_invalid_snapshot_count: 0 },
          coverage: { relationship_types: [{ relationship_type: 'sector', count: 1 }] },
          snapshots: [{ source_artifact: 'graph_relational_snapshots:AAPL:1700000000000:graph_relational_v1', relationship_hash: 'abc' }],
          warnings: ['USE_GRAPH_RELATIONAL_FEATURES is disabled']
        }
      };

      const markup = buildFeatureVisibilityMarkup(payload);
      const mount = { innerHTML: '' };
      renderDecisionAttribution(mount, {
        available: true,
        explanation_type: 'feature_value_proxy',
        top_features: [{
          feature_id: 'structured_doc_events_v1.guidance_cut_confidence',
          attribution: -0.2,
          value: 0.71,
          feature_visibility: {
            shadow_only: true,
            feature_available: true,
            point_in_time_valid: true,
            status: 'shadow_only',
            confidence: { max: 0.71 },
            source_artifact: 'structured_document_events:doc-1',
            pit_status: { reason_codes: [] }
          }
        }]
      });

      process.stdout.write(JSON.stringify({
        structured: markup.structuredHtml,
        graph: markup.graphHtml,
        decision: mount.innerHTML
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
    assert "shadow-only" in payload["structured"]
    assert "structured_document_events:doc-1" in payload["structured"]
    assert "graph_relational_snapshots:AAPL" in payload["graph"]
    assert "PIT valid" in payload["decision"]
    assert "source structured_document_events:doc-1" in payload["decision"]
