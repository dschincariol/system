from __future__ import annotations

import importlib
import json
import sqlite3
from pathlib import Path

import pytest

from engine.artifacts.store import LocalArtifactStore
from engine.api.feature_visibility import graph_feature_visibility
from engine.strategy.graph_challenger import (
    GRAPH_CHALLENGER_MODEL_FAMILY,
    GRAPH_CHALLENGER_MODEL_NAME,
    build_graph_challenger_dataset,
    ensure_graph_challenger_schema,
    load_graph_challenger_artifact,
    run_graph_challenger_benchmark,
)
from engine.strategy.graph_relational import (
    GRAPH_RELATIONAL_FEATURE_IDS,
    GRAPH_RELATIONAL_GRAPH_ID,
    GRAPH_RELATIONAL_GROUP,
    GRAPH_RELATIONAL_SNAPSHOT_VERSION,
    build_graph_relational_snapshot,
    ensure_graph_relational_schema,
    evaluate_graph_promotion_gate,
    graph_metadata_from_snapshot,
    graph_train_serve_parity,
    load_graph_relational_snapshot,
    store_graph_relational_snapshots,
)


def _conn() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    return con


def _create_graph_challenger_fixture(
    con: sqlite3.Connection,
    *,
    anchor: int,
    feature_set_tag: str = "graph_challenger_node_v1",
) -> None:
    ensure_graph_challenger_schema(con)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS model_feature_snapshots(
          symbol TEXT NOT NULL,
          ts_ms INTEGER NOT NULL,
          feature_set_tag TEXT NOT NULL,
          snapshot_version INTEGER NOT NULL DEFAULT 1,
          feature_ids_json TEXT NOT NULL,
          vector_json TEXT NOT NULL,
          features_json TEXT NOT NULL,
          source_timestamps_json TEXT NOT NULL,
          availability_json TEXT NOT NULL,
          created_ts_ms INTEGER NOT NULL,
          PRIMARY KEY(symbol, ts_ms, feature_set_tag)
        )
        """
    )
    con.execute("CREATE TABLE IF NOT EXISTS prices(symbol TEXT, ts_ms INTEGER, price REAL, px REAL)")
    feature_ids = ["price.last", "price.log_ret_5m"]
    for step in range(6):
        ts_ms = anchor + step * 60_000
        for symbol, base, drift in (("AAPL", 100.0, 1.0), ("MSFT", 200.0, 1.5), ("NVDA", 300.0, -0.5)):
            price = base + drift * step
            con.execute("INSERT INTO prices(symbol, ts_ms, price, px) VALUES (?,?,?,NULL)", (symbol, ts_ms, price))
            features = {"price.last": price, "price.log_ret_5m": 0.01 * drift}
            source_timestamps = {
                "anchor_ts_ms": ts_ms,
                "price": {"quote_ts_ms": ts_ms, "history_last_ts_ms": ts_ms},
            }
            if symbol == "NVDA" and step == 0:
                source_timestamps["price"] = {"quote_ts_ms": ts_ms + 60_000, "history_last_ts_ms": ts_ms + 60_000}
            con.execute(
                """
                INSERT OR REPLACE INTO model_feature_snapshots(
                  symbol, ts_ms, feature_set_tag, snapshot_version, feature_ids_json,
                  vector_json, features_json, source_timestamps_json, availability_json, created_ts_ms
                )
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    symbol,
                    ts_ms,
                    feature_set_tag,
                    1,
                    json.dumps(feature_ids),
                    json.dumps([features[fid] for fid in feature_ids]),
                    json.dumps(features),
                    json.dumps(source_timestamps),
                    json.dumps({"price": True}),
                    ts_ms,
                ),
            )
    con.execute(
        """
        INSERT INTO graph_relationship_edges(
          source_symbol, target_symbol, relationship_type, weight, source_ts_ms,
          availability_ts_ms, source, meta_json
        ) VALUES (?,?,?,?,?,?,?,?)
        """,
        ("AAPL", "MSFT", "supply_chain", 0.75, anchor - 5_000, anchor - 4_000, "unit", "{}"),
    )
    con.execute(
        """
        INSERT INTO graph_relationship_edges(
          source_symbol, target_symbol, relationship_type, weight, source_ts_ms,
          availability_ts_ms, source, meta_json
        ) VALUES (?,?,?,?,?,?,?,?)
        """,
        ("AAPL", "NVDA", "supply_chain", 0.95, anchor + 5_000, anchor + 5_000, "future", "{}"),
    )


def test_graph_feature_group_is_shadow_only_and_registered() -> None:
    from engine.strategy.feature_registry import (
        FEATURE_GROUPS,
        assert_no_shadow_features,
        shadow_feature_ids,
    )

    assert FEATURE_GROUPS[GRAPH_RELATIONAL_GROUP] == GRAPH_RELATIONAL_FEATURE_IDS
    assert shadow_feature_ids([GRAPH_RELATIONAL_FEATURE_IDS[0]]) == [GRAPH_RELATIONAL_FEATURE_IDS[0]]
    with pytest.raises(ValueError, match="live_model_serving_shadow_features_forbidden"):
        assert_no_shadow_features([GRAPH_RELATIONAL_FEATURE_IDS[0]], context="live_model_serving")


def test_graph_snapshot_is_versioned_pit_safe_and_filters_future_edges(monkeypatch: pytest.MonkeyPatch) -> None:
    anchor = 1_700_000_000_000
    monkeypatch.setenv("GRAPH_RELATIONAL_CORR_MIN_ABS", "0.05")
    con = _conn()
    ensure_graph_relational_schema(con)
    con.execute(
        "CREATE TABLE symbols(symbol TEXT PRIMARY KEY, meta_json TEXT, updated_ts_ms INTEGER, status TEXT, score REAL)"
    )
    con.execute("CREATE TABLE prices(symbol TEXT, ts_ms INTEGER, price REAL, px REAL)")
    con.execute(
        """
        CREATE TABLE inst_13f_holdings(
          manager_cik TEXT, symbol TEXT, report_ts_ms INTEGER, ts_ms INTEGER,
          availability_ts_ms INTEGER, value_usd REAL, value_thousands REAL
        )
        """
    )

    for symbol, sector, industry, score in (
        ("AAPL", "Technology", "Hardware", 10.0),
        ("MSFT", "Technology", "Hardware", 9.0),
        ("TSLA", "Consumer", "Autos", 8.0),
        ("NVDA", "Technology", "Semis", 7.0),
    ):
        con.execute(
            "INSERT INTO symbols(symbol, meta_json, updated_ts_ms, status, score) VALUES (?,?,?,?,?)",
            (symbol, json.dumps({"sector": sector, "industry": industry}), anchor - 10_000, "ACTIVE", score),
        )
    for idx in range(10):
        ts_ms = anchor - (10 - idx) * 60_000
        con.execute("INSERT INTO prices(symbol, ts_ms, price, px) VALUES (?,?,?,NULL)", ("AAPL", ts_ms, 100.0 + idx))
        con.execute("INSERT INTO prices(symbol, ts_ms, price, px) VALUES (?,?,?,NULL)", ("MSFT", ts_ms, 200.0 + idx * 2.0))
    for symbol in ("AAPL", "MSFT"):
        con.execute(
            """
            INSERT INTO inst_13f_holdings(
              manager_cik, symbol, report_ts_ms, ts_ms, availability_ts_ms, value_usd, value_thousands
            ) VALUES (?,?,?,?,?,?,?)
            """,
            ("0001", symbol, anchor - 20_000, anchor - 20_000, anchor - 15_000, 1_000_000.0, None),
        )
    con.execute(
        """
        INSERT INTO graph_relationship_edges(
          source_symbol, target_symbol, relationship_type, weight, source_ts_ms,
          availability_ts_ms, source, meta_json
        ) VALUES (?,?,?,?,?,?,?,?)
        """,
        ("AAPL", "TSLA", "supply_chain", 0.8, anchor - 12_000, anchor - 11_000, "test", "{}"),
    )
    con.execute(
        """
        INSERT INTO graph_relationship_edges(
          source_symbol, target_symbol, relationship_type, weight, source_ts_ms,
          availability_ts_ms, source, meta_json
        ) VALUES (?,?,?,?,?,?,?,?)
        """,
        ("AAPL", "NVDA", "supply_chain", 0.9, anchor + 1_000, anchor + 1_000, "future", "{}"),
    )

    snap = build_graph_relational_snapshot(
        symbol="AAPL",
        ts_ms=anchor,
        peer_symbols=["MSFT", "TSLA", "NVDA"],
        con=con,
    )
    assert snap["graph_id"] == GRAPH_RELATIONAL_GRAPH_ID
    assert snap["snapshot_version"] == GRAPH_RELATIONAL_SNAPSHOT_VERSION
    assert snap["metadata"]["pit_safe"] is True
    assert snap["source_timestamps"]["max_availability_ts_ms"] <= anchor
    assert snap["features"]["graph.relational_v1.sector_peer_count"] >= 1.0
    assert snap["features"]["graph.relational_v1.supply_chain_degree"] == 1.0
    assert not any(
        edge["target_symbol"] == "NVDA" and edge["relationship_type"] == "supply_chain"
        for edge in snap["relationships"]
    )

    assert store_graph_relational_snapshots([snap], con=con) == 1
    loaded = load_graph_relational_snapshot(symbol="AAPL", ts_ms=anchor, con=con)
    assert loaded["metadata"]["relationship_hash"] == snap["metadata"]["relationship_hash"]


def test_model_feature_snapshot_zeroes_graph_features_when_availability_is_future(monkeypatch: pytest.MonkeyPatch) -> None:
    from engine.strategy import model_feature_snapshots

    anchor = 1_700_000_000_000
    fid = GRAPH_RELATIONAL_FEATURE_IDS[0]

    def fake_graph_group(con, *, symbol: str, ts_ms: int, feature_ids=None):
        return {fid: 7.0}, {"max_source_ts_ms": anchor, "max_availability_ts_ms": anchor + 1}, True

    monkeypatch.setattr(model_feature_snapshots, "_load_graph_relational_group", fake_graph_group)
    snap = model_feature_snapshots.build_model_feature_snapshot(
        symbol="AAPL",
        ts_ms=anchor,
        feature_ids=[fid],
        con=_conn(),
    )

    assert snap["features"][fid] == 0.0
    assert snap["availability"][GRAPH_RELATIONAL_GROUP] is False
    assert "availability_after_decision" in snap["pit_controls"][GRAPH_RELATIONAL_GROUP]["reason_codes"]


def test_graph_promotion_gate_requires_metadata_and_keeps_valid_graph_shadow_only() -> None:
    fid = GRAPH_RELATIONAL_FEATURE_IDS[0]
    passed, diagnostics = evaluate_graph_promotion_gate({"metrics": {"feature_ids": [fid]}})
    assert passed is False
    assert diagnostics["status"] == "graph_metadata_missing"

    bad_meta = {
        "graph_id": GRAPH_RELATIONAL_GRAPH_ID,
        "snapshot_version": GRAPH_RELATIONAL_SNAPSHOT_VERSION,
        "feature_ids": [fid],
        "snapshot_available": True,
        "pit_safe": False,
        "max_source_ts_ms": 1,
        "max_availability_ts_ms": 1,
    }
    passed, diagnostics = evaluate_graph_promotion_gate({"metrics": {"feature_ids": [fid], "graph_relational": bad_meta}})
    assert passed is False
    assert diagnostics["status"] == "pit_safety_missing"

    valid_meta = graph_metadata_from_snapshot(
        {
            "graph_id": GRAPH_RELATIONAL_GRAPH_ID,
            "snapshot_version": GRAPH_RELATIONAL_SNAPSHOT_VERSION,
            "feature_ids": [fid],
            "metadata": {
                "snapshot_available": True,
                "pit_safe": True,
                "max_source_ts_ms": 1,
                "max_availability_ts_ms": 1,
            },
        }
    )
    passed, diagnostics = evaluate_graph_promotion_gate({"metrics": {"feature_ids": [fid], "graph_relational": valid_meta}})
    assert passed is False
    assert diagnostics["status"] == "graph_relational_shadow_only"

    parity = graph_train_serve_parity(
        valid_meta,
        {**valid_meta, "snapshot_version": GRAPH_RELATIONAL_SNAPSHOT_VERSION + 1},
    )
    assert parity["ok"] is False
    assert "snapshot_version_mismatch" in parity["blockers"]

    passed, diagnostics = evaluate_graph_promotion_gate(
        {"metrics": {"feature_ids": [fid], "graph_relational": {**valid_meta, "train_serve_parity": parity}}}
    )
    assert passed is False
    assert diagnostics["status"] == "train_serve_parity_failed"


def test_model_registry_blocks_graph_candidate_before_other_promotion_gates(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "graph_registry.sqlite"))
    storage = importlib.reload(importlib.import_module("engine.runtime.storage"))
    registry = importlib.reload(importlib.import_module("engine.model_registry"))
    storage.init_db()
    registry.init_model_registry()

    registry.register_model(
        model_name="graph_candidate",
        model_kind="shadow_graph",
        model_ts_ms=123,
        stage="challenger",
        metrics={"feature_ids": [GRAPH_RELATIONAL_FEATURE_IDS[0]]},
        regime="global",
    )

    with pytest.raises(RuntimeError, match="graph relational promotion gate blocked: status=graph_metadata_missing"):
        registry.promote_to_champion("graph_candidate", "global")


def test_graph_challenger_dataset_is_pit_safe_filters_future_features_and_edges() -> None:
    anchor = 1_700_100_000_000
    con = _conn()
    _create_graph_challenger_fixture(con, anchor=anchor)

    dataset = build_graph_challenger_dataset(
        con=con,
        symbols=["AAPL", "MSFT", "NVDA"],
        sample_ts_ms=[anchor],
        horizons_s=[60],
        node_feature_ids=["price.last", "price.log_ret_5m"],
        window_count=1,
        window_stride_ms=60_000,
    )

    assert dataset["metadata"]["pit_safe"] is True
    assert dataset["metadata"]["edge_types"] == ["supply_chain"]
    sample = next(row for row in dataset["samples"] if row["symbol"] == "AAPL")
    assert all(edge["target_symbol"] != "NVDA" for edge in sample["edges"])
    nvda_window = sample["nodes"]["NVDA"]["windows"][0]
    assert nvda_window["vector"] == [0.0, 0.0]
    assert "price.last:feature_after_anchor" in nvda_window["reason_codes"]
    for feature_meta in nvda_window["feature_metadata"].values():
        assert feature_meta["available"] is False


def test_graph_challenger_missing_edge_fallback_and_deterministic_metadata() -> None:
    anchor = 1_700_200_000_000
    con = _conn()
    _create_graph_challenger_fixture(con, anchor=anchor)
    con.execute("DELETE FROM graph_relationship_edges")

    kwargs = {
        "con": con,
        "symbols": ["AAPL", "MSFT"],
        "sample_ts_ms": [anchor],
        "horizons_s": [60],
        "node_feature_ids": ["price.last", "price.log_ret_5m"],
        "window_count": 1,
        "window_stride_ms": 60_000,
    }
    first = build_graph_challenger_dataset(**kwargs)
    second = build_graph_challenger_dataset(**kwargs)

    assert first["dataset_id"] == second["dataset_id"]
    assert first["metadata"]["fingerprint"] == second["metadata"]["fingerprint"]
    sample = first["samples"][0]
    assert sample["edges"] == []
    assert sample["metadata"]["missing_edge_fallback"] is True
    assert sample["metadata"]["edge_feature_schema"]["fields"] == [
        "source_symbol",
        "target_symbol",
        "edge_type",
        "weight",
        "source_ts_ms",
        "availability_ts_ms",
        "source",
    ]


def test_graph_challenger_benchmark_persists_artifact_oos_marketplace_and_visibility(tmp_path: Path) -> None:
    anchor = 1_700_300_000_000
    con = _conn()
    _create_graph_challenger_fixture(con, anchor=anchor)
    artifact_db = tmp_path / "artifact.sqlite"

    def _artifact_connect() -> sqlite3.Connection:
        return sqlite3.connect(artifact_db)

    artifact_store = LocalArtifactStore(root=tmp_path / "artifacts", connect_factory=_artifact_connect)
    result = run_graph_challenger_benchmark(
        con=con,
        symbols=["AAPL", "MSFT"],
        sample_ts_ms=[anchor + idx * 60_000 for idx in range(4)],
        horizons_s=[60],
        node_feature_ids=["price.last", "price.log_ret_5m"],
        window_count=1,
        window_stride_ms=60_000,
        holdout_fraction=0.5,
        artifact_store=artifact_store,
    )

    assert result["ok"] is True
    assert result["shadow_only"] is True
    assert result["direct_trading_authority"] is False
    assert result["artifact"]["artifact_sha256"]
    artifact_payload = load_graph_challenger_artifact(result["artifact"]["artifact_alias"], artifact_store=artifact_store)
    assert artifact_payload["model"]["model_name"] == GRAPH_CHALLENGER_MODEL_NAME
    assert artifact_payload["model"]["edge_feature_schema"]["edge_types"] == ["supply_chain"]

    oos_count = con.execute(
        "SELECT COUNT(*) FROM model_oos_predictions WHERE family=?",
        (GRAPH_CHALLENGER_MODEL_FAMILY,),
    ).fetchone()[0]
    assert int(oos_count) == result["oos_prediction_count"]
    marketplace = con.execute(
        """
        SELECT stage, meta_json
        FROM model_marketplace_scores
        WHERE model_name=?
        LIMIT 1
        """,
        (GRAPH_CHALLENGER_MODEL_NAME,),
    ).fetchone()
    assert marketplace is not None
    assert marketplace["stage"] == "shadow"
    meta = json.loads(marketplace["meta_json"])
    assert meta["score_source"] == "model_oos_predictions"
    assert meta["graph_metadata"]["train_serve_parity"]["ok"] is True

    visibility = graph_feature_visibility(con, now_ms=anchor + 240_000, symbol="AAPL")
    assert visibility["shadow_only"] is True
    assert visibility["challenger"]["available"] is True
    assert visibility["challenger"]["status"] == "shadow_only"
    assert visibility["challenger"]["train_serve_parity"]["ok"] is True

    passed, diagnostics = evaluate_graph_promotion_gate({"meta": meta})
    assert passed is False
    assert diagnostics["status"] == "graph_relational_shadow_only"


def test_graph_challenger_ablation_reports_incremental_evidence(tmp_path: Path) -> None:
    anchor = 1_700_400_000_000
    con = _conn()
    _create_graph_challenger_fixture(con, anchor=anchor)
    artifact_db = tmp_path / "artifact.sqlite"
    artifact_store = LocalArtifactStore(
        root=tmp_path / "artifacts",
        connect_factory=lambda: sqlite3.connect(artifact_db),
    )

    result = run_graph_challenger_benchmark(
        con=con,
        symbols=["AAPL", "MSFT"],
        sample_ts_ms=[anchor + idx * 60_000 for idx in range(4)],
        horizons_s=[60],
        node_feature_ids=["price.last", "price.log_ret_5m"],
        window_count=1,
        window_stride_ms=60_000,
        holdout_fraction=0.5,
        artifact_store=artifact_store,
    )

    ablation = result["ablation"]
    assert set(ablation) >= {
        "baseline_node_only",
        "graph_message_passing",
        "incremental_mae",
        "incremental_rmse",
        "comparison",
    }
    assert ablation["baseline_node_only"]["n"] == ablation["graph_message_passing"]["n"]
    assert ablation["comparison"] == "graph_message_passing_minus_node_only"
