from __future__ import annotations

import importlib
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class _EmptyCursor:
    description: list[tuple[str]] = []

    def fetchone(self):
        return None

    def fetchall(self):
        return []


class _EmptyCon:
    def execute(self, *_args, **_kwargs):
        return _EmptyCursor()

    def close(self):
        return None


def _reload(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _assert_future_group_zeroed(monkeypatch, *, loader_name: str, group: str, feature_id: str, meta: dict[str, int]) -> None:
    (snapshots,) = _reload("engine.strategy.model_feature_snapshots")
    anchor = 1_700_000_000_000
    monkeypatch.setattr(
        snapshots,
        loader_name,
        lambda *args, **kwargs: ({feature_id: 7.0}, dict(meta), True),
    )

    snap = snapshots.build_model_feature_snapshot(
        symbol="AAPL",
        ts_ms=anchor,
        feature_ids=[feature_id],
        con=_EmptyCon(),
    )

    assert float(snap["features"][feature_id]) == 0.0
    assert snap["availability"][group] is False
    assert group in snap["feature_metadata"]
    assert any("after_decision" in code for code in snap["pit_controls"][group]["reason_codes"])


def test_delayed_macro_future_availability_is_zeroed(monkeypatch) -> None:
    _assert_future_group_zeroed(
        monkeypatch,
        loader_name="_load_macro_group",
        group="macro",
        feature_id="macro.cpi_yoy",
        meta={"asof_ts_ms": 1_700_000_000_001, "effective_ts_ms": 1_699_999_999_000},
    )


@pytest.mark.parametrize(
    ("loader_name", "group", "feature_id", "meta"),
    [
        (
            "_load_news_flow_group",
            "news_flow",
            "news_velocity_z",
            {"latest_availability_ts_ms": 1_700_000_000_001},
        ),
        (
            "_load_options_group",
            "options",
            "options_symbol.iv_rank",
            {"bucket_ts_ms": 1_699_999_999_000, "snapshot_ts_ms": 1_700_000_000_001},
        ),
        (
            "_load_fundamentals_group",
            "fundamentals",
            "fund_eps",
            {"latest_publish_ts_ms": 1_700_000_000_001},
        ),
        (
            "_load_gov_group",
            "gov",
            "congress_committee_buy_30d",
            {"latest_availability_ts_ms": 1_700_000_000_001, "latest_disclosure_ts_ms": 1_700_000_000_001},
        ),
    ],
)
def test_future_availability_is_zeroed_for_delayed_families(
    monkeypatch,
    loader_name: str,
    group: str,
    feature_id: str,
    meta: dict[str, int],
) -> None:
    _assert_future_group_zeroed(
        monkeypatch,
        loader_name=loader_name,
        group=group,
        feature_id=feature_id,
        meta=meta,
    )


def test_congressional_features_use_disclosure_availability_not_transaction_date() -> None:
    (snapshots,) = _reload("engine.strategy.model_feature_snapshots")
    snapshots.CONGRESSIONAL_FEATURE_IDS = [
        "congressional.buy_count_30d",
        "congressional.sell_count_30d",
        "congressional.net_signal_30d",
    ]
    con = sqlite3.connect(":memory:")
    con.execute(
        """
        CREATE TABLE congressional_trades (
          id INTEGER PRIMARY KEY,
          symbol TEXT,
          transaction_ts_ms INTEGER,
          disclosure_ts_ms INTEGER,
          ingested_ts_ms INTEGER,
          created_ts_ms INTEGER,
          direction TEXT
        )
        """
    )
    con.execute(
        """
        INSERT INTO congressional_trades(
          symbol, transaction_ts_ms, disclosure_ts_ms, ingested_ts_ms, created_ts_ms, direction
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("AAPL", 1_000, 10_000, 10_000, 10_000, "buy"),
    )

    before, before_meta, before_available = snapshots._load_congressional_group(con, symbol="AAPL", ts_ms=5_000)
    after, after_meta, after_available = snapshots._load_congressional_group(con, symbol="AAPL", ts_ms=10_001)

    assert before_available is False
    assert float(before.get("congressional.buy_count_30d", 0.0)) == 0.0
    assert after_available is True
    assert float(after["congressional.buy_count_30d"]) == 1.0
    assert after_meta["latest_availability_ts_ms"] == 10_000
    assert after_meta["latest_transaction_ts_ms"] == 1_000


def test_nlp_filing_cache_filters_future_same_day_blobs(monkeypatch) -> None:
    (feature_registry,) = _reload("engine.strategy.feature_registry")
    import engine.runtime.storage as storage

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE nlp_text_blobs(hash TEXT PRIMARY KEY, source TEXT, ts INTEGER, symbol TEXT, text TEXT)")
    con.execute("CREATE TABLE nlp_embeddings(hash TEXT, model_name TEXT, dim INTEGER, vector BLOB)")
    dim = int(feature_registry.NLP_EMBEDDING_DIM)
    vector = np.zeros((dim,), dtype=np.float32).tobytes()
    day_start = 1_700_006_400_000
    anchor = day_start + 12 * 3_600_000
    before_ts = anchor - 60_000
    future_ts = anchor + 60_000
    for blob_hash, ts_ms in (("before", before_ts), ("future", future_ts)):
        con.execute(
            "INSERT INTO nlp_text_blobs(hash, source, ts, symbol, text) VALUES (?, ?, ?, ?, ?)",
            (blob_hash, "filing", int(ts_ms), "AAPL", blob_hash),
        )
        con.execute(
            "INSERT INTO nlp_embeddings(hash, model_name, dim, vector) VALUES (?, ?, ?, ?)",
            (blob_hash, feature_registry.NLP_SENTENCE_MODEL_NAME, dim, vector),
        )
    monkeypatch.setattr(storage, "connect", lambda readonly=True: con)

    features = feature_registry._load_nlp_cached_features(
        "AAPL",
        int(anchor),
        ["nlp.filings_v1.paragraph_count"],
    )

    assert float(features["nlp.filings_v1.paragraph_count"]) == 1.0


def test_discovered_llm_feature_created_after_decision_is_unavailable(monkeypatch) -> None:
    (feature_registry,) = _reload("engine.strategy.feature_registry")
    anchor = 1_700_000_000_000

    monkeypatch.setattr(
        feature_registry,
        "_load_discovered_feature_definition",
        lambda _fid: {
            "feature_id": "discovered.llm.future",
            "source": "llm_factor",
            "created_ts": anchor + 1,
            "expression": "(x0*x1)",
            "params": {"feature_map": {"x0": "price.last", "x1": "macro.cpi_yoy"}},
        },
    )

    assert feature_registry._evaluate_discovered_feature(
        "discovered.llm.future",
        event={"ts_ms": anchor},
        symbol="AAPL",
    ) == 0.0


def test_predictor_cache_rejects_feature_snapshot_after_decision(monkeypatch) -> None:
    (predictor,) = _reload("engine.strategy.predictor")
    from engine.cache.wrappers import feature_snapshots

    monkeypatch.setattr(predictor, "_registry_feature_set_tag", lambda _ids: "unit_tag")
    monkeypatch.setattr(
        feature_snapshots,
        "latest",
        lambda _symbol, _tag: {
            "symbol": "AAPL",
            "ts_ms": 101,
            "feature_ids": ["price.last"],
            "features": {"price.last": 123.0},
            "availability": {"price": True},
            "source_timestamps": {"price": {"quote_ts_ms": 101, "history_last_ts_ms": 101}},
        },
    )

    assert predictor._latest_feature_snapshot_features("AAPL", ["price.last"], decision_ts_ms=100) is None
