from __future__ import annotations

import importlib
import json
import sqlite3
from typing import Any

import pytest


def _reload():
    structured = importlib.reload(importlib.import_module("engine.data.structured_document_events"))
    llm_events = importlib.reload(importlib.import_module("engine.data.llm_event_extraction"))
    return structured, llm_events


def _db() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.execute(
        """
        CREATE TABLE events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER,
          timestamp INTEGER,
          event_type TEXT,
          symbol TEXT,
          source TEXT,
          title TEXT,
          body TEXT,
          url TEXT,
          importance_score REAL,
          raw_payload TEXT,
          derived_features TEXT,
          meta_json TEXT,
          source_id TEXT,
          dedupe_hash TEXT,
          event_key TEXT
        )
        """
    )
    return con


def _insert_event(
    con: sqlite3.Connection,
    *,
    source_id: str,
    symbol: str = "AAPL",
    ts_ms: int = 1_000,
    availability_ts_ms: int | None = None,
    title: str = "Apple update",
    body: str = "Apple lowered guidance and warned about margin pressure.",
    event_type: str = "news",
    source: str = "rss:unit",
) -> int:
    con.execute(
        """
        INSERT INTO events(
          ts_ms, timestamp, event_type, symbol, source, title, body, url,
          importance_score, raw_payload, derived_features, meta_json, source_id,
          dedupe_hash, event_key
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            int(ts_ms),
            int(ts_ms),
            event_type,
            symbol,
            source,
            title,
            body,
            f"https://example.test/{source_id}",
            0.9,
            json.dumps({"availability_ts_ms": availability_ts_ms if availability_ts_ms is not None else ts_ms}),
            "{}",
            "{}",
            source_id,
            f"hash-{source_id}",
            f"unit:{source_id}",
        ),
    )
    return int(con.execute("SELECT last_insert_rowid()").fetchone()[0])


class _InvalidJsonAdapter:
    provider = "fake"
    model = "fake-invalid"

    def extract(self, *, prompt: str, schema: dict[str, Any], config: Any):
        del prompt, schema, config
        from engine.data.llm_event_extraction import AdapterResponse

        return AdapterResponse(text="{not-json", provider=self.provider, model=self.model, raw_excerpt="{not-json")


class _CountingFakeAdapter:
    provider = "fake"
    model = "fake-llm-event-extractor-v1"

    def __init__(self, wrapped: Any) -> None:
        self.wrapped = wrapped
        self.calls = 0

    def extract(self, *, prompt: str, schema: dict[str, Any], config: Any):
        self.calls += 1
        return self.wrapped.extract(prompt=prompt, schema=schema, config=config)


def test_llm_event_schema_validation_rejects_extra_keys_and_invalid_json() -> None:
    _structured, llm_events = _reload()
    con = _db()
    _insert_event(con, source_id="doc-invalid-json")

    docs = llm_events.select_source_documents(con, decision_ts_ms=2_000, limit=1)
    assert len(docs) == 1
    prompt, prompt_hash = llm_events.build_extraction_prompt(docs[0], decision_ts_ms=2_000, max_input_chars=6000)
    payload = json.loads(
        llm_events.FakeLLMEventExtractionAdapter()
        .extract(
            prompt=prompt,
            schema=llm_events.LLM_EVENT_RESPONSE_SCHEMA,
            config=llm_events.LLMEventExtractionConfig(enabled=True, provider="fake", decision_ts_ms=2_000),
        )
        .text
    )
    payload["events"][0]["unexpected"] = True
    with pytest.raises(ValueError, match="extra_keys"):
        llm_events.validate_extraction_payload(
            payload,
            doc=docs[0],
            decision_ts_ms=2_000,
            provider="fake",
            model="fake-llm-event-extractor-v1",
            prompt_hash=prompt_hash,
            model_hash="model-hash",
        )

    summary = llm_events.run_llm_event_extraction_batch(
        con=con,
        adapter=_InvalidJsonAdapter(),
        config=llm_events.LLMEventExtractionConfig(enabled=True, provider="fake", decision_ts_ms=2_000, max_docs=1),
    )
    assert summary["events_written"] == 0
    assert summary["rejected_docs"] == 1
    audit = con.execute("SELECT status, rejection_reason FROM llm_event_extraction_audit ORDER BY id DESC LIMIT 1").fetchone()
    assert audit[0] == "rejected"
    assert "invalid_json" in audit[1]


def test_llm_event_source_selection_excludes_future_documents() -> None:
    _structured, llm_events = _reload()
    con = _db()
    _insert_event(con, source_id="doc-available", availability_ts_ms=2_000)
    _insert_event(con, source_id="doc-future", availability_ts_ms=5_000, body="Apple lowered guidance after the close.")

    docs = llm_events.select_source_documents(con, decision_ts_ms=3_000, limit=10)

    assert [doc.source_doc_id for doc in docs] == ["doc-available"]
    assert all(doc.availability_ts_ms <= 3_000 for doc in docs)


def test_llm_event_fake_fixture_persists_spans_hashes_and_pit_features() -> None:
    structured, llm_events = _reload()
    con = _db()
    _insert_event(
        con,
        source_id="doc-old",
        availability_ts_ms=2_000,
        body="Apple lowered guidance and warned about margin pressure.",
    )
    _insert_event(
        con,
        source_id="doc-future",
        availability_ts_ms=5_000,
        body="Apple cited supply chain delays from a key supplier.",
    )

    cfg = llm_events.LLMEventExtractionConfig(enabled=True, provider="fake", decision_ts_ms=3_000, max_docs=10)
    summary = llm_events.run_llm_event_extraction_batch(
        con=con,
        adapter=llm_events.FakeLLMEventExtractionAdapter(),
        config=cfg,
    )
    assert summary["events_written"] >= 2

    persisted = con.execute(
        """
        SELECT source_doc_id, evidence_start, evidence_end, evidence_text,
               prompt_hash, model_hash, source_hash, direct_trading_authority
        FROM llm_extracted_events
        ORDER BY event_type
        """
    ).fetchall()
    assert {row[0] for row in persisted} == {"doc-old"}
    assert all(int(row[1]) >= 0 and int(row[2]) > int(row[1]) for row in persisted)
    assert all(str(row[3]).strip() for row in persisted)
    assert all(str(row[4]).strip() and str(row[5]).strip() and str(row[6]).strip() for row in persisted)
    assert all(int(row[7]) == 0 for row in persisted)

    audit = con.execute(
        """
        SELECT status, prompt_hash, model_hash, source_hash, events_accepted
        FROM llm_event_extraction_audit
        WHERE source_doc_id='doc-old'
        ORDER BY id DESC LIMIT 1
        """
    ).fetchone()
    assert audit[0] == "accepted"
    assert all(str(value).strip() for value in audit[1:4])
    assert int(audit[4]) >= 2

    before, _before_meta, _before_available = structured.resolve_structured_document_event_features(
        con,
        symbol="AAPL",
        ts_ms=3_000,
    )
    assert before["structured_doc_events_v1.guidance_cut_confidence"] == pytest.approx(0.86)
    assert before["structured_doc_events_v1.margin_pressure_confidence"] == pytest.approx(0.86)

    llm_events.run_llm_event_extraction_batch(
        con=con,
        adapter=llm_events.FakeLLMEventExtractionAdapter(),
        config=llm_events.LLMEventExtractionConfig(enabled=True, provider="fake", decision_ts_ms=6_000, max_docs=10),
    )
    at_4k, _meta_4k, _available_4k = structured.resolve_structured_document_event_features(
        con,
        symbol="AAPL",
        ts_ms=4_000,
    )
    at_6k, meta_6k, _available_6k = structured.resolve_structured_document_event_features(
        con,
        symbol="AAPL",
        ts_ms=6_000,
    )
    assert at_4k["structured_doc_events_v1.supply_chain_exposure_confidence"] == 0.0
    assert meta_6k["latest_availability_ts_ms"] == 5_000
    assert at_6k["structured_doc_events_v1.supply_chain_exposure_confidence"] == pytest.approx(0.86)


def test_llm_event_missing_key_noops_without_provider_call(monkeypatch) -> None:
    _structured, llm_events = _reload()
    con = _db()
    _insert_event(con, source_id="doc-no-key")
    monkeypatch.setattr(llm_events, "get_data_credential", lambda *_args, **_kwargs: "")

    summary = llm_events.run_llm_event_extraction_batch(
        con=con,
        config=llm_events.LLMEventExtractionConfig(enabled=True, provider="openai", decision_ts_ms=3_000),
    )

    assert summary["status"] == "missing_key"
    assert summary["events_written"] == 0
    assert con.execute("SELECT COUNT(*) FROM llm_extracted_events").fetchone()[0] == 0
    assert con.execute("SELECT status FROM llm_event_extraction_audit").fetchone()[0] == "missing_key"


def test_llm_event_cost_and_rate_limits_are_enforced() -> None:
    _structured, llm_events = _reload()
    con = _db()
    _insert_event(con, source_id="doc-cost")
    summary = llm_events.run_llm_event_extraction_batch(
        con=con,
        adapter=llm_events.FakeLLMEventExtractionAdapter(),
        config=llm_events.LLMEventExtractionConfig(enabled=True, provider="fake", decision_ts_ms=3_000, max_cost_usd=0.0),
    )
    assert summary["processed_docs"] == 0
    assert summary["events_written"] == 0
    assert con.execute("SELECT status FROM llm_event_extraction_audit ORDER BY id DESC LIMIT 1").fetchone()[0] == "cost_exhausted"

    con2 = _db()
    _insert_event(con2, source_id="doc-rate-1", availability_ts_ms=1_000)
    _insert_event(con2, source_id="doc-rate-2", availability_ts_ms=1_500)
    adapter = _CountingFakeAdapter(llm_events.FakeLLMEventExtractionAdapter())
    sleeps: list[float] = []
    summary2 = llm_events.run_llm_event_extraction_batch(
        con=con2,
        adapter=adapter,
        config=llm_events.LLMEventExtractionConfig(
            enabled=True,
            provider="fake",
            decision_ts_ms=3_000,
            max_docs=2,
            max_cost_usd=1.0,
            min_interval_ms=1000,
        ),
        sleep_fn=sleeps.append,
    )
    assert adapter.calls == 2
    assert summary2["processed_docs"] == 2
    assert sleeps and sleeps[0] > 0


def test_llm_event_features_remain_shadow_and_non_authoritative(monkeypatch) -> None:
    monkeypatch.delenv("MODEL_FEATURE_IDS", raising=False)
    feature_registry = importlib.reload(importlib.import_module("engine.strategy.feature_registry"))
    metadata = feature_registry.list_groups()["structured_doc_events_v1"]
    ids = list(feature_registry.FEATURE_GROUPS["structured_doc_events_v1"])

    assert "llm_event_extraction" in metadata["accepted_extractors"]
    assert metadata["llm_extractor_schema_version"] == "llm_financial_event_v1"
    assert metadata["direct_trading_authority"] is False
    assert all(feature_registry.feature_stage(fid) == feature_registry.FEATURE_STAGE_SHADOW for fid in ids)
    with pytest.raises(ValueError, match="live_model_serving_shadow_features_forbidden"):
        feature_registry.assert_no_shadow_features(ids, context="live_model_serving", model_name="unit")
