from __future__ import annotations

import importlib
import sqlite3

import pytest


def _reload(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def test_structured_document_extractor_covers_news_transcripts_and_filings() -> None:
    (structured,) = _reload("engine.data.structured_document_events")

    news_rows = structured.extract_structured_document_events(
        {
            "event_id": 10,
            "event_type": "news",
            "symbol": "AAPL",
            "source": "rss:unit",
            "source_id": "news-doc-1",
            "ts_ms": 1_000,
            "title": "Apple cuts guidance as margins face pressure",
            "body": "Management lowered revenue guidance and said higher costs caused gross margin pressure.",
        }
    )
    transcript_rows = structured.extract_structured_document_events(
        {
            "event_id": 11,
            "event_type": "news",
            "symbol": "MSFT",
            "source": "fmp_transcript",
            "source_id": "transcript-doc-1",
            "ts_ms": 2_000,
            "meta_json": {"transcript": True},
            "title": "Microsoft earnings call transcript",
            "body": "CEO: We raised full-year guidance. CFO: Visibility remains limited and uncertainty remains.",
        }
    )
    filing_rows = structured.extract_structured_document_events(
        {
            "event_id": 12,
            "event_type": "filing",
            "symbol": "TSLA",
            "source": "sec",
            "source_id": "filing-doc-1",
            "ts_ms": 3_000,
            "title": "TSLA 10-Q filing",
            "body": (
                "There is substantial doubt about our ability to continue as a going concern. "
                "One major customer accounted for 42% of revenue."
            ),
        }
    )

    assert {row["event_type"] for row in news_rows} >= {"guidance_cut", "margin_pressure"}
    assert {row["event_type"] for row in transcript_rows} >= {"guidance_raise", "management_uncertainty"}
    assert {row["event_type"] for row in filing_rows} >= {"liquidity_stress", "customer_concentration"}
    for row in [*news_rows, *transcript_rows, *filing_rows]:
        assert row["source_document_id"]
        assert row["event_ts_ms"] > 0
        assert row["availability_ts_ms"] >= row["event_ts_ms"]
        assert 0.0 < row["extraction_confidence"] <= 1.0
        assert row["pit_metadata_json"]["availability_timestamp_field"] == "availability_ts_ms"
        assert row["pit_metadata_json"]["direct_trading_authority"] is False


def test_structured_document_event_resolver_is_point_in_time() -> None:
    (structured,) = _reload("engine.data.structured_document_events")
    con = sqlite3.connect(":memory:")
    structured.ensure_structured_document_event_schema(con)
    structured.put_structured_document_events(
        con,
        [
            {
                "source_document_id": "doc-old",
                "source_event_id": 1,
                "symbol": "AAPL",
                "document_type": "news",
                "source": "unit",
                "event_type": "guidance_raise",
                "event_ts_ms": 1_000,
                "availability_ts_ms": 1_000,
                "extraction_confidence": 0.81,
                "polarity": 1.0,
                "feature_id": structured.EVENT_FEATURE_ID["guidance_raise"],
                "extractor_name": structured.EXTRACTOR_NAME,
                "extractor_version": structured.EXTRACTOR_VERSION,
                "created_ts_ms": 1_001,
                "pit_metadata_json": {"availability_ts_ms": 1_000},
            },
            {
                "source_document_id": "doc-future",
                "source_event_id": 2,
                "symbol": "AAPL",
                "document_type": "filing",
                "source": "sec",
                "event_type": "guidance_cut",
                "event_ts_ms": 5_000,
                "availability_ts_ms": 5_000,
                "extraction_confidence": 0.91,
                "polarity": -1.0,
                "feature_id": structured.EVENT_FEATURE_ID["guidance_cut"],
                "extractor_name": structured.EXTRACTOR_NAME,
                "extractor_version": structured.EXTRACTOR_VERSION,
                "created_ts_ms": 5_001,
                "pit_metadata_json": {"availability_ts_ms": 5_000},
            },
        ],
    )

    before, before_meta, before_available = structured.resolve_structured_document_event_features(
        con,
        symbol="AAPL",
        ts_ms=3_000,
    )
    after, after_meta, after_available = structured.resolve_structured_document_event_features(
        con,
        symbol="AAPL",
        ts_ms=6_000,
    )

    assert before_available is True
    assert before["structured_doc_events_v1.guidance_raise_confidence"] == pytest.approx(0.81)
    assert before["structured_doc_events_v1.guidance_cut_confidence"] == 0.0
    assert before_meta["latest_availability_ts_ms"] == 1_000
    assert after_available is True
    assert after["structured_doc_events_v1.guidance_cut_confidence"] == pytest.approx(0.91)
    assert after_meta["latest_availability_ts_ms"] == 5_000


def test_structured_document_features_are_registered_shadow_only(monkeypatch) -> None:
    monkeypatch.delenv("MODEL_FEATURE_IDS", raising=False)
    feature_registry, feature_pit = _reload("engine.strategy.feature_registry", "engine.strategy.feature_pit")

    feature_ids = list(feature_registry.FEATURE_GROUPS["structured_doc_events_v1"])
    assert feature_ids == feature_registry.resolve_feature_ids(feature_ids=feature_ids, fallback_to_default=False)
    assert feature_registry.feature_set_tag_from_ids(feature_ids).endswith("structured_doc_events_v1_shadow")
    assert all(feature_registry.feature_stage(fid) == feature_registry.FEATURE_STAGE_SHADOW for fid in feature_ids)
    metadata = feature_registry.list_groups()["structured_doc_events_v1"]
    assert metadata["direct_trading_authority"] is False
    assert metadata["availability_timestamp_field"] == "latest_availability_ts_ms"
    assert feature_pit.group_for_feature_id(feature_ids[0]) == "structured_doc_events"
    with pytest.raises(ValueError, match="live_model_serving_shadow_features_forbidden"):
        feature_registry.assert_no_shadow_features(feature_ids, context="live_model_serving", model_name="unit")


def test_structured_document_features_flow_through_model_snapshot() -> None:
    structured, model_feature_snapshots = _reload(
        "engine.data.structured_document_events",
        "engine.strategy.model_feature_snapshots",
    )
    con = sqlite3.connect(":memory:")
    structured.ensure_structured_document_event_schema(con)
    structured.put_structured_document_events(
        con,
        [
            {
                "source_document_id": "doc-snapshot",
                "source_event_id": 3,
                "symbol": "AAPL",
                "document_type": "transcript",
                "source": "fmp_transcript",
                "event_type": "management_uncertainty",
                "event_ts_ms": 2_000,
                "availability_ts_ms": 2_000,
                "extraction_confidence": 0.77,
                "polarity": -0.5,
                "feature_id": structured.EVENT_FEATURE_ID["management_uncertainty"],
                "extractor_name": structured.EXTRACTOR_NAME,
                "extractor_version": structured.EXTRACTOR_VERSION,
                "created_ts_ms": 2_001,
                "pit_metadata_json": {"availability_ts_ms": 2_000},
            }
        ],
    )

    feature_ids = [
        "structured_doc_events_v1.management_uncertainty_confidence",
        "structured_doc_events_v1.event_count_30d",
    ]
    snap = model_feature_snapshots.build_model_feature_snapshot(
        symbol="AAPL",
        ts_ms=3_000,
        feature_ids=feature_ids,
        con=con,
    )

    assert snap["features"]["structured_doc_events_v1.management_uncertainty_confidence"] == pytest.approx(0.77)
    assert snap["features"]["structured_doc_events_v1.event_count_30d"] == pytest.approx(1.0)
    assert snap["availability"]["structured_doc_events"] is True
    assert snap["source_timestamps"]["structured_doc_events"]["latest_availability_ts_ms"] == 2_000
    validation = model_feature_snapshots.summarize_model_feature_snapshots([snap])
    assert validation["ok"] is True


def test_put_normalized_event_writes_structured_document_rows(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("TS_TESTING", "1")
    monkeypatch.setenv("DB_PATH", str(tmp_path / "structured-events.sqlite"))
    storage = importlib.import_module("engine.runtime.storage")
    storage = importlib.reload(storage)
    event_normalization = importlib.reload(importlib.import_module("engine.data.event_normalization"))

    storage.init_db()
    event_id = storage.put_normalized_event(
        event_normalization.normalize_news_event(
            {
                "ts_ms": 10_000,
                "source": "unit_news",
                "symbol": "AAPL",
                "source_id": "normalized-doc-1",
                "title": "Apple cuts guidance",
                "body": "Apple lowered guidance as higher costs caused margin pressure.",
                "event_key": "unit:structured-doc",
            }
        )
    )

    con = storage.connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT source_event_id, source_document_id, event_type, availability_ts_ms, extraction_confidence
            FROM structured_document_events
            WHERE source_event_id = ?
            ORDER BY event_type
            """,
            (int(event_id),),
        ).fetchall()
    finally:
        con.close()

    assert {str(row[2]) for row in rows} >= {"guidance_cut", "margin_pressure"}
    assert all(str(row[1]) == "normalized-doc-1" for row in rows)
    assert all(int(row[3]) >= 10_000 for row in rows)
    assert all(float(row[4]) > 0.0 for row in rows)

    transcript_event_id = storage.put_normalized_event(
        {
            "ts_ms": 20_000,
            "event_type": "transcript",
            "source": "fmp_transcript",
            "symbol": "MSFT",
            "source_id": "normalized-transcript-1",
            "title": "Microsoft earnings call transcript",
            "body": "CEO: We raised full-year guidance. CFO: Visibility remains limited and uncertainty remains.",
            "event_key": "unit:structured-doc-transcript",
            "meta_json": {"provider": "fmp", "transcript": True},
        }
    )

    con = storage.connect(readonly=True)
    try:
        transcript_rows = con.execute(
            """
            SELECT source_event_id, source_document_id, document_type, event_type,
                   availability_ts_ms, extraction_confidence, pit_metadata_json
            FROM structured_document_events
            WHERE source_event_id = ?
            ORDER BY event_type
            """,
            (int(transcript_event_id),),
        ).fetchall()
    finally:
        con.close()

    assert {str(row[2]) for row in transcript_rows} == {"transcript"}
    assert {str(row[3]) for row in transcript_rows} >= {"guidance_raise", "management_uncertainty"}
    assert all(str(row[1]) == "normalized-transcript-1" for row in transcript_rows)
    assert all(int(row[4]) >= 20_000 for row in transcript_rows)
    assert all(float(row[5]) > 0.0 for row in transcript_rows)
    assert all("direct_trading_authority" in str(row[6]) for row in transcript_rows)
