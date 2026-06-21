"""Smoke test for enriched news ingestion, dedupe, and transcript feature extraction."""

import json
import os
import sys
import tempfile
import time
from pathlib import Path


def _ensure_column(con, table: str, name: str, column_type: str) -> None:
    existing = {
        str(row[1])
        for row in con.execute(f"PRAGMA table_info({table})").fetchall()
        if row and len(row) > 1
    }
    if name in existing:
        return
    con.execute(f"ALTER TABLE {table} ADD COLUMN {name} {column_type}")


def _prepare_news_selftest_schema(con) -> None:
    from engine.data.news_flow import ensure_news_flow_tables

    ensure_news_flow_tables(con)
    for name, column_type in (
        ("cluster_key", "TEXT"),
        ("headline_key", "TEXT"),
        ("duplicate_count", "INTEGER NOT NULL DEFAULT 0"),
        ("company_match_method", "TEXT"),
        ("company_match_conf", "REAL NOT NULL DEFAULT 0.0"),
        ("source_count", "INTEGER NOT NULL DEFAULT 0"),
        ("meta_json", "TEXT"),
    ):
        _ensure_column(con, "news_event_features", name, column_type)
    for name, column_type in (
        ("bucket_ts_ms", "INTEGER"),
        ("bucket_sec", "INTEGER"),
        ("news_velocity", "REAL NOT NULL DEFAULT 0.0"),
        ("sentiment_trend", "REAL NOT NULL DEFAULT 0.0"),
        ("event_density", "REAL NOT NULL DEFAULT 0.0"),
        ("event_count", "INTEGER NOT NULL DEFAULT 0"),
        ("distinct_cluster_count", "INTEGER NOT NULL DEFAULT 0"),
        ("avg_sentiment", "REAL NOT NULL DEFAULT 0.0"),
        ("avg_novelty", "REAL NOT NULL DEFAULT 0.0"),
        ("duplicate_share", "REAL NOT NULL DEFAULT 0.0"),
    ):
        _ensure_column(con, "news_symbol_features", name, column_type)


def main() -> None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    root = Path(tempfile.mkdtemp(prefix="news_ingestion_selftest_"))
    path = root / "selftest.db"
    os.environ["DB_PATH"] = str(path)
    os.environ["TS_STORAGE_BACKEND"] = "sqlite"
    os.environ["TRADING_DATA"] = str(root / "data")
    os.environ["DATA_DIR"] = str(root / "data")
    os.environ["TS_ARTIFACTS_ROOT"] = str(root / "artifacts")

    from engine.runtime.storage import init_db, connect, put_normalized_event, put_news_event_feature
    from engine.data.event_normalization import normalize_news_event
    from engine.data.ingest.news_enrichment import build_enriched_news_records, refresh_news_symbol_features

    init_db()
    con = connect()
    try:
        _prepare_news_selftest_schema(con)
        now_ms = int(time.time() * 1000)
        samples = [
            {
                "ts_ms": now_ms,
                "source": "rss:test",
                "title": "Apple raises guidance after strong iPhone demand",
                "body": "Apple Inc. raised guidance after strong demand and profit growth.",
                "url": "https://example.com/apple-guidance",
                "event_key": "rss:test:apple-guidance",
                "meta_json": {"provider": "rss-test"},
            },
            {
                "ts_ms": now_ms + 5000,
                "source": "finnhub_company_news",
                "title": "Apple raises guidance after strong iPhone demand",
                "body": "Apple reported strong iPhone demand and raised guidance again.",
                "url": "https://example.com/apple-outlook",
                "event_key": "finnhub:test:apple-outlook",
                "meta_json": {"provider": "finnhub"},
            },
            {
                "ts_ms": now_ms + 10000,
                "source": "fmp_transcript",
                "title": "Apple earnings call transcript",
                "body": "Operator: Welcome everyone.\nTim Cook: Demand remained strong.\nQuestion-and-answer session follows.",
                "url": "https://example.com/apple-transcript",
                "event_key": "fmp_transcript:test:apple",
                "meta_json": {"provider": "fmp", "transcript": True},
            },
        ]

        for sample in samples:
            rows = build_enriched_news_records(con, sample, allowed_symbols=["AAPL", "MSFT"])
            assert rows, sample["title"]
            for row in rows:
                if row["event"].get("symbol") != "AAPL":
                    continue
                event_id = put_normalized_event(normalize_news_event(row["event"]))
                feature = dict(row["feature"])
                feature["event_id"] = event_id
                put_news_event_feature(feature)

        snapshot = refresh_news_symbol_features(con, "AAPL")
        event_count = con.execute("SELECT COUNT(*) FROM events WHERE symbol='AAPL'").fetchone()[0]
        dupes = con.execute("SELECT COUNT(*) FROM news_event_features WHERE symbol='AAPL' AND is_duplicate=1").fetchone()[0]
        transcript_row = con.execute(
            """
            SELECT derived_features, meta_json
            FROM events
            WHERE source='fmp_transcript'
            ORDER BY ts_ms DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        assert event_count >= 3, event_count
        assert dupes >= 1, dupes
        assert snapshot and snapshot["event_count"] >= 3, snapshot
        assert transcript_row is not None
        derived = json.loads(transcript_row[0] or "{}")
        meta = json.loads(transcript_row[1] or "{}")
        assert derived.get("transcript_speaker_count", 0) >= 1, derived
        assert bool(meta.get("derived_features", {}).get("transcript_meta") or meta.get("transcript_meta")), meta
        print(
            json.dumps(
                {
                    "db": str(path),
                    "event_count": int(event_count),
                    "duplicate_events": int(dupes),
                    "snapshot": snapshot,
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
    finally:
        con.close()


if __name__ == "__main__":
    main()
