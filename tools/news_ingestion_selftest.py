"""Smoke test for enriched news ingestion, dedupe, and transcript feature extraction."""

import json
import os
import sys
import tempfile
import time
from pathlib import Path


def main() -> None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    fd, path = tempfile.mkstemp(prefix="news_ingestion_selftest_", suffix=".db")
    os.close(fd)
    os.environ["DB_PATH"] = path

    from engine.runtime.storage import init_db, connect, put_normalized_event, put_news_event_feature
    from engine.data.event_normalization import normalize_news_event
    from engine.data.ingest.news_enrichment import build_enriched_news_records, refresh_news_symbol_features

    init_db()
    con = connect()
    try:
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
                    "db": path,
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
