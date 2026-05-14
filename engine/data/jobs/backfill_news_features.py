"""
Rebuild news enrichment and symbol feature tables for existing normalized news events.
"""

import json
import os
import logging
import time

from engine.data.ingest.news_enrichment import (
    build_enriched_news_records,
    infer_symbols,
    refresh_news_symbol_features,
)
from engine.runtime.storage import (
    connect,
    init_db,
    put_news_event_feature as _storage_put_news_event_feature,
)
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("engine.data.jobs.backfill_news_features")


def _put_news_event_feature(con, row) -> None:
    _storage_put_news_event_feature(row, con=con)


def _safe_json_dict(value):
    if isinstance(value, dict):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except Exception as e:
        log_failure(
            LOG,
            event="backfill_news_features_parse_meta_failed",
            code="BACKFILL_NEWS_FEATURES_PARSE_META_FAILED",
            message="Backfill news feature metadata parse failed.",
            error=e,
            level=logging.WARNING,
            component="engine.data.jobs.backfill_news_features",
            persist=False,
        )
        return {}
    return parsed if isinstance(parsed, dict) else {}


def main() -> None:
    init_db()
    batch_size = int(os.environ.get("NEWS_BACKFILL_BATCH_SIZE", "500"))
    remap_existing = str(os.environ.get("NEWS_BACKFILL_REMAP_EXISTING", "1")).strip().lower() in ("1", "true", "yes", "on")
    updated = 0
    con = connect()
    try:
        if remap_existing:
            con.execute(
                """
                DELETE FROM news_event_features
                WHERE event_id IN (
                  SELECT nef.event_id
                  FROM news_event_features nef
                  LEFT JOIN events e ON e.id = nef.event_id
                  WHERE e.id IS NULL
                )
                """
            )
            con.execute("DELETE FROM news_symbol_features")
        rows = con.execute(
            """
            SELECT e.id, e.ts_ms, e.event_type, e.symbol, e.source, e.title, e.body, e.url,
                   e.source_id, e.event_key, e.raw_payload, e.meta_json, e.derived_features,
                   nef.event_id, nef.ts_ms, nef.symbol, nef.cluster_key, nef.headline_key,
                   nef.sentiment_score, nef.novelty_score, nef.is_duplicate, nef.duplicate_count,
                   nef.company_match_method, nef.company_match_conf, nef.source_count, nef.meta_json
            FROM events e
            LEFT JOIN news_event_features nef ON nef.event_id = e.id
            WHERE e.event_type = 'news'
              AND (nef.event_id IS NULL OR (? = 1))
            ORDER BY e.ts_ms ASC
            LIMIT ?
            """,
            (1 if remap_existing else 0, int(batch_size)),
        ).fetchall()
        touched_symbols = set()
        existing_symbols = set()
        for row in rows or []:
            event_id = int(row[0])
            if row[3]:
                existing_symbols.add(str(row[3]).upper().strip())
            if row[15]:
                existing_symbols.add(str(row[15]).upper().strip())
            payload = {}
            try:
                payload = json.loads(str(row[10] or "{}"))
                if not isinstance(payload, dict):
                    payload = {}
            except Exception:
                payload = {}
            payload.update(
                {
                    "ts_ms": int(row[1]),
                    "event_type": str(row[2] or "news"),
                    "symbol": str(row[3]).upper().strip() if row[3] else None,
                    "source": str(row[4] or ""),
                    "title": str(row[5] or ""),
                    "body": str(row[6] or ""),
                    "url": str(row[7] or ""),
                    "source_id": str(row[8] or "") or None,
                    "event_key": str(row[9] or f"backfill:{event_id}"),
                    "meta_json": row[11],
                }
            )
            feature_exists = row[13] is not None
            chosen_event = {}
            if remap_existing and feature_exists:
                remap_payload = dict(payload)
                remap_payload.pop("symbol", None)
                remap_payload.pop("ticker", None)
                remap_payload.pop("symbols", None)
                symbol_info = infer_symbols(remap_payload, None)
                matched_symbols = list(symbol_info.get("symbols") or [])
                current_symbol = str(payload.get("symbol") or "").upper().strip() or None
                chosen_symbol = current_symbol if current_symbol and current_symbol in matched_symbols else (matched_symbols[0] if matched_symbols else None)
                method = symbol_info.get("match_method", {}).get(chosen_symbol or "", "none") if chosen_symbol else "none"
                conf = float(symbol_info.get("match_confidence", {}).get(chosen_symbol or "", 0.0)) if chosen_symbol else 0.0

                derived = _safe_json_dict(row[12])
                derived.update(
                    {
                        "matched_symbols": matched_symbols,
                        "symbol_match_method": method,
                        "symbol_match_confidence": float(conf),
                    }
                )
                event_meta = _safe_json_dict(row[11])
                event_meta.update(
                    {
                        "matched_symbols": matched_symbols,
                        "matched_symbol": chosen_symbol,
                        "symbol_match_method": method,
                        "symbol_match_confidence": float(conf),
                    }
                )
                feature_meta = _safe_json_dict(row[25])
                feature_meta.update(
                    {
                        "matched_symbols": matched_symbols,
                        "title": str(row[5] or ""),
                        "source": str(row[4] or ""),
                    }
                )
                chosen_event = {
                    "symbol": chosen_symbol,
                    "derived_features": derived,
                    "meta_json": json.dumps(event_meta, separators=(",", ":"), sort_keys=True),
                }
                feature_row = {
                    "event_id": int(event_id),
                    "ts_ms": int(row[14] or row[1]),
                    "symbol": chosen_symbol,
                    "cluster_key": row[16],
                    "headline_key": row[17],
                    "sentiment_score": float(row[18] or 0.0),
                    "novelty_score": float(row[19] or 0.0),
                    "is_duplicate": bool(row[20]),
                    "duplicate_count": int(row[21] or 0),
                    "company_match_method": method,
                    "company_match_conf": float(conf),
                    "source_count": int(row[24] or 0),
                    "meta_json": feature_meta,
                }
            else:
                enriched = build_enriched_news_records(con, payload, allowed_symbols=None)
                chosen = None
                if enriched:
                    for candidate in enriched:
                        if candidate["event"].get("symbol") == payload.get("symbol"):
                            chosen = candidate
                            break
                    chosen = chosen or enriched[0]
                    feature_row = dict(chosen.get("feature") or {})
                else:
                    feature_row = {
                        "ts_ms": int(row[1]),
                        "symbol": payload.get("symbol"),
                        "cluster_key": str(row[9] or ""),
                        "headline_key": str(row[5] or "").lower(),
                        "sentiment_score": 0.0,
                        "novelty_score": 0.0,
                        "is_duplicate": False,
                        "duplicate_count": 0,
                        "company_match_method": "backfill_fallback",
                        "company_match_conf": 0.0,
                        "source_count": 1,
                        "meta_json": {"backfill_fallback": True},
                    }
                feature_row["event_id"] = int(event_id)
                chosen_event = dict(chosen.get("event") or {}) if enriched and chosen is not None else {}

            con.execute(
                """
                UPDATE events
                SET
                  symbol = ?,
                  derived_features = ?,
                  meta_json = ?
                WHERE id = ?
                """,
                (
                    chosen_event.get("symbol"),
                    json.dumps(chosen_event.get("derived_features") or {}, separators=(",", ":"), sort_keys=True),
                    chosen_event.get("meta_json"),
                    int(event_id),
                ),
            )
            _put_news_event_feature(con, feature_row)
            if feature_row.get("symbol"):
                touched_symbols.add(str(feature_row["symbol"]).upper().strip())
            updated += 1

        for symbol in sorted(existing_symbols | touched_symbols):
            refresh_news_symbol_features(con, symbol)
        con.commit()
        print(
            json.dumps(
                {
                    "ts_ms": int(time.time() * 1000),
                    "updated_events": int(updated),
                    "updated_symbols": len(touched_symbols),
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
    finally:
        con.close()


if __name__ == "__main__":
    main()
