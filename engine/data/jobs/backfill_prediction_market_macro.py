"""Replay stored prediction-market macro snapshots without lookahead."""

from __future__ import annotations

import json
import os
import time

from engine.data.prediction_market_providers import (
    DEFAULT_MACRO_ASSETS,
    PREDICTION_MARKET_MACRO_FEATURE_IDS,
    parse_list,
)
from engine.runtime.storage import connect, init_db
from engine.strategy.model_feature_snapshots import build_model_feature_snapshot, store_model_feature_snapshots


JOB_NAME = "backfill_prediction_market_macro"


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)) or default)
    except Exception:
        return int(default)


def _symbols() -> list[str]:
    raw = parse_list(os.environ.get("PREDICTION_MARKET_BACKFILL_SYMBOLS"))
    if raw:
        return [symbol.upper() for symbol in raw]
    return list(DEFAULT_MACRO_ASSETS)


def _timestamps(con, *, start_ts_ms: int, end_ts_ms: int, limit: int) -> list[int]:
    rows = con.execute(
        """
        SELECT DISTINCT availability_ts_ms
        FROM prediction_market_markets
        WHERE provider_category = 'macro'
          AND availability_ts_ms >= ?
          AND availability_ts_ms <= ?
        ORDER BY availability_ts_ms ASC
        LIMIT ?
        """,
        (int(start_ts_ms), int(end_ts_ms), int(limit)),
    ).fetchall()
    return [int(row[0]) for row in rows or [] if row and int(row[0] or 0) > 0]


def run_backfill(*, start_ts_ms: int, end_ts_ms: int, limit: int, symbols: list[str]) -> dict:
    con = connect(readonly=False)
    try:
        timestamps = _timestamps(con, start_ts_ms=int(start_ts_ms), end_ts_ms=int(end_ts_ms), limit=int(limit))
        snapshots = []
        for ts_ms in timestamps:
            for symbol in symbols:
                snapshots.append(
                    build_model_feature_snapshot(
                        symbol=str(symbol),
                        ts_ms=int(ts_ms),
                        feature_ids=list(PREDICTION_MARKET_MACRO_FEATURE_IDS),
                        con=con,
                    )
                )
        written = store_model_feature_snapshots(snapshots, con=con)
        con.commit()
        return {
            "ok": True,
            "timestamps": int(len(timestamps)),
            "symbols": int(len(symbols)),
            "snapshots": int(len(snapshots)),
            "written": int(written),
            "feature_ids": list(PREDICTION_MARKET_MACRO_FEATURE_IDS),
        }
    finally:
        con.close()


def main() -> None:
    init_db()
    now_ms = int(time.time() * 1000)
    start_ts_ms = _env_int("PREDICTION_MARKET_BACKFILL_START_TS_MS", now_ms - 90 * 24 * 60 * 60 * 1000)
    end_ts_ms = _env_int("PREDICTION_MARKET_BACKFILL_END_TS_MS", now_ms)
    limit = _env_int("PREDICTION_MARKET_BACKFILL_LIMIT", 10000)
    result = run_backfill(start_ts_ms=start_ts_ms, end_ts_ms=end_ts_ms, limit=limit, symbols=_symbols())
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
