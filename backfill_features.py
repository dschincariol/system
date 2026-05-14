"""Replay historical events into the versioned TimescaleDB feature store."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

from engine.runtime.storage import connect
from engine.strategy.feature_registry import compute_feature_snapshot, resolve_feature_ids
from engine.strategy.feature_store import FEATURE_STORE_VERSION, FeatureStore


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill historical feature rows into TimescaleDB feature_store.")
    parser.add_argument("--start-ts-ms", type=int, required=True, help="Inclusive event start timestamp in ms.")
    parser.add_argument("--end-ts-ms", type=int, required=True, help="Inclusive event end timestamp in ms.")
    parser.add_argument("--symbols", type=str, default="", help="Optional comma-separated symbol filter.")
    parser.add_argument(
        "--feature-ids",
        type=str,
        default="",
        help="Optional comma-separated feature ids. Defaults to current resolved feature set.",
    )
    parser.add_argument("--version", type=int, default=int(FEATURE_STORE_VERSION), help="Feature store version to write.")
    parser.add_argument("--fetch-size", type=int, default=500, help="SQLite fetch batch size.")
    parser.add_argument("--flush-timeout-s", type=float, default=30.0, help="Writer shutdown timeout.")
    return parser.parse_args(argv)


def _parse_symbol_filter(raw: str) -> list[str]:
    return [str(item or "").strip().upper() for item in str(raw or "").split(",") if str(item or "").strip()]


def _event_query(symbols: list[str]) -> tuple[str, tuple[Any, ...]]:
    clauses = [
        "symbol IS NOT NULL",
        "COALESCE(ts_ms, 0) > 0",
        "ts_ms >= ?",
        "ts_ms <= ?",
    ]
    params: list[Any] = []
    if symbols:
        clauses.append("UPPER(symbol) IN (" + ",".join("?" for _ in symbols) + ")")
        params.extend(list(symbols))
    sql = f"""
        SELECT ts_ms, symbol, source, title, body
        FROM events
        WHERE {" AND ".join(clauses)}
        ORDER BY ts_ms ASC, symbol ASC
    """
    return sql, tuple(params)


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    feature_ids = resolve_feature_ids(
        [item.strip() for item in str(args.feature_ids or "").split(",") if item.strip()]
        or None
    )
    store = FeatureStore()
    if not store.enabled:
        raise RuntimeError("feature_store_disabled_or_dsn_missing")

    symbols = _parse_symbol_filter(str(args.symbols or ""))
    con = connect(readonly=True)
    processed = 0
    scheduled = 0
    failed = 0
    last_ts_ms = 0
    try:
        sql, extra_params = _event_query(symbols)
        cur = con.execute(sql, (int(args.start_ts_ms), int(args.end_ts_ms), *extra_params))
        while True:
            rows = cur.fetchmany(max(1, int(args.fetch_size)))
            if not rows:
                break
            for ts_ms, symbol, source, title, body in rows:
                symbol_key = str(symbol or "").strip().upper()
                if not symbol_key:
                    continue
                event = {
                    "ts_ms": int(ts_ms or 0),
                    "ref_ts_ms": int(ts_ms or 0),
                    "source": str(source or ""),
                    "title": str(title or ""),
                    "body": str(body or ""),
                }
                features = compute_feature_snapshot(
                    event=event,
                    symbol=symbol_key,
                    feature_ids=list(feature_ids),
                )
                ok = await store.write_features(
                    symbol=symbol_key,
                    timestamp=int(event["ts_ms"]),
                    feature_dict=features,
                    version=int(args.version),
                )
                processed += 1
                last_ts_ms = int(event["ts_ms"])
                if ok:
                    scheduled += 1
                else:
                    failed += 1
    finally:
        try:
            con.close()
        except Exception:
            pass
        store.close(timeout_s=float(args.flush_timeout_s))

    return {
        "ok": bool(failed == 0),
        "processed": int(processed),
        "scheduled": int(scheduled),
        "failed": int(failed),
        "feature_ids": list(feature_ids),
        "feature_dim": int(len(feature_ids)),
        "version": int(args.version),
        "start_ts_ms": int(args.start_ts_ms),
        "end_ts_ms": int(args.end_ts_ms),
        "last_ts_ms": int(last_ts_ms),
        "symbols": list(symbols),
    }


def main(argv: list[str] | None = None) -> int:
    """Run the feature-store backfill CLI and return a process exit code."""
    args = _parse_args(list(argv or sys.argv[1:]))
    try:
        result = asyncio.run(_run(args))
    except Exception as exc:
        sys.stderr.write(f"backfill_features_failed: {type(exc).__name__}: {exc}\n")
        return 1
    sys.stdout.write(json.dumps(result, separators=(",", ":"), sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
