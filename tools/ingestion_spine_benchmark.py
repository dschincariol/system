#!/usr/bin/env python3
"""Focused ingestion-spine throughput benchmark.

The local non-price durable spool benchmark is safe by default. The Postgres
price write benchmark requires an explicit DSN and refuses non-loopback hosts
unless --allow-production-target is supplied.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlsplit

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.runtime.non_price_ingestion_spool import SQLiteNonPriceIngestionSpool
from engine.runtime.platform import default_local_artifacts_dir, is_loopback_host


def _now_ms() -> int:
    return int(time.time() * 1000)


def _is_loopback_dsn(dsn: str) -> bool:
    text = str(dsn or "").strip()
    if not text:
        return False
    if "://" in text:
        parsed = urlsplit(text)
        return is_loopback_host(parsed.hostname)
    pieces = dict(parse_qsl(text.replace(" ", "&"), keep_blank_values=True))
    host = str(pieces.get("host") or pieces.get("hostaddr") or "").strip()
    return (not host) or is_loopback_host(host)


def _rows_per_second(rows: int, elapsed_s: float) -> float:
    return float(int(rows) / max(0.000001, float(elapsed_s)))


def _default_output_path() -> Path:
    return default_local_artifacts_dir() / "ingestion_spine_benchmark.json"


def _non_price_spool_benchmark(*, rows: int, batch_size: int) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="ingestion-spine-spool-") as tmp:
        spool = SQLiteNonPriceIngestionSpool(
            path=Path(tmp) / "non_price_spool.sqlite",
            max_rows=max(rows * 2, batch_size),
            max_bytes=256 * 1024 * 1024,
            busy_timeout_ms=50,
            synchronous="NORMAL",
        )
        payload_rows = [
            (
                1_700_000_000_000 + idx,
                "poll_prices",
                1,
                10,
                idx,
                idx,
                1_700_000_000_000 + idx,
                None,
                "{}",
            )
            for idx in range(int(rows))
        ]
        enqueue_started = time.perf_counter()
        for offset in range(0, int(rows), int(batch_size)):
            spool.enqueue(
                table="ingestion_pipeline_health",
                rows=payload_rows[offset : offset + int(batch_size)],
                created_ts_ms=_now_ms(),
            )
        enqueue_elapsed_s = time.perf_counter() - enqueue_started

        selected_rows = 0
        selected_batches = 0
        drain_started = time.perf_counter()
        while True:
            records, corrupt = spool.select_batch(
                limit_rows=int(batch_size),
                tables=("ingestion_pipeline_health",),
            )
            if corrupt:
                raise RuntimeError(f"unexpected_corrupt_spool_records:{len(corrupt)}")
            if not records:
                break
            selected_rows += sum(int(record.total_rows) for record in records)
            selected_batches += len(records)
            spool.delete(record.id for record in records)
        drain_elapsed_s = time.perf_counter() - drain_started
        stats = spool.stats()
        return {
            "ok": True,
            "mode": "non_price_durable_spool",
            "rows": int(rows),
            "batch_size": int(batch_size),
            "enqueue_elapsed_s": float(enqueue_elapsed_s),
            "enqueue_rows_s": _rows_per_second(int(rows), enqueue_elapsed_s),
            "drain_elapsed_s": float(drain_elapsed_s),
            "drain_rows_s": _rows_per_second(int(selected_rows), drain_elapsed_s),
            "selected_batches": int(selected_batches),
            "dropped_rows": 0,
            "pending_rows_after": int(stats.get("pending_rows") or 0),
            "pending_bytes_after": int(stats.get("pending_bytes") or 0),
        }


def _synthetic_price_rows(rows_per_table: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    prices: list[dict[str, Any]] = []
    quotes: list[dict[str, Any]] = []
    raw: list[dict[str, Any]] = []
    base_ts_ms = 1_700_000_000_000
    for idx in range(int(rows_per_table)):
        symbol = f"BENCH{idx % 1000:04d}"
        ts_ms = base_ts_ms + idx
        prices.append(
            {
                "ts_ms": ts_ms,
                "symbol": symbol,
                "price": 100.0 + (idx % 100) / 100.0,
                "provider": "benchmark",
                "bid": 99.9,
                "ask": 100.1,
                "spread": 0.2,
                "volume": float(idx),
                "latency_ms": 1,
                "last_update_ts_ms": ts_ms,
                "ingest_ts_ms": ts_ms,
            }
        )
        quotes.append(
            {
                "ts_ms": ts_ms,
                "symbol": symbol,
                "last": 100.0 + (idx % 100) / 100.0,
                "bid": 99.9,
                "ask": 100.1,
                "spread": 0.2,
                "volume": float(idx),
                "provider": "benchmark",
                "last_trade_ts_ms": ts_ms,
                "last_quote_ts_ms": ts_ms,
                "last_update_ts_ms": ts_ms,
            }
        )
        raw.append(
            {
                "ts_ms": ts_ms,
                "symbol": symbol,
                "provider": "benchmark",
                "event_key": f"benchmark-{idx}",
                "event_type": "QUOTE",
                "event_ts_ms": ts_ms,
                "last": 100.0,
                "bid": 99.9,
                "ask": 100.1,
                "spread": 0.2,
                "volume": float(idx),
                "trade_ts_ms": ts_ms,
                "quote_ts_ms": ts_ms,
                "ingest_ts_ms": ts_ms,
                "source": "benchmark",
            }
        )
    return prices, quotes, raw


def _price_row_copy_boundary_benchmark(*, rows_per_table: int) -> dict[str, Any]:
    from engine.runtime.storage_pg_prices import _normalize_price_write_rows

    prices, quotes, raw = _synthetic_price_rows(int(rows_per_table))
    total_rows = len(prices) + len(quotes) + len(raw)
    started = time.perf_counter()
    normalized = _normalize_price_write_rows(prices=prices, quotes=quotes, raw=raw)
    elapsed_s = time.perf_counter() - started
    normalized_rows = len(normalized.price_rows) + len(normalized.quote_rows) + len(normalized.raw_rows)
    dropped_rows = int(sum(int(value) for value in normalized.dropped_rows.values()))
    return {
        "ok": True,
        "mode": "price_row_copy_boundary",
        "rows": int(total_rows),
        "rows_per_table": int(rows_per_table),
        "normalized_rows": int(normalized_rows),
        "dropped_rows": int(dropped_rows),
        "elapsed_s": float(elapsed_s),
        "rows_s": _rows_per_second(int(total_rows), elapsed_s),
        "row_copy_avoided_rows": int(normalized.row_copy_avoided_rows),
        "row_copy_fallback_rows": int(normalized.row_copy_fallback_rows),
        "safe_float_calls": int(normalized.safe_float_calls),
        "safe_int_calls": int(normalized.safe_int_calls),
        "datetime_conversions": int(normalized.datetime_conversions),
        "symbol_parses": int(normalized.symbol_parses),
        "event_key_normalizations": int(normalized.event_key_normalizations),
        "safe_float_calls_per_row": float(normalized.safe_float_calls) / max(1, int(total_rows)),
        "safe_int_calls_per_row": float(normalized.safe_int_calls) / max(1, int(total_rows)),
    }


def _price_shared_row_normalization_benchmark(*, rows_per_table: int) -> dict[str, Any]:
    from engine.runtime.storage_pg_prices import _normalize_price_write_rows

    rows: list[dict[str, Any]] = []
    base_ts_ms = 1_700_000_000_000
    for idx in range(int(rows_per_table)):
        ts_ms = base_ts_ms + idx
        rows.append(
            {
                "timestamp": ts_ms,
                "symbol": f"BENCH{idx % 1000:04d}",
                "price": 100.0 + (idx % 100) / 100.0,
                "last": 100.0 + (idx % 100) / 100.0,
                "provider": "benchmark",
                "source": "benchmark",
                "event_key": f"benchmark-{idx}",
                "event_type": "QUOTE",
                "event_ts_ms": ts_ms + 1,
                "bid": 99.9,
                "ask": 100.1,
                "spread": 0.2,
                "volume": float(idx),
                "latency_ms": 1,
                "provider_score": 1.0,
                "last_trade_ts_ms": ts_ms + 2,
                "last_quote_ts_ms": ts_ms + 3,
                "last_update_ts_ms": ts_ms + 4,
                "trade_ts_ms": ts_ms + 5,
                "quote_ts_ms": ts_ms + 6,
                "ingest_ts_ms": ts_ms + 7,
            }
        )
    total_rows = int(rows_per_table) * 3
    started = time.perf_counter()
    normalized = _normalize_price_write_rows(prices=rows, quotes=rows, raw=rows)
    elapsed_s = time.perf_counter() - started
    normalized_rows = len(normalized.price_rows) + len(normalized.quote_rows) + len(normalized.raw_rows)
    dropped_rows = int(sum(int(value) for value in normalized.dropped_rows.values()))
    legacy_safe_float_calls = int(rows_per_table) * (6 + 5 + 5)
    legacy_safe_int_calls = int(rows_per_table) * (4 + 4 + 5)
    legacy_datetime_conversions = int(rows_per_table) * 3
    legacy_symbol_parses = int(rows_per_table) * 3
    return {
        "ok": True,
        "mode": "price_shared_row_normalization",
        "rows": int(total_rows),
        "rows_per_table": int(rows_per_table),
        "unique_input_rows": int(rows_per_table),
        "normalized_rows": int(normalized_rows),
        "dropped_rows": int(dropped_rows),
        "elapsed_s": float(elapsed_s),
        "rows_s": _rows_per_second(int(total_rows), elapsed_s),
        "safe_float_calls": int(normalized.safe_float_calls),
        "legacy_safe_float_calls": int(legacy_safe_float_calls),
        "safe_int_calls": int(normalized.safe_int_calls),
        "legacy_safe_int_calls": int(legacy_safe_int_calls),
        "datetime_conversions": int(normalized.datetime_conversions),
        "legacy_datetime_conversions": int(legacy_datetime_conversions),
        "symbol_parses": int(normalized.symbol_parses),
        "legacy_symbol_parses": int(legacy_symbol_parses),
        "event_key_normalizations": int(normalized.event_key_normalizations),
        "safe_float_calls_saved": int(legacy_safe_float_calls) - int(normalized.safe_float_calls),
        "safe_int_calls_saved": int(legacy_safe_int_calls) - int(normalized.safe_int_calls),
        "datetime_conversions_saved": int(legacy_datetime_conversions) - int(normalized.datetime_conversions),
        "symbol_parses_saved": int(legacy_symbol_parses) - int(normalized.symbol_parses),
    }


def _price_pg_benchmark(
    *,
    dsn: str,
    rows_per_table: int,
    copy_enabled: bool,
    allow_production_target: bool,
) -> dict[str, Any]:
    if not allow_production_target and not _is_loopback_dsn(dsn):
        return {
            "ok": False,
            "mode": "price_copy_staging" if copy_enabled else "price_values_fallback",
            "skipped": True,
            "reason": "postgres_dsn_not_loopback; pass --allow-production-target to override",
        }
    from engine.runtime.storage_pg_prices import PostgresPriceStorage, PostgresPriceStorageConfig

    schema_name = f"ingestion_spine_bench_{os.getpid()}_{'copy' if copy_enabled else 'values'}"
    config = PostgresPriceStorageConfig(
        enabled=True,
        dsn=str(dsn),
        schema_name=schema_name,
        pool_min_size=1,
        pool_max_size=1,
        connect_timeout_s=5.0,
        lock_timeout_s=5.0,
        command_timeout_s=60.0,
        idle_in_txn_timeout_s=60.0,
        retry_attempts=1,
        retry_base_s=0.01,
        retry_max_s=0.1,
        application_name="ingestion-spine-benchmark",
        retention_days=0,
        compression_after_days=0,
        copy_enabled=bool(copy_enabled),
        copy_fallback_enabled=True,
    )
    storage = PostgresPriceStorage(config)
    prices, quotes, raw = _synthetic_price_rows(int(rows_per_table))
    total_rows = len(prices) + len(quotes) + len(raw)
    started = time.perf_counter()
    try:
        result = storage.write_batch(prices=prices, quotes=quotes, raw=raw)
        elapsed_s = time.perf_counter() - started
        return {
            "ok": True,
            "mode": "price_copy_staging" if copy_enabled else "price_values_fallback",
            "rows": int(total_rows),
            "rows_per_table": int(rows_per_table),
            "elapsed_s": float(elapsed_s),
            "rows_s": _rows_per_second(total_rows, elapsed_s),
            "write_path": str(result.get("write_path") or ""),
            "write_duration_ms": float(result.get("write_duration_ms") or 0.0),
            "dropped_rows": int(sum(int(v) for v in dict(result.get("dropped_rows") or {}).values())),
            "normalization_safe_float_calls": int(result.get("normalization_safe_float_calls") or 0),
            "normalization_safe_int_calls": int(result.get("normalization_safe_int_calls") or 0),
            "normalization_datetime_conversions": int(
                result.get("normalization_datetime_conversions") or 0
            ),
            "normalization_symbol_parses": int(result.get("normalization_symbol_parses") or 0),
            "normalization_event_key_normalizations": int(
                result.get("normalization_event_key_normalizations") or 0
            ),
        }
    finally:
        try:
            with storage._connection() as con:
                with con.cursor() as cur:
                    cur.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
                con.commit()
        except Exception:  # no-op-guard: allow - benchmark schema cleanup is best effort before closing storage.
            pass
        storage.close()


def run(args: argparse.Namespace) -> dict[str, Any]:
    results: dict[str, Any] = {
        "ok": True,
        "generated_ts_ms": _now_ms(),
        "rows_per_table": int(args.rows_per_table),
        "batch_size": int(args.batch_size),
        "benchmarks": [],
    }
    non_price = _non_price_spool_benchmark(
        rows=int(args.non_price_rows),
        batch_size=int(args.batch_size),
    )
    results["benchmarks"].append(non_price)
    results["benchmarks"].append(
        _price_row_copy_boundary_benchmark(rows_per_table=int(args.rows_per_table))
    )
    results["benchmarks"].append(
        _price_shared_row_normalization_benchmark(rows_per_table=int(args.rows_per_table))
    )
    if not bool(args.skip_postgres):
        dsn = str(args.postgres_dsn or os.environ.get("TIMESCALE_PRICES_DSN") or os.environ.get("TIMESCALE_DSN") or "").strip()
        if not dsn:
            results["benchmarks"].append(
                {
                    "ok": False,
                    "mode": "price_copy_staging",
                    "skipped": True,
                    "reason": "no_postgres_dsn",
                }
            )
            results["benchmarks"].append(
                {
                    "ok": False,
                    "mode": "price_values_fallback",
                    "skipped": True,
                    "reason": "no_postgres_dsn",
                }
            )
        else:
            results["benchmarks"].append(
                _price_pg_benchmark(
                    dsn=dsn,
                    rows_per_table=int(args.rows_per_table),
                    copy_enabled=True,
                    allow_production_target=bool(args.allow_production_target),
                )
            )
            if bool(args.include_values_fallback):
                results["benchmarks"].append(
                    _price_pg_benchmark(
                        dsn=dsn,
                        rows_per_table=int(args.rows_per_table),
                        copy_enabled=False,
                        allow_production_target=bool(args.allow_production_target),
                    )
                )
    results["ok"] = all(bool(item.get("ok") or item.get("skipped")) for item in list(results["benchmarks"]))
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-dsn", default="", help="Explicit local Postgres/Timescale DSN for price write benchmarks")
    parser.add_argument("--allow-production-target", action="store_true", help="Allow non-loopback Postgres DSNs")
    parser.add_argument("--skip-postgres", action="store_true", help="Run only the local durable-spool benchmark")
    parser.add_argument("--include-values-fallback", action="store_true", help="Also benchmark COPY-disabled VALUES upsert fallback")
    parser.add_argument("--rows-per-table", type=int, default=2000, help="Synthetic rows per price table")
    parser.add_argument("--non-price-rows", type=int, default=10000, help="Rows for the local durable non-price spool benchmark")
    parser.add_argument("--batch-size", type=int, default=500, help="Rows per synthetic enqueue/drain batch")
    parser.add_argument("--output", default=str(_default_output_path()), help="JSON output path")
    args = parser.parse_args(argv)

    if int(args.rows_per_table) <= 0 or int(args.non_price_rows) <= 0 or int(args.batch_size) <= 0:
        raise SystemExit("rows and batch-size must be positive")
    results = run(args)
    output_path = Path(str(args.output)).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(results, sort_keys=True))
    return 0 if bool(results.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
