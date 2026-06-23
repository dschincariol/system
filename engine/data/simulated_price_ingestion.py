"""One-shot simulated price ingestion through the production price router."""

from __future__ import annotations

import time
from typing import Any, Dict, Iterable

from engine.data.live_prices.simulated import (
    PROVIDER_NAME,
    configured_simulated_symbols,
    simulated_quote_for_symbol,
)
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.price_router import publish_price_events
from engine.runtime.telemetry_append_buffer import append_price_provider_health, flush_telemetry_append_buffers


def build_simulated_price_events(
    symbols: Iterable[object] | None = None,
    *,
    ts_ms: int | None = None,
) -> list[dict[str, Any]]:
    now_ms = int(ts_ms if ts_ms is not None else time.time() * 1000)
    clean_symbols = [
        str(symbol or "").strip().upper()
        for symbol in (list(symbols or []) or configured_simulated_symbols())
        if str(symbol or "").strip()
    ]
    return [simulated_quote_for_symbol(symbol, ts_ms=now_ms) for symbol in dict.fromkeys(clean_symbols)]


def run_simulated_price_ingestion_once(
    *,
    con: Any = None,
    symbols: Iterable[object] | None = None,
    ts_ms: int | None = None,
    job_name: str = "ingest_simulated_prices",
) -> Dict[str, Any]:
    started = time.monotonic()
    now_ms = int(ts_ms if ts_ms is not None else time.time() * 1000)
    events = build_simulated_price_events(symbols, ts_ms=now_ms)
    counts = publish_price_events(
        events,
        con=con,
        write_prices=True,
        write_quotes=True,
        write_raw=True,
        emit_telemetry=False,
        component="engine.data.simulated_price_ingestion",
        job=str(job_name),
        default_provider=PROVIDER_NAME,
    )
    price_rows = int(counts.get("prices") or 0)
    quote_rows = int(counts.get("quotes") or 0)
    raw_rows = int(counts.get("raw") or 0)
    ok = bool(price_rows > 0)
    latency_ms = int(max(0.0, (time.monotonic() - started) * 1000.0))
    error = "" if ok else "simulated_price_ingestion_empty"
    try:
        append_price_provider_health(
            provider=PROVIDER_NAME,
            ok=ok,
            latency_ms=latency_ms,
            n_symbols=len(events),
            error=(None if ok else error),
            ts_ms=now_ms,
        )
    except Exception:
        pass
    try:
        record_pipeline_status(
            str(job_name),
            ok=ok,
            raw_rows=raw_rows,
            event_rows=0,
            last_ingested_ts_ms=now_ms if ok else None,
            error=(None if ok else error),
            latency_ms=latency_ms,
            meta={
                "provider": PROVIDER_NAME,
                "simulated": True,
                "symbols": [str(event.get("symbol") or "") for event in events],
                "price_rows": price_rows,
                "quote_rows": quote_rows,
                "raw_rows": raw_rows,
            },
            best_effort=True,
        )
    except Exception:
        pass
    try:
        flush_telemetry_append_buffers(
            max_batches=16,
            tables=("price_quotes_raw", "price_provider_health", "ingestion_pipeline_health"),
            path="simulated_price_ingestion",
        )
    except Exception:
        pass
    return {
        "ok": ok,
        "provider": PROVIDER_NAME,
        "simulated": True,
        "ts_ms": now_ms,
        "symbols": [str(event.get("symbol") or "") for event in events],
        "counts": dict(counts),
        "price_rows": price_rows,
        "quote_rows": quote_rows,
        "raw_rows": raw_rows,
        "latency_ms": latency_ms,
        **({"error": error} if error else {}),
    }
