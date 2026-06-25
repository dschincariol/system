# FILE: engine/data/poll_prices.py
"""
Live price poller:
- Yahoo Finance + CCXT
- Dynamic symbol auto-discovery (ACTIVE/WATCH)
- Staleness detection + alerts
- Outlier price detection
- Shadow-mode realized return capture (for calibration)

Writes canonical price events through engine.runtime.price_router.
"""

import os
import sys
import time
import random
import statistics
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError, as_completed
from dataclasses import dataclass, field
from typing import Dict, Any, Tuple, List, Optional

from engine.runtime import dbapi_compat as dbapi
from engine.runtime.storage import (
    _pid_is_running,
    connect,
    get_timescale_client,
    init_db,
    acquire_job_lock,
    register_after_commit,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
    run_write_txn,
)

from engine.data.live_prices.provider import get_price_provider_by_name
from engine.data.default_symbols import fx_pair_to_oanda_instrument, is_fx_major_symbol, load_default_symbols
from engine.data.provider_registry import get_polling_provider_names
from engine.data.provider_router import compute_provider_health, detect_cross_provider_anomalies, select_best_quotes_from_snapshots
from engine.data.provider_sessions import BaseProviderSession, ProviderSessionManager
from engine.data.price_hygiene import is_explained_split, is_split_like_price_jump, log_split_like_price_row
from engine.runtime.alerts import emit_alert
from engine.runtime.ingestion_shards import (
    current_ingestion_shard,
    filter_symbol_mapping_for_shard,
    ingestion_shard_job_name,
)
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.json_codec import dumps_text as _json_dumps_text
from engine.runtime.json_codec import loads as _json_loads
from engine.runtime.logging import get_logger, log_event
from engine.runtime.metrics import emit_counter, emit_gauge, emit_timing
from engine.runtime.runtime_meta import meta_get, meta_set, meta_set_if_missing
from engine.runtime.lifecycle_state import LIVE, WARMING_UP, set_state
from engine.runtime.price_router import price_persistence_backpressure_status, publish_price_events
from engine.runtime.telemetry_append_buffer import (
    enqueue_ingest_slippage_rows,
    enqueue_price_provider_health,
    get_telemetry_append_buffer_snapshot,
)
from engine.runtime.timeseries_write_policy import get_timeseries_write_policy
from engine.runtime.tracing import trace_event
from engine.runtime.failure_diagnostics import log_failure
from services.data_source_manager import get_manager

LOG = get_logger("engine.data.poll_prices")
_WARNED_NONFATAL_KEYS: set[str] = set()

if os.environ.get("ENGINE_SUPERVISED") != "1":
    print("WARN: poll_prices running without ENGINE_SUPERVISED=1 (continuing)", flush=True)


def _log_nonfatal(event: str, exc: BaseException, **context: Any) -> None:
    try:
        warn_key = context.pop("warn_key", None)
        if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
            return
        log_failure(
            LOG,
            event=str(event),
            code=str(event).upper(),
            message=str(event),
            error=exc,
            level=30,
            component="engine.data.poll_prices",
            extra=context or None,
            persist=False,
        )
        if warn_key:
            _WARNED_NONFATAL_KEYS.add(str(warn_key))
    except Exception as log_error:
        print(
            f"poll_prices_nonfatal_log_failed event={event} error={type(exc).__name__}: {exc} "
            f"log_error={type(log_error).__name__}: {log_error} context={context or {}}",
            file=sys.stderr,
            flush=True,
        )

# ------------------------------------------------------
# Runtime config
# ------------------------------------------------------
# poll_prices is the canonical polling market-data job. Session-managed streams
# may feed raw/provider state too, but this job is the fallback ensemble/poller.

POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "30"))
PRICE_STALE_AFTER_S = int(os.environ.get("PRICE_STALE_AFTER_S", "120"))
POLL_PRICES_STALE_WRITE_TIMEOUT_S = max(
    0.05,
    float(os.environ.get("POLL_PRICES_STALE_WRITE_TIMEOUT_S", "0.25") or 0.25),
)
POLL_PRICES_STALE_BUSY_TIMEOUT_MS = max(
    25,
    int(float(os.environ.get("POLL_PRICES_STALE_BUSY_TIMEOUT_MS", "250") or 250.0)),
)
POLL_PRICES_STARTUP_MERGED_WRITE_TIMEOUT_S = max(
    0.25,
    float(os.environ.get("POLL_PRICES_STARTUP_MERGED_WRITE_TIMEOUT_S", "3.0") or 3.0),
)
POLL_PRICES_STARTUP_MERGED_BUSY_TIMEOUT_MS = max(
    250,
    int(float(os.environ.get("POLL_PRICES_STARTUP_MERGED_BUSY_TIMEOUT_MS", "3000") or 3000.0)),
)
POLL_PRICES_STARTUP_MERGED_ATTEMPTS = max(
    1,
    int(float(os.environ.get("POLL_PRICES_STARTUP_MERGED_ATTEMPTS", "2") or 2.0)),
)
POLL_PRICES_STEADY_MERGED_WRITE_TIMEOUT_S = max(
    0.25,
    float(os.environ.get("POLL_PRICES_STEADY_MERGED_WRITE_TIMEOUT_S", "3.0") or 3.0),
)
POLL_PRICES_STEADY_MERGED_BUSY_TIMEOUT_MS = max(
    250,
    int(float(os.environ.get("POLL_PRICES_STEADY_MERGED_BUSY_TIMEOUT_MS", "3000") or 3000.0)),
)
OUTLIER_LOOKBACK = int(os.environ.get("PRICE_OUTLIER_LOOKBACK", "30"))
OUTLIER_Z = float(os.environ.get("PRICE_OUTLIER_Z", "3.5"))
PRICE_OUTLIER_REJECT_ENABLED = str(os.environ.get("PRICE_OUTLIER_REJECT_ENABLED", "0")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

JOB_NAME = "poll_prices"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
INGESTION_SHARD = current_ingestion_shard()
JOB_LIVENESS_NAME = ingestion_shard_job_name(JOB_NAME, INGESTION_SHARD)

FAIL_BASE_S = float(os.environ.get("POLL_FAIL_BASE_S", "2.0"))
FAIL_MAX_S = float(os.environ.get("POLL_FAIL_MAX_S", "60.0"))
PROVIDER_HEALTH_EVERY_S = float(os.environ.get("STREAM_PRICES_PROVIDER_HEALTH_EVERY_S", "15.0"))
POLL_PROVIDER_DEAD_AFTER_MS = int(os.environ.get("POLL_PROVIDER_DEAD_AFTER_MS", str(max(POLL_SECONDS * 3000, 30000))))
POLL_PRICES_PROVIDER_MAX_WORKERS = max(
    1,
    int(float(os.environ.get("POLL_PRICES_PROVIDER_MAX_WORKERS", "3") or 3.0)),
)
POLL_PRICES_PROVIDER_TIMEOUT_S = max(
    0.25,
    float(
        os.environ.get("POLL_PRICES_PROVIDER_TIMEOUT_S", str(max(float(POLL_SECONDS), 1.0)))
        or max(float(POLL_SECONDS), 1.0)
    ),
)

HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
INIT_DB_RETRY_ATTEMPTS = max(1, int(os.environ.get("POLL_PRICES_INIT_DB_RETRY_ATTEMPTS", os.environ.get("SQLITE_WRITE_RETRY_ATTEMPTS", "5"))))
INIT_DB_RETRY_BASE_MS = max(25, int(os.environ.get("POLL_PRICES_INIT_DB_RETRY_BASE_MS", os.environ.get("SQLITE_WRITE_RETRY_BASE_MS", "150"))))
INIT_DB_RETRY_MAX_MS = max(INIT_DB_RETRY_BASE_MS, int(os.environ.get("POLL_PRICES_INIT_DB_RETRY_MAX_MS", os.environ.get("SQLITE_WRITE_RETRY_MAX_MS", "2000"))))


def _put_ingest_slippage_batch(con, rows) -> None:
    """
    rows: [(ts_ms, symbol, provider, last, bid, ask, mid, spread, px_minus_mid, abs_px_minus_mid), ...]
    """
    con.executemany(
        """
        INSERT INTO ingest_slippage(
          ts_ms, symbol, provider,
          last, bid, ask, mid, spread,
          px_minus_mid, abs_px_minus_mid
        )
        VALUES (?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, provider, ts_ms) DO UPDATE SET
          last=excluded.last,
          bid=excluded.bid,
          ask=excluded.ask,
          mid=excluded.mid,
          spread=excluded.spread,
          px_minus_mid=excluded.px_minus_mid,
          abs_px_minus_mid=excluded.abs_px_minus_mid
        """,
        rows,
    )


def _float_or_none(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return None
    if out != out or out in (float("inf"), float("-inf")):
        return None
    return float(out)


def _build_futures_contract_bar_row(
    *,
    provider_name: str,
    symbol: str,
    row: Dict[str, Any],
    provider_symbol_map: Dict[str, str],
    now_ts_ms: int,
) -> Optional[Tuple[str, int, float, float, float, float, Optional[float], Optional[float], str]]:
    if str(provider_name or "").strip().lower() != "futures":
        return None
    sym = str(symbol or "").strip()
    contract = str(
        provider_symbol_map.get(sym)
        or provider_symbol_map.get(sym.upper())
        or row.get("contract")
        or row.get("symbol")
        or sym
    ).strip()
    if not contract:
        return None
    close = _float_or_none(row.get("close", row.get("price", row.get("last"))))
    if close is None:
        return None
    open_px = _float_or_none(row.get("open"))
    high_px = _float_or_none(row.get("high"))
    low_px = _float_or_none(row.get("low"))
    volume = _float_or_none(row.get("volume"))
    open_interest = _float_or_none(row.get("open_interest"))
    try:
        ts_ms = int(row.get("ts_ms") or now_ts_ms)
    except Exception:
        ts_ms = int(now_ts_ms)
    return (
        str(contract),
        int(ts_ms),
        float(open_px if open_px is not None else close),
        float(high_px if high_px is not None else close),
        float(low_px if low_px is not None else close),
        float(close),
        volume,
        open_interest,
        str(row.get("source") or provider_name or "futures"),
    )


def _put_futures_contract_bars_batch(con, rows) -> None:
    if not rows:
        return
    from engine.data.live_prices.futures_live import ensure_futures_bars_table

    ensure_futures_bars_table(con)
    con.executemany(
        """
        INSERT INTO futures_contract_bars(
          contract, ts_ms, open, high, low, close, volume, open_interest, source
        )
        VALUES (?,?,?,?,?,?,?,?,?)
        ON CONFLICT(contract, ts_ms) DO UPDATE SET
          open=excluded.open,
          high=excluded.high,
          low=excluded.low,
          close=excluded.close,
          volume=excluded.volume,
          open_interest=excluded.open_interest,
          source=excluded.source
        """,
        rows,
    )


def _enqueue_provider_auxiliary_rows(
    raw_quote_rows,
    slip_rows,
    futures_bar_rows=None,
) -> bool:
    ok = True

    if raw_quote_rows:
        raw_events = [
            {
                "timestamp": int(row[0]),
                "symbol": str(row[1]),
                "provider": str(row[2]),
                "source": str(row[2]),
                "last": row[3],
                "bid": row[4],
                "ask": row[5],
                "volume": row[7],
            }
            for row in raw_quote_rows
        ]
        try:
            publish_counts = publish_price_events(
                raw_events,
                con=None,
                write_prices=False,
                write_quotes=False,
                write_raw=True,
                emit_telemetry=False,
                component="engine.data.poll_prices",
                job=JOB_NAME,
            )
            async_status = price_persistence_backpressure_status(publish_counts)
            if bool(async_status.get("backpressure")):
                ok = False
                reason = str(async_status.get("reason") or "async_price_writer_backpressure")
                _log_nonfatal(
                    "poll_prices_provider_raw_async_backpressure",
                    RuntimeError(reason),
                    warn_key="poll_prices_provider_raw_async_backpressure",
                    raw_rows=int(len(raw_quote_rows)),
                    async_persistence=async_status,
                )
        except Exception as e:
            ok = False
            _log_nonfatal(
                "poll_prices_provider_raw_enqueue_failed",
                e,
                warn_key="poll_prices_provider_raw_enqueue_failed",
                raw_rows=int(len(raw_quote_rows)),
            )

    if futures_bar_rows:
        provider_rows_ok, _ = _persist_futures_contract_bars_sync(futures_bar_rows)
        if not provider_rows_ok:
            ok = False

    if slip_rows:
        try:
            accepted = enqueue_ingest_slippage_rows(tuple(slip_rows))
            if not accepted:
                raise RuntimeError("ingest_slippage_buffer_rejected")
        except Exception as e:
            ok = False
            snapshot: Dict[str, Any]
            try:
                snapshot = dict(get_telemetry_append_buffer_snapshot() or {})
            except Exception:
                snapshot = {}
            _log_nonfatal(
                "poll_prices_ingest_slippage_enqueue_failed",
                e,
                warn_key="poll_prices_ingest_slippage_enqueue_failed",
                slip_rows=int(len(slip_rows)),
                telemetry_append_buffer=snapshot or None,
            )

    return bool(ok)


def _persist_provider_auxiliary_rows_sync(
    raw_quote_rows,
    slip_rows,
    futures_bar_rows=None,
):
    if raw_quote_rows:
        raw_events = [
            {
                "timestamp": int(row[0]),
                "symbol": str(row[1]),
                "provider": str(row[2]),
                "source": str(row[2]),
                "last": row[3],
                "bid": row[4],
                "ask": row[5],
                "volume": row[7],
            }
            for row in raw_quote_rows
        ]
    else:
        raw_events = []

    def _write_provider_rows(conw):
        if raw_events:
            publish_price_events(
                raw_events,
                con=conw,
                write_prices=False,
                write_quotes=False,
                write_raw=True,
                emit_telemetry=False,
                component="engine.data.poll_prices",
                job=JOB_NAME,
            )
        if slip_rows:
            _put_ingest_slippage_batch(conw, slip_rows)
        if futures_bar_rows:
            _put_futures_contract_bars_batch(conw, futures_bar_rows)

    return _run_write_txn_allow_busy(
        _write_provider_rows,
        default=None,
        table="prices",
        operation="ingest_provider_price_rows",
        context={
            "job": JOB_NAME,
            "raw_rows": int(len(raw_quote_rows)),
            "slip_rows": int(len(slip_rows)),
            "futures_bar_rows": int(len(futures_bar_rows or ())),
        },
        attempts=1,
        maintenance=False,
        busy_event="poll_prices_provider_rows_write_busy",
        warn_key="poll_prices_provider_rows_write_busy",
        extra={
            "raw_rows": int(len(raw_quote_rows)),
            "slip_rows": int(len(slip_rows)),
            "futures_bar_rows": int(len(futures_bar_rows or ())),
        },
        timeout_s=0.5,
        busy_timeout_ms=500,
    )


def _persist_futures_contract_bars_sync(futures_bar_rows):
    def _write_futures_bars(conw):
        _put_futures_contract_bars_batch(conw, futures_bar_rows)

    return _run_write_txn_allow_busy(
        _write_futures_bars,
        default=None,
        table="futures_contract_bars",
        operation="ingest_futures_contract_bars",
        context={"job": JOB_NAME, "futures_bar_rows": int(len(futures_bar_rows or ()))},
        attempts=1,
        maintenance=False,
        busy_event="poll_prices_futures_bars_write_busy",
        warn_key="poll_prices_futures_bars_write_busy",
        extra={"futures_bar_rows": int(len(futures_bar_rows or ()))},
        timeout_s=0.5,
        busy_timeout_ms=500,
    )


def _is_database_slowdown_error(error: Exception) -> bool:
    if dbapi.is_transient_write_error(error):
        return True
    text = str(error or "").strip().lower()
    return any(
        marker in text
        for marker in (
            "pool timeout",
            "pooltimeout",
            "connection pool timeout",
            "couldn't get a connection",
            "could not get a connection",
            "postgres_recently_unavailable",
            "statement timeout",
            "canceling statement due to statement timeout",
            "too many connections",
        )
    )


def _run_write_txn_allow_busy(
    fn,
    *,
    default,
    table: str,
    operation: str,
    context: Optional[Dict[str, Any]] = None,
    attempts: Optional[int] = None,
    maintenance: bool = True,
    busy_event: str,
    warn_key: str,
    extra: Optional[Dict[str, Any]] = None,
    timeout_s: Optional[float] = None,
    busy_timeout_ms: Optional[int] = None,
):
    try:
        result = run_write_txn(
            fn,
            table=table,
            operation=operation,
            context=context,
            attempts=attempts,
            maintenance=maintenance,
            timeout_s=timeout_s,
            busy_timeout_ms=busy_timeout_ms,
        )
        return True, result
    except Exception as e:
        if _is_database_slowdown_error(e):
            _log_nonfatal(
                busy_event,
                e,
                warn_key=warn_key,
                **(extra or {}),
            )
            return False, default
        raise


def _has_first_price_tick() -> bool:
    return bool(str(meta_get("first_price_ts_ms") or "").strip())


def _should_persist_provider_rows() -> bool:
    return _has_first_price_tick()


def _merged_price_write_budget() -> Dict[str, Any]:
    if not _has_first_price_tick():
        return {
            "attempts": int(POLL_PRICES_STARTUP_MERGED_ATTEMPTS),
            "timeout_s": float(POLL_PRICES_STARTUP_MERGED_WRITE_TIMEOUT_S),
            "busy_timeout_ms": int(POLL_PRICES_STARTUP_MERGED_BUSY_TIMEOUT_MS),
        }
    return {
        "attempts": 1,
        "timeout_s": float(POLL_PRICES_STEADY_MERGED_WRITE_TIMEOUT_S),
        "busy_timeout_ms": int(POLL_PRICES_STEADY_MERGED_BUSY_TIMEOUT_MS),
    }


def _register_timescale_price_rows_after_commit(con, price_rows, quote_rows) -> None:
    if not price_rows:
        return
    client = get_timescale_client()
    if client is None or not bool(getattr(client, "enabled", False)):
        return

    quote_by_key = {(int(row[0]), str(row[1])): row for row in (quote_rows or [])}
    payload = []
    for ts_ms, sym, px in price_rows:
        qrow = quote_by_key.get((int(ts_ms), str(sym)))
        volume = qrow[6] if qrow and len(qrow) > 6 else None
        try:
            volume_f = float(volume) if volume is not None else 0.0
        except Exception:
            volume_f = 0.0
        payload.append(
            {
                "symbol": str(sym),
                "timestamp": int(ts_ms),
                "open": float(px),
                "high": float(px),
                "low": float(px),
                "close": float(px),
                "volume": float(volume_f),
            }
        )
    if not payload:
        return

    rows = tuple(payload)

    def _enqueue() -> None:
        try:
            client.enqueue_price_data(rows)
        except Exception as e:
            _log_nonfatal(
                "poll_prices_timescale_enqueue_failed",
                e,
                warn_key="poll_prices_timescale_enqueue_failed",
                rows=int(len(rows)),
            )

    register_after_commit(con, _enqueue)


# ------------------------------------------------------
# Helpers
# ------------------------------------------------------

def _sleep_with_jitter(seconds: float) -> None:
    if seconds <= 0:
        return
    j = seconds * 0.2
    time.sleep(max(0.05, seconds + random.uniform(-j, j)))


def _next_fail_backoff_s(current_s: float) -> float:
    return min(float(FAIL_MAX_S), float(current_s) * 2.0 if float(current_s) > 0.0 else float(FAIL_BASE_S))


def _successful_price_cycle_backoff_s(*, current_fail_s: float, producer_backpressure: bool) -> float:
    if producer_backpressure:
        return _next_fail_backoff_s(float(current_fail_s))
    return 0.0


def _ensure_price_feed_lock_table(con) -> None:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ANY (current_schemas(false))
          AND table_name='price_feed_lock'
        LIMIT 1
        """
    ).fetchone()
    if row:
        return
    raise dbapi.OperationalError(
        "price_feed_lock missing; call engine.runtime.storage.init_db() before poll_prices"
    )


def _acquire_price_feed_lock() -> bool:
    now_ts_ms = int(time.time() * 1000)
    stale_after_ms = int(LOCK_STALE_AFTER_S * 1000)
    def _write(con_lock) -> bool:
        row = con_lock.execute(
            "SELECT owner,pid,ts_ms FROM price_feed_lock WHERE id=1"
        ).fetchone()

        if row:
            owner, pid, ts_ms = row
            pid = int(pid or 0)
            ts_ms = int(ts_ms or 0)
            is_same_owner = (str(owner) == JOB_NAME and int(pid) == int(PID))
            pid_running = _pid_is_running(pid)
            is_stale = (ts_ms <= 0) or ((now_ts_ms - ts_ms) >= stale_after_ms)

            if is_same_owner:
                con_lock.execute(
                    "UPDATE price_feed_lock SET owner=?, pid=?, ts_ms=? WHERE id=1",
                    (JOB_NAME, int(PID), int(now_ts_ms)),
                )
                return True

            if pid_running and not is_stale:
                return False

            log_event(
                LOG,
                30,
                "poll_prices_recovering_feed_lock",
                component="engine.data.poll_prices",
                extra={
                    "job": JOB_NAME,
                    "current_pid": int(PID),
                    "previous_owner": str(owner),
                    "previous_pid": int(pid),
                    "pid_running": bool(pid_running),
                    "is_stale": bool(is_stale),
                    "previous_ts_ms": int(ts_ms),
                },
            )
            con_lock.execute("DELETE FROM price_feed_lock WHERE id=1")

        con_lock.execute(
            "INSERT OR REPLACE INTO price_feed_lock(id,owner,pid,ts_ms) VALUES(1,?,?,?)",
            (JOB_NAME, PID, now_ts_ms),
        )
        return True
    return bool(
        run_write_txn(
            _write,
            table="price_feed_lock",
            operation="acquire_price_feed_lock",
            context={"job": JOB_NAME, "pid": int(PID)},
            direct=True,
            maintenance=False,
        )
    )

def _touch_price_feed_lock(now_ts_ms: int) -> None:
    def _write(con) -> None:
        con.execute(
            """
            UPDATE price_feed_lock
            SET owner=?, pid=?, ts_ms=?
            WHERE id=1 AND pid=?
            """,
            (JOB_NAME, PID, int(now_ts_ms), PID),
        )
    run_write_txn(
        _write,
        table="price_feed_lock",
        operation="touch_price_feed_lock",
        context={"job": JOB_NAME, "pid": int(PID)},
        direct=True,
        maintenance=False,
    )


def _release_price_feed_lock() -> None:
    def _write(con) -> None:
        con.execute("DELETE FROM price_feed_lock WHERE id=1 AND pid=?", (PID,))
    run_write_txn(
        _write,
        table="price_feed_lock",
        operation="release_price_feed_lock",
        context={"job": JOB_NAME, "pid": int(PID)},
        direct=True,
        maintenance=False,
    )


class PollingProviderSession(BaseProviderSession):
    def __init__(self, provider_name: str, provider_obj: Any, poll_interval_s: float) -> None:
        super().__init__(provider_name)
        self.provider_name = str(provider_name)
        self._provider = provider_obj
        self._poll_interval_s = float(max(1.0, poll_interval_s))
        self._symbol_map: Dict[str, str] = {}
        self._last_snapshot: Dict[str, Dict[str, Any]] = {}
        self._last_poll_ts_ms = 0
        self._last_latency_ms = 0
        self.set_capability("streaming", False)
        self.set_capability("polling", True)
        self.set_capability("gap_fill", True)
        self.set_capability("historical_catchup", "snapshot")
        self.set_capability("authentication", "provider_object")
        self.set_capability("supports_quotes", True)
        self.set_capability("supports_trades", True)
        self.set_capability("supports_historical", True)
        self.set_capability("supports_snapshot", True)
        # Poll cadence is already enforced inside snapshot(); sharing a tiny
        # per-minute bucket with connect/authenticate/subscription lifecycle
        # calls can leave the session "connected" but unable to fetch fresh
        # prices after boot.
        self.set_capability("rate_limit_per_min", None)
        self.set_capability("capability_source", "provider_session")

    def set_symbol_map(self, symbol_map: Dict[str, str]) -> None:
        clean = {str(k): str(v) for k, v in (symbol_map or {}).items() if str(k).strip() and str(v).strip()}
        with self._lock:
            self._symbol_map = clean

    def connect(self) -> None:
        if self._provider is None:
            raise RuntimeError(f"{self.provider_name}_provider_unavailable")
        self.note_connected()

    def authenticate(self) -> None:
        if self._provider is None:
            raise RuntimeError(f"{self.provider_name}_provider_unavailable")
        self.note_authenticated()

    def detect_capabilities(self) -> Dict[str, Any]:
        self.set_capability("supports_snapshot", True)
        self.set_capability("supports_historical", True)
        return self.telemetry_snapshot().get("capabilities") or {}

    def subscribe(self, symbols) -> None:
        clean = {str(x).strip() for x in (symbols or []) if str(x).strip()}
        self.update_subscribed_symbols(self.subscribed_symbols() | clean)

    def unsubscribe(self, symbols) -> None:
        clean = {str(x).strip() for x in (symbols or []) if str(x).strip()}
        self.update_subscribed_symbols(self.subscribed_symbols() - clean)

    def close(self) -> None:
        self.note_disconnected("closed")
        self.update_subscribed_symbols(set())

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        if self._provider is None:
            return {}
        now_ts_ms = int(time.time() * 1000)
        if self._last_poll_ts_ms and (now_ts_ms - self._last_poll_ts_ms) < int(self._poll_interval_s * 1000.0):
            with self._lock:
                return {k: dict(v) for k, v in self._last_snapshot.items()}

        with self._lock:
            symbol_map = dict(self._symbol_map)
        if not symbol_map:
            return {}

        t0 = int(time.time() * 1000)
        got = self._provider.fetch_last_prices(symbol_map) or {}
        self._last_latency_ms = max(0, int(time.time() * 1000) - t0)
        self._last_poll_ts_ms = int(time.time() * 1000)

        normalized: Dict[str, Dict[str, Any]] = {}
        max_msg_ts_ms = 0
        for sym, rec in (got or {}).items():
            if not isinstance(rec, dict):
                continue
            row = dict(rec)
            if not row.get("source"):
                row["source"] = self.provider_name
            ts_ms = int(row.get("ts_ms") or self._last_poll_ts_ms)
            row["ts_ms"] = ts_ms
            normalized[str(sym)] = row
            max_msg_ts_ms = max(max_msg_ts_ms, ts_ms)

        if normalized:
            with self._lock:
                self._last_snapshot.update(normalized)
            # Polling providers can legitimately return rows whose market timestamp
            # does not advance on every successful fetch (for example on weekends or
            # when the upstream snapshot carries the last trade timestamp). The
            # supervisor's liveness checks should track successful poll cadence, not
            # only the source row timestamp, otherwise healthy polling jobs get
            # misclassified as stalled and restarted in a loop.
            self.note_message(max(max_msg_ts_ms, self._last_poll_ts_ms))

        with self._lock:
            return {k: dict(v) for k, v in self._last_snapshot.items()}

    def merge_snapshot(self, rows: Dict[str, Dict[str, Any]]) -> None:
        with self._lock:
            for sym, rec in (rows or {}).items():
                cur = dict(self._last_snapshot.get(str(sym)) or {})
                cur.update(dict(rec or {}))
                self._last_snapshot[str(sym)] = cur

    def perform_gap_fill(self, symbols, since_ts_ms: int) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            symbol_map = {str(sym): self._symbol_map.get(str(sym), str(sym)) for sym in (symbols or []) if str(sym).strip()}
        if not symbol_map or self._provider is None:
            return {}
        got = self._provider.fetch_last_prices(symbol_map) or {}
        out: Dict[str, Dict[str, Any]] = {}
        for sym, rec in (got or {}).items():
            if not isinstance(rec, dict):
                continue
            row = dict(rec)
            row["gap_fill"] = True
            row["gap_fill_since_ts_ms"] = int(since_ts_ms or 0)
            if not row.get("source"):
                row["source"] = self.provider_name
            row["ts_ms"] = int(row.get("ts_ms") or int(time.time() * 1000))
            out[str(sym)] = row
        return out

    def latency_ms(self) -> int:
        return int(self._last_latency_ms)


@dataclass(frozen=True)
class ActiveSymbolUniverse:
    active_symbol_rows: Tuple[Tuple[str, Any], ...]
    provider_symbol_rows: Tuple[Tuple[str, Any], ...]
    yf_map: Dict[str, str]
    ccxt_map: Dict[str, str]
    polygon_map: Dict[str, str]
    oanda_map: Dict[str, str] = field(default_factory=dict)
    futures_map: Dict[str, str] = field(default_factory=dict)

    @property
    def assigned_symbol_count(self) -> int:
        return int(
            len(set(self.yf_map) | set(self.ccxt_map) | set(self.polygon_map) | set(self.oanda_map) | set(self.futures_map))
        )


@dataclass(frozen=True)
class PollPriceCycleSymbolPlan:
    universe: ActiveSymbolUniverse
    provider_symbol_maps: Dict[str, Dict[str, str]]

    @property
    def assigned_symbol_count(self) -> int:
        return int(self.universe.assigned_symbol_count)


def _normalize_symbol_rows(rows) -> Tuple[Tuple[str, Any], ...]:
    out: List[Tuple[str, Any]] = []
    for row in rows or []:
        try:
            raw_symbol = row[0]
            meta_json = row[1] if len(row) > 1 else None
        except Exception:
            continue
        sym_s = str(raw_symbol or "").strip().upper()
        if not sym_s:
            continue
        out.append((sym_s, meta_json))
    return tuple(out)


def _fetch_active_symbol_rows(con) -> Tuple[Tuple[str, Any], ...]:
    rows = con.execute(
        """
        SELECT symbol, meta_json
        FROM symbols
        WHERE status IN ('ACTIVE','WATCH')
        """
    ).fetchall() or []
    return _normalize_symbol_rows(rows)


def _fetch_fallback_symbol_rows(con) -> Tuple[Tuple[str, Any], ...]:
    rows = con.execute(
        """
        SELECT symbol, meta_json
        FROM symbols
        ORDER BY updated_ts_ms DESC, created_ts_ms DESC, symbol
        LIMIT 250
        """
    ).fetchall() or []
    return _normalize_symbol_rows(rows)


def _load_active_symbol_universe() -> ActiveSymbolUniverse:
    con = None
    owns = False
    active_rows: Tuple[Tuple[str, Any], ...] = tuple()
    provider_rows: Tuple[Tuple[str, Any], ...] = tuple()
    try:
        con = connect()
        owns = True
        active_rows = _fetch_active_symbol_rows(con)
        provider_rows = active_rows
        if not provider_rows:
            provider_rows = _fetch_fallback_symbol_rows(con)
    finally:
        if owns and con is not None:
            try:
                con.close()
            except Exception as e:
                _log_nonfatal(
                    "poll_prices_symbol_discovery_connection_close_failed",
                    e,
                    warn_key="poll_prices_symbol_discovery_connection_close_failed",
                )

    yf_map: Dict[str, str] = {}
    ccxt_map: Dict[str, str] = {}
    polygon_map: Dict[str, str] = {}
    oanda_map: Dict[str, str] = {}
    futures_map: Dict[str, str] = {}

    for sym, meta_json in provider_rows:
        try:
            meta = _json_loads(meta_json) if meta_json else {}
        except Exception:
            meta = {}

        sym_s = str(sym).strip().upper()
        if not sym_s:
            continue

        provider = str(meta.get("price_provider") or "").strip().lower()

        if provider == "oanda" or str(meta.get("oanda_instrument") or "").strip():
            try:
                oanda_map[sym_s] = str(meta.get("oanda_instrument") or fx_pair_to_oanda_instrument(sym_s))
            except ValueError as e:
                _log_nonfatal("poll_prices_oanda_symbol_map_failed", e, symbol=sym_s)
            continue

        if provider == "futures" or str(meta.get("futures_contract") or "").strip():
            contract = str(meta.get("futures_contract") or sym_s).strip()
            if contract:
                futures_map[sym_s] = contract
            continue

        if provider == "ccxt":
            mkt = meta.get("ccxt_market")
            if mkt:
                ccxt_map[sym_s] = str(mkt)
            continue

        if provider in ("polygon", "polygon_ws"):
            polygon_map[sym_s] = str(meta.get("polygon_ticker") or sym_s)
            continue

        yf_ticker = meta.get("yf_ticker") or sym_s
        yf_map[sym_s] = str(yf_ticker)

    env_symbols = load_default_symbols()
    for sym in env_symbols:
        if is_fx_major_symbol(sym):
            try:
                oanda_map.setdefault(sym, fx_pair_to_oanda_instrument(sym))
            except ValueError as e:
                _log_nonfatal("poll_prices_oanda_env_symbol_map_failed", e, symbol=str(sym))
            continue
        yf_map.setdefault(sym, sym)
        polygon_map.setdefault(sym, sym)

    if "VIX" not in yf_map:
        yf_map["VIX"] = "^VIX"

    if os.environ.get("FORCE_FACTOR_PROXY_TICKERS", "1") == "1":
        yf_map.setdefault("TNX", "^TNX")
        yf_map.setdefault("FVX", "^FVX")
        yf_map.setdefault("HYG", "HYG")
        yf_map.setdefault("LQD", "LQD")
        yf_map.setdefault("SPY", "SPY")
        yf_map.setdefault("AGG", "AGG")

    if not polygon_map:
        polygon_map["SPY"] = "SPY"

    yf_map = filter_symbol_mapping_for_shard(yf_map, INGESTION_SHARD)
    ccxt_map = filter_symbol_mapping_for_shard(ccxt_map, INGESTION_SHARD)
    polygon_map = filter_symbol_mapping_for_shard(polygon_map, INGESTION_SHARD)
    oanda_map = filter_symbol_mapping_for_shard(oanda_map, INGESTION_SHARD)
    futures_map = filter_symbol_mapping_for_shard(futures_map, INGESTION_SHARD)
    return ActiveSymbolUniverse(
        active_symbol_rows=active_rows,
        provider_symbol_rows=provider_rows,
        yf_map=yf_map,
        ccxt_map=ccxt_map,
        polygon_map=polygon_map,
        oanda_map=oanda_map,
        futures_map=futures_map,
    )


def _load_symbol_providers() -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
    universe = _load_active_symbol_universe()
    return universe.yf_map, universe.ccxt_map, universe.polygon_map


def _provider_symbol_map_for_cycle(provider_name: str, universe: ActiveSymbolUniverse) -> Dict[str, str]:
    provider = str(provider_name or "").strip().lower()
    if provider in {"polygon", "polygon_ws"}:
        return dict(universe.polygon_map)
    if provider == "simulated":
        symbol_map = dict(universe.yf_map)
        if not symbol_map:
            try:
                from engine.data.live_prices.simulated import configured_simulated_symbols

                symbol_map = {sym: sym for sym in configured_simulated_symbols()}
            except Exception as e:
                _log_nonfatal(
                    "poll_prices_simulated_symbol_map_failed",
                    e,
                    warn_key="poll_prices_simulated_symbol_map_failed",
                )
                symbol_map = {"SPY": "SPY", "AAPL": "AAPL"}
        return symbol_map
    if provider == "oanda":
        return dict(universe.oanda_map)
    if provider == "futures":
        return dict(universe.futures_map)
    if provider == "ccxt":
        return dict(universe.ccxt_map)
    return dict(universe.yf_map)


def _simulated_market_data_enabled() -> bool:
    try:
        from engine.data.live_prices.simulated import simulated_market_data_enabled

        return bool(simulated_market_data_enabled())
    except Exception as e:
        _log_nonfatal(
            "poll_prices_simulated_enabled_check_failed",
            e,
            warn_key="poll_prices_simulated_enabled_check_failed",
        )
        return False


def _append_simulated_provider_fallback(chain: List[str]) -> List[str]:
    out = [str(p or "").strip().lower() for p in (chain or []) if str(p or "").strip()]
    if _simulated_market_data_enabled() and "simulated" not in out:
        out.append("simulated")
    return list(dict.fromkeys(out))


def _build_cycle_symbol_plan(
    provider_names: List[str],
    *,
    universe: Optional[ActiveSymbolUniverse] = None,
) -> PollPriceCycleSymbolPlan:
    cycle_universe = universe if universe is not None else _load_active_symbol_universe()
    provider_symbol_maps: Dict[str, Dict[str, str]] = {}
    for provider_name in provider_names or []:
        name = str(provider_name or "").strip()
        if not name:
            continue
        symbol_map = _provider_symbol_map_for_cycle(name, cycle_universe)
        if symbol_map:
            provider_symbol_maps[name] = symbol_map
    return PollPriceCycleSymbolPlan(
        universe=cycle_universe,
        provider_symbol_maps=provider_symbol_maps,
    )


def _detect_outlier(prices: List[float], latest: float) -> bool:
    if len(prices) < OUTLIER_LOOKBACK:
        return False
    try:
        recent_tail = [float(p) for p in prices[-5:] if p is not None]
        tolerance = max(abs(float(latest)) * 1e-4, 1e-6)
        if recent_tail and sum(1 for p in recent_tail if abs(float(p) - float(latest)) <= tolerance) >= min(3, len(recent_tail)):
            return False

        med = statistics.median(prices)
        mad = statistics.median([abs(p - med) for p in prices])
        if mad is None or float(mad) <= max(abs(float(med)) * 1e-6, 1e-9):
            return False
        z = abs(latest - med) / mad
        return z >= OUTLIER_Z
    except Exception as e:
        _log_nonfatal(
            "poll_prices_outlier_detection_failed",
            e,
            warn_key="poll_prices_outlier_detection_failed",
            prices_count=int(len(prices)),
            latest=latest,
        )
        return False


def _normalized_recent_price_symbols(symbols: List[str]) -> List[str]:
    names: List[str] = []
    seen: set[str] = set()
    for sym in symbols or []:
        name = str(sym).strip().upper()
        if not name or name in seen:
            continue
        names.append(name)
        seen.add(name)
    return names


def _recent_price_rows(con, symbols: List[str], limit_n: int):
    if dbapi.is_sqlite_connection(con):
        placeholders = ",".join("?" for _ in symbols)
        return con.execute(
            f"""
            WITH ranked AS (
                SELECT
                    symbol,
                    ts_ms,
                    price,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol
                        ORDER BY ts_ms DESC
                    ) AS rn
                FROM prices
                WHERE symbol IN ({placeholders})
                  AND price IS NOT NULL
            )
            SELECT symbol, price
            FROM ranked
            WHERE rn <= ?
            ORDER BY symbol ASC, ts_ms ASC
            """,
            tuple(symbols) + (int(limit_n),),
        ).fetchall() or []

    values_sql = ",".join("(?)" for _ in symbols)
    return con.execute(
        f"""
        WITH requested(symbol) AS (
            VALUES {values_sql}
        )
        SELECT requested.symbol, recent.price
        FROM requested
        JOIN LATERAL (
            SELECT ts_ms, price
            FROM prices
            WHERE prices.symbol = requested.symbol
              AND price IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT ?
        ) AS recent ON TRUE
        ORDER BY requested.symbol ASC, recent.ts_ms ASC
        """,
        tuple(symbols) + (int(limit_n),),
    ).fetchall() or []


def _recent_prices_map(symbols: List[str], limit_n: int) -> Dict[str, List[float]]:
    names = _normalized_recent_price_symbols(symbols)
    if not names:
        return {}
    histories: Dict[str, List[float]] = {str(sym): [] for sym in names}
    limit_value = max(0, int(limit_n))
    if limit_value <= 0:
        return histories
    con = connect(readonly=True)
    try:
        rows = _recent_price_rows(con, names, limit_value)
        for row in rows:
            if not row or len(row) < 2:
                continue
            sym = str(row[0] or "").strip().upper()
            if not sym or sym not in histories:
                continue
            p = row[1]
            try:
                if p is None:
                    continue
                histories[sym].append(float(p))
            except Exception as e:
                _log_nonfatal(
                    "poll_prices_recent_price_parse_failed",
                    e,
                    symbol=str(sym),
                    raw_price=p,
                )
                continue
        return histories
    finally:
        try:
            con.close()
        except Exception as e:
            _log_nonfatal(
                "poll_prices_recent_prices_map_close_failed",
                e,
                warn_key="poll_prices_recent_prices_map_close_failed",
            )


def _reject_split_like_price_row(
    *,
    symbol: str,
    ts_ms: int,
    current_price: float,
    price_payload: Dict[str, Any],
    hist: List[float],
) -> bool:
    if not hist:
        return False
    previous_price = float(hist[-1])
    if not is_split_like_price_jump(previous_price, current_price):
        return False
    try:
        with connect(readonly=True) as con:
            if is_explained_split(con, symbol=str(symbol), ts_ms=int(ts_ms)):
                return False
    except Exception as e:
        _log_nonfatal(
            "poll_prices_split_explanation_lookup_failed",
            e,
            warn_key="split_explanation_lookup",
            symbol=str(symbol),
        )
    log_split_like_price_row(
        symbol=str(symbol),
        ts_ms=int(ts_ms),
        previous_price=float(previous_price),
        current_price=float(current_price),
        source=str(price_payload.get("source") or price_payload.get("provider") or "poll_prices"),
    )
    return True


def _compute_provider_weights(con, provider_names, now_ts_ms: int) -> Dict[str, float]:
    """
    Weights by recent OK-rate and low ingest slippage.
    Returns: {provider: weight}
    """
    window_ms = int(float(os.environ.get("ENSEMBLE_WEIGHT_WINDOW_S", "300")) * 1000.0)
    cutoff = int(now_ts_ms - window_ms)

    names = [str(p) for p in (provider_names or []) if p]
    if not names:
        return {}

    w = {p: 1.0 for p in names}

    try:
        q = ",".join(["?"] * len(names))

        rows = con.execute(
            f"""
            SELECT provider,
                   AVG(CASE WHEN ok=1 THEN 1.0 ELSE 0.0 END) AS ok_rate,
                   AVG(COALESCE(latency_ms,0)) AS avg_lat
            FROM price_provider_health
            WHERE ts_ms >= ? AND provider IN ({q})
            GROUP BY provider
            """,
            (int(cutoff), *names),
        ).fetchall() or []

        ok_rate = {str(p): float(r) for (p, r, _lat) in rows if p is not None and r is not None}

        rows2 = con.execute(
            f"""
            SELECT provider, AVG(abs_px_minus_mid) AS avg_abs
            FROM ingest_slippage
            WHERE ts_ms >= ? AND provider IN ({q})
            GROUP BY provider
            """,
            (int(cutoff), *names),
        ).fetchall() or []

        avg_abs = {str(p): float(a) for (p, a) in rows2 if p is not None and a is not None}

        slip_scale = float(os.environ.get("ENSEMBLE_SLIP_SCALE", "1.0"))
        min_ok = float(os.environ.get("ENSEMBLE_MIN_OK_RATE", "0.20"))

        for p in names:
            r = ok_rate.get(p, 1.0)
            if r < min_ok:
                w[p] = 0.05
                continue
            a = avg_abs.get(p, 0.0)
            w[p] = max(0.05, float(r) / (1.0 + slip_scale * float(a)))

        s = sum(w.values()) or 1.0
        for p in list(w.keys()):
            w[p] = float(w[p]) / float(s)

        return w
    except Exception as e:
        _log_nonfatal(
            "poll_prices_provider_weight_compute_failed",
            e,
            warn_key="poll_prices_provider_weight_compute_failed",
            provider_count=int(len(names)),
            now_ts_ms=int(now_ts_ms),
        )
        s = float(len(names)) or 1.0
        return {p: 1.0 / s for p in names}


def _mark_stale(
    now_ts_ms: int,
    *,
    active_symbol_rows: Optional[Tuple[Tuple[str, Any], ...]] = None,
    fresh_symbols: Optional[List[str]] = None,
    fresh_symbol_ts_ms: Optional[Dict[str, int]] = None,
) -> None:
    if not _has_first_price_tick():
        return
    cutoff = now_ts_ms - PRICE_STALE_AFTER_S * 1000
    fresh_symbol_set = {str(sym or "").strip().upper() for sym in (fresh_symbols or []) if str(sym or "").strip()}
    fresh_ts_by_symbol = {
        str(sym or "").strip().upper(): int(ts_ms)
        for sym, ts_ms in (fresh_symbol_ts_ms or {}).items()
        if str(sym or "").strip()
    }
    stale_alerts: List[Dict[str, Any]] = []

    def _write(con) -> None:
        rows = active_symbol_rows if active_symbol_rows is not None else _fetch_active_symbol_rows(con)

        for sym, meta_json in rows:
            sym_s = str(sym or "").strip().upper()
            if not sym_s:
                continue
            try:
                meta = _json_loads(meta_json) if meta_json else {}
            except Exception:
                meta = {}

            ps = meta.get("price_status", {}) or {}
            last_seen = ps.get("last_seen_ts_ms")
            already_stale = bool(ps.get("stale"))
            if sym_s in fresh_symbol_set:
                if already_stale:
                    meta.setdefault("price_status", {})
                    meta["price_status"]["stale"] = False
                    meta["price_status"]["last_seen_ts_ms"] = int(fresh_ts_by_symbol.get(sym_s) or now_ts_ms)
                    con.execute(
                        "UPDATE symbols SET meta_json=?, updated_ts_ms=? WHERE symbol=?",
                        (_json_dumps_text(meta), int(now_ts_ms), str(sym_s)),
                    )
                continue

            if last_seen and int(last_seen) < cutoff:
                if not already_stale:
                    meta.setdefault("price_status", {})
                    meta["price_status"]["stale"] = True

                    stale_alerts.append(
                        {
                            "event_title": f"Price stale: {sym}",
                            "symbol": str(sym_s),
                            "horizon_s": 0,
                            "expected_z": 0.0,
                            "confidence": 1.0,
                            "explain": {
                                "last_seen_ts_ms": int(last_seen),
                                "stale_for_s": int((now_ts_ms - int(last_seen)) / 1000),
                                "type": "price_stale",
                            },
                        }
                    )

                    con.execute(
                        "UPDATE symbols SET meta_json=?, updated_ts_ms=? WHERE symbol=?",
                        (_json_dumps_text(meta), int(now_ts_ms), str(sym_s)),
                    )
            else:
                if already_stale:
                    meta.setdefault("price_status", {})
                    meta["price_status"]["stale"] = False
                    con.execute(
                        "UPDATE symbols SET meta_json=?, updated_ts_ms=? WHERE symbol=?",
                        (_json_dumps_text(meta), int(now_ts_ms), str(sym_s)),
                    )

    write_ok, _ = _run_write_txn_allow_busy(
        _write,
        default=None,
        table="symbols",
        operation="mark_stale_prices",
        context={"job": JOB_NAME, "cutoff": int(cutoff)},
        attempts=1,
        maintenance=False,
        busy_event="poll_prices_mark_stale_write_busy",
        warn_key="poll_prices_mark_stale_write_busy",
        extra={"stale_alerts": int(len(stale_alerts))},
        timeout_s=float(POLL_PRICES_STALE_WRITE_TIMEOUT_S),
        busy_timeout_ms=int(POLL_PRICES_STALE_BUSY_TIMEOUT_MS),
    )
    if not write_ok:
        return
    for alert in stale_alerts:
        try:
            emit_alert(**alert)
        except Exception as e:
            _log_nonfatal(
                "poll_prices_stale_alert_emit_failed",
                e,
                warn_key="poll_prices_stale_alert_emit_failed",
                alert_title=alert.get("title"),
                symbol=alert.get("symbol"),
            )


def _finalize_post_commit_price_cycle(
    pending_outlier_alerts: List[Dict[str, Any]],
    post_commit_first_tick: Dict[str, Any],
) -> None:
    for alert in pending_outlier_alerts:
        try:
            emit_alert(**alert)
        except Exception as e:
            _log_nonfatal("poll_prices_outlier_alert_emit_failed", e)

    first_provider = str(post_commit_first_tick.get("provider") or "").strip()
    first_ts_ms = int(post_commit_first_tick.get("first_ts_ms") or 0)

    if first_ts_ms > 0:
        try:
            did = meta_set_if_missing("first_price_ts_ms", str(int(first_ts_ms)))
            set_state(LIVE, "first_market_data_tick" if did else "market_data_healthy")
        except Exception as e:
            _log_nonfatal("poll_prices_first_tick_state_update_failed", e)

    if first_provider:
        try:
            meta_set("price_provider_active", first_provider, best_effort=True)
        except Exception as e:
            _log_nonfatal("poll_prices_active_provider_update_failed", e)


def _record_poll_prices_status(
    source_manager,
    *,
    ok: bool,
    raw_rows: int = 0,
    price_rows: int = 0,
    quote_rows: int = 0,
    dedup_drops: int = 0,
    gap_events: int = 0,
    normalization_failures: int = 0,
    last_ingested_ts_ms: Optional[int] = None,
    error: Optional[str] = None,
    providers: Optional[List[str]] = None,
    provider_errors: Optional[Dict[str, str]] = None,
    provider_latencies_ms: Optional[Dict[str, int]] = None,
    provider_result_counts: Optional[Dict[str, int]] = None,
    have_price_feed_lock: bool = False,
    fail_backoff_s: float = 0.0,
    latency_ms: Optional[int] = None,
    disabled: bool = False,
    message: str = "",
) -> Dict[str, Any]:
    provider_names = sorted({str(name).strip() for name in (providers or []) if str(name).strip()})
    clean_provider_errors = {
        str(name).strip(): str(value or "")[:500]
        for name, value in (provider_errors or {}).items()
        if str(name).strip() and str(value or "").strip()
    }
    clean_provider_latencies_ms = {
        str(name).strip(): max(0, int(value or 0))
        for name, value in (provider_latencies_ms or {}).items()
        if str(name).strip()
    }
    clean_provider_result_counts = {
        str(name).strip(): max(0, int(value or 0))
        for name, value in (provider_result_counts or {}).items()
        if str(name).strip()
    }
    provider_error_classifications = {
        name: _classify_provider_error(error)
        for name, error in clean_provider_errors.items()
    }
    status: Dict[str, Any] = {
        "pipeline_name": JOB_NAME,
        "ok": bool(ok),
        "raw_rows": int(raw_rows or 0),
        "event_rows": 0,
        "last_ingested_ts_ms": (None if last_ingested_ts_ms is None else int(last_ingested_ts_ms)),
        "error": (None if ok else str(error or "")[:1000]),
        "latency_ms": (None if latency_ms is None else int(latency_ms)),
        "meta": {
            "disabled": bool(disabled),
            "fail_backoff_s": float(fail_backoff_s or 0.0),
            "have_price_feed_lock": bool(have_price_feed_lock),
            "liveness_job_name": str(JOB_LIVENESS_NAME),
            "shard": INGESTION_SHARD.as_dict(),
            "dedup_drops": int(dedup_drops or 0),
            "gap_events": int(gap_events or 0),
            "normalization_failures": int(normalization_failures or 0),
            "price_rows": int(price_rows or 0),
            "provider_errors": clean_provider_errors,
            "provider_error_classifications": provider_error_classifications,
            "provider_latencies_ms": clean_provider_latencies_ms,
            "provider_result_counts": clean_provider_result_counts,
            "providers": provider_names,
            "quote_rows": int(quote_rows or 0),
        },
    }
    try:
        status = record_pipeline_status(
            JOB_NAME,
            ok=bool(ok),
            raw_rows=int(raw_rows or 0),
            event_rows=0,
            last_ingested_ts_ms=(None if last_ingested_ts_ms is None else int(last_ingested_ts_ms)),
            error=(None if ok else str(error or "")[:1000]),
            latency_ms=(None if latency_ms is None else int(latency_ms)),
            meta={
                "disabled": bool(disabled),
                "fail_backoff_s": float(fail_backoff_s or 0.0),
                "have_price_feed_lock": bool(have_price_feed_lock),
                "liveness_job_name": str(JOB_LIVENESS_NAME),
                "shard": INGESTION_SHARD.as_dict(),
                "dedup_drops": int(dedup_drops or 0),
                "gap_events": int(gap_events or 0),
                "normalization_failures": int(normalization_failures or 0),
                "price_rows": int(price_rows or 0),
                "provider_errors": clean_provider_errors,
                "provider_error_classifications": provider_error_classifications,
                "provider_latencies_ms": clean_provider_latencies_ms,
                "provider_result_counts": clean_provider_result_counts,
                "providers": provider_names,
                "quote_rows": int(quote_rows or 0),
            },
            best_effort=True,
        )
    except Exception as e:
        _log_nonfatal(
            "poll_prices_pipeline_status_write_failed",
            e,
            warn_key="poll_prices_pipeline_status_write_failed",
            ok=bool(ok),
            providers=provider_names,
        )
    try:
        source_manager.record_job_status(
            JOB_NAME,
            ok=bool(ok),
            message=str(message or ("poll_prices cycle complete" if ok else "poll_prices cycle failed")),
            error=(str(error or "") if not ok else ""),
            meta={
                "disabled": bool(disabled),
                "fail_backoff_s": float(fail_backoff_s or 0.0),
                "have_price_feed_lock": bool(have_price_feed_lock),
                "liveness_job_name": str(JOB_LIVENESS_NAME),
                "shard": INGESTION_SHARD.as_dict(),
                "dedup_drops": int(dedup_drops or 0),
                "gap_events": int(gap_events or 0),
                "normalization_failures": int(normalization_failures or 0),
                "latency_ms": (None if latency_ms is None else int(latency_ms)),
                "price_rows": int(price_rows or 0),
                "provider_errors": clean_provider_errors,
                "provider_error_classifications": provider_error_classifications,
                "provider_latencies_ms": clean_provider_latencies_ms,
                "provider_result_counts": clean_provider_result_counts,
                "providers": provider_names,
                "quote_rows": int(quote_rows or 0),
                "raw_rows": int(raw_rows or 0),
            },
            best_effort=True,
        )
    except Exception as e:
        _log_nonfatal(
            "poll_prices_record_job_status_failed",
            e,
            warn_key="poll_prices_record_job_status_failed",
            ok=bool(ok),
            providers=provider_names,
        )
    return status


def _classify_provider_error(error: object) -> str:
    text = str(error or "").strip().lower()
    if not text:
        return ""
    if any(token in text for token in ("api_key", "api key", "access_token", "token", "credential", "not set", "missing")):
        return "missing_credentials"
    if "429" in text or ("rate" in text and "limit" in text):
        return "rate_limited"
    if any(token in text for token in ("timeout", "timed out", "connection", "network", "temporarily", "503", "502", "504")):
        return "transient_network"
    return "provider_error"


def _record_provider_health_telemetry(
    source_manager,
    *,
    provider: str,
    ok: bool,
    latency_ms: Optional[int],
    n_symbols: int,
    error: Optional[str] = None,
    ts_ms: Optional[int] = None,
) -> bool:
    provider_name = str(provider or "").strip()
    if not provider_name:
        return False
    now_ts_ms = int(ts_ms if ts_ms is not None else time.time() * 1000)
    latency_ms_value = None if latency_ms is None else int(latency_ms)
    symbol_count = int(n_symbols or 0)
    buffered = False

    try:
        buffered = enqueue_price_provider_health(
            provider=provider_name,
            ok=bool(ok),
            latency_ms=latency_ms_value,
            n_symbols=symbol_count,
            error=error,
            ts_ms=int(now_ts_ms),
        )
    except Exception as e:
        _log_nonfatal(
            "poll_prices_provider_health_enqueue_failed",
            e,
            provider=provider_name,
            ok=bool(ok),
        )
        buffered = False

    if buffered:
        try:
            from engine.runtime.state_cache import cache_invalidate_namespace

            cache_invalidate_namespace("api_read", prefix="feed_status")
            cache_invalidate_namespace("provider_health")
        except Exception as e:
            _log_nonfatal(
                "poll_prices_provider_health_cache_invalidate_failed",
                e,
                provider=provider_name,
            )
    else:
        snapshot: Dict[str, Any]
        try:
            snapshot = dict(get_telemetry_append_buffer_snapshot() or {})
        except Exception as e:
            snapshot = {
                "enabled": False,
                "error": f"{type(e).__name__}:{e}",
                "ts_ms": int(time.time() * 1000),
            }
        _log_nonfatal(
            "poll_prices_provider_health_buffer_rejected",
            RuntimeError("telemetry_append_buffer_rejected"),
            warn_key=(
                f"poll_prices_provider_health_buffer_rejected:"
                f"{provider_name}:{snapshot.get('last_rejected_reason') or 'unknown'}"
            ),
            provider=provider_name,
            ok=bool(ok),
            latency_ms=latency_ms_value,
            n_symbols=symbol_count,
            provider_error=(str(error or "") or None),
            telemetry_append_buffer=snapshot,
        )

    try:
        source_manager.record_source_status(
            provider_name,
            ok=bool(ok),
            message="price provider health update",
            error=str(error or ""),
            meta={
                "latency_ms": latency_ms_value,
                "n_symbols": symbol_count,
                "job_name": JOB_NAME,
            },
            ts_ms=int(now_ts_ms),
            best_effort=True,
        )
    except Exception as e:
        _log_nonfatal(
            "poll_prices_provider_health_source_status_failed",
            e,
            provider=provider_name,
            ok=bool(ok),
        )
    return bool(buffered)


def _snapshot_rest_provider(
    provider_name: str,
    manager: ProviderSessionManager,
    session: PollingProviderSession,
) -> Dict[str, Any]:
    started = time.perf_counter()
    got: Dict[str, Any] = {}
    error: Optional[str] = None
    telemetry: Dict[str, Any] = {}
    manager_ok = False

    try:
        got = manager.snapshot() or {}
    except Exception as e:
        error = f"{type(e).__name__}: {e}"
        try:
            session.note_error(error)
        except Exception as note_error:
            _log_nonfatal(
                "poll_prices_provider_snapshot_note_error_failed",
                note_error,
                provider=str(provider_name),
            )

    try:
        telemetry = manager.provider_telemetry() or {}
    except Exception as e:
        if error is None:
            error = f"{type(e).__name__}: {e}"
        _log_nonfatal(
            "poll_prices_provider_snapshot_telemetry_failed",
            e,
            provider=str(provider_name),
        )
        telemetry = {}

    try:
        manager_ok = bool(manager.ok())
    except Exception as e:
        if error is None:
            error = f"{type(e).__name__}: {e}"
        _log_nonfatal(
            "poll_prices_provider_snapshot_ok_check_failed",
            e,
            provider=str(provider_name),
        )
        manager_ok = False

    try:
        latency_ms = int(session.latency_ms())
    except Exception as e:
        _log_nonfatal(
            "poll_prices_provider_snapshot_latency_read_failed",
            e,
            provider=str(provider_name),
        )
        latency_ms = 0
    if latency_ms <= 0:
        latency_ms = max(0, int((time.perf_counter() - started) * 1000.0))

    ok = bool(got or manager_ok)
    if error is None and not got and not ok:
        last_error = str(telemetry.get("last_error") or "").strip()
        error = last_error or "provider_snapshot_empty"

    return {
        "provider": str(provider_name),
        "got": got,
        "ok": bool(ok),
        "error": (str(error)[:500] if error else None),
        "latency_ms": int(latency_ms),
        "telemetry": telemetry,
    }


def _collect_rest_provider_snapshots(
    jobs: List[Tuple[str, ProviderSessionManager, PollingProviderSession]],
    *,
    timeout_s: Optional[float] = None,
) -> Dict[str, Dict[str, Any]]:
    clean_jobs = [
        (str(provider_name), manager, session)
        for provider_name, manager, session in (jobs or [])
        if str(provider_name).strip()
    ]
    if not clean_jobs:
        return {}

    worker_count = max(
        1,
        min(int(POLL_PRICES_PROVIDER_MAX_WORKERS), int(len(clean_jobs))),
    )
    if worker_count <= 1:
        return {
            provider_name: _snapshot_rest_provider(provider_name, manager, session)
            for provider_name, manager, session in clean_jobs
        }

    collection_timeout_s = max(
        0.25 if timeout_s is None else 0.01,
        float(POLL_PRICES_PROVIDER_TIMEOUT_S if timeout_s is None else timeout_s),
    )
    results: Dict[str, Dict[str, Any]] = {}
    executor = ThreadPoolExecutor(
        max_workers=worker_count,
        thread_name_prefix="poll-prices-provider",
    )
    try:
        futures = {
            executor.submit(_snapshot_rest_provider, provider_name, manager, session): provider_name
            for provider_name, manager, session in clean_jobs
        }
        completed = set()
        try:
            for future in as_completed(futures, timeout=collection_timeout_s):
                provider_name = futures[future]
                completed.add(future)
                try:
                    results[provider_name] = future.result()
                except Exception as e:
                    # _snapshot_rest_provider isolates provider exceptions; this
                    # is a final guard for executor/runtime failures.
                    error = f"{type(e).__name__}: {e}"
                    _log_nonfatal(
                        "poll_prices_provider_snapshot_executor_failed",
                        e,
                        provider=str(provider_name),
                    )
                    results[provider_name] = {
                        "provider": str(provider_name),
                        "got": {},
                        "ok": False,
                        "error": error[:500],
                        "latency_ms": 0,
                        "telemetry": {},
                    }
        except FutureTimeoutError:
            timed_out_providers = len(futures) - len(completed)
            _log_nonfatal(
                "poll_prices_provider_snapshot_collection_timeout",
                TimeoutError(f"provider_snapshot_collection_timeout_after_{collection_timeout_s:.3f}s"),
                timed_out_providers=int(max(0, timed_out_providers)),
                timeout_s=float(collection_timeout_s),
            )

        session_by_provider = {provider_name: session for provider_name, _manager, session in clean_jobs}
        for future, provider_name in futures.items():
            if future in completed:
                continue
            timed_out = False
            if future.done():
                try:
                    results[provider_name] = future.result()
                    continue
                except Exception as e:
                    error = f"{type(e).__name__}: {e}"
            else:
                future.cancel()
                error = f"provider_snapshot_timeout_after_{collection_timeout_s:.3f}s"
                timed_out = True

            try:
                session_by_provider[provider_name].note_error(error)
            except Exception as note_error:
                _log_nonfatal(
                    (
                        "poll_prices_provider_snapshot_timeout_note_error_failed"
                        if timed_out
                        else "poll_prices_provider_snapshot_executor_note_error_failed"
                    ),
                    note_error,
                    provider=str(provider_name),
                )
            _log_nonfatal(
                (
                    "poll_prices_provider_snapshot_timeout"
                    if timed_out
                    else "poll_prices_provider_snapshot_executor_failed"
                ),
                TimeoutError(error),
                provider=str(provider_name),
                **({"timeout_s": float(collection_timeout_s)} if timed_out else {}),
            )
            results[provider_name] = {
                "provider": str(provider_name),
                "got": {},
                "ok": False,
                "error": error[:500],
                "latency_ms": int(collection_timeout_s * 1000.0),
                "telemetry": {},
            }
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    return {
        provider_name: results[provider_name]
        for provider_name, _manager, _session in clean_jobs
        if provider_name in results
    }


def _init_db_with_retry() -> None:
    last_error: Optional[Exception] = None
    for attempt in range(max(1, INIT_DB_RETRY_ATTEMPTS)):
        try:
            init_db()
            return
        except Exception as e:
            last_error = e
            message = str(e or "").lower()
            if ("database is locked" not in message and "database busy" not in message) or attempt >= (INIT_DB_RETRY_ATTEMPTS - 1):
                raise
            delay_ms = min(INIT_DB_RETRY_MAX_MS, int(INIT_DB_RETRY_BASE_MS * (2 ** attempt)))
            delay_ms += random.randint(0, max(25, INIT_DB_RETRY_BASE_MS // 2))
            _log_nonfatal(
                "poll_prices_init_db_retry",
                e,
                attempt=int(attempt) + 1,
                attempts=int(INIT_DB_RETRY_ATTEMPTS),
                delay_ms=int(delay_ms),
            )
            time.sleep(delay_ms / 1000.0)
    if last_error is not None:
        raise last_error


def _is_supervised_ingestion_child() -> bool:
    supervised = str(
        os.environ.get("ENGINE_LAUNCHED_BY_SUPERVISOR", os.environ.get("ENGINE_SUPERVISED", "0")) or "0"
    ).strip().lower() in {"1", "true", "yes", "on"}
    return bool(supervised and str(os.environ.get("ENGINE_INGESTION_CHILD", "0") or "0").strip() == "1")


def _can_reuse_supervisor_db_bootstrap() -> bool:
    if not _is_supervised_ingestion_child():
        return False
    try:
        bootstrap_ready = str(meta_get("data_sources_bootstrap_ready", "") or "").strip()
        schema_ready = str(meta_get("schema_version", "") or "").strip()
    except Exception as e:
        _log_nonfatal("poll_prices_bootstrap_marker_read_failed", e)
        return False
    return bool(bootstrap_ready == "1" and schema_ready)


def _ensure_runtime_db_ready() -> None:
    # Supervised ingestion children inherit a fully bootstrapped runtime DB from
    # the ingestion supervisor. Re-running init_db() in every child creates
    # avoidable lock contention during cold startup.
    if _can_reuse_supervisor_db_bootstrap():
        return
    _init_db_with_retry()


def _uses_price_feed_lock() -> bool:
    # Under the ingestion supervisor, singleton authority is already enforced by
    # the canonical poll_prices job lock. Requiring a second DB-backed feed lock
    # during cold start only adds contention before the poller can emit its
    # first heartbeat or price batch.
    return not _is_supervised_ingestion_child()


def _uses_child_job_lock() -> bool:
    # Supervised ingestion children already have singleton ownership via the
    # parent ingestion runtime. Re-acquiring a per-child job lock can wedge
    # startup behind stale child rows before the first heartbeat lands.
    return not _is_supervised_ingestion_child()


def _write_liveness_heartbeat(
    *,
    now_ts_ms: int,
    fail_s: float,
    have_price_feed_lock: bool,
    rest_managers: Dict[str, ProviderSessionManager],
) -> None:
    if _uses_child_job_lock():
        touch_job_lock(JOB_LIVENESS_NAME, OWNER, PID, best_effort=True)
    if _uses_price_feed_lock():
        _touch_price_feed_lock(now_ts_ms)
    put_job_heartbeat(
        JOB_LIVENESS_NAME,
        OWNER,
        PID,
        extra_json=_json_dumps_text(
            {
                "job_name": JOB_NAME,
                "liveness_job_name": JOB_LIVENESS_NAME,
                "shard": INGESTION_SHARD.as_dict(),
                "poll_seconds": POLL_SECONDS,
                "heartbeat_every_s": HEARTBEAT_EVERY_S,
                "fail_backoff_s": fail_s,
                "have_price_feed_lock": bool(have_price_feed_lock),
                "providers": {k: v.provider_telemetry() for k, v in rest_managers.items()},
            },
            sort_keys=True,
        ),
        best_effort=True,
    )


# ------------------------------------------------------
# Main loop
# ------------------------------------------------------

def main() -> None:
    _ensure_runtime_db_ready()
    source_manager = get_manager()
    if not source_manager.is_job_enabled(JOB_NAME, default=True):
        _record_poll_prices_status(
            source_manager,
            ok=True,
            providers=[],
            have_price_feed_lock=False,
            disabled=True,
            message="poll_prices disabled by data source control plane",
        )
        return

    # ------------------------------------------------------
    # Acquire price feed ownership lock
    # ------------------------------------------------------
    have_price_feed_lock = True
    have_job_lock = False
    if _uses_price_feed_lock():
        have_price_feed_lock = _acquire_price_feed_lock()

    if _uses_child_job_lock():
        if not acquire_job_lock(JOB_LIVENESS_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
            raise SystemExit(2)
        have_job_lock = True

    # Provider chain (REST / snapshot): dynamic registry + explicit env override.
    chain = [
        p.strip().lower()
        for p in os.environ.get("LIVE_PRICE_PROVIDER_CHAIN", "").split(",")
        if p.strip()
    ]

    if not chain:
        chain = list(get_polling_provider_names())

    if not chain:
        chain = [os.environ.get("LIVE_PRICE_PROVIDER", "yfinance").lower().strip()]

    chain = _append_simulated_provider_fallback([p for p in chain if p])
    if not chain:
        chain = ["yfinance"]

    # Instantiate REST snapshot providers behind provider sessions so the poll
    # loop can fetch them concurrently while keeping DB writes ordered.
    rest_sessions: Dict[str, PollingProviderSession] = {}
    rest_managers: Dict[str, ProviderSessionManager] = {}
    provider_init_errors: Dict[str, str] = {}
    for name in chain:
        try:
            provider_obj = get_price_provider_by_name(name)
            session = PollingProviderSession(name, provider_obj, poll_interval_s=float(POLL_SECONDS))
            session_manager = ProviderSessionManager(
                session,
                provider_name=name,
                heartbeat_interval_s=max(1.0, min(float(POLL_SECONDS), HEARTBEAT_EVERY_S)),
                dead_after_ms=int(POLL_PROVIDER_DEAD_AFTER_MS),
                reconnect_base_s=float(FAIL_BASE_S),
                reconnect_max_s=float(FAIL_MAX_S),
                startup_grace_ms=max(30000, int(float(POLL_SECONDS) * 1000.0)),
            )
            rest_sessions[name] = session
            rest_managers[name] = session_manager
        except Exception as e:
            provider_init_errors[str(name)] = f"{type(e).__name__}: {e}"[:500]
            _log_nonfatal("poll_prices_provider_init_failed", e, provider=name)
            continue

    if not rest_managers:
        for name in list(get_polling_provider_names()) or ["yfinance"]:
            try:
                provider_obj = get_price_provider_by_name(name)
                session = PollingProviderSession(name, provider_obj, poll_interval_s=float(POLL_SECONDS))
                session_manager = ProviderSessionManager(
                    session,
                    provider_name=name,
                    heartbeat_interval_s=max(1.0, min(float(POLL_SECONDS), HEARTBEAT_EVERY_S)),
                    dead_after_ms=int(POLL_PROVIDER_DEAD_AFTER_MS),
                    reconnect_base_s=float(FAIL_BASE_S),
                    reconnect_max_s=float(FAIL_MAX_S),
                    startup_grace_ms=max(30000, int(float(POLL_SECONDS) * 1000.0)),
                )
                rest_sessions[name] = session
                rest_managers[name] = session_manager
                break
            except Exception as e:
                provider_init_errors[str(name)] = f"{type(e).__name__}: {e}"[:500]
                _log_nonfatal("poll_prices_provider_init_failed", e, provider=name)
                continue

    if not rest_managers:
        log_event(
            LOG,
            30,
            "poll_prices_rest_provider_fallback",
            component="engine.data.poll_prices",
            extra={"job": JOB_NAME},
        )
        try:
            provider_obj = get_price_provider_by_name("yfinance")
            session = PollingProviderSession("yfinance", provider_obj, poll_interval_s=float(POLL_SECONDS))
            session_manager = ProviderSessionManager(
                session,
                provider_name="yfinance",
                heartbeat_interval_s=max(1.0, min(float(POLL_SECONDS), HEARTBEAT_EVERY_S)),
                dead_after_ms=int(POLL_PROVIDER_DEAD_AFTER_MS),
                reconnect_base_s=float(FAIL_BASE_S),
                reconnect_max_s=float(FAIL_MAX_S),
                startup_grace_ms=max(30000, int(float(POLL_SECONDS) * 1000.0)),
            )
            rest_sessions["yfinance"] = session
            rest_managers["yfinance"] = session_manager
        except Exception as e:
            provider_init_errors["yfinance"] = f"{type(e).__name__}: {e}"[:500]
            _log_nonfatal("poll_prices_fallback_provider_init_failed", e, provider="yfinance")

    fail_s = 0.0
    last_hb_s = 0.0
    last_provider_health_s = 0.0

    try:
        while True:
            if not source_manager.is_job_enabled(JOB_NAME, default=True):
                _record_poll_prices_status(
                    source_manager,
                    ok=True,
                    providers=list(rest_managers.keys()),
                    have_price_feed_lock=bool(have_price_feed_lock),
                    fail_backoff_s=fail_s,
                    disabled=True,
                    message="poll_prices disabled by data source control plane",
                )
                break
            now_s = time.time()
            now_ts_ms = int(now_s * 1000)
            pipeline_error = ""
            pipeline_latency_ms = None
            pipeline_last_ingested_ts_ms = None
            pipeline_price_rows = 0
            pipeline_quote_rows = 0
            pipeline_raw_rows = 0
            pipeline_dedup_drops = 0
            pipeline_gap_events = 0
            pipeline_normalization_failures = 0
            pipeline_ok = False
            producer_backpressure = False
            provider_errors: Dict[str, str] = {}
            provider_latencies_ms: Dict[str, int] = {}
            provider_result_counts: Dict[str, int] = {}
            if provider_init_errors:
                provider_errors.update(provider_init_errors)
                provider_result_counts.update({name: 0 for name in provider_init_errors})
            cycle_fresh_symbol_ts_ms: Dict[str, int] = {}
            persist_provider_health = (now_s - last_provider_health_s) >= PROVIDER_HEALTH_EVERY_S

            if now_s - last_hb_s >= HEARTBEAT_EVERY_S:
                try:
                    _write_liveness_heartbeat(
                        now_ts_ms=now_ts_ms,
                        fail_s=fail_s,
                        have_price_feed_lock=bool(have_price_feed_lock),
                        rest_managers=rest_managers,
                    )
                except Exception as e:
                    _log_nonfatal(
                        "poll_prices_heartbeat_write_failed",
                        e,
                        warn_key="poll_prices_heartbeat_write_failed",
                        have_price_feed_lock=bool(have_price_feed_lock),
                    )
                last_hb_s = now_s

            if _uses_price_feed_lock() and not have_price_feed_lock:
                have_price_feed_lock = _acquire_price_feed_lock()
                if not have_price_feed_lock:
                    _sleep_with_jitter(min(float(POLL_SECONDS), 5.0))
                    continue

            cycle_symbols = _build_cycle_symbol_plan(list(rest_managers.keys()))
            cycle_universe = cycle_symbols.universe
            assigned_symbol_count = cycle_symbols.assigned_symbol_count
            if INGESTION_SHARD.enabled and assigned_symbol_count <= 0:
                fail_s = 0.0
                _record_poll_prices_status(
                    source_manager,
                    ok=True,
                    providers=list(rest_managers.keys()),
                    provider_result_counts={name: 0 for name in rest_managers.keys()},
                    have_price_feed_lock=bool(have_price_feed_lock),
                    fail_backoff_s=fail_s,
                    message="poll_prices shard has no assigned symbols",
                )
                _sleep_with_jitter(float(POLL_SECONDS))
                continue

            merged: Dict[str, Dict[str, Any]] = {}
            got_by_provider: Dict[str, Dict[str, Any]] = {}
            raw_quote_rows = []
            slip_rows = []
            futures_bar_rows = []

            # -----------------------------
            # REST / snapshot providers
            # -----------------------------
            if rest_managers:
                provider_symbol_maps: Dict[str, Dict[str, str]] = {}
                provider_order: List[str] = []
                provider_jobs: List[Tuple[str, ProviderSessionManager, PollingProviderSession]] = []
                snapshot_results: Dict[str, Dict[str, Any]] = {}

                for pname, manager in rest_managers.items():
                    session = rest_sessions[pname]
                    provider_symbol_map = dict(cycle_symbols.provider_symbol_maps.get(str(pname)) or {})
                    if not provider_symbol_map:
                        continue
                    provider_order.append(str(pname))
                    provider_symbol_maps[pname] = provider_symbol_map
                    session.set_symbol_map(provider_symbol_map)
                    try:
                        manager.ensure_subscriptions(sorted(provider_symbol_map.keys()))
                        provider_jobs.append((str(pname), manager, session))
                    except Exception as e:
                        err = f"{type(e).__name__}: {e}"
                        try:
                            session.note_error(err)
                        except Exception as note_error:
                            _log_nonfatal(
                                "poll_prices_provider_subscription_note_error_failed",
                                note_error,
                                provider=str(pname),
                            )
                        snapshot_results[str(pname)] = {
                            "provider": str(pname),
                            "got": {},
                            "ok": False,
                            "error": err[:500],
                            "latency_ms": 0,
                            "telemetry": {},
                        }

                snapshot_results.update(_collect_rest_provider_snapshots(provider_jobs))

                snapshot_success_count = sum(
                    1
                    for result in snapshot_results.values()
                    if bool((result or {}).get("got")) or bool((result or {}).get("ok"))
                )

                for pname in provider_order:
                    provider_symbol_map = dict(provider_symbol_maps.get(pname) or {})
                    result = dict(snapshot_results.get(pname) or {})
                    got = dict(result.get("got") or {})
                    ok = bool(result.get("ok"))
                    err = str(result.get("error") or "").strip() or None
                    latency_ms = max(0, int(result.get("latency_ms") or 0))

                    provider_latencies_ms[str(pname)] = int(latency_ms)
                    provider_result_counts[str(pname)] = int(len(got))
                    if err:
                        provider_errors[str(pname)] = str(err)[:500]

                    emit_timing(
                        "market_data_latency_ms",
                        latency_ms,
                        component="engine.data.poll_prices",
                        job=JOB_NAME,
                        provider=pname,
                    )
                    emit_timing(
                        "provider_snapshot_latency_ms",
                        latency_ms,
                        component="engine.data.poll_prices",
                        job=JOB_NAME,
                        provider=pname,
                    )
                    emit_gauge(
                        "provider_uptime",
                        1.0 if ok else 0.0,
                        component="engine.data.poll_prices",
                        job=JOB_NAME,
                        provider=pname,
                    )
                    got_by_provider[pname] = got
                    if err:
                        emit_counter(
                            "provider_snapshot_failure",
                            1,
                            component="engine.data.poll_prices",
                            job=JOB_NAME,
                            provider=pname,
                        )
                        log_event(
                            LOG,
                            30,
                            "poll_prices_provider_snapshot_failed",
                            component="engine.data.poll_prices",
                            extra={
                                "job": JOB_NAME,
                                "provider": str(pname),
                                "latency_ms": int(latency_ms),
                                "n_symbols": int(len(provider_symbol_map)),
                                "error": str(err)[:500],
                                "partial_failure": bool(snapshot_success_count > 0),
                            },
                        )

                    if persist_provider_health:
                        try:
                            _record_provider_health_telemetry(
                                source_manager,
                                provider=str(pname),
                                ok=bool(ok),
                                latency_ms=(None if latency_ms is None else int(latency_ms)),
                                n_symbols=int(len(provider_symbol_map)),
                                error=err,
                                ts_ms=int(now_ts_ms),
                            )
                        except Exception as e:
                            _log_nonfatal(
                                "poll_prices_provider_health_update_failed",
                                e,
                                provider=pname,
                            )

                    if got:
                        emit_counter(
                            "market_data_event",
                            len(got),
                            component="engine.data.poll_prices",
                            job=JOB_NAME,
                            provider=pname,
                        )

                        for sym, p in (got or {}).items():
                            if not isinstance(p, dict):
                                continue

                            if not p.get("source"):
                                p["source"] = pname

                            ts_ms = int(p.get("ts_ms") or now_ts_ms)
                            futures_bar = _build_futures_contract_bar_row(
                                provider_name=str(pname),
                                symbol=str(sym),
                                row=p,
                                provider_symbol_map=provider_symbol_map,
                                now_ts_ms=int(ts_ms),
                            )
                            if futures_bar is not None:
                                futures_bar_rows.append(futures_bar)
                            last = p.get("price")
                            bid = p.get("bid")
                            ask = p.get("ask")
                            spr = p.get("spread")
                            vol = p.get("volume")

                            if last is not None:
                                raw_quote_rows.append(
                                    (
                                        int(ts_ms),
                                        str(sym),
                                        str(pname),
                                        float(last),
                                        (float(bid) if bid is not None else None),
                                        (float(ask) if ask is not None else None),
                                        (float(spr) if spr is not None else (float(ask) - float(bid) if (bid is not None and ask is not None) else None)),
                                        (float(vol) if vol is not None else None),
                                    )
                                )

                            if (last is not None) and (bid is not None) and (ask is not None):
                                try:
                                    mid = (float(bid) + float(ask)) / 2.0
                                    pxm = float(last) - float(mid)
                                    spread = float(spr) if spr is not None else (float(ask) - float(bid))
                                    slip_rows.append(
                                        (
                                            int(ts_ms),
                                            str(sym),
                                            str(pname),
                                            float(last),
                                            float(bid),
                                            float(ask),
                                            float(mid),
                                            float(spread),
                                            float(pxm),
                                            float(abs(pxm)),
                                        )
                                    )
                                except Exception as e:
                                    _log_nonfatal(
                                        "poll_prices_provider_raw_row_build_failed",
                                        e,
                                        warn_key=f"poll_prices_provider_raw_row_build_failed:{pname}",
                                        provider=str(pname),
                                        symbol=str(sym),
                                    )

                        got_by_provider[pname] = got

                if provider_errors:
                    emit_gauge(
                        "provider_snapshot_partial_failures",
                        len(provider_errors),
                        component="engine.data.poll_prices",
                        job=JOB_NAME,
                    )

                if persist_provider_health:
                    last_provider_health_s = now_s

            # -----------------------------
            # Router-based arbitration for overlapping REST providers
            # -----------------------------
            if got_by_provider:
                normalized_snapshots: Dict[str, Dict[str, Any]] = {}
                for pname, got in got_by_provider.items():
                    provider_rows: Dict[str, Any] = {}
                    for sym, p in (got or {}).items():
                        if not isinstance(p, dict):
                            continue
                        row = dict(p)
                        if row.get("last") is None and row.get("price") is not None:
                            row["last"] = row.get("price")
                        if not row.get("source"):
                            row["source"] = pname
                        row["provider"] = str(pname)
                        provider_rows[str(sym)] = row
                    if provider_rows:
                        normalized_snapshots[str(pname)] = provider_rows

                provider_health = compute_provider_health() if normalized_snapshots else {}
                selected = (
                    select_best_quotes_from_snapshots(
                        normalized_snapshots,
                        provider_health=provider_health,
                        publish_selected=False,
                    )
                    if normalized_snapshots
                    else {}
                )

                for sym, rec in (selected or {}).items():
                    px = rec.get("last")
                    if px is None:
                        continue
                    try:
                        px_f = float(px)
                    except Exception as e:
                        _log_nonfatal(
                            "poll_prices_selected_quote_price_parse_failed",
                            e,
                            symbol=str(sym),
                            provider=str(rec.get("provider") or rec.get("source") or ""),
                            raw_price=px,
                        )
                        continue

                    source_ts_ms = int(rec.get("ts_ms") or 0)
                    # Polling providers may return a last-trade timestamp that
                    # does not advance on every successful fetch. For the
                    # canonical `prices` table we need observation freshness, so
                    # use the current successful poll time and preserve the
                    # provider timestamp separately for audit/debugging.
                    ts_sel_ms = int(now_ts_ms)
                    bid_val = rec.get("bid")
                    ask_val = rec.get("ask")
                    spread_val = rec.get("spread")
                    volume_val = rec.get("volume")
                    quorum_price_val = rec.get("quorum_price")
                    merged[str(sym)] = {
                        "ts_ms": int(ts_sel_ms),
                        "source_ts_ms": int(source_ts_ms),
                        "price": float(px_f),
                        "bid": (float(bid_val) if bid_val is not None else None),
                        "ask": (float(ask_val) if ask_val is not None else None),
                        "spread": (float(spread_val) if spread_val is not None else None),
                        "volume": (float(volume_val) if volume_val is not None else None),
                        "source": str(rec.get("source") or rec.get("provider") or "router"),
                        "provider": str(rec.get("provider") or rec.get("source") or "router"),
                        "failover_used": bool(rec.get("failover_used")),
                        "failover_reason": (str(rec.get("failover_reason")) if rec.get("failover_reason") else None),
                        "preferred_provider": (str(rec.get("preferred_provider")) if rec.get("preferred_provider") else None),
                        "provider_age_ms": int(rec.get("provider_age_ms") or 0),
                        "latency_ms": max(0, int(now_ts_ms) - int(ts_sel_ms)),
                        "latency_score": float(rec.get("latency_score") or 0.0),
                        "provider_score": float(rec.get("provider_score") or 0.0),
                        "quorum_count": int(rec.get("quorum_count") or 0),
                        "quorum_price": (float(quorum_price_val) if quorum_price_val is not None else None),
                    }

                if persist_provider_health:
                    try:
                        healthy = sorted(
                            [v for v in (provider_health or {}).values() if bool(v.get("ok"))],
                            key=lambda v: (-float(v.get("score") or 0.0), int(v.get("latency_ms") or 10**9), str(v.get("provider") or "")),
                        )
                        meta_set(
                            "price_provider_active",
                            str(healthy[0].get("provider") or "") if healthy else "",
                            best_effort=True,
                        )
                        meta_set(
                            "price_provider_health_snapshot",
                            _json_dumps_text(
                                {
                                    str(k): {
                                        "ok": bool(v.get("ok")),
                                        "score": float(v.get("score") or 0.0),
                                        "latency_ms": int(v.get("latency_ms") or 0),
                                        "latency_score": float(v.get("latency_score") or 0.0),
                                        "age_ms": int(v.get("age_ms") or 0),
                                    }
                                    for k, v in (provider_health or {}).items()
                                },
                                sort_keys=True,
                            ),
                            best_effort=True,
                        )
                    except Exception as e:
                        _log_nonfatal(
                            "poll_prices_provider_health_meta_persist_failed",
                            e,
                            warn_key="poll_prices_provider_health_meta_persist_failed",
                            provider_count=int(len(provider_health or {})),
                        )

                try:
                    detect_cross_provider_anomalies()
                except Exception as e:
                    _log_nonfatal(
                        "poll_prices_cross_provider_anomaly_detection_failed",
                        e,
                        warn_key="poll_prices_cross_provider_anomaly_detection_failed",
                        provider_count=int(len(got_by_provider or {})),
                    )

            # -----------------------------
            # Outlier detection + persist
            # -----------------------------
            emit_gauge(
                "queue_depth",
                len(merged or {}),
                component="engine.data.poll_prices",
                job=JOB_NAME,
                extra_tags={"queue_name": "poll_prices_merged"},
            )

            if isinstance(merged, dict) and merged:
                pending_outlier_alerts: List[Dict[str, Any]] = []
                post_commit_first_tick: Dict[str, Any] = {}
                recent_price_history = _recent_prices_map(
                    list(merged.keys()),
                    max(OUTLIER_LOOKBACK, 10),
                )
                publish_counts: Dict[str, Any] = {}

                def _write_merged_prices(conp):
                    price_rows = []
                    quote_rows = []

                    for sym, p in merged.items():
                        ts_ms = int(p.get("ts_ms") or now_ts_ms)
                        px = p.get("price")
                        if px is None:
                            continue

                        try:
                            px_f = float(px)
                        except Exception as e:
                            _log_nonfatal(
                                "poll_prices_merged_price_parse_failed",
                                e,
                                symbol=str(sym),
                                raw_price=px,
                            )
                            continue

                        # Outlier check against recent DB prices
                        hist = list(recent_price_history.get(str(sym), []))
                        if _detect_outlier(hist, px_f):
                            pending_outlier_alerts.append(
                                {
                                    "event_title": f"Price outlier: {sym}",
                                    "symbol": str(sym),
                                    "horizon_s": 0,
                                    "expected_z": 0.0,
                                    "confidence": 1.0,
                                    "explain": {
                                        "type": "price_outlier",
                                        "latest": float(px_f),
                                        "lookback_n": int(len(hist)),
                                        "threshold_z": float(OUTLIER_Z),
                                    },
                                }
                            )
                            if PRICE_OUTLIER_REJECT_ENABLED:
                                continue

                        if _reject_split_like_price_row(
                            symbol=str(sym),
                            ts_ms=int(ts_ms),
                            current_price=float(px_f),
                            price_payload=p,
                            hist=hist,
                        ):
                            continue

                        price_rows.append((int(ts_ms), str(sym), float(px_f)))

                        bid = p.get("bid")
                        ask = p.get("ask")
                        spread = p.get("spread")
                        vol = p.get("volume")
                        src = p.get("source")

                        if (
                            (bid is not None)
                            or (ask is not None)
                            or (spread is not None)
                            or (vol is not None)
                            or (src is not None)
                        ):
                            quote_rows.append(
                                (
                                    int(ts_ms),
                                    str(sym),
                                    float(px_f),
                                    (float(bid) if bid is not None else None),
                                    (float(ask) if ask is not None else None),
                                    (float(spread) if spread is not None else None),
                                    (float(vol) if vol is not None else None),
                                    (str(src) if src is not None else None),
                                )
                            )

                    if price_rows or quote_rows:
                        merged_events = []
                        quote_by_key = {(int(r[0]), str(r[1])): r for r in quote_rows}
                        for ts_ms, sym, px in price_rows:
                            qrow = quote_by_key.get((int(ts_ms), str(sym)))
                            merged_events.append(
                                {
                                    "timestamp": int(ts_ms),
                                    "symbol": str(sym),
                                    "provider": str(
                                        qrow[7] if qrow and qrow[7] is not None else "poll_prices"
                                    ),
                                    "source": str(
                                        qrow[7] if qrow and qrow[7] is not None else "poll_prices"
                                    ),
                                    "last": float(px),
                                    "bid": (qrow[3] if qrow else None),
                                    "ask": (qrow[4] if qrow else None),
                                    "volume": (qrow[6] if qrow else None),
                                    "latency_ms": max(0, int(now_ts_ms) - int(ts_ms)),
                                }
                            )
                        publish_counts.update(
                            publish_price_events(
                                merged_events,
                                con=conp,
                                write_prices=True,
                                write_quotes=True,
                                write_raw=False,
                                emit_telemetry=False,
                                component="engine.data.poll_prices",
                                job=JOB_NAME,
                                update_symbols=True,
                            )
                        )

                        try:
                            first_ts_ms = min(int(evt["timestamp"]) for evt in merged_events)
                            first_provider = str(merged_events[0].get("provider") or "poll_prices")
                            post_commit_first_tick["provider"] = str(first_provider)
                            post_commit_first_tick["first_ts_ms"] = int(first_ts_ms)
                        except Exception as e:
                            _log_nonfatal(
                                "poll_prices_first_tick_extract_failed",
                                e,
                                warn_key="poll_prices_first_tick_extract_failed",
                                merged_event_count=int(len(merged_events or [])),
                            )

                    return price_rows, quote_rows

                merged_write_budget = _merged_price_write_budget()
                merged_write_ok, merged_write_result = _run_write_txn_allow_busy(
                    _write_merged_prices,
                    default=([], []),
                    table="prices",
                    operation="ingest_merged_prices",
                    context={"job": JOB_NAME, "merged_symbols": int(len(merged or {}))},
                    attempts=int(merged_write_budget["attempts"]),
                    maintenance=False,
                    busy_event="poll_prices_merged_prices_write_busy",
                    warn_key="poll_prices_merged_prices_write_busy",
                    extra={"merged_symbols": int(len(merged or {}))},
                    timeout_s=float(merged_write_budget["timeout_s"]),
                    busy_timeout_ms=int(merged_write_budget["busy_timeout_ms"]),
                )
                price_rows, quote_rows = merged_write_result
                for row in price_rows or []:
                    if len(row) <= 1:
                        continue
                    sym_s = str(row[1] or "").strip().upper()
                    if not sym_s:
                        continue
                    try:
                        row_ts_ms = int(row[0])
                    except Exception:
                        row_ts_ms = int(now_ts_ms)
                    cycle_fresh_symbol_ts_ms[sym_s] = max(
                        int(cycle_fresh_symbol_ts_ms.get(sym_s) or 0),
                        int(row_ts_ms),
                    )
                if merged_write_ok:
                    _finalize_post_commit_price_cycle(
                        pending_outlier_alerts,
                        post_commit_first_tick,
                    )
                    if (raw_quote_rows or slip_rows or futures_bar_rows) and _should_persist_provider_rows():
                        if get_timeseries_write_policy().sync_provider_aux_sqlite_write_enabled:
                            provider_rows_ok, _ = _persist_provider_auxiliary_rows_sync(
                                raw_quote_rows,
                                slip_rows,
                                futures_bar_rows,
                            )
                        else:
                            provider_rows_ok = _enqueue_provider_auxiliary_rows(
                                raw_quote_rows,
                                slip_rows,
                                futures_bar_rows,
                            )
                        if not provider_rows_ok and not pipeline_error:
                            pipeline_error = "provider_rows_write_busy"
                        if not provider_rows_ok:
                            producer_backpressure = True
                else:
                    pipeline_raw_rows = int(len(raw_quote_rows) + len(futures_bar_rows))
                    pipeline_last_ingested_ts_ms = max(
                        [int(row[0]) for row in raw_quote_rows] + [int(row[1]) for row in futures_bar_rows],
                        default=None,
                    )
                    pipeline_error = "price_write_busy"
                    fail_s = _next_fail_backoff_s(fail_s)

                pipeline_price_rows = int(len(price_rows))
                pipeline_quote_rows = int(len(quote_rows))
                pipeline_raw_rows = int(len(raw_quote_rows) + len(futures_bar_rows))
                pipeline_dedup_drops = int(publish_counts.get("dedup_drops") or 0)
                pipeline_gap_events = int(publish_counts.get("gap_events") or 0)
                pipeline_normalization_failures = int(publish_counts.get("normalization_failures") or 0)
                async_status = price_persistence_backpressure_status(publish_counts)
                async_backpressure = bool(async_status.get("backpressure"))
                if merged_write_ok and async_backpressure:
                    if not pipeline_error:
                        pipeline_error = str(async_status.get("reason") or "async_price_writer_backpressure")
                    producer_backpressure = True
                pipeline_ok = bool(
                    merged_write_ok and (pipeline_price_rows > 0 or pipeline_quote_rows > 0 or pipeline_raw_rows > 0)
                    and not async_backpressure
                )

                if merged_write_ok:
                    log_event(
                        LOG,
                        20,
                        "poll_prices_batch_written",
                        component="engine.data.poll_prices",
                        extra={
                            "job": JOB_NAME,
                            "symbols": int(len(price_rows)),
                            "providers": sorted(list(got_by_provider.keys())),
                        },
                    )

                    emit_counter(
                        "order_throughput",
                        len(price_rows),
                        component="engine.data.poll_prices",
                        job=JOB_NAME,
                        extra_tags={"throughput_type": "price_rows"},
                    )

                    if price_rows:
                        newest_ts_ms = max(int(r[0]) for r in price_rows)
                        latency_ms = max(0, int(now_ts_ms) - int(newest_ts_ms))
                        pipeline_last_ingested_ts_ms = int(newest_ts_ms)
                        pipeline_latency_ms = int(latency_ms)

                        emit_timing(
                            "market_data_latency_ms",
                            latency_ms,
                            component="engine.data.poll_prices",
                            job=JOB_NAME,
                        )

                        trace_event(
                            "market_data_event",
                            component="engine.data.poll_prices",
                            entity_type="job",
                            entity_id=JOB_NAME,
                            payload={"symbols": int(len(price_rows)), "latency_ms": int(latency_ms)},
                            job=JOB_NAME,
                        )
                    elif raw_quote_rows or futures_bar_rows:
                        pipeline_last_ingested_ts_ms = max(
                            [int(row[0]) for row in raw_quote_rows] + [int(row[1]) for row in futures_bar_rows],
                            default=None,
                        )

                    fail_s = _successful_price_cycle_backoff_s(
                        current_fail_s=fail_s,
                        producer_backpressure=producer_backpressure,
                    )
            else:
                # fallback: write minimal heartbeat price if possible
                if "SPY" not in merged:
                    merged["SPY"] = {"ts_ms": now_ts_ms, "price": None}

                try:
                    set_state(WARMING_UP, "waiting_for_first_market_data")
                except Exception as e:
                    _log_nonfatal("poll_prices_waiting_state_update_failed", e)

                pipeline_ok = any(bool(session_manager.ok()) for session_manager in rest_managers.values())
                pipeline_raw_rows = int(len(raw_quote_rows) + len(futures_bar_rows))
                pipeline_dedup_drops = 0
                pipeline_gap_events = 0
                pipeline_normalization_failures = 0
                pipeline_last_ingested_ts_ms = max(
                    [int(row[0]) for row in raw_quote_rows] + [int(row[1]) for row in futures_bar_rows],
                    default=None,
                )
                if not pipeline_ok:
                    pipeline_error = "no_live_quotes_merged"
                fail_s = _next_fail_backoff_s(fail_s)

            _record_poll_prices_status(
                source_manager,
                ok=bool(pipeline_ok),
                raw_rows=pipeline_raw_rows,
                price_rows=pipeline_price_rows,
                quote_rows=pipeline_quote_rows,
                dedup_drops=pipeline_dedup_drops,
                gap_events=pipeline_gap_events,
                normalization_failures=pipeline_normalization_failures,
                last_ingested_ts_ms=pipeline_last_ingested_ts_ms,
                error=(pipeline_error or None),
                providers=list(rest_managers.keys()),
                provider_errors=provider_errors,
                provider_latencies_ms=provider_latencies_ms,
                provider_result_counts=provider_result_counts,
                have_price_feed_lock=bool(have_price_feed_lock),
                fail_backoff_s=fail_s,
                latency_ms=pipeline_latency_ms,
                message=("poll_prices cycle complete" if pipeline_ok else "poll_prices awaiting live quotes"),
            )
            _mark_stale(
                now_ts_ms,
                active_symbol_rows=cycle_universe.active_symbol_rows,
                fresh_symbols=sorted(cycle_fresh_symbol_ts_ms.keys()),
                fresh_symbol_ts_ms=cycle_fresh_symbol_ts_ms,
            )
            _sleep_with_jitter(fail_s or float(POLL_SECONDS))

    finally:
        try:
            for _mgr in rest_managers.values():
                try:
                    _mgr.close()
                except Exception as e:
                    _log_nonfatal("poll_prices_manager_close_failed", e)
        except Exception as e:
            _log_nonfatal("poll_prices_manager_close_loop_failed", e)

        try:
            from engine.runtime.async_writer import shutdown_async_writer

            shutdown_async_writer(timeout_s=5.0)
        except Exception as e:
            _log_nonfatal("poll_prices_async_writer_shutdown_failed", e)

        if _uses_price_feed_lock() and have_price_feed_lock:
            try:
                _release_price_feed_lock()
            except Exception as e:
                _log_nonfatal("poll_prices_release_feed_lock_failed", e)

        if _uses_child_job_lock() and have_job_lock:
            release_job_lock(JOB_LIVENESS_NAME, OWNER, PID)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except BaseException as e:
        try:
            _record_poll_prices_status(
                get_manager(),
                ok=False,
                providers=[],
                have_price_feed_lock=False,
                error=f"{type(e).__name__}: {e}",
                message="poll_prices unhandled exception",
            )
            _log_nonfatal(
                "poll_prices_unhandled_exception",
                RuntimeError(f"{type(e).__name__}: {e}"),
                error_type=type(e).__name__,
                error_message=str(e),
            )
        except Exception as status_error:
            _log_nonfatal(
                "poll_prices_unhandled_exception_record_failed",
                status_error,
                error_type=type(e).__name__,
                error_message=str(e),
            )
        raise
