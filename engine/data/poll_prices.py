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
import json
import random
import statistics
import traceback
import importlib
from typing import Dict, Any, Tuple, List, Optional

from engine.runtime import dbapi_compat as dbapi
from engine.runtime.storage import (
    _pid_is_running,
    connect,
    close_pooled_connections,
    get_timescale_client,
    init_db,
    acquire_job_lock,
    register_after_commit,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
    run_write_txn,
)

_CCXT_FETCHER = None
_CCXT_IMPORT_ERROR = None


def _fetch_last_prices_ccxt(*args, **kwargs):
    global _CCXT_FETCHER, _CCXT_IMPORT_ERROR
    if _CCXT_FETCHER is None and _CCXT_IMPORT_ERROR is None:
        try:
            module = importlib.import_module("engine.data.live_prices.ccxt_live")
            _CCXT_FETCHER = getattr(module, "fetch_last_prices_ccxt")
        except Exception as e:
            _CCXT_IMPORT_ERROR = e
    if _CCXT_IMPORT_ERROR is not None:
        _log_nonfatal(
            "poll_prices_ccxt_import_failed",
            RuntimeError(f"{type(_CCXT_IMPORT_ERROR).__name__}: {_CCXT_IMPORT_ERROR}"),
            warn_key="ccxt_import_failed",
        )
        return {}
    return _CCXT_FETCHER(*args, **kwargs)

from engine.data.live_prices.provider import get_price_provider_by_name
from engine.data.default_symbols import load_default_symbols
from engine.data.provider_registry import get_polling_provider_names
from engine.data.provider_router import compute_provider_health, detect_cross_provider_anomalies, select_best_quotes_from_snapshots
from engine.data.provider_sessions import BaseProviderSession, ProviderSessionManager
from engine.runtime.alerts import emit_alert
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.logging import get_logger, log_event
from engine.runtime.metrics import emit_counter, emit_gauge, emit_timing
from engine.runtime.runtime_meta import meta_get, meta_set, meta_set_if_missing
from engine.runtime.lifecycle_state import LIVE, WARMING_UP, set_state
from engine.runtime.price_router import publish_price_events
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


def _log_nonfatal(event: str, error: Exception, **context: Any) -> None:
    try:
        warn_key = context.pop("warn_key", None)
        if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
            return
        log_failure(
            LOG,
            event=str(event),
            code=str(event).upper(),
            message=str(event),
            error=error,
            level=30,
            component="engine.data.poll_prices",
            extra=context or None,
            persist=False,
        )
        if warn_key:
            _WARNED_NONFATAL_KEYS.add(str(warn_key))
    except Exception as log_error:
        print(
            f"poll_prices_nonfatal_log_failed event={event} error={type(error).__name__}: {error} "
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

FAIL_BASE_S = float(os.environ.get("POLL_FAIL_BASE_S", "2.0"))
FAIL_MAX_S = float(os.environ.get("POLL_FAIL_MAX_S", "60.0"))
PROVIDER_HEALTH_EVERY_S = float(os.environ.get("STREAM_PRICES_PROVIDER_HEALTH_EVERY_S", "15.0"))
POLL_PROVIDER_DEAD_AFTER_MS = int(os.environ.get("POLL_PROVIDER_DEAD_AFTER_MS", str(max(POLL_SECONDS * 3000, 30000))))

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


def _enqueue_provider_auxiliary_rows(
    raw_quote_rows,
    slip_rows,
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
            publish_price_events(
                raw_events,
                con=None,
                write_prices=False,
                write_quotes=False,
                write_raw=True,
                emit_telemetry=False,
                component="engine.data.poll_prices",
                job=JOB_NAME,
            )
        except Exception as e:
            ok = False
            _log_nonfatal(
                "poll_prices_provider_raw_enqueue_failed",
                e,
                warn_key="poll_prices_provider_raw_enqueue_failed",
                raw_rows=int(len(raw_quote_rows)),
            )

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

    return _run_write_txn_allow_busy(
        _write_provider_rows,
        default=None,
        table="prices",
        operation="ingest_provider_price_rows",
        context={"job": JOB_NAME, "raw_rows": int(len(raw_quote_rows)), "slip_rows": int(len(slip_rows))},
        attempts=1,
        maintenance=False,
        busy_event="poll_prices_provider_rows_write_busy",
        warn_key="poll_prices_provider_rows_write_busy",
        extra={"raw_rows": int(len(raw_quote_rows)), "slip_rows": int(len(slip_rows))},
        timeout_s=0.5,
        busy_timeout_ms=500,
    )
def _is_sqlite_busy_error(error: Exception) -> bool:
    return dbapi.is_transient_write_error(error)


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
        if _is_sqlite_busy_error(e):
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


def _load_symbol_providers() -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
    con = None
    owns = False
    rows = []
    try:
        con = connect()
        owns = True
        rows = con.execute(
            """
            SELECT symbol, meta_json
            FROM symbols
            WHERE status IN ('ACTIVE','WATCH')
            """
        ).fetchall() or []

        if not rows:
            rows = con.execute(
                """
                SELECT symbol, meta_json
                FROM symbols
                ORDER BY updated_ts_ms DESC, created_ts_ms DESC, symbol
                LIMIT 250
                """
            ).fetchall() or []
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

    for sym, meta_json in rows:
        try:
            meta = json.loads(meta_json) if meta_json else {}
        except Exception:
            meta = {}

        sym_s = str(sym).strip().upper()
        if not sym_s:
            continue

        provider = str(meta.get("price_provider") or "").strip().lower()

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

    return yf_map, ccxt_map, polygon_map


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


def _recent_prices(con, symbol: str, limit_n: int) -> List[float]:
    rows = con.execute(
        """
        SELECT price
        FROM prices
        WHERE symbol=?
        ORDER BY ts_ms DESC
        LIMIT ?
        """,
        (str(symbol), int(limit_n)),
    ).fetchall() or []
    out: List[float] = []
    for (p,) in rows:
        try:
            if p is None:
                continue
            out.append(float(p))
        except Exception as e:
            _log_nonfatal(
                "poll_prices_recent_price_parse_failed",
                e,
                symbol=str(symbol),
                raw_price=p,
            )
            continue
    out.reverse()
    return out


def _recent_prices_map(symbols: List[str], limit_n: int) -> Dict[str, List[float]]:
    names = [str(sym).strip().upper() for sym in (symbols or []) if str(sym).strip()]
    if not names:
        return {}
    con = connect(readonly=True)
    try:
        return {str(sym): _recent_prices(con, str(sym), int(limit_n)) for sym in names}
    finally:
        try:
            con.close()
        except Exception as e:
            _log_nonfatal(
                "poll_prices_recent_prices_map_close_failed",
                e,
                warn_key="poll_prices_recent_prices_map_close_failed",
            )


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


def _mark_stale(now_ts_ms: int) -> None:
    if not _has_first_price_tick():
        return
    cutoff = now_ts_ms - PRICE_STALE_AFTER_S * 1000
    stale_alerts: List[Dict[str, Any]] = []
    def _write(con) -> None:

        rows = con.execute(
            "SELECT symbol, meta_json FROM symbols WHERE status IN ('ACTIVE','WATCH')"
        ).fetchall() or []

        for sym, meta_json in rows:
            try:
                meta = json.loads(meta_json) if meta_json else {}
            except Exception:
                meta = {}

            ps = meta.get("price_status", {}) or {}
            last_seen = ps.get("last_seen_ts_ms")
            already_stale = bool(ps.get("stale"))

            if last_seen and int(last_seen) < cutoff:
                if not already_stale:
                    meta.setdefault("price_status", {})
                    meta["price_status"]["stale"] = True

                    stale_alerts.append(
                        {
                            "event_title": f"Price stale: {sym}",
                            "symbol": str(sym),
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
                        (json.dumps(meta, separators=(",", ":")), int(now_ts_ms), str(sym)),
                    )
            else:
                if already_stale:
                    meta.setdefault("price_status", {})
                    meta["price_status"]["stale"] = False
                    con.execute(
                        "UPDATE symbols SET meta_json=?, updated_ts_ms=? WHERE symbol=?",
                        (json.dumps(meta, separators=(",", ":")), int(now_ts_ms), str(sym)),
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

    # Standalone post-commit helpers may leave thread-local pooled handles open.
    # Reset them before the runtime_meta writes so leaked helper state cannot
    # contaminate the next standalone transaction boundary.
    close_pooled_connections()

    first_provider = str(post_commit_first_tick.get("provider") or "").strip()
    first_ts_ms = int(post_commit_first_tick.get("first_ts_ms") or 0)

    if first_ts_ms > 0:
        try:
            did = meta_set_if_missing("first_price_ts_ms", str(int(first_ts_ms)))
            if did:
                set_state(LIVE, "first_market_data_tick")
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
    have_price_feed_lock: bool = False,
    fail_backoff_s: float = 0.0,
    latency_ms: Optional[int] = None,
    disabled: bool = False,
    message: str = "",
) -> Dict[str, Any]:
    provider_names = sorted({str(name).strip() for name in (providers or []) if str(name).strip()})
    close_pooled_connections()
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
            "dedup_drops": int(dedup_drops or 0),
            "gap_events": int(gap_events or 0),
            "normalization_failures": int(normalization_failures or 0),
            "price_rows": int(price_rows or 0),
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
                "dedup_drops": int(dedup_drops or 0),
                "gap_events": int(gap_events or 0),
                "normalization_failures": int(normalization_failures or 0),
                "price_rows": int(price_rows or 0),
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
                "dedup_drops": int(dedup_drops or 0),
                "gap_events": int(gap_events or 0),
                "normalization_failures": int(normalization_failures or 0),
                "latency_ms": (None if latency_ms is None else int(latency_ms)),
                "price_rows": int(price_rows or 0),
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
            error=(str(error or "") or None),
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
        if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
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

    chain = [p for p in chain if p]
    if not chain:
        chain = ["yfinance"]

    # Instantiate non-CCXT providers behind provider sessions; ccxt stays on its own helper path.
    rest_sessions: Dict[str, PollingProviderSession] = {}
    rest_managers: Dict[str, ProviderSessionManager] = {}
    for name in chain:
        if name == "ccxt":
            continue
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
            _log_nonfatal("poll_prices_provider_init_failed", e, provider=name)
            continue

    if not rest_managers:
        for name in list(get_polling_provider_names()) or ["yfinance"]:
            if name == "ccxt":
                continue
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
            persist_provider_health = (now_s - last_provider_health_s) >= PROVIDER_HEALTH_EVERY_S

            if now_s - last_hb_s >= HEARTBEAT_EVERY_S:
                try:
                    close_pooled_connections()
                    if _uses_child_job_lock():
                        touch_job_lock(JOB_NAME, OWNER, PID, best_effort=True)
                    if _uses_price_feed_lock():
                        _touch_price_feed_lock(now_ts_ms)
                    put_job_heartbeat(
                        JOB_NAME,
                        OWNER,
                        PID,
                        extra_json=json.dumps(
                            {
                                "poll_seconds": POLL_SECONDS,
                                "heartbeat_every_s": HEARTBEAT_EVERY_S,
                                "fail_backoff_s": fail_s,
                                "have_price_feed_lock": bool(have_price_feed_lock),
                                "providers": {k: v.provider_telemetry() for k, v in rest_managers.items()},
                            },
                            separators=(",", ":"),
                            sort_keys=True,
                        ),
                        best_effort=True,
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

            yf_map, ccxt_map, polygon_map = _load_symbol_providers()

            merged: Dict[str, Dict[str, Any]] = {}
            got_by_provider: Dict[str, Dict[str, Any]] = {}
            raw_quote_rows = []
            slip_rows = []

            # -----------------------------
            # REST / snapshot providers
            # -----------------------------
            if rest_managers:
                for pname, manager in rest_managers.items():
                    session = rest_sessions[pname]
                    provider_symbol_map = dict(yf_map)
                    if pname == "polygon":
                        provider_symbol_map = dict(polygon_map)
                    if not provider_symbol_map:
                        continue
                    session.set_symbol_map(provider_symbol_map)
                    manager.ensure_subscriptions(sorted(provider_symbol_map.keys()))

                    t0 = time.time()
                    got: Dict[str, Any] = {}
                    try:
                        got = manager.snapshot() or {}
                    except Exception:
                        got = {}

                    telemetry = manager.provider_telemetry() or {}
                    ok = 1 if (got or manager.ok()) else 0
                    err = None if got else (telemetry.get("last_error") if not ok else None)
                    latency_ms = int(getattr(session, "latency_ms")())
                    if latency_ms <= 0:
                        latency_ms = int((time.time() - t0) * 1000)

                    emit_timing(
                        "market_data_latency_ms",
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

                    if persist_provider_health:
                        try:
                            _record_provider_health_telemetry(
                                get_manager(),
                                provider=str(pname),
                                ok=bool(int(ok) == 1),
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

                if persist_provider_health:
                    last_provider_health_s = now_s

            # -----------------------------
            # CCXT (kept separate)
            # -----------------------------
            if ccxt_map and ("ccxt" in chain):
                try:
                    merged.update(_fetch_last_prices_ccxt("binance", ccxt_map) or {})
                except Exception as e:
                    _log_nonfatal(
                        "poll_prices_ccxt_fetch_failed",
                        e,
                        warn_key="poll_prices_ccxt_fetch_failed",
                        symbol_count=int(len(ccxt_map or {})),
                    )

            # -----------------------------
            # Router-based arbitration for overlapping REST providers
            # -----------------------------
            if got_by_provider and yf_map:
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
                            json.dumps(
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
                                separators=(",", ":"),
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
                if merged_write_ok:
                    _finalize_post_commit_price_cycle(
                        pending_outlier_alerts,
                        post_commit_first_tick,
                    )
                    if (raw_quote_rows or slip_rows) and _should_persist_provider_rows():
                        if get_timeseries_write_policy().sync_provider_aux_sqlite_write_enabled:
                            provider_rows_ok, _ = _persist_provider_auxiliary_rows_sync(
                                raw_quote_rows,
                                slip_rows,
                            )
                        else:
                            provider_rows_ok = _enqueue_provider_auxiliary_rows(
                                raw_quote_rows,
                                slip_rows,
                            )
                        if not provider_rows_ok and not pipeline_error:
                            pipeline_error = "provider_rows_write_busy"
                else:
                    pipeline_raw_rows = int(len(raw_quote_rows))
                    pipeline_last_ingested_ts_ms = max((int(row[0]) for row in raw_quote_rows), default=None)
                    pipeline_error = "price_write_busy"
                    fail_s = min(FAIL_MAX_S, fail_s * 2.0 if fail_s else FAIL_BASE_S)

                pipeline_price_rows = int(len(price_rows))
                pipeline_quote_rows = int(len(quote_rows))
                pipeline_raw_rows = int(len(raw_quote_rows))
                pipeline_dedup_drops = int(publish_counts.get("dedup_drops") or 0)
                pipeline_gap_events = int(publish_counts.get("gap_events") or 0)
                pipeline_normalization_failures = int(publish_counts.get("normalization_failures") or 0)
                pipeline_ok = bool(
                    merged_write_ok and (pipeline_price_rows > 0 or pipeline_quote_rows > 0 or pipeline_raw_rows > 0)
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
                    elif raw_quote_rows:
                        pipeline_last_ingested_ts_ms = max(int(row[0]) for row in raw_quote_rows)

                    fail_s = 0.0
            else:
                # fallback: write minimal heartbeat price if possible
                if "SPY" not in merged:
                    merged["SPY"] = {"ts_ms": now_ts_ms, "price": None}

                try:
                    set_state(WARMING_UP, "waiting_for_first_market_data")
                except Exception as e:
                    _log_nonfatal("poll_prices_waiting_state_update_failed", e)

                pipeline_ok = any(bool(session_manager.ok()) for session_manager in rest_managers.values())
                pipeline_raw_rows = int(len(raw_quote_rows))
                pipeline_dedup_drops = 0
                pipeline_gap_events = 0
                pipeline_normalization_failures = 0
                pipeline_last_ingested_ts_ms = max((int(row[0]) for row in raw_quote_rows), default=None)
                if not pipeline_ok:
                    pipeline_error = "no_live_quotes_merged"
                fail_s = min(FAIL_MAX_S, fail_s * 2.0 if fail_s else FAIL_BASE_S)

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
                have_price_feed_lock=bool(have_price_feed_lock),
                fail_backoff_s=fail_s,
                latency_ms=pipeline_latency_ms,
                message=("poll_prices cycle complete" if pipeline_ok else "poll_prices awaiting live quotes"),
            )
            _mark_stale(now_ts_ms)
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

        if _uses_price_feed_lock() and have_price_feed_lock:
            try:
                _release_price_feed_lock()
            except Exception as e:
                _log_nonfatal("poll_prices_release_feed_lock_failed", e)

        if _uses_child_job_lock() and have_job_lock:
            release_job_lock(JOB_NAME, OWNER, PID)


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
            msg = f"poll_prices_unhandled_exception {type(e).__name__}: {e}"
            print(msg, file=sys.stderr, flush=True)
            traceback.print_exc()
        except Exception as status_error:
            _log_nonfatal(
                "poll_prices_unhandled_exception_record_failed",
                status_error,
                error_type=type(e).__name__,
                error_message=str(e),
            )
        raise
