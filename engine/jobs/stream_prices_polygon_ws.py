# FILE: engine/jobs/stream_prices_polygon_ws.py
"""
Daemon: Polygon WebSocket live prices -> SQLite

Writes:
  - price_quotes_raw (per-provider)
  - price_quotes (last/bid/ask/spread/volume)
  - prices (last trade price, for downstream compatibility)

Env:
  POLYGON_API_KEY (required)
  POLYGON_WS_ENDPOINT (default: wss://socket.polygon.io/stocks)
  POLYGON_WS_SUBSCRIBE_TRADES (default: 1)
  POLYGON_WS_SUBSCRIBE_QUOTES (default: 1)

  STREAM_PRICES_FLUSH_MS (default: 250)
  STREAM_PRICES_HEARTBEAT_S (default: 2.0)
  STREAM_PRICES_MIN_WRITE_INTERVAL_MS (default: 250)

  STREAM_PRICES_WS_DEAD_AFTER_MS (default: 8000)
  STREAM_PRICES_WS_RESTART_COOLDOWN_S (default: 10.0)
  STREAM_PRICES_PROVIDER_HEALTH_EVERY_S (default: 2.0)

  JOB_LOCK_STALE_AFTER_S (default: 180)

Notes:
  - Subscribes to ACTIVE/WATCH symbols that have meta_json.price_provider == 'polygon'
    OR no explicit provider (defaults to polygon for equities in the early live phase).
"""

import json
import logging
import os
import signal
import threading
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
from typing import Any, Dict, List, Optional, Set, Tuple

from engine.data._credentials import get_data_credential
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.runtime_meta import meta_set_if_missing, meta_set, meta_get
from services.data_source_manager import get_manager
from engine.runtime.lifecycle_state import set_state, LIVE, WARMING_UP, DEGRADED
from engine.runtime.logging import get_logger
from engine.runtime.metrics import emit_counter, emit_gauge, emit_timing
from engine.runtime.price_router import publish_price_events
from engine.runtime.telemetry_append_buffer import append_price_provider_health
from engine.runtime.tracing import trace_event

if os.environ.get("ENGINE_SUPERVISED") != "1":
    # Do NOT hard-exit: JobManager/supervisor wrappers may omit this env.
    # Exiting here causes restart loops and UI connection resets.
    print("WARN: stream_prices_polygon_ws running without ENGINE_SUPERVISED=1 (continuing)", flush=True)

log = get_logger("runtime.stream_prices_polygon_ws")
_WARNED_NONFATAL_KEYS: set[str] = set()

try:
    import websocket  # websocket-client
    _WEBSOCKET_IMPORT_ERROR = None
except Exception as e:
    websocket = None
    _WEBSOCKET_IMPORT_ERROR = e

from engine.runtime.storage import (
    connect,
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
    run_write_txn,
)

JOB_NAME = "stream_prices_polygon_ws"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))

PROVIDER_NAME = "polygon_ws"

WS_DEAD_AFTER_MS = int(os.environ.get("STREAM_PRICES_WS_DEAD_AFTER_MS", "8000"))
WS_RESTART_COOLDOWN_S = float(os.environ.get("STREAM_PRICES_WS_RESTART_COOLDOWN_S", "10.0"))
PROVIDER_HEALTH_EVERY_S = float(os.environ.get("STREAM_PRICES_PROVIDER_HEALTH_EVERY_S", "2.0"))
STARTUP_SILENCE_FATAL_MS = int(os.environ.get("STREAM_PRICES_STARTUP_SILENCE_FATAL_MS", "120000"))
WS_RECONNECT_BASE_S = float(os.environ.get("POLYGON_WS_RECONNECT_BASE_S", "1.0"))
WS_RECONNECT_MAX_S = float(os.environ.get("POLYGON_WS_RECONNECT_MAX_S", "30.0"))
WS_MAX_RECONNECT_ATTEMPTS = int(
    os.environ.get(
        "POLYGON_WS_MAX_RECONNECT_ATTEMPTS",
        os.environ.get("PROVIDER_MAX_RECONNECT_ATTEMPTS", "20"),
    )
)
STREAM_DEGRADED_AFTER_MS = int(
    os.environ.get("STREAM_PRICES_DEGRADED_AFTER_MS", str(max(int(WS_DEAD_AFTER_MS) * 3, 30000)))
)
STREAM_EVENT_BATCH = int(os.environ.get("STREAM_PRICES_EVENT_BATCH", "50000"))
MAX_SNAPSHOT_RECORDS = max(1, int(os.environ.get("STREAM_PRICES_MAX_SNAPSHOT", "50000")))
SNAPSHOT_CAP_PAUSE_STREAK = 3
_WATERMARK_META_KEY = f"provider_session_{PROVIDER_NAME}_committed_watermarks"

_STOP_EVENT = threading.Event()


def _warn_nonfatal(code: str, error: Exception, *, once_key: str | None = None, **extra: Any) -> None:
    key = str(once_key or "")
    if key:
        if key in _WARNED_NONFATAL_KEYS:
            return
        _WARNED_NONFATAL_KEYS.add(key)
    log_failure(
        log,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.jobs.stream_prices_polygon_ws",
        extra={"job": JOB_NAME, "provider": PROVIDER_NAME, **(extra or {})},
        include_health=False,
        persist=False,
    )


def _request_stop(signum=None, _frame=None) -> None:
    try:
        log.warning("polygon stream stop requested signal=%s", signum)
    except Exception as e:
        _warn_nonfatal("POLYGON_WS_STOP_LOG_FAILED", e, once_key="stop_log", signal=signum)
    _STOP_EVENT.set()


for _sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
    if _sig is None:
        continue
    try:
        signal.signal(_sig, _request_stop)
    except Exception as e:
        _warn_nonfatal("POLYGON_WS_SIGNAL_REGISTER_FAILED", e, once_key=f"signal_register:{_sig}", signal=str(_sig))


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_json_loads(s: str) -> Any:
    try:
        return json.loads(s)
    except Exception as e:
        _warn_nonfatal(
            "POLYGON_WS_JSON_PARSE_FAILED",
            e,
            once_key="safe_json_loads",
            payload=str(s)[:200],
        )
        return None


def _classify_startup_wait_detail(
    err: Optional[str],
    telemetry: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str]:
    detail = str(err or "").strip()
    low = detail.lower()
    telemetry = dict(telemetry or {})
    desired_count = int(telemetry.get("desired_symbol_count") or 0)
    subscribed_count = int(telemetry.get("subscribed_symbol_count") or 0)
    authenticated = bool(telemetry.get("authenticated"))
    connected = bool(telemetry.get("connected"))

    # Startup classification is operationally important because the supervisor
    # uses these details to distinguish "waiting for data" from auth/config failure.
    if desired_count <= 0:
        return DEGRADED, "polygon_ws_no_symbols_subscribed"
    if authenticated and connected and desired_count > 0 and subscribed_count <= 0:
        return DEGRADED, "polygon_ws_authenticated_waiting_for_subscription_data"
    if authenticated and connected and subscribed_count > 0:
        return WARMING_UP, "polygon_ws_authenticated_waiting_for_first_tick"
    if not detail:
        return WARMING_UP, "waiting_for_first_market_data"
    if "api_key_missing" in low or "missing_api_key" in low:
        return DEGRADED, "polygon_ws_config_missing_api_key"
    if "auth" in low or "not authorized" in low or "authentication failed" in low:
        return DEGRADED, "polygon_ws_auth_failed"
    if "open_timeout" in low:
        return DEGRADED, "polygon_ws_open_timeout"
    if "timeout" in low or "timed out" in low:
        return DEGRADED, "polygon_ws_timeout"
    if "handshake" in low or "connection refused" in low or "dns" in low:
        return DEGRADED, "polygon_ws_network_error"
    return WARMING_UP, "waiting_for_first_market_data"





_MICRO_EWMA_ALPHA = float(os.environ.get("MICRO_EWMA_ALPHA", "0.20"))
_MICRO_TRADE_DECAY = float(os.environ.get("MICRO_TRADE_DECAY", "0.92"))


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception as e:
        _warn_nonfatal(
            "POLYGON_WS_SAFE_FLOAT_FAILED",
            e,
            once_key="safe_float",
            value=repr(x)[:120],
        )
        return None


def _clamp(x: float, lo: float, hi: float) -> float:
    return float(max(float(lo), min(float(hi), float(x))))


def _trade_side_from_rec(rec: Dict[str, Any], trade_px: Optional[float]) -> float:
    px = _safe_float(trade_px)
    if px is None:
        return 0.0

    bid = _safe_float(rec.get("bid"))
    ask = _safe_float(rec.get("ask"))
    mid = None
    if bid is not None and ask is not None and ask >= bid:
        mid = (float(bid) + float(ask)) / 2.0

    if ask is not None and px >= float(ask):
        return 1.0
    if bid is not None and px <= float(bid):
        return -1.0
    if mid is not None:
        if px > mid:
            return 1.0
        if px < mid:
            return -1.0

    prev_px = _safe_float(rec.get("last_trade_px"))
    if prev_px is not None:
        if px > prev_px:
            return 1.0
        if px < prev_px:
            return -1.0

    prev_side = _safe_float(rec.get("last_trade_side"))
    if prev_side is not None:
        return 1.0 if prev_side >= 0.0 else -1.0
    return 0.0


def _update_trade_microstructure(rec: Dict[str, Any], trade_px: Optional[float], trade_sz: Optional[float]) -> None:
    sz = _safe_float(trade_sz)
    px = _safe_float(trade_px)
    if sz is None or sz <= 0.0:
        sz = 0.0

    buy_v = float(_safe_float(rec.get("buy_vol_ema")) or 0.0) * float(_MICRO_TRADE_DECAY)
    sell_v = float(_safe_float(rec.get("sell_vol_ema")) or 0.0) * float(_MICRO_TRADE_DECAY)

    side = _trade_side_from_rec(rec, px)
    if side > 0.0:
        buy_v += float(sz)
    elif side < 0.0:
        sell_v += float(sz)

    rec["buy_vol_ema"] = float(buy_v)
    rec["sell_vol_ema"] = float(sell_v)
    rec["last_trade_side"] = float(side)

    if px is not None:
        rec["last_trade_px"] = float(px)

    denom = float(buy_v) + float(sell_v)
    # These microstructure fields are lightweight derived context for downstream
    # routing/analytics; the raw quote/trade payload remains the canonical feed record.
    if denom > 0.0:
        rec["trade_aggressor_imbalance"] = float((float(buy_v) - float(sell_v)) / denom)
    else:
        rec["trade_aggressor_imbalance"] = 0.0


def _update_quote_microstructure(rec: Dict[str, Any]) -> None:
    bid = _safe_float(rec.get("bid"))
    ask = _safe_float(rec.get("ask"))
    bid_sz = _safe_float(rec.get("bid_sz"))
    ask_sz = _safe_float(rec.get("ask_sz"))

    if bid is not None and ask is not None and ask >= bid:
        mid = (float(bid) + float(ask)) / 2.0
        spread = float(ask) - float(bid)
        rec["mid_px"] = float(mid)
        rec["spread"] = float(spread)

        spread_bps = 0.0
        if mid > 1e-12:
            spread_bps = 10000.0 * float(spread) / float(mid)
        rec["spread_bps"] = float(spread_bps)

        prev_ewma = _safe_float(rec.get("spread_ewma_bps"))
        if prev_ewma is None:
            ewma = float(spread_bps)
            var = 0.0
        else:
            alpha = float(_MICRO_EWMA_ALPHA)
            ewma = (alpha * float(spread_bps)) + ((1.0 - alpha) * float(prev_ewma))
            prev_var = float(_safe_float(rec.get("spread_var_ewma")) or 0.0)
            diff = float(spread_bps) - float(prev_ewma)
            var = (alpha * diff * diff) + ((1.0 - alpha) * prev_var)

        rec["spread_ewma_bps"] = float(ewma)
        rec["spread_var_ewma"] = float(max(0.0, var))

        std = float(max(0.0, var)) ** 0.5
        if std > 1e-9:
            rec["spread_z"] = float((float(spread_bps) - float(ewma)) / std)
        else:
            rec["spread_z"] = 0.0
        rec["spread_widening"] = float(max(0.0, float(spread_bps) - float(ewma)))

    if bid_sz is not None:
        rec["bid_sz"] = float(bid_sz)
    if ask_sz is not None:
        rec["ask_sz"] = float(ask_sz)

    if bid_sz is not None and ask_sz is not None:
        denom = float(bid_sz) + float(ask_sz)
        if denom > 0.0:
            rec["order_book_imbalance"] = float((float(bid_sz) - float(ask_sz)) / denom)
        else:
            rec["order_book_imbalance"] = 0.0


def _put_provider_health(
    provider: str,
    ok: bool,
    latency_ms: Optional[int],
    n_symbols: int,
    error: Optional[str],
) -> None:
    from engine.runtime.state_cache import cache_invalidate_namespace

    now_ms = int(_now_ms())
    append_price_provider_health(
        provider=str(provider),
        ok=bool(ok),
        latency_ms=(None if latency_ms is None else int(latency_ms)),
        n_symbols=int(n_symbols),
        error=(str(error)[:400] if error else None),
        ts_ms=int(now_ms),
    )
    cache_invalidate_namespace("api_read", prefix="feed_status")
    cache_invalidate_namespace("provider_health")
    try:
        get_manager().record_source_status(
            str(provider),
            ok=bool(ok),
            message="stream provider health update",
            error=str(error or ""),
            meta={
                "job_name": JOB_NAME,
                "latency_ms": None if latency_ms is None else int(latency_ms),
                "n_symbols": int(n_symbols),
            },
            ts_ms=int(now_ms),
            best_effort=True,
        )

        emit_gauge(
            "provider_uptime",
            1.0 if ok else 0.0,
            component="engine.jobs.stream_prices_polygon_ws",
            job=JOB_NAME,
            provider=provider,
        )
        emit_gauge(
            "job_health",
            1.0 if ok else 0.0,
            component="engine.jobs.stream_prices_polygon_ws",
            job=JOB_NAME,
            provider=provider,
            extra_tags={"metric_scope": "provider_health"},
        )
        if latency_ms is not None:
            emit_timing(
                "market_data_latency_ms",
                int(latency_ms),
                component="engine.jobs.stream_prices_polygon_ws",
                job=JOB_NAME,
                provider=provider,
            )
    except Exception as e:
        _warn_nonfatal(
            "POLYGON_WS_PROVIDER_HEALTH_UPDATE_FAILED",
            e,
            once_key=f"provider_health:{provider}",
            provider=str(provider),
            ok=bool(ok),
            latency_ms=None if latency_ms is None else int(latency_ms),
            n_symbols=int(n_symbols),
        )

def _load_symbol_map() -> Dict[str, str]:
    """Return mapping internal symbol -> polygon ticker."""
    con = connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT symbol, meta_json
            FROM symbols
            WHERE status IN ('ACTIVE','WATCH')
            """
        ).fetchall()

        if not rows:
            rows = con.execute(
                """
                SELECT symbol, meta_json
                FROM symbols
                ORDER BY updated_ts_ms DESC, created_ts_ms DESC, symbol
                LIMIT 250
                """
            ).fetchall()
    finally:
        con.close()

    out: Dict[str, str] = {}

    for sym, meta_json in rows:
        try:
            meta = json.loads(meta_json) if meta_json else {}
        except Exception:
            meta = {}

        provider = (meta.get("price_provider") or "").lower().strip()
        poly = (meta.get("polygon_ticker") or sym)
        poly = str(poly).strip() or str(sym)

        if provider in ("", "polygon", "polygon_ws"):
            out[str(sym)] = str(poly)

    if not out:
        env_symbols = [
            str(s).strip().upper()
            for s in os.environ.get("DEFAULT_SYMBOLS", "SPY,QQQ,IWM,DIA").split(",")
            if str(s).strip()
        ]
        for sym in env_symbols:
            out[str(sym)] = str(sym)

    if not out:
        out = {"SPY": "SPY"}

    return out

class _WsIngest:
    def __init__(self, api_key: str, endpoint: str, subscribe_trades: bool, subscribe_quotes: bool):
        if websocket is None:
            raise RuntimeError("websocket-client is not installed")

        self.api_key = api_key
        self.endpoint = endpoint
        self.sub_trades = bool(subscribe_trades)
        self.sub_quotes = bool(subscribe_quotes)

        self._lock = threading.RLock()
        self._ws: Any = None
        self._thread: Optional[threading.Thread] = None
        self._stop = False

        self._subscribed: Set[str] = set()

        # ticker -> latest fields
        self._last: Dict[str, Dict[str, Any]] = {}
        self._last_msg_ts_ms = 0
        self._session_started_ts_ms = _now_ms()
        self._last_error: Optional[str] = None

        self._start()

    def close(self) -> None:
        self._stop = True
        try:
            if self._ws:
                self._ws.close()
            self._ws = None
        except Exception as e:
            _warn_nonfatal("POLYGON_WS_CLOSE_FAILED", e, once_key="ws_close")

    def restart(self) -> None:
        try:
            with self._lock:
                self._subscribed = set()
            if self._ws:
                self._ws.close()
            self._ws = None
        except Exception as e:
            _warn_nonfatal("POLYGON_WS_RESTART_FAILED", e, once_key="ws_restart")

        # force subscription refresh
        self._last_msg_ts_ms = 0
        self._session_started_ts_ms = _now_ms()

    def last_msg_age_ms(self) -> int:
        ts = int(self._last_msg_ts_ms or 0)
        if ts <= 0:
            return 10**9
        return _now_ms() - ts

    def session_age_ms(self) -> int:
        ts = int(self._session_started_ts_ms or 0)
        if ts <= 0:
            return 10**9
        return max(0, _now_ms() - ts)

    def last_error(self) -> Optional[str]:
        return None if self._last_error is None else str(self._last_error)

    def ensure_subscriptions(self, poly_tickers: Set[str]) -> None:
        poly_tickers = {str(x).strip() for x in (poly_tickers or set()) if str(x).strip()}
        if not poly_tickers:
            return

        to_add: Set[str] = set()
        with self._lock:
            for t in poly_tickers:
                if t not in self._subscribed:
                    to_add.add(t)

        if not to_add:
            return

        params: List[str] = []
        if self.sub_trades:
            params.extend([f"T.{t}" for t in sorted(to_add)])
        if self.sub_quotes:
            params.extend([f"Q.{t}" for t in sorted(to_add)])

        if not params:
            return

        msg = {"action": "subscribe", "params": ",".join(params)}

        try:
            if self._ws:
                self._ws.send(json.dumps(msg))
                with self._lock:
                    self._subscribed |= set(to_add)
        except Exception as e:
            _warn_nonfatal(
                "POLYGON_WS_SUBSCRIBE_SEND_FAILED",
                e,
                once_key=f"subscribe:{','.join(sorted(to_add))[:120]}",
                symbol_count=int(len(to_add)),
            )

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {k: dict(v) for k, v in (self._last or {}).items()}

    # ----------------------------
    # WS lifecycle
    # ----------------------------

    def _start(self) -> None:
        t = threading.Thread(target=self._run, name="polygon_ws_ingest", daemon=True)
        self._thread = t
        t.start()

    def _run(self) -> None:
        raise RuntimeError("_WsIngest is obsolete; use PolygonWSSession + ProviderSessionManager")

    def _on_open(self, ws):
        trace_event(
            "provider_session_open",
            component="engine.jobs.stream_prices_polygon_ws",
            entity_type="provider",
            entity_id=PROVIDER_NAME,
            payload={"endpoint": self.endpoint},
            job=JOB_NAME,
            provider=PROVIDER_NAME,
        )

        # New socket session => clear subscription tracking.
        try:
            with self._lock:
                self._subscribed = set()
        except Exception as e:
            _warn_nonfatal("POLYGON_WS_SUBSCRIPTION_RESET_FAILED", e, once_key="subscription_reset")

        self._last_error = None

        try:
            ws.send(json.dumps({"action": "auth", "params": self.api_key}))
        except Exception as e:
            self._last_error = (repr(e) or 'ws_auth_failed')[:400]
            _warn_nonfatal("POLYGON_WS_AUTH_SEND_FAILED", e, once_key="auth_send")

        # mark message time so dead detector doesn't immediately trigger
        try:
            self._last_msg_ts_ms = _now_ms()
        except Exception as e:
            _warn_nonfatal("POLYGON_WS_LAST_MSG_TS_UPDATE_FAILED", e, once_key="last_msg_ts_update")

    def _on_close(self, ws, code=None, msg=None):
        parts = []
        if code is not None:
            parts.append(f"code={code}")
        if msg:
            parts.append(str(msg))
        if parts:
            self._last_error = ("ws_closed:" + " ".join(parts))[:400]

        trace_event(
            "provider_session_close",
            component="engine.jobs.stream_prices_polygon_ws",
            entity_type="provider",
            entity_id=PROVIDER_NAME,
            payload={"code": code, "message": msg},
            job=JOB_NAME,
            provider=PROVIDER_NAME,
        )
        return

    def _on_error(self, ws, error):
        try:
            self._last_error = (repr(error) if error is not None else "ws_error")[:400]
        except Exception:
            self._last_error = "ws_error"

        trace_event(
            "provider_session_error",
            component="engine.jobs.stream_prices_polygon_ws",
            entity_type="provider",
            entity_id=PROVIDER_NAME,
            payload={"error": str(error)},
            job=JOB_NAME,
            provider=PROVIDER_NAME,
        )
        return

    def _on_message(self, ws, message: str):
        now_ms = _now_ms()
        self._last_msg_ts_ms = now_ms

        emit_counter(
            "market_data_event",
            1,
            component="engine.jobs.stream_prices_polygon_ws",
            job=JOB_NAME,
            provider=PROVIDER_NAME,
        )
        if int(self._session_started_ts_ms or 0) <= 0:
            self._session_started_ts_ms = now_ms

        payload = _safe_json_loads(message)
        if payload is None:
            return

        if isinstance(payload, dict):
            payload = [payload]
        if not isinstance(payload, list):
            return

        with self._lock:
            for ev in payload:
                if not isinstance(ev, dict):
                    continue

                et = str(ev.get("ev") or "")
                if et == "status":
                    continue

                sym = ev.get("sym") or ev.get("symbol")
                if not sym:
                    continue
                sym = str(sym)

                rec = self._last.get(sym) or {"ts_ms": now_ms}

                if et == "T":
                    p = ev.get("p")
                    if p is not None:
                        try:
                            rec["last"] = float(p)
                        except Exception as e:
                            _warn_nonfatal(
                                "POLYGON_WS_TRADE_PRICE_PARSE_FAILED",
                                e,
                                once_key=f"trade_price:{sym}",
                                symbol=str(sym),
                                raw_value=p,
                            )
                    sz = ev.get("s")
                    if sz is not None:
                        try:
                            rec["volume"] = float(sz)
                        except Exception as e:
                            _warn_nonfatal(
                                "POLYGON_WS_TRADE_SIZE_PARSE_FAILED",
                                e,
                                once_key=f"trade_size:{sym}",
                                symbol=str(sym),
                                raw_value=sz,
                            )

                    _update_trade_microstructure(rec, p, sz)

                    tms = ev.get("t")
                    if tms is not None:
                        try:
                            rec["ts_ms"] = int(tms)
                        except Exception:
                            rec["ts_ms"] = now_ms
                    else:
                        rec["ts_ms"] = now_ms

                elif et == "Q":
                    bp = ev.get("bp")
                    ap = ev.get("ap")
                    bs = ev.get("bs")
                    a_sz = ev.get("as")

                    if bp is not None:
                        try:
                            rec["bid"] = float(bp)
                        except Exception as e:
                            _warn_nonfatal(
                                "POLYGON_WS_QUOTE_BID_PARSE_FAILED",
                                e,
                                once_key=f"quote_bid:{sym}",
                                symbol=str(sym),
                                raw_value=bp,
                            )
                    if ap is not None:
                        try:
                            rec["ask"] = float(ap)
                        except Exception as e:
                            _warn_nonfatal(
                                "POLYGON_WS_QUOTE_ASK_PARSE_FAILED",
                                e,
                                once_key=f"quote_ask:{sym}",
                                symbol=str(sym),
                                raw_value=ap,
                            )
                    if bs is not None:
                        try:
                            rec["bid_sz"] = float(bs)
                        except Exception as e:
                            _warn_nonfatal(
                                "POLYGON_WS_QUOTE_BID_SIZE_PARSE_FAILED",
                                e,
                                once_key=f"quote_bid_size:{sym}",
                                symbol=str(sym),
                                raw_value=bs,
                            )
                    if a_sz is not None:
                        try:
                            rec["ask_sz"] = float(a_sz)
                        except Exception as e:
                            _warn_nonfatal(
                                "POLYGON_WS_QUOTE_ASK_SIZE_PARSE_FAILED",
                                e,
                                once_key=f"quote_ask_size:{sym}",
                                symbol=str(sym),
                                raw_value=a_sz,
                            )

                    if ("bid" in rec) and ("ask" in rec):
                        try:
                            rec["spread"] = float(rec["ask"]) - float(rec["bid"])
                        except Exception as e:
                            _warn_nonfatal(
                                "POLYGON_WS_QUOTE_SPREAD_COMPUTE_FAILED",
                                e,
                                once_key=f"quote_spread:{sym}",
                                symbol=str(sym),
                                bid=rec.get("bid"),
                                ask=rec.get("ask"),
                            )

                    _update_quote_microstructure(rec)

                    tms = ev.get("t")
                    if tms is not None:
                        try:
                            rec["ts_ms"] = int(tms)
                        except Exception:
                            rec["ts_ms"] = now_ms
                    else:
                        rec["ts_ms"] = now_ms

                else:
                    continue

                self._last[sym] = rec

                try:
                    meta_set("price_provider_active", PROVIDER_NAME, best_effort=True)
                    did = meta_set_if_missing("first_price_ts_ms", str(int(rec.get("ts_ms") or now_ms)))
                    if did:
                        set_state(LIVE, "first_market_data_tick")
                except Exception as e:
                    log.warning("failed to update first market data lifecycle latch: %s", repr(e))

def _flush_to_db(
    con,
    ts_ms: int,
    sym_to_poly: Dict[str, str],
    poly_snapshot: Dict[str, Dict[str, Any]],
    min_write_interval_ms: int,
    last_write_by_symbol: Dict[str, int],
    write_price_events: bool = True,
) -> Tuple[int, int, int, Dict[str, Dict[str, int]]]:
    n_raw = 0
    n_q = 0
    n_px = 0

    raw_rows: List[tuple] = []
    q_rows: List[tuple] = []
    px_rows: List[tuple] = []
    micro_rows: List[tuple] = []
    watermark_updates: Dict[str, Dict[str, int]] = {}

    write_cap = 2000
    write_count = 0

    for sym, poly in (sym_to_poly or {}).items():
        if write_count >= write_cap:
            break

        rec = poly_snapshot.get(str(poly))
        if not rec:
            continue

        rts = int(rec.get("ts_ms") or ts_ms)
        if rts <= 0:
            rts = ts_ms

        last_ts = int(last_write_by_symbol.get(sym) or 0)
        if last_ts > 0 and rts <= last_ts:
            continue
        if last_ts > 0 and (rts - last_ts) < int(min_write_interval_ms):
            continue

        last_write_by_symbol[sym] = rts
        write_count += 1

        last = rec.get("last")
        bid = rec.get("bid")
        ask = rec.get("ask")
        spread = rec.get("spread")
        vol = rec.get("volume")

        try:
            last_f = float(last) if last is not None else None
        except Exception:
            last_f = None
        try:
            bid_f = float(bid) if bid is not None else None
        except Exception:
            bid_f = None
        try:
            ask_f = float(ask) if ask is not None else None
        except Exception:
            ask_f = None
        try:
            spread_f = _safe_float(spread)
            if spread_f is None and ask_f is not None and bid_f is not None:
                spread_f = float(ask_f) - float(bid_f)
        except Exception:
            spread_f = None
        try:
            vol_f = float(vol) if vol is not None else None
        except Exception:
            vol_f = None

        raw_rows.append((int(rts), str(sym), str(PROVIDER_NAME), last_f, bid_f, ask_f, spread_f, vol_f))
        q_rows.append((int(rts), str(sym), last_f, bid_f, ask_f, spread_f, vol_f, str(PROVIDER_NAME)))
        trade_ts_ms = int(rec.get("trade_ts_ms") or 0)
        quote_ts_ms = int(rec.get("quote_ts_ms") or 0)
        if trade_ts_ms > 0 or quote_ts_ms > 0:
            mark = watermark_updates.setdefault(str(sym), {})
            if trade_ts_ms > 0:
                mark["T"] = max(int(mark.get("T") or 0), int(trade_ts_ms))
            if quote_ts_ms > 0:
                mark["Q"] = max(int(mark.get("Q") or 0), int(quote_ts_ms))

        mid_px = _safe_float(rec.get("mid_px"))

        effective_price = last_f
        if effective_price is None and mid_px is not None:
            effective_price = float(mid_px)
        if effective_price is None and bid_f is not None and ask_f is not None:
            effective_price = float(bid_f + ask_f) / 2.0

        if effective_price is not None:
            px_rows.append((int(rts), str(sym), float(effective_price)))

        mid_px = _safe_float(rec.get("mid_px"))
        bid_sz = _safe_float(rec.get("bid_sz"))
        ask_sz = _safe_float(rec.get("ask_sz"))
        spread_bps = _safe_float(rec.get("spread_bps"))
        spread_z = _safe_float(rec.get("spread_z"))
        spread_widening = _safe_float(rec.get("spread_widening"))
        order_book_imbalance = _safe_float(rec.get("order_book_imbalance"))
        trade_buy_volume = _safe_float(rec.get("buy_vol_ema"))
        trade_sell_volume = _safe_float(rec.get("sell_vol_ema"))
        trade_aggressor_imbalance = _safe_float(rec.get("trade_aggressor_imbalance"))

        directional = 0.0
        if order_book_imbalance is not None:
            directional += 0.5 * float(order_book_imbalance)
        if trade_aggressor_imbalance is not None:
            directional += 0.5 * float(trade_aggressor_imbalance)

        spread_penalty = 0.0
        if spread_z is not None and spread_z > 0.0:
            spread_penalty = _clamp(float(spread_z) / 5.0, 0.0, 1.0)

        composite_score = float(directional) * float(1.0 - spread_penalty)

        if any(
            v is not None
            for v in (
                mid_px,
                bid_f,
                ask_f,
                bid_sz,
                ask_sz,
                spread_bps,
                spread_z,
                spread_widening,
                order_book_imbalance,
                trade_buy_volume,
                trade_sell_volume,
                trade_aggressor_imbalance,
            )
        ):
            details = {
                "last_trade_px": _safe_float(rec.get("last_trade_px")),
                "last_trade_side": _safe_float(rec.get("last_trade_side")),
                "spread_ewma_bps": _safe_float(rec.get("spread_ewma_bps")),
                "spread_var_ewma": _safe_float(rec.get("spread_var_ewma")),
            }
            micro_rows.append(
                (
                    int(rts),
                    str(sym),
                    str(PROVIDER_NAME),
                    mid_px,
                    bid_f,
                    ask_f,
                    bid_sz,
                    ask_sz,
                    spread_bps,
                    spread_z,
                    spread_widening,
                    order_book_imbalance,
                    trade_buy_volume,
                    trade_sell_volume,
                    trade_aggressor_imbalance,
                    composite_score,
                    json.dumps(details, separators=(",", ":"), sort_keys=True),
                )
            )

    canonical_events = []
    for rts, sym, provider, last_f, bid_f, ask_f, spread_f, vol_f in raw_rows:
        canonical_events.append(
            {
                "timestamp": int(rts),
                "symbol": str(sym),
                "provider": str(provider),
                "source": str(provider),
                "last": last_f,
                "bid": bid_f,
                "ask": ask_f,
                "volume": vol_f,
                "latency_ms": max(0, int(ts_ms) - int(rts)),
            }
        )

    if write_price_events and canonical_events:
        counts = publish_price_events(
            canonical_events,
            con=con,
            write_prices=True,
            write_quotes=True,
            write_raw=True,
            emit_telemetry=False,
            component="engine.jobs.stream_prices_polygon_ws",
            job=JOB_NAME,
            default_provider=PROVIDER_NAME,
        )
        n_raw = int(counts.get("raw") or 0)
        n_q = int(counts.get("quotes") or 0)
        n_px = int(counts.get("prices") or 0)

    if micro_rows:
        con.executemany(
            """
            INSERT OR REPLACE INTO market_microstructure_signals(
              ts_ms, symbol, provider,
              mid_px, bid_px, ask_px, bid_sz, ask_sz,
              spread_bps, spread_z, spread_widening,
              order_book_imbalance,
              trade_buy_volume, trade_sell_volume, trade_aggressor_imbalance,
              composite_score, details_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            micro_rows,
        )

    return n_raw, n_q, n_px, watermark_updates


def _telemetry_field(telemetry: Optional[Dict[str, Any]], key: str, default: Any = None) -> Any:
    if not isinstance(telemetry, dict):
        return default
    return telemetry.get(key, default)


def _writes_paused(telemetry: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(telemetry, dict):
        return True
    manager_state = str(telemetry.get("manager_state") or "").strip().lower()
    connection_state = str(telemetry.get("connection_state") or "").strip().lower()
    connected = bool(telemetry.get("connected"))
    authenticated = bool(telemetry.get("authenticated"))
    if manager_state in {"created", "starting", "connecting", "reconnecting", "backoff", "error", "failed", "closed"}:
        return True
    if connection_state in {"reconnecting", "stale", "disconnected"}:
        return True
    return not (connected and authenticated)


def _snapshot_record_ts(record: Dict[str, Any]) -> int:
    try:
        return int((record or {}).get("ts_ms") or (record or {}).get("event_ts_ms") or (record or {}).get("timestamp") or 0)
    except Exception:
        return 0


def _cap_snapshot_records(
    poly_snapshot: Dict[str, Dict[str, Any]],
    max_records: int,
) -> Tuple[Dict[str, Dict[str, Any]], int]:
    limit = max(1, int(max_records or 1))
    rows = {str(symbol): dict(record or {}) for symbol, record in (poly_snapshot or {}).items()}
    excess = max(0, int(len(rows)) - int(limit))
    if excess <= 0:
        return rows, 0
    ordered = sorted(rows.items(), key=lambda item: (_snapshot_record_ts(item[1]), str(item[0])))
    drop_symbols = {str(symbol) for symbol, _record in ordered[:excess]}
    capped = {symbol: record for symbol, record in rows.items() if str(symbol) not in drop_symbols}
    return capped, int(excess)


def _cap_manager_snapshot(manager: Any, snap: Dict[str, Dict[str, Any]], max_records: int) -> Tuple[Dict[str, Dict[str, Any]], int]:
    if int(len(snap or {})) <= max(1, int(max_records or 1)):
        return dict(snap or {}), 0
    session = getattr(manager, "session", None)
    cap_snapshot_records = getattr(session, "cap_snapshot_records", None)
    if callable(cap_snapshot_records):
        dropped = int(cap_snapshot_records(max_records) or 0)
        if dropped > 0 and manager is not None:
            try:
                return dict(manager.snapshot() or {}), int(dropped)
            except Exception as e:
                _warn_nonfatal("POLYGON_WS_SNAPSHOT_CAP_RESNAPSHOT_FAILED", e, once_key="snapshot_cap_resnapshot")
                capped, fallback_dropped = _cap_snapshot_records(snap, max_records)
                return capped, max(int(dropped), int(fallback_dropped))
    return _cap_snapshot_records(snap, max_records)


def _emit_snapshot_cap_metric(dropped_count: int, snapshot_size: int, max_records: int) -> None:
    try:
        emit_counter(
            "stream_prices_snapshot_capped",
            int(max(0, dropped_count)),
            component="engine.jobs.stream_prices_polygon_ws",
            job=JOB_NAME,
            provider=PROVIDER_NAME,
            extra_tags={
                "snapshot_size": str(int(snapshot_size)),
                "snapshot_limit": str(int(max_records)),
            },
        )
    except Exception as e:
        _warn_nonfatal(
            "POLYGON_WS_SNAPSHOT_CAP_METRIC_FAILED",
            e,
            once_key="snapshot_cap_metric",
            dropped_count=int(max(0, dropped_count)),
        )


def _pause_snapshot_subscriptions(manager: Any, cap_state: Dict[str, Any], reason: str) -> None:
    if manager is None or bool(cap_state.get("subscriptions_paused")):
        return
    session = getattr(manager, "session", None)
    if session is None:
        return
    try:
        desired_fn = getattr(session, "desired_symbols", None)
        subscribed_fn = getattr(session, "subscribed_symbols", None)
        desired = set(desired_fn() if callable(desired_fn) else set())
        subscribed = set(subscribed_fn() if callable(subscribed_fn) else set())
        cap_state["paused_desired_symbols"] = sorted(str(symbol) for symbol in desired if str(symbol).strip())
        replace_desired = getattr(session, "replace_desired_symbols", None)
        if callable(replace_desired):
            replace_desired(set())
        unsubscribe = getattr(session, "unsubscribe", None)
        if subscribed and callable(unsubscribe):
            unsubscribe(sorted(subscribed))
        cap_state["subscriptions_paused"] = True
        log.warning(
            "polygon_ws snapshot cap backpressure paused subscriptions reason=%s streak=%s subscribed=%s",
            str(reason)[:240],
            int(cap_state.get("streak") or 0),
            int(len(subscribed)),
        )
    except Exception as e:
        _warn_nonfatal(
            "POLYGON_WS_SNAPSHOT_CAP_PAUSE_FAILED",
            e,
            once_key="snapshot_cap_pause",
            reason=str(reason)[:200],
        )


def _resume_snapshot_subscriptions(manager: Any, sym_to_poly: Dict[str, str], cap_state: Dict[str, Any]) -> None:
    if manager is None or not bool(cap_state.get("subscriptions_paused")):
        return
    desired = {str(symbol).strip() for symbol in (sym_to_poly or {}).values() if str(symbol).strip()}
    if not desired:
        desired = {str(symbol).strip() for symbol in cap_state.get("paused_desired_symbols") or [] if str(symbol).strip()}
    try:
        manager.ensure_subscriptions(desired)
        cap_state["subscriptions_paused"] = False
        cap_state["paused_desired_symbols"] = []
        log.info("polygon_ws snapshot cap backpressure resumed subscriptions count=%s", int(len(desired)))
    except Exception as e:
        _warn_nonfatal(
            "POLYGON_WS_SNAPSHOT_CAP_RESUME_FAILED",
            e,
            once_key="snapshot_cap_resume",
            desired_count=int(len(desired)),
        )


def _stale_detail(telemetry: Optional[Dict[str, Any]], ws_age_ms: int, last_error: Optional[str]) -> str:
    manager_state = str(_telemetry_field(telemetry, "manager_state", "") or "").strip().lower()
    connection_state = str(_telemetry_field(telemetry, "connection_state", "") or "").strip().lower()
    last_msg_ts_ms = int(_telemetry_field(telemetry, "last_msg_ts_ms", 0) or 0)
    base = (
        f"polygon_ws_no_data age_ms={int(ws_age_ms)} "
        f"manager_state={manager_state or 'unknown'} connection_state={connection_state or 'unknown'} "
        f"last_msg_ts_ms={int(last_msg_ts_ms)}"
    )
    if last_error:
        return f"{base} err={str(last_error)[:200]}"
    return base


def _load_committed_watermarks(sym_to_poly: Dict[str, str]) -> Dict[str, Dict[str, int]]:
    raw = str(meta_get(_WATERMARK_META_KEY, "") or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw) or {}
    except Exception as e:
        _warn_nonfatal(
            "POLYGON_WS_REPLAY_STATE_PARSE_FAILED",
            e,
            once_key="replay_state_parse",
            path=str(_WATERMARK_META_KEY),
        )
        return {}
    out: Dict[str, Dict[str, int]] = {}
    for sym in (sym_to_poly or {}).keys():
        row = payload.get(str(sym)) if isinstance(payload, dict) else None
        if isinstance(row, dict):
            out[str(sym)] = {
                "T": int(row.get("T") or 0),
                "Q": int(row.get("Q") or 0),
            }
    return out


def _save_committed_watermarks(current: Dict[str, Dict[str, int]], updates: Dict[str, Dict[str, int]]) -> Dict[str, Dict[str, int]]:
    merged: Dict[str, Dict[str, int]] = {str(sym): {"T": int(v.get("T") or 0), "Q": int(v.get("Q") or 0)} for sym, v in (current or {}).items()}
    changed = False
    for sym, mark in (updates or {}).items():
        cur = merged.setdefault(str(sym), {"T": 0, "Q": 0})
        for channel in ("T", "Q"):
            next_ts = int((mark or {}).get(channel) or 0)
            if next_ts > int(cur.get(channel) or 0):
                cur[channel] = next_ts
                changed = True
    if changed:
        meta_set(_WATERMARK_META_KEY, json.dumps(merged, separators=(",", ":"), sort_keys=True))
    return merged


def _event_watermarks(events: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}
    for event in events or []:
        sym = str(event.get("symbol") or "").strip()
        et = str(event.get("event_type") or "").strip().upper()
        ts_ms = int(event.get("event_ts_ms") or event.get("timestamp") or 0)
        if not sym or et not in {"T", "Q"} or ts_ms <= 0:
            continue
        row = out.setdefault(sym, {})
        row[et] = max(int(row.get(et) or 0), int(ts_ms))
    return out

def main():
    source_manager = get_manager()
    if not source_manager.is_job_enabled(JOB_NAME, default=True):
        source_manager.record_job_status(JOB_NAME, ok=True, message="polygon ws disabled by data source control plane")
        return

    if os.environ.get("ENGINE_SUPERVISED") != "1":
        try:
            log.warning(
                "stream_prices_polygon_ws starting without ENGINE_SUPERVISED=1; continuing for startup diagnostics"
            )
        except Exception:
            print("WARN: stream_prices_polygon_ws starting without ENGINE_SUPERVISED=1; continuing", flush=True)

    if websocket is None:
        log.error("websocket-client missing/unimportable: %r", _WEBSOCKET_IMPORT_ERROR)
        log.error("Polygon WS cannot start without websocket-client library")
        raise SystemExit(3)

    init_db()

    try:
        con = connect(readonly=True)
        try:
            con.execute("SELECT 1 FROM symbols LIMIT 1").fetchone()
        finally:
            con.close()
    except Exception as e:
        log.error("failed to verify symbols table before polygon stream boot: %s", repr(e))
        raise

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("POLYGON_WS_JOB_LOCK_RELEASE_FAILED", e, once_key="job_lock_release_bootstrap")
        if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
            raise SystemExit(2)

    endpoint = os.environ.get("POLYGON_WS_ENDPOINT", "wss://socket.polygon.io/stocks").strip()
    sub_trades = os.environ.get("POLYGON_WS_SUBSCRIBE_TRADES", "1") == "1"
    sub_quotes = os.environ.get("POLYGON_WS_SUBSCRIBE_QUOTES", "1") == "1"

    flush_ms = int(os.environ.get("STREAM_PRICES_FLUSH_MS", "250"))
    hb_s = float(os.environ.get("STREAM_PRICES_HEARTBEAT_S", "2.0"))
    min_write_ms = int(os.environ.get("STREAM_PRICES_MIN_WRITE_INTERVAL_MS", str(flush_ms)))

    from engine.data.provider_sessions.polygon_ws_session import PolygonWSSession
    from engine.data.provider_sessions.session_manager import ProviderSessionManager

    manager: Optional[ProviderSessionManager] = None
    last_build_error: Optional[str] = None
    bootstrap_attempts = 0
    bootstrap_next_retry_s = 0.0
    bootstrap_backoff_s = max(0.25, float(WS_RECONNECT_BASE_S))
    bootstrap_exhausted = False

    def _maybe_build_manager() -> Optional[ProviderSessionManager]:
        nonlocal last_build_error

        if websocket is None:
            last_build_error = "websocket_client_missing"
            return None

        api_key = get_data_credential("POLYGON_API_KEY")
        if not api_key:
            last_build_error = "POLYGON_API_KEY_missing"
            log.error("POLYGON_API_KEY not set — price daemon cannot start")
            return None

        try:
            last_build_error = None
            session = PolygonWSSession(
                api_key=api_key,
                endpoint=endpoint,
                subscribe_trades=sub_trades,
                subscribe_quotes=sub_quotes,
            )
            return ProviderSessionManager(
                session,
                provider_name=PROVIDER_NAME,
                heartbeat_interval_s=max(0.25, float(hb_s)),
                dead_after_ms=int(WS_DEAD_AFTER_MS),
                reconnect_base_s=float(WS_RECONNECT_BASE_S),
                reconnect_max_s=float(WS_RECONNECT_MAX_S),
                max_reconnect_attempts=int(WS_MAX_RECONNECT_ATTEMPTS),
                startup_grace_ms=60000,
            )
        except Exception as e:
            last_build_error = (repr(e) or "ws_init_failed")[:400]
            try:
                log.warning("failed to initialize polygon provider session: %s", last_build_error)
            except Exception as log_err:
                _warn_nonfatal("POLYGON_WS_PROVIDER_INIT_LOG_FAILED", log_err, once_key="provider_init_log", error_detail=last_build_error)
            return None

    def _record_bootstrap_failure(now_s: float, reason: Optional[str]) -> None:
        nonlocal bootstrap_attempts, bootstrap_next_retry_s, bootstrap_backoff_s, bootstrap_exhausted
        bootstrap_attempts += 1
        sleep_s = min(float(WS_RECONNECT_MAX_S), max(float(WS_RECONNECT_BASE_S), float(bootstrap_backoff_s)))
        bootstrap_next_retry_s = float(now_s) + float(sleep_s)
        bootstrap_backoff_s = min(float(WS_RECONNECT_MAX_S), max(float(WS_RECONNECT_BASE_S), float(bootstrap_backoff_s) * 2.0))
        bootstrap_exhausted = bool(int(WS_MAX_RECONNECT_ATTEMPTS) > 0 and int(bootstrap_attempts) >= int(WS_MAX_RECONNECT_ATTEMPTS))
        try:
            log.warning(
                "polygon_ws reconnect_attempt attempt=%s max_attempts=%s sleep_s=%.2f reason=%s",
                int(bootstrap_attempts),
                int(WS_MAX_RECONNECT_ATTEMPTS),
                float(sleep_s),
                str(reason or last_build_error or "polygon_session_manager_init_failed")[:400],
            )
        except Exception as e:
            _warn_nonfatal("POLYGON_WS_RECONNECT_ATTEMPT_LOG_FAILED", e, once_key="reconnect_attempt_log")
        if bootstrap_exhausted:
            try:
                log.error(
                    "polygon_ws reconnect_cap_reached attempts=%s reason=%s",
                    int(bootstrap_attempts),
                    str(reason or last_build_error or "polygon_session_manager_init_failed")[:400],
                )
            except Exception as e:
                _warn_nonfatal("POLYGON_WS_RECONNECT_CAP_LOG_FAILED", e, once_key="reconnect_cap_log")

    def _reset_bootstrap_backoff() -> None:
        nonlocal bootstrap_attempts, bootstrap_next_retry_s, bootstrap_backoff_s, bootstrap_exhausted
        bootstrap_attempts = 0
        bootstrap_next_retry_s = 0.0
        bootstrap_backoff_s = max(0.25, float(WS_RECONNECT_BASE_S))
        bootstrap_exhausted = False

    manager = _maybe_build_manager()
    if manager is None:
        _record_bootstrap_failure(time.time(), last_build_error or "polygon_session_manager_init_failed")
    else:
        _reset_bootstrap_backoff()

    last_hb = 0.0
    last_provider_health = 0.0
    last_sym_reload_ms = 0
    startup_started_ms = _now_ms()
    flush_error_streak = 0
    flush_retry_backoff_s = max(0.5, float(WS_RECONNECT_BASE_S))
    last_data_ts_ms = 0
    last_data_activity_ts_ms = 0
    last_pause_state: Optional[bool] = None
    snapshot_cap_state: Dict[str, Any] = {"streak": 0, "subscriptions_paused": False, "paused_desired_symbols": []}
    last_stale_log_ts_ms = 0

    sym_to_poly: Dict[str, str] = _load_symbol_map() or {"SPY": "SPY"}
    last_write_by_symbol: Dict[str, int] = {}
    last_flush_error: Optional[str] = None
    committed_watermarks = _load_committed_watermarks(sym_to_poly)
    last_replay_disconnect_ts_ms = 0

    if manager is not None:
        try:
            manager.ensure_subscriptions(set(sym_to_poly.values()))
        except Exception as e:
            last_build_error = (repr(e) or "subscription_init_failed")[:400]
            log.warning("initial polygon subscription sync failed: %s", last_build_error)

    try:
        while not _STOP_EVENT.is_set():
            if not source_manager.is_job_enabled(JOB_NAME, default=True):
                source_manager.record_job_status(JOB_NAME, ok=True, message="polygon ws disabled by data source control plane")
                break
            now_s = time.time()
            now_ms = _now_ms()

            if (now_ms - last_sym_reload_ms) >= 30_000 or not sym_to_poly:
                new_map = _load_symbol_map()
                if new_map:
                    sym_to_poly = new_map

                if not sym_to_poly:
                    sym_to_poly = {"SPY": "SPY"}

                last_write_by_symbol = {
                    str(sym): int(ts)
                    for sym, ts in (last_write_by_symbol or {}).items()
                    if str(sym) in sym_to_poly
                }
                committed_watermarks = {
                    str(sym): dict(mark)
                    for sym, mark in (committed_watermarks or {}).items()
                    if str(sym) in sym_to_poly
                }

                if manager is not None and not bool(snapshot_cap_state.get("subscriptions_paused")):
                    try:
                        manager.ensure_subscriptions(set(sym_to_poly.values()))
                    except Exception as e:
                        last_build_error = (repr(e) or "subscription_refresh_failed")[:400]
                        log.warning("polygon subscription refresh failed: %s", last_build_error)

                last_sym_reload_ms = now_ms

            if manager is None and (not bootstrap_exhausted) and now_s >= float(bootstrap_next_retry_s or 0.0):
                candidate = _maybe_build_manager()
                if candidate is None:
                    _record_bootstrap_failure(now_s, last_build_error or "polygon_session_manager_init_failed")
                else:
                    manager = candidate
                    _reset_bootstrap_backoff()
                    startup_started_ms = int(now_ms)
                    try:
                        manager.ensure_subscriptions(set(sym_to_poly.values()))
                    except Exception as e:
                        last_build_error = (repr(e) or "subscription_init_failed")[:400]
                        log.warning("initial polygon subscription sync failed: %s", last_build_error)

            telemetry: Dict[str, Any] = {}
            if manager is not None:
                try:
                    telemetry = dict(manager.provider_telemetry() or {})
                except Exception as e:
                    last_build_error = (repr(e) or "provider_telemetry_error")[:400]
                    telemetry = {}

            ws_age_ms = int(_telemetry_field(telemetry, "last_msg_age_ms", 10**9) or 10**9)
            ws_error = (
                str(_telemetry_field(telemetry, "last_error", "") or "").strip()
                or (last_build_error or "")
            )
            connection_state = str(_telemetry_field(telemetry, "connection_state", "") or "").strip().lower()
            manager_state = str(_telemetry_field(telemetry, "manager_state", "") or "").strip().lower()
            writes_paused = _writes_paused(telemetry)
            if manager is None:
                writes_paused = True
                if bootstrap_exhausted:
                    connection_state = "disconnected"
                    manager_state = "failed"
                else:
                    connection_state = "reconnecting"
                    manager_state = "reconnecting"

            if last_pause_state is None or bool(last_pause_state) != bool(writes_paused):
                try:
                    if writes_paused:
                        log.warning(
                            "polygon_ws writes_paused state=%s connection_state=%s age_ms=%s error=%s",
                            manager_state or "unknown",
                            connection_state or "unknown",
                            int(ws_age_ms),
                            ws_error or last_flush_error or "",
                        )
                    else:
                        log.info(
                            "polygon_ws writes_resumed state=%s connection_state=%s last_msg_ts_ms=%s",
                            manager_state or "unknown",
                            connection_state or "unknown",
                            int(_telemetry_field(telemetry, "last_msg_ts_ms", 0) or 0),
                        )
                except Exception as e:
                    _warn_nonfatal("POLYGON_WS_WRITE_PAUSE_STATE_LOG_FAILED", e, once_key="write_pause_state_log")
                last_pause_state = bool(writes_paused)

            if (now_s - last_hb) >= hb_s:
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(
                    JOB_NAME,
                    OWNER,
                    PID,
                    extra_json=json.dumps(
                        {
                            "provider": PROVIDER_NAME,
                            "ws_age_ms": int(ws_age_ms),
                            "n_symbols": int(len(sym_to_poly)),
                            "ws_ready": bool(manager is not None),
                            "ws_error": (ws_error or last_build_error),
                            "connection_state": connection_state,
                            "manager_state": manager_state,
                            "writes_paused": bool(writes_paused),
                            "last_message_ts_ms": int(_telemetry_field(telemetry, "last_msg_ts_ms", 0) or 0),
                            "last_flush_error": last_flush_error,
                            "telemetry": (telemetry or None),
                        },
                        separators=(",", ":"),
                    ),
                )
                last_hb = now_s

            if (now_s - last_provider_health) >= float(PROVIDER_HEALTH_EVERY_S):
                provider_ok = bool(manager is not None and manager.ok() and not writes_paused and not last_flush_error)
                err = last_flush_error
                if provider_ok:
                    err = None
                elif manager is None:
                    err = (err or ws_error or last_build_error or "ws_not_ready")[:400]
                elif ws_error:
                    err = (err or ws_error or last_build_error or "ws_error")[:400]
                elif ws_age_ms >= 10**8:
                    err = (err or ws_error or last_build_error or "waiting_for_first_message")[:400]
                else:
                    err = (
                        err
                        or ws_error
                        or last_build_error
                        or f"state={manager_state or 'unknown'} connection_state={connection_state or 'unknown'} age_ms={int(ws_age_ms)}"
                    )[:400]

                _put_provider_health(
                    PROVIDER_NAME,
                    ok=provider_ok,
                    latency_ms=int(ws_age_ms),
                    n_symbols=int(len(sym_to_poly)),
                    error=err,
                )

                if provider_ok:
                    try:
                        meta_set("price_provider_active", PROVIDER_NAME, best_effort=True)
                    except Exception as e:
                        log.warning("failed to set active price provider meta: %s", repr(e))
                else:
                    try:
                        meta_set("price_provider_active", "", best_effort=True)
                    except Exception as e:
                        log.warning("failed to clear active price provider meta: %s", repr(e))

                last_provider_health = now_s

            raw_event_counts = {"raw": 0, "quotes": 0, "prices": 0}
            replay_counts = {"raw": 0, "quotes": 0, "prices": 0}
            snap: Dict[str, Dict[str, Any]] = {}
            disconnect_ts_ms = int(_telemetry_field(telemetry, "last_disconnect_ts_ms", 0) or 0)

            if manager is not None and not writes_paused:
                polygon_session = manager.session if isinstance(manager.session, PolygonWSSession) else None
                replay_needed = bool(disconnect_ts_ms > int(last_replay_disconnect_ts_ms or 0))
                if replay_needed and sym_to_poly and polygon_session is not None:
                    try:
                        replay_events = polygon_session.fetch_replay_events(
                            dict(sym_to_poly),
                            watermarks=dict(committed_watermarks or {}),
                            until_ts_ms=int(now_ms),
                        ) or []
                        if replay_events:
                            def _write_replay(con):
                                return publish_price_events(
                                    replay_events,
                                    con=con,
                                    write_prices=True,
                                    write_quotes=True,
                                    write_raw=True,
                                    emit_telemetry=False,
                                    component="engine.jobs.stream_prices_polygon_ws",
                                    job=JOB_NAME,
                                    default_provider=PROVIDER_NAME,
                                )

                            replay_counts = run_write_txn(_write_replay)
                            committed_watermarks = _save_committed_watermarks(
                                committed_watermarks,
                                _event_watermarks(replay_events),
                            )
                            polygon_session.merge_replay_events(replay_events)
                            last_data_ts_ms = max(
                                int(last_data_ts_ms or 0),
                                max(int(ev.get("event_ts_ms") or ev.get("timestamp") or 0) for ev in replay_events),
                            )
                            last_data_activity_ts_ms = int(now_ms)
                            try:
                                log.info(
                                    "polygon_ws reconnect_replay_success events=%s disconnect_ts_ms=%s",
                                    int(replay_counts.get("raw") or len(replay_events)),
                                    int(disconnect_ts_ms),
                                )
                            except Exception as e:
                                _warn_nonfatal("POLYGON_WS_REPLAY_SUCCESS_LOG_FAILED", e, once_key="replay_success_log")
                        last_replay_disconnect_ts_ms = int(disconnect_ts_ms)
                    except Exception as e:
                        last_build_error = (repr(e) or "replay_fill_error")[:400]
                        try:
                            log.warning("polygon replay fill failed: %s", last_build_error)
                        except Exception as log_err:
                            _warn_nonfatal("POLYGON_WS_REPLAY_FILL_LOG_FAILED", log_err, once_key="replay_fill_log", error_detail=last_build_error)

                try:
                    live_events = (
                        polygon_session.drain_pending_events(limit=int(STREAM_EVENT_BATCH))
                        if polygon_session is not None
                        else []
                    )
                except Exception as e:
                    last_build_error = (repr(e) or "pending_event_drain_error")[:400]
                    live_events = []

                if live_events:
                    try:
                        def _write_live(con):
                            return publish_price_events(
                                live_events,
                                con=con,
                                write_prices=True,
                                write_quotes=True,
                                write_raw=True,
                                emit_telemetry=False,
                                component="engine.jobs.stream_prices_polygon_ws",
                                job=JOB_NAME,
                                default_provider=PROVIDER_NAME,
                            )

                        raw_event_counts = run_write_txn(_write_live)
                        committed_watermarks = _save_committed_watermarks(
                            committed_watermarks,
                            _event_watermarks(live_events),
                        )
                        last_data_ts_ms = max(
                            int(last_data_ts_ms or 0),
                            max(int(ev.get("event_ts_ms") or ev.get("timestamp") or 0) for ev in live_events),
                        )
                        last_data_activity_ts_ms = int(now_ms)
                    except Exception as e:
                        try:
                            if polygon_session is not None:
                                polygon_session.requeue_pending_events(live_events)
                        except Exception as requeue_err:
                            _warn_nonfatal(
                                "POLYGON_WS_REQUEUE_PENDING_EVENTS_FAILED",
                                requeue_err,
                                once_key="requeue_pending_events",
                                event_count=int(len(live_events)),
                            )
                        last_build_error = (repr(e) or "live_event_flush_error")[:400]
                        raise RuntimeError(last_build_error)

                try:
                    snap = manager.snapshot()
                except Exception as e:
                    last_build_error = (repr(e) or "snapshot_error")[:400]
                    log.warning("polygon snapshot read failed: %s", last_build_error)
                    snap = {}

            if not snap and manager is not None and not writes_paused and sym_to_poly:
                since_ts_ms = max(
                    int(last_data_ts_ms or 0),
                    int(disconnect_ts_ms or 0),
                )
                try:
                    gap_rows = manager.session.perform_gap_fill(sorted(set(sym_to_poly.values())), int(since_ts_ms)) or {}
                    if gap_rows:
                        snap = gap_rows
                        manager.session.merge_snapshot(gap_rows)
                except Exception as e:
                    last_build_error = (repr(e) or "rest_gap_fill_error")[:400]
                    try:
                        log.warning("polygon REST snapshot fallback failed: %s", last_build_error)
                    except Exception as log_err:
                        _warn_nonfatal("POLYGON_WS_GAP_FILL_LOG_FAILED", log_err, once_key="gap_fill_log", error_detail=last_build_error)

            if snap:
                snapshot_size_before_cap = int(len(snap or {}))
                snap, dropped_count = _cap_manager_snapshot(manager, snap, MAX_SNAPSHOT_RECORDS)
                if dropped_count > 0:
                    snapshot_cap_state["streak"] = int(snapshot_cap_state.get("streak") or 0) + 1
                    _emit_snapshot_cap_metric(
                        int(dropped_count),
                        snapshot_size=int(snapshot_size_before_cap),
                        max_records=int(MAX_SNAPSHOT_RECORDS),
                    )
                else:
                    snapshot_cap_state["streak"] = 0
            else:
                snapshot_cap_state["streak"] = 0

            if writes_paused or (not snap and int(raw_event_counts.get("raw") or 0) <= 0 and int(replay_counts.get("raw") or 0) <= 0):
                session_failure = {}
                try:
                    session_failure = _safe_json_loads(
                        str(meta_get(f"provider_session_{PROVIDER_NAME}_last_failure", "") or "")
                    ) or {}
                except Exception:
                    session_failure = {}

                startup_err = (
                    last_flush_error
                    or ws_error
                    or str((session_failure or {}).get("error") or "")
                    or last_build_error
                    or f"no_market_data_after_startup:{int(now_ms - startup_started_ms)}ms"
                )
                no_data_since_ms = int(now_ms - int(last_data_activity_ts_ms or startup_started_ms))
                lifecycle_state, lifecycle_detail = _classify_startup_wait_detail(startup_err, telemetry)
                if no_data_since_ms >= int(STREAM_DEGRADED_AFTER_MS):
                    lifecycle_state = DEGRADED
                    lifecycle_detail = _stale_detail(telemetry, int(ws_age_ms), startup_err)[:500]
                try:
                    set_state(lifecycle_state, lifecycle_detail)
                except Exception as e:
                    _warn_nonfatal("POLYGON_WS_SET_STATE_FAILED", e, once_key=f"set_state:{lifecycle_state}", state=str(lifecycle_state), detail=str(lifecycle_detail)[:200])

                if no_data_since_ms >= int(STREAM_DEGRADED_AFTER_MS):
                    timeout_detail = _stale_detail(telemetry, int(ws_age_ms), startup_err)[:500]
                    try:
                        set_state(DEGRADED, timeout_detail[:500])
                    except Exception as e:
                        _warn_nonfatal("POLYGON_WS_SET_DEGRADED_STATE_FAILED", e, once_key="set_state:degraded", detail=str(timeout_detail)[:200])
                    if (int(now_ms) - int(last_stale_log_ts_ms or 0)) >= max(int(STREAM_DEGRADED_AFTER_MS), 15000):
                        try:
                            log.warning("polygon_ws stale_detected detail=%s", timeout_detail[:400])
                        except Exception as e:
                            _warn_nonfatal("POLYGON_WS_STALE_LOG_FAILED", e, once_key="stale_log")
                        last_stale_log_ts_ms = int(now_ms)
                    if _STOP_EVENT.wait(timeout=min(float(WS_RESTART_COOLDOWN_S), 5.0)):
                        break
                    continue

                if _STOP_EVENT.wait(timeout=0.05):
                    break
                continue

            try:
                newest_ts_ms = max(int((rec or {}).get("ts_ms") or 0) for rec in (snap or {}).values()) if snap else int(now_ms)
                snapshot_should_write_prices = bool(
                    snap and int(raw_event_counts.get("raw") or 0) <= 0 and int(replay_counts.get("raw") or 0) <= 0
                )

                def _write_flush(con):
                    return _flush_to_db(
                        con,
                        ts_ms=now_ms,
                        sym_to_poly=sym_to_poly,
                        poly_snapshot=snap,
                        min_write_interval_ms=min_write_ms,
                        last_write_by_symbol=last_write_by_symbol,
                        write_price_events=bool(snapshot_should_write_prices),
                    )

                n_raw, n_q, n_px, watermark_updates = run_write_txn(_write_flush)
                if watermark_updates:
                    committed_watermarks = _save_committed_watermarks(committed_watermarks, watermark_updates)
                if snapshot_should_write_prices or snap:
                    last_data_ts_ms = max(int(last_data_ts_ms or 0), int(newest_ts_ms or now_ms))
                    last_data_activity_ts_ms = int(now_ms)
                try:
                    meta_set("price_provider_active", PROVIDER_NAME, best_effort=True)
                    did = meta_set_if_missing("first_price_ts_ms", str(int(newest_ts_ms or now_ms)))
                    set_state(LIVE, "first_market_data_tick" if did else "polygon_ws_recovered")
                except Exception as e:
                    log.warning("failed to update lifecycle state from polygon flush: %s", repr(e))
                try:
                    emit_gauge(
                        "queue_depth",
                        len(snap or {}),
                        component="engine.jobs.stream_prices_polygon_ws",
                        job=JOB_NAME,
                        provider=PROVIDER_NAME,
                        extra_tags={"queue_name": "polygon_snapshot"},
                    )
                    emit_counter(
                        "market_data_event",
                        int((raw_event_counts.get("quotes") or 0) + (replay_counts.get("quotes") or 0) + (n_q or n_px or 0)),
                        component="engine.jobs.stream_prices_polygon_ws",
                        job=JOB_NAME,
                    )
                    emit_counter(
                        "order_throughput",
                        int((raw_event_counts.get("prices") or 0) + (replay_counts.get("prices") or 0) + (n_px or 0)),
                        component="engine.jobs.stream_prices_polygon_ws",
                        job=JOB_NAME,
                        provider=PROVIDER_NAME,
                        extra_tags={"throughput_type": "price_rows"},
                    )
                    if snap:
                        latency_ms = max(0, int(now_ms) - int(newest_ts_ms or now_ms))
                        emit_timing(
                            "market_data_latency_ms",
                            latency_ms,
                            component="engine.jobs.stream_prices_polygon_ws",
                            job=JOB_NAME,
                            provider=PROVIDER_NAME,
                        )
                        trace_event(
                            "market_data_event",
                            component="engine.jobs.stream_prices_polygon_ws",
                            entity_type="provider",
                            entity_id=PROVIDER_NAME,
                            payload={
                                "n_raw": int((raw_event_counts.get("raw") or 0) + (replay_counts.get("raw") or 0) + (n_raw or 0)),
                                "n_quotes": int((raw_event_counts.get("quotes") or 0) + (replay_counts.get("quotes") or 0) + (n_q or 0)),
                                "n_prices": int((raw_event_counts.get("prices") or 0) + (replay_counts.get("prices") or 0) + (n_px or 0)),
                                "latency_ms": int(latency_ms),
                            },
                            job=JOB_NAME,
                            provider=PROVIDER_NAME,
                        )
                except Exception as e:
                    log.warning("failed to emit polygon flush telemetry: %s", repr(e))
                flush_error_streak = 0
                flush_retry_backoff_s = max(0.5, float(WS_RECONNECT_BASE_S))
                last_flush_error = None
                snapshot_cap_state["streak"] = 0
                _resume_snapshot_subscriptions(manager, sym_to_poly, snapshot_cap_state)

            except Exception as e:
                flush_error_streak += 1
                last_flush_error = (repr(e) or "flush_error")[:400]

                try:
                    log.warning("polygon flush failed: %s", last_flush_error)
                except Exception as log_err:
                    _warn_nonfatal("POLYGON_WS_FLUSH_LOG_FAILED", log_err, once_key="flush_log", error_detail=last_flush_error)

                if int(snapshot_cap_state.get("streak") or 0) >= int(SNAPSHOT_CAP_PAUSE_STREAK):
                    _pause_snapshot_subscriptions(manager, snapshot_cap_state, last_flush_error or "flush_error")

                if flush_error_streak >= 3:
                    flush_backoff_s = min(
                        float(WS_RECONNECT_MAX_S),
                        max(float(WS_RECONNECT_BASE_S), float(flush_retry_backoff_s)),
                    )
                    try:
                        set_state(
                            DEGRADED,
                            f"polygon_ws_flush_error streak={int(flush_error_streak)} err={str(last_flush_error)[:240]}",
                        )
                    except Exception as state_err:
                        _warn_nonfatal(
                            "POLYGON_WS_SET_FLUSH_DEGRADED_STATE_FAILED",
                            state_err,
                            once_key="set_flush_degraded_state",
                            detail=str(last_flush_error)[:200],
                        )
                    try:
                        log.warning(
                            "polygon_ws flush_backoff streak=%s sleep_s=%.2f err=%s",
                            int(flush_error_streak),
                            float(flush_backoff_s),
                            str(last_flush_error)[:240],
                        )
                    except Exception as backoff_log_err:
                        _warn_nonfatal(
                            "POLYGON_WS_FLUSH_BACKOFF_LOG_FAILED",
                            backoff_log_err,
                            once_key="flush_backoff_log",
                        )
                    flush_retry_backoff_s = min(
                        float(WS_RECONNECT_MAX_S),
                        max(float(WS_RECONNECT_BASE_S), float(flush_retry_backoff_s) * 2.0),
                    )
                    if _STOP_EVENT.wait(timeout=max(0.5, float(flush_backoff_s))):
                        break
                    continue

                if _STOP_EVENT.wait(timeout=0.5):
                    break
                continue

            if _STOP_EVENT.wait(timeout=max(0.05, float(flush_ms) / 1000.0)):
                break
    finally:
        _STOP_EVENT.set()
        try:
            if manager is not None:
                manager.close()
        except Exception as e:
            _warn_nonfatal("POLYGON_WS_MANAGER_CLOSE_FAILED", e, once_key="manager_close")

        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("POLYGON_WS_JOB_LOCK_RELEASE_FAILED", e, once_key="job_lock_release_finally")


if __name__ == "__main__":
    main()
