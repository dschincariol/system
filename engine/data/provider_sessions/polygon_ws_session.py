"""
FILE: polygon_ws_session.py

Provider session management module for `polygon_ws_session`.
"""

import json
import logging
import threading
import os
from collections import deque
from typing import Any, Dict, Iterable, List, Optional

from engine.data.live_prices.polygon_live import PolygonPriceProvider
from engine.runtime.failure_diagnostics import log_failure

from .base_session import BaseProviderSession, now_ms


log = logging.getLogger("polygon_ws_session")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: Optional[str] = None, **extra: Any) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        log,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component="polygon_ws_session",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)

try:
    import websocket  # type: ignore
except Exception:
    websocket = None


class PolygonWSSession(BaseProviderSession):
    provider_name = "polygon_ws"

    def __init__(self, api_key: str, endpoint: str, subscribe_trades: bool, subscribe_quotes: bool) -> None:
        super().__init__("polygon_ws")
        if websocket is None:
            raise RuntimeError("websocket-client is not installed")
        self.api_key = str(api_key or "").strip()
        self.endpoint = str(endpoint or "").strip()
        self.subscribe_trades = bool(subscribe_trades)
        self.subscribe_quotes = bool(subscribe_quotes)
        self._ws: Optional[websocket.WebSocketApp] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._stop = False
        self._opened = threading.Event()
        self._auth_event = threading.Event()
        self._last: Dict[str, Dict[str, Any]] = {}
        self._last_event_ts_by_stream: Dict[str, int] = {}
        self._pending_events = deque()
        self._max_pending_events = int(os.environ.get("POLYGON_WS_PENDING_EVENT_CAP", "250000"))
        self._rest_provider: Optional[PolygonPriceProvider] = None
        self._last_latency_ms = 0
        self._ping_interval_s = float(os.environ.get("POLYGON_WS_PING_INTERVAL_S", "20.0"))
        self._ping_timeout_s = float(os.environ.get("POLYGON_WS_PING_TIMEOUT_S", "10.0"))
        self._pong_stale_after_ms = int(
            os.environ.get(
                "POLYGON_WS_PONG_STALE_AFTER_MS",
                str(int((self._ping_interval_s + self._ping_timeout_s + 5.0) * 1000.0)),
            )
        )
        self._last_ping_ts_ms = 0
        self._last_pong_ts_ms = 0
        self._last_transport_ts_ms = 0
        self.set_capability("streaming", True)
        self.set_capability("polling", False)
        self.set_capability("gap_fill", True)
        self.set_capability("historical_catchup", "rest_snapshot")
        self.set_capability("authentication", "api_key")

        self.set_capability("supports_quotes", bool(self.subscribe_quotes))
        self.set_capability("supports_trades", bool(self.subscribe_trades))
        self.set_capability("supports_snapshot", True)
        self.set_capability("supports_historical_bars", True)
        self.set_capability("supports_tick_stream", True)

        self.set_capability(
            "rate_limit_per_min",
            int(os.environ.get("POLYGON_REST_RATE_LIMIT_PER_MIN", "240")),
        )

    def connect(self) -> None:
        if not self.api_key:
            raise RuntimeError("POLYGON_API_KEY_missing")
        open_timeout_s = float(os.environ.get("POLYGON_WS_OPEN_TIMEOUT_S", "20.0"))
        self._stop = False
        self._opened.clear()
        self._auth_event.clear()
        self._ws = websocket.WebSocketApp(
            self.endpoint,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_ping=self._on_ping,
            on_pong=self._on_pong,
        )
        self._ws_thread = threading.Thread(target=self._run_forever, name="polygon_ws_session", daemon=True)
        self._ws_thread.start()
        if not self._opened.wait(timeout=open_timeout_s):
            self.note_disconnected("polygon_ws_open_timeout")
            self.close()
            raise RuntimeError("polygon_ws_open_timeout")
        self.note_connected()

    def authenticate(self) -> None:
        if not self.api_key:
            raise RuntimeError("POLYGON_API_KEY_missing")
        if not self._ws:
            raise RuntimeError("polygon_ws_not_ready")
        auth_timeout_s = float(os.environ.get("POLYGON_WS_AUTH_TIMEOUT_S", "20.0"))
        self._auth_event.clear()
        # Polygon auth status arrives asynchronously over the same socket, so we
        # gate startup on the auth event instead of assuming send() means success.
        self._ws.send(json.dumps({"action": "auth", "params": self.api_key}))
        if not self._auth_event.wait(timeout=auth_timeout_s):
            last_error = str((self.telemetry_snapshot() or {}).get("last_error") or "").strip()
            if last_error:
                self.note_disconnected(last_error)
                raise RuntimeError(last_error)
            self.note_disconnected("polygon_ws_auth_timeout_no_status")
            raise RuntimeError("polygon_ws_auth_timeout_no_status")
        telemetry = self.telemetry_snapshot() or {}
        if not bool(telemetry.get("authenticated")):
            last_error = str(telemetry.get("last_error") or "").strip() or "polygon_ws_auth_failed"
            self.note_disconnected(last_error)
            raise RuntimeError(last_error)
        self.note_authenticated()

    def detect_capabilities(self) -> Dict[str, Any]:
        self.set_capability("supports_quotes", bool(self.subscribe_quotes))
        self.set_capability("supports_trades", bool(self.subscribe_trades))
        self.set_capability("supports_snapshot", True)
        return self.telemetry_snapshot().get("capabilities") or {}

    def subscribe(self, symbols: Iterable[str]) -> None:
        clean = {str(x).strip() for x in (symbols or []) if str(x).strip()}
        if not clean:
            return
        # Polygon uses stream-specific channel prefixes; the base session only
        # tracks bare symbols, while this adapter handles the transport encoding.
        params: List[str] = []
        if self.subscribe_trades:
            params.extend([f"T.{t}" for t in sorted(clean)])
        if self.subscribe_quotes:
            params.extend([f"Q.{t}" for t in sorted(clean)])
        ws = self._ws
        if not params or ws is None:
            return
        if (not self._opened.is_set()) or (not self._auth_event.is_set()):
            return
        sock = getattr(ws, "sock", None)
        if sock is None or not bool(getattr(sock, "connected", False)):
            return
        try:
            ws.send(json.dumps({"action": "subscribe", "params": ",".join(params)}))
        except Exception as e:
            self.note_error(e)
            _warn_nonfatal(
                "polygon_ws_session_subscribe_failed",
                "POLYGON_WS_SESSION_SUBSCRIBE_FAILED",
                e,
                warn_key="subscribe",
                symbol_count=len(clean),
            )
            return
        self.update_subscribed_symbols(self.subscribed_symbols() | clean)

    def unsubscribe(self, symbols: Iterable[str]) -> None:
        clean = {str(x).strip() for x in (symbols or []) if str(x).strip()}
        ws = self._ws
        if not clean or ws is None:
            return
        params: List[str] = []
        if self.subscribe_trades:
            params.extend([f"T.{t}" for t in sorted(clean)])
        if self.subscribe_quotes:
            params.extend([f"Q.{t}" for t in sorted(clean)])
        sock = getattr(ws, "sock", None)
        if params and self._opened.is_set() and self._auth_event.is_set() and sock is not None and bool(getattr(sock, "connected", False)):
            try:
                ws.send(json.dumps({"action": "unsubscribe", "params": ",".join(params)}))
            except Exception as e:
                self.note_error(e)
        self.update_subscribed_symbols(self.subscribed_symbols() - clean)
        with self._lock:
            for symbol in clean:
                self._last.pop(str(symbol), None)
                self._last_event_ts_by_stream.pop(f"T:{symbol}", None)
                self._last_event_ts_by_stream.pop(f"Q:{symbol}", None)
                self._last_symbol_event_key.pop(f"T:{symbol}", None)
                self._last_symbol_event_key.pop(f"Q:{symbol}", None)

    def heartbeat(self) -> Dict[str, Any]:
        if self._ws is None or self._stop:
            raise RuntimeError("polygon_ws_closed")
        if self._ws_thread is None or not self._ws_thread.is_alive():
            self.note_disconnected("polygon_ws_thread_dead")
            raise RuntimeError("polygon_ws_thread_dead")
        now = now_ms()
        ws = self._ws
        if ws is not None and (now - int(self._last_ping_ts_ms or 0)) >= int(max(1.0, self._ping_interval_s) * 1000.0):
            try:
                if getattr(ws, "sock", None) is not None and bool(getattr(ws.sock, "connected", False)):
                    ws.sock.ping("hb")
                    self._last_ping_ts_ms = now
            except Exception as e:
                self.note_error(e)
        last_pong_ts_ms = int(self._last_pong_ts_ms or 0)
        if last_pong_ts_ms > 0 and (now - last_pong_ts_ms) > self._pong_stale_after_ms:
            err = f"polygon_ws_pong_stale age_ms={int(now - last_pong_ts_ms)}"
            self.note_stale(err)
            raise RuntimeError(err)
        return super().heartbeat()

    def close(self) -> None:
        self._stop = True
        ws = self._ws
        thread = self._ws_thread
        try:
            if ws:
                ws.close()
        finally:
            self._ws = None
            if thread is not None and thread.is_alive() and thread is not threading.current_thread():
                try:
                    thread.join(timeout=5.0)
                except Exception:
                    log.exception("polygon_ws_thread_join_failed")
            self._ws_thread = None
            self._opened.clear()
            self.note_disconnected("closed")
            self.update_subscribed_symbols(set())

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return {k: dict(v) for k, v in self._last.items()}

    def drain_pending_events(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        max_n = int(limit or 0)
        with self._lock:
            while self._pending_events and (max_n <= 0 or len(out) < max_n):
                out.append(dict(self._pending_events.popleft() or {}))
        return out

    def requeue_pending_events(self, events: Iterable[Dict[str, Any]]) -> None:
        rows = [dict(event or {}) for event in (events or []) if isinstance(event, dict)]
        if not rows:
            return
        with self._lock:
            for event in reversed(rows):
                self._pending_events.appendleft(event)
            while len(self._pending_events) > int(self._max_pending_events):
                self._pending_events.pop()
                self.note_error("polygon_ws_pending_event_cap_reached")

    def telemetry_snapshot(self) -> Dict[str, Any]:
        out = super().telemetry_snapshot()
        now = now_ms()
        out["last_ping_ts_ms"] = int(self._last_ping_ts_ms or 0)
        out["last_pong_ts_ms"] = int(self._last_pong_ts_ms or 0)
        out["last_transport_ts_ms"] = int(self._last_transport_ts_ms or 0)
        out["last_transport_age_ms"] = int((now - self._last_transport_ts_ms) if self._last_transport_ts_ms else 10**9)
        out["pong_age_ms"] = int((now - self._last_pong_ts_ms) if self._last_pong_ts_ms else 10**9)
        out["ws_open"] = bool(self._opened.is_set())
        return out

    def merge_snapshot(self, rows: Dict[str, Dict[str, Any]]) -> None:
        with self._lock:
            for symbol, rec in (rows or {}).items():
                cur = dict(self._last.get(str(symbol)) or {})
                cur.update(dict(rec or {}))
                self._last[str(symbol)] = cur

    def cap_snapshot_records(self, max_records: int) -> int:
        limit = max(1, int(max_records or 1))
        with self._lock:
            excess = max(0, int(len(self._last)) - int(limit))
            if excess <= 0:
                return 0
            ordered = sorted(
                self._last.items(),
                key=lambda item: (int((item[1] or {}).get("ts_ms") or 0), str(item[0])),
            )
            for symbol, _record in ordered[:excess]:
                self._last.pop(str(symbol), None)
            return int(excess)

    def merge_replay_events(self, events: Iterable[Dict[str, Any]]) -> None:
        with self._lock:
            for event in events or []:
                self._apply_event_locked(dict(event or {}), queue_event=False)

    def perform_gap_fill(self, symbols: Iterable[str], since_ts_ms: int) -> Dict[str, Dict[str, Any]]:
        if not symbols:
            return {}
        if self._rest_provider is None:
            self._rest_provider = PolygonPriceProvider()

        # Gap fill is best-effort and deliberately REST-backed. Streaming is the
        # primary truth source; REST only bridges reconnect windows.
        symbol_map = {str(sym): str(sym) for sym in symbols}
        bars_by_symbol = self._rest_provider.fetch_recent_bars(symbol_map, since_ts_ms) or {}
        out: Dict[str, Dict[str, Any]] = {}

        for symbol, bars in (bars_by_symbol or {}).items():
            if bars:
                last_bar = list(bars)[-1]
                close_px = last_bar.get("close")
                if close_px is not None:
                    out[str(symbol)] = {
                        "ts_ms": int(last_bar.get("ts_ms") or now_ms()),
                        "last": close_px,
                        "bid": None,
                        "ask": None,
                        "spread": None,
                        "volume": last_bar.get("volume"),
                        "gap_fill": True,
                        "gap_fill_kind": "recent_bars",
                        "gap_fill_since_ts_ms": int(since_ts_ms or 0),
                    }

        missing = [str(sym) for sym in symbols if str(sym) not in out]
        if missing:
            snap = self._rest_provider.fetch_last_prices({str(sym): str(sym) for sym in missing}) or {}
            for symbol, rec in snap.items():
                out[str(symbol)] = {
                    "ts_ms": int(rec.get("ts_ms") or now_ms()),
                    "last": rec.get("price"),
                    "bid": rec.get("bid"),
                    "ask": rec.get("ask"),
                    "spread": rec.get("spread"),
                    "volume": rec.get("volume"),
                    "gap_fill": True,
                    "gap_fill_kind": "snapshot_fallback",
                    "gap_fill_since_ts_ms": int(since_ts_ms or 0),
                }
        return out

    def fetch_replay_events(
        self,
        ticker_map: Dict[str, str],
        watermarks: Optional[Dict[str, Dict[str, int]]] = None,
        until_ts_ms: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        if not ticker_map:
            return []
        if self._rest_provider is None:
            self._rest_provider = PolygonPriceProvider()

        replay: List[Dict[str, Any]] = []
        trade_since: Dict[str, int] = {}
        quote_since: Dict[str, int] = {}
        for symbol in (ticker_map or {}).keys():
            marks = dict((watermarks or {}).get(str(symbol)) or {})
            trade_since[str(symbol)] = int(marks.get("T") or 0)
            quote_since[str(symbol)] = int(marks.get("Q") or 0)

        if self.subscribe_trades:
            try:
                trades = self._rest_provider.fetch_historical_trades(ticker_map, trade_since, until_ts_ms=until_ts_ms) or {}
            except Exception:
                trades = {}
            for symbol, rows in (trades or {}).items():
                for row in rows or []:
                    event = self._normalize_replay_trade(str(symbol), row)
                    if event:
                        replay.append(event)

        if self.subscribe_quotes:
            try:
                quotes = self._rest_provider.fetch_historical_quotes(ticker_map, quote_since, until_ts_ms=until_ts_ms) or {}
            except Exception:
                quotes = {}
            for symbol, rows in (quotes or {}).items():
                for row in rows or []:
                    event = self._normalize_replay_quote(str(symbol), row)
                    if event:
                        replay.append(event)

        replay.sort(
            key=lambda row: (
                int(row.get("timestamp") or 0),
                0 if str(row.get("event_type") or "") == "Q" else 1,
                str(row.get("event_key") or ""),
            )
        )
        return replay

    def latency_ms(self) -> int:
        return int(self._last_latency_ms)

    def _run_forever(self) -> None:
        ws = self._ws
        if ws is None:
            return
        try:
            ws.run_forever(
                ping_interval=max(1.0, self._ping_interval_s),
                ping_timeout=max(1.0, self._ping_timeout_s),
            )
        except Exception as e:
            log.exception("polygon_ws_run_forever_failed")
            self.note_error(e)
            self.note_disconnected(str(e))
        finally:
            if not self._stop:
                log.error("polygon_ws_run_forever_exited_without_stop")
                self.note_disconnected("polygon_ws_run_forever_exited")

    def _on_open(self, ws) -> None:
        ts_now = now_ms()
        self._last_transport_ts_ms = ts_now
        self._last_pong_ts_ms = ts_now
        self._opened.set()
        self.note_connected()
        try:
            log.info("polygon_ws_connect endpoint=%s state=connected", self.endpoint)
        except Exception as e:
            _warn_nonfatal(
                "polygon_ws_connect_log_failed",
                "POLYGON_WS_CONNECT_LOG_FAILED",
                e,
                warn_key="polygon_ws_connect_log_failed",
                endpoint=str(self.endpoint),
            )

    def _on_close(self, ws, code=None, msg=None) -> None:
        self._opened.clear()
        self._auth_event.set()
        self.note_disconnected(msg or code or "polygon_ws_closed")
        try:
            log_failure(
                log,
                event="polygon_ws_disconnect",
                code="POLYGON_WS_DISCONNECT",
                message="Polygon websocket disconnected.",
                error=None,
                level=logging.WARNING,
                component="engine.data.provider_sessions.polygon_ws_session",
                extra={"code": code, "message": msg},
                persist=False,
            )
        except Exception as e:
            _warn_nonfatal(
                "polygon_ws_disconnect_log_failed",
                "POLYGON_WS_DISCONNECT_LOG_FAILED",
                e,
                warn_key="polygon_ws_disconnect_log_failed",
                code=code,
                message=msg,
            )

    def _on_error(self, ws, error) -> None:
        self.note_error(error)
        self._auth_event.set()
        if not self._stop:
            self.note_disconnected(error)

    def _on_ping(self, ws, message) -> None:
        self._last_transport_ts_ms = now_ms()

    def _on_pong(self, ws, message) -> None:
        ts_now = now_ms()
        self._last_transport_ts_ms = ts_now
        self._last_pong_ts_ms = ts_now

    def _on_message(self, ws, message: str) -> None:
        ts_now = now_ms()
        self._last_transport_ts_ms = ts_now
        payload = None
        try:
            payload = json.loads(message)
        except Exception as e:
            _warn_nonfatal(
                "polygon_ws_session_message_parse_failed",
                "POLYGON_WS_SESSION_MESSAGE_PARSE_FAILED",
                e,
                warn_key="message_parse",
                message=str(message)[:200],
            )
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
                    status = str(ev.get("status") or "").lower()
                    status_msg = str(ev.get("message") or "").strip()
                    if status in ("auth_success", "authenticated"):
                        self.note_authenticated()
                        self._auth_event.set()
                    elif status in ("auth_failed", "error"):
                        err = status_msg or status or "polygon_ws_auth_failed"
                        self.note_error(err)
                        self.note_disconnected(err)
                        self._auth_event.set()
                    elif status == "connected":
                        self._opened.set()
                    continue
                event = self._normalize_ws_event(ev, ts_now)
                if not event:
                    continue
                self._apply_event_locked(event, queue_event=True)

    def _normalize_ws_event(self, ev: Dict[str, Any], ts_now: int) -> Optional[Dict[str, Any]]:
        et = str(ev.get("ev") or "")
        sym = ev.get("sym") or ev.get("symbol")
        if not sym or et not in {"T", "Q"}:
            return None
        sym_s = str(sym)
        event_ts_ms = int(ev.get("t") or ts_now)
        if et == "T":
            event_key = f"T|{sym_s}|{event_ts_ms}|{ev.get('i')}|{ev.get('q')}|{ev.get('x')}|{ev.get('p')}|{ev.get('s')}"
            return {
                "symbol": sym_s,
                "provider": self.provider_name,
                "source": self.provider_name,
                "event_type": "T",
                "event_key": event_key,
                "timestamp": event_ts_ms,
                "event_ts_ms": event_ts_ms,
                "trade_ts_ms": event_ts_ms,
                "last": ev.get("p"),
                "price": ev.get("p"),
                "volume": ev.get("s"),
                "size": ev.get("s"),
                "exchange": ev.get("x"),
                "sequence_number": ev.get("q"),
                "trade_id": ev.get("i"),
            }
        event_key = f"Q|{sym_s}|{event_ts_ms}|{ev.get('q')}|{ev.get('bx')}|{ev.get('ax')}|{ev.get('bp')}|{ev.get('ap')}|{ev.get('bs')}|{ev.get('as')}"
        return {
            "symbol": sym_s,
            "provider": self.provider_name,
            "source": self.provider_name,
            "event_type": "Q",
            "event_key": event_key,
            "timestamp": event_ts_ms,
            "event_ts_ms": event_ts_ms,
            "quote_ts_ms": event_ts_ms,
            "bid": ev.get("bp"),
            "ask": ev.get("ap"),
            "bid_size": ev.get("bs"),
            "ask_size": ev.get("as"),
            "sequence_number": ev.get("q"),
        }

    def _normalize_replay_trade(self, symbol: str, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ts_ms = int(row.get("event_ts_ms") or row.get("timestamp") or 0)
        if ts_ms <= 0:
            return None
        event_key = (
            str(row.get("trade_id") or "").strip()
            or f"T|{symbol}|{ts_ms}|{row.get('sequence_number')}|{row.get('exchange')}|{row.get('price')}|{row.get('size')}"
        )
        return {
            "symbol": str(symbol),
            "provider": self.provider_name,
            "source": str(row.get("source") or "polygon_rest_trade_replay"),
            "event_type": "T",
            "event_key": event_key,
            "timestamp": ts_ms,
            "event_ts_ms": ts_ms,
            "trade_ts_ms": ts_ms,
            "last": row.get("price"),
            "price": row.get("price"),
            "volume": row.get("size"),
            "size": row.get("size"),
            "exchange": row.get("exchange"),
            "sequence_number": row.get("sequence_number"),
            "trade_id": row.get("trade_id"),
        }

    def _normalize_replay_quote(self, symbol: str, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ts_ms = int(row.get("event_ts_ms") or row.get("timestamp") or 0)
        if ts_ms <= 0:
            return None
        event_key = f"Q|{symbol}|{ts_ms}|{row.get('sequence_number')}|{row.get('bid')}|{row.get('ask')}|{row.get('bid_size')}|{row.get('ask_size')}"
        return {
            "symbol": str(symbol),
            "provider": self.provider_name,
            "source": str(row.get("source") or "polygon_rest_quote_replay"),
            "event_type": "Q",
            "event_key": event_key,
            "timestamp": ts_ms,
            "event_ts_ms": ts_ms,
            "quote_ts_ms": ts_ms,
            "bid": row.get("bid"),
            "ask": row.get("ask"),
            "bid_size": row.get("bid_size"),
            "ask_size": row.get("ask_size"),
            "sequence_number": row.get("sequence_number"),
        }

    def _apply_event_locked(self, event: Dict[str, Any], *, queue_event: bool) -> None:
        sym = str(event.get("symbol") or "").strip()
        et = str(event.get("event_type") or "").strip().upper()
        if not sym or et not in {"T", "Q"}:
            return
        event_ts_ms = int(event.get("event_ts_ms") or event.get("timestamp") or now_ms())
        stream_key = f"{et}:{sym}"
        prev_stream_ts_ms = int(self._last_event_ts_by_stream.get(stream_key) or 0)
        if prev_stream_ts_ms > 0 and event_ts_ms < prev_stream_ts_ms:
            return
        event_key = str(event.get("event_key") or "")
        if self.should_drop_duplicate_event(stream_key, event_key):
            return
        self._last_event_ts_by_stream[stream_key] = max(prev_stream_ts_ms, event_ts_ms)

        rec = self._last.get(sym) or {"ts_ms": event_ts_ms}
        prev_ts_ms = int(rec.get("ts_ms") or 0)
        if et == "T":
            price = event.get("last", event.get("price"))
            if price is not None:
                try:
                    rec["last"] = float(price)
                except Exception as e:
                    _warn_nonfatal(
                        "polygon_ws_trade_price_parse_failed",
                        "POLYGON_WS_TRADE_PRICE_PARSE_FAILED",
                        e,
                        warn_key="polygon_ws_trade_price_parse_failed",
                        symbol=str(sym),
                        value=price,
                    )
            size = event.get("volume", event.get("size"))
            if size is not None:
                try:
                    rec["volume"] = float(size)
                except Exception as e:
                    _warn_nonfatal(
                        "polygon_ws_trade_volume_parse_failed",
                        "POLYGON_WS_TRADE_VOLUME_PARSE_FAILED",
                        e,
                        warn_key="polygon_ws_trade_volume_parse_failed",
                        symbol=str(sym),
                        value=size,
                    )
            rec["trade_ts_ms"] = int(event_ts_ms)
            rec["ts_ms"] = max(int(rec.get("quote_ts_ms") or 0), int(rec.get("trade_ts_ms") or 0), int(prev_ts_ms or 0))
        else:
            bid = event.get("bid")
            ask = event.get("ask")
            bid_size = event.get("bid_size")
            ask_size = event.get("ask_size")
            if bid is not None:
                try:
                    rec["bid"] = float(bid)
                except Exception as e:
                    _warn_nonfatal(
                        "polygon_ws_quote_bid_parse_failed",
                        "POLYGON_WS_QUOTE_BID_PARSE_FAILED",
                        e,
                        warn_key="polygon_ws_quote_bid_parse_failed",
                        symbol=str(sym),
                        value=bid,
                    )
            if ask is not None:
                try:
                    rec["ask"] = float(ask)
                except Exception as e:
                    _warn_nonfatal(
                        "polygon_ws_quote_ask_parse_failed",
                        "POLYGON_WS_QUOTE_ASK_PARSE_FAILED",
                        e,
                        warn_key="polygon_ws_quote_ask_parse_failed",
                        symbol=str(sym),
                        value=ask,
                    )
            if bid_size is not None:
                try:
                    rec["bid_sz"] = float(bid_size)
                except Exception as e:
                    _warn_nonfatal(
                        "polygon_ws_quote_bid_size_parse_failed",
                        "POLYGON_WS_QUOTE_BID_SIZE_PARSE_FAILED",
                        e,
                        warn_key="polygon_ws_quote_bid_size_parse_failed",
                        symbol=str(sym),
                        value=bid_size,
                    )
            if ask_size is not None:
                try:
                    rec["ask_sz"] = float(ask_size)
                except Exception as e:
                    _warn_nonfatal(
                        "polygon_ws_quote_ask_size_parse_failed",
                        "POLYGON_WS_QUOTE_ASK_SIZE_PARSE_FAILED",
                        e,
                        warn_key="polygon_ws_quote_ask_size_parse_failed",
                        symbol=str(sym),
                        value=ask_size,
                    )
            if ("bid" in rec) and ("ask" in rec):
                try:
                    rec["spread"] = float(rec["ask"]) - float(rec["bid"])
                except Exception as e:
                    _warn_nonfatal(
                        "polygon_ws_quote_spread_compute_failed",
                        "POLYGON_WS_QUOTE_SPREAD_COMPUTE_FAILED",
                        e,
                        warn_key="polygon_ws_quote_spread_compute_failed",
                        symbol=str(sym),
                        bid=rec.get("bid"),
                        ask=rec.get("ask"),
                    )
            rec["quote_ts_ms"] = int(event_ts_ms)
            rec["ts_ms"] = max(int(rec.get("trade_ts_ms") or 0), int(rec.get("quote_ts_ms") or 0), int(prev_ts_ms or 0))

        curr_ts_ms = int(rec.get("ts_ms") or event_ts_ms)
        self.note_message(now_ms())
        if prev_ts_ms > 0 and curr_ts_ms > prev_ts_ms and (curr_ts_ms - prev_ts_ms) > 60_000:
            self.note_gap_event()
            rec["gap_detected"] = True
            rec["gap_delta_ms"] = int(curr_ts_ms - prev_ts_ms)
        self._last_latency_ms = max(0, now_ms() - curr_ts_ms)
        self._last[sym] = rec

        if queue_event:
            event_out = dict(event)
            event_out["symbol"] = sym
            event_out["timestamp"] = int(event.get("timestamp") or event_ts_ms)
            event_out["event_ts_ms"] = int(event_ts_ms)
            event_out["trade_ts_ms"] = int(event.get("trade_ts_ms") or 0) or None
            event_out["quote_ts_ms"] = int(event.get("quote_ts_ms") or 0) or None
            event_out["ingest_ts_ms"] = now_ms()
            if len(self._pending_events) >= int(self._max_pending_events):
                self._pending_events.popleft()
                self.note_error("polygon_ws_pending_event_cap_reached")
            self._pending_events.append(event_out)
