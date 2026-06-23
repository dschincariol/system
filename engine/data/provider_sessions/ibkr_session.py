"""
FILE: ibkr_session.py

Provider session management module for `ibkr_session`.
"""

"""
IBKR Provider Session

Handles:
• TWS / IB Gateway connection
• reqMktData streaming
• subscription reconciliation
• heartbeat / stalled feed detection
• snapshot gap fill after reconnect
"""

import logging
import queue
import threading
import time
from typing import Any, Dict, Iterable, Optional
from engine.data.price_event_keys import compute_price_raw_event_key
from engine.runtime.failure_diagnostics import log_failure

try:
    from ibapi.client import EClient
    from ibapi.contract import Contract
    from ibapi.wrapper import EWrapper
    _IBAPI_IMPORT_ERROR = None
except Exception as _ibapi_import_error:
    _IBAPI_IMPORT_ERROR = _ibapi_import_error

    class EWrapper:  # type: ignore
        pass

    class EClient:  # type: ignore
        def __init__(self, *_args, **_kwargs):
            raise RuntimeError(f"ibapi_unavailable:{_IBAPI_IMPORT_ERROR}")

    class Contract:  # type: ignore
        def __init__(self, *_args, **_kwargs):
            raise RuntimeError(f"ibapi_unavailable:{_IBAPI_IMPORT_ERROR}")

try:
    from engine.data.live_prices.ibkr_live import IBKRPriceProvider
    _IBKR_LIVE_IMPORT_ERROR = None
except Exception as _ibkr_live_import_error:
    _IBKR_LIVE_IMPORT_ERROR = _ibkr_live_import_error

    class IBKRPriceProvider:  # type: ignore
        def __init__(self, *_args, **_kwargs):
            raise RuntimeError(f"ibkr_live_unavailable:{_IBKR_LIVE_IMPORT_ERROR}")

from .base_session import BaseProviderSession, now_ms

log = logging.getLogger("ibkr_session")
_WARNED_NONFATAL_KEYS: set[str] = set()


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
        component="engine.data.provider_sessions.ibkr_session",
        extra=extra or None,
        include_health=False,
        persist=False,
    )


class _IBKRWrapper(EWrapper):
    def __init__(self, event_q: "queue.Queue[Dict[str, Any]]"):
        super().__init__()
        self.q = event_q
        self.reqId_symbol: Dict[int, str] = {}
        self.last_ts_ms = now_ms()
        self.historical_q_by_req: Dict[int, "queue.Queue[Dict[str, Any]]"] = {}

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        try:
            self.q.put_nowait(
                {
                    "event": "error",
                    "reqId": int(reqId) if reqId is not None else 0,
                    "errorCode": int(errorCode) if errorCode is not None else 0,
                    "errorString": str(errorString or ""),
                    "ts_ms": now_ms(),
                }
            )
        except Exception as e:
            _warn_nonfatal(
                "IBKR_SESSION_ERROR_QUEUE_PUT_FAILED",
                e,
                once_key="error_queue_put",
                req_id=int(reqId) if reqId is not None else 0,
                error_code=int(errorCode) if errorCode is not None else 0,
            )

    def tickPrice(self, reqId, tickType, price, attrib):
        sym = self.reqId_symbol.get(int(reqId))
        if not sym:
            return

        ts = now_ms()
        evt: Dict[str, Any] = {"symbol": sym, "ts_ms": int(ts)}

        try:
            tt = int(tickType)
        except Exception:
            tt = None

        try:
            px = float(price)
        except Exception:
            px = None

        if px is None:
            return

        # Normalize IBKR's tick-type vocabulary into the generic bid/ask/last
        # schema expected by the provider-session manager and downstream router.
        if tt == 1:
            evt["tick_type"] = "bid"
            evt["bid"] = px
        elif tt == 2:
            evt["tick_type"] = "ask"
            evt["ask"] = px
        elif tt == 4:
            evt["tick_type"] = "last"
            evt["last"] = px
        else:
            return

        try:
            self.q.put_nowait(evt)
        except Exception as e:
            _warn_nonfatal(
                "IBKR_SESSION_TICK_PRICE_QUEUE_PUT_FAILED",
                e,
                once_key=f"tick_price_queue_put:{sym}",
                symbol=str(sym),
                req_id=int(reqId),
            )

        self.last_ts_ms = int(ts)

    def tickSize(self, reqId, tickType, size):
        sym = self.reqId_symbol.get(int(reqId))
        if not sym:
            return

        ts = now_ms()
        evt: Dict[str, Any] = {"symbol": sym, "ts_ms": int(ts)}

        try:
            tt = int(tickType)
        except Exception:
            tt = None

        try:
            sz = float(size)
        except Exception:
            sz = None

        if sz is None:
            return

        if tt == 5:
            evt["tick_type"] = "volume"
            evt["volume"] = sz
        else:
            return

        try:
            self.q.put_nowait(evt)
        except Exception as e:
            _warn_nonfatal(
                "IBKR_SESSION_TICK_SIZE_QUEUE_PUT_FAILED",
                e,
                once_key=f"tick_size_queue_put:{sym}",
                symbol=str(sym),
                req_id=int(reqId),
            )

        self.last_ts_ms = int(ts)

    def historicalData(self, reqId, bar):
        q = self.historical_q_by_req.get(int(reqId))
        if q is None:
            return

        try:
            bar_ts_ms = int(float(bar.date) * 1000)
        except Exception:
            try:
                bar_ts_ms = int(bar.date.timestamp() * 1000)
            except Exception:
                bar_ts_ms = now_ms()

        try:
            q.put_nowait(
                {
                    "event": "historical_bar",
                    "ts_ms": int(bar_ts_ms),
                    "open": (float(bar.open) if bar.open is not None else None),
                    "high": (float(bar.high) if bar.high is not None else None),
                    "low": (float(bar.low) if bar.low is not None else None),
                    "close": (float(bar.close) if bar.close is not None else None),
                    "volume": (float(bar.volume) if bar.volume is not None else None),
                }
            )
        except Exception as e:
            _warn_nonfatal(
                "IBKR_SESSION_HISTORICAL_BAR_QUEUE_PUT_FAILED",
                e,
                once_key=f"historical_bar_queue_put:{int(reqId)}",
                req_id=int(reqId),
            )

    def historicalDataEnd(self, reqId, start, end):
        q = self.historical_q_by_req.get(int(reqId))
        if q is None:
            return
        try:
            q.put_nowait({"event": "historical_end", "start": start, "end": end})
        except Exception as e:
            _warn_nonfatal(
                "IBKR_SESSION_HISTORICAL_END_QUEUE_PUT_FAILED",
                e,
                once_key=f"historical_end_queue_put:{int(reqId)}",
                req_id=int(reqId),
            )


class _IBKRClient(EClient):
    def __init__(self, wrapper: _IBKRWrapper):
        super().__init__(wrapper)


class IBKRSession(BaseProviderSession):
    provider_name = "ibkr"

    def __init__(self, host: str, port: int, client_id: int, data_type: int = 1):
        if _IBAPI_IMPORT_ERROR is not None:
            raise RuntimeError(f"ibapi_unavailable:{_IBAPI_IMPORT_ERROR}")
        if _IBKR_LIVE_IMPORT_ERROR is not None:
            raise RuntimeError(f"ibkr_live_unavailable:{_IBKR_LIVE_IMPORT_ERROR}")

        super().__init__("ibkr")

        self.host = str(host)
        self.port = int(port)
        self.client_id = int(client_id)
        self.data_type = int(data_type)

        self._lock = threading.RLock()
        self._event_q: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=200000)

        self.wrapper = _IBKRWrapper(self._event_q)
        self.client = _IBKRClient(self.wrapper)

        self._client_thread: Optional[threading.Thread] = None

        self._req_id = 1
        self._symbol_req: Dict[str, int] = {}

        self._last: Dict[str, Dict[str, Any]] = {}
        self._last_latency_ms = 0
        self._gap_fill_provider: Optional[IBKRPriceProvider] = None

        self.set_capability("streaming", True)
        self.set_capability("polling", False)
        self.set_capability("gap_fill", True)
        self.set_capability("historical_catchup", "snapshot")
        self.set_capability("authentication", "gateway_session")
        self.set_capability("supports_quotes", True)
        self.set_capability("supports_trades", True)

        self.set_capability("supports_historical", True)
        self.set_capability("supports_historical_bars", True)
        self.set_capability("supports_tick_stream", True)
        self.set_capability("supports_snapshot", True)
        self.set_capability("historical_replay_mode", "reqHistoricalData")

        self.set_capability(
            "rate_limit_per_min",
            int(__import__("os").environ.get("IBKR_REST_RATE_LIMIT_PER_MIN", "120")),
        )

        self.set_capability("capability_source", "provider_session")

    def connect(self) -> None:
        log.info("connecting IBKR %s:%s client_id=%s", self.host, self.port, self.client_id)

        try:
            self.client.connect(self.host, self.port, self.client_id)
        except Exception as e:
            self.note_disconnected(str(e))
            raise

        if not bool(self.client.isConnected()):
            self.note_disconnected("ibkr_connect_failed")
            raise RuntimeError("ibkr_connect_failed")

        # The IB API requires a dedicated network loop thread; without it the
        # socket is technically connected but no market data will ever flow.
        if self._client_thread is None or not self._client_thread.is_alive():
            self._client_thread = threading.Thread(target=self.client.run, name="ibkr_client_run", daemon=True)
            self._client_thread.start()

        time.sleep(0.5)

        self.note_connected()

        try:
            self.client.reqMarketDataType(int(self.data_type))
        except Exception as e:
            self.note_error(e)

    def authenticate(self) -> None:
        if not bool(self.client.isConnected()):
            raise RuntimeError("ibkr_not_connected")
        self.note_authenticated()

    def detect_capabilities(self) -> Dict[str, Any]:
        self.set_capability("supports_quotes", True)
        self.set_capability("supports_trades", True)
        self.set_capability("supports_snapshot", True)
        self.set_capability("supports_historical", True)
        return self.telemetry_snapshot().get("capabilities") or {}

    def _build_contract(self, symbol: str) -> Contract:
        contract = Contract()
        contract.symbol = str(symbol)
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        return contract

    def subscribe(self, symbols: Iterable[str]) -> None:
        clean = [str(x).strip() for x in (symbols or []) if str(x).strip()]
        if not clean:
            return

        for sym in clean:
            if sym in self._symbol_req:
                continue

            contract = self._build_contract(sym)

            req_id = int(self._req_id)
            self._req_id += 1

            self._symbol_req[sym] = req_id
            self.wrapper.reqId_symbol[req_id] = sym

            self.client.reqMktData(
                req_id,
                contract,
                "",
                False,
                False,
                [],
            )

        self.update_subscribed_symbols(self.subscribed_symbols() | set(clean))

    def unsubscribe(self, symbols: Iterable[str]) -> None:
        clean = [str(x).strip() for x in (symbols or []) if str(x).strip()]
        if not clean:
            return

        for sym in clean:
            req_id = self._symbol_req.pop(sym, None)
            if req_id is None:
                continue
            try:
                self.wrapper.reqId_symbol.pop(int(req_id), None)
            except Exception as e:
                _warn_nonfatal(
                    "IBKR_SESSION_UNSUBSCRIBE_REQID_POP_FAILED",
                    e,
                    symbol=str(sym),
                    req_id=int(req_id),
                )
            try:
                self.client.cancelMktData(int(req_id))
            except Exception as e:
                _warn_nonfatal(
                    "IBKR_SESSION_UNSUBSCRIBE_CANCEL_MARKET_DATA_FAILED",
                    e,
                    symbol=str(sym),
                    req_id=int(req_id),
                )

        self.update_subscribed_symbols(self.subscribed_symbols() - set(clean))

    def heartbeat(self) -> Dict[str, Any]:
        if not bool(self.client.isConnected()):
            self.note_disconnected("ibkr_disconnected")
            raise RuntimeError("ibkr_disconnected")
        self._drain_events(max_items=2500)
        return super().heartbeat()

    def close(self) -> None:
        try:
            try:
                for sym, req_id in list(self._symbol_req.items()):
                    try:
                        self.client.cancelMktData(int(req_id))
                    except Exception as e:
                        _warn_nonfatal(
                            "IBKR_SESSION_CLOSE_CANCEL_MARKET_DATA_FAILED",
                            e,
                            symbol=str(sym),
                            req_id=int(req_id),
                        )
                self._symbol_req.clear()
                self.wrapper.reqId_symbol.clear()
            except Exception as e:
                _warn_nonfatal("IBKR_SESSION_CLOSE_CLEAR_STATE_FAILED", e)

            try:
                self.client.disconnect()
            except Exception as e:
                _warn_nonfatal("IBKR_SESSION_CLOSE_DISCONNECT_FAILED", e)

        finally:
            self.note_disconnected("closed")
            self.update_subscribed_symbols(set())

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        self._drain_events()
        with self._lock:
            return {k: dict(v) for k, v in self._last.items()}

    def merge_snapshot(self, rows: Dict[str, Dict[str, Any]]) -> None:
        with self._lock:
            for symbol, rec in (rows or {}).items():
                cur = dict(self._last.get(str(symbol)) or {})
                cur.update(dict(rec or {}))
                self._last[str(symbol)] = cur

    def perform_gap_fill(self, symbols: Iterable[str], since_ts_ms: int) -> Dict[str, Dict[str, Any]]:
        clean = [str(x).strip() for x in (symbols or []) if str(x).strip()]
        if not clean:
            return {}

        now_ts_ms = now_ms()
        since_ts_ms = int(since_ts_ms or 0)
        lookback_ms = max(60_000, now_ts_ms - since_ts_ms) if since_ts_ms > 0 else 120_000
        duration_s = max(120, min(86_400, int(lookback_ms / 1000.0) + 120))
        duration_str = f"{int(duration_s)} S"

        out: Dict[str, Dict[str, Any]] = {}
        missing = []

        for sym in clean:
            req_id = int(self._req_id)
            self._req_id += 1
            hist_q: "queue.Queue[Dict[str, Any]]" = queue.Queue()
            self.wrapper.historical_q_by_req[req_id] = hist_q
            bars = []

            try:
                contract = self._build_contract(sym)
                self.client.reqHistoricalData(
                    req_id,
                    contract,
                    "",
                    duration_str,
                    "1 min",
                    "TRADES",
                    0,
                    2,
                    False,
                    [],
                )

                deadline = time.time() + 8.0
                while time.time() < deadline:
                    timeout_s = max(0.1, deadline - time.time())
                    try:
                        evt = hist_q.get(timeout=timeout_s)
                    except queue.Empty:
                        break

                    if evt.get("event") == "historical_end":
                        break
                    if evt.get("event") != "historical_bar":
                        continue

                    bar_ts_ms = int(evt.get("ts_ms") or 0)
                    if since_ts_ms > 0 and bar_ts_ms < since_ts_ms:
                        continue
                    bars.append(evt)
            except Exception as e:
                self.note_error(e)
            finally:
                try:
                    self.client.cancelHistoricalData(req_id)
                except Exception as e:
                    _warn_nonfatal(
                        "IBKR_SESSION_GAP_FILL_CANCEL_HISTORICAL_FAILED",
                        e,
                        req_id=int(req_id),
                        once_key=f"cancel_historical:{int(req_id)}",
                    )
                self.wrapper.historical_q_by_req.pop(req_id, None)

            if bars:
                last_bar = bars[-1]
                px = last_bar.get("close")
                if px is not None:
                    out[str(sym)] = {
                        "ts_ms": int(last_bar.get("ts_ms") or now_ms()),
                        "last": px,
                        "bid": None,
                        "ask": None,
                        "spread": None,
                        "volume": last_bar.get("volume"),
                        "gap_fill": True,
                        "gap_fill_kind": "historical_replay",
                        "gap_fill_since_ts_ms": int(since_ts_ms or 0),
                        "gap_fill_bar_count": int(len(bars)),
                    }
                    continue

            missing.append(sym)

        if missing:
            if self._gap_fill_provider is None:
                self._gap_fill_provider = IBKRPriceProvider()
            snap = self._gap_fill_provider.fetch_last_prices({sym: sym for sym in missing}) or {}
            for symbol, price in (snap or {}).items():
                if isinstance(price, dict):
                    px = price.get("price")
                    ts_ms = int(price.get("ts_ms") or now_ms())
                    bid = price.get("bid")
                    ask = price.get("ask")
                    volume = price.get("volume")
                else:
                    px = price
                    ts_ms = now_ms()
                    bid = None
                    ask = None
                    volume = None
                out[str(symbol)] = {
                    "ts_ms": int(ts_ms),
                    "last": px,
                    "bid": bid,
                    "ask": ask,
                    "spread": (float(ask) - float(bid)) if bid is not None and ask is not None else None,
                    "volume": volume,
                    "gap_fill": True,
                    "gap_fill_kind": "snapshot_fallback",
                    "gap_fill_since_ts_ms": int(since_ts_ms or 0),
                }
        return out

    def _drain_events(self, max_items: int = 5000) -> None:
        drained = 0
        ts_now = now_ms()

        while drained < int(max_items):
            try:
                evt = self._event_q.get_nowait()
            except queue.Empty:
                break
            except Exception:
                break

            drained += 1

            if evt.get("event") == "error":
                err_code = int(evt.get("errorCode") or 0)
                err_msg = str(evt.get("errorString") or "")
                if err_code not in (2104, 2106, 2158):
                    self.note_error(f"ibkr_error code={err_code} msg={err_msg}")
                continue

            sym = evt.get("symbol")
            if not sym:
                continue
            sym = str(sym)

            ts = int(evt.get("ts_ms") or ts_now)
            event_key = compute_price_raw_event_key(
                evt,
                provider=self.provider_name,
                symbol=sym,
                event_type="U",
                event_ts_ms=ts,
                ts_ms=ts,
            )
            if self.should_drop_duplicate_event(sym, event_key):
                continue
            self.note_message(ts)

            try:
                self._last_latency_ms = max(0, int(ts_now) - int(ts))
            except Exception as e:
                _warn_nonfatal(
                    "IBKR_SESSION_LATENCY_UPDATE_FAILED",
                    e,
                    once_key="latency_update",
                )

            with self._lock:
                rec = dict(self._last.get(sym) or {"ts_ms": ts})
                prev_ts_ms = int(rec.get("ts_ms") or 0)
                for k, v in evt.items():
                    if k in ("symbol", "event"):
                        continue
                    rec[k] = v
                rec["ts_ms"] = int(rec.get("ts_ms") or ts)

                try:
                    if rec.get("bid") is not None and rec.get("ask") is not None:
                        rec["spread"] = float(rec["ask"]) - float(rec["bid"])
                except Exception as e:
                    _warn_nonfatal(
                        "IBKR_SESSION_SPREAD_UPDATE_FAILED",
                        e,
                        once_key=f"spread_update:{sym}",
                        symbol=str(sym),
                    )

                if prev_ts_ms > 0 and int(rec.get("ts_ms") or ts) > prev_ts_ms and (int(rec.get("ts_ms") or ts) - prev_ts_ms) > 60_000:
                    self.note_gap_event()
                    rec["gap_detected"] = True
                    rec["gap_delta_ms"] = int(int(rec.get("ts_ms") or ts) - prev_ts_ms)
                self._last[sym] = rec

    def latency_ms(self) -> int:
        return int(self._last_latency_ms)
