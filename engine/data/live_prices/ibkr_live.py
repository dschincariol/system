"""
FILE: ibkr_live.py

Live price feed integration for `ibkr_live`.
"""

import os
import time
from typing import Dict, List, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.platform import default_ibkr_host

try:
    from ib_insync import IB, Stock, Contract
    _IB_INSYNC_IMPORT_ERROR = None
except Exception as _ib_insync_import_error:
    _IB_INSYNC_IMPORT_ERROR = _ib_insync_import_error

    class Contract:  # type: ignore
        def __init__(self, *_args, **_kwargs):
            raise RuntimeError(f"ib_insync_unavailable:{_IB_INSYNC_IMPORT_ERROR}")


    class Stock(Contract):  # type: ignore
        pass


    class IB:  # type: ignore
        def __init__(self, *_args, **_kwargs):
            raise RuntimeError(f"ib_insync_unavailable:{_IB_INSYNC_IMPORT_ERROR}")


LOG = get_logger("engine.data.live_prices.ibkr_live")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.data.live_prices.ibkr_live",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


class IBKRPriceProvider:
    """
    Snapshot-based IBKR market data provider.

    ticker_map usage (symbol -> value):
      - if value is digits: treated as conId
      - else: treated as a symbol for Stock(value, 'SMART', currency)

    For GLOBAL equities:
      - prefer storing IBKR conId in symbols.meta_json and pass conId strings in ticker_map
      - conId avoids exchange/currency ambiguity
    """

    def __init__(self):
        if _IB_INSYNC_IMPORT_ERROR is not None:
            raise RuntimeError(f"ib_insync_unavailable:{_IB_INSYNC_IMPORT_ERROR}")

        strict_runtime = (
            str(os.environ.get("ENGINE_SUPERVISED", "")).strip().lower() in ("1", "true", "yes", "y", "on")
            or str(os.environ.get("ENV", "")).strip().lower() in ("prod", "production")
        )

        host_raw = str(os.environ.get("IBKR_HOST", "") or "").strip()
        port_raw = str(os.environ.get("IBKR_PORT", "") or "").strip()
        client_id_raw = str(os.environ.get("IBKR_CLIENT_ID", "") or "").strip()

        if strict_runtime and (not host_raw or not port_raw or not client_id_raw):
            raise RuntimeError("IBKR_HOST/IBKR_PORT/IBKR_CLIENT_ID are required in supervised/prod runtime")

        self.host = host_raw or default_ibkr_host()
        self.port = int(port_raw or "7497")
        self.client_id = int(client_id_raw or "77")
        self.currency = os.environ.get("IBKR_CURRENCY", "USD").strip() or "USD"
        self.timeout_s = float(os.environ.get("IBKR_SNAPSHOT_TIMEOUT_S", "3.5"))
        self.sleep_after_req_s = float(os.environ.get("IBKR_SNAPSHOT_SETTLE_S", "0.35"))
        self._ib: Optional[IB] = None

    def _ensure_connected(self) -> IB:
        if self._ib is not None and self._ib.isConnected():
            return self._ib

        ib = IB()
        ib.connect(self.host, self.port, clientId=self.client_id, timeout=self.timeout_s)
        self._ib = ib
        return ib

    def _contract_from_value(self, val: str) -> Contract:
        v = str(val or "").strip()
        if v.isdigit():
            c = Contract()
            c.conId = int(v)
            return c
        sym = v
        return Stock(sym, "SMART", self.currency)

    def fetch_last_prices(self, ticker_map: Dict[str, str]) -> Dict[str, dict]:
        if not ticker_map:
            return {}

        ib = self._ensure_connected()

        syms = []
        contracts = []
        for sym, val in (ticker_map or {}).items():
            try:
                c = self._contract_from_value(val)
                syms.append(str(sym))
                contracts.append(c)
            except Exception as e:
                _warn_nonfatal("IBKR_LIVE_CONTRACT_PARSE_FAILED", e, once_key=f"contract:{sym}", symbol=str(sym), contract=repr(val)[:200])
                continue

        if not contracts:
            return {}

        try:
            ib.qualifyContracts(*contracts)
        except Exception as e:
            _warn_nonfatal(
                "IBKR_LIVE_QUALIFY_CONTRACTS_FAILED",
                e,
                once_key="ibkr_live_qualify_contracts",
                contract_count=len(contracts),
            )

        tickers = []
        for c in contracts:
            try:
                t = ib.reqMktData(c, "", snapshot=True, regulatorySnapshot=False)
                tickers.append(t)
            except Exception:
                tickers.append(None)

        try:
            ib.sleep(self.sleep_after_req_s)
        except Exception:
            time.sleep(self.sleep_after_req_s)

        now_ts_ms = int(time.time() * 1000)
        out: Dict[str, dict] = {}
        skipped = 0
        for i, t in enumerate(tickers):
            sym = syms[i]
            if not t:
                skipped += 1
                _warn_nonfatal(
                    "IBKR_LIVE_SKIP_NO_PRICE",
                    RuntimeError("ibkr_live_skip_no_price"),
                    once_key=f"skip_no_price:{sym}",
                    symbol=str(sym),
                    reason="ticker_unavailable",
                )
                continue

            last = None
            bid = None
            ask = None
            volume = None

            try:
                if t.last is not None and float(t.last) > 0:
                    last = float(t.last)
            except Exception:
                last = None

            try:
                if t.bid is not None and float(t.bid) > 0:
                    bid = float(t.bid)
            except Exception:
                bid = None

            try:
                if t.ask is not None and float(t.ask) > 0:
                    ask = float(t.ask)
            except Exception:
                ask = None

            try:
                if t.volume is not None:
                    volume = float(t.volume)
            except Exception:
                volume = None

            if last is None:
                try:
                    mp = t.marketPrice()
                    if mp is not None and float(mp) > 0:
                        last = float(mp)
                except Exception:
                    last = None

            if last is None:
                try:
                    if t.close is not None and float(t.close) > 0:
                        last = float(t.close)
                except Exception:
                    last = None

            if last is None or last <= 0:
                skipped += 1
                _warn_nonfatal(
                    "IBKR_LIVE_SKIP_NO_PRICE",
                    RuntimeError("ibkr_live_skip_no_price"),
                    once_key=f"skip_no_price:{sym}",
                    symbol=str(sym),
                    reason="missing_last_price",
                )
                continue

            spread = None
            try:
                if bid is not None and ask is not None and ask >= bid:
                    spread = float(ask) - float(bid)
            except Exception:
                spread = None

            out[sym] = {
                "ts_ms": now_ts_ms,
                "price": float(last),
                "last": float(last),
                "bid": bid,
                "ask": ask,
                "spread": spread,
                "volume": volume,
                "source": "ibkr",
            }

        for t in tickers:
            try:
                if t and t.contract:
                    ib.cancelMktData(t.contract)
            except Exception as e:
                _warn_nonfatal(
                    "IBKR_LIVE_CANCEL_MARKET_DATA_FAILED",
                    e,
                    once_key="ibkr_live_cancel_market_data",
                )

        LOG.info("fetch_complete provider=ibkr requested=%d returned=%d skipped=%d", len(syms), len(out), skipped)
        return out

    def fetch_recent_bars(self, ticker_map: Dict[str, str], since_ts_ms: int) -> Dict[str, List[Dict[str, float]]]:
        if not ticker_map:
            return {}

        ib = self._ensure_connected()
        now_ts_ms = int(time.time() * 1000)
        lookback_s = max(60, int((now_ts_ms - int(since_ts_ms or (now_ts_ms - 5 * 60 * 1000))) / 1000))
        duration_s = max(120, min(86400, lookback_s + 120))
        duration_str = f"{int(duration_s)} S"

        out: Dict[str, List[Dict[str, float]]] = {}
        for sym, val in (ticker_map or {}).items():
            try:
                contract = self._contract_from_value(val)
                try:
                    ib.qualifyContracts(contract)
                except Exception as e:
                    _warn_nonfatal(
                        "IBKR_LIVE_QUALIFY_HISTORICAL_CONTRACT_FAILED",
                        e,
                        once_key="ibkr_live_qualify_historical_contract",
                        symbol=str(sym),
                    )
                bars = ib.reqHistoricalData(
                    contract,
                    endDateTime="",
                    durationStr=duration_str,
                    barSizeSetting="1 min",
                    whatToShow="TRADES",
                    useRTH=False,
                    formatDate=2,
                    keepUpToDate=False,
                )
                rows: List[Dict[str, float]] = []
                for bar in list(bars or []):
                    try:
                        bar_ts_ms = int(float(bar.date.timestamp()) * 1000)
                    except Exception:
                        try:
                            bar_ts_ms = int(bar.date.timestamp() * 1000)
                        except Exception as e:
                            _warn_nonfatal("IBKR_LIVE_BAR_TS_PARSE_FAILED", e, once_key=f"bar_ts:{sym}", symbol=str(sym), bar=repr(bar)[:200])
                            continue
                    if bar_ts_ms < int(since_ts_ms or 0):
                        continue
                    rows.append(
                        {
                            "ts_ms": int(bar_ts_ms),
                            "open": float(bar.open) if bar.open is not None else None,
                            "high": float(bar.high) if bar.high is not None else None,
                            "low": float(bar.low) if bar.low is not None else None,
                            "close": float(bar.close) if bar.close is not None else None,
                            "volume": float(bar.volume) if bar.volume is not None else None,
                            "source": "ibkr",
                        }
                    )
                out[str(sym)] = rows
            except Exception:
                out[str(sym)] = []
        return out
