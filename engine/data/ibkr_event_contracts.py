"""Read-only IBKR event-contract market-data adapter."""

from __future__ import annotations

import json
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Mapping

from engine.data.forecastex_event_contracts import (
    DEFAULT_FORECASTEX_ASSET_BASKETS,
    _clean_product_id,
    _infer_event_type,
    _json_obj,
    _probability,
    _slug,
)
from engine.data.prediction_market_storage import PROVIDER_CATEGORY_EVENT_SIGNAL, raw_payload_hash, safe_float, safe_int
from engine.runtime.platform import default_ibkr_host

try:
    from ibapi.client import EClient
    from ibapi.contract import Contract
    from ibapi.wrapper import EWrapper
    _IBAPI_IMPORT_ERROR = None
except Exception as _import_error:
    _IBAPI_IMPORT_ERROR = _import_error

    class EWrapper:  # type: ignore
        pass

    class EClient:  # type: ignore
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError(f"ibapi_unavailable:{_IBAPI_IMPORT_ERROR}")

    class Contract:  # type: ignore
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError(f"ibapi_unavailable:{_IBAPI_IMPORT_ERROR}")


IBKR_EVENT_CONTRACT_PROVIDER_NAME = "ibkr_event_contracts"
IBKR_EVENT_CONTRACT_EXCHANGE = "FORECASTX"
IBKR_EVENT_CONTRACT_SEC_TYPE = "OPT"
IBKR_EVENT_CONTRACT_CURRENCY = "USD"


def _env_bool_value(value: Any, default: bool = False) -> bool:
    if value is None or str(value).strip() == "":
        return bool(default)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_allowlist(value: Any) -> list[dict[str, Any]]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        raw_rows = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except Exception:
            raw_rows = [{"conid": item.strip()} for item in text.split(",") if item.strip()]
        else:
            raw_rows = parsed if isinstance(parsed, list) else []
    else:
        raw_rows = []
    out: list[dict[str, Any]] = []
    for item in raw_rows:
        if isinstance(item, Mapping):
            row = dict(item)
        else:
            row = {"conid": str(item or "").strip()}
        conid = str(row.get("conid") or row.get("contract_id") or row.get("provider_contract_id") or "").strip()
        if not conid:
            continue
        row["conid"] = conid
        out.append(row)
    return out


def ibkr_event_contracts_enabled(settings: Mapping[str, Any] | None = None) -> bool:
    settings_map = dict(settings or {})
    return _env_bool_value(settings_map.get("ibkr_enabled") or settings_map.get("include_ibkr_event_contracts"), False)


def _asset_map(settings: Mapping[str, Any] | None) -> dict[str, list[str]]:
    out = {key: list(values) for key, values in DEFAULT_FORECASTEX_ASSET_BASKETS.items()}
    parsed = _json_obj((settings or {}).get("asset_map_json"))
    for key, values in parsed.items():
        assets = sorted({str(item or "").upper().strip().replace("$", "") for item in _parse_list_like(values) if str(item or "").strip()})
        if assets:
            out[str(key).upper().strip()] = assets
            out[_slug(key)] = assets
    return out


def _parse_list_like(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    return [item.strip() for item in text.split(",") if item.strip()]


@dataclass
class _IBKREventContractSpec:
    conid: str
    symbol: str
    product_id: str
    event_type: str
    affected_assets: list[str]
    official_resolution_source: str
    resolution_ts_ms: int | None = None


def _contract_spec(raw: Mapping[str, Any], settings: Mapping[str, Any] | None = None) -> _IBKREventContractSpec:
    conid = str(raw.get("conid") or "").strip()
    symbol = str(raw.get("symbol") or raw.get("local_symbol") or raw.get("product_id") or conid).strip().upper()
    product_id = _clean_product_id(raw.get("product_id") or symbol)
    event_type = str(raw.get("event_type") or "").strip()
    if not event_type:
        event_type = _infer_event_type(
            product_id=product_id,
            product_name=str(raw.get("product_name") or raw.get("title") or ""),
            product_category=str(raw.get("product_category") or raw.get("category") or ""),
            contract_id=symbol,
        )
    assets = [str(item or "").upper().strip().replace("$", "") for item in _parse_list_like(raw.get("affected_assets") or raw.get("assets"))]
    if not assets:
        maps = _asset_map(settings)
        assets = list(maps.get(product_id) or maps.get(_slug(event_type)) or maps.get(str(event_type)) or [])
    return _IBKREventContractSpec(
        conid=conid,
        symbol=symbol,
        product_id=product_id,
        event_type=event_type,
        affected_assets=sorted({asset for asset in assets if asset}),
        official_resolution_source=str(raw.get("official_resolution_source") or raw.get("resolution_source") or "").strip(),
        resolution_ts_ms=safe_int(raw.get("resolution_ts_ms"), 0) or None,
    )


class _MarketDataWrapper(EWrapper):
    def __init__(self, event_q: "queue.Queue[dict[str, Any]]") -> None:
        super().__init__()
        self.event_q = event_q
        self.req_specs: dict[int, _IBKREventContractSpec] = {}

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson="") -> None:  # noqa: N802
        try:
            self.event_q.put_nowait(
                {
                    "event": "error",
                    "req_id": int(reqId or 0),
                    "error_code": int(errorCode or 0),
                    "error": str(errorString or ""),
                    "ts_ms": int(time.time() * 1000),
                }
            )
        except Exception:
            return

    def tickPrice(self, reqId, tickType, price, attrib) -> None:  # noqa: N802
        spec = self.req_specs.get(int(reqId))
        if spec is None:
            return
        try:
            px = float(price)
        except Exception:
            return
        tick_type = int(tickType or 0)
        field = "last"
        if tick_type == 1:
            field = "bid"
        elif tick_type == 2:
            field = "ask"
        elif tick_type == 4:
            field = "last"
        else:
            return
        self.event_q.put_nowait({"event": "tick", "conid": spec.conid, field: px, "ts_ms": int(time.time() * 1000)})

    def tickSize(self, reqId, tickType, size) -> None:  # noqa: N802
        spec = self.req_specs.get(int(reqId))
        if spec is None:
            return
        try:
            qty = float(size)
        except Exception:
            return
        tick_type = int(tickType or 0)
        if tick_type in {5, 8, 29, 30}:
            self.event_q.put_nowait({"event": "tick", "conid": spec.conid, "size": qty, "ts_ms": int(time.time() * 1000)})


class _MarketDataClient(EClient):
    def __init__(self, wrapper: _MarketDataWrapper) -> None:
        super().__init__(wrapper)


def _build_contract(spec: _IBKREventContractSpec) -> Contract:
    contract = Contract()
    contract.conId = int(spec.conid)
    contract.exchange = IBKR_EVENT_CONTRACT_EXCHANGE
    contract.secType = IBKR_EVENT_CONTRACT_SEC_TYPE
    contract.currency = IBKR_EVENT_CONTRACT_CURRENCY
    return contract


def _market_record_from_snapshot(spec: _IBKREventContractSpec, snapshot: Mapping[str, Any], *, now_ms: int) -> dict[str, Any] | None:
    bid = _probability(snapshot.get("bid"))
    ask = _probability(snapshot.get("ask"))
    last = _probability(snapshot.get("last"))
    if bid is not None and ask is not None:
        probability = (bid + ask) / 2.0
        spread = max(0.0, ask - bid)
    else:
        probability = last if last is not None else bid
        spread = None
    if probability is None:
        return None
    source_ts_ms = safe_int(snapshot.get("ts_ms"), now_ms) or int(now_ms)
    size = max(0.0, safe_float(snapshot.get("size"), 0.0))
    raw = {"spec": spec.__dict__, "snapshot": dict(snapshot), "read_only": True}
    status = "active" if (not spec.resolution_ts_ms or int(spec.resolution_ts_ms) > int(now_ms)) else "expired"
    return {
        "provider_name": IBKR_EVENT_CONTRACT_PROVIDER_NAME,
        "provider_market_id": f"ibkr:{spec.conid}:YES",
        "provider_contract_id": spec.conid,
        "provider_event_id": f"ibkr:{spec.conid}",
        "market_ticker": spec.symbol or spec.conid,
        "series_ticker": spec.product_id,
        "title": spec.symbol or spec.conid,
        "subtitle": "YES",
        "product_id": spec.product_id,
        "official_resolution_source": spec.official_resolution_source,
        "provider_category": PROVIDER_CATEGORY_EVENT_SIGNAL,
        "event_type": spec.event_type,
        "condition_id": spec.conid,
        "token_id": f"{spec.conid}:YES",
        "outcome_name": "YES",
        "semantic_event_id": f"ibkr_event_contract:{spec.product_id}:{spec.event_type}",
        "resolution_semantics": spec.official_resolution_source,
        "status": status,
        "probability": probability,
        "bid_probability": bid,
        "ask_probability": ask,
        "last_price": last if last is not None else probability,
        "liquidity": size,
        "volume": size,
        "volume_24h": size,
        "open_interest": 0.0,
        "spread": spread,
        "event_ts_ms": spec.resolution_ts_ms,
        "close_ts_ms": spec.resolution_ts_ms,
        "resolution_ts_ms": spec.resolution_ts_ms,
        "source_ts_ms": source_ts_ms,
        "availability_ts_ms": int(now_ms),
        "affected_assets": list(spec.affected_assets),
        "source_file_kind": "ibkr_market_data",
        "refresh_cadence": "snapshot",
        "provider_timestamp_ms": source_ts_ms,
        "raw_payload_hash": raw_payload_hash(raw),
        "raw_payload": raw,
    }


def fetch_ibkr_event_contract_batch(
    *,
    settings: Mapping[str, Any] | None = None,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Fetch allowed IBKR event-contract market data without account/order calls."""

    now_value = int(now_ms or time.time() * 1000)
    settings_map = dict(settings or {})
    enabled = ibkr_event_contracts_enabled(settings_map)
    allowlist = _parse_allowlist(settings_map.get("ibkr_contract_allowlist") or settings_map.get("ibkr_event_contract_allowlist"))
    health: dict[str, Any] = {
        "provider": IBKR_EVENT_CONTRACT_PROVIDER_NAME,
        "enabled": bool(enabled),
        "ok": True,
        "read_only": True,
        "direct_trading_authority": False,
        "contracts_requested": int(len(allowlist)),
        "rows_parsed": 0,
        "rows_skipped": 0,
        "parse_error_count": 0,
    }
    if not enabled:
        health["status"] = "disabled"
        return {"events": [], "markets": [], "orderbooks": [], "trades": [], "health": health}
    if not allowlist:
        health.update({"ok": False, "status": "allowlist_required", "parse_error_count": 1})
        return {"events": [], "markets": [], "orderbooks": [], "trades": [], "health": health}
    if _IBAPI_IMPORT_ERROR is not None:
        health.update({"ok": False, "status": "ibapi_unavailable", "error": str(_IBAPI_IMPORT_ERROR)[:500], "parse_error_count": 1})
        return {"events": [], "markets": [], "orderbooks": [], "trades": [], "health": health}

    specs = [_contract_spec(row, settings=settings_map) for row in allowlist]
    host = str(settings_map.get("ibkr_host") or settings_map.get("host") or default_ibkr_host()).strip()
    port = int(str(settings_map.get("ibkr_port") or settings_map.get("port") or "7497").strip())
    client_id = int(str(settings_map.get("ibkr_client_id") or settings_map.get("client_id") or "44").strip())
    timeout_s = max(1.0, safe_float(settings_map.get("ibkr_timeout_s") or settings_map.get("timeout_s"), 5.0))
    event_q: "queue.Queue[dict[str, Any]]" = queue.Queue()
    wrapper = _MarketDataWrapper(event_q)
    client = _MarketDataClient(wrapper)
    thread: threading.Thread | None = None
    snapshots: dict[str, dict[str, Any]] = {spec.conid: {"conid": spec.conid} for spec in specs}
    try:
        client.connect(host, port, client_id)
        if not bool(client.isConnected()):
            raise RuntimeError("ibkr_event_contract_connect_failed")
        thread = threading.Thread(target=client.run, name="ibkr_event_contract_market_data", daemon=True)
        thread.start()
        try:
            client.reqMarketDataType(int(str(settings_map.get("ibkr_market_data_type") or settings_map.get("market_data_type") or "1")))
        except Exception:
            pass
        for idx, spec in enumerate(specs, start=1):
            wrapper.req_specs[int(idx)] = spec
            client.reqMktData(int(idx), _build_contract(spec), "", True, False, [])
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                event = event_q.get(timeout=max(0.05, min(0.25, deadline - time.time())))
            except queue.Empty:
                continue
            if str(event.get("event") or "") == "error":
                if int(event.get("error_code") or 0) not in {2104, 2106, 2158}:
                    health.setdefault("errors", []).append(dict(event))
                continue
            conid = str(event.get("conid") or "")
            if conid and conid in snapshots:
                snapshots[conid].update({k: v for k, v in event.items() if k not in {"event", "conid"}})
    except Exception as exc:
        health.update({"ok": False, "status": "failed", "error": str(exc)[:500], "parse_error_count": 1})
    finally:
        for idx in range(1, len(specs) + 1):
            try:
                client.cancelMktData(int(idx))
            except Exception:
                pass
        try:
            client.disconnect()
        except Exception:
            pass
        if thread is not None and thread.is_alive():
            try:
                thread.join(timeout=1.0)
            except Exception:
                pass

    markets = []
    for spec in specs:
        market = _market_record_from_snapshot(spec, snapshots.get(spec.conid) or {}, now_ms=now_value)
        if market is None:
            health["rows_skipped"] = safe_int(health.get("rows_skipped"), 0) + 1
            continue
        markets.append(market)
        health["rows_parsed"] = safe_int(health.get("rows_parsed"), 0) + 1
    health["status"] = "ok" if bool(health.get("ok")) else str(health.get("status") or "failed")
    return {"events": [], "markets": markets, "orderbooks": [], "trades": [], "health": health}
