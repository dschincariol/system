"""Read-only broker data adapters for the data-source control plane.

This module is intentionally outside ``engine.execution``.  Data-source
connectivity tests may read broker account, position, and market-data surfaces,
but they must never gain access to broker order authority.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Mapping
from urllib.parse import urljoin, urlparse


LOG = logging.getLogger(__name__)
ALPACA_PAPER_BASE_URL = "https://paper-api.alpaca.markets"
ALPACA_LIVE_BASE_URL = "https://api.alpaca.markets"
ALLOW_LIVE_ALPACA_BROKER_DATA_ENV = "DATA_SOURCE_ALLOW_LIVE_ALPACA_BROKER_DATA"

ALPACA_READONLY_HTTP_METHODS = frozenset({"GET"})
ALPACA_READONLY_PATHS = frozenset(("/v2/account", "/v2/positions"))
ALPACA_READONLY_ENDPOINTS = (
    {"surface": "account", "path": "/v2/account", "params": None, "require_payload": True},
    {"surface": "positions", "path": "/v2/positions", "params": None, "require_payload": False},
)

IBKR_READONLY_CLIENT_METHODS = frozenset(
    (
        "connect",
        "disconnect",
        "isConnected",
        "qualifyContracts",
        "reqHistoricalData",
        "reqMarketDataType",
    )
)

FORBIDDEN_BROKER_DATA_OPERATION_NAMES = frozenset(
    (
        "apply_latest_portfolio_orders_live",
        "apply_new_portfolio_orders_router",
        "broker_apply_orders",
        "cancel_and_flatten",
        "cancel_order",
        "cancel_open_orders",
        "cancel_open_orders_for_broker",
        "cancelOrder",
        "flatten_positions",
        "flatten_positions_for_broker",
        "place_order",
        "placeOrder",
        "replace_limit_order",
        "submit_limit_order",
        "submit_market_order",
        "submit_order",
    )
)

BROKER_DATA_SOURCE_ALLOWED_RUNTIME = {
    "alpaca_broker_data": {
        "provider_name": "alpaca",
        "source_type": "broker_data_provider",
        "runtime_runnable": False,
        "job_names": frozenset({"alpaca_broker_data_readonly"}),
    },
    "ibkr": {
        "provider_name": "ibkr",
        "source_type": "price_provider",
        "runtime_runnable": True,
        "job_names": frozenset({"stream_prices_ibkr"}),
    },
}


class BrokerDataReadOnlyViolation(RuntimeError):
    """Raised when broker data-source code attempts a forbidden operation."""


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(str(name), "") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def _normalized_path(path_or_url: Any) -> str:
    parsed = urlparse(str(path_or_url or ""))
    path = parsed.path if parsed.scheme or parsed.netloc else str(path_or_url or "")
    path = "/" + path.lstrip("/")
    return path.rstrip("/") or "/"


def _looks_like_forbidden_operation(name: Any) -> bool:
    lowered = str(name or "").strip()
    if lowered in FORBIDDEN_BROKER_DATA_OPERATION_NAMES:
        return True
    token = lowered.lower()
    return any(
        item in token
        for item in (
            "submit_order",
            "submitmarket",
            "submitlimit",
            "placeorder",
            "cancelorder",
            "cancel_open_orders",
            "flatten_positions",
            "replace_limit_order",
        )
    )


def assert_alpaca_readonly_request(method: Any, path_or_url: Any) -> str:
    """Validate an Alpaca broker-data request against the static allowlist."""

    method_s = str(method or "").strip().upper()
    path = _normalized_path(path_or_url)
    if method_s not in ALPACA_READONLY_HTTP_METHODS or path not in ALPACA_READONLY_PATHS:
        raise BrokerDataReadOnlyViolation(f"alpaca_broker_data_forbidden_request:{method_s}:{path}")
    return path


def assert_ibkr_readonly_method(method_name: Any) -> str:
    """Validate an IBKR client method against the static market-data allowlist."""

    method_s = str(method_name or "").strip()
    if method_s not in IBKR_READONLY_CLIENT_METHODS or _looks_like_forbidden_operation(method_s):
        raise BrokerDataReadOnlyViolation(f"ibkr_broker_data_forbidden_method:{method_s}")
    return method_s


def assert_data_source_broker_runtime_allowed(
    *,
    source_key: Any,
    source_type: Any,
    provider_name: Any,
    job_name: Any,
    runtime_runnable: bool,
) -> None:
    """Enforce that broker data-source rows cannot point at order jobs."""

    source_key_s = str(source_key or "").strip()
    provider_s = str(provider_name or "").strip().lower()
    source_type_s = str(source_type or "").strip().lower()
    job_s = str(job_name or "").strip()
    spec = BROKER_DATA_SOURCE_ALLOWED_RUNTIME.get(source_key_s)
    if spec is None and provider_s not in {"alpaca", "ibkr"} and source_type_s != "broker_data_provider":
        return
    if _looks_like_forbidden_operation(job_s):
        raise BrokerDataReadOnlyViolation(f"broker_data_source_forbidden_job:{source_key_s}:{job_s}")
    if spec is None:
        return
    if provider_s and provider_s != str(spec["provider_name"]):
        raise BrokerDataReadOnlyViolation(f"broker_data_source_provider_mismatch:{source_key_s}:{provider_s}")
    if source_type_s and source_type_s != str(spec["source_type"]):
        raise BrokerDataReadOnlyViolation(f"broker_data_source_type_mismatch:{source_key_s}:{source_type_s}")
    if bool(runtime_runnable) != bool(spec["runtime_runnable"]):
        raise BrokerDataReadOnlyViolation(f"broker_data_source_runtime_policy_mismatch:{source_key_s}")
    if job_s and job_s not in set(spec["job_names"]):
        raise BrokerDataReadOnlyViolation(f"broker_data_source_job_not_allowlisted:{source_key_s}:{job_s}")


def alpaca_base_url_policy(base_url: Any, *, allow_live: bool | None = None) -> Dict[str, Any]:
    """Return the data-source policy for an Alpaca broker-data base URL."""

    raw = str(base_url or "").strip() or ALPACA_PAPER_BASE_URL
    parsed = urlparse(raw)
    host = str(parsed.netloc or "").strip().lower()
    live_base_url = host == "api.alpaca.markets"
    paper_base_url = host == "paper-api.alpaca.markets"
    allowed = _env_flag(ALLOW_LIVE_ALPACA_BROKER_DATA_ENV, False) if allow_live is None else bool(allow_live)
    ok = bool(not live_base_url or allowed)
    return {
        "ok": ok,
        "base_url": raw.rstrip("/"),
        "safe_default": ALPACA_PAPER_BASE_URL,
        "paper_base_url": bool(paper_base_url),
        "live_base_url": bool(live_base_url),
        "live_base_url_allowed": bool(allowed),
        "policy": "allow" if ok else "block_live_base_url",
    }


def readonly_guard_snapshot() -> Dict[str, Any]:
    """Expose static guard policy for production diagnostics and tests."""

    return {
        "alpaca_allowed_http_methods": sorted(ALPACA_READONLY_HTTP_METHODS),
        "alpaca_allowed_paths": sorted(ALPACA_READONLY_PATHS),
        "ibkr_allowed_client_methods": sorted(IBKR_READONLY_CLIENT_METHODS),
        "forbidden_broker_operations": sorted(FORBIDDEN_BROKER_DATA_OPERATION_NAMES),
        "broker_data_source_allowed_runtime": {
            key: {
                "provider_name": str(value["provider_name"]),
                "source_type": str(value["source_type"]),
                "runtime_runnable": bool(value["runtime_runnable"]),
                "job_names": sorted(value["job_names"]),
            }
            for key, value in BROKER_DATA_SOURCE_ALLOWED_RUNTIME.items()
        },
    }


@dataclass(frozen=True)
class AlpacaBrokerDataSettings:
    """Settings accepted by the Alpaca broker-data read-only adapter."""

    base_url: str = ALPACA_PAPER_BASE_URL
    stream_url: str = ""
    trade_updates_ws_enabled: bool = False
    timeout_s: float = 10.0
    allow_live_base_url: bool | None = None


@dataclass(frozen=True)
class AlpacaReadOnlyProbe:
    """One Alpaca read-only HTTP response and its allowlist metadata."""

    surface: str
    path: str
    url: str
    params: Mapping[str, Any] | None
    require_payload: bool
    response: Any = field(repr=False)


class AlpacaBrokerDataReadOnlyClient:
    """Alpaca broker-data client exposing only read-only GET probes."""

    def __init__(
        self,
        *,
        key_id: str,
        secret_key: str,
        settings: AlpacaBrokerDataSettings | None = None,
        http_get: Callable[..., Any],
    ) -> None:
        self._key_id = str(key_id or "").strip()
        self._secret_key = str(secret_key or "").strip()
        self.settings = settings or AlpacaBrokerDataSettings()
        self._http_get = http_get

    def __getattr__(self, name: str) -> Any:
        if _looks_like_forbidden_operation(name):
            raise BrokerDataReadOnlyViolation(f"alpaca_broker_data_forbidden_attribute:{name}")
        raise AttributeError(name)

    def guard_evidence(self) -> Dict[str, Any]:
        policy = alpaca_base_url_policy(
            self.settings.base_url,
            allow_live=self.settings.allow_live_base_url,
        )
        return {
            "broker_data_readonly": True,
            "order_authority": False,
            "readonly_guard": readonly_guard_snapshot(),
            "base_url_policy": policy["policy"],
            "paper_base_url": bool(policy["paper_base_url"]),
            "live_base_url": bool(policy["live_base_url"]),
            "trade_updates_ws_observation": bool(self.settings.trade_updates_ws_enabled),
        }

    def base_url_policy(self) -> Dict[str, Any]:
        return alpaca_base_url_policy(
            self.settings.base_url,
            allow_live=self.settings.allow_live_base_url,
        )

    def _headers(self) -> Dict[str, str]:
        return {
            "APCA-API-KEY-ID": self._key_id,
            "APCA-API-SECRET-KEY": self._secret_key,
            "Accept": "application/json",
        }

    def get(self, path: str, *, params: Mapping[str, Any] | None = None) -> Any:
        clean_path = assert_alpaca_readonly_request("GET", path)
        policy = self.base_url_policy()
        if not bool(policy["ok"]):
            raise BrokerDataReadOnlyViolation("alpaca_live_base_url_blocked")
        url = urljoin(str(policy["base_url"]).rstrip("/") + "/", clean_path.lstrip("/"))
        return self._http_get(
            url,
            params=(dict(params) if params is not None else None),
            headers=self._headers(),
            timeout=float(self.settings.timeout_s),
        )

    def probe_account_positions(self) -> list[AlpacaReadOnlyProbe]:
        probes: list[AlpacaReadOnlyProbe] = []
        for endpoint in ALPACA_READONLY_ENDPOINTS:
            path = assert_alpaca_readonly_request("GET", endpoint["path"])
            policy = self.base_url_policy()
            if not bool(policy["ok"]):
                raise BrokerDataReadOnlyViolation("alpaca_live_base_url_blocked")
            url = urljoin(str(policy["base_url"]).rstrip("/") + "/", path.lstrip("/"))
            params = endpoint.get("params")
            response = self.get(path, params=params if isinstance(params, Mapping) else None)
            probes.append(
                AlpacaReadOnlyProbe(
                    surface=str(endpoint["surface"]),
                    path=path,
                    url=url,
                    params=params if isinstance(params, Mapping) else None,
                    require_payload=bool(endpoint["require_payload"]),
                    response=response,
                )
            )
        return probes

@dataclass(frozen=True)
class IBKRBrokerDataSettings:
    """Settings accepted by the IBKR broker-data read-only adapter."""

    host: str
    port: int
    client_id: int
    market_data_type: int = 1
    currency: str = "USD"
    timeout_s: float = 5.0


class IBKRBrokerDataReadOnlyClient:
    """IBKR client exposing only authenticated market-data probes."""

    def __init__(
        self,
        *,
        ib_factory: Callable[..., Any],
        stock_factory: Callable[..., Any],
        settings: IBKRBrokerDataSettings,
    ) -> None:
        self._ib_factory = ib_factory
        self._stock_factory = stock_factory
        self.settings = settings

    def __getattr__(self, name: str) -> Any:
        if _looks_like_forbidden_operation(name):
            raise BrokerDataReadOnlyViolation(f"ibkr_broker_data_forbidden_attribute:{name}")
        raise AttributeError(name)

    def guard_evidence(self) -> Dict[str, Any]:
        return {
            "broker_data_readonly": True,
            "order_authority": False,
            "readonly": True,
            "authenticated_read": True,
            "readonly_guard": readonly_guard_snapshot(),
        }

    def _call(self, ib: Any, method_name: str, *args: Any, **kwargs: Any) -> Any:
        method_s = assert_ibkr_readonly_method(method_name)
        method = getattr(ib, method_s)
        return method(*args, **kwargs)

    def probe_historical_data(self) -> Dict[str, Any]:
        ib = self._ib_factory()
        connected = False
        try:
            self._call(
                ib,
                "connect",
                self.settings.host,
                int(self.settings.port),
                clientId=int(self.settings.client_id),
                timeout=float(self.settings.timeout_s),
                readonly=True,
            )
            connected = True
            if hasattr(ib, "reqMarketDataType"):
                self._call(ib, "reqMarketDataType", int(self.settings.market_data_type))
            contract = self._stock_factory("SPY", "SMART", str(self.settings.currency or "USD"))
            qualified = self._call(ib, "qualifyContracts", contract)
            target = qualified[0] if qualified else contract
            bars = self._call(
                ib,
                "reqHistoricalData",
                target,
                endDateTime="",
                durationStr="1 D",
                barSizeSetting="1 hour",
                whatToShow="TRADES",
                useRTH=True,
                formatDate=1,
            )
            return {
                **self.guard_evidence(),
                "bars": bars,
                "payload_count": len(bars or []),
                "market_data_type": int(self.settings.market_data_type),
            }
        finally:
            try:
                if connected and bool(self._call(ib, "isConnected")):
                    self._call(ib, "disconnect")
            except Exception as exc:
                LOG.debug("ibkr_broker_data_disconnect_failed error_type=%s", type(exc).__name__)
