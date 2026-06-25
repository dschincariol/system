"""
FILE: provider_registry.py

Data subsystem module for `provider_registry`.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from engine.data._credentials import get_data_credential
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("engine.data.provider_registry")
_WARNED_NONFATAL_KEYS: set[str] = set()
_MARKET_DATA_JOB_NAMES = ("stream_prices_polygon_ws", "stream_prices_ibkr", "poll_prices", "options_poll")


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
        component="engine.data.provider_registry",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

@dataclass(frozen=True)
class PriceProviderDefinition:
    provider_name: str
    mode: str
    implementation_kind: str
    enabled: bool = True
    daemon_job_name: Optional[str] = None
    daemon_script: Optional[str] = None
    priority: int = 100
    supports: Optional[Dict[str, Any]] = None
    build_price_provider: Optional[Callable[[], Any]] = None


def _builtin_provider_definitions() -> Dict[str, PriceProviderDefinition]:
    def _env_enabled(name: str, default: bool = True) -> bool:
        raw = os.environ.get(str(name), "")
        if raw is None or str(raw).strip() == "":
            return bool(default)
        return str(raw).strip().lower() in ("1", "true", "yes", "on")

    def _build_polygon():
        from engine.data.live_prices.polygon_live import PolygonPriceProvider
        return PolygonPriceProvider()

    def _build_ccxt():
        from engine.data.live_prices.ccxt_live import CCXTPriceProvider
        return CCXTPriceProvider()

    def _build_yfinance():
        from engine.data.live_prices.yfinance_live import YFinancePriceProvider
        return YFinancePriceProvider()

    def _build_simulated():
        from engine.data.live_prices.simulated import SimulatedPriceProvider
        return SimulatedPriceProvider()

    def _build_ibkr():
        from engine.data.live_prices.ibkr_live import IBKRPriceProvider
        return IBKRPriceProvider()

    def _build_oanda():
        from engine.data.live_prices.oanda_live import OANDAPriceProvider
        return OANDAPriceProvider()

    def _build_futures():
        from engine.data.live_prices.futures_live import FuturesPriceProvider
        return FuturesPriceProvider()

    # Built-ins define the canonical provider catalog. Dynamic plugins can
    # override or extend this catalog, but these entries are the baseline
    # control-plane expectation for market-data jobs.
    defs = [
        PriceProviderDefinition(
            provider_name="polygon_ws",
            mode="streaming",
            implementation_kind="daemon",
            enabled=_env_enabled("POLYGON_WS_ENABLED", True),
            daemon_job_name="stream_prices_polygon_ws",
            daemon_script="engine/jobs/stream_prices_polygon_ws.py",
            priority=10,
            supports={"asset_classes": ["equities"], "transport": "websocket"},
            build_price_provider=_build_polygon,
        ),
        PriceProviderDefinition(
            provider_name="ibkr",
            mode="streaming",
            implementation_kind="daemon",
            enabled=_env_enabled("IBKR_ENABLED", False),
            daemon_job_name="stream_prices_ibkr",
            daemon_script="engine/data/providers/ibkr/daemon_stream.py",
            priority=20,
            supports={"asset_classes": ["equities", "fx", "futures"], "transport": "gateway"},
            build_price_provider=_build_ibkr,
        ),
        PriceProviderDefinition(
            provider_name="polygon",
            mode="polling",
            implementation_kind="live_price_provider",
            enabled=_env_enabled("POLYGON_REST_ENABLED", True),
            daemon_job_name="poll_prices",
            daemon_script="engine/data/poll_prices.py",
            priority=30,
            supports={"asset_classes": ["equities", "options"], "transport": "rest"},
            build_price_provider=_build_polygon,
        ),
        PriceProviderDefinition(
            provider_name="tradier",
            mode="polling",
            implementation_kind="options_chain_provider",
            enabled=_env_enabled("TRADIER_ENABLED", True),
            daemon_job_name="options_poll",
            daemon_script="engine/data/options_poll.py",
            priority=35,
            supports={"asset_classes": ["options"], "transport": "rest"},
        ),
        PriceProviderDefinition(
            provider_name="yfinance",
            mode="polling",
            implementation_kind="live_price_provider",
            enabled=_env_enabled("YFINANCE_ENABLED", True),
            daemon_job_name="poll_prices",
            daemon_script="engine/data/poll_prices.py",
            priority=40,
            supports={"asset_classes": ["equities"], "transport": "rest"},
            build_price_provider=_build_yfinance,
        ),
        PriceProviderDefinition(
            provider_name="simulated",
            mode="polling",
            implementation_kind="live_price_provider",
            enabled=_simulated_market_data_enabled(),
            daemon_job_name="poll_prices",
            daemon_script="engine/data/poll_prices.py",
            priority=42,
            supports={"asset_classes": ["equities", "fx", "crypto"], "transport": "local", "simulated": True},
            build_price_provider=_build_simulated,
        ),
        PriceProviderDefinition(
            provider_name="oanda",
            mode="polling",
            implementation_kind="live_price_provider",
            enabled=_env_enabled("OANDA_ENABLED", False),
            daemon_job_name="poll_prices",
            daemon_script="engine/data/poll_prices.py",
            priority=45,
            supports={"asset_classes": ["fx"], "transport": "rest"},
            build_price_provider=_build_oanda,
        ),
        PriceProviderDefinition(
            provider_name="futures",
            mode="polling",
            implementation_kind="live_price_provider",
            enabled=_env_enabled("FUTURES_ENABLED", False),
            daemon_job_name="poll_prices",
            daemon_script="engine/data/poll_prices.py",
            priority=47,
            supports={"asset_classes": ["futures"], "transport": "rest"},
            build_price_provider=_build_futures,
        ),
        PriceProviderDefinition(
            provider_name="ccxt",
            mode="polling",
            implementation_kind="live_price_provider",
            enabled=_env_enabled("CCXT_ENABLED", True),
            daemon_job_name="poll_prices",
            daemon_script="engine/data/poll_prices.py",
            priority=50,
            supports={"asset_classes": ["crypto"], "transport": "rest"},
            build_price_provider=_build_ccxt,
        ),
    ]
    return {d.provider_name: d for d in defs}

def _providers_dir() -> Path:
    return Path(__file__).resolve().parent / "providers"


def _load_plugin_module(path: Path):
    rel = path.relative_to(_providers_dir()).with_suffix("")
    module_name = "engine.data.providers." + ".".join(rel.parts)
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"provider_plugin_spec_error:{path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _dynamic_provider_definitions() -> Dict[str, PriceProviderDefinition]:
    out: Dict[str, PriceProviderDefinition] = {}
    providers_dir = _providers_dir()
    if not providers_dir.exists():
        return out

    candidate_paths = sorted(providers_dir.glob("*.py")) + sorted(providers_dir.glob("*/provider.py"))

    # Dynamic providers are loaded from engine/data/providers so deployments can
    # extend the provider set without editing the registry core.
    for path in candidate_paths:
        if path.name.startswith("_") or path.name == "__init__.py":
            continue
        try:
            module = _load_plugin_module(path)
        except Exception as e:
            _warn_nonfatal("PROVIDER_REGISTRY_PLUGIN_LOAD_FAILED", e, once_key=f"plugin_load_{path}", path=str(path))
            continue

        raw = getattr(module, "PROVIDER_DEFINITION", None)
        if not isinstance(raw, dict):
            raw = getattr(module, "PROVIDER", None)
        if not isinstance(raw, dict):
            continue

        name = str(raw.get("provider_name") or raw.get("name") or path.parent.name).strip().lower()
        if not name:
            continue

        builder = getattr(module, "build_price_provider", None)
        if builder is None:
            builder = getattr(module, "build_provider", None)
        if builder is not None and not callable(builder):
            builder = None

        mode = str(raw.get("mode") or "polling").strip().lower()
        default_implementation_kind = "daemon" if mode == "streaming" else "live_price_provider"
        implementation_kind = str(raw.get("implementation_kind") or default_implementation_kind).strip().lower()
        daemon_job_name = raw.get("daemon_job_name") or raw.get("daemon")
        daemon_script = raw.get("daemon_script")

        if name == "ibkr":
            if not daemon_job_name:
                daemon_job_name = "stream_prices_ibkr"
            if not daemon_script:
                daemon_script = "engine/data/providers/ibkr/daemon_stream.py"

        out[name] = PriceProviderDefinition(
            provider_name=name,
            mode=mode,
            implementation_kind=implementation_kind,
            enabled=bool(raw.get("enabled", True)),
            daemon_job_name=(str(daemon_job_name).strip() if daemon_job_name else None),
            daemon_script=(str(daemon_script).strip() if daemon_script else None),
            priority=int(raw.get("priority") or 100),
            supports=dict(raw.get("supports") or {}),
            build_price_provider=builder,
        )
    return out


def list_provider_definitions() -> List[PriceProviderDefinition]:
    merged = _builtin_provider_definitions()

    # Dynamic definitions win field-by-field so plugins can replace built-in
    # behavior while inheriting defaults they do not explicitly set.
    for name, dynamic_definition in _dynamic_provider_definitions().items():
        builtin_definition = merged.get(name)
        if builtin_definition is None:
            merged[name] = dynamic_definition
            continue

        merged[name] = PriceProviderDefinition(
            provider_name=str(dynamic_definition.provider_name or builtin_definition.provider_name),
            mode=str(dynamic_definition.mode or builtin_definition.mode),
            implementation_kind=str(dynamic_definition.implementation_kind or builtin_definition.implementation_kind),
            enabled=bool(dynamic_definition.enabled),
            daemon_job_name=(dynamic_definition.daemon_job_name or builtin_definition.daemon_job_name),
            daemon_script=(dynamic_definition.daemon_script or builtin_definition.daemon_script),
            priority=int(dynamic_definition.priority),
            supports=(dynamic_definition.supports or builtin_definition.supports),
            build_price_provider=(dynamic_definition.build_price_provider or builtin_definition.build_price_provider),
        )

    try:
        from services.data_source_manager import inject_provider_registry

        for name, override in (inject_provider_registry() or {}).items():
            builtin_definition = merged.get(name)
            if builtin_definition is None:
                continue
            merged[name] = PriceProviderDefinition(
                provider_name=str(builtin_definition.provider_name),
                mode=str(builtin_definition.mode),
                implementation_kind=str(builtin_definition.implementation_kind),
                enabled=bool(override.get("enabled", builtin_definition.enabled)),
                daemon_job_name=builtin_definition.daemon_job_name,
                daemon_script=builtin_definition.daemon_script,
                priority=int(builtin_definition.priority),
                supports=builtin_definition.supports,
                build_price_provider=builtin_definition.build_price_provider,
            )
    except Exception as e:
        _warn_nonfatal(
            "PROVIDER_REGISTRY_INJECT_OVERRIDE_FAILED",
            e,
            once_key="provider_registry_inject_override",
        )

    return sorted(merged.values(), key=lambda d: (int(d.priority), d.provider_name))


def get_provider_definition(provider_name: str) -> Optional[PriceProviderDefinition]:
    name = str(provider_name or "").strip().lower()
    if not name:
        return None
    for definition in list_provider_definitions():
        if definition.provider_name == name:
            return definition
    return None


def get_polling_provider_names() -> List[str]:
    return [
        d.provider_name
        for d in list_provider_definitions()
        if d.enabled and d.mode == "polling" and d.implementation_kind == "live_price_provider"
    ]


def get_market_data_job_names() -> List[str]:
    seen = set()
    out: List[str] = []
    for d in list_provider_definitions():
        if not d.enabled or not d.daemon_job_name:
            continue
        if d.daemon_job_name in seen:
            continue
        seen.add(d.daemon_job_name)
        out.append(d.daemon_job_name)
    if "poll_prices" not in seen:
        out.append("poll_prices")
    return out


def _env_enabled(name: str, default: bool = False) -> bool:
    raw_value = os.environ.get(str(name), "")
    if raw_value is None or str(raw_value).strip() == "":
        return bool(default)
    return str(raw_value).strip().lower() in ("1", "true", "yes", "on")


def _simulated_market_data_enabled() -> bool:
    raw = os.environ.get("SIMULATED_MARKET_DATA_ENABLED")
    if raw is not None and str(raw).strip() != "":
        return str(raw).strip().lower() in ("1", "true", "yes", "on")
    mode = str(os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower()
    execution_mode = str(os.environ.get("EXECUTION_MODE", "safe") or "safe").strip().lower()
    broker = str(os.environ.get("BROKER", "sim") or "sim").strip().lower()
    broker_name = str(os.environ.get("BROKER_NAME", broker) or broker).strip().lower()
    return bool(
        mode in {"safe", "sim", "simulation", "test"}
        and execution_mode in {"safe", "sim", "simulation", "sim-paper", "sim_paper", "paper"}
        and broker == "sim"
        and broker_name == "sim"
    )


def _provider_chain() -> list[str]:
    return [
        x.strip().lower()
        for x in str(os.environ.get("LIVE_PRICE_PROVIDER_CHAIN", "") or "").split(",")
        if x.strip()
    ]


def _operational_market_data_job_names(candidates: list[str]) -> list[str]:
    chain = _provider_chain()
    polygon_key = bool(get_data_credential("POLYGON_API_KEY"))
    tradier_key = bool(get_data_credential("TRADIER_API_TOKEN"))
    polygon_ws_enabled = _env_enabled("POLYGON_WS_ENABLED", True)
    polygon_rest_enabled = _env_enabled("POLYGON_REST_ENABLED", True)
    ibkr_enabled = _env_enabled("IBKR_ENABLED", False)
    yfinance_enabled = _env_enabled("YFINANCE_ENABLED", True)
    simulated_enabled = _simulated_market_data_enabled()
    oanda_enabled = _env_enabled("OANDA_ENABLED", False)
    oanda_key = bool(get_data_credential("OANDA_ACCESS_TOKEN") or get_data_credential("OANDA_API_KEY"))
    futures_enabled = _env_enabled("FUTURES_ENABLED", False)
    futures_key = bool(get_data_credential("DATABENTO_API_KEY"))
    ccxt_enabled = _env_enabled("CCXT_ENABLED", False)
    tradier_enabled = _env_enabled("TRADIER_ENABLED", False)

    out: list[str] = []
    for raw_name in candidates:
        name = str(raw_name or "").strip()
        if name not in _MARKET_DATA_JOB_NAMES or name in out:
            continue
        if name == "stream_prices_polygon_ws":
            if polygon_key and polygon_ws_enabled and ((not chain) or ("polygon_ws" in chain)):
                out.append(name)
            continue
        if name == "stream_prices_ibkr":
            if ibkr_enabled:
                out.append(name)
            continue
        if name == "options_poll":
            polygon_options = polygon_key and polygon_rest_enabled and ((not chain) or ("polygon" in chain))
            tradier_options = tradier_key and tradier_enabled and ((not chain) or ("tradier" in chain))
            if polygon_options or tradier_options:
                out.append(name)
            continue
        if name == "poll_prices":
            if (
                yfinance_enabled
                or simulated_enabled
                or ccxt_enabled
                or (oanda_key and oanda_enabled and ((not chain) or ("oanda" in chain)))
                or (futures_key and futures_enabled and ((not chain) or ("futures" in chain)))
                or (polygon_key and polygon_rest_enabled and ((not chain) or ("polygon" in chain)))
            ):
                out.append(name)
    return out


def _warn_paid_equity_downgrade_if_needed(
    out: List[str],
    *,
    polygon_key: bool,
    polygon_ws_enabled: bool,
    polygon_rest_enabled: bool,
    ibkr_enabled: bool,
) -> None:
    configured_polygon = bool(polygon_key) and bool(polygon_ws_enabled or polygon_rest_enabled)
    configured_paid_equity = bool(configured_polygon or ibkr_enabled)
    paid_job_present = any(
        str(job) in {"stream_prices_polygon_ws", "stream_prices_ibkr"}
        for job in list(out or [])
    )
    free_fallback_only = list(out or []) == ["poll_prices"]
    if not configured_paid_equity or paid_job_present or not free_fallback_only:
        return
    _warn_nonfatal(
        "PROVIDER_REGISTRY_PAID_EQUITY_DOWNGRADE",
        RuntimeError("configured paid equity provider dropped to free fallback"),
        once_key="paid_equity_downgrade",
        configured_polygon=bool(configured_polygon),
        polygon_enabled=bool(polygon_ws_enabled or polygon_rest_enabled),
        polygon_ws_enabled=bool(polygon_ws_enabled),
        polygon_rest_enabled=bool(polygon_rest_enabled),
        ibkr_enabled=bool(ibkr_enabled),
        returned_jobs=list(out or []),
    )


def get_enabled_market_data_job_names() -> List[str]:
    raw = [x.strip() for x in os.environ.get("INGESTION_CHILD_JOBS", "").split(",") if x.strip()]
    if raw:
        # Explicit env override is the highest-priority operational control,
        # but provider disable flags and missing credentials still fail closed.
        filtered = _operational_market_data_job_names(raw)
        out = filtered or ["poll_prices"]
        _warn_paid_equity_downgrade_if_needed(
            out,
            polygon_key=bool(get_data_credential("POLYGON_API_KEY")),
            polygon_ws_enabled=_env_enabled("POLYGON_WS_ENABLED", True),
            polygon_rest_enabled=_env_enabled("POLYGON_REST_ENABLED", True),
            ibkr_enabled=_env_enabled("IBKR_ENABLED", False),
        )
        return out

    try:
        from services.data_source_manager import desired_ingestion_jobs

        desired = _operational_market_data_job_names([
            str(name)
            for name in (desired_ingestion_jobs(read_only=True) or [])
            if str(name).strip() in _MARKET_DATA_JOB_NAMES
        ])
        if desired:
            out = list(dict.fromkeys(desired))
            _warn_paid_equity_downgrade_if_needed(
                out,
                polygon_key=bool(get_data_credential("POLYGON_API_KEY")),
                polygon_ws_enabled=_env_enabled("POLYGON_WS_ENABLED", True),
                polygon_rest_enabled=_env_enabled("POLYGON_REST_ENABLED", True),
                ibkr_enabled=_env_enabled("IBKR_ENABLED", False),
            )
            return out
    except Exception as e:
        _warn_nonfatal(
            "PROVIDER_REGISTRY_DESIRED_JOBS_PARSE_FAILED",
            e,
            once_key="provider_registry_desired_jobs_parse",
        )

    chain = _provider_chain()
    polygon_key = get_data_credential("POLYGON_API_KEY")
    polygon_ws_enabled = _env_enabled("POLYGON_WS_ENABLED", True)
    polygon_rest_enabled = _env_enabled("POLYGON_REST_ENABLED", True)
    ibkr_enabled = _env_enabled("IBKR_ENABLED", False)
    oanda_enabled = _env_enabled("OANDA_ENABLED", False)
    oanda_key = get_data_credential("OANDA_ACCESS_TOKEN") or get_data_credential("OANDA_API_KEY")
    futures_enabled = _env_enabled("FUTURES_ENABLED", False)
    futures_key = get_data_credential("DATABENTO_API_KEY")
    simulated_enabled = _simulated_market_data_enabled()

    out: List[str] = []

    if polygon_key and polygon_ws_enabled and ((not chain) or ("polygon_ws" in chain)):
        out.append("stream_prices_polygon_ws")
        if "poll_prices" not in out:
            out.append("poll_prices")
        return out

    if polygon_key and polygon_rest_enabled and ((not chain) or ("polygon" in chain)):
        out.append("poll_prices")

    if oanda_key and oanda_enabled and ((not chain) or ("oanda" in chain)):
        if "poll_prices" not in out:
            out.append("poll_prices")

    if futures_key and futures_enabled and ((not chain) or ("futures" in chain)):
        if "poll_prices" not in out:
            out.append("poll_prices")

    if ibkr_enabled:
        out.append("stream_prices_ibkr")
        if "poll_prices" not in out:
            out.append("poll_prices")
        return out

    if not out:
        out.append("poll_prices")
    elif simulated_enabled and "poll_prices" not in out:
        out.append("poll_prices")

    _warn_paid_equity_downgrade_if_needed(
        out,
        polygon_key=bool(polygon_key),
        polygon_ws_enabled=bool(polygon_ws_enabled),
        polygon_rest_enabled=bool(polygon_rest_enabled),
        ibkr_enabled=bool(ibkr_enabled),
    )
    return out


def build_price_provider(provider_name: str):
    definition = get_provider_definition(provider_name)
    if definition is None:
        raise RuntimeError(f"Unknown live price provider: {provider_name}")
    if definition.build_price_provider is None:
        raise RuntimeError(f"provider_builder_unavailable:{definition.provider_name}")
    return definition.build_price_provider()
