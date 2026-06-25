"""Futures instrument semantics and canonical symbol parsing.

This module is intentionally pure: it performs no I/O, DB access, network
access, broker access, or order routing. It recognizes only explicit futures
contract forms:

* Continuous aliases: ``<ROOT>.c.<N>``, such as ``ES.c.0``.
* Dated contracts: ``<ROOT><MONTHCODE><YY>``, such as ``ESZ26``.

Bare roots such as ``ES``, ``GC``, ``CL``, and ``ZN`` intentionally return
``None`` so existing commodity/rates/COT-root behavior is not reclassified.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import re

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("data.futures_instrument")

FUTURES_FAIL_CLOSED_MARGIN_REF = 1_000_000_000.0
FUTURES_DEFAULT_ROLL_METHOD = "oi_volume"
FUTURES_MONTH_CODES = frozenset("FGHJKMNQUVXZ")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="data_futures_instrument_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.futures_instrument",
        extra=dict(extra or {}) or None,
        persist=False,
    )


@dataclass(frozen=True)
class FuturesContractMetadata:
    symbol: str
    asset_class: str
    instrument_kind: str
    root: str
    exchange: str
    multiplier: float
    tick_size: float
    tick_value: float
    price_ccy: str
    margin_ref: float
    expiry_rule: str
    roll_method: str
    continuous_alias: str | None
    session_calendar: str
    source: str

    def to_dict(self) -> dict:
        data = {
            "asset_class": self.asset_class,
            "continuous_alias": self.continuous_alias,
            "exchange": self.exchange,
            "expiry_rule": self.expiry_rule,
            "instrument_kind": self.instrument_kind,
            "margin_ref": float(self.margin_ref),
            "multiplier": float(self.multiplier),
            "price_ccy": self.price_ccy,
            "roll_method": self.roll_method,
            "root": self.root,
            "session_calendar": self.session_calendar,
            "source": self.source,
            "symbol": self.symbol,
            "tick_size": float(self.tick_size),
            "tick_value": float(self.tick_value),
        }
        return {key: data[key] for key in sorted(data)}


def _spec(
    *,
    exchange: str,
    multiplier: float,
    tick_size: float,
    tick_value: float,
    price_ccy: str = "USD",
    settlement_type: str,
    expiry_rule: str,
    session_calendar: str,
    micro: bool = False,
) -> dict:
    return {
        "exchange": exchange,
        "expiry_rule": expiry_rule,
        # FUT-07 owns live margin enforcement. This deliberately high reference
        # fails closed if anything consumes it before broker/exchange margins are refreshed.
        "margin_ref": FUTURES_FAIL_CLOSED_MARGIN_REF,
        "micro": bool(micro),
        "multiplier": float(multiplier),
        "price_ccy": price_ccy,
        "session_calendar": session_calendar,
        "settlement_type": settlement_type,
        "tick_size": float(tick_size),
        "tick_value": float(tick_value),
    }


FUTURES_ROOT_SPECS = {
    "ES": _spec(
        exchange="CME",
        multiplier=50.0,
        tick_size=0.25,
        tick_value=12.50,
        settlement_type="cash",
        expiry_rule="quarterly_index_cash_settlement",
        session_calendar="CME_EQUITY",
    ),
    "MES": _spec(
        exchange="CME",
        multiplier=5.0,
        tick_size=0.25,
        tick_value=1.25,
        settlement_type="cash",
        expiry_rule="quarterly_index_cash_settlement",
        session_calendar="CME_EQUITY",
        micro=True,
    ),
    "NQ": _spec(
        exchange="CME",
        multiplier=20.0,
        tick_size=0.25,
        tick_value=5.0,
        settlement_type="cash",
        expiry_rule="quarterly_index_cash_settlement",
        session_calendar="CME_EQUITY",
    ),
    "MNQ": _spec(
        exchange="CME",
        multiplier=2.0,
        tick_size=0.25,
        tick_value=0.50,
        settlement_type="cash",
        expiry_rule="quarterly_index_cash_settlement",
        session_calendar="CME_EQUITY",
        micro=True,
    ),
    "RTY": _spec(
        exchange="CME",
        multiplier=50.0,
        tick_size=0.10,
        tick_value=5.0,
        settlement_type="cash",
        expiry_rule="quarterly_index_cash_settlement",
        session_calendar="CME_EQUITY",
    ),
    "M2K": _spec(
        exchange="CME",
        multiplier=5.0,
        tick_size=0.10,
        tick_value=0.50,
        settlement_type="cash",
        expiry_rule="quarterly_index_cash_settlement",
        session_calendar="CME_EQUITY",
        micro=True,
    ),
    "YM": _spec(
        exchange="CBOT",
        multiplier=5.0,
        tick_size=1.0,
        tick_value=5.0,
        settlement_type="cash",
        expiry_rule="quarterly_index_cash_settlement",
        session_calendar="CME_EQUITY",
    ),
    "MYM": _spec(
        exchange="CBOT",
        multiplier=0.50,
        tick_size=1.0,
        tick_value=0.50,
        settlement_type="cash",
        expiry_rule="quarterly_index_cash_settlement",
        session_calendar="CME_EQUITY",
        micro=True,
    ),
    "CL": _spec(
        exchange="NYMEX",
        multiplier=1000.0,
        tick_size=0.01,
        tick_value=10.0,
        settlement_type="physical",
        expiry_rule="monthly_physical_delivery",
        session_calendar="CME_GLOBEX_24x5",
    ),
    "MCL": _spec(
        exchange="NYMEX",
        multiplier=100.0,
        tick_size=0.01,
        tick_value=1.0,
        settlement_type="physical",
        expiry_rule="monthly_physical_delivery",
        session_calendar="CME_GLOBEX_24x5",
        micro=True,
    ),
    "NG": _spec(
        exchange="NYMEX",
        multiplier=10000.0,
        tick_size=0.001,
        tick_value=10.0,
        settlement_type="physical",
        expiry_rule="monthly_physical_delivery",
        session_calendar="CME_GLOBEX_24x5",
    ),
    "MNG": _spec(
        exchange="NYMEX",
        multiplier=1000.0,
        tick_size=0.001,
        tick_value=1.0,
        settlement_type="cash",
        expiry_rule="monthly_cash_settlement",
        session_calendar="CME_GLOBEX_24x5",
        micro=True,
    ),
    "GC": _spec(
        exchange="COMEX",
        multiplier=100.0,
        tick_size=0.10,
        tick_value=10.0,
        settlement_type="physical",
        expiry_rule="listed_months_physical_delivery",
        session_calendar="CME_GLOBEX_24x5",
    ),
    "MGC": _spec(
        exchange="COMEX",
        multiplier=10.0,
        tick_size=0.10,
        tick_value=1.0,
        settlement_type="physical",
        expiry_rule="listed_months_physical_delivery",
        session_calendar="CME_GLOBEX_24x5",
        micro=True,
    ),
    "SI": _spec(
        exchange="COMEX",
        multiplier=5000.0,
        tick_size=0.005,
        tick_value=25.0,
        settlement_type="physical",
        expiry_rule="listed_months_physical_delivery",
        session_calendar="CME_GLOBEX_24x5",
    ),
    "SIL": _spec(
        exchange="COMEX",
        multiplier=1000.0,
        tick_size=0.005,
        tick_value=5.0,
        settlement_type="physical",
        expiry_rule="listed_months_physical_delivery",
        session_calendar="CME_GLOBEX_24x5",
        micro=True,
    ),
    "HG": _spec(
        exchange="COMEX",
        multiplier=25000.0,
        tick_size=0.0005,
        tick_value=12.50,
        settlement_type="physical",
        expiry_rule="listed_months_physical_delivery",
        session_calendar="CME_GLOBEX_24x5",
    ),
    "MHG": _spec(
        exchange="COMEX",
        multiplier=2500.0,
        tick_size=0.0005,
        tick_value=1.25,
        settlement_type="cash",
        expiry_rule="listed_months_cash_settlement",
        session_calendar="CME_GLOBEX_24x5",
        micro=True,
    ),
    "ZB": _spec(
        exchange="CBOT",
        multiplier=100000.0,
        tick_size=0.03125,
        tick_value=31.25,
        settlement_type="deliverable",
        expiry_rule="quarterly_deliverable_treasury",
        session_calendar="CME_GLOBEX_24x5",
    ),
    "ZN": _spec(
        exchange="CBOT",
        multiplier=100000.0,
        tick_size=0.015625,
        tick_value=15.625,
        settlement_type="deliverable",
        expiry_rule="quarterly_deliverable_treasury",
        session_calendar="CME_GLOBEX_24x5",
    ),
    "ZF": _spec(
        exchange="CBOT",
        multiplier=100000.0,
        tick_size=0.0078125,
        tick_value=7.8125,
        settlement_type="deliverable",
        expiry_rule="quarterly_deliverable_treasury",
        session_calendar="CME_GLOBEX_24x5",
    ),
    "ZT": _spec(
        exchange="CBOT",
        multiplier=200000.0,
        tick_size=0.00390625,
        tick_value=7.8125,
        settlement_type="deliverable",
        expiry_rule="quarterly_deliverable_treasury",
        session_calendar="CME_GLOBEX_24x5",
    ),
    "ZC": _spec(
        exchange="CBOT",
        multiplier=5000.0,
        tick_size=0.0025,
        tick_value=12.50,
        settlement_type="physical",
        expiry_rule="listed_months_physical_delivery",
        session_calendar="CME_AG_GLOBEX",
    ),
    "ZS": _spec(
        exchange="CBOT",
        multiplier=5000.0,
        tick_size=0.0025,
        tick_value=12.50,
        settlement_type="physical",
        expiry_rule="listed_months_physical_delivery",
        session_calendar="CME_AG_GLOBEX",
    ),
    "ZW": _spec(
        exchange="CBOT",
        multiplier=5000.0,
        tick_size=0.0025,
        tick_value=12.50,
        settlement_type="physical",
        expiry_rule="listed_months_physical_delivery",
        session_calendar="CME_AG_GLOBEX",
    ),
    "6E": _spec(
        exchange="CME",
        multiplier=125000.0,
        tick_size=0.00005,
        tick_value=6.25,
        settlement_type="physical",
        expiry_rule="quarterly_fx_delivery",
        session_calendar="CME_GLOBEX_24x5",
    ),
    "M6E": _spec(
        exchange="CME",
        multiplier=12500.0,
        tick_size=0.0001,
        tick_value=1.25,
        settlement_type="physical",
        expiry_rule="quarterly_fx_delivery",
        session_calendar="CME_GLOBEX_24x5",
        micro=True,
    ),
    "6J": _spec(
        exchange="CME",
        multiplier=12500000.0,
        tick_size=0.0000005,
        tick_value=6.25,
        settlement_type="physical",
        expiry_rule="quarterly_fx_delivery",
        session_calendar="CME_GLOBEX_24x5",
    ),
    "MJY": _spec(
        exchange="CME",
        multiplier=1250000.0,
        tick_size=0.000001,
        tick_value=1.25,
        settlement_type="financial",
        expiry_rule="quarterly_fx_cash_settlement",
        session_calendar="CME_GLOBEX_24x5",
        micro=True,
    ),
    "6B": _spec(
        exchange="CME",
        multiplier=62500.0,
        tick_size=0.0001,
        tick_value=6.25,
        settlement_type="physical",
        expiry_rule="quarterly_fx_delivery",
        session_calendar="CME_GLOBEX_24x5",
    ),
    "M6B": _spec(
        exchange="CME",
        multiplier=6250.0,
        tick_size=0.0001,
        tick_value=0.625,
        settlement_type="physical",
        expiry_rule="quarterly_fx_delivery",
        session_calendar="CME_GLOBEX_24x5",
        micro=True,
    ),
}

_ROOT_PATTERN = "|".join(re.escape(root) for root in sorted(FUTURES_ROOT_SPECS, key=len, reverse=True))
_CONTINUOUS_RE = re.compile(rf"^({_ROOT_PATTERN})\.C\.(\d+)$")
_DATED_RE = re.compile(rf"^({_ROOT_PATTERN})([FGHJKMNQUVXZ])(\d{{2}})$")


def _metadata(
    *,
    symbol: str,
    instrument_kind: str,
    root: str,
    continuous_alias: str | None,
) -> FuturesContractMetadata:
    spec = FUTURES_ROOT_SPECS[root]
    return FuturesContractMetadata(
        symbol=symbol,
        asset_class="FUTURES",
        instrument_kind=instrument_kind,
        root=root,
        exchange=str(spec["exchange"]),
        multiplier=float(spec["multiplier"]),
        tick_size=float(spec["tick_size"]),
        tick_value=float(spec["tick_value"]),
        price_ccy=str(spec["price_ccy"]),
        margin_ref=float(spec["margin_ref"]),
        expiry_rule=str(spec["expiry_rule"]),
        roll_method=FUTURES_DEFAULT_ROLL_METHOD,
        continuous_alias=continuous_alias,
        session_calendar=str(spec["session_calendar"]),
        source="parser",
    )


def parse_futures_symbol(symbol: object) -> FuturesContractMetadata | None:
    """Return futures metadata for explicit contract forms, else ``None``.

    Bare roots are intentionally not futures symbols here. For example,
    ``parse_futures_symbol("ES")`` and ``parse_futures_symbol("GC")`` return
    ``None`` to preserve existing commodity/rates/COT-root behavior, while
    ``parse_futures_symbol("ES.c.0")`` and ``parse_futures_symbol("ESZ26")``
    return metadata.
    """
    try:
        normalized = str(symbol or "").upper().strip()
        if not normalized:
            return None
        match = _CONTINUOUS_RE.match(normalized)
        if match:
            root, ordinal = match.groups()
            canonical = f"{root}.c.{int(ordinal)}"
            return _metadata(
                symbol=canonical,
                instrument_kind="fut_continuous",
                root=root,
                continuous_alias=canonical,
            )
        match = _DATED_RE.match(normalized)
        if match:
            root, month_code, year = match.groups()
            if month_code not in FUTURES_MONTH_CODES:
                return None
            canonical = f"{root}{month_code}{year}"
            return _metadata(
                symbol=canonical,
                instrument_kind="fut_dated",
                root=root,
                continuous_alias=None,
            )
        return None
    except Exception as exc:
        _warn_nonfatal(
            "FUTURES_INSTRUMENT_PARSE_FAILED",
            exc,
            symbol_preview=repr(symbol)[:120],
        )
        return None


def is_futures_symbol(symbol: object) -> bool:
    return parse_futures_symbol(symbol) is not None
