"""Option instrument semantics and canonical OCC symbol parsing.

This module is intentionally pure: it performs no I/O, DB access, network
access, broker access, or order routing. Later options workstreams should
consume these helpers instead of re-deriving OCC contract semantics.
"""

from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
import logging
import re

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("data.options_instrument")

OCC_COMPACT_RE = re.compile(r"^[A-Z]{1,6}\d{6}[CP]\d{8}$")
# OCC compact symbols prove root, expiry, right, and strike. These contract
# fields are operational parser defaults only, not externally verified specs.
OPTION_PARSER_DEFAULT_MULTIPLIER = 100.0
OPTION_PARSER_DEFAULT_EXERCISE_STYLE = "american"
OPTION_PARSER_DEFAULT_SETTLEMENT = "physical"
OPTION_PARSER_DEFAULT_PRICE_CCY = "USD"
OPTION_PARSER_DEFAULT_SESSION_CALENDAR = "US_EQUITY_OPTION"
OPTION_PARSER_DEFAULT_SOURCE = "parser_default_unverified"
OPTION_PARSER_DEFAULT_MULTIPLIER_SOURCE = "parser_default_unverified"
OPTION_PARSER_DEFAULT_SPECS_VERIFIED = False


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="data_options_instrument_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.options_instrument",
        extra=dict(extra or {}) or None,
        persist=False,
    )


@dataclass(frozen=True)
class OptionContractMetadata:
    occ_symbol: str
    asset_class: str
    instrument_kind: str
    underlying: str
    expiry: dt.date
    right: str
    strike: float
    multiplier: float
    exercise_style: str
    settlement: str
    price_ccy: str
    session_calendar: str
    source: str
    contract_specs_verified: bool
    contract_spec_source: str
    multiplier_source: str

    def to_dict(self) -> dict:
        data = {
            "asset_class": self.asset_class,
            "contract_spec_source": self.contract_spec_source,
            "contract_specs_verified": bool(self.contract_specs_verified),
            "exercise_style": self.exercise_style,
            "expiry": self.expiry.isoformat(),
            "instrument_kind": self.instrument_kind,
            "multiplier": float(self.multiplier),
            "multiplier_source": self.multiplier_source,
            "occ_symbol": self.occ_symbol,
            "price_ccy": self.price_ccy,
            "right": self.right,
            "session_calendar": self.session_calendar,
            "settlement": self.settlement,
            "source": self.source,
            "strike": float(self.strike),
            "underlying": self.underlying,
        }
        return {key: data[key] for key in sorted(data)}


def _normalize_option_symbol(symbol: object) -> str:
    text = str(symbol or "").upper().strip().replace(" ", "")
    if text.startswith("O:"):
        return text[2:]
    return text


def parse_option_symbol(symbol: object) -> OptionContractMetadata | None:
    """Return OCC option metadata for compact or ``O:``-prefixed symbols."""
    try:
        normalized = _normalize_option_symbol(symbol)
        if not normalized or not OCC_COMPACT_RE.match(normalized):
            return None
        underlying = normalized[:-15]
        expiry_raw = normalized[-15:-9]
        right = normalized[-9]
        strike_raw = normalized[-8:]
        expiry = dt.date(2000 + int(expiry_raw[:2]), int(expiry_raw[2:4]), int(expiry_raw[4:6]))
        strike = float(int(strike_raw) / 1000.0)
        return OptionContractMetadata(
            occ_symbol=normalized,
            asset_class="OPTION",
            instrument_kind="option",
            underlying=underlying,
            expiry=expiry,
            right=right,
            strike=float(strike),
            multiplier=OPTION_PARSER_DEFAULT_MULTIPLIER,
            exercise_style=OPTION_PARSER_DEFAULT_EXERCISE_STYLE,
            settlement=OPTION_PARSER_DEFAULT_SETTLEMENT,
            price_ccy=OPTION_PARSER_DEFAULT_PRICE_CCY,
            session_calendar=OPTION_PARSER_DEFAULT_SESSION_CALENDAR,
            source=OPTION_PARSER_DEFAULT_SOURCE,
            contract_specs_verified=OPTION_PARSER_DEFAULT_SPECS_VERIFIED,
            contract_spec_source=OPTION_PARSER_DEFAULT_SOURCE,
            multiplier_source=OPTION_PARSER_DEFAULT_MULTIPLIER_SOURCE,
        )
    except Exception as exc:
        _warn_nonfatal(
            "OPTIONS_INSTRUMENT_PARSE_FAILED",
            exc,
            symbol_preview=repr(symbol)[:120],
        )
        return None


def is_option_symbol(symbol: object) -> bool:
    return parse_option_symbol(symbol) is not None
