"""FX instrument semantics and canonical symbol parsing.

This module is intentionally pure: it performs no I/O, DB access, network
access, broker access, or order routing. Later FX workstreams should consume
these helpers instead of re-deriving FX pair semantics.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("data.fx_instrument")

KNOWN_CCY = frozenset(
    {
        "AUD",
        "CAD",
        "CHF",
        "EUR",
        "GBP",
        "JPY",
        "MXN",
        "NOK",
        "NZD",
        "SEK",
        "USD",
    }
)
FX_SPOT_CONTRACT_SIZE = 100000.0
FX_REFERENCE_LEVERAGE_CAP = 20.0
FX_SESSION_CALENDAR = "FX_24x5"


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="data_fx_instrument_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.fx_instrument",
        extra=dict(extra or {}) or None,
        persist=False,
    )


@dataclass(frozen=True)
class InstrumentMetadata:
    symbol: str
    asset_class: str
    instrument_kind: str
    base_ccy: Optional[str]
    quote_ccy: Optional[str]
    pip_size: float
    contract_size: float
    pnl_ccy: str
    leverage_cap: float
    session_calendar: str
    source: str

    def to_dict(self) -> dict:
        data = {
            "asset_class": self.asset_class,
            "base_ccy": self.base_ccy,
            "contract_size": float(self.contract_size),
            "instrument_kind": self.instrument_kind,
            "leverage_cap": float(self.leverage_cap),
            "pip_size": float(self.pip_size),
            "pnl_ccy": self.pnl_ccy,
            "quote_ccy": self.quote_ccy,
            "session_calendar": self.session_calendar,
            "source": self.source,
            "symbol": self.symbol,
        }
        return {key: data[key] for key in sorted(data)}


def _normalize_fx_symbol(symbol: object) -> str:
    text = str(symbol or "").upper().strip()
    if len(text) == 7 and text[3] in {"/", "_"}:
        return text[:3] + text[4:]
    return text


def parse_fx_symbol(symbol: object) -> InstrumentMetadata | None:
    """Return FX metadata for a known pair/index, else ``None``.

    The canonical stored form for FX spot pairs is the 6-letter uppercase
    ``BASE+QUOTE`` string, with no separator. The parser accepts friendly
    variants such as ``EUR/USD`` and ``EUR_USD`` but normalizes them to
    ``EURUSD``.
    """
    try:
        normalized = _normalize_fx_symbol(symbol)
        if not normalized:
            return None
        if normalized == "DXY":
            return InstrumentMetadata(
                symbol="DXY",
                asset_class="FX",
                instrument_kind="fx_index",
                base_ccy=None,
                quote_ccy="USD",
                pip_size=0.01,
                contract_size=1.0,
                pnl_ccy="USD",
                leverage_cap=FX_REFERENCE_LEVERAGE_CAP,
                session_calendar=FX_SESSION_CALENDAR,
                source="parser",
            )
        if len(normalized) != 6 or not normalized.isalpha():
            return None
        base = normalized[:3]
        quote = normalized[3:]
        if base == quote or base not in KNOWN_CCY or quote not in KNOWN_CCY:
            return None
        pip_size = 0.01 if quote == "JPY" else 0.0001
        # Reference metadata only. FX-05 owns enforcement by reconciling this
        # value against regulatory caps with min(reference, regulatory).
        return InstrumentMetadata(
            symbol=f"{base}{quote}",
            asset_class="FX",
            instrument_kind="fx_spot",
            base_ccy=base,
            quote_ccy=quote,
            pip_size=float(pip_size),
            contract_size=FX_SPOT_CONTRACT_SIZE,
            pnl_ccy=quote,
            leverage_cap=FX_REFERENCE_LEVERAGE_CAP,
            session_calendar=FX_SESSION_CALENDAR,
            source="parser",
        )
    except Exception as exc:
        _warn_nonfatal(
            "FX_INSTRUMENT_PARSE_FAILED",
            exc,
            symbol_preview=repr(symbol)[:120],
        )
        return None


def is_fx_symbol(symbol: object) -> bool:
    return parse_fx_symbol(symbol) is not None
