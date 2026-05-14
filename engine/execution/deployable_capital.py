"""
FILE: deployable_capital.py

Execution subsystem module for `deployable_capital`.
"""

# engine/execution/deployable_capital.py
"""
Deployable Capital Helpers

Goal:
- Convert broker/account fields into a conservative "deployable equity" base for weight->qty sizing.

Env:
  DEPLOYABLE_EQUITY_MODE=min_equity_bp   # equity|cash|buying_power|min_equity_bp
  DEPLOYABLE_BP_FACTOR=0.50              # applied to buying_power in min_equity_bp or buying_power mode
  DEPLOYABLE_CASH_FACTOR=1.00            # applied to cash in cash mode
  DEPLOYABLE_EQUITY_FACTOR=1.00          # applied to equity in equity mode or min_equity_bp
  DEPLOYABLE_EQUITY_MIN=0.0              # floor
  DEPLOYABLE_EQUITY_MAX=1e18             # cap

IBKR env support (when no live account dict):
  IBKR_AVAILABLE_FUNDS_USD
  IBKR_BUYING_POWER_USD
  IBKR_CASH_USD
  IBKR_EQUITY_USD
"""

import os
import math
import logging
from typing import Any, Dict, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("engine.execution.deployable_capital")


def _sf(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else float(default)
    except Exception as e:
        log_failure(
            LOG,
            event="deployable_capital_safe_float_failed",
            code="DEPLOYABLE_CAPITAL_SAFE_FLOAT_FAILED",
            message="Deployable capital float parse failed.",
            error=e,
            level=logging.WARNING,
            component="engine.execution.deployable_capital",
            extra={"value": repr(x)[:120]},
            persist=False,
        )
        return float(default)


def _clamp(v: float, lo: float, hi: float) -> float:
    try:
        vv = float(v)
    except Exception:
        vv = float(lo)
    return float(max(float(lo), min(float(hi), vv)))


_MODE = str(os.environ.get("DEPLOYABLE_EQUITY_MODE", "min_equity_bp") or "min_equity_bp").strip()
_BP_FACTOR = float(os.environ.get("DEPLOYABLE_BP_FACTOR", "0.50") or 0.50)
_CASH_FACTOR = float(os.environ.get("DEPLOYABLE_CASH_FACTOR", "1.00") or 1.00)
_EQ_FACTOR = float(os.environ.get("DEPLOYABLE_EQUITY_FACTOR", "1.00") or 1.00)

_EQ_MIN = float(os.environ.get("DEPLOYABLE_EQUITY_MIN", "0.0") or 0.0)
_EQ_MAX = float(os.environ.get("DEPLOYABLE_EQUITY_MAX", "1e18") or 1e18)


def compute_deployable_equity(account: Dict[str, Any], *, default_equity: Optional[float] = None) -> float:
    """
    account: dict that may contain (case-insensitive usage by callers):
      - equity
      - cash
      - buying_power

    Returns conservative deployable base (>=0).
    This is a sizing helper, not an accounting truth source.
    """
    acct = account or {}

    eq = _sf(acct.get("equity"), _sf(default_equity, 0.0))
    cash = _sf(acct.get("cash"), 0.0)
    bp = _sf(acct.get("buying_power"), 0.0)

    mode = _MODE.lower()

    if mode == "equity":
        out = eq * float(_EQ_FACTOR)
    elif mode == "cash":
        out = cash * float(_CASH_FACTOR)
    elif mode == "buying_power":
        out = bp * float(_BP_FACTOR)
    else:
        # min_equity_bp (default): conservative across mark-to-market equity and leverage allowance
        eq_adj = eq * float(_EQ_FACTOR)
        bp_adj = bp * float(_BP_FACTOR)
        if bp_adj > 0.0 and eq_adj > 0.0:
            out = min(eq_adj, bp_adj)
        elif bp_adj > 0.0:
            out = bp_adj
        else:
            out = eq_adj

    return _clamp(float(out), float(_EQ_MIN), float(_EQ_MAX))


def compute_deployable_equity_from_env(prefix: str, *, default_equity: float = 0.0) -> float:
    """
    For brokers where sizing uses env-provided account fields.
    Example: prefix="IBKR" reads:
      IBKR_AVAILABLE_FUNDS_USD, IBKR_BUYING_POWER_USD, IBKR_CASH_USD, IBKR_EQUITY_USD
    """
    p = str(prefix or "").strip().upper()
    if not p:
        return compute_deployable_equity({"equity": float(default_equity)})

    avail = os.environ.get(f"{p}_AVAILABLE_FUNDS_USD", None)
    bp = os.environ.get(f"{p}_BUYING_POWER_USD", None)
    cash = os.environ.get(f"{p}_CASH_USD", None)
    eq = os.environ.get(f"{p}_EQUITY_USD", None)

    acct: Dict[str, Any] = {}

    # Prefer AvailableFunds if provided (maps most closely to deployable)
    if avail is not None and str(avail).strip() != "":
        acct["buying_power"] = _sf(avail, 0.0)
    elif bp is not None and str(bp).strip() != "":
        acct["buying_power"] = _sf(bp, 0.0)

    if cash is not None and str(cash).strip() != "":
        acct["cash"] = _sf(cash, 0.0)

    if eq is not None and str(eq).strip() != "":
        acct["equity"] = _sf(eq, float(default_equity))
    else:
        acct["equity"] = float(default_equity)

    return compute_deployable_equity(acct, default_equity=float(default_equity))
