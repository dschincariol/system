"""
FILE: risk.py

Human-readable purpose:
Provides opt-in risk helpers for portfolio sizing, especially volatility-based
scaling using recent realized price volatility. This module adjusts target
weights; it does not own the broader portfolio decision engine.

Risk helpers (opt-in).
Vol targeting scales target weights by recent realized vol from prices table.

Env:
  PORTFOLIO_USE_VOL_TARGET=1
  PORTFOLIO_VOL_LOOKBACK=240       # number of price points
  PORTFOLIO_TARGET_VOL=0.020       # target stdev of returns (per-step)
  PORTFOLIO_VOL_FLOOR=0.005
  PORTFOLIO_VOL_CEIL=0.080
  PORTFOLIO_TARGET_VOL_SCALE_MIN=0.10
  PORTFOLIO_TARGET_VOL_SCALE_MAX=3.00
  PORTFOLIO_SYMBOL_VOL_SCALE_MIN=0.25
  PORTFOLIO_SYMBOL_VOL_SCALE_MAX=2.50
"""

import math
import os
import logging
from typing import Any, Dict, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

PORTFOLIO_USE_VOL_TARGET = os.environ.get("PORTFOLIO_USE_VOL_TARGET", "1") == "1"
VOL_LOOKBACK = int(os.environ.get("PORTFOLIO_VOL_LOOKBACK", "240"))
TARGET_VOL = float(os.environ.get("PORTFOLIO_TARGET_VOL", "0.020"))
VOL_FLOOR = float(os.environ.get("PORTFOLIO_VOL_FLOOR", "0.005"))
VOL_CEIL = float(os.environ.get("PORTFOLIO_VOL_CEIL", "0.080"))
TARGET_VOL_SCALE_MIN = float(os.environ.get("PORTFOLIO_TARGET_VOL_SCALE_MIN", "0.10"))
TARGET_VOL_SCALE_MAX = float(os.environ.get("PORTFOLIO_TARGET_VOL_SCALE_MAX", "3.00"))
SYMBOL_VOL_SCALE_MIN = float(os.environ.get("PORTFOLIO_SYMBOL_VOL_SCALE_MIN", "0.25"))
SYMBOL_VOL_SCALE_MAX = float(os.environ.get("PORTFOLIO_SYMBOL_VOL_SCALE_MAX", "2.50"))
LOG = get_logger("strategy.risk")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
  if once_key and once_key in _WARNED_NONFATAL_KEYS:
    return
  log_failure(
      LOG,
      event="strategy_risk_nonfatal",
      code=code,
      message=code,
      error=error,
      level=logging.WARNING,
      component="engine.strategy.risk",
      extra=dict(extra or {}) or None,
      persist=False,
  )
  if once_key:
    _WARNED_NONFATAL_KEYS.add(once_key)

def _stdev(xs):
  n = len(xs)
  if n < 3:
    return None
  m = sum(xs) / n
  v = sum((x - m) * (x - m) for x in xs) / (n - 1)
  return math.sqrt(max(0.0, v))

def _safe_float(x, d=0.0) -> float:
  try:
    v = float(x)
    if not math.isfinite(v):
      return float(d)
    return float(v)
  except Exception as e:
    _warn_nonfatal("STRATEGY_RISK_SAFE_FLOAT_FAILED", e, once_key="safe_float", value=repr(x)[:120])
    return float(d)

def _signed_weight_from_target(tgt: Optional[Dict[str, Any]]) -> float:
  row = tgt or {}
  w = _safe_float(row.get("weight", 0.0), 0.0)
  side = str(row.get("side", "") or "").upper().strip()
  if side == "SHORT":
    return -abs(float(w))
  if side == "LONG":
    return abs(float(w))
  return float(w)

def realized_vol_from_prices(con, symbol: str, lookback: int = VOL_LOOKBACK):
  rows = con.execute(
    """
    SELECT price
    FROM prices
    WHERE symbol = ?
    ORDER BY ts_ms DESC
    LIMIT ?
    """,
    (str(symbol), int(lookback)),
  ).fetchall()
  px = [float(r[0]) for r in rows if r and r[0] is not None]
  # Query returns newest first; reverse so returns are computed in time order.
  px.reverse()
  # Very short histories create unstable realized-vol estimates, so bail out.
  if len(px) < 4:
    return None

  rets = []
  for i in range(1, len(px)):
    if px[i-1] > 0 and px[i] > 0:
      rets.append(math.log(px[i] / px[i-1]))
  v = _stdev(rets)
  if v is None:
    return None
  return max(VOL_FLOOR, min(VOL_CEIL, float(v)))

def symbol_vol_scale(vol: float) -> float:
  # Scale is clipped so one bad volatility print cannot collapse the whole book.
  if not vol or float(vol) <= 0.0:
    return 1.0
  m = float(TARGET_VOL / float(vol))
  m = max(float(SYMBOL_VOL_SCALE_MIN), min(float(SYMBOL_VOL_SCALE_MAX), float(m)))
  return float(m)

def vol_scale_weight(weight: float, vol: float):
  # scale so higher vol => smaller weight
  if not vol or vol <= 0:
    return float(weight)
  m = symbol_vol_scale(float(vol))
  return float(weight) * float(m)

def _logrets_from_prices(con, symbol: str, lookback: int = 240):
  rows = con.execute(
    """
    SELECT price
    FROM prices
    WHERE symbol = ?
    ORDER BY ts_ms DESC
    LIMIT ?
    """,
    (str(symbol), int(lookback)),
  ).fetchall()
  px = [float(r[0]) for r in rows if r and r[0] is not None]
  px.reverse()
  if len(px) < 6:
    return None

  rets = []
  for i in range(1, len(px)):
    if px[i-1] > 0 and px[i] > 0:
      rets.append(math.log(px[i] / px[i-1]))
  if len(rets) < 5:
    return None
  return rets

def corr_from_prices(con, a: str, b: str, lookback: int = 240):
  try:
    from engine.risk.covariance import correlation_for_pair

    corr = correlation_for_pair(con, a, b, lookback=int(lookback))
    if corr is not None:
      return float(corr)
  except Exception as e:
    _warn_nonfatal(
        "STRATEGY_RISK_COVARIANCE_CORR_FAILED",
        e,
        once_key=f"covariance_corr:{a}:{b}",
        left_symbol=str(a),
        right_symbol=str(b),
    )

  ra = _logrets_from_prices(con, a, lookback=lookback)
  rb = _logrets_from_prices(con, b, lookback=lookback)
  if not ra or not rb:
    return None

  n = min(len(ra), len(rb))
  if n < 6:
    return None

  xa = ra[-n:]
  xb = rb[-n:]
  ma = sum(xa) / n
  mb = sum(xb) / n
  va = sum((x - ma) * (x - ma) for x in xa)
  vb = sum((x - mb) * (x - mb) for x in xb)
  if va <= 1e-12 or vb <= 1e-12:
    return None

  cov = sum((xa[i] - ma) * (xb[i] - mb) for i in range(n))
  return float(cov / math.sqrt(va * vb))

def portfolio_realized_vol(
  con,
  desired: Dict[str, Dict[str, Any]],
  lookback: int = VOL_LOOKBACK,
) -> Optional[float]:
  active_weights: Dict[str, float] = {}
  for sym, tgt in (desired or {}).items():
    sw = _signed_weight_from_target(tgt)
    if abs(float(sw)) <= 1e-12:
      continue
    active_weights[str(sym)] = float(sw)

  if not active_weights:
    return None

  # Preserve the established single-asset path exactly: it uses the existing
  # realized-vol floors/ceilings and skips missing histories.
  if len(active_weights) == 1:
    sym, sw = next(iter(active_weights.items()))
    vol = realized_vol_from_prices(con, str(sym), lookback=int(lookback))
    if vol is None:
      return None
    return float(abs(float(sw)) * float(vol))

  try:
    from engine.risk.covariance import estimate_covariance, portfolio_volatility_from_estimate

    estimate = estimate_covariance(con, list(active_weights.keys()), lookback=int(lookback))
    pv = portfolio_volatility_from_estimate(estimate, active_weights)
    if pv is not None:
      return float(pv)
  except Exception as e:
    _warn_nonfatal("STRATEGY_RISK_COVARIANCE_PORTFOLIO_VOL_FAILED", e, once_key="covariance_portfolio_vol")

  items = []
  for sym, sw in active_weights.items():
    vol = realized_vol_from_prices(con, str(sym), lookback=int(lookback))
    if vol is None:
      continue
    items.append((str(sym), float(sw), float(vol)))

  n = len(items)
  if n == 0:
    return None

  var = 0.0
  # First accumulate diagonal variance, then pairwise covariance terms.
  for i in range(n):
    _, wi, voli = items[i]
    var += float(wi) * float(wi) * float(voli) * float(voli)

  for i in range(n):
    si, wi, voli = items[i]
    for j in range(i + 1, n):
      sj, wj, volj = items[j]
      c = corr_from_prices(con, si, sj, lookback=int(lookback))
      if c is None:
        cc = 0.0
      else:
        cc = max(-1.0, min(1.0, _safe_float(c, 0.0)))
      var += 2.0 * float(wi) * float(wj) * float(cc) * float(voli) * float(volj)

  if var <= 0.0:
    return 0.0
  return float(math.sqrt(var))

def portfolio_vol_target_scale(
  con,
  desired: Dict[str, Dict[str, Any]],
  lookback: int = VOL_LOOKBACK,
  target_vol: float = TARGET_VOL,
) -> Tuple[float, Optional[float]]:
  pv = portfolio_realized_vol(con, desired, lookback=int(lookback))
  if pv is None:
    return 1.0, None
  if pv <= 1e-12:
    return 1.0, float(pv)

  scale = float(target_vol) / float(pv)
  scale = max(float(TARGET_VOL_SCALE_MIN), min(float(TARGET_VOL_SCALE_MAX), float(scale)))
  return float(scale), float(pv)
