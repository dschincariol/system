"""Point-in-time Form 4 routine/opportunistic trade classification."""

from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

ROUTINE = "routine"
OPPORTUNISTIC = "opportunistic"
UNCLASSIFIED = "unclassified"

OPEN_MARKET_CODES = {"P", "S"}
_PLAN_RE = re.compile(r"\b10\s*b\s*5\s*[- ]\s*1\b", re.IGNORECASE)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def parse_ts_ms(value: Any) -> Optional[int]:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.isdigit():
        if len(raw) == 8:
            try:
                parsed = datetime.strptime(raw, "%Y%m%d").replace(tzinfo=timezone.utc)
                return int(parsed.timestamp() * 1000)
            except ValueError:
                return None
        if len(raw) == 14:
            try:
                parsed = datetime.strptime(raw, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                return int(parsed.timestamp() * 1000)
            except ValueError:
                return None
        if len(raw) >= 13:
            return int(raw[:13])
    normalized = raw
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    for fmt in (
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
    ):
        try:
            parsed = datetime.strptime(raw.rstrip("Z"), fmt)
            parsed = parsed.replace(tzinfo=timezone.utc)
            return int(parsed.timestamp() * 1000)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    except Exception:
        return None


def calendar_year_month(row: dict[str, Any]) -> tuple[int, int] | None:
    date_text = str(row.get("transaction_date") or "").strip()
    if date_text:
        try:
            parsed = datetime.strptime(date_text[:10], "%Y-%m-%d")
            return int(parsed.year), int(parsed.month)
        # system-audit: ignore[silent_except] invalid transaction_date falls back to transaction_ts_ms.
        except ValueError:
            pass
    ts_ms = safe_int(row.get("transaction_ts_ms"), 0)
    if ts_ms <= 0:
        return None
    parsed = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return int(parsed.year), int(parsed.month)


def availability_ts_ms(row: dict[str, Any]) -> int:
    return safe_int(
        row.get("availability_ts_ms")
        or row.get("filing_ts_ms")
        or row.get("ingested_ts_ms")
        or row.get("created_ts_ms"),
        0,
    )


def insider_key(row: dict[str, Any]) -> str:
    return str(row.get("insider_cik") or row.get("insider_name") or "").strip().upper()


def transaction_value(row: dict[str, Any]) -> float:
    value = safe_float(row.get("value"), 0.0)
    if value > 0.0:
        return float(value)
    shares = safe_float(row.get("shares"), 0.0)
    price = safe_float(row.get("price"), 0.0)
    if shares > 0.0 and price > 0.0:
        return float(shares * price)
    return 0.0


def is_plan_trade(row: dict[str, Any]) -> bool:
    explicit = row.get("is_10b5_1_plan")
    if isinstance(explicit, bool):
        return bool(explicit)
    if explicit is not None and str(explicit).strip().lower() in {"1", "true", "t", "yes", "y"}:
        return True
    for key in ("payload_json", "diagnostics_json"):
        payload = row.get(key)
        if isinstance(payload, (dict, list)):
            text = json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)
        else:
            text = str(payload or "")
        if _PLAN_RE.search(text):
            return True
    return False


def is_open_market_trade(row: dict[str, Any]) -> bool:
    code = str(row.get("transaction_code") or "").strip().upper()
    if code not in OPEN_MARKET_CODES:
        return False
    if is_plan_trade(row):
        return False
    return True


def classify_insider_trade(
    trade: dict[str, Any],
    history: Iterable[dict[str, Any]],
    *,
    asof_ts_ms: int | None = None,
) -> str:
    """Classify one Form 4 trade using only available prior trade history."""

    if not is_open_market_trade(trade):
        return UNCLASSIFIED
    key = insider_key(trade)
    target_ym = calendar_year_month(trade)
    if not key or target_ym is None:
        return UNCLASSIFIED

    target_year, target_month = target_ym
    cutoff_avail = int(asof_ts_ms) if asof_ts_ms is not None else availability_ts_ms(trade)
    trade_id = str(trade.get("source_transaction_id") or "").strip()

    prior_years: set[int] = set()
    same_month_years: set[int] = set()
    for row in history or []:
        if not isinstance(row, dict):
            continue
        if insider_key(row) != key or not is_open_market_trade(row):
            continue
        if trade_id and str(row.get("source_transaction_id") or "").strip() == trade_id:
            continue
        if cutoff_avail > 0 and availability_ts_ms(row) > cutoff_avail:
            continue
        ym = calendar_year_month(row)
        if ym is None:
            continue
        year, month = ym
        if year >= target_year:
            continue
        prior_years.add(int(year))
        if int(month) == int(target_month) and int(year) in {target_year - 1, target_year - 2, target_year - 3}:
            same_month_years.add(int(year))

    if not prior_years or min(prior_years) > target_year - 3:
        return UNCLASSIFIED
    if {target_year - 1, target_year - 2, target_year - 3}.issubset(same_month_years):
        return ROUTINE
    return OPPORTUNISTIC


def role_buy_weight(row: dict[str, Any]) -> float:
    role = str(row.get("insider_role") or "").strip().lower()
    title = str(row.get("insider_title") or "").strip().lower()
    if "officer" in role or "director" in role or title:
        return 1.0
    if "ten_percent" in role or "10" in role:
        return 0.5
    return 0.25
