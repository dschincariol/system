"""Futures roll calendar and ratio-adjusted continuous-series helpers.

Raw per-contract bars remain the source of truth.  This module derives the
roll calendar, the ratio-adjusted front-month series used for return math, and
the roll-yield series consumed by later futures workstreams.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
import re
import time
from typing import Any, Dict, Iterable, Mapping, Sequence

from engine.data.calendar.futures_sessions import futures_window_spans_closed_gap
from engine.data.futures_instrument import parse_futures_symbol


MONTH_CODES: dict[str, int] = {
    "F": 1,
    "G": 2,
    "H": 3,
    "J": 4,
    "K": 5,
    "M": 6,
    "N": 7,
    "Q": 8,
    "U": 9,
    "V": 10,
    "X": 11,
    "Z": 12,
}
_CONTRACT_RE = re.compile(r"^([A-Z0-9]+)([FGHJKMNQUVXZ])(\d{2})$")


@dataclass(frozen=True)
class FuturesBar:
    contract: str
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    open_interest: float


@dataclass(frozen=True)
class RollEvent:
    root: str
    roll_ts_ms: int
    from_contract: str
    to_contract: str
    gap_ratio: float
    method: str = "oi_volume"


@dataclass(frozen=True)
class ContBar:
    continuous_symbol: str
    ts_ms: int
    adj_method: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    roll_flag: bool
    source_contract: str


@dataclass(frozen=True)
class RollYieldPoint:
    root: str
    ts_ms: int
    roll_yield: float


def _finite_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _field(row: Any, name: str, default: Any = None) -> Any:
    if isinstance(row, Mapping):
        return row.get(name, default)
    return getattr(row, name, default)


def _contract_parts(contract: str) -> tuple[str, int, int] | None:
    text = str(contract or "").strip().upper()
    match = _CONTRACT_RE.match(text)
    if not match:
        return None
    root = match.group(1)
    month = MONTH_CODES.get(match.group(2))
    if month is None:
        return None
    year = 2000 + int(match.group(3))
    return root, year, int(month)


def _contract_root(contract: str) -> str:
    parts = _contract_parts(contract)
    if parts is not None:
        return parts[0]
    text = str(contract or "").strip().upper()
    if ".C." in text:
        return text.split(".C.", 1)[0]
    match = re.match(r"^([A-Z0-9]+)", text)
    return str(match.group(1) if match else text)


def _contract_sort_key(contract: str) -> tuple[str, int, int, str]:
    text = str(contract or "").strip().upper()
    parts = _contract_parts(text)
    if parts is None:
        return (_contract_root(text), 9999, 99, text)
    root, year, month = parts
    return (root, int(year), int(month), text)


def _bar_from_value(contract: str, value: Any) -> FuturesBar | None:
    try:
        ts_ms = int(_field(value, "ts_ms", _field(value, "timestamp", 0)) or 0)
    except Exception:
        return None
    if ts_ms <= 0:
        return None
    close = _finite_float(_field(value, "close", _field(value, "settlement", _field(value, "price", 0.0))), 0.0)
    open_px = _finite_float(_field(value, "open", close), close)
    high = _finite_float(_field(value, "high", max(open_px, close)), max(open_px, close))
    low = _finite_float(_field(value, "low", min(open_px, close)), min(open_px, close))
    return FuturesBar(
        contract=str(_field(value, "contract", contract) or contract).strip().upper(),
        ts_ms=int(ts_ms),
        open=float(open_px),
        high=float(high),
        low=float(low),
        close=float(close),
        volume=_finite_float(_field(value, "volume", 0.0), 0.0),
        open_interest=_finite_float(_field(value, "open_interest", _field(value, "oi", 0.0)), 0.0),
    )


def _normalize_bars(bars_by_contract: Mapping[str, Iterable[Any]] | None) -> dict[str, list[FuturesBar]]:
    out: dict[str, list[FuturesBar]] = {}
    for raw_contract, values in dict(bars_by_contract or {}).items():
        contract = str(raw_contract or "").strip().upper()
        if not contract:
            continue
        rows: list[FuturesBar] = []
        for value in list(values or []):
            bar = _bar_from_value(contract, value)
            if bar is not None:
                rows.append(bar)
        rows.sort(key=lambda item: int(item.ts_ms))
        if rows:
            out[contract] = rows
    return out


def _bars_by_ts(rows: Sequence[FuturesBar]) -> dict[int, FuturesBar]:
    return {int(row.ts_ms): row for row in rows}


def _group_contracts_by_root(contracts: Iterable[str]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for contract in contracts:
        root = _contract_root(contract)
        if not root:
            continue
        grouped.setdefault(root, []).append(str(contract).strip().upper())
    for root, values in list(grouped.items()):
        grouped[root] = sorted(set(values), key=_contract_sort_key)
    return grouped


def detect_rolls(bars_by_contract: Mapping[str, Iterable[Any]] | None) -> list[RollEvent]:
    """Detect OI/volume-confirmed rolls across adjacent dated contracts."""
    normalized = _normalize_bars(bars_by_contract)
    if len(normalized) < 2:
        return []

    events: list[RollEvent] = []
    for root, contracts in _group_contracts_by_root(normalized.keys()).items():
        if len(contracts) < 2:
            continue
        for front, deferred in zip(contracts, contracts[1:]):
            front_by_ts = _bars_by_ts(normalized.get(front, ()))
            deferred_by_ts = _bars_by_ts(normalized.get(deferred, ()))
            common_ts = sorted(set(front_by_ts) & set(deferred_by_ts))
            for ts_ms in common_ts:
                front_bar = front_by_ts[ts_ms]
                deferred_bar = deferred_by_ts[ts_ms]
                oi_crossed = float(deferred_bar.open_interest) > float(front_bar.open_interest)
                volume_confirmed = float(deferred_bar.volume) >= float(front_bar.volume)
                if not (oi_crossed and volume_confirmed):
                    continue
                gap_ratio = 1.0
                if front_bar.close > 0.0 and deferred_bar.close > 0.0:
                    gap_ratio = float(deferred_bar.close) / float(front_bar.close)
                events.append(
                    RollEvent(
                        root=str(root),
                        roll_ts_ms=int(ts_ms),
                        from_contract=str(front),
                        to_contract=str(deferred),
                        gap_ratio=float(gap_ratio) if math.isfinite(gap_ratio) and gap_ratio > 0.0 else 1.0,
                    )
                )
                break
    events.sort(key=lambda item: (str(item.root), int(item.roll_ts_ms), str(item.from_contract), str(item.to_contract)))
    return events


def _future_adjustment_factor(rolls: Sequence[RollEvent], ts_ms: int) -> float:
    factor = 1.0
    for roll in rolls:
        ratio = float(roll.gap_ratio)
        if int(roll.roll_ts_ms) > int(ts_ms) and math.isfinite(ratio) and ratio > 0.0:
            factor *= ratio
    return float(factor)


def _active_contract_for_ts(contracts: Sequence[str], rolls: Sequence[RollEvent], ts_ms: int) -> str:
    active = str(rolls[0].from_contract if rolls else (contracts[0] if contracts else ""))
    for roll in rolls:
        if int(ts_ms) >= int(roll.roll_ts_ms):
            active = str(roll.to_contract)
        else:
            break
    return active


def _build_ratio_adjusted_for_root(
    root: str,
    bars_by_contract: Mapping[str, list[FuturesBar]],
    rolls: Sequence[RollEvent],
) -> list[ContBar]:
    contracts = sorted(set(bars_by_contract), key=_contract_sort_key)
    if not contracts:
        return []
    by_contract_ts = {contract: _bars_by_ts(bars_by_contract.get(contract, ())) for contract in contracts}
    all_ts = sorted({ts for rows in by_contract_ts.values() for ts in rows})
    roll_ts = {int(roll.roll_ts_ms) for roll in rolls}
    out: list[ContBar] = []
    continuous_symbol = f"{root}.c.0"
    for ts_ms in all_ts:
        active = _active_contract_for_ts(contracts, rolls, int(ts_ms))
        bar = by_contract_ts.get(active, {}).get(int(ts_ms))
        if bar is None or bar.close <= 0.0:
            continue
        factor = _future_adjustment_factor(rolls, int(ts_ms))
        out.append(
            ContBar(
                continuous_symbol=continuous_symbol,
                ts_ms=int(ts_ms),
                adj_method="ratio",
                open=float(bar.open) * factor,
                high=float(bar.high) * factor,
                low=float(bar.low) * factor,
                close=float(bar.close) * factor,
                volume=float(bar.volume),
                roll_flag=int(ts_ms) in roll_ts,
                source_contract=str(active),
            )
        )
    return out


def build_ratio_adjusted_continuous(
    bars_by_contract: Mapping[str, Iterable[Any]] | None,
    rolls: Sequence[RollEvent] | None,
) -> list[ContBar]:
    """Build a ratio-adjusted front-month series from raw contract bars."""
    normalized = _normalize_bars(bars_by_contract)
    if not normalized:
        return []
    grouped_contracts = _group_contracts_by_root(normalized.keys())
    grouped_rolls: dict[str, list[RollEvent]] = {}
    for roll in list(rolls or []):
        grouped_rolls.setdefault(str(roll.root), []).append(roll)
    out: list[ContBar] = []
    for root, contracts in grouped_contracts.items():
        root_bars = {contract: normalized[contract] for contract in contracts if contract in normalized}
        root_rolls = sorted(grouped_rolls.get(root, []), key=lambda item: int(item.roll_ts_ms))
        out.extend(_build_ratio_adjusted_for_root(root, root_bars, root_rolls))
    out.sort(key=lambda item: (item.continuous_symbol, int(item.ts_ms)))
    return out


def _delivery_month_index(contract: str) -> int | None:
    parts = _contract_parts(contract)
    if parts is None:
        return None
    _root, year, month = parts
    return int(year) * 12 + int(month)


def _days_between_contracts(front_contract: str, next_contract: str) -> float:
    front_idx = _delivery_month_index(front_contract)
    next_idx = _delivery_month_index(next_contract)
    if front_idx is None or next_idx is None or next_idx <= front_idx:
        return 30.0
    return max(1.0, float(next_idx - front_idx) * 30.4375)


def compute_roll_yield(front_settle: float, next_settle: float, days_between: float) -> float:
    """Annualized log slope: backwardation positive, contango negative."""
    front = _finite_float(front_settle, 0.0)
    nxt = _finite_float(next_settle, 0.0)
    days = _finite_float(days_between, 0.0)
    if front <= 0.0 or nxt <= 0.0 or days <= 0.0:
        return 0.0
    try:
        out = math.log(front / nxt) * (365.0 / days)
    except Exception:
        return 0.0
    if not math.isfinite(out):
        return 0.0
    return float(out)


def build_roll_yield_series(
    bars_by_contract: Mapping[str, Iterable[Any]] | None,
    rolls: Sequence[RollEvent] | None,
) -> list[RollYieldPoint]:
    normalized = _normalize_bars(bars_by_contract)
    if len(normalized) < 2:
        return []
    grouped_contracts = _group_contracts_by_root(normalized.keys())
    grouped_rolls: dict[str, list[RollEvent]] = {}
    for roll in list(rolls or []):
        grouped_rolls.setdefault(str(roll.root), []).append(roll)

    points: list[RollYieldPoint] = []
    for root, contracts in grouped_contracts.items():
        if len(contracts) < 2:
            continue
        by_contract_ts = {contract: _bars_by_ts(normalized.get(contract, ())) for contract in contracts}
        all_ts = sorted({ts for rows in by_contract_ts.values() for ts in rows})
        root_rolls = sorted(grouped_rolls.get(root, []), key=lambda item: int(item.roll_ts_ms))
        for ts_ms in all_ts:
            active = _active_contract_for_ts(contracts, root_rolls, int(ts_ms))
            try:
                active_idx = contracts.index(active)
            except ValueError:
                continue
            if active_idx + 1 >= len(contracts):
                continue
            next_contract = contracts[active_idx + 1]
            front_bar = by_contract_ts.get(active, {}).get(int(ts_ms))
            next_bar = by_contract_ts.get(next_contract, {}).get(int(ts_ms))
            if front_bar is None or next_bar is None:
                continue
            points.append(
                RollYieldPoint(
                    root=str(root),
                    ts_ms=int(ts_ms),
                    roll_yield=compute_roll_yield(
                        float(front_bar.close),
                        float(next_bar.close),
                        _days_between_contracts(active, next_contract),
                    ),
                )
            )
    points.sort(key=lambda item: (str(item.root), int(item.ts_ms)))
    return points


def ensure_futures_roll_tables(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS futures_roll_calendar (
            root TEXT NOT NULL,
            roll_ts_ms BIGINT NOT NULL,
            from_contract TEXT NOT NULL,
            to_contract TEXT NOT NULL,
            gap_ratio DOUBLE PRECISION NOT NULL,
            method TEXT NOT NULL,
            ingested_ts_ms BIGINT,
            PRIMARY KEY(root, roll_ts_ms)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_futures_roll_calendar_root_ts
          ON futures_roll_calendar(root, roll_ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS futures_continuous_bars (
            continuous_symbol TEXT NOT NULL,
            ts_ms BIGINT NOT NULL,
            adj_method TEXT NOT NULL,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            volume DOUBLE PRECISION,
            roll_flag BIGINT NOT NULL DEFAULT 0,
            source_contract TEXT,
            PRIMARY KEY(continuous_symbol, ts_ms, adj_method)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_futures_continuous_bars_symbol_ts
          ON futures_continuous_bars(continuous_symbol, ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS futures_roll_yield (
            root TEXT NOT NULL,
            ts_ms BIGINT NOT NULL,
            roll_yield DOUBLE PRECISION NOT NULL,
            PRIMARY KEY(root, ts_ms)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_futures_roll_yield_root_ts
          ON futures_roll_yield(root, ts_ms DESC)
        """
    )


def futures_continuous_symbol_for(symbol: object) -> str | None:
    """Return the canonical ratio-adjusted front continuous alias for a futures symbol."""
    meta = parse_futures_symbol(symbol)
    if meta is None:
        return None
    if meta.continuous_alias:
        return str(meta.continuous_alias).strip()
    root = str(meta.root or "").upper().strip()
    return f"{root}.c.0" if root else None


def futures_root_for_symbol(symbol: object) -> str | None:
    meta = parse_futures_symbol(symbol)
    if meta is None:
        return None
    root = str(meta.root or "").upper().strip()
    return root or None


def read_ratio_adjusted_continuous_close_at_or_after(con, symbol: object, ts_ms: int) -> float | None:
    """Read the first ratio-adjusted continuous futures close at or after ``ts_ms``.

    Production label and validation paths must not fall back to raw front-month
    rows because contract rolls create artificial returns. Missing continuous
    bars are therefore represented as ``None`` so the caller can skip the label
    or validation sample fail-closed.
    """
    continuous_symbol = futures_continuous_symbol_for(symbol)
    if not continuous_symbol:
        return None
    try:
        row = con.execute(
            """
            SELECT close
            FROM futures_continuous_bars
            WHERE continuous_symbol=? AND adj_method='ratio' AND ts_ms>=?
            ORDER BY ts_ms ASC
            LIMIT 1
            """,
            (str(continuous_symbol), int(ts_ms)),
        ).fetchone()
    except Exception:
        return None
    if not row or row[0] is None:
        return None
    value = _finite_float(row[0], float("nan"))
    return float(value) if math.isfinite(value) and value > 0.0 else None


def futures_window_spans_roll(con, symbol: object, start_ts_ms: int, end_ts_ms: int) -> bool:
    """Return true when the label/validation window crosses a known futures roll."""
    root = futures_root_for_symbol(symbol)
    if not root:
        return False
    start = int(start_ts_ms)
    end = int(end_ts_ms)
    if end < start:
        start, end = end, start
    try:
        row = con.execute(
            """
            SELECT 1
            FROM futures_roll_calendar
            WHERE root=? AND roll_ts_ms>? AND roll_ts_ms<=?
            ORDER BY roll_ts_ms ASC
            LIMIT 1
            """,
            (str(root), int(start), int(end)),
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def futures_roll_calendar_available(con, symbol: object) -> bool:
    """Return true only when a roll calendar is present for ``symbol``'s root."""

    root = futures_root_for_symbol(symbol)
    if not root:
        return False
    try:
        row = con.execute(
            """
            SELECT 1
            FROM futures_roll_calendar
            WHERE root=?
            LIMIT 1
            """,
            (str(root),),
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def futures_label_window_block_reason(con, symbol: object, start_ts_ms: int, end_ts_ms: int) -> str | None:
    """Return the futures label block reason, or ``None`` when the window is usable.

    Futures labels are only valid when the roll calendar is available for the
    root and the forward window avoids both market-closed gaps and known roll
    boundaries. Calendar unavailability fails closed so callers never silently
    fall back to raw front-month returns.
    """

    start = int(start_ts_ms)
    end = int(end_ts_ms)
    if futures_window_spans_closed_gap(start, end):
        return "closed_gap"
    if not futures_roll_calendar_available(con, symbol):
        return "roll_calendar_unavailable"
    if futures_window_spans_roll(con, symbol, start, end):
        return "roll_boundary"
    return None


def load_futures_roll_boundaries(
    con,
    *,
    symbols: Sequence[object] | None = None,
    start_ts_ms: int | float | None = None,
    end_ts_ms: int | float | None = None,
) -> list[int]:
    """Load futures roll timestamps for the roots represented in ``symbols``.

    CPCV production callers pass these boundaries into the purged splitter so
    train samples whose label windows straddle a roll are excluded.
    """
    roots = sorted({root for root in (futures_root_for_symbol(sym) for sym in list(symbols or [])) if root})
    if not roots:
        return []
    clauses = [f"root IN ({','.join(['?'] * len(roots))})"]
    params: list[object] = list(roots)
    if start_ts_ms is not None:
        clauses.append("roll_ts_ms>=?")
        params.append(int(float(start_ts_ms)))
    if end_ts_ms is not None:
        clauses.append("roll_ts_ms<=?")
        params.append(int(float(end_ts_ms)))
    try:
        rows = con.execute(
            f"""
            SELECT DISTINCT roll_ts_ms
            FROM futures_roll_calendar
            WHERE {' AND '.join(clauses)}
            ORDER BY roll_ts_ms ASC
            """,
            tuple(params),
        ).fetchall()
    except Exception:
        return []
    out: list[int] = []
    for row in rows or []:
        try:
            value = int(row[0])
        except Exception:
            continue
        out.append(value)
    return sorted(set(out))


def _read_raw_futures_bars(con) -> dict[str, list[FuturesBar]]:
    try:
        rows = con.execute(
            """
            SELECT contract, ts_ms, open, high, low, close, volume, open_interest
            FROM futures_contract_bars
            ORDER BY contract, ts_ms
            """
        ).fetchall()
    except Exception:
        return {}
    out: dict[str, list[FuturesBar]] = {}
    for row in rows or []:
        contract = str(row[0] if not hasattr(row, "keys") else row["contract"] or "").strip().upper()
        if not contract:
            continue
        bar = _bar_from_value(
            contract,
            {
                "contract": contract,
                "ts_ms": row[1] if not hasattr(row, "keys") else row["ts_ms"],
                "open": row[2] if not hasattr(row, "keys") else row["open"],
                "high": row[3] if not hasattr(row, "keys") else row["high"],
                "low": row[4] if not hasattr(row, "keys") else row["low"],
                "close": row[5] if not hasattr(row, "keys") else row["close"],
                "volume": row[6] if not hasattr(row, "keys") else row["volume"],
                "open_interest": row[7] if not hasattr(row, "keys") else row["open_interest"],
            },
        )
        if bar is not None:
            out.setdefault(contract, []).append(bar)
    return out


def _write_outputs(
    con,
    *,
    rolls: Sequence[RollEvent],
    continuous: Sequence[ContBar],
    roll_yield: Sequence[RollYieldPoint],
    now_ms: int,
) -> int:
    ensure_futures_roll_tables(con)
    written = 0
    for roll in rolls:
        con.execute(
            """
            INSERT INTO futures_roll_calendar(
              root, roll_ts_ms, from_contract, to_contract, gap_ratio, method, ingested_ts_ms
            ) VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(root, roll_ts_ms) DO UPDATE SET
              from_contract=excluded.from_contract,
              to_contract=excluded.to_contract,
              gap_ratio=excluded.gap_ratio,
              method=excluded.method,
              ingested_ts_ms=excluded.ingested_ts_ms
            """,
            (
                str(roll.root),
                int(roll.roll_ts_ms),
                str(roll.from_contract),
                str(roll.to_contract),
                float(roll.gap_ratio),
                str(roll.method or "oi_volume"),
                int(now_ms),
            ),
        )
        written += 1
    for bar in continuous:
        con.execute(
            """
            INSERT INTO futures_continuous_bars(
              continuous_symbol, ts_ms, adj_method, open, high, low, close, volume, roll_flag, source_contract
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(continuous_symbol, ts_ms, adj_method) DO UPDATE SET
              open=excluded.open,
              high=excluded.high,
              low=excluded.low,
              close=excluded.close,
              volume=excluded.volume,
              roll_flag=excluded.roll_flag,
              source_contract=excluded.source_contract
            """,
            (
                str(bar.continuous_symbol),
                int(bar.ts_ms),
                str(bar.adj_method),
                float(bar.open),
                float(bar.high),
                float(bar.low),
                float(bar.close),
                float(bar.volume),
                1 if bool(bar.roll_flag) else 0,
                str(bar.source_contract),
            ),
        )
        written += 1
    for point in roll_yield:
        con.execute(
            """
            INSERT INTO futures_roll_yield(root, ts_ms, roll_yield)
            VALUES (?,?,?)
            ON CONFLICT(root, ts_ms) DO UPDATE SET
              roll_yield=excluded.roll_yield
            """,
            (str(point.root), int(point.ts_ms), float(point.roll_yield)),
        )
        written += 1
    return int(written)


def ingest_futures_rolls_batch(*, now_ms: int | None = None) -> Dict[str, Any]:
    """Read raw futures bars, derive roll artifacts, and upsert them."""
    from engine.runtime.storage import connect, run_write_txn

    anchor_ms = int(now_ms or time.time() * 1000)
    con = connect()
    try:
        ensure_futures_roll_tables(con)
        raw = _read_raw_futures_bars(con)
    finally:
        try:
            con.close()
        except Exception:
            pass

    rolls = detect_rolls(raw)
    continuous = build_ratio_adjusted_continuous(raw, rolls)
    roll_yield = build_roll_yield_series(raw, rolls)

    def _write(conw) -> int:
        return _write_outputs(
            conw,
            rolls=rolls,
            continuous=continuous,
            roll_yield=roll_yield,
            now_ms=int(anchor_ms),
        )

    written = int(
        run_write_txn(_write, table="futures_roll_calendar", operation="ingest_futures_rolls")
        if (rolls or continuous or roll_yield)
        else 0
    )
    return {
        "ok": True,
        "raw_contracts": int(len(raw)),
        "raw_rows": int(sum(len(rows) for rows in raw.values())),
        "rolls": int(len(rolls)),
        "continuous_rows": int(len(continuous)),
        "roll_yield_rows": int(len(roll_yield)),
        "written": int(written),
        "errors": [],
        "last_ingested_ts_ms": int(anchor_ms),
    }
