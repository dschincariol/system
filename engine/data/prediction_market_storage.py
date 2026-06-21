"""Provider-neutral prediction-market storage helpers.

The tables in this module store market expectations as read-only external
facts.  They intentionally do not model account state, orders, positions, or
any write-capable trading endpoint.
"""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Mapping, Sequence


PROVIDER_CATEGORY_MACRO = "macro"
PROVIDER_CATEGORY_EVENT_SIGNAL = "event_signal"


def canonical_json(value: Any) -> str:
    return json.dumps(value if value is not None else {}, separators=(",", ":"), sort_keys=True, default=str)


def raw_payload_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


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


def ensure_prediction_market_schema(con) -> None:
    """Create prediction-market tables in SQLite or Postgres-compatible SQL."""

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS prediction_market_events (
            id INTEGER PRIMARY KEY,
            provider_name TEXT NOT NULL,
            provider_event_id TEXT NOT NULL,
            event_ticker TEXT NOT NULL,
            series_ticker TEXT,
            title TEXT,
            product_id TEXT,
            official_resolution_source TEXT,
            source_file_date TEXT,
            source_file_kind TEXT,
            refresh_cadence TEXT,
            provider_timestamp_ms INTEGER,
            provider_category TEXT NOT NULL,
            event_type TEXT,
            semantic_event_id TEXT,
            resolution_semantics TEXT,
            event_ts_ms INTEGER,
            resolution_ts_ms INTEGER,
            source_ts_ms INTEGER NOT NULL,
            availability_ts_ms INTEGER NOT NULL,
            affected_assets_json TEXT NOT NULL DEFAULT '[]',
            raw_payload_hash TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            created_ts_ms INTEGER NOT NULL,
            updated_ts_ms INTEGER NOT NULL,
            UNIQUE(provider_name, provider_event_id)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS prediction_market_markets (
            id INTEGER PRIMARY KEY,
            provider_name TEXT NOT NULL,
            provider_market_id TEXT NOT NULL,
            provider_event_id TEXT NOT NULL,
            market_ticker TEXT NOT NULL,
            series_ticker TEXT,
            title TEXT,
            subtitle TEXT,
            provider_contract_id TEXT,
            product_id TEXT,
            official_resolution_source TEXT,
            source_file_date TEXT,
            source_file_kind TEXT,
            refresh_cadence TEXT,
            provider_timestamp_ms INTEGER,
            provider_category TEXT NOT NULL,
            event_type TEXT,
            condition_id TEXT,
            token_id TEXT,
            outcome_name TEXT,
            semantic_event_id TEXT,
            resolution_semantics TEXT,
            status TEXT,
            probability REAL,
            previous_probability REAL,
            probability_delta REAL,
            bid_probability REAL,
            ask_probability REAL,
            last_price REAL,
            liquidity REAL,
            volume REAL,
            volume_24h REAL,
            open_interest REAL,
            spread REAL,
            event_ts_ms INTEGER,
            close_ts_ms INTEGER,
            resolution_ts_ms INTEGER,
            source_ts_ms INTEGER NOT NULL,
            availability_ts_ms INTEGER NOT NULL,
            affected_assets_json TEXT NOT NULL DEFAULT '[]',
            raw_payload_hash TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            created_ts_ms INTEGER NOT NULL,
            updated_ts_ms INTEGER NOT NULL,
            UNIQUE(provider_name, provider_market_id)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS prediction_market_orderbook_snapshots (
            id INTEGER PRIMARY KEY,
            provider_name TEXT NOT NULL,
            provider_market_id TEXT NOT NULL,
            condition_id TEXT,
            token_id TEXT,
            provider_contract_id TEXT,
            product_id TEXT,
            source_file_date TEXT,
            source_file_kind TEXT,
            source_ts_ms INTEGER NOT NULL,
            availability_ts_ms INTEGER NOT NULL,
            best_yes_bid REAL,
            best_yes_ask REAL,
            best_no_bid REAL,
            best_no_ask REAL,
            mid_probability REAL,
            spread REAL,
            yes_depth REAL,
            no_depth REAL,
            liquidity REAL,
            imbalance REAL,
            raw_payload_hash TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            created_ts_ms INTEGER NOT NULL,
            UNIQUE(provider_name, provider_market_id, availability_ts_ms, raw_payload_hash)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS prediction_market_price_history (
            id INTEGER PRIMARY KEY,
            provider_name TEXT NOT NULL,
            provider_market_id TEXT NOT NULL,
            condition_id TEXT,
            token_id TEXT,
            provider_contract_id TEXT,
            product_id TEXT,
            source_file_date TEXT,
            source_file_kind TEXT,
            trade_id TEXT NOT NULL,
            trade_ts_ms INTEGER NOT NULL,
            source_ts_ms INTEGER NOT NULL,
            availability_ts_ms INTEGER NOT NULL,
            price REAL,
            size REAL,
            side TEXT,
            raw_payload_hash TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            created_ts_ms INTEGER NOT NULL,
            UNIQUE(provider_name, provider_market_id, trade_id)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS prediction_market_backfill_state (
            provider_name TEXT NOT NULL,
            state_key TEXT NOT NULL,
            status TEXT,
            cursor_json TEXT,
            updated_ts_ms INTEGER NOT NULL,
            error TEXT,
            PRIMARY KEY(provider_name, state_key)
        )
        """
    )
    _ensure_compat_columns(con)
    for stmt in (
        "CREATE INDEX IF NOT EXISTS idx_prediction_market_events_avail ON prediction_market_events(provider_category, availability_ts_ms DESC)",
        "CREATE INDEX IF NOT EXISTS idx_prediction_market_events_resolution ON prediction_market_events(provider_name, resolution_ts_ms)",
        "CREATE INDEX IF NOT EXISTS idx_prediction_market_markets_avail ON prediction_market_markets(provider_category, availability_ts_ms DESC)",
        "CREATE INDEX IF NOT EXISTS idx_prediction_market_markets_event ON prediction_market_markets(provider_name, provider_event_id, availability_ts_ms DESC)",
        "CREATE INDEX IF NOT EXISTS idx_prediction_market_markets_semantic ON prediction_market_markets(semantic_event_id, resolution_semantics, availability_ts_ms DESC)",
        "CREATE INDEX IF NOT EXISTS idx_prediction_market_orderbook_avail ON prediction_market_orderbook_snapshots(provider_name, provider_market_id, availability_ts_ms DESC)",
        "CREATE INDEX IF NOT EXISTS idx_prediction_market_price_history_avail ON prediction_market_price_history(provider_name, provider_market_id, availability_ts_ms DESC)",
    ):
        con.execute(stmt)


def _table_columns(con, table_name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall()
        if rows:
            return {str(row[1]) for row in rows}
    except Exception:
        pass
    try:
        rows = con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = ?
            """,
            (str(table_name),),
        ).fetchall()
        return {str(row[0]) for row in rows or []}
    except Exception:
        return set()


def _add_column_if_missing(con, table_name: str, column_name: str, column_type: str) -> None:
    columns = _table_columns(con, table_name)
    if columns and str(column_name) in columns:
        return
    try:
        con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
    except Exception:
        # Postgres migrations own production ALTERs; this compatibility helper
        # mainly keeps in-memory SQLite tables created by older tests usable.
        pass


def _ensure_compat_columns(con) -> None:
    for column_name in (
        "product_id",
        "official_resolution_source",
        "source_file_date",
        "source_file_kind",
        "refresh_cadence",
    ):
        _add_column_if_missing(con, "prediction_market_events", column_name, "TEXT")
    _add_column_if_missing(con, "prediction_market_events", "provider_timestamp_ms", "INTEGER")
    for column_name in ("semantic_event_id", "resolution_semantics"):
        _add_column_if_missing(con, "prediction_market_events", column_name, "TEXT")
    for column_name in (
        "provider_contract_id",
        "product_id",
        "official_resolution_source",
        "source_file_date",
        "source_file_kind",
        "refresh_cadence",
    ):
        _add_column_if_missing(con, "prediction_market_markets", column_name, "TEXT")
    _add_column_if_missing(con, "prediction_market_markets", "provider_timestamp_ms", "INTEGER")
    for column_name in ("condition_id", "token_id", "outcome_name", "semantic_event_id", "resolution_semantics"):
        _add_column_if_missing(con, "prediction_market_markets", column_name, "TEXT")
    for column_name in ("provider_contract_id", "product_id", "source_file_date", "source_file_kind"):
        _add_column_if_missing(con, "prediction_market_orderbook_snapshots", column_name, "TEXT")
        _add_column_if_missing(con, "prediction_market_price_history", column_name, "TEXT")
    for column_name in ("condition_id", "token_id"):
        _add_column_if_missing(con, "prediction_market_orderbook_snapshots", column_name, "TEXT")
        _add_column_if_missing(con, "prediction_market_price_history", column_name, "TEXT")


def _json_text(value: Any, default: Any) -> str:
    if value is None:
        return canonical_json(default)
    return canonical_json(value)


def _event_row(record: Mapping[str, Any], now_ms: int) -> tuple[Any, ...]:
    raw = record.get("raw_payload", record)
    return (
        str(record.get("provider_name") or ""),
        str(record.get("provider_event_id") or record.get("event_ticker") or ""),
        str(record.get("event_ticker") or record.get("provider_event_id") or ""),
        str(record.get("series_ticker") or ""),
        str(record.get("title") or ""),
        str(record.get("product_id") or ""),
        str(record.get("official_resolution_source") or ""),
        str(record.get("source_file_date") or ""),
        str(record.get("source_file_kind") or ""),
        str(record.get("refresh_cadence") or ""),
        safe_int(record.get("provider_timestamp_ms"), 0) or None,
        str(record.get("provider_category") or PROVIDER_CATEGORY_MACRO),
        str(record.get("event_type") or ""),
        str(record.get("semantic_event_id") or ""),
        str(record.get("resolution_semantics") or ""),
        safe_int(record.get("event_ts_ms"), 0) or None,
        safe_int(record.get("resolution_ts_ms"), 0) or None,
        safe_int(record.get("source_ts_ms"), now_ms) or int(now_ms),
        safe_int(record.get("availability_ts_ms"), now_ms) or int(now_ms),
        _json_text(record.get("affected_assets"), []),
        str(record.get("raw_payload_hash") or raw_payload_hash(raw)),
        canonical_json(raw),
        int(now_ms),
        int(now_ms),
    )


def _market_row(record: Mapping[str, Any], now_ms: int) -> tuple[Any, ...]:
    raw = record.get("raw_payload", record)
    probability = record.get("probability")
    previous = record.get("previous_probability")
    probability_delta = record.get("probability_delta")
    if probability_delta is None and probability is not None and previous is not None:
        probability_delta = safe_float(probability) - safe_float(previous)
    return (
        str(record.get("provider_name") or ""),
        str(record.get("provider_market_id") or record.get("market_ticker") or ""),
        str(record.get("provider_event_id") or record.get("event_ticker") or ""),
        str(record.get("market_ticker") or record.get("provider_market_id") or ""),
        str(record.get("series_ticker") or ""),
        str(record.get("title") or ""),
        str(record.get("subtitle") or ""),
        str(record.get("provider_contract_id") or record.get("provider_market_id") or record.get("market_ticker") or ""),
        str(record.get("product_id") or ""),
        str(record.get("official_resolution_source") or ""),
        str(record.get("source_file_date") or ""),
        str(record.get("source_file_kind") or ""),
        str(record.get("refresh_cadence") or ""),
        safe_int(record.get("provider_timestamp_ms"), 0) or None,
        str(record.get("provider_category") or PROVIDER_CATEGORY_MACRO),
        str(record.get("event_type") or ""),
        str(record.get("condition_id") or ""),
        str(record.get("token_id") or ""),
        str(record.get("outcome_name") or ""),
        str(record.get("semantic_event_id") or ""),
        str(record.get("resolution_semantics") or ""),
        str(record.get("status") or ""),
        safe_float(probability, 0.0) if probability is not None else None,
        safe_float(previous, 0.0) if previous is not None else None,
        safe_float(probability_delta, 0.0) if probability_delta is not None else None,
        safe_float(record.get("bid_probability"), 0.0) if record.get("bid_probability") is not None else None,
        safe_float(record.get("ask_probability"), 0.0) if record.get("ask_probability") is not None else None,
        safe_float(record.get("last_price"), 0.0) if record.get("last_price") is not None else None,
        safe_float(record.get("liquidity"), 0.0),
        safe_float(record.get("volume"), 0.0),
        safe_float(record.get("volume_24h"), 0.0),
        safe_float(record.get("open_interest"), 0.0),
        safe_float(record.get("spread"), 0.0) if record.get("spread") is not None else None,
        safe_int(record.get("event_ts_ms"), 0) or None,
        safe_int(record.get("close_ts_ms"), 0) or None,
        safe_int(record.get("resolution_ts_ms"), 0) or None,
        safe_int(record.get("source_ts_ms"), now_ms) or int(now_ms),
        safe_int(record.get("availability_ts_ms"), now_ms) or int(now_ms),
        _json_text(record.get("affected_assets"), []),
        str(record.get("raw_payload_hash") or raw_payload_hash(raw)),
        canonical_json(raw),
        int(now_ms),
        int(now_ms),
    )


def _orderbook_row(record: Mapping[str, Any], now_ms: int) -> tuple[Any, ...]:
    raw = record.get("raw_payload", record)
    return (
        str(record.get("provider_name") or ""),
        str(record.get("provider_market_id") or record.get("market_ticker") or ""),
        str(record.get("condition_id") or ""),
        str(record.get("token_id") or ""),
        str(record.get("provider_contract_id") or record.get("provider_market_id") or record.get("market_ticker") or ""),
        str(record.get("product_id") or ""),
        str(record.get("source_file_date") or ""),
        str(record.get("source_file_kind") or ""),
        safe_int(record.get("source_ts_ms"), now_ms) or int(now_ms),
        safe_int(record.get("availability_ts_ms"), now_ms) or int(now_ms),
        safe_float(record.get("best_yes_bid"), 0.0) if record.get("best_yes_bid") is not None else None,
        safe_float(record.get("best_yes_ask"), 0.0) if record.get("best_yes_ask") is not None else None,
        safe_float(record.get("best_no_bid"), 0.0) if record.get("best_no_bid") is not None else None,
        safe_float(record.get("best_no_ask"), 0.0) if record.get("best_no_ask") is not None else None,
        safe_float(record.get("mid_probability"), 0.0) if record.get("mid_probability") is not None else None,
        safe_float(record.get("spread"), 0.0) if record.get("spread") is not None else None,
        safe_float(record.get("yes_depth"), 0.0),
        safe_float(record.get("no_depth"), 0.0),
        safe_float(record.get("liquidity"), 0.0),
        safe_float(record.get("imbalance"), 0.0),
        str(record.get("raw_payload_hash") or raw_payload_hash(raw)),
        canonical_json(raw),
        int(now_ms),
    )


def _trade_row(record: Mapping[str, Any], now_ms: int) -> tuple[Any, ...]:
    raw = record.get("raw_payload", record)
    trade_id = str(record.get("trade_id") or raw_payload_hash(raw))
    return (
        str(record.get("provider_name") or ""),
        str(record.get("provider_market_id") or record.get("market_ticker") or ""),
        str(record.get("condition_id") or ""),
        str(record.get("token_id") or ""),
        str(record.get("provider_contract_id") or record.get("provider_market_id") or record.get("market_ticker") or ""),
        str(record.get("product_id") or ""),
        str(record.get("source_file_date") or ""),
        str(record.get("source_file_kind") or ""),
        trade_id,
        safe_int(record.get("trade_ts_ms"), record.get("source_ts_ms") or now_ms) or int(now_ms),
        safe_int(record.get("source_ts_ms"), now_ms) or int(now_ms),
        safe_int(record.get("availability_ts_ms"), now_ms) or int(now_ms),
        safe_float(record.get("price"), 0.0) if record.get("price") is not None else None,
        safe_float(record.get("size"), 0.0) if record.get("size") is not None else None,
        str(record.get("side") or ""),
        str(record.get("raw_payload_hash") or raw_payload_hash(raw)),
        canonical_json(raw),
        int(now_ms),
    )


def put_prediction_market_batch(
    con,
    *,
    events: Sequence[Mapping[str, Any]] | None = None,
    markets: Sequence[Mapping[str, Any]] | None = None,
    orderbooks: Sequence[Mapping[str, Any]] | None = None,
    trades: Sequence[Mapping[str, Any]] | None = None,
    now_ms: int,
) -> dict[str, int]:
    """Idempotently persist normalized prediction-market rows."""

    ensure_prediction_market_schema(con)
    counts = {"events": 0, "markets": 0, "orderbooks": 0, "trades": 0}

    for record in events or []:
        con.execute(
            """
            INSERT INTO prediction_market_events (
              provider_name, provider_event_id, event_ticker, series_ticker, title,
              product_id, official_resolution_source, source_file_date, source_file_kind,
              refresh_cadence, provider_timestamp_ms,
              provider_category, event_type, semantic_event_id, resolution_semantics,
              event_ts_ms, resolution_ts_ms, source_ts_ms, availability_ts_ms,
              affected_assets_json, raw_payload_hash, raw_json,
              created_ts_ms, updated_ts_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider_name, provider_event_id) DO UPDATE SET
              event_ticker=excluded.event_ticker,
              series_ticker=excluded.series_ticker,
              title=excluded.title,
              product_id=excluded.product_id,
              official_resolution_source=excluded.official_resolution_source,
              source_file_date=excluded.source_file_date,
              source_file_kind=excluded.source_file_kind,
              refresh_cadence=excluded.refresh_cadence,
              provider_timestamp_ms=excluded.provider_timestamp_ms,
              provider_category=excluded.provider_category,
              event_type=excluded.event_type,
              semantic_event_id=excluded.semantic_event_id,
              resolution_semantics=excluded.resolution_semantics,
              event_ts_ms=excluded.event_ts_ms,
              resolution_ts_ms=excluded.resolution_ts_ms,
              source_ts_ms=excluded.source_ts_ms,
              availability_ts_ms=excluded.availability_ts_ms,
              affected_assets_json=excluded.affected_assets_json,
              raw_payload_hash=excluded.raw_payload_hash,
              raw_json=excluded.raw_json,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            _event_row(record, int(now_ms)),
        )
        counts["events"] += 1

    for record in markets or []:
        con.execute(
            """
            INSERT INTO prediction_market_markets (
              provider_name, provider_market_id, provider_event_id, market_ticker,
              series_ticker, title, subtitle,
              provider_contract_id, product_id, official_resolution_source,
              source_file_date, source_file_kind, refresh_cadence, provider_timestamp_ms,
              provider_category, event_type,
              condition_id, token_id, outcome_name, semantic_event_id,
              resolution_semantics, status, probability, previous_probability, probability_delta,
              bid_probability, ask_probability, last_price, liquidity, volume, volume_24h,
              open_interest, spread, event_ts_ms, close_ts_ms, resolution_ts_ms,
              source_ts_ms, availability_ts_ms, affected_assets_json, raw_payload_hash, raw_json,
              created_ts_ms, updated_ts_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider_name, provider_market_id) DO UPDATE SET
              provider_event_id=excluded.provider_event_id,
              market_ticker=excluded.market_ticker,
              series_ticker=excluded.series_ticker,
              title=excluded.title,
              subtitle=excluded.subtitle,
              provider_contract_id=excluded.provider_contract_id,
              product_id=excluded.product_id,
              official_resolution_source=excluded.official_resolution_source,
              source_file_date=excluded.source_file_date,
              source_file_kind=excluded.source_file_kind,
              refresh_cadence=excluded.refresh_cadence,
              provider_timestamp_ms=excluded.provider_timestamp_ms,
              provider_category=excluded.provider_category,
              event_type=excluded.event_type,
              status=excluded.status,
              condition_id=excluded.condition_id,
              token_id=excluded.token_id,
              outcome_name=excluded.outcome_name,
              semantic_event_id=excluded.semantic_event_id,
              resolution_semantics=excluded.resolution_semantics,
              probability=excluded.probability,
              previous_probability=excluded.previous_probability,
              probability_delta=excluded.probability_delta,
              bid_probability=excluded.bid_probability,
              ask_probability=excluded.ask_probability,
              last_price=excluded.last_price,
              liquidity=excluded.liquidity,
              volume=excluded.volume,
              volume_24h=excluded.volume_24h,
              open_interest=excluded.open_interest,
              spread=excluded.spread,
              event_ts_ms=excluded.event_ts_ms,
              close_ts_ms=excluded.close_ts_ms,
              resolution_ts_ms=excluded.resolution_ts_ms,
              source_ts_ms=excluded.source_ts_ms,
              availability_ts_ms=excluded.availability_ts_ms,
              affected_assets_json=excluded.affected_assets_json,
              raw_payload_hash=excluded.raw_payload_hash,
              raw_json=excluded.raw_json,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            _market_row(record, int(now_ms)),
        )
        counts["markets"] += 1

    for record in orderbooks or []:
        con.execute(
            """
            INSERT INTO prediction_market_orderbook_snapshots (
              provider_name, provider_market_id, condition_id, token_id,
              provider_contract_id, product_id, source_file_date, source_file_kind,
              source_ts_ms, availability_ts_ms,
              best_yes_bid, best_yes_ask, best_no_bid, best_no_ask, mid_probability,
              spread, yes_depth, no_depth, liquidity, imbalance, raw_payload_hash,
              raw_json, created_ts_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider_name, provider_market_id, availability_ts_ms, raw_payload_hash) DO NOTHING
            """,
            _orderbook_row(record, int(now_ms)),
        )
        counts["orderbooks"] += 1

    for record in trades or []:
        con.execute(
            """
            INSERT INTO prediction_market_price_history (
              provider_name, provider_market_id, condition_id, token_id,
              provider_contract_id, product_id, source_file_date, source_file_kind, trade_id,
              trade_ts_ms, source_ts_ms, availability_ts_ms, price, size, side,
              raw_payload_hash, raw_json, created_ts_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider_name, provider_market_id, trade_id) DO UPDATE SET
              condition_id=excluded.condition_id,
              token_id=excluded.token_id,
              provider_contract_id=excluded.provider_contract_id,
              product_id=excluded.product_id,
              source_file_date=excluded.source_file_date,
              source_file_kind=excluded.source_file_kind,
              trade_ts_ms=excluded.trade_ts_ms,
              source_ts_ms=excluded.source_ts_ms,
              availability_ts_ms=excluded.availability_ts_ms,
              price=excluded.price,
              size=excluded.size,
              side=excluded.side,
              raw_payload_hash=excluded.raw_payload_hash,
              raw_json=excluded.raw_json
            """,
            _trade_row(record, int(now_ms)),
        )
        counts["trades"] += 1

    return counts
