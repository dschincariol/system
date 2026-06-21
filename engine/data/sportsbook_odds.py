"""Read-only sportsbook and betting-exchange odds research utilities.

This module treats odds as alternative research data only.  It normalizes
provider feeds, removes vig before feature use, enforces explicit asset
mappings, and never models betting execution, accounts, balances, or wagers.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

import requests


SPORTSBOOK_ODDS_FEATURE_GROUP = "sports_odds_sector_v1"
SPORTSBOOK_ODDS_FEATURE_PREFIX = f"{SPORTSBOOK_ODDS_FEATURE_GROUP}."
SPORTSBOOK_ODDS_FEATURE_IDS = [
    f"{SPORTSBOOK_ODDS_FEATURE_PREFIX}no_vig_probability_level",
    f"{SPORTSBOOK_ODDS_FEATURE_PREFIX}no_vig_probability_move",
    f"{SPORTSBOOK_ODDS_FEATURE_PREFIX}liquidity_score",
    f"{SPORTSBOOK_ODDS_FEATURE_PREFIX}market_count",
    f"{SPORTSBOOK_ODDS_FEATURE_PREFIX}provider_count",
    f"{SPORTSBOOK_ODDS_FEATURE_PREFIX}available",
]

SPORTSBOOK_ODDS_FORBIDDEN_KEYS = {
    "account",
    "balance",
    "bet",
    "betting",
    "customer",
    "login",
    "order",
    "password",
    "private_key",
    "session_token",
    "stake",
    "trade",
    "trading",
    "username",
    "wallet",
    "wager",
}
SPORTSBOOK_ODDS_FORBIDDEN_KEY_TOKENS = {
    "account",
    "balance",
    "bet",
    "betting",
    "customer",
    "login",
    "order",
    "password",
    "private",
    "session",
    "stake",
    "trade",
    "trading",
    "username",
    "wallet",
    "wager",
}

SPORTSBOOK_ODDS_NARROW_ASSET_BASKETS: dict[str, tuple[str, ...]] = {
    "sportsbook_equities": ("DKNG", "FLUT", "PENN", "MGM", "WYNN", "LVS", "CHDN", "RSI"),
    "sports_data_providers": ("GENI", "SRAD"),
    "sports_media": ("DIS", "FOXA", "FOX", "PARA", "WBD", "CMCSA", "NFLX", "ROKU"),
    "ad_sensitive": ("TTD", "META", "GOOG", "GOOGL", "SNAP", "PINS"),
    "gaming": ("EA", "TTWO", "RBLX", "SONY"),
    "apparel_sponsors": ("NKE", "UAA", "UA", "LULU", "SKX", "DECK"),
}
SPORTSBOOK_ODDS_ALLOWED_NARROW_ASSETS = frozenset(
    symbol for values in SPORTSBOOK_ODDS_NARROW_ASSET_BASKETS.values() for symbol in values
)
SPORTSBOOK_ODDS_MAPPING_APPROVAL_STATUSES = frozenset({"research", "pending", "approved", "rejected", "disabled"})
SPORTSBOOK_ODDS_MIN_OOS_SAMPLES = int(os.environ.get("SPORTSBOOK_ODDS_MIN_OOS_SAMPLES", "30"))
SPORTSBOOK_ODDS_MIN_OOS_MEAN_NET_RETURN = float(os.environ.get("SPORTSBOOK_ODDS_MIN_OOS_MEAN_NET_RETURN", "0.0"))
SPORTSBOOK_ODDS_MIN_OOS_HIT_RATE = float(os.environ.get("SPORTSBOOK_ODDS_MIN_OOS_HIT_RATE", "0.50"))
SPORTSBOOK_ODDS_MAX_FDR_Q = float(os.environ.get("SPORTSBOOK_ODDS_MAX_FDR_Q", "0.10"))
SPORTSBOOK_ODDS_MAX_ABS_BENCHMARK_CORR = float(os.environ.get("SPORTSBOOK_ODDS_MAX_ABS_BENCHMARK_CORR", "0.70"))


def utc_ms() -> int:
    return int(time.time() * 1000)


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
        out = int(float(value))
    except Exception:
        return int(default)
    return int(out)


def parse_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    return [part.strip() for part in text.split(",") if part.strip()]


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except Exception:
            return parse_list(value)
        if isinstance(parsed, list):
            return list(parsed)
    return []


def _feature_id_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return parse_list(value)
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item or "").strip()]
    return [str(value).strip()] if str(value or "").strip() else []


def _clean_symbol(value: Any) -> str:
    return str(value or "").upper().strip().replace("$", "").replace(".", "-")


def _clean_key(value: Any) -> str:
    return str(value or "").strip().lower().replace(" ", "_").replace("-", "_")


def _bool_value(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _clip(value: float, lo: float, hi: float) -> float:
    return float(max(float(lo), min(float(hi), float(value))))


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_model_config_paths() -> list[Path]:
    return [_repo_root() / "data" / "model_configs.json"]


def validate_sportsbook_odds_read_only_settings(
    settings: Mapping[str, Any] | None = None,
    credentials: Mapping[str, Any] | None = None,
) -> None:
    """Reject betting-account or execution-shaped settings and credentials."""

    keys = {
        str(key or "").strip().lower()
        for source in (settings or {}, credentials or {})
        for key in dict(source or {}).keys()
    }
    forbidden: list[str] = []
    for key in keys:
        normalized = re.sub(r"[^a-z0-9]+", "_", key).strip("_")
        tokens = {part for part in normalized.split("_") if part}
        if normalized in SPORTSBOOK_ODDS_FORBIDDEN_KEYS or tokens.intersection(SPORTSBOOK_ODDS_FORBIDDEN_KEY_TOKENS):
            forbidden.append(key)
    forbidden = sorted(set(forbidden))
    if forbidden:
        raise ValueError(f"sportsbook_odds_execution_credentials_forbidden:{','.join(forbidden)}")


def implied_probability_from_odds(value: Any, odds_format: str = "american") -> float:
    """Convert American, decimal, fractional, or probability-style odds."""

    fmt = str(odds_format or "").strip().lower()
    if fmt in {"probability", "prob", "implied_probability"}:
        return _clip(safe_float(value, 0.0), 0.0, 1.0)
    if fmt in {"decimal", "dec"}:
        decimal = safe_float(value, 0.0)
        if decimal <= 1.0:
            raise ValueError("sportsbook_odds_decimal_odds_must_exceed_one")
        return _clip(1.0 / decimal, 0.0, 1.0)
    if fmt in {"fractional", "frac"}:
        text = str(value or "").strip()
        if "/" not in text:
            raise ValueError("sportsbook_odds_fractional_odds_invalid")
        numerator, denominator = text.split("/", 1)
        num = safe_float(numerator, 0.0)
        den = safe_float(denominator, 0.0)
        if num <= 0.0 or den <= 0.0:
            raise ValueError("sportsbook_odds_fractional_odds_invalid")
        return _clip(den / (num + den), 0.0, 1.0)
    american = safe_float(value, 0.0)
    if american == 0.0:
        raise ValueError("sportsbook_odds_american_odds_zero")
    if american > 0:
        return _clip(100.0 / (american + 100.0), 0.0, 1.0)
    return _clip(abs(american) / (abs(american) + 100.0), 0.0, 1.0)


def remove_vig(raw_probabilities: Sequence[Any]) -> list[float]:
    """Normalize multi-outcome implied probabilities to a no-vig sum of 1."""

    raw = [_clip(safe_float(value, 0.0), 0.0, 1.0) for value in list(raw_probabilities or [])]
    total = sum(raw)
    if total <= 0.0:
        raise ValueError("sportsbook_odds_probability_sum_nonpositive")
    return [float(value / total) for value in raw]


def normalize_multi_outcome_probabilities(outcomes: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return outcomes with raw implied and no-vig probabilities attached."""

    parsed: list[dict[str, Any]] = []
    raw_probs: list[float] = []
    for outcome in outcomes or []:
        item = dict(outcome or {})
        raw_probability = item.get("raw_implied_probability")
        if raw_probability is None:
            raw_probability = implied_probability_from_odds(
                item.get("odds") if item.get("odds") is not None else item.get("price"),
                str(item.get("odds_format") or item.get("format") or "american"),
            )
        raw = _clip(safe_float(raw_probability, 0.0), 0.0, 1.0)
        item["raw_implied_probability"] = raw
        parsed.append(item)
        raw_probs.append(raw)
    normalized = remove_vig(raw_probs)
    for item, probability in zip(parsed, normalized):
        item["no_vig_probability"] = float(probability)
    return parsed


def _market_id(payload: Mapping[str, Any], market_type: str) -> str:
    value = payload.get("provider_market_id") or payload.get("market_id") or payload.get("id")
    if str(value or "").strip():
        return str(value).strip()
    return f"{payload.get('event_id') or payload.get('provider_event_id') or ''}:{market_type}"


def _settlement_status_if_resolved(
    status: Any,
    *,
    resolution_ts_ms: int | None,
    availability_ts_ms: int,
    now_ms: int,
) -> str:
    if not str(status or "").strip():
        return ""
    if not resolution_ts_ms:
        return ""
    if int(resolution_ts_ms) > min(int(availability_ts_ms), int(now_ms)):
        return ""
    return str(status or "").strip().lower()


def normalize_event_odds(event: Mapping[str, Any], *, now_ms: int | None = None) -> list[dict[str, Any]]:
    """Normalize one event/market payload into one row per outcome."""

    observed = int(now_ms if now_ms is not None else utc_ms())
    payload = dict(event or {})
    provider = str(payload.get("provider") or payload.get("provider_name") or "sportsbook_odds").strip().lower()
    sport_key = _clean_key(payload.get("sport_key") or payload.get("sport") or payload.get("sport_title"))
    league = _clean_key(payload.get("league") or payload.get("competition") or payload.get("sport_league"))
    event_category = _clean_key(payload.get("event_category") or payload.get("category") or f"{sport_key}_{league}".strip("_"))
    provider_event_id = str(payload.get("provider_event_id") or payload.get("event_id") or payload.get("id") or "").strip()
    market_type = _clean_key(payload.get("market_type") or payload.get("market") or "moneyline")
    if not provider_event_id:
        raise ValueError("sportsbook_odds_provider_event_id_required")
    if not sport_key or not market_type:
        raise ValueError("sportsbook_odds_sport_and_market_required")
    outcomes = _json_list(payload.get("outcomes"))
    if not outcomes and payload.get("outcome"):
        outcomes = [dict(payload)]
    if len(outcomes) < 2:
        raise ValueError("sportsbook_odds_multi_outcome_market_required")

    source_ts_ms = safe_int(payload.get("source_ts_ms") or payload.get("timestamp_ms") or payload.get("last_update_ms"), observed)
    availability_ts_ms = safe_int(payload.get("availability_ts_ms"), observed) or observed
    event_start_ts_ms = safe_int(payload.get("event_start_ts_ms") or payload.get("commence_time_ms"), 0) or None
    resolution_ts_ms = safe_int(payload.get("resolution_ts_ms") or payload.get("settled_ts_ms"), 0) or None
    provider_market_id = _market_id(payload, market_type)
    normalized = normalize_multi_outcome_probabilities([dict(item or {}) for item in outcomes if isinstance(item, Mapping)])
    rows: list[dict[str, Any]] = []
    for outcome in normalized:
        raw = dict(outcome or {})
        outcome_name = str(raw.get("outcome") or raw.get("name") or raw.get("runner_name") or raw.get("selection") or "").strip()
        if not outcome_name:
            raise ValueError("sportsbook_odds_outcome_name_required")
        odds_format = str(raw.get("odds_format") or raw.get("format") or payload.get("odds_format") or "american").strip().lower()
        odds_value = raw.get("odds") if raw.get("odds") is not None else raw.get("price")
        line_value = raw.get("line") if raw.get("line") is not None else payload.get("line")
        spread_value = raw.get("spread") if raw.get("spread") is not None else payload.get("spread")
        total_value = raw.get("total") if raw.get("total") is not None else payload.get("total")
        row_resolution_ts = safe_int(raw.get("resolution_ts_ms"), 0) or resolution_ts_ms
        row_availability = safe_int(raw.get("availability_ts_ms"), availability_ts_ms) or availability_ts_ms
        row = {
            "provider": provider,
            "sport_key": sport_key,
            "league": league,
            "provider_event_id": provider_event_id,
            "provider_market_id": str(raw.get("provider_market_id") or raw.get("market_id") or provider_market_id),
            "event_category": event_category,
            "market_type": market_type,
            "outcome_name": outcome_name,
            "odds_format": odds_format,
            "odds_value": safe_float(odds_value, 0.0) if odds_value not in (None, "") and odds_format != "fractional" else None,
            "raw_implied_probability": _clip(safe_float(raw.get("raw_implied_probability"), 0.0), 0.0, 1.0),
            "no_vig_probability": _clip(safe_float(raw.get("no_vig_probability"), 0.0), 0.0, 1.0),
            "line": safe_float(line_value, 0.0) if line_value not in (None, "") else None,
            "spread": safe_float(spread_value, 0.0) if spread_value not in (None, "") else None,
            "total": safe_float(total_value, 0.0) if total_value not in (None, "") else None,
            "event_start_ts_ms": event_start_ts_ms,
            "source_ts_ms": safe_int(raw.get("source_ts_ms"), source_ts_ms) or source_ts_ms,
            "availability_ts_ms": row_availability,
            "volume": safe_float(raw.get("volume") if raw.get("volume") is not None else payload.get("volume"), 0.0),
            "liquidity": safe_float(raw.get("liquidity") if raw.get("liquidity") is not None else payload.get("liquidity"), 0.0),
            "settlement_status": _settlement_status_if_resolved(
                raw.get("settlement_status") or payload.get("settlement_status"),
                resolution_ts_ms=row_resolution_ts,
                availability_ts_ms=row_availability,
                now_ms=observed,
            ),
            "settlement_ts_ms": row_resolution_ts,
            "resolution_ts_ms": row_resolution_ts,
            "raw_payload_hash": raw_payload_hash({"event": payload, "outcome": raw}),
            "raw_payload": {"event": payload, "outcome": raw},
        }
        rows.append(row)
    return rows


def _sqlite_columns(con, table_name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall() or []
        return {str(row[1]) for row in rows}
    except Exception:
        return set()


def _ensure_sqlite_column(con, table_name: str, column_name: str, definition: str) -> None:
    if str(column_name) in _sqlite_columns(con, table_name):
        return
    try:
        con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")
    except Exception:
        # Postgres migrations own production schema evolution; this fallback is
        # for SQLite tests and local research stores.
        return


def ensure_sportsbook_odds_schema(con) -> None:
    """Create SQLite-compatible sportsbook odds research tables."""

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS sportsbook_odds_snapshots (
          id INTEGER PRIMARY KEY,
          provider TEXT NOT NULL,
          sport_key TEXT NOT NULL,
          league TEXT NOT NULL DEFAULT '',
          provider_event_id TEXT NOT NULL,
          provider_market_id TEXT NOT NULL DEFAULT '',
          event_category TEXT NOT NULL DEFAULT '',
          market_type TEXT NOT NULL,
          outcome_name TEXT NOT NULL,
          odds_format TEXT NOT NULL,
          odds_value REAL,
          raw_implied_probability REAL NOT NULL,
          no_vig_probability REAL NOT NULL,
          line REAL,
          spread REAL,
          total REAL,
          event_start_ts_ms INTEGER,
          source_ts_ms INTEGER NOT NULL,
          availability_ts_ms INTEGER NOT NULL,
          volume REAL,
          liquidity REAL,
          settlement_status TEXT NOT NULL DEFAULT '',
          settlement_ts_ms INTEGER,
          resolution_ts_ms INTEGER,
          raw_payload_hash TEXT NOT NULL,
          raw_json TEXT NOT NULL,
          created_ts_ms INTEGER NOT NULL,
          UNIQUE(provider, provider_event_id, provider_market_id, market_type, outcome_name, availability_ts_ms, raw_payload_hash)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS sportsbook_odds_asset_mappings (
          mapping_key TEXT PRIMARY KEY,
          sport_key TEXT NOT NULL,
          league TEXT NOT NULL DEFAULT '',
          event_category TEXT NOT NULL DEFAULT '',
          market_type TEXT NOT NULL,
          asset_symbol TEXT NOT NULL DEFAULT '',
          research_label TEXT NOT NULL DEFAULT '',
          stage TEXT NOT NULL DEFAULT 'research',
          enabled BOOLEAN NOT NULL DEFAULT TRUE,
          allow_feature_use BOOLEAN NOT NULL DEFAULT FALSE,
          direct_trading_authority BOOLEAN NOT NULL DEFAULT FALSE,
          mapping_version TEXT NOT NULL DEFAULT 'v1',
          mapping_rationale TEXT NOT NULL DEFAULT '',
          owner TEXT NOT NULL DEFAULT '',
          approval_status TEXT NOT NULL DEFAULT 'research',
          approved_by TEXT NOT NULL DEFAULT '',
          approved_ts_ms INTEGER,
          approval_reason TEXT NOT NULL DEFAULT '',
          approved_for_promotion BOOLEAN NOT NULL DEFAULT FALSE,
          source_control_ref TEXT NOT NULL DEFAULT '',
          notes TEXT,
          created_ts_ms INTEGER NOT NULL,
          updated_ts_ms INTEGER NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS sportsbook_odds_promotion_evidence (
          evidence_key TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          ts_ms INTEGER NOT NULL,
          feature_group TEXT NOT NULL,
          mapping_key TEXT NOT NULL DEFAULT '',
          symbol TEXT NOT NULL DEFAULT '',
          mapping_version TEXT NOT NULL DEFAULT '',
          dataset_hash TEXT NOT NULL DEFAULT '',
          feature_ids_json TEXT NOT NULL DEFAULT '[]',
          start_ts_ms INTEGER NOT NULL,
          end_ts_ms INTEGER NOT NULL,
          train_end_ts_ms INTEGER,
          test_start_ts_ms INTEGER,
          horizon_s INTEGER NOT NULL,
          latency_ms INTEGER NOT NULL,
          fee_bps REAL NOT NULL,
          slippage_bps REAL NOT NULL,
          sample_count INTEGER NOT NULL,
          oos_sample_count INTEGER NOT NULL,
          mean_net_return REAL NOT NULL,
          oos_mean_net_return REAL NOT NULL,
          hit_rate REAL NOT NULL,
          oos_hit_rate REAL NOT NULL,
          p_value REAL NOT NULL,
          fdr_q REAL NOT NULL,
          benchmark_symbol TEXT NOT NULL DEFAULT '',
          benchmark_corr REAL,
          provider_readiness_passed BOOLEAN NOT NULL DEFAULT FALSE,
          oos_passed BOOLEAN NOT NULL DEFAULT FALSE,
          net_after_cost_passed BOOLEAN NOT NULL DEFAULT FALSE,
          pit_passed BOOLEAN NOT NULL DEFAULT FALSE,
          deconfounded_passed BOOLEAN NOT NULL DEFAULT FALSE,
          production_readiness_passed BOOLEAN NOT NULL DEFAULT FALSE,
          approval_passed BOOLEAN NOT NULL DEFAULT FALSE,
          passed BOOLEAN NOT NULL DEFAULT FALSE,
          no_go_reason TEXT NOT NULL DEFAULT '',
          direct_trading_authority BOOLEAN NOT NULL DEFAULT FALSE,
          evidence_json TEXT NOT NULL DEFAULT '{}',
          CHECK(direct_trading_authority = FALSE),
          CHECK(
            passed = FALSE OR (
              provider_readiness_passed = TRUE
              AND oos_passed = TRUE
              AND net_after_cost_passed = TRUE
              AND pit_passed = TRUE
              AND deconfounded_passed = TRUE
              AND production_readiness_passed = TRUE
              AND approval_passed = TRUE
            )
          )
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS sportsbook_odds_event_studies (
          id INTEGER PRIMARY KEY,
          run_id TEXT NOT NULL,
          ts_ms INTEGER NOT NULL,
          feature_group TEXT NOT NULL,
          mapping_key TEXT,
          symbol TEXT NOT NULL DEFAULT '',
          sport_key TEXT NOT NULL DEFAULT '',
          league TEXT NOT NULL DEFAULT '',
          market_type TEXT NOT NULL DEFAULT '',
          horizon_s INTEGER NOT NULL,
          latency_ms INTEGER NOT NULL,
          fee_bps REAL NOT NULL,
          slippage_bps REAL NOT NULL,
          sample_count INTEGER NOT NULL,
          mean_odds_move REAL NOT NULL,
          mean_forward_return REAL NOT NULL,
          mean_net_return REAL NOT NULL,
          hit_rate REAL NOT NULL,
          corr REAL,
          no_go_reason TEXT,
          direct_trading_authority BOOLEAN NOT NULL DEFAULT FALSE,
          evidence_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_sportsbook_odds_snapshots_lookup ON sportsbook_odds_snapshots(sport_key, league, event_category, market_type, availability_ts_ms DESC)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_sportsbook_odds_snapshots_event ON sportsbook_odds_snapshots(provider, provider_event_id, market_type, outcome_name, availability_ts_ms DESC)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_sportsbook_odds_mappings_asset ON sportsbook_odds_asset_mappings(asset_symbol, enabled, allow_feature_use)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_sportsbook_odds_event_studies_run ON sportsbook_odds_event_studies(run_id, ts_ms DESC)"
    )
    for column_name, definition in (
        ("mapping_version", "TEXT NOT NULL DEFAULT 'v1'"),
        ("mapping_rationale", "TEXT NOT NULL DEFAULT ''"),
        ("owner", "TEXT NOT NULL DEFAULT ''"),
        ("approval_status", "TEXT NOT NULL DEFAULT 'research'"),
        ("approved_by", "TEXT NOT NULL DEFAULT ''"),
        ("approved_ts_ms", "INTEGER"),
        ("approval_reason", "TEXT NOT NULL DEFAULT ''"),
        ("approved_for_promotion", "BOOLEAN NOT NULL DEFAULT FALSE"),
        ("source_control_ref", "TEXT NOT NULL DEFAULT ''"),
    ):
        _ensure_sqlite_column(con, "sportsbook_odds_asset_mappings", column_name, definition)
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_sportsbook_odds_mappings_approval ON sportsbook_odds_asset_mappings(approval_status, approved_for_promotion, asset_symbol)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_sportsbook_odds_promotion_evidence_symbol ON sportsbook_odds_promotion_evidence(symbol, passed, ts_ms DESC)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_sportsbook_odds_promotion_evidence_mapping ON sportsbook_odds_promotion_evidence(mapping_key, passed, ts_ms DESC)"
    )


def mapping_key_for(mapping: Mapping[str, Any]) -> str:
    parts = [
        _clean_key(mapping.get("sport_key") or mapping.get("sport")),
        _clean_key(mapping.get("league")),
        _clean_key(mapping.get("event_category") or mapping.get("category")),
        _clean_key(mapping.get("market_type") or mapping.get("market")),
        _clean_symbol(mapping.get("asset_symbol") or mapping.get("symbol")),
        _clean_key(mapping.get("research_label") or mapping.get("label")),
    ]
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def normalize_sportsbook_mapping(mapping: Mapping[str, Any], *, now_ms: int | None = None) -> dict[str, Any]:
    """Validate one explicit sports/event-to-asset or research-label mapping."""

    observed = int(now_ms if now_ms is not None else utc_ms())
    item = dict(mapping or {})
    sport_key = _clean_key(item.get("sport_key") or item.get("sport"))
    league = _clean_key(item.get("league"))
    event_category = _clean_key(item.get("event_category") or item.get("category"))
    market_type = _clean_key(item.get("market_type") or item.get("market") or "moneyline")
    asset_symbol = _clean_symbol(item.get("asset_symbol") or item.get("symbol"))
    research_label = _clean_key(item.get("research_label") or item.get("label"))
    stage = _clean_key(item.get("stage") or "research")
    if stage not in {"research", "shadow"}:
        raise ValueError("sportsbook_odds_mapping_stage_must_be_research_or_shadow")
    if not sport_key or not event_category or not market_type:
        raise ValueError("sportsbook_odds_mapping_requires_sport_category_market")
    if not asset_symbol and not research_label:
        raise ValueError("sportsbook_odds_mapping_requires_asset_or_research_label")
    direct_trading_authority = _bool_value(item.get("direct_trading_authority"), False)
    if direct_trading_authority:
        raise ValueError("sportsbook_odds_direct_trading_authority_forbidden")
    allow_feature_use = _bool_value(item.get("allow_feature_use"), False)
    if allow_feature_use:
        if not asset_symbol:
            raise ValueError("sportsbook_odds_feature_mapping_requires_asset_symbol")
        if asset_symbol not in SPORTSBOOK_ODDS_ALLOWED_NARROW_ASSETS:
            raise ValueError(f"sportsbook_odds_asset_not_in_narrow_allowlist:{asset_symbol}")
    mapping_version = str(item.get("mapping_version") or item.get("version") or "v1").strip() or "v1"
    mapping_rationale = str(item.get("mapping_rationale") or item.get("rationale") or "").strip()
    owner = str(item.get("owner") or item.get("mapping_owner") or "").strip()
    approval_status = _clean_key(item.get("approval_status") or "research")
    if approval_status not in SPORTSBOOK_ODDS_MAPPING_APPROVAL_STATUSES:
        raise ValueError(f"sportsbook_odds_mapping_approval_status_invalid:{approval_status}")
    approved_by = str(item.get("approved_by") or "").strip()
    approved_ts_ms = safe_int(item.get("approved_ts_ms"), 0) or None
    approval_reason = str(item.get("approval_reason") or item.get("approval_notes") or "").strip()
    approved_for_promotion = _bool_value(item.get("approved_for_promotion"), False)
    if approved_for_promotion and approval_status != "approved":
        raise ValueError("sportsbook_odds_mapping_promotion_requires_approved_status")
    if approval_status == "approved":
        if not asset_symbol:
            raise ValueError("sportsbook_odds_approved_mapping_requires_asset_symbol")
        if asset_symbol not in SPORTSBOOK_ODDS_ALLOWED_NARROW_ASSETS:
            raise ValueError(f"sportsbook_odds_asset_not_in_narrow_allowlist:{asset_symbol}")
        if not approved_by:
            raise ValueError("sportsbook_odds_approved_mapping_requires_approved_by")
        if not approved_ts_ms:
            raise ValueError("sportsbook_odds_approved_mapping_requires_approved_ts_ms")
        if not (approval_reason or mapping_rationale):
            raise ValueError("sportsbook_odds_approved_mapping_requires_rationale")
    normalized = {
        "mapping_key": str(item.get("mapping_key") or mapping_key_for(item)),
        "sport_key": sport_key,
        "league": league,
        "event_category": event_category,
        "market_type": market_type,
        "asset_symbol": asset_symbol,
        "research_label": research_label,
        "stage": stage,
        "enabled": _bool_value(item.get("enabled"), True),
        "allow_feature_use": allow_feature_use,
        "direct_trading_authority": False,
        "mapping_version": mapping_version,
        "mapping_rationale": mapping_rationale,
        "owner": owner,
        "approval_status": approval_status,
        "approved_by": approved_by,
        "approved_ts_ms": approved_ts_ms,
        "approval_reason": approval_reason,
        "approved_for_promotion": approved_for_promotion,
        "source_control_ref": str(item.get("source_control_ref") or item.get("change_ref") or "").strip(),
        "notes": str(item.get("notes") or ""),
        "created_ts_ms": safe_int(item.get("created_ts_ms"), observed) or observed,
        "updated_ts_ms": observed,
    }
    return normalized


def mappings_from_settings(settings: Mapping[str, Any] | None = None, *, now_ms: int | None = None) -> list[dict[str, Any]]:
    raw = (settings or {}).get("asset_mapping_json") or (settings or {}).get("mapping_json") or os.environ.get("SPORTSBOOK_ODDS_ASSET_MAPPING_JSON")
    parsed = _json_list(raw)
    if not parsed:
        return []
    return [normalize_sportsbook_mapping(item, now_ms=now_ms) for item in parsed if isinstance(item, Mapping)]


def _snapshot_row(record: Mapping[str, Any], now_ms: int) -> tuple[Any, ...]:
    raw = record.get("raw_payload", record)
    return (
        str(record.get("provider") or record.get("provider_name") or ""),
        str(record.get("sport_key") or ""),
        str(record.get("league") or ""),
        str(record.get("provider_event_id") or ""),
        str(record.get("provider_market_id") or ""),
        str(record.get("event_category") or ""),
        str(record.get("market_type") or ""),
        str(record.get("outcome_name") or ""),
        str(record.get("odds_format") or ""),
        safe_float(record.get("odds_value"), 0.0) if record.get("odds_value") is not None else None,
        _clip(safe_float(record.get("raw_implied_probability"), 0.0), 0.0, 1.0),
        _clip(safe_float(record.get("no_vig_probability"), 0.0), 0.0, 1.0),
        safe_float(record.get("line"), 0.0) if record.get("line") is not None else None,
        safe_float(record.get("spread"), 0.0) if record.get("spread") is not None else None,
        safe_float(record.get("total"), 0.0) if record.get("total") is not None else None,
        safe_int(record.get("event_start_ts_ms"), 0) or None,
        safe_int(record.get("source_ts_ms"), now_ms) or int(now_ms),
        safe_int(record.get("availability_ts_ms"), now_ms) or int(now_ms),
        safe_float(record.get("volume"), 0.0),
        safe_float(record.get("liquidity"), 0.0),
        str(record.get("settlement_status") or ""),
        safe_int(record.get("settlement_ts_ms"), 0) or None,
        safe_int(record.get("resolution_ts_ms"), 0) or None,
        str(record.get("raw_payload_hash") or raw_payload_hash(raw)),
        canonical_json(raw),
        int(now_ms),
    )


def put_sportsbook_odds_batch(
    con,
    *,
    odds: Sequence[Mapping[str, Any]] | None = None,
    mappings: Sequence[Mapping[str, Any]] | None = None,
    now_ms: int,
) -> dict[str, int]:
    """Persist normalized sportsbook odds rows and explicit mapping rows."""

    ensure_sportsbook_odds_schema(con)
    counts = {"odds": 0, "mappings": 0}
    for mapping in mappings or []:
        row = normalize_sportsbook_mapping(mapping, now_ms=int(now_ms))
        con.execute(
            """
            INSERT INTO sportsbook_odds_asset_mappings (
              mapping_key, sport_key, league, event_category, market_type,
              asset_symbol, research_label, stage, enabled, allow_feature_use,
              direct_trading_authority, mapping_version, mapping_rationale, owner,
              approval_status, approved_by, approved_ts_ms, approval_reason,
              approved_for_promotion, source_control_ref, notes, created_ts_ms,
              updated_ts_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(mapping_key) DO UPDATE SET
              sport_key=excluded.sport_key,
              league=excluded.league,
              event_category=excluded.event_category,
              market_type=excluded.market_type,
              asset_symbol=excluded.asset_symbol,
              research_label=excluded.research_label,
              stage=excluded.stage,
              enabled=excluded.enabled,
              allow_feature_use=excluded.allow_feature_use,
              direct_trading_authority=excluded.direct_trading_authority,
              mapping_version=excluded.mapping_version,
              mapping_rationale=excluded.mapping_rationale,
              owner=excluded.owner,
              approval_status=excluded.approval_status,
              approved_by=excluded.approved_by,
              approved_ts_ms=excluded.approved_ts_ms,
              approval_reason=excluded.approval_reason,
              approved_for_promotion=excluded.approved_for_promotion,
              source_control_ref=excluded.source_control_ref,
              notes=excluded.notes,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            (
                row["mapping_key"],
                row["sport_key"],
                row["league"],
                row["event_category"],
                row["market_type"],
                row["asset_symbol"],
                row["research_label"],
                row["stage"],
                bool(row["enabled"]),
                bool(row["allow_feature_use"]),
                False,
                row["mapping_version"],
                row["mapping_rationale"],
                row["owner"],
                row["approval_status"],
                row["approved_by"],
                row["approved_ts_ms"],
                row["approval_reason"],
                bool(row["approved_for_promotion"]),
                row["source_control_ref"],
                row["notes"],
                int(row["created_ts_ms"]),
                int(row["updated_ts_ms"]),
            ),
        )
        counts["mappings"] += 1

    for record in odds or []:
        con.execute(
            """
            INSERT INTO sportsbook_odds_snapshots (
              provider, sport_key, league, provider_event_id, provider_market_id,
              event_category, market_type, outcome_name, odds_format, odds_value,
              raw_implied_probability, no_vig_probability, line, spread, total,
              event_start_ts_ms, source_ts_ms, availability_ts_ms, volume, liquidity,
              settlement_status, settlement_ts_ms, resolution_ts_ms, raw_payload_hash,
              raw_json, created_ts_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(provider, provider_event_id, provider_market_id, market_type, outcome_name, availability_ts_ms, raw_payload_hash)
            DO NOTHING
            """,
            _snapshot_row(record, int(now_ms)),
        )
        counts["odds"] += 1
    return counts


def _row_dict(cursor, row: Any) -> dict[str, Any]:
    names = [str(desc[0]) for desc in cursor.description or []]
    return {names[idx]: row[idx] for idx in range(min(len(names), len(row)))}


def _zero_features() -> dict[str, float]:
    return {fid: 0.0 for fid in SPORTSBOOK_ODDS_FEATURE_IDS}


def _fetch_mapped_odds_rows(con, *, symbol: str, ts_ms: int, limit: int = 500) -> list[dict[str, Any]]:
    cursor = con.execute(
        """
        SELECT
          o.provider,
          o.sport_key,
          o.league,
          o.provider_event_id,
          o.provider_market_id,
          o.event_category,
          o.market_type,
          o.outcome_name,
          o.no_vig_probability,
          o.raw_implied_probability,
          o.source_ts_ms,
          o.availability_ts_ms,
          o.volume,
          o.liquidity,
          o.settlement_status,
          o.resolution_ts_ms,
          m.mapping_key,
          m.asset_symbol,
          m.research_label,
          m.stage,
          m.allow_feature_use,
          m.direct_trading_authority,
          m.mapping_version,
          m.approval_status,
          m.approved_for_promotion
        FROM sportsbook_odds_snapshots o
        JOIN sportsbook_odds_asset_mappings m
          ON lower(o.sport_key) = lower(m.sport_key)
         AND lower(o.league) = lower(m.league)
         AND lower(o.event_category) = lower(m.event_category)
         AND lower(o.market_type) = lower(m.market_type)
        WHERE upper(m.asset_symbol) = ?
          AND m.enabled = TRUE
          AND m.allow_feature_use = TRUE
          AND m.direct_trading_authority = FALSE
          AND o.availability_ts_ms <= ?
          AND (o.resolution_ts_ms IS NULL OR o.resolution_ts_ms > ?)
          AND COALESCE(o.settlement_status, '') = ''
        ORDER BY o.availability_ts_ms DESC, o.source_ts_ms DESC
        LIMIT ?
        """,
        (str(symbol).upper().strip(), int(ts_ms), int(ts_ms), int(limit)),
    )
    return [_row_dict(cursor, row) for row in cursor.fetchall() or []]


def resolve_sportsbook_odds_snapshot(con, *, symbol: str, ts_ms: int) -> tuple[dict[str, float], dict[str, Any], bool]:
    """Resolve PIT-safe, shadow-only odds features for an explicitly mapped asset."""

    features = _zero_features()
    source_meta: dict[str, Any] = {
        "latest_source_ts_ms": None,
        "latest_availability_ts_ms": None,
        "providers": [],
        "mapping_keys": [],
        "stage": "shadow",
        "research_only": True,
        "direct_trading_authority": False,
        "requires_explicit_mapping": True,
        "broad_market_default_allowed": False,
    }
    symbol_key = _clean_symbol(symbol)
    if not symbol_key:
        return features, source_meta, False
    if symbol_key not in SPORTSBOOK_ODDS_ALLOWED_NARROW_ASSETS:
        source_meta["unavailable_reason"] = "symbol_not_in_sports_odds_narrow_allowlist"
        return features, source_meta, False
    try:
        rows = _fetch_mapped_odds_rows(con, symbol=symbol_key, ts_ms=int(ts_ms))
    except Exception:
        source_meta["unavailable_reason"] = "sportsbook_odds_tables_unavailable"
        return features, source_meta, False
    if not rows:
        source_meta["unavailable_reason"] = "asset_mapping_missing_or_no_pit_odds"
        return features, source_meta, False

    latest_by_key: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    previous_by_key: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            str(row.get("provider") or ""),
            str(row.get("provider_event_id") or ""),
            str(row.get("provider_market_id") or ""),
            str(row.get("market_type") or ""),
            str(row.get("outcome_name") or ""),
        )
        if key not in latest_by_key:
            latest_by_key[key] = row
        elif key not in previous_by_key:
            previous_by_key[key] = row

    selected = list(latest_by_key.values())
    if not selected:
        return features, source_meta, False

    weighted_prob_sum = 0.0
    weighted_move_sum = 0.0
    weight_sum = 0.0
    attention_sum = 0.0
    latest_source = 0
    latest_availability = 0
    providers: set[str] = set()
    mapping_keys: set[str] = set()
    mapping_versions: set[str] = set()
    mapping_approval_statuses: set[str] = set()
    for key, row in latest_by_key.items():
        probability = _clip(safe_float(row.get("no_vig_probability"), 0.0), 0.0, 1.0)
        previous = previous_by_key.get(key)
        move = 0.0
        if previous is not None:
            move = probability - _clip(safe_float(previous.get("no_vig_probability"), probability), 0.0, 1.0)
        attention = max(0.0, safe_float(row.get("liquidity"), 0.0) + safe_float(row.get("volume"), 0.0))
        weight = max(1.0, math.log1p(attention))
        weighted_prob_sum += probability * weight
        weighted_move_sum += move * weight
        weight_sum += weight
        attention_sum += attention
        latest_source = max(latest_source, safe_int(row.get("source_ts_ms"), 0))
        latest_availability = max(latest_availability, safe_int(row.get("availability_ts_ms"), 0))
        if str(row.get("provider") or "").strip():
            providers.add(str(row.get("provider")))
        if str(row.get("mapping_key") or "").strip():
            mapping_keys.add(str(row.get("mapping_key")))
        if str(row.get("mapping_version") or "").strip():
            mapping_versions.add(str(row.get("mapping_version")))
        if str(row.get("approval_status") or "").strip():
            mapping_approval_statuses.add(str(row.get("approval_status")))

    features[f"{SPORTSBOOK_ODDS_FEATURE_PREFIX}no_vig_probability_level"] = _clip(
        weighted_prob_sum / max(1.0, weight_sum),
        0.0,
        1.0,
    )
    features[f"{SPORTSBOOK_ODDS_FEATURE_PREFIX}no_vig_probability_move"] = _clip(
        weighted_move_sum / max(1.0, weight_sum),
        -1.0,
        1.0,
    )
    features[f"{SPORTSBOOK_ODDS_FEATURE_PREFIX}liquidity_score"] = _clip(math.log1p(attention_sum) / 12.0, 0.0, 1.0)
    features[f"{SPORTSBOOK_ODDS_FEATURE_PREFIX}market_count"] = float(len(selected))
    features[f"{SPORTSBOOK_ODDS_FEATURE_PREFIX}provider_count"] = float(len(providers))
    features[f"{SPORTSBOOK_ODDS_FEATURE_PREFIX}available"] = 1.0
    source_meta.update(
        {
            "latest_source_ts_ms": int(latest_source) if latest_source > 0 else None,
            "latest_availability_ts_ms": int(latest_availability) if latest_availability > 0 else None,
            "providers": sorted(providers),
            "mapping_keys": sorted(mapping_keys),
            "mapping_versions": sorted(mapping_versions),
            "mapping_approval_statuses": sorted(mapping_approval_statuses),
            "market_count": int(len(selected)),
        }
    )
    return features, source_meta, True


def _model_config_records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, Mapping):
        for key in ("models", "configs", "model_configs"):
            nested = value.get(key)
            if isinstance(nested, list):
                return [dict(item) for item in nested if isinstance(item, Mapping)]
        return [dict(value)]
    if isinstance(value, list):
        return [dict(item) for item in value if isinstance(item, Mapping)]
    return []


def _model_config_name(record: Mapping[str, Any], index: int) -> str:
    name = str(record.get("instance_name") or record.get("model_name") or record.get("name") or record.get("id") or "")
    family = str(record.get("family") or "").strip()
    if name and family:
        return f"{family}:{name}"
    if name:
        return name
    if family:
        return f"{family}:config_{index}"
    return f"model_config_{index}"


def _model_config_symbols(record: Mapping[str, Any]) -> list[str]:
    symbols: list[str] = []
    for key in ("symbol_universe", "symbols", "asset_universe"):
        for symbol in parse_list(record.get(key)):
            cleaned = _clean_symbol(symbol)
            if cleaned and cleaned not in symbols:
                symbols.append(cleaned)
    return symbols


def _inventory_model_config_sportsbook_symbols(
    model_config_paths: Sequence[str | Path] | None,
) -> dict[str, Any]:
    paths = [Path(path) for path in (model_config_paths if model_config_paths is not None else _default_model_config_paths())]
    matched: dict[str, list[str]] = {bucket: [] for bucket in SPORTSBOOK_ODDS_NARROW_ASSET_BASKETS}
    matches: list[dict[str, Any]] = []
    missing_paths: list[str] = []
    unreadable_paths: list[dict[str, str]] = []
    wildcard_model_configs: list[str] = []
    inactive_model_configs: list[str] = []
    explicit_non_narrow_symbols: list[dict[str, Any]] = []
    record_count = 0

    for path in paths:
        if not path.exists():
            missing_paths.append(str(path))
            continue
        try:
            parsed = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            unreadable_paths.append({"path": str(path), "error": f"{type(exc).__name__}: {exc}"})
            continue
        for index, record in enumerate(_model_config_records(parsed)):
            record_count += 1
            name = _model_config_name(record, index)
            enabled = _bool_value(record.get("enabled"), True)
            prediction_enabled = _bool_value(record.get("prediction_enabled"), True)
            symbols = _model_config_symbols(record)
            if "*" in symbols:
                wildcard_model_configs.append(name)
            if not enabled or not prediction_enabled:
                inactive_model_configs.append(name)
                continue
            explicit_symbols = [symbol for symbol in symbols if symbol != "*"]
            non_narrow = [symbol for symbol in explicit_symbols if symbol not in SPORTSBOOK_ODDS_ALLOWED_NARROW_ASSETS]
            if non_narrow:
                explicit_non_narrow_symbols.append({"model_config": name, "symbols": non_narrow})
            narrow_symbols = [symbol for symbol in explicit_symbols if symbol in SPORTSBOOK_ODDS_ALLOWED_NARROW_ASSETS]
            if not narrow_symbols:
                continue
            for symbol in narrow_symbols:
                for bucket, values in SPORTSBOOK_ODDS_NARROW_ASSET_BASKETS.items():
                    if symbol in values and symbol not in matched[bucket]:
                        matched[bucket].append(symbol)
            matches.append(
                {
                    "model_config": name,
                    "symbols": sorted(set(narrow_symbols)),
                    "feature_groups": parse_list(record.get("feature_groups")),
                    "experimental": _bool_value(record.get("experimental"), False),
                }
            )

    matched_symbols = sorted({symbol for values in matched.values() for symbol in values})
    return {
        "ok": not unreadable_paths,
        "paths": [str(path) for path in paths],
        "missing_paths": missing_paths,
        "unreadable_paths": unreadable_paths,
        "record_count": int(record_count),
        "wildcard_model_configs": sorted(set(wildcard_model_configs)),
        "inactive_model_configs": sorted(set(inactive_model_configs)),
        "explicit_non_narrow_symbols": explicit_non_narrow_symbols,
        "matched": {key: sorted(value) for key, value in matched.items() if value},
        "matched_symbols": matched_symbols,
        "matches": matches,
    }


def inventory_sportsbook_relevant_universe(
    con,
    *,
    limit: int = 5000,
    model_config_paths: Sequence[str | Path] | None = None,
) -> dict[str, Any]:
    """Inventory active/watch symbols and explicit model config symbols eligible for odds research."""

    try:
        rows = con.execute(
            """
            SELECT symbol, status, asset_class, score, meta_json
            FROM symbols
            WHERE status IN ('ACTIVE', 'WATCH')
            ORDER BY score DESC, updated_ts_ms DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall() or []
        symbol_table_ok = True
        symbol_table_error = ""
    except Exception:
        rows = []
        symbol_table_ok = False
        symbol_table_error = "symbols_table_unavailable"
    matched: dict[str, list[str]] = {bucket: [] for bucket in SPORTSBOOK_ODDS_NARROW_ASSET_BASKETS}
    for row in rows:
        symbol = _clean_symbol(row[0] if row else "")
        for bucket, values in SPORTSBOOK_ODDS_NARROW_ASSET_BASKETS.items():
            if symbol in values and symbol not in matched[bucket]:
                matched[bucket].append(symbol)
    active_watch_matched = {key: sorted(value) for key, value in matched.items() if value}
    model_config_inventory = _inventory_model_config_sportsbook_symbols(model_config_paths)
    combined: dict[str, list[str]] = {bucket: list(symbols) for bucket, symbols in active_watch_matched.items()}
    for bucket, symbols in dict(model_config_inventory.get("matched") or {}).items():
        target = combined.setdefault(str(bucket), [])
        for symbol in symbols:
            cleaned = _clean_symbol(symbol)
            if cleaned and cleaned not in target:
                target.append(cleaned)
    matched_symbols = sorted({symbol for values in combined.values() for symbol in values})
    if matched_symbols:
        reason = ""
    elif not symbol_table_ok:
        reason = "symbols_table_unavailable_and_no_explicit_sportsbook_model_config_symbols"
    else:
        reason = (
            "no_sportsbook_media_gaming_data_provider_apparel_or_ad_sensitive_assets_"
            "in_active_watch_universe_or_explicit_model_configs"
        )
    return {
        "ok": bool(symbol_table_ok and model_config_inventory.get("ok", False)),
        "symbol_table_ok": bool(symbol_table_ok),
        "symbol_table_error": symbol_table_error,
        "no_go_for_production_features": not bool(matched_symbols),
        "reason": reason,
        "matched": {key: sorted(value) for key, value in combined.items() if value},
        "matched_symbols": matched_symbols,
        "active_watch_matched": active_watch_matched,
        "model_config_matched": model_config_inventory.get("matched", {}),
        "model_config_matched_symbols": model_config_inventory.get("matched_symbols", []),
        "model_config_inventory": model_config_inventory,
    }


def sportsbook_odds_provider_readiness(
    odds: Sequence[Mapping[str, Any]] | None = None,
    mappings: Sequence[Mapping[str, Any]] | None = None,
    *,
    now_ms: int | None = None,
    stale_threshold_ms: int | None = None,
    require_mapping: bool = False,
    require_approved_mapping: bool = False,
    require_liquidity: bool = False,
) -> dict[str, Any]:
    """Return fail-closed diagnostics for provider and mapping readiness."""

    observed = int(now_ms if now_ms is not None else utc_ms())
    rows = [dict(row or {}) for row in list(odds or []) if isinstance(row, Mapping)]
    mapping_rows = [dict(row or {}) for row in list(mappings or []) if isinstance(row, Mapping)]
    stale_threshold = (
        stale_threshold_ms
        if stale_threshold_ms is not None
        else safe_int(os.environ.get("SPORTSBOOK_ODDS_STALE_THRESHOLD_MS"), 30 * 60 * 1000)
    )
    market_groups: dict[tuple[str, str, str, str, str], list[float]] = {}
    stale_count = 0
    missing_liquidity_count = 0
    settlement_lookahead_count = 0
    for row in rows:
        source_ts = safe_int(row.get("availability_ts_ms") or row.get("source_ts_ms"), observed)
        if stale_threshold and int(stale_threshold) > 0 and source_ts < observed - int(stale_threshold):
            stale_count += 1
        if safe_float(row.get("liquidity"), 0.0) <= 0.0 and safe_float(row.get("volume"), 0.0) <= 0.0:
            missing_liquidity_count += 1
        if str(row.get("settlement_status") or "").strip():
            resolution_ts = safe_int(row.get("resolution_ts_ms") or row.get("settlement_ts_ms"), 0)
            availability_ts = safe_int(row.get("availability_ts_ms"), observed)
            if not resolution_ts or int(resolution_ts) > min(int(availability_ts), int(observed)):
                settlement_lookahead_count += 1
        key = (
            str(row.get("provider") or ""),
            str(row.get("provider_event_id") or ""),
            str(row.get("provider_market_id") or ""),
            str(row.get("market_type") or ""),
            str(safe_int(row.get("availability_ts_ms"), 0)),
        )
        market_groups.setdefault(key, []).append(_clip(safe_float(row.get("no_vig_probability"), 0.0), 0.0, 1.0))

    no_vig_market_fail_count = 0
    multi_outcome_market_count = 0
    for probabilities in market_groups.values():
        if len(probabilities) >= 2:
            multi_outcome_market_count += 1
        if len(probabilities) < 2 or abs(sum(probabilities) - 1.0) > 1e-6:
            no_vig_market_fail_count += 1

    eligible_mapping_count = 0
    approved_mapping_count = 0
    for mapping in mapping_rows:
        asset_symbol = _clean_symbol(mapping.get("asset_symbol") or mapping.get("symbol"))
        enabled = _bool_value(mapping.get("enabled"), True)
        allow_feature_use = _bool_value(mapping.get("allow_feature_use"), False)
        direct_trading_authority = _bool_value(mapping.get("direct_trading_authority"), False)
        if enabled and allow_feature_use and not direct_trading_authority and asset_symbol in SPORTSBOOK_ODDS_ALLOWED_NARROW_ASSETS:
            eligible_mapping_count += 1
        if (
            enabled
            and allow_feature_use
            and not direct_trading_authority
            and asset_symbol in SPORTSBOOK_ODDS_ALLOWED_NARROW_ASSETS
            and _clean_key(mapping.get("approval_status")) == "approved"
            and _bool_value(mapping.get("approved_for_promotion"), False)
            and str(mapping.get("approved_by") or "").strip()
            and safe_int(mapping.get("approved_ts_ms"), 0) > 0
        ):
            approved_mapping_count += 1

    blockers: list[str] = []
    if not rows:
        blockers.append("odds_rows_missing")
    if stale_count:
        blockers.append("stale_odds_rows")
    if settlement_lookahead_count:
        blockers.append("settlement_lookahead_rows")
    if no_vig_market_fail_count:
        blockers.append("no_vig_market_normalization_failed")
    if bool(require_liquidity) and missing_liquidity_count:
        blockers.append("liquidity_missing")
    if bool(require_mapping) and eligible_mapping_count <= 0:
        blockers.append("eligible_mapping_missing")
    if bool(require_approved_mapping) and approved_mapping_count <= 0:
        blockers.append("approved_mapping_missing")
    return {
        "passed": not blockers,
        "blockers": blockers,
        "row_count": int(len(rows)),
        "provider_count": int(len({str(row.get("provider") or "") for row in rows if str(row.get("provider") or "")})),
        "market_count": int(len(market_groups)),
        "multi_outcome_market_count": int(multi_outcome_market_count),
        "stale_count": int(stale_count),
        "missing_liquidity_count": int(missing_liquidity_count),
        "settlement_lookahead_count": int(settlement_lookahead_count),
        "no_vig_market_fail_count": int(no_vig_market_fail_count),
        "mapping_count": int(len(mapping_rows)),
        "eligible_mapping_count": int(eligible_mapping_count),
        "approved_mapping_count": int(approved_mapping_count),
        "direct_trading_authority": False,
        "research_only": True,
    }


def approved_sportsbook_odds_mappings(con, *, symbols: Sequence[str] | None = None) -> list[dict[str, Any]]:
    """Return explicit mappings that are approved for promotion review."""

    ensure_sportsbook_odds_schema(con)
    symbol_filter = sorted({_clean_symbol(symbol) for symbol in list(symbols or []) if _clean_symbol(symbol)})
    params: list[Any] = []
    where_symbol = ""
    if symbol_filter:
        where_symbol = "AND upper(asset_symbol) IN (" + ",".join("?" for _ in symbol_filter) + ")"
        params.extend(symbol_filter)
    cursor = con.execute(
        f"""
        SELECT *
        FROM sportsbook_odds_asset_mappings
        WHERE enabled = TRUE
          AND allow_feature_use = TRUE
          AND direct_trading_authority = FALSE
          AND approval_status = 'approved'
          AND approved_for_promotion = TRUE
          AND owner <> ''
          AND mapping_rationale <> ''
          AND mapping_version <> ''
          AND approved_by <> ''
          AND approved_ts_ms IS NOT NULL
          AND approval_reason <> ''
          {where_symbol}
        ORDER BY asset_symbol, sport_key, league, event_category, market_type, mapping_key
        """,
        tuple(params),
    )
    rows = [_row_dict(cursor, row) for row in cursor.fetchall() or []]
    approved: list[dict[str, Any]] = []
    for row in rows:
        if _clean_symbol(row.get("asset_symbol")) not in SPORTSBOOK_ODDS_ALLOWED_NARROW_ASSETS:
            continue
        if safe_int(row.get("approved_ts_ms"), 0) <= 0:
            continue
        approved.append(row)
    return approved


def _table_columns(con, table_name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall() or []
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


def _price_after(con, *, symbol: str, ts_ms: int) -> tuple[int, float] | None:
    columns = _table_columns(con, "prices")
    price_col = next((name for name in ("price", "px", "close", "c") if name in columns), None)
    ts_col = next((name for name in ("ts_ms", "time_ms") if name in columns), None)
    if not price_col or not ts_col or "symbol" not in columns:
        return None
    cursor = con.execute(
        f"""
        SELECT {ts_col}, {price_col}
        FROM prices
        WHERE symbol = ?
          AND {ts_col} >= ?
          AND {price_col} IS NOT NULL
        ORDER BY {ts_col} ASC
        LIMIT 1
        """,
        (_clean_symbol(symbol), int(ts_ms)),
    )
    row = cursor.fetchone()
    if not row:
        return None
    price = safe_float(row[1], 0.0)
    if price <= 0.0:
        return None
    return int(row[0]), float(price)


def _pearson(left: Sequence[float], right: Sequence[float]) -> float | None:
    n = min(len(left), len(right))
    if n < 2:
        return None
    x = [float(value) for value in left[:n]]
    y = [float(value) for value in right[:n]]
    mean_x = sum(x) / n
    mean_y = sum(y) / n
    dx = [value - mean_x for value in x]
    dy = [value - mean_y for value in y]
    denom = math.sqrt(sum(value * value for value in dx) * sum(value * value for value in dy))
    if denom <= 1e-12:
        return None
    return float(sum(a * b for a, b in zip(dx, dy)) / denom)


def _sportsbook_odds_event_samples(
    con,
    *,
    symbols: Sequence[str] | None,
    start_ts_ms: int,
    end_ts_ms: int,
    horizon_s: int,
    latency_ms: int,
    fee_bps: float,
    slippage_bps: float,
    approved_only: bool = False,
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    symbol_filter = sorted({_clean_symbol(symbol) for symbol in list(symbols or []) if _clean_symbol(symbol)})
    params: list[Any] = [int(start_ts_ms), int(end_ts_ms)]
    where_symbol = ""
    if symbol_filter:
        where_symbol = "AND upper(m.asset_symbol) IN (" + ",".join("?" for _ in symbol_filter) + ")"
        params.extend(symbol_filter)
    where_approval = ""
    if bool(approved_only):
        where_approval = """
          AND m.approval_status = 'approved'
          AND m.approved_for_promotion = TRUE
          AND COALESCE(m.approved_by, '') <> ''
          AND m.approved_ts_ms IS NOT NULL
        """
    cursor = con.execute(
        f"""
        SELECT
          o.provider,
          o.provider_event_id,
          o.provider_market_id,
          o.sport_key,
          o.league,
          o.event_category,
          o.market_type,
          o.outcome_name,
          o.no_vig_probability,
          o.availability_ts_ms,
          o.resolution_ts_ms,
          m.mapping_key,
          m.asset_symbol,
          m.mapping_version,
          m.approval_status,
          m.approved_for_promotion,
          m.approved_by,
          m.approved_ts_ms
        FROM sportsbook_odds_snapshots o
        JOIN sportsbook_odds_asset_mappings m
          ON lower(o.sport_key) = lower(m.sport_key)
         AND lower(o.league) = lower(m.league)
         AND lower(o.event_category) = lower(m.event_category)
         AND lower(o.market_type) = lower(m.market_type)
        WHERE o.availability_ts_ms >= ?
          AND o.availability_ts_ms <= ?
          AND m.enabled = TRUE
          AND m.allow_feature_use = TRUE
          AND m.direct_trading_authority = FALSE
          AND COALESCE(o.settlement_status, '') = ''
          AND (o.resolution_ts_ms IS NULL OR o.resolution_ts_ms > o.availability_ts_ms)
          {where_symbol}
          {where_approval}
        ORDER BY m.asset_symbol, m.mapping_key, o.provider, o.provider_event_id, o.provider_market_id,
                 o.market_type, o.outcome_name, o.availability_ts_ms ASC
        """,
        tuple(params),
    )
    rows = [_row_dict(cursor, row) for row in cursor.fetchall() or []]
    samples_by_mapping_symbol: dict[tuple[str, str], list[dict[str, Any]]] = {}
    previous: dict[tuple[str, str, str, str, str, str, str], dict[str, Any]] = {}
    cost_return = (float(fee_bps) + float(slippage_bps)) / 10_000.0
    for row in rows:
        symbol = _clean_symbol(row.get("asset_symbol"))
        mapping_key = str(row.get("mapping_key") or "")
        if symbol not in SPORTSBOOK_ODDS_ALLOWED_NARROW_ASSETS:
            continue
        key = (
            mapping_key,
            symbol,
            str(row.get("provider") or ""),
            str(row.get("provider_event_id") or ""),
            str(row.get("provider_market_id") or ""),
            str(row.get("market_type") or ""),
            str(row.get("outcome_name") or ""),
        )
        prev = previous.get(key)
        previous[key] = row
        if prev is None:
            continue
        odds_move = safe_float(row.get("no_vig_probability"), 0.0) - safe_float(prev.get("no_vig_probability"), 0.0)
        if odds_move == 0.0:
            continue
        availability_ts = safe_int(row.get("availability_ts_ms"), 0)
        entry_ts = availability_ts + int(latency_ms)
        exit_ts = entry_ts + int(horizon_s) * 1000
        entry = _price_after(con, symbol=symbol, ts_ms=entry_ts)
        exit_ = _price_after(con, symbol=symbol, ts_ms=exit_ts)
        if entry is None or exit_ is None:
            continue
        forward_return = (float(exit_[1]) / float(entry[1])) - 1.0
        signed_return = (1.0 if odds_move > 0 else -1.0) * forward_return
        net_return = signed_return - cost_return
        samples_by_mapping_symbol.setdefault((mapping_key, symbol), []).append(
            {
                "mapping_key": mapping_key,
                "mapping_version": str(row.get("mapping_version") or ""),
                "symbol": symbol,
                "provider": str(row.get("provider") or ""),
                "provider_event_id": str(row.get("provider_event_id") or ""),
                "provider_market_id": str(row.get("provider_market_id") or ""),
                "sport_key": str(row.get("sport_key") or ""),
                "league": str(row.get("league") or ""),
                "event_category": str(row.get("event_category") or ""),
                "market_type": str(row.get("market_type") or ""),
                "outcome_name": str(row.get("outcome_name") or ""),
                "availability_ts_ms": int(availability_ts),
                "entry_ts_ms": int(entry[0]),
                "exit_ts_ms": int(exit_[0]),
                "odds_move": float(odds_move),
                "forward_return": float(forward_return),
                "net_return": float(net_return),
                "hit": 1.0 if net_return > 0.0 else 0.0,
                "approval_status": str(row.get("approval_status") or ""),
                "approved_for_promotion": bool(row.get("approved_for_promotion")),
                "approved_by": str(row.get("approved_by") or ""),
                "approved_ts_ms": safe_int(row.get("approved_ts_ms"), 0),
            }
        )
    return samples_by_mapping_symbol


def _mean(values: Sequence[float]) -> float:
    series = [float(value) for value in list(values or []) if math.isfinite(float(value))]
    return float(sum(series) / max(1, len(series)))


def _one_sided_positive_mean_p_value(values: Sequence[float]) -> float:
    series = [float(value) for value in list(values or []) if math.isfinite(float(value))]
    n = len(series)
    if n < 2:
        return 1.0
    mean_value = sum(series) / n
    variance = sum((value - mean_value) ** 2 for value in series) / max(1, n - 1)
    standard_error = math.sqrt(max(0.0, variance)) / math.sqrt(max(1, n))
    if standard_error <= 1e-12:
        return 0.0 if mean_value > 0.0 else 1.0
    z_score = mean_value / standard_error
    p_value = 0.5 * math.erfc(float(z_score) / math.sqrt(2.0))
    return _clip(p_value, 0.0, 1.0)


def _bh_q_values(p_values: Sequence[float]) -> list[float]:
    indexed = sorted(
        [(idx, _clip(safe_float(value, 1.0), 0.0, 1.0)) for idx, value in enumerate(list(p_values or []))],
        key=lambda item: item[1],
    )
    m = len(indexed)
    out = [1.0] * m
    running = 1.0
    for rank, (idx, p_value) in reversed(list(enumerate(indexed, start=1))):
        running = min(running, p_value * m / max(1, rank))
        out[idx] = _clip(running, 0.0, 1.0)
    return out


def _benchmark_returns_for_samples(con, *, benchmark_symbol: str, samples: Sequence[Mapping[str, Any]]) -> list[float]:
    returns: list[float] = []
    for sample in samples or []:
        entry = _price_after(con, symbol=str(benchmark_symbol), ts_ms=safe_int(sample.get("entry_ts_ms"), 0))
        exit_ = _price_after(con, symbol=str(benchmark_symbol), ts_ms=safe_int(sample.get("exit_ts_ms"), 0))
        if entry is None or exit_ is None:
            continue
        returns.append(float(float(exit_[1]) / float(entry[1]) - 1.0))
    return returns


def _promotion_dataset_hash(samples: Sequence[Mapping[str, Any]], *, settings: Mapping[str, Any]) -> str:
    payload = {
        "feature_group": SPORTSBOOK_ODDS_FEATURE_GROUP,
        "feature_ids": SPORTSBOOK_ODDS_FEATURE_IDS,
        "settings": dict(settings or {}),
        "samples": [
            {
                "mapping_key": str(sample.get("mapping_key") or ""),
                "symbol": str(sample.get("symbol") or ""),
                "availability_ts_ms": safe_int(sample.get("availability_ts_ms"), 0),
                "entry_ts_ms": safe_int(sample.get("entry_ts_ms"), 0),
                "exit_ts_ms": safe_int(sample.get("exit_ts_ms"), 0),
                "odds_move": safe_float(sample.get("odds_move"), 0.0),
                "net_return": safe_float(sample.get("net_return"), 0.0),
            }
            for sample in samples or []
        ],
    }
    return raw_payload_hash(payload)


def _fetch_provider_readiness_rows(con, *, start_ts_ms: int, end_ts_ms: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    odds_cursor = con.execute(
        """
        SELECT *
        FROM sportsbook_odds_snapshots
        WHERE availability_ts_ms >= ?
          AND availability_ts_ms <= ?
        """,
        (int(start_ts_ms), int(end_ts_ms)),
    )
    mapping_cursor = con.execute("SELECT * FROM sportsbook_odds_asset_mappings")
    return (
        [_row_dict(odds_cursor, row) for row in odds_cursor.fetchall() or []],
        [_row_dict(mapping_cursor, row) for row in mapping_cursor.fetchall() or []],
    )


def run_sportsbook_odds_promotion_research(
    con,
    *,
    symbols: Sequence[str] | None = None,
    start_ts_ms: int,
    end_ts_ms: int,
    horizon_s: int = 86_400,
    latency_ms: int = 15 * 60 * 1000,
    fee_bps: float = 1.0,
    slippage_bps: float = 5.0,
    train_fraction: float = 0.60,
    min_oos_samples: int | None = None,
    min_oos_mean_net_return: float | None = None,
    min_oos_hit_rate: float | None = None,
    max_fdr_q: float | None = None,
    max_abs_benchmark_corr: float | None = None,
    benchmark_symbol: str = "SPY",
    require_liquidity: bool = False,
    persist: bool = True,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Persist strict GO-candidate evidence; remains NO-GO unless every gate passes."""

    ensure_sportsbook_odds_schema(con)
    study_run_id = str(run_id or f"sportsbook_odds_promotion_research:{int(utc_ms())}")
    min_samples = int(min_oos_samples if min_oos_samples is not None else SPORTSBOOK_ODDS_MIN_OOS_SAMPLES)
    min_mean_net = float(
        min_oos_mean_net_return
        if min_oos_mean_net_return is not None
        else SPORTSBOOK_ODDS_MIN_OOS_MEAN_NET_RETURN
    )
    min_hit_rate = float(min_oos_hit_rate if min_oos_hit_rate is not None else SPORTSBOOK_ODDS_MIN_OOS_HIT_RATE)
    max_q = float(max_fdr_q if max_fdr_q is not None else SPORTSBOOK_ODDS_MAX_FDR_Q)
    max_corr = float(
        max_abs_benchmark_corr
        if max_abs_benchmark_corr is not None
        else SPORTSBOOK_ODDS_MAX_ABS_BENCHMARK_CORR
    )
    odds_rows, mapping_rows = _fetch_provider_readiness_rows(con, start_ts_ms=int(start_ts_ms), end_ts_ms=int(end_ts_ms))
    provider_readiness = sportsbook_odds_provider_readiness(
        odds_rows,
        mapping_rows,
        now_ms=int(end_ts_ms),
        stale_threshold_ms=0,
        require_mapping=True,
        require_approved_mapping=True,
        require_liquidity=bool(require_liquidity),
    )
    samples_by_key = _sportsbook_odds_event_samples(
        con,
        symbols=symbols,
        start_ts_ms=int(start_ts_ms),
        end_ts_ms=int(end_ts_ms),
        horizon_s=int(horizon_s),
        latency_ms=int(latency_ms),
        fee_bps=float(fee_bps),
        slippage_bps=float(slippage_bps),
        approved_only=True,
    )
    preliminary: list[dict[str, Any]] = []
    p_values: list[float] = []
    for (mapping_key, symbol), samples in sorted(samples_by_key.items()):
        ordered = sorted(samples, key=lambda sample: safe_int(sample.get("entry_ts_ms"), 0))
        if not ordered:
            continue
        split_idx = max(1, min(len(ordered), int(math.floor(len(ordered) * _clip(float(train_fraction), 0.05, 0.95)))))
        if split_idx >= len(ordered):
            oos = []
        else:
            oos = ordered[split_idx:]
        net_returns = [safe_float(sample.get("net_return"), 0.0) for sample in ordered]
        oos_net_returns = [safe_float(sample.get("net_return"), 0.0) for sample in oos]
        hits = [safe_float(sample.get("hit"), 0.0) for sample in ordered]
        oos_hits = [safe_float(sample.get("hit"), 0.0) for sample in oos]
        benchmark_returns = _benchmark_returns_for_samples(con, benchmark_symbol=str(benchmark_symbol), samples=oos)
        benchmark_corr = _pearson(oos_net_returns, benchmark_returns) if benchmark_returns else None
        p_value = _one_sided_positive_mean_p_value(oos_net_returns)
        p_values.append(float(p_value))
        approval_passed = all(
            str(sample.get("approval_status") or "") == "approved"
            and bool(sample.get("approved_for_promotion"))
            and str(sample.get("approved_by") or "").strip()
            and safe_int(sample.get("approved_ts_ms"), 0) > 0
            for sample in ordered
        )
        pit_passed = all(
            safe_int(sample.get("availability_ts_ms"), 0) <= safe_int(sample.get("entry_ts_ms"), 0)
            for sample in ordered
        )
        deconfounded_passed = benchmark_corr is not None and abs(float(benchmark_corr)) <= float(max_corr)
        preliminary.append(
            {
                "mapping_key": str(mapping_key),
                "mapping_version": str(ordered[-1].get("mapping_version") or ""),
                "symbol": str(symbol),
                "samples": ordered,
                "oos_samples": oos,
                "sample_count": int(len(ordered)),
                "oos_sample_count": int(len(oos)),
                "mean_net_return": _mean(net_returns),
                "oos_mean_net_return": _mean(oos_net_returns),
                "hit_rate": _mean(hits),
                "oos_hit_rate": _mean(oos_hits),
                "p_value": float(p_value),
                "benchmark_corr": benchmark_corr,
                "benchmark_symbol": str(benchmark_symbol).upper().strip(),
                "approval_passed": bool(approval_passed),
                "pit_passed": bool(pit_passed),
                "deconfounded_passed": bool(deconfounded_passed),
                "provider_readiness_passed": bool(provider_readiness.get("passed")),
            }
        )

    q_values = _bh_q_values(p_values)
    evidence_rows: list[dict[str, Any]] = []
    for idx, summary in enumerate(preliminary):
        q_value = float(q_values[idx] if idx < len(q_values) else 1.0)
        no_go_reasons: list[str] = []
        oos_passed = int(summary["oos_sample_count"]) >= int(min_samples)
        net_after_cost_passed = float(summary["oos_mean_net_return"]) > float(min_mean_net)
        hit_rate_passed = float(summary["oos_hit_rate"]) >= float(min_hit_rate)
        fdr_passed = q_value <= float(max_q)
        production_readiness_passed = bool(provider_readiness.get("passed"))
        checks = {
            "approved_mapping_required": bool(summary["approval_passed"]),
            "provider_readiness_required": bool(summary["provider_readiness_passed"]),
            "oos_sample_count_required": bool(oos_passed),
            "net_after_cost_required": bool(net_after_cost_passed),
            "oos_hit_rate_required": bool(hit_rate_passed),
            "pit_required": bool(summary["pit_passed"]),
            "deconfounded_required": bool(summary["deconfounded_passed"]),
            "fdr_required": bool(fdr_passed),
            "production_readiness_required": bool(production_readiness_passed),
        }
        for reason, passed in checks.items():
            if not bool(passed):
                no_go_reasons.append(reason)
        dataset_hash = _promotion_dataset_hash(
            summary["samples"],
            settings={
                "start_ts_ms": int(start_ts_ms),
                "end_ts_ms": int(end_ts_ms),
                "horizon_s": int(horizon_s),
                "latency_ms": int(latency_ms),
                "fee_bps": float(fee_bps),
                "slippage_bps": float(slippage_bps),
                "train_fraction": float(train_fraction),
                "benchmark_symbol": str(benchmark_symbol).upper().strip(),
            },
        )
        train_end_ts_ms = safe_int(summary["samples"][min(len(summary["samples"]) - 1, max(0, len(summary["samples"]) - len(summary["oos_samples"]) - 1))].get("entry_ts_ms"), 0)
        test_start_ts_ms = safe_int(summary["oos_samples"][0].get("entry_ts_ms"), 0) if summary["oos_samples"] else None
        passed = not no_go_reasons
        evidence_key = raw_payload_hash(
            {
                "run_id": study_run_id,
                "mapping_key": summary["mapping_key"],
                "symbol": summary["symbol"],
                "mapping_version": summary["mapping_version"],
                "dataset_hash": dataset_hash,
                "horizon_s": int(horizon_s),
                "latency_ms": int(latency_ms),
            }
        )[:40]
        evidence = {
            "evidence_key": evidence_key,
            "run_id": study_run_id,
            "ts_ms": int(utc_ms()),
            "feature_group": SPORTSBOOK_ODDS_FEATURE_GROUP,
            "mapping_key": summary["mapping_key"],
            "symbol": summary["symbol"],
            "mapping_version": summary["mapping_version"],
            "dataset_hash": dataset_hash,
            "feature_ids": list(SPORTSBOOK_ODDS_FEATURE_IDS),
            "start_ts_ms": int(start_ts_ms),
            "end_ts_ms": int(end_ts_ms),
            "train_end_ts_ms": train_end_ts_ms,
            "test_start_ts_ms": test_start_ts_ms,
            "horizon_s": int(horizon_s),
            "latency_ms": int(latency_ms),
            "fee_bps": float(fee_bps),
            "slippage_bps": float(slippage_bps),
            "sample_count": int(summary["sample_count"]),
            "oos_sample_count": int(summary["oos_sample_count"]),
            "mean_net_return": float(summary["mean_net_return"]),
            "oos_mean_net_return": float(summary["oos_mean_net_return"]),
            "hit_rate": float(summary["hit_rate"]),
            "oos_hit_rate": float(summary["oos_hit_rate"]),
            "p_value": float(summary["p_value"]),
            "fdr_q": float(q_value),
            "benchmark_symbol": summary["benchmark_symbol"],
            "benchmark_corr": summary["benchmark_corr"],
            "provider_readiness_passed": bool(summary["provider_readiness_passed"]),
            "oos_passed": bool(oos_passed),
            "net_after_cost_passed": bool(net_after_cost_passed),
            "pit_passed": bool(summary["pit_passed"]),
            "deconfounded_passed": bool(summary["deconfounded_passed"]),
            "production_readiness_passed": bool(production_readiness_passed),
            "approval_passed": bool(summary["approval_passed"]),
            "passed": bool(passed),
            "no_go_reason": ",".join(no_go_reasons),
            "direct_trading_authority": False,
            "evidence": {
                "provider_readiness": dict(provider_readiness),
                "thresholds": {
                    "min_oos_samples": int(min_samples),
                    "min_oos_mean_net_return": float(min_mean_net),
                    "min_oos_hit_rate": float(min_hit_rate),
                    "max_fdr_q": float(max_q),
                    "max_abs_benchmark_corr": float(max_corr),
                },
                "no_go_reasons": list(no_go_reasons),
                "research_only": True,
                "direct_trading_authority": False,
            },
        }
        evidence_rows.append(evidence)
        if persist:
            con.execute(
                """
                INSERT OR REPLACE INTO sportsbook_odds_promotion_evidence (
                  evidence_key, run_id, ts_ms, feature_group, mapping_key, symbol,
                  mapping_version, dataset_hash, feature_ids_json, start_ts_ms,
                  end_ts_ms, train_end_ts_ms, test_start_ts_ms, horizon_s,
                  latency_ms, fee_bps, slippage_bps, sample_count,
                  oos_sample_count, mean_net_return, oos_mean_net_return,
                  hit_rate, oos_hit_rate, p_value, fdr_q, benchmark_symbol,
                  benchmark_corr, provider_readiness_passed, oos_passed,
                  net_after_cost_passed, pit_passed, deconfounded_passed,
                  production_readiness_passed, approval_passed, passed,
                  no_go_reason, direct_trading_authority, evidence_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    evidence["evidence_key"],
                    evidence["run_id"],
                    int(evidence["ts_ms"]),
                    evidence["feature_group"],
                    evidence["mapping_key"],
                    evidence["symbol"],
                    evidence["mapping_version"],
                    evidence["dataset_hash"],
                    canonical_json(evidence["feature_ids"]),
                    int(evidence["start_ts_ms"]),
                    int(evidence["end_ts_ms"]),
                    evidence["train_end_ts_ms"],
                    evidence["test_start_ts_ms"],
                    int(evidence["horizon_s"]),
                    int(evidence["latency_ms"]),
                    float(evidence["fee_bps"]),
                    float(evidence["slippage_bps"]),
                    int(evidence["sample_count"]),
                    int(evidence["oos_sample_count"]),
                    float(evidence["mean_net_return"]),
                    float(evidence["oos_mean_net_return"]),
                    float(evidence["hit_rate"]),
                    float(evidence["oos_hit_rate"]),
                    float(evidence["p_value"]),
                    float(evidence["fdr_q"]),
                    evidence["benchmark_symbol"],
                    evidence["benchmark_corr"],
                    bool(evidence["provider_readiness_passed"]),
                    bool(evidence["oos_passed"]),
                    bool(evidence["net_after_cost_passed"]),
                    bool(evidence["pit_passed"]),
                    bool(evidence["deconfounded_passed"]),
                    bool(evidence["production_readiness_passed"]),
                    bool(evidence["approval_passed"]),
                    bool(evidence["passed"]),
                    evidence["no_go_reason"],
                    False,
                    canonical_json(evidence["evidence"]),
                ),
            )

    no_go_reason = "no_approved_mapping_price_aligned_oos_samples" if not evidence_rows else "passing_evidence_missing"
    if any(bool(row.get("passed")) for row in evidence_rows):
        no_go_reason = ""
    return {
        "ok": True,
        "run_id": study_run_id,
        "feature_group": SPORTSBOOK_ODDS_FEATURE_GROUP,
        "research_only": True,
        "direct_trading_authority": False,
        "provider_readiness": dict(provider_readiness),
        "evidence": evidence_rows,
        "no_go_for_production_features": not any(bool(row.get("passed")) for row in evidence_rows),
        "no_go_reason": no_go_reason,
    }


def run_sportsbook_odds_event_study(
    con,
    *,
    symbols: Sequence[str] | None = None,
    start_ts_ms: int,
    end_ts_ms: int,
    horizon_s: int = 86_400,
    latency_ms: int = 15 * 60 * 1000,
    fee_bps: float = 1.0,
    slippage_bps: float = 5.0,
    persist: bool = True,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Measure whether mapped odds moves lead target assets after costs."""

    ensure_sportsbook_odds_schema(con)
    symbol_filter = sorted({_clean_symbol(symbol) for symbol in list(symbols or []) if _clean_symbol(symbol)})
    params: list[Any] = [int(start_ts_ms), int(end_ts_ms)]
    where_symbol = ""
    if symbol_filter:
        where_symbol = "AND upper(m.asset_symbol) IN (" + ",".join("?" for _ in symbol_filter) + ")"
        params.extend(symbol_filter)
    cursor = con.execute(
        f"""
        SELECT
          o.provider,
          o.provider_event_id,
          o.provider_market_id,
          o.sport_key,
          o.league,
          o.event_category,
          o.market_type,
          o.outcome_name,
          o.no_vig_probability,
          o.availability_ts_ms,
          m.mapping_key,
          m.asset_symbol
        FROM sportsbook_odds_snapshots o
        JOIN sportsbook_odds_asset_mappings m
          ON lower(o.sport_key) = lower(m.sport_key)
         AND lower(o.league) = lower(m.league)
         AND lower(o.event_category) = lower(m.event_category)
         AND lower(o.market_type) = lower(m.market_type)
        WHERE o.availability_ts_ms >= ?
          AND o.availability_ts_ms <= ?
          AND m.enabled = TRUE
          AND m.allow_feature_use = TRUE
          AND m.direct_trading_authority = FALSE
          AND COALESCE(o.settlement_status, '') = ''
          {where_symbol}
        ORDER BY m.asset_symbol, o.provider, o.provider_event_id, o.provider_market_id,
                 o.market_type, o.outcome_name, o.availability_ts_ms ASC
        """,
        tuple(params),
    )
    rows = [_row_dict(cursor, row) for row in cursor.fetchall() or []]
    samples_by_symbol: dict[str, list[dict[str, float]]] = {}
    previous: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    cost_return = (float(fee_bps) + float(slippage_bps)) / 10_000.0
    for row in rows:
        symbol = _clean_symbol(row.get("asset_symbol"))
        key = (
            symbol,
            str(row.get("provider") or ""),
            str(row.get("provider_event_id") or ""),
            str(row.get("provider_market_id") or ""),
            str(row.get("market_type") or ""),
            str(row.get("outcome_name") or ""),
        )
        prev = previous.get(key)
        previous[key] = row
        if prev is None:
            continue
        odds_move = safe_float(row.get("no_vig_probability"), 0.0) - safe_float(prev.get("no_vig_probability"), 0.0)
        if odds_move == 0.0:
            continue
        entry_ts = int(row.get("availability_ts_ms") or 0) + int(latency_ms)
        exit_ts = entry_ts + int(horizon_s) * 1000
        entry = _price_after(con, symbol=symbol, ts_ms=entry_ts)
        exit_ = _price_after(con, symbol=symbol, ts_ms=exit_ts)
        if entry is None or exit_ is None:
            continue
        forward_return = (float(exit_[1]) / float(entry[1])) - 1.0
        signed_return = (1.0 if odds_move > 0 else -1.0) * forward_return
        net_return = signed_return - cost_return
        samples_by_symbol.setdefault(symbol, []).append(
            {
                "odds_move": float(odds_move),
                "forward_return": float(forward_return),
                "net_return": float(net_return),
                "hit": 1.0 if net_return > 0.0 else 0.0,
            }
        )

    study_run_id = str(run_id or f"sportsbook_odds_event_study:{int(utc_ms())}")
    summaries: list[dict[str, Any]] = []
    for symbol, samples in sorted(samples_by_symbol.items()):
        odds_moves = [float(item["odds_move"]) for item in samples]
        forwards = [float(item["forward_return"]) for item in samples]
        nets = [float(item["net_return"]) for item in samples]
        summary = {
            "run_id": study_run_id,
            "symbol": symbol,
            "sample_count": int(len(samples)),
            "mean_odds_move": sum(odds_moves) / max(1, len(odds_moves)),
            "mean_forward_return": sum(forwards) / max(1, len(forwards)),
            "mean_net_return": sum(nets) / max(1, len(nets)),
            "hit_rate": sum(float(item["hit"]) for item in samples) / max(1, len(samples)),
            "corr": _pearson(odds_moves, forwards),
            "no_go_reason": "research_only_no_promotion_evidence",
        }
        summaries.append(summary)
        if persist:
            con.execute(
                """
                INSERT INTO sportsbook_odds_event_studies (
                  run_id, ts_ms, feature_group, symbol, horizon_s, latency_ms,
                  fee_bps, slippage_bps, sample_count, mean_odds_move,
                  mean_forward_return, mean_net_return, hit_rate, corr,
                  no_go_reason, direct_trading_authority, evidence_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    study_run_id,
                    int(utc_ms()),
                    SPORTSBOOK_ODDS_FEATURE_GROUP,
                    symbol,
                    int(horizon_s),
                    int(latency_ms),
                    float(fee_bps),
                    float(slippage_bps),
                    int(summary["sample_count"]),
                    float(summary["mean_odds_move"]),
                    float(summary["mean_forward_return"]),
                    float(summary["mean_net_return"]),
                    float(summary["hit_rate"]),
                    summary["corr"],
                    str(summary["no_go_reason"]),
                    False,
                    canonical_json({"cost_return": cost_return, "research_only": True}),
                ),
            )
    no_go = not summaries
    return {
        "ok": True,
        "run_id": study_run_id,
        "feature_group": SPORTSBOOK_ODDS_FEATURE_GROUP,
        "research_only": True,
        "direct_trading_authority": False,
        "no_go_for_production_features": True,
        "no_go_reason": "no_mapped_price_aligned_samples" if no_go else "promotion_requires_oos_net_after_cost_pit_deconfounded_readiness",
        "symbols": [row["symbol"] for row in summaries],
        "summaries": summaries,
    }


def latest_sportsbook_odds_promotion_evidence(
    con,
    *,
    symbols: Sequence[str] | None = None,
    mapping_keys: Sequence[str] | None = None,
    passed_only: bool = False,
) -> list[dict[str, Any]]:
    """Return latest persisted promotion evidence rows for sportsbook odds."""

    ensure_sportsbook_odds_schema(con)
    symbol_filter = sorted({_clean_symbol(symbol) for symbol in list(symbols or []) if _clean_symbol(symbol)})
    mapping_filter = sorted({str(key or "").strip() for key in list(mapping_keys or []) if str(key or "").strip()})
    clauses: list[str] = []
    params: list[Any] = []
    if symbol_filter:
        clauses.append("upper(symbol) IN (" + ",".join("?" for _ in symbol_filter) + ")")
        params.extend(symbol_filter)
    if mapping_filter:
        clauses.append("mapping_key IN (" + ",".join("?" for _ in mapping_filter) + ")")
        params.extend(mapping_filter)
    if bool(passed_only):
        clauses.append("passed = TRUE")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    cursor = con.execute(
        f"""
        SELECT *
        FROM sportsbook_odds_promotion_evidence
        {where}
        ORDER BY symbol, mapping_key, ts_ms DESC
        """,
        tuple(params),
    )
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for row in cursor.fetchall() or []:
        item = _row_dict(cursor, row)
        key = (str(item.get("symbol") or ""), str(item.get("mapping_key") or ""))
        if key not in latest:
            if isinstance(item.get("feature_ids_json"), str):
                item["feature_ids"] = _json_list(item.get("feature_ids_json"))
            if isinstance(item.get("evidence_json"), str):
                item["evidence"] = _json_obj(item.get("evidence_json"))
            latest[key] = item
    return list(latest.values())


def evaluate_sportsbook_odds_go_gate(
    con=None,
    *,
    feature_ids: Sequence[str] | None = None,
    symbols: Sequence[str] | None = None,
) -> tuple[bool, dict[str, Any]]:
    """Fail-closed promotion gate for sportsbook odds-derived features."""

    requested_ids = [fid for fid in _feature_id_list(feature_ids) if fid.startswith(SPORTSBOOK_ODDS_FEATURE_PREFIX)]
    if not requested_ids:
        return True, {
            "enabled": True,
            "applied": False,
            "feature_group": SPORTSBOOK_ODDS_FEATURE_GROUP,
            "passed": True,
            "status": "not_applicable",
        }

    owns = False
    if con is None:
        from engine.runtime.storage import connect

        con = connect(readonly=True)
        owns = True
    try:
        ensure_sportsbook_odds_schema(con)
        blockers: list[str] = []
        diagnostics: dict[str, Any] = {
            "enabled": True,
            "applied": True,
            "feature_group": SPORTSBOOK_ODDS_FEATURE_GROUP,
            "feature_ids": list(requested_ids),
            "direct_trading_authority": False,
            "research_only": True,
            "passed": False,
        }
        try:
            from engine.strategy.feature_registry import FEATURE_STAGE_SHADOW, default_feature_ids, feature_stage, list_groups

            defaults = list(default_feature_ids())
            if any(str(fid).startswith(SPORTSBOOK_ODDS_FEATURE_PREFIX) for fid in defaults):
                blockers.append("sportsbook_odds_in_default_feature_ids")
            metadata = dict(list_groups().get(SPORTSBOOK_ODDS_FEATURE_GROUP) or {})
            diagnostics["feature_group_metadata"] = metadata
            if bool(metadata.get("direct_trading_authority")):
                blockers.append("direct_trading_authority_enabled")
            if bool(metadata.get("broad_market_default_allowed")):
                blockers.append("broad_market_default_allowed")
            for fid in requested_ids:
                if feature_stage(fid) != FEATURE_STAGE_SHADOW:
                    blockers.append(f"feature_not_shadow:{fid}")
        except Exception as exc:
            blockers.append(f"feature_registry_check_failed:{type(exc).__name__}")

        symbol_filter = sorted({_clean_symbol(symbol) for symbol in list(symbols or []) if _clean_symbol(symbol)})
        if any(symbol not in SPORTSBOOK_ODDS_ALLOWED_NARROW_ASSETS for symbol in symbol_filter):
            blockers.append("symbol_not_in_narrow_sportsbook_allowlist")
        approved_mappings = approved_sportsbook_odds_mappings(con, symbols=symbol_filter)
        diagnostics["approved_mapping_count"] = int(len(approved_mappings))
        diagnostics["approved_mapping_keys"] = [str(row.get("mapping_key") or "") for row in approved_mappings]
        if not approved_mappings:
            blockers.append("approved_mapping_missing")
        if symbol_filter:
            approved_symbols = {_clean_symbol(row.get("asset_symbol")) for row in approved_mappings}
            missing = [symbol for symbol in symbol_filter if symbol not in approved_symbols]
            if missing:
                blockers.append("approved_mapping_missing_for_symbols:" + ",".join(missing))

        evidence = latest_sportsbook_odds_promotion_evidence(
            con,
            symbols=symbol_filter,
            mapping_keys=[str(row.get("mapping_key") or "") for row in approved_mappings],
            passed_only=False,
        )
        diagnostics["latest_evidence"] = evidence
        passing = [
            row
            for row in evidence
            if bool(row.get("passed"))
            and bool(row.get("provider_readiness_passed"))
            and bool(row.get("oos_passed"))
            and bool(row.get("net_after_cost_passed"))
            and bool(row.get("pit_passed"))
            and bool(row.get("deconfounded_passed"))
            and bool(row.get("production_readiness_passed"))
            and bool(row.get("approval_passed"))
            and not bool(row.get("direct_trading_authority"))
        ]
        diagnostics["passing_evidence_count"] = int(len(passing))
        if not passing:
            blockers.append("passing_promotion_evidence_missing")
        if symbol_filter:
            passing_symbols = {_clean_symbol(row.get("symbol")) for row in passing}
            missing_passing_symbols = [symbol for symbol in symbol_filter if symbol not in passing_symbols]
            if missing_passing_symbols:
                blockers.append("passing_evidence_missing_for_symbols:" + ",".join(missing_passing_symbols))

        diagnostics["blockers"] = list(dict.fromkeys(blockers))
        diagnostics["no_go_reasons"] = list(dict.fromkeys(blockers))
        diagnostics["passed"] = not diagnostics["blockers"]
        diagnostics["status"] = "passed" if bool(diagnostics["passed"]) else "no_go"
        diagnostics["no_go_for_production_features"] = not bool(diagnostics["passed"])
        return bool(diagnostics["passed"]), diagnostics
    finally:
        if owns:
            try:
                con.close()
            except Exception:
                pass


class OddsProvider(Protocol):
    provider_name: str

    def fetch_event_odds(
        self,
        *,
        settings: Mapping[str, Any],
        credentials: Mapping[str, Any],
        now_ms: int,
    ) -> list[dict[str, Any]]:
        """Fetch normalized read-only event odds rows."""


@dataclass(frozen=True)
class GenericJsonOddsProvider:
    """Read-only JSON provider for backfills or simple HTTP odds feeds."""

    provider_name: str = "sportsbook_odds"

    def fetch_event_odds(
        self,
        *,
        settings: Mapping[str, Any],
        credentials: Mapping[str, Any],
        now_ms: int,
    ) -> list[dict[str, Any]]:
        validate_sportsbook_odds_read_only_settings(settings, credentials)
        events: list[Any] = []
        file_path = str(settings.get("file_path") or settings.get("historical_file") or "").strip()
        if file_path:
            raw = Path(file_path).read_text(encoding="utf-8")
            parsed = json.loads(raw)
            events = parsed if isinstance(parsed, list) else list((parsed or {}).get("events") or [])
        else:
            base_url = str(settings.get("base_url") or settings.get("url") or "").rstrip("/")
            path = str(settings.get("events_path") or settings.get("path") or "").strip("/")
            if not base_url:
                return []
            headers = dict(_json_obj(settings.get("headers_json")))
            params = dict(_json_obj(settings.get("params_json")))
            api_key = str(credentials.get("api_key") or "").strip()
            if api_key:
                if str(settings.get("api_key_mode") or "query").strip().lower() == "header":
                    headers[str(settings.get("api_key_header") or "Authorization")] = f"Bearer {api_key}"
                else:
                    params[str(settings.get("api_key_param") or "apiKey")] = api_key
            response = requests.get(
                f"{base_url}/{path}" if path else base_url,
                params=params,
                headers=headers,
                timeout=float(settings.get("timeout_s") or os.environ.get("SPORTSBOOK_ODDS_TIMEOUT_S") or 10.0),
            )
            response.raise_for_status()
            payload = response.json()
            events = payload if isinstance(payload, list) else list((payload or {}).get("events") or (payload or {}).get("data") or [])
        rows: list[dict[str, Any]] = []
        for event in events:
            if not isinstance(event, Mapping):
                continue
            item = dict(event)
            item.setdefault("provider", self.provider_name)
            rows.extend(normalize_event_odds(item, now_ms=int(now_ms)))
        return rows


def fetch_sportsbook_odds_batch(
    *,
    settings: Mapping[str, Any] | None = None,
    credentials: Mapping[str, Any] | None = None,
    provider: OddsProvider | None = None,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Fetch read-only odds rows plus explicit mappings from settings."""

    observed = int(now_ms if now_ms is not None else utc_ms())
    source_settings = dict(settings or {})
    source_credentials = dict(credentials or {})
    validate_sportsbook_odds_read_only_settings(source_settings, source_credentials)
    provider_name = str(source_settings.get("provider") or source_settings.get("provider_name") or "sportsbook_odds").strip().lower()
    adapter = provider or GenericJsonOddsProvider(provider_name=provider_name)
    odds = adapter.fetch_event_odds(settings=source_settings, credentials=source_credentials, now_ms=observed)
    mappings = mappings_from_settings(source_settings, now_ms=observed)
    readiness = sportsbook_odds_provider_readiness(
        odds,
        mappings,
        now_ms=observed,
        stale_threshold_ms=safe_int(source_settings.get("stale_threshold_ms"), 30 * 60 * 1000),
        require_mapping=False,
        require_approved_mapping=False,
        require_liquidity=_bool_value(source_settings.get("require_liquidity"), False),
    )
    return {
        "odds": odds,
        "mappings": mappings,
        "provider_state": {
            "provider": provider_name,
            "read_only": True,
            "research_only": True,
            "direct_trading_authority": False,
            "odds_rows": int(len(odds)),
            "mappings": int(len(mappings)),
            "readiness": readiness,
            "readiness_passed": bool(readiness.get("passed")),
        },
    }
