"""PIT-safe feature resolver for macro prediction-market expectations."""

from __future__ import annotations

import json
import math
from typing import Any, Mapping

from engine.data.prediction_market_providers import (
    DEFAULT_MACRO_ASSETS,
    PREDICTION_MARKET_EVENT_FEATURE_IDS,
    PREDICTION_MARKET_EVENT_PREFIX,
    PREDICTION_MARKET_MACRO_FEATURE_IDS,
    PREDICTION_MARKET_MACRO_PREFIX,
)
from engine.data.prediction_market_storage import safe_float, safe_int


def _zero_features() -> dict[str, float]:
    return {fid: 0.0 for fid in PREDICTION_MARKET_MACRO_FEATURE_IDS}


def _zero_event_features() -> dict[str, float]:
    return {fid: 0.0 for fid in PREDICTION_MARKET_EVENT_FEATURE_IDS}


def _clip(value: float, lo: float, hi: float) -> float:
    return float(max(float(lo), min(float(hi), float(value))))


def _json_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).upper().strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(item).upper().strip() for item in parsed if str(item).strip()]
    except Exception:
        pass
    return []


def _symbol_allowed(row: Mapping[str, Any], symbol: str) -> bool:
    assets = _json_list(row.get("affected_assets_json"))
    if not assets:
        assets = list(DEFAULT_MACRO_ASSETS)
    return str(symbol).upper().strip() in set(assets)


def _row_dict(cursor, row: Any) -> dict[str, Any]:
    names = [str(desc[0]) for desc in cursor.description or []]
    return {names[idx]: row[idx] for idx in range(min(len(names), len(row)))}


def _fetch_candidate_markets(con, *, symbol: str, ts_ms: int) -> list[dict[str, Any]]:
    cursor = con.execute(
        """
        SELECT
          provider_name,
          provider_market_id,
          provider_event_id,
          probability,
          previous_probability,
          probability_delta,
          liquidity,
          volume,
          volume_24h,
          open_interest,
          spread,
          event_ts_ms,
          resolution_ts_ms,
          source_ts_ms,
          availability_ts_ms,
          affected_assets_json
        FROM prediction_market_markets
        WHERE provider_category = ?
          AND availability_ts_ms <= ?
          AND (resolution_ts_ms IS NULL OR resolution_ts_ms > ?)
        ORDER BY availability_ts_ms DESC, source_ts_ms DESC
        LIMIT 200
        """,
        ("macro", int(ts_ms), int(ts_ms)),
    )
    rows = [_row_dict(cursor, row) for row in cursor.fetchall() or []]
    return [row for row in rows if _symbol_allowed(row, symbol)]


def _fetch_candidate_event_markets(con, *, symbol: str, ts_ms: int) -> list[dict[str, Any]]:
    cursor = con.execute(
        """
        SELECT
          provider_name,
          provider_market_id,
          provider_event_id,
          title,
          event_type,
          status,
          provider_contract_id,
          product_id,
          official_resolution_source,
          source_file_date,
          source_file_kind,
          refresh_cadence,
          provider_timestamp_ms,
          probability,
          previous_probability,
          probability_delta,
          liquidity,
          volume,
          volume_24h,
          open_interest,
          spread,
          event_ts_ms,
          resolution_ts_ms,
          source_ts_ms,
          availability_ts_ms,
          affected_assets_json,
          semantic_event_id,
          resolution_semantics
        FROM prediction_market_markets
        WHERE availability_ts_ms <= ?
          AND (resolution_ts_ms IS NULL OR resolution_ts_ms > ?)
          AND (
            provider_name = 'polymarket'
            OR provider_name = 'forecastex'
            OR provider_name = 'ibkr_event_contracts'
            OR COALESCE(semantic_event_id, '') <> ''
          )
        ORDER BY availability_ts_ms DESC, source_ts_ms DESC
        LIMIT 500
        """,
        (int(ts_ms), int(ts_ms)),
    )
    rows = [_row_dict(cursor, row) for row in cursor.fetchall() or []]
    return [row for row in rows if _symbol_allowed(row, symbol)]


def _fetch_latest_orderbook(con, *, provider_name: str, market_id: str, ts_ms: int) -> dict[str, Any] | None:
    cursor = con.execute(
        """
        SELECT
          provider_name,
          provider_market_id,
          source_ts_ms,
          availability_ts_ms,
          mid_probability,
          spread,
          liquidity,
          imbalance
        FROM prediction_market_orderbook_snapshots
        WHERE provider_name = ?
          AND provider_market_id = ?
          AND availability_ts_ms <= ?
        ORDER BY availability_ts_ms DESC, source_ts_ms DESC
        LIMIT 1
        """,
        (str(provider_name), str(market_id), int(ts_ms)),
    )
    row = cursor.fetchone()
    return _row_dict(cursor, row) if row else None


def _is_live_status(status: Any) -> bool:
    text = str(status or "").strip().lower()
    if not text:
        return True
    if any(token in text for token in ("closed", "halt", "paused", "resolved", "settled", "suspended", "cancel")):
        return False
    return text in {"active", "open", "live", "trading", "tradable"}


def _is_regulated_event_provider(provider_name: Any) -> bool:
    return str(provider_name or "").strip().lower() in {"forecastex", "ibkr_event_contracts"}


def _regulated_event_feature_key(event_type: Any) -> str | None:
    text = str(event_type or "").strip().lower().replace("-", "_")
    aliases = {
        "macro": "regulated_macro_probability",
        "economic": "regulated_macro_probability",
        "economic_indicators": "regulated_macro_probability",
        "energy": "regulated_energy_probability",
        "climate": "regulated_climate_weather_probability",
        "weather": "regulated_climate_weather_probability",
        "climate_weather": "regulated_climate_weather_probability",
        "fx": "regulated_fx_rates_probability",
        "rates": "regulated_fx_rates_probability",
        "fx_rates": "regulated_fx_rates_probability",
        "interest_rates": "regulated_fx_rates_probability",
        "equity": "regulated_equity_index_probability",
        "equity_index": "regulated_equity_index_probability",
        "equity_indexes": "regulated_equity_index_probability",
        "commodity": "regulated_commodity_probability",
        "commodities": "regulated_commodity_probability",
    }
    suffix = aliases.get(text)
    return f"{PREDICTION_MARKET_EVENT_PREFIX}{suffix}" if suffix else None


def _market_attention(row: Mapping[str, Any]) -> float:
    return max(
        0.0,
        safe_float(row.get("liquidity"), 0.0)
        + safe_float(row.get("volume"), 0.0)
        + safe_float(row.get("volume_24h"), 0.0)
        + safe_float(row.get("open_interest"), 0.0),
    )


def resolve_prediction_market_macro_snapshot(con, *, symbol: str, ts_ms: int) -> tuple[dict[str, float], dict[str, Any], bool]:
    """Resolve shadow-only macro expectation features as of ``ts_ms``."""

    features = _zero_features()
    source_meta: dict[str, Any] = {
        "latest_source_ts_ms": None,
        "latest_availability_ts_ms": None,
        "providers": [],
        "direct_trading_authority": False,
        "stage": "shadow",
    }
    symbol_key = str(symbol or "").upper().strip()
    if not symbol_key:
        return features, source_meta, False
    try:
        rows = _fetch_candidate_markets(con, symbol=symbol_key, ts_ms=int(ts_ms))
    except Exception:
        return features, source_meta, False
    if not rows:
        return features, source_meta, False

    latest_by_provider: dict[str, dict[str, Any]] = {}
    for row in rows:
        provider = str(row.get("provider_name") or "")
        if provider and provider not in latest_by_provider:
            latest_by_provider[provider] = row
    selected = list(latest_by_provider.values())
    if not selected:
        return features, source_meta, False

    weighted_prob_sum = 0.0
    weighted_delta_sum = 0.0
    weight_sum = 0.0
    urgency = 0.0
    latest_source = 0
    latest_availability = 0
    provider_probs: dict[str, float] = {}
    best_spread: float | None = None
    best_imbalance = 0.0
    best_book_liquidity = 0.0

    for row in selected:
        provider = str(row.get("provider_name") or "")
        probability = safe_float(row.get("probability"), 0.0)
        delta = safe_float(row.get("probability_delta"), 0.0)
        liquidity = max(
            0.0,
            safe_float(row.get("liquidity"), 0.0)
            + safe_float(row.get("volume"), 0.0)
            + safe_float(row.get("volume_24h"), 0.0)
            + safe_float(row.get("open_interest"), 0.0),
        )
        weight = max(1.0, math.log1p(liquidity))
        weighted_prob_sum += probability * weight
        weighted_delta_sum += delta * weight
        weight_sum += weight
        provider_probs[provider] = probability
        source_ts = safe_int(row.get("source_ts_ms"), 0)
        availability_ts = safe_int(row.get("availability_ts_ms"), 0)
        latest_source = max(latest_source, source_ts)
        latest_availability = max(latest_availability, availability_ts)

        event_ts = safe_int(row.get("event_ts_ms"), 0)
        if event_ts > int(ts_ms):
            days = max(0.0, (float(event_ts) - float(ts_ms)) / float(24 * 60 * 60 * 1000))
            urgency = max(urgency, 1.0 / (1.0 + (days / 30.0)))
        spread = row.get("spread")
        if spread is not None:
            parsed_spread = max(0.0, safe_float(spread, 0.0))
            best_spread = parsed_spread if best_spread is None else min(best_spread, parsed_spread)

        if provider == "kalshi":
            orderbook = _fetch_latest_orderbook(
                con,
                provider_name=provider,
                market_id=str(row.get("provider_market_id") or ""),
                ts_ms=int(ts_ms),
            )
            if orderbook:
                book_liquidity = max(0.0, safe_float(orderbook.get("liquidity"), 0.0))
                latest_source = max(latest_source, safe_int(orderbook.get("source_ts_ms"), 0))
                latest_availability = max(latest_availability, safe_int(orderbook.get("availability_ts_ms"), 0))
                book_spread = orderbook.get("spread")
                if book_spread is not None:
                    parsed_book_spread = max(0.0, safe_float(book_spread, 0.0))
                    best_spread = parsed_book_spread if best_spread is None else min(best_spread, parsed_book_spread)
                if book_liquidity >= best_book_liquidity:
                    best_book_liquidity = book_liquidity
                    best_imbalance = safe_float(orderbook.get("imbalance"), 0.0)

    probability_level = weighted_prob_sum / max(1.0, weight_sum)
    probability_delta = weighted_delta_sum / max(1.0, weight_sum)
    spread_quality = 0.0 if best_spread is None else _clip(1.0 - (float(best_spread) / 0.25), 0.0, 1.0)
    liquidity_scale = _clip(math.log1p(max(weight_sum, best_book_liquidity)) / 10.0, 0.0, 1.0)

    features[f"{PREDICTION_MARKET_MACRO_PREFIX}probability_level"] = _clip(probability_level, 0.0, 1.0)
    features[f"{PREDICTION_MARKET_MACRO_PREFIX}probability_delta"] = _clip(probability_delta, -1.0, 1.0)
    features[f"{PREDICTION_MARKET_MACRO_PREFIX}event_urgency"] = _clip(urgency, 0.0, 1.0)
    features[f"{PREDICTION_MARKET_MACRO_PREFIX}liquidity_adjusted_probability_move"] = _clip(
        probability_delta * liquidity_scale,
        -1.0,
        1.0,
    )
    features[f"{PREDICTION_MARKET_MACRO_PREFIX}orderbook_imbalance"] = _clip(best_imbalance, -1.0, 1.0)
    features[f"{PREDICTION_MARKET_MACRO_PREFIX}spread_quality"] = float(spread_quality)
    if "cme_fedwatch" in provider_probs and "kalshi" in provider_probs:
        features[f"{PREDICTION_MARKET_MACRO_PREFIX}cme_vs_kalshi_disagreement"] = abs(
            provider_probs["cme_fedwatch"] - provider_probs["kalshi"]
        )
    features[f"{PREDICTION_MARKET_MACRO_PREFIX}kalshi_available"] = 1.0 if "kalshi" in provider_probs else 0.0
    features[f"{PREDICTION_MARKET_MACRO_PREFIX}cme_available"] = 1.0 if "cme_fedwatch" in provider_probs else 0.0
    features[f"{PREDICTION_MARKET_MACRO_PREFIX}available"] = 1.0

    source_meta.update(
        {
            "latest_source_ts_ms": int(latest_source) if latest_source > 0 else None,
            "latest_availability_ts_ms": int(latest_availability) if latest_availability > 0 else None,
            "providers": sorted(provider_probs),
            "kalshi_available": bool("kalshi" in provider_probs),
            "cme_available": bool("cme_fedwatch" in provider_probs),
            "market_count": int(len(selected)),
        }
    )
    return features, source_meta, True


def resolve_prediction_market_event_snapshot(con, *, symbol: str, ts_ms: int) -> tuple[dict[str, float], dict[str, Any], bool]:
    """Resolve shadow-only Polymarket event expectation features as of ``ts_ms``."""

    features = _zero_event_features()
    source_meta: dict[str, Any] = {
        "latest_source_ts_ms": None,
        "latest_availability_ts_ms": None,
        "providers": [],
        "semantic_event_ids": [],
        "unavailable_reason_counts": {},
        "direct_trading_authority": False,
        "stage": "shadow",
    }
    symbol_key = str(symbol or "").upper().strip()
    if not symbol_key:
        return features, source_meta, False
    try:
        rows = _fetch_candidate_event_markets(con, symbol=symbol_key, ts_ms=int(ts_ms))
    except Exception:
        return features, source_meta, False
    if not rows:
        return features, source_meta, False

    eligible: list[dict[str, Any]] = []
    unavailable_reason_counts: dict[str, int] = {}

    def _note_unavailable(reason: str) -> None:
        unavailable_reason_counts[str(reason)] = int(unavailable_reason_counts.get(str(reason), 0)) + 1

    for row in rows:
        if not _is_live_status(row.get("status")):
            _note_unavailable("inactive_status")
            continue
        if safe_float(row.get("probability"), -1.0) < 0.0:
            _note_unavailable("probability_missing")
            continue
        if _market_attention(row) <= 0.0:
            _note_unavailable("sparse_or_zero_liquidity")
            continue
        if not _json_list(row.get("affected_assets_json")):
            _note_unavailable("asset_mapping_missing")
            continue
        eligible.append(row)
    polymarket_rows = [row for row in eligible if str(row.get("provider_name") or "") == "polymarket"]
    regulated_rows = [row for row in eligible if _is_regulated_event_provider(row.get("provider_name"))]
    if not polymarket_rows and not regulated_rows:
        source_meta["unavailable_reason_counts"] = dict(unavailable_reason_counts)
        return features, source_meta, False

    weighted_prob_sum = 0.0
    weighted_delta_sum = 0.0
    weight_sum = 0.0
    crypto_reg_sum = 0.0
    crypto_reg_weight = 0.0
    urgency = 0.0
    attention_sum = 0.0
    latest_source = 0
    latest_availability = 0
    best_spread: float | None = None
    best_imbalance = 0.0
    best_book_liquidity = 0.0
    regulated_by_feature: dict[str, dict[str, float]] = {}

    for row in list(polymarket_rows) + list(regulated_rows):
        probability = _clip(safe_float(row.get("probability"), 0.0), 0.0, 1.0)
        delta = _clip(safe_float(row.get("probability_delta"), 0.0), -1.0, 1.0)
        attention = _market_attention(row)
        weight = max(1.0, math.log1p(attention))
        weighted_prob_sum += probability * weight
        weighted_delta_sum += delta * weight
        weight_sum += weight
        attention_sum += attention
        if str(row.get("event_type") or "").strip().lower() == "crypto_regulation":
            crypto_reg_sum += probability * weight
            crypto_reg_weight += weight
        if _is_regulated_event_provider(row.get("provider_name")):
            feature_key = _regulated_event_feature_key(row.get("event_type"))
            if feature_key:
                bucket = regulated_by_feature.setdefault(feature_key, {"sum": 0.0, "weight": 0.0})
                bucket["sum"] += probability * weight
                bucket["weight"] += weight

        source_ts = safe_int(row.get("source_ts_ms"), 0)
        availability_ts = safe_int(row.get("availability_ts_ms"), 0)
        latest_source = max(latest_source, source_ts)
        latest_availability = max(latest_availability, availability_ts)

        event_ts = safe_int(row.get("event_ts_ms"), 0)
        if event_ts > int(ts_ms):
            days = max(0.0, (float(event_ts) - float(ts_ms)) / float(24 * 60 * 60 * 1000))
            urgency = max(urgency, 1.0 / (1.0 + (days / 30.0)))
        spread = row.get("spread")
        if spread is not None:
            parsed_spread = max(0.0, safe_float(spread, 0.0))
            best_spread = parsed_spread if best_spread is None else min(best_spread, parsed_spread)

        orderbook = _fetch_latest_orderbook(
            con,
            provider_name=str(row.get("provider_name") or ""),
            market_id=str(row.get("provider_market_id") or ""),
            ts_ms=int(ts_ms),
        )
        if orderbook:
            book_liquidity = max(0.0, safe_float(orderbook.get("liquidity"), 0.0))
            latest_source = max(latest_source, safe_int(orderbook.get("source_ts_ms"), 0))
            latest_availability = max(latest_availability, safe_int(orderbook.get("availability_ts_ms"), 0))
            book_spread = orderbook.get("spread")
            if book_spread is not None:
                parsed_book_spread = max(0.0, safe_float(book_spread, 0.0))
                best_spread = parsed_book_spread if best_spread is None else min(best_spread, parsed_book_spread)
            if book_liquidity >= best_book_liquidity:
                best_book_liquidity = book_liquidity
                best_imbalance = safe_float(orderbook.get("imbalance"), 0.0)

    probability_momentum = weighted_delta_sum / max(1.0, weight_sum)
    crypto_reg_probability = crypto_reg_sum / max(1.0, crypto_reg_weight) if crypto_reg_weight > 0.0 else 0.0
    spread_quality = 0.0 if best_spread is None else _clip(1.0 - (float(best_spread) / 0.25), 0.0, 1.0)
    attention_score = _clip(math.log1p(attention_sum) / 12.0, 0.0, 1.0)
    liquidity_scale = _clip(math.log1p(max(attention_sum, best_book_liquidity)) / 12.0, 0.0, 1.0)

    comparable: dict[tuple[str, str], dict[str, list[float]]] = {}
    for row in eligible:
        semantic_event_id = str(row.get("semantic_event_id") or "").strip()
        resolution_semantics = str(row.get("resolution_semantics") or "").strip()
        provider = str(row.get("provider_name") or "").strip()
        if not semantic_event_id or not resolution_semantics or not provider:
            continue
        key = (semantic_event_id, resolution_semantics)
        comparable.setdefault(key, {}).setdefault(provider, []).append(_clip(safe_float(row.get("probability"), 0.0), 0.0, 1.0))

    dispersion = 0.0
    mapped_event_ids: set[str] = set()
    for (semantic_event_id, _semantics), provider_probs in comparable.items():
        if "polymarket" not in provider_probs or len(provider_probs) < 2:
            continue
        averages = {provider: sum(values) / max(1, len(values)) for provider, values in provider_probs.items()}
        if len(averages) < 2:
            continue
        mapped_event_ids.add(semantic_event_id)
        dispersion = max(dispersion, max(averages.values()) - min(averages.values()))

    providers = sorted({str(row.get("provider_name") or "") for row in eligible if str(row.get("provider_name") or "")})
    features[f"{PREDICTION_MARKET_EVENT_PREFIX}crypto_regulation_probability"] = _clip(crypto_reg_probability, 0.0, 1.0)
    for feature_key, bucket in regulated_by_feature.items():
        features[str(feature_key)] = _clip(
            safe_float(bucket.get("sum"), 0.0) / max(1.0, safe_float(bucket.get("weight"), 0.0)),
            0.0,
            1.0,
        )
    features[f"{PREDICTION_MARKET_EVENT_PREFIX}probability_momentum"] = _clip(probability_momentum, -1.0, 1.0)
    features[f"{PREDICTION_MARKET_EVENT_PREFIX}liquidity_adjusted_event_shock"] = _clip(
        probability_momentum * liquidity_scale,
        -1.0,
        1.0,
    )
    features[f"{PREDICTION_MARKET_EVENT_PREFIX}orderbook_imbalance"] = _clip(best_imbalance, -1.0, 1.0)
    features[f"{PREDICTION_MARKET_EVENT_PREFIX}spread_quality"] = float(spread_quality)
    features[f"{PREDICTION_MARKET_EVENT_PREFIX}event_urgency"] = _clip(urgency, 0.0, 1.0)
    features[f"{PREDICTION_MARKET_EVENT_PREFIX}market_attention"] = float(attention_score)
    features[f"{PREDICTION_MARKET_EVENT_PREFIX}cross_provider_dispersion"] = _clip(dispersion, 0.0, 1.0)
    features[f"{PREDICTION_MARKET_EVENT_PREFIX}polymarket_available"] = 1.0 if polymarket_rows else 0.0
    features[f"{PREDICTION_MARKET_EVENT_PREFIX}forecastex_available"] = 1.0 if any(str(row.get("provider_name") or "") == "forecastex" for row in regulated_rows) else 0.0
    features[f"{PREDICTION_MARKET_EVENT_PREFIX}ibkr_event_contract_available"] = 1.0 if any(str(row.get("provider_name") or "") == "ibkr_event_contracts" for row in regulated_rows) else 0.0
    features[f"{PREDICTION_MARKET_EVENT_PREFIX}available"] = 1.0

    source_meta.update(
        {
            "latest_source_ts_ms": int(latest_source) if latest_source > 0 else None,
            "latest_availability_ts_ms": int(latest_availability) if latest_availability > 0 else None,
            "providers": providers,
            "semantic_event_ids": sorted(mapped_event_ids),
            "polymarket_available": bool(polymarket_rows),
            "forecastex_available": bool(any(str(row.get("provider_name") or "") == "forecastex" for row in regulated_rows)),
            "ibkr_event_contract_available": bool(any(str(row.get("provider_name") or "") == "ibkr_event_contracts" for row in regulated_rows)),
            "regulated_market_count": int(len(regulated_rows)),
            "market_count": int(len(polymarket_rows) + len(regulated_rows)),
            "unavailable_reason_counts": dict(unavailable_reason_counts),
        }
    )
    return features, source_meta, True
