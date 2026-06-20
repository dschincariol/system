"""Point-in-time feature freshness and leakage policy helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


MS_MINUTE = 60 * 1000
MS_HOUR = 60 * MS_MINUTE
MS_DAY = 24 * MS_HOUR


@dataclass(frozen=True)
class FeaturePITPolicy:
    """Metadata and enforcement policy for one feature group."""

    group: str
    source_timestamp_field: str
    availability_timestamp_field: str
    freshness_ttl_ms: int
    lag_policy: str
    stale_behavior: str
    pit_eligible: bool = True
    source_timestamp_candidates: tuple[str, ...] = ()
    availability_timestamp_candidates: tuple[str, ...] = ()
    required_lag_ms: int = 0
    availability_required: bool = True

    def source_fields(self) -> tuple[str, ...]:
        return _dedupe((self.source_timestamp_field, *self.source_timestamp_candidates))

    def availability_fields(self) -> tuple[str, ...]:
        return _dedupe((self.availability_timestamp_field, *self.availability_timestamp_candidates))

    def to_metadata(self) -> dict[str, Any]:
        return {
            "source_timestamp_field": str(self.source_timestamp_field),
            "source_timestamp_candidates": list(self.source_fields()),
            "availability_timestamp_field": str(self.availability_timestamp_field),
            "availability_timestamp_candidates": list(self.availability_fields()),
            "freshness_ttl_ms": int(self.freshness_ttl_ms),
            "lag_policy": str(self.lag_policy),
            "required_lag_ms": int(self.required_lag_ms),
            "stale_behavior": str(self.stale_behavior),
            "pit_eligible": bool(self.pit_eligible),
            "availability_required": bool(self.availability_required),
        }


def _dedupe(values: Sequence[str]) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = str(value or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return tuple(out)


FEATURE_PIT_POLICIES: dict[str, FeaturePITPolicy] = {
    "price": FeaturePITPolicy(
        group="price",
        source_timestamp_field="history_last_ts_ms",
        source_timestamp_candidates=("quote_ts_ms", "benchmark_last_ts_ms"),
        availability_timestamp_field="quote_ts_ms",
        availability_timestamp_candidates=("history_last_ts_ms",),
        freshness_ttl_ms=15 * MS_MINUTE,
        lag_policy="event_time_or_quote_time_available_immediately",
        stale_behavior="zero_and_mark_unavailable",
    ),
    "tech": FeaturePITPolicy(
        group="tech",
        source_timestamp_field="har_forecast_asof_ts_ms",
        source_timestamp_candidates=("har_forecast_ts_ms",),
        availability_timestamp_field="har_forecast_ts_ms",
        freshness_ttl_ms=2 * MS_DAY,
        lag_policy="forecast_row_timestamp_must_be_available",
        stale_behavior="zero_and_mark_unavailable",
        availability_required=False,
    ),
    "events": FeaturePITPolicy(
        group="events",
        source_timestamp_field="latest_event_ts_ms",
        availability_timestamp_field="latest_event_availability_ts_ms",
        availability_timestamp_candidates=("latest_event_ts_ms",),
        freshness_ttl_ms=24 * MS_HOUR,
        lag_policy="event_availability_or_event_timestamp",
        stale_behavior="zero_and_mark_unavailable",
    ),
    "macro": FeaturePITPolicy(
        group="macro",
        source_timestamp_field="effective_ts_ms",
        availability_timestamp_field="asof_ts_ms",
        freshness_ttl_ms=180 * MS_DAY,
        lag_policy="vendor_release_vintage_availability",
        stale_behavior="zero_and_mark_unavailable",
    ),
    "options": FeaturePITPolicy(
        group="options",
        source_timestamp_field="bucket_ts_ms",
        availability_timestamp_field="snapshot_ts_ms",
        freshness_ttl_ms=30 * MS_MINUTE,
        lag_policy="chain_snapshot_timestamp",
        stale_behavior="zero_and_mark_unavailable",
    ),
    "insider": FeaturePITPolicy(
        group="insider",
        source_timestamp_field="latest_transaction_ts_ms",
        availability_timestamp_field="latest_availability_ts_ms",
        freshness_ttl_ms=390 * MS_DAY,
        lag_policy="edgar_filing_availability_not_transaction_date",
        stale_behavior="zero_and_mark_unavailable",
    ),
    "short": FeaturePITPolicy(
        group="short",
        source_timestamp_field="latest_short_interest_settlement_ts_ms",
        source_timestamp_candidates=("latest_short_volume_trade_ts_ms",),
        availability_timestamp_field="latest_short_interest_availability_ts_ms",
        availability_timestamp_candidates=("latest_short_volume_availability_ts_ms",),
        freshness_ttl_ms=75 * MS_DAY,
        lag_policy="finra_publication_availability",
        stale_behavior="zero_and_mark_unavailable",
    ),
    "crypto_positioning": FeaturePITPolicy(
        group="crypto_positioning",
        source_timestamp_field="latest_funding_ts_ms",
        availability_timestamp_field="latest_availability_ts_ms",
        freshness_ttl_ms=2 * MS_DAY,
        lag_policy="exchange_funding_timestamp",
        stale_behavior="zero_and_mark_unavailable",
    ),
    "news_flow": FeaturePITPolicy(
        group="news_flow",
        source_timestamp_field="latest_availability_ts_ms",
        availability_timestamp_field="latest_availability_ts_ms",
        freshness_ttl_ms=24 * MS_HOUR,
        lag_policy="embedding_availability_timestamp",
        stale_behavior="zero_and_mark_unavailable",
    ),
    "structured_doc_events": FeaturePITPolicy(
        group="structured_doc_events",
        source_timestamp_field="latest_event_ts_ms",
        availability_timestamp_field="latest_availability_ts_ms",
        freshness_ttl_ms=180 * MS_DAY,
        lag_policy="document_event_extraction_availability_timestamp",
        stale_behavior="zero_and_mark_unavailable",
    ),
    "etf_flow": FeaturePITPolicy(
        group="etf_flow",
        source_timestamp_field="latest_asof_ts_ms",
        source_timestamp_candidates=("latest_availability_ts_ms",),
        availability_timestamp_field="latest_availability_ts_ms",
        freshness_ttl_ms=14 * MS_DAY,
        lag_policy="issuer_vendor_next_morning_availability",
        stale_behavior="zero_and_mark_unavailable",
    ),
    "cot": FeaturePITPolicy(
        group="cot",
        source_timestamp_field="latest_report_ts_ms",
        source_timestamp_candidates=("latest_availability_ts_ms",),
        availability_timestamp_field="latest_availability_ts_ms",
        freshness_ttl_ms=21 * MS_DAY,
        lag_policy="cftc_release_timestamp",
        stale_behavior="zero_and_mark_unavailable",
    ),
    "inst_13f": FeaturePITPolicy(
        group="inst_13f",
        source_timestamp_field="latest_report_ts_ms",
        source_timestamp_candidates=("latest_availability_ts_ms",),
        availability_timestamp_field="latest_availability_ts_ms",
        freshness_ttl_ms=550 * MS_DAY,
        lag_policy="edgar_acceptance_timestamp",
        stale_behavior="zero_and_mark_unavailable",
    ),
    "gov": FeaturePITPolicy(
        group="gov",
        source_timestamp_field="latest_disclosure_ts_ms",
        availability_timestamp_field="latest_availability_ts_ms",
        freshness_ttl_ms=366 * MS_DAY,
        lag_policy="provider_disclosure_or_publish_timestamp",
        stale_behavior="zero_and_mark_unavailable",
    ),
    "fundamentals": FeaturePITPolicy(
        group="fundamentals",
        source_timestamp_field="latest_period_end_ts_ms",
        source_timestamp_candidates=("latest_publish_ts_ms",),
        availability_timestamp_field="latest_publish_ts_ms",
        freshness_ttl_ms=550 * MS_DAY,
        lag_policy="vendor_publish_timestamp",
        stale_behavior="zero_and_mark_unavailable",
    ),
    "congressional": FeaturePITPolicy(
        group="congressional",
        source_timestamp_field="latest_transaction_ts_ms",
        availability_timestamp_field="latest_availability_ts_ms",
        availability_timestamp_candidates=("latest_trade_ts_ms",),
        freshness_ttl_ms=366 * MS_DAY,
        lag_policy="disclosure_timestamp_not_transaction_date",
        stale_behavior="zero_and_mark_unavailable",
    ),
    "social": FeaturePITPolicy(
        group="social",
        source_timestamp_field="bucket_ts_ms",
        availability_timestamp_field="bucket_ts_ms",
        freshness_ttl_ms=30 * MS_MINUTE,
        lag_policy="bucket_timestamp_available_after_close",
        stale_behavior="zero_and_mark_unavailable",
    ),
    "sentiment": FeaturePITPolicy(
        group="sentiment",
        source_timestamp_field="ts_ms",
        availability_timestamp_field="ts_ms",
        freshness_ttl_ms=24 * MS_HOUR,
        lag_policy="sentiment_enrichment_timestamp",
        stale_behavior="zero_and_mark_unavailable",
    ),
    "weather": FeaturePITPolicy(
        group="weather",
        source_timestamp_field="forecast_run_ts_ms",
        source_timestamp_candidates=("alert_issued_ts_ms",),
        availability_timestamp_field="forecast_run_ts_ms",
        availability_timestamp_candidates=("alert_issued_ts_ms",),
        freshness_ttl_ms=3 * MS_DAY,
        lag_policy="forecast_or_alert_issue_timestamp",
        stale_behavior="zero_and_mark_unavailable",
        availability_required=False,
    ),
    "bocpd_regime": FeaturePITPolicy(
        group="bocpd_regime",
        source_timestamp_field="summary_ts_ms",
        availability_timestamp_field="summary_ts_ms",
        freshness_ttl_ms=7 * MS_DAY,
        lag_policy="summary_timestamp",
        stale_behavior="zero_and_mark_unavailable",
        availability_required=False,
    ),
    "nlp": FeaturePITPolicy(
        group="nlp",
        source_timestamp_field="latest_blob_ts_ms",
        availability_timestamp_field="latest_blob_ts_ms",
        freshness_ttl_ms=90 * MS_DAY,
        lag_policy="cached_text_blob_timestamp",
        stale_behavior="zero_and_mark_unavailable",
        availability_required=False,
    ),
    "discovered_llm": FeaturePITPolicy(
        group="discovered_llm",
        source_timestamp_field="created_ts",
        availability_timestamp_field="created_ts",
        freshness_ttl_ms=365 * MS_DAY,
        lag_policy="discovery_registration_created_timestamp",
        stale_behavior="zero_and_mark_unavailable",
        availability_required=False,
    ),
    "ts_foundation_chronos": FeaturePITPolicy(
        group="ts_foundation_chronos",
        source_timestamp_field="price_history_last_ts_ms",
        availability_timestamp_field="encoder_artifact_created_ts_ms",
        availability_timestamp_candidates=("price_history_last_ts_ms",),
        freshness_ttl_ms=2 * MS_DAY,
        lag_policy="price_history_asof_and_frozen_encoder_artifact_available",
        stale_behavior="zero_and_mark_unavailable",
    ),
    "graph_relational_v1": FeaturePITPolicy(
        group="graph_relational_v1",
        source_timestamp_field="max_source_ts_ms",
        availability_timestamp_field="max_availability_ts_ms",
        freshness_ttl_ms=7 * MS_DAY,
        lag_policy="relationship_source_availability_at_or_before_decision",
        stale_behavior="zero_and_mark_unavailable",
    ),
}


_PREFIX_GROUPS: tuple[tuple[str, str], ...] = (
    ("price.", "price"),
    ("tech.", "tech"),
    ("events.", "events"),
    ("macro.", "macro"),
    ("options_symbol.", "options"),
    ("insider.", "insider"),
    ("insider_", "insider"),
    ("short_", "short"),
    ("si_", "short"),
    ("days_to_cover_", "short"),
    ("funding_", "crypto_positioning"),
    ("perp_", "crypto_positioning"),
    ("basis_", "crypto_positioning"),
    ("news_", "news_flow"),
    ("fresh_neg_news_", "news_flow"),
    ("structured_doc_events_v1.", "structured_doc_events"),
    ("etf_", "etf_flow"),
    ("cot_", "cot"),
    ("13f_", "inst_13f"),
    ("congress_", "gov"),
    ("lobbying_", "gov"),
    ("gov_", "gov"),
    ("fund_", "fundamentals"),
    ("congressional.", "congressional"),
    ("social.", "social"),
    ("social_regime.", "social"),
    ("sentiment.", "sentiment"),
    ("weather.", "weather"),
    ("bocpd_", "bocpd_regime"),
    ("nlp.", "nlp"),
    ("discovered.llm.", "discovered_llm"),
    ("tsfm.chronos_v2.", "ts_foundation_chronos"),
    ("graph.relational_v1.", "graph_relational_v1"),
)


def group_for_feature_id(feature_id: str) -> str | None:
    fid = str(feature_id or "").strip()
    if not fid:
        return None
    for prefix, group in _PREFIX_GROUPS:
        if fid.startswith(prefix):
            return group
    return None


def feature_ids_by_group(feature_ids: Sequence[str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for fid in feature_ids or []:
        group = group_for_feature_id(str(fid))
        if not group:
            continue
        out.setdefault(group, []).append(str(fid))
    return out


def policy_metadata_for_groups(groups: Sequence[str] | None = None) -> dict[str, dict[str, Any]]:
    selected = list(groups or FEATURE_PIT_POLICIES.keys())
    return {
        str(group): FEATURE_PIT_POLICIES[str(group)].to_metadata()
        for group in selected
        if str(group) in FEATURE_PIT_POLICIES
    }


def _safe_int_or_none(value: Any) -> int | None:
    try:
        out = int(value)
    except Exception:
        return None
    return int(out) if out > 0 else None


def _latest_timestamp(meta: Mapping[str, Any], fields: Sequence[str]) -> int | None:
    values = [
        int(value)
        for field in fields
        if (value := _safe_int_or_none((meta or {}).get(str(field)))) is not None
    ]
    return max(values) if values else None


def evaluate_group_policy(
    *,
    group: str,
    source_meta: Mapping[str, Any],
    anchor_ts_ms: int,
    available: bool,
) -> dict[str, Any]:
    policy = FEATURE_PIT_POLICIES.get(str(group))
    if policy is None:
        return {
            "group": str(group),
            "ok": True,
            "reason_codes": [],
            "pit_eligible": True,
        }

    source_ts_ms = _latest_timestamp(source_meta, policy.source_fields())
    availability_ts_ms = _latest_timestamp(source_meta, policy.availability_fields())
    reason_codes: list[str] = []

    if bool(available):
        if not bool(policy.pit_eligible):
            reason_codes.append("pit_ineligible")
        if policy.availability_required and availability_ts_ms is None:
            reason_codes.append("availability_timestamp_missing")
        if availability_ts_ms is not None and int(availability_ts_ms) > int(anchor_ts_ms):
            reason_codes.append("availability_after_decision")
        if source_ts_ms is not None and int(source_ts_ms) > int(anchor_ts_ms):
            reason_codes.append("source_after_decision")
        if (
            int(policy.required_lag_ms) > 0
            and source_ts_ms is not None
            and availability_ts_ms is not None
            and int(availability_ts_ms) < int(source_ts_ms) + int(policy.required_lag_ms)
        ):
            reason_codes.append("lag_policy_violation")
        if (
            int(policy.freshness_ttl_ms) > 0
            and availability_ts_ms is not None
            and int(anchor_ts_ms) - int(availability_ts_ms) > int(policy.freshness_ttl_ms)
        ):
            reason_codes.append("feature_stale")

    return {
        "group": str(group),
        "ok": not bool(reason_codes),
        "reason_codes": list(reason_codes),
        "source_ts_ms": source_ts_ms,
        "availability_ts_ms": availability_ts_ms,
        "freshness_ttl_ms": int(policy.freshness_ttl_ms),
        "lag_policy": str(policy.lag_policy),
        "required_lag_ms": int(policy.required_lag_ms),
        "stale_behavior": str(policy.stale_behavior),
        "pit_eligible": bool(policy.pit_eligible),
    }


def enforce_feature_pit_controls(
    *,
    features: Mapping[str, Any],
    availability: Mapping[str, Any],
    source_timestamps: Mapping[str, Any],
    anchor_ts_ms: int,
    feature_ids: Sequence[str],
) -> tuple[dict[str, float], dict[str, bool], dict[str, Any]]:
    feature_map = {str(k): float(v or 0.0) for k, v in dict(features or {}).items()}
    availability_map = {str(k): bool(v) for k, v in dict(availability or {}).items()}
    grouped_ids = feature_ids_by_group(feature_ids)
    pit_controls: dict[str, Any] = {}

    groups = sorted(set(grouped_ids) | {str(group) for group in availability_map if str(group) in FEATURE_PIT_POLICIES})
    for group in groups:
        policy = FEATURE_PIT_POLICIES.get(str(group))
        if policy is None:
            continue
        meta = dict((source_timestamps or {}).get(str(group)) or {})
        detail = evaluate_group_policy(
            group=str(group),
            source_meta=meta,
            anchor_ts_ms=int(anchor_ts_ms),
            available=bool(availability_map.get(str(group), False)),
        )
        pit_controls[str(group)] = detail
        if bool(detail.get("ok", True)):
            continue
        if str(policy.stale_behavior).startswith("zero"):
            availability_map[str(group)] = False
            for fid in grouped_ids.get(str(group), []):
                feature_map[str(fid)] = 0.0

    return feature_map, availability_map, pit_controls
