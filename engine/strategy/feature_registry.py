"""
FILE: feature_registry.py

Schema-driven feature resolution for train/serve parity.

Public feature-id and expected-column helpers preserve explicit registry
insertion order by default. Serving column order must not be derived from
unordered containers; callers that pass an unordered feature-id collection are
canonicalized by feature_id before validation so training and online inference
receive the same deterministic feature vector.
"""

from __future__ import annotations

import math
import os
import re
import time
from typing import Any, Dict, List, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.config import (
    FEATURE_STORE_READS_ENABLED,
    FEATURE_STORE_VERSION,
    USE_CONGRESSIONAL_TRADE_DATA,
)
from engine.data.asset_map import asset_class_for_symbol
from engine.data.finbert_sentiment import (
    FINBERT_FEATURE_IDS as _FINBERT_FEATURE_IDS,
    USE_FINBERT_SENTIMENT,
    resolve_finbert_sentiment_snapshot,
)
from engine.data.structured_document_events import STRUCTURED_DOCUMENT_EVENT_FEATURE_IDS
from engine.data.prediction_market_providers import (
    PREDICTION_MARKET_EVENT_FEATURE_GROUP,
    PREDICTION_MARKET_EVENT_FEATURE_IDS,
    PREDICTION_MARKET_EVENT_PREFIX,
    PREDICTION_MARKET_MACRO_FEATURE_GROUP,
    PREDICTION_MARKET_MACRO_FEATURE_IDS,
    PREDICTION_MARKET_MACRO_PREFIX,
)
from engine.data.deribit_crypto_derivatives import (
    DERIBIT_FEATURE_GROUP,
    DERIBIT_FEATURE_IDS,
    DERIBIT_FEATURE_PREFIX,
)
from engine.data.sportsbook_odds import (
    SPORTSBOOK_ODDS_FEATURE_GROUP,
    SPORTSBOOK_ODDS_FEATURE_IDS,
    SPORTSBOOK_ODDS_FEATURE_PREFIX,
)
from engine.strategy.tsfresh_features import (
    TSFRESH_FEATURE_PREFIX,
    get_default_tsfresh_feature_ids,
    get_tsfresh_feature_ids,
    resolve_tsfresh_features,
)
from engine.strategy.feature_pit import FEATURE_PIT_POLICIES, policy_metadata_for_groups
from engine.strategy.graph_relational import (
    GRAPH_RELATIONSHIP_TYPES,
    GRAPH_RELATIONAL_FEATURE_IDS,
    GRAPH_RELATIONAL_GROUP,
    GRAPH_RELATIONAL_PREFIX,
    GRAPH_RELATIONAL_SNAPSHOT_VERSION,
    graph_max_neighbors,
)
from engine.strategy.ts_foundation_encoder import (
    TS_FOUNDATION_CHRONOS_FEATURE_IDS,
    TS_FOUNDATION_CHRONOS_GROUP,
    TS_FOUNDATION_CHRONOS_PREFIX,
    chronos_model_id,
)

USE_TECH_FEATURES = os.environ.get("USE_TECH_FEATURES", "0") == "1"
USE_STRESS_FEATURES = os.environ.get("USE_STRESS_FEATURES", "0") == "1"
USE_MACRO_FEATURES = os.environ.get("USE_MACRO_FEATURES", "1") == "1"
USE_SOCIAL_FEATURES = os.environ.get("USE_SOCIAL_FEATURES", "0") == "1"
USE_SOCIAL_REGIME = os.environ.get("USE_SOCIAL_REGIME", "0") == "1"
USE_WEATHER_FEATURES = os.environ.get("USE_WEATHER_FEATURES", "0") == "1"
USE_OPTIONS_FEATURES = os.environ.get("USE_OPTIONS_FEATURES", "0") == "1"
USE_FACTOR_UNIVERSE = os.environ.get("USE_FACTOR_UNIVERSE", "0") == "1"
USE_SYMBOL_SNAPSHOT_FEATURES = os.environ.get("USE_SYMBOL_SNAPSHOT_FEATURES", "1") == "1"
USE_TSFRESH_FEATURES = os.environ.get("USE_TSFRESH_FEATURES", "0") == "1"
USE_NLP_FEATURES = os.environ.get("USE_NLP_FEATURES", "0") == "1"
FINBERT_FEATURE_IDS = list(_FINBERT_FEATURE_IDS) if USE_FINBERT_SENTIMENT else []


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name))
    if raw is None:
        return bool(default)
    text = str(raw).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


USE_INSIDER_FEATURES = _env_bool("USE_INSIDER_FEATURES", False)
USE_SHORT_FEATURES = _env_bool("USE_SHORT_FEATURES", False)
USE_FUNDING_FEATURES = _env_bool("USE_FUNDING_FEATURES", False)
USE_NEWS_FLOW_FEATURES = _env_bool("USE_NEWS_FLOW_FEATURES", False)
USE_ETF_FLOW_FEATURES = _env_bool("USE_ETF_FLOW_FEATURES", False)
USE_COT_FEATURES = _env_bool("USE_COT_FEATURES", False)
USE_13F_FEATURES = _env_bool("USE_13F_FEATURES", False)
USE_GOV_FEATURES = _env_bool("USE_GOV_FEATURES", False)
USE_FUNDAMENTALS_PIT_FEATURES = _env_bool("USE_FUNDAMENTALS_PIT_FEATURES", False)
USE_BOCPD_FEATURES = _env_bool("USE_BOCPD_FEATURES", False)
USE_DERIBIT_CRYPTO_DERIVATIVES_FEATURES = _env_bool("USE_DERIBIT_CRYPTO_DERIVATIVES_FEATURES", False)

BASE_FEATURE_IDS = [
    "base.source_credibility",
    "base.log_recency_hours",
    "base.normalized_text_len",
    "base.scheduled_flag",
    "base.session_asia",
    "base.session_eu",
    "base.session_us",
    "base.asset_class_match",
]

TECH_FEATURE_IDS = [
    "tech.kama_level",
    "tech.kama_slope",
    "tech.price_kama_z",
    "tech.atr_14",
    "tech.atr_pct",
    "tech.rv_20",
    "tech.vol_of_vol",
    "tech.har_rv_forecast_1d",
    "tech.har_rv_forecast_ratio",
]

META_LABEL_FEATURE_IDS = [
    "meta_label.primary_abs_z",
    "meta_label.primary_confidence",
    "meta_label.side_sign",
    "meta_label.vol_level",
    "meta_label.vol_ratio",
    "meta_label.rolling_hit_rate",
    "meta_label.regime_risk_off",
    "meta_label.regime_confidence",
    "meta_label.ood_distance",
]

STRESS_FEATURE_IDS = [
    "stress.z_vix",
    "stress.z_vvix",
    "stress.z_move",
    "stress.z_term",
    "stress.z_credit",
    "stress.stress_score",
]

MACRO_FEATURE_IDS = [
    "macro.cpi_yoy",
    "macro.cpi_yoy_z",
    "macro.cpi_yoy_d1",
    "macro.policy_rate_upper",
    "macro.policy_rate_upper_z",
    "macro.policy_rate_upper_d5",
    "macro.unemployment_rate",
    "macro.unemployment_rate_z",
    "macro.unemployment_rate_d1",
    "macro.gdp_real_qoq_ann",
    "macro.gdp_real_qoq_ann_z",
    "macro.gdp_real_qoq_ann_d1",
    "macro.oil_wti_spot",
    "macro.oil_wti_spot_z",
    "macro.oil_wti_spot_d5",
    "macro.natgas_spot",
    "macro.natgas_spot_z",
    "macro.natgas_spot_d5",
]

SOCIAL_FEATURE_IDS = [
    "social.mention_rate_z",
    "social.unique_authors",
    "social.new_author_ratio",
    "social.sentiment_mean",
    "social.sentiment_dispersion",
    "social.manip_risk",
    "social.attention_shock",
    "social.promo_likelihood_mean",
]

WEATHER_FEATURE_IDS = [
    "weather.hdd_3d",
    "weather.hdd_7d",
    "weather.cdd_3d",
    "weather.cdd_7d",
    "weather.precip_7d",
    "weather.wind_3d",
    "weather.spread_7d",
    "weather.anomaly_score",
    "weather.extreme_event_score",
    "weather.alert_severity",
    "weather.temp_anomaly_3d",
    "weather.wind_anomaly_3d",
    "weather.precip_anomaly_7d",
    "weather.storm_risk",
]

_BASE_OPTIONS_FEATURE_IDS = [
    "options_symbol.iv_rank",
    "options_symbol.iv_rank_short",
    "options_symbol.skew_25d",
    "options_symbol.term_structure_slope",
    "options_symbol.unusual_volume_score",
    "options_symbol.call_put_volume_ratio",
    "options_symbol.call_put_oi_ratio",
    "options_symbol.signal_score",
]

_OPTIONS_GEX_FLOW_FEATURE_IDS = [
    "options_symbol.gex_norm_z",
    "options_symbol.gex_sign",
    "options_symbol.opt_flow_imbalance_z",
]

OPTIONS_FEATURE_IDS = list(_BASE_OPTIONS_FEATURE_IDS) + (
    list(_OPTIONS_GEX_FLOW_FEATURE_IDS) if USE_OPTIONS_FEATURES else []
)

_ALL_INSIDER_FEATURE_IDS = [
    "insider_opp_net_buy_30d",
    "insider_opp_buy_count_30d",
    "insider_cluster_buy_5d",
    "insider_officer_buy_flag",
    "insider_opp_sell_z",
]

_ALL_SHORT_FEATURE_IDS = [
    "short_vol_ratio_z20",
    "si_surprise",
    "days_to_cover_delta",
    "si_surprise_x_earnings_window",
]

_ALL_CRYPTO_POSITIONING_FEATURE_IDS = [
    "funding_rate_now",
    "funding_z_30d",
    "funding_extreme_flag",
    "funding_cum_3d",
    "perp_basis_pct",
    "basis_z_30d",
]

_ALL_NEWS_FLOW_FEATURE_IDS = [
    "news_novelty_max_24h",
    "news_stale_share_24h",
    "news_velocity_z",
    "fresh_neg_news_flag",
]

_ALL_ETF_FLOW_FEATURE_IDS = [
    "etf_unexpected_flow_z",
    "etf_flow_3d_sum_z",
    "etf_flow_reversal_flag",
]

_ALL_COT_FEATURE_IDS = [
    "cot_commercial_net_pctile_3y",
    "cot_noncomm_net_z",
    "cot_noncomm_extreme_flag",
    "cot_open_interest_z",
]

_ALL_INST_13F_FEATURE_IDS = [
    "13f_consensus_holders",
    "13f_conviction_max",
    "13f_new_position_flag",
    "13f_add_flag",
]

_ALL_GOV_FEATURE_IDS = [
    "congress_committee_buy_30d",
    "congress_leadership_trade_flag",
    "congress_sale_signal_30d",
    "lobbying_spend_z_yoy",
    "gov_contract_award_z",
]

_ALL_FUNDAMENTALS_PIT_FEATURE_IDS = [
    "fund_revenue",
    "fund_eps",
    "fund_gross_margin",
    "fund_net_margin",
    "fund_shares",
    "fund_book_value",
    "fund_fcf",
]

_ALL_CONGRESSIONAL_FEATURE_IDS = [
    "congressional.buy_count_30d",
    "congressional.sell_count_30d",
    "congressional.net_signal_30d",
]

INSIDER_FEATURE_IDS = list(_ALL_INSIDER_FEATURE_IDS) if USE_INSIDER_FEATURES else []
SHORT_FEATURE_IDS = list(_ALL_SHORT_FEATURE_IDS) if USE_SHORT_FEATURES else []
CRYPTO_POSITIONING_FEATURE_IDS = list(_ALL_CRYPTO_POSITIONING_FEATURE_IDS) if USE_FUNDING_FEATURES else []
DERIBIT_CRYPTO_DERIVATIVES_FEATURE_IDS = (
    list(DERIBIT_FEATURE_IDS) if USE_DERIBIT_CRYPTO_DERIVATIVES_FEATURES else []
)
NEWS_FLOW_FEATURE_IDS = list(_ALL_NEWS_FLOW_FEATURE_IDS) if USE_NEWS_FLOW_FEATURES else []
ETF_FLOW_FEATURE_IDS = list(_ALL_ETF_FLOW_FEATURE_IDS) if USE_ETF_FLOW_FEATURES else []
COT_FEATURE_IDS = list(_ALL_COT_FEATURE_IDS) if USE_COT_FEATURES else []
INST_13F_FEATURE_IDS = list(_ALL_INST_13F_FEATURE_IDS) if USE_13F_FEATURES else []
GOV_FEATURE_IDS = list(_ALL_GOV_FEATURE_IDS) if USE_GOV_FEATURES else []
FUNDAMENTALS_PIT_FEATURE_IDS = list(_ALL_FUNDAMENTALS_PIT_FEATURE_IDS) if USE_FUNDAMENTALS_PIT_FEATURES else []
CONGRESSIONAL_FEATURE_IDS = list(_ALL_CONGRESSIONAL_FEATURE_IDS) if USE_CONGRESSIONAL_TRADE_DATA else []

SOCIAL_REGIME_FEATURE_IDS = [
    "social_regime.mania_score",
    "social_regime.fear_score",
    "social_regime.churn_score",
    "social_regime.regime_quiet",
    "social_regime.regime_churn",
    "social_regime.regime_fear",
    "social_regime.regime_mania",
    "social_regime.regime_conf",
]

HMM_REGIME_FEATURE_IDS = [
    "hmm_regime.enabled",
    "hmm_regime.model_available",
    "hmm_regime.confidence",
    "hmm_regime.entropy",
    "hmm_regime.state_0_prob",
    "hmm_regime.state_1_prob",
    "hmm_regime.state_2_prob",
    "hmm_regime.state_3_prob",
    "hmm_regime.state_4_prob",
    "hmm_regime.label_risk_on_prob",
    "hmm_regime.label_recovery_prob",
    "hmm_regime.label_neutral_prob",
    "hmm_regime.label_volatile_prob",
    "hmm_regime.label_risk_off_prob",
]

BOCPD_FEATURE_IDS = [
    "bocpd_cp_prob_5d",
    "bocpd_run_length_z",
]

PRICE_FEATURE_IDS = [
    "price.last",
    "price.spread_bps",
    "price.volume",
    "price.log_ret_5m",
    "price.log_ret_1h",
    "price.log_ret_1d",
    "price.pct_ret_5m",
    "price.pct_ret_1h",
    "price.pct_ret_1d",
    "price.momentum_5m",
    "price.momentum_1h",
    "price.momentum_1d",
    "price.rv_20",
    "price.atr_pct_14",
    "price.vol_std_20",
    "price.vol_std_60",
    "price.cross_asset_rel_1h",
    "price.cross_asset_rel_1d",
    "price.cross_asset_corr_20",
    "price.cross_asset_beta_20",
    "price.vol_regime_low",
    "price.vol_regime_mid",
    "price.vol_regime_high",
    "price.vol_regime_ratio",
    "price.trend_regime_trend",
    "price.trend_regime_mean_reversion",
    "price.trend_strength_20",
]

EVENT_FEATURE_IDS = [
    "events.count_1h",
    "events.count_6h",
    "events.count_24h",
    "events.velocity_6h",
    "events.sentiment_trend_6h",
    "events.avg_novelty_6h",
    "events.duplicate_share_6h",
    "events.importance_mean_24h",
    "events.hours_since_last",
]

UNIFIED_MACRO_FEATURE_IDS = list(MACRO_FEATURE_IDS) + [
    "macro.gdelt_doc_count",
    "macro.gdelt_tone_mean",
    "macro.gdelt_tone_std",
    "macro.gdelt_conflict_share",
    "macro.gdelt_econ_share",
]

UNIFIED_SOCIAL_FEATURE_IDS = list(SOCIAL_FEATURE_IDS) + list(SOCIAL_REGIME_FEATURE_IDS)

AVAILABILITY_FEATURE_IDS = [
    "availability.price",
    "availability.events",
    "availability.macro",
    "availability.options",
    "availability.social",
    "availability.weather",
]

TSFRESH_FEATURE_IDS = list(get_tsfresh_feature_ids())

NLP_EMBEDDING_DIM = max(1, int(os.environ.get("NLP_EMBEDDING_DIM", "384")))
NLP_FINBERT_MODEL_NAME = str(os.environ.get("NLP_FINBERT_MODEL_NAME", "ProsusAI/finbert") or "ProsusAI/finbert")
NLP_SENTENCE_MODEL_NAME = str(os.environ.get("NLP_SENTENCE_MODEL_NAME", "all-MiniLM-L6-v2") or "all-MiniLM-L6-v2")
NLP_FINBERT_NEWS_FEATURE_IDS = [
    "nlp.finbert_news_v1.score_mean",
    "nlp.finbert_news_v1.score_weighted_mean",
    "nlp.finbert_news_v1.score_max",
    "nlp.finbert_news_v1.article_count",
    "nlp.finbert_news_v1.positive_mean",
    "nlp.finbert_news_v1.negative_mean",
    "nlp.finbert_news_v1.neutral_mean",
]
NLP_FILINGS_FEATURE_IDS = (
    [f"nlp.filings_v1.embedding_mean_{idx:03d}" for idx in range(NLP_EMBEDDING_DIM)]
    + [f"nlp.filings_v1.embedding_max_{idx:03d}" for idx in range(NLP_EMBEDDING_DIM)]
    + ["nlp.filings_v1.paragraph_count"]
)
NLP_TRANSCRIPTS_FEATURE_IDS = (
    [f"nlp.transcripts_v1.embedding_mean_{idx:03d}" for idx in range(NLP_EMBEDDING_DIM)]
    + [f"nlp.transcripts_v1.embedding_max_{idx:03d}" for idx in range(NLP_EMBEDDING_DIM)]
    + [
        "nlp.transcripts_v1.section_count",
        "nlp.transcripts_v1.qa_score_mean",
        "nlp.transcripts_v1.qa_score_weighted_mean",
        "nlp.transcripts_v1.qa_score_max",
        "nlp.transcripts_v1.qa_section_count",
    ]
)
NLP_FEATURE_IDS = (
    list(NLP_FINBERT_NEWS_FEATURE_IDS)
    + list(NLP_FILINGS_FEATURE_IDS)
    + list(NLP_TRANSCRIPTS_FEATURE_IDS)
)
LEXICAL_SENTIMENT_DEPRECATED_AFTER = "877896fd3878f5e93381df5dc30d0a19dec94995"
_LEXICAL_SENTIMENT_DEPRECATED_AFTER_RE = re.compile(
    r"^(?:[0-9a-fA-F]{7,40}|\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)?)$"
)
_LEXICAL_SENTIMENT_PLACEHOLDER_RE = re.compile(
    r"^\s*(?:|tbd|todo|pending|placeholder|replace-me|unknown|none|null|n/a|na|fixme|xxx)\s*$",
    re.IGNORECASE,
)

UNIFIED_SYMBOL_FEATURE_IDS = (
    list(PRICE_FEATURE_IDS)
    + list(EVENT_FEATURE_IDS)
    + list(UNIFIED_MACRO_FEATURE_IDS)
    + list(OPTIONS_FEATURE_IDS)
    + list(INSIDER_FEATURE_IDS)
    + list(SHORT_FEATURE_IDS)
    + list(CRYPTO_POSITIONING_FEATURE_IDS)
    + list(DERIBIT_CRYPTO_DERIVATIVES_FEATURE_IDS)
    + list(NEWS_FLOW_FEATURE_IDS)
    + list(ETF_FLOW_FEATURE_IDS)
    + list(COT_FEATURE_IDS)
    + list(INST_13F_FEATURE_IDS)
    + list(GOV_FEATURE_IDS)
    + list(FUNDAMENTALS_PIT_FEATURE_IDS)
    + list(CONGRESSIONAL_FEATURE_IDS)
    + list(UNIFIED_SOCIAL_FEATURE_IDS)
    + list(WEATHER_FEATURE_IDS)
    + (list(BOCPD_FEATURE_IDS) if USE_BOCPD_FEATURES else [])
    + list(FINBERT_FEATURE_IDS)
    + (list(NLP_FEATURE_IDS) if USE_NLP_FEATURES else [])
    + list(AVAILABILITY_FEATURE_IDS)
)

FEATURE_GROUPS = {
    "base": list(BASE_FEATURE_IDS),
    "price": list(PRICE_FEATURE_IDS),
    "events": list(EVENT_FEATURE_IDS),
    "macro": list(UNIFIED_MACRO_FEATURE_IDS),
    "hmm_regime": list(HMM_REGIME_FEATURE_IDS),
    "regime": list(HMM_REGIME_FEATURE_IDS) + list(BOCPD_FEATURE_IDS),
    "bocpd_regime": list(BOCPD_FEATURE_IDS),
    "options_symbol": list(OPTIONS_FEATURE_IDS),
    "options": list(OPTIONS_FEATURE_IDS),
    "social": list(UNIFIED_SOCIAL_FEATURE_IDS),
    "weather": list(WEATHER_FEATURE_IDS),
    "availability": list(AVAILABILITY_FEATURE_IDS),
    "tech": list(TECH_FEATURE_IDS),
    "meta_label": list(META_LABEL_FEATURE_IDS),
    "stress": list(STRESS_FEATURE_IDS),
    "tsfresh": list(TSFRESH_FEATURE_IDS),
    "discovered_llm": [],
    "nlp_finbert_news_v1": list(NLP_FINBERT_NEWS_FEATURE_IDS),
    "nlp_filings_v1": list(NLP_FILINGS_FEATURE_IDS),
    "nlp_transcripts_v1": list(NLP_TRANSCRIPTS_FEATURE_IDS),
    "structured_doc_events_v1": list(STRUCTURED_DOCUMENT_EVENT_FEATURE_IDS),
    DERIBIT_FEATURE_GROUP: list(DERIBIT_FEATURE_IDS),
    SPORTSBOOK_ODDS_FEATURE_GROUP: list(SPORTSBOOK_ODDS_FEATURE_IDS),
    PREDICTION_MARKET_MACRO_FEATURE_GROUP: list(PREDICTION_MARKET_MACRO_FEATURE_IDS),
    PREDICTION_MARKET_EVENT_FEATURE_GROUP: list(PREDICTION_MARKET_EVENT_FEATURE_IDS),
    TS_FOUNDATION_CHRONOS_GROUP: list(TS_FOUNDATION_CHRONOS_FEATURE_IDS),
    GRAPH_RELATIONAL_GROUP: list(GRAPH_RELATIONAL_FEATURE_IDS),
}
if FINBERT_FEATURE_IDS:
    FEATURE_GROUPS["sentiment"] = list(FINBERT_FEATURE_IDS)
if INSIDER_FEATURE_IDS:
    FEATURE_GROUPS["insider"] = list(INSIDER_FEATURE_IDS)
if SHORT_FEATURE_IDS:
    FEATURE_GROUPS["short"] = list(SHORT_FEATURE_IDS)
if CRYPTO_POSITIONING_FEATURE_IDS:
    FEATURE_GROUPS["crypto_positioning"] = list(CRYPTO_POSITIONING_FEATURE_IDS)
if NEWS_FLOW_FEATURE_IDS:
    FEATURE_GROUPS["news_flow"] = list(NEWS_FLOW_FEATURE_IDS)
if ETF_FLOW_FEATURE_IDS:
    FEATURE_GROUPS["etf_flow"] = list(ETF_FLOW_FEATURE_IDS)
if COT_FEATURE_IDS:
    FEATURE_GROUPS["cot"] = list(COT_FEATURE_IDS)
if INST_13F_FEATURE_IDS:
    FEATURE_GROUPS["inst_13f"] = list(INST_13F_FEATURE_IDS)
if GOV_FEATURE_IDS:
    FEATURE_GROUPS["gov"] = list(GOV_FEATURE_IDS)
if FUNDAMENTALS_PIT_FEATURE_IDS:
    FEATURE_GROUPS["fundamentals"] = list(FUNDAMENTALS_PIT_FEATURE_IDS)
if CONGRESSIONAL_FEATURE_IDS:
    FEATURE_GROUPS["congressional"] = list(CONGRESSIONAL_FEATURE_IDS)

FEATURE_STAGE_SHADOW = "shadow"
FEATURE_STAGE_LIVE = "live"
FEATURE_STAGES: Dict[str, str] = {
    str(fid): FEATURE_STAGE_LIVE
    for ids in FEATURE_GROUPS.values()
    for fid in list(ids or [])
    if str(fid or "").strip()
}
for _fid in TS_FOUNDATION_CHRONOS_FEATURE_IDS:
    FEATURE_STAGES[str(_fid)] = FEATURE_STAGE_SHADOW
for _fid in GRAPH_RELATIONAL_FEATURE_IDS:
    FEATURE_STAGES[str(_fid)] = FEATURE_STAGE_SHADOW
for _fid in STRUCTURED_DOCUMENT_EVENT_FEATURE_IDS:
    FEATURE_STAGES[str(_fid)] = FEATURE_STAGE_SHADOW
for _fid in PREDICTION_MARKET_MACRO_FEATURE_IDS:
    FEATURE_STAGES[str(_fid)] = FEATURE_STAGE_SHADOW
for _fid in PREDICTION_MARKET_EVENT_FEATURE_IDS:
    FEATURE_STAGES[str(_fid)] = FEATURE_STAGE_SHADOW
for _fid in DERIBIT_FEATURE_IDS:
    FEATURE_STAGES[str(_fid)] = FEATURE_STAGE_SHADOW
for _fid in SPORTSBOOK_ODDS_FEATURE_IDS:
    FEATURE_STAGES[str(_fid)] = FEATURE_STAGE_SHADOW

FEATURE_GROUP_METADATA: Dict[str, Dict[str, Any]] = {
    name: {
        "feature_ids": list(ids),
        "schema_version": str(name.rsplit("_v", 1)[-1]) if "_v" in str(name) else "legacy",
    }
    for name, ids in FEATURE_GROUPS.items()
}
FEATURE_GROUP_METADATA["lexical_sentiment_v0"] = {
    "feature_ids": ["sentiment_score"],
    "schema_version": "0",
    "deprecated_after": LEXICAL_SENTIMENT_DEPRECATED_AFTER,
    "serving_path": "news_event_features.sentiment_score",
}
FEATURE_GROUP_METADATA[TS_FOUNDATION_CHRONOS_GROUP].update(
    {
        "default_enabled": False,
        "direct_trading_authority": False,
        "encoder_mode": "frozen",
        "model_family": "chronos",
        "model_family_provenance": {
            "backend": "chronos",
            "direct_trading_authority": False,
            "frozen_encoder": True,
            "model_id": chronos_model_id(),
            "package": "chronos-forecasting",
            "source": "pretrained_time_series_foundation_model",
        },
        "stage": FEATURE_STAGE_SHADOW,
    }
)
FEATURE_GROUP_METADATA[GRAPH_RELATIONAL_GROUP].update(
    {
        "default_enabled": False,
        "direct_trading_authority": False,
        "feature_prefix": GRAPH_RELATIONAL_PREFIX,
        "graph_id": GRAPH_RELATIONAL_GROUP,
        "relationship_sources": sorted(GRAPH_RELATIONSHIP_TYPES),
        "max_neighbors": int(graph_max_neighbors()),
        "snapshot_version": int(GRAPH_RELATIONAL_SNAPSHOT_VERSION),
        "stage": FEATURE_STAGE_SHADOW,
        **FEATURE_PIT_POLICIES[GRAPH_RELATIONAL_GROUP].to_metadata(),
    }
)
FEATURE_GROUP_METADATA["structured_doc_events_v1"].update(
    {
        "default_enabled": False,
        "direct_trading_authority": False,
        "extractor_name": "structured_document_events",
        "extractor_version": "structured_document_events_v1",
        "source_documents": ["filing", "transcript", "news"],
        "stage": FEATURE_STAGE_SHADOW,
        **FEATURE_PIT_POLICIES["structured_doc_events"].to_metadata(),
    }
)
FEATURE_GROUP_METADATA[PREDICTION_MARKET_MACRO_FEATURE_GROUP].update(
    {
        "default_enabled": False,
        "direct_trading_authority": False,
        "provider_category": "macro",
        "providers": ["kalshi", "cme_fedwatch"],
        "stage": FEATURE_STAGE_SHADOW,
        **FEATURE_PIT_POLICIES[PREDICTION_MARKET_MACRO_FEATURE_GROUP].to_metadata(),
    }
)
FEATURE_GROUP_METADATA[PREDICTION_MARKET_EVENT_FEATURE_GROUP].update(
    {
        "default_enabled": False,
        "direct_trading_authority": False,
        "provider_category": "event_signal",
        "providers": ["polymarket", "forecastex", "ibkr_event_contracts"],
        "stage": FEATURE_STAGE_SHADOW,
        "requires_explicit_semantic_mapping_for_dispersion": True,
        "regulated_event_types": ["macro", "energy", "climate_weather", "fx_rates", "equity_index", "commodity"],
        **FEATURE_PIT_POLICIES[PREDICTION_MARKET_EVENT_FEATURE_GROUP].to_metadata(),
    }
)
FEATURE_GROUP_METADATA[DERIBIT_FEATURE_GROUP].update(
    {
        "default_enabled": False,
        "direct_trading_authority": False,
        "feature_prefix": DERIBIT_FEATURE_PREFIX,
        "provider": "deribit",
        "provider_category": "crypto_derivatives",
        "source_type": "derivatives_provider",
        "stage": FEATURE_STAGE_SHADOW,
        "shadow_only_until": [
            "out_of_sample",
            "net_after_cost",
            "pit",
            "deconfounded",
            "production_readiness",
        ],
        "evaluation_scope": ["BTC", "ETH", "SOL", "crypto_equities_explicit_mapping_only"],
        **FEATURE_PIT_POLICIES[DERIBIT_FEATURE_GROUP].to_metadata(),
    }
)
FEATURE_GROUP_METADATA[SPORTSBOOK_ODDS_FEATURE_GROUP].update(
    {
        "default_enabled": False,
        "direct_trading_authority": False,
        "feature_prefix": SPORTSBOOK_ODDS_FEATURE_PREFIX,
        "provider_category": "sportsbook_odds",
        "providers": ["betfair_historical", "the_odds_api", "opticodds", "oddsjam", "generic_json"],
        "source_type": "odds_provider",
        "stage": FEATURE_STAGE_SHADOW,
        "research_only": True,
        "requires_explicit_mapping": True,
        "no_fuzzy_event_mapping": True,
        "broad_market_default_allowed": False,
        "shadow_only_until": [
            "out_of_sample",
            "net_after_cost",
            "pit",
            "deconfounded",
            "production_readiness",
        ],
        "evaluation_scope": [
            "sportsbook_equities",
            "sports_media",
            "sports_data_providers",
            "advertising_sensitive_event_studies",
            "model_calibration_research",
        ],
        **FEATURE_PIT_POLICIES[SPORTSBOOK_ODDS_FEATURE_GROUP].to_metadata(),
    }
)
for _group_name, _pit_meta in policy_metadata_for_groups(FEATURE_GROUP_METADATA.keys()).items():
    FEATURE_GROUP_METADATA.setdefault(str(_group_name), {})
    FEATURE_GROUP_METADATA[str(_group_name)].update(_pit_meta)


def list_groups() -> Dict[str, Dict[str, Any]]:
    """Return schema-versioned feature group metadata."""

    out = {str(name): dict(meta) for name, meta in FEATURE_GROUP_METADATA.items()}
    llm_ids = [
        str(getattr(record, "feature_id", "") or "")
        for record in _load_discovered_feature_records(stage=None)
        if str(getattr(record, "source", "") or "") == "llm_factor"
    ]
    out["discovered_llm"] = {
        **dict(out.get("discovered_llm") or {}),
        "feature_ids": [fid for fid in llm_ids if fid],
        "schema_version": "experimental",
        "default_enabled": False,
        "stage": FEATURE_STAGE_SHADOW,
        **FEATURE_PIT_POLICIES["discovered_llm"].to_metadata(),
    }
    return out

_SOURCE_CRED = {
    "rss:reuters": 0.9,
    "rss:bloomberg": 0.9,
    "rss:ft": 0.9,
    "rss:wsj": 0.9,
    "rss:coindesk": 0.8,
    "rss:cointelegraph": 0.7,
}

_SNAPSHOT_PREFIXES = (
    "price.",
    "events.",
    "macro.",
    "options_symbol.",
    "insider.",
    "insider_",
    "short_",
    "si_",
    "days_to_cover_",
    "funding_",
    "perp_",
    "basis_",
    "news_",
    "fresh_neg_news_",
    "structured_doc_events_v1.",
    PREDICTION_MARKET_MACRO_PREFIX,
    PREDICTION_MARKET_EVENT_PREFIX,
    DERIBIT_FEATURE_PREFIX,
    SPORTSBOOK_ODDS_FEATURE_PREFIX,
    "etf_",
    "cot_",
    "13f_",
    "congress_",
    "lobbying_",
    "gov_",
    "fund_",
    "congressional.",
    "social.",
    "social_regime.",
    "weather.",
    "sentiment.",
    "nlp.",
    "availability.",
    "meta_label.",
    "bocpd_",
    TS_FOUNDATION_CHRONOS_PREFIX,
    GRAPH_RELATIONAL_PREFIX,
)
LOG = get_logger("engine.strategy.feature_registry")
_WARNED_NONFATAL_KEYS: set[str] = set()


def validate_lexical_sentiment_deprecation_marker(value: str | None = None) -> bool:
    marker = str(LEXICAL_SENTIMENT_DEPRECATED_AFTER if value is None else value).strip()
    ok = (
        bool(marker)
        and _LEXICAL_SENTIMENT_PLACEHOLDER_RE.fullmatch(marker) is None
        and _LEXICAL_SENTIMENT_DEPRECATED_AFTER_RE.fullmatch(marker) is not None
    )
    if not ok:
        getattr(LOG, "warning")("startup_warning lexical sentiment deprecation marker is invalid marker=%s", marker)
        log_failure(
            LOG,
            event="feature_registry_lexical_sentiment_marker_invalid",
            code="FEATURE_REGISTRY_LEXICAL_SENTIMENT_MARKER_INVALID",
            message="lexical sentiment deprecation marker is invalid",
            level=30,
            component="engine.strategy.feature_registry",
            extra={"marker": marker},
            persist=False,
        )
    return bool(ok)


validate_lexical_sentiment_deprecation_marker()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.strategy.feature_registry",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _load_discovered_feature_records(stage: str | None = None) -> List[Any]:
    try:
        from engine.strategy.discovery.registry import list_registered_features

        return list(list_registered_features(stage=stage, limit=5000) or [])
    except Exception:
        return []


def _discovered_feature_ids(*, stage: str | None = None) -> List[str]:
    out: List[str] = []
    seen = set()
    for record in _load_discovered_feature_records(stage=stage):
        fid = str(getattr(record, "feature_id", "") or "").strip()
        if not fid or fid in seen:
            continue
        seen.add(fid)
        out.append(fid)
    return out


def feature_stage(feature_id: str) -> str | None:
    """Return ``live`` or ``shadow`` for a registered feature id."""

    fid = str(feature_id or "").strip()
    if not fid:
        return None
    if fid in FEATURE_STAGES:
        return str(FEATURE_STAGES[fid])
    for record in _load_discovered_feature_records(stage=None):
        if str(getattr(record, "feature_id", "") or "").strip() == fid:
            return str(getattr(record, "stage", "") or FEATURE_STAGE_SHADOW)
    return None


def shadow_feature_ids(feature_ids: List[str] | tuple[str, ...] | None) -> List[str]:
    """Return feature ids that are registered for shadow use only."""

    out: List[str] = []
    seen = set()
    for fid in _parse_feature_ids(feature_ids):
        if fid in seen:
            continue
        if feature_stage(str(fid)) == FEATURE_STAGE_SHADOW:
            seen.add(fid)
            out.append(str(fid))
    return out


def assert_no_shadow_features(
    feature_ids: List[str] | tuple[str, ...] | None,
    *,
    context: str = "live_model_serving",
    model_name: str = "",
) -> None:
    """Raise when a live-sensitive path tries to use shadow-only features."""

    shadow_ids = shadow_feature_ids(feature_ids)
    if not shadow_ids:
        return
    prefix = str(context or "live_model_serving").strip() or "live_model_serving"
    model_part = f":{str(model_name).strip()}" if str(model_name or "").strip() else ""
    raise ValueError(f"{prefix}_shadow_features_forbidden{model_part}:{','.join(shadow_ids)}")


def _feature_uses_symbol_snapshot(fid: str) -> bool:
    text = str(fid or "").strip()
    if not text:
        return False
    return text.startswith(_SNAPSHOT_PREFIXES)


def _feature_uses_tsfresh_snapshot(fid: str) -> bool:
    text = str(fid or "").strip()
    if not text:
        return False
    return text.startswith(TSFRESH_FEATURE_PREFIX)


def _feature_uses_symbolic_snapshot(fid: str) -> bool:
    text = str(fid or "").strip()
    if not text:
        return False
    return text.startswith("symbolic.")


def _feature_uses_discovered_snapshot(fid: str) -> bool:
    text = str(fid or "").strip()
    if not text:
        return False
    return text.startswith("discovered.")


def _is_registered_symbolic_feature(fid: str) -> bool:
    text = str(fid or "").strip()
    if not _feature_uses_symbolic_snapshot(text):
        return False
    try:
        from engine.research.symbolic_alpha_generator import load_symbolic_feature_definition

        return isinstance(load_symbolic_feature_definition(text), dict)
    except Exception as e:
        _warn_nonfatal(
            "FEATURE_REGISTRY_SYMBOLIC_DEFINITION_LOAD_FAILED",
            e,
            once_key="load_symbolic_feature_definition",
            feature_id=str(text),
        )
        return False


def _factor_feature_has_passing_evidence(fid: str) -> bool:
    text = str(fid or "").strip()
    if not text.startswith("factor."):
        return False
    try:
        from engine.strategy.promotion_audit import latest_feature_statistical_evidence_decision

        evidence = latest_feature_statistical_evidence_decision(feature_id=str(text), q_threshold=0.10)
        return bool(evidence.get("passed"))
    except Exception as e:
        _warn_nonfatal(
            "FEATURE_REGISTRY_FACTOR_EVIDENCE_CHECK_FAILED",
            e,
            once_key=f"factor_evidence:{text}",
            feature_id=str(text),
        )
        return False


def _load_symbol_snapshot(symbol: str, ts_ms: int, feature_ids: Optional[List[str]] = None) -> Dict[str, float]:
    try:
        from engine.strategy.model_feature_snapshots import (
            FEATURE_SET_TAG as _SNAP_TAG,
            build_model_feature_snapshot,
            load_model_feature_snapshot,
            store_model_feature_snapshots,
        )
        requested_ids = [
            str(fid)
            for fid in list(feature_ids or [])
            if str(fid or "").strip() and _feature_uses_symbol_snapshot(str(fid))
        ]
        feature_set_tag = str(feature_set_tag_from_ids(requested_ids) if requested_ids else _SNAP_TAG)
        snap = load_model_feature_snapshot(
            symbol=str(symbol),
            ts_ms=int(ts_ms),
            feature_set_tag=str(feature_set_tag),
            exact=True,
        )
        if not isinstance(snap, dict):
            snap = build_model_feature_snapshot(
                symbol=str(symbol),
                ts_ms=int(ts_ms),
                feature_ids=(list(requested_ids) if requested_ids else None),
            )
            if isinstance(snap, dict) and snap:
                try:
                    store_model_feature_snapshots([snap])
                except Exception as e:
                    _warn_nonfatal(
                        "FEATURE_REGISTRY_SNAPSHOT_STORE_FAILED",
                        e,
                        once_key="store_model_feature_snapshots",
                        symbol=str(symbol),
                        ts_ms=int(ts_ms),
                    )
        features = dict((snap or {}).get("features") or {})
        return {str(k): float(v or 0.0) for k, v in features.items()}
    except Exception as e:
        _warn_nonfatal(
            "FEATURE_REGISTRY_SYMBOL_SNAPSHOT_LOAD_FAILED",
            e,
            once_key="load_symbol_snapshot",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return {}


def _load_tsfresh(symbol: str, ts_ms: int) -> Dict[str, float]:
    try:
        return resolve_tsfresh_features(symbol=str(symbol), ts_ms=int(ts_ms)) or {}
    except Exception as e:
        _warn_nonfatal(
            "FEATURE_REGISTRY_TSFRESH_LOAD_FAILED",
            e,
            once_key="load_tsfresh",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return {}


def default_feature_ids() -> List[str]:
    out = list(BASE_FEATURE_IDS)
    if USE_SYMBOL_SNAPSHOT_FEATURES:
        out.extend(UNIFIED_SYMBOL_FEATURE_IDS)
    else:
        if USE_MACRO_FEATURES:
            out.extend(MACRO_FEATURE_IDS)
        if USE_WEATHER_FEATURES:
            out.extend(WEATHER_FEATURE_IDS)
        if USE_OPTIONS_FEATURES:
            out.extend(OPTIONS_FEATURE_IDS)
        if USE_INSIDER_FEATURES:
            out.extend(INSIDER_FEATURE_IDS)
        if USE_SHORT_FEATURES:
            out.extend(SHORT_FEATURE_IDS)
        if USE_FUNDING_FEATURES:
            out.extend(CRYPTO_POSITIONING_FEATURE_IDS)
        if USE_DERIBIT_CRYPTO_DERIVATIVES_FEATURES:
            out.extend(DERIBIT_CRYPTO_DERIVATIVES_FEATURE_IDS)
        if USE_NEWS_FLOW_FEATURES:
            out.extend(NEWS_FLOW_FEATURE_IDS)
        if USE_ETF_FLOW_FEATURES:
            out.extend(ETF_FLOW_FEATURE_IDS)
        if USE_COT_FEATURES:
            out.extend(COT_FEATURE_IDS)
        if USE_13F_FEATURES:
            out.extend(INST_13F_FEATURE_IDS)
        if USE_GOV_FEATURES:
            out.extend(GOV_FEATURE_IDS)
        if USE_FUNDAMENTALS_PIT_FEATURES:
            out.extend(FUNDAMENTALS_PIT_FEATURE_IDS)
        if USE_CONGRESSIONAL_TRADE_DATA:
            out.extend(CONGRESSIONAL_FEATURE_IDS)
        if USE_SOCIAL_FEATURES:
            out.extend(SOCIAL_FEATURE_IDS)
        if USE_SOCIAL_REGIME:
            out.extend(SOCIAL_REGIME_FEATURE_IDS)
        if FINBERT_FEATURE_IDS:
            out.extend(FINBERT_FEATURE_IDS)
    if USE_TECH_FEATURES:
        out.extend(TECH_FEATURE_IDS)
    if USE_STRESS_FEATURES:
        out.extend(STRESS_FEATURE_IDS)
    if USE_BOCPD_FEATURES:
        out.extend(BOCPD_FEATURE_IDS)
    if USE_TSFRESH_FEATURES:
        out.extend(get_default_tsfresh_feature_ids())
    if USE_FACTOR_UNIVERSE:
        try:
            from engine.runtime.factor_universe import FACTOR_FEATURE_ORDER
            out.extend([f"factor.{fid}" for fid in list(FACTOR_FEATURE_ORDER or [])])
        except Exception as e:
            _warn_nonfatal(
                "FEATURE_REGISTRY_FACTOR_ORDER_FAILED",
                e,
                once_key="default_feature_ids_factor_order",
            )
    out.extend(_discovered_feature_ids(stage=FEATURE_STAGE_LIVE))
    return out


def registered_feature_ids(*, include_shadow: bool = True) -> List[str]:
    out: List[str] = []
    seen = set()
    for ids in FEATURE_GROUPS.values():
        for fid in ids:
            key = str(fid or "").strip()
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(key)
    if USE_FACTOR_UNIVERSE:
        try:
            from engine.runtime.factor_universe import FACTOR_FEATURE_ORDER

            for fid in list(FACTOR_FEATURE_ORDER or []):
                key = f"factor.{str(fid or '').strip()}"
                if not key or key in seen:
                    continue
                seen.add(key)
                out.append(key)
        except Exception as e:
            _warn_nonfatal(
                "FEATURE_REGISTRY_FACTOR_ORDER_FAILED",
                e,
                once_key="registered_feature_ids_factor_order",
            )
    discovered_stage = None if bool(include_shadow) else FEATURE_STAGE_LIVE
    for fid in _discovered_feature_ids(stage=discovered_stage):
        key = str(fid or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _parse_feature_ids(value: Any) -> List[str]:
    if isinstance(value, list):
        raw = value
    elif isinstance(value, tuple):
        raw = list(value)
    elif isinstance(value, (set, frozenset)):
        raw = sorted(value, key=lambda item: str(item or "").strip())
    elif isinstance(value, dict):
        raw = sorted(value.keys(), key=lambda item: str(item or "").strip())
    elif isinstance(value, str):
        raw = [part.strip() for part in value.split(",")]
    else:
        raw = []
    out: List[str] = []
    seen = set()
    for item in raw:
        fid = str(item or "").strip()
        if not fid or fid in seen:
            continue
        seen.add(fid)
        out.append(fid)
    return out


def _feature_env_candidates(model_name: Optional[str]) -> List[str]:
    out: List[str] = []
    name = str(model_name or "").strip().upper()
    if name:
        safe = re.sub(r"[^A-Z0-9]+", "_", name).strip("_")
        if safe:
            out.append(f"MODEL_FEATURE_IDS_{safe}")
    out.append("MODEL_FEATURE_IDS")
    return out


def resolve_feature_ids(
    feature_ids: Optional[List[str]] = None,
    *,
    model_name: Optional[str] = None,
    model_spec: Optional[Dict[str, Any]] = None,
    fallback_to_default: bool = True,
) -> List[str]:
    requested = _parse_feature_ids(feature_ids)
    if not requested and isinstance(model_spec, dict):
        requested = _parse_feature_ids(model_spec.get("feature_ids"))
        if not requested and isinstance(model_spec.get("feature_schema"), dict):
            requested = _parse_feature_ids((model_spec.get("feature_schema") or {}).get("feature_ids"))
    if not requested:
        for env_key in _feature_env_candidates(model_name):
            requested = _parse_feature_ids(os.environ.get(env_key, ""))
            if requested:
                break
    if not requested and fallback_to_default:
        requested = list(default_feature_ids())

    allowed = dict.fromkeys(registered_feature_ids())
    out: List[str] = []
    seen = set()
    for fid in requested:
        if fid in seen:
            continue
        if fid in allowed or _is_registered_symbolic_feature(fid):
            seen.add(fid)
            out.append(fid)
            continue
        if fid.startswith("factor."):
            if _factor_feature_has_passing_evidence(fid):
                seen.add(fid)
                out.append(fid)
            else:
                _warn_nonfatal(
                    "FEATURE_REGISTRY_FACTOR_EVIDENCE_MISSING",
                    RuntimeError("factor feature lacks passing statistical evidence"),
                    once_key=f"factor_evidence_missing:{fid}",
                    feature_id=str(fid),
                )

    if out or not fallback_to_default:
        return out
    return list(default_feature_ids())


def expected_columns(
    feature_ids: Optional[List[str]] = None,
    *,
    model_name: Optional[str] = None,
    model_spec: Optional[Dict[str, Any]] = None,
    fallback_to_default: bool = True,
) -> List[str]:
    """Return the canonical ordered feature columns for model train/serve."""
    return resolve_feature_ids(
        feature_ids,
        model_name=model_name,
        model_spec=model_spec,
        fallback_to_default=bool(fallback_to_default),
    )


def feature_set_tag_from_ids(feature_ids: List[str]) -> str:
    ids = list(feature_ids or [])
    if ids == BASE_FEATURE_IDS:
        return "base"
    parts = ["base"]
    if any(_feature_uses_symbol_snapshot(fid) for fid in ids):
        parts.append("symbol_snapshot")
    if any(fid.startswith("macro.") for fid in ids):
        parts.append("macro")
    if any(fid.startswith("tech.") for fid in ids):
        parts.append("tech")
    if any(fid.startswith("stress.") for fid in ids):
        parts.append("stress")
    if any(fid.startswith("weather.") for fid in ids):
        parts.append("wx")
    if any(fid.startswith("options_symbol.") for fid in ids):
        parts.append("options")
    if any(fid.startswith("insider.") or fid.startswith("insider_") for fid in ids):
        parts.append("insider")
    if any(fid in SHORT_FEATURE_IDS or fid.startswith(("short_", "si_", "days_to_cover_")) for fid in ids):
        parts.append("short")
    if any(fid in CRYPTO_POSITIONING_FEATURE_IDS or fid.startswith(("funding_", "perp_", "basis_")) for fid in ids):
        parts.append("crypto_positioning")
    if any(fid.startswith(DERIBIT_FEATURE_PREFIX) for fid in ids):
        parts.append("deribit_crypto_derivatives_v1_shadow")
    if any(fid.startswith(SPORTSBOOK_ODDS_FEATURE_PREFIX) for fid in ids):
        parts.append("sports_odds_sector_v1_shadow")
    if any(fid in NEWS_FLOW_FEATURE_IDS or fid.startswith(("news_", "fresh_neg_news_")) for fid in ids):
        parts.append("news_flow")
    if any(fid.startswith("structured_doc_events_v1.") for fid in ids):
        parts.append("structured_doc_events_v1_shadow")
    if any(fid.startswith(PREDICTION_MARKET_MACRO_PREFIX) for fid in ids):
        parts.append("prediction_market_macro_v1_shadow")
    if any(fid.startswith(PREDICTION_MARKET_EVENT_PREFIX) for fid in ids):
        parts.append("prediction_market_event_v1_shadow")
    if any(fid in ETF_FLOW_FEATURE_IDS or fid.startswith("etf_") for fid in ids):
        parts.append("etf_flow")
    if any(fid in COT_FEATURE_IDS or fid.startswith("cot_") for fid in ids):
        parts.append("cot")
    if any(fid in INST_13F_FEATURE_IDS or fid.startswith("13f_") for fid in ids):
        parts.append("inst_13f")
    if any(fid in GOV_FEATURE_IDS or fid.startswith(("congress_", "lobbying_", "gov_")) for fid in ids):
        parts.append("gov")
    if any(fid in FUNDAMENTALS_PIT_FEATURE_IDS or fid.startswith("fund_") for fid in ids):
        parts.append("fundamentals")
    if any(fid.startswith("congressional.") for fid in ids):
        parts.append("congressional")
    if any(fid.startswith("social.") for fid in ids):
        parts.append("social")
    if any(fid.startswith("social_regime.") for fid in ids):
        parts.append("social_regime")
    if any(fid.startswith("hmm_regime.") for fid in ids):
        parts.append("hmm_regime")
    if any(fid.startswith("sentiment.finbert.") for fid in ids):
        parts.append("finbert")
    if any(fid.startswith("nlp.") for fid in ids):
        parts.append("nlp_v1")
    if any(fid.startswith(TS_FOUNDATION_CHRONOS_PREFIX) for fid in ids):
        parts.append("tsfm_chronos_v2_shadow")
    if any(fid.startswith(GRAPH_RELATIONAL_PREFIX) for fid in ids):
        parts.append("graph_relational_v1_shadow")
    if any(fid.startswith(TSFRESH_FEATURE_PREFIX) for fid in ids):
        parts.append("tsfresh")
    if any(fid.startswith("factor.") for fid in ids):
        parts.append("factors")
    if any(fid.startswith("symbolic.") for fid in ids):
        parts.append("symbolic")
    if any(fid.startswith("discovered.llm.") for fid in ids):
        parts.append("discovered_llm")
    if any(fid.startswith("discovered.") for fid in ids):
        parts.append("discovery")
    return "+".join(parts)


def _source_credibility(source: str) -> float:
    s = str(source or "").lower()
    for k, v in _SOURCE_CRED.items():
        if k in s:
            return float(v)
    return 0.5


def _session_flags(ts_ms: int):
    h = time.gmtime(ts_ms / 1000).tm_hour
    return (
        1.0 if 0 <= h < 7 else 0.0,
        1.0 if 7 <= h < 13 else 0.0,
        1.0 if 13 <= h < 22 else 0.0,
    )


def _build_context(*, event: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    ts_ms = int(event.get("ts_ms", 0) or 0)
    ref_ts_ms = int(event.get("ref_ts_ms", ts_ms) or ts_ms or 0)
    title = str(event.get("title", "") or "")
    body = str(event.get("body", "") or "")
    source = str(event.get("source", "") or "")
    asia, eu, us = _session_flags(ts_ms)
    asset_class = asset_class_for_symbol(symbol)
    ctx: Dict[str, Any] = {
        "ts_ms": ts_ms,
        "ref_ts_ms": ref_ts_ms,
        "title": title,
        "body": body,
        "source": source,
        "asia": asia,
        "eu": eu,
        "us": us,
        "asset_match": 1.0 if asset_class and asset_class != "UNKNOWN" else 0.0,
    }
    return ctx


def _load_tech(symbol: str, ts_ms: int) -> Dict[str, Any]:
    try:
        from engine.strategy.tech_indicators import compute_tech_features
        return compute_tech_features(str(symbol), int(ts_ms)) or {}
    except Exception as e:
        _warn_nonfatal(
            "FEATURE_REGISTRY_TECH_LOAD_FAILED",
            e,
            once_key="load_tech",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return {}


def _load_stress(ts_ms: int) -> Dict[str, Any]:
    try:
        from engine.strategy.market_stress import get_market_stress_snapshot
        return get_market_stress_snapshot(ts_ms=int(ts_ms)) or {}
    except Exception as e:
        _warn_nonfatal(
            "FEATURE_REGISTRY_STRESS_LOAD_FAILED",
            e,
            once_key="load_stress",
            ts_ms=int(ts_ms),
        )
        return {}


def _load_macro(ts_ms: int) -> Dict[str, float]:
    try:
        from engine.data.factor_ingestion import macro_feature_row_asof
        from engine.runtime.storage import connect

        con = connect()
        try:
            out: Dict[str, float] = {}
            for fid in MACRO_FEATURE_IDS:
                value, _asof_ts, _effective_ts = macro_feature_row_asof(con, feature_id=str(fid), ts_ms=int(ts_ms))
                out[str(fid)] = float(value or 0.0)
            return out
        finally:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal(
                    "FEATURE_REGISTRY_MACRO_CLOSE_FAILED",
                    e,
                    once_key="load_macro_close",
                    ts_ms=int(ts_ms),
                )
    except Exception as e:
        _warn_nonfatal(
            "FEATURE_REGISTRY_MACRO_LOAD_FAILED",
            e,
            once_key="load_macro",
            ts_ms=int(ts_ms),
        )
        return {}


def _load_social(symbol: str, ts_ms: int) -> Dict[str, Any]:
    try:
        from engine.strategy.social_context import get_social_feature_vector
        return get_social_feature_vector(symbol=str(symbol), ts_ms=int(ts_ms)) or {}
    except Exception as e:
        _warn_nonfatal(
            "FEATURE_REGISTRY_SOCIAL_LOAD_FAILED",
            e,
            once_key="load_social",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return {}


def _load_weather(symbol: str, ts_ms: int) -> Dict[str, Any]:
    try:
        from engine.data.weather_features import get_weather_feature_snapshot
        return get_weather_feature_snapshot(symbol=str(symbol), ts_ms=int(ts_ms)) or {}
    except Exception as e:
        _warn_nonfatal(
            "FEATURE_REGISTRY_WEATHER_LOAD_FAILED",
            e,
            once_key="load_weather",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return {}


def _load_options(symbol: str, ts_ms: int) -> Dict[str, Any]:
    try:
        from engine.strategy.options_context import get_options_feature_vector
        return get_options_feature_vector(symbol=str(symbol), ts_ms=int(ts_ms)) or {}
    except Exception as e:
        _warn_nonfatal(
            "FEATURE_REGISTRY_OPTIONS_LOAD_FAILED",
            e,
            once_key="load_options",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return {}


def _load_social_regime(symbol: str, ts_ms: int) -> Dict[str, Any]:
    try:
        from engine.strategy.social_regime import get_social_regime_vector
        return get_social_regime_vector(symbol=str(symbol), ts_ms=int(ts_ms)) or {}
    except Exception as e:
        _warn_nonfatal(
            "FEATURE_REGISTRY_SOCIAL_REGIME_LOAD_FAILED",
            e,
            once_key="load_social_regime",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return {}


def _load_hmm_regime(symbol: str, ts_ms: int) -> Dict[str, float]:
    try:
        from engine.strategy.hmm_regime import build_hmm_feature_map, resolve_hmm_regime_snapshot

        return build_hmm_feature_map(
            resolve_hmm_regime_snapshot(symbol=str(symbol), ts_ms=int(ts_ms))
        )
    except Exception as e:
        _warn_nonfatal(
            "FEATURE_REGISTRY_HMM_REGIME_LOAD_FAILED",
            e,
            once_key="load_hmm_regime",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return {}


def _load_bocpd_regime(symbol: str, ts_ms: int) -> Dict[str, float]:
    try:
        from engine.runtime.storage import connect
        from engine.strategy.bocpd import feature_map_from_summary, load_latest_summary

        con = connect()
        try:
            summary = load_latest_summary(
                con,
                symbol=str(symbol),
                series_type="realized_vol",
                as_of_ts_ms=int(ts_ms),
            )
            if not summary:
                summary = load_latest_summary(
                    con,
                    symbol="*",
                    series_type="portfolio_correlation",
                    as_of_ts_ms=int(ts_ms),
                )
            return feature_map_from_summary(summary)
        finally:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal(
                    "FEATURE_REGISTRY_BOCPD_REGIME_CLOSE_FAILED",
                    e,
                    once_key="load_bocpd_regime_close",
                    symbol=str(symbol),
                    ts_ms=int(ts_ms),
                )
    except Exception as e:
        _warn_nonfatal(
            "FEATURE_REGISTRY_BOCPD_REGIME_LOAD_FAILED",
            e,
            once_key="load_bocpd_regime",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return {}


def _day_start_ms(ts_ms: int) -> int:
    return int(int(ts_ms or 0) // 86_400_000) * 86_400_000


def _vector_from_blob(payload: Any, dim: Any) -> Any:
    import numpy as np

    raw = payload.tobytes() if isinstance(payload, memoryview) else bytes(payload or b"")
    arr = np.frombuffer(raw, dtype=np.float32)
    if int(dim or 0) > 0:
        arr = arr[: int(dim)]
    if arr.size < NLP_EMBEDDING_DIM:
        arr = np.pad(arr, (0, NLP_EMBEDDING_DIM - int(arr.size)))
    return arr[:NLP_EMBEDDING_DIM].astype(np.float32)


def _weights_for_ts(ts_values: List[int], *, half_life_hours: float = 36.0) -> Any:
    import numpy as np

    if not ts_values:
        return np.zeros((0,), dtype=np.float64)
    anchor = max(int(ts) for ts in ts_values)
    half_life_ms = max(1.0, float(half_life_hours) * 3_600_000.0)
    weights = np.asarray(
        [0.5 ** max(0.0, (float(anchor) - float(ts)) / half_life_ms) for ts in ts_values],
        dtype=np.float64,
    )
    total = float(weights.sum())
    if total <= 0.0 or not math.isfinite(total):
        return np.ones((len(ts_values),), dtype=np.float64) / max(1, len(ts_values))
    return weights / total


def _load_nlp_cached_features(symbol: str, ts_ms: int, feature_ids: List[str]) -> Dict[str, float]:
    requested = set(str(fid or "") for fid in list(feature_ids or []))
    if not requested or not any(fid.startswith("nlp.") for fid in requested):
        return {}
    symbol_key = str(symbol or "").upper().strip()
    if not symbol_key or int(ts_ms or 0) <= 0:
        return {}

    import numpy as np

    day_start = _day_start_ms(int(ts_ms))
    day_end = int(day_start + 86_400_000)
    out: Dict[str, float] = {}
    con = None
    try:
        from engine.runtime.storage import connect

        con = connect(readonly=True)

        if any(fid in requested for fid in NLP_FINBERT_NEWS_FEATURE_IDS):
            rows = con.execute(
                """
                SELECT b.ts, e.dim, e.vector, s.score
                FROM nlp_text_blobs b
                JOIN nlp_embeddings e ON e.hash = b.hash
                LEFT JOIN nlp_sentiments s ON s.hash = b.hash AND s.model_name = e.model_name
                WHERE b.symbol = ?
                  AND b.source = 'news'
                  AND b.ts >= ?
                  AND b.ts < ?
                  AND b.ts <= ?
                  AND e.model_name = ?
                """,
                (symbol_key, int(day_start), int(day_end), int(ts_ms), NLP_FINBERT_MODEL_NAME),
            ).fetchall()
            if rows:
                ts_values = [int(row[0] or 0) for row in rows]
                weights = _weights_for_ts(ts_values)
                probs = np.vstack([_vector_from_blob(row[2], row[1])[:3] for row in rows]).astype(np.float64)
                scores = np.asarray(
                    [
                        float(row[3]) if row[3] is not None else float(prob[0] - prob[1])
                        for row, prob in zip(rows, probs)
                    ],
                    dtype=np.float64,
                )
                out["nlp.finbert_news_v1.score_mean"] = float(scores.mean())
                out["nlp.finbert_news_v1.score_weighted_mean"] = float(np.dot(scores, weights))
                out["nlp.finbert_news_v1.score_max"] = float(scores.max())
                out["nlp.finbert_news_v1.article_count"] = float(len(rows))
                out["nlp.finbert_news_v1.positive_mean"] = float(probs[:, 0].mean())
                out["nlp.finbert_news_v1.negative_mean"] = float(probs[:, 1].mean())
                out["nlp.finbert_news_v1.neutral_mean"] = float(probs[:, 2].mean())

        if any(fid in requested for fid in NLP_FILINGS_FEATURE_IDS):
            rows = con.execute(
                """
                SELECT b.ts, e.dim, e.vector
                FROM nlp_text_blobs b
                JOIN nlp_embeddings e ON e.hash = b.hash
                WHERE b.symbol = ?
                  AND b.source = 'filing'
                  AND b.ts >= ?
                  AND b.ts < ?
                  AND b.ts <= ?
                  AND e.model_name = ?
                """,
                (symbol_key, int(day_start), int(day_end), int(ts_ms), NLP_SENTENCE_MODEL_NAME),
            ).fetchall()
            if rows:
                matrix = np.vstack([_vector_from_blob(row[2], row[1]) for row in rows]).astype(np.float32)
                mean_vec = matrix.mean(axis=0)
                max_vec = matrix.max(axis=0)
                for idx, value in enumerate(mean_vec[:NLP_EMBEDDING_DIM]):
                    out[f"nlp.filings_v1.embedding_mean_{idx:03d}"] = float(value)
                for idx, value in enumerate(max_vec[:NLP_EMBEDDING_DIM]):
                    out[f"nlp.filings_v1.embedding_max_{idx:03d}"] = float(value)
                out["nlp.filings_v1.paragraph_count"] = float(len(rows))

        if any(fid in requested for fid in NLP_TRANSCRIPTS_FEATURE_IDS):
            rows = con.execute(
                """
                SELECT b.ts, e.dim, e.vector
                FROM nlp_text_blobs b
                JOIN nlp_embeddings e ON e.hash = b.hash
                WHERE b.symbol = ?
                  AND b.source = 'transcript'
                  AND b.ts >= ?
                  AND b.ts < ?
                  AND b.ts <= ?
                  AND e.model_name = ?
                """,
                (symbol_key, int(day_start), int(day_end), int(ts_ms), NLP_SENTENCE_MODEL_NAME),
            ).fetchall()
            if rows:
                matrix = np.vstack([_vector_from_blob(row[2], row[1]) for row in rows]).astype(np.float32)
                mean_vec = matrix.mean(axis=0)
                max_vec = matrix.max(axis=0)
                for idx, value in enumerate(mean_vec[:NLP_EMBEDDING_DIM]):
                    out[f"nlp.transcripts_v1.embedding_mean_{idx:03d}"] = float(value)
                for idx, value in enumerate(max_vec[:NLP_EMBEDDING_DIM]):
                    out[f"nlp.transcripts_v1.embedding_max_{idx:03d}"] = float(value)
                out["nlp.transcripts_v1.section_count"] = float(len(rows))

            qa_rows = con.execute(
                """
                SELECT b.ts, s.score
                FROM nlp_text_blobs b
                JOIN nlp_sentiments s ON s.hash = b.hash
                WHERE b.symbol = ?
                  AND b.source = 'transcript_qa'
                  AND b.ts >= ?
                  AND b.ts < ?
                  AND b.ts <= ?
                  AND s.model_name = ?
                """,
                (symbol_key, int(day_start), int(day_end), int(ts_ms), NLP_FINBERT_MODEL_NAME),
            ).fetchall()
            if qa_rows:
                scores = np.asarray([float(row[1] or 0.0) for row in qa_rows], dtype=np.float64)
                weights = _weights_for_ts([int(row[0] or 0) for row in qa_rows])
                out["nlp.transcripts_v1.qa_score_mean"] = float(scores.mean())
                out["nlp.transcripts_v1.qa_score_weighted_mean"] = float(np.dot(scores, weights))
                out["nlp.transcripts_v1.qa_score_max"] = float(scores.max())
                out["nlp.transcripts_v1.qa_section_count"] = float(len(qa_rows))
    except Exception as e:
        _warn_nonfatal(
            "FEATURE_REGISTRY_NLP_CACHE_LOAD_FAILED",
            e,
            once_key=f"nlp_cache_load:{symbol_key}",
            symbol=symbol_key,
            ts_ms=int(ts_ms or 0),
        )
        return {}
    finally:
        if con is not None:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal(
                    "FEATURE_REGISTRY_NLP_CACHE_CLOSE_FAILED",
                    e,
                    once_key="nlp_cache_close",
                )
    return {str(k): float(v or 0.0) for k, v in out.items()}


def _load_factors(ts_ms: int) -> Dict[str, float]:
    try:
        from engine.runtime.factor_universe import FACTOR_FEATURE_ORDER, get_factor_universe_vector
        vec = get_factor_universe_vector(ts_ms=int(ts_ms)) or []
        order = list(FACTOR_FEATURE_ORDER or [])
        if len(vec) != len(order):
            return {}
        return {f"factor.{fid}": float(vec[i] or 0.0) for i, fid in enumerate(order)}
    except Exception as e:
        _warn_nonfatal(
            "FEATURE_REGISTRY_FACTOR_VECTOR_LOAD_FAILED",
            e,
            once_key="load_factors",
            ts_ms=int(ts_ms),
        )
        return {}


def _load_discovered_feature_definition(fid: str) -> Dict[str, Any] | None:
    target = str(fid or "").strip()
    if not target:
        return None
    for record in _load_discovered_feature_records(stage=None):
        if str(getattr(record, "feature_id", "") or "").strip() != target:
            continue
        return {
            "feature_id": str(getattr(record, "feature_id", "") or ""),
            "stage": str(getattr(record, "stage", "") or FEATURE_STAGE_SHADOW),
            "source": str(getattr(record, "source", "") or ""),
            "expression": str(getattr(record, "expression", "") or ""),
            "params": dict(getattr(record, "params", {}) or {}),
            "hash": str(getattr(record, "hash", "") or ""),
            "created_ts": int(getattr(record, "created_ts", 0) or 0),
        }
    return None


def _evaluate_discovered_feature(fid: str, *, event: Dict[str, Any], symbol: str) -> float:
    definition = _load_discovered_feature_definition(str(fid))
    if not isinstance(definition, dict):
        return 0.0
    source = str(definition.get("source") or "").strip().lower()
    params = dict(definition.get("params") or {})
    decision_ts_ms = int((event or {}).get("ts_ms", 0) or 0)
    created_ts_ms = int(definition.get("created_ts") or 0)
    if source == "llm_factor" and decision_ts_ms > 0 and created_ts_ms > int(decision_ts_ms):
        return 0.0

    if source in {"pysr", "llm_factor"}:
        try:
            import pandas as pd

            from engine.strategy.discovery.pysr_discoverer import evaluate_pysr_expression

            feature_map = dict(params.get("feature_map") or {})
            source_feature_ids = [
                str(feature_id)
                for feature_id in dict(feature_map).values()
                if str(feature_id).strip() and str(feature_id).strip() != str(fid)
            ]
            if not source_feature_ids:
                return 0.0
            source_values = compute_feature_snapshot(
                event=dict(event or {}),
                symbol=str(symbol),
                feature_ids=list(source_feature_ids),
            )
            values = evaluate_pysr_expression(
                str(definition.get("expression") or ""),
                pd.DataFrame([{key: float(source_values.get(key, 0.0) or 0.0) for key in source_feature_ids}]),
                feature_map=feature_map,
            )
            return float(values[0]) if len(values) else 0.0
        except Exception as e:
            _warn_nonfatal(
                "FEATURE_REGISTRY_DISCOVERED_PYSR_EVAL_FAILED",
                e,
                    once_key=f"discovered_expr_eval:{fid}",
                feature_id=str(fid),
            )
            return 0.0

    if source == "tsfresh":
        try:
            feature_column = str(params.get("feature_column") or "")
            calculator = feature_column.split("__", 1)[1] if "__" in feature_column else feature_column
            if "__" in calculator:
                return 0.0
            tsfresh_values = _load_tsfresh(str(symbol), int((event or {}).get("ts_ms", 0) or 0))
            return float((tsfresh_values or {}).get(f"{TSFRESH_FEATURE_PREFIX}{calculator}", 0.0) or 0.0)
        except Exception as e:
            _warn_nonfatal(
                "FEATURE_REGISTRY_DISCOVERED_TSFRESH_EVAL_FAILED",
                e,
                once_key=f"discovered_tsfresh_eval:{fid}",
                feature_id=str(fid),
            )
            return 0.0

    return 0.0


def _schedule_feature_store_write(*, symbol: str, ts_ms: int, snap: Dict[str, float]) -> None:
    if int(ts_ms or 0) <= 0 or not snap:
        return
    try:
        from engine.strategy.feature_store import enqueue_feature_write

        enqueue_feature_write(
            symbol=str(symbol),
            timestamp=int(ts_ms),
            feature_dict=dict(snap or {}),
            version=int(FEATURE_STORE_VERSION),
        )
    except Exception as e:
        _warn_nonfatal(
            "FEATURE_REGISTRY_FEATURE_STORE_SCHEDULE_FAILED",
            e,
            once_key="feature_registry_feature_store_schedule_failed",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )


def _load_feature_store_snapshot(*, symbol: str, ts_ms: int, feature_ids: List[str]) -> Dict[str, float] | None:
    if not FEATURE_STORE_READS_ENABLED or int(ts_ms or 0) <= 0:
        return None
    if _features_require_fresh_resolution(feature_ids):
        return None
    try:
        from engine.strategy.feature_store import get_feature_store

        payload = get_feature_store().get_features_blocking(
            symbol=str(symbol),
            timestamp=int(ts_ms),
            version=int(FEATURE_STORE_VERSION),
        )
    except Exception as e:
        _warn_nonfatal(
            "FEATURE_REGISTRY_FEATURE_STORE_READ_FAILED",
            e,
            once_key="feature_registry_feature_store_read_failed",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
            version=int(FEATURE_STORE_VERSION),
        )
        return None

    feature_map = dict((payload or {}).get("features") or {})
    if not feature_map:
        return None
    if any(fid not in feature_map for fid in list(feature_ids or [])):
        return None
    return {str(fid): float(feature_map.get(fid, 0.0) or 0.0) for fid in list(feature_ids or [])}


def _features_require_fresh_resolution(feature_ids: List[str]) -> bool:
    late_arriving_prefixes = (
        "sentiment.finbert.",
        "structured_doc_events_v1.",
    )
    return any(str(fid or "").startswith(late_arriving_prefixes) for fid in list(feature_ids or []))


def _requires_event_scoped_resolution(*, event: Dict[str, Any], feature_ids: List[str]) -> bool:
    event_id = str((event or {}).get("event_id") or "").strip()
    if not event_id:
        return False
    return any(str(fid or "").startswith("sentiment.finbert.") for fid in list(feature_ids or []))


def compute_feature_snapshot(*, event: Dict[str, Any], symbol: str, feature_ids: Optional[List[str]] = None) -> Dict[str, float]:
    ids = resolve_feature_ids(feature_ids)
    ctx = _build_context(event=event, symbol=str(symbol))
    snap: Dict[str, float] = {}
    tech = stress = macro = social = weather = options = social_regime = hmm_regime = bocpd_regime = tsfresh = factors = finbert = nlp = None
    snapshot = None
    event_scoped_resolution = _requires_event_scoped_resolution(event=dict(event or {}), feature_ids=list(ids))
    snapshot_ids = [
        fid
        for fid in ids
        if _feature_uses_symbol_snapshot(fid)
        and not (event_scoped_resolution and str(fid).startswith("sentiment.finbert."))
    ]
    if int(ctx["ts_ms"] or 0) > 0 and snapshot_ids:
        snapshot = _load_symbol_snapshot(str(symbol), int(ctx["ts_ms"]), feature_ids=list(snapshot_ids))

    for fid in ids:
        if isinstance(snapshot, dict) and fid in snapshot:
            snap[fid] = float(snapshot.get(fid, 0.0) or 0.0)
        elif fid == "base.source_credibility":
            snap[fid] = float(_source_credibility(ctx["source"]))
        elif fid == "base.log_recency_hours":
            age_h = max(0.0, (ctx["ref_ts_ms"] - ctx["ts_ms"]) / 3_600_000)
            snap[fid] = float(math.log1p(age_h) / 6.0)
        elif fid == "base.normalized_text_len":
            n = len(ctx["title"] + " " + ctx["body"])
            snap[fid] = float(min(1.0, n / 1000.0))
        elif fid == "base.scheduled_flag":
            t = ctx["title"].lower()
            snap[fid] = 1.0 if any(k in t for k in ("cpi", "ppi", "fed", "ecb", "boj", "earnings", "gdp", "jobs", "unemployment", "rate")) else 0.0
        elif fid == "base.session_asia":
            snap[fid] = float(ctx["asia"])
        elif fid == "base.session_eu":
            snap[fid] = float(ctx["eu"])
        elif fid == "base.session_us":
            snap[fid] = float(ctx["us"])
        elif fid == "base.asset_class_match":
            snap[fid] = float(ctx["asset_match"])
        elif fid.startswith("macro."):
            if macro is None:
                macro = _load_macro(int(ctx["ts_ms"]))
            snap[fid] = float((macro or {}).get(fid, 0.0) or 0.0)
        elif fid.startswith("tech."):
            if tech is None:
                tech = _load_tech(str(symbol), int(ctx["ts_ms"]))
            snap[fid] = float(tech.get(fid.split(".", 1)[1], 0.0) or 0.0)
        elif fid.startswith("stress."):
            if stress is None:
                stress = _load_stress(int(ctx["ts_ms"]))
            snap[fid] = float(stress.get(fid.split(".", 1)[1], 0.0) or 0.0)
        elif fid.startswith("social."):
            if social is None:
                social = _load_social(str(symbol), int(ctx["ts_ms"]))
            snap[fid] = float(social.get(fid.split(".", 1)[1], 0.0) or 0.0)
        elif fid.startswith("weather."):
            if weather is None:
                weather = _load_weather(str(symbol), int(ctx["ts_ms"]))
            snap[fid] = float(weather.get(fid.split(".", 1)[1], 0.0) or 0.0)
        elif fid.startswith("options_symbol."):
            if options is None:
                options = _load_options(str(symbol), int(ctx["ts_ms"]))
            snap[fid] = float(options.get(fid.split(".", 1)[1], 0.0) or 0.0)
        elif fid.startswith("social_regime."):
            if social_regime is None:
                social_regime = _load_social_regime(str(symbol), int(ctx["ts_ms"]))
            snap[fid] = float(social_regime.get(fid.split(".", 1)[1], 0.0) or 0.0)
        elif fid.startswith("hmm_regime."):
            if hmm_regime is None:
                hmm_regime = _load_hmm_regime(str(symbol), int(ctx["ts_ms"]))
            snap[fid] = float((hmm_regime or {}).get(fid, 0.0) or 0.0)
        elif fid.startswith("bocpd_"):
            if bocpd_regime is None:
                bocpd_regime = _load_bocpd_regime(str(symbol), int(ctx["ts_ms"]))
            snap[fid] = float((bocpd_regime or {}).get(fid, 0.0) or 0.0)
        elif fid.startswith("sentiment.finbert."):
            if finbert is None:
                finbert, _finbert_meta, _ = resolve_finbert_sentiment_snapshot(
                    symbol=str(symbol),
                    ts_ms=int(ctx["ts_ms"]),
                    event=dict(event or {}),
                )
            snap[fid] = float((finbert or {}).get(fid, 0.0) or 0.0)
        elif fid.startswith("nlp."):
            if nlp is None:
                nlp = _load_nlp_cached_features(str(symbol), int(ctx["ts_ms"]), list(ids))
            snap[fid] = float((nlp or {}).get(fid, 0.0) or 0.0)
        elif fid.startswith(TSFRESH_FEATURE_PREFIX):
            if tsfresh is None:
                tsfresh = _load_tsfresh(str(symbol), int(ctx["ts_ms"]))
            snap[fid] = float((tsfresh or {}).get(fid, 0.0) or 0.0)
        elif fid.startswith("factor."):
            if factors is None:
                factors = _load_factors(int(ctx["ts_ms"]))
            snap[fid] = float((factors or {}).get(fid, 0.0) or 0.0)
        elif _feature_uses_symbolic_snapshot(fid):
            try:
                from engine.research.symbolic_alpha_generator import (
                    evaluate_symbolic_expression,
                    load_symbolic_feature_definition,
                )

                definition = load_symbolic_feature_definition(fid)
                if not isinstance(definition, dict):
                    snap[fid] = 0.0
                    continue
                source_feature_ids = [
                    str(source_fid)
                    for source_fid in list(definition.get("source_feature_ids") or [])
                    if str(source_fid).strip() and str(source_fid).strip() != str(fid)
                ]
                source_values = {
                    str(source_fid): float(snap.get(source_fid, 0.0) or 0.0)
                    for source_fid in source_feature_ids
                    if source_fid in snap
                }
                missing = [source_fid for source_fid in source_feature_ids if source_fid not in source_values]
                if missing:
                    missing_snap = compute_feature_snapshot(
                        event=dict(event or {}),
                        symbol=str(symbol),
                        feature_ids=list(missing),
                    )
                    for source_fid in missing:
                        source_values[str(source_fid)] = float((missing_snap or {}).get(source_fid, 0.0) or 0.0)
                snap[fid] = float(
                    evaluate_symbolic_expression(
                        str(definition.get("expression_text") or ""),
                        source_values,
                    )
                    or 0.0
                )
            except Exception as e:
                _warn_nonfatal(
                    "FEATURE_REGISTRY_SYMBOLIC_EVAL_FAILED",
                    e,
                    once_key=f"symbolic_eval:{fid}",
                    feature_id=str(fid),
                    symbol=str(symbol),
                    ts_ms=int(ctx["ts_ms"]),
                )
                snap[fid] = 0.0
        elif _feature_uses_discovered_snapshot(fid):
            snap[fid] = float(
                _evaluate_discovered_feature(
                    str(fid),
                    event=dict(event or {}),
                    symbol=str(symbol),
                )
                or 0.0
            )
        else:
            snap[fid] = 0.0
    return snap


def build_feature_snapshot(*, event: Dict[str, Any], symbol: str, feature_ids: Optional[List[str]] = None) -> Dict[str, float]:
    ids = resolve_feature_ids(feature_ids)
    try:
        ts_ms = int((event or {}).get("ts_ms", 0) or 0)
    except Exception:
        ts_ms = 0
    if not _requires_event_scoped_resolution(event=dict(event or {}), feature_ids=list(ids)):
        snap = _load_feature_store_snapshot(symbol=str(symbol), ts_ms=int(ts_ms), feature_ids=list(ids))
        if snap is not None:
            return snap
    snap = compute_feature_snapshot(event=event, symbol=str(symbol), feature_ids=ids)
    _schedule_feature_store_write(symbol=str(symbol), ts_ms=int(ts_ms), snap=snap)
    return snap
