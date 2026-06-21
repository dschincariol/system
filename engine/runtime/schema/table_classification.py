"""Production table classification for Postgres + TimescaleDB.

This module is intentionally import-only metadata. Migrations, verifier tests,
and future schema reviews should use this as the single source of truth before
shipping a table.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Union


@dataclass(frozen=True)
class Hypertable:
    chunk: str
    compress_after: str | None
    retain: str | None
    segmentby: tuple[str, ...] = ()
    time_column: str = "ts_ms"
    rationale: str = "append-mostly rows primarily read by time range"
    write_rate: str = "medium"
    read_pattern: str = "time-range scans"
    audit: bool = False


@dataclass(frozen=True)
class Regular:
    rationale: str = "mutable state, registry, bounded catalog, or non-time primary lookup"
    write_rate: str = "low"
    read_pattern: str = "primary-key or latest-state lookup"
    cleanup: str | None = None
    audit: bool = False


TableClass = Union[Hypertable, Regular]


def _h(
    *,
    chunk: str,
    compress_after: str | None,
    retain: str | None,
    segmentby: tuple[str, ...] = (),
    time_column: str = "ts_ms",
    rationale: str,
    write_rate: str,
    read_pattern: str,
    audit: bool = False,
) -> Hypertable:
    return Hypertable(
        chunk=chunk,
        compress_after=compress_after,
        retain=retain,
        segmentby=tuple(segmentby),
        time_column=str(time_column),
        rationale=str(rationale),
        write_rate=str(write_rate),
        read_pattern=str(read_pattern),
        audit=bool(audit),
    )


def _r(
    rationale: str,
    *,
    write_rate: str = "low",
    read_pattern: str = "primary-key or latest-state lookup",
    cleanup: str | None = None,
    audit: bool = False,
) -> Regular:
    return Regular(
        rationale=str(rationale),
        write_rate=str(write_rate),
        read_pattern=str(read_pattern),
        cleanup=(None if cleanup is None else str(cleanup)),
        audit=bool(audit),
    )


TICK_QUOTE = _h(
    chunk="1 day",
    compress_after="7 days",
    retain="30 days",
    segmentby=("symbol",),
    rationale="high-rate market data stream; append-mostly and queried by symbol/time windows",
    write_rate="very high",
    read_pattern="latest and intraday ranges by (symbol, time)",
)
BAR_SERIES = _h(
    chunk="1 day",
    compress_after="7 days",
    retain="1 year",
    segmentby=("symbol",),
    rationale="derived bar/price series; append-mostly and read by symbol/time ranges",
    write_rate="high",
    read_pattern="dashboard and model windows by (symbol, time)",
)
FEATURE_SERIES = _h(
    chunk="1 week",
    compress_after="30 days",
    retain="3 years",
    segmentby=("symbol",),
    rationale="point-in-time feature series; append-mostly and replayed by symbol/time",
    write_rate="medium",
    read_pattern="latest feature snapshot and historical replay by (symbol, time)",
)
GLOBAL_FEATURE_SERIES = _h(
    chunk="1 week",
    compress_after="30 days",
    retain="3 years",
    rationale="append-mostly feature/evaluation series keyed primarily by time",
    write_rate="medium",
    read_pattern="time-range replay and dashboard scans",
)
HEALTH_SERIES = _h(
    chunk="1 day",
    compress_after="14 days",
    retain="180 days",
    rationale="operational health metric stream; append-mostly and dashboarded by time",
    write_rate="medium",
    read_pattern="recent operational time windows",
)
AUDIT_SERIES = _h(
    chunk="1 week",
    compress_after="90 days",
    retain=None,
    rationale="forensic audit ledger; append-only and retained indefinitely",
    write_rate="low",
    read_pattern="time-range audit review and actor/entity lookup",
)
EXECUTION_SERIES = _h(
    chunk="1 week",
    compress_after="90 days",
    retain=None,
    segmentby=("symbol",),
    rationale="execution ledger; append-mostly financial evidence retained indefinitely",
    write_rate="medium",
    read_pattern="order, symbol, and time-range execution analysis",
)
DECISION_SERIES = _h(
    chunk="1 week",
    compress_after="30 days",
    retain="3 years",
    segmentby=("symbol",),
    rationale="model decision/prediction stream; append-mostly and replayed by symbol/time",
    write_rate="high",
    read_pattern="decision replay by (symbol, time) and JSON predicates",
)


TABLE_CLASS: dict[str, TableClass] = {}


def _add(names: tuple[str, ...], cls: TableClass) -> None:
    for name in names:
        TABLE_CLASS[str(name)] = cls


_add(
    (
        "price_quotes_raw",
        "price_quotes",
        "prices",
        "price_ticks",
    ),
    TICK_QUOTE,
)
TABLE_CLASS["price_ticks"] = _h(**{**TICK_QUOTE.__dict__, "time_column": "time"})
_add(("price_bars",), BAR_SERIES)
TABLE_CLASS["price_data"] = _h(**{**BAR_SERIES.__dict__, "time_column": "timestamp"})

TABLE_CLASS["market_microstructure_signals"] = FEATURE_SERIES
TABLE_CLASS["price_anomalies"] = FEATURE_SERIES
TABLE_CLASS["market_features"] = FEATURE_SERIES
TABLE_CLASS["model_feature_snapshots"] = _h(
    chunk="1 week",
    compress_after="30 days",
    retain="3 years",
    segmentby=("symbol",),
    rationale="canonical point-in-time feature snapshots; latest lookup is by symbol, feature_set_tag, and time",
    write_rate="high",
    read_pattern="latest snapshot by (symbol, feature_set_tag, ts_ms) and replay windows",
)
TABLE_CLASS["har_rv_forecasts"] = _h(
    chunk="1 week",
    compress_after="30 days",
    retain="3 years",
    segmentby=("symbol",),
    rationale="point-in-time HAR-RV volatility forecasts used by sizing and Monte Carlo risk inputs",
    write_rate="medium",
    read_pattern="latest forecast by (symbol, ts_ms) and walk-forward validation windows",
)
_add(("news_event_features", "options_event_features"), FEATURE_SERIES)
TABLE_CLASS["news_symbol_features"] = _h(**{**FEATURE_SERIES.__dict__, "time_column": "bucket_ts_ms"})
TABLE_CLASS["news_story_embeddings"] = _r(
    "backend-aware per-story news embeddings and novelty scores keyed by event/symbol/model",
    write_rate="medium",
    read_pattern="symbol/backend availability-window novelty comparisons and feature snapshots",
)
TABLE_CLASS["news_flow_features"] = _h(
    chunk="1 week",
    compress_after="30 days",
    retain="3 years",
    segmentby=("symbol",),
    time_column="asof_ts_ms",
    rationale="materialized point-in-time news novelty/staleness feature cache",
    write_rate="medium",
    read_pattern="latest news-flow feature snapshot by symbol/time/backend",
)
TABLE_CLASS["structured_document_events"] = _r(
    "structured extracted events from filings, transcripts, and news keyed by source document and extractor version",
    write_rate="medium",
    read_pattern="symbol availability-window PIT feature snapshots and source-document audits",
)
TABLE_CLASS["options_symbol_features"] = _h(**{**FEATURE_SERIES.__dict__, "time_column": "bucket_ts_ms"})
TABLE_CLASS["social_features"] = _h(**{**FEATURE_SERIES.__dict__, "time_column": "bucket_ts_ms"})
TABLE_CLASS["social_regimes"] = _h(**{**FEATURE_SERIES.__dict__, "time_column": "bucket_ts_ms"})
TABLE_CLASS["social_posts"] = FEATURE_SERIES
TABLE_CLASS["gdelt_macro_features"] = _h(
    chunk="1 week",
    compress_after="30 days",
    retain="3 years",
    time_column="bucket_ts_ms",
    rationale="macro feature buckets; append-mostly and read by historical time windows",
    write_rate="medium",
    read_pattern="macro replay windows by bucket time",
)
TABLE_CLASS["graph_relationship_edges"] = _r(
    "point-in-time graph relationship edge catalog keyed by source/target symbol, relation type, and availability time",
    write_rate="medium",
    read_pattern="PIT graph snapshot construction by source or target symbol, relationship type, and availability time",
    cleanup="source-specific retention; preserve rows needed by retained graph snapshots and promotion evidence",
)
TABLE_CLASS["graph_relational_snapshots"] = _h(
    chunk="1 week",
    compress_after="30 days",
    retain="3 years",
    segmentby=("symbol",),
    rationale="versioned point-in-time graph/relational feature snapshots for shadow-only train/serve parity and promotion evidence",
    write_rate="medium",
    read_pattern="latest graph snapshot by symbol/graph_id/time and historical replay windows",
)
TABLE_CLASS["feature_data"] = _h(
    chunk="1 week",
    compress_after="30 days",
    retain="3 years",
    segmentby=("symbol",),
    time_column="timestamp",
    rationale="Timescale sidecar feature vectors; append-mostly and read by symbol/time",
    write_rate="high",
    read_pattern="feature-store replay by (symbol, timestamp)",
)
TABLE_CLASS["feature_store"] = _h(
    chunk="1 week",
    compress_after="30 days",
    retain="3 years",
    segmentby=("symbol",),
    time_column="time",
    rationale="versioned Timescale feature store; append/update-current bucket by symbol/time/version",
    write_rate="high",
    read_pattern="latest feature vector at or before time",
)
TABLE_CLASS["factor_features"] = _h(**{**GLOBAL_FEATURE_SERIES.__dict__, "time_column": "asof_ts"})
TABLE_CLASS["factor_observations"] = _h(**{**GLOBAL_FEATURE_SERIES.__dict__, "time_column": "asof_ts"})
TABLE_CLASS["factor_group_scores"] = _h(**{**GLOBAL_FEATURE_SERIES.__dict__, "time_column": "ts"})
TABLE_CLASS["model_weather_effect"] = GLOBAL_FEATURE_SERIES
TABLE_CLASS["nlp_text_blobs"] = _r(
    "content-addressed NLP source text cache keyed by hash",
    write_rate="medium",
    read_pattern="primary-key lookup and symbol/time backfill scans",
)
TABLE_CLASS["nlp_embeddings"] = _r(
    "content-addressed embedding cache keyed by text hash and local model name",
    write_rate="medium",
    read_pattern="primary-key lookup by hash/model",
)
TABLE_CLASS["nlp_sentiments"] = _r(
    "content-addressed sentiment cache keyed by text hash and local model name",
    write_rate="medium",
    read_pattern="primary-key lookup by hash/model",
)
TABLE_CLASS["sec_filings"] = FEATURE_SERIES
TABLE_CLASS["insider_transactions"] = _r(
    "low-rate alternative data table upserted by source transaction id",
    write_rate="low",
    read_pattern="source id upsert and symbol/availability-time feature snapshot reads",
)
TABLE_CLASS["congressional_trades"] = _r(
    "low-rate alternative data table upserted by source trade id",
    write_rate="low",
    read_pattern="source id upsert and symbol/time feature snapshot reads",
)
TABLE_CLASS["finra_short_sale_volume"] = _r(
    "daily FINRA short-sale volume rows upserted by source record id",
    write_rate="low",
    read_pattern="source id upsert and symbol/availability-time feature snapshot reads",
)
TABLE_CLASS["finra_short_interest"] = _r(
    "bi-monthly FINRA short-interest rows upserted by source record id",
    write_rate="low",
    read_pattern="source id upsert and symbol/availability-time feature snapshot reads",
)
TABLE_CLASS["finbert_sentiment_enrichments"] = _h(
    chunk="1 week",
    compress_after="30 days",
    retain="3 years",
    segmentby=("symbol",),
    time_column="asof_ts_ms",
    rationale="point-in-time FinBERT sentiment enrichments keyed by symbol/source availability",
    write_rate="medium",
    read_pattern="latest sentiment enrichment by symbol/time/source",
)
TABLE_CLASS["crypto_funding_rates"] = _r(
    "hourly crypto perpetual funding and basis rows upserted by exchange funding event",
    write_rate="low",
    read_pattern="source id upsert and symbol/availability-time feature snapshot reads",
)
TABLE_CLASS["deribit_instruments"] = _r(
    "Deribit public instrument metadata keyed by instrument name",
    write_rate="low",
    read_pattern="instrument upsert and active instrument diagnostics by base asset/type",
)
TABLE_CLASS["deribit_market_snapshots"] = _r(
    "read-only Deribit crypto derivatives market snapshots keyed by source record id",
    write_rate="medium",
    read_pattern="latest base-asset availability-time feature snapshot and provider diagnostics",
)
TABLE_CLASS["deribit_provider_state"] = _r(
    "latest Deribit public market-data readiness payload",
    write_rate="low",
    read_pattern="provider health lookup by source key",
)
TABLE_CLASS["sportsbook_odds_snapshots"] = _r(
    "append-only read-only sportsbook and betting-exchange odds snapshots with no-vig probabilities",
    write_rate="medium",
    read_pattern="latest explicitly mapped asset/category odds before decision timestamp",
)
TABLE_CLASS["sportsbook_odds_asset_mappings"] = _r(
    "strict sports/event category to narrow asset or research-label mapping allowlist",
    write_rate="low",
    read_pattern="mapping lookup by asset and normalized sports category",
)
TABLE_CLASS["sportsbook_odds_event_studies"] = _r(
    "research-only sportsbook odds event-study evidence after latency, fees, and slippage",
    write_rate="low",
    read_pattern="event-study audit lookup by run and timestamp",
)
TABLE_CLASS["sportsbook_odds_promotion_evidence"] = _r(
    "sportsbook odds promotion evidence gated by OOS, net-after-cost, PIT, deconfounding, readiness, and mapping approval checks",
    write_rate="low",
    read_pattern="promotion evidence lookup by symbol, mapping, pass/fail status, and timestamp",
)
TABLE_CLASS["etf_shares_outstanding"] = _r(
    "daily ETF shares-outstanding rows upserted by symbol/as-of source record id",
    write_rate="low",
    read_pattern="source id upsert and symbol/availability-time feature snapshot reads",
)
TABLE_CLASS["etf_flow_features"] = _h(
    chunk="1 week",
    compress_after="30 days",
    retain="3 years",
    segmentby=("symbol",),
    time_column="asof_ts_ms",
    rationale="materialized point-in-time ETF unexpected-flow feature cache",
    write_rate="medium",
    read_pattern="latest ETF flow feature snapshot by symbol/time",
)
TABLE_CLASS["cftc_cot_positions"] = _r(
    "weekly CFTC COT rows upserted by source report/contract id",
    write_rate="low",
    read_pattern="source id upsert and contract/availability-time feature snapshot reads",
)
TABLE_CLASS["cot_contract_symbol_map"] = _r(
    "config table mapping CFTC futures contracts into model symbols and macro topics",
    write_rate="low",
    read_pattern="symbol-to-contract mapping lookups",
)
TABLE_CLASS["cot_symbol_features"] = _h(
    chunk="1 week",
    compress_after="30 days",
    retain="3 years",
    segmentby=("symbol",),
    time_column="asof_ts_ms",
    rationale="materialized point-in-time COT positioning feature cache",
    write_rate="low",
    read_pattern="latest COT feature snapshot by symbol/time",
)
TABLE_CLASS["inst_13f_manager_universe"] = _r(
    "configured 13F manager universe with active flags and turnover thresholds",
    write_rate="low",
    read_pattern="manager configuration lookup",
)
TABLE_CLASS["inst_13f_filings"] = _r(
    "quarterly SEC 13F filing metadata keyed by manager/accession and EDGAR acceptance time",
    write_rate="low",
    read_pattern="manager/latest filing lookup by availability time",
)
TABLE_CLASS["inst_13f_holdings"] = _r(
    "raw 13F information-table holdings keyed by manager/accession/CUSIP row",
    write_rate="low",
    read_pattern="symbol and manager/report feature snapshot reads",
)
TABLE_CLASS["inst_13f_cusip_symbol_map"] = _r(
    "13F CUSIP-to-symbol mapping cache and manual review table",
    write_rate="low",
    read_pattern="CUSIP mapping lookups",
)
TABLE_CLASS["inst_13f_symbol_features"] = _h(
    chunk="1 week",
    compress_after="30 days",
    retain="3 years",
    segmentby=("symbol",),
    time_column="asof_ts_ms",
    rationale="materialized point-in-time 13F low-turnover manager overlay cache",
    write_rate="low",
    read_pattern="latest 13F overlay feature snapshot by symbol/time",
)
TABLE_CLASS["quiver_congressional_trades"] = _r(
    "Quiver congressional trade disclosures keyed by source record id and disclosure availability time",
    write_rate="low",
    read_pattern="source id upsert, dedupe-key lookup, and symbol/disclosure-time feature snapshot reads",
)
TABLE_CLASS["quiver_lobbying_filings"] = _r(
    "Quiver lobbying spend disclosures keyed by source record id",
    write_rate="low",
    read_pattern="source id upsert and symbol/sector availability-time feature snapshot reads",
)
TABLE_CLASS["quiver_gov_contracts"] = _r(
    "Quiver government contract award disclosures keyed by source record id",
    write_rate="low",
    read_pattern="source id upsert and symbol/sector availability-time feature snapshot reads",
)
TABLE_CLASS["gov_member_committee_map"] = _r(
    "static congressional member-to-committee conditioning map",
    write_rate="low",
    read_pattern="member committee lookup for gov feature conditioning",
)
TABLE_CLASS["gov_committee_sector_map"] = _r(
    "static committee-to-sector conditioning map",
    write_rate="low",
    read_pattern="committee sector lookup for gov feature conditioning",
)
TABLE_CLASS["gov_member_leadership_map"] = _r(
    "static congressional leadership member map",
    write_rate="low",
    read_pattern="member leadership lookup for gov feature conditioning",
)
TABLE_CLASS["gov_symbol_sector_map"] = _r(
    "symbol-to-sector map for government-flow feature conditioning",
    write_rate="low",
    read_pattern="symbol sector lookup",
)
TABLE_CLASS["gov_symbol_features"] = _h(
    chunk="1 week",
    compress_after="30 days",
    retain="3 years",
    segmentby=("symbol",),
    time_column="asof_ts_ms",
    rationale="materialized point-in-time Quiver government-flow feature cache",
    write_rate="low",
    read_pattern="latest gov feature snapshot by symbol/time",
)
TABLE_CLASS["fundamentals_pit"] = _r(
    "immutable point-in-time fundamentals vendor metric publications keyed by source record id",
    write_rate="low",
    read_pattern="source id upsert and symbol/metric publish-time feature snapshot reads",
)
TABLE_CLASS["fundamentals_pit_backfill_state"] = _r(
    "resumable PIT fundamentals bulk-load cursors by vendor",
    write_rate="low",
    read_pattern="vendor/state-key backfill cursor lookup",
)
TABLE_CLASS["fundamentals_pit_symbol_features"] = _h(
    chunk="1 week",
    compress_after="30 days",
    retain="3 years",
    segmentby=("symbol",),
    time_column="asof_ts_ms",
    rationale="materialized point-in-time fundamentals feature cache",
    write_rate="low",
    read_pattern="latest fundamentals feature snapshot by symbol/time",
)
TABLE_CLASS["macro_series_vintages"] = _r(
    "ALFRED/FRED macro observations keyed by series, observation date, and vintage date",
    write_rate="low",
    read_pattern="series vintage upserts and point-in-time macro feature materialization",
)
TABLE_CLASS["macro_vintage_backfill_state"] = _r(
    "resumable state for one-time macro vintage backfills",
    write_rate="low",
    read_pattern="primary-key lookup by macro series id",
)
TABLE_CLASS["prediction_market_events"] = _r(
    "provider-neutral prediction-market event metadata keyed by provider and event id",
    write_rate="medium",
    read_pattern="macro event availability and resolution-window feature filtering",
)
TABLE_CLASS["prediction_market_markets"] = _r(
    "provider-neutral prediction-market market metadata and implied probabilities",
    write_rate="medium",
    read_pattern="latest macro expectation by provider, event, market, and availability timestamp",
)
TABLE_CLASS["prediction_market_orderbook_snapshots"] = _r(
    "append-only read-only prediction-market order-book snapshots",
    write_rate="medium",
    read_pattern="latest provider market order-book snapshot before decision timestamp",
)
TABLE_CLASS["prediction_market_price_history"] = _r(
    "read-only prediction-market trade and price history when available",
    write_rate="medium",
    read_pattern="provider market replay by trade and availability timestamp",
)
TABLE_CLASS["prediction_market_backfill_state"] = _r(
    "resumable prediction-market backfill and replay cursors",
    write_rate="low",
    read_pattern="provider/state-key backfill cursor lookup",
)
TABLE_CLASS["weather_alerts"] = _h(**{**GLOBAL_FEATURE_SERIES.__dict__, "time_column": "issued_ts"})
TABLE_CLASS["weather_forecast_region_daily"] = _h(**{**GLOBAL_FEATURE_SERIES.__dict__, "time_column": "run_ts"})

_add(
    (
        "options_chain_v2",
        "options_chain",
        "options_surface",
    ),
    _h(
        chunk="1 day",
        compress_after="7 days",
        retain="3 years",
        segmentby=("underlying",),
        rationale="options market data stream; append-mostly and queried by underlying/time",
        write_rate="high",
        read_pattern="underlying/time option surface and chain scans",
    ),
)
TABLE_CLASS["options_surface_agg"] = _h(
    chunk="1 day",
    compress_after="7 days",
    retain="3 years",
    rationale="global options surface aggregate stream; append-mostly and read by time",
    write_rate="medium",
    read_pattern="dashboard scans by time",
)

_add(
    (
        "runtime_metrics",
        "ingest_slippage",
        "ingestion_pipeline_health",
        "price_provider_health",
        "weather_provider_health",
        "broker_connection_health",
        "execution_divergence",
        "data_source_logs",
    ),
    HEALTH_SERIES,
)
TABLE_CLASS["broker_connection_health"] = _h(
    chunk="1 day",
    compress_after="14 days",
    retain="180 days",
    segmentby=("broker",),
    rationale="broker liveness samples; append-mostly and dashboarded by time/broker",
    write_rate="medium",
    read_pattern="recent broker health by broker/time",
)
TABLE_CLASS["runtime_metrics"] = _h(
    chunk="1 day",
    compress_after="14 days",
    retain="180 days",
    segmentby=("metric",),
    rationale="runtime metric stream; append-mostly and rolled up for dashboards",
    write_rate="high",
    read_pattern="metric/time dashboard windows",
)

_add(
    (
        "decision_log",
        "decision_views",
        "prediction_history",
        "predictions",
        "shadow_predictions",
        "ensemble_predictions",
        "model_predictions",
        "model_oos_predictions",
        "temporal_predictions",
        "rl_strategy_policy_decisions",
        "tracked_predictions",
        "prediction_explanations",
        "triple_barrier_labels",
        "rl_shadow_decisions",
        "policy_ope_observations",
    ),
    DECISION_SERIES,
)
TABLE_CLASS["model_predictions"] = _h(**{**DECISION_SERIES.__dict__, "time_column": "timestamp"})

_add(
    (
        "broker_fills",
        "execution_fills",
        "execution_metrics",
        "pnl_attribution",
        "capital_efficiency",
        "execution_capital_efficiency",
        "execution_fill_quality",
        "execution_policy_feedback",
        "execution_strategy_attribution",
        "execution_slippage_feedback",
        "alpha_preservation_kpis",
        "execution_analytics",
        "trades",
        "suppression_opportunity",
        "order_events",
        "trade_outcomes",
        "terminal_intent_rejections",
    ),
    EXECUTION_SERIES,
)
TABLE_CLASS["trade_outcomes"] = _h(**{**EXECUTION_SERIES.__dict__, "time_column": "timestamp"})
TABLE_CLASS["trade_attribution_ledger"] = _h(
    chunk="1 week",
    compress_after=None,
    retain=None,
    segmentby=("symbol",),
    rationale="compliance attribution ledger; append-only, never compressed, never deleted",
    write_rate="medium",
    read_pattern="order/source_alert/model/symbol lookup and time-range forensic review",
)

_add(
    (
        "kill_switch_audit",
        "execution_mode_audit",
        "execution_policy_audit",
        "position_reconcile_audit",
        "trade_suppression_audit",
        "model_promotion_audit",
        "crash_recovery_audit",
        "rules_audit",
        "pipeline_stage_audit",
        "universe_audit",
        "capital_preservation_audit",
        "risk_events",
        "model_governance_log",
        "alert_interactions",
        "alert_acks",
        "alert_resolutions",
        "promotion_statistical_evidence",
        "portfolio_kill_snapshots",
        "portfolio_risk_snapshots",
        "data_source_audit",
        "drawdown_bootstrap_baseline",
        "position_reconcile_bootstrap_audit",
        "alert_lifecycle_events",
        "broker_config_audit",
    ),
    AUDIT_SERIES,
)
TABLE_CLASS["alert_acks"] = _h(**{**AUDIT_SERIES.__dict__, "time_column": "acked_ts_ms"})
TABLE_CLASS["alert_resolutions"] = _h(**{**AUDIT_SERIES.__dict__, "time_column": "resolved_ts_ms"})
TABLE_CLASS["promotion_statistical_evidence"] = _h(**{**AUDIT_SERIES.__dict__, "time_column": "ts"})
TABLE_CLASS["strategy_promotion_candidates"] = _r(
    "mutable governed promotion candidate state; immutable decisions are mirrored to model_promotion_audit",
    read_pattern="pending candidate and operator approval lookup by strategy/status",
)

_add(
    (
        "event_log",
        "events",
        "equity_history",
        "portfolio_bt_points",
        "portfolio_equity_state",
        "trade_decision_snapshot",
        "strategy_allocator_history",
        "shadow_metrics",
        "shadow_training_runs",
        "self_critic_alerts",
        "exec_conf_calib",
        "execution_ai_advisory",
        "execution_ai_advisory_actions",
        "execution_alerts",
        "labels_exec",
        "model_metrics",
        "model_version_performance",
        "portfolio_orders",
        "portfolio_position_corr_snapshots",
        "portfolio_model_corr_snapshots",
        "pnl_decomposition",
        "realized_outcomes",
        "equity_drift",
        "rl_shadow_eval",
        "rl_shadow_actions",
        "strategy_promotion_log",
        "strategy_shadow_runs",
        "temporal_shadow_eval",
        "alpha_lifecycle",
        "nlp_embeddings",
        "nlp_sentiments",
        "nlp_text_blobs",
        "causal_scores",
        "alerts_archive",
    ),
    GLOBAL_FEATURE_SERIES,
)
TABLE_CLASS["net_after_cost_labels"] = _h(
    chunk="1 week",
    compress_after="30 days",
    retain="3 years",
    time_column="label_ts_ms",
    rationale="timestamp-safe net-after-cost label artifacts keyed to prediction time and replayed by model/symbol/horizon",
    write_rate="medium",
    read_pattern="training, evaluation, and promotion scans by label time and model identity",
)
TABLE_CLASS["labels_price"] = _h(
    chunk="1 week",
    compress_after="30 days",
    retain="3 years",
    segmentby=("symbol",),
    time_column="ts_eval_ms",
    rationale="derived realized price labels keyed to prediction and evaluation time for confidence calibration and validation",
    write_rate="medium",
    read_pattern="calibration and validation scans by symbol, prediction time, evaluation time, and horizon",
)
TABLE_CLASS["model_version_performance"] = _h(**{**GLOBAL_FEATURE_SERIES.__dict__, "time_column": "recorded_ts_ms"})
TABLE_CLASS["shadow_metrics"] = _h(**{**GLOBAL_FEATURE_SERIES.__dict__, "time_column": "window_end_ms"})

TABLE_CLASS["audit_chain_findings"] = _r(
    rationale="tamper-evidence verifier findings; append-only diagnostics retained indefinitely",
    write_rate="low",
    read_pattern="time-range audit review and table/row investigation",
)
TABLE_CLASS["credential_access_log"] = _h(
    chunk="1 week",
    compress_after=None,
    retain="1 year",
    time_column="ts",
    rationale="credential read audit trail; append-only and reviewed by time and credential name",
    write_rate="low",
    read_pattern="time-range credential access review",
    audit=True,
)


_REGISTRY_RATIONALE = "registry/catalog table; primary access is by natural key, not by time range"
_STATE_RATIONALE = "mutable state table; current value is the contract and rows are updated in place"
_BOUNDED_RATIONALE = "bounded or low-rate operational table; primary lookup is not a time-range scan"
_TRAINING_RATIONALE = "training/model artifact metadata; looked up by model/run identifiers"

_add(
    (
        "model_registry",
        "tracked_model_registry",
        "feature_registry",
        "strategy_registry",
        "sleeve_registry",
        "data_sources",
        "factor_registry",
        "models",
        "model_versions",
        "gbm_models",
        "hmm_regime_models",
        "temporal_models",
        "embed_models2",
        "rl_policies",
        "rl_strategy_policy_models",
        "causal_dags",
        "hypothesis_registry",
        "timescale_schema_version",
        "artifacts",
        "artifact_aliases",
        "artifact_fsck_findings",
    ),
    _r(_REGISTRY_RATIONALE),
)
_add(
    (
        "kill_switch_state",
        "execution_mode",
        "execution_health_state",
        "broker_order_state",
        "broker_shadow_order_state",
        "position_reconcile_baseline",
        "trade_suppression_state",
        "position_reconcile_state",
        "regime_state",
        "bocpd_regime_state",
        "champion_residual_adwin_state",
        "runtime_meta",
        "execution_meta",
        "runtime_metrics_state",
        "risk_state",
        "event_log_state",
        "job_locks",
        "job_heartbeats",
        "job_checkpoints",
        "alert_shelves",
        "ipc_channels",
        "price_feed_lock",
        "options_symbol_ingestion_state",
        "portfolio_state",
        "model_position_state",
        "broker_meta",
        "broker_shadow_meta",
        "broker_account",
        "broker_positions",
        "broker_shadow_account",
        "broker_shadow_positions",
    ),
    _r(_STATE_RATIONALE),
)
_add(
    (
        "schema_version",
        "schema_migrations",
        "active_feature_policy",
        "alerts",
        "job_history",
        "domain_blacklist",
        "domain_perf",
        "earnings_calendar",
        "confidence_calibration",
        "champion_assignments",
        "strategy_allocations",
        "strategy_allocator_scores",
        "strategy_cooldowns",
        "sleeve_allocations",
        "sleeve_metrics",
        "model_promotion_cooldown",
        "model_post_promo_watch",
        "model_post_promo_results",
        "alpha_decay_strategy_metrics",
        "alpha_decay_runtime_history",
        "feature_distribution_drift",
        "production_monitoring_metrics",
        "model_competition_rankings",
        "model_marketplace_scores",
        "model_promotion_guard",
        "symbol_universe",
        "symbols",
        "symbol_blacklist",
        "universe_pit",
        "notification_channel_tests",
        "ipc_messages",
        "execution_order_idempotency",
        "exec_open_orders",
        "exec_order_events",
        "execution_orders",
        "order_commands",
    ),
    _r(_BOUNDED_RATIONALE, cleanup="job_history and alerts use app-managed rotation where configured"),
)
_add(
    (
        "alpha_decay_metrics",
        "alpha_candidates",
        "backtest_scores",
        "backtest_cpcv_runs",
        "backtest_cpcv_path_results",
        "bocpd_ensemble_triggers",
        "challenger_shadow_orders",
        "embed_conf_calib",
        "embed_model_eval",
        "event_embeddings",
        "event_embeddings_seq",
        "factor_groups",
        "labels",
        "model_drift",
        "model_lifecycle_runs",
        "model_post_promo_results",
        "model_runs",
        "model_stats",
        "model_stats_regime",
        "model_stats_regime_versions",
        "model_stats_versions",
        "model_version_performance",
        "model_versions",
        "portfolio_bt_runs",
        "portfolio_meta",
        "regime_compat_scores",
        "residual_distribution_drift",
        "shadow_capital_scores",
        "shadow_order_intents",
        "size_policy",
        "size_policy_points",
        "sleeve_registry",
        "spillover_beta",
        "spillover_beta_versions",
        "strategy_metrics",
        "strategy_registry",
        "symbolic_alpha_candidates",
        "temporal_model_eval",
        "validation_scores",
        "walk_forward_runs",
        "walk_forward_scores",
        "embed_model_feature_schema",
        "temporal_model_feature_schema",
        "feature_candidates",
        "feature_evaluation",
        "ensemble_blend_weights",
        "ensemble_weights",
        "ensemble_family_performance",
        "drift_retrain_events",
        "narrative_clusters",
        "narrative_members",
        "competition_post_commit_actions",
        "model_hyperparameter_registry",
        "model_best_params",
        "model_performance",
        "experiment_ledger",
        "policy_ope_evidence",
        "rl_training_runs",
    ),
    _r(_TRAINING_RATIONALE, write_rate="low to medium", read_pattern="model/run keyed lookup and latest status reads"),
)


SOURCE_DECLARED_TABLES = frozenset(
    {
        "competition_post_commit_actions",
        "feature_store",
        "price_ticks",
        "timescale_schema_version",
        "price_data",
        "feature_data",
        "model_predictions",
        "trade_outcomes",
    }
)

FUTURE_CLASSIFIED_TABLES = frozenset(
    {
        "alerts_archive",
        "model_oos_predictions",
        "nlp_embeddings",
        "nlp_sentiments",
        "nlp_text_blobs",
        "causal_scores",
        "promotion_statistical_evidence",
        "runtime_metrics_state",
    }
)

AUDIT_CHAIN_TABLES = frozenset(
    {
        "decision_log",
        "drawdown_bootstrap_baseline",
        "execution_mode_audit",
        "execution_policy_audit",
        "kill_switch_audit",
        "model_promotion_audit",
        "position_reconcile_audit",
        "experiment_ledger",
        "policy_ope_observations",
        "policy_ope_evidence",
        "promotion_statistical_evidence",
        "trade_attribution_ledger",
    }
)

for _audit_table in AUDIT_CHAIN_TABLES:
    if _audit_table in TABLE_CLASS:
        TABLE_CLASS[_audit_table] = replace(TABLE_CLASS[_audit_table], audit=True)


def hypertables() -> dict[str, Hypertable]:
    return {name: cls for name, cls in TABLE_CLASS.items() if isinstance(cls, Hypertable)}


def regular_tables() -> dict[str, Regular]:
    return {name: cls for name, cls in TABLE_CLASS.items() if isinstance(cls, Regular)}


def is_hypertable(table_name: str) -> bool:
    return isinstance(TABLE_CLASS.get(str(table_name)), Hypertable)


def audit_tables() -> tuple[str, ...]:
    return tuple(sorted(name for name, cls in TABLE_CLASS.items() if getattr(cls, "audit", False)))


__all__ = [
    "FUTURE_CLASSIFIED_TABLES",
    "AUDIT_CHAIN_TABLES",
    "Hypertable",
    "Regular",
    "SOURCE_DECLARED_TABLES",
    "TABLE_CLASS",
    "TableClass",
    "audit_tables",
    "hypertables",
    "is_hypertable",
    "regular_tables",
]
