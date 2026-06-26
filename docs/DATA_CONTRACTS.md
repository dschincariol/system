# Data Contracts

This document records the concrete contracts that are visible in the inspected code paths. The focus is on payloads and rows that cross module boundaries and matter for runtime safety, attribution, operator debugging, and control-plane correctness.

## Conventions

| Convention | Meaning |
| --- | --- |
| `INTEGER` timestamps | Unix epoch in milliseconds unless the field name ends with `_s`. |
| `REAL` | Floating-point numeric value. Units are listed per field. |
| `TEXT` | Stored string value. |
| `BOOLEAN` | Logical true/false value in API payloads and Postgres-backed rows. The isolated SQLite test backend may persist `0/1`-style flags. |
| `JSON object` / `JSON array` | Parsed JSON in API responses. Postgres stores JSON-shaped columns as JSON/JSONB where migrations define them; compatibility cursors may expose JSONB values as JSON-encoded text to preserve older SQLite-shaped callers. |
| Required | The producer always writes it, or the consumer path assumes it exists. |
| Optional | Present only when the producer has enough context, or only when the underlying table version contains that column. |

Runtime storage note: production and production-like operation use the public facade in `engine/runtime/storage.py`, which validates the selected backend before exposing it. The concrete production backend is `engine/runtime/storage_pg.py`. SQLite remains an isolated Python test backend and a compatibility dialect for older call sites; it is not the production source of truth.

## 0. Data-Source Populate Evidence

Producer:
- `services.data_source_manager.DataSourceManager.populate_now(...)`

Consumers:
- `GET /api/data_sources`
- Data Sources UI detail panel
- runtime source health attachment in `attach_runtime_states_to_sources(...)`

Failure if malformed:
- a source can appear healthy before any real provider row has landed
- missing required fields or wrong timestamp/source keys can hide ingestion drift
- stale rows can be mistaken for current provider availability
- operators lose the row-count and storage evidence needed to distinguish bad credentials, provider outages, and schema mismatch

Production enforcement:
- each source has a code-defined `DataSourceContract` with normalized shape, required fields, units, symbol namespace, timestamp timezone, point-in-time availability semantics, unique key, idempotent upsert behavior, storage table, consumer, timestamp field, source field, and stale threshold
- `populate_now(...)` is a bounded one-shot path, not a broad backfill; it obeys the provider connection-test rate limiter and stops on provider 429 or 503 responses
- broker data-source populate paths are read-only only; Alpaca is limited to account and positions reads, and order/cancel/replace/flatten paths are rejected by `engine.data.broker_readonly`
- `data_source_populate_evidence` stores the latest evidence row per source
- `attach_runtime_states_to_sources(...)` downgrades otherwise healthy sources to `degraded` with `contract_health_gate` when no evidence exists, no rows landed, or the latest contract status is not `pass`
- logs and API responses use the data-source redaction boundary; tests use generated canary values and assert they do not appear in responses or logs

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `source_key` | `TEXT` | Yes | Data-source key whose storage proof was run. | source key |
| `ts_ms` | `INTEGER` | Yes | Populate evidence write time. | ms |
| `status` | `TEXT` | Yes | Provider/proof status: `pass`, `warn`, `fail`, or `degraded`. | enum |
| `contract_status` | `TEXT` | Yes | Data-contract result: `pass`, `warn`, or `fail`. | enum |
| `row_count` | `INTEGER` | Yes | Matching rows found in the contract storage table after populate. | count |
| `storage_table` | `TEXT` | Yes | Table named by the source contract. | table name |
| `latest_ts_ms` | `INTEGER` | No | Latest contract timestamp found for matching rows. | ms |
| `latency_ms` | `INTEGER` | Yes | End-to-end populate and verification latency. | ms |
| `missing_null_counts_json` | `JSON object` | Yes | Required field names mapped to missing/null counts. | count map |
| `duplicate_drops` | `INTEGER` | Yes | Duplicate rows implied by the contract unique key. | count |
| `stale_gap_status` | `TEXT` | Yes | Freshness result such as `fresh`, `stale`, `no_rows`, or `missing_timestamp`. | enum |
| `provider_evidence_json` | `JSON object` | Yes | Sanitized provider evidence and endpoint/probe metadata. | JSON |
| `contract_json` | `JSON object` | Yes | Embedded `DataSourceContract.payload()` for the proof. | JSON |
| `error` | `TEXT` | No | Failure or warning detail. | text |
| `actor` | `TEXT` | No | Operator or automation actor that requested populate. | actor |

## 1. Canonical Model Intent

Producer:
- `engine.strategy.model_intent.build_model_intent(...)`

Consumers:
- portfolio-construction and event-processing paths that expect canonical model intents
- `engine.strategy.model_intent.is_canonical_model_intent(...)`

Failure if malformed:
- a missing or invalid `schema_version` causes canonical-intent checks to fail
- wrong `side`, `should_trade`, or sizing fields can produce the wrong portfolio action
- missing `selected_features` or tradability fields reduces explainability and downstream sizing quality

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `schema_version` | `INTEGER` | Yes | Canonical payload version. Current builder writes `1`. | version |
| `symbol` | `TEXT` | Yes | Upper-cased asset symbol. | symbol |
| `horizon_s` | `INTEGER` | Yes | Prediction horizon. | seconds |
| `should_trade` | `BOOLEAN` | Yes | Whether the model intends the symbol to remain actionable. | boolean |
| `timing` | `TEXT` | Yes | Timing hint. Builder default is `enter_now`. | enum-like string |
| `side` | `TEXT` | Yes | `LONG`, `SHORT`, or `FLAT` inferred from `expected_z`. | enum-like string |
| `expected_z` | `REAL` | Yes | Signed expected move score. | z-score |
| `confidence` | `REAL` | Yes | Confidence score used by downstream gating. | dimensionless |
| `probability` | `REAL` | Yes | Probability-like score copied from explainability data or confidence. | 0-1 style score |
| `uncertainty` | `REAL` | Yes | Uncertainty estimate copied from explainability data or `1 - confidence`. | dimensionless |
| `confidence_raw` | `REAL` | Yes | Raw confidence before downstream normalization. | dimensionless |
| `prediction_strength` | `REAL` | Yes | Canonical strength score. | dimensionless |
| `score` | `REAL` | Yes | Alias of prediction strength. | dimensionless |
| `selection_score` | `REAL` | Yes | Universe-selection score. | dimensionless |
| `trade_score` | `REAL` | Yes | Trade-selection score. | dimensionless |
| `include_in_universe` | `BOOLEAN` | Yes | Whether the symbol remains eligible for the active universe. | boolean |
| `universe_score` | `REAL` | Yes | Ranking score for universe inclusion. | dimensionless |
| `selected_features` | `JSON array[string]` | Yes | Canonical list of feature identifiers inferred from explainability fields. | feature ids |
| `regime` | `TEXT` | No | Regime label when provided. | regime key |
| `target_weight` | `REAL` | No | Desired portfolio weight when the caller has a target allocation. | weight fraction |
| `size_mult` | `REAL` | No | Sizing multiplier. | multiplier |
| `expected_ret_net` | `REAL` | No | Tradability-derived expected net return. | return fraction |
| `p_win` | `REAL` | No | Tradability-derived win probability. | 0-1 probability |
| `expected_dd` | `REAL` | No | Tradability-derived expected drawdown. | drawdown fraction |

## 2. Prediction Rows

Producers:
- `engine.strategy.validation.py`

Consumers:
- downstream explainability and lifecycle tracing in `engine.runtime.trade_lifecycle`
- dashboard and diagnostics readers that inspect `predictions` and `prediction_history`
- strategy and governance paths that need regime-tagged prediction lineage

Failure if malformed:
- `event_id`, `symbol`, and `horizon_s` mismatches break joins to alerts, decisions, and trade lifecycle traces
- stale or incorrect `predictions` rows can overwrite the latest point-in-time prediction for a `(event_id, symbol, horizon_s)` key
- missing model identity reduces attribution, governance, and replay traceability

Storage notes:

- `predictions` is the latest point-in-time table. It is unique on `(event_id, symbol, horizon_s)`.
- `prediction_history` is append-only and preserves historical writes.

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `id` | `INTEGER` | Yes | Row id. | row id |
| `ts_ms` | `INTEGER` | Yes | Prediction write time. | ms |
| `event_id` | `INTEGER` | Yes | Upstream event identifier used for joins. | event id |
| `symbol` | `TEXT` | Yes | Asset symbol. | symbol |
| `horizon_s` | `INTEGER` | Yes | Prediction horizon. | seconds |
| `predicted_z` | `REAL` | Yes | Signed expected move. | z-score |
| `confidence` | `REAL` | Yes | Confidence score. | dimensionless |
| `confidence_raw` | `REAL` | Yes | Raw confidence before downstream normalization. | dimensionless |
| `prediction_strength` | `REAL` | Yes | Canonical strength metric written by validation. | dimensionless |
| `model_name` | `TEXT` | Yes | Human-readable model name. | name |
| `model_id` | `TEXT` | Yes | Stable model identity when available. | id |
| `model_version` | `TEXT` | Yes | Model version string when available. | version |
| `regime_time_ms` | `INTEGER` | No | Timestamp for the regime snapshot attached to the prediction. | ms |
| `volatility_regime` | `TEXT` | Yes | Volatility regime label. | regime |
| `trend_regime` | `TEXT` | Yes | Trend regime label. | regime |
| `liquidity_regime` | `TEXT` | Yes | Liquidity regime label. | regime |

## 3. Decision Log Row

Producer:
- `engine.strategy.decision_log.log_decision(...)`

Consumers:
- `/api/ui/decisions`
- `/api/ui/decision`
- `engine.runtime.trade_lifecycle.trace_trade_lifecycle(...)`
- explainability and operator diagnostics paths

Failure if malformed:
- the UI loses the decision-to-features explainability layer
- trade-lifecycle traces show a gap between prediction and execution
- feature-level debugging becomes guesswork instead of table-backed investigation

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `id` | `INTEGER` | Yes | Row id. | row id |
| `ts_ms` | `INTEGER` | Yes | Decision write time. | ms |
| `event_id` | `INTEGER` | Yes | Upstream event id. | event id |
| `symbol` | `TEXT` | Yes | Asset symbol. | symbol |
| `horizon_s` | `INTEGER` | Yes | Prediction horizon tied to this decision. | seconds |
| `predicted_z` | `REAL` | Yes | Signed expected move at decision time. | z-score |
| `confidence` | `REAL` | Yes | Confidence used by the decision layer. | dimensionless |
| `model_name` | `TEXT` | Yes | Model name. | name |
| `model_kind` | `TEXT` | Yes | Model family or kind. | kind |
| `model_ts_ms` | `INTEGER` | Yes | Model artifact timestamp. | ms |
| `features_hash` | `TEXT` | Yes | Hash of the decision feature set. | hash |
| `features_json` | `JSON object` | Yes | Feature values used by the decision. | feature payload |
| `explain_json` | `JSON object` | Yes | Explainability payload. | JSON |
| `extra_json` | `JSON object` | Yes | Auxiliary metadata. | JSON |

## 3A. Point-In-Time Model Feature Snapshot

Producer:
- `engine.strategy.model_feature_snapshots.build_model_feature_snapshot(...)`

Consumers:
- `engine.strategy.feature_registry.build_feature_snapshot(...)`
- training jobs that call the schema-driven feature registry
- predictor serving cache reads in `engine.strategy.predictor`
- replay/backfill and model-feature snapshot materialization

Failure if malformed:
- a delayed source can leak a value whose vendor availability was after the decision timestamp
- a stale source can look valid because the numeric feature value is present
- serving can use a latest cached feature snapshot that belongs to a later decision

Production enforcement:
- `build_model_feature_snapshot(...)` applies the PIT policy before returning or storing vectors.
- `feature_registry` resolves schema-driven symbol snapshots through the same model-feature snapshot path and filters cached NLP rows with `b.ts <= decision_ts_ms`.
- `predictor._latest_feature_snapshot_features(...)` rejects cached snapshots whose `ts_ms` or source availability is after the decision timestamp.
- Shadow-only time-series foundation features such as `tsfm.chronos_v2.*` are produced through the same snapshot path. Live model serving and live preflight reject model feature contracts that contain shadow-stage feature ids.

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `symbol` | `TEXT` | Yes | Upper-cased asset symbol. | symbol |
| `ts_ms` | `INTEGER` | Yes | Decision timestamp for the feature vector. | ms |
| `feature_ids` | `JSON array[string]` | Yes | Ordered feature ids used for the vector. | feature ids |
| `features` | `JSON object` | Yes | Feature-id to numeric value map after PIT enforcement. Ineligible or stale groups are zeroed. | feature payload |
| `vector` | `JSON array[number]` | Yes | Numeric vector in `feature_ids` order after PIT enforcement. | numeric |
| `availability` | `JSON object` | Yes | Group-level booleans after PIT and freshness enforcement. | boolean map |
| `source_timestamps` | `JSON object` | Yes | Group-level source and availability timestamps used to validate PIT safety. | ms |
| `source_timestamps._pit_controls` | `JSON object` | Yes | Per-group enforcement detail with `source_ts_ms`, `availability_ts_ms`, `reason_codes`, and `ok`. | JSON |
| `source_timestamps._feature_metadata` | `JSON object` | Yes | Per-group metadata: `source_timestamp_field`, `availability_timestamp_field`, `freshness_ttl_ms`, `lag_policy`, `stale_behavior`, and `pit_eligible`. | JSON |
| `feature_metadata` | `JSON object` | Yes | Top-level copy of the PIT metadata for live callers. Persisted rows recover it from `source_timestamps._feature_metadata`. | JSON |
| `pit_controls` | `JSON object` | Yes | Top-level copy of PIT enforcement results. Persisted rows recover it from `source_timestamps._pit_controls`. | JSON |

For the optional Chronos frozen encoder group, `source_timestamps.ts_foundation_chronos` includes `price_history_first_ts_ms`, `price_history_last_ts_ms`, `price_history_rows`, `encoder_artifact_created_ts_ms`, `artifact_alias`, `artifact_sha256`, `model_family`, `model_id`, `frozen_encoder=true`, and `direct_trading_authority=false`. PIT enforcement zeroes the group when price history or artifact availability is after the decision timestamp or stale under the group TTL.

Optional graph/relational features use `graph.relational_v1.*` ids and are shadow-only. `engine.strategy.graph_relational.build_graph_relational_snapshot(...)` builds versioned snapshots from PIT-safe relationships: sector, industry, rolling correlation, ETF ownership, supply chain edges, 13F shared ownership, options co-movement, and news co-mentions when those source tables are available. The snapshot metadata includes `graph_id`, `snapshot_version`, `relationship_hash`, `max_source_ts_ms`, `max_availability_ts_ms`, `snapshot_available`, `pit_safe`, and `direct_trading_authority=false`. `build_model_feature_snapshot(...)` applies the `graph_relational_v1` PIT policy and zeroes graph features when availability/source timestamps are after the decision timestamp or stale. Live model serving rejects `graph.relational_v1.*` feature contracts through the shadow-feature registry, and `engine.model_registry` plus `engine.strategy.champion_manager` block graph candidate promotion when graph metadata, PIT safety, train/serve parity, or snapshot availability is missing. Fully valid graph metadata still remains non-promotable because the scaffold is shadow-only.

### 3B. Structured Document Event Rows

Producer:
- `engine.runtime.storage_pg.put_normalized_event(...)` calls `engine.data.structured_document_events.extract_structured_document_events(...)` for normalized `news`, `filing`, and `transcript` rows. Transcript documents are also detected from `meta_json.transcript=true` or `source='fmp_transcript'`.

Consumers:
- `engine.data.structured_document_events.resolve_structured_document_event_features(...)`
- `engine.strategy.model_feature_snapshots.build_model_feature_snapshot(...)`
- `engine.strategy.feature_registry` via explicit `structured_doc_events_v1.*` feature ids

Production enforcement:
- durable rows live in `structured_document_events`, created by migration `0060_structured_document_events.py`
- feature joins only use `availability_ts_ms <= decision ts_ms`
- PIT metadata is attached to both raw rows (`pit_metadata_json`) and model snapshots (`source_timestamps.structured_doc_events`)
- all `structured_doc_events_v1.*` features are registry stage `shadow` with `direct_trading_authority=false`; live model serving rejects them through `feature_registry.assert_no_shadow_features(...)`

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `source_document_id` | `TEXT` | Yes | Stable document key from `source_id`, `event_key`, artifact id, URL, or deterministic hash. | id |
| `source_event_id` | `INTEGER` | No | Normalized `events.id` when available. | event id |
| `symbol` | `TEXT` | Yes | Upper-cased asset symbol; empty string when document is market-wide. | symbol |
| `document_type` | `TEXT` | Yes | `filing`, `transcript`, or `news`. | enum |
| `event_type` | `TEXT` | Yes | Extracted event kind such as `guidance_raise`, `guidance_cut`, `margin_pressure`, `liquidity_stress`, `capex_increase`, `capex_cut`, `debt_refinancing_risk`, `regulatory_litigation_risk`, `customer_concentration`, or `management_uncertainty`. | enum |
| `event_ts_ms` | `INTEGER` | Yes | Source document event timestamp. | ms |
| `availability_ts_ms` | `INTEGER` | Yes | Earliest timestamp the extraction may be used by PIT features. | ms |
| `extraction_confidence` | `REAL` | Yes | Deterministic extractor confidence for the matched evidence. | 0-1 score |
| `feature_id` | `TEXT` | Yes | Registered `structured_doc_events_v1.*` feature receiving the event. | feature id |
| `pit_metadata_json` | `JSON object` | Yes | Source timestamp, availability timestamp, extractor version, lag policy, and `direct_trading_authority=false`. | JSON |

### 3C. Structured Document And Graph Feature Visibility API

Producer:
- `engine.api.feature_visibility.build_feature_visibility(...)`

HTTP route:
- `GET /api/data/feature_visibility`

Consumers:
- Data Health panels in [ui/dashboard.html](../ui/dashboard.html) and [ui/feature_visibility.js](../ui/feature_visibility.js)
- `/api/ui/decision` attribution rows through `feature_visibility` metadata on structured-document and graph feature contributions

Production enforcement:
- This route is read-only and explanatory. It does not authorize feature use, promotion, allocation, or execution.
- Structured-document and graph feature groups remain `stage=shadow` with `direct_trading_authority=false`.
- Live model serving and promotion gates remain authoritative through `engine.strategy.feature_registry`, `engine.strategy.graph_relational`, model registry checks, champion competition, runtime gates, and execution controls.
- Missing tables, stale PIT inputs, missing failure telemetry, and unavailable snapshots are serialized as explicit warnings or unavailable states rather than silent absence.

Example:

```json
{
  "ok": true,
  "ts_ms": 1700000000000,
  "structured_documents": {
    "status": "available",
    "shadow_only": true,
    "direct_trading_authority": false,
    "counts": {
      "events": 12,
      "source_documents": 7,
      "symbols": 4,
      "low_confidence": 2
    },
    "latest_extraction_ts_ms": 1699999900000,
    "latest_availability_ts_ms": 1699999800000,
    "confidence": {
      "low_confidence_threshold": 0.6,
      "low_confidence_count": 2,
      "buckets": []
    },
    "coverage": {
      "symbols": [],
      "event_types": []
    },
    "lineage": {
      "source_documents": []
    },
    "pit_status": {
      "ok": true,
      "reason_codes": []
    },
    "warnings": []
  },
  "graph_features": {
    "status": "shadow_only",
    "enabled": false,
    "shadow_only": true,
    "direct_trading_authority": false,
    "feature_group": "graph_relational_v1",
    "graph_id": "graph_relational_v1",
    "snapshot_freshness": {
      "latest_snapshot_ts_ms": 1699999900000,
      "stale": false
    },
    "feature_availability": {
      "expected_feature_count": 12,
      "observed_feature_count": 12,
      "missing_feature_ids": []
    },
    "pit_status": {
      "ok": true,
      "latest_snapshot_pit_safe": true
    },
    "snapshots": [],
    "warnings": [
      "USE_GRAPH_RELATIONAL_FEATURES is disabled; graph features are unavailable unless precomputed snapshots exist"
    ]
  },
  "explanation_paths": {
    "decision_detail_route": "/api/ui/decision",
    "feature_prefixes": ["structured_doc_events_v1.", "graph.relational_v1."]
  }
}
```

## 4. Execution Decision Result

Producer:
- `engine.decision_engine.DecisionEngine.evaluate(...)`

Consumers:
- execution-target shaping in the portfolio-execution path
- any caller that needs an auditable reason for a downgrade to `shadow` or a blocked execution candidate

Failure if malformed:
- real orders can avoid an intended downgrade to `shadow`
- operators lose the threshold and risk context that explains why an order was blocked

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `enabled` | `BOOLEAN` | Yes | Whether the decision engine itself is enabled. | boolean |
| `execute` | `BOOLEAN` | Yes | Whether the input should remain executable. | boolean |
| `reason` | `TEXT` | Yes | Primary reason string. | reason code |
| `reasons` | `JSON array[string]` | Yes | Full set of blocking or downgrade reasons. | reason codes |
| `risk_increasing` | `BOOLEAN` | Yes | Whether the proposed action increases risk. | boolean |
| `prediction` | `REAL` | Yes | Input prediction value. | model score |
| `confidence` | `REAL` | Yes | Input confidence value. | dimensionless |
| `thresholds` | `JSON object` | Yes | Threshold bundle used by the gate. | mixed |
| `risk` | `JSON object` | Yes | Risk context copied into the decision record. | mixed |

`thresholds` currently carries:

- `min_confidence`
- `min_abs_prediction`
- `max_risk_score`
- `max_expected_drawdown`
- `max_market_stress`
- `max_signal_age_s`
- `max_open_positions`
- `max_positions_per_symbol`

## 5. Portfolio Order Intent Row

Producers:
- `engine.strategy.portfolio.py`
- `engine.terminal.api.api_terminal_orders.py`

The strategy producer runs `compute_rebalance()` through explicit stages:
input loading, allocator loading, target construction, normalization, overlays,
risk gates, execution blocking, order emission, and persistence. The stage split
is an orchestration refactor only; emitted `portfolio_orders` rows keep the
same action, side, weight, lineage, and `explain_json` contract described below.

Consumers:
- `engine.strategy.portfolio_execution_intents.load_latest_execution_intents(...)`
- `engine.terminal.api.api_terminal.py`
- `engine.runtime.trade_lifecycle.trace_trade_lifecycle(...)`

Failure if malformed:
- the execution pipeline cannot build the latest intent batch
- terminal markers and order tables become inconsistent
- attribution loses the link between alert lineage and later broker activity

Important caveat:

- The portfolio path uses `from_weight`, `to_weight`, and `delta_weight` as allocation-style fields.
- The browser terminal order-entry path writes manual quantity intent rows with `from_weight = 0.0`, `to_weight = 0.0`, and `delta_weight = 0.0`.
- Terminal quantity is stored in `explain_json.terminal_order`, not in any weight field. That object carries `sizing="quantity"`, `symbol`, `side`, positive `qty`, signed `signed_qty`, and `flatten`.
- Keeping terminal `delta_weight` neutral prevents weight-based risk, stability, and budget readers from mistaking a share quantity for a portfolio allocation.
- Terminal routes apply backend pre-trade controls before writing this row. Missing/stale price, max quantity, max notional, and duplicate-recent-order rejections are written to `terminal_intent_rejections` instead of `portfolio_orders`.

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `id` | `INTEGER` | Yes | Order-intent row id. | row id |
| `ts_ms` | `INTEGER` | Yes | Intent creation time. | ms |
| `model_id` | `TEXT` | Yes | Producing model id. Terminal writes `baseline`. | model id |
| `symbol` | `TEXT` | Yes | Asset symbol. | symbol |
| `action` | `TEXT` | Yes | Portfolio action. The strategy path uses values such as `OPEN`, `INCREASE`, `DECREASE`, `CLOSE`, `REVERSE`, `HOLD`. The terminal path writes `BUY`, `SELL`, and `FLATTEN`. | action code |
| `from_side` | `TEXT` | Yes | Prior side. Stored `TEXT NOT NULL` (migration `0022_portfolio_orders.py`); writers must always supply a value. | side |
| `to_side` | `TEXT` | Yes | Target side. Stored `TEXT NOT NULL` (migration `0022_portfolio_orders.py`); writers must always supply a value. | side |
| `from_weight` | `REAL` | Yes | Prior portfolio weight in the strategy path. | weight fraction |
| `to_weight` | `REAL` | Yes | Target portfolio weight in the strategy path. | weight fraction |
| `delta_weight` | `REAL` | Yes | Weight delta in the portfolio path. Terminal quantity rows keep this at `0.0`; the quantity lives in `explain_json.terminal_order`. | weight fraction |
| `source_alert_id` | `INTEGER` | No | Link back to the alert/signal row. | alert id |
| `explain_json` | `JSON object` | No | Explainability and model metadata. Terminal quantity rows include `terminal_order` with `sizing`, `symbol`, `side`, `qty`, `signed_qty`, and `flatten`. | JSON |

Optional columns that the execution-intent loader reads when present:

- `reason`
- `source_rule_id`

## 6. Latest Execution Intents

Producer:
- `engine.strategy.portfolio_execution_intents.load_latest_execution_intents(...)`

Consumers:
- `engine.execution.broker_apply_orders.py`
- dashboards and diagnostics that inspect intent batches and execution decisions

Failure if malformed:
- `broker_apply_orders.py` can block the whole batch
- shadow-vs-real splitting can become incorrect
- competition, budget, and latency lineage can disappear before execution

### Envelope

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Whether the loader succeeded. | boolean |
| `batch_id` | `INTEGER` | No | Source batch identifier when available. | batch id |
| `batch_ts_ms` | `INTEGER` | No | Timestamp for the loaded batch. | ms |
| `intents` | `JSON array[object]` | Yes | Execution-targeted intents. | list |
| `shadowed_intents` | `JSON array[object]` | Yes | Intents downgraded or budgeted into shadow execution. | list |
| `decision_summary` | `JSON object` | Yes | Batch-level summary of decision-engine outcomes. | mixed |

### Per-intent fields

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `source_order_id` | `INTEGER` | Yes | Source `portfolio_orders.id`. | row id |
| `ts_ms` | `INTEGER` | Yes | Source order timestamp. | ms |
| `model_id` | `TEXT` | Yes | Model id. | model id |
| `symbol` | `TEXT` | Yes | Asset symbol. | symbol |
| `action` | `TEXT` | Yes | Copied portfolio action. | action code |
| `from_side` | `TEXT` | No | Prior side. | side |
| `to_side` | `TEXT` | No | Target side. | side |
| `from_weight` | `REAL` | Yes | Copied source weight. | weight fraction |
| `to_weight` | `REAL` | Yes | Copied target weight. | weight fraction |
| `delta_weight` | `REAL` | Yes | Effective weight delta. Manual terminal quantity intents keep this at `0.0` so downstream weight consumers do not treat shares as allocation. | weight fraction |
| `qty` | `REAL` | No | Signed explicit quantity for terminal-originated quantity intents. Positive means buy/increase long exposure; negative means sell/increase short or reduce long exposure. | broker quantity |
| `order_sizing` | `TEXT` | No | Set to `quantity` when `qty` came from a terminal quantity payload. | sizing mode |
| `terminal_order` | `BOOLEAN` | No | Present and true when the execution intent was derived from `explain_json.terminal_order`. | boolean |
| `reason` | `TEXT` | No | Optional reason column when the table version has it. | reason |
| `source_alert_id` | `INTEGER` | No | Alert lineage. | alert id |
| `source_rule_id` | `TEXT` | No | Optional source rule column when present. | rule id |
| `explain` | `JSON object` | No | Parsed explanation payload. | JSON |
| `model_name` | `TEXT` | No | Extracted from explainability metadata. | name |
| `model_kind` | `TEXT` | No | Extracted model family. | kind |
| `model_ts_ms` | `INTEGER` | No | Model artifact timestamp. | ms |
| `model_version` | `TEXT` | No | Model version. | version |
| `regime` | `TEXT` | No | Extracted regime label. | regime |
| `signal_ts_ms` | `INTEGER` | No | Alert timestamp. | ms |
| `alpha_ttl_ms` | `INTEGER` | No | Time-to-live attached to the alpha. | ms |
| `alpha_half_life_ms` | `INTEGER` | No | Alpha half-life. | ms |
| `horizon_s` | `INTEGER` | No | Horizon copied from the source alert. | seconds |
| `volatility` | `REAL` | No | Optional alert volatility field. | producer-defined |
| `market_regime` | `TEXT` | No | Optional market-regime label from the alert. | regime |
| `market_regime_snapshot` | `JSON object` | No | Optional market-regime payload. | JSON |
| `source_event_ts_ms` | `INTEGER` | No | Original upstream event time when present. | ms |
| `db_observed_ts_ms` | `INTEGER` | No | Time when the source event reached the DB. | ms |
| `ingestion_to_db_latency_ms` | `INTEGER` | No | Ingestion latency carried forward from the alert metadata. | ms |
| `db_to_prediction_latency_ms` | `INTEGER` | No | Prediction latency carried forward from the alert metadata. | ms |
| `prediction_ts_ms` | `INTEGER` | No | Prediction timestamp. | ms |
| `prediction_to_decision_latency_ms` | `INTEGER` | No | Decision latency carried forward from the alert metadata. | ms |
| `decision_ts_ms` | `INTEGER` | No | Decision timestamp. | ms |
| `competition` | `JSON object` | No | Competition policy returned by `champion_manager.get_competition_policy_for_intent(...)`. | JSON |
| `execution_target` | `TEXT` | Yes | `real` or `shadow`. | target |
| `competition_block_reason` | `TEXT` | No | Reason an intent was blocked by competition policy. | reason |
| `competition_capital_block_reason` | `TEXT` | No | Reason an intent was blocked by capital budgeting. | reason |
| `group_budget_fraction` | `REAL` | No | Group-level budget fraction. | weight fraction |
| `remaining_group_budget_fraction` | `REAL` | No | Remaining group budget. | weight fraction |
| `model_budget_fraction` | `REAL` | No | Model-level budget fraction. | weight fraction |
| `remaining_budget_fraction` | `REAL` | No | Remaining model budget. | weight fraction |
| `deployable_equity` | `REAL` | No | Budgeted equity available to deploy. | currency |
| `exec_regime` | `JSON object` | No | Execution-regime payload with `ts_ref`, `skew_z`, `flow_z`, `stress_mag`, `stress_mult`, `earnings_decay`, `earnings_mult`, and `final_mult`. | mixed |
| `decision` | `JSON object` | No | Decision-engine output. When downgraded, the payload includes `downgraded_execution_target="shadow"`. | JSON |

### `decision_summary`

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `enabled` | `BOOLEAN` | Yes | Whether the decision engine was enabled for the batch. | boolean |
| `evaluated` | `INTEGER` | Yes | Number of evaluated intents. | count |
| `shadowed` | `INTEGER` | Yes | Number of intents pushed to shadow execution. | count |
| `allowed` | `INTEGER` | Yes | Number of intents still allowed through the execution pipeline. | count |
| `open_positions_start` | `INTEGER` | Yes | Open-position count before evaluation. | count |
| `open_positions_end` | `INTEGER` | Yes | Open-position count after evaluation. | count |

## 7. Execution Order Ledger Row

Producers:
- the live routing path reached from `engine.execution.broker_apply_orders.py`
- broker routing helpers that persist the canonical execution ledger

Consumers:
- `engine.execution.execution_poll_and_attrib.py`
- `engine.runtime.trade_lifecycle.py`
- execution metrics and detailed execution readers

Failure if malformed:
- fills cannot be joined back to their source orders
- idempotency, broker status, and lineage debugging all degrade

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `client_order_id` | `TEXT` | Yes | Canonical primary key for the execution order. | order id |
| `order_uid` | `TEXT` | Yes | Additional unique order identifier. | order id |
| `idempotency_status` | `TEXT` | Yes | Idempotency tracking state. | status |
| `broker` | `TEXT` | Yes | Broker integration name. | broker |
| `portfolio_orders_id` | `INTEGER` | No | Source portfolio-order row. | row id |
| `source_alert_id` | `INTEGER` | No | Alert lineage. | alert id |
| `prediction_id` | `INTEGER` | No | Prediction lineage. | prediction id |
| `model_id` | `TEXT` | No | Model id. | model id |
| `model_version` | `TEXT` | No | Model version. | version |
| `symbol` | `TEXT` | Yes | Asset symbol. | symbol |
| `qty` | `REAL` | Yes | Submitted quantity. | broker quantity |
| `submit_ts_ms` | `INTEGER` | Yes | Submit timestamp. | ms |
| `ref_px` | `REAL` | No | Reference price. | price |
| `expected_px` | `REAL` | No | Expected execution price. | price |
| `mid_px` | `REAL` | No | Mid price at submission. | price |
| `bid_px` | `REAL` | No | Bid at submission. | price |
| `ask_px` | `REAL` | No | Ask at submission. | price |
| `spread_bps` | `REAL` | No | Bid-ask spread. | basis points |
| `broker_order_id` | `TEXT` | No | Broker-native order id. | broker order id |
| `status` | `TEXT` | Yes | Current order status. | status |
| `extra_json` | `JSON object` | No | Extra broker or routing metadata. | JSON |

## 8. Fill Ledger Row

Producers:
- broker fill pollers called by `engine.execution.execution_poll_and_attrib.py`
  - `engine.execution.broker_alpaca_rest.poll_and_log_fills(...)`
  - `engine.execution.broker_ibkr_gateway.poll_and_log_fills(...)`

Consumers:
- execution metrics readers
- `compute_pnl_attribution_snapshot(...)`
- terminal APIs
- `engine.runtime.trade_lifecycle.py`

Failure if malformed:
- fill-to-order joins fail
- slippage, fees, fill latency, and PnL attribution become untrustworthy
- browser-terminal markers and fill tables degrade

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `id` | `INTEGER` | Yes | Fill row id. | row id |
| `client_order_id` | `TEXT` | Yes | Join key back to `execution_orders`. | order id |
| `fill_id` | `TEXT` | Yes | Broker fill id. | fill id |
| `broker` | `TEXT` | Yes | Broker integration name. | broker |
| `model_id` | `TEXT` | No | Model id. | model id |
| `model_version` | `TEXT` | No | Model version. | version |
| `symbol` | `TEXT` | Yes | Asset symbol. | symbol |
| `ts_ms` | `INTEGER` | Yes | Fill row write time. | ms |
| `submit_ts_ms` | `INTEGER` | No | Original submit time if known. | ms |
| `fill_ts_ms` | `INTEGER` | Yes | Broker-reported fill time. | ms |
| `fill_qty` | `REAL` | Yes | Filled quantity. | broker quantity |
| `fill_px` | `REAL` | Yes | Filled price. | price |
| `expected_px` | `REAL` | No | Expected execution price. | price |
| `mid_px` | `REAL` | No | Mid price used for slippage math. | price |
| `bid_px` | `REAL` | No | Bid price. | price |
| `ask_px` | `REAL` | No | Ask price. | price |
| `spread_bps` | `REAL` | No | Bid-ask spread at fill time. | basis points |
| `slippage_bps` | `REAL` | No | Realized slippage. | basis points |
| `fill_latency_ms` | `INTEGER` | No | Time from submit to fill. | ms |
| `fees` | `REAL` | No | All-in fees. | currency |
| `commission` | `REAL` | No | Commission component when available. | currency |
| `liquidity` | `TEXT` | No | Liquidity flag from the broker when available. | liquidity code |
| `raw_json` | `JSON object` | No | Raw broker payload. | JSON |
| `extra_json` | `JSON object` | No | Additional normalized metadata. | JSON |

## 8A. Execution Diagnostics API Payload

Producer:
- `engine.execution.execution_diagnostics.build_execution_diagnostics(...)`
  exposed through `GET /api/execution/diagnostics`

Consumers:
- dashboard execution screen TCA, outcome, trace, LOB/DeepLOB, and learned
  slicing panels
- operator/debug tooling that needs a normalized read-only execution-quality
  contract rather than raw table JSON

Failure if malformed:
- operators lose by-symbol execution-quality visibility
- rejected, suppressed, stale L2, and partial-fill states can be hidden behind
  raw API/table differences
- UI may misrepresent advisory learned-slicing state as unavailable or active

Authority:
- explanatory only; it does not arm execution, mutate orders, bypass broker
  controls, alter risk gates, or authorize learned slicing

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Whether the diagnostics serializer completed. | boolean |
| `ts_ms` | `INTEGER` | Yes | Serialization timestamp. | ms |
| `state` | `TEXT` | Yes | Overall read state such as `fresh`, `stale`, `partial`, or `unavailable`. | state |
| `inventory.routes[]` | `ARRAY<object>` | Yes | Route/source availability for stats, rolling metrics, by-symbol metrics, advisories, terminal orders/fills, rejected/suppressed intents, LOB, DeepLOB, and learned slicing. | route metadata |
| `tca.by_symbol[]` | `ARRAY<object>` | Yes | Symbol-level fills, filled quantity, slippage, latency, fill quality, costs, implementation shortfall, and VWAP fields where available. | mixed |
| `tca.rolling[]` | `ARRAY<object>` | Yes | Rolling execution windows with fill count, slippage, latency, fees, and implementation-shortfall summaries. | mixed |
| `tca.partial_fills[]` | `ARRAY<object>` | Yes | Parent-order fill aggregation with ordered, filled, remaining quantity, fill ratio, VWAP, and lineage ids. | mixed |
| `order_flow.rejected_intents[]` | `ARRAY<object>` | Yes | Rejected terminal intents with `reason_code`, `reason`, side, quantity, source, and detail metadata. | mixed |
| `order_flow.suppressed_intents[]` | `ARRAY<object>` | Yes | Suppressed portfolio/execution intents from attribution or policy audit rows with machine-readable and human-readable reasons. | mixed |
| `lob.l2_feed` | `OBJECT` | Yes | L2 freshness state, latest timestamp, age, required/sample rows, and top-of-book depth summary. | mixed |
| `lob.simulation` | `OBJECT` | Yes | LOB replay/simulation readiness and calibration evidence from recent simulated fills. | mixed |
| `lob.deeplob` | `OBJECT` | Yes | Shadow-only DeepLOB enablement, readiness, blockers, and authority constraints. | mixed |
| `learned_slicing` | `OBJECT` | Yes | Contextual-bandit policy state, selected action distribution, baseline comparison, recent non-application reasons, and explicit no-new-authority flags. | mixed |
| `drilldowns[]` | `ARRAY<object>` | Yes | Intent-to-route-to-fill, rejection, or suppression trace rows with lineage identifiers where available. | mixed |

## 9. Net-After-Cost Label Artifact

Producers:
- `engine/execution/jobs/compute_exec_labels.py` writes timestamp-safe market-data labels after the forward horizon has elapsed
- `engine/execution/jobs/compute_exec_labels_from_fills.py` overwrites with realized broker-fill evidence when real fills are available

Consumers:
- training loaders that prefer realized `labels_exec.net_z` over gross `labels.impact_z`
- PatchTST supervised fine-tuning, which first uses realized `net_after_cost_labels.net_return` for `net_edge` targets or `net_after_cost_labels.realized_forward_return` for `forward_return` targets before falling back to legacy labels
- `engine.strategy.pipeline_train_and_eval._net_eval_metrics(...)`
- `engine.strategy.model_marketplace` and `engine.strategy.champion_manager`, with shared `model_marketplace_scores` and `champion_assignments` writes routed through `engine.strategy.model_competition.repository`
- `engine.strategy.promotion_guard.metrics_have_net_cost_evidence(...)`

Failure if malformed:
- promotion can no longer prove whether edge survives slippage, spread, fees, and financing/borrow costs
- training can silently optimize gross-only targets
- order/fill attribution cannot be reconciled back to model intent, regime, or confidence

Storage notes:

- `net_after_cost_labels` is keyed by `(event_id, symbol, horizon_s, label_ts_ms)` so the label is tied to the original prediction timestamp and remains compatible with Timescale hypertable uniqueness.
- `label_ts_ms` is the prediction timestamp, not the computation time. `computed_at_ts_ms` records when the label was materialized.
- Rows from synthetic market-data execution have `realized=0`. Promotion-grade evidence requires realized fill-derived rows with cost fields populated.
- Promotion gates fail closed when challenger metrics lack `net_cost_evidence.available=true` and a positive `net_cost_label_count`.
- RL, bandit, sizing-policy, and execution-policy challengers also require doubly robust off-policy evaluation before moving beyond shadow. The raw inputs live in `policy_ope_observations` or compatible OPE payloads embedded in `shadow_predictions.extra_json`, `execution_policy_audit.decision_json`, or `challenger_shadow_orders.meta_json`. Each usable row must include behavior propensity, target propensity or target/logged actions, realized outcome, logged-action model estimate, and target-policy model estimate.
- `policy_ope_evidence` stores the append-only DR estimate, effective sample size, support, confidence bounds, and pass/fail reason consumed by `engine.strategy.champion_manager`, `engine.model_registry`, `engine.strategy.size_policy`, `engine.execution.execution_policy_engine`, and `engine.strategy.jobs.strategy_governance_job`.
- PatchTST masked pretraining does not consume labels. It reconstructs historical feature/price windows, persists a `model:patchtst:<model_name>:*:pretrained` artifact, and supervised fine-tuning records the pretraining artifact alias/SHA in the final shadow model config and registry metrics.
- iTransformer supervised training consumes the same sequence rows and net-after-cost label preference as PatchTST, then writes OOS validation rows to `model_oos_predictions` with `family='itransformer'`. Its shadow marketplace row uses `score_source='model_oos_predictions'` only for champion-manager visibility; it is not realized execution evidence and cannot satisfy live-promotion gates by itself.

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `event_id` | `INTEGER` | Yes | Prediction/event lineage key. | event id |
| `prediction_id` | `INTEGER` | No | Typed prediction row id when available. | row id |
| `source_alert_id` | `INTEGER` | No | Alert or signal id that led to execution. | alert id |
| `symbol` | `TEXT` | Yes | Asset symbol. | symbol |
| `horizon_s` | `INTEGER` | Yes | Forward-return horizon. | seconds |
| `label_ts_ms` | `INTEGER` | Yes | Original prediction timestamp used for the label. | ms |
| `entry_ts_ms`, `exit_ts_ms` | `INTEGER` | No | Entry and exit price/fill timestamps used for realized return. | ms |
| `computed_at_ts_ms` | `INTEGER` | Yes | Label materialization time. | ms |
| `model_name`, `model_id`, `model_version` | `TEXT` | No | Model identity attached to the prediction/alert. | model identity |
| `model_family` | `TEXT` | Yes | Normalized family such as `patchtst`, `itransformer`, `temporal`, or `gbm`. | family |
| `regime` | `TEXT` | Yes | Regime label extracted from prediction, alert, or model intent metadata. | regime |
| `confidence`, `confidence_raw` | `REAL` | No | Confidence values attached to the model intent. | dimensionless |
| `confidence_metadata_json` | `JSON object` | No | Prediction score, confidence, raw confidence, and regime components. | JSON |
| `side` | `INTEGER` | Yes | `1` for long-style label, `-1` for short-style label. | side |
| `realized` | `INTEGER` | Yes | `1` when backed by real broker fills, else `0`. | 0/1 |
| `gross_return` | `REAL` | Yes | Direction-adjusted forward return before execution costs. | return fraction |
| `realized_forward_return` | `REAL` | Yes | Raw forward return from entry to exit before side adjustment. | return fraction |
| `execution_cost_return` | `REAL` | Yes | Return drag from execution and carry costs. | return fraction |
| `net_return` | `REAL` | Yes | Direction-adjusted return after all available costs. | return fraction |
| `fees_bps`, `slippage_bps`, `spread_bps` | `REAL` | Yes | Execution cost decomposition. | basis points |
| `borrow_bps`, `financing_bps` | `REAL` | Yes | Borrow/financing cost when available from broker or attribution metadata. | basis points |
| `total_cost_bps` | `REAL` | Yes | Maximum known all-in cost basis points. | basis points |
| `fees_cost`, `slippage_cost`, `spread_cost`, `borrow_cost`, `financing_cost`, `total_cost` | `REAL` | No | Currency-denominated cost evidence where available. | currency |
| `source` | `TEXT` | Yes | Label source such as `synthetic_market_data` or `broker_fills_v2`. | source |
| `order_count`, `fill_count` | `INTEGER` | Yes | Number of linked execution orders/fills found for the label. | count |
| `label_metadata_json` | `JSON object` | No | Timestamp-safety flag, execution trace ids, carry availability, and source details. | JSON |

## 10. Learned Alpha Decay, Capacity, And Crowding

Producer:
- `engine/strategy/jobs/train_learned_alpha_decay.py`

Source evidence:
- realized `net_after_cost_labels` rows first
- legacy `labels_exec` rows only when richer net-after-cost rows are absent

Consumers:
- `engine.execution.execution_policy_engine.apply_execution_policy(...)` shortens TTL/half-life, blocks stale learned cohorts, blocks low-capacity/crowded risk-increasing orders, and records the learned gate in execution audit payloads
- `engine.strategy.portfolio_execution_intents.load_latest_portfolio_execution_intents(...)` applies learned capacity/crowding multipliers before model/group/portfolio caps are enforced
- `engine.strategy.position_sizing.position_from_signal(...)` accepts the learned estimate object and scales or blocks direct sizing calls
- `engine.strategy.champion_manager.evaluate_competition_cycle(...)` treats learned low-capacity/crowded cohorts as candidate/current blockers during champion evaluation

Failure if malformed:
- stale signals can survive longer than realized edge supports
- capacity-constrained alphas can receive full target weights
- crowded model cohorts can be promoted even when realized net edge has decayed

Storage notes:

- `learned_alpha_decay_runs` records each training run and freshness metadata. Runtime lookups ignore stale runs beyond `LEARNED_ALPHA_MAX_LOOKUP_AGE_MS`.
- `learned_alpha_decay_estimates` stores latest cohort estimates by model family, symbol, regime, liquidity bucket, spread bucket, volatility bucket, and factor group, with hierarchical fallback rows for sparse cohorts.
- `learned_alpha_decay_age_edges` stores the realized edge curve by signal-age bucket so half-life and max useful age are auditable.
- Runtime consumers fail open when no fresh matching estimate exists, but enforce the learned block/size multipliers when an estimate is present.

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `cohort_key` | `TEXT` | Yes | Pipe-delimited normalized cohort key. | key |
| `cohort_level` | `TEXT` | Yes | Exact or fallback aggregation level. | label |
| `model_family`, `symbol`, `regime` | `TEXT` | Yes | Core model and market context. | mixed |
| `liquidity_bucket`, `spread_bucket`, `volatility_bucket`, `factor_group` | `TEXT` | Yes | Execution and factor-context cohort dimensions. | labels |
| `n_obs` | `INTEGER` | Yes | Realized observations in the cohort. | count |
| `mean_realized_edge`, `positive_rate` | `REAL` | Yes | Net-after-cost edge summary. | return fraction / ratio |
| `half_life_ms`, `max_useful_age_ms` | `INTEGER` | Yes | Learned decay timing used by execution TTL/half-life policy. | ms |
| `capacity_estimate` | `REAL` | Yes | Normalized useful capacity estimate. | portfolio fraction |
| `crowding_penalty`, `size_multiplier` | `REAL` | Yes | Crowding penalty and final sizing multiplier. | 0..1 |
| `block_signal` | `INTEGER` | Yes | Hard block flag for risk-increasing consumers. | 0/1 |
| `detail_json` | `JSON object` | No | Estimator diagnostics and source counts. | JSON |

## 11. Position Contracts

### Broker position row

Producer:
- broker position synchronization paths that update `broker_positions`

Consumers:
- `engine.terminal.api.api_terminal.py`
- system and dashboard position readers

Failure if malformed:
- the browser terminal cannot display current positions correctly
- flatten operations can compute the wrong offset size

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ts_ms` | `INTEGER` | Yes | Snapshot write time. | ms |
| `symbol` | `TEXT` | Yes | Asset symbol. | symbol |
| `qty` | `REAL` | Yes | Net broker position. | broker quantity |
| `avg_px` | `REAL` | Yes | Average entry price. | price |
| `market_px` | `REAL` | No | Current market price. | price |
| `market_value` | `REAL` | No | Current marked value. | currency |
| `unrealized_pnl` | `REAL` | No | Unrealized PnL. | currency |
| `realized_pnl` | `REAL` | No | Realized PnL. | currency |
| `side` | `TEXT` | No | Position side. | side |
| `updated_ts_ms` | `INTEGER` | Yes | Last update time. | ms |
| `extra_json` | `JSON object` | No | Broker-specific metadata. | JSON |

### Model position state row

Producer:
- post-trade attribution paths that maintain `model_position_state`

Consumers:
- `engine.runtime.trade_lifecycle.py`
- model-level attribution and governance readers

Failure if malformed:
- the runtime loses model-level open-position and realized-PnL state
- promotion and attribution analysis can no longer reconcile live positions by model

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `model_id` | `TEXT` | Yes | Model id. | model id |
| `symbol` | `TEXT` | Yes | Asset symbol. | symbol |
| `net_qty` | `REAL` | Yes | Net model position. | quantity |
| `avg_entry_price` | `REAL` | Yes | Average entry price for the model position. | price |
| `realized_pnl` | `REAL` | Yes | Realized PnL for the model position. | currency |
| `last_update_ts_ms` | `INTEGER` | Yes | Last update time. | ms |

## 11. Runtime Execution Barrier Snapshot

Producer:
- `engine.runtime.gates.execution_gate_snapshot(...)`

Consumers:
- `engine.execution.broker_apply_orders.py`
- `engine.api.api_system.api_get_execution_barrier(...)`
- `engine.terminal.api.api_terminal_orders.py`
- dashboard and operator UIs

Failure if malformed:
- callers may confuse pipeline permission with real-trading permission
- orders can remain blocked for the wrong reason, or worse, skip a required safety stop

Important semantic detail:

- `allowed` is the same concept as `allow_execution_pipeline`.
- `real_trading_allowed` is stricter and only becomes true in live mode when the runtime is armed and not otherwise blocked.
- `mode` is the reported effective execution mode after reconciling `ENGINE_MODE`, `EXECUTION_MODE`, and persisted execution-mode state. Safety policies such as `disable_live_execution_env` may still be the applied block while `mode` remains `safe`, `paper`, or `shadow`.
- When `DISABLE_LIVE_EXECUTION` is unset or not explicitly false (`0`, `false`, `no`, or `off`), live-mode snapshots return `reason=disable_live_execution_env`, `allowed=false`, and `real_trading_allowed=false` even if runtime state is `LIVE` and armed.

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Whether the barrier evaluation itself succeeded. | boolean |
| `ts_ms` | `INTEGER` | Yes | Snapshot time. | ms |
| `mode` | `TEXT` | Yes | Execution mode: `safe`, `paper`, `shadow`, or `live`. | mode |
| `armed` | `INTEGER` | Yes | Live-mode arming flag. | 0 or 1 |
| `allow_execution` | `BOOLEAN` | Yes | Whether the current mode may submit executable orders at all. | boolean |
| `allow_execution_pipeline` | `BOOLEAN` | Yes | Whether the execution pipeline is allowed to run. | boolean |
| `allow_simulation` | `BOOLEAN` | Yes | Whether simulation/shadow execution is allowed. | boolean |
| `real_trading_allowed` | `BOOLEAN` | Yes | Whether real live trading is currently allowed. | boolean |
| `allowed` | `BOOLEAN` | Yes | Alias for `allow_execution_pipeline`. | boolean |
| `reason` | `TEXT` | Yes | Primary barrier reason. | reason |
| `source` | `TEXT` | Yes | Gate source label. | source |
| `runtime_state` | `TEXT` | Yes | Lifecycle state used by the gate. | lifecycle state |
| `runtime_detail` | `TEXT` | No | Extra runtime detail. | text |
| `runtime_source` | `TEXT` | No | Runtime-state source label. | source |
| `severity` | `TEXT` | Yes | Severity classification. | severity |
| `severity_reasons` | `JSON array[string]` | Yes | Detailed severity drivers. | reasons |
| `active` | `JSON object` | No | Active kill-switch summary when present. | JSON |
| `portfolio_risk` | `JSON object` | No | Portfolio-risk payload when present. | JSON |
| `conditional_allow` | `BOOLEAN` | No | Conditional allow flag when present. | boolean |
| `disable_live_execution` | `BOOLEAN` | No | True when the `DISABLE_LIVE_EXECUTION` env emergency block is active. | boolean |

## 12. Terminal Pre-Trade Rejection Row

Producer:
- `engine.terminal.api.api_terminal_orders._record_terminal_rejection(...)`

Consumers:
- `engine.terminal.api.api_terminal.py`
- terminal charts/markers and operator diagnostics that need to explain why a manual intent was rejected

Failure if malformed:
- operators lose evidence for why a terminal order or flatten request did not become a `portfolio_orders` intent
- duplicate, cap, stale-price, or missing-price safeguards become hard to audit

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `id` | `INTEGER` | Yes | Rejection row id. | row id |
| `ts_ms` | `INTEGER` | Yes | Rejection time. | ms |
| `symbol` | `TEXT` | Yes | Requested symbol, upper-cased or `UNKNOWN`. | symbol |
| `side` | `TEXT` | No | Requested side for the rejected intent. | side |
| `qty` | `REAL` | No | Requested positive quantity. | broker quantity |
| `reason_code` | `TEXT` | Yes | Stable code such as `missing_price`, `stale_price`, `max_qty_exceeded`, `max_notional_exceeded`, or `duplicate_recent_order`. | code |
| `reason` | `TEXT` | Yes | Operator-facing reason. | text |
| `source` | `TEXT` | Yes | Source surface. Current writer uses `terminal`. | source |
| `detail_json` | `JSON object` | Yes | Price snapshot, cap, duplicate-window, or other rejection evidence. | JSON |

## 13. Broker Configuration Control Plane

Producers:
- `engine.api.api_broker_config.api_post_broker_config(...)`
- `engine.api.api_broker_config.api_post_broker_test_connection(...)`

Consumers:
- dashboard/operator broker configuration UI
- production support checks and audit review

Failure if malformed:
- operators can activate a broker without a passing test result
- encrypted credentials can be exposed or become unreadable
- broker configuration changes lose the audit trail needed for live handoff

### `GET /api/broker/config`

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Route success flag. | boolean |
| `ts_ms` | `INTEGER` | Yes | Response time. | ms |
| `config` | `JSON object` | Yes | Public broker configuration. | JSON |

The returned `config` object includes:

- `active_broker`
- `paper_live_mode`
- `active`
- `disabled`
- `failover_order`
- `base_url`
- `host`
- `port`
- `client_id`
- `timeout_s`
- `retry_policy`
- `credentials_configured`
- `masked_credentials`
- `credential_age`
- `last_test_result`
- `secrets_masked=true`

Credentials are never returned in clear text. Stored credentials live under the encrypted `broker.credentials_enc` key in `broker_meta`.

### `broker_meta`

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `key` | `TEXT` | Yes | Metadata key. Current broker config keys include `broker.config`, `broker.credentials_enc`, `broker.credentials_key_version`, and `broker.last_test`. | key |
| `value` | `TEXT` | Yes | JSON string or encrypted credential blob. | mixed |
| `updated_ts_ms` | `INTEGER` | Yes | Last update time. | ms |

### `broker_config_audit`

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `id` | `INTEGER` | Yes | Audit row id. | row id |
| `ts_ms` | `INTEGER` | Yes | Audit time. | ms |
| `action` | `TEXT` | Yes | `config_update`, `test_connection`, or a blocked activation action. | action |
| `actor` | `TEXT` | Yes | Operator/source actor. | actor |
| `active_broker` | `TEXT` | No | Broker involved in the action. | broker |
| `success` | `BOOLEAN` | Yes | Whether the action succeeded. SQLite compatibility may expose this as `0/1`. | boolean |
| `message` | `TEXT` | No | Short result message. | text |
| `detail_json` | `JSON object` | Yes | Structured details. Credentials must be omitted, encrypted, or represented only by `credentials_supplied`. | JSON |

### `POST /api/broker/config`

Mutation input may include config fields plus optional `credentials` and `actor`. The handler normalizes:

- `active_broker`
- `paper_live_mode`
- `active`
- `disabled`
- `failover_order`
- `base_url`
- `host`
- `port`
- `client_id`
- `timeout_s`
- `retry_policy`

Activation of a non-`sim` broker is blocked with `error=broker_test_required` unless the stored last test passed for the same broker.

### `POST /api/broker/test_connection`

The test response includes:

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Test result. | boolean |
| `broker` | `TEXT` | Yes | Tested broker. | broker |
| `state` | `TEXT` | Yes | `passed` or `failed`. | state |
| `latency_ms` | `REAL` | Yes | Test duration. | ms |
| `reasons` | `JSON array[string]` | Yes | Failure reasons. | reason codes |
| `tested_ts_ms` | `INTEGER` | Yes | Test timestamp. | ms |

## 14. Alert Lifecycle Rows

Producers:
- `engine.api.api_write.ack_alert(...)`
- `engine.api.api_write.shelve_alert(...)`
- `engine.api.api_write.resolve_alert(...)`

Consumers:
- dashboard alert surfaces
- operator audit and incident review

Failure if malformed:
- alert acknowledgement expiry and shelving state become ambiguous
- incident reviewers lose actor/reason/source evidence

Lifecycle rows are only valid for a parent row in `alerts`. `ack_alert(...)`, `shelve_alert(...)`, and `resolve_alert(...)` check that parent inside the write transaction and return `ok=false`, `error=not_found`, `meta.status=404` without writing lifecycle rows when the alert id is unknown. Migration `0078_alert_lifecycle_orphan_cleanup` removes legacy orphan rows from the lifecycle tables.

### `alert_acks`

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `alert_id` | `INTEGER` | Yes | Acknowledged alert id. | alert id |
| `acked_ts_ms` | `INTEGER` | Yes | Acknowledgement time. | ms |
| `acked_by` | `TEXT` | No | Operator/source actor. | actor |
| `source` | `TEXT` | No | Source surface. | source |
| `expires_ts_ms` | `INTEGER` | No | Expiry time for temporary acknowledgement. | ms |
| `reason` | `TEXT` | No | Operator reason. | text |

### `alert_shelves`

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `alert_id` | `INTEGER` | Yes | Shelved alert id. | alert id |
| `shelved_ts_ms` | `INTEGER` | Yes | Shelving time. | ms |
| `expires_ts_ms` | `INTEGER` | Yes | Expiry time. | ms |
| `shelved_by` | `TEXT` | No | Operator/source actor. | actor |
| `reason` | `TEXT` | Yes | Required shelving reason. | text |
| `source` | `TEXT` | No | Source surface. | source |
| `severity` | `TEXT` | No | Alert severity at shelving time. | severity |
| `detail_json` | `JSON object` | Yes | Structured shelving metadata, currently including duration. | JSON |

### `alert_lifecycle_events`

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `id` | `INTEGER` | Yes | Event row id. | row id |
| `alert_id` | `INTEGER` | Yes | Alert id. | alert id |
| `ts_ms` | `INTEGER` | Yes | Lifecycle event time. | ms |
| `lifecycle_state` | `TEXT` | Yes | `acknowledged`, `shelved`, or `resolved`. | state |
| `actor` | `TEXT` | No | Operator/source actor. | actor |
| `reason` | `TEXT` | No | Operator reason. | text |
| `source` | `TEXT` | No | Source surface. | source |
| `detail_json` | `JSON object` | Yes | Structured action detail. | JSON |

## 15. Operator Emergency Stop Response

Producer:
- `engine.api.api_operator_handlers.api_post_operator_emergency_stop(...)`

Consumers:
- operator UI
- operator automation

Failure if malformed:
- operators can think the runtime is stopped while the kill switch or execution disarm actually failed

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | True only when job stop succeeded and no safety errors were recorded. | boolean |
| `status` | `TEXT` | Yes | Forced to `KILL_SWITCH`. | status |
| `execution_allowed` | `BOOLEAN` | Yes | Forced to `false`. | boolean |
| `reasons` | `JSON array[string]` | Yes | Existing status reasons plus `operator_emergency_stop` and any safety errors. | reasons |
| `operator_stop` | `JSON object` | Yes | Embedded response from `api_post_operator_stop(...)`. | JSON |
| `safety_errors` | `JSON array[string]` | Yes | Errors from kill-switch activation or execution disarming. | errors |

## 16. Engine Support Snapshot

Producer:
- `engine.api.api_system.api_get_support_snapshot(...)`

Consumers:
- operator UI
- `services/operator_ai/agent.js`
- any repair workflow that needs a stable, machine-readable diagnostics bundle

Failure if malformed:
- automated diagnosis loses its stable evidence bundle
- support flows fall back to scraping multiple endpoints instead of using a single snapshot

Snapshot schema:

- `name = "operator_repair_snapshot"`
- `version = 2`
- `producer = "engine.api.api_system"`

### Top-level sections

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `snapshot_schema` | `JSON object` | Yes | Schema metadata including stable sections. | JSON |
| `snapshot_mode` | `TEXT` | Yes | Requested snapshot mode such as `repair` or `quick`. | mode |
| `system_health` | `JSON object` | Yes | Copy of the health snapshot. | JSON |
| `trading_readiness` | `JSON object` | Yes | Copy of trading readiness. | JSON |
| `preflight_report` | `JSON object` | Yes | Preflight result bundle. | JSON |
| `database_counts` | `JSON object` | Yes | Table counts used for startup/support checks. | counts |
| `database_debug` | `JSON object` | Yes | DB debug snapshot. | JSON |
| `job_registry_validation` | `JSON object` | Yes | Runtime graph validation result. | JSON |
| `job_status` | `JSON array[object]` | Yes | Job rows from the runtime snapshot. | list |
| `daemon_status` | `JSON object` | Yes | Graph/service daemon status. | JSON |
| `runtime_watchdogs` | `JSON object` | Yes | Provider, metrics, ingestion, event, label, and model watchdogs. | JSON |
| `recent_errors` | `JSON array[object]` | Yes | Recent runtime error records. | list |
| `startup_trace` | `JSON object` | Yes | Startup trace copied from `runtime_meta`. | JSON |
| `import_smoke` | `JSON object` | Yes | Import-smoke snapshot copied from `runtime_meta`. | JSON |
| `job_launch_trace` | `JSON array[object]` | Yes | Job-launch breadcrumb list. | list |
| `db_validation` | `JSON object` | Yes | DB validation details. | JSON |
| `ingestion_state` | `JSON object` | Yes | Current persisted ingestion runtime state. | JSON |
| `ingestion_state.children[*].restart_guard` | `JSON object` | No | Persisted ingestion child restart-storm sliding-window snapshot copied from `job_locks` accounting rows. Contains `count`, `limit`, `remaining`, `window_s`, `suppressed`, `suppressed_until_ts_ms`, `liveness_job`, and `updated_ts_ms`. | JSON |
| `supervisor_analysis` | `JSON object` | Yes | Supervisor analysis persisted in `runtime_meta`. | JSON |
| `failure_classification` | `JSON object` | Yes | Failure-classification payload from DB debug state. | JSON |
| `diagnostics` | `JSON object` | Yes | Operator-oriented synthesized diagnosis. | JSON |
| `evidence` | `JSON object` | Yes | Original evidence blocks used to build diagnostics. | JSON |

### `runtime_watchdogs`

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Response-envelope success for the watchdog request. Operational watchdog health is reported by `watchdogs_ok`. | boolean |
| `error` | `TEXT/null` | Yes | `null` for a successful watchdog response; transport/request errors use a string error code. | code |
| `watchdogs_ok` | `BOOLEAN` | Yes | Health-derived watchdog status for price freshness and stale critical jobs. | boolean |
| `ready` | `BOOLEAN` | Yes | Alias for `watchdogs_ok` for UI readiness checks. | boolean |
| `watchdog_reasons` | `JSON array[string]` | Yes | Reasons such as `price_feed_not_ok`, `provider_monitor_stale`, or `metrics_collector_stale`. | list |
| `ts_ms` | `INTEGER` | Yes | Snapshot time. | ms |
| `provider_monitor` | `JSON object` | Yes | Running/staleness state for the `provider_monitor` job. | JSON |
| `metrics_collector` | `JSON object` | Yes | Running/staleness state for the `metrics_collector` job. | JSON |
| `price_feed_freshness` | `JSON object` | Yes | Price freshness block copied from health. | JSON |
| `pipeline_watchdog_state` | `JSON object` | Yes | Watchdog summaries for ingestion, events, labels, and model freshness. | JSON |
| `ingestion_freshness` | `JSON object` | Yes | Ingestion freshness block. | JSON |
| `job_restart_counters` | `JSON object` | Yes | Restart counters keyed by job name. | counts |
| `job_summary` | `JSON object` | Yes | Aggregate job summary. | JSON |
| `meta.status` | `INTEGER` | Yes | HTTP status mirrored in the JSON envelope; 200 for a successfully generated watchdog snapshot. | status |

### `market_session`

`GET /api/market/session` separates exchange-clock state from data availability.

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Response-envelope success for the session request. | boolean |
| `state` | `TEXT` | Yes | Exchange-clock state, currently `OPEN` or `CLOSED`. | enum |
| `data_ready` | `BOOLEAN` | Yes | Whether the price-read path returned at least one usable latest price row. | boolean |
| `data_reason` | `TEXT` | Yes | `ok`, `no_price_rows`, or `price_read_error:<ExceptionType>`. | code |
| `data_symbol` | `TEXT/null` | Yes | Optional requested symbol used for the readiness probe. | symbol |
| `data_source` | `TEXT` | Yes | Source of the latest price row or `price_read_router` when unavailable. | source |
| `last_price_ts_ms` | `INTEGER/null` | Yes | Timestamp of the latest readable price row. | ms |
| `data_count` | `INTEGER` | Yes | Number of rows returned by the bounded readiness probe. | count |
| `ts_ms` | `INTEGER` | Yes | Snapshot time. | ms |
| `meta.status` | `INTEGER` | Yes | HTTP status mirrored in the JSON envelope. | status |
| `meta.data_ready` | `BOOLEAN` | Yes | Copy of `data_ready` for envelope-aware clients. | boolean |

Ingestion child restart-storm accounting is stored in `job_locks` with the
reserved row-name prefix `ingestion_restart_guard/v1::`. These rows are not
supervised-job liveness rows. Exits, feed-stall restarts, and spawn failures
write one expiring row per restart attempt. Health and self-repair stale-lock
scans ignore the prefix, while `ingestion_runtime` counts unexpired rows to
enforce `INGESTION_RUNTIME_CHILD_MAX_RESTARTS` within
`INGESTION_RUNTIME_CHILD_RESTART_WINDOW_S`. A data-source reload marker newer
than the latest guard attempt clears the affected persisted rows during
supervisor reconciliation.

## 17. Operator Snapshot From `boot/operator_server.js`

Producer:
- `boot/operator_server.js` via `buildOperatorSnapshot(mode)`

Consumers:
- operator-side UI
- `services/operator_ai/agent.js` via `/api/operator/snapshot?mode=quick`

Failure if malformed:
- the operator sidecar loses its view of dashboard reachability, runtime logs, stderr, and environment state

Snapshot schema:

- `name = "operator_repair_snapshot"`
- `version = 3`
- `producer = "boot/operator_server.js"`

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `snapshot_schema` | `JSON object` | Yes | Operator-side snapshot schema descriptor. | JSON |
| `snapshot_mode` | `TEXT` | Yes | Requested snapshot mode. | mode |
| `at` | `TEXT` | Yes | ISO timestamp for the snapshot. | ISO datetime |
| `operator` | `JSON object` | Yes | Operator-server state. | JSON |
| `engine` | `JSON object` | Yes | Engine/runtime status as seen from the operator sidecar. | JSON |
| `dashboard` | `JSON object` | Yes | Dashboard reachability checks, including endpoint checks. | JSON |
| `readiness` | `JSON object` | Yes | Readiness result. | JSON |
| `preflight` | `JSON object` | Yes | Operator-side preflight result. | JSON |
| `health` | `JSON object` | Yes | Health snapshot. | JSON |
| `support_snapshot` | `JSON object` | Yes | Embedded engine support snapshot. | JSON |
| `db_schema` | `JSON object` | Yes | DB schema inspection result. | JSON |
| `runtime_log_tail` | `TEXT` | Yes | Runtime log tail. | text |
| `python_stderr_tail` | `TEXT` | Yes | Python stderr tail. | text |
| `env` | `JSON object` | Yes | Sanitized environment summary. | JSON |
| `snapshot_meta` | `JSON object` | Yes | Snapshot metadata. | JSON |
| `diagnostics` | `JSON object` | Yes | Operator-side diagnostics summary. | JSON |

## 18. Diagnostics-Only Operator AI Result

Producer:
- `services/operator_ai/agent.js`

Consumers:
- callers that want an AI-normalized diagnosis but not an automated action

Failure if malformed:
- operator automation can mistake a non-actionable diagnosis for an executable fix
- postmortem logs in `var/log/ai_operator_log.jsonl` lose the normalized analysis shape

Current mutability constraint:

- `ALLOWED_ACTIONS = []` in the inspected module, so `action` is intentionally `null`.
- `analysis.patch` is normalized as a short string recommendation, not a structured find/replace patch object.

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `analysis.summary` | `TEXT` | Yes | High-level diagnosis summary. | text |
| `analysis.root_cause` | `TEXT` | Yes | Root-cause statement. | text |
| `analysis.failing_component` | `TEXT` | Yes | Named failing subsystem. | text |
| `analysis.file` | `TEXT` | Yes | File path if the diagnosis can anchor to code. | path |
| `analysis.patch` | `TEXT` | Yes | Short exact patch recommendation or empty string. | text |
| `analysis.action` | `null` | Yes | Explicitly non-executable in this module. | null |
| `action` | `null` | Yes | Top-level action, also non-executable here. | null |
| `executed` | `null` | Yes | Reserved execution result. | null |

## 19. Data-Source Control-Plane Record

Producers:
- `services.data_source_manager._materialize_source(...)`
- `services.data_source_manager.list_sources()`

Consumers:
- `routes/data_sources_routes.py`
- `ui/data_sources.js`
- ingestion job reconciliation in `engine/runtime/ingestion_runtime.py`

Failure if malformed:
- enabled sources can project the wrong env vars into jobs
- ingestion reconciliation can start the wrong jobs or miss required ones
- the data-source UI can expose the wrong editability or credential state

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `id` | `INTEGER` | Yes | Row id. | row id |
| `source_key` | `TEXT` | Yes | Stable source identifier. | key |
| `display_name` | `TEXT` | Yes | Human-readable name. | name |
| `source_type` | `TEXT` | Yes | Source category. | type |
| `provider_name` | `TEXT` | Yes | Provider identifier. | provider |
| `job_name` | `TEXT` | Yes | Ingestion job that this source feeds. | job name |
| `enabled` | `BOOLEAN` | Yes | Whether the source is active. | boolean |
| `settings` | `JSON object` | Yes | Structured source-specific settings. | JSON |
| `status` | `TEXT` | Yes | Last known status such as `ok`, `tested`, `test_failed`, `test_degraded`, or `test_unsupported`. | status |
| `last_error` | `TEXT` | No | Most recent error message. | text |
| `last_success_ts_ms` | `INTEGER` | No | Most recent success time. | ms |
| `last_test_ts_ms` | `INTEGER` | No | Most recent test time. | ms |
| `error_count` | `INTEGER` | Yes | Error counter. | count |
| `config_hash` | `TEXT` | Yes | Hash used by ingestion reconciliation to detect config changes. | hash |
| `created_ts_ms` | `INTEGER` | Yes | Creation time. | ms |
| `updated_ts_ms` | `INTEGER` | Yes | Last update time. | ms |
| `credentials_configured` | `BOOLEAN` | Yes | Whether credentials are readable and configured. | boolean |
| `credentials_stored` | `BOOLEAN` | Yes | Whether any stored credential blob exists. | boolean |
| `credential_error` | `TEXT` | No | Credential decode or read failure. | text |
| `credential_fields` | `JSON array[object]` | Yes | Credential field schema for the source template. | list |
| `setting_fields` | `JSON array[object]` | Yes | Structured setting field schema. | list |
| `masked_credentials` | `JSON object` | Yes | Masked credential preview for UI display. | JSON |
| `template_key` | `TEXT` | Yes | Source template key. | template key |
| `builtin` | `BOOLEAN` | Yes | Whether the source is built in. | boolean |
| `singleton` | `BOOLEAN` | Yes | Whether only one source of this template may exist. | boolean |
| `can_delete` | `BOOLEAN` | Yes | Whether the record can be deleted. | boolean |
| `can_edit_identity` | `BOOLEAN` | Yes | Whether display name/source key are editable. | boolean |
| `can_edit_routing` | `BOOLEAN` | Yes | Whether routing/job assignment is editable. | boolean |
| `supports_test` | `BOOLEAN` | Yes | Whether the source exposes a test operation. | boolean |
| `credentials` | `JSON object` | No | Only included when the manager is explicitly asked for full credentials. | JSON |

## 20. Data-Source List, Lifecycle, And Test Responses

Producer:
- `routes/data_sources_routes.py`

Consumers:
- `ui/data_sources.js`
- operator and support tooling

Failure if malformed:
- the UI cannot tell whether actor or token input is required
- ingestion-runtime reconciliation cannot show the desired job set after a control-plane change

### `GET /api/data_sources`

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Route success flag. | boolean |
| `ts_ms` | `INTEGER` | Yes | Response time. | ms |
| `sources` | `JSON array[object]` | Yes | Materialized source records. | list |
| `templates` | `JSON array[object]` | Yes | Enriched source template records from `list_source_templates()`. The UI renders provider guidance and field metadata from this backend-owned catalog. | list |
| `runtime` | `JSON object` | Yes | Runtime snapshot from the data-source manager. | JSON |
| `auth` | `JSON object` | Yes | Auth requirements. | JSON |
| `desired_ingestion_jobs` | `JSON array[string]` | Yes | Job names the manager wants the ingestion runtime to own. | list |

`auth` currently contains:

- `dashboard_token_configured`
- `mutation_token_required`
- `mutation_safe_dev_localhost_fallback_enabled`
- `mutation_auth_model`
- `actor_required`
- `sensitive_read_token_required`
- `read_open_on_loopback`
- `read_token_required_on_loopback`
- `read_token_required_on_lan`
- `read_fail_closed_on_remote_bind`
- `network_mode`
- `strict_reasons`
- `remote_bind_reasons`

`runtime` currently contains:

- `provider_telemetry`
- `pipeline_health`
- `updated_ts_ms`

Each `templates[]` row contains identity/routing policy, mutation policy, a `guide` object, `credential_fields[]`, and `setting_fields[]`.

`guide` contains `category`, `summary`, `needs`, `setup`, `when_enabled`, `docs_url`, `signup_url`, `plan_note`, and `safety_warnings`.

Each field object contains `field`, `env_var`/`env_name`, `label`, `help_text`, `docs_url`, `signup_url`, `plan_note`, `required`, `required_state`, `secret`, `validation_hint`, `validation_regex`, `placeholder`, `safety_warning`, and `type`/`input_type`. Secret fields are metadata only; plaintext credential values are not returned by the route.

Mutating source requests reject submitted credential, setting, or clear-field names that are not declared by the selected template. Submitted non-empty field values are checked against the optional field regex and validation failures name only the field, not the submitted value.

### Lifecycle response after create, update, delete, enable, or disable

The mutating routes all return a `lifecycle` object from `services.data_source_manager.manage_lifecycle(...)`.

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Lifecycle management success flag. | boolean |
| `reason` | `TEXT` | Yes | Human-readable reason such as `api_update:<source_key>`. | text |
| `desired_jobs` | `JSON array[string]` | Yes | Desired ingestion jobs after the mutation. | list |
| `ingestion_runtime_started` | `BOOLEAN` | Yes | Whether lifecycle management had to start ingestion runtime. | boolean |

### `POST /api/data_sources/test`

`manager.test_connection(...)` returns a structured provider-test result from the explicit provider-test registry. Passing probes update the source row to `tested`; failing probes update it to `test_failed`; fallback/partial probes update it to `test_degraded`; registered non-testable sources update it to `test_unsupported`. Only `status=pass` sets `ok=true` or updates `last_success_ts_ms`.

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Whether the registered provider probe passed. | boolean |
| `source_key` | `TEXT` | Yes | Source under test. | key |
| `status` | `TEXT` | Yes | `pass`, `fail`, `degraded`, or `unsupported`. | enum |
| `classification` | `TEXT` | Yes | Result class such as `success`, `missing_credentials`, `wrong_credentials`, `provider_unreachable`, `rate_limited`, `entitlement_missing`, `empty_payload`, `degraded_fallback`, `partial_success`, or `unsupported`. | enum |
| `message` | `TEXT` | Yes | Provider-specific result code. | text |
| `evidence` | `JSON object` | Yes | Sanitized non-secret probe evidence. Missing credentials include exact `missing_env_vars` plus catalog guidance in `missing_credentials`. | JSON |
| `next_steps` | `JSON array[string]` | Yes | Operator remediation guidance. | list |
| `error` | `TEXT` | No | Present when `ok=false`; mirrors `message`. | text |

Before each probe the manager clears `engine.data._credentials.get_data_credential()` cache and resolves credentials through the ingestion path: projected source credentials, inherited provider-account credentials, strict runtime credential files, external `<ENV>_FILE`, `<ENV>_SECRET`, secret provider, and compatible plain env. HTTP 429 and 503 responses immediately return degraded results with retry guidance and `evidence.stop_testing=true`; composite providers do not continue to fallback probes after those statuses.

### `POST /api/data_sources/test_save`

The Test & Save route accepts the same source mutation payload as create/update
plus optional `create=true`. It validates input, encrypts credentials, clears
the credential cache, runs the registered liveness probe, and returns:

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Mirrors `test.ok` when save succeeded; false when save failed. | boolean |
| `saved` | `BOOLEAN` | Yes | Whether the submitted source payload was stored. | boolean |
| `source_key` | `TEXT` | No | Source that was saved and tested. | key |
| `source` | `JSON object` | No | Updated source response without plaintext credentials. | JSON |
| `test` | `JSON object` | No | Structured `/api/data_sources/test` result. | JSON |
| `error` | `TEXT` | No | Save failure or test failure code; must not include submitted credential values. | text |

### `GET /api/data_sources/logs`

`manager.list_logs(...)` returns rows through `engine.runtime.telemetry_read_router.fetch_data_source_logs(...)`. The detail object is sanitized on read even though `engine.runtime.data_source_log_store` already sanitizes before persistence.

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Route success flag. | boolean |
| `source_key` | `TEXT` | Yes | Source whose log stream was requested. | key |
| `logs` | `JSON array[object]` | Yes | Newest-first data-source log rows. | list |

Each `logs[]` row contains `ts_ms`, `source_key`, `level`, `event_type`, `message`, and `detail`. Credential-bearing keys inside `detail`, including `credentials`, `credentials_enc`, `api_key`, `api_token`, `client_secret`, `secret`, `token`, and `password`, must be returned as `[REDACTED]`. Non-secret status fields must remain unchanged.

## 21. Job Catalog API Contract

Producer:
- `engine.runtime.job_catalog.build_job_catalog(...)`
- `engine.api.api_jobs.api_get_jobs(...)`
- `engine.api.api_jobs.api_get_jobs_catalog(...)`

Consumers:
- dashboard Job Console and Job Catalog
- command palette job actions
- operator/support tooling that needs to discover registered jobs

Failure if malformed:
- operators cannot discover registered jobs or understand prerequisites
- browser surfaces may mislabel dangerous job starts
- execution-sensitive or destructive/admin jobs may appear unguarded

`GET /api/jobs` remains backward-compatible and still returns `jobs`, `pipeline_order`, and `allowed`. Each job row now also carries the catalog fields below. `GET /api/jobs/catalog` returns the same row contract in both `jobs` and `catalog`; when the jobs manager is unavailable it returns the static registry catalog with `status = "static"`.

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `id` | `TEXT` | Yes | Stable job id; same value as `name`. | job name |
| `name` | `TEXT` | Yes | Registered job name. | job name |
| `label` | `TEXT` | Yes | Human-readable label derived from the registry name. | text |
| `group` | `TEXT` | Yes | Registry group such as `price_feed`, or empty string. | group |
| `workflow` | `TEXT` | Yes | Operator grouping used by the dashboard catalog. | group |
| `script` | `TEXT` | Yes | Repository-relative Python entrypoint. | path |
| `module` | `TEXT` | Yes | Python module path derived from `script`. | module |
| `mode` | `TEXT` | Yes | `daemon` or `oneshot`. | mode |
| `schedule` | `TEXT` | Yes | Registry schedule/cadence text when available. | text |
| `cadence_seconds` | `INTEGER/null` | Yes | Numeric cadence when available. | seconds |
| `stage` | `TEXT` | Yes | Pipeline/default stage when available. | stage |
| `owner_subsystem` | `TEXT` | Yes | Owning subsystem derived from script path. | subsystem |
| `dependencies` | `JSON array[string]` | Yes | Explicit registry dependencies or recovery jobs. | list |
| `required_secrets` | `JSON array[string]` | Yes | Secret names that must all be configured. Values are never returned. | list |
| `required_secret_any` | `JSON array[string]` | Yes | Secret alternatives where any one satisfies the prerequisite. | list |
| `required_providers` | `JSON array[string]` | Yes | Provider names inferred from required secrets or job path/name. | list |
| `missing_prerequisites` | `JSON array[object]` | Yes | Missing secret prerequisites. | list |
| `prerequisites` | `JSON object` | Yes | Structured prerequisite summary with `ok`, required fields, providers, and missing entries. | JSON |
| `safety` | `TEXT` | Yes | One of `read_only`, `data_refresh`, `training_research`, `execution_sensitive`, `destructive_admin`, or `unavailable`. | class |
| `base_safety` | `TEXT` | Yes | Safety classification before prerequisite availability is applied. | class |
| `execution_sensitivity` | `TEXT` | Yes | `live_execution`, `admin_destructive`, or `none`. | class |
| `resource_class` | `TEXT` | Yes | Registry or derived runtime resource class. | class |
| `purpose` | `TEXT` | Yes | Operator-facing explanation of what the job does. | text |
| `latest_run` | `JSON object` | No | Merged live/history state when available. | JSON |
| `log_url` | `TEXT` | Yes | Read API for recent job logs. | URL path |
| `history_url` | `TEXT` | Yes | Read API for job history. | URL path |
| `last_output_url` | `TEXT` | Yes | Current operator link for latest output/log inspection. | URL path |
| `action_policy` | `JSON object` | Yes | Backend-owned start/stop enablement, confirmation, and disabled-reason policy. | JSON |

Job starts for `execution_sensitive` or `destructive_admin` jobs require a backend confirmation payload with `confirmation = "JOB_ACTION"` and `consequence_ack = true`. Jobs whose `safety` is `unavailable` are rejected by the API handler even when a browser attempts to submit the action.

## 22. Governance Evidence Center API

Producer:
- `engine.api.governance_evidence.build_governance_evidence_summary(...)`
- route wrappers in `engine.api.api_governance` and `dashboard_server.py`

Consumers:
- dashboard Governance Evidence Center
- operator/support tooling that needs current promotion, generated-candidate, model-risk, monitoring, and shadow-capital evidence without reading logs or raw files

Routes:
- `GET /api/governance/evidence`
- `GET /api/governance/evidence/promotion_blockers`
- `GET /api/governance/evidence/generated_candidates`
- `GET /api/governance/evidence/shadow_capital`
- `GET /api/governance/shadow_capital/scores`

Production enforcement:
- the evidence routes are read-only sensitive GET routes
- they do not promote challengers, train models, recompute shadow-capital scores, allocate capital, or arm execution
- promotion remains gated by `promotion_guard`, `champion_manager`, `strategy_promotion_governance`, OPE, experiment-ledger, replay, statistical, and audit controls
- allocation and execution remain gated by the runtime allocator, execution barrier, risk, kill-switch, and broker controls

### Evidence Summary Envelope

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Route success flag. | boolean |
| `ts_ms` | `INTEGER` | Yes | Response time. | ms |
| `state` | `TEXT` | Yes | Overall evidence state: `pass`, `block`, or `unknown`. | enum |
| `authority` | `JSON object` | Yes | Read-only authority note and the backend control modules that remain authoritative. | JSON |
| `evidence` | `JSON array[object]` | Yes | One row per evidence producer. | list |
| `blockers` | `JSON array[object]` | Yes | Evidence rows whose state is `block`. | list |
| `unknowns` | `JSON array[object]` | Yes | Evidence rows whose state is `unknown`. | list |
| `promotion_blockers` | `JSON object` | Yes | Exact promotion guard and evidence blockers. | JSON |
| `generated_candidates` | `JSON object` | Yes | Experiment-ledger provenance rows and candidate-level blockers. | JSON |
| `production_monitoring` | `JSON object` | Yes | Latest production-monitoring payload used by the evidence rows. | JSON |
| `shadow_capital` | `JSON object` | Yes | Masked shadow-capital score payload. | JSON |
| `drilldowns` | `JSON object` | Yes | URL paths for the detail routes. | JSON |

### Evidence Row

Every row in `evidence`, `blockers`, and `unknowns` uses:

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `key` | `TEXT` | Yes | Stable evidence id such as `ope_gate`, `experiment_ledger`, `net_after_cost_labels`, `learned_alpha_decay`, `alpha_shrinkage`, `production_monitoring`, `shadow_live_monitoring`, or `shadow_capital_scores`. | id |
| `label` | `TEXT` | Yes | Operator-facing evidence label. | text |
| `state` | `TEXT` | Yes | `pass`, `block`, or `unknown`. Missing, stale, failed, or insufficient required evidence is `block`. | enum |
| `freshness` | `TEXT` | Yes | `fresh`, `stale`, `missing`, or `unknown`. | enum |
| `sample_count` | `INTEGER` | Yes | Evidence-specific row/sample count. | count |
| `last_update_ts_ms` | `INTEGER` | Yes | Latest source timestamp, or `0` when missing. | ms |
| `source_artifact` | `TEXT` | Yes | Exact table/meta source, optionally with row id, such as `policy_ope_evidence#12` or `runtime_meta.last_alpha_shrinkage`. | source |
| `remediation` | `TEXT` | Yes | Concrete operator/maintainer action to refresh or repair missing evidence. | text |
| `details` | `JSON object` | Yes | Source-specific metrics, gate reasons, thresholds, or selected source rows. | JSON |

### Generated-Candidate Provenance

`GET /api/governance/evidence/generated_candidates` returns latest `experiment_ledger` rows with feature ids, prompt/model hashes when available, search-space metadata, trial budget/count, CPCV/PBO/DSR/FDR evidence, redundancy evidence, promotion decision, and `blockers`.

Candidate rows are `block` when the latest ledger evidence is missing or has non-passing promotion decision, missing trial budget, missing trial count, exceeded trial budget, missing statistical evidence, or missing redundancy checks.

### Promotion Blockers

`GET /api/governance/evidence/promotion_blockers` returns:

- `guard.allowed` and exact `promotion_guard` blockers
- `guard.reason` from the backend guard
- `evidence_blockers` derived from the evidence row contract

This route explains blockers only. It does not change the promotion switch or override any guard.

### Shadow-Capital Scores

`GET /api/governance/evidence/shadow_capital` and `GET /api/governance/shadow_capital/scores` return:

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Route success flag. | boolean |
| `regime` | `TEXT` | Yes | Requested regime, default `global`. | regime |
| `rows` | `JSON array[object]` | Yes | Whitelisted score rows from `shadow_capital_scores`. | list |
| `masking` | `JSON object` | Yes | Masking metadata. Current policy is `score_fields_only`; sensitive component keys such as account, broker-account, credential, password, secret, and token are omitted. | JSON |
| `evidence` | `JSON object/null` | Yes | Evidence row for shadow-capital freshness and sample sufficiency. | JSON |
| `authority` | `TEXT` | Yes | `read_only_governance_evidence`. | enum |

Per-row fields include `ts_ms`, `window_s`, `regime`, `model_name`, optional `model_kind`/`model_ts_ms`, `n`, `rmse`, `dir_acc`, `net_rmse`, slippage metrics, execution-latency metrics when the schema has them, `drawdown_proxy`, `cap_eff`, PnL fields, `score`, sanitized numeric/string `components`, and `source_artifact`.

## 23. Portfolio Backtest API Payload

Producers:
- `engine.api.api_read_advanced.get_latest_portfolio_backtest()`
- `engine.strategy.portfolio_backtest.run_backtest()`
- market-data ingestion jobs that populate `prices`

Consumers:
- `ui/portfolio_backtest.js`
- `ui/pro_chart_engine.js` as a fallback portfolio-equity overlay source
- operator diagnostics that compare latest backtest output to live or paper equity

Failure if malformed:
- the portfolio backtest chart can imply outperformance without a real market baseline
- missing benchmark prices can look like a broken chart instead of an unavailable data source
- zero or null portfolio values can be treated as real observations and distort the equity curve

`GET /api/portfolio/backtest/latest` returns the latest persisted run:

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Route success flag. `false` means no run tables or no latest run. | boolean |
| `run.id` | `INTEGER` | Yes when `ok=true` | Portfolio backtest run id. | id |
| `run.ts_ms` | `INTEGER` | Yes when `ok=true` | Run creation timestamp. | ms |
| `run.start_ts_ms` | `INTEGER` | Yes when `ok=true` | Backtest window start. | ms |
| `run.end_ts_ms` | `INTEGER` | Yes when `ok=true` | Backtest window end. | ms |
| `run.metrics` | `JSON object` | Yes when `ok=true` | Persisted run metrics. | mixed |
| `run.points` | `JSON array[object]` | Yes when `ok=true` | Ordered portfolio curve points. | array |
| `run.points[].ret` | `REAL/null` | Yes | Step return. Missing database values are JSON `null`, not `0.0`. | return fraction |
| `run.points[].equity` | `REAL/null` | Yes | Portfolio equity at the point. Missing database values are JSON `null`. | equity units |
| `run.points[].drawdown` | `REAL/null` | Yes | Drawdown at the point. Missing database values are JSON `null`. | fraction |
| `run.points[].detail` | `JSON object` | Yes | Position, cost, and marker detail decoded from `detail_json`. | JSON |
| `run.benchmark` | `JSON object` | Yes when `ok=true` | Optional benchmark overlay metadata and points. | object |
| `meta.benchmark_ready` | `BOOLEAN` | Yes | Mirrors `run.benchmark.available`. | boolean |
| `meta.benchmark_symbol` | `TEXT` | Yes | Current canonical benchmark symbol. | symbol |

The canonical benchmark is `SPY` from the production `prices` table. The API reads raw benchmark price with `COALESCE(price, px)` for rows between the first and last finite portfolio-equity points in the run, capped to 3000 ordered points. It emits `run.benchmark.points[].value` normalized as:

`normalized_value = raw_spy_price / first_raw_spy_price * first_portfolio_equity`

`run.benchmark.available` is `true` only when at least two usable SPY prices are available. Otherwise `points` is empty or insufficient, the route still returns `ok=true` for the portfolio run, and `run.benchmark.unavailable_reason` explains the missing overlay with reason codes such as `prices_table_missing`, `benchmark_prices_missing`, `benchmark_prices_insufficient`, or `portfolio_start_value_missing`.

## 24. Market Stress API Payloads

Producers:
- `engine.strategy.market_stress.get_market_stress_snapshot(...)`
- `dashboard_server.api_get_market_stress(...)`
- `dashboard_server.api_get_market_stress_history(...)`

Consumers:
- `ui/market_stress.js`
- operator overview and runtime summary surfaces that display the market condition
- strategy and capital guards that read the stress snapshot directly

Failure if malformed:
- stress spikes above the base normalized range can render off-canvas or disappear from the operator sparkline
- warning/critical badges can disagree with chart reference lines
- GDELT conflict stress can be silently misread as a clamped `0..1` score

`/api/market_stress` returns:

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Whether the snapshot request succeeded. | boolean |
| `stress` | `JSON object` | Yes | Latest market-stress snapshot. Empty on error. | object |
| `thresholds.warning` | `REAL` | Yes | Score where the UI enters elevated/warning stress. | stress score |
| `thresholds.critical` | `REAL` | Yes | Score where the UI enters high/critical stress. | stress score |
| `error` | `TEXT` | No | Error detail when `ok=false`. | message |

`stress` includes the cross-asset component levels and z-scores produced by `engine.strategy.market_stress`. The base cross-asset `stress_score` is normalized to `0..1`, but the optional GDELT conflict adjustment is added after that normalization. Therefore the final exposed `stress_score` is non-negative and may exceed `1.0`; consumers must preserve the real magnitude and scale visualizations dynamically rather than clamping the value.

`/api/market_stress_history` returns:

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Whether the history request succeeded. | boolean |
| `series` | `JSON array[object]` | Yes | Ordered historical stress points. Empty when source prices are unavailable. | array |
| `series[].ts_ms` | `INTEGER` | Yes | Timestamp for the stress point. | ms |
| `series[].stress_score` | `REAL` | Yes | Historical stress score, preserving post-GDELT values above `1.0`. | stress score |
| `thresholds.warning` | `REAL` | Yes | Same warning threshold used by badges and chart reference lines. | stress score |
| `thresholds.critical` | `REAL` | Yes | Same critical threshold used by badges and chart reference lines. | stress score |
| `ready` | `BOOLEAN` | Yes when `series=[]` | `false` for a reasoned empty history. | boolean |
| `reason` | `TEXT` | Yes when `series=[]` | `prices_table_missing` when the source table is absent, or `no_market_stress_history_yet` when it is present but has no VIX history. | code |
| `source` | `TEXT` | Yes when `series=[]` | Source table used for the history read, normally `prices`. | table |
| `table_present` | `BOOLEAN` | Yes when `series=[]` | Distinguishes absent source table from present-but-empty source. | boolean |
| `meta.ready` | `BOOLEAN` | Yes when `series=[]` | Mirrors top-level `ready`. | boolean |
| `meta.reason` | `TEXT` | Yes when `series=[]` | Mirrors top-level `reason`. | code |
| `error` | `TEXT` | No | Error detail when `ok=false`. | message |

## 25. Optional Dashboard Read Empty-State Markers

The dashboard read endpoints below are optional in safe/sim and may be empty
before training, promotion, or ingestion has produced rows. When they are empty
but the read succeeds, the response must include `ready:false`, `reason`,
`source`, `table_present`, and matching `meta.ready` / `meta.reason` fields:

| Endpoint(s) | Empty payload key | Missing-table reason | Present-empty reason |
| --- | --- | --- | --- |
| `/api/promotion_audit`, `/api/promotion/audit` | `data:[]`, `rows:[]`, `audit:[]` | `model_promotion_audit_table_missing` | `no_promotions_yet` |
| `/api/governance/summary`, `/api/promotion/explain` | `audit:[]` plus `audit_meta` | `model_promotion_audit_table_missing` | `no_promotions_yet` |
| `/api/relevance_stats`, `/api/relevance/stats` | `stats:{}` | `labels_table_missing` | `relevance_stats_no_labels_yet` |
| `/api/strategy_metrics`, `/api/strategy/metrics` | `data:[]`, `rows:[]`, `strategies:[]` | `strategy_metrics_table_missing` | `no_strategy_metrics_yet` |
| `/api/causal/scores` | `data:[]`, `rows:[]` | `causal_scores_table_missing` | `no_causal_scores_yet` |
| `/api/size_policy`, `/api/strategy/size_policy` | `policy:null`, `points:[]` | `size_policy_table_missing` | `size_policy_untrained` |

If a source table exists but lacks required columns, the endpoint returns an
explicit schema reason such as `strategy_metrics_schema_incomplete`. If size
policy rows exist but are not yet usable because supporting evidence is absent,
the size-policy read returns `size_policy_not_ready` or
`size_policy_points_table_missing` while keeping `policy:null`.
