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

Runtime storage note: production and production-like operation use the Postgres-backed facade in `engine/runtime/storage_pg.py`. SQLite remains an isolated Python test backend and a compatibility dialect for older call sites; it is not the production source of truth.

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
| `from_side` | `TEXT` | No | Prior side when known. | side |
| `to_side` | `TEXT` | No | Target side when known. | side |
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

## 9. Position Contracts

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

## 10. Runtime Execution Barrier Snapshot

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
- When `DISABLE_LIVE_EXECUTION` is truthy, live-mode snapshots return `reason=disable_live_execution_env`, `allowed=false`, and `real_trading_allowed=false` even if runtime state is `LIVE` and armed.

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

## 11. Terminal Pre-Trade Rejection Row

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

## 12. Broker Configuration Control Plane

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

## 13. Alert Lifecycle Rows

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

## 14. Operator Emergency Stop Response

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

## 15. Engine Support Snapshot

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
| `supervisor_analysis` | `JSON object` | Yes | Supervisor analysis persisted in `runtime_meta`. | JSON |
| `failure_classification` | `JSON object` | Yes | Failure-classification payload from DB debug state. | JSON |
| `diagnostics` | `JSON object` | Yes | Operator-oriented synthesized diagnosis. | JSON |
| `evidence` | `JSON object` | Yes | Original evidence blocks used to build diagnostics. | JSON |

### `runtime_watchdogs`

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Health-derived top-level status. | boolean |
| `ts_ms` | `INTEGER` | Yes | Snapshot time. | ms |
| `provider_monitor` | `JSON object` | Yes | Running/staleness state for the `provider_monitor` job. | JSON |
| `metrics_collector` | `JSON object` | Yes | Running/staleness state for the `metrics_collector` job. | JSON |
| `price_feed_freshness` | `JSON object` | Yes | Price freshness block copied from health. | JSON |
| `pipeline_watchdog_state` | `JSON object` | Yes | Watchdog summaries for ingestion, events, labels, and model freshness. | JSON |
| `ingestion_freshness` | `JSON object` | Yes | Ingestion freshness block. | JSON |
| `job_restart_counters` | `JSON object` | Yes | Restart counters keyed by job name. | counts |
| `job_summary` | `JSON object` | Yes | Aggregate job summary. | JSON |

## 16. Operator Snapshot From `boot/operator_server.js`

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

## 17. Diagnostics-Only Operator AI Result

Producer:
- `services/operator_ai/agent.js`

Consumers:
- callers that want an AI-normalized diagnosis but not an automated action

Failure if malformed:
- operator automation can mistake a non-actionable diagnosis for an executable fix
- postmortem logs in `data/ai_operator_log.jsonl` lose the normalized analysis shape

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

## 18. Data-Source Control-Plane Record

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
| `status` | `TEXT` | Yes | Last known status such as `ok`, `tested`, `error`, or `test_failed`. | status |
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

## 19. Data-Source List, Lifecycle, And Test Responses

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
| `templates` | `JSON array[object]` | Yes | Source template records from `list_source_templates()`. | list |
| `runtime` | `JSON object` | Yes | Runtime snapshot from the data-source manager. | JSON |
| `auth` | `JSON object` | Yes | Auth requirements. | JSON |
| `desired_ingestion_jobs` | `JSON array[string]` | Yes | Job names the manager wants the ingestion runtime to own. | list |

`auth` currently contains:

- `token_required`
- `actor_required`

`runtime` currently contains:

- `provider_telemetry`
- `pipeline_health`
- `updated_ts_ms`

### Lifecycle response after create, update, delete, enable, or disable

The mutating routes all return a `lifecycle` object from `services.data_source_manager.manage_lifecycle(...)`.

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | Lifecycle management success flag. | boolean |
| `reason` | `TEXT` | Yes | Human-readable reason such as `api_update:<source_key>`. | text |
| `desired_jobs` | `JSON array[string]` | Yes | Desired ingestion jobs after the mutation. | list |
| `ingestion_runtime_started` | `BOOLEAN` | Yes | Whether lifecycle management had to start ingestion runtime. | boolean |

### `POST /api/data_sources/test`

`manager.test_connection(...)` returns one of two stable shapes.

Success shape:

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | `true`. | boolean |
| `source_key` | `TEXT` | Yes | Source under test. | key |
| `message` | `TEXT` | Yes | Success detail. | text |
| `...extra` | mixed | No | Provider-specific test output. | mixed |

Failure shape:

| Field | Type | Req | Meaning | Units |
| --- | --- | --- | --- | --- |
| `ok` | `BOOLEAN` | Yes | `false`. | boolean |
| `source_key` | `TEXT` | Yes | Source under test. | key |
| `error` | `TEXT` | Yes | Failure detail. | text |
| `...extra` | mixed | No | Provider-specific failure output. | mixed |
