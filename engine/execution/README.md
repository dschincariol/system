# Execution Subsystem

The `engine/execution/` tree turns approved portfolio or strategy intents into broker-facing actions while enforcing execution safety.

## Responsibilities

- execution-mode gating
- kill switches and safety barriers
- broker routing and broker-specific adapters
- order persistence and idempotency
- execution cost and liquidity modeling
- fill polling and attribution
- execution-focused training and analytics jobs

## Core Files

- [broker_router.py](broker_router.py)
  Broker selection and broker abstraction boundary.
- [broker_sim.py](broker_sim.py)
  Simulation broker used in non-live modes.
- [kill_switch.py](kill_switch.py)
  Execution safety switches.
- [execution_mode.py](execution_mode.py)
  Runtime execution-mode state and policy.
- [execution_policy_engine.py](execution_policy_engine.py)
  Policy layer before actual broker submission.
- [contextual_bandit_slicer.py](contextual_bandit_slicer.py)
  Execution-only contextual-bandit prototype for bounded slice percentage,
  participation, interval, and entry-delay adjustments after the execution
  policy has already approved symbol, side, and size.
- [execution_poll_and_attrib.py](execution_poll_and_attrib.py)
  Polling fills and attribution path.
- [execution_ledger.py](execution_ledger.py)
  Shared persistence and read helpers for broker order state, fills, and lifecycle evidence.
  The ledger owns a single `_table_exists` helper for its internal schema probes. Its public `audit_execution_integrity()` payload is covered by a golden-shape regression test so downstream diagnostics can rely on stable keys during later refactors.
- [execution_ledger_serialization.py](execution_ledger_serialization.py)
  Pure JSON, numeric coercion, strategy/model identity, and trade-outcome label
  helpers extracted from `execution_ledger.py`. The ledger facade still exports
  the legacy helper names and delegates to this module; schema, idempotency,
  fill replay, and accounting remain owned by `execution_ledger.py`.
- [trade_attribution_ledger.py](trade_attribution_ledger.py)
  Post-trade attribution ledger.
- [almgren_chriss.py](almgren_chriss.py)
  Optional transaction-cost estimator used by newer execution-analytics and slicing decisions.
  Opt-in, env-gated (`ALMGREN_CHRISS_*` knobs) Almgren-Chriss impact estimator for simulation/validation; exposes `estimate_almgren_chriss_costs()`. See "Almgren-Chriss Modules" below for how it differs from the cost-model dataclass of the same basename.
- [cost_models/almgren_chriss.py](cost_models/almgren_chriss.py)
  `AlmgrenChrissCost` expected market-impact cost-model dataclass used by simulation and backtest cost accounting. See "Almgren-Chriss Modules" below.
- [position_reconcile.py](position_reconcile.py)
  Pre-live broker-vs-runtime position reconciliation gate. `pre_live_position_reconcile(...)` reconciles broker positions against a stored confirmed baseline before live orders and, on out-of-tolerance mismatch or persistent position-fetch failure, trips the global kill switch and blocks execution. Env-gated by `EXECUTION_PRELIVE_RECONCILE`, baseline/bootstrap tokens, and `POSITION_RECONCILE_*` tolerances; also exposes `position_reconcile_evidence_snapshot(...)` for operator diagnostics.
- [order_idempotency.py](order_idempotency.py)
  Order idempotency / dedup module behind the "Broker-Native Idempotency" section below. Owns the `execution_order_idempotency` table and the durable claim/mark contract: `compute_order_uid`, `make_client_order_id`, `claim_order_submission` (and its `_durable` variant), the open-order replacement claim helpers, and the `mark_order_submission_submitted` / `_unrecorded` / `_unknown` markers (each with a `_durable` variant) used by Alpaca, IBKR, and the open-order manager.
- [trade_suppression_engine.py](trade_suppression_engine.py)
  Execution-degradation suppression engine. `evaluate_trade_suppression(...)` scores false-positive streaks, slippage/latency z-scores, and the execution-degradation snapshot into one of three tiers — `HARD_BLOCK` (size multiplier 0.0), `SOFT_THROTTLE`, or `SIZE_COMPRESSION` — and persists the resulting runtime suppression state and audit detail.
- [execution_analytics_engine.py](execution_analytics_engine.py)
  Post-trade execution analytics and slippage-attribution engine. Computes realized slippage vs decision reference price, alpha decay at fill, TTL-breach detection, cancel/replace impact, aggressiveness attribution, and broker performance stats; exposes `build_execution_analytics(...)`, `get_execution_degradation_snapshot(...)`, slippage/latency z-scores, rolling expectancy, adaptive half-life, and `rank_brokers_by_cost(...)`.
- [execution_microstructure.py](execution_microstructure.py)
  Execution microstructure layer that maintains the open-order registry and reprices/replaces limit orders by attempt and aggressiveness. Fail-soft (never throws, never blocks other jobs); exposes `manage_open_orders()`, `record_open_order(...)`, `verify_cancel_before_replace(...)`, `try_native_limit_replace(...)`, and `mark_cancel_replace_needs_reconcile(...)` (see "Cancel/Replace Safety" below).
- [broker_fill_utils.py](broker_fill_utils.py)
  Normalization helpers that turn broker-specific fill payloads into common execution records.
- [broker_alpaca_rest.py](broker_alpaca_rest.py)
  Alpaca adapter used for broker-side order submission and status reads.
- [broker_ibkr_gateway.py](broker_ibkr_gateway.py)
  IBKR gateway adapter used by live broker-routing paths.
- [fx_session.py](fx_session.py)
  Pure FX 24/5 session and rollover-timing helper derived from the canonical
  FX-04 clock in `engine.data.prices.fx_clock`.
- [broker_apply_orders.py](broker_apply_orders.py)
  Main order-application path that enforces execution barriers, intent loading, shaping, and broker submission.
- [broker_failover_policy.py](broker_failover_policy.py)
  Broker alias normalization, live failover-chain validation, broker environment contract checks, startup broker preflight, and non-retryable broker failure classification.
- [broker_submission_recovery.py](broker_submission_recovery.py)
  Recovery marker and fail-closed gate for broker-accepted submissions that could not be durably recorded locally.
- [execution_ai_advisor.py](execution_ai_advisor.py)
  Advisory-only read layer that summarizes historical slippage/latency and persists operator-facing execution guidance.
- [lob_simulation.py](lob_simulation.py)
  Reactive LOB simulation helpers and shadow-only DeepLOB-style readiness/signal path for execution timing and adverse-selection research.

## Important Constraint

In `safe` mode, execution is intentionally blocked. If the dashboard shows execution as not started while the runtime is in `safe`, that is normally expected behavior rather than a startup failure.

Live broker routing has additional fail-closed constraints:

- Execution modes are parsed through `mode_safety.py`. Canonical modes are
  `safe`, `paper`, `shadow`, and `live`; known development aliases normalize to
  `safe`, and simulation aliases normalize to `paper`. Unknown, malformed, or
  empty non-default mode inputs are invalid and fail closed as
  `invalid_execution_mode`.
- `broker_router.py` enforces execution mode again at the live broker boundary
  before any Alpaca or IBKR adapter is called. Shadow and paper modes can record
  audit/simulation evidence, but they cannot route live broker submissions even
  if an upstream caller forgets a mode check.
- `BROKER`, `BROKER_NAME`, `LIVE_BROKER`, and every `BROKER_FAILOVER` entry must identify the same intended live broker.
- Live failover chains must not include `sim`, `paper`, `sandbox`, or mixed live brokers.
- `DISABLE_LIVE_EXECUTION` blocks real trading when it is unset or set to any value other than `0`, `false`, `no`, or `off`.
- Pre-live reconciliation must run in live mode unless the break-glass environment contract is explicitly filled and audited.
- `broker_apply_orders.py` revalidates aggregate gross and net exposure in
  `apply_execution_risk_governor(...)` after EPE shaping and model/kill-switch
  gates but before the broker router or any broker adapter sees the payload.
  The check uses the final broker-bound orders plus local `broker_positions`
  and pending `broker_order_state` rows. It resizes orders when there is
  remaining headroom, suppresses orders when there is no headroom, and blocks
  fail-closed on missing or invalid exposure data.
- Broker auth/configuration failures and unrecorded broker submissions stop failover instead of retrying into another broker.

## FX Execution Path

FX execution is IBKR-only in this workstream. The broker boundary constructs IBKR
Forex `CASH` contracts on `IDEALPRO` through `broker_ibkr_gateway.py`, using
FX-02 instrument parsing for base/quote semantics. Equity symbols continue to
build `STK`/`SMART` contracts. OANDA execution is intentionally not implemented
here; OANDA remains a read-only pricing/data concern until a separate execution
workstream explicitly adds and gates it.

`broker_router.py` keeps the existing failover-chain validation and execution
gates, then prefers the FX-capable broker (`ibkr`) at the front of that already
validated chain when the batch contains FX spot pairs. The same execution-mode,
kill-switch, pre-live reconcile, live broker contract, options, and RL-source
guards still run before any adapter can submit.

`fx_session.py` derives weekend boundaries from `engine.data.prices.fx_clock`,
the FX-04 canonical clock. The shared default is Sunday 17:00 ET open and Friday
17:00 ET close, which maps to roughly 21:00 UTC during US daylight time and
22:00 UTC during US standard time. `execution_policy_engine.py` suppresses
weekend-closed FX orders via the existing suppression/audit path and biases
daily-rollover FX orders toward passive limit timing with additional delay.
Spread, swap, carry, and P&L cost accounting remain FX-07 scope.

## Equity Market Session

`equity_session.py` is the pure US equity regular-trading-hours clock used by
the execution policy layer. It mirrors the `engine.data.prices.fx_clock`
structure: all boundaries are anchored in `ZoneInfo("America/New_York")`, with
a fixed-offset ET fallback only if zoneinfo is unavailable. The default regular
session is 9:30 a.m. ET inclusive through 4:00 p.m. ET exclusive, and seeded
half-days close at 1:00 p.m. ET.

The policy engine applies this clock only when `asset_map.asset_class_for_symbol`
returns `EQUITY` and `EPE_EQUITY_SESSION_ENFORCE=1` (default on). Closed seeded
sessions, holidays, and weekends are suppressed through the existing
`_log_suppression_event` / `log_suppression` / `execution_policy_audit` path;
there is no new table or `storage.py` schema change. Near-close and half-day
equity orders that remain in-session are biased toward passive limit timing by
adjusting the already-computed execution decision before broker-bound fields are
read. Non-equity symbols, `UNKNOWN` symbols, and `EPE_EQUITY_SESSION_ENFORCE=0`
keep the legacy shape and audit behavior.

Operator knobs are `EPE_EQUITY_SESSION_ENFORCE`, `EQUITY_RTH_OPEN_HOUR_ET`,
`EQUITY_RTH_OPEN_MIN_ET`, `EQUITY_RTH_CLOSE_HOUR_ET`,
`EQUITY_RTH_CLOSE_MIN_ET`, `EQUITY_HALFDAY_CLOSE_HOUR_ET`,
`EQUITY_HALFDAY_CLOSE_MIN_ET`, `EQUITY_MARKET_HOLIDAYS_JSON`,
`EQUITY_MARKET_HALFDAYS_JSON`, `EQUITY_NEAR_CLOSE_BIAS_MIN`, and
`EQUITY_SESSION_UNKNOWN_YEAR_POLICY`. The JSON override variables accept ISO
date arrays such as `["2029-07-03"]` so operators can extend calendars without
code changes.

The hand-rolled holiday and half-day seed covers the NYSE/ICE published
2025-2028 calendars from the NYSE holidays/hours page and ICE NYSE Group
holiday-calendar releases. For years outside the seeded or overridden table,
the session state reports `holiday_table_covered=false`. The default unknown-year
policy is `open_rth` to preserve permissive legacy behavior; setting
`EQUITY_SESSION_UNKNOWN_YEAR_POLICY=fail_closed` blocks such equity orders until
the calendar is extended.

Residual: the US equity Almgren-Chriss coefficients in
`cost_models/almgren_chriss.py` are still calibrated for regular-hours trading.
EQ-04 prevents or annotates out-of-session emission; recalibrating those
coefficients outside RTH is separately owned.

### Per-Broker Share Rounding And Minimum Notional

`share_rounding.py` owns the runtime-only equity share-count normalization at
the broker boundary. It is opt-in through `EXEC_USE_SHARE_ROUNDING=1`; with the
flag unset or false, the adapters keep the legacy quantity exactly unchanged.

When enabled, IBKR defaults to whole-share equity orders
(`EXEC_IBKR_SHARE_INCREMENT=1`) because fractional share support is limited by
venue/order type, while Alpaca defaults to fractional quantities
(`EXEC_ALPACA_SHARE_INCREMENT=0`). The simulator uses
`EXEC_SIM_ROUNDING_BROKER` (default `ibkr`) so paper fills mirror the selected
live broker policy. `EXEC_EQUITY_MIN_NOTIONAL_USD` defaults to `1.0`, and
`EXEC_SHARE_ROUNDING_DROP_SUB_MIN_NOTIONAL=1` drops sub-minimum equity orders by
returning zero so the existing zero-delta guards skip submission/fill.

Rounding truncates toward zero to avoid increasing exposure beyond the model's
intent. The decision is written into existing order metadata, broker-action
audit payloads, and simulator fill `explain_json`; there is no storage schema
change. Symbols classified `UNKNOWN` at an equity broker boundary are treated
as equity by default (`EXEC_SHARE_ROUNDING_UNKNOWN_AS_EQUITY=1`) so real stocks
not yet present in the asset map still use broker-realistic share conventions.

FX is an explicit pass-through. This layer does not implement the still-unowned
FX weight-to-lots conversion at the same weight-to-quantity seam; a future FX
owner should convert FX weights to lots before this helper and continue passing
`asset_class="FX"` so equity share rounding remains a no-op for FX quantities.

## Crypto Execution Path

Crypto execution support in this workstream is an IBKR-PAXOS contract path, not
a new exchange adapter. `broker_ibkr_gateway.py` constructs IBKR `CRYPTO`
contracts on exchange `PAXOS` from bare-root crypto symbols such as `BTC`, with
`symbol=BTC` and `currency=USD`. The local normalization fallback keeps the
stored symbol convention aligned with `asset_map.py` and `crypto_funding_rates`
until a canonical `crypto_instrument.py` owner exists.

`broker_router.py` keeps the existing failover-chain validation and global
execution gates, then prefers the crypto-capable broker (`ibkr`) at the front of
that already validated chain when a dry-run/sim/paper batch contains crypto
orders. No live order, cancel, replace, flatten, pre-live reconcile,
kill-switch, execution-mode, options-block, or live broker contract mechanics
are changed by this path.

`crypto_session.py` models crypto as 24/7 open by default, including weekends.
The execution policy engine applies this asset-class-aware clock behind
`EPE_CRYPTO_SESSION_ENFORCE` (default on) and suppresses only during an
explicitly configured `CRYPTO_MAINTENANCE_*` UTC window. Equity and FX session
logic remain unchanged; crypto feature rows use always-on base session flags so
fixed equity/FX UTC windows do not label crypto as out of session.

## Broker Simulation Pipeline

`broker_sim.py::apply_new_portfolio_orders(...)` keeps its public signature stable
but now routes through named private phases:

1. load override orders or the latest portfolio-execution intent batch
2. validate/gate dry-run, empty-batch, and already-applied batch behavior
3. build sizing and risk-cap context from account, positions, prices, and stress signals
4. simulate child fills with existing spread, slippage, chunking, LOB, and cost-model logic
5. persist broker account, positions, fills, order state, and mark-to-market account state
6. log execution-ledger submit/fill effects for real paper-sim books while keeping shadow books isolated
7. return the same summary shape used by existing callers

Dry-run override orders still return `dry_run_preview` with the supplied orders and
do not write broker fills, positions, order state, or execution-ledger rows. Live
paper-sim override orders still write the broker and execution-ledger evidence
used by attribution, idempotency, and operator diagnostics.

### Broker Simulation Options

`broker_sim.py` treats canonical OCC option symbols as option contracts only when
`engine.data.options_instrument.parse_option_symbol(...)` can parse valid
metadata. Unparseable symbols stay on the existing non-option path.

Option fills fail closed on missing, stale, or invalid `options_chain_v2`
bid/ask rows; the simulator never prices an option from the underlying `prices`
row. Weight sizing and notional accounting use option midpoint times the
metadata multiplier, contract quantities round to whole contracts, and execution
prices walk from midpoint toward the chain ask for buys or bid for sells. Short
option fills record a reference-only margin debit in `broker_fills` under
`option_margin_debit` and annotate fills with `option_sim_margin_reference`.
Broker mark-to-market uses the latest valid option-chain midpoint times
multiplier; missing option quotes appear in `broker_equity_at(...).missing_prices`.

## Learned Execution Slicing

`contextual_bandit_slicer.py` is a prototype learned execution policy. It is
disabled unless `LEARNED_EXECUTION_SLICING_ENABLED=1` or an input intent sets
`learned_execution_slicing=1`.

The learned policy is execution-only:

- It cannot choose assets, side/direction, portfolio size, broker, notional, or quantity.
- It can only choose `slice_pct`, `target_participation`, `slice_interval_ms`, and `entry_delay_ms`.
- Bounds come from `execution_policy_engine.py` after kill switches, trade suppression, live AI safety, capital preservation, alpha decay, regime, conformal, OOD, and uncertainty gates have already run.
- The default production bounds are conservative: learned slicing can reduce slice percentage or participation and can slow timing, but cannot exceed the base execution-policy slice/participation limits.
- Every learned output row is stamped with `execution_policy_locked=1`, `learned_execution_locked=1`, `learned_execution_policy_scope="execution_only"`, and `learned_execution_guard`.
- `broker_router.py` calls `validate_routed_learned_orders(...)` before broker failover. Direct learned-policy orders without the EPE guard, with changed symbol/side, with out-of-bounds execution parameters, or with learned slices whose total exceeds the parent quantity are rejected before any broker adapter runs.

The module also includes `evaluate_against_baselines(...)`, which compares the
learned slicer with TWAP, VWAP, POV, and adaptive baselines on implementation
shortfall, slippage, fill risk, and adverse selection for historical or
synthetic execution-context rows.

## Execution Diagnostics Serializer

[execution_diagnostics.py](execution_diagnostics.py) is the read-only
serializer behind `GET /api/execution/diagnostics`. It aggregates existing
ledger and analytics evidence for the dashboard execution screen:

- route/source inventory for execution stats, rolling metrics, by-symbol TCA,
  advisories, terminal orders/fills, rejected intents, suppressed intents, LOB,
  DeepLOB, and learned slicing
- by-symbol TCA, rolling slippage/latency, fill-quality scores,
  partial-fill aggregation, implementation-shortfall fields, and VWAP fields
  where persisted
- rejected terminal intents and suppressed portfolio/execution intents with
  machine-readable `reason_code` plus human-readable `reason`
- intent-to-route-to-fill trace rows for operator drilldowns
- L2 freshness, top-of-book depth, LOB simulation calibration, replay
  readiness, and shadow DeepLOB readiness
- learned slicing policy state, action distribution, baseline comparison,
  recent non-application reasons, and explicit authority flags

The serializer is explanatory only. It does not arm execution, alter learned
slicing bounds, bypass risk gates, or submit broker orders. Live authority
remains controlled by the existing execution barrier, kill switches, execution
policy engine, broker router, risk controls, and broker adapters.

## Reactive LOB Simulation And Shadow DeepLOB

`broker_sim.py` now applies `lob_simulation.py` to each simulated child fill
when fresh `market_microstructure_signals` rows provide usable bid/ask size.
The simulator records queue-ahead quantity, queue-position percentage, spread
crossing, queue-aware partial-fill caps, adverse-selection bps, top-of-book
participation, sweep bps, and calibrated market-impact bps in
`broker_fills.explain_json.lob_simulation`. If L2 data is missing or lacks
top-of-book depth, the simulator falls back to the prior spread/slippage path
and records the unavailable reason instead of inventing depth.

`EXEC_LOB_DEEPLOB_SHADOW_ENABLED=1` enables an execution-only, shadow
DeepLOB-style feature path. It can emit only adverse-selection and execution
timing diagnostics; it cannot choose assets, side, broker, portfolio size,
target weights, or route orders. The execution policy engine logs the shadow
payload under `lob_deeplob_shadow` and does not consume it to alter live order
parameters.

The shadow path is blocked unless all readiness checks pass:

- enough fresh L2/top-of-book rows in `market_microstructure_signals`
- positive and bounded latency assumptions from `EXEC_LOB_ASSUMED_LATENCY_MS`
  or `BROKER_LATENCY_MS`, with provider latency not breaching configured bounds
- enough recent simulated fills whose explain JSON contains applied
  `lob_simulation` calibration evidence

`live_trading_preflight()` and `prod_preflight.py` include this readiness
snapshot. When the shadow path is enabled and readiness fails, production
preflight fails closed and live readiness includes the LOB blockers.

## Almgren-Chriss Modules

The tree contains two distinct Almgren-Chriss modules that share the same
basename. They are separate, both imported, and not duplicates:

- [almgren_chriss.py](almgren_chriss.py) (196 lines) is an opt-in,
  env-gated impact **estimator** for simulation and validation. It is disabled
  unless `ALMGREN_CHRISS_ENABLED=1`, reads its coefficients and bounds from the
  `ALMGREN_CHRISS_*` knobs, and exposes `estimate_almgren_chriss_costs(...)`,
  which returns bounded temporary, permanent, and risk-term bps for a single
  order from a liquidity snapshot. It is imported by
  [broker_sim.py](broker_sim.py) as a compatibility patch target and by
  `engine/strategy/portfolio_backtest.py`.
- [cost_models/almgren_chriss.py](cost_models/almgren_chriss.py) (132 lines)
  is the `AlmgrenChrissCost` expected market-impact **cost-model** dataclass
  (`eta`/`gamma`/asset-class coefficient overrides) exposing `components_bps(...)`
  and `cost_bps(...)`. It is imported by [broker_sim.py](broker_sim.py),
  `engine/rl/portfolio_env.py`, `engine/strategy/cpcv.py`, and
  `engine/strategy/gated_backtest.py`.

The shared basename is a mild readability hazard — one is the estimator, the
other is the cost-model dataclass — but the two modules are distinct and both
are in active use. They are not duplicates and neither is a dedupe candidate.

## Broker-Native Idempotency

Live Alpaca and IBKR order submission uses dedicated durable idempotency
connections. The adapters call `claim_order_submission_durable(...)` before the
broker submit call, then persist `submitted`, `submit_inflight_unknown`, or
`submission_unrecorded` with the matching durable marker helpers after the
broker response path is known. These helpers open and commit their own storage
connection so the idempotency row survives an ambient Postgres transaction that
is later rolled back when the adapter connection closes.

The original connection-scoped idempotency helpers remain transactional for
non-live and batch callers. Do not switch the storage pool or shared runtime
connections to autocommit to solve live idempotency; live broker paths must use
the focused durable helper APIs instead.

IBKR submissions must copy the local `client_order_id` into the IBKR `Order.orderRef` field before every `placeOrder` call. The adapter validates that the value is non-empty, ASCII-safe (`A-Z`, `a-z`, `0-9`, `_`, `.`, `:`, `-`), and no longer than the configured IBKR order-reference cap before submitting. Direct helper paths perform the same validation before opening a broker connection. Invalid references fail closed with `invalid_order_ref` and stop failover.

For IBKR retries, an existing durable claim for the same `order_uid` is treated
as a duplicate before `_consume_next_order_id()` and `placeOrder()` run. This
prevents a retry from burning or placing a fresh broker order id for an order
that was already claimed locally.

Adaptive and configured multi-slice live orders use slice-scoped idempotency.
The router stamps each submitted slice with `parent_order_id`, `slice_index`,
and `slice_count`; those fields are part of the durable `order_uid` payload and
are also stored on `execution_order_idempotency`. Alpaca and IBKR still use
their coarse `*_last_portfolio_orders_id` cursor for unsliced parent batches,
but multi-slice override calls defer that parent cursor. On restart, the router
walks the parent slices again: already submitted slice UIDs are skipped by the
durable idempotency table, while missing slice UIDs continue to broker submit.

Open-order replacement and resubmission uses the same durable contract.
`execution_open_order_manager.py` creates a deterministic replacement
`order_uid` from the open-order row, replacement attempt, remaining quantity,
side, symbol, broker venue, order type, and replacement limit price, while
preserving the existing replacement client-order-id suffix (`_rN` or `_mN`).
The manager commits that idempotency claim before any replacement broker submit.
After the broker response it durably marks the claim `submitted`,
`submission_unrecorded`, or `submit_inflight_unknown` before updating the local
`exec_open_orders` row.

On restart, a duplicate replacement claim is never submitted to the broker
again. `submitted` claims recover the local open-order row from the durable
broker id, `submission_unrecorded` claims keep the fail-closed reconciliation
state, and `claimed` or `submit_inflight_unknown` claims are treated as
ambiguous broker-submit state that requires operator reconciliation.

## Cancel/Replace Safety

Open LIMIT order maintenance must never submit a second broker order merely because a cancel request was accepted. `execution_open_order_manager.py` and `execution_microstructure.py` first prefer broker-native in-place LIMIT replacement when Alpaca can modify the existing order without creating a second order. Partial-fill cases and market escalation use the safe fallback: cancel the original order, verify broker state is terminal canceled or zero remaining, then submit only the remaining quantity.

Ambiguous cancel outcomes fail closed. Cancel exceptions, unverified adapter responses, post-cancel query failures, or post-cancel broker state that is still open/fillable mark the `exec_open_orders` row as `needs_reconcile`, write a `cancel_replace_needs_reconcile` event, and emit a critical `limit_cancel_replace_needs_reconcile` execution alert. Operators must reconcile the broker order before the manager will retry it.

Broker adapter contracts reflect that distinction:

- Alpaca `cancel_order()` sends DELETE, then polls the broker order until it observes `canceled` or zero remaining; otherwise it returns `cancel_not_verified`.
- IBKR `cancel_order()` calls `cancelOrder()`, then re-queries open orders; it returns verified only when the order disappears from open orders, reports canceled, or reports zero remaining.

## Shutdown And Emergency Broker Risk

[broker_shutdown_risk.py](broker_shutdown_risk.py) owns broker-risk handling for
runtime shutdown and operator emergency intervention. Live mode requires an
explicit `BROKER_SHUTDOWN_POLICY`; missing policy fails closed and records an
audit event instead of assuming a broker action.

Supported policies are:

- `observe_only`: list broker open orders and record the result.
- `cancel_only` / `cancel_open_orders`: cancel broker open orders, then record
  the adapter result.
- `flatten_positions`: flatten reconciled positions without first canceling
  open orders.
- `cancel_and_flatten`: cancel open orders, then flatten reconciled positions.

Flattening is deliberately narrower than ordinary order routing. The shutdown
handler requires fresh pre-live position reconciliation and positive
`BROKER_SHUTDOWN_FLATTEN_MAX_ABS_QTY_PER_SYMBOL` and
`BROKER_SHUTDOWN_FLATTEN_MAX_TOTAL_ABS_QTY` limits before any adapter can submit
market flatten orders. Alpaca uses deterministic shutdown `client_order_id`
values and IBKR uses deterministic `orderRef` values derived from the durable
command id, symbol, and quantity so retries are idempotent at the broker-facing
identifier boundary.

`runtime_shutdown()` calls the handler before stopping runtime jobs. The
operator dashboard and sidecar expose `POST /api/operator/broker_risk` for
emergency cancel/flatten commands when the runtime is unhealthy or stopped. All
paths persist the command/result through `order_commands`, `order_events`, and
broker action audit rows; duplicate command ids return the previous result
without submitting another broker action.

## Extending Execution

When adding new execution logic:

1. Decide whether the feature is broker-facing, simulation-only, or purely analytical.
2. Update execution gating and kill-switch surfaces if the feature can place or modify orders.
3. Keep execution-ledger, fill-normalization, and attribution tables in sync with any new broker workflow.
4. Document analytical execution helpers such as cost models and slicing logic when they start influencing live routing decisions.
