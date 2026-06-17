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
- [execution_poll_and_attrib.py](execution_poll_and_attrib.py)
  Polling fills and attribution path.
- [execution_ledger.py](execution_ledger.py)
  Shared persistence and read helpers for broker order state, fills, and lifecycle evidence.
- [trade_attribution_ledger.py](trade_attribution_ledger.py)
  Post-trade attribution ledger.
- [almgren_chriss.py](almgren_chriss.py)
  Optional transaction-cost estimator used by newer execution-analytics and slicing decisions.
- [broker_fill_utils.py](broker_fill_utils.py)
  Normalization helpers that turn broker-specific fill payloads into common execution records.
- [broker_alpaca_rest.py](broker_alpaca_rest.py)
  Alpaca adapter used for broker-side order submission and status reads.
- [broker_ibkr_gateway.py](broker_ibkr_gateway.py)
  IBKR gateway adapter used by live broker-routing paths.
- [broker_apply_orders.py](broker_apply_orders.py)
  Main order-application path that enforces execution barriers, intent loading, shaping, and broker submission.
- [broker_failover_policy.py](broker_failover_policy.py)
  Broker alias normalization, live failover-chain validation, broker environment contract checks, startup broker preflight, and non-retryable broker failure classification.
- [broker_submission_recovery.py](broker_submission_recovery.py)
  Recovery marker and fail-closed gate for broker-accepted submissions that could not be durably recorded locally.
- [execution_ai_advisor.py](execution_ai_advisor.py)
  Advisory-only read layer that summarizes historical slippage/latency and persists operator-facing execution guidance.

## Important Constraint

In `safe` mode, execution is intentionally blocked. If the dashboard shows execution as not started while the runtime is in `safe`, that is normally expected behavior rather than a startup failure.

Live broker routing has additional fail-closed constraints:

- `BROKER`, `BROKER_NAME`, `LIVE_BROKER`, and the first `BROKER_FAILOVER` entry must identify the intended live broker.
- Live failover chains must not include `sim`, `paper`, or `sandbox`.
- `DISABLE_LIVE_EXECUTION` blocks real trading when it is set to any non-empty value other than `0`, `false`, `no`, or `off`.
- Pre-live reconciliation must run in live mode unless the break-glass environment contract is explicitly filled and audited.
- Broker auth/configuration failures and unrecorded broker submissions stop failover instead of retrying into another broker.

## Extending Execution

When adding new execution logic:

1. Decide whether the feature is broker-facing, simulation-only, or purely analytical.
2. Update execution gating and kill-switch surfaces if the feature can place or modify orders.
3. Keep execution-ledger, fill-normalization, and attribution tables in sync with any new broker workflow.
4. Document analytical execution helpers such as cost models and slicing logic when they start influencing live routing decisions.
