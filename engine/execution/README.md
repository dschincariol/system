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

- [broker_router.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\execution\broker_router.py)
  Broker selection and broker abstraction boundary.
- [broker_sim.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\execution\broker_sim.py)
  Simulation broker used in non-live modes.
- [kill_switch.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\execution\kill_switch.py)
  Execution safety switches.
- [execution_mode.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\execution\execution_mode.py)
  Runtime execution-mode state and policy.
- [execution_policy_engine.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\execution\execution_policy_engine.py)
  Policy layer before actual broker submission.
- [execution_poll_and_attrib.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\execution\execution_poll_and_attrib.py)
  Polling fills and attribution path.
- [execution_ledger.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\execution\execution_ledger.py)
  Shared persistence and read helpers for broker order state, fills, and lifecycle evidence.
- [trade_attribution_ledger.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\execution\trade_attribution_ledger.py)
  Post-trade attribution ledger.
- [almgren_chriss.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\execution\almgren_chriss.py)
  Optional transaction-cost estimator used by newer execution-analytics and slicing decisions.
- [broker_fill_utils.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\execution\broker_fill_utils.py)
  Normalization helpers that turn broker-specific fill payloads into common execution records.
- [broker_alpaca_rest.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\execution\broker_alpaca_rest.py)
  Alpaca adapter used for broker-side order submission and status reads.
- [broker_ibkr_gateway.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\execution\broker_ibkr_gateway.py)
  IBKR gateway adapter used by live broker-routing paths.
- [broker_apply_orders.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\execution\broker_apply_orders.py)
  Main order-application path that enforces execution barriers, intent loading, shaping, and broker submission.
- [execution_ai_advisor.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\execution\execution_ai_advisor.py)
  Advisory-only read layer that summarizes historical slippage/latency and persists operator-facing execution guidance.

## Important Constraint

In `safe` mode, execution is intentionally blocked. If the dashboard shows execution as not started while the runtime is in `safe`, that is normally expected behavior rather than a startup failure.

## Extending Execution

When adding new execution logic:

1. Decide whether the feature is broker-facing, simulation-only, or purely analytical.
2. Update execution gating and kill-switch surfaces if the feature can place or modify orders.
3. Keep execution-ledger, fill-normalization, and attribution tables in sync with any new broker workflow.
4. Document analytical execution helpers such as cost models and slicing logic when they start influencing live routing decisions.
