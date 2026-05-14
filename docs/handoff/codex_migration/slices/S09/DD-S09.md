Slice ID: S09
Goal: Add a durable `order_commands` / `order_events` execution boundary at the `broker_apply_orders` entrypoint so approved execution payloads and terminal blocked/result events are persisted without changing broker adapter behavior.

In scope:
- engine/execution/order_command_boundary.py
- engine/execution/broker_apply_orders.py
- engine/runtime/storage.py
- tests/test_broker_apply_orders_modes.py
- tests/test_storage_contracts.py

Out of scope:
- broker adapter behavior changes in `engine/execution/broker_router.py`
- broker simulator fill/idempotency logic changes in `engine/execution/broker_sim.py`
- post-trade projector or attribution refactors
- dashboard/control-plane routing changes
- schema changes outside the new execution-boundary family

Required reading:
- engine/execution/broker_apply_orders.py
- engine/execution/broker_router.py
- engine/execution/broker_sim.py
- engine/execution/execution_ledger.py
- tests/test_broker_apply_orders_modes.py
- tests/test_broker_order_idempotency_regressions.py
- tests/test_storage_contracts.py

Required changes:
- No edits during DD.
- Determine the smallest durable boundary that adds:
  - one `order_commands` table for pre-dispatch command snapshots
  - one `order_events` table for blocked and terminal execution outcomes
- Keep broker adapters and simulator behavior unchanged behind the new boundary.
- Keep the slice bounded to the `broker_apply_orders` entrypoint plus one schema module and bootstrap hook.
- Determine the focused verification set needed to prove:
  - bootstrap creates the new tables
  - paper and shadow paths persist commands and terminal events
  - blocked paths persist blocked events without creating commands
  - existing broker idempotency and trade lifecycle reads remain intact

Required verification:
- none during DD

Acceptance criteria:
- The DD output names one new schema module and one entrypoint wiring point.
- The DD output keeps broker adapters out of scope.
- The DD output names the exact verification set for mode-path, schema-bootstrap, and compatibility checks.

Stop and report if:
- The slice requires broker adapter rewrites.
- The slice requires post-trade projector changes.
- The slice requires schema work outside `order_commands` / `order_events`.

## DD Findings

- The clean bounded seam is `engine/execution/broker_apply_orders.py`, not the broker adapters.
- Current behavior already branches all execution modes through that entrypoint:
  - `shadow` executes simulated challenger groups
  - `paper` routes the real payload through broker-sim and may also execute shadow groups
  - `live` performs the final risk/health checks, may execute shadow groups, then dispatches the real payload to the broker router
- Existing durable execution tables (`execution_orders`, `execution_fills`, idempotency state) already live below this boundary and do not need to move in `S09`.
- The minimum safe schema addition is one new module owning:
  - `order_commands`
  - `order_events`
- `engine.runtime.storage.init_db()` must bootstrap that new module, otherwise repo schema-contract tests will drift.
- The focused `S09` touch set is:
  - `engine/execution/order_command_boundary.py`
  - `engine/execution/broker_apply_orders.py`
  - `engine/runtime/storage.py`
  - `tests/test_broker_apply_orders_modes.py`
  - `tests/test_storage_contracts.py`
- Compatibility verification should also include:
  - `tests/test_broker_order_idempotency_regressions.py`
  - `tests/test_trade_lifecycle_regressions.py`
- No broker adapter changes, no post-trade projector changes, and no schema work outside the new command/event family are required for `S09`.
