Slice ID: S09
Goal: Independently audit the `S09` diff and verify that the execution entrypoint now persists a durable `order_commands` / `order_events` boundary without changing broker adapter behavior.

In scope:
- engine/execution/order_command_boundary.py
- engine/execution/broker_apply_orders.py
- engine/runtime/storage.py
- tests/test_broker_apply_orders_modes.py
- tests/test_storage_contracts.py
- the `S09` diff only

Out of scope:
- broker adapter rewrites
- broker simulator fill/idempotency behavior changes
- post-trade projector work
- dashboard/control-plane work
- unrelated schema refactors

Required reading:
- the `S09` diff
- engine/execution/order_command_boundary.py
- engine/execution/broker_apply_orders.py
- engine/runtime/storage.py
- tests/test_broker_apply_orders_modes.py
- tests/test_storage_contracts.py
- tests/test_broker_order_idempotency_regressions.py
- tests/test_trade_lifecycle_regressions.py

Required changes:
- No code changes unless the audit finds a concrete defect.
- Review the implementation as a code audit.
- Findings must come first.
- Explicitly check for:
  - durable bootstrap of `order_commands` and `order_events`
  - command snapshots written before broker behavior in shadow/paper/live branches
  - blocked paths emitting durable `order_events` rows without creating commands
  - broker adapter behavior staying unchanged
  - no schema drift outside the new command/event family

Required verification:
- python -m pytest tests/test_broker_apply_orders_modes.py -q
- python -m pytest tests/test_broker_order_idempotency_regressions.py -q
- python -m pytest tests/test_storage_contracts.py -q
- python -m pytest tests/test_trade_lifecycle_regressions.py -q

Acceptance criteria:
- Findings-first audit output.
- Explicit statement whether `S09` is complete or needs follow-up.
- Verification results included.

Stop and report if:
- The diff leaks into broker adapter behavior.
- The diff introduces schema drift outside the new command/event family.
- Any required fix expands beyond the approved `S09` touch set.

## Audit Result

- Findings: none within the approved `S09` slice.
- `engine/execution/order_command_boundary.py` now owns the bounded durable execution boundary tables and helpers.
- `engine/runtime/storage.py` now bootstraps that schema in the canonical init flow.
- `engine/execution/broker_apply_orders.py` now persists:
  - command snapshots for shadow, paper, and live execution branches
  - blocked execution events for `_blocked(...)` exits and the remaining direct blocked returns
  - terminal command-result or execution-error events that update command status
- Broker adapter behavior remains unchanged; compatibility verification for router/sim idempotency stayed green.
- No schema drift outside `order_commands` / `order_events` was introduced.

## Verification Result

- `python -m pytest tests/test_broker_apply_orders_modes.py -q`
- `python -m pytest tests/test_broker_order_idempotency_regressions.py -q`
- `python -m pytest tests/test_storage_contracts.py -q`
- `python -m pytest tests/test_trade_lifecycle_regressions.py -q`

## Follow-up Notes

- `S09` is complete within the approved boundary.
- The next clean step is `DD-S10` for moving trade lifecycle and attribution reads behind projector-style consumption of durable order/fill events.
