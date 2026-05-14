Slice ID: S09
Goal: Add a durable `order_commands` / `order_events` execution boundary at `broker_apply_orders` while preserving existing broker adapter behavior.

In scope:
- engine/execution/order_command_boundary.py
- engine/execution/broker_apply_orders.py
- engine/runtime/storage.py
- tests/test_broker_apply_orders_modes.py
- tests/test_storage_contracts.py
- tests/test_broker_order_idempotency_regressions.py
- tests/test_trade_lifecycle_regressions.py

Out of scope:
- broker adapter rewrites
- broker simulator fill/idempotency behavior changes
- post-trade projector work
- dashboard/control-plane work
- unrelated schema refactors

Required reading:
- engine/execution/broker_apply_orders.py
- engine/execution/execution_ledger.py
- tests/test_broker_apply_orders_modes.py
- tests/test_broker_order_idempotency_regressions.py
- tests/test_storage_contracts.py
- tests/test_trade_lifecycle_regressions.py

Required changes:
- Add `engine/execution/order_command_boundary.py` owning:
  - `order_commands`
  - `order_events`
  - insert/update helpers for command snapshots and terminal events
- Bootstrap the new boundary from `engine.runtime.storage.init_db()`.
- Update `engine/execution/broker_apply_orders.py` so:
  - `shadow`, `paper`, and `live` paths persist a command snapshot before broker behavior
  - blocked exits persist `order_events` rows
  - terminal branch results persist `order_events` rows and update command status
  - broker adapters remain unchanged
- Extend focused tests to verify:
  - shadow and paper modes persist one command and one terminal event
  - blocked paths persist blocked events and do not create commands
  - storage contracts track the new schema owner, tables, columns, and indexes
- No “while here” refactors, placeholder tables, or schema changes outside the new family.

Required verification:
- python -m pytest tests/test_broker_apply_orders_modes.py -q
- python -m pytest tests/test_broker_order_idempotency_regressions.py -q
- python -m pytest tests/test_storage_contracts.py -q
- python -m pytest tests/test_trade_lifecycle_regressions.py -q

Acceptance criteria:
- `order_commands` and `order_events` are durably bootstrapped.
- `broker_apply_orders` persists the new boundary for shadow, paper, live-result, and blocked paths.
- Existing broker idempotency and trade lifecycle tests remain green.
- No broker adapter behavior changes.
- No unrelated file edits.

Stop and report if:
- The slice requires changes in broker adapters or projector reads.
- The slice requires schema changes outside the new command/event family.

## Implementation Result

- Added `engine/execution/order_command_boundary.py` as the new schema owner for:
  - `order_commands`
  - `order_events`
- Hooked `engine.runtime.storage.init_db()` to call `init_order_command_boundary()`.
- Updated `engine/execution/broker_apply_orders.py` to:
  - record durable command snapshots for `shadow`, `paper`, and `live` execution branches
  - emit durable blocked events for `_blocked(...)` exits and the remaining direct blocked returns
  - emit terminal command-result or execution-error events and update command status
- Extended `tests/test_broker_apply_orders_modes.py` to assert the new command/event behavior in:
  - shadow mode
  - paper mode
  - blocked live mode
  - blocked kill-switch mode
- Updated `tests/test_storage_contracts.py` so the repo contract now recognizes:
  - `engine.execution.order_command_boundary` as a schema owner
  - the new tables, columns, and indexes

## Verification Result

- `python -m pytest tests/test_broker_apply_orders_modes.py -q`
- `python -m pytest tests/test_broker_order_idempotency_regressions.py -q`
- `python -m pytest tests/test_storage_contracts.py -q`
- `python -m pytest tests/test_trade_lifecycle_regressions.py -q`
