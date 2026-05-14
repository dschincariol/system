Slice ID: S10
Goal: Move trade lifecycle and attribution reads behind projector-style consumption of durable `order_events` while preserving current lifecycle report outputs.

In scope:
- engine/runtime/trade_lifecycle_projection.py
- engine/runtime/trade_lifecycle.py
- engine/execution/execution_ledger.py
- engine/execution/trade_attribution_ledger.py
- tests/test_trade_lifecycle_regressions.py
- tests/test_audit_invariants.py

Out of scope:
- broker adapter rewrites
- dashboard/control-plane work
- schema changes outside projector helpers

Required reading:
- engine/runtime/trade_lifecycle.py
- engine/runtime/trade_lifecycle_projection.py
- engine/execution/execution_ledger.py
- engine/execution/trade_attribution_ledger.py
- tests/test_trade_lifecycle_regressions.py
- tests/test_audit_invariants.py

Required changes:
- Add projector helpers that rehydrate:
  - `execution_orders`
  - `fills`
  - `order_events`
  - `order_commands`
- Switch `trade_lifecycle.py` to use the projector first and keep the existing report shape intact.
- Switch `trade_attribution_ledger.py` to projected execution-order lookup.
- Ensure `execution_ledger.py` persists durable `fill` events for both fill-write paths.
- Add focused regressions for:
  - event-only lifecycle reconstruction without legacy execution tables
  - attribution execution-order context loaded from projected events

Required verification:
- python -m pytest tests/test_trade_lifecycle_regressions.py -q
- python -m pytest tests/test_audit_invariants.py -q -k "trade_attribution_ignores_legacy_pnl_fields or trade_attribution_loads_execution_order_context_from_projected_events"

Acceptance criteria:
- Lifecycle reports can reconstruct from durable `order_events` without legacy execution tables.
- Trade attribution can resolve latest execution-order context from projected events.
- Raw execution tables remain fallback-only.
- No broker behavior or dashboard changes.

Stop and report if:
- The slice requires broker adapter changes.
- The slice requires new schema outside projector helpers.

## Implementation Result

- Added `engine/runtime/trade_lifecycle_projection.py` as the bounded projector helper over:
  - `order_events`
  - `order_commands`
  - raw fallback `execution_orders`
  - raw fallback `execution_fills`
- Updated `engine/runtime/trade_lifecycle.py` to:
  - read projected `execution_orders` and `fills`
  - expose `order_events` and `order_commands` in `report["steps"]`
  - preserve the existing lifecycle report shape
- Updated `engine/execution/trade_attribution_ledger.py` to resolve execution-order context through projected reads.
- Completed durable `fill` event recording in both `execution_ledger.py` fill paths.
- Added focused regressions for:
  - event-only lifecycle reconstruction without `execution_orders` / `execution_fills`
  - attribution execution-order context from projected events

## Verification Result

- `python -m pytest tests/test_trade_lifecycle_regressions.py -q`
- `python -m pytest tests/test_audit_invariants.py -q -k "trade_attribution_ignores_legacy_pnl_fields or trade_attribution_loads_execution_order_context_from_projected_events"`
