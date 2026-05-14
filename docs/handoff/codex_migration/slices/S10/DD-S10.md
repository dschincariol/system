Slice ID: S10
Goal: Move trade lifecycle and attribution reads behind projector-style consumption of durable `order_events` while preserving the current lifecycle report shape and execution compatibility.

In scope:
- engine/runtime/trade_lifecycle_projection.py
- engine/runtime/trade_lifecycle.py
- engine/execution/execution_ledger.py
- engine/execution/trade_attribution_ledger.py
- tests/test_trade_lifecycle_regressions.py
- tests/test_audit_invariants.py

Out of scope:
- broker adapter rewrites
- `broker_apply_orders` command-boundary changes
- dashboard/control-plane routing work
- schema changes outside projector read helpers

Required reading:
- engine/runtime/trade_lifecycle.py
- engine/runtime/trade_lifecycle_projection.py
- engine/execution/execution_ledger.py
- engine/execution/trade_attribution_ledger.py
- tests/test_trade_lifecycle_regressions.py
- tests/test_audit_invariants.py

Required changes:
- No edits during DD.
- Determine the smallest bounded read-side cutover that:
  - projects `execution_orders` and `fills` from durable `order_events`
  - keeps raw `execution_orders` / `execution_fills` as fallback-only sources
  - exposes projected `order_events` and `order_commands` in the lifecycle report
  - lets trade attribution recover latest execution-order context without direct `execution_orders` reads
- Verify whether `execution_ledger.py` already emits the needed durable `fill` events for both fill paths.

Required verification:
- none during DD

Acceptance criteria:
- The DD output names one projector helper module and the exact read-side files to switch.
- The DD output keeps broker behavior and dashboard work out of scope.
- The DD output names focused lifecycle and attribution verification.

Stop and report if:
- The slice requires broker adapter changes.
- The slice requires schema changes outside projector helpers.
- The slice requires dashboard/control-plane edits.

## DD Findings

- `engine/runtime/trade_lifecycle.py` and `engine/execution/trade_attribution_ledger.py` were still reading `execution_orders` / `execution_fills` directly.
- The clean bounded seam is one projector helper module that:
  - reads durable `order_events`
  - rehydrates `execution_orders` and `fills`
  - merges raw-table fallback data only when present
  - exposes matching `order_commands`
- `engine/execution/execution_ledger.py` already had the right emission seams:
  - `order_submit`
  - `fill` in both fill-write paths
- The approved `S10` touch set is:
  - `engine/runtime/trade_lifecycle_projection.py`
  - `engine/runtime/trade_lifecycle.py`
  - `engine/execution/execution_ledger.py`
  - `engine/execution/trade_attribution_ledger.py`
  - `tests/test_trade_lifecycle_regressions.py`
  - `tests/test_audit_invariants.py`
- Focused verification should cover:
  - full lifecycle regression replay
  - an event-only lifecycle reconstruction path without legacy execution tables
  - attribution context recovery from projected execution-order events
