Slice ID: S10
Goal: Independently audit the projector-style post-trade read cutover and verify that lifecycle and attribution paths now consume durable `order_events` without breaking existing report behavior.

In scope:
- engine/runtime/trade_lifecycle_projection.py
- engine/runtime/trade_lifecycle.py
- engine/execution/execution_ledger.py
- engine/execution/trade_attribution_ledger.py
- tests/test_trade_lifecycle_regressions.py
- tests/test_audit_invariants.py
- the `S10` diff only

Out of scope:
- broker adapter rewrites
- dashboard/control-plane work
- schema changes outside projector helpers

Required reading:
- the `S10` diff
- engine/runtime/trade_lifecycle_projection.py
- engine/runtime/trade_lifecycle.py
- engine/execution/trade_attribution_ledger.py
- tests/test_trade_lifecycle_regressions.py
- tests/test_audit_invariants.py

Required changes:
- No code changes unless the audit finds a concrete defect.
- Findings must come first.
- Explicitly check:
  - lifecycle reconstruction from durable `order_events`
  - raw-table fallback remaining compatible
  - attribution execution-order context recovery from projected events
  - no broker or dashboard scope leak

Required verification:
- python -m pytest tests/test_trade_lifecycle_regressions.py -q
- python -m pytest tests/test_audit_invariants.py -q -k "trade_attribution_ignores_legacy_pnl_fields or trade_attribution_loads_execution_order_context_from_projected_events"

Acceptance criteria:
- Findings-first audit output.
- Explicit statement whether `S10` is complete or needs follow-up.

Stop and report if:
- The diff leaks into broker behavior.
- The diff introduces schema work outside projector helpers.

## Audit Result

- Findings: none within the approved `S10` slice.
- Lifecycle reads now reconstruct `execution_orders` and `fills` from durable `order_events`, with raw execution tables preserved as fallback-only compatibility sources.
- Trade attribution now resolves latest execution-order context from projected events instead of direct `execution_orders` reads.
- Durable `fill` events are present in both `execution_ledger.py` fill paths.
- No broker adapter, dashboard, or schema-surface leak was introduced.

## Verification Result

- `python -m pytest tests/test_trade_lifecycle_regressions.py -q`
- `python -m pytest tests/test_audit_invariants.py -q -k "trade_attribution_ignores_legacy_pnl_fields or trade_attribution_loads_execution_order_context_from_projected_events"`

## Follow-up Notes

- `S10` is complete within the approved boundary.
- The next clean step is the control-plane API entrypoint split in `S11`.
