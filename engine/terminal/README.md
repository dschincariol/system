# Terminal Subsystem

The `engine/terminal/` package owns the browser-terminal API surface that is served alongside the main dashboard UI.

## Files

- [api/api_terminal.py](api/api_terminal.py)
  Read-mostly terminal endpoints for watchlists, positions, orders, fills, equity history, markers, and the one-call terminal snapshot.
- [api/api_terminal_orders.py](api/api_terminal_orders.py)
  Risk-gated terminal order-entry handlers for `BUY`, `SELL`, and `FLATTEN`.
- [api/__init__.py](api/__init__.py)
  Lazy route export used by the dashboard server.

## UI Counterparts

- [../../ui/terminal/terminal.html](../../ui/terminal/terminal.html)
- [../../ui/terminal/terminal.js](../../ui/terminal/terminal.js)
- [../../ui/terminal/pro_charting.js](../../ui/terminal/pro_charting.js)
- [../../ui/terminal/terminal_theme.css](../../ui/terminal/terminal_theme.css)

## Safety Boundary

Terminal order-entry routes do not bypass the normal execution stack.

- `POST /api/terminal/order` writes a row into `portfolio_orders` only when `execution_gate_snapshot()` reports that real trading is currently allowed.
- Browser BUY/SELL entry must pass through the shared confirmation modal before the first `/api/terminal/order` POST. The modal requires the `TRADE` typed token, consequence acknowledgement, and a 1000 ms hold; keyboard shortcuts remain behind the arm toggle but still use the same confirming submit path.
- `POST /api/terminal/flatten` derives the current broker position, then writes another `portfolio_orders` intent instead of mutating positions directly.
- Both mutation routes also check `DISABLE_LIVE_EXECUTION` directly before storage writes. Unset and any value except `0`, `false`, `no`, or `off` block terminal order creation. The returned gate still reports the effective mode resolved from `ENGINE_MODE`/`EXECUTION_MODE` and persisted execution mode, so a safe-mode block reports `mode=safe` even when the applied policy is `disable_live_execution_env`.
- Before any live terminal write the routes apply the pre-live reconcile gate `prelive_reconcile_policy_gate` (from `engine/runtime/live_execution_control.py`): if broker-vs-runtime position reconciliation has not satisfied the pre-live policy, the gate returns an `execution_blocked` response and no `portfolio_orders` intent is written. This is the same gate enforced by the broker router and the live broker adapters, so terminal order entry cannot skip the pre-live reconciliation barrier.
- Both mutation routes run backend pre-trade controls before writing an intent: positive quantity, fresh price, max quantity, max notional, optional per-symbol caps, and duplicate-recent-order detection.
- Rejected terminal requests are persisted in `terminal_intent_rejections` with stable reason codes such as `missing_price`, `stale_price`, `max_qty_exceeded`, `max_notional_exceeded`, and `duplicate_recent_order`.

That keeps terminal-originated actions inside the same audit, gating, and routing path used by the rest of the runtime.
