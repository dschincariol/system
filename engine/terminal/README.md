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
- `POST /api/terminal/flatten` derives the current broker position, then writes another `portfolio_orders` intent instead of mutating positions directly.

That keeps terminal-originated actions inside the same audit, gating, and routing path used by the rest of the runtime.
