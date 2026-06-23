# LAN Access — Operator/Dashboard UI from another computer

This guide explains how to reach the trading system's browser UI from another
machine on your **trusted local network** (for example, a Windows desktop),
instead of only from the Ubuntu host itself.

> **Do not use NoMachine / remote-desktop screen streaming for normal UI
> viewing.** The UI is a normal web app; open it directly in a browser over the
> LAN. NoMachine should be reserved for actual desktop sessions, not for
> watching the dashboard.

Ubuntu host LAN IP used in the examples below: **`192.168.0.165`**
(substitute your own if different).

---

## 1. What runs where

| Service           | Default port | Purpose                                   | Bind host (default) |
|-------------------|--------------|-------------------------------------------|---------------------|
| Dashboard / UI    | `8000`       | Primary operator dashboard (HTML + APIs)  | `127.0.0.1`         |
| Operator console  | `4001`       | Internal Node control-plane sidecar       | internal/loopback   |

The dashboard at **:8000 is the primary entry point.** It proxies the operator
console at `/operator/` (same-origin), so LAN workflows use **:8000 only**.
Port **:4001** is not a LAN entrypoint in the production compose contract. It is
exposed only inside the Docker network as `operator:4001`, or loopback-only on a
standalone host-side operator process.

By default the dashboard binds to loopback (`127.0.0.1`) and is therefore only
reachable from the Ubuntu host. LAN access is **opt-in**.

LAN access URLs once enabled:

- `http://192.168.0.165:8000`  ← dashboard (start here)
- `http://192.168.0.165:8000/operator/`  ← operator bridge through dashboard auth

---

## 2. Enable LAN access

Set one toggle and a token in your environment file (`.env`, or the systemd
`trading.env`):

```ini
# Bind the dashboard (:8000) to 0.0.0.0 for the LAN.
TRADING_NETWORK_MODE=lan

# REQUIRED for any non-loopback bind. Startup fails closed without it.
# Generate a strong value, e.g.:  openssl rand -hex 32
DASHBOARD_API_TOKEN=<your-strong-token>

# Optional: only changes the LAN URL printed in startup logs.
TRADING_LAN_IP=192.168.0.165
```

Equivalent explicit form (if you prefer not to use the mode toggle):

```ini
DASHBOARD_HOST=0.0.0.0
DASHBOARD_API_TOKEN=<your-strong-token>
```

`TRADING_NETWORK_MODE=lan` only sets the bind host **default**; an explicit
`DASHBOARD_HOST` always wins. It does not publish the operator sidecar. Local
development is unchanged — leave `TRADING_NETWORK_MODE` unset (or `local`) to
keep loopback-only behavior.

Keep these operator settings in the production compose env:

```ini
OPERATOR_SIDECAR_INTERNAL_ONLY=1
OPERATOR_PUBLIC_PORT=
OPERATOR_ALLOW_DANGEROUS_PUBLIC_BIND=0
```

A non-empty `OPERATOR_PUBLIC_PORT` or `OPERATOR_ALLOW_DANGEROUS_PUBLIC_BIND=1`
under `OPERATOR_SIDECAR_INTERNAL_ONLY=1` is treated as a production-preflight
blocker because it implies direct sidecar exposure while the compose service has
no host `ports:` mapping.

### Why the token is mandatory

The startup gate **refuses to bind a non-loopback host without
`DASHBOARD_API_TOKEN`** (see `engine/runtime/startup_gates.py`). This is
intentional fail-closed behavior: exposing an unauthenticated control plane to
the network is never allowed. The startup banner also prints a warning if it
detects a wildcard bind with no token.

In **live/prod** mode, a public dashboard bind additionally
requires an explicit acknowledgement:
`TRADING_PUBLIC_NETWORK_EXPOSURE_ACK` + `_OWNER` + `_REASON`
(see `engine/runtime/live_trading_preflight.py`). Safe/monitoring mode does not
require the ACK, only the token.

---

## 3. Open the LAN firewall port

Open **only** the dashboard UI port to your LAN subnet — nothing else, and
**never** to the public internet. Example with `ufw`, scoped to the local
subnet:

```bash
sudo ufw allow from 192.168.0.0/24 to any port 8000 proto tcp
```

Do **not** add router/NAT port-forwarding for these ports. Do **not** expose
Postgres/Timescale, Redis, MinIO, the operator sidecar, or any other backend
port.

---

## 4. Verify

### On the Ubuntu host

```bash
# Dashboard listening on the LAN bind, with no wildcard/direct operator sidecar?
ss -H -ltnp | grep -E '(:8000|:4001|:4000|:7002|:5201)\b' || true

# Health endpoint over loopback and over the LAN IP:
curl http://127.0.0.1:8000/api/health
curl http://192.168.0.165:8000/api/health

# Production preflight also runs the passive listener diagnostic when enabled:
PREFLIGHT_CHECK_NETWORK_LISTENERS=1 python engine/runtime/prod_preflight.py --json
```

Expected listener evidence: `:8000` may be bound to the reviewed dashboard host;
`:4001` must not be a wildcard/LAN host listener. Unexpected wildcard listeners
on `:4000`, `:7002`, or `:5201` are flagged for operator review.

### From the Windows desktop (PowerShell)

```powershell
Test-NetConnection 192.168.0.165 -Port 8000
```

This should report `TcpTestSucceeded : True`. Then open in a browser:

- `http://192.168.0.165:8000`
- `http://192.168.0.165:8000/operator/`

Open browser **DevTools → Network**: you should see **no failed requests to
`127.0.0.1` or `localhost`**. API calls are same-origin (`/api/...`), and the
operator bridge stays same-origin through the dashboard — never a hard-coded
client-local address.

---

## 5. Security notes

- LAN access is intended for a **trusted LAN only**. Keep it off the public
  internet (no port-forwarding).
- Authentication is the `DASHBOARD_API_TOKEN` (and operator token). Sensitive
  dashboard GETs, bridged operator reads, and all mutations require dashboard
  auth before the server forwards the sidecar token; CORS reflects only
  allow-listed origins and never falls back to `*` while credentials are
  enabled.
- CSP is **not** disabled. The operator server's `connect-src` is derived from
  the request host so same-origin LAN access works without weakening the policy.
- Do not publish `:4001` for normal operations. A direct sidecar diagnostic must
  stay loopback/internal, send `X-Operator-Token` on every request, and not be
  carried into production.

---

## 6. Performance over the LAN

These are enabled automatically and are tunable via env:

- **gzip** compression for JSON API responses above
  `DASHBOARD_GZIP_MIN_BYTES` (default `1024`) when the browser sends
  `Accept-Encoding: gzip`.
- **Cache-Control** on static assets:
  `public, max-age=DASHBOARD_STATIC_CACHE_MAX_AGE_S` (default `60s`,
  `must-revalidate`); HTML shells are `no-cache`.
- **Polling** pauses on hidden tabs, never overlaps runs, and backs off on
  failures. Override the base cadence in the browser with
  `window.DASHBOARD_REFRESH_MS` if you want to trim LAN traffic.

---

## 7. Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| Startup aborts with `DASHBOARD_API_TOKEN is required when DASHBOARD_HOST is not loopback` | Set `DASHBOARD_API_TOKEN`. |
| `ss` shows `127.0.0.1:8000` not `0.0.0.0:8000` | `TRADING_NETWORK_MODE` not set to `lan`, or `DASHBOARD_HOST` explicitly pinned to loopback. |
| `Test-NetConnection` fails but `ss` shows `0.0.0.0:8000` | LAN firewall port closed — open only `:8000` to the subnet. |
| `ss` shows `0.0.0.0:4001` or `*:4001` | Operator sidecar is directly exposed; remove `OPERATOR_PUBLIC_PORT`, keep `OPERATOR_SIDECAR_INTERNAL_ONLY=1`, and restart the sidecar/compose stack. |
| Preflight reports `operator_public_port_ignored_internal_only` | Clear `OPERATOR_PUBLIC_PORT`; production compose does not publish host `:4001`. |
| Live/prod refuses public bind | Provide `TRADING_PUBLIC_NETWORK_EXPOSURE_ACK` + owner + reason, or run in safe/monitoring mode. |
