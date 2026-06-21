# Services

The `services/` tree contains sidecar or auxiliary services that are not part of the main Python runtime package.

## Current Content

- `operator_ai/`
  Operator-adjacent service code such as [agent.js](operator_ai/agent.js).
- [data_source_manager.py](data_source_manager.py)
  DB-backed source catalog and source-of-truth manager for configurable ingestion sources, including lifecycle reconciliation, health snapshots, one-time legacy env import, and runtime environment projection.
  It includes disabled-by-default Kalshi, CME FedWatch, Polymarket, and
  ForecastEx prediction-market sources whose settings are projected only for
  read-only ingestion jobs. The Polymarket template has no credential fields
  and does not accept wallet, bridge, private-key, or order-placement settings.
  The ForecastEx template reads public CSVs; optional IBKR event-contract market
  data is disabled by default and requires an explicit contract allowlist.
  It also includes a disabled-by-default Deribit crypto-derivatives source with
  no credential fields; settings only project public market-data controls such
  as enabled assets, instrument types, expiries, liquidity thresholds, poll
  cadence, HTTP/WebSocket mode, ticker/order-book limits, and stale thresholds.
  It also includes a disabled-by-default sportsbook/Betfair odds research
  source. Credentials remain in the data-source manager, settings can point to
  a read-only odds API or historical file, and connection tests reject
  sportsbook account, wager, wallet, order, private-key, or trading-shaped
  fields. The source only feeds read-only ingestion; production feature
  promotion additionally requires explicit narrow mappings with complete
  approval metadata, active/watch or exact model-config symbol inventory, and
  persisted sportsbook promotion evidence.
- [credential_encryption.py](credential_encryption.py)
  AES-GCM helpers used to store data-source credentials encrypted at rest in the database and return masked copies to operator-facing UIs.

## Data Source Management Contract

The current contract for provider/source configuration is:

- provider credentials and source-specific settings are stored in `data_sources`
- the Data Sources Control Center at [ui/data_sources.html](../ui/data_sources.html) is the human-facing setup surface
- `.env` is no longer the live source of truth for provider credentials
- legacy provider values can be imported once into the DB during manager initialization
- runtime env projection still exists only so existing jobs that read `os.environ` continue to work while the DB remains authoritative

Keep the encryption root outside the database. The database stores encrypted provider credentials, but the master-key material still comes from deployment-local configuration.

## Operator AI Contract

[operator_ai/agent.js](operator_ai/agent.js) is now a real bounded service module, not a placeholder.

It currently:

- collects operator evidence from runtime health, service status, logs, support snapshot, provider telemetry, watchdogs, and execution barrier endpoints
- sends a strict JSON prompt to the configured LLM backend
- normalizes the response into summary, root cause, failing component, file, patch hint, and `action: null`
- logs decisions to `var/log/ai_operator_log.jsonl` locally, or to `AI_OPERATOR_LOG_PATH` / `OPERATOR_AI_LOG_PATH` when configured
- does not execute runtime-control actions directly

It does not have direct trading authority. Guarded patch preview, apply, and rollback remain mediated by [../boot/operator_server.js](../boot/operator_server.js).

## Guidance

- Keep service boundaries explicit.
- Document network contracts and environment variables whenever a sidecar is introduced or expanded.
- Keep the operator AI contract narrow: strict JSON in, diagnostics out, no free-form autonomous control.
