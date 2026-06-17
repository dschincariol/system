# OpenAPI Baseline

This directory is the canonical home for the repository's OpenAPI source of truth.

## Current State

- `openapi.yaml` establishes the baseline OpenAPI 3.1 document.
- The baseline now covers the current system, jobs, data-source, broker-config, and browser-terminal paths that are highest value for operators and maintainers.
- Coverage is still incomplete for the rest of the aggregated `/api/*` surface.
- Contributors should add or update the paths they touch instead of waiting for a full repo-wide backfill.

## Scope

The intended scope includes the HTTP surfaces assembled across:

- `dashboard_server.py`
- `engine/api/`
- `engine/terminal/api/`
- `routes/data_sources_routes.py`

It does not yet model the separate Node operator server routes in `boot/operator_server.js`.

## Maintenance Rule

If a change alters an HTTP contract, update `openapi.yaml` in the same change.

If a touched endpoint is not yet represented in the spec, add that endpoint instead of leaving the gap for later.
