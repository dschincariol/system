# ADR 0002: API Specification Choice

- Status: Accepted
- Date: 2026-04-12

## Context

The repository exposes a combined HTTP surface assembled across `dashboard_server.py`, `engine/api/`, `engine/terminal/api/`, and `routes/data_sources_routes.py`. The current docs describe those modules and route owners, but they do not yet provide a single API contract format that clients can update against consistently.

## Decision

The repository will use OpenAPI 3.1 as the canonical API specification format.

- The human-maintained source of truth lives at `docs/openapi/openapi.yaml`.
- Coverage is incremental from the current baseline rather than retroactively generated from the entire repo at once.
- Contributors must add or update the paths they touch in the same change that modifies the HTTP contract.
- Local README files and route lists remain useful orientation material, but they are not the contract format for client integrations.

## Consequences

- The spec will begin incomplete and become more useful over time as touched endpoints are added.
- The repo does not depend on route-generation frameworks, so the spec is maintained by contributors rather than generated automatically from handlers.
- API changes now have a defined home for request, response, and status-code documentation.
