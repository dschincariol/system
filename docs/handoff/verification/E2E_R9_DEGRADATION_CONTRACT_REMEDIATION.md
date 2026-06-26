# E2E-R9 Degradation Contract Remediation

Date: 2026-06-25

## Contract

Read endpoints that hit an optional or not-yet-migrated table must not surface raw database exception text or return HTTP 500. A known missing table degrades to HTTP 200 with `ok:true`, an empty `rows` list, and a stable `<table>_missing` reason. Bad table input remains a client error and returns HTTP 400.

## Production Changes

- `GET /api/execution/advisories` now recognizes missing `execution_ai_advisory` or `execution_ai_advisory_actions` tables and returns a degraded 200 payload with empty rows/items and a stable reason.
- `GET /api/validation` now recognizes a missing `validation_scores` table and returns `ok:true`, `rows:[]`, and `reason:"validation_scores_missing"` instead of returning raw SQL exception text.
- `GET /api/audit/records` now catches the stricter audit-table classifier error after generic identifier validation. Syntactically valid but non-audit tables such as `alerts` return HTTP 400 with `error:"not_audit_table"` and do not fall through to the generic 500 handler.
- Generic advisory and audit read failures still log server-side diagnostics, but the client-facing `error` field is now a stable code rather than raw database exception text.

## Enforcement

The shared helper in `engine/api/degradation.py` centralizes missing-table detection and degraded read payload construction. The HTTP regression in `tests/test_api_degradation_contract.py` runs the real handlers through `engine.api.http_transport.build_handler` against a cold SQLite simulation database with the optional read tables absent. It asserts:

- advisories returns HTTP 200 with `reason:"execution_ai_advisory_missing"`;
- validation returns HTTP 200 with `reason:"validation_scores_missing"`;
- neither response contains raw `no such table` text;
- `audit/records?table=alerts` returns HTTP 400 `not_audit_table`;
- the SQL-injection table path still returns HTTP 400 `unauthorized_table`;
- a real audit table, `trade_attribution_ledger`, still returns HTTP 200.
