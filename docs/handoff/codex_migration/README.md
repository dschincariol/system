# Codex Staged Migration Program

This handoff package is the repo-local control surface for the staged Codex implementation program.

Use it to keep large migration work bounded, auditable, and verifiable across many turns.

## Operating Rules

- Use a strict `DD -> IMPL -> AUDIT` loop for every slice.
- Keep each slice to one boundary change.
- Default slice limit:
  - at most 3 production modules
  - 1 schema family
  - 5 targeted tests
  - 1 doc or ADR update
- Do not let an implementation turn self-certify completeness.
- Record every slice in [SLICE_LEDGER.md](SLICE_LEDGER.md).

## Prompt Files

- Generic prompt template:
  [PROMPT_TEMPLATE.md](PROMPT_TEMPLATE.md)
- Full-scope validation matrix:
  [FULL_SCOPE_VALIDATION.md](FULL_SCOPE_VALIDATION.md)
- Seeded first slice:
  [slices/S01/DD-S01.md](slices/S01/DD-S01.md)
  [slices/S01/IMPL-S01.md](slices/S01/IMPL-S01.md)
  [slices/S01/AUDIT-S01.md](slices/S01/AUDIT-S01.md)

## Default Workflow

1. Run the `DD` prompt for the next slice. No edits.
2. Approve the exact touch set and acceptance criteria.
3. Run the matching `IMPL` prompt.
4. Merge only after the matching `AUDIT` prompt reports no findings.
5. After every 2-3 slices, run an integration prompt and update the ledger.

## Wave Gates

- After `S01-S03`, run:
  - `python -m pytest tests/test_sqlite_contention_relief.py -q`
  - `python -m pytest tests/test_ingestion_runtime_reliability.py -q`
  - `python -m pytest tests/test_storage_contracts.py -q`
  - `python -m pytest tests/test_trade_lifecycle_regressions.py -q`
- After `S04-S08`, run:
  - `python -m pytest tests/test_timescale_integration_hooks.py -q`
  - `python -m pytest tests/test_feature_store.py -q`
  - `python -m pytest tests/test_strategy_feature_store.py -q`
  - `python -m pytest tests/test_inference_engine.py -q`
  - `python -m pytest tests/test_model_registry_catalog.py -q`
- After `S09-S12`, run:
  - `python -m pytest tests/test_broker_order_idempotency_regressions.py -q`
  - `python -m pytest tests/test_broker_apply_orders_modes.py -q`
  - `python -m pytest tests/test_trade_lifecycle_regressions.py -q`
  - `python -m pytest tests/test_dashboard_route_contracts.py -q`
  - `python -m pytest tests/test_validate_repo_contract.py -q`

## Current Starting Point

- `S01` is the first active slice.
- Its goal is to keep raw quote evidence, provider health, and ingestion pipeline health off the immediate SQLite live-stream path.
- The seeded `S01` prompt pack and ledger entry below are the current source of truth for that slice.
