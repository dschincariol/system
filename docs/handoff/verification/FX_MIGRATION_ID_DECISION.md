# FX Migration ID Decision

Date: 2026-06-24

## Decision

Accept `engine/runtime/schema/migrations/0071_fx_instrument_metadata.py` with `id = 71` as the FX-02 instrument metadata migration.

Do not renumber the committed migration chain to create `0070_fx_instrument_metadata.py`.

## Rationale

The original FX-02 implementation prompt was written when the highest migration was `0069_data_source_provider_accounts.py`, so it reserved `0070_fx_instrument_metadata.py`.

The current committed chain already uses:

- `0070_data_source_populate_evidence.py` with `id = 70`
- `0071_fx_instrument_metadata.py` with `id = 71`

Both files are present in committed `HEAD` (`39fc99f Stabilize repo for next additions`). Renumbering them would change the meaning of applied migration IDs for any database that has already recorded `schema_migrations.id` 70 or 71.

Later handoff material also treats the current numbering as canonical. In particular, the futures enablement prompt states that FX-02 already holds migration `0071` and that futures must take the next slot. Options verification likewise treats `0071_fx_instrument_metadata.py` as the FX no-touch template.

## Verification Criteria

FX verification should now prove all of the following:

- `engine.runtime.schema.migrations.0070_data_source_populate_evidence.id == 70`
- `engine.runtime.schema.migrations.0071_fx_instrument_metadata.id == 71`
- `expected_migration_ids()` is strictly sorted and contiguous
- `0071_fx_instrument_metadata.py` references all nine FX metadata columns
- no prior migration is silently edited or renumbered

The original `0070_fx_instrument_metadata.py` requirement is superseded by this decision.
