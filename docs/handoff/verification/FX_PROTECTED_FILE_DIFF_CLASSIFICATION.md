# FX Protected-File Diff Classification

Date: 2026-06-24
Repo: `/home/david/gitsandbox/system/system`

## Decision

FX-GO-05 is resolved by explicit exclusion rationale, not by reverting unrelated
working-tree work. The protected-file diffs are nonempty, but none are FX-04
prediction-routing/clock/schema changes or FX-08 backend read-model changes.

The FX verifier should treat these paths as owner-exempt for the FX no-touch
guard while still reporting that the working tree is dirty:

```text
engine/runtime/schema/table_classification.py | 46 +++++++++++++++++++++++++++
engine/runtime/storage_pg.py                  |  7 ++--
engine/runtime/storage_sqlite.py              | 42 +++++++++++++++++++++++-
engine/strategy/models/itransformer.py        | 18 ++++++++++-
engine/strategy/models/lgbm_regressor.py      | 12 +++++++
engine/strategy/models/patchtst.py            | 19 ++++++++++-
6 files changed, 139 insertions(+), 5 deletions(-)
```

No source files were reverted or moved for FX-GO-05.

## Classification

| Path | Classification | Evidence | FX impact |
| --- | --- | --- | --- |
| `engine/strategy/models/itransformer.py` | Owner-exempt non-FX model guard | Adds `feature_schema_flags` and `assert_feature_schema_runtime_parity` at `:25`, records flags at `:109`, and fails stale artifacts with `runtime_feature_flag_mismatch` at `:245`. | Not an FX-04 routing, regime, label, or clock change. |
| `engine/strategy/models/lgbm_regressor.py` | Owner-exempt non-FX model guard | Adds feature flag metadata at `:148` and runtime feature-flag parity rejection at `:447`. | Not an FX model-selection or FX regime-routing change. |
| `engine/strategy/models/patchtst.py` | Owner-exempt non-FX model guard | Adds feature flag metadata at `:116` and runtime feature-flag parity rejection at `:246`. | Not an FX-04 routing, regime, label, or clock change. |
| `engine/runtime/storage_pg.py` | Owner-exempt storage reliability change | Replaces `SET LOCAL ... = %s` with parameterized `SELECT set_config(..., true)` at `:1012` and `:1015`. | No FX schema or label/read-model payload change. |
| `engine/runtime/storage_sqlite.py` | Owner-exempt futures/options metadata affinity change; FX-02 affinity already accepted separately | Adds `fut_*` and `opt_*` explicit column affinities at `:273` and audit columns at `:2768`. | Not an FX-04/FX-08 change; existing FX `pip_size`/`pnl_ccy` affinity remains FX-02 evidence. |
| `engine/runtime/schema/table_classification.py` | Owner-exempt equity/options/futures table classification change | Adds `corporate_actions` at `:381`, `options_predictor_shadow` at `:548`, and futures tables at `:557`. | Not an FX table, schema, clock, or UI read-model change. |

## Validation

Command:

```bash
python -m pytest -q tests/test_itransformer_model.py tests/test_lgbm_regressor.py tests/test_patchtst_model.py tests/test_model_feature_schema_drift.py tests/test_storage_pg_write_timeouts.py tests/test_storage_sqlite_decomposition_contract.py tests/test_schema_classification.py tests/test_storage_migrator.py tests/test_futures_instrument_metadata_storage.py tests/test_futures_instrument_migration.py tests/test_universe_option_metadata.py
```

Exit: `0`.

Key output:

```text
.........................................sss............... [100%]
```

Interpretation: relevant model schema, storage timeout, SQLite affinity,
classification, migration, futures metadata, and option metadata contract tests
pass with the protected-file diffs present.

## Verification Use

For FX-04 and FX-08 no-touch checks, the accepted result is now:

- empty protected-file diff, or
- nonempty diff fully covered by this classification document.

This does not change the FX acceptance requirement that new FX work must avoid
model adapter edits, backend schema edits, and UI read-model payload additions.
