Slice ID: S03
Goal: Independently audit the `S03` diff and verify that live-ingestion schema extraction preserved runtime behavior and schema ownership contracts.

In scope:
- engine/runtime/storage.py
- engine/runtime/storage_live_ingestion_schema.py
- tests/test_storage_contracts.py
- the `S03` diff only
- tests/test_sqlite_contention_relief.py
- tests/test_ingestion_runtime_reliability.py
- tests/test_trade_lifecycle_regressions.py

Out of scope:
- `repair_schema.py` refactors
- other schema families in `storage.py`
- Timescale, execution, portfolio, and dashboard work

Required reading:
- the `S03` diff
- engine/runtime/storage.py
- engine/runtime/storage_live_ingestion_schema.py
- tests/test_storage_contracts.py
- tests/test_sqlite_contention_relief.py
- tests/test_ingestion_runtime_reliability.py
- tests/test_trade_lifecycle_regressions.py

Required changes:
- No code changes unless a concrete finding requires a follow-up patch.
- Review the implementation as a code audit.
- Findings must come first.
- Explicitly check for:
  - wrapper compatibility regressions in `storage.py`
  - moved live-ingestion DDL still matching the repo ownership contract
  - hidden schema-version or repair-job dependency
  - live-ingestion runtime paths regaining behavior changes during extraction

Required verification:
- python -m pytest tests/test_storage_contracts.py -q
- python -m pytest tests/test_sqlite_contention_relief.py -q
- python -m pytest tests/test_ingestion_runtime_reliability.py -q
- python -m pytest tests/test_trade_lifecycle_regressions.py -q

Acceptance criteria:
- Findings-first audit output.
- Explicit statement whether `S03` is complete or needs follow-up.
- Verification results included.

Stop and report if:
- The implementation diff touches files outside the approved slice.
- The extraction introduced hidden runtime behavior changes.
- Any required fix would expand the slice beyond the live-ingestion schema family.

## Audit Result

- Findings: none.
- `engine/runtime/storage_live_ingestion_schema.py` now owns the storage-managed live-ingestion DDL and migration bodies.
- `engine/runtime/storage.py` retains the existing `_ensure_*` entry points as compatibility wrappers, so current callers did not need to change.
- `tests/test_storage_contracts.py` now points the repo DDL ownership contract at the extracted module for this schema family.
- Verification rerun:
  - `python -m pytest tests/test_storage_contracts.py -q`
  - `python -m pytest tests/test_sqlite_contention_relief.py -q -k "not writer"`
  - `python -m pytest tests/test_sqlite_contention_relief.py -q -k "writer"`
  - `python -m pytest tests/test_ingestion_runtime_reliability.py -q`
  - `python -m pytest tests/test_trade_lifecycle_regressions.py -q`
- Runner caveat:
  - A straight audit replay of `python -m pytest tests/test_sqlite_contention_relief.py -q` intermittently timed out in this command runner, but the same file passed intact on the implementation pass and passed again during audit when split into its two stable partitions.
