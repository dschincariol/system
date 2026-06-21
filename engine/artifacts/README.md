# Artifacts Subsystem

The `engine/artifacts/` package is the content-addressed blob store for the trading runtime. It owns every large binary the system persists outside the Postgres runtime facade — model checkpoints, joblib/pickle/torch payloads, and similar blobs — addressing each by the SHA256 of its bytes, deduplicating identical content, and tracking named aliases plus reference counts so blobs can be promoted, versioned, verified, and garbage-collected. Producers (model training, ensemble blending, NLP/FinBERT enrichment, ingest) write through it; consumers (predictor, scoring, governance) resolve aliases and read verified bytes back.

## Files

- [paths.py](paths.py)
  Resolves the artifacts root (honoring `TS_ARTIFACTS_ROOT`, then `TRADING_DATA`/`DATA_DIR`, then test/local/data-root defaults), validates 64-char lowercase-hex SHA256 strings, and builds the on-disk object path using a 2-level shard layout: `objects/<sha[:2]>/<sha[2:4]>/<sha>`.
- [refs.py](refs.py)
  Defines the frozen `ArtifactRef` value object (sha256, size, content_type, kind, created_ts, metadata) with an `artifact:<sha256>` URI and a `to_metadata()` projection. This file holds only the reference value type; reference counting and alias bookkeeping live in `store.py`.
- [store.py](store.py)
  The read/write API (`LocalArtifactStore`, `ArtifactStore` Protocol, `default_store`). Stages bytes/files to a temp file, hashes them, atomically places the object (with a cross-device copy-and-replace fallback), and records rows in `artifacts`/`artifact_aliases`. Owns alias resolution/versioning, ref-count increment/decrement on alias moves, hash-verified reads, the `ArtifactCorruption` error, and the blob serializer payload builders (`dumps_pickle_artifact_payload`, `dumps_joblib_artifact`, `dumps_torch_artifact_payload`).
- [serialization.py](serialization.py)
  The single sanctioned facade for blob (de)serialization — `dumps/loads/dump/load` for pickle, joblib (when available), and torch. Delegates the actual serializer calls into `store.py` so no serializer call escapes the artifact layer.
- [fsck.py](fsck.py)
  Integrity verifier and garbage collector. `verify()` checks each artifact row for a present object file, matching size, and matching content hash, flags dangling aliases and orphan object files, and logs findings to `artifact_fsck_findings`. `garbage_collect()` deletes objects whose `ref_count` is 0 and whose `created_ts` is older than `older_than_days` (default 30), supporting a `dry_run`.

## Storage Model

Two-tier persistence: object bytes live on the filesystem under `<root>/objects/<sha[:2]>/<sha[2:4]>/<sha>`, while metadata lives in the Postgres-backed runtime store (SQLite-compatible). The store creates and uses three tables:

- `artifacts` — one row per unique blob: `sha256` (PK), `size_bytes`, `content_type`, `kind`, `created_ts`, `metadata` (JSONB), and `ref_count`.
- `artifact_aliases` — append-only `(alias, sha256, set_at)` rows; the newest `set_at` per alias is the current target, giving free alias version history (`resolve`, `list_versions`).
- `artifact_fsck_findings` — fsck and GC audit log (`missing_object`, `size_mismatch`, `hash_mismatch`, `dangling_alias`, `orphan_object`, `gc_deleted`/`gc_would_delete`).

Schema is created on demand only when the connection is not migration-owned; against a `storage_pg`/`psycopg` connection the store defers to the migration-owned schema.

## Reference Counting

Reference counts are driven entirely by aliases. `put`/`put_path` insert artifact rows with `ref_count=0`. Each `set_alias` to a new target increments the new sha's `ref_count` and decrements the previously-aliased sha's count (floored at 0); repointing an alias to the same sha is a no-op. An artifact with `ref_count > 0` is retained; only zero-ref artifacts past the age cutoff are eligible for garbage collection.

## Centralized Serialization Rule

All blob serialization in `engine/` and `tools/` must go through this package. The rule is enforced as a pytest AST lint, `tests/test_no_loose_blob_writes.py`, which walks every non-`__pycache__` module under `engine/` and `tools/`:

- `test_no_serializer_blob_writes_outside_artifact_layer` rejects any call to `joblib.dump`, `torch.save`, `pickle.dump`, or `pickle.dumps` that is not inside `engine/artifacts/`.
- `test_no_binary_file_writes_outside_artifact_layer` rejects `.write_bytes(...)` and `open(..., "wb"/"ab"/"xb")` calls whose target text looks like a blob (markers include `model`, `blob`, `artifact`, `checkpoint`, `weight`, `joblib`, `pickle`, `.pkl`, `.pt`, `.pth`) outside `engine/artifacts/`.
- `test_artifact_package_has_no_hardcoded_linux_artifact_root` keeps the package free of hardcoded `/var/lib/trading` paths.

Callers therefore ask `engine.artifacts.serialization` for bytes (or write through the store) instead of invoking a serializer or binary file write directly; the serializer call itself only happens inside `store.py`.

## Operational Entry Point

There is no CLI in this package. Verification runs as the registered `artifacts_fsck` job (`engine/strategy/jobs/artifacts_fsck.py`), which calls `fsck.verify(store, log_findings=True)` daily and reports `ok` plus the structured findings list.
