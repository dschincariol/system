# Codex DB Prompt 05 — Object Storage for Model Artifacts and Large Blobs

You are working in a Python systematic trading system. Today, model
artifacts (joblib pickles, PatchTST `state_dict`, PyTorch / FinBERT
weights), raw text bodies (full SEC filings, full earnings-call
transcripts, full news article bodies), and any other large binary
payloads either live on local disk in ad-hoc paths or risk drifting
into the database as `BYTEA` columns. Both paths hurt: file system
sprawl makes retention impossible; binary in the OLTP store balloons
backups and slows queries.

This prompt introduces a **content-addressed local object store** at
`/var/lib/trading/artifacts/` — a simple, reproducible filesystem
layout that the application uses through a single API. The object
store is the system of record for binary blobs; Postgres holds the
metadata, the content hash, and the path / URI. The same API can be
later swapped for MinIO or S3 without touching call sites — but for
the single-server deployment, local filesystem is the right answer.

## Linux-only note

This is **Linux-only application code** for development, staging, and
production hosts. The artifact root is read from `TS_ARTIFACTS_ROOT`
with a default of `/var/lib/trading/artifacts` computed by
`engine/runtime/platform.py`. Every path uses `pathlib.Path`. Atomic
writes use `os.replace()`. Read
`docs/codex_prompts/database/CROSS_PLATFORM.md` first.

## Goal

1. A `engine/artifacts/` subpackage with one API for "put a blob,
   get a blob, list versions of a logical artifact, garbage-collect
   orphans."
2. Content-addressed layout under `${TS_ARTIFACTS_ROOT}/`:
   `objects/<sha2:0..2>/<sha2:2..4>/<sha2>` (Git-style sharding).
   The root is platform-defaulted; the layout under the root is
   identical on every platform.
3. Postgres metadata table `artifacts` keyed by content hash, with
   logical-name indirection table `artifact_aliases` for "current
   model" pointers.
4. Migration of every current on-disk model location into the new
   store.
5. A daily fsck-style verifier that confirms every metadata row has
   a matching object on disk and vice versa, and a garbage-collector
   for unreferenced objects.

## Files to read first (read-only)

- `engine/model_registry.py` — current model registration; this is
  where artifact paths live today.
- `engine/strategy/champion_manager.py`,
  `engine/strategy/promotion_audit.py` — readers of artifact paths
  during promotion.
- `engine/strategy/temporal_predictor.py`,
  `engine/strategy/embed_regressor.py` — current save / load paths.
- Any existing `joblib.dump` / `torch.save` calls (grep) — every one
  must route through the new API after this prompt.
- `engine/data/ingest/sec_edgar_ingest.py` and the transcripts and
  news ingestion modules — sources of large text payloads that should
  go to artifact storage instead of TEXT columns.
- `engine/runtime/schema/table_classification.py` — to register the
  two new tables.

## Files to create

- `engine/artifacts/__init__.py`
- `engine/artifacts/store.py` — `ArtifactStore` Protocol + the
  default `LocalArtifactStore` implementation. API:
  - `put(data: bytes, *, content_type: str, kind: str,
         alias: str | None = None,
         metadata: dict | None = None) -> ArtifactRef`
  - `put_path(path: Path, ...) -> ArtifactRef` — for streaming
    large files without loading them into memory.
  - `get_bytes(ref: ArtifactRef) -> bytes`
  - `open(ref: ArtifactRef) -> BinaryIO` — preferred for large
    blobs; streams from disk.
  - `resolve(alias: str) -> ArtifactRef | None` — alias →
    content hash.
  - `set_alias(alias: str, ref: ArtifactRef)` — atomic alias
    update; previous alias target retained as a row in
    `artifact_aliases` history.
  - `list_aliases(prefix: str | None = None) -> list[str]`.
- `engine/artifacts/refs.py` — `ArtifactRef` dataclass:
  `sha256: str, size: int, content_type: str, kind: str, created_ts:
  datetime, metadata: dict`.
- `engine/artifacts/paths.py` — `object_path(sha256) ->
  Path` that returns
  `${TS_ARTIFACTS_ROOT}/objects/<00..ff>/<00..ff>/<sha256>` resolved
  via `engine/runtime/platform.default_data_root()` /
  `TS_ARTIFACTS_ROOT`. Pure `pathlib.Path`; no string-joined paths.
- `engine/artifacts/fsck.py` — verifier and garbage collector.
- `engine/strategy/jobs/artifacts_fsck.py` — daily systemd-timer
  job (registered in `job_registry`).
- `engine/runtime/schema/migrations/0005_artifacts.py` — adds
  `artifacts` and `artifact_aliases` tables. Schema:
  ```
  CREATE TABLE artifacts (
      sha256 TEXT PRIMARY KEY,
      size_bytes BIGINT NOT NULL,
      content_type TEXT NOT NULL,
      kind TEXT NOT NULL,                 -- e.g. 'model','filing','transcript','news'
      created_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      ref_count INTEGER NOT NULL DEFAULT 0
  );
  CREATE INDEX artifacts_kind_created ON artifacts (kind, created_ts DESC);
  CREATE INDEX artifacts_metadata_gin ON artifacts USING GIN (metadata jsonb_path_ops);

  CREATE TABLE artifact_aliases (
      alias TEXT NOT NULL,
      sha256 TEXT NOT NULL REFERENCES artifacts(sha256),
      set_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (alias, set_at)
  );
  CREATE INDEX artifact_aliases_current
      ON artifact_aliases (alias, set_at DESC);
  ```
- `tools/migrate_artifacts.py` — one-shot script: scan known model
  directories, register each file as an artifact, set aliases (e.g.
  `model:temporal_predictor:AAPL:current` → latest hash), update
  callers.
- `tests/test_artifact_store_local.py`
- `tests/test_artifact_store_alias_history.py`
- `tests/test_artifact_fsck.py`
- `tests/test_artifact_migrations.py`
- `tests/test_no_loose_blob_writes.py` — guard test: no `joblib.dump`
  / `torch.save` calls outside `engine/artifacts/` (only the store
  may write the on-disk format).

## Files to modify

- `engine/model_registry.py` — model rows reference an artifact by
  alias (`model:<family>:<symbol>:current`) rather than a raw path.
- `engine/strategy/temporal_predictor.py`,
  `engine/strategy/embed_regressor.py`, and the new model families
  (prompt 05 of the original 1–10 series) — `save()` / `load()`
  go through `ArtifactStore`.
- `engine/strategy/promotion_audit.py` — record the artifact hash
  promoted, not a path.
- `engine/runtime/job_registry.py` — register `artifacts_fsck` as a
  daily job.
- `engine/runtime/schema/table_classification.py` — classify
  `artifacts` and `artifact_aliases` as **regular** tables.

## Implementation plan

1. **Layout.** `objects/<00..ff>/<00..ff>/<sha256>`. Two-byte
   sharding caps directory entries at ~65 k. Files are written
   atomically: `temp/...` then `rename` into place.
2. **Atomic alias updates.** `set_alias` opens a Postgres
   transaction, inserts the new `(alias, sha256, set_at)` row,
   commits. Reads use the latest row by `alias` ordered by `set_at
   DESC`. No symlink games.
3. **Reference counting.** `ref_count` on `artifacts` is incremented
   when an alias points at it and decremented when the last alias
   moves away. Garbage collection only removes objects with
   `ref_count = 0` AND age > 30 days, to give human reviewers time
   to roll back.
4. **Streaming.** `put_path(...)` reads the file in 1 MB chunks while
   computing SHA-256, streams into the destination file. Memory is
   bounded.
5. **`open()`** returns a real file handle backed by the on-disk
   object so callers (PyTorch `torch.load`, `joblib.load`) work
   directly with it.
6. **fsck.** Walk metadata: every row has an existing file with the
   declared size and SHA matches. Walk filesystem: every object has
   a metadata row. Discrepancies are logged to a new
   `artifact_fsck_findings` table; nothing is auto-deleted.
7. **GC.** Separate from fsck; only acts when `ref_count = 0` and
   age > 30 days, and even then it logs every deletion.
8. **No mutable artifacts.** Once an object is written, its content
   is fixed. Updates produce a new object with a new hash and update
   the alias.

## Performance targets

- `put_path()` of a 100 MB model file completes in
  **< 2 s** on a single NVMe disk (limited by SHA-256 streaming).
- `get_bytes()` of a 1 KB blob from a warm cache returns in
  **< 0.5 ms** (filesystem cache).
- `resolve(alias)` for a current-pointer lookup returns in
  **< 0.5 ms** through the storage layer.
- fsck of 100 000 objects completes in **< 5 minutes**.

## Acceptance criteria

- [ ] No `joblib.dump`, `torch.save`, or open(...).write of model
      / blob payloads exists outside `engine/artifacts/`.
- [ ] Every model row in `model_registry` references an artifact
      alias.
- [ ] Re-saving the same model bytes produces the same SHA-256 and
      reuses the existing object (no duplication on disk).
- [ ] Setting an alias is atomic; mid-update reads always see one
      consistent target.
- [ ] fsck on a healthy store reports zero findings; tampering with
      one object on disk produces one finding.
- [ ] GC never deletes an object with `ref_count > 0`; never deletes
      an object younger than 30 days.
- [ ] `tools/migrate_artifacts.py` is idempotent.
- [ ] All tests pass on Linux runners; the artifact directory layout
      is identical across Linux dev, staging, and production hosts.
- [ ] No hardcoded `/var/lib/trading` string literal anywhere in
      `engine/artifacts/` (lint-tested).

## Test plan

- `tests/test_artifact_store_local.py` — put / get / streaming
  put_path; idempotency for identical content; sharded path layout.
- `tests/test_artifact_store_alias_history.py` — alias history is
  preserved; latest pointer reads cleanly.
- `tests/test_artifact_fsck.py` — synthetic discrepancies (missing
  file, size mismatch, hash mismatch, orphan file) each produce
  exactly one finding.
- `tests/test_artifact_migrations.py` — schema migration applies
  cleanly.
- `tests/test_no_loose_blob_writes.py` — AST scan; fails build on
  any external `joblib.dump` / `torch.save`.

Run: `pytest -q tests/test_artifact_store_local.py
tests/test_artifact_store_alias_history.py tests/test_artifact_fsck.py
tests/test_artifact_migrations.py tests/test_no_loose_blob_writes.py`

## Out of scope

- MinIO / S3 backend. The Protocol is designed to allow it, but the
  default `LocalArtifactStore` is what ships. Swap in a follow-up
  prompt only when the deployment grows beyond one server.
- Replication of the artifact store. Backups are prompt 07's job
  (the artifact directory is included in the backup set).
- Encryption at rest of artifact files. Disk-level encryption (LUKS)
  is the right place for this; out of scope here.
- A web UI for browsing artifacts. CLI only.
