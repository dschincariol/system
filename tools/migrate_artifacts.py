"""One-shot migration of known local model files into artifact storage."""

from __future__ import annotations
import logging

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.artifacts.store import LocalArtifactStore
from engine.runtime import dbapi_compat as dbapi
from engine.runtime.storage import connect

MODEL_PATTERNS = (
    "*.pkl",
    "*.pickle",
    "*.joblib",
    "*.pt",
    "*.pth",
    "*.bin",
)


def _default_source_roots() -> list[Path]:
    candidates = [
        Path(os.environ.get("MODEL_ARTIFACT_ROOT", "")),
        Path(os.environ.get("ARTIFACT_ROOT", "")),
        Path(os.environ.get("DB_PATH", "")) / "models",
        ROOT / "models",
        ROOT / "artifacts",
        ROOT / "data" / "models",
    ]
    out: list[Path] = []
    for candidate in candidates:
        text = str(candidate)
        if not text or text == ".":
            continue
        path = candidate.expanduser()
        if path.exists() and path.is_dir() and path not in out:
            out.append(path)
    return out


def _iter_model_files(source_root: Path):
    seen: set[Path] = set()
    for pattern in MODEL_PATTERNS:
        for path in source_root.rglob(pattern):
            resolved = path.resolve()
            if resolved in seen or not resolved.is_file():
                continue
            seen.add(resolved)
            yield resolved


def _alias_for(path: Path, source_root: Path) -> str:
    rel = path.resolve().relative_to(source_root.resolve())
    parts = [part for part in rel.with_suffix("").parts if part]
    family = parts[0] if parts else path.stem
    symbol = parts[-2] if len(parts) >= 2 else "global"
    return f"model:{family}:{str(symbol).upper()}:current"


def _commit(con: Any) -> None:
    commit = getattr(con, "commit", None)
    if callable(commit):
        commit()


def _close(con: Any) -> None:
    close = getattr(con, "close", None)
    if callable(close):
        close()


def _table_available(con: Any, table: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def _ensure_columns(con: Any, table: str, columns: tuple[str, ...]) -> None:
    for column in columns:
        try:
            con.execute(f"ALTER TABLE {table} ADD COLUMN {column}")
        except Exception:
            logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


def _store_blob(
    store: LocalArtifactStore,
    blob: Any,
    *,
    alias: str,
    content_type: str = "application/octet-stream",
    metadata: dict[str, Any] | None = None,
) -> tuple[str, int]:
    payload = bytes(blob or b"")
    ref = store.put(
        payload,
        content_type=content_type,
        kind="model",
        alias=alias,
        metadata=dict(metadata or {}),
    )
    return ref.sha256, int(ref.size)


def _migrate_embed_models(con: Any, store: LocalArtifactStore, *, dry_run: bool) -> list[dict[str, Any]]:
    if not _table_available(con, "embed_models2"):
        return []
    if not dry_run:
        _ensure_columns(con, "embed_models2", ("artifact_sha256 TEXT", "artifact_alias TEXT"))
    rows = con.execute(
        """
        SELECT key_type, key, horizon_s, ts_ms, n, dim, model_blob
        FROM embed_models2
        WHERE model_blob IS NOT NULL AND LENGTH(model_blob) > 0
        """
    ).fetchall() or []
    migrated: list[dict[str, Any]] = []
    for key_type, key, horizon_s, ts_ms, n, dim, blob in rows:
        alias = f"model:embed_regressor:{key_type}:{key}:{int(horizon_s)}:current"
        item = {"table": "embed_models2", "alias": alias, "key_type": str(key_type), "key": str(key)}
        if not dry_run:
            sha, size = _store_blob(
                store,
                blob,
                alias=alias,
                metadata={
                    "migration": "tools/migrate_artifacts.py",
                    "model_name": "embed_regressor",
                    "key_type": str(key_type),
                    "key": str(key),
                    "horizon_s": int(horizon_s),
                    "ts_ms": int(ts_ms or 0),
                    "n": int(n or 0),
                    "dim": int(dim or 0),
                },
            )
            con.execute(
                """
                UPDATE embed_models2
                SET model_blob=?, artifact_sha256=?, artifact_alias=?
                WHERE key_type=? AND key=? AND horizon_s=?
                """,
                (dbapi.Binary(b""), sha, alias, str(key_type), str(key), int(horizon_s)),
            )
            item.update({"sha256": sha, "size_bytes": size})
        migrated.append(item)
    return migrated


def _migrate_temporal_models(con: Any, store: LocalArtifactStore, *, dry_run: bool) -> list[dict[str, Any]]:
    if not _table_available(con, "temporal_models"):
        return []
    if not dry_run:
        _ensure_columns(con, "temporal_models", ("artifact_sha256 TEXT", "artifact_alias TEXT"))
    rows = con.execute(
        """
        SELECT key_type, key, horizon_s, ts_ms, n, embed_dim, seq_len, model_kind, model_blob
        FROM temporal_models
        WHERE model_blob IS NOT NULL AND LENGTH(model_blob) > 0
        """
    ).fetchall() or []
    migrated: list[dict[str, Any]] = []
    for key_type, key, horizon_s, ts_ms, n, embed_dim, seq_len, model_kind, blob in rows:
        alias = f"model:temporal_predictor:{key_type}:{key}:{int(horizon_s)}:current"
        item = {"table": "temporal_models", "alias": alias, "key_type": str(key_type), "key": str(key)}
        if not dry_run:
            sha, size = _store_blob(
                store,
                blob,
                alias=alias,
                metadata={
                    "migration": "tools/migrate_artifacts.py",
                    "model_name": "temporal_predictor",
                    "key_type": str(key_type),
                    "key": str(key),
                    "horizon_s": int(horizon_s),
                    "ts_ms": int(ts_ms or 0),
                    "n": int(n or 0),
                    "embed_dim": int(embed_dim or 0),
                    "seq_len": int(seq_len or 0),
                    "model_kind": str(model_kind or ""),
                },
            )
            con.execute(
                """
                UPDATE temporal_models
                SET model_blob=?, artifact_sha256=?, artifact_alias=?
                WHERE key_type=? AND key=? AND horizon_s=?
                """,
                (dbapi.Binary(b""), sha, alias, str(key_type), str(key), int(horizon_s)),
            )
            item.update({"sha256": sha, "size_bytes": size})
        migrated.append(item)
    return migrated


def _migrate_gbm_models(con: Any, store: LocalArtifactStore, *, dry_run: bool) -> list[dict[str, Any]]:
    if not _table_available(con, "gbm_models"):
        return []
    if not dry_run:
        _ensure_columns(con, "gbm_models", ("artifact_sha256 TEXT", "artifact_alias TEXT"))
    rows = con.execute(
        """
        SELECT model_name, version, created_ts, blob
        FROM gbm_models
        WHERE blob IS NOT NULL AND LENGTH(blob) > 0
        """
    ).fetchall() or []
    migrated: list[dict[str, Any]] = []
    for model_name, version, created_ts, blob in rows:
        alias = f"model:gbm_regressor:{model_name}:current"
        item = {"table": "gbm_models", "alias": alias, "model_name": str(model_name), "version": str(version)}
        if not dry_run:
            sha, size = _store_blob(
                store,
                blob,
                alias=alias,
                content_type="application/vnd.lightgbm.text+json",
                metadata={
                    "migration": "tools/migrate_artifacts.py",
                    "model_name": str(model_name),
                    "version": str(version),
                    "created_ts": int(created_ts or 0),
                },
            )
            con.execute(
                """
                UPDATE gbm_models
                SET blob=?, artifact_sha256=?, artifact_alias=?
                WHERE model_name=? AND version=?
                """,
                (dbapi.Binary(b""), sha, alias, str(model_name), str(version)),
            )
            item.update({"sha256": sha, "size_bytes": size})
        migrated.append(item)
    return migrated


def _migrate_hmm_models(con: Any, store: LocalArtifactStore, *, dry_run: bool) -> list[dict[str, Any]]:
    if not _table_available(con, "hmm_regime_models"):
        return []
    if not dry_run:
        _ensure_columns(con, "hmm_regime_models", ("artifact_sha256 TEXT", "artifact_alias TEXT"))
    rows = con.execute(
        """
        SELECT id, created_ts_ms, symbol, num_states, model_blob
        FROM hmm_regime_models
        WHERE model_blob IS NOT NULL AND LENGTH(model_blob) > 0
        """
    ).fetchall() or []
    migrated: list[dict[str, Any]] = []
    for row_id, created_ts_ms, symbol, num_states, blob in rows:
        alias = f"model:hmm_regime:{str(symbol or 'SPY').upper().strip()}:current"
        item = {"table": "hmm_regime_models", "alias": alias, "id": int(row_id)}
        if not dry_run:
            sha, size = _store_blob(
                store,
                blob,
                alias=alias,
                content_type="application/python-pickle",
                metadata={
                    "migration": "tools/migrate_artifacts.py",
                    "model_name": "hmm_regime",
                    "symbol": str(symbol or ""),
                    "created_ts_ms": int(created_ts_ms or 0),
                    "num_states": int(num_states or 0),
                },
            )
            con.execute(
                """
                UPDATE hmm_regime_models
                SET model_blob=?, artifact_sha256=?, artifact_alias=?
                WHERE id=?
                """,
                (dbapi.Binary(b""), sha, alias, int(row_id)),
            )
            item.update({"sha256": sha, "size_bytes": size})
        migrated.append(item)
    return migrated


def _migrate_rl_policy_models(con: Any, store: LocalArtifactStore, *, dry_run: bool) -> list[dict[str, Any]]:
    if not _table_available(con, "rl_strategy_policy_models"):
        return []
    if not dry_run:
        _ensure_columns(con, "rl_strategy_policy_models", ("artifact_sha256 TEXT", "artifact_alias TEXT"))
    rows = con.execute(
        """
        SELECT policy_name, ts_ms, n, dim, model_blob
        FROM rl_strategy_policy_models
        WHERE model_blob IS NOT NULL AND LENGTH(model_blob) > 0
        """
    ).fetchall() or []
    migrated: list[dict[str, Any]] = []
    for policy_name, ts_ms, n, dim, blob in rows:
        alias = f"model:rl_strategy_policy:{policy_name}:current"
        item = {"table": "rl_strategy_policy_models", "alias": alias, "policy_name": str(policy_name)}
        if not dry_run:
            sha, size = _store_blob(
                store,
                blob,
                alias=alias,
                metadata={
                    "migration": "tools/migrate_artifacts.py",
                    "model_name": "rl_strategy_policy",
                    "policy_name": str(policy_name),
                    "ts_ms": int(ts_ms or 0),
                    "n": int(n or 0),
                    "dim": int(dim or 0),
                },
            )
            con.execute(
                """
                UPDATE rl_strategy_policy_models
                SET model_blob=?, artifact_sha256=?, artifact_alias=?
                WHERE policy_name=?
                """,
                (dbapi.Binary(b""), sha, alias, str(policy_name)),
            )
            item.update({"sha256": sha, "size_bytes": size})
        migrated.append(item)
    return migrated


def _migrate_ensemble_meta(con: Any, store: LocalArtifactStore, *, dry_run: bool) -> list[dict[str, Any]]:
    if not _table_available(con, "ensemble_blend_weights"):
        return []
    if not dry_run:
        _ensure_columns(con, "ensemble_blend_weights", ("meta_artifact_sha256 TEXT", "meta_artifact_alias TEXT"))
    rows = con.execute(
        """
        SELECT id, created_ts, mode, regime, meta_blob
        FROM ensemble_blend_weights
        WHERE meta_blob IS NOT NULL AND LENGTH(meta_blob) > 0
        """
    ).fetchall() or []
    migrated: list[dict[str, Any]] = []
    for row_id, created_ts, mode, regime, blob in rows:
        alias = f"model:ensemble_blender:{str(mode or 'stacked').lower()}:{str(regime or 'global')}:current"
        item = {"table": "ensemble_blend_weights", "alias": alias, "id": int(row_id)}
        if not dry_run:
            sha, size = _store_blob(
                store,
                blob,
                alias=alias,
                content_type="application/python-pickle",
                metadata={
                    "migration": "tools/migrate_artifacts.py",
                    "model_name": "ensemble_blender",
                    "mode": str(mode or ""),
                    "regime": str(regime or "global"),
                    "created_ts": int(created_ts or 0),
                },
            )
            con.execute(
                """
                UPDATE ensemble_blend_weights
                SET meta_blob=NULL, meta_artifact_sha256=?, meta_artifact_alias=?
                WHERE id=?
                """,
                (sha, alias, int(row_id)),
            )
            item.update({"sha256": sha, "size_bytes": size})
        migrated.append(item)
    return migrated


def _migrate_database_blobs(store: LocalArtifactStore, *, dry_run: bool) -> list[dict[str, Any]]:
    con = connect()
    try:
        migrated: list[dict[str, Any]] = []
        for fn in (
            _migrate_embed_models,
            _migrate_temporal_models,
            _migrate_gbm_models,
            _migrate_hmm_models,
            _migrate_rl_policy_models,
            _migrate_ensemble_meta,
        ):
            migrated.extend(fn(con, store, dry_run=dry_run))
        if not dry_run:
            _commit(con)
        return migrated
    finally:
        _close(con)


def migrate(source_roots: list[Path], *, dry_run: bool = False, include_database: bool = True) -> dict[str, object]:
    store = LocalArtifactStore()
    migrated: list[dict[str, object]] = []
    for source_root in source_roots:
        root = source_root.expanduser().resolve()
        for path in _iter_model_files(root):
            alias = _alias_for(path, root)
            item = {
                "path": str(path),
                "alias": str(alias),
            }
            if not dry_run:
                ref = store.put_path(
                    path,
                    content_type="application/octet-stream",
                    kind="model",
                    alias=alias,
                    metadata={"source_path": str(path), "migration": "tools/migrate_artifacts.py"},
                )
                item["sha256"] = ref.sha256
                item["size_bytes"] = int(ref.size)
            migrated.append(item)
    database_migrated = _migrate_database_blobs(store, dry_run=bool(dry_run)) if include_database else []
    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "migrated": migrated,
        "database_migrated": database_migrated,
        "count": len(migrated) + len(database_migrated),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", action="append", default=[], help="Directory to scan for existing model files")
    parser.add_argument("--dry-run", action="store_true", help="Print planned registrations without writing")
    parser.add_argument("--skip-database", action="store_true", help="Only scan filesystem model files")
    args = parser.parse_args(argv)

    roots = [Path(value) for value in args.source_root] if args.source_root else _default_source_roots()
    result = migrate(roots, dry_run=bool(args.dry_run), include_database=not bool(args.skip_database))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
