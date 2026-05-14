"""Helpers for materializing training dataset provenance bundles."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse

try:
    import pandas as pd
except Exception:  # pragma: no cover - dependency is required in prod but guarded for import safety
    pd = None  # type: ignore[assignment]

DATASET_STORE_ROOT_ENV = "TRAINING_DATASET_STORE_ROOT"
DATASET_URI_PREFIX_ENV = "TRAINING_DATASET_URI_PREFIX"
DEFAULT_DATASET_DIRNAME = "training_datasets"
PARQUET_ENGINE = "pyarrow"


def _clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _safe_int(value: Any, default: int | None = None) -> int | None:
    if value in (None, ""):
        return default
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except Exception:
        return default


def _json_ready(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in dict(value).items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    if hasattr(value, "tolist") and callable(getattr(value, "tolist")):
        try:
            return _json_ready(value.tolist())
        except Exception:
            return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def _parquet_value(value: Any) -> Any:
    normalized = _json_ready(value)
    if normalized is None or isinstance(normalized, (str, int, float, bool)):
        return normalized
    return json.dumps(normalized, separators=(",", ":"), sort_keys=True)


def _default_store_root() -> Path:
    explicit = _clean_text(os.environ.get(DATASET_STORE_ROOT_ENV))
    if explicit:
        return Path(explicit).expanduser()
    try:
        from engine.runtime.db_guard import resolve_db_path

        return Path(resolve_db_path()).resolve().parent / DEFAULT_DATASET_DIRNAME
    except Exception:
        return (Path.cwd() / DEFAULT_DATASET_DIRNAME).resolve()


def _storage_backend() -> str:
    prefix = _clean_text(os.environ.get(DATASET_URI_PREFIX_ENV))
    if not prefix:
        return "local"
    parsed = urlparse(prefix)
    return "object" if str(parsed.scheme or "").strip() else "local"


def _join_uri(prefix: str, *parts: str) -> str:
    clean_prefix = str(prefix or "").rstrip("/")
    clean_parts = [str(part or "").strip("/").replace("\\", "/") for part in parts if str(part or "").strip("/")]
    return "/".join([clean_prefix, *clean_parts]) if clean_prefix else "/".join(clean_parts)


def _derive_dataset_id(dataset: Mapping[str, Any]) -> str:
    model_name = str(dataset.get("model_name") or "dataset").strip().replace(".", "_")
    captured_ts_ms = int(_safe_int(dataset.get("captured_ts_ms"), 0) or 0)
    fingerprint = str(dataset.get("fingerprint") or "").strip()
    fingerprint_part = fingerprint[:12] if fingerprint else "snapshot"
    return f"{model_name}-{captured_ts_ms or 0}-{fingerprint_part}"


def _rows_from_dataset(dataset: Mapping[str, Any]) -> list[dict[str, Any]]:
    sources = dict(dataset.get("sources") or {})
    rows: list[dict[str, Any]] = []
    for source_name, payload in sources.items():
        row = {
            "source_name": str(source_name),
        }
        if isinstance(payload, Mapping):
            for key, value in dict(payload).items():
                row[str(key)] = _parquet_value(value)
        else:
            row["value"] = _parquet_value(payload)
        rows.append(row)
    if rows:
        return rows
    return [{"source_name": "dataset_snapshot", "row_count": int(_safe_int(dataset.get("row_count"), 0) or 0)}]


def normalize_feature_schema(
    *,
    feature_ids: Iterable[Any] | None = None,
    feature_schema: Mapping[str, Any] | None = None,
    feature_set_tag: Any = None,
) -> dict[str, Any]:
    schema = dict(_json_ready(feature_schema) or {}) if isinstance(feature_schema, Mapping) else {}
    ids = [str(fid).strip() for fid in (feature_ids or schema.get("feature_ids") or []) if str(fid).strip()]
    if ids and not isinstance(schema.get("feature_ids"), list):
        schema["feature_ids"] = list(ids)
    if ids and not int(_safe_int(schema.get("feature_count"), 0) or 0):
        schema["feature_count"] = int(len(ids))
    tag = _clean_text(feature_set_tag) or _clean_text(schema.get("feature_set_tag"))
    if not tag and ids:
        try:
            from engine.strategy.feature_registry import feature_set_tag_from_ids

            tag = _clean_text(feature_set_tag_from_ids(list(ids)))
        except Exception:
            tag = None
    if tag:
        schema["feature_set_tag"] = str(tag)
    return schema


def normalize_training_window(
    *,
    captured_ts_ms: int,
    lookback_days: int | None = None,
    lookback_rows: int | None = None,
    training_window: Mapping[str, Any] | None = None,
    symbols: Iterable[Any] | None = None,
    horizons: Iterable[Any] | None = None,
) -> dict[str, Any]:
    window = dict(_json_ready(training_window) or {}) if isinstance(training_window, Mapping) else {}
    if int(_safe_int(lookback_days, 0) or 0) > 0 and not int(_safe_int(window.get("lookback_days"), 0) or 0):
        window["lookback_days"] = int(lookback_days or 0)
    if int(_safe_int(lookback_rows, 0) or 0) > 0 and not int(_safe_int(window.get("lookback_rows"), 0) or 0):
        window["lookback_rows"] = int(lookback_rows or 0)
    if "end_ts_ms" not in window and int(captured_ts_ms) > 0 and int(_safe_int(lookback_days, 0) or 0) > 0:
        window["end_ts_ms"] = int(captured_ts_ms)
    if "start_ts_ms" not in window and int(captured_ts_ms) > 0 and int(_safe_int(lookback_days, 0) or 0) > 0:
        window["start_ts_ms"] = int(captured_ts_ms - (int(lookback_days or 0) * 24 * 60 * 60 * 1000))
    symbol_list = [str(sym).upper().strip() for sym in (symbols or []) if str(sym).strip()]
    if symbol_list and "symbols" not in window:
        window["symbols"] = list(symbol_list)
    horizon_list = [int(value) for value in (horizons or []) if int(_safe_int(value, 0) or 0) > 0]
    if horizon_list and "horizons" not in window:
        window["horizons"] = list(horizon_list)
    return window


def materialize_dataset_snapshot(
    dataset: Mapping[str, Any],
    *,
    row_records: Iterable[Mapping[str, Any]] | None = None,
    feature_schema: Mapping[str, Any] | None = None,
    training_window: Mapping[str, Any] | None = None,
    extra_manifest: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if pd is None:
        raise RuntimeError("pandas_required_for_dataset_store")

    dataset_dict = dict(_json_ready(dataset) or {})
    captured_ts_ms = int(_safe_int(dataset_dict.get("captured_ts_ms"), 0) or 0)
    schema_dict = normalize_feature_schema(
        feature_ids=list(dataset_dict.get("feature_ids") or []),
        feature_schema=feature_schema or dataset_dict.get("feature_schema"),
        feature_set_tag=(feature_schema or {}).get("feature_set_tag") if isinstance(feature_schema, Mapping) else None,
    )
    window_dict = normalize_training_window(
        captured_ts_ms=int(captured_ts_ms),
        lookback_days=_safe_int(dataset_dict.get("lookback_days")),
        lookback_rows=_safe_int(dataset_dict.get("lookback_rows")),
        training_window=training_window or dataset_dict.get("training_window"),
        symbols=list(dataset_dict.get("symbols") or []),
        horizons=list(dataset_dict.get("horizons") or []),
    )
    if schema_dict:
        dataset_dict["feature_schema"] = dict(schema_dict)
    if window_dict:
        dataset_dict["training_window"] = dict(window_dict)

    dataset_id = _derive_dataset_id(dataset_dict)
    store_root = _default_store_root()
    model_family = str(dataset_dict.get("model_name") or "dataset").strip().replace(".", "_")
    bundle_dir = store_root / model_family / dataset_id
    bundle_dir.mkdir(parents=True, exist_ok=True)

    dataset_path = bundle_dir / "dataset.parquet"
    manifest_path = bundle_dir / "manifest.json"

    rows = [dict(record) for record in (row_records or []) if isinstance(record, Mapping)]
    if not rows:
        rows = _rows_from_dataset(dataset_dict)
    parquet_rows = [{str(key): _parquet_value(value) for key, value in row.items()} for row in rows]
    frame = pd.DataFrame(parquet_rows)
    frame.to_parquet(dataset_path, engine=PARQUET_ENGINE, index=False)

    backend = _storage_backend()
    uri_prefix = _clean_text(os.environ.get(DATASET_URI_PREFIX_ENV))
    if backend == "object" and uri_prefix:
        dataset_uri = _join_uri(uri_prefix, model_family, dataset_id, "dataset.parquet")
        manifest_uri = _join_uri(uri_prefix, model_family, dataset_id, "manifest.json")
    else:
        dataset_uri = str(dataset_path)
        manifest_uri = str(manifest_path)

    manifest = {
        "dataset_id": str(dataset_id),
        "model_name": str(dataset_dict.get("model_name") or ""),
        "fingerprint": str(dataset_dict.get("fingerprint") or ""),
        "captured_ts_ms": int(captured_ts_ms),
        "storage_backend": str(backend),
        "dataset_format": "parquet",
        "dataset_uri": str(dataset_uri),
        "dataset_local_path": str(dataset_path),
        "dataset_manifest_uri": str(manifest_uri),
        "dataset_manifest_local_path": str(manifest_path),
        "row_count": int(len(parquet_rows)),
        "columns": [str(column) for column in list(frame.columns)],
        "feature_schema": dict(schema_dict),
        "training_window": dict(window_dict),
        "sources": dict(dataset_dict.get("sources") or {}),
        "extra": dict(_json_ready(extra_manifest) or {}) if isinstance(extra_manifest, Mapping) else {},
    }
    manifest_path.write_text(json.dumps(_json_ready(manifest), separators=(",", ":"), sort_keys=True), encoding="utf-8")

    dataset_dict.update(
        {
            "dataset_id": str(dataset_id),
            "dataset_format": "parquet",
            "dataset_uri": str(dataset_uri),
            "dataset_local_path": str(dataset_path),
            "dataset_manifest_uri": str(manifest_uri),
            "dataset_manifest_local_path": str(manifest_path),
            "storage_backend": str(backend),
            "row_count": int(len(parquet_rows)),
        }
    )
    return dataset_dict


__all__ = [
    "DATASET_STORE_ROOT_ENV",
    "DATASET_URI_PREFIX_ENV",
    "materialize_dataset_snapshot",
    "normalize_feature_schema",
    "normalize_training_window",
]
