"""First-class LightGBM tabular regressor family.

The family intentionally resolves its feature order through
``feature_registry.expected_columns`` during both training and serving so the
model artifact remains bound to the same schema contract used by live feature
construction.
"""

from __future__ import annotations
import logging

import json
import math
import os
import time
import uuid
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from engine.artifacts.serialization import (
    dump_pickle_artifact,
    dumps_pickle_artifact,
    load_pickle_artifact,
    loads_pickle_artifact,
)
from engine.artifacts.store import LocalArtifactStore
from engine.model_registry import register_model, register_model_family
from engine.runtime.storage import connect, init_db
from engine.strategy import feature_registry
from engine.strategy.feature_registry import build_feature_snapshot, feature_set_tag_from_ids
from engine.strategy.model_lifecycle import (
    load_lifecycle_plan,
    record_version_performance,
    register_model_version,
    update_model_version_status,
    version_from_ts,
)
from engine.strategy.ensemble.oos_store import upsert_oos_predictions

FAMILY = "lgbm_regressor"
DEFAULT_MODEL_NAME = FAMILY
DEFAULT_MODEL_KIND = "lightgbm"
DEFAULT_MIN_SAMPLES = int(os.environ.get("LGBM_MIN_SAMPLES", "20"))
DEFAULT_LOOKBACK_DAYS = int(os.environ.get("LGBM_LOOKBACK_DAYS", "365"))
DEFAULT_HORIZON_S = int(os.environ.get("LGBM_HORIZON_S", os.environ.get("MODEL_HORIZON_MEDIUM_S", "3600")))
LOG = logging.getLogger(__name__)


def _register_family() -> None:
    try:
        register_model_family(
            FAMILY,
            training_entrypoint="engine.strategy.jobs.train_lgbm_models",
            inference_entrypoint="engine.strategy.models.lgbm_regressor.LGBMRegressorModel",
            default_stage="shadow",
            promotion_guard="engine.strategy.promotion_guard.assess_challenger",
        )
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


_register_family()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _expected_columns(
    feature_ids: Sequence[Any] | None = None,
    *,
    model_name: str = FAMILY,
    model_spec: Mapping[str, Any] | None = None,
) -> list[str]:
    fn = getattr(feature_registry, "expected_columns", None)
    ids = [str(item).strip() for item in list(feature_ids or []) if str(item or "").strip()]
    spec = dict(model_spec or {})
    if ids and "feature_ids" not in spec:
        spec["feature_ids"] = list(ids)
    if callable(fn):
        try:
            return [
                str(item).strip()
                for item in fn(ids or None, model_name=str(model_name), model_spec=spec or None)
                if str(item or "").strip()
            ]
        except TypeError:
            try:
                return [str(item).strip() for item in fn(str(model_name)) if str(item or "").strip()]
            except TypeError:
                return [str(item).strip() for item in fn() if str(item or "").strip()]
    return feature_registry.resolve_feature_ids(
        ids or None,
        model_name=str(model_name),
        model_spec=spec or None,
    )


def _feature_schema(feature_ids: Sequence[Any]) -> dict[str, Any]:
    columns = [str(item).strip() for item in list(feature_ids or []) if str(item or "").strip()]
    return {
        "feature_ids": list(columns),
        "feature_set_tag": str(feature_set_tag_from_ids(list(columns))),
        "feature_count": int(len(columns)),
    }


def _assert_loaded_feature_schema_current(loaded: Any) -> None:
    current = _expected_columns(
        loaded.feature_ids,
        model_name=loaded.model_name,
        model_spec={"feature_ids": loaded.feature_ids},
    )
    if current != loaded.feature_ids:
        raise ValueError(
            f"feature_schema_drift: model trained with {loaded.feature_ids} but registry expects {current}"
        )


def _current_model_artifact_alias(family: str, model_name: str, symbol: str = "*") -> str:
    return f"model:{str(family)}:{str(model_name)}:{str(symbol or '*').upper()}:current"


def _load_previous_feature_schema(family: str, model_name: str) -> dict[str, Any]:
    alias = _current_model_artifact_alias(str(family), str(model_name), "*")
    try:
        ref = LocalArtifactStore(ensure_schema=False).resolve(alias)
    except Exception:
        LOG.debug("Ignored recoverable exception.", exc_info=True)
        return {}
    if ref is None:
        return {}
    metadata = dict(getattr(ref, "metadata", {}) or {})
    schema = metadata.get("feature_schema")
    if isinstance(schema, Mapping):
        return dict(schema)
    return {}


def _bumped_training_version_id(
    *,
    family: str,
    model_name: str,
    cfg: Mapping[str, Any],
    previous_schema: Mapping[str, Any],
) -> str:
    explicit = str(cfg.get("training_version_id") or cfg.get("model_version") or "").strip()
    previous = str(previous_schema.get("training_version_id") or previous_schema.get("model_version") or "").strip()
    if explicit and explicit != previous:
        return explicit
    return version_from_ts(str(model_name), int(time.time() * 1000), prefix=str(family))


def _resolve_retrain_schema_guard(
    *,
    family: str,
    model_name: str,
    feature_ids: Sequence[Any],
    cfg: Mapping[str, Any],
    schema_builder: Any = _feature_schema,
) -> dict[str, Any]:
    current_schema = dict(schema_builder(feature_ids))
    current_tag = str(current_schema.get("feature_set_tag") or "").strip()
    previous_schema = _load_previous_feature_schema(str(family), str(model_name))
    previous_tag = str(previous_schema.get("feature_set_tag") or "").strip()
    if not previous_tag or previous_tag == current_tag:
        return {
            "feature_schema": dict(current_schema),
            "feature_set_tag": str(current_tag),
            "training_version_id": str(cfg.get("training_version_id") or cfg.get("model_version") or "").strip(),
            "feature_schema_changed": False,
        }

    training_version_id = _bumped_training_version_id(
        family=str(family),
        model_name=str(model_name),
        cfg=cfg,
        previous_schema=previous_schema,
    )
    LOG.info(
        "feature_schema_changed model_name=%s family=%s previous_feature_set_tag=%s new_feature_set_tag=%s training_version_id=%s",
        str(model_name),
        str(family),
        str(previous_tag),
        str(current_tag),
        str(training_version_id),
    )
    if str(os.environ.get("TS_ALLOW_SCHEMA_CHANGE", "") or "").strip() != "1":
        raise RuntimeError(
            "feature_schema_change_requires_ack:"
            f"model_name={str(model_name)}:"
            f"previous={str(previous_tag)}:"
            f"current={str(current_tag)}"
        )
    return {
        "feature_schema": dict(current_schema),
        "feature_set_tag": str(current_tag),
        "previous_feature_set_tag": str(previous_tag),
        "training_version_id": str(training_version_id),
        "model_version": str(training_version_id),
        "feature_schema_changed": True,
    }


def _coerce_feature_map(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        nested = row.get("features")
        if isinstance(nested, Mapping):
            return dict(nested)
        return dict(row)
    return {}


def _matrix_from_features(features: Any, columns: Sequence[str]) -> np.ndarray:
    cols = [str(col) for col in list(columns or [])]
    if not cols:
        raise ValueError("feature_columns_required")

    if isinstance(features, np.ndarray):
        arr = np.asarray(features, dtype=np.float32)
        if arr.ndim == 1:
            arr = arr.reshape(1, -1)
        if arr.ndim != 2:
            raise ValueError("feature_matrix_invalid_shape")
        if int(arr.shape[1]) != int(len(cols)):
            raise ValueError(f"feature_count_mismatch:{int(arr.shape[1])}:{int(len(cols))}")
        return np.nan_to_num(arr.astype(np.float32, copy=False), nan=0.0, posinf=0.0, neginf=0.0)

    if hasattr(features, "loc") and hasattr(features, "columns"):
        arr = features.loc[:, cols].to_numpy(dtype=np.float32)
        return np.nan_to_num(arr.astype(np.float32, copy=False), nan=0.0, posinf=0.0, neginf=0.0)

    if isinstance(features, Mapping):
        feature_map = _coerce_feature_map(features)
        values = [feature_map.get(col, 0.0) for col in cols]
        if any(isinstance(value, (list, tuple, np.ndarray)) for value in values):
            columns_values = [np.asarray(value, dtype=np.float32).reshape(-1) for value in values]
            row_count = max(int(value.shape[0]) for value in columns_values)
            matrix = np.zeros((row_count, len(cols)), dtype=np.float32)
            for idx, value in enumerate(columns_values):
                if int(value.shape[0]) == 1 and row_count > 1:
                    matrix[:, idx] = float(value[0])
                elif int(value.shape[0]) == row_count:
                    matrix[:, idx] = value
                else:
                    raise ValueError("feature_column_length_mismatch")
            return np.nan_to_num(matrix, nan=0.0, posinf=0.0, neginf=0.0)
        return np.asarray([[_safe_float(feature_map.get(col), 0.0) for col in cols]], dtype=np.float32)

    if isinstance(features, Sequence) and not isinstance(features, (str, bytes, bytearray)):
        rows = list(features)
        if rows and all(isinstance(row, Mapping) for row in rows):
            matrix = [
                [_safe_float(_coerce_feature_map(row).get(col), 0.0) for col in cols]
                for row in rows
            ]
            return np.asarray(matrix, dtype=np.float32)
        arr = np.asarray(rows, dtype=np.float32)
        if arr.ndim == 1:
            arr = arr.reshape(1, -1)
        if int(arr.shape[1]) != int(len(cols)):
            raise ValueError(f"feature_count_mismatch:{int(arr.shape[1])}:{int(len(cols))}")
        return np.nan_to_num(arr.astype(np.float32, copy=False), nan=0.0, posinf=0.0, neginf=0.0)

    raise TypeError(f"unsupported_feature_payload:{type(features).__name__}")


def _dump_joblib_to_bytes(value: Any) -> bytes:
    return dumps_pickle_artifact(value, prefer_joblib=True)


def _load_joblib_from_bytes(payload: bytes) -> Any:
    return loads_pickle_artifact(payload, prefer_joblib=True)


def _artifact_payload_from_alias(alias: str, sha256: str = "") -> bytes:
    store = LocalArtifactStore()
    ref = store.resolve(alias) if str(alias or "").strip() else None
    if ref is None and str(sha256 or "").strip():
        from datetime import datetime, timezone

        from engine.artifacts.refs import ArtifactRef

        ref = ArtifactRef(
            sha256=str(sha256).strip(),
            size=0,
            content_type="application/vnd.joblib",
            kind="model",
            created_ts=datetime.now(timezone.utc),
            metadata={},
        )
    return store.get_bytes(ref) if ref is not None else b""


def _fit_eval_metrics(model: Any, X: np.ndarray, y: np.ndarray) -> dict[str, float]:
    if int(y.shape[0]) <= 1:
        return {"rmse": 0.0, "directional_acc": 0.0}
    preds = np.asarray(model.predict(X), dtype=np.float32).reshape(-1)
    err = np.asarray(y, dtype=np.float32).reshape(-1) - preds
    rmse = float(np.sqrt(np.mean(err * err)))
    directional = float(np.mean(np.sign(y) == np.sign(preds)))
    return {"rmse": float(rmse), "directional_acc": float(directional)}


class LGBMRegressorModel:
    """LightGBM regressor with schema-bound feature vectorization."""

    family = FAMILY
    model_kind = DEFAULT_MODEL_KIND

    def __init__(
        self,
        *,
        model_name: str = DEFAULT_MODEL_NAME,
        feature_ids: Sequence[Any] | None = None,
        hyperparams: Mapping[str, Any] | None = None,
        model: Any = None,
        training_metrics: Mapping[str, Any] | None = None,
    ) -> None:
        self.model_name = str(model_name or DEFAULT_MODEL_NAME).strip() or DEFAULT_MODEL_NAME
        self.feature_ids = _expected_columns(feature_ids, model_name=self.model_name)
        self.hyperparams = self._default_hyperparams()
        self.hyperparams.update(dict(hyperparams or {}))
        self.model = model
        self.training_metrics = dict(training_metrics or {})

    @staticmethod
    def _default_hyperparams() -> dict[str, Any]:
        return {
            "objective": "regression",
            "num_leaves": 31,
            "learning_rate": 0.05,
            "n_estimators": 100,
            "min_child_samples": 2,
            "random_state": 42,
            "n_jobs": 1,
            "verbosity": -1,
            "deterministic": True,
            "force_col_wise": True,
        }

    @property
    def feature_schema(self) -> dict[str, Any]:
        return _feature_schema(self.feature_ids)

    def _new_estimator(self) -> Any:
        try:
            import lightgbm as lgb
        except ImportError as exc:  # pragma: no cover - dependency is declared
            raise RuntimeError("lightgbm_not_installed") from exc
        return lgb.LGBMRegressor(**dict(self.hyperparams))

    def fit(self, X: Any, y: Any, sample_weight: Any = None) -> "LGBMRegressorModel":
        columns = _expected_columns(self.feature_ids, model_name=self.model_name, model_spec=self.feature_schema)
        X_arr = _matrix_from_features(X, columns)
        y_arr = np.asarray(y, dtype=np.float32).reshape(-1)
        if int(X_arr.shape[0]) != int(y_arr.shape[0]):
            raise ValueError("lgbm_row_count_mismatch")
        self.feature_ids = list(columns)
        model = self._new_estimator()
        fit_kwargs: dict[str, Any] = {"feature_name": list(columns)}
        if sample_weight is not None:
            fit_kwargs["sample_weight"] = np.asarray(sample_weight, dtype=np.float32).reshape(-1)
        model.fit(X_arr, y_arr, **fit_kwargs)
        self.model = model
        self.training_metrics = {
            "n_train": int(y_arr.shape[0]),
            "model_family": str(self.family),
            "model_kind": str(self.model_kind),
            "feature_schema": self.feature_schema,
            **_fit_eval_metrics(model, X_arr, y_arr),
        }
        return self

    def predict(self, X: Any) -> np.ndarray:
        if self.model is None:
            raise RuntimeError("lgbm_model_not_fitted")
        columns = _expected_columns(self.feature_ids, model_name=self.model_name, model_spec=self.feature_schema)
        X_arr = _matrix_from_features(X, columns)
        raw = self.model.predict(X_arr)
        return np.asarray(raw, dtype=np.float32).reshape(-1)

    def predict_one(self, features: Mapping[str, Any]) -> float:
        return float(self.predict(features)[0])

    def save(self, path: str | Path) -> Path:
        target = Path(path)
        if not target.suffix:
            target = target / "model.joblib"
        return dump_pickle_artifact(self, target, prefer_joblib=True)

    @classmethod
    def load(cls, path: str | Path) -> "LGBMRegressorModel":
        loaded = load_pickle_artifact(path, prefer_joblib=True)
        if not isinstance(loaded, cls):
            raise TypeError("invalid_lgbm_regressor_artifact")
        _assert_loaded_feature_schema_current(loaded)
        return loaded

    def to_bytes(self) -> bytes:
        return _dump_joblib_to_bytes(self)

    @classmethod
    def from_bytes(cls, payload: bytes) -> "LGBMRegressorModel":
        loaded = _load_joblib_from_bytes(payload)
        if not isinstance(loaded, cls):
            raise TypeError("invalid_lgbm_regressor_payload")
        _assert_loaded_feature_schema_current(loaded)
        return loaded


def train_lgbm_regressor(
    X: Any,
    y: Any,
    *,
    feature_ids: Sequence[Any] | None = None,
    sample_weight: Any = None,
    hyperparams: Mapping[str, Any] | None = None,
    model_name: str = DEFAULT_MODEL_NAME,
) -> LGBMRegressorModel:
    return LGBMRegressorModel(
        model_name=str(model_name or DEFAULT_MODEL_NAME),
        feature_ids=feature_ids,
        hyperparams=hyperparams,
    ).fit(X, y, sample_weight=sample_weight)


def persist_model_artifact(
    model: LGBMRegressorModel,
    *,
    family: str = FAMILY,
    symbol: str = "*",
    version: str,
) -> dict[str, Any]:
    alias = f"model:{family}:{str(model.model_name)}:{str(symbol or '*').upper()}:current"
    payload = model.to_bytes()
    ref = LocalArtifactStore().put(
        payload,
        content_type="application/vnd.joblib",
        kind="model",
        alias=alias,
        metadata={
            "model_name": str(model.model_name),
            "family": str(family),
            "symbol": str(symbol or "*").upper(),
            "version": str(version),
            "feature_schema": dict(model.feature_schema),
        },
    )
    return {
        "alias": str(alias),
        "sha256": str(ref.sha256),
        "size_bytes": int(ref.size),
        "content_type": str(ref.content_type),
    }


def register_shadow_model(
    model: LGBMRegressorModel,
    *,
    symbol: str = "*",
    version: str | None = None,
    family: str = FAMILY,
    model_kind: str = DEFAULT_MODEL_KIND,
    performance_metrics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    version_s = str(version or version_from_ts(str(model.model_name), int(time.time() * 1000), prefix=str(family)))
    manifest = persist_model_artifact(model, family=str(family), symbol=str(symbol), version=version_s)
    metrics = {
        **dict(model.training_metrics or {}),
        **dict(performance_metrics or {}),
        "model_name": str(model.model_name),
        "model_version": str(version_s),
        "model_family": str(family),
        "model_kind": str(model_kind),
        "feature_ids": list(model.feature_ids),
        "feature_set_tag": str(model.feature_schema.get("feature_set_tag") or ""),
        "feature_schema": dict(model.feature_schema),
        "artifact_alias": str(manifest.get("alias") or ""),
        "artifact_sha256": str(manifest.get("sha256") or ""),
    }
    model_ts_ms = int(time.time() * 1000)
    register_model(
        model_name=str(model.model_name),
        model_kind=str(model_kind),
        model_ts_ms=int(model_ts_ms),
        stage="shadow",
        metrics=dict(metrics),
        regime="global",
    )
    register_model_version(
        model_name=str(model.model_name),
        model_version=str(version_s),
        model_kind=str(model_kind),
        stage="shadow",
        status="trained",
        live_ready=False,
        training_job_name=f"train_{family}_models",
        train_scope={
            "symbol": str(symbol or "*").upper(),
            "feature_ids": list(model.feature_ids),
            "feature_schema": dict(model.feature_schema),
        },
        meta=dict(metrics),
    )
    catalog_symbol = str(symbol or "*").upper()
    if catalog_symbol != "*":
        register_model(
            symbol=catalog_symbol,
            model_name=str(model.model_name),
            model_kind=str(model_kind),
            version=str(version_s),
            status="shadow",
            is_active=False,
            metadata={"artifact_manifest": dict(manifest), **dict(metrics)},
            performance_metrics=dict(metrics),
            artifact_uri=str(manifest.get("alias") or ""),
        )
    return {"version": version_s, "stage": "shadow", "artifact_manifest": manifest, "metrics": metrics}


def load_model_from_artifact(alias: str = "", sha256: str = "", path: str | Path | None = None) -> LGBMRegressorModel:
    if path is not None and str(path).strip():
        return LGBMRegressorModel.load(Path(path))
    payload = _artifact_payload_from_alias(str(alias or ""), str(sha256 or ""))
    if not payload:
        raise FileNotFoundError("lgbm_artifact_not_found")
    return LGBMRegressorModel.from_bytes(payload)


def _resolve_training_config(family: str, plan: Mapping[str, Any] | None = None) -> dict[str, Any]:
    from engine.strategy.model_config import get_model_config, load_model_configs

    plan_dict = dict(plan or {})
    model_name = str(plan_dict.get("model_name") or "").strip()
    cfg = get_model_config(model_name, family=family) if model_name else {}
    if not cfg:
        configs = load_model_configs(family=family, include_disabled=True)
        cfg = dict(configs[0]) if configs else {"family": family, "model_name": family}
    model_name = str(model_name or cfg.get("model_name") or family).strip() or family
    feature_ids = _expected_columns(cfg.get("feature_ids"), model_name=model_name, model_spec=cfg)
    schema_guard = _resolve_retrain_schema_guard(
        family=str(family),
        model_name=str(model_name),
        feature_ids=list(feature_ids),
        cfg=cfg,
    )
    horizons = [int(h) for h in list(cfg.get("horizons_s") or cfg.get("horizons") or [DEFAULT_HORIZON_S]) if int(h) > 0]
    return {
        **cfg,
        **schema_guard,
        "family": str(family),
        "model_name": str(model_name),
        "feature_ids": list(feature_ids),
        "horizon_s": int(cfg.get("horizon_s") or (horizons[0] if horizons else DEFAULT_HORIZON_S)),
        "horizons_s": list(horizons or [DEFAULT_HORIZON_S]),
        "symbol_universe": list(cfg.get("symbol_universe") or cfg.get("symbols") or ["*"]),
        "training_window_days": int(cfg.get("training_window_days") or cfg.get("lookback_days") or DEFAULT_LOOKBACK_DAYS),
        "hyperparams": dict(cfg.get("hyperparams") or {}),
    }


def _load_training_rows(
    *,
    cutoff_ms: int,
    horizon_s: int,
    symbols: Sequence[str],
    feature_ids: Sequence[str],
    include_metadata: bool = False,
) -> tuple[list[dict[str, float]], list[float]] | tuple[list[dict[str, float]], list[float], list[dict[str, int | str]]]:
    symbol_filter = {str(s).upper().strip() for s in list(symbols or []) if str(s or "").strip() and str(s).strip() != "*"}
    con = connect()
    try:
        rows = con.execute(
            """
            SELECT l.symbol, l.horizon_s, COALESCE(le.net_z, l.impact_z) AS impact_z,
                   e.ts_ms, e.title, e.body, e.source
            FROM labels l
            JOIN events e ON e.id = l.event_id
            LEFT JOIN labels_exec le
              ON le.event_id = l.event_id
             AND le.symbol = l.symbol
             AND le.horizon_s = l.horizon_s
             AND le.realized = 1
            WHERE e.ts_ms >= ?
              AND COALESCE(le.net_z, l.impact_z) IS NOT NULL
            ORDER BY e.ts_ms ASC, l.event_id ASC, l.symbol ASC
            """,
            (int(cutoff_ms),),
        ).fetchall()
    finally:
        con.close()
    X_rows: list[dict[str, float]] = []
    y_rows: list[float] = []
    meta_rows: list[dict[str, int | str]] = []
    for symbol, row_horizon_s, impact_z, ts_ms, title, body, source in rows or []:
        sym = str(symbol or "").upper().strip()
        if not sym or (symbol_filter and sym not in symbol_filter):
            continue
        if int(row_horizon_s or 0) != int(horizon_s):
            continue
        event = {
            "ts_ms": int(ts_ms or 0),
            "title": str(title or ""),
            "body": str(body or ""),
            "source": str(source or ""),
        }
        snapshot = build_feature_snapshot(event=event, symbol=sym, feature_ids=list(feature_ids))
        X_rows.append({feature_id: _safe_float(dict(snapshot).get(feature_id), 0.0) for feature_id in feature_ids})
        y_rows.append(_safe_float(impact_z, 0.0))
        meta_rows.append({"symbol": str(sym), "ts": int(ts_ms or 0), "horizon": int(horizon_s)})
    if include_metadata:
        return X_rows, y_rows, meta_rows
    return X_rows, y_rows


def run_tabular_training_job(
    *,
    family: str,
    model_cls: type[LGBMRegressorModel],
    model_kind: str,
    version_prefix: str,
) -> int:
    init_db()
    plan = load_lifecycle_plan(str(family))
    cfg = _resolve_training_config(str(family), plan)
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - int(cfg.get("training_window_days") or DEFAULT_LOOKBACK_DAYS) * 86_400_000
    feature_ids = list(cfg.get("feature_ids") or [])
    loaded_rows = _load_training_rows(
        cutoff_ms=int(cutoff_ms),
        horizon_s=int(cfg.get("horizon_s") or DEFAULT_HORIZON_S),
        symbols=list(cfg.get("symbol_universe") or ["*"]),
        feature_ids=list(feature_ids),
        include_metadata=True,
    )
    X_rows, y_rows, meta_rows = loaded_rows
    min_samples = int(os.environ.get(f"{family.upper()}_MIN_SAMPLES", str(DEFAULT_MIN_SAMPLES)))
    if len(y_rows) < max(2, min_samples):
        print(f"{family}: insufficient_samples n={len(y_rows)} min_required={max(2, min_samples)}")
        return 0
    split = min(max(1, int(len(y_rows) * 0.8)), int(len(y_rows) - 1))
    X_train = X_rows[:split]
    y_train = y_rows[:split]
    X_eval = X_rows[split:]
    y_eval = y_rows[split:]
    meta_eval = meta_rows[split:]

    model = model_cls(
        model_name=str(cfg.get("model_name") or family),
        feature_ids=list(feature_ids),
        hyperparams=dict(cfg.get("hyperparams") or {}),
    )
    model.fit(X_train, y_train)
    try:
        eval_pred = model.predict(X_eval)
        oos_run_id = str(uuid.uuid4())
        upsert_oos_predictions(
            [
                {
                    "symbol": str(meta.get("symbol") or "*"),
                    "horizon": int(meta.get("horizon") or cfg.get("horizon_s") or DEFAULT_HORIZON_S),
                    "family": str(family),
                    "ts": int(meta.get("ts") or 0),
                    "run_id": str(oos_run_id),
                    "prediction": float(eval_pred[idx]),
                    "target": float(y_eval[idx]),
                }
                for idx, meta in enumerate(meta_eval)
            ]
        )
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    version = str(
        plan.get("model_version")
        or cfg.get("training_version_id")
        or version_from_ts(str(model.model_name), now_ms, prefix=str(version_prefix))
    )
    result = register_shadow_model(
        model,
        symbol="*",
        version=str(version),
        family=str(family),
        model_kind=str(model_kind),
    )
    metrics = dict(result.get("metrics") or {})
    record_version_performance(
        model_name=str(model.model_name),
        model_version=str(version),
        metric_scope="training",
        metrics={
            "avg_rmse": float(metrics.get("rmse") or 0.0),
            "avg_directional_acc": float(metrics.get("directional_acc") or 0.0),
            "quality_score": float(max(0.0, min(1.0, _safe_float(metrics.get("directional_acc"), 0.0)))),
            "trained_models": 1,
        },
        sample_n=int(len(y_rows)),
        meta={"job_name": f"train_{family}_models"},
    )
    update_model_version_status(
        str(model.model_name),
        str(version),
        stage="shadow",
        status="trained",
        live_ready=False,
        meta_patch={"training_completed_ts_ms": int(time.time() * 1000)},
    )
    print(json.dumps({"ok": True, "family": str(family), "version": str(version), "stage": "shadow"}))
    return 0


def main() -> int:
    return run_tabular_training_job(
        family=FAMILY,
        model_cls=LGBMRegressorModel,
        model_kind=DEFAULT_MODEL_KIND,
        version_prefix="lgbm",
    )


__all__ = [
    "FAMILY",
    "LGBMRegressorModel",
    "load_model_from_artifact",
    "main",
    "persist_model_artifact",
    "register_shadow_model",
    "run_tabular_training_job",
    "train_lgbm_regressor",
]


if __name__ == "__main__":
    raise SystemExit(main())
