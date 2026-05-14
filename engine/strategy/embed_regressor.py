"""
FILE: embed_regressor.py

Trains and serves supervised embedding-to-impact regressors. This module is one
of the core model implementations in the strategy stack and supports both a
legacy ridge path and a torch MLP path under the same storage schema.
"""

import io
import logging
import os
import time
import uuid
from typing import Any, Dict, Tuple, Optional, List

import math
import json
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.isotonic import IsotonicRegression

import torch
import torch.nn as nn
from torch.optim.adamw import AdamW

from engine.artifacts.serialization import dumps_torch_payload
from engine.artifacts.store import LocalArtifactStore
from engine.runtime import dbapi_compat as dbapi
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db
from engine.strategy.feature_expansion import build_feature_vector, feature_set_tag
from engine.strategy.feature_registry import resolve_feature_ids
from engine.strategy.model_config import DEFAULT_FAMILY
from engine.strategy.ensemble.oos_store import upsert_oos_predictions
from engine.strategy.tuning.catalog import default_for
from engine.strategy.tuning.study import fetch_best_params
from engine.data.asset_map import asset_class_for_symbol

# Magic prefix differentiates torch payloads from legacy ridge blobs.
_MLP_MAGIC = b"MLP1"
LOG = get_logger("engine.strategy.embed_regressor")
_WARNED_NONFATAL_KEYS: set[str] = set()

# --- Determinism defaults ---
_TORCH_SEED = 42


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: str | None = None, **extra: Any) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.embed_regressor",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


def init_embed_models_db() -> None:
    init_db()


def _ensure_embed_artifact_columns(con) -> None:
    cols = {
        str(row[1] or "").strip().lower()
        for row in (con.execute("PRAGMA table_info(embed_models2)").fetchall() or [])
        if row and len(row) >= 2
    }
    if "artifact_sha256" not in cols:
        con.execute("ALTER TABLE embed_models2 ADD COLUMN artifact_sha256 TEXT")
    if "artifact_alias" not in cols:
        con.execute("ALTER TABLE embed_models2 ADD COLUMN artifact_alias TEXT")


def _normalize_model_namespace(model_name: Optional[str]) -> str:
    return str(model_name or "").strip()


def _uses_legacy_namespace(model_name: Optional[str]) -> bool:
    name = _normalize_model_namespace(model_name)
    return (not name) or name == DEFAULT_FAMILY


def _namespaced_model_key(model_name: Optional[str], key: str) -> str:
    base_key = str(key or "").strip()
    namespace = _normalize_model_namespace(model_name)
    if not base_key:
        return ""
    if _uses_legacy_namespace(namespace):
        return base_key
    return f"{namespace}|{base_key}"


def _ensure_feature_schema_table(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS embed_model_feature_schema (
          ts_ms INTEGER NOT NULL,
          feature_set_tag TEXT NOT NULL,
          feature_ids_json TEXT NOT NULL,
          model_kind TEXT,
          created_ts_ms INTEGER NOT NULL,
          PRIMARY KEY (ts_ms, feature_set_tag)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_embed_model_feature_schema_created
        ON embed_model_feature_schema(created_ts_ms)
        """
    )


def persist_feature_schema(con, *, ts_ms: int, feature_ids: List[str], feature_set: str, model_kind: str = "") -> None:
    _ensure_feature_schema_table(con)
    con.execute(
        """
        INSERT OR REPLACE INTO embed_model_feature_schema(
          ts_ms, feature_set_tag, feature_ids_json, model_kind, created_ts_ms
        )
        VALUES (?,?,?,?,?)
        """,
        (
            int(ts_ms),
            str(feature_set),
            json.dumps(list(feature_ids or []), separators=(",", ":"), sort_keys=False),
            str(model_kind or ""),
            int(time.time() * 1000),
        ),
    )


def load_feature_schema(*, ts_ms: int) -> Optional[Dict[str, Any]]:
    init_embed_models_db()
    con = connect()
    try:
        _ensure_feature_schema_table(con)
        row = con.execute(
            """
            SELECT feature_set_tag, feature_ids_json, model_kind
            FROM embed_model_feature_schema
            WHERE ts_ms=?
            ORDER BY created_ts_ms DESC
            LIMIT 1
            """,
            (int(ts_ms),),
        ).fetchone()
        if not row:
            return None
        feature_set, feature_ids_json, model_kind = row
        try:
            feature_ids = json.loads(feature_ids_json or "[]")
        except Exception:
            feature_ids = []
        if not isinstance(feature_ids, list):
            feature_ids = []
        return {
            "feature_set_tag": str(feature_set or ""),
            "feature_ids": list(feature_ids),
            "model_kind": str(model_kind or ""),
            "ts_ms": int(ts_ms),
        }
    finally:
        con.close()


# =========================
# Ridge (legacy) serialization
# =========================

def _serialize_ridge(model: Ridge) -> bytes:
    coef = np.asarray(model.coef_, dtype=np.float32).reshape(-1)
    intercept = np.asarray([float(model.intercept_)], dtype=np.float32)
    buf = io.BytesIO()
    buf.write(np.int32(coef.shape[0]).tobytes())
    buf.write(coef.tobytes())
    buf.write(intercept.tobytes())
    return buf.getvalue()


def _deserialize_ridge(blob: bytes) -> Tuple[np.ndarray, float]:
    b = memoryview(blob)
    dim = int(np.frombuffer(b[:4], dtype=np.int32)[0])
    off = 4
    coef = np.frombuffer(b[off:off + (dim * 4)], dtype=np.float32).copy()
    off += dim * 4
    intercept = float(np.frombuffer(b[off:off + 4], dtype=np.float32)[0])
    return coef, intercept


# =========================
# MLP model + serialization
# =========================

class _MLPRegressor(nn.Module):
    def __init__(self, input_dim: int, hidden: List[int], dropout: float = 0.0):
        super().__init__()
        dims = [int(input_dim)] + [int(h) for h in (hidden or [])] + [1]
        layers: List[nn.Module] = []
        for i in range(len(dims) - 2):
            layers.append(nn.Linear(dims[i], dims[i + 1]))
            layers.append(nn.ReLU())
            if float(dropout) > 0.0:
                layers.append(nn.Dropout(float(dropout)))
        layers.append(nn.Linear(dims[-2], dims[-1]))
        self.net = nn.Sequential(*layers)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x).squeeze(-1)


class _TemporalMLP(nn.Module):
    def __init__(self, input_dim: int, hidden: List[int]):
        super().__init__()
        dims = [int(input_dim)] + [int(h) for h in (hidden or [])] + [int(input_dim)]
        layers: List[nn.Module] = []
        for i in range(len(dims) - 2):
            layers.append(nn.Linear(dims[i], dims[i + 1]))
            layers.append(nn.ReLU())
        layers.append(nn.Linear(dims[-2], dims[-1]))
        self.net = nn.Sequential(*layers)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x)


def _serialize_mlp_payload(payload: Dict) -> bytes:
    return _MLP_MAGIC + dumps_torch_payload(payload)


def _deserialize_mlp_payload(blob: bytes) -> Dict:
    if not blob.startswith(_MLP_MAGIC):
        raise ValueError("not an MLP blob")
    raw = blob[len(_MLP_MAGIC):]
    buf = io.BytesIO(raw)
    payload = torch.load(buf, map_location="cpu")
    if not isinstance(payload, dict):
        raise ValueError("invalid MLP payload")
    return payload


def _upsert_model(con, key_type: str, key: str, horizon_s: int, now_ms: int, n: int, dim: int, blob_out: bytes) -> None:
    _ensure_embed_artifact_columns(con)
    artifact_alias = f"model:embed_regressor:{str(key_type)}:{str(key)}:{int(horizon_s)}:current"
    ref = LocalArtifactStore().put(
        bytes(blob_out or b""),
        content_type="application/octet-stream",
        kind="model",
        alias=artifact_alias,
        metadata={
            "model_name": "embed_regressor",
            "key_type": str(key_type),
            "key": str(key),
            "horizon_s": int(horizon_s),
            "ts_ms": int(now_ms),
            "n": int(n),
            "dim": int(dim),
        },
    )
    con.execute(
        """
        INSERT INTO embed_models2(key_type, key, horizon_s, ts_ms, n, dim, model_blob, artifact_sha256, artifact_alias)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(key_type, key, horizon_s) DO UPDATE SET
          ts_ms=excluded.ts_ms,
          n=excluded.n,
          dim=excluded.dim,
          model_blob=excluded.model_blob,
          artifact_sha256=excluded.artifact_sha256,
          artifact_alias=excluded.artifact_alias
        """,
        (
            str(key_type),
            str(key),
            int(horizon_s),
            int(now_ms),
            int(n),
            int(dim),
            dbapi.Binary(b""),
            str(ref.sha256),
            str(artifact_alias),
        ),
    )


def _load_artifact_blob(alias: str, sha256: str) -> bytes:
    store = LocalArtifactStore()
    ref = store.resolve(alias) if str(alias or "").strip() else None
    if ref is None and str(sha256 or "").strip():
        from datetime import datetime, timezone

        from engine.artifacts.refs import ArtifactRef

        ref = ArtifactRef(
            sha256=str(sha256).strip(),
            size=0,
            content_type="application/octet-stream",
            kind="model",
            created_ts=datetime.now(timezone.utc),
            metadata={},
        )
    if ref is None:
        return b""
    return store.get_bytes(ref)


def _eval_predictions(y_true: np.ndarray, y_pred: np.ndarray) -> Tuple[float, float, float]:
    """
    Returns: (rmse, spearman_ic, directional_accuracy)
    """
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)

    if y_true.size == 0:
        return 0.0, 0.0, 0.0

    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))

    # Spearman via rank-corr
    try:
        rt = y_true.argsort().argsort()
        rp = y_pred.argsort().argsort()
        spearman = float(np.corrcoef(rt, rp)[0, 1])
        if not np.isfinite(spearman):
            spearman = 0.0
    except Exception:
        spearman = 0.0

    try:
        directional = float(np.mean(np.sign(y_true) == np.sign(y_pred)))
    except Exception:
        directional = 0.0

    return rmse, spearman, directional


def _compute_train_eval_split(n: int) -> Tuple[int, int, int]:
    total = max(0, int(n))
    if total <= 1:
        return total, total, 0
    try:
        split_ratio = float(default_for("embed_regressor", "train_split", 0.8))
    except Exception:
        split_ratio = 0.8
    if not math.isfinite(split_ratio):
        split_ratio = 0.8
    split_ratio = max(0.5, min(0.95, float(split_ratio)))
    eval_start = max(1, min(total - 1, int(total * split_ratio)))
    gap_n = max(1, int(total * 0.02))
    gap_n = min(gap_n, max(0, eval_start - 1))
    train_end = int(eval_start - gap_n)
    if train_end <= 0:
        train_end = max(1, eval_start)
        gap_n = max(0, eval_start - train_end)
    return int(train_end), int(eval_start), int(gap_n)


def _conf_from_n(n_train: int, conf_k: float) -> float:
    # conf_raw = 1 - exp(-n/k)
    try:
        n = max(0, int(n_train))
        k = float(conf_k)
        if k <= 1e-9:
            return 0.0
        c = 1.0 - math.exp(-float(n) / float(k))
        return float(max(0.0, min(1.0, c)))
    except Exception as e:
        _warn_nonfatal(
            "embed_regressor_confidence_from_n_failed",
            "EMBED_REGRESSOR_CONFIDENCE_FROM_N_FAILED",
            e,
            warn_key="confidence_from_n",
            n=repr(n)[:120],
        )
        return 0.0


def _fit_isotonic_curve(xs, ys) -> Optional[Tuple[List[float], List[float]]]:
    """
    Fit isotonic regression y=f(x) with clipping, return sorted unique curve points.
    xs, ys are lists of floats.
    """
    xs = [float(x) for x in (xs or []) if x is not None and np.isfinite(float(x))]
    ys = [float(y) for y in (ys or []) if y is not None and np.isfinite(float(y))]
    if len(xs) < 5 or len(xs) != len(ys):
        return None

    # sort by x
    pairs = sorted(zip(xs, ys), key=lambda t: t[0])
    x = np.asarray([p[0] for p in pairs], dtype=float)
    y = np.asarray([p[1] for p in pairs], dtype=float)

    try:
        iso = IsotonicRegression(out_of_bounds="clip")
        yhat = iso.fit_transform(x, y)
    except Exception as e:
        _warn_nonfatal(
            "embed_regressor_isotonic_fit_failed",
            "EMBED_REGRESSOR_ISOTONIC_FIT_FAILED",
            e,
            warn_key="isotonic_fit",
            n_points=int(len(xs or [])),
        )
        return None

    # compress to unique x points (keeps last y per x)
    out_x: List[float] = []
    out_y: List[float] = []
    last_x: Optional[float] = None
    for xi, yi in zip(x.tolist(), yhat.tolist()):
        if last_x is None or float(xi) != float(last_x):
            out_x.append(float(xi))
            out_y.append(float(yi))
            last_x = float(xi)
        else:
            out_y[-1] = float(yi)

    if len(out_x) < 2:
        return None

    return out_x, out_y

def train_embed_models(
    symbols: List[str],
    horizons: List[int],
    min_samples: int = 50,
    alpha: float = 1.0,
    lookback_days: int = 365,
    train_by_class: bool = True,
    feature_ids: Optional[List[str]] = None,
    kind: str = "ridge",  # 'ridge' | 'mlp' | 'auto'
    mlp_hidden: Optional[List[int]] = None,
    mlp_dropout: float = 0.0,
    mlp_lr: float = 3e-3,
    mlp_epochs: int = 120,
    mlp_weight_decay: float = 1e-4,
    model_name: Optional[str] = None,
) -> Dict[Tuple[str, str, int], int]:
    """
    Trains models for:
      (key_type='symbol', key=symbol, horizon_s)
    and optionally:
      (key_type='class', key=asset_class, horizon_s)

    Returns dict[(key_type,key,horizon_s)] = n_trained
    """
    init_embed_models_db()

    kind = str(kind or "ridge").strip().lower()
    if kind not in ("ridge", "mlp", "auto"):
        kind = "ridge"

    tuned_params = fetch_best_params("embed_regressor", "GLOBAL")
    conf_k = float(tuned_params.get("conf_k", default_for("embed_regressor", "conf_k", 75.0)))

    if mlp_hidden is None:
        mlp_hidden = [128, 64]

    now_ms = int(time.time() * 1000)
    oos_run_id = str(uuid.uuid4())

    cutoff_ms = now_ms - int(lookback_days) * 24 * 3600 * 1000

    feature_ids = resolve_feature_ids(
        feature_ids,
        model_name=(model_name or DEFAULT_FAMILY),
    )
    tag = feature_set_tag(feature_ids)
    def _tag_key(k: str) -> str:
        # Keep backward-compatible keys for existing deployments.
        # Only namespace when tag != "base".
        tagged = str(k) if tag == "base" else f"{str(k)}#{tag}"
        return _namespaced_model_key(model_name, tagged)
    symset = set(str(s).upper() for s in (symbols or []))
    hset = set(int(h) for h in (horizons or []))

    con = connect()
    try:
        # A2: calibration samples accumulator: (horizon_s, model_kind) -> {"x":[], "y":[]}
        _calib: Dict[Tuple[int, str], Dict[str, List[float]]] = {}

        rows = con.execute(
            """
            SELECT
              l.event_id,
              l.symbol,
              l.horizon_s,
              COALESCE(le.net_z, l.impact_z) AS impact_z,
              emb.vec,
              e.ts_ms
            FROM labels l
            JOIN events e ON e.id = l.event_id
            JOIN event_embeddings emb ON emb.event_id = l.event_id
            LEFT JOIN labels_exec le
              ON le.event_id = l.event_id
             AND le.symbol   = l.symbol
             AND le.horizon_s = l.horizon_s
             AND le.realized = 1
            WHERE e.ts_ms >= ?
              AND COALESCE(le.net_z, l.impact_z) IS NOT NULL
            """,
            (int(cutoff_ms),),
        ).fetchall()

        # buckets for symbol: (sym,h) -> list[(impact_z, emb_blob, event_ts_ms, sym_u)]
        sym_buckets: Dict[Tuple[str, int], List[Tuple[float, bytes, int, str]]] = {}

        # buckets for class: (cls,h) -> list[(impact_z, emb_blob, event_ts_ms, sym_u)]
        cls_buckets: Dict[Tuple[str, int], List[Tuple[float, bytes, int, str]]] = {}

        for _eid, sym, h, z, blob, ts_ms in rows or []:
            sym_u = str(sym).upper()
            h_i = int(h)
            if symset and sym_u not in symset:
                continue
            if hset and h_i not in hset:
                continue
            if blob is None:
                continue
            try:
                zz = float(z)
            except Exception as e:
                _warn_nonfatal(
                    "embed_regressor_training_row_parse_failed",
                    "EMBED_REGRESSOR_TRAINING_ROW_PARSE_FAILED",
                    e,
                    warn_key="training_row_parse",
                    symbol=str(sym_u),
                    horizon_s=int(h_i),
                )
                continue

            ts_i = int(ts_ms or 0)

            sym_buckets.setdefault((sym_u, h_i), []).append((zz, blob, ts_i, sym_u))

            if train_by_class:
                cls = asset_class_for_symbol(sym_u)
                cls_buckets.setdefault((str(cls).upper(), h_i), []).append((zz, blob, ts_i, sym_u))

        out: Dict[Tuple[str, str, int], int] = {}

        def _build_xy(items: List[Tuple[float, bytes, int, str]]) -> Optional[Tuple[np.ndarray, np.ndarray]]:
            if len(items) < int(min_samples):
                return None

            ordered_items = sorted(items, key=lambda row: (int(row[2]), str(row[3])))
            vecs: List[np.ndarray] = []
            ys: List[float] = []
            for zz, b, ts_i, sym_u in ordered_items:
                v0 = np.frombuffer(b, dtype=np.float32)
                feats = build_feature_vector(
                    event={"ts_ms": int(ts_i), "title": "", "body": "", "source": ""},
                    symbol=str(sym_u),
                    feature_ids=feature_ids,
                )
                v = np.concatenate([v0, np.asarray(feats, dtype=np.float32)])
                vecs.append(v)
                ys.append(float(zz))

            X = np.stack(vecs).astype(np.float32, copy=False)
            y = np.asarray(ys, dtype=np.float32)
            return X, y

        def _train_mlp(X: np.ndarray, y: np.ndarray) -> bytes:
            # deterministic
            np.random.seed(_TORCH_SEED)
            torch.manual_seed(_TORCH_SEED)

            Xf = X.astype(np.float32, copy=False)
            yf = y.astype(np.float32, copy=False)

            x_mean = Xf.mean(axis=0).astype(np.float32)
            x_std = Xf.std(axis=0).astype(np.float32)
            x_std = np.where(x_std < 1e-6, 1.0, x_std).astype(np.float32)

            y_mean = np.float32(yf.mean())
            y_std_val = float(yf.std())
            y_std = np.float32(y_std_val if y_std_val > 1e-6 else 1.0)

            Xn = (Xf - x_mean) / x_std
            yn = (yf - y_mean) / y_std

            xt = torch.from_numpy(Xn)
            yt = torch.from_numpy(yn)

            model = _MLPRegressor(input_dim=int(Xn.shape[1]), hidden=list(mlp_hidden or []), dropout=float(mlp_dropout))
            opt = AdamW(model.parameters(), lr=float(mlp_lr), weight_decay=float(mlp_weight_decay))
            loss_fn = nn.MSELoss()

            model.train()
            for _epoch in range(int(mlp_epochs)):
                opt.zero_grad(set_to_none=True)
                pred = model(xt)
                loss = loss_fn(pred, yt)
                loss.backward()
                opt.step()

            payload = {
                "kind": "mlp1",
                "input_dim": int(Xn.shape[1]),
                "hidden": [int(h) for h in (mlp_hidden or [])],
                "dropout": float(mlp_dropout),
                "state_dict": model.state_dict(),
                "x_mean": x_mean,
                "x_std": x_std,
                "y_mean": y_mean,
                "y_std": y_std,
            }
            return _serialize_mlp_payload(payload)

        def _train_one(items: List[Tuple[float, bytes, int, str]]):
            built = _build_xy(items)
            if not built:
                return None
            X, y = built
            dim = int(X.shape[1])

            n = int(len(y))
            train_end, eval_start, gap_n = _compute_train_eval_split(n)
            Xtr, Xev = X[:train_end], X[eval_start:]
            ytr, yev = y[:train_end], y[eval_start:]
            if Xev.shape[0] == 0 or Xtr.shape[0] == 0:
                return None
            LOG.info(
                "embed_regressor split n=%d train_n=%d gap_n=%d eval_n=%d",
                n,
                int(len(ytr)),
                int(gap_n),
                int(len(yev)),
            )

            results: Dict[str, Tuple[bytes, Dict[str, float]]] = {}
            ordered_items = sorted(items, key=lambda row: (int(row[2]), str(row[3])))
            eval_items = ordered_items[eval_start:]
            oos_by_kind: Dict[str, List[Dict[str, Any]]] = {}

            # --------------------------------------------------
            # Weather contribution test (ridge-only, same split)
            # --------------------------------------------------
            try:
                built_base = _build_xy(items)
                built_weather = _build_xy(items)

                if built_base is not None and built_weather is not None:
                    Xb, yb = built_base
                    Xw, yw = built_weather
                    mr = Ridge(alpha=float(alpha), fit_intercept=True)
                    mr.fit(Xb[:train_end], yb[:train_end])
                    pb = mr.predict(Xb[eval_start:])
                    brmse, bsp, _ = _eval_predictions(yb[eval_start:], pb)

                    mw = Ridge(alpha=float(alpha), fit_intercept=True)
                    mw.fit(Xw[:train_end], yw[:train_end])
                    pw = mw.predict(Xw[eval_start:])
                    wrmse, wsp, _ = _eval_predictions(yw[eval_start:], pw)

                    con.execute(
                        """
                        INSERT OR REPLACE INTO model_weather_effect(
                          key_type, key, horizon_s, ts_ms,
                          base_rmse, wx_rmse, rmse_delta,
                          base_spearman, wx_spearman, spearman_delta,
                          n_eval
                        )
                        VALUES ('__PENDING__','__PENDING__',-1,?,?,?,?,?,?,?,?)
                        """,
                        (
                            int(now_ms),
                            float(brmse),
                            float(wrmse),
                            float(brmse) - float(wrmse),
                            float(bsp),
                            float(wsp),
                            float(wsp) - float(bsp),
                            int(len(yb[eval_start:])),
                        ),
                    )
            except Exception as exc:
                _warn_nonfatal(
                    "embed_regressor_weather_effect_probe_failed",
                    "EMBED_REGRESSOR_WEATHER_EFFECT_PROBE_FAILED",
                    exc,
                    warn_key="embed_regressor_weather_effect_probe_failed",
                )

            # --- Ridge ---

            try:
                model_r = Ridge(alpha=float(alpha), fit_intercept=True)
                model_r.fit(Xtr, ytr)
                pred_r = model_r.predict(Xev)
                blob_r = _serialize_ridge(model_r)
                rmse, sp, da = _eval_predictions(yev, pred_r)
                oos_by_kind["ridge"] = [
                    {"ts": int(eval_items[idx][2]), "prediction": float(pred_r[idx]), "target": float(yev[idx])}
                    for idx in range(min(len(eval_items), len(yev)))
                ]
                results["ridge"] = (blob_r, {
                    "n_train": int(len(ytr)),
                    "n_eval": int(len(yev)),
                    "rmse": float(rmse),
                    "spearman": float(sp),
                    "directional_acc": float(da),
                })
            except Exception as exc:
                _warn_nonfatal(
                    "embed_regressor_ridge_train_failed",
                    "EMBED_REGRESSOR_RIDGE_TRAIN_FAILED",
                    exc,
                    warn_key="embed_regressor_ridge_train_failed",
                )

            # --- MLP ---
            try:
                blob_m = _train_mlp(Xtr, ytr)
                payload = _deserialize_mlp_payload(blob_m)
                model = _MLPRegressor(
                    input_dim=int(payload["input_dim"]),
                    hidden=payload["hidden"],
                    dropout=float(payload["dropout"]),
                )
                model.load_state_dict(payload["state_dict"])
                model.eval()

                xn = (Xev - payload["x_mean"]) / payload["x_std"]
                with torch.no_grad():
                    pred_n = model(torch.from_numpy(xn.astype(np.float32, copy=False))).numpy()
                pred_m = np.asarray(pred_n * float(payload["y_std"]) + float(payload["y_mean"]), dtype=float).reshape(-1)
                rmse, sp, da = _eval_predictions(yev, pred_m)
                oos_by_kind["mlp"] = [
                    {"ts": int(eval_items[idx][2]), "prediction": float(pred_m[idx]), "target": float(yev[idx])}
                    for idx in range(min(len(eval_items), len(yev)))
                ]
                results["mlp"] = (blob_m, {
                    "n_train": int(len(ytr)),
                    "n_eval": int(len(yev)),
                    "rmse": float(rmse),
                    "spearman": float(sp),
                    "directional_acc": float(da),
                })
            except Exception as exc:
                _warn_nonfatal(
                    "embed_regressor_mlp_train_failed",
                    "EMBED_REGRESSOR_MLP_TRAIN_FAILED",
                    exc,
                    warn_key="embed_regressor_mlp_train_failed",
                )

            if not results:
                return None

            # choose best (A3) if auto; otherwise force requested kind
            if kind == "auto":
                best_kind = None
                best_em = None
                for k, (_blob, em) in results.items():
                    if best_em is None:
                        best_kind = k
                        best_em = em
                        continue
                    if float(em["rmse"]) < float(best_em["rmse"]) - 1e-12:
                        best_kind = k
                        best_em = em
                    elif abs(float(em["rmse"]) - float(best_em["rmse"])) <= 1e-12 and float(em["spearman"]) > float(best_em["spearman"]):
                        best_kind = k
                        best_em = em
                chosen_kind = best_kind
            else:
                chosen_kind = kind if kind in results else ("ridge" if "ridge" in results else list(results.keys())[0])

            chosen_blob, _ = results[str(chosen_kind)]
            return dim, str(chosen_kind), chosen_blob, results, list(oos_by_kind.get(str(chosen_kind), []))

        # train symbol models
        for (sym_u, h_i), items in sym_buckets.items():
            res = _train_one(items)
            if not res:
                continue
            dim, _chosen_kind, blob_out, results, oos_rows = res

            # A1: store eval rows for all trained kinds
            for mk, (_b, em) in (results or {}).items():
                con.execute(
                    """
                    INSERT OR REPLACE INTO embed_model_eval(
                      key_type, key, horizon_s, model_kind, ts_ms,
                      n_train, n_eval, rmse, spearman, directional_acc
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        "symbol",
                        sym_u,
                        int(h_i),
                        str(mk),
                        int(now_ms),
                        int(em["n_train"]),
                        int(em["n_eval"]),
                        float(em["rmse"]),
                        float(em["spearman"]),
                        float(em["directional_acc"]),
                    ),
                )

                # A2 calibration samples
                try:
                    _calib.setdefault((int(h_i), str(mk)), {"x": [], "y": []})
                    _calib[(int(h_i), str(mk))]["x"].append(
                        _conf_from_n(int(em["n_train"]), float(conf_k))
                    )
                    _calib[(int(h_i), str(mk))]["y"].append(
                        float(em["directional_acc"])
                    )
                except Exception as exc:
                    _warn_nonfatal(
                        "embed_regressor_symbol_calibration_accumulate_failed",
                        "EMBED_REGRESSOR_SYMBOL_CALIBRATION_ACCUMULATE_FAILED",
                        exc,
                        warn_key="embed_regressor_symbol_calibration_accumulate_failed",
                    )

            # A3: store only winner blob
            _upsert_model(con, "symbol", _tag_key(sym_u), h_i, now_ms, len(items), int(dim), blob_out)
            try:
                upsert_oos_predictions(
                    [
                        {
                            "symbol": str(sym_u),
                            "horizon": int(h_i),
                            "family": "embed_regressor",
                            "ts": int(row.get("ts") or 0),
                            "run_id": str(oos_run_id),
                            "prediction": float(row.get("prediction") or 0.0),
                            "target": float(row.get("target") or 0.0),
                        }
                        for row in list(oos_rows or [])
                    ],
                    con=con,
                )
            except Exception as exc:
                _warn_nonfatal(
                    "embed_regressor_oos_persist_failed",
                    "EMBED_REGRESSOR_OOS_PERSIST_FAILED",
                    exc,
                    warn_key=f"oos_persist:{sym_u}:{h_i}",
                    symbol=str(sym_u),
                    horizon=int(h_i),
                )
            out[("symbol", _tag_key(sym_u), h_i)] = int(len(items))

            # Fill pending weather-effect row (if any) for this (symbol,h)
            try:
                con.execute(
                    """
                    UPDATE model_weather_effect
                    SET key_type='symbol', key=?, horizon_s=?
                    WHERE key_type='__PENDING__' AND key='__PENDING__' AND horizon_s=-1 AND ts_ms=?
                    """,
                    (str(sym_u), int(h_i), int(now_ms)),
                )
            except Exception as exc:
                _warn_nonfatal(
                    "embed_regressor_weather_effect_update_failed",
                    "EMBED_REGRESSOR_WEATHER_EFFECT_UPDATE_FAILED",
                    exc,
                    warn_key="embed_regressor_weather_effect_update_failed",
                )

        # train class models
        if train_by_class:
            for (cls, h_i), items in cls_buckets.items():
                res = _train_one(items)
                if not res:
                    continue
                dim, _chosen_kind, blob_out, results, _oos_rows = res

                for mk, (_b, em) in (results or {}).items():

                    try:
                        _calib.setdefault((int(h_i), str(mk)), {"x": [], "y": []})
                        _calib[(int(h_i), str(mk))]["x"].append(
                            _conf_from_n(int(em["n_train"]), float(conf_k))
                        )
                        _calib[(int(h_i), str(mk))]["y"].append(
                            float(em["directional_acc"])
                        )
                    except Exception as exc:
                        _warn_nonfatal(
                            "embed_regressor_class_calibration_accumulate_failed",
                            "EMBED_REGRESSOR_CLASS_CALIBRATION_ACCUMULATE_FAILED",
                            exc,
                            warn_key="embed_regressor_class_calibration_accumulate_failed",
                        )

                _upsert_model(
                    con,
                    "class",
                    _tag_key(str(cls).upper()),
                    h_i,
                    now_ms,
                    len(items),
                    int(dim),
                    blob_out,
                )
                out[("class", _tag_key(str(cls).upper()), h_i)] = int(len(items))

        # -----------------------------------
        # A2: fit + persist confidence calibration curves
        # -----------------------------------

        persist_feature_schema(
            con,
            ts_ms=int(now_ms),
            feature_ids=feature_ids,
            feature_set=str(tag),
            model_kind=str(kind),
        )
        con.commit()
        return out

    finally:
        con.close()


def _predict_raw(
    key_type: str,
    key: str,
    horizon_s: int,
    query_vec: np.ndarray
) -> Optional[Tuple[float, int, int, str, str, str]]:
    """
    Returns:
      (predicted_z, n_support, model_ts_ms, model_key_type, model_key, model_kind)
    """
    init_embed_models_db()
    con = connect()
    try:
        _ensure_embed_artifact_columns(con)
        row = con.execute(
            """
            SELECT ts_ms, n, dim, model_blob, artifact_sha256, artifact_alias
            FROM embed_models2
            WHERE key_type=? AND key=? AND horizon_s=?
            """,
            (str(key_type), str(key), int(horizon_s)),
        ).fetchone()

        if not row:
            return None

        ts_ms, n, dim, blob, artifact_sha256, artifact_alias = row
        if (not blob) and (artifact_sha256 or artifact_alias):
            blob = _load_artifact_blob(str(artifact_alias or ""), str(artifact_sha256 or ""))
        if blob is None or bytes(blob) == b"":
            return None

        q = np.asarray(query_vec, dtype=np.float32).reshape(-1)
        if q.shape[0] != int(dim):
            return None

        # --- MLP path ---
        try:
            if bytes(blob).startswith(_MLP_MAGIC):
                payload = _deserialize_mlp_payload(bytes(blob))
                if int(payload.get("input_dim") or 0) != int(dim):
                    return None

                x_mean = np.asarray(payload.get("x_mean"), dtype=np.float32).reshape(-1)
                x_std = np.asarray(payload.get("x_std"), dtype=np.float32).reshape(-1)
                if x_mean.shape[0] != int(dim) or x_std.shape[0] != int(dim):
                    return None
                x_std = np.where(x_std < 1e-6, 1.0, x_std).astype(np.float32)

                y_mean = float(payload.get("y_mean") or 0.0)
                y_std = float(payload.get("y_std") or 1.0)
                if abs(float(y_std)) < 1e-6:
                    y_std = 1.0

                hidden = payload.get("hidden") or [128, 64]
                dropout = float(payload.get("dropout") or 0.0)

                model = _MLPRegressor(input_dim=int(dim), hidden=[int(h) for h in hidden], dropout=float(dropout))
                sd = payload.get("state_dict")
                if not isinstance(sd, dict):
                    return None
                model.load_state_dict(sd)
                model.eval()

                xn = (q.astype(np.float32) - x_mean) / x_std
                with torch.no_grad():
                    pred_n = float(model(torch.from_numpy(xn.reshape(1, -1))).item())
                pred = float(pred_n * y_std + y_mean)
                return pred, int(n), int(ts_ms), str(key_type), str(key), "mlp"
        except Exception as exc:
            # if MLP blob decode fails, fall through to ridge decode attempt
            _warn_nonfatal(
                "embed_regressor_mlp_decode_failed",
                "EMBED_REGRESSOR_MLP_DECODE_FAILED",
                exc,
                warn_key="embed_regressor_mlp_decode_failed",
                key_type=str(key_type),
                key=str(key),
                horizon_s=int(horizon_s),
            )

        # --- Ridge (legacy) path ---
        coef, intercept = _deserialize_ridge(bytes(blob))
        if coef.shape[0] != int(dim):
            return None
        pred = float(np.dot(coef, q) + float(intercept))
        return pred, int(n), int(ts_ms), str(key_type), str(key), "ridge"

    finally:
        con.close()


def predict_with_embed_model(
    symbol: str,
    horizon_s: int,
    query_vec: np.ndarray,
    *,
    feature_ids: Optional[List[str]] = None,
    model_name: Optional[str] = None,
) -> Optional[Tuple[float, int, int, str, str, str]]:
    """
    Returns:
      (predicted_z, n_support, model_ts_ms, model_key_type, model_key, model_kind)
    Tries symbol model first, then asset-class model.

    NOTE:
    - When feature flags change (e.g. weather on/off), we namespace model keys
      with "#<feature_set_tag>" to avoid overwriting existing models.
    - If a namespaced model is missing, we fall back to the legacy key.
    """
    sym_u = str(symbol).upper()
    h = int(horizon_s)
    namespace = _normalize_model_namespace(model_name)

    feature_ids = resolve_feature_ids(
        feature_ids,
        model_name=(model_name or DEFAULT_FAMILY),
    )
    tag = feature_set_tag(feature_ids)
    sym_base_key = sym_u if tag == "base" else f"{sym_u}#{tag}"
    sym_key = _namespaced_model_key(namespace, sym_base_key)

    r1 = _predict_raw("symbol", sym_key, h, query_vec)
    if r1 is None and _uses_legacy_namespace(namespace) and sym_key != sym_u:
        r1 = _predict_raw("symbol", sym_u, h, query_vec)
    if r1 is not None:
        return r1

    cls = asset_class_for_symbol(sym_u)
    if cls and str(cls).upper() != "UNKNOWN":
        cls_u = str(cls).upper()
        cls_base_key = cls_u if tag == "base" else f"{cls_u}#{tag}"
        cls_key = _namespaced_model_key(namespace, cls_base_key)

        r2 = _predict_raw("class", cls_key, h, query_vec)
        if r2 is None and _uses_legacy_namespace(namespace) and cls_key != cls_u:
            r2 = _predict_raw("class", cls_u, h, query_vec)
        if r2 is not None:
            return r2

    return None
