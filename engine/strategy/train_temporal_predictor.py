"""Training job for temporal sequence models used in the repo's ML stack.

The temporal trainer builds sequence-aware models from recent embeddings,
records the exact feature schema and dataset fingerprint used for training, and
registers the resulting version in lifecycle/governance tables so promotion can
stay auditable.
"""

import io
import json
import logging
import os
import time
import socket
import uuid
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import torch
import torch.nn as nn
from torch.optim.adamw import AdamW
from engine.artifacts.store import LocalArtifactStore
from engine.artifacts.serialization import dumps_torch_payload
from engine.runtime.failure_diagnostics import log_failure
from engine.strategy.tuning.catalog import default_for
from engine.strategy.tuning.study import fetch_best_params

from engine.runtime import dbapi_compat as dbapi

LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: Exception, *, once_key: str | None = None, **extra: Any) -> None:
    key = str(once_key or "")
    if key:
        if key in _WARNED_NONFATAL_KEYS:
            return
        _WARNED_NONFATAL_KEYS.add(key)
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.train_temporal_predictor",
        extra=extra or None,
        include_health=False,
        persist=False,
    )

# Training defaults to a conservative CPU path so an unattended job cannot grab
# a GPU unless the operator explicitly opts in.
if os.environ.get("TEMPORAL_USE_CUDA", "0") != "1":
    torch.set_default_device("cpu")
else:
    try:
        torch.set_default_device("cuda")
    except Exception as e:
        _warn_nonfatal("TRAIN_TEMPORAL_PREDICTOR_SET_CUDA_DEVICE_FAILED", e, once_key="set_cuda_device")

# Performance flags (TF32 + cuDNN benchmark when not deterministic)
_DET = os.environ.get("TORCH_DETERMINISTIC", "0") == "1"
try:
    torch.use_deterministic_algorithms(_DET)
except Exception as e:
    _warn_nonfatal("TRAIN_TEMPORAL_PREDICTOR_DETERMINISTIC_CONFIG_FAILED", e, once_key="deterministic_config")
try:
    torch.backends.cudnn.deterministic = _DET
    torch.backends.cudnn.benchmark = (not _DET) and (os.environ.get("CUDNN_BENCHMARK", "1") == "1")
except Exception as e:
    _warn_nonfatal("TRAIN_TEMPORAL_PREDICTOR_CUDNN_CONFIG_FAILED", e, once_key="cudnn_config")
try:
    torch.set_float32_matmul_precision(os.environ.get("TORCH_MATMUL_PRECISION", "high"))
except Exception as e:
    _warn_nonfatal("TRAIN_TEMPORAL_PREDICTOR_MATMUL_PRECISION_FAILED", e, once_key="matmul_precision")
try:
    torch.backends.cuda.matmul.allow_tf32 = os.environ.get("TORCH_ALLOW_TF32", "1") == "1"
except Exception as e:
    _warn_nonfatal("TRAIN_TEMPORAL_PREDICTOR_CUDA_TF32_CONFIG_FAILED", e, once_key="cuda_tf32")
try:
    torch.backends.cudnn.allow_tf32 = os.environ.get("CUDNN_ALLOW_TF32", "1") == "1"
except Exception as e:
    _warn_nonfatal("TRAIN_TEMPORAL_PREDICTOR_CUDNN_TF32_CONFIG_FAILED", e, once_key="cudnn_tf32")

from engine.runtime.storage import connect, init_db, acquire_job_lock, release_job_lock
from engine.data.asset_map import asset_class_for_symbol
from engine.strategy.model_lifecycle import (
    finish_lifecycle_run,
    load_lifecycle_plan,
    publish_lifecycle_status,
    record_version_performance,
    register_model_version,
    start_lifecycle_run,
    update_model_version_status,
    version_from_ts,
)
from engine.strategy.learning_loop import build_dataset_snapshot
from engine.backtest.cpcv import CombinatorialPurgedKFold
from engine.strategy.ensemble.oos_store import upsert_oos_predictions
from engine.training_guard import training_allowed

# ------            -- ------------------------------------------------------
# Job identity
# ------            -- ------------------------------------------------------

JOB_NAME = "train_temporal_predictor"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", socket.gethostname())),
)
PID = os.getpid()

# ------            -- ------------------------------------------------------
# Constants / schema
# ------            -- ------------------------------------------------------

_TMAGIC = b"TMP1"
_TORCH_SEED = 42

_SCHEMA = """
CREATE TABLE IF NOT EXISTS temporal_models (
  key_type TEXT NOT NULL,
  key TEXT NOT NULL,
  horizon_s INTEGER NOT NULL,
  ts_ms INTEGER NOT NULL,
  n INTEGER NOT NULL,
  embed_dim INTEGER NOT NULL,
  seq_len INTEGER NOT NULL,
  model_kind TEXT NOT NULL,
  model_blob BLOB,
  artifact_sha256 TEXT,
  artifact_alias TEXT,
  PRIMARY KEY (key_type, key, horizon_s)
);

CREATE INDEX IF NOT EXISTS idx_temporal_models_ts
  ON temporal_models(ts_ms);

CREATE TABLE IF NOT EXISTS temporal_model_eval (
  key_type TEXT NOT NULL,
  key TEXT NOT NULL,
  horizon_s INTEGER NOT NULL,
  model_kind TEXT NOT NULL,
  ts_ms INTEGER NOT NULL,
  n_train INTEGER NOT NULL,
  n_eval INTEGER NOT NULL,
  rmse REAL NOT NULL,
  spearman REAL NOT NULL,
  directional_acc REAL NOT NULL,
  PRIMARY KEY (key_type, key, horizon_s, model_kind)
);
CREATE INDEX IF NOT EXISTS idx_temporal_model_eval_ts
  ON temporal_model_eval(ts_ms DESC);

CREATE TABLE IF NOT EXISTS temporal_model_feature_schema (
  ts_ms INTEGER PRIMARY KEY,
  feature_set_tag TEXT NOT NULL,
  feature_ids_json TEXT NOT NULL,
  schema_json TEXT NOT NULL,
  created_ts_ms INTEGER NOT NULL
);
"""

# ------            -- ------------------------------------------------------
# Model
# ------            -- ------------------------------------------------------

class _TemporalMLP(nn.Module):
    def __init__(self, input_dim: int, hidden: List[int]):
        super().__init__()
        dims = [int(input_dim)] + [int(h) for h in (hidden or [])] + [1]
        layers: List[nn.Module] = []
        for i in range(len(dims) - 2):
            layers.append(nn.Linear(dims[i], dims[i + 1]))
            layers.append(nn.ReLU())
        layers.append(nn.Linear(dims[-2], dims[-1]))
        self.net = nn.Sequential(*layers)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x).squeeze(-1)


# ------            -- ------------------------------------------------------
# Helpers
# ------            -- ------------------------------------------------------
def _serialize_payload(payload: Dict) -> bytes:
    return _TMAGIC + dumps_torch_payload(payload)


def _ensure_temporal_artifact_columns(con) -> None:
    cols = {
        str(row[1] or "").strip().lower()
        for row in (con.execute("PRAGMA table_info(temporal_models)").fetchall() or [])
        if row and len(row) >= 2
    }
    if "artifact_sha256" not in cols:
        con.execute("ALTER TABLE temporal_models ADD COLUMN artifact_sha256 TEXT")
    if "artifact_alias" not in cols:
        con.execute("ALTER TABLE temporal_models ADD COLUMN artifact_alias TEXT")


def _embedding_table() -> str:
    return "event_embeddings_seq" if os.environ.get("USE_TEMPORAL_EMB_TABLE", "0") == "1" else "event_embeddings"


def _eval_predictions(y_true: np.ndarray, y_pred: np.ndarray) -> Tuple[float, float, float]:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    if y_true.size == 0:
        return 0.0, 0.0, 0.0

    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))

    try:
        rt = y_true.argsort().argsort()
        rp = y_pred.argsort().argsort()
        spearman = float(np.corrcoef(rt, rp)[0, 1])
        if not np.isfinite(spearman):
            spearman = 0.0
    except Exception:
        spearman = 0.0

    try:
        # Tiny magnitudes are flattened to zero so directional accuracy is not
        # dominated by numerical noise around the origin.
        eps = 1e-9
        yt_s = np.sign(np.where(np.abs(y_true) < eps, 0.0, y_true))
        yp_s = np.sign(np.where(np.abs(y_pred) < eps, 0.0, y_pred))
        directional = float(np.mean(yt_s == yp_s))
    except Exception:
        directional = 0.0

    return rmse, spearman, directional


def _safe_float_env(name: str, default: float) -> float:
    raw = os.environ.get(name, "")
    if raw in (None, ""):
        return float(default)
    try:
        value = float(raw)
    except Exception as e:
        _warn_nonfatal(
            "TRAIN_TEMPORAL_PREDICTOR_ENV_FLOAT_PARSE_FAILED",
            e,
            once_key=f"env_float_{name}",
            name=str(name),
            raw_value=repr(raw),
        )
        return float(default)
    return float(value) if np.isfinite(value) else float(default)


def _safe_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name, "")
    if raw in (None, ""):
        return int(default)
    try:
        return int(raw)
    except Exception as e:
        _warn_nonfatal(
            "TRAIN_TEMPORAL_PREDICTOR_ENV_INT_PARSE_FAILED",
            e,
            once_key=f"env_int_{name}",
            name=str(name),
            raw_value=repr(raw),
        )
        return int(default)


def _temporal_holdout_indices(
    n_samples: int,
    *,
    eval_fraction: float,
    label_horizon_rows: int,
    embargo_pct: float = 0.0,
) -> Tuple[np.ndarray, np.ndarray]:
    total = int(max(0, int(n_samples or 0)))
    if total <= 1:
        return np.asarray([], dtype=int), np.asarray([], dtype=int)

    if int(label_horizon_rows or 0) > 1 and total >= 12:
        n_splits = max(2, min(_safe_int_env("TEMPORAL_CPCV_N_SPLITS", 6), total // 2))
        n_test_splits = max(1, min(_safe_int_env("TEMPORAL_CPCV_N_TEST_SPLITS", 2), n_splits - 1))
        splitter = CombinatorialPurgedKFold(
            n_splits=int(n_splits),
            n_test_splits=int(n_test_splits),
            embargo=float(max(0.0, embargo_pct)),
            label_horizon=int(label_horizon_rows),
        )
        candidates = list(splitter.split(np.arange(total, dtype=float)))
        if candidates:
            train_idx, eval_idx = max(
                candidates,
                key=lambda pair: (
                    int(pair[1][-1]) if pair[1].size else -1,
                    int(pair[0].size),
                ),
            )
            return np.sort(train_idx.astype(int, copy=False)), np.sort(eval_idx.astype(int, copy=False))

    frac = float(eval_fraction if np.isfinite(float(eval_fraction)) else 0.20)
    frac = min(max(frac, 1.0 / float(total)), 0.50)
    eval_n = max(1, int(np.ceil(float(total) * frac)))
    if eval_n >= total:
        eval_n = max(1, total - 1)

    eval_start = int(total - eval_n)
    eval_idx = np.arange(eval_start, total, dtype=int)
    train_end = max(0, eval_start - max(1, int(label_horizon_rows or 1)))
    train_idx = np.arange(0, train_end, dtype=int)
    return np.sort(train_idx.astype(int, copy=False)), np.sort(eval_idx.astype(int, copy=False))


def _load_recent_event_ids(con, ts_ms: int, seq_len: int) -> List[Tuple[int, int]]:
    rows = con.execute(
        f"""
        SELECT e.id, e.ts_ms
        FROM events e
        JOIN {_embedding_table()} emb ON emb.event_id = e.id
        WHERE e.ts_ms <= ?
        ORDER BY e.ts_ms DESC
        LIMIT ?
        """,
        (int(ts_ms), int(seq_len)),
    ).fetchall()
    out = [(int(r[0]), int(r[1])) for r in (rows or [])]
    out.reverse()
    return out


def _load_embedding(con, event_id: int) -> Optional[np.ndarray]:
    row = con.execute(
        f"SELECT vec FROM {_embedding_table()} WHERE event_id=?",
        (int(event_id),),
    ).fetchone()
    if not row or row[0] is None:
        return None
    return np.frombuffer(row[0], dtype=np.float32)


def _build_sequence_flat(con, ts_ms: int, seq_len: int) -> Optional[Tuple[np.ndarray, int]]:
    ids = _load_recent_event_ids(con, int(ts_ms), int(seq_len))
    if len(ids) < int(seq_len):
        return None

    vecs: List[np.ndarray] = []
    prev_ts: Optional[int] = None
    embed_dim: Optional[int] = None

    for (eid, ets) in ids:
        v = _load_embedding(con, int(eid))
        if v is None:
            return None
        if embed_dim is None:
            embed_dim = int(v.shape[0])
        if int(v.shape[0]) != int(embed_dim):
            return None

        if prev_ts is None:
            dt_s = 0.0
        else:
            dt_s = float(max(0, int(ets) - int(prev_ts))) / 1000.0
        prev_ts = int(ets)

        vecs.append(
            np.concatenate(
                [v.astype(np.float32, copy=False), np.asarray([dt_s], dtype=np.float32)]
            )
        )

    flat = np.concatenate(vecs).astype(np.float32, copy=False)
    return flat, int(embed_dim or 0)


def _upsert_model(
    con,
    key_type: str,
    key: str,
    horizon_s: int,
    now_ms: int,
    n: int,
    embed_dim: int,
    seq_len: int,
    kind: str,
    blob: bytes,
):
    _ensure_temporal_artifact_columns(con)
    artifact_alias = f"model:temporal_predictor:{str(key_type)}:{str(key)}:{int(horizon_s)}:current"
    ref = LocalArtifactStore().put(
        bytes(blob or b""),
        content_type="application/octet-stream",
        kind="model",
        alias=artifact_alias,
        metadata={
            "model_name": "temporal_predictor",
            "key_type": str(key_type),
            "key": str(key),
            "horizon_s": int(horizon_s),
            "ts_ms": int(now_ms),
            "n": int(n),
            "embed_dim": int(embed_dim),
            "seq_len": int(seq_len),
            "model_kind": str(kind),
        },
    )
    con.execute(
        """
        INSERT INTO temporal_models(
          key_type, key, horizon_s, ts_ms, n, embed_dim, seq_len, model_kind,
          model_blob, artifact_sha256, artifact_alias
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(key_type, key, horizon_s) DO UPDATE SET
          ts_ms=excluded.ts_ms,
          n=excluded.n,
          embed_dim=excluded.embed_dim,
          seq_len=excluded.seq_len,
          model_kind=excluded.model_kind,
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
            int(embed_dim),
            int(seq_len),
            str(kind),
            dbapi.Binary(b""),
            str(ref.sha256),
            str(artifact_alias),
        ),
    )


def _set_deterministic(seed: int = _TORCH_SEED) -> None:
    np.random.seed(int(seed))
    torch.manual_seed(int(seed))
    try:
        torch.use_deterministic_algorithms(True)
    except Exception as e:
        _warn_nonfatal("TRAIN_TEMPORAL_PREDICTOR_FORCE_DETERMINISTIC_FAILED", e, once_key="force_deterministic")


def _temporal_feature_schema(seq_len: int) -> Dict[str, Any]:
    # The temporal family has a small but explicit feature contract. Persisting
    # it here is what lets serving later verify it is using the same layout.
    emb_table = _embedding_table()
    return {
        "feature_set_tag": "temporal.sequence.v1",
        "feature_ids": [
            "temporal.sequence.embedding",
            "temporal.sequence.delta_t_seconds",
        ],
        "sequence_schema": {
            "seq_len": int(seq_len),
            "embedding_table": str(emb_table),
            "per_step_layout": [
                "embedding_vector",
                "delta_t_seconds",
            ],
        },
    }
    try:
        torch.backends.cudnn.deterministic = True
        torch.backends.cudnn.benchmark = False
    except Exception as e:
        _warn_nonfatal("TRAIN_TEMPORAL_PREDICTOR_FORCE_CUDNN_DETERMINISTIC_FAILED", e, once_key="force_cudnn_deterministic")


# ------            -- ------------------------------------------------------
# Main
# ------            -- ------------------------------------------------------

def main() -> int:
    init_db()
    plan = load_lifecycle_plan("temporal_predictor")
    lifecycle_run_id = int(plan.get("lifecycle_run_id") or 0)
    model_version = ""

    if not training_allowed():
        print("training disabled by training_guard")
        return 0

    if not acquire_job_lock(JOB_NAME, OWNER, PID):
        print("another training job is running; exiting")
        return 0

    try:
        con0 = connect()
        try:
            con0.executescript(_SCHEMA)
            con0.commit()
        finally:
            con0.close()

        tuned_params = fetch_best_params("temporal_predictor", "GLOBAL")
        seq_len = int(tuned_params.get("seq_len", default_for("temporal_predictor", "seq_len", 6)))
        feature_schema = _temporal_feature_schema(seq_len)
        min_samples = int(os.environ.get("TEMPORAL_MIN_SAMPLES", "120"))
        lookback_days = int(os.environ.get("TEMPORAL_LOOKBACK_DAYS", "365"))
        train_by_class = os.environ.get("TEMPORAL_TRAIN_BY_CLASS", "1") == "1"

        hidden = [int(x) for x in os.environ.get("TEMPORAL_HIDDEN", "256,128").split(",") if x.strip()]
        if not hidden:
            hidden = [256, 128]

        lr = float(tuned_params.get("lr", default_for("temporal_predictor", "lr", 0.003)))
        epochs = int(tuned_params.get("epochs", default_for("temporal_predictor", "epochs", 120)))
        weight_decay = float(os.environ.get("TEMPORAL_WEIGHT_DECAY", "0.0001"))
        eval_fraction = _safe_float_env("TEMPORAL_EVAL_FRACTION", 0.20)
        embargo_pct = _safe_float_env("TEMPORAL_EMBARGO_PCT", 0.0)
        label_horizon_rows = _safe_int_env(
            "TEMPORAL_LABEL_HORIZON_ROWS",
            max(0, int(seq_len) - 1),
        )

        # Optional: cap CPU threads for stability in 24/7 envs (leave default if unset)
        try:
            _threads = int(os.environ.get("TORCH_NUM_THREADS", "0"))
            if _threads > 0:
                torch.set_num_threads(_threads)
        except Exception as e:
            _warn_nonfatal("TRAIN_TEMPORAL_PREDICTOR_SET_NUM_THREADS_FAILED", e, once_key="set_num_threads")

        now_ms = int(time.time() * 1000)
        oos_run_id = str(uuid.uuid4())
        model_version = str(plan.get("model_version") or version_from_ts("temporal_predictor", int(now_ms), prefix="temporal"))
        cutoff_ms = now_ms - int(lookback_days) * 86400 * 1000
        training_started_ts_ms = int(time.time() * 1000)
        # Capture a dataset fingerprint up front so lifecycle and governance
        # readers can later answer "what exactly was this model trained on?"
        dataset_used = build_dataset_snapshot(
            model_name="temporal_predictor",
            lookback_days=int(lookback_days),
            feature_ids=list(feature_schema.get("feature_ids") or []),
            feature_schema=dict(feature_schema),
            training_window={
                "lookback_days": int(lookback_days),
                "end_ts_ms": int(training_started_ts_ms),
                "start_ts_ms": int(cutoff_ms),
                "label_horizon_rows": int(label_horizon_rows),
            },
            extra={
                "job_name": JOB_NAME,
                "seq_len": int(seq_len),
                "min_samples": int(min_samples),
                "train_by_class": bool(train_by_class),
                "eval_fraction": float(eval_fraction),
                "embargo_pct": float(embargo_pct),
                "label_horizon_rows": int(label_horizon_rows),
            },
        )
        if lifecycle_run_id <= 0:
            lifecycle_run_id = int(
                start_lifecycle_run(
                    model_name="temporal_predictor",
                    model_version=str(model_version),
                    parent_version=plan.get("parent_version"),
                    action=JOB_NAME,
                    status="running",
                    triggered_by=JOB_NAME,
                    mutation_kind=plan.get("mutation_kind"),
                    details={"variation": dict(plan or {})},
                )
                or 0
            )

        # ----------------------------
        # Phase 1: READ + BUILD DATA (no torch training yet)
        # ----------------------------
        con_r = connect()
        try:
            rows = con_r.execute(
                """
                SELECT l.event_id, l.symbol, l.horizon_s, l.impact_z, e.ts_ms
                FROM labels l
                JOIN events e ON e.id = l.event_id
                WHERE e.ts_ms >= ?
                """,
                (int(cutoff_ms),),
            ).fetchall()

            buckets: Dict[Tuple[str, str, int], List[Tuple[int, np.ndarray, float, int]]] = {}
            total_label_rows = 0
            null_impact_count = 0

            for eid, sym, h, z, ts_ms in rows or []:
                total_label_rows += 1
                sym_u = str(sym or "").upper().strip()
                if not sym_u:
                    continue
                h_i = int(h or 0)
                if h_i <= 0:
                    continue

                if z is None:
                    null_impact_count += 1
                    continue

                built = _build_sequence_flat(con_r, int(ts_ms), int(seq_len))
                if not built:
                    continue
                x, embed_dim = built

                try:
                    y = float(z)
                    if not np.isfinite(y):
                        null_impact_count += 1
                        continue
                except Exception as e:
                    _warn_nonfatal(
                        "TRAIN_TEMPORAL_PREDICTOR_LABEL_PARSE_FAILED",
                        e,
                        once_key=f"label_parse_{sym_u}_{h_i}",
                        symbol=str(sym_u),
                        horizon_s=int(h_i),
                    )
                    continue

                buckets.setdefault(("symbol", sym_u, h_i), []).append((int(ts_ms or 0), x, y, embed_dim))

                if train_by_class:
                    cls = asset_class_for_symbol(sym_u)
                    if cls and str(cls).upper() != "UNKNOWN":
                        buckets.setdefault(("class", str(cls).upper(), h_i), []).append((int(ts_ms or 0), x, y, embed_dim))

            # Add global bucket per horizon (from symbol buckets only)
            for (kt, key, h_i), items in list(buckets.items()):
                if kt != "symbol":
                    continue
                buckets.setdefault(("global", "ALL", h_i), []).extend(items)

            null_impact_ratio = (
                float(null_impact_count) / float(total_label_rows)
                if int(total_label_rows) > 0
                else 0.0
            )
            LOGGER.info(
                "temporal_predictor label_coverage total_rows=%d null_impact_rows=%d null_impact_ratio=%.4f",
                int(total_label_rows),
                int(null_impact_count),
                float(null_impact_ratio),
            )
            if int(total_label_rows) > 0 and float(null_impact_ratio) > 0.10:
                _warn_nonfatal(
                    "TRAIN_TEMPORAL_PREDICTOR_NULL_IMPACT_RATIO_HIGH",
                    RuntimeError(f"null_impact_ratio_high:{null_impact_ratio:.4f}"),
                    once_key="null_impact_ratio_high",
                    total_rows=int(total_label_rows),
                    null_impact_rows=int(null_impact_count),
                    null_impact_ratio=float(null_impact_ratio),
                )

        finally:
            con_r.close()

        # ----------------------------
        # Phase 2: TRAIN (no DB open)
        # ----------------------------
        trained = 0
        to_write: List[Tuple[str, str, int, int, int, int, int, str, bytes, int, int, float, float, float]] = []
        oos_rows: List[Dict[str, Any]] = []
        # tuple fields:
        # (kt, key, h_i, now_ms, n, embed_dim, seq_len, kind, blob, n_train, n_eval, rmse, spearman, directional_acc)

        for (kt, key, h_i), items in buckets.items():
            if len(items) < int(min_samples):
                continue

            ordered_items = sorted(items, key=lambda row: int(row[0]))
            embed_dim = int(ordered_items[0][3])
            xs: List[np.ndarray] = []
            ys: List[float] = []
            ok = True
            for _ts_ms, x, yv, ed in ordered_items:
                if int(ed) != int(embed_dim):
                    ok = False
                    break
                if not np.isfinite(float(yv)):
                    ok = False
                    break
                xs.append(x)
                ys.append(float(yv))
            if not ok:
                continue

            X = np.stack(xs).astype(np.float32, copy=False)
            y = np.asarray(ys, dtype=np.float32)

            n = int(len(y))
            train_idx, eval_idx = _temporal_holdout_indices(
                n,
                eval_fraction=float(eval_fraction),
                label_horizon_rows=int(label_horizon_rows),
                embargo_pct=float(embargo_pct),
            )
            if train_idx.size <= 0 or eval_idx.size <= 0:
                continue
            Xtr, Xev = X[train_idx], X[eval_idx]
            ytr, yev = y[train_idx], y[eval_idx]

            _set_deterministic()

            x_mean = Xtr.mean(axis=0).astype(np.float32)
            x_std = Xtr.std(axis=0).astype(np.float32)
            x_std = np.where(x_std < 1e-6, 1.0, x_std)

            y_mean = float(ytr.mean())
            y_std_val = float(ytr.std())
            y_std = float(y_std_val if np.isfinite(y_std_val) and y_std_val > 1e-6 else 1.0)

            Xtrn = (Xtr - x_mean) / x_std
            ytrn = (ytr - y_mean) / y_std
            Xevn = (Xev - x_mean) / x_std

            xt = torch.from_numpy(Xtrn)
            yt = torch.from_numpy(ytrn)

            model = _TemporalMLP(input_dim=int(Xtr.shape[1]), hidden=hidden)
            opt = AdamW(model.parameters(), lr=lr, weight_decay=weight_decay)
            loss_fn = nn.MSELoss()

            model.train()
            for _ in range(int(epochs)):
                opt.zero_grad(set_to_none=True)
                pred_t = model(xt)
                loss = loss_fn(pred_t, yt)
                loss.backward()
                opt.step()

            model.eval()
            with torch.no_grad():
                pred_n = model(torch.from_numpy(Xevn)).cpu().numpy().astype(np.float32, copy=False)
            pred = pred_n * y_std + y_mean

            rmse, sp, da = _eval_predictions(yev, pred)
            if str(kt) == "symbol":
                for local_idx, source_idx in enumerate(eval_idx.tolist()):
                    oos_rows.append(
                        {
                            "symbol": str(key),
                            "horizon": int(h_i),
                            "family": "temporal_predictor",
                            "ts": int(ordered_items[int(source_idx)][0]),
                            "run_id": str(oos_run_id),
                            "prediction": float(pred[int(local_idx)]),
                            "target": float(yev[int(local_idx)]),
                        }
                    )

            payload = {
                "kind": "temporal_mlp",
                "input_dim": int(Xtr.shape[1]),
                "hidden": hidden,
                "state_dict": model.state_dict(),
                "x_mean": x_mean,
                "x_std": x_std,
                "y_mean": float(y_mean),
                "y_std": float(y_std),
                "seq_len": int(seq_len),
                "embed_dim": int(embed_dim),
                "feature_set_tag": str(feature_schema.get("feature_set_tag") or ""),
                "feature_ids": list(feature_schema.get("feature_ids") or []),
                "feature_schema": dict(feature_schema.get("sequence_schema") or {}),
            }

            blob = _serialize_payload(payload)

            to_write.append(
                (
                    kt,
                    key,
                    int(h_i),
                    int(now_ms),
                    int(n),
                    int(embed_dim),
                    int(seq_len),
                    "temporal_mlp",
                    blob,
                    int(len(ytr)),
                    int(len(yev)),
                    float(rmse),
                    float(sp),
                    float(da),
                )
            )

        # ----------------------------
        # Phase 3: WRITE (short transaction)
        # ----------------------------
        con_w = connect()
        try:
            con_w.execute(
                """
                INSERT OR REPLACE INTO temporal_model_feature_schema(
                  ts_ms, feature_set_tag, feature_ids_json, schema_json, created_ts_ms
                )
                VALUES (?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    str(feature_schema.get("feature_set_tag") or "temporal.sequence.v1"),
                    json.dumps(list(feature_schema.get("feature_ids") or []), separators=(",", ":"), sort_keys=False),
                    json.dumps(dict(feature_schema.get("sequence_schema") or {}), separators=(",", ":"), sort_keys=True),
                    int(time.time() * 1000),
                ),
            )
            for (
                kt,
                key,
                h_i,
                now_ms2,
                n2,
                embed_dim2,
                seq_len2,
                kind,
                blob,
                n_train,
                n_eval,
                rmse,
                sp,
                da,
            ) in to_write:
                _upsert_model(
                    con_w,
                    key_type=kt,
                    key=key,
                    horizon_s=h_i,
                    now_ms=now_ms2,
                    n=n2,
                    embed_dim=embed_dim2,
                    seq_len=seq_len2,
                    kind=kind,
                    blob=blob,
                )

                con_w.execute(
                    """
                    INSERT OR REPLACE INTO temporal_model_eval(
                      key_type, key, horizon_s, model_kind, ts_ms,
                      n_train, n_eval, rmse, spearman, directional_acc
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        kt,
                        key,
                        h_i,
                        kind,
                        now_ms2,
                        n_train,
                        n_eval,
                        rmse,
                        sp,
                        da,
                    ),
                )

                trained += 1
            try:
                upsert_oos_predictions(oos_rows, con=con_w)
            except Exception as e:
                _warn_nonfatal(
                    "TRAIN_TEMPORAL_PREDICTOR_OOS_PERSIST_FAILED",
                    e,
                    once_key=f"oos_persist:{h_i}:{kind}",
                    horizon=int(h_i),
                    model_kind=str(kind),
                )

            con_w.commit()
        finally:
            con_w.close()

        print(json.dumps({"ok": True, "trained": trained}, indent=2))
        if trained > 0:
            avg_rmse = 0.0
            avg_spearman = 0.0
            avg_dir = 0.0
            total_eval = 0
            for row in to_write:
                _kt, _key, _h_i, _now_ms2, _n2, _embed_dim2, _seq_len2, kind, _blob, _n_train, n_eval, rmse, sp, da = row
                weight = max(1, int(n_eval or 0))
                total_eval += weight
                avg_rmse += float(rmse) * float(weight)
                avg_spearman += float(sp) * float(weight)
                avg_dir += float(da) * float(weight)
            if total_eval > 0:
                avg_rmse /= float(total_eval)
                avg_spearman /= float(total_eval)
                avg_dir /= float(total_eval)

            # Registering the version here makes training append-only: serving
            # and promotion read this state later instead of inferring it from
            # one mutable "current model" row.
            register_model_version(
                model_name="temporal_predictor",
                model_version=str(model_version),
                model_kind="temporal_mlp",
                parent_version=plan.get("parent_version"),
                mutation_kind=str(plan.get("mutation_kind") or "baseline_retrain"),
                stage="shadow",
                status="trained",
                live_ready=False,
                training_job_name=JOB_NAME,
                train_scope={
                    **dict(plan.get("train_scope") or {
                        "seq_len": int(seq_len),
                        "lookback_days": int(lookback_days),
                        "min_samples": int(min_samples),
                        "train_by_class": bool(train_by_class),
                        "eval_fraction": float(eval_fraction),
                        "embargo_pct": float(embargo_pct),
                        "label_horizon_rows": int(label_horizon_rows),
                        "null_impact_rows": int(null_impact_count),
                        "total_label_rows": int(total_label_rows),
                        "null_impact_ratio": float(null_impact_ratio),
                    }),
                    "dataset_used": dataset_used,
                },
                meta={
                    "feature_schema": feature_schema,
                    "trigger": plan.get("trigger") or {},
                    "dataset_used": dataset_used,
                    "training_started_ts_ms": int(training_started_ts_ms),
                    "eval_fraction": float(eval_fraction),
                    "embargo_pct": float(embargo_pct),
                    "label_horizon_rows": int(label_horizon_rows),
                    "null_impact_rows": int(null_impact_count),
                    "total_label_rows": int(total_label_rows),
                    "null_impact_ratio": float(null_impact_ratio),
                },
            )
            record_version_performance(
                model_name="temporal_predictor",
                model_version=str(model_version),
                metric_scope="training",
                metrics={
                    "avg_rmse": float(avg_rmse),
                    "avg_spearman": float(avg_spearman),
                    "avg_directional_acc": float(avg_dir),
                    "quality_score": float(max(0.0, min(1.0, avg_dir))),
                    "trained_models": int(trained),
                    "null_impact_rows": int(null_impact_count),
                    "total_label_rows": int(total_label_rows),
                    "null_impact_ratio": float(null_impact_ratio),
                    "ts_ms": int(now_ms),
                },
                sample_n=int(total_eval),
                meta={"job_name": JOB_NAME},
            )
            update_model_version_status(
                "temporal_predictor",
                str(model_version),
                stage="shadow",
                status="trained",
                live_ready=False,
                meta_patch={
                    "dataset_used": dataset_used,
                    "training_started_ts_ms": int(training_started_ts_ms),
                    "training_completed_ts_ms": int(time.time() * 1000),
                },
            )
            if lifecycle_run_id > 0:
                finish_lifecycle_run(
                    int(lifecycle_run_id),
                    status="ok",
                    details={
                        "model_version": str(model_version),
                        "trained_models": int(trained),
                        "total_eval": int(total_eval),
                        "dataset_used": dataset_used,
                    },
                )
            publish_lifecycle_status(
                {
                    "ok": True,
                    "model_name": "temporal_predictor",
                    "active_job": JOB_NAME,
                    "version": str(model_version),
                    "mutation_kind": str(plan.get("mutation_kind") or "baseline_retrain"),
                    "trained_models": int(trained),
                    "dataset_used": dataset_used,
                    "ts_ms": int(time.time() * 1000),
                }
            )
        return 0
    except Exception:
        if model_version:
            try:
                update_model_version_status(
                    "temporal_predictor",
                    str(model_version),
                    stage="retired",
                    status="error",
                    live_ready=False,
                    meta_patch={"error_ts_ms": int(time.time() * 1000)},
                )
            except Exception as inner:
                _warn_nonfatal(
                    "TRAIN_TEMPORAL_PREDICTOR_UPDATE_MODEL_VERSION_STATUS_FAILED",
                    inner,
                    once_key="update_model_version_status_error",
                    model_version=str(model_version),
                )
        if lifecycle_run_id > 0:
            try:
                finish_lifecycle_run(
                    int(lifecycle_run_id),
                    status="error",
                    details={"error_ts_ms": int(time.time() * 1000)},
                )
            except Exception as inner:
                _warn_nonfatal(
                    "TRAIN_TEMPORAL_PREDICTOR_FINISH_LIFECYCLE_RUN_FAILED",
                    inner,
                    once_key="finish_lifecycle_run_error",
                    lifecycle_run_id=int(lifecycle_run_id),
                )
        raise

    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("TRAIN_TEMPORAL_PREDICTOR_RELEASE_JOB_LOCK_FAILED", e, once_key="release_job_lock")


if __name__ == "__main__":
    raise SystemExit(main())
