"""
FILE: temporal_encoder.py

Builds sequence-aware event embeddings from recent embedding history.

This is an offline feature-construction module: it learns a compact temporal
representation and stores the result in `event_embeddings_seq` for downstream
training and inference code.
"""

import logging
from typing import List, Optional

import numpy as np
import torch
import torch.nn as nn
from torch.optim.adamw import AdamW

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

_TORCH_SEED = 42
LOG = get_logger("strategy.temporal_encoder")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_temporal_encoder_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.temporal_encoder",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


class TemporalMLP(nn.Module):
    def __init__(self, input_dim: int, hidden: List[int]):
        super().__init__()
        dims = [input_dim] + list(hidden) + [input_dim]
        layers = []
        for i in range(len(dims) - 2):
            layers.append(nn.Linear(dims[i], dims[i + 1]))
            layers.append(nn.ReLU())
        layers.append(nn.Linear(dims[-2], dims[-1]))
        self.net = nn.Sequential(*layers)

    def forward(self, x):
        return self.net(x)


def _load_recent_embeddings(con, event_ts_ms: int, window: int):
    rows = con.execute(
        """
        SELECT e.ts_ms, emb.vec
        FROM events e
        JOIN event_embeddings emb ON emb.event_id = e.id
        WHERE e.ts_ms < ?
        ORDER BY e.ts_ms DESC
        LIMIT ?
        """,
        (int(event_ts_ms), int(window)),
    ).fetchall()

    seq = []
    prev_ts = event_ts_ms

    for ts, blob in rows:
        v = np.frombuffer(blob, dtype=np.float32)
        dt = float(prev_ts - ts) / 1000.0
        seq.append(np.concatenate([v, np.array([dt], dtype=np.float32)]))
        prev_ts = ts

    # Return the sequence in chronological order so downstream models can treat
    # the last element as the most recent context.
    return seq[::-1]


def build_temporal_embeddings(
    window: int = 5,
    hidden: Optional[List[int]] = None,
    epochs: int = 80,
    lr: float = 3e-3,
):
    if hidden is None:
        hidden = [128, 64]

    torch.manual_seed(_TORCH_SEED)
    np.random.seed(_TORCH_SEED)

    con = connect()
    try:
        # Fail-soft if labels table isn't created yet
        try:
            con.execute("SELECT 1 FROM labels LIMIT 1").fetchone()
        except Exception as e:
            _warn_nonfatal("TEMPORAL_ENCODER_LABELS_PROBE_FAILED", e, once_key="labels_probe")
            return {"ok": True, "trained": 0}

        rows = con.execute("SELECT id, ts_ms FROM events ORDER BY ts_ms ASC").fetchall()
        if not rows:
            return {"ok": True, "trained": 0}

        # Infer embedding dimensionality from stored vectors so the encoder does
        # not need a separate schema constant.
        row = con.execute("SELECT vec FROM event_embeddings LIMIT 1").fetchone()
        if not row:
            return {"ok": True, "trained": 0}

        base_dim = int(len(np.frombuffer(row[0], dtype=np.float32)))
        input_dim = base_dim + 1

        model = TemporalMLP(input_dim=input_dim, hidden=hidden)
        opt = AdamW(model.parameters(), lr=float(lr))
        loss_fn = nn.MSELoss()

        samples = []
        for eid, ts in rows:
            seq = _load_recent_embeddings(con, int(ts), window)
            if len(seq) < window:
                continue
            X = np.stack(seq).astype(np.float32)
            # The training target is the current embedding, using recent history
            # and a time-delta feature as the input context.
            y = X[-1, :-1]
            samples.append((int(eid), X, y))

        if not samples:
            return {"ok": True, "trained": 0}

        model.train()
        for _ in range(int(epochs)):
            for _eid, X, y in samples:
                xt = torch.from_numpy(X)
                yt = torch.from_numpy(y)
                opt.zero_grad(set_to_none=True)
                pred = model(xt[-1])
                loss = loss_fn(pred, yt)
                loss.backward()
                opt.step()

        # persist embeddings
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS event_embeddings_seq (
              event_id INTEGER PRIMARY KEY,
              dim INTEGER NOT NULL,
              vec BLOB NOT NULL
            )
            """
        )

        trained = 0
        for eid, X, _y in samples:
            xt = torch.from_numpy(X)
            with torch.no_grad():
                out = model(xt[-1]).numpy().astype(np.float32)

            con.execute(
                """
                INSERT OR REPLACE INTO event_embeddings_seq(event_id, dim, vec)
                VALUES (?,?,?)
                """,
                (int(eid), int(len(out)), out.tobytes()),
            )
            trained += 1

        con.commit()
        return {"ok": True, "trained": trained}

    finally:
        con.close()
