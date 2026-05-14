"""Shared contracts for automated feature discovery."""

from __future__ import annotations

import hashlib
import json
import math
import time
from dataclasses import dataclass, field, replace
from typing import Any, Mapping, Protocol, Sequence

import numpy as np
import pandas as pd

from engine.strategy.statistics.factor_threshold import harvey_liu_zhu_threshold_result


def now_ms() -> int:
    return int(time.time() * 1000)


def _json_default(value: Any) -> Any:
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, (set, tuple)):
        return list(value)
    return str(value)


def stable_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=_json_default)


def content_hash(value: Any) -> str:
    return hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()


def candidate_hash(*, source: str, symbol: str, expression: str, params: Mapping[str, Any] | None = None) -> str:
    return content_hash(
        {
            "source": str(source or "").strip(),
            "symbol": str(symbol or "").strip().upper(),
            "expression": str(expression or "").strip(),
            "params": dict(params or {}),
        }
    )


def feature_id_from_hash(source: str, digest: str) -> str:
    prefix = str(source or "candidate").strip().lower().replace("_", ".") or "candidate"
    return f"discovered.{prefix}.{str(digest)[:16]}"


@dataclass(frozen=True)
class CandidateFeature:
    """One proposed feature and enough metadata to reproduce it."""

    source: str
    symbol: str
    expression: str
    params: Mapping[str, Any] = field(default_factory=dict)
    feature_id: str = ""
    hash: str = ""
    score: float | None = None

    def __post_init__(self) -> None:
        digest = str(self.hash or "").strip() or candidate_hash(
            source=str(self.source),
            symbol=str(self.symbol),
            expression=str(self.expression),
            params=dict(self.params or {}),
        )
        object.__setattr__(self, "hash", digest)
        if not str(self.feature_id or "").strip():
            object.__setattr__(self, "feature_id", feature_id_from_hash(str(self.source), digest))

    def params_json(self) -> str:
        return stable_json(dict(self.params or {}))


@dataclass(frozen=True)
class EvaluationResult:
    """Out-of-sample evidence for a candidate feature."""

    candidate_hash: str
    feature_id: str
    t_stat: float
    p_value: float
    q_value: float | None = None
    oos_ic: float | None = None
    decision: str = "pending"
    n_obs: int = 0
    diagnostics: Mapping[str, Any] = field(default_factory=dict)

    def with_gate(self, *, q_value: float, decision: str) -> "EvaluationResult":
        return replace(self, q_value=float(q_value), decision=str(decision))


class Discoverer(Protocol):
    """Discovery engine contract."""

    source: str

    def propose(self, symbol: str, train_df: pd.DataFrame) -> list[CandidateFeature]:
        ...

    def evaluate(
        self,
        candidate: CandidateFeature,
        test_df: pd.DataFrame,
        target: str | Sequence[float] | pd.Series,
    ) -> EvaluationResult:
        ...


def as_numeric_series(values: Any, *, name: str = "value") -> pd.Series:
    if isinstance(values, pd.Series):
        series = values.copy()
    else:
        series = pd.Series(values, name=name)
    return pd.to_numeric(series, errors="coerce").astype(float)


def target_series(
    test_df: pd.DataFrame,
    target: str | Sequence[float] | pd.Series,
) -> pd.Series:
    if isinstance(target, str):
        if target not in set(test_df.columns):
            raise ValueError(f"target_column_missing:{target}")
        return as_numeric_series(test_df[target], name=str(target))
    return as_numeric_series(target, name="target")


def finite_aligned(values: Any, target: Any) -> tuple[np.ndarray, np.ndarray]:
    x = np.asarray(as_numeric_series(values), dtype=float).reshape(-1)
    y = np.asarray(as_numeric_series(target), dtype=float).reshape(-1)
    n = min(int(x.size), int(y.size))
    if n <= 0:
        return np.asarray([], dtype=float), np.asarray([], dtype=float)
    x = x[:n]
    y = y[:n]
    finite = np.isfinite(x) & np.isfinite(y)
    return x[finite].astype(float), y[finite].astype(float)


def information_coefficient(values: Any, target: Any) -> float | None:
    x, y = finite_aligned(values, target)
    if int(x.size) < 3:
        return None
    if float(np.nanstd(x)) <= 1.0e-12 or float(np.nanstd(y)) <= 1.0e-12:
        return None
    x_rank = pd.Series(x).rank(method="average").to_numpy(dtype=float)
    y_rank = pd.Series(y).rank(method="average").to_numpy(dtype=float)
    corr = float(np.corrcoef(x_rank, y_rank)[0, 1])
    return corr if math.isfinite(corr) else None


def evaluate_feature_vector(
    *,
    candidate: CandidateFeature,
    values: Any,
    target: Any,
    min_obs: int = 8,
) -> EvaluationResult:
    x, y = finite_aligned(values, target)
    diagnostics: dict[str, Any] = {}
    if int(x.size) < int(min_obs):
        return EvaluationResult(
            candidate_hash=str(candidate.hash),
            feature_id=str(candidate.feature_id),
            t_stat=0.0,
            p_value=1.0,
            oos_ic=None,
            decision="degenerate",
            n_obs=int(x.size),
            diagnostics={"reason": "insufficient_observations", "min_obs": int(min_obs)},
        )
    if float(np.nanstd(x)) <= 1.0e-12 or float(np.nanstd(y)) <= 1.0e-12:
        return EvaluationResult(
            candidate_hash=str(candidate.hash),
            feature_id=str(candidate.feature_id),
            t_stat=0.0,
            p_value=1.0,
            oos_ic=None,
            decision="degenerate",
            n_obs=int(x.size),
            diagnostics={"reason": "constant_vector"},
        )

    threshold = harvey_liu_zhu_threshold_result(
        y=y,
        x=x,
        feature_id=str(candidate.feature_id),
    )
    ic = information_coefficient(x, y)
    diagnostics.update(
        {
            "threshold": float(threshold.threshold),
            "beta": float(threshold.beta),
            "standard_error": float(threshold.standard_error),
            "lags": int(threshold.lags),
        }
    )
    return EvaluationResult(
        candidate_hash=str(candidate.hash),
        feature_id=str(candidate.feature_id),
        t_stat=float(threshold.t_stat),
        p_value=float(threshold.p_value),
        oos_ic=(None if ic is None else float(ic)),
        decision="pending",
        n_obs=int(threshold.n_obs),
        diagnostics=diagnostics,
    )

