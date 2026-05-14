"""tsfresh-backed statistical feature discovery."""

from __future__ import annotations

import importlib
from typing import Any, Sequence

import numpy as np
import pandas as pd

from engine.strategy.discovery.base import (
    CandidateFeature,
    EvaluationResult,
    candidate_hash,
    evaluate_feature_vector,
    target_series,
)


class TsfreshDiscoverer:
    """Extract rolling tsfresh candidates without using tsfresh's selector."""

    source = "tsfresh"

    def __init__(
        self,
        *,
        window: int = 180,
        max_candidates: int | None = None,
        min_periods: int | None = None,
        n_jobs: int = 0,
        value_columns: Sequence[str] | None = None,
    ) -> None:
        self.window = max(2, int(window))
        self.max_candidates = None if max_candidates is None else max(1, int(max_candidates))
        self.min_periods = max(2, int(min_periods or window))
        self.n_jobs = int(n_jobs)
        self.value_columns = tuple(str(col).strip() for col in list(value_columns or []) if str(col).strip())

    def propose(self, symbol: str, train_df: pd.DataFrame) -> list[CandidateFeature]:
        value_columns = _numeric_value_columns(train_df, requested=self.value_columns)
        proposals: list[CandidateFeature] = []
        seen: set[str] = set()
        for value_column in value_columns:
            matrix = self._rolling_feature_matrix(train_df, value_column=value_column)
            if matrix.empty:
                continue
            for feature_column in sorted(str(col) for col in matrix.columns):
                values = pd.to_numeric(matrix[feature_column], errors="coerce")
                finite = values[np.isfinite(values)]
                if int(finite.size) < 2:
                    continue
                if float(np.nanstd(finite.to_numpy(dtype=float))) <= 1.0e-12:
                    continue
                candidate = self._candidate_from_feature_column(
                    symbol=str(symbol),
                    value_column=str(value_column),
                    feature_column=str(feature_column),
                )
                if candidate.hash in seen:
                    continue
                seen.add(str(candidate.hash))
                proposals.append(candidate)
                if self.max_candidates is not None and len(proposals) >= int(self.max_candidates):
                    return proposals
        return proposals

    def evaluate(
        self,
        candidate: CandidateFeature,
        test_df: pd.DataFrame,
        target: str | Sequence[float] | pd.Series,
    ) -> EvaluationResult:
        params = dict(candidate.params or {})
        value_column = str(params.get("value_column") or "")
        feature_column = str(params.get("feature_column") or "")
        if not value_column or not feature_column:
            return _degenerate(candidate, "candidate_params_missing")
        matrix = self._rolling_feature_matrix(test_df, value_column=value_column)
        if matrix.empty or feature_column not in set(str(col) for col in matrix.columns):
            return _degenerate(candidate, "candidate_feature_missing")

        values = pd.to_numeric(matrix[feature_column], errors="coerce")
        y_all = target_series(test_df, target)
        positions = [int(pos) for pos in list(matrix.index)]
        if positions and max(positions) < int(len(y_all.index)):
            y = y_all.iloc[positions].reset_index(drop=True)
        else:
            y = y_all.iloc[-len(values) :].reset_index(drop=True)
        return evaluate_feature_vector(candidate=candidate, values=values.reset_index(drop=True), target=y)

    def _candidate_from_feature_column(
        self,
        *,
        symbol: str,
        value_column: str,
        feature_column: str,
    ) -> CandidateFeature:
        params = {
            "value_column": str(value_column),
            "feature_column": str(feature_column),
            "window": int(self.window),
            "parameter_set": "comprehensive",
        }
        expression = f"tsfresh({value_column}__{feature_column},window={int(self.window)})"
        digest = candidate_hash(
            source=self.source,
            symbol=str(symbol),
            expression=str(expression),
            params=params,
        )
        return CandidateFeature(
            source=self.source,
            symbol=str(symbol),
            expression=str(expression),
            params=params,
            hash=str(digest),
            feature_id=f"discovered.tsfresh.{str(digest)[:16]}",
        )

    def _rolling_feature_matrix(self, df: pd.DataFrame, *, value_column: str) -> pd.DataFrame:
        tsfresh, fc_params = _load_tsfresh()
        work = pd.DataFrame(df).copy()
        if value_column not in set(work.columns):
            return pd.DataFrame()
        values = pd.to_numeric(work[value_column], errors="coerce")
        valid = values[np.isfinite(values)]
        if int(valid.size) < int(self.min_periods):
            return pd.DataFrame()

        normalized = pd.DataFrame(
            {
                "position": np.arange(len(work), dtype=int),
                "value": values.to_numpy(dtype=float),
            }
        ).dropna(subset=["value"])
        if len(normalized.index) < int(self.min_periods):
            return pd.DataFrame()

        frames: list[pd.DataFrame] = []
        window = int(self.window)
        for end_idx in range(window - 1, len(normalized.index)):
            chunk = normalized.iloc[end_idx - window + 1 : end_idx + 1]
            if len(chunk.index) < int(self.min_periods):
                continue
            anchor = int(chunk["position"].iloc[-1])
            frames.append(
                pd.DataFrame(
                    {
                        "id": anchor,
                        "sort": np.arange(len(chunk.index), dtype=int),
                        "value": chunk["value"].to_numpy(dtype=float),
                    }
                )
            )
        if not frames:
            return pd.DataFrame()

        long_df = pd.concat(frames, ignore_index=True)
        extracted = tsfresh.extract_features(
            long_df,
            column_id="id",
            column_sort="sort",
            column_value="value",
            default_fc_parameters=fc_params,
            disable_progressbar=True,
            n_jobs=int(self.n_jobs),
        )
        if extracted is None or getattr(extracted, "empty", True):
            return pd.DataFrame()
        out = pd.DataFrame(extracted).copy()
        out.index = [int(idx) for idx in list(out.index)]
        out = out.replace([np.inf, -np.inf], np.nan)
        return out


def _load_tsfresh():
    tsfresh = importlib.import_module("tsfresh")
    params_mod = importlib.import_module("tsfresh.feature_extraction")
    return tsfresh, params_mod.ComprehensiveFCParameters()


def _numeric_value_columns(df: pd.DataFrame, *, requested: Sequence[str] | None = None) -> list[str]:
    frame = pd.DataFrame(df)
    excluded = {
        "target",
        "forward_return",
        "forward_returns",
        "future_return",
        "label",
        "y",
        "ts",
        "ts_ms",
        "timestamp",
        "date",
        "datetime",
    }
    preferred = ["close", "price", "px", "last", "value", "open", "high", "low", "volume"]
    numeric = []
    for column in frame.columns:
        name = str(column)
        if name.lower() in excluded:
            continue
        series = pd.to_numeric(frame[column], errors="coerce")
        if int(series[np.isfinite(series)].size) >= 2:
            numeric.append(name)
    if requested:
        selected = [str(name) for name in requested if str(name) in set(numeric)]
        if selected:
            return selected
    ordered = [name for name in preferred if name in numeric]
    ordered.extend(name for name in numeric if name not in set(ordered))
    return ordered


def _degenerate(candidate: CandidateFeature, reason: str) -> EvaluationResult:
    return EvaluationResult(
        candidate_hash=str(candidate.hash),
        feature_id=str(candidate.feature_id),
        t_stat=0.0,
        p_value=1.0,
        decision="degenerate",
        n_obs=0,
        diagnostics={"reason": str(reason)},
    )
