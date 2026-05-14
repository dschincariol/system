"""Ridge meta-learner for stacked model predictions."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.linear_model import Ridge


class RidgeStackEnsemble:
    """Linear stacker trained only on out-of-sample model predictions."""

    def __init__(
        self,
        *,
        alpha: float = 1.0,
        nonneg: bool = True,
        prior_weights: Mapping[str, float] | None = None,
        lambda_prior: float = 0.0,
    ) -> None:
        self.alpha = float(alpha)
        self.nonneg = bool(nonneg)
        self.prior_weights = {str(key): float(value) for key, value in dict(prior_weights or {}).items()}
        self.lambda_prior = float(lambda_prior)
        self.prior_weights_: dict[str, float] = {}
        self.families_: list[str] = []
        self.weights_: dict[str, float] = {}
        self.intercept_: float = 0.0
        self.n_train_obs_: int = 0
        self.val_metric_: float | None = None

    @staticmethod
    def _records(data: Any) -> list[dict[str, Any]]:
        if data is None:
            return []
        if hasattr(data, "to_dict"):
            try:
                records = data.to_dict("records")
                return [dict(row) for row in records]
            except TypeError:
                pass
        if isinstance(data, Mapping):
            return [dict(data)]
        return [dict(row) if isinstance(row, Mapping) else dict(enumerate(row)) for row in data]

    @staticmethod
    def _is_long_format(records: list[dict[str, Any]]) -> bool:
        if not records:
            return False
        keys = set(records[0].keys())
        return {"family", "prediction"}.issubset(keys)

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _pivot_long(
        cls,
        records: list[dict[str, Any]],
        *,
        symbol: str | None = None,
        horizon: int | None = None,
    ) -> tuple[np.ndarray, np.ndarray, list[str], list[int]]:
        preds_by_ts: dict[int, dict[str, float]] = defaultdict(dict)
        target_by_ts: dict[int, float] = {}
        families_seen: list[str] = []
        for row in records:
            if symbol is not None and str(row.get("symbol")) != str(symbol):
                continue
            row_horizon = row.get("horizon", row.get("horizon_s"))
            if horizon is not None and int(row_horizon) != int(horizon):
                continue
            target = cls._coerce_float(row.get("target"))
            pred = cls._coerce_float(row.get("prediction", row.get("pred")))
            if target is None or pred is None:
                continue
            family = str(row.get("family") or "").strip()
            if not family:
                continue
            ts = int(row.get("ts", row.get("ts_ms")))
            preds_by_ts[ts][family] = float(pred)
            target_by_ts.setdefault(ts, float(target))
            if family not in families_seen:
                families_seen.append(family)
        families = sorted(families_seen)
        complete_ts = [
            ts
            for ts in sorted(preds_by_ts)
            if ts in target_by_ts and all(family in preds_by_ts[ts] for family in families)
        ]
        if not complete_ts or not families:
            raise ValueError("no complete OOS prediction rows are available for ridge stacking")
        X = np.asarray([[preds_by_ts[ts][family] for family in families] for ts in complete_ts], dtype=float)
        y = np.asarray([target_by_ts[ts] for ts in complete_ts], dtype=float)
        return X, y, families, complete_ts

    @classmethod
    def _wide_matrix(
        cls,
        data: Any,
        families: Sequence[str],
        *,
        require_target: bool = False,
    ) -> tuple[np.ndarray, np.ndarray | None]:
        records = cls._records(data)
        if cls._is_long_format(records):
            X, y, pivot_families, _ = cls._pivot_long(records)
            family_index = {family: idx for idx, family in enumerate(pivot_families)}
            X = np.asarray([[row[family_index[family]] for family in families] for row in X], dtype=float)
            return X, y if require_target else None
        rows: list[list[float]] = []
        targets: list[float] = []
        for record in records:
            values: list[float] = []
            ok = True
            for family in families:
                pred = cls._coerce_float(record.get(family))
                if pred is None:
                    ok = False
                    break
                values.append(float(pred))
            if not ok:
                continue
            rows.append(values)
            target = cls._coerce_float(record.get("target"))
            if target is not None:
                targets.append(float(target))
        if not rows:
            raise ValueError("no complete rows are available for prediction")
        y_arr = np.asarray(targets, dtype=float) if require_target and len(targets) == len(rows) else None
        return np.asarray(rows, dtype=float), y_arr

    def fit(
        self,
        data: Any,
        *,
        symbol: str | None = None,
        horizon: int | None = None,
        alpha: float | None = None,
        nonneg: bool | None = None,
        prior_weights: Mapping[str, float] | None = None,
        lambda_prior: float | None = None,
    ) -> "RidgeStackEnsemble":
        alpha_value = self.alpha if alpha is None else float(alpha)
        nonneg_value = self.nonneg if nonneg is None else bool(nonneg)
        records = self._records(data)
        if not records:
            raise ValueError("ridge stack fit requires OOS prediction rows")
        if self._is_long_format(records):
            X, y, families, _ = self._pivot_long(records, symbol=symbol, horizon=horizon)
        else:
            target_rows = [row for row in records if self._coerce_float(row.get("target")) is not None]
            if not target_rows:
                raise ValueError("ridge stack fit requires target values")
            families = sorted(
                str(key)
                for key in target_rows[0].keys()
                if key not in {"symbol", "horizon", "horizon_s", "ts", "ts_ms", "target"}
            )
            X, y = self._wide_matrix(target_rows, families, require_target=True)
            if y is None:
                raise ValueError("ridge stack fit requires target values")
        if X.shape[0] == 0 or X.shape[1] == 0:
            raise ValueError("ridge stack fit requires at least one complete observation and one family")

        prior_source = self.prior_weights if prior_weights is None else dict(prior_weights or {})
        prior_map = {str(key): float(value) for key, value in dict(prior_source or {}).items()}
        lambda_value = self.lambda_prior if lambda_prior is None else float(lambda_prior)
        use_prior = bool(prior_map) and float(lambda_value) > 0.0
        prior_vector = np.asarray([prior_map.get(family, 0.0) for family in families], dtype=float)

        if nonneg_value:
            from scipy.optimize import nnls

            x_mean = X.mean(axis=0)
            y_mean = float(y.mean())
            Xc = X - x_mean
            yc = y - y_mean
            if alpha_value > 0:
                penalty = np.sqrt(float(alpha_value)) * np.eye(X.shape[1], dtype=float)
                X_aug = np.vstack([Xc, penalty])
                y_aug = np.concatenate([yc, np.zeros(X.shape[1], dtype=float)])
            else:
                X_aug = Xc
                y_aug = yc
            if use_prior:
                prior_penalty = np.sqrt(float(lambda_value)) * np.eye(X.shape[1], dtype=float)
                X_aug = np.vstack([X_aug, prior_penalty])
                y_aug = np.concatenate([y_aug, np.sqrt(float(lambda_value)) * prior_vector])
            coef, _ = nnls(X_aug, y_aug)
            intercept = float(y_mean - float(np.dot(x_mean, coef)))
        elif use_prior:
            x_mean = X.mean(axis=0)
            y_mean = float(y.mean())
            Xc = X - x_mean
            yc = y - y_mean
            penalty = (float(alpha_value) + float(lambda_value)) * np.eye(X.shape[1], dtype=float)
            lhs = Xc.T @ Xc + penalty
            rhs = Xc.T @ yc + float(lambda_value) * prior_vector
            try:
                coef = np.linalg.solve(lhs, rhs)
            except np.linalg.LinAlgError:
                coef = np.linalg.pinv(lhs) @ rhs
            intercept = float(y_mean - float(np.dot(x_mean, coef)))
        else:
            model = Ridge(alpha=float(alpha_value), fit_intercept=True)
            model.fit(X, y)
            coef = np.asarray(model.coef_, dtype=float)
            intercept = float(model.intercept_)

        preds = intercept + X.dot(coef)
        ss_res = float(np.sum((y - preds) ** 2))
        ss_tot = float(np.sum((y - float(y.mean())) ** 2))
        self.val_metric_ = float(1.0 - ss_res / ss_tot) if ss_tot > 0 else 0.0
        self.alpha = float(alpha_value)
        self.nonneg = bool(nonneg_value)
        self.prior_weights = dict(prior_map)
        self.lambda_prior = float(lambda_value)
        self.prior_weights_ = (
            {family: float(prior_vector[idx]) for idx, family in enumerate(families)}
            if use_prior
            else {}
        )
        self.families_ = list(families)
        self.weights_ = {family: float(coef[idx]) for idx, family in enumerate(self.families_)}
        self.intercept_ = float(intercept)
        self.n_train_obs_ = int(X.shape[0])
        return self

    def predict(self, data: Any) -> np.ndarray:
        if not self.families_:
            raise ValueError("ridge stack ensemble is not fitted")
        if isinstance(data, Mapping) and all(family in data for family in self.families_):
            X = np.asarray([[float(data[family]) for family in self.families_]], dtype=float)
        else:
            X, _ = self._wide_matrix(data, self.families_)
        coef = np.asarray([self.weights_.get(family, 0.0) for family in self.families_], dtype=float)
        return np.asarray(float(self.intercept_) + X.dot(coef), dtype=float)

    def to_dict(self) -> dict[str, Any]:
        out = {
            "alpha": float(self.alpha),
            "nonneg": bool(self.nonneg),
            "families": list(self.families_),
            "weights": {str(key): float(value) for key, value in self.weights_.items()},
            "intercept": float(self.intercept_),
            "n_train_obs": int(self.n_train_obs_),
            "val_metric": (float(self.val_metric_) if self.val_metric_ is not None else None),
        }
        if self.prior_weights_:
            out["prior_weights"] = {str(key): float(value) for key, value in self.prior_weights_.items()}
            out["lambda_prior"] = float(self.lambda_prior)
        return out

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "RidgeStackEnsemble":
        model = cls(
            alpha=float(payload.get("alpha", 1.0)),
            nonneg=bool(payload.get("nonneg", True)),
            prior_weights=dict(payload.get("prior_weights") or {}),
            lambda_prior=float(payload.get("lambda_prior") or 0.0),
        )
        weights = dict(payload.get("weights") or {})
        families = list(payload.get("families") or sorted(weights.keys()))
        model.families_ = [str(family) for family in families]
        model.weights_ = {str(key): float(value) for key, value in weights.items()}
        model.prior_weights_ = {str(key): float(value) for key, value in dict(payload.get("prior_weights") or {}).items()}
        model.intercept_ = float(payload.get("intercept") or 0.0)
        model.n_train_obs_ = int(payload.get("n_train_obs") or 0)
        val_metric = payload.get("val_metric")
        model.val_metric_ = float(val_metric) if val_metric is not None else None
        return model

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_json(cls, payload: str) -> "RidgeStackEnsemble":
        return cls.from_dict(json.loads(payload))

    def save(self, path: str | Path) -> None:
        Path(path).write_text(self.to_json(), encoding="utf-8")

    @classmethod
    def load(cls, path: str | Path) -> "RidgeStackEnsemble":
        return cls.from_json(Path(path).read_text(encoding="utf-8"))
