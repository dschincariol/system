"""PySR-backed symbolic feature discovery."""

from __future__ import annotations

import ast
import importlib
import math
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from engine.runtime.failure_diagnostics import log_failure
from engine.strategy.discovery.base import (
    CandidateFeature,
    EvaluationResult,
    content_hash,
    evaluate_feature_vector,
    information_coefficient,
    target_series,
)
from engine.runtime.logging import get_logger

_EPS = 1.0e-12
_BINARY_OPERATORS = ("+", "-", "*", "/")
_UNARY_OPERATORS = ("log", "abs", "sqrt")
LOG = get_logger("engine.strategy.discovery.pysr_discoverer")


def _warn_nonfatal(code: str, error: BaseException | None = None, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error or code),
        error=error,
        level=30,
        component="engine.strategy.discovery.pysr_discoverer",
        extra=extra or None,
        persist=False,
    )


class PySRDiscoverer:
    """Bounded symbolic regression discoverer.

    PySR is used when installed. A deterministic symbolic fallback keeps local
    tests and lightweight development environments useful without weakening the
    production path.
    """

    source = "pysr"

    def __init__(
        self,
        *,
        target_column: str = "target",
        niterations: int = 100,
        timeout_seconds: int = 30,
        max_complexity: int = 12,
        top_k: int = 10,
        random_state: int = 42,
        primitive_columns: Sequence[str] | None = None,
    ) -> None:
        self.target_column = str(target_column)
        self.niterations = max(1, min(100, int(niterations)))
        self.timeout_seconds = max(1, min(600, int(timeout_seconds)))
        self.max_complexity = max(1, min(12, int(max_complexity)))
        self.top_k = max(1, int(top_k))
        self.random_state = int(random_state)
        self.primitive_columns = tuple(str(col).strip() for col in list(primitive_columns or []) if str(col).strip())

    def propose(self, symbol: str, train_df: pd.DataFrame) -> list[CandidateFeature]:
        frame = pd.DataFrame(train_df).copy()
        if self.target_column not in set(frame.columns):
            return []
        feature_columns = _feature_columns(
            frame,
            target_column=self.target_column,
            allowed=self.primitive_columns,
        )
        if not feature_columns:
            return []

        y = pd.to_numeric(frame[self.target_column], errors="coerce").astype(float)
        x = frame.loc[:, feature_columns].apply(pd.to_numeric, errors="coerce").astype(float)
        valid = np.isfinite(y.to_numpy(dtype=float))
        for column in feature_columns:
            valid &= np.isfinite(x[column].to_numpy(dtype=float))
        x = x.loc[valid].reset_index(drop=True)
        y = y.loc[valid].reset_index(drop=True)
        if len(x.index) < 8:
            return []

        try:
            return self._propose_with_pysr(str(symbol), x, y, feature_columns)
        except Exception as exc:
            return self._fallback_after_pysr_failure(
                str(symbol),
                x,
                y,
                feature_columns,
                reason="pysr_exception",
                error=exc,
            )

    def evaluate(
        self,
        candidate: CandidateFeature,
        test_df: pd.DataFrame,
        target: str | Sequence[float] | pd.Series,
    ) -> EvaluationResult:
        params = dict(candidate.params or {})
        feature_map = dict(params.get("feature_map") or {})
        if not feature_map:
            return _degenerate(candidate, "feature_map_missing")
        try:
            values = evaluate_pysr_expression(
                str(candidate.expression),
                pd.DataFrame(test_df),
                feature_map=feature_map,
            )
        except Exception as exc:
            return _degenerate(candidate, f"expression_eval_failed:{type(exc).__name__}")
        y = target_series(pd.DataFrame(test_df), target)
        return evaluate_feature_vector(candidate=candidate, values=values, target=y)

    def _propose_with_pysr(
        self,
        symbol: str,
        x: pd.DataFrame,
        y: pd.Series,
        feature_columns: Sequence[str],
    ) -> list[CandidateFeature]:
        pysr_mod = importlib.import_module("pysr")
        model_cls = getattr(pysr_mod, "PySRRegressor")
        safe_names = [f"x{i}" for i in range(len(feature_columns))]
        safe_x = x.copy()
        safe_x.columns = safe_names

        model = model_cls(
            niterations=int(self.niterations),
            timeout_in_seconds=int(self.timeout_seconds),
            maxsize=int(self.max_complexity),
            binary_operators=list(_BINARY_OPERATORS),
            unary_operators=list(_UNARY_OPERATORS),
            model_selection="score",
            random_state=int(self.random_state),
            verbosity=0,
            progress=False,
        )
        model.fit(safe_x, y.to_numpy(dtype=float))
        equations = pd.DataFrame(getattr(model, "equations_", pd.DataFrame())).copy()
        if equations.empty or "equation" not in set(equations.columns):
            return self._fallback_after_pysr_failure(
                symbol,
                x,
                y,
                feature_columns,
                reason="pysr_no_equations",
            )
        score_col = "score" if "score" in set(equations.columns) else None
        if score_col:
            equations = equations.sort_values(score_col, ascending=False)

        feature_map = {safe: str(original) for safe, original in zip(safe_names, feature_columns)}
        out: list[CandidateFeature] = []
        seen: set[str] = set()
        for _idx, row in equations.iterrows():
            expression = _simplify_expression(str(row.get("equation") or ""))
            complexity = int(row.get("complexity") or expression_complexity(expression))
            if not expression or complexity > int(self.max_complexity):
                continue
            if not _expression_names(expression).issubset(set(safe_names)):
                continue
            digest = content_hash({"source": self.source, "symbol": str(symbol).upper(), "expression": expression})
            if digest in seen:
                continue
            seen.add(str(digest))
            out.append(
                CandidateFeature(
                    source=self.source,
                    symbol=str(symbol),
                    expression=str(expression),
                    params={
                        "feature_map": dict(feature_map),
                        "source_feature_ids": [feature_map[name] for name in sorted(_expression_names(expression))],
                        "complexity": int(complexity),
                        "score": _safe_float(row.get(score_col)) if score_col else None,
                        "niterations": int(self.niterations),
                        "max_complexity": int(self.max_complexity),
                        "operators": {"binary": list(_BINARY_OPERATORS), "unary": list(_UNARY_OPERATORS)},
                        "engine": "pysr",
                    },
                    hash=str(digest),
                    feature_id=f"discovered.pysr.{str(digest)[:16]}",
                    score=_safe_float(row.get(score_col)) if score_col else None,
                )
            )
            if len(out) >= int(self.top_k):
                break
        if out:
            return out
        return self._fallback_after_pysr_failure(
            symbol,
            x,
            y,
            feature_columns,
            reason="pysr_no_valid_candidates",
        )

    def _fallback_after_pysr_failure(
        self,
        symbol: str,
        x: pd.DataFrame,
        y: pd.Series,
        feature_columns: Sequence[str],
        *,
        reason: str,
        error: BaseException | None = None,
    ) -> list[CandidateFeature]:
        error_type = type(error).__name__ if error is not None else ""
        _warn_nonfatal(
            "PYSR_FALLBACK_USED",
            error,
            symbol=str(symbol),
            reason=str(reason),
            error_type=error_type,
        )
        try:
            from engine.runtime.metrics import emit_counter

            emit_counter(
                "pysr_fallback_used",
                1,
                component="engine.strategy.discovery.pysr_discoverer",
                symbol=str(symbol),
                extra_tags={
                    "reason": str(reason),
                    "error_type": error_type,
                },
            )
        except Exception as metrics_exc:
            metric_error_type = type(metrics_exc).__name__
            _warn_nonfatal(
                "PYSR_FALLBACK_METRIC_EMIT_FAILED",
                metrics_exc,
                symbol=str(symbol),
                error_type=metric_error_type,
            )
        try:
            return self._propose_with_fallback(str(symbol), x, y, feature_columns)
        except Exception as fallback_exc:
            _warn_nonfatal(
                "PYSR_FALLBACK_FAILED",
                fallback_exc,
                symbol=str(symbol),
                reason=str(reason),
                error_type=type(fallback_exc).__name__,
            )
            return []

    def _propose_with_fallback(
        self,
        symbol: str,
        x: pd.DataFrame,
        y: pd.Series,
        feature_columns: Sequence[str],
    ) -> list[CandidateFeature]:
        safe_names = [f"x{i}" for i in range(len(feature_columns))]
        feature_map = {safe: str(original) for safe, original in zip(safe_names, feature_columns)}
        safe_x = x.copy()
        safe_x.columns = safe_names

        ranked = []
        for name in safe_names:
            score = abs(float(information_coefficient(safe_x[name], y) or 0.0))
            ranked.append((name, score))
        ranked.sort(key=lambda item: (item[1], item[0]), reverse=True)
        top_names = [name for name, _score in ranked[: min(6, len(ranked))]]

        expressions = _fallback_expressions(top_names)
        scored: list[tuple[float, str, int]] = []
        for expression in expressions:
            complexity = expression_complexity(expression)
            if complexity > int(self.max_complexity):
                continue
            try:
                values = _eval_expression_array(expression, {name: safe_x[name].to_numpy(dtype=float) for name in safe_names})
            except Exception:
                continue
            score = abs(float(information_coefficient(values, y) or 0.0))
            if score <= 0.0:
                continue
            scored.append((float(score), str(expression), int(complexity)))
        scored.sort(key=lambda item: (item[0], item[1]), reverse=True)

        out: list[CandidateFeature] = []
        seen: set[str] = set()
        for score, expression, complexity in scored:
            simplified = _simplify_expression(expression)
            digest = content_hash({"source": self.source, "symbol": str(symbol).upper(), "expression": simplified})
            if digest in seen:
                continue
            seen.add(str(digest))
            names = sorted(_expression_names(simplified))
            out.append(
                CandidateFeature(
                    source=self.source,
                    symbol=str(symbol),
                    expression=str(simplified),
                    params={
                        "feature_map": dict(feature_map),
                        "source_feature_ids": [feature_map[name] for name in names],
                        "complexity": int(complexity),
                        "score": float(score),
                        "niterations": int(self.niterations),
                        "max_complexity": int(self.max_complexity),
                        "operators": {"binary": list(_BINARY_OPERATORS), "unary": list(_UNARY_OPERATORS)},
                        "engine": "fallback",
                    },
                    hash=str(digest),
                    feature_id=f"discovered.pysr.{str(digest)[:16]}",
                    score=float(score),
                )
            )
            if len(out) >= int(self.top_k):
                break
        return out


def evaluate_pysr_expression(
    expression: str,
    df: pd.DataFrame,
    *,
    feature_map: Mapping[str, str],
) -> np.ndarray:
    frame = pd.DataFrame(df)
    env: dict[str, np.ndarray] = {}
    for safe_name, original in dict(feature_map or {}).items():
        column = str(original)
        if column not in set(frame.columns):
            raise ValueError(f"source_feature_missing:{column}")
        env[str(safe_name)] = pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype=float)
    return _eval_expression_array(str(expression), env)


def expression_complexity(expression: str) -> int:
    try:
        tree = ast.parse(str(expression), mode="eval")
    except Exception:
        return 10**9
    count = 0
    for node in ast.walk(tree):
        if isinstance(node, (ast.BinOp, ast.UnaryOp, ast.Call, ast.Name)):
            count += 1
    return int(count)


def _feature_columns(
    df: pd.DataFrame,
    *,
    target_column: str,
    allowed: Sequence[str] | None = None,
) -> list[str]:
    excluded = {
        str(target_column).lower(),
        "ts",
        "ts_ms",
        "timestamp",
        "date",
        "datetime",
        "symbol",
    }
    allowed_set = {str(col) for col in list(allowed or []) if str(col)}
    out: list[str] = []
    for column in df.columns:
        name = str(column)
        if name.lower() in excluded:
            continue
        if allowed_set and name not in allowed_set:
            continue
        values = pd.to_numeric(df[column], errors="coerce")
        finite = values[np.isfinite(values)]
        if int(finite.size) >= 8 and float(np.nanstd(finite.to_numpy(dtype=float))) > 1.0e-12:
            out.append(name)
    return out


def _fallback_expressions(names: Sequence[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    def add(expr: str) -> None:
        if expr not in seen:
            seen.add(str(expr))
            out.append(str(expr))

    for name in names:
        add(str(name))
        add(f"abs({name})")
        add(f"log(abs({name}))")
        add(f"sqrt(abs({name}))")
    for left_idx, left in enumerate(names):
        for right in names[left_idx + 1 :]:
            add(f"({left}+{right})")
            add(f"({left}-{right})")
            add(f"({right}-{left})")
            add(f"({left}*{right})")
            add(f"({left}/{right})")
            add(f"({right}/{left})")
    return out


def _eval_expression_array(expression: str, env: Mapping[str, np.ndarray]) -> np.ndarray:
    tree = ast.parse(str(expression), mode="eval")
    return _eval_node(tree.body, dict(env))


def _eval_node(node: ast.AST, env: Mapping[str, np.ndarray]) -> np.ndarray:
    if isinstance(node, ast.Name):
        if node.id not in env:
            raise ValueError(f"unknown_variable:{node.id}")
        return np.asarray(env[node.id], dtype=float)
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        length = len(next(iter(env.values()))) if env else 1
        return np.full(length, float(node.value), dtype=float)
    if isinstance(node, ast.UnaryOp):
        value = _eval_node(node.operand, env)
        if isinstance(node.op, ast.USub):
            return -value
        if isinstance(node.op, ast.UAdd):
            return value
    if isinstance(node, ast.BinOp):
        left = _eval_node(node.left, env)
        right = _eval_node(node.right, env)
        if isinstance(node.op, ast.Add):
            return left + right
        if isinstance(node.op, ast.Sub):
            return left - right
        if isinstance(node.op, ast.Mult):
            return left * right
        if isinstance(node.op, ast.Div):
            denom = np.where(np.abs(right) < _EPS, np.nan, right)
            return left / denom
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
        if len(node.args) != 1:
            raise ValueError("invalid_unary_arity")
        value = _eval_node(node.args[0], env)
        name = str(node.func.id)
        if name == "abs":
            return np.abs(value)
        if name == "log":
            return np.log(np.abs(value) + _EPS)
        if name == "sqrt":
            return np.sqrt(np.abs(value))
    raise ValueError(f"unsupported_expression_node:{type(node).__name__}")


def _expression_names(expression: str) -> set[str]:
    try:
        tree = ast.parse(str(expression), mode="eval")
    except Exception:
        return set()
    names: set[str] = set()

    class _NameVisitor(ast.NodeVisitor):
        def visit_Call(self, node: ast.Call) -> Any:
            for arg in list(node.args or []):
                self.visit(arg)

        def visit_Name(self, node: ast.Name) -> Any:
            names.add(str(node.id))

    _NameVisitor().visit(tree.body)
    return names


def _simplify_expression(expression: str) -> str:
    text = str(expression or "").strip()
    if not text:
        return ""
    text = text.replace("^", "**")
    try:
        sympy = importlib.import_module("sympy")
        symbols = {name: sympy.Symbol(name) for name in _expression_names(text)}
        parsed = sympy.sympify(text, locals={**symbols, "log": sympy.log, "sqrt": sympy.sqrt, "abs": sympy.Abs})
        simplified = str(sympy.simplify(parsed))
        return simplified.replace("Abs", "abs")
    except Exception:
        return text.replace(" ", "")


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return float(out) if math.isfinite(out) else None


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
