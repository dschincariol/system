"""Optional DoWhy integration for curated causal DAGs."""

from __future__ import annotations
import logging

import importlib
import math
from dataclasses import dataclass, field
from typing import Any

from engine.causal.dag import CausalDAG


@dataclass(frozen=True)
class DoWhyResult:
    effect: float | None = None
    effect_se: float | None = None
    p_value: float | None = None
    decision: str = "skipped"
    error: str | None = None
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "effect": self.effect,
            "effect_se": self.effect_se,
            "p_value": self.p_value,
            "decision": self.decision,
            "error": self.error,
            "diagnostics": dict(self.diagnostics or {}),
        }


def _load_causal_model():
    module = importlib.import_module("dowhy")
    return getattr(module, "CausalModel")


def _to_dataframe(data: Any):
    try:
        import pandas as pd
    except Exception as exc:
        raise ModuleNotFoundError("pandas is required for DoWhy execution") from exc
    if hasattr(data, "copy") and hasattr(data, "columns"):
        return data.copy()
    return pd.DataFrame(data)


def _finite(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _extract_standard_error(estimate: Any) -> float | None:
    for name in ("get_standard_error", "get_std_error"):
        fn = getattr(estimate, name, None)
        if callable(fn):
            try:
                value = fn()
                if isinstance(value, (list, tuple)) and value:
                    value = value[0]
                return _finite(value)
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    stderr = getattr(estimate, "stderr", None)
    if stderr is not None:
        return _finite(stderr)
    return None


def _extract_significance_p(estimate: Any) -> float | None:
    fn = getattr(estimate, "test_stat_significance", None)
    if callable(fn):
        try:
            value = fn()
            if isinstance(value, dict):
                for key in ("p_value", "p", "p-value"):
                    if key in value:
                        return _finite(value[key])
            return _finite(value)
        except Exception:
            logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    return None


def _extract_refuter_p(refuter: Any) -> float | None:
    for attr in ("refutation_result", "p_value", "pvalue", "p"):
        if hasattr(refuter, attr):
            value = getattr(refuter, attr)
            if isinstance(value, dict):
                for key in ("p_value", "p", "p-value"):
                    if key in value:
                        p_value = _finite(value[key])
                        if p_value is not None:
                            return p_value
            p_value = _finite(value)
            if p_value is not None:
                return p_value
    return None


def run_dowhy(data: Any, dag: CausalDAG) -> DoWhyResult:
    """Run backdoor identification and linear effect estimation for ``dag``.

    ``dowhy`` is intentionally imported inside this function. Missing optional
    dependencies are recorded as a skipped diagnostic instead of escaping as an
    ImportError.
    """

    try:
        causal_model = _load_causal_model()
    except (ImportError, ModuleNotFoundError) as exc:
        return DoWhyResult(decision="skipped_no_dependency", error=str(exc))

    try:
        frame = _to_dataframe(data)
    except (ImportError, ModuleNotFoundError) as exc:
        return DoWhyResult(decision="skipped_no_dependency", error=str(exc))
    except Exception as exc:
        return DoWhyResult(decision="failed", error=str(exc))

    required = {dag.treatment, dag.outcome, *dag.confounders}
    missing = sorted(required.difference(set(str(col) for col in frame.columns)))
    if missing:
        return DoWhyResult(decision="skipped_missing_columns", diagnostics={"missing_columns": missing})

    try:
        model = causal_model(
            data=frame,
            treatment=dag.treatment,
            outcome=dag.outcome,
            graph=dag.to_dot(),
            common_causes=list(dag.confounders),
        )
        estimand = model.identify_effect(proceed_when_unidentifiable=True)
        estimate = model.estimate_effect(
            estimand,
            method_name="backdoor.linear_regression",
            test_significance=True,
        )
        effect = _finite(getattr(estimate, "value", None))
        effect_se = _extract_standard_error(estimate)
        p_value = _extract_significance_p(estimate)
        refuter_p = None
        try:
            refuter = model.refute_estimate(
                estimand,
                estimate,
                method_name="bootstrap_refuter",
                num_simulations=100,
            )
            refuter_p = _extract_refuter_p(refuter)
        except Exception as exc:
            refuter_p = None
            refuter_error = str(exc)
        else:
            refuter_error = None
        return DoWhyResult(
            effect=effect,
            effect_se=effect_se,
            p_value=refuter_p if refuter_p is not None else p_value,
            decision="estimated",
            diagnostics={
                "significance_p": p_value,
                "refuter_p": refuter_p,
                **({"refuter_error": refuter_error} if refuter_error else {}),
            },
        )
    except Exception as exc:
        return DoWhyResult(decision="failed", error=str(exc))
