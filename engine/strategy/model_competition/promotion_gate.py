"""Promotion stat-gate orchestration for model competition."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple


GateEvaluateFn = Callable[..., Tuple[bool, Dict[str, Any]]]
CacheKeyFn = Callable[[Optional[Dict[str, Any]]], Tuple[str, str]]
CandidateVersionFn = Callable[[Optional[Dict[str, Any]]], str]
EnqueueFn = Callable[..., None]
SafeIntFn = Callable[[Any, int], int]
SafeFloatFn = Callable[[Any, float], float]


class PromotionStatGateEvaluator:
    """Evaluate and cache stat-gate results, including legacy audit actions."""

    def __init__(
        self,
        *,
        evaluate_gate: GateEvaluateFn,
        cache_key: CacheKeyFn,
        candidate_version: CandidateVersionFn,
        enqueue_legacy_hypothesis: Optional[EnqueueFn] = None,
        safe_int: SafeIntFn = lambda value, default=0: int(value or default),
        safe_float: SafeFloatFn = lambda value, default=0.0: float(value or default),
        con=None,
    ) -> None:
        self._evaluate_gate = evaluate_gate
        self._cache_key = cache_key
        self._candidate_version = candidate_version
        self._enqueue_legacy_hypothesis = enqueue_legacy_hypothesis
        self._safe_int = safe_int
        self._safe_float = safe_float
        self._con = con
        self._cache: Dict[Tuple[str, str], Tuple[bool, Dict[str, Any]]] = {}

    def evaluate(
        self,
        target_row: Optional[Dict[str, Any]],
        trial_count: int,
        *,
        candidate_returns: Optional[Dict[str, List[float]]],
        incumbent_row: Optional[Dict[str, Any]],
    ) -> Tuple[bool, Dict[str, Any]]:
        cache_key = self._cache_key(target_row)
        if cache_key[0] and cache_key in self._cache:
            cached_ok, cached_payload = self._cache[cache_key]
            payload = dict(cached_payload or {})
            payload["cache_hit"] = True
            self.enqueue_legacy_hypothesis_if_requested(target_row, payload)
            return bool(cached_ok), payload

        ok, payload = self._evaluate_gate(
            target_row,
            int(trial_count),
            models_returns=candidate_returns,
            champion_row=incumbent_row,
            con=self._con,
        )
        payload = dict(payload or {})
        if cache_key[0]:
            self._cache[cache_key] = (bool(ok), dict(payload))
        self.enqueue_legacy_hypothesis_if_requested(target_row, payload)
        return bool(ok), payload

    def enqueue_legacy_hypothesis_if_requested(
        self,
        target_row: Optional[Dict[str, Any]],
        payload: Optional[Dict[str, Any]],
    ) -> None:
        if not self._enqueue_legacy_hypothesis:
            return
        diagnostics = dict(payload or {})
        if not bool(diagnostics.get("record_legacy_hypothesis")):
            return
        target = dict(target_row or {})
        self._enqueue_legacy_hypothesis(
            "record_hypothesis_result",
            model_name=str(target.get("model_name") or ""),
            candidate_version=self._candidate_version(target),
            n_observations=self._safe_int(diagnostics.get("n_observations"), 0),
            t_statistic=self._safe_float(diagnostics.get("t_statistic"), 0.0),
            deflated_sharpe=self._safe_float(diagnostics.get("deflated_sharpe"), 0.0),
            threshold_t=self._safe_float(diagnostics.get("threshold_t"), 0.0),
            n_competing_trials=self._safe_int(diagnostics.get("n_competing_trials"), 0),
            passed=bool(diagnostics.get("passed")),
            diagnostics=dict(diagnostics),
        )
