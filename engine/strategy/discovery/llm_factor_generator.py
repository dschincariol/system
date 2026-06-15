"""LLM-assisted factor hypotheses for the existing discovery framework.

The language model is only a hypothesis generator.  It receives aggregate
diagnostics, returns expression text in the same bounded DSL used by PySR, and
never executes code or interacts with the order path.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd
import requests

from engine.backtest.cpcv import CombinatorialPurgedKFold
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.strategy.discovery.base import (
    CandidateFeature,
    EvaluationResult,
    content_hash,
    evaluate_feature_vector,
    information_coefficient,
    now_ms,
)
from engine.strategy.discovery.pysr_discoverer import (
    _expression_names,
    _simplify_expression,
    evaluate_pysr_expression,
    expression_complexity,
)
from engine.strategy.discovery.registry import (
    ACCEPTED_DECISION,
    FEATURE_STAGE_SHADOW,
    ensure_discovery_schema,
    list_evaluations,
    list_registered_features,
    record_candidate,
    record_evaluation,
    register_feature,
)
from engine.strategy.jobs.discover_features import (
    _resolve_feature_ids_for_discovery,
    _resolve_frames,
    _resolve_symbols,
)
from engine.strategy.statistics.multiple_testing import bh_fdr

LOG = get_logger("engine.strategy.discovery.llm_factor_generator")
_WARNED_NONFATAL_KEYS: set[str] = set()

SOURCE = "llm_factor"
DEFAULT_MODEL = "claude-sonnet-4-6"
DEFAULT_CANDIDATES = 20
DEFAULT_MAX_COMPLEXITY = 12
DEFAULT_MAX_TOKENS = 1200
DEFAULT_REDUNDANCY_MAX = 0.80
DEFAULT_Q_THRESHOLD = 0.10
DEFAULT_T_THRESHOLD = 3.0
DEFAULT_MIN_OBS = 24
STAT_TEST_DECISIONS = frozenset({ACCEPTED_DECISION, "fdr_failed", "tstat_failed", "degenerate", "leakage_failed"})


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.discovery.llm_factor_generator",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


@dataclass(frozen=True)
class ParsedLLMCandidate:
    expression: str
    hypothesis: str


class LLMFactorDiscoverer:
    """Propose bounded symbolic factors from an Anthropic-backed LLM."""

    source = SOURCE

    def __init__(
        self,
        *,
        model: str | None = None,
        max_candidates: int | None = None,
        max_complexity: int = DEFAULT_MAX_COMPLEXITY,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        primitive_columns: Sequence[str] | None = None,
        llm_client: Callable[..., str] | None = None,
        api_key: str | None = None,
        prior_experiments: Sequence[Mapping[str, Any]] | None = None,
    ) -> None:
        self.model = str(model or os.environ.get("LLM_FACTOR_MODEL") or DEFAULT_MODEL).strip() or DEFAULT_MODEL
        self.max_candidates = _bounded_int(
            max_candidates if max_candidates is not None else os.environ.get("LLM_FACTOR_CANDIDATES"),
            DEFAULT_CANDIDATES,
            low=1,
            high=50,
        )
        self.max_complexity = _bounded_int(max_complexity, DEFAULT_MAX_COMPLEXITY, low=1, high=12)
        self.max_tokens = _bounded_int(max_tokens, DEFAULT_MAX_TOKENS, low=128, high=4096)
        self.primitive_columns = tuple(str(col).strip() for col in list(primitive_columns or []) if str(col).strip())
        self.llm_client = llm_client
        self.api_key = str(api_key or "").strip()
        self.prior_experiments = [dict(item or {}) for item in list(prior_experiments or [])]
        self.last_prompt_hash = ""
        self.last_prompt = ""
        self.last_model_id = self.model
        self.last_parse_rejected = 0
        self.last_parse_errors: list[dict[str, Any]] = []

    def propose(self, symbol: str, train_df: pd.DataFrame) -> list[CandidateFeature]:
        frame = pd.DataFrame(train_df).copy()
        feature_columns = _feature_columns(frame, allowed=self.primitive_columns)
        if not feature_columns:
            return []
        feature_columns = feature_columns[: _bounded_int(os.environ.get("LLM_FACTOR_MAX_PRIMITIVES"), 64, low=2, high=256)]
        safe_names = [f"x{i}" for i in range(len(feature_columns))]
        feature_map = {safe: original for safe, original in zip(safe_names, feature_columns)}
        prompt = build_factor_prompt(
            symbol=str(symbol),
            train_df=frame,
            feature_map=feature_map,
            max_candidates=int(self.max_candidates),
            max_complexity=int(self.max_complexity),
            prior_experiments=self.prior_experiments,
        )
        prompt_hash = content_hash({"source": self.source, "prompt": prompt, "model": self.model})
        self.last_prompt = str(prompt)
        self.last_prompt_hash = str(prompt_hash)
        raw = self._complete(prompt)
        parsed = parse_llm_candidates(
            raw,
            allowed_names=set(safe_names),
            max_complexity=int(self.max_complexity),
            max_candidates=int(self.max_candidates),
        )
        self.last_parse_rejected = int(parsed["rejected"])
        self.last_parse_errors = [dict(item) for item in list(parsed.get("errors") or [])]
        out: list[CandidateFeature] = []
        seen: set[str] = set()
        for item in list(parsed.get("candidates") or []):
            expression = str(item.expression)
            hypothesis = str(item.hypothesis)
            digest = content_hash(
                {
                    "source": self.source,
                    "symbol": str(symbol).upper(),
                    "expression": expression,
                    "feature_map": dict(feature_map),
                    "hypothesis": hypothesis,
                }
            )
            if digest in seen:
                continue
            seen.add(str(digest))
            names = sorted(_expression_names(expression))
            source_feature_ids = [str(feature_map[name]) for name in names if name in feature_map]
            out.append(
                CandidateFeature(
                    source=self.source,
                    symbol=str(symbol),
                    expression=expression,
                    params={
                        "feature_map": dict(feature_map),
                        "source_feature_ids": list(source_feature_ids),
                        "complexity": int(expression_complexity(expression)),
                        "operators": {"binary": ["+", "-", "*", "/"], "unary": ["log", "abs", "sqrt"]},
                        "engine": "llm_factor_generator",
                        "hypothesis": hypothesis,
                        "prompt_hash": str(prompt_hash),
                        "model_id": str(self.model),
                    },
                    hash=str(digest),
                    feature_id=f"discovered.llm.{str(digest)[:16]}",
                )
            )
        return out

    def evaluate(self, candidate: CandidateFeature, test_df: pd.DataFrame, target: str | Sequence[float] | pd.Series):
        return evaluate_llm_candidate(candidate, test_df, target=target)

    def _complete(self, prompt: str) -> str:
        if callable(self.llm_client):
            return str(
                self.llm_client(
                    prompt=prompt,
                    model=self.model,
                    max_tokens=int(self.max_tokens),
                    max_candidates=int(self.max_candidates),
                )
            )
        api_key = self.api_key or load_anthropic_api_key()
        if not api_key:
            raise RuntimeError("anthropic_api_key_missing")
        return call_anthropic_messages_api(
            prompt,
            api_key=api_key,
            model=self.model,
            max_tokens=int(self.max_tokens),
        )


def run_llm_factor_discovery(
    *,
    symbols: Sequence[str] | None = None,
    train_frames: Mapping[str, pd.DataFrame] | None = None,
    test_frames: Mapping[str, pd.DataFrame] | None = None,
    target: str | Sequence[float] | pd.Series = "target",
    con=None,
    feature_ids: Sequence[str] | None = None,
    llm_client: Callable[..., str] | None = None,
    q_threshold: float = DEFAULT_Q_THRESHOLD,
    t_threshold: float = DEFAULT_T_THRESHOLD,
) -> dict[str, Any]:
    if not _env_bool("LLM_FACTOR_DISCOVERY", False) and llm_client is None:
        return {"ok": True, "enabled": False, "reason": "LLM_FACTOR_DISCOVERY_disabled"}

    batch_ts = now_ms()
    owns = con is None
    if owns:
        from engine.runtime.storage import connect, init_db

        init_db()
        con = connect(readonly=False)
    try:
        ensure_discovery_schema(con)
        api_key = ""
        if llm_client is None:
            api_key = load_anthropic_api_key()
            if not api_key:
                LOG.info("llm_factor_discovery_noop reason=anthropic_api_key_missing")
                return {
                    "ok": True,
                    "enabled": True,
                    "proposed": 0,
                    "accepted": 0,
                    "registered_experimental": 0,
                    "reason": "anthropic_api_key_missing",
                }
        registry_feature_ids = _resolve_feature_ids_for_discovery(feature_ids)
        symbol_list = _resolve_symbols(symbols, train_frames=train_frames, con=con)
        prior = load_prior_experiment_log(con=con)
        summary: dict[str, Any] = {
            "ok": True,
            "enabled": True,
            "symbols": len(symbol_list),
            "feature_primitives": len(registry_feature_ids),
            "proposed": 0,
            "parse_rejected": 0,
            "evaluated": 0,
            "accepted": 0,
            "registered_experimental": 0,
            "redundant": 0,
            "rejected": 0,
            "degenerate": 0,
            "leakage_failed": 0,
            "batch_ts": int(batch_ts),
            "cumulative_n_tests": cumulative_trial_count(con=con),
            "by_symbol": {},
        }
        for symbol in symbol_list:
            train_df, test_df = _resolve_frames(
                str(symbol),
                train_frames=train_frames,
                test_frames=test_frames,
                con=con,
                feature_ids=registry_feature_ids,
            )
            symbol_stats = {
                "proposed": 0,
                "parse_rejected": 0,
                "evaluated": 0,
                "accepted": 0,
                "redundant": 0,
                "rejected": 0,
                "degenerate": 0,
            }
            if train_df.empty or test_df.empty:
                summary["by_symbol"][str(symbol)] = {**symbol_stats, "skipped_reason": "empty_dataset"}
                continue
            discoverer = LLMFactorDiscoverer(
                primitive_columns=list(registry_feature_ids),
                llm_client=llm_client,
                api_key=api_key,
                prior_experiments=prior,
            )
            try:
                candidates = list(discoverer.propose(str(symbol), train_df) or [])
            except Exception as exc:
                LOG.info("llm_factor_discovery_noop symbol=%s reason=%s", str(symbol), type(exc).__name__)
                summary["by_symbol"][str(symbol)] = {
                    **symbol_stats,
                    "skipped_reason": f"proposal_failed:{type(exc).__name__}",
                }
                continue
            summary["proposed"] += len(candidates)
            summary["parse_rejected"] += int(discoverer.last_parse_rejected)
            symbol_stats["proposed"] += len(candidates)
            symbol_stats["parse_rejected"] += int(discoverer.last_parse_rejected)

            full_df = pd.concat([pd.DataFrame(train_df), pd.DataFrame(test_df)], ignore_index=True)
            _validate_candidates(
                candidates,
                con=con,
                batch_ts=int(batch_ts),
                frame=full_df,
                target=target,
                q_threshold=float(q_threshold),
                t_threshold=float(t_threshold),
                redundancy_max=_safe_float(os.environ.get("FACTOR_REDUNDANCY_MAX"), DEFAULT_REDUNDANCY_MAX),
                eval_min_ts=parse_ts_ms(os.environ.get("LLM_EVAL_MIN_TS"), default=0),
                summary=summary,
                symbol_stats=symbol_stats,
            )
            summary["by_symbol"][str(symbol)] = dict(symbol_stats)
        summary["cumulative_n_tests"] = cumulative_trial_count(con=con)
        if owns:
            con.commit()
        return summary
    finally:
        if owns and con is not None:
            con.close()


def build_factor_prompt(
    *,
    symbol: str,
    train_df: pd.DataFrame,
    feature_map: Mapping[str, str],
    max_candidates: int,
    max_complexity: int,
    prior_experiments: Sequence[Mapping[str, Any]] | None = None,
) -> str:
    frame = pd.DataFrame(train_df).copy()
    ic_summary = _recent_ic_summary(frame, feature_map=dict(feature_map))
    corr = _feature_corr_matrix(frame, feature_map=dict(feature_map))
    feature_lines = [
        {
            "var": str(var),
            "feature_id": str(fid),
            "description": _feature_description(str(fid)),
            "recent_abs_ic": ic_summary.get(str(var)),
        }
        for var, fid in dict(feature_map).items()
    ]
    payload = {
        "task": "propose_factor_hypotheses",
        "bounds": {
            "grammar": "expr := var | number | (expr + expr) | (expr - expr) | (expr * expr) | (expr / expr) | abs(expr) | log(expr) | sqrt(expr)",
            "variables": list(feature_map.keys()),
            "operators": ["+", "-", "*", "/", "abs", "log", "sqrt"],
            "max_complexity": int(max_complexity),
            "max_candidates": int(max_candidates),
            "return_json_only": True,
        },
        "symbol": str(symbol).upper(),
        "features": feature_lines,
        "aggregate_diagnostics_only": {
            "recent_ic_summary": ic_summary,
            "feature_corr_matrix": corr,
            "factor_pool_ic_matrix": corr,
        },
        "prior_experiment_log": [dict(item) for item in list(prior_experiments or [])[:50]],
        "output_schema": {"candidates": [{"expression": "(x0-x1)", "hypothesis": "economic rationale"}]},
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def parse_llm_candidates(
    raw_text: str,
    *,
    allowed_names: set[str],
    max_complexity: int,
    max_candidates: int,
) -> dict[str, Any]:
    payload = _json_object_from_text(raw_text)
    raw_candidates = payload.get("candidates")
    if not isinstance(raw_candidates, list):
        return {"candidates": [], "rejected": 1, "errors": [{"reason": "missing_candidates_array"}]}
    candidates: list[ParsedLLMCandidate] = []
    errors: list[dict[str, Any]] = []
    seen: set[str] = set()
    for idx, item in enumerate(raw_candidates[: max(1, int(max_candidates)) * 2]):
        if not isinstance(item, Mapping):
            errors.append({"idx": int(idx), "reason": "candidate_not_object"})
            continue
        expression = _simplify_expression(str(item.get("expression") or ""))
        hypothesis = str(item.get("hypothesis") or "").strip()
        if not expression:
            errors.append({"idx": int(idx), "reason": "expression_missing"})
            continue
        names = _expression_names(expression)
        complexity = expression_complexity(expression)
        if complexity > int(max_complexity):
            errors.append({"idx": int(idx), "expression": expression, "reason": "complexity_exceeded"})
            continue
        if not names or not names.issubset(set(allowed_names)):
            errors.append({"idx": int(idx), "expression": expression, "reason": "unknown_variable"})
            continue
        try:
            dummy = pd.DataFrame({name: [1.0, 2.0, 3.0] for name in sorted(allowed_names)})
            evaluate_pysr_expression(expression, dummy, feature_map={name: name for name in sorted(allowed_names)})
        except Exception as exc:
            errors.append({"idx": int(idx), "expression": expression, "reason": f"parse_failed:{type(exc).__name__}"})
            continue
        key = str(expression)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(ParsedLLMCandidate(expression=expression, hypothesis=hypothesis))
        if len(candidates) >= int(max_candidates):
            break
    return {"candidates": candidates, "rejected": len(errors), "errors": errors}


def evaluate_llm_candidate(
    candidate: CandidateFeature,
    test_df: pd.DataFrame,
    *,
    target: str | Sequence[float] | pd.Series = "target",
    min_obs: int = DEFAULT_MIN_OBS,
) -> EvaluationResult:
    frame = pd.DataFrame(test_df).copy()
    params = dict(candidate.params or {})
    feature_map = dict(params.get("feature_map") or {})
    if not feature_map:
        return _degenerate(candidate, "feature_map_missing")
    try:
        values = evaluate_pysr_expression(str(candidate.expression), frame, feature_map=feature_map)
    except Exception as exc:
        return _degenerate(candidate, f"expression_eval_failed:{type(exc).__name__}")
    if isinstance(target, str):
        if target not in set(frame.columns):
            return _degenerate(candidate, f"target_column_missing:{target}")
        y = frame[str(target)]
    else:
        y = target
    result = evaluate_feature_vector(candidate=candidate, values=values, target=y, min_obs=int(min_obs))
    cpcv_diag = _cpcv_ic_diagnostics(values, y, frame.get("ts_ms"))
    diagnostics = {**dict(result.diagnostics or {}), **cpcv_diag}
    return EvaluationResult(
        candidate_hash=str(result.candidate_hash),
        feature_id=str(result.feature_id),
        t_stat=float(result.t_stat),
        p_value=float(result.p_value),
        q_value=result.q_value,
        oos_ic=result.oos_ic,
        decision=str(result.decision),
        n_obs=int(result.n_obs),
        diagnostics=diagnostics,
    )


def cumulative_trial_count(*, con) -> int:
    return len(_past_statistical_trials(con=con)[0])


def load_prior_experiment_log(*, con, limit: int = 50) -> list[dict[str, Any]]:
    ensure_discovery_schema(con)
    rows = con.execute(
        """
        SELECT c.ts, c.source, c.symbol, c.expression, c.params_json,
               e.t_stat, e.p_value, e.q_value, e.oos_ic, e.decision
        FROM feature_candidates c
        LEFT JOIN feature_evaluation e ON e.candidate_id = c.id
        WHERE c.source = ?
        ORDER BY c.ts DESC
        LIMIT ?
        """,
        (SOURCE, max(1, int(limit or 50))),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows or []:
        params = _json_loads(row[4], {})
        out.append(
            {
                "ts": int(row[0] or 0),
                "symbol": str(row[2] or ""),
                "expression": str(row[3] or ""),
                "hypothesis": str(dict(params).get("hypothesis") or ""),
                "decision": str(row[9] or "pending"),
                "oos_ic": _finite_or_none(row[8]),
                "q_value": _finite_or_none(row[7]),
            }
        )
    return out


def load_anthropic_api_key() -> str:
    secret_name = str(os.environ.get("ANTHROPIC_API_KEY_SECRET") or "ANTHROPIC_API_KEY").strip()
    try:
        from services.secrets.loader import SecretNotAvailable, load_secret

        try:
            return bytes(load_secret(secret_name)).decode("utf-8", "ignore").strip()
        # system-audit: ignore[silent_except] missing managed secret falls back to environment config below.
        except SecretNotAvailable:
            pass
    except Exception as e:
        _warn_nonfatal(
            "LLM_FACTOR_SECRET_LOAD_FAILED",
            e,
            once_key=f"secret_load:{secret_name}",
            secret_name=str(secret_name),
        )
    if str(os.environ.get("TS_SECRETS_PROVIDER") or "").strip():
        return ""
    if str(os.environ.get("TS_ENV") or "").strip().lower() == "production":
        return ""
    return str(os.environ.get("ANTHROPIC_API_KEY") or "").strip()


def call_anthropic_messages_api(prompt: str, *, api_key: str, model: str, max_tokens: int) -> str:
    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": str(api_key),
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": str(model),
            "max_tokens": int(max_tokens),
            "messages": [
                {
                    "role": "user",
                    "content": str(prompt),
                }
            ],
        },
        timeout=float(_safe_float(os.environ.get("LLM_FACTOR_API_TIMEOUT_S"), 30.0)),
    )
    response.raise_for_status()
    payload = response.json()
    parts: list[str] = []
    for block in list(payload.get("content") or []):
        if isinstance(block, Mapping) and str(block.get("type") or "") == "text":
            parts.append(str(block.get("text") or ""))
    return "\n".join(parts).strip()


def parse_ts_ms(value: Any, *, default: int = 0) -> int:
    text = str(value or "").strip()
    if not text:
        return int(default)
    try:
        if re.fullmatch(r"\d+", text):
            return int(text)
        ts = pd.Timestamp(text)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return int(ts.timestamp() * 1000)
    except Exception:
        return int(default)


def _validate_candidates(
    candidates: Sequence[CandidateFeature],
    *,
    con,
    batch_ts: int,
    frame: pd.DataFrame,
    target: str | Sequence[float] | pd.Series,
    q_threshold: float,
    t_threshold: float,
    redundancy_max: float,
    eval_min_ts: int,
    summary: dict[str, Any],
    symbol_stats: dict[str, int],
) -> None:
    if not candidates:
        return
    full_frame = pd.DataFrame(frame).copy()
    eval_frame = _post_cutoff_frame(full_frame, int(eval_min_ts))
    existing_series = _existing_feature_series(full_frame, con=con)
    pending_series: list[tuple[str, np.ndarray]] = []
    statistical_pending: list[tuple[CandidateFeature, int, EvaluationResult]] = []

    for candidate in candidates:
        record = record_candidate(candidate, con=con, ts=batch_ts)
        try:
            values_full = evaluate_pysr_expression(
                str(candidate.expression),
                full_frame,
                feature_map=dict((candidate.params or {}).get("feature_map") or {}),
            )
        except Exception as exc:
            result = _degenerate(candidate, f"expression_eval_failed:{type(exc).__name__}").with_gate(
                q_value=1.0,
                decision="degenerate",
            )
            record_evaluation(int(record.id), result, con=con, ts=batch_ts)
            summary["degenerate"] += 1
            symbol_stats["degenerate"] += 1
            continue
        redundant_with = _redundant_with(values_full, existing_series + pending_series, max_abs_corr=float(redundancy_max))
        if redundant_with:
            result = EvaluationResult(
                candidate_hash=str(candidate.hash),
                feature_id=str(candidate.feature_id),
                t_stat=0.0,
                p_value=1.0,
                q_value=1.0,
                oos_ic=None,
                decision="redundant",
                n_obs=0,
                diagnostics={"redundant_with": str(redundant_with), "max_abs_corr": float(redundancy_max)},
            )
            record_evaluation(int(record.id), result, con=con, ts=batch_ts)
            summary["redundant"] += 1
            symbol_stats["redundant"] += 1
            continue
        pending_series.append((str(candidate.feature_id), np.asarray(values_full, dtype=float).reshape(-1)))
        result = evaluate_llm_candidate(candidate, eval_frame, target=target, min_obs=DEFAULT_MIN_OBS)
        if str(result.decision) == "degenerate":
            result = result.with_gate(q_value=1.0, decision="degenerate")
            record_evaluation(int(record.id), result, con=con, ts=batch_ts)
            summary["degenerate"] += 1
            symbol_stats["degenerate"] += 1
            continue
        statistical_pending.append((candidate, int(record.id), result))

    if not statistical_pending:
        return
    past_p, past_labels = _past_statistical_trials(con=con)
    current_p = [float(result.p_value if math.isfinite(float(result.p_value)) else 1.0) for _candidate, _id, result in statistical_pending]
    labels = list(past_labels) + [str(candidate.hash) for candidate, _id, _result in statistical_pending]
    correction = bh_fdr(list(past_p) + list(current_p), q=float(q_threshold), labels=labels)
    offset = len(past_p)
    cumulative_n = int(correction.n_tests)
    for idx, (candidate, candidate_id, result) in enumerate(statistical_pending):
        q_value = float(correction.q_values[offset + idx])
        diagnostics = {
            **dict(result.diagnostics or {}),
            "cumulative_n_tests": int(cumulative_n),
            "llm_eval_min_ts": int(eval_min_ts),
            "post_cutoff_rows": int(len(eval_frame.index)),
        }
        if not (q_value < float(q_threshold)):
            decision = "fdr_failed"
        elif not (abs(float(result.t_stat)) > float(t_threshold)):
            decision = "tstat_failed"
        else:
            decision = ACCEPTED_DECISION
        gated = EvaluationResult(
            candidate_hash=str(result.candidate_hash),
            feature_id=str(result.feature_id),
            t_stat=float(result.t_stat),
            p_value=float(result.p_value),
            q_value=float(q_value),
            oos_ic=result.oos_ic,
            decision=str(decision),
            n_obs=int(result.n_obs),
            diagnostics=diagnostics,
        )
        record_evaluation(int(candidate_id), gated, con=con, ts=batch_ts)
        summary["evaluated"] += 1
        symbol_stats["evaluated"] += 1
        if decision == ACCEPTED_DECISION:
            metadata = {
                "experimental": True,
                "default_enabled": False,
                "feature_group": "discovered_llm",
                "discovery_job": "llm_factor_discovery",
                "prompt_hash": str((candidate.params or {}).get("prompt_hash") or ""),
                "model_id": str((candidate.params or {}).get("model_id") or ""),
                "hypothesis": str((candidate.params or {}).get("hypothesis") or ""),
                "t_stat": float(gated.t_stat),
                "p_value": float(gated.p_value),
                "q_value": float(gated.q_value or 0.0),
                "oos_ic": gated.oos_ic,
                "cumulative_n_tests": int(cumulative_n),
                "llm_eval_min_ts": int(eval_min_ts),
                "diagnostics": diagnostics,
            }
            register_feature(
                candidate,
                candidate_id=int(candidate_id),
                stage=FEATURE_STAGE_SHADOW,
                metadata=metadata,
                con=con,
                ts=batch_ts,
            )
            summary["accepted"] += 1
            summary["registered_experimental"] += 1
            symbol_stats["accepted"] += 1
        elif decision == "degenerate":
            summary["degenerate"] += 1
            symbol_stats["degenerate"] += 1
        else:
            summary["rejected"] += 1
            symbol_stats["rejected"] += 1


def _past_statistical_trials(*, con) -> tuple[list[float], list[str]]:
    ensure_discovery_schema(con)
    rows = con.execute(
        """
        SELECT c.hash, e.p_value, e.decision
        FROM feature_evaluation e
        JOIN feature_candidates c ON c.id = e.candidate_id
        WHERE c.source = ?
        ORDER BY e.ts ASC, c.id ASC
        """,
        (SOURCE,),
    ).fetchall()
    p_values: list[float] = []
    labels: list[str] = []
    for digest, p_value, decision in rows or []:
        if str(decision or "") not in STAT_TEST_DECISIONS:
            continue
        p_values.append(max(0.0, min(1.0, _safe_float(p_value, 1.0))))
        labels.append(str(digest or ""))
    return p_values, labels


def _post_cutoff_frame(frame: pd.DataFrame, eval_min_ts: int) -> pd.DataFrame:
    if int(eval_min_ts or 0) <= 0 or "ts_ms" not in set(frame.columns):
        return pd.DataFrame(frame).copy().reset_index(drop=True)
    ts = pd.to_numeric(frame["ts_ms"], errors="coerce").fillna(0).astype(np.int64)
    return frame.loc[ts >= int(eval_min_ts)].copy().reset_index(drop=True)


def _existing_feature_series(frame: pd.DataFrame, *, con) -> list[tuple[str, np.ndarray]]:
    out: list[tuple[str, np.ndarray]] = []
    columns = set(str(col) for col in frame.columns)
    for column in sorted(columns):
        if column in {"target", "ts_ms", "ts", "symbol", "close"}:
            continue
        values = pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype=float)
        if np.isfinite(values).sum() >= 3 and float(np.nanstd(values)) > 1e-12:
            out.append((str(column), values))
    for record in list_registered_features(con=con, limit=5000):
        if str(getattr(record, "source", "") or "") not in {SOURCE, "pysr"}:
            continue
        params = dict(getattr(record, "params", {}) or {})
        feature_map = dict(params.get("feature_map") or {})
        if not feature_map:
            continue
        if not set(feature_map.values()).issubset(columns):
            continue
        try:
            values = evaluate_pysr_expression(str(getattr(record, "expression", "") or ""), frame, feature_map=feature_map)
        except Exception:
            continue
        out.append((str(getattr(record, "feature_id", "") or ""), np.asarray(values, dtype=float).reshape(-1)))
    return out


def _redundant_with(values: Sequence[Any], existing: Sequence[tuple[str, np.ndarray]], *, max_abs_corr: float) -> str:
    x = np.asarray(values, dtype=float).reshape(-1)
    for name, other in list(existing or []):
        y = np.asarray(other, dtype=float).reshape(-1)
        n = min(int(x.size), int(y.size))
        if n < 3:
            continue
        mask = np.isfinite(x[:n]) & np.isfinite(y[:n])
        if int(np.sum(mask)) < 3:
            continue
        a = x[:n][mask]
        b = y[:n][mask]
        if float(np.nanstd(a)) <= 1e-12 or float(np.nanstd(b)) <= 1e-12:
            continue
        corr = float(np.corrcoef(a, b)[0, 1])
        if math.isfinite(corr) and abs(corr) > float(max_abs_corr):
            return str(name)
    return ""


def _cpcv_ic_diagnostics(values: Any, target: Any, ts_ms: Any = None) -> dict[str, Any]:
    x = np.asarray(pd.to_numeric(pd.Series(values), errors="coerce"), dtype=float).reshape(-1)
    y = np.asarray(pd.to_numeric(pd.Series(target), errors="coerce"), dtype=float).reshape(-1)
    n = min(int(x.size), int(y.size))
    if n < 8:
        return {"cpcv_folds": 0, "cpcv_ic_mean": None, "cpcv_fold_ics": []}
    x = x[:n]
    y = y[:n]
    starts = np.arange(n, dtype=float)
    if ts_ms is not None:
        try:
            starts = pd.to_numeric(pd.Series(ts_ms), errors="coerce").fillna(0).to_numpy(dtype=float)[:n]
        except Exception:
            starts = np.arange(n, dtype=float)
    n_splits = min(_bounded_int(os.environ.get("LLM_FACTOR_CPCV_SPLITS"), 4, low=2, high=8), max(2, n // 4))
    splitter = CombinatorialPurgedKFold(n_splits=int(n_splits), n_test_splits=1, embargo=0.0, label_start_times=starts, label_end_times=starts)
    fold_ics: list[float] = []
    try:
        splits = list(splitter.split(np.arange(n)))
    except Exception:
        splits = []
    for _train_idx, test_idx in splits:
        if int(test_idx.size) < 3:
            continue
        ic = information_coefficient(x[test_idx], y[test_idx])
        if ic is not None and math.isfinite(float(ic)):
            fold_ics.append(float(ic))
    return {
        "cpcv_folds": int(len(fold_ics)),
        "cpcv_ic_mean": (None if not fold_ics else float(np.mean(fold_ics))),
        "cpcv_fold_ics": [float(value) for value in fold_ics],
    }


def _recent_ic_summary(frame: pd.DataFrame, *, feature_map: Mapping[str, str]) -> dict[str, float | None]:
    if "target" not in set(frame.columns):
        return {str(var): None for var in dict(feature_map)}
    window = _bounded_int(os.environ.get("LLM_FACTOR_IC_WINDOW"), 250, low=16, high=2000)
    recent = frame.tail(int(window))
    y = recent["target"]
    out: dict[str, float | None] = {}
    for var, fid in dict(feature_map).items():
        if str(fid) not in set(recent.columns):
            out[str(var)] = None
            continue
        ic = information_coefficient(recent[str(fid)], y)
        out[str(var)] = None if ic is None else float(ic)
    return out


def _feature_corr_matrix(frame: pd.DataFrame, *, feature_map: Mapping[str, str]) -> dict[str, dict[str, float | None]]:
    max_cols = min(20, len(feature_map))
    items = list(feature_map.items())[:max_cols]
    out: dict[str, dict[str, float | None]] = {}
    for var_a, fid_a in items:
        row: dict[str, float | None] = {}
        a = pd.to_numeric(frame.get(str(fid_a)), errors="coerce") if str(fid_a) in set(frame.columns) else pd.Series(dtype=float)
        for var_b, fid_b in items:
            b = pd.to_numeric(frame.get(str(fid_b)), errors="coerce") if str(fid_b) in set(frame.columns) else pd.Series(dtype=float)
            row[str(var_b)] = _corr_or_none(a, b)
        out[str(var_a)] = row
    return out


def _feature_columns(df: pd.DataFrame, *, allowed: Sequence[str] | None = None) -> list[str]:
    excluded = {"target", "ts", "ts_ms", "timestamp", "date", "datetime", "symbol"}
    allowed_set = {str(item) for item in list(allowed or []) if str(item)}
    out: list[str] = []
    for column in df.columns:
        name = str(column)
        if name.lower() in excluded:
            continue
        if allowed_set and name not in allowed_set:
            continue
        values = pd.to_numeric(df[column], errors="coerce")
        finite = values[np.isfinite(values)]
        if int(finite.size) >= 8 and float(np.nanstd(finite.to_numpy(dtype=float))) > 1e-12:
            out.append(name)
    return out


def _feature_description(feature_id: str) -> str:
    text = str(feature_id or "").strip()
    if "." in text:
        group, leaf = text.split(".", 1)
        return f"{group} feature: {leaf.replace('_', ' ')}"
    return text.replace("_", " ")


def _json_object_from_text(raw_text: str) -> dict[str, Any]:
    text = str(raw_text or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            return {}
        try:
            payload = json.loads(text[start : end + 1])
        except Exception:
            return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _json_loads(value: Any, default: Any) -> Any:
    try:
        return json.loads(str(value or ""))
    except Exception:
        return default


def _corr_or_none(a: Any, b: Any) -> float | None:
    x = np.asarray(pd.Series(a), dtype=float).reshape(-1)
    y = np.asarray(pd.Series(b), dtype=float).reshape(-1)
    n = min(int(x.size), int(y.size))
    if n < 3:
        return None
    mask = np.isfinite(x[:n]) & np.isfinite(y[:n])
    if int(np.sum(mask)) < 3:
        return None
    x = x[:n][mask]
    y = y[:n][mask]
    if float(np.nanstd(x)) <= 1e-12 or float(np.nanstd(y)) <= 1e-12:
        return None
    corr = float(np.corrcoef(x, y)[0, 1])
    return corr if math.isfinite(corr) else None


def _degenerate(candidate: CandidateFeature, reason: str) -> EvaluationResult:
    return EvaluationResult(
        candidate_hash=str(candidate.hash),
        feature_id=str(candidate.feature_id),
        t_stat=0.0,
        p_value=1.0,
        q_value=1.0,
        oos_ic=None,
        decision="degenerate",
        n_obs=0,
        diagnostics={"reason": str(reason)},
    )


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(str(name), "1" if default else "0") or "").strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _bounded_int(value: Any, default: int, *, low: int, high: int) -> int:
    try:
        out = int(value)
    except Exception:
        out = int(default)
    return max(int(low), min(int(high), int(out)))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _finite_or_none(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    return float(out) if math.isfinite(out) else None


__all__ = [
    "DEFAULT_MODEL",
    "LLMFactorDiscoverer",
    "build_factor_prompt",
    "call_anthropic_messages_api",
    "cumulative_trial_count",
    "evaluate_llm_candidate",
    "load_anthropic_api_key",
    "load_prior_experiment_log",
    "parse_llm_candidates",
    "parse_ts_ms",
    "run_llm_factor_discovery",
]
