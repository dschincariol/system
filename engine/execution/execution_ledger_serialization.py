"""Pure serialization and identity helpers for the execution ledger."""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, Optional

WarnNonfatal = Callable[..., None]


def trade_outcome_label(pnl_value: float) -> str:
    if float(pnl_value) > 0.0:
        return "win"
    if float(pnl_value) < 0.0:
        return "loss"
    return "flat"


def safe_json_dict(
    v: Any, *, warn_nonfatal: Optional[WarnNonfatal] = None
) -> Dict[str, Any]:
    if isinstance(v, dict):
        return dict(v)
    if isinstance(v, str) and v.strip():
        try:
            obj = json.loads(v)
            return dict(obj) if isinstance(obj, dict) else {}
        except Exception as e:
            if warn_nonfatal is not None:
                warn_nonfatal(
                    "EXECUTION_LEDGER_SAFE_JSON_DICT_FAILED",
                    e,
                    once_key=f"safe_json_dict:{str(v)[:80]}",
                    raw_preview=str(v)[:200],
                )
            return {}
    return {}


def safe_json_obj(
    v: Any, *, warn_nonfatal: Optional[WarnNonfatal] = None
) -> Dict[str, Any]:
    return safe_json_dict(v, warn_nonfatal=warn_nonfatal)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def pick_float(
    *vals: Any, warn_nonfatal: Optional[WarnNonfatal] = None
) -> Optional[float]:
    for v in vals:
        try:
            if v is None or v == "":
                continue
            return float(v)
        except Exception as e:
            if warn_nonfatal is not None:
                warn_nonfatal(
                    "EXECUTION_LEDGER_PICK_FLOAT_FAILED",
                    e,
                    once_key=f"pick_float:{v}",
                    raw_value=v,
                )
            continue
    return None


def normalize_model_id(model_id: Any) -> str:
    mid = str(model_id or "").strip()
    return mid or "baseline"


def extract_strategy_name(
    extra_payload: Any,
    *,
    warn_nonfatal: Optional[WarnNonfatal] = None,
) -> Optional[str]:
    obj = (
        safe_json_dict(extra_payload, warn_nonfatal=warn_nonfatal)
        if isinstance(extra_payload, dict)
        else safe_json_obj(extra_payload, warn_nonfatal=warn_nonfatal)
    )

    try:
        v = obj.get("strategy_name")
        if v:
            return str(v)
    except Exception as e:
        if warn_nonfatal is not None:
            warn_nonfatal(
                "EXECUTION_LEDGER_STRATEGY_NAME_EXTRACT_FAILED",
                e,
                once_key="strategy_name_extract:strategy_name",
            )

    try:
        v = obj.get("model_name")
        if v:
            return str(v)
    except Exception as e:
        if warn_nonfatal is not None:
            warn_nonfatal(
                "EXECUTION_LEDGER_STRATEGY_NAME_EXTRACT_FAILED",
                e,
                once_key="strategy_name_extract:model_name",
            )

    try:
        strategy_obj = obj.get("strategy")
        if isinstance(strategy_obj, dict):
            v = strategy_obj.get("name")
            if v:
                return str(v)
        elif isinstance(strategy_obj, str) and strategy_obj.strip():
            return str(strategy_obj).strip()
    except Exception as e:
        if warn_nonfatal is not None:
            warn_nonfatal(
                "EXECUTION_LEDGER_STRATEGY_NAME_EXTRACT_FAILED",
                e,
                once_key="strategy_name_extract:strategy",
            )

    try:
        ex = obj.get("explain")
        if isinstance(ex, dict):
            st = ex.get("strategy")
            if isinstance(st, dict) and st.get("name"):
                return str(st.get("name"))
            if isinstance(st, str) and st.strip():
                return str(st).strip()
    except Exception as e:
        if warn_nonfatal is not None:
            warn_nonfatal(
                "EXECUTION_LEDGER_STRATEGY_NAME_EXTRACT_FAILED",
                e,
                once_key="strategy_name_extract:explain",
            )

    try:
        ex = obj.get("explain")
        if isinstance(ex, dict):
            reason = ex.get("reason")
            if isinstance(reason, dict):
                v = reason.get("strategy")
                if v:
                    return str(v)

                sa = reason.get("strategy_alloc")
                if isinstance(sa, dict) and len(sa) == 1:
                    only_key = next(iter(sa.keys()), None)
                    if only_key:
                        return str(only_key)
    except Exception as e:
        if warn_nonfatal is not None:
            warn_nonfatal(
                "EXECUTION_LEDGER_STRATEGY_NAME_EXTRACT_FAILED",
                e,
                once_key="strategy_name_extract:explain_reason",
            )

    try:
        ex = obj.get("execution")
        if isinstance(ex, dict):
            sa = ex.get("strategy_alloc")
            if isinstance(sa, dict) and len(sa) == 1:
                only_key = next(iter(sa.keys()), None)
                if only_key:
                    return str(only_key)
    except Exception as e:
        if warn_nonfatal is not None:
            warn_nonfatal(
                "EXECUTION_LEDGER_STRATEGY_NAME_EXTRACT_FAILED",
                e,
                once_key="strategy_name_extract:execution",
            )

    return None


def extract_model_identity(
    extra_payload: Any,
    *,
    warn_nonfatal: Optional[WarnNonfatal] = None,
) -> Dict[str, Any]:
    obj = (
        safe_json_dict(extra_payload, warn_nonfatal=warn_nonfatal)
        if isinstance(extra_payload, dict)
        else safe_json_obj(extra_payload, warn_nonfatal=warn_nonfatal)
    )
    candidates = []
    seen = set()

    def _walk(candidate: Any, depth: int = 0) -> None:
        if not isinstance(candidate, dict) or depth > 4:
            return
        key = id(candidate)
        if key in seen:
            return
        seen.add(key)
        candidates.append(dict(candidate))
        for nested_key in (
            "meta",
            "original_order",
            "order",
            "intent",
            "signal",
            "explain",
            "model",
            "strategy",
        ):
            nested = candidate.get(nested_key)
            if isinstance(nested, dict):
                _walk(nested, depth + 1)

    _walk(obj, 0)

    out: Dict[str, Any] = {}

    for candidate in candidates:
        if not out.get("model_id"):
            for key in ("model_id", "agent_id"):
                val = candidate.get(key)
                if isinstance(val, str) and val.strip():
                    out["model_id"] = normalize_model_id(val)
                    break

        if not out.get("model_name"):
            for key in ("model_name", "strategy_name", "strategy", "model"):
                val = candidate.get(key)
                if isinstance(val, str) and val.strip():
                    out["model_name"] = str(val).strip()
                    break

        if not out.get("model_kind"):
            for key in ("model_kind", "kind", "type"):
                val = candidate.get(key)
                if isinstance(val, str) and val.strip():
                    out["model_kind"] = str(val).strip()
                    break

        if out.get("model_ts_ms") is None:
            for key in ("model_ts_ms", "ts_ms", "trained_ts_ms"):
                val = candidate.get(key)
                if val is not None:
                    try:
                        out["model_ts_ms"] = int(val)
                        break
                    except Exception as e:
                        if warn_nonfatal is not None:
                            warn_nonfatal(
                                "EXECUTION_LEDGER_MODEL_TS_PARSE_FAILED",
                                e,
                                once_key=f"model_ts_parse:{key}",
                                field=str(key),
                                raw_value=val,
                            )

        if not out.get("model_version"):
            for key in ("model_version", "version"):
                val = candidate.get(key)
                if isinstance(val, str) and val.strip():
                    out["model_version"] = str(val).strip()
                    break

        if not out.get("regime"):
            for key in (
                "regime",
                "current_regime",
                "regime_label",
                "market_regime",
                "market_regime_label",
            ):
                val = candidate.get(key)
                if isinstance(val, str) and val.strip():
                    out["regime"] = str(val).strip()
                    break

        if out.get("horizon_s") is None:
            for key in ("horizon_s", "horizon"):
                val = candidate.get(key)
                if val is not None:
                    try:
                        out["horizon_s"] = int(val)
                        break
                    except Exception as e:
                        if warn_nonfatal is not None:
                            warn_nonfatal(
                                "EXECUTION_LEDGER_HORIZON_PARSE_FAILED",
                                e,
                                once_key=f"horizon_parse:{key}",
                                field=str(key),
                                raw_value=val,
                            )

        strategy_obj = candidate.get("strategy")
        if isinstance(strategy_obj, dict) and not out.get("model_name"):
            val = strategy_obj.get("name")
            if isinstance(val, str) and val.strip():
                out["model_name"] = str(val).strip()

        model_obj = candidate.get("model")
        if isinstance(model_obj, dict):
            if not out.get("model_id"):
                for key in ("model_id", "id", "agent_id"):
                    val = model_obj.get(key)
                    if isinstance(val, str) and val.strip():
                        out["model_id"] = normalize_model_id(val)
                        break
            if not out.get("model_name"):
                for key in ("model_name", "name", "id"):
                    val = model_obj.get(key)
                    if isinstance(val, str) and val.strip():
                        out["model_name"] = str(val).strip()
                        break
            if not out.get("model_kind"):
                for key in ("model_kind", "kind", "type"):
                    val = model_obj.get(key)
                    if isinstance(val, str) and val.strip():
                        out["model_kind"] = str(val).strip()
                        break
            if out.get("model_ts_ms") is None:
                for key in ("model_ts_ms", "ts_ms", "trained_ts_ms"):
                    val = model_obj.get(key)
                    if val is not None:
                        try:
                            out["model_ts_ms"] = int(val)
                            break
                        except Exception as e:
                            if warn_nonfatal is not None:
                                warn_nonfatal(
                                    "EXECUTION_LEDGER_MODEL_TS_PARSE_FAILED",
                                    e,
                                    once_key=f"model_obj_ts_parse:{key}",
                                    field=str(key),
                                    raw_value=val,
                                )

    out["model_id"] = normalize_model_id(out.get("model_id"))
    return out
