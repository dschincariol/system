"""
Bounded symbolic alpha candidate discovery for offline training workflows.

This module is intentionally dependency-light and off by default. It searches a
small, deterministic expression space over existing registered features, persists
accepted candidates, and can materialize shadow-only model configs that feed the
existing training/validation pipeline.
"""

from __future__ import annotations

import ast
import hashlib
import json
import math
import os
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("engine.research.symbolic_alpha_generator")
_WARNED_NONFATAL_KEYS: set[str] = set()
_FEATURE_DEFS_LOCK = threading.Lock()
_FEATURE_DEFS_CACHE: Dict[str, Any] = {"loaded_at": 0.0, "records": []}
_FEATURE_DEFS_CACHE_TTL_S = 5.0

SUPPORTED_OPERATORS: Dict[str, Dict[str, Any]] = {
    "add": {"arity": 2},
    "sub": {"arity": 2},
    "mul": {"arity": 2},
    "div": {"arity": 2},
    "abs": {"arity": 1},
    "neg": {"arity": 1},
    "sqrt_abs": {"arity": 1},
    "log1p_abs": {"arity": 1},
    "min": {"arity": 2},
    "max": {"arity": 2},
}
DEFAULT_ALLOWED_OPERATORS = ("add", "sub", "mul", "div", "abs", "neg")
_MAX_DATASET_ROWS = 200
_MAX_SOURCE_FEATURE_POOL = 48
_MAX_SOURCE_FEATURES = 6
_MIN_CANDIDATE_SCORE = 0.01


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.research.symbolic_alpha_generator",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def symbolic_alpha_enabled() -> bool:
    """Return whether symbolic alpha discovery is enabled."""
    return str(os.environ.get("SYMBOLIC_ALPHA_ENABLED", "0")).strip().lower() in {"1", "true", "yes", "on"}


def symbolic_alpha_require_shadow_only() -> bool:
    """Return whether generated symbolic candidates must remain shadow-only."""
    return str(os.environ.get("SYMBOLIC_ALPHA_REQUIRE_SHADOW_ONLY", "1")).strip().lower() not in {"0", "false", "no", "off"}


def symbolic_alpha_max_expressions() -> int:
    """Return the maximum number of symbolic expressions to accept per run."""
    return max(1, min(32, int(os.environ.get("SYMBOLIC_ALPHA_MAX_EXPRESSIONS", "4") or 4)))


def symbolic_alpha_max_complexity() -> int:
    """Return the maximum operator-depth budget for one symbolic expression."""
    return max(1, min(6, int(os.environ.get("SYMBOLIC_ALPHA_MAX_COMPLEXITY", "2") or 2)))


def symbolic_alpha_allowed_operators() -> List[str]:
    """Return the supported symbolic operators enabled for candidate search."""
    raw = str(os.environ.get("SYMBOLIC_ALPHA_ALLOWED_OPERATORS", ",".join(DEFAULT_ALLOWED_OPERATORS)) or "").strip()
    values = [str(part or "").strip().lower() for part in raw.split(",")]
    out: List[str] = []
    seen = set()
    for name in values:
        if not name or name in seen or name not in SUPPORTED_OPERATORS:
            continue
        seen.add(name)
        out.append(name)
    return out or list(DEFAULT_ALLOWED_OPERATORS)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def _safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _clamp_numeric(value: float) -> float:
    try:
        out = float(value)
    except Exception:
        return 0.0
    if not math.isfinite(out):
        return 0.0
    if out > 1.0e12:
        return 1.0e12
    if out < -1.0e12:
        return -1.0e12
    return out


def _safe_div(numer: float, denom: float) -> float:
    d = float(denom)
    if not math.isfinite(d) or abs(d) < 1.0e-9:
        d = 1.0e-9 if d >= 0.0 else -1.0e-9
    return _clamp_numeric(float(numer) / d)


def _render_constant(value: Any) -> str:
    if isinstance(value, str):
        return json.dumps(str(value))
    num = float(value)
    if float(num).is_integer():
        return str(int(num))
    return format(float(num), ".12g")


def _render_expression_node(node: ast.AST) -> str:
    if isinstance(node, ast.Constant):
        return _render_constant(node.value)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
        return f"{node.func.id}({','.join(_render_expression_node(arg) for arg in list(node.args or []))})"
    raise ValueError(f"unsupported_expression_node:{type(node).__name__}")


def _validate_expression_node(
    node: ast.AST,
    *,
    allowed_operators: set[str],
    max_complexity: int,
    state: Dict[str, Any],
) -> None:
    if isinstance(node, ast.Constant):
        if isinstance(node.value, str):
            fid = str(node.value or "").strip()
            if not fid:
                raise ValueError("empty_feature_reference")
            if fid.startswith("symbolic."):
                raise ValueError("symbolic_feature_recursion_forbidden")
            if fid not in state["source_feature_ids_seen"]:
                state["source_feature_ids_seen"].add(fid)
                state["source_feature_ids"].append(fid)
            return
        if isinstance(node.value, (int, float)) and not isinstance(node.value, bool):
            if not math.isfinite(float(node.value)):
                raise ValueError("non_finite_constant")
            return
        raise ValueError(f"unsupported_constant:{type(node.value).__name__}")

    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
        raise ValueError(f"unsupported_node:{type(node).__name__}")

    op_name = str(node.func.id or "").strip()
    if op_name not in SUPPORTED_OPERATORS:
        raise ValueError(f"unsupported_operator:{op_name}")
    if op_name not in allowed_operators:
        raise ValueError(f"operator_not_allowed:{op_name}")
    expected_arity = int(SUPPORTED_OPERATORS[op_name]["arity"])
    if len(list(node.args or [])) != expected_arity or list(node.keywords or []):
        raise ValueError(f"invalid_arity:{op_name}")

    state["complexity"] = int(state.get("complexity") or 0) + 1
    if int(state["complexity"]) > int(max_complexity):
        raise ValueError("max_complexity_exceeded")

    for arg in list(node.args or []):
        _validate_expression_node(
            arg,
            allowed_operators=allowed_operators,
            max_complexity=int(max_complexity),
            state=state,
        )


def validate_symbolic_expression(
    expression_text: str,
    *,
    allowed_operators: Optional[Sequence[str]] = None,
    max_complexity: Optional[int] = None,
) -> Dict[str, Any]:
    """Validate and normalize one symbolic alpha expression definition."""
    text = str(expression_text or "").strip()
    if not text:
        raise ValueError("expression_text_required")

    allowed = set(str(name).strip().lower() for name in (allowed_operators or symbolic_alpha_allowed_operators()) if str(name).strip())
    if not allowed:
        raise ValueError("allowed_operators_required")
    complexity_limit = int(max_complexity or symbolic_alpha_max_complexity())

    try:
        tree = ast.parse(text, mode="eval")
    except Exception as e:
        raise ValueError(f"expression_parse_failed:{type(e).__name__}") from e

    state: Dict[str, Any] = {
        "complexity": 0,
        "source_feature_ids": [],
        "source_feature_ids_seen": set(),
    }
    _validate_expression_node(
        tree.body,
        allowed_operators=allowed,
        max_complexity=complexity_limit,
        state=state,
    )
    if not state["source_feature_ids"]:
        raise ValueError("expression_has_no_source_features")

    normalized = _render_expression_node(tree.body)
    return {
        "expression_text": str(normalized),
        "complexity": int(state["complexity"]),
        "source_feature_ids": list(state["source_feature_ids"]),
        "allowed_operators": sorted(str(name) for name in allowed),
    }


def _evaluate_expression_node(node: ast.AST, feature_values: Dict[str, float]) -> float:
    if isinstance(node, ast.Constant):
        if isinstance(node.value, str):
            return _clamp_numeric(feature_values.get(str(node.value), 0.0) or 0.0)
        return _clamp_numeric(float(node.value))

    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
        raise ValueError(f"unsupported_expression_node:{type(node).__name__}")

    op_name = str(node.func.id or "").strip()
    args = [_evaluate_expression_node(arg, feature_values) for arg in list(node.args or [])]
    if op_name == "add":
        return _clamp_numeric(args[0] + args[1])
    if op_name == "sub":
        return _clamp_numeric(args[0] - args[1])
    if op_name == "mul":
        return _clamp_numeric(args[0] * args[1])
    if op_name == "div":
        return _safe_div(args[0], args[1])
    if op_name == "abs":
        return _clamp_numeric(abs(args[0]))
    if op_name == "neg":
        return _clamp_numeric(-args[0])
    if op_name == "sqrt_abs":
        return _clamp_numeric(math.sqrt(abs(args[0])))
    if op_name == "log1p_abs":
        return _clamp_numeric(math.log1p(abs(args[0])))
    if op_name == "min":
        return _clamp_numeric(min(args[0], args[1]))
    if op_name == "max":
        return _clamp_numeric(max(args[0], args[1]))
    raise ValueError(f"unsupported_operator:{op_name}")


def evaluate_symbolic_expression(
    expression_text: str,
    feature_values: Dict[str, float],
    *,
    allowed_operators: Optional[Sequence[str]] = None,
    max_complexity: Optional[int] = None,
) -> float:
    """Evaluate one validated symbolic alpha expression against a feature map."""
    validated = validate_symbolic_expression(
        expression_text,
        allowed_operators=allowed_operators,
        max_complexity=max_complexity,
    )
    tree = ast.parse(str(validated["expression_text"]), mode="eval")
    return _evaluate_expression_node(tree.body, dict(feature_values or {}))


def symbolic_feature_id(expression_text: str, source_feature_ids: Sequence[str]) -> str:
    """Build a stable feature id for one symbolic expression definition."""
    payload = {
        "expression_text": str(expression_text),
        "source_feature_ids": [str(fid) for fid in list(source_feature_ids or [])],
    }
    digest = hashlib.sha1(_json_dumps(payload).encode("utf-8")).hexdigest()[:12]
    return f"symbolic.alpha.{digest}"


def _parse_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return list(value)
    if value in (None, "", b"", bytearray()):
        return []
    try:
        raw = value.decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) else str(value)
        obj = json.loads(raw)
    except Exception:
        return []
    return list(obj) if isinstance(obj, list) else []


def _parse_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value in (None, "", b"", bytearray()):
        return {}
    try:
        raw = value.decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) else str(value)
        obj = json.loads(raw)
    except Exception:
        return {}
    return dict(obj) if isinstance(obj, dict) else {}


def _ensure_symbolic_alpha_schema(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS symbolic_alpha_candidates (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          created_ts INTEGER NOT NULL,
          expression_text TEXT NOT NULL,
          source_feature_ids_json TEXT NOT NULL,
          complexity INTEGER NOT NULL,
          score REAL,
          status TEXT NOT NULL,
          diagnostics_json TEXT
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_symbolic_alpha_candidates_status_created
          ON symbolic_alpha_candidates(status, created_ts DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_symbolic_alpha_candidates_created
          ON symbolic_alpha_candidates(created_ts DESC)
        """
    )


def _clear_feature_definition_cache() -> None:
    with _FEATURE_DEFS_LOCK:
        _FEATURE_DEFS_CACHE["loaded_at"] = 0.0
        _FEATURE_DEFS_CACHE["records"] = []


def persist_symbolic_alpha_candidate(
    *,
    expression_text: str,
    score: Optional[float] = None,
    status: str = "accepted",
    diagnostics: Optional[Dict[str, Any]] = None,
    created_ts: Optional[int] = None,
) -> Dict[str, Any]:
    """Persist one validated symbolic alpha candidate and its metadata."""
    validated = validate_symbolic_expression(expression_text)
    now_ms = int(created_ts if created_ts is not None else int(time.time() * 1000))
    feature_id = symbolic_feature_id(validated["expression_text"], validated["source_feature_ids"])
    diag = dict(diagnostics or {})
    diag.setdefault("feature_id", str(feature_id))
    diag.setdefault("allowed_operators", list(validated["allowed_operators"]))
    diag.setdefault("normalized_expression", str(validated["expression_text"]))

    from engine.runtime.storage import init_db, run_write_txn

    init_db()
    inserted: Dict[str, Any] = {"id": 0}

    def _write(con) -> int:
        _ensure_symbolic_alpha_schema(con)
        cur = con.execute(
            """
            INSERT INTO symbolic_alpha_candidates(
              created_ts, expression_text, source_feature_ids_json, complexity,
              score, status, diagnostics_json
            )
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                int(now_ms),
                str(validated["expression_text"]),
                json.dumps(list(validated["source_feature_ids"]), separators=(",", ":"), sort_keys=False),
                int(validated["complexity"]),
                (_safe_float(score) if score is not None else None),
                str(status or "accepted"),
                _json_dumps(diag),
            ),
        )
        inserted["id"] = int(cur.lastrowid or 0)
        return int(inserted["id"])

    run_write_txn(_write, table="symbolic_alpha_candidates", operation="persist_symbolic_alpha_candidate")
    _clear_feature_definition_cache()
    return {
        "id": int(inserted["id"] or 0),
        "created_ts": int(now_ms),
        "expression_text": str(validated["expression_text"]),
        "source_feature_ids": list(validated["source_feature_ids"]),
        "complexity": int(validated["complexity"]),
        "score": (_safe_float(score) if score is not None else None),
        "status": str(status or "accepted"),
        "diagnostics": dict(diag),
        "feature_id": str(feature_id),
    }


def list_symbolic_alpha_candidates(
    *,
    statuses: Optional[Sequence[str]] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """List persisted symbolic alpha candidates filtered by status."""
    from engine.runtime.storage import connect, init_db

    init_db()
    con = connect(readonly=True)
    try:
        where: List[str] = ["1=1"]
        params: List[Any] = []
        normalized_statuses = [str(status).strip() for status in list(statuses or []) if str(status).strip()]
        if normalized_statuses:
            where.append(f"status IN ({','.join('?' for _ in normalized_statuses)})")
            params.extend(normalized_statuses)
        rows = con.execute(
            f"""
            SELECT id, created_ts, expression_text, source_feature_ids_json, complexity, score, status, diagnostics_json
            FROM symbolic_alpha_candidates
            WHERE {" AND ".join(where)}
            ORDER BY created_ts DESC, id DESC
            LIMIT ?
            """,
            tuple(params + [int(max(1, min(500, int(limit or 100))))]),
        ).fetchall()
    finally:
        con.close()

    out: List[Dict[str, Any]] = []
    for row in rows or []:
        diagnostics = _parse_json_dict(row[7])
        source_feature_ids = [str(fid) for fid in _parse_json_list(row[3]) if str(fid).strip()]
        expression_text = str(row[2] or "")
        feature_id = str(diagnostics.get("feature_id") or symbolic_feature_id(expression_text, source_feature_ids))
        diagnostics["feature_id"] = feature_id
        out.append(
            {
                "id": int(row[0] or 0),
                "created_ts": int(row[1] or 0),
                "expression_text": str(expression_text),
                "source_feature_ids": source_feature_ids,
                "complexity": int(row[4] or 0),
                "score": _safe_float(row[5]),
                "status": str(row[6] or ""),
                "diagnostics": diagnostics,
                "feature_id": feature_id,
            }
        )
    return out


def _accepted_symbolic_feature_definitions(force: bool = False) -> List[Dict[str, Any]]:
    now = time.time()
    with _FEATURE_DEFS_LOCK:
        cached_rows = list(_FEATURE_DEFS_CACHE.get("records") or [])
        loaded_at = float(_FEATURE_DEFS_CACHE.get("loaded_at") or 0.0)
    if (not force) and cached_rows and (now - loaded_at) < _FEATURE_DEFS_CACHE_TTL_S:
        return cached_rows

    rows = list_symbolic_alpha_candidates(statuses=("accepted",), limit=500)
    by_feature_id: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        feature_id = str(row.get("feature_id") or "").strip()
        if not feature_id or feature_id in by_feature_id:
            continue
        by_feature_id[feature_id] = dict(row)
    out = list(by_feature_id.values())
    with _FEATURE_DEFS_LOCK:
        _FEATURE_DEFS_CACHE["loaded_at"] = float(now)
        _FEATURE_DEFS_CACHE["records"] = list(out)
    return out


def load_symbolic_feature_definition(feature_id: str) -> Optional[Dict[str, Any]]:
    """Load one accepted symbolic feature definition by feature id."""
    target = str(feature_id or "").strip()
    if not target:
        return None
    for row in _accepted_symbolic_feature_definitions():
        if str(row.get("feature_id") or "").strip() == target:
            return dict(row)
    return None


def evaluate_symbolic_feature(
    feature_id: str,
    feature_values: Dict[str, float],
) -> Optional[float]:
    """Evaluate one persisted symbolic feature against runtime feature values."""
    definition = load_symbolic_feature_definition(feature_id)
    if not definition:
        return None
    return evaluate_symbolic_expression(
        str(definition.get("expression_text") or ""),
        dict(feature_values or {}),
    )


def _pearson_abs(xs: Sequence[float], ys: Sequence[float]) -> Optional[float]:
    pairs: List[Tuple[float, float]] = []
    for x, y in zip(list(xs or []), list(ys or [])):
        fx = _safe_float(x)
        fy = _safe_float(y)
        if fx is None or fy is None:
            continue
        pairs.append((float(fx), float(fy)))
    if len(pairs) < 8:
        return None

    xs_f = [row[0] for row in pairs]
    ys_f = [row[1] for row in pairs]
    mean_x = sum(xs_f) / float(len(xs_f))
    mean_y = sum(ys_f) / float(len(ys_f))
    var_x = sum((x - mean_x) ** 2 for x in xs_f)
    var_y = sum((y - mean_y) ** 2 for y in ys_f)
    if var_x <= 1.0e-12 or var_y <= 1.0e-12:
        return None
    cov = sum((x - mean_x) * (y - mean_y) for x, y in pairs)
    corr = cov / math.sqrt(var_x * var_y)
    if not math.isfinite(corr):
        return None
    return abs(float(corr))


def _load_training_rows(model_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    from engine.runtime.storage import connect, init_db

    init_db()
    lookback_days = max(1, int(model_config.get("training_window_days") or 365))
    now_ms = int(time.time() * 1000)
    cutoff_ms = int(now_ms - (lookback_days * 24 * 60 * 60 * 1000))
    symbols = [str(sym).upper().strip() for sym in list(model_config.get("symbol_universe") or []) if str(sym).strip()]
    horizons = [int(h) for h in list(model_config.get("horizons_s") or model_config.get("horizons") or []) if int(h) > 0]

    where = ["e.ts_ms >= ?", "COALESCE(le.net_z, l.impact_z) IS NOT NULL"]
    params: List[Any] = [int(cutoff_ms)]
    if symbols:
        where.append(f"l.symbol IN ({','.join('?' for _ in symbols)})")
        params.extend(symbols)
    if horizons:
        where.append(f"l.horizon_s IN ({','.join('?' for _ in horizons)})")
        params.extend([int(h) for h in horizons])

    con = connect(readonly=True)
    try:
        rows = con.execute(
            f"""
            SELECT l.event_id, l.symbol, l.horizon_s, COALESCE(le.net_z, l.impact_z) AS target_z, e.ts_ms
            FROM labels l
            JOIN events e ON e.id = l.event_id
            LEFT JOIN labels_exec le
              ON le.event_id = l.event_id
             AND le.symbol = l.symbol
             AND le.horizon_s = l.horizon_s
             AND le.realized = 1
            WHERE {" AND ".join(where)}
            ORDER BY e.ts_ms DESC
            LIMIT ?
            """,
            tuple(params + [int(_MAX_DATASET_ROWS)]),
        ).fetchall()
    finally:
        con.close()

    out: List[Dict[str, Any]] = []
    for row in rows or []:
        target_z = _safe_float(row[3])
        ts_ms = int(row[4] or 0)
        symbol = str(row[1] or "").upper().strip()
        horizon_s = int(row[2] or 0)
        if target_z is None or ts_ms <= 0 or not symbol or horizon_s <= 0:
            continue
        out.append(
            {
                "event_id": int(row[0] or 0),
                "symbol": str(symbol),
                "horizon_s": int(horizon_s),
                "target_z": float(target_z),
                "event": {
                    "ts_ms": int(ts_ms),
                    "title": "",
                    "body": "",
                    "source": "",
                },
            }
        )
    out.reverse()
    return out


def _discover_source_feature_ids(model_config: Dict[str, Any]) -> List[str]:
    try:
        from engine.strategy.feature_registry import resolve_feature_ids
    except Exception as e:
        _warn_nonfatal("SYMBOLIC_ALPHA_FEATURE_REGISTRY_IMPORT_FAILED", e, once_key="feature_registry_import")
        return []

    resolved = resolve_feature_ids(
        list(model_config.get("feature_ids") or []),
        model_name=str(model_config.get("model_name") or "").strip() or None,
    )
    out: List[str] = []
    seen = set()
    for fid in list(resolved or []):
        name = str(fid or "").strip()
        if not name or name.startswith("symbolic.") or name in seen:
            continue
        seen.add(name)
        out.append(name)
        if len(out) >= _MAX_SOURCE_FEATURE_POOL:
            break
    return out


def _build_feature_matrix(rows: List[Dict[str, Any]], feature_ids: List[str]) -> Dict[str, List[float]]:
    try:
        from engine.strategy.feature_registry import build_feature_snapshot
    except Exception as e:
        _warn_nonfatal("SYMBOLIC_ALPHA_FEATURE_REGISTRY_IMPORT_FAILED", e, once_key="feature_registry_import_matrix")
        return {}

    matrix: Dict[str, List[float]] = {str(fid): [] for fid in list(feature_ids or [])}
    for row in rows or []:
        try:
            snap = build_feature_snapshot(
                event=dict(row.get("event") or {}),
                symbol=str(row.get("symbol") or ""),
                feature_ids=list(feature_ids or []),
            )
        except Exception as e:
            _warn_nonfatal(
                "SYMBOLIC_ALPHA_BUILD_FEATURE_SNAPSHOT_FAILED",
                e,
                once_key="build_feature_snapshot",
                symbol=str(row.get("symbol") or ""),
                horizon_s=int(row.get("horizon_s") or 0),
            )
            continue
        for fid in list(feature_ids or []):
            matrix[str(fid)].append(float(snap.get(fid, 0.0) or 0.0))
    return matrix


def _score_source_features(rows: List[Dict[str, Any]], feature_ids: List[str]) -> List[Tuple[str, float]]:
    targets = [float(row.get("target_z") or 0.0) for row in rows or []]
    matrix = _build_feature_matrix(rows, feature_ids)
    scored: List[Tuple[str, float]] = []
    for fid in list(feature_ids or []):
        score = _pearson_abs(matrix.get(str(fid), []), targets)
        if score is None or score <= 0.0:
            continue
        scored.append((str(fid), float(score)))
    scored.sort(key=lambda item: (item[1], item[0]), reverse=True)
    return scored


def _candidate_expression_templates(feature_ids: Sequence[str], allowed_operators: Sequence[str]) -> List[str]:
    ordered_features = [str(fid) for fid in list(feature_ids or []) if str(fid).strip()]
    allowed = set(str(name).strip().lower() for name in list(allowed_operators or []))
    out: List[str] = []
    seen = set()

    def _push(expr: str) -> None:
        normalized = str(expr or "").strip()
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        out.append(normalized)

    for fid in ordered_features:
        encoded = json.dumps(str(fid))
        if "abs" in allowed:
            _push(f"abs({encoded})")
        if "neg" in allowed:
            _push(f"neg({encoded})")
        if "sqrt_abs" in allowed:
            _push(f"sqrt_abs({encoded})")
        if "log1p_abs" in allowed:
            _push(f"log1p_abs({encoded})")

    for idx, left in enumerate(ordered_features):
        for right in ordered_features[idx + 1:]:
            ltxt = json.dumps(str(left))
            rtxt = json.dumps(str(right))
            if "sub" in allowed:
                _push(f"sub({ltxt},{rtxt})")
                _push(f"sub({rtxt},{ltxt})")
            if "add" in allowed:
                _push(f"add({ltxt},{rtxt})")
            if "mul" in allowed:
                _push(f"mul({ltxt},{rtxt})")
            if "div" in allowed:
                _push(f"div({ltxt},{rtxt})")
                _push(f"div({rtxt},{ltxt})")
            if "min" in allowed:
                _push(f"min({ltxt},{rtxt})")
            if "max" in allowed:
                _push(f"max({ltxt},{rtxt})")
    return out


def generate_symbolic_alpha_candidates(
    model_config: Dict[str, Any],
    *,
    max_expressions: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Search the bounded symbolic expression space for promising candidates."""
    if not symbolic_alpha_enabled():
        return []

    config = dict(model_config or {})
    source_feature_ids = _discover_source_feature_ids(config)
    if not source_feature_ids:
        return []

    rows = _load_training_rows(config)
    if len(rows) < 16:
        return []

    allowed_operators = symbolic_alpha_allowed_operators()
    max_candidates = int(max_expressions or symbolic_alpha_max_expressions())
    max_complexity = symbolic_alpha_max_complexity()
    targets = [float(row.get("target_z") or 0.0) for row in rows or []]

    scored_sources = _score_source_features(rows, source_feature_ids)
    if not scored_sources:
        return []
    top_features = [fid for fid, _score in scored_sources[:_MAX_SOURCE_FEATURES]]
    candidate_texts = _candidate_expression_templates(top_features, allowed_operators)

    accepted: List[Tuple[str, float, Dict[str, Any]]] = []
    seen_feature_ids = set()
    feature_matrix = _build_feature_matrix(rows, top_features)
    feature_value_rows: List[Dict[str, float]] = []
    row_count = min(len(targets), min((len(values) for values in feature_matrix.values()), default=0))
    if row_count <= 0:
        return []
    for idx in range(int(row_count)):
        feature_value_rows.append(
            {
                str(fid): float(feature_matrix.get(str(fid), [0.0] * row_count)[idx] or 0.0)
                for fid in list(top_features or [])
            }
        )
    targets = targets[:row_count]

    for expression_text in candidate_texts:
        try:
            validated = validate_symbolic_expression(
                expression_text,
                allowed_operators=allowed_operators,
                max_complexity=max_complexity,
            )
        except Exception:
            continue
        if int(validated.get("complexity") or 0) > int(max_complexity):
            continue
        values: List[float] = []
        for feature_value_row in feature_value_rows:
            try:
                values.append(
                    float(
                        evaluate_symbolic_expression(
                            str(validated["expression_text"]),
                            feature_value_row,
                            allowed_operators=allowed_operators,
                            max_complexity=max_complexity,
                        )
                    )
                )
            except Exception:
                values = []
                break
        score = _pearson_abs(values, targets)
        if score is None or score < _MIN_CANDIDATE_SCORE:
            continue
        feature_id = symbolic_feature_id(validated["expression_text"], validated["source_feature_ids"])
        if feature_id in seen_feature_ids:
            continue
        seen_feature_ids.add(feature_id)
        accepted.append(
            (
                str(validated["expression_text"]),
                float(score),
                {
                    "feature_id": str(feature_id),
                    "source_scores": {str(fid): float(src_score) for fid, src_score in scored_sources[:_MAX_SOURCE_FEATURES]},
                    "score_kind": "abs_pearson",
                    "row_count": int(row_count),
                    "base_model_name": str(config.get("model_name") or ""),
                    "shadow_only": bool(symbolic_alpha_require_shadow_only()),
                },
            )
        )
        if len(accepted) >= int(max_candidates):
            break

    accepted.sort(key=lambda item: (item[1], item[0]), reverse=True)
    persisted: List[Dict[str, Any]] = []
    for expression_text, score, diagnostics in accepted[:int(max_candidates)]:
        persisted.append(
            persist_symbolic_alpha_candidate(
                expression_text=str(expression_text),
                score=float(score),
                status="accepted",
                diagnostics=dict(diagnostics),
            )
        )
    return persisted


def build_symbolic_candidate_model_configs(
    base_model_configs: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Extend base model configs with accepted symbolic feature candidates."""
    if not symbolic_alpha_enabled():
        return []

    out: List[Dict[str, Any]] = []
    remaining = symbolic_alpha_max_expressions()
    for raw_cfg in list(base_model_configs or []):
        if remaining <= 0:
            break
        base_cfg = dict(raw_cfg or {})
        if not base_cfg:
            continue
        base_feature_ids = [str(fid) for fid in list(base_cfg.get("feature_ids") or []) if str(fid).strip()]
        if not base_feature_ids:
            continue
        candidates = generate_symbolic_alpha_candidates(base_cfg, max_expressions=remaining)
        for candidate in candidates:
            feature_id = str(candidate.get("feature_id") or "").strip()
            if not feature_id:
                continue
            suffix = feature_id.rsplit(".", 1)[-1]
            model_name = f"{str(base_cfg.get('model_name') or 'embed_regressor')}.symbolic_{suffix}"
            feature_ids = list(base_feature_ids) + [str(feature_id)]
            candidate_meta = {
                "feature_id": str(feature_id),
                "expression_text": str(candidate.get("expression_text") or ""),
                "complexity": int(candidate.get("complexity") or 0),
                "score": _safe_float(candidate.get("score")),
                "source_feature_ids": list(candidate.get("source_feature_ids") or []),
                "status": str(candidate.get("status") or "accepted"),
                "shadow_only": bool(symbolic_alpha_require_shadow_only()),
            }
            out.append(
                {
                    **base_cfg,
                    "model_name": str(model_name),
                    "model_id": str(model_name),
                    "instance_name": str(model_name),
                    "feature_ids": feature_ids,
                    "prediction_enabled": False,
                    "experimental": True,
                    "symbolic_candidate": candidate_meta,
                    "shadow_only": bool(symbolic_alpha_require_shadow_only()),
                }
            )
            remaining -= 1
            if remaining <= 0:
                break
    return out


__all__ = [
    "build_symbolic_candidate_model_configs",
    "evaluate_symbolic_expression",
    "evaluate_symbolic_feature",
    "generate_symbolic_alpha_candidates",
    "list_symbolic_alpha_candidates",
    "load_symbolic_feature_definition",
    "persist_symbolic_alpha_candidate",
    "symbolic_alpha_allowed_operators",
    "symbolic_alpha_enabled",
    "symbolic_alpha_max_complexity",
    "symbolic_alpha_max_expressions",
    "symbolic_alpha_require_shadow_only",
    "symbolic_feature_id",
    "validate_symbolic_expression",
]
