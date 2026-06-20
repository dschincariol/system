"""Append-only experiment ledger for generated alpha/model candidates."""

from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any, Mapping, Sequence

from engine.audit.chain import append_chain_row
from engine.runtime.failure_diagnostics import log_failure


LOG = logging.getLogger(__name__)

GENERATED_CANDIDATE_SOURCES = frozenset(
    {
        "alpha_discovery",
        "llm_alpha",
        "llm_factor",
        "model_challenger",
        "optuna",
        "optuna_cpcv",
        "pysr",
        "search_feature",
        "symbolic_alpha",
        "symbolic_alpha_discovery",
        "symbolic_factor",
        "tsfresh",
        "tsfresh_feature",
    }
)
GENERATED_MUTATION_KINDS = frozenset(
    {
        "alpha_discovery",
        "llm_alpha_discovery",
        "symbolic_alpha_discovery",
        "optuna_challenger",
    }
)
PASS_DECISIONS = frozenset({"pass", "passed", "accepted", "approved", "promote", "promoted"})
FAIL_DECISIONS = frozenset({"fail", "failed", "reject", "rejected", "blocked", "block"})


def _now_ms() -> int:
    return int(time.time() * 1000)


def _log_nonfatal(code: str, error: BaseException) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.experiment_ledger",
        persist=False,
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, (set, tuple)):
        return list(value)
    return str(value)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=_json_default)


def _json_load(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if value in (None, "", b"", bytearray()):
        return default
    try:
        raw = value.decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) else str(value)
        parsed = json.loads(raw)
    except Exception:
        return default
    return parsed


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return float(out) if math.isfinite(out) else None


def _table_columns(con, table_name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({str(table_name)})").fetchall() or []
    except Exception:
        return set()
    return {str(row[1] or "").strip() for row in rows if row and len(row) > 1 and str(row[1] or "").strip()}


def _table_exists(con, table_name: str) -> bool:
    return bool(_table_columns(con, table_name))


def _add_column_if_missing(con, table_name: str, column_name: str, definition: str) -> None:
    if column_name in _table_columns(con, table_name):
        return
    con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def ensure_experiment_ledger_schema(con) -> None:
    """Create the append-only generated-candidate ledger if it is absent."""

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS experiment_ledger (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts INTEGER NOT NULL,
          candidate_key TEXT NOT NULL,
          candidate_name TEXT,
          candidate_version TEXT,
          candidate_type TEXT NOT NULL,
          source TEXT NOT NULL,
          parent_candidate_key TEXT,
          model_name TEXT,
          model_family TEXT,
          feature_ids_json TEXT NOT NULL DEFAULT '[]',
          prompt_hash TEXT,
          model_hash TEXT,
          search_space_json TEXT NOT NULL DEFAULT '{}',
          trial_budget INTEGER NOT NULL DEFAULT 0,
          trial_count INTEGER NOT NULL DEFAULT 0,
          cpcv_json TEXT NOT NULL DEFAULT '{}',
          pbo REAL,
          dsr REAL,
          fdr_json TEXT NOT NULL DEFAULT '{}',
          redundancy_json TEXT NOT NULL DEFAULT '{}',
          evidence_json TEXT NOT NULL DEFAULT '{}',
          promotion_decision TEXT NOT NULL DEFAULT 'pending',
          status TEXT NOT NULL DEFAULT 'recorded',
          diagnostics_json TEXT NOT NULL DEFAULT '{}',
          prev_hash BLOB,
          row_hash BLOB
        )
        """
    )
    for column_name, definition in (
        ("parent_candidate_key", "TEXT"),
        ("model_name", "TEXT"),
        ("model_family", "TEXT"),
        ("feature_ids_json", "TEXT NOT NULL DEFAULT '[]'"),
        ("prompt_hash", "TEXT"),
        ("model_hash", "TEXT"),
        ("search_space_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("trial_budget", "INTEGER NOT NULL DEFAULT 0"),
        ("trial_count", "INTEGER NOT NULL DEFAULT 0"),
        ("cpcv_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("pbo", "REAL"),
        ("dsr", "REAL"),
        ("fdr_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("redundancy_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("evidence_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("promotion_decision", "TEXT NOT NULL DEFAULT 'pending'"),
        ("status", "TEXT NOT NULL DEFAULT 'recorded'"),
        ("diagnostics_json", "TEXT NOT NULL DEFAULT '{}'"),
        ("prev_hash", "BLOB"),
        ("row_hash", "BLOB"),
    ):
        _add_column_if_missing(con, "experiment_ledger", column_name, definition)
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_experiment_ledger_candidate_ts
          ON experiment_ledger(candidate_key, ts DESC, id DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_experiment_ledger_model_version_ts
          ON experiment_ledger(candidate_name, candidate_version, ts DESC, id DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_experiment_ledger_source_status_ts
          ON experiment_ledger(source, status, ts DESC, id DESC)
        """
    )


def candidate_key_for(candidate_name: str, candidate_version: str | None = None) -> str:
    name = str(candidate_name or "").strip()
    version = str(candidate_version or "").strip()
    return f"{name}:{version}" if name and version else name


def record_experiment_ledger(
    *,
    candidate_key: str | None = None,
    candidate_name: str | None = None,
    candidate_version: str | None = None,
    candidate_type: str,
    source: str,
    parent_candidate_key: str | None = None,
    model_name: str | None = None,
    model_family: str | None = None,
    feature_ids: Sequence[Any] | None = None,
    prompt_hash: str | None = None,
    model_hash: str | None = None,
    search_space: Mapping[str, Any] | None = None,
    trial_budget: int | None = None,
    trial_count: int | None = None,
    cpcv: Mapping[str, Any] | None = None,
    pbo: float | None = None,
    dsr: float | None = None,
    fdr: Mapping[str, Any] | None = None,
    redundancy: Mapping[str, Any] | None = None,
    evidence: Mapping[str, Any] | None = None,
    promotion_decision: str = "pending",
    status: str = "recorded",
    diagnostics: Mapping[str, Any] | None = None,
    ts: int | None = None,
    con=None,
) -> int:
    """Append one ledger row and return its row id when available."""

    key = str(candidate_key or "").strip() or candidate_key_for(str(candidate_name or model_name or ""), candidate_version)
    if not key:
        raise ValueError("experiment_ledger_candidate_key_required")
    row = {
        "ts": int(ts if ts is not None else _now_ms()),
        "candidate_key": str(key),
        "candidate_name": (str(candidate_name).strip() if candidate_name else None),
        "candidate_version": (str(candidate_version).strip() if candidate_version else None),
        "candidate_type": str(candidate_type or "candidate").strip() or "candidate",
        "source": str(source or "unknown").strip() or "unknown",
        "parent_candidate_key": (str(parent_candidate_key).strip() if parent_candidate_key else None),
        "model_name": (str(model_name).strip() if model_name else None),
        "model_family": (str(model_family).strip() if model_family else None),
        "feature_ids_json": [str(fid) for fid in list(feature_ids or []) if str(fid or "").strip()],
        "prompt_hash": (str(prompt_hash).strip() if prompt_hash else None),
        "model_hash": (str(model_hash).strip() if model_hash else None),
        "search_space_json": dict(search_space or {}),
        "trial_budget": max(0, int(trial_budget or 0)),
        "trial_count": max(0, int(trial_count or 0)),
        "cpcv_json": dict(cpcv or {}),
        "pbo": _safe_float_or_none(pbo),
        "dsr": _safe_float_or_none(dsr),
        "fdr_json": dict(fdr or {}),
        "redundancy_json": dict(redundancy or {}),
        "evidence_json": dict(evidence or {}),
        "promotion_decision": str(promotion_decision or "pending").strip().lower() or "pending",
        "status": str(status or "recorded").strip().lower() or "recorded",
        "diagnostics_json": dict(diagnostics or {}),
    }

    owns = con is None
    if owns:
        from engine.runtime.storage import connect, init_db

        init_db()
        con = connect(readonly=False)
    try:
        ensure_experiment_ledger_schema(con)
        result = append_chain_row("experiment_ledger", row, con)
        if owns:
            con.commit()
        return int(result.row_id or 0)
    finally:
        if owns and con is not None:
            con.close()


def fetch_experiment_ledger(
    *,
    candidate_key: str | None = None,
    candidate_name: str | None = None,
    candidate_version: str | None = None,
    model_name: str | None = None,
    limit: int = 20,
    con=None,
) -> list[dict[str, Any]]:
    owns = con is None
    if owns:
        from engine.runtime.storage import connect, init_db

        init_db()
        con = connect(readonly=True)
    try:
        try:
            ensure_experiment_ledger_schema(con)
        except Exception:
            return []
        where: list[str] = []
        params: list[Any] = []
        key = str(candidate_key or "").strip()
        if key:
            where.append("candidate_key=?")
            params.append(str(key))
        if str(candidate_name or "").strip():
            where.append("candidate_name=?")
            params.append(str(candidate_name).strip())
        if str(candidate_version or "").strip():
            where.append("candidate_version=?")
            params.append(str(candidate_version).strip())
        if str(model_name or "").strip():
            where.append("(model_name=? OR candidate_name=?)")
            params.extend([str(model_name).strip(), str(model_name).strip()])
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        columns = (
            "id",
            "ts",
            "candidate_key",
            "candidate_name",
            "candidate_version",
            "candidate_type",
            "source",
            "parent_candidate_key",
            "model_name",
            "model_family",
            "feature_ids_json",
            "prompt_hash",
            "model_hash",
            "search_space_json",
            "trial_budget",
            "trial_count",
            "cpcv_json",
            "pbo",
            "dsr",
            "fdr_json",
            "redundancy_json",
            "evidence_json",
            "promotion_decision",
            "status",
            "diagnostics_json",
        )
        rows = con.execute(
            f"""
            SELECT {', '.join(columns)}
            FROM experiment_ledger
            {where_sql}
            ORDER BY ts DESC, id DESC
            LIMIT ?
            """,
            tuple(params) + (max(1, min(500, int(limit or 20))),),
        ).fetchall()
        return [_row_to_dict(row, columns) for row in rows or []]
    finally:
        if owns and con is not None:
            con.close()


def _row_to_dict(row: Any, columns: Sequence[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for idx, column in enumerate(columns):
        try:
            value = row[column]
        except Exception:
            value = row[idx]
        if str(column).endswith("_json"):
            value = _json_load(value, [] if str(column) == "feature_ids_json" else {})
        out[str(column)] = value
    return out


def _candidate_is_generated_from_tables(
    con,
    *,
    model_name: str,
    candidate_version: str | None = None,
) -> bool:
    name = str(model_name or "").strip()
    version = str(candidate_version or "").strip()
    if not name:
        return False
    try:
        if _table_exists(con, "alpha_candidates"):
            row = con.execute(
                """
                SELECT generation_method
                FROM alpha_candidates
                WHERE candidate_name=?
                  AND (?='' OR candidate_version=?)
                ORDER BY created_ts DESC, id DESC
                LIMIT 1
                """,
                (name, version, version),
            ).fetchone()
            if row and str(row[0] or "").strip():
                return True
    except Exception as exc:
        _log_nonfatal("EXPERIMENT_LEDGER_ALPHA_CANDIDATE_LOOKUP_FAILED", exc)
    try:
        if _table_exists(con, "model_versions"):
            row = con.execute(
                """
                SELECT mutation_kind
                FROM model_versions
                WHERE model_name=?
                  AND (?='' OR model_version=?)
                ORDER BY updated_ts_ms DESC, created_ts_ms DESC
                LIMIT 1
                """,
                (name, version, version),
            ).fetchone()
            if row and str(row[0] or "").strip().lower() in GENERATED_MUTATION_KINDS:
                return True
    except Exception as exc:
        _log_nonfatal("EXPERIMENT_LEDGER_MODEL_VERSION_LOOKUP_FAILED", exc)
    return False


def evaluate_experiment_ledger_promotion_gate(
    *,
    model_name: str,
    candidate_version: str | None = None,
    candidate_key: str | None = None,
    generated_hint: bool = False,
    con=None,
) -> tuple[bool, dict[str, Any]]:
    """Return whether a generated candidate has mandatory ledger evidence."""

    required_by_env = str(os.environ.get("PROMOTION_EXPERIMENT_LEDGER_REQUIRED", "1")).strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    owns = con is None
    if owns:
        from engine.runtime.storage import connect, init_db

        init_db()
        con = connect(readonly=True)
    try:
        try:
            ensure_experiment_ledger_schema(con)
        except Exception as exc:
            _log_nonfatal("EXPERIMENT_LEDGER_SCHEMA_ENSURE_FAILED", exc)
        rows = fetch_experiment_ledger(
            candidate_key=candidate_key,
            candidate_name=model_name,
            candidate_version=candidate_version,
            model_name=model_name,
            limit=20,
            con=con,
        )
        generated = bool(generated_hint or _candidate_is_generated_from_tables(con, model_name=model_name, candidate_version=candidate_version))
        if rows and str(rows[0].get("source") or "").strip().lower() in GENERATED_CANDIDATE_SOURCES:
            generated = True
        if not generated:
            return True, {"enabled": bool(required_by_env), "required": False, "status": "not_generated", "passed": True}
        if not required_by_env:
            return True, {"enabled": False, "required": True, "status": "disabled", "passed": True}
        if not rows:
            return False, {
                "enabled": True,
                "required": True,
                "status": "missing_experiment_ledger",
                "passed": False,
                "model_name": str(model_name or ""),
                "candidate_version": str(candidate_version or ""),
            }
        latest = dict(rows[0])
        decision = str(latest.get("promotion_decision") or "").strip().lower()
        trial_budget = _safe_int(latest.get("trial_budget"), 0)
        trial_count = _safe_int(latest.get("trial_count"), 0)
        evidence = dict(latest.get("evidence_json") or {})
        cpcv = dict(latest.get("cpcv_json") or {})
        fdr = dict(latest.get("fdr_json") or {})
        redundancy = dict(latest.get("redundancy_json") or {})
        blockers: list[str] = []
        if decision not in PASS_DECISIONS:
            blockers.append("ledger_decision_not_passing")
        if trial_budget <= 0:
            blockers.append("trial_budget_missing")
        if trial_count <= 0:
            blockers.append("trial_count_missing")
        if trial_budget > 0 and trial_count > trial_budget:
            blockers.append("trial_budget_exceeded")
        if not any(bool(item) for item in (evidence, cpcv, fdr)):
            blockers.append("ledger_evidence_missing")
        if not redundancy:
            blockers.append("redundancy_check_missing")
        return not blockers, {
            "enabled": True,
            "required": True,
            "status": "passed" if not blockers else "failed",
            "passed": not blockers,
            "blockers": blockers,
            "latest": latest,
            "decision": decision,
            "trial_budget": int(trial_budget),
            "trial_count": int(trial_count),
        }
    finally:
        if owns and con is not None:
            con.close()
