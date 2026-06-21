"""Repository boundary for durable model competition tables.

Production code should mutate ``model_marketplace_scores`` and
``champion_assignments`` through this module so score and assignment state do
not drift between marketplace scoring and champion promotion paths.
"""

from __future__ import annotations

import json
import logging
import math
import time
from typing import Any, Dict, Iterable, Optional


MARKETPLACE_SCORES_TABLE = "model_marketplace_scores"
CHAMPION_ASSIGNMENTS_TABLE = "champion_assignments"
LOGGER = logging.getLogger(__name__)

ASSIGNMENT_STATES = {"shadow", "challenger", "champion", "retired"}
ALLOWED_ASSIGNMENT_TRANSITIONS = {
    ("shadow", "challenger"),
    ("challenger", "champion"),
    ("champion", "retired"),
    ("challenger", "shadow"),
}


class IllegalChampionTransition(RuntimeError):
    """Raised when a model attempts to bypass the shadow/challenger path."""


def now_ms() -> int:
    return int(time.time() * 1000)


def normalize_model_id(model_id: Any) -> str:
    mid = str(model_id or "").strip()
    return mid or "baseline"


def safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return int(default)
    if isinstance(value, str) and not value.strip():
        return int(default)
    try:
        return int(value)
    except Exception:
        return int(default)


def safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return float(default)
    if isinstance(value, str) and not value.strip():
        return float(default)
    try:
        return float(value)
    except Exception:
        return float(default)


def json_sanitize(value: Any) -> Any:
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(key): json_sanitize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_sanitize(item) for item in value]
    return value


def json_dumps(value: Any) -> str:
    return json.dumps(json_sanitize(value), separators=(",", ":"), sort_keys=True, allow_nan=False)


def parse_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _table_columns_or_none(con, table_name: str) -> Optional[set[str]]:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall()
    except Exception:
        return None
    return {str(row[1] or "").strip() for row in (rows or []) if row and len(row) > 1}


def _alter_add_column_if_missing(con, table_name: str, column_name: str, ddl: str) -> None:
    columns = _table_columns_or_none(con, table_name)
    if columns is None or str(column_name) in columns:
        return
    con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}")


class CompetitionRepository:
    """Single write boundary for marketplace scores and champion assignments."""

    def __init__(self, con) -> None:
        self.con = con

    def ensure_champion_assignments_schema(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS champion_assignments (
              scope TEXT NOT NULL,
              symbol TEXT NOT NULL,
              horizon_s INTEGER NOT NULL DEFAULT 0,
              model_name TEXT NOT NULL,
              challenger_name TEXT,
              regime TEXT NOT NULL DEFAULT 'global',
              state TEXT NOT NULL DEFAULT 'champion',
              assigned_ts_ms INTEGER NOT NULL,
              updated_ts_ms INTEGER NOT NULL,
              meta_json TEXT,
              PRIMARY KEY (scope, symbol, horizon_s)
            )
            """
        )
        for column_name, ddl in (
            ("scope", "TEXT NOT NULL DEFAULT ''"),
            ("symbol", "TEXT NOT NULL DEFAULT ''"),
            ("horizon_s", "INTEGER NOT NULL DEFAULT 0"),
            ("model_name", "TEXT NOT NULL DEFAULT ''"),
            ("challenger_name", "TEXT"),
            ("regime", "TEXT NOT NULL DEFAULT 'global'"),
            ("state", "TEXT NOT NULL DEFAULT 'champion'"),
            ("assigned_ts_ms", "INTEGER NOT NULL DEFAULT 0"),
            ("updated_ts_ms", "INTEGER NOT NULL DEFAULT 0"),
            ("meta_json", "TEXT"),
        ):
            _alter_add_column_if_missing(self.con, CHAMPION_ASSIGNMENTS_TABLE, column_name, ddl)
        try:
            self.con.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_champion_assignments_scope_key
                  ON champion_assignments(scope, symbol, horizon_s)
                """
            )
        except Exception:
            # Existing SQLite fixtures may already define the primary key.
            LOGGER.debug("champion assignment unique index creation skipped", exc_info=True)
        try:
            self.con.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_champion_assignments_state
                  ON champion_assignments(state, updated_ts_ms)
                """
            )
            self.con.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_champion_assignments_model
                  ON champion_assignments(model_name, regime, updated_ts_ms)
                """
            )
        except Exception:
            LOGGER.warning("champion assignment secondary index creation failed", exc_info=True)

    def current_assignment_state(self, payload: Dict[str, Any]) -> Optional[str]:
        row = self.con.execute(
            """
            SELECT state
            FROM champion_assignments
            WHERE scope=?
              AND symbol=?
              AND horizon_s=?
              AND model_name=?
            LIMIT 1
            """,
            (
                str(payload["scope"]),
                str(payload["symbol"]),
                int(payload["horizon_s"]),
                str(payload["model_name"]),
            ),
        ).fetchone()
        if not row:
            return None
        return str(row[0] or "").strip().lower() or None

    def validate_assignment_transition(self, payload: Dict[str, Any]) -> None:
        new_state = str(payload.get("state") or "champion").strip().lower()
        if new_state not in ASSIGNMENT_STATES:
            raise IllegalChampionTransition(f"unknown champion assignment state: {new_state}")
        current_state = self.current_assignment_state(payload)
        if new_state == "champion" and current_state != "challenger":
            raise IllegalChampionTransition(
                f"cannot transition model {payload['model_name']} to champion from "
                f"{current_state or 'unassigned'}; current state must be challenger"
            )
        if current_state is None or current_state == new_state:
            return
        if (current_state, new_state) not in ALLOWED_ASSIGNMENT_TRANSITIONS:
            raise IllegalChampionTransition(
                f"illegal champion assignment transition for model {payload['model_name']}: "
                f"{current_state} -> {new_state}"
            )

    def set_champion_assignment(
        self,
        *,
        scope: str,
        symbol: str,
        model_name: str,
        horizon_s: int = 0,
        challenger_name: str = "",
        regime: str = "global",
        state: str = "champion",
        meta: Optional[Dict[str, Any]] = None,
        assigned_ts_ms: Optional[int] = None,
        updated_ts_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        self.ensure_champion_assignments_schema()
        ts_now = int(updated_ts_ms or now_ms())
        payload = {
            "scope": str(scope),
            "symbol": str(symbol).upper().strip(),
            "horizon_s": int(horizon_s),
            "model_name": str(model_name),
            "challenger_name": str(challenger_name or ""),
            "regime": str(regime or "global"),
            "state": str(state or "champion").strip().lower(),
            "assigned_ts_ms": int(assigned_ts_ms or ts_now),
            "updated_ts_ms": int(ts_now),
            "meta": dict(meta or {}),
        }
        self.validate_assignment_transition(payload)
        self.con.execute(
            """
            INSERT INTO champion_assignments(
              scope, symbol, horizon_s, model_name, challenger_name, regime, state, assigned_ts_ms, updated_ts_ms, meta_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(scope, symbol, horizon_s) DO UPDATE SET
              model_name=excluded.model_name,
              challenger_name=excluded.challenger_name,
              regime=excluded.regime,
              state=excluded.state,
              updated_ts_ms=excluded.updated_ts_ms,
              meta_json=excluded.meta_json
            """,
            (
                payload["scope"],
                payload["symbol"],
                payload["horizon_s"],
                payload["model_name"],
                payload["challenger_name"],
                payload["regime"],
                payload["state"],
                payload["assigned_ts_ms"],
                payload["updated_ts_ms"],
                json_dumps(payload["meta"]),
            ),
        )
        return payload

    def get_champion_assignment(self, *, scope: str, symbol: str, horizon_s: int = 0) -> Dict[str, Any]:
        self.ensure_champion_assignments_schema()
        row = self.con.execute(
            """
            SELECT scope, symbol, horizon_s, model_name, challenger_name, regime, state, assigned_ts_ms, updated_ts_ms, meta_json
            FROM champion_assignments
            WHERE scope=? AND symbol=? AND horizon_s=?
            """,
            (str(scope), str(symbol).upper().strip(), int(horizon_s)),
        ).fetchone()
        if not row:
            return {}
        return {
            "scope": str(row[0] or ""),
            "symbol": str(row[1] or ""),
            "horizon_s": safe_int(row[2], 0),
            "model_name": str(row[3] or ""),
            "challenger_name": str(row[4] or ""),
            "regime": str(row[5] or "global"),
            "state": str(row[6] or "champion"),
            "assigned_ts_ms": safe_int(row[7], 0),
            "updated_ts_ms": safe_int(row[8], 0),
            "meta": parse_json_dict(row[9]),
        }

    def clear_champion_assignment(self, *, scope: str, symbol: str, horizon_s: int) -> None:
        self.ensure_champion_assignments_schema()
        self.con.execute(
            """
            DELETE FROM champion_assignments
            WHERE scope=? AND symbol=? AND horizon_s=?
            """,
            (
                str(scope or ""),
                str(symbol or "").upper().strip(),
                int(horizon_s or 0),
            ),
        )

    def set_marketplace_stage_for_score_keys(
        self,
        rows: Iterable[Dict[str, Any]],
        *,
        champion_name: str,
        updated_ts_ms: Optional[int] = None,
    ) -> None:
        ts_now = int(updated_ts_ms or now_ms())
        champion = str(champion_name or "")
        for row in rows or []:
            self.con.execute(
                """
                UPDATE model_marketplace_scores
                SET stage=?, updated_ts_ms=?
                WHERE model_name=? AND symbol=? AND horizon_s=? AND regime=?
                  AND model_id=?
                """,
                (
                    "champion" if str((row or {}).get("model_name") or "") == champion else "challenger",
                    int(ts_now),
                    str((row or {}).get("model_name") or ""),
                    str((row or {}).get("symbol") or "").upper().strip(),
                    int((row or {}).get("horizon_s") or 0),
                    str((row or {}).get("regime") or "global"),
                    normalize_model_id((row or {}).get("model_id")),
                ),
            )

    def upsert_marketplace_score(
        self,
        row: Dict[str, Any],
        *,
        meta: Optional[Dict[str, Any]] = None,
        updated_ts_ms: Optional[int] = None,
        update_pnl_on_conflict: bool = True,
    ) -> Dict[str, Any]:
        ts_now = int(updated_ts_ms or row.get("updated_ts_ms") or now_ms())
        meta_payload = dict(meta if meta is not None else row.get("meta") or {})
        set_clause = """
          stage=excluded.stage,
          score=excluded.score,
          trades=excluded.trades,
          wins=excluded.wins,
          losses=excluded.losses,
          gross_pnl=excluded.gross_pnl,
          net_pnl=excluded.net_pnl,
          avg_confidence=excluded.avg_confidence,
          last_signal_ts_ms=excluded.last_signal_ts_ms,
          updated_ts_ms=excluded.updated_ts_ms,
          meta_json=excluded.meta_json
        """
        if not update_pnl_on_conflict:
            set_clause = """
              stage=excluded.stage,
              score=excluded.score,
              trades=excluded.trades,
              wins=excluded.wins,
              losses=excluded.losses,
              avg_confidence=excluded.avg_confidence,
              last_signal_ts_ms=excluded.last_signal_ts_ms,
              updated_ts_ms=excluded.updated_ts_ms,
              meta_json=excluded.meta_json
            """
        self.con.execute(
            f"""
            INSERT INTO model_marketplace_scores(
              model_id, model_name, symbol, horizon_s, regime, stage, score, trades, wins, losses,
              gross_pnl, net_pnl, avg_confidence, last_signal_ts_ms, updated_ts_ms, meta_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(model_id, model_name, symbol, horizon_s, regime) DO UPDATE SET
            {set_clause}
            """,
            (
                normalize_model_id(row.get("model_id")),
                str(row.get("model_name") or ""),
                str(row.get("symbol") or "").upper().strip(),
                int(row.get("horizon_s") or 0),
                str(row.get("regime") or "global"),
                str(row.get("stage") or "challenger"),
                safe_float(row.get("score"), 0.0),
                safe_int(row.get("trades"), 0),
                safe_int(row.get("wins"), 0),
                safe_int(row.get("losses"), 0),
                safe_float(row.get("gross_pnl"), 0.0),
                safe_float(row.get("net_pnl"), 0.0),
                safe_float(row.get("avg_confidence"), 0.0),
                safe_int(row.get("last_signal_ts_ms"), 0),
                int(ts_now),
                json_dumps(meta_payload),
            ),
        )
        out = dict(row or {})
        out["model_id"] = normalize_model_id(out.get("model_id"))
        out["symbol"] = str(out.get("symbol") or "").upper().strip()
        out["updated_ts_ms"] = int(ts_now)
        out["meta"] = meta_payload
        return out

    def delete_all_marketplace_scores(self) -> None:
        self.con.execute("DELETE FROM model_marketplace_scores")
