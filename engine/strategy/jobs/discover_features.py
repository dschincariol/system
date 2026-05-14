"""Automated feature discovery job.

Accepted candidates are registered at stage ``shadow`` only. This job never
promotes discovered features into the live serving path.
"""

from __future__ import annotations
import logging

import json
import os
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from engine.strategy.discovery.base import CandidateFeature, EvaluationResult, now_ms
from engine.strategy.discovery.pysr_discoverer import PySRDiscoverer
from engine.strategy.discovery.registry import (
    ACCEPTED_DECISION,
    FEATURE_STAGE_SHADOW,
    ensure_discovery_schema,
    has_evaluation,
    record_candidate,
    record_evaluation,
    register_feature,
)
from engine.strategy.discovery.tsfresh_discoverer import TsfreshDiscoverer
from engine.strategy.statistics.multiple_testing import bh_fdr

JOB_NAME = "discover_features"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
DEFAULT_TSFRESH_VALUE_COLUMNS = ("close", "price", "px", "last", "value")


def default_discoverers(*, feature_ids: Sequence[str] | None = None) -> list[Any]:
    return [
        TsfreshDiscoverer(
            window=int(os.environ.get("DISCOVERY_TSFRESH_WINDOW", "180") or 180),
            max_candidates=int(os.environ.get("DISCOVERY_TSFRESH_MAX_CANDIDATES", "200") or 200),
            value_columns=_env_csv("DISCOVERY_TSFRESH_VALUE_COLUMNS", DEFAULT_TSFRESH_VALUE_COLUMNS),
        ),
        PySRDiscoverer(
            target_column=str(os.environ.get("DISCOVERY_TARGET_COLUMN", "target") or "target"),
            niterations=int(os.environ.get("DISCOVERY_PYSR_ITERATIONS", "100") or 100),
            timeout_seconds=int(os.environ.get("DISCOVERY_PYSR_TIMEOUT_SECONDS", "30") or 30),
            max_complexity=int(os.environ.get("DISCOVERY_PYSR_MAX_COMPLEXITY", "12") or 12),
            top_k=int(os.environ.get("DISCOVERY_PYSR_TOP_K", "10") or 10),
            primitive_columns=list(feature_ids or []),
        ),
    ]


def run_discovery(
    *,
    symbols: Sequence[str] | None = None,
    train_frames: Mapping[str, pd.DataFrame] | None = None,
    test_frames: Mapping[str, pd.DataFrame] | None = None,
    target: str | Sequence[float] | pd.Series = "target",
    discoverers: Sequence[Any] | None = None,
    con=None,
    q_threshold: float = 0.10,
    t_threshold: float = 3.0,
    feature_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    batch_ts = now_ms()
    owns = con is None
    if owns:
        from engine.runtime.storage import connect, init_db

        init_db()
        con = connect(readonly=False)
    try:
        ensure_discovery_schema(con)
        symbol_list = _resolve_symbols(symbols, train_frames=train_frames, con=con)
        registry_feature_ids = _resolve_feature_ids_for_discovery(feature_ids)
        engines = list(discoverers) if discoverers is not None else default_discoverers(feature_ids=registry_feature_ids)
        summary: dict[str, Any] = {
            "symbols": len(symbol_list),
            "feature_primitives": len(registry_feature_ids),
            "proposed": 0,
            "evaluated": 0,
            "skipped": 0,
            "accepted": 0,
            "rejected": 0,
            "degenerate": 0,
            "registered_shadow": 0,
            "batch_ts": int(batch_ts),
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
            symbol_stats = {"proposed": 0, "evaluated": 0, "skipped": 0, "accepted": 0, "rejected": 0}
            if train_df.empty or test_df.empty:
                summary["by_symbol"][str(symbol)] = {**symbol_stats, "skipped_reason": "empty_dataset"}
                continue

            for discoverer in engines:
                try:
                    candidates = list(discoverer.propose(str(symbol), train_df) or [])
                except Exception:
                    candidates = []
                summary["proposed"] += len(candidates)
                symbol_stats["proposed"] += len(candidates)
                pending: list[tuple[CandidateFeature, int, EvaluationResult]] = []

                for candidate in candidates:
                    record = record_candidate(candidate, con=con, ts=batch_ts)
                    if has_evaluation(int(record.id), con=con):
                        summary["skipped"] += 1
                        symbol_stats["skipped"] += 1
                        continue
                    try:
                        result = discoverer.evaluate(candidate, test_df, target)
                    except Exception as exc:
                        result = EvaluationResult(
                            candidate_hash=str(candidate.hash),
                            feature_id=str(candidate.feature_id),
                            t_stat=0.0,
                            p_value=1.0,
                            decision="degenerate",
                            n_obs=0,
                            diagnostics={"reason": f"evaluation_failed:{type(exc).__name__}"},
                        )
                    pending.append((candidate, int(record.id), result))

                _gate_and_persist(
                    pending,
                    con=con,
                    batch_ts=batch_ts,
                    q_threshold=float(q_threshold),
                    t_threshold=float(t_threshold),
                    summary=summary,
                    symbol_stats=symbol_stats,
                )

            summary["by_symbol"][str(symbol)] = dict(symbol_stats)
        if owns:
            con.commit()
        return summary
    finally:
        if owns and con is not None:
            con.close()


def main() -> int:
    from engine.runtime.storage import (
        acquire_job_lock,
        init_db,
        put_job_heartbeat,
        release_job_lock,
        touch_job_lock,
    )

    init_db()
    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        return 0
    try:
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps({"phase": "start"}, separators=(",", ":"), sort_keys=True),
        )
        summary = run_discovery()
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps({"phase": "done", **summary}, separators=(",", ":"), sort_keys=True),
        )
        return 0
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


def _gate_and_persist(
    pending: Sequence[tuple[CandidateFeature, int, EvaluationResult]],
    *,
    con,
    batch_ts: int,
    q_threshold: float,
    t_threshold: float,
    summary: dict[str, Any],
    symbol_stats: dict[str, int],
) -> None:
    if not pending:
        return
    p_values = [float(result.p_value if np.isfinite(result.p_value) else 1.0) for _candidate, _id, result in pending]
    correction = bh_fdr(p_values, q=float(q_threshold), labels=[candidate.hash for candidate, _id, _result in pending])
    for idx, (candidate, candidate_id, result) in enumerate(pending):
        q_value = float(correction.q_values[idx])
        if str(result.decision) == "degenerate":
            decision = "degenerate"
            summary["degenerate"] += 1
        elif not (q_value < float(q_threshold)):
            decision = "fdr_failed"
        elif not (abs(float(result.t_stat)) > float(t_threshold)):
            decision = "tstat_failed"
        else:
            decision = ACCEPTED_DECISION

        gated = result.with_gate(q_value=q_value, decision=decision)
        record_evaluation(int(candidate_id), gated, con=con, ts=batch_ts)
        summary["evaluated"] += 1
        symbol_stats["evaluated"] += 1
        if decision == ACCEPTED_DECISION:
            register_feature(
                candidate,
                candidate_id=int(candidate_id),
                stage=FEATURE_STAGE_SHADOW,
                metadata={
                    "t_stat": float(gated.t_stat),
                    "p_value": float(gated.p_value),
                    "q_value": float(gated.q_value or 0.0),
                    "oos_ic": gated.oos_ic,
                    "discovery_job": JOB_NAME,
                },
                con=con,
                ts=batch_ts,
            )
            summary["accepted"] += 1
            summary["registered_shadow"] += 1
            symbol_stats["accepted"] += 1
        else:
            summary["rejected"] += 1
            symbol_stats["rejected"] += 1


def _resolve_symbols(
    symbols: Sequence[str] | None,
    *,
    train_frames: Mapping[str, pd.DataFrame] | None,
    con,
) -> list[str]:
    if symbols:
        return _dedupe_symbols(symbols)
    if train_frames:
        return _dedupe_symbols(train_frames.keys())
    env_symbols = str(os.environ.get("DISCOVERY_SYMBOLS", "") or "").strip()
    if env_symbols:
        return _dedupe_symbols(part for part in env_symbols.split(","))
    try:
        rows = con.execute(
            """
            SELECT symbol
            FROM symbols
            WHERE COALESCE(status, '') IN ('ACTIVE', 'WATCH', 'active', 'watch')
            ORDER BY updated_ts_ms DESC
            LIMIT 10
            """
        ).fetchall()
        resolved = _dedupe_symbols(row[0] for row in rows or [])
        if resolved:
            return resolved
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    return ["SPY"]


def _resolve_frames(
    symbol: str,
    *,
    train_frames: Mapping[str, pd.DataFrame] | None,
    test_frames: Mapping[str, pd.DataFrame] | None,
    con,
    feature_ids: Sequence[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if train_frames is not None and test_frames is not None:
        return pd.DataFrame(train_frames.get(str(symbol), pd.DataFrame())).copy(), pd.DataFrame(
            test_frames.get(str(symbol), pd.DataFrame())
        ).copy()
    full = _load_symbol_frame(str(symbol), con=con, feature_ids=feature_ids)
    if full.empty:
        return pd.DataFrame(), pd.DataFrame()
    split = max(1, min(len(full.index) - 1, int(len(full.index) * 0.6)))
    return full.iloc[:split].reset_index(drop=True), full.iloc[split:].reset_index(drop=True)


def _load_symbol_frame(
    symbol: str,
    *,
    con,
    limit: int = 1200,
    feature_ids: Sequence[str] | None = None,
) -> pd.DataFrame:
    labeled = _load_labeled_feature_frame(str(symbol), con=con, feature_ids=feature_ids, limit=int(limit))
    min_labeled_rows = int(os.environ.get("DISCOVERY_MIN_LABELED_ROWS", "64") or 64)
    if len(labeled.index) >= max(2, int(min_labeled_rows)):
        return labeled
    return _load_price_frame(str(symbol), con=con, limit=int(limit))


def _load_price_frame(symbol: str, *, con, limit: int = 1200) -> pd.DataFrame:
    rows = con.execute(
        """
        SELECT ts_ms, COALESCE(price, px) AS close
        FROM prices
        WHERE symbol=?
          AND COALESCE(price, px) IS NOT NULL
        ORDER BY ts_ms DESC
        LIMIT ?
        """,
        (str(symbol).upper(), int(limit)),
    ).fetchall()
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(
        [{"ts_ms": int(row[0]), "close": float(row[1])} for row in rows or []],
        columns=["ts_ms", "close"],
    ).sort_values("ts_ms")
    frame["return_1"] = frame["close"].pct_change()
    frame["log_return_1"] = np.log(frame["close"]).diff()
    frame["target"] = frame["close"].pct_change().shift(-1)
    return frame.replace([np.inf, -np.inf], np.nan).dropna().reset_index(drop=True)


def _load_labeled_feature_frame(
    symbol: str,
    *,
    con,
    feature_ids: Sequence[str] | None,
    limit: int = 1200,
) -> pd.DataFrame:
    ids = _dedupe_feature_ids(feature_ids or [])
    if not ids:
        return pd.DataFrame()
    target_expr = "COALESCE(le.net_z, le.gross_z, l.impact_z, le.net_ret, le.gross_ret, l.realized_ret, l.baseline_ret)"
    try:
        rows = con.execute(
            f"""
            SELECT
              l.event_id,
              COALESCE(e.ts_ms, le.ts_ms) AS ts_ms,
              {target_expr} AS target,
              COALESCE(e.title, '') AS title,
              COALESCE(e.body, '') AS body,
              COALESCE(e.source, le.source, '') AS source,
              COALESCE(l.horizon_s, le.horizon_s, 0) AS horizon_s
            FROM labels l
            JOIN events e ON e.id = l.event_id
            LEFT JOIN labels_exec le
              ON le.event_id = l.event_id
             AND UPPER(le.symbol) = UPPER(l.symbol)
             AND (le.horizon_s = l.horizon_s OR le.horizon_s IS NULL OR l.horizon_s IS NULL)
            WHERE UPPER(l.symbol) = ?
              AND {target_expr} IS NOT NULL
              AND COALESCE(e.ts_ms, le.ts_ms) IS NOT NULL
            ORDER BY COALESCE(e.ts_ms, le.ts_ms) DESC
            LIMIT ?
            """,
            (str(symbol).upper(), int(limit)),
        ).fetchall()
    except Exception:
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()

    try:
        from engine.strategy.feature_registry import compute_feature_snapshot
    except Exception:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for row in reversed(list(rows or [])):
        try:
            ts_ms = int(row[1])
            target_value = float(row[2])
        except Exception:
            continue
        if not np.isfinite(target_value):
            continue
        event = {
            "id": row[0],
            "event_id": row[0],
            "ts_ms": int(ts_ms),
            "ref_ts_ms": int(ts_ms),
            "title": str(row[3] or ""),
            "body": str(row[4] or ""),
            "source": str(row[5] or ""),
            "horizon_s": int(row[6] or 0),
        }
        try:
            snapshot = compute_feature_snapshot(event=event, symbol=str(symbol), feature_ids=list(ids)) or {}
        except Exception:
            snapshot = {}
        record: dict[str, Any] = {
            "ts_ms": int(ts_ms),
            "close": _price_at_or_before(str(symbol), int(ts_ms), con=con),
            "target": float(target_value),
        }
        for feature_id in ids:
            record[str(feature_id)] = _finite_float(snapshot.get(str(feature_id)))
        records.append(record)

    if not records:
        return pd.DataFrame()
    frame = pd.DataFrame(records).sort_values("ts_ms").replace([np.inf, -np.inf], np.nan)
    if "close" in frame.columns:
        frame["return_1"] = pd.to_numeric(frame["close"], errors="coerce").pct_change()
        frame["log_return_1"] = np.log(pd.to_numeric(frame["close"], errors="coerce")).diff()
    return frame.dropna(subset=["target"]).reset_index(drop=True)


def _price_at_or_before(symbol: str, ts_ms: int, *, con) -> float:
    try:
        row = con.execute(
            """
            SELECT COALESCE(price, px) AS close
            FROM prices
            WHERE symbol=?
              AND ts_ms <= ?
              AND COALESCE(price, px) IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (str(symbol).upper(), int(ts_ms)),
        ).fetchone()
    except Exception:
        row = None
    if not row:
        return float("nan")
    return _finite_float(row[0])


def _resolve_feature_ids_for_discovery(feature_ids: Sequence[str] | None = None) -> list[str]:
    explicit = _dedupe_feature_ids(feature_ids or [])
    if explicit:
        return explicit
    env_feature_ids = _env_csv("DISCOVERY_FEATURE_IDS", ())
    if env_feature_ids:
        return _dedupe_feature_ids(env_feature_ids)
    try:
        from engine.strategy.feature_registry import registered_feature_ids

        ids = registered_feature_ids(include_shadow=False)
    except Exception:
        ids = []
    if not ids:
        try:
            from engine.strategy.feature_registry import default_feature_ids

            ids = default_feature_ids()
        except Exception:
            ids = []
    max_features = int(os.environ.get("DISCOVERY_MAX_REGISTRY_FEATURES", "128") or 128)
    return _dedupe_feature_ids(ids)[: max(1, int(max_features))]


def _env_csv(name: str, default: Sequence[str]) -> list[str]:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return [str(item) for item in list(default or []) if str(item)]
    return [part.strip() for part in raw.split(",") if part.strip()]


def _dedupe_feature_ids(feature_ids: Sequence[str] | Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for feature_id in list(feature_ids or []):
        text = str(feature_id or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _finite_float(value: Any) -> float:
    try:
        out = float(value)
    except Exception:
        return float("nan")
    return float(out) if np.isfinite(out) else float("nan")


def _dedupe_symbols(symbols: Sequence[str] | Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for symbol in list(symbols or []):
        text = str(symbol or "").strip().upper()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


if __name__ == "__main__":
    raise SystemExit(main())
