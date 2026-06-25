"""Gated equity symbol lifecycle retirement helpers.

This module is intentionally DB-centric and side-effect narrow. It computes
retirement decisions from existing market-data evidence plus optional injected
reference/broker probes, then records decisions in the existing symbols and
universe_audit tables.
"""

from __future__ import annotations

import json
import os
import time
from collections import Counter
from typing import Any, Callable, Dict, Mapping, Optional

from engine.data.asset_map import asset_class_for_symbol
from engine.data.universe import get_instrument_metadata, get_symbols_by_status

_DAY_MS = 24 * 60 * 60 * 1000
_DEFAULT_STALE_DAYS = 45
_ACTIVE_STATUSES = ["ACTIVE", "WATCH", "COOLDOWN"]
_EXCLUDED_ASSET_CLASSES = frozenset({"FX", "CRYPTO", "COMMODITY"})


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0") or "").strip().lower()
    if not raw:
        return bool(default)
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def universe_lifecycle_enabled() -> bool:
    """Return whether equity lifecycle retirement is enabled."""
    return _env_flag("UNIVERSE_LIFECYCLE_ENABLED", False)


def reference_lifecycle_enabled() -> bool:
    """Return whether optional external lifecycle-reference checks are enabled."""
    return _env_flag("UNIVERSE_LIFECYCLE_REFERENCE_ENABLED", False)


def include_unknown_lifecycle_enabled() -> bool:
    """Return whether UNKNOWN asset-class symbols may be lifecycle evaluated."""
    return _env_flag("UNIVERSE_LIFECYCLE_INCLUDE_UNKNOWN", False)


def lifecycle_stale_ms() -> int:
    try:
        days = float(os.environ.get("UNIVERSE_LIFECYCLE_STALE_DAYS", str(_DEFAULT_STALE_DAYS)))
    except Exception:
        days = float(_DEFAULT_STALE_DAYS)
    if days <= 0:
        days = float(_DEFAULT_STALE_DAYS)
    return int(days * _DAY_MS)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _clean_symbol(symbol: Any) -> str:
    return str(symbol or "").upper().strip()


def _safe_json_loads(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if not isinstance(raw, str) or not raw.strip():
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _safe_json_dumps(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value or {}), separators=(",", ":"), sort_keys=True, default=str)


def _table_exists(con: Any, table: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table),),
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def _table_columns(con: Any, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in con.execute(f"PRAGMA table_info({table})").fetchall() or []}
    except Exception:
        return set()


def _lifecycle_asset_class(con: Any, symbol: str) -> str:
    sym = _clean_symbol(symbol)
    try:
        metadata = get_instrument_metadata(con, sym)
        if isinstance(metadata, Mapping):
            asset_class = str(metadata.get("asset_class") or "").upper().strip()
            if asset_class:
                return asset_class
    except Exception:
        pass
    try:
        return str(asset_class_for_symbol(sym) or "UNKNOWN").upper().strip() or "UNKNOWN"
    except Exception:
        return "UNKNOWN"


def _is_lifecycle_symbol(con: Any, symbol: str) -> bool:
    asset_class = _lifecycle_asset_class(con, symbol)
    if asset_class == "EQUITY":
        return True
    if asset_class == "UNKNOWN" and include_unknown_lifecycle_enabled():
        return True
    if asset_class in _EXCLUDED_ASSET_CLASSES:
        return False
    return False


def _max_ts(con: Any, table: str, symbol: str) -> int:
    if not _table_exists(con, table):
        return 0
    try:
        row = con.execute(
            f"""
            SELECT MAX(ts_ms)
            FROM {table}
            WHERE UPPER(TRIM(symbol))=?
            """,
            (_clean_symbol(symbol),),
        ).fetchone()
    except Exception:
        return 0
    try:
        return int(row[0] or 0) if row else 0
    except Exception:
        return 0


def last_market_ts_ms(con: Any, symbol: str) -> int:
    """Return the latest price/quote timestamp for a symbol, or 0 if unknown."""
    sym = _clean_symbol(symbol)
    return max(_max_ts(con, "prices", sym), _max_ts(con, "price_quotes", sym))


def _decision(
    symbol: str,
    *,
    reason: str,
    asset_class: str,
    now_ms: int,
    delist_ts_ms: Optional[int] = None,
    evidence: Optional[Mapping[str, Any]] = None,
    renamed_to: Optional[str] = None,
    merged_into: Optional[str] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "symbol": _clean_symbol(symbol),
        "asset_class": str(asset_class or "UNKNOWN").upper().strip(),
        "reason": str(reason),
        "evaluated_ts_ms": int(now_ms),
        "evidence": dict(evidence or {}),
    }
    if delist_ts_ms is not None and int(delist_ts_ms) > 0:
        out["delist_ts_ms"] = int(delist_ts_ms)
    if renamed_to:
        out["renamed_to"] = _clean_symbol(renamed_to)
    if merged_into:
        out["merged_into"] = _clean_symbol(merged_into)
    return out


def staleness_retire_candidate(
    con: Any,
    symbol: str,
    *,
    now_ms: int,
    stale_ms: Optional[int] = None,
    asset_class: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Return a stale-market-data retirement decision for an equity symbol."""
    sym = _clean_symbol(symbol)
    last_ts = last_market_ts_ms(con, sym)
    if last_ts <= 0:
        return None
    threshold_ms = int(stale_ms if stale_ms is not None else lifecycle_stale_ms())
    age_ms = int(now_ms) - int(last_ts)
    if age_ms <= threshold_ms:
        return None
    ac = str(asset_class or _lifecycle_asset_class(con, sym) or "UNKNOWN").upper().strip()
    return _decision(
        sym,
        reason="stale_inactive",
        asset_class=ac,
        now_ms=int(now_ms),
        delist_ts_ms=int(last_ts),
        evidence={
            "source": "market_data_staleness",
            "last_market_ts_ms": int(last_ts),
            "age_ms": int(age_ms),
            "stale_ms": int(threshold_ms),
        },
    )


def _reference_record(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, Mapping):
        results = payload.get("results")
        if isinstance(results, Mapping):
            return dict(results)
        if isinstance(results, list) and results and isinstance(results[0], Mapping):
            return dict(results[0])
        return dict(payload)
    return {}


def _scrub_reference_evidence(record: Mapping[str, Any]) -> Dict[str, Any]:
    allowed = {
        "active",
        "ticker",
        "symbol",
        "delisted_utc",
        "delisted_date",
        "market",
        "locale",
        "primary_exchange",
        "type",
        "status",
        "tradable",
        "merged_into",
        "renamed_to",
        "isActivelyTrading",
    }
    return {str(k): record.get(k) for k in sorted(allowed) if k in record}


def reference_retire_candidate(
    symbol: str,
    *,
    fetch_reference: Optional[Callable[[str], Any]],
    now_ms: int,
    asset_class: str,
) -> Optional[Dict[str, Any]]:
    """Return a retirement decision from an injected reference-data fetcher."""
    if fetch_reference is None:
        return None
    sym = _clean_symbol(symbol)
    try:
        record = _reference_record(fetch_reference(sym))
    except Exception:
        return None
    if not record:
        return None

    evidence = {"source": "reference", "record": _scrub_reference_evidence(record)}
    successor = _clean_symbol(record.get("renamed_to") or record.get("ticker") or record.get("symbol"))
    merged_into = _clean_symbol(record.get("merged_into"))
    active = record.get("active")
    fmp_active = record.get("isActivelyTrading")
    status = str(record.get("status") or "").strip().lower()
    delisted_raw = record.get("delisted_utc") or record.get("delisted_date")

    if successor and successor != sym:
        return _decision(
            sym,
            reason="renamed_reference",
            asset_class=asset_class,
            now_ms=int(now_ms),
            renamed_to=successor,
            evidence=evidence,
        )
    if merged_into and merged_into != sym:
        return _decision(
            sym,
            reason="merged_reference",
            asset_class=asset_class,
            now_ms=int(now_ms),
            merged_into=merged_into,
            evidence=evidence,
        )
    if active is False or fmp_active is False or status in {"inactive", "delisted", "disabled"} or delisted_raw:
        return _decision(
            sym,
            reason="delisted_reference",
            asset_class=asset_class,
            now_ms=int(now_ms),
            evidence=evidence,
        )
    return None


def broker_tradability_retire_candidate(
    symbol: str,
    *,
    probe_asset: Optional[Callable[[str], Any]],
    now_ms: int,
    asset_class: str,
) -> Optional[Dict[str, Any]]:
    """Return a retirement decision from an injected read-only broker probe."""
    if probe_asset is None:
        return None
    sym = _clean_symbol(symbol)
    try:
        payload = probe_asset(sym)
    except Exception:
        return None
    if not isinstance(payload, Mapping):
        return None
    status = str(payload.get("status") or "").strip().lower()
    tradable = payload.get("tradable")
    if status in {"inactive", "delisted", "disabled"} or tradable is False:
        return _decision(
            sym,
            reason="broker_non_tradable",
            asset_class=asset_class,
            now_ms=int(now_ms),
            evidence={
                "source": "broker_tradability",
                "status": status,
                "tradable": bool(tradable) if tradable is not None else None,
            },
        )
    return None


def evaluate_symbol_retirement(
    con: Any,
    symbol: str,
    *,
    now_ms: Optional[int] = None,
    stale_ms: Optional[int] = None,
    fetch_reference: Optional[Callable[[str], Any]] = None,
    probe_asset: Optional[Callable[[str], Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Compute the strongest lifecycle-retirement decision for one symbol."""
    sym = _clean_symbol(symbol)
    if not sym or not _is_lifecycle_symbol(con, sym):
        return None
    effective_now_ms = int(now_ms if now_ms is not None else _now_ms())
    asset_class = _lifecycle_asset_class(con, sym)
    for candidate in (
        reference_retire_candidate(sym, fetch_reference=fetch_reference, now_ms=effective_now_ms, asset_class=asset_class),
        broker_tradability_retire_candidate(sym, probe_asset=probe_asset, now_ms=effective_now_ms, asset_class=asset_class),
        staleness_retire_candidate(con, sym, now_ms=effective_now_ms, stale_ms=stale_ms, asset_class=asset_class),
    ):
        if candidate:
            return candidate
    return None


def _load_symbol_row(con: Any, symbol: str) -> Optional[Dict[str, Any]]:
    if not _table_exists(con, "symbols"):
        return None
    try:
        row = con.execute(
            """
            SELECT status, score, meta_json, last_traded_ts_ms
            FROM symbols
            WHERE UPPER(TRIM(symbol))=?
            LIMIT 1
            """,
            (_clean_symbol(symbol),),
        ).fetchone()
    except Exception:
        return None
    if not row:
        return None
    return {
        "status": str(row[0] or "").upper().strip(),
        "score": row[1],
        "meta": _safe_json_loads(row[2]),
        "last_traded_ts_ms": int(row[3] or 0) if len(row) > 3 and row[3] is not None else 0,
    }


def retire_symbol(con: Any, decision: Mapping[str, Any], *, now_ms: Optional[int] = None) -> bool:
    """Persist one retirement decision into symbols and universe_audit."""
    sym = _clean_symbol(decision.get("symbol"))
    if not sym:
        return False
    effective_now_ms = int(now_ms if now_ms is not None else _now_ms())
    if not _table_exists(con, "universe_audit"):
        return False
    row = _load_symbol_row(con, sym)
    if not row or row.get("status") == "DISABLED":
        return False

    delist_ts_ms = int(decision.get("delist_ts_ms") or 0)
    meta = dict(row.get("meta") or {})
    lifecycle = dict(meta.get("lifecycle") or {})
    lifecycle.update(
        {
            "retired": True,
            "reason": str(decision.get("reason") or ""),
            "retired_ts_ms": int(effective_now_ms),
            "asset_class": str(decision.get("asset_class") or "UNKNOWN").upper().strip(),
            "evidence": dict(decision.get("evidence") or {}),
        }
    )
    if delist_ts_ms > 0:
        lifecycle["delist_ts_ms"] = int(delist_ts_ms)
    if decision.get("renamed_to"):
        lifecycle["renamed_to"] = _clean_symbol(decision.get("renamed_to"))
    if decision.get("merged_into"):
        lifecycle["merged_into"] = _clean_symbol(decision.get("merged_into"))
    meta["lifecycle"] = lifecycle

    columns = _table_columns(con, "symbols")
    params: list[Any] = []
    assignments = ["status='DISABLED'", "meta_json=?", "updated_ts_ms=?"]
    params.extend([_safe_json_dumps(meta), int(effective_now_ms)])
    if "last_traded_ts_ms" in columns and delist_ts_ms > 0:
        assignments.append("last_traded_ts_ms=COALESCE(?, last_traded_ts_ms)")
        params.append(int(delist_ts_ms))
    params.append(sym)
    cur = con.execute(
        f"""
        UPDATE symbols
        SET {", ".join(assignments)}
        WHERE UPPER(TRIM(symbol))=? AND status != 'DISABLED'
        """,
        tuple(params),
    )
    if int(getattr(cur, "rowcount", 0) or 0) <= 0:
        return False

    con.execute(
        """
        INSERT OR REPLACE INTO universe_audit(
          ts_ms, symbol, status_before, status_after, include, score,
          reasons_json, features_json
        )
        VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            int(effective_now_ms),
            sym,
            str(row.get("status") or ""),
            "DISABLED",
            0,
            row.get("score"),
            _safe_json_dumps(
                {
                    "reason": str(decision.get("reason") or ""),
                    "renamed_to": decision.get("renamed_to"),
                    "merged_into": decision.get("merged_into"),
                    "delist_ts_ms": delist_ts_ms or None,
                }
            ),
            _safe_json_dumps(dict(decision.get("evidence") or {})),
        ),
    )
    return True


def run_lifecycle_once(
    con: Any | None = None,
    *,
    now_ms: Optional[int] = None,
    stale_ms: Optional[int] = None,
    fetch_reference: Optional[Callable[[str], Any]] = None,
    probe_asset: Optional[Callable[[str], Any]] = None,
) -> Dict[str, Any]:
    """Evaluate active/watch/cooldown symbols once and retire confirmed equities."""
    if not universe_lifecycle_enabled():
        return {"ok": True, "enabled": False, "scanned": 0, "retired": 0}

    owns_connection = con is None
    if owns_connection:
        from engine.runtime.storage import connect

        con = connect()

    assert con is not None
    effective_now_ms = int(now_ms if now_ms is not None else _now_ms())
    effective_stale_ms = int(stale_ms if stale_ms is not None else lifecycle_stale_ms())
    reference_requested = reference_lifecycle_enabled()
    reference_fetcher = fetch_reference if reference_requested else None
    reason_counts: Counter[str] = Counter()
    summary: Dict[str, Any] = {
        "ok": True,
        "enabled": True,
        "reference_enabled": bool(reference_requested),
        "reference_fetcher_configured": bool(reference_fetcher),
        "broker_probe_configured": bool(probe_asset),
        "now_ms": int(effective_now_ms),
        "stale_ms": int(effective_stale_ms),
        "scanned": 0,
        "evaluated": 0,
        "skipped_non_equity": 0,
        "candidates": 0,
        "retired": 0,
        "errors": [],
    }
    if reference_requested and reference_fetcher is None:
        summary["reference_blocked"] = True
        summary["reference_blocker"] = "reference_fetcher_unconfigured"

    try:
        symbols = get_symbols_by_status(con, list(_ACTIVE_STATUSES), limit=None)
    except Exception as e:
        summary["ok"] = False
        summary["errors"].append(f"symbol_scan_failed:{type(e).__name__}")
        if owns_connection:
            try:
                con.close()
            except Exception:
                pass
        return summary

    for symbol in symbols:
        sym = _clean_symbol(symbol)
        if not sym:
            continue
        summary["scanned"] += 1
        try:
            if not _is_lifecycle_symbol(con, sym):
                summary["skipped_non_equity"] += 1
                continue
            summary["evaluated"] += 1
            decision = evaluate_symbol_retirement(
                con,
                sym,
                now_ms=effective_now_ms,
                stale_ms=effective_stale_ms,
                fetch_reference=reference_fetcher,
                probe_asset=probe_asset,
            )
            if not decision:
                continue
            summary["candidates"] += 1
            if retire_symbol(con, decision, now_ms=effective_now_ms):
                summary["retired"] += 1
                reason_counts.update([str(decision.get("reason") or "unknown")])
        except Exception as e:
            summary["ok"] = False
            if len(summary["errors"]) < 10:
                summary["errors"].append(f"{sym}:{type(e).__name__}")

    summary["reason_counts"] = dict(sorted(reason_counts.items()))
    try:
        con.commit()
    except Exception:
        pass
    if owns_connection:
        try:
            con.close()
        except Exception:
            pass
    return summary


__all__ = [
    "broker_tradability_retire_candidate",
    "evaluate_symbol_retirement",
    "include_unknown_lifecycle_enabled",
    "last_market_ts_ms",
    "lifecycle_stale_ms",
    "reference_lifecycle_enabled",
    "reference_retire_candidate",
    "retire_symbol",
    "run_lifecycle_once",
    "staleness_retire_candidate",
    "universe_lifecycle_enabled",
]
