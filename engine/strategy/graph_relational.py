"""Shadow-only graph and relational feature snapshots.

The graph layer is deliberately constrained to point-in-time inputs and shadow
outputs.  It can emit feature vectors or shadow predictions for research, but
promotion and live serving must not treat these features as live-authoritative.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from typing import Any, Iterable, Mapping, Sequence

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger


GRAPH_RELATIONAL_GROUP = "graph_relational_v1"
GRAPH_RELATIONAL_PREFIX = "graph.relational_v1."
GRAPH_RELATIONAL_GRAPH_ID = "graph_relational_v1"
GRAPH_RELATIONAL_SNAPSHOT_VERSION = 1
GRAPH_RELATIONAL_FEATURE_IDS = [
    f"{GRAPH_RELATIONAL_PREFIX}neighbor_count",
    f"{GRAPH_RELATIONAL_PREFIX}relation_type_count",
    f"{GRAPH_RELATIONAL_PREFIX}sector_peer_count",
    f"{GRAPH_RELATIONAL_PREFIX}industry_peer_count",
    f"{GRAPH_RELATIONAL_PREFIX}rolling_corr_mean_abs",
    f"{GRAPH_RELATIONAL_PREFIX}rolling_corr_max_abs",
    f"{GRAPH_RELATIONAL_PREFIX}etf_ownership_weight",
    f"{GRAPH_RELATIONAL_PREFIX}inst_13f_shared_manager_count",
    f"{GRAPH_RELATIONAL_PREFIX}options_comovement_score",
    f"{GRAPH_RELATIONAL_PREFIX}news_co_mentions_count",
    f"{GRAPH_RELATIONAL_PREFIX}supply_chain_degree",
    f"{GRAPH_RELATIONAL_PREFIX}pit_available",
]
GRAPH_RELATIONSHIP_TYPES = {
    "sector",
    "industry",
    "rolling_correlation",
    "etf_ownership",
    "supply_chain",
    "inst_13f_ownership",
    "options_comovement",
    "news_co_mentions",
}

LOG = get_logger("engine.strategy.graph_relational")
_WARNED_NONFATAL_KEYS: set[str] = set()


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
        component="engine.strategy.graph_relational",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name))
    if raw is None or str(raw).strip() == "":
        return bool(default)
    text = str(raw).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = os.environ.get(str(name))
    try:
        value = int(str(raw).strip()) if raw is not None and str(raw).strip() else int(default)
    except Exception:
        value = int(default)
    return int(max(int(minimum), min(int(maximum), int(value))))


def _env_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
    raw = os.environ.get(str(name))
    try:
        value = float(str(raw).strip()) if raw is not None and str(raw).strip() else float(default)
    except Exception:
        value = float(default)
    if not math.isfinite(value):
        value = float(default)
    return float(max(float(minimum), min(float(maximum), float(value))))


def graph_relational_features_enabled() -> bool:
    return bool(_env_bool("USE_GRAPH_RELATIONAL_FEATURES", False))


def graph_max_neighbors() -> int:
    return _env_int("GRAPH_RELATIONAL_MAX_NEIGHBORS", 24, minimum=1, maximum=512)


def graph_corr_lookback_rows() -> int:
    return _env_int("GRAPH_RELATIONAL_CORR_LOOKBACK_ROWS", 96, minimum=8, maximum=4096)


def graph_corr_min_abs() -> float:
    return _env_float("GRAPH_RELATIONAL_CORR_MIN_ABS", 0.35, minimum=0.0, maximum=1.0)


def graph_news_lookback_hours() -> int:
    return _env_int("GRAPH_RELATIONAL_NEWS_LOOKBACK_HOURS", 72, minimum=1, maximum=24 * 30)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if value in (None, "", b"", bytearray()):
        return {}
    try:
        raw = value.decode("utf-8", "replace") if isinstance(value, (bytes, bytearray)) else str(value)
        parsed = json.loads(raw)
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if value in (None, "", b"", bytearray()):
        return []
    try:
        raw = value.decode("utf-8", "replace") if isinstance(value, (bytes, bytearray)) else str(value)
        parsed = json.loads(raw)
    except Exception:
        return []
    return list(parsed) if isinstance(parsed, list) else []


def _row_get(row: Any, key: str, index: int, default: Any = None) -> Any:
    if hasattr(row, "keys"):
        try:
            return row[key]
        except Exception:
            return default
    try:
        return row[index]
    except Exception:
        return default


def _symbol(value: Any) -> str:
    return str(value or "").upper().strip()


def _table_exists(con: Any, table: str) -> bool:
    name = str(table or "").strip()
    if not name or not name.replace("_", "").isalnum():
        return False
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def _columns(con: Any, table: str) -> set[str]:
    name = str(table or "").strip()
    if not name or not name.replace("_", "").isalnum():
        return set()
    try:
        rows = con.execute(f"PRAGMA table_info({name})").fetchall()
        cols = {str(_row_get(row, "name", 1) or "") for row in rows or []}
        cols.discard("")
        if cols:
            return cols
    except Exception:
        # no-op-guard: allow - non-SQLite adapters fall through to information_schema.
        pass
    try:
        rows = con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name=?
            """,
            (str(name),),
        ).fetchall()
        return {str(_row_get(row, "column_name", 0) or "") for row in rows or [] if str(_row_get(row, "column_name", 0) or "")}
    except Exception:
        return set()


def ensure_graph_relational_schema(con: Any) -> None:
    """Create versioned graph snapshot and optional generic edge tables."""

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS graph_relationship_edges (
          source_symbol TEXT NOT NULL,
          target_symbol TEXT NOT NULL,
          relationship_type TEXT NOT NULL,
          weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
          source_ts_ms BIGINT,
          availability_ts_ms BIGINT NOT NULL,
          source TEXT,
          meta_json JSONB NOT NULL DEFAULT '{}',
          PRIMARY KEY(source_symbol, target_symbol, relationship_type, availability_ts_ms)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_graph_relationship_edges_source_avail
          ON graph_relationship_edges(source_symbol, relationship_type, availability_ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_graph_relationship_edges_target_avail
          ON graph_relationship_edges(target_symbol, relationship_type, availability_ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS graph_relational_snapshots (
          symbol TEXT NOT NULL,
          ts_ms BIGINT NOT NULL,
          graph_id TEXT NOT NULL,
          snapshot_version BIGINT NOT NULL,
          feature_ids_json JSONB NOT NULL,
          features_json JSONB NOT NULL,
          edge_counts_json JSONB NOT NULL,
          relationships_json JSONB NOT NULL,
          source_timestamps_json JSONB NOT NULL,
          availability_json JSONB NOT NULL,
          metadata_json JSONB NOT NULL,
          created_ts_ms BIGINT NOT NULL,
          PRIMARY KEY(symbol, ts_ms, graph_id)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_graph_relational_snapshots_symbol_ts
          ON graph_relational_snapshots(symbol, ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_graph_relational_snapshots_graph_ts
          ON graph_relational_snapshots(graph_id, ts_ms DESC)
        """
    )


def _edge(
    *,
    source_symbol: str,
    target_symbol: str,
    relationship_type: str,
    weight: float = 1.0,
    source_ts_ms: Any = None,
    availability_ts_ms: Any = None,
    source: str = "",
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "source_symbol": _symbol(source_symbol),
        "target_symbol": _symbol(target_symbol),
        "relationship_type": str(relationship_type or "").strip(),
        "weight": float(_safe_float(weight, 1.0)),
        "source_ts_ms": (_safe_int(source_ts_ms, 0) or None),
        "availability_ts_ms": (_safe_int(availability_ts_ms, 0) or None),
        "source": str(source or "").strip(),
        "metadata": dict(metadata or {}),
    }


def _valid_edge(edge: Mapping[str, Any], *, symbol: str, ts_ms: int) -> bool:
    if _symbol(edge.get("source_symbol")) != _symbol(symbol) and _symbol(edge.get("target_symbol")) != _symbol(symbol):
        return False
    peer = _symbol(edge.get("target_symbol")) if _symbol(edge.get("source_symbol")) == _symbol(symbol) else _symbol(edge.get("source_symbol"))
    if not peer or peer == _symbol(symbol):
        return False
    if str(edge.get("relationship_type") or "") not in GRAPH_RELATIONSHIP_TYPES:
        return False
    availability = _safe_int(edge.get("availability_ts_ms"), 0)
    source_ts = _safe_int(edge.get("source_ts_ms"), 0)
    if availability <= 0 or availability > int(ts_ms):
        return False
    if source_ts > int(ts_ms):
        return False
    return True


def _dedupe_edges(edges: Iterable[Mapping[str, Any]], *, symbol: str, ts_ms: int) -> list[dict[str, Any]]:
    best: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in edges or []:
        edge = dict(raw or {})
        if not _valid_edge(edge, symbol=str(symbol), ts_ms=int(ts_ms)):
            continue
        rel = str(edge.get("relationship_type") or "")
        peer = _symbol(edge.get("target_symbol")) if _symbol(edge.get("source_symbol")) == _symbol(symbol) else _symbol(edge.get("source_symbol"))
        key = (rel, peer)
        existing = best.get(key)
        if existing is None or abs(_safe_float(edge.get("weight"), 0.0)) > abs(_safe_float(existing.get("weight"), 0.0)):
            best[key] = dict(edge)
    out = list(best.values())
    out.sort(
        key=lambda edge: (
            str(edge.get("relationship_type") or ""),
            _symbol(edge.get("target_symbol")) if _symbol(edge.get("source_symbol")) == _symbol(symbol) else _symbol(edge.get("source_symbol")),
        )
    )
    return out[: max(1, graph_max_neighbors()) * 8]


def _profile_from_meta(meta: Mapping[str, Any]) -> tuple[str, str]:
    sector = ""
    industry = ""
    for key in ("sector", "gics_sector", "industry_sector", "sector_name"):
        value = str((meta or {}).get(key) or "").strip()
        if value:
            sector = value
            break
    for key in ("industry", "gics_industry", "industry_name", "sub_industry"):
        value = str((meta or {}).get(key) or "").strip()
        if value:
            industry = value
            break
    return sector, industry


def _symbol_profile(con: Any, symbol: str, ts_ms: int) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "symbol": _symbol(symbol),
        "sector": "",
        "industry": "",
        "availability_ts_ms": None,
        "source_ts_ms": None,
        "sources": [],
    }
    sym = _symbol(symbol)
    if _table_exists(con, "symbols"):
        try:
            row = con.execute(
                """
                SELECT meta_json, updated_ts_ms
                FROM symbols
                WHERE symbol=?
                  AND COALESCE(updated_ts_ms, 0) <= ?
                LIMIT 1
                """,
                (sym, int(ts_ms)),
            ).fetchone()
        except Exception as exc:
            _warn_nonfatal("GRAPH_RELATIONAL_SYMBOL_PROFILE_LOAD_FAILED", exc, once_key="symbol_profile_symbols")
            row = None
        if row:
            meta = _json_obj(_row_get(row, "meta_json", 0))
            sector, industry = _profile_from_meta(meta)
            if sector:
                profile["sector"] = str(sector)
            if industry:
                profile["industry"] = str(industry)
            updated = _safe_int(_row_get(row, "updated_ts_ms", 1), 0)
            if updated > 0:
                profile["availability_ts_ms"] = updated
                profile["source_ts_ms"] = updated
            profile["sources"].append("symbols.meta_json")
    if _table_exists(con, "gov_symbol_sector_map"):
        try:
            row = con.execute(
                """
                SELECT sector, updated_ts_ms
                FROM gov_symbol_sector_map
                WHERE symbol=?
                  AND COALESCE(updated_ts_ms, 0) <= ?
                LIMIT 1
                """,
                (sym, int(ts_ms)),
            ).fetchone()
        except Exception as exc:
            _warn_nonfatal("GRAPH_RELATIONAL_GOV_SECTOR_LOAD_FAILED", exc, once_key="symbol_profile_gov")
            row = None
        if row and str(_row_get(row, "sector", 0) or "").strip():
            profile["sector"] = str(_row_get(row, "sector", 0) or "").strip()
            updated = _safe_int(_row_get(row, "updated_ts_ms", 1), 0)
            if updated > 0:
                profile["availability_ts_ms"] = max(_safe_int(profile.get("availability_ts_ms"), 0), updated)
                profile["source_ts_ms"] = max(_safe_int(profile.get("source_ts_ms"), 0), updated)
            profile["sources"].append("gov_symbol_sector_map")
    return profile


def _candidate_symbols(con: Any, *, ts_ms: int, peer_symbols: Sequence[str] | None = None) -> list[str]:
    explicit = [_symbol(value) for value in list(peer_symbols or []) if _symbol(value)]
    if explicit:
        return sorted(dict.fromkeys(explicit))
    symbols: list[str] = []
    if _table_exists(con, "symbols"):
        try:
            rows = con.execute(
                """
                SELECT symbol
                FROM symbols
                WHERE COALESCE(updated_ts_ms, 0) <= ?
                  AND UPPER(COALESCE(status, '')) IN ('ACTIVE','WATCH','COOLDOWN','')
                ORDER BY score DESC, updated_ts_ms DESC, symbol ASC
                LIMIT ?
                """,
                (int(ts_ms), int(max(2, graph_max_neighbors() * 3))),
            ).fetchall()
            symbols.extend(_symbol(_row_get(row, "symbol", 0)) for row in rows or [])
        except Exception as exc:
            _warn_nonfatal("GRAPH_RELATIONAL_SYMBOL_CANDIDATES_LOAD_FAILED", exc, once_key="candidate_symbols")
    if not symbols and _table_exists(con, "prices"):
        try:
            rows = con.execute(
                """
                SELECT DISTINCT symbol
                FROM prices
                WHERE ts_ms <= ?
                ORDER BY symbol ASC
                LIMIT ?
                """,
                (int(ts_ms), int(max(2, graph_max_neighbors() * 3))),
            ).fetchall()
            symbols.extend(_symbol(_row_get(row, "symbol", 0)) for row in rows or [])
        except Exception as exc:
            _warn_nonfatal("GRAPH_RELATIONAL_PRICE_SYMBOLS_LOAD_FAILED", exc, once_key="price_candidate_symbols")
    return sorted(dict.fromkeys(sym for sym in symbols if sym))


def _sector_industry_edges(con: Any, *, symbol: str, ts_ms: int, peer_symbols: Sequence[str] | None) -> list[dict[str, Any]]:
    target = _symbol_profile(con, symbol, int(ts_ms))
    sector = str(target.get("sector") or "").strip().lower()
    industry = str(target.get("industry") or "").strip().lower()
    if not sector and not industry:
        return []
    edges: list[dict[str, Any]] = []
    for peer in _candidate_symbols(con, ts_ms=int(ts_ms), peer_symbols=peer_symbols):
        if peer == _symbol(symbol):
            continue
        profile = _symbol_profile(con, peer, int(ts_ms))
        availability = max(_safe_int(target.get("availability_ts_ms"), 0), _safe_int(profile.get("availability_ts_ms"), 0))
        source_ts = max(_safe_int(target.get("source_ts_ms"), 0), _safe_int(profile.get("source_ts_ms"), 0))
        if sector and str(profile.get("sector") or "").strip().lower() == sector:
            edges.append(
                _edge(
                    source_symbol=symbol,
                    target_symbol=peer,
                    relationship_type="sector",
                    weight=1.0,
                    source_ts_ms=source_ts,
                    availability_ts_ms=availability,
                    source="symbol_sector_metadata",
                    metadata={"sector": str(target.get("sector") or "")},
                )
            )
        if industry and str(profile.get("industry") or "").strip().lower() == industry:
            edges.append(
                _edge(
                    source_symbol=symbol,
                    target_symbol=peer,
                    relationship_type="industry",
                    weight=1.0,
                    source_ts_ms=source_ts,
                    availability_ts_ms=availability,
                    source="symbol_industry_metadata",
                    metadata={"industry": str(target.get("industry") or "")},
                )
            )
    return edges


def _price_points(con: Any, symbol: str, ts_ms: int, limit: int) -> list[tuple[int, float]]:
    if not _table_exists(con, "prices"):
        return []
    try:
        rows = con.execute(
            """
            SELECT ts_ms, COALESCE(price, px) AS value
            FROM prices
            WHERE symbol=?
              AND ts_ms <= ?
              AND COALESCE(price, px) IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (_symbol(symbol), int(ts_ms), int(limit)),
        ).fetchall()
    except Exception as exc:
        _warn_nonfatal("GRAPH_RELATIONAL_PRICE_POINTS_LOAD_FAILED", exc, once_key=f"price_points:{_symbol(symbol)}")
        rows = []
    points = []
    for row in reversed(list(rows or [])):
        row_ts = _safe_int(_row_get(row, "ts_ms", 0), 0)
        price = _safe_float(_row_get(row, "value", 1), math.nan)
        if row_ts > 0 and math.isfinite(price):
            points.append((int(row_ts), float(price)))
    return points


def _returns(points: Sequence[tuple[int, float]]) -> list[float]:
    values: list[float] = []
    series = list(points or [])
    for idx in range(1, len(series)):
        prev = float(series[idx - 1][1])
        cur = float(series[idx][1])
        if prev > 0.0 and cur > 0.0:
            values.append(float(math.log(cur / prev)))
    return values


def _corr(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    n = min(len(xs or []), len(ys or []))
    if n < 4:
        return None
    x = [float(v) for v in list(xs)[-n:]]
    y = [float(v) for v in list(ys)[-n:]]
    mean_x = sum(x) / float(n)
    mean_y = sum(y) / float(n)
    var_x = sum((v - mean_x) ** 2 for v in x)
    var_y = sum((v - mean_y) ** 2 for v in y)
    if var_x <= 1e-18 or var_y <= 1e-18:
        return None
    cov = sum((a - mean_x) * (b - mean_y) for a, b in zip(x, y))
    return float(max(-1.0, min(1.0, cov / math.sqrt(var_x * var_y))))


def _rolling_correlation_edges(con: Any, *, symbol: str, ts_ms: int, peer_symbols: Sequence[str] | None) -> list[dict[str, Any]]:
    target_points = _price_points(con, symbol, int(ts_ms), graph_corr_lookback_rows())
    target_returns = _returns(target_points)
    if len(target_returns) < 4:
        return []
    target_last_ts = int(target_points[-1][0])
    edges: list[dict[str, Any]] = []
    for peer in _candidate_symbols(con, ts_ms=int(ts_ms), peer_symbols=peer_symbols):
        if peer == _symbol(symbol):
            continue
        peer_points = _price_points(con, peer, int(ts_ms), graph_corr_lookback_rows())
        value = _corr(target_returns, _returns(peer_points))
        if value is None or abs(float(value)) < graph_corr_min_abs():
            continue
        peer_last_ts = int(peer_points[-1][0]) if peer_points else 0
        edges.append(
            _edge(
                source_symbol=symbol,
                target_symbol=peer,
                relationship_type="rolling_correlation",
                weight=abs(float(value)),
                source_ts_ms=max(int(target_last_ts), int(peer_last_ts)),
                availability_ts_ms=max(int(target_last_ts), int(peer_last_ts)),
                source="prices.rolling_correlation",
                metadata={"correlation": float(value), "lookback_rows": int(graph_corr_lookback_rows())},
            )
        )
    edges.sort(key=lambda edge: abs(_safe_float(edge.get("weight"), 0.0)), reverse=True)
    return edges[: graph_max_neighbors()]


def _generic_relationship_edges(
    con: Any,
    *,
    symbol: str,
    ts_ms: int,
    relationship_types: Sequence[str],
) -> list[dict[str, Any]]:
    if not _table_exists(con, "graph_relationship_edges"):
        return []
    wanted = {str(rel) for rel in relationship_types if str(rel) in GRAPH_RELATIONSHIP_TYPES}
    if not wanted:
        return []
    try:
        rows = con.execute(
            """
            SELECT source_symbol, target_symbol, relationship_type, weight, source_ts_ms,
                   availability_ts_ms, source, meta_json
            FROM graph_relationship_edges
            WHERE (source_symbol=? OR target_symbol=?)
              AND COALESCE(availability_ts_ms, 0) <= ?
              AND COALESCE(source_ts_ms, availability_ts_ms, 0) <= ?
            ORDER BY availability_ts_ms DESC
            LIMIT ?
            """,
            (_symbol(symbol), _symbol(symbol), int(ts_ms), int(ts_ms), int(max(1, graph_max_neighbors() * 8))),
        ).fetchall()
    except Exception as exc:
        _warn_nonfatal("GRAPH_RELATIONAL_GENERIC_EDGES_LOAD_FAILED", exc, once_key="generic_relationship_edges")
        return []
    out: list[dict[str, Any]] = []
    for row in rows or []:
        rel = str(_row_get(row, "relationship_type", 2) or "").strip()
        if rel not in wanted:
            continue
        out.append(
            _edge(
                source_symbol=_row_get(row, "source_symbol", 0),
                target_symbol=_row_get(row, "target_symbol", 1),
                relationship_type=rel,
                weight=_safe_float(_row_get(row, "weight", 3), 1.0),
                source_ts_ms=_row_get(row, "source_ts_ms", 4),
                availability_ts_ms=_row_get(row, "availability_ts_ms", 5),
                source=str(_row_get(row, "source", 6) or "graph_relationship_edges"),
                metadata=_json_obj(_row_get(row, "meta_json", 7)),
            )
        )
    return out


def _inst_13f_edges(con: Any, *, symbol: str, ts_ms: int) -> list[dict[str, Any]]:
    if not _table_exists(con, "inst_13f_holdings"):
        return []
    try:
        rows = con.execute(
            """
            WITH target_managers AS (
              SELECT DISTINCT manager_cik
              FROM inst_13f_holdings
              WHERE symbol = ?
                AND COALESCE(availability_ts_ms, 0) <= ?
            )
            SELECT h.symbol,
                   COUNT(DISTINCT h.manager_cik) AS manager_count,
                   MAX(COALESCE(h.report_ts_ms, h.ts_ms, 0)) AS source_ts_ms,
                   MAX(COALESCE(h.availability_ts_ms, 0)) AS availability_ts_ms,
                   SUM(COALESCE(h.value_usd, h.value_thousands * 1000, 0)) AS value_usd
            FROM inst_13f_holdings h
            JOIN target_managers tm ON tm.manager_cik = h.manager_cik
            WHERE h.symbol <> ?
              AND COALESCE(h.availability_ts_ms, 0) <= ?
              AND h.symbol IS NOT NULL
              AND h.symbol <> ''
            GROUP BY h.symbol
            ORDER BY manager_count DESC, value_usd DESC, h.symbol ASC
            LIMIT ?
            """,
            (_symbol(symbol), int(ts_ms), _symbol(symbol), int(ts_ms), int(graph_max_neighbors())),
        ).fetchall()
    except Exception as exc:
        _warn_nonfatal("GRAPH_RELATIONAL_13F_EDGES_LOAD_FAILED", exc, once_key="inst_13f_edges")
        return []
    return [
        _edge(
            source_symbol=symbol,
            target_symbol=_row_get(row, "symbol", 0),
            relationship_type="inst_13f_ownership",
            weight=_safe_float(_row_get(row, "manager_count", 1), 0.0),
            source_ts_ms=_row_get(row, "source_ts_ms", 2),
            availability_ts_ms=_row_get(row, "availability_ts_ms", 3),
            source="inst_13f_holdings.shared_manager",
            metadata={"shared_manager_count": _safe_int(_row_get(row, "manager_count", 1), 0)},
        )
        for row in rows or []
    ]


def _options_score_points(con: Any, symbol: str, ts_ms: int) -> list[tuple[int, int, float]]:
    if not _table_exists(con, "options_symbol_features"):
        return []
    try:
        rows = con.execute(
            """
            SELECT bucket_ts_ms, snapshot_ts_ms,
                   COALESCE(signal_score, unusual_volume_score, call_put_volume_ratio, 0.0) AS score
            FROM options_symbol_features
            WHERE symbol=?
              AND bucket_ts_ms <= ?
              AND snapshot_ts_ms <= ?
            ORDER BY bucket_ts_ms DESC, snapshot_ts_ms DESC
            LIMIT ?
            """,
            (_symbol(symbol), int(ts_ms), int(ts_ms), int(graph_corr_lookback_rows())),
        ).fetchall()
    except Exception as exc:
        _warn_nonfatal("GRAPH_RELATIONAL_OPTIONS_POINTS_LOAD_FAILED", exc, once_key=f"options_points:{_symbol(symbol)}")
        return []
    return [
        (
            _safe_int(_row_get(row, "bucket_ts_ms", 0), 0),
            _safe_int(_row_get(row, "snapshot_ts_ms", 1), 0),
            _safe_float(_row_get(row, "score", 2), 0.0),
        )
        for row in reversed(list(rows or []))
        if _safe_int(_row_get(row, "bucket_ts_ms", 0), 0) > 0
    ]


def _options_comovement_edges(con: Any, *, symbol: str, ts_ms: int, peer_symbols: Sequence[str] | None) -> list[dict[str, Any]]:
    target_points = _options_score_points(con, symbol, int(ts_ms))
    if len(target_points) < 4:
        return []
    target_values = [point[2] for point in target_points]
    target_avail = max(point[1] for point in target_points)
    edges: list[dict[str, Any]] = []
    for peer in _candidate_symbols(con, ts_ms=int(ts_ms), peer_symbols=peer_symbols):
        if peer == _symbol(symbol):
            continue
        peer_points = _options_score_points(con, peer, int(ts_ms))
        value = _corr(target_values, [point[2] for point in peer_points])
        if value is None or abs(float(value)) < graph_corr_min_abs():
            continue
        peer_avail = max((point[1] for point in peer_points), default=0)
        edges.append(
            _edge(
                source_symbol=symbol,
                target_symbol=peer,
                relationship_type="options_comovement",
                weight=abs(float(value)),
                source_ts_ms=max(target_avail, peer_avail),
                availability_ts_ms=max(target_avail, peer_avail),
                source="options_symbol_features.comovement",
                metadata={"correlation": float(value)},
            )
        )
    edges.sort(key=lambda edge: abs(_safe_float(edge.get("weight"), 0.0)), reverse=True)
    return edges[: graph_max_neighbors()]


def _news_co_mentions_edges(con: Any, *, symbol: str, ts_ms: int) -> list[dict[str, Any]]:
    if not _table_exists(con, "events"):
        return []
    window_start = int(ts_ms) - int(graph_news_lookback_hours() * 3600 * 1000)
    try:
        rows = con.execute(
            """
            SELECT ts_ms, title, body
            FROM events
            WHERE symbol=?
              AND ts_ms <= ?
              AND ts_ms >= ?
            ORDER BY ts_ms DESC
            LIMIT 256
            """,
            (_symbol(symbol), int(ts_ms), int(window_start)),
        ).fetchall()
    except Exception as exc:
        _warn_nonfatal("GRAPH_RELATIONAL_NEWS_CO_MENTIONS_LOAD_FAILED", exc, once_key="news_co_mentions_edges")
        return []
    try:
        from engine.data.universe import extract_symbol_candidates
    except Exception:
        extract_symbol_candidates = None
    counts: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        row_ts = _safe_int(_row_get(row, "ts_ms", 0), 0)
        text = f"{str(_row_get(row, 'title', 1) or '')} {str(_row_get(row, 'body', 2) or '')}"
        if callable(extract_symbol_candidates):
            peers = [_symbol(value) for value in extract_symbol_candidates(text)]
        else:
            peers = []
        for peer in peers:
            if not peer or peer == _symbol(symbol):
                continue
            rec = counts.setdefault(peer, {"count": 0, "latest_ts_ms": 0})
            rec["count"] = _safe_int(rec.get("count"), 0) + 1
            rec["latest_ts_ms"] = max(_safe_int(rec.get("latest_ts_ms"), 0), int(row_ts))
    edges = [
        _edge(
            source_symbol=symbol,
            target_symbol=peer,
            relationship_type="news_co_mentions",
            weight=float(rec.get("count") or 0.0),
            source_ts_ms=int(rec.get("latest_ts_ms") or 0),
            availability_ts_ms=int(rec.get("latest_ts_ms") or 0),
            source="events.news_co_mentions",
            metadata={"count": int(rec.get("count") or 0), "lookback_hours": int(graph_news_lookback_hours())},
        )
        for peer, rec in counts.items()
    ]
    edges.sort(key=lambda edge: _safe_float(edge.get("weight"), 0.0), reverse=True)
    return edges[: graph_max_neighbors()]


def _edge_counts(edges: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts = {rel: 0 for rel in sorted(GRAPH_RELATIONSHIP_TYPES)}
    for edge in edges or []:
        rel = str(edge.get("relationship_type") or "")
        if rel in counts:
            counts[rel] += 1
    return counts


def _relationship_hash(edges: Sequence[Mapping[str, Any]]) -> str:
    stable = [
        {
            "source_symbol": _symbol(edge.get("source_symbol")),
            "target_symbol": _symbol(edge.get("target_symbol")),
            "relationship_type": str(edge.get("relationship_type") or ""),
            "weight": round(_safe_float(edge.get("weight"), 0.0), 10),
            "source_ts_ms": _safe_int(edge.get("source_ts_ms"), 0),
            "availability_ts_ms": _safe_int(edge.get("availability_ts_ms"), 0),
            "source": str(edge.get("source") or ""),
        }
        for edge in edges or []
    ]
    raw = _json_dumps(stable).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _features_from_edges(edges: Sequence[Mapping[str, Any]], *, feature_ids: Sequence[str]) -> dict[str, float]:
    counts = _edge_counts(edges)
    corr_weights = [
        abs(_safe_float(edge.get("weight"), 0.0))
        for edge in edges or []
        if str(edge.get("relationship_type") or "") == "rolling_correlation"
    ]
    values = {
        f"{GRAPH_RELATIONAL_PREFIX}neighbor_count": float(
            len(
                {
                    _symbol(edge.get("target_symbol")) if _symbol(edge.get("source_symbol")) else ""
                    for edge in edges or []
                }
            )
        ),
        f"{GRAPH_RELATIONAL_PREFIX}relation_type_count": float(sum(1 for count in counts.values() if count > 0)),
        f"{GRAPH_RELATIONAL_PREFIX}sector_peer_count": float(counts.get("sector", 0)),
        f"{GRAPH_RELATIONAL_PREFIX}industry_peer_count": float(counts.get("industry", 0)),
        f"{GRAPH_RELATIONAL_PREFIX}rolling_corr_mean_abs": float(sum(corr_weights) / len(corr_weights)) if corr_weights else 0.0,
        f"{GRAPH_RELATIONAL_PREFIX}rolling_corr_max_abs": float(max(corr_weights)) if corr_weights else 0.0,
        f"{GRAPH_RELATIONAL_PREFIX}etf_ownership_weight": float(
            sum(_safe_float(edge.get("weight"), 0.0) for edge in edges or [] if str(edge.get("relationship_type") or "") == "etf_ownership")
        ),
        f"{GRAPH_RELATIONAL_PREFIX}inst_13f_shared_manager_count": float(
            sum(_safe_float(edge.get("weight"), 0.0) for edge in edges or [] if str(edge.get("relationship_type") or "") == "inst_13f_ownership")
        ),
        f"{GRAPH_RELATIONAL_PREFIX}options_comovement_score": float(
            sum(_safe_float(edge.get("weight"), 0.0) for edge in edges or [] if str(edge.get("relationship_type") or "") == "options_comovement")
        ),
        f"{GRAPH_RELATIONAL_PREFIX}news_co_mentions_count": float(
            sum(_safe_float(edge.get("weight"), 0.0) for edge in edges or [] if str(edge.get("relationship_type") or "") == "news_co_mentions")
        ),
        f"{GRAPH_RELATIONAL_PREFIX}supply_chain_degree": float(counts.get("supply_chain", 0)),
        f"{GRAPH_RELATIONAL_PREFIX}pit_available": 1.0,
    }
    return {str(fid): float(values.get(str(fid), 0.0)) for fid in list(feature_ids or GRAPH_RELATIONAL_FEATURE_IDS)}


def build_graph_relational_snapshot(
    *,
    symbol: str,
    ts_ms: int,
    feature_ids: Sequence[str] | None = None,
    peer_symbols: Sequence[str] | None = None,
    con: Any = None,
) -> dict[str, Any]:
    """Build a versioned, PIT-safe graph snapshot for one symbol."""

    owns = False
    if con is None:
        from engine.runtime.storage import connect

        con = connect(readonly=True)
        owns = True
    symbol_key = _symbol(symbol)
    anchor_ts_ms = int(ts_ms)
    ids = [
        str(fid)
        for fid in list(feature_ids or GRAPH_RELATIONAL_FEATURE_IDS)
        if str(fid or "").startswith(GRAPH_RELATIONAL_PREFIX)
    ] or list(GRAPH_RELATIONAL_FEATURE_IDS)
    try:
        raw_edges: list[dict[str, Any]] = []
        raw_edges.extend(_sector_industry_edges(con, symbol=symbol_key, ts_ms=anchor_ts_ms, peer_symbols=peer_symbols))
        raw_edges.extend(_rolling_correlation_edges(con, symbol=symbol_key, ts_ms=anchor_ts_ms, peer_symbols=peer_symbols))
        raw_edges.extend(
            _generic_relationship_edges(
                con,
                symbol=symbol_key,
                ts_ms=anchor_ts_ms,
                relationship_types=("etf_ownership", "supply_chain", "inst_13f_ownership", "options_comovement", "news_co_mentions"),
            )
        )
        raw_edges.extend(_inst_13f_edges(con, symbol=symbol_key, ts_ms=anchor_ts_ms))
        raw_edges.extend(_options_comovement_edges(con, symbol=symbol_key, ts_ms=anchor_ts_ms, peer_symbols=peer_symbols))
        raw_edges.extend(_news_co_mentions_edges(con, symbol=symbol_key, ts_ms=anchor_ts_ms))
        edges = _dedupe_edges(raw_edges, symbol=symbol_key, ts_ms=anchor_ts_ms)
        max_availability_ts_ms = max([_safe_int(edge.get("availability_ts_ms"), 0) for edge in edges] or [int(anchor_ts_ms)])
        max_source_ts_ms = max([_safe_int(edge.get("source_ts_ms"), 0) for edge in edges] or [int(anchor_ts_ms)])
        pit_safe = bool(max_availability_ts_ms <= int(anchor_ts_ms) and max_source_ts_ms <= int(anchor_ts_ms))
        edge_counts = _edge_counts(edges)
        features = _features_from_edges(edges, feature_ids=ids)
        if not pit_safe:
            features = {fid: 0.0 for fid in ids}
            features[f"{GRAPH_RELATIONAL_PREFIX}pit_available"] = 0.0
        relationship_hash = _relationship_hash(edges)
        source_timestamps = {
            "anchor_ts_ms": int(anchor_ts_ms),
            "max_source_ts_ms": int(max_source_ts_ms),
            "max_availability_ts_ms": int(max_availability_ts_ms),
            "relationship_count": int(len(edges)),
            "relationship_hash": str(relationship_hash),
            "graph_id": GRAPH_RELATIONAL_GRAPH_ID,
            "snapshot_version": int(GRAPH_RELATIONAL_SNAPSHOT_VERSION),
        }
        availability = {
            GRAPH_RELATIONAL_GROUP: bool(pit_safe),
            "graph_relational": bool(pit_safe),
        }
        metadata = {
            "graph_id": GRAPH_RELATIONAL_GRAPH_ID,
            "snapshot_version": int(GRAPH_RELATIONAL_SNAPSHOT_VERSION),
            "feature_ids": list(ids),
            "feature_prefix": GRAPH_RELATIONAL_PREFIX,
            "relationship_types": sorted(GRAPH_RELATIONSHIP_TYPES),
            "edge_counts": dict(edge_counts),
            "relationship_hash": str(relationship_hash),
            "snapshot_available": True,
            "pit_safe": bool(pit_safe),
            "max_source_ts_ms": int(max_source_ts_ms),
            "max_availability_ts_ms": int(max_availability_ts_ms),
            "direct_trading_authority": False,
            "stage": "shadow",
            "source": "graph_relational_snapshot_builder",
        }
        return {
            "symbol": str(symbol_key),
            "ts_ms": int(anchor_ts_ms),
            "graph_id": GRAPH_RELATIONAL_GRAPH_ID,
            "snapshot_version": int(GRAPH_RELATIONAL_SNAPSHOT_VERSION),
            "feature_ids": list(ids),
            "features": dict(features),
            "edge_counts": dict(edge_counts),
            "relationships": list(edges),
            "source_timestamps": dict(source_timestamps),
            "availability": dict(availability),
            "metadata": dict(metadata),
        }
    finally:
        if owns:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal("GRAPH_RELATIONAL_CONNECTION_CLOSE_FAILED", exc, once_key="graph_relational_close")


def store_graph_relational_snapshots(snapshots: Iterable[Mapping[str, Any]], *, con: Any = None) -> int:
    snapshot_list = [dict(snap or {}) for snap in snapshots or []]
    owns = False
    if con is None:
        from engine.runtime.storage import connect

        con = connect(readonly=False)
        owns = True
    try:
        ensure_graph_relational_schema(con)
        now_ms = int(time.time() * 1000)
        rows = []
        for snap in snapshot_list:
            symbol_key = _symbol(snap.get("symbol"))
            if not symbol_key:
                continue
            rows.append(
                (
                    symbol_key,
                    int(snap.get("ts_ms") or 0),
                    str(snap.get("graph_id") or GRAPH_RELATIONAL_GRAPH_ID),
                    int(snap.get("snapshot_version") or GRAPH_RELATIONAL_SNAPSHOT_VERSION),
                    _json_dumps(list(snap.get("feature_ids") or [])),
                    _json_dumps(dict(snap.get("features") or {})),
                    _json_dumps(dict(snap.get("edge_counts") or {})),
                    _json_dumps(list(snap.get("relationships") or [])),
                    _json_dumps(dict(snap.get("source_timestamps") or {})),
                    _json_dumps(dict(snap.get("availability") or {})),
                    _json_dumps(dict(snap.get("metadata") or {})),
                    int(now_ms),
                )
            )
        if not rows:
            return 0
        con.executemany(
            """
            INSERT OR REPLACE INTO graph_relational_snapshots(
              symbol, ts_ms, graph_id, snapshot_version, feature_ids_json,
              features_json, edge_counts_json, relationships_json,
              source_timestamps_json, availability_json, metadata_json, created_ts_ms
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
        )
        if owns:
            con.commit()
        return int(len(rows))
    finally:
        if owns:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal("GRAPH_RELATIONAL_STORE_CLOSE_FAILED", exc, once_key="graph_relational_store_close")


def load_graph_relational_snapshot(
    *,
    symbol: str,
    ts_ms: int,
    graph_id: str = GRAPH_RELATIONAL_GRAPH_ID,
    exact: bool = False,
    con: Any = None,
) -> dict[str, Any] | None:
    owns = False
    if con is None:
        from engine.runtime.storage import connect

        con = connect(readonly=True)
        owns = True
    try:
        if not _table_exists(con, "graph_relational_snapshots"):
            return None
        comparator = "=" if bool(exact) else "<="
        row = con.execute(
            f"""
            SELECT symbol, ts_ms, graph_id, snapshot_version, feature_ids_json,
                   features_json, edge_counts_json, relationships_json,
                   source_timestamps_json, availability_json, metadata_json, created_ts_ms
            FROM graph_relational_snapshots
            WHERE symbol=?
              AND graph_id=?
              AND ts_ms {comparator} ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (_symbol(symbol), str(graph_id), int(ts_ms)),
        ).fetchone()
        if not row:
            return None
        return {
            "symbol": str(_row_get(row, "symbol", 0) or ""),
            "ts_ms": _safe_int(_row_get(row, "ts_ms", 1), 0),
            "graph_id": str(_row_get(row, "graph_id", 2) or ""),
            "snapshot_version": _safe_int(_row_get(row, "snapshot_version", 3), 0),
            "feature_ids": [str(fid) for fid in _json_list(_row_get(row, "feature_ids_json", 4))],
            "features": _json_obj(_row_get(row, "features_json", 5)),
            "edge_counts": _json_obj(_row_get(row, "edge_counts_json", 6)),
            "relationships": _json_list(_row_get(row, "relationships_json", 7)),
            "source_timestamps": _json_obj(_row_get(row, "source_timestamps_json", 8)),
            "availability": _json_obj(_row_get(row, "availability_json", 9)),
            "metadata": _json_obj(_row_get(row, "metadata_json", 10)),
            "created_ts_ms": _safe_int(_row_get(row, "created_ts_ms", 11), 0),
        }
    finally:
        if owns:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal("GRAPH_RELATIONAL_LOAD_CLOSE_FAILED", exc, once_key="graph_relational_load_close")


def graph_metadata_from_snapshot(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    snap = dict(snapshot or {})
    metadata = {**snap, **dict(snap.get("metadata") or {})}
    source_timestamps = dict(snap.get("source_timestamps") or {})
    return {
        "graph_id": str(metadata.get("graph_id") or snap.get("graph_id") or GRAPH_RELATIONAL_GRAPH_ID),
        "snapshot_version": int(metadata.get("snapshot_version") or snap.get("snapshot_version") or 0),
        "feature_ids": [str(fid) for fid in list(metadata.get("feature_ids") or snap.get("feature_ids") or [])],
        "relationship_hash": str(metadata.get("relationship_hash") or source_timestamps.get("relationship_hash") or ""),
        "snapshot_available": bool(metadata.get("snapshot_available", bool(snap))),
        "pit_safe": bool(metadata.get("pit_safe", False)),
        "max_source_ts_ms": _safe_int(metadata.get("max_source_ts_ms") or source_timestamps.get("max_source_ts_ms"), 0),
        "max_availability_ts_ms": _safe_int(
            metadata.get("max_availability_ts_ms") or source_timestamps.get("max_availability_ts_ms"),
            0,
        ),
        "direct_trading_authority": bool(metadata.get("direct_trading_authority", False)),
        "stage": str(metadata.get("stage") or "shadow"),
    }


def graph_train_serve_parity(
    train_metadata: Mapping[str, Any],
    serve_metadata: Mapping[str, Any],
) -> dict[str, Any]:
    train = graph_metadata_from_snapshot(train_metadata)
    serve = graph_metadata_from_snapshot(serve_metadata)
    blockers: list[str] = []
    if str(train.get("graph_id") or "") != str(serve.get("graph_id") or ""):
        blockers.append("graph_id_mismatch")
    if int(train.get("snapshot_version") or 0) != int(serve.get("snapshot_version") or 0):
        blockers.append("snapshot_version_mismatch")
    train_ids = [str(fid) for fid in list(train.get("feature_ids") or [])]
    serve_ids = [str(fid) for fid in list(serve.get("feature_ids") or [])]
    if train_ids and serve_ids and train_ids != serve_ids:
        blockers.append("feature_ids_mismatch")
    if not bool(serve.get("snapshot_available")):
        blockers.append("serve_snapshot_unavailable")
    if not bool(serve.get("pit_safe")):
        blockers.append("serve_snapshot_not_pit_safe")
    return {
        "ok": not blockers,
        "status": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "train": train,
        "serve": serve,
    }


def assert_graph_train_serve_parity(train_metadata: Mapping[str, Any], serve_metadata: Mapping[str, Any]) -> dict[str, Any]:
    parity = graph_train_serve_parity(train_metadata, serve_metadata)
    if not bool(parity.get("ok")):
        raise ValueError("graph_train_serve_parity_failed:" + ",".join(str(x) for x in parity.get("blockers") or []))
    return parity


def build_graph_shadow_prediction(
    *,
    symbol: str,
    ts_ms: int,
    feature_ids: Sequence[str] | None = None,
    con: Any = None,
) -> dict[str, Any]:
    """Emit a deterministic shadow prediction from graph features."""

    snapshot = build_graph_relational_snapshot(symbol=str(symbol), ts_ms=int(ts_ms), feature_ids=feature_ids, con=con)
    features = dict(snapshot.get("features") or {})
    values = [abs(_safe_float(features.get(fid), 0.0)) for fid in list(snapshot.get("feature_ids") or [])]
    raw_score = float(sum(values) / max(1, len(values)))
    prediction = math.tanh(raw_score / 10.0)
    confidence = min(0.5, 0.05 * _safe_float(features.get(f"{GRAPH_RELATIONAL_PREFIX}neighbor_count"), 0.0))
    return {
        "symbol": _symbol(symbol),
        "ts_ms": int(ts_ms),
        "model_name": "graph_relational_shadow_v1",
        "model_family": "graph_relational",
        "prediction": float(prediction),
        "confidence": float(confidence),
        "stage": "shadow",
        "direct_trading_authority": False,
        "graph_metadata": graph_metadata_from_snapshot(snapshot),
        "snapshot": snapshot,
    }


def _candidate_maps(row: Mapping[str, Any] | None) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    candidate = dict(row or {})
    meta = dict(candidate.get("meta") or {})
    metrics = dict(candidate.get("metrics") or {})
    return candidate, meta, metrics


def graph_feature_ids_from_candidate(row: Mapping[str, Any] | None) -> list[str]:
    candidate, meta, metrics = _candidate_maps(row)
    values: list[Any] = []
    for source in (candidate, meta, metrics):
        raw = source.get("feature_ids")
        if isinstance(raw, list):
            values.extend(raw)
        schema = source.get("feature_schema")
        if isinstance(schema, Mapping) and isinstance(schema.get("feature_ids"), list):
            values.extend(schema.get("feature_ids") or [])
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        fid = str(raw or "").strip()
        if fid.startswith(GRAPH_RELATIONAL_PREFIX) and fid not in seen:
            seen.add(fid)
            out.append(fid)
    return out


def candidate_uses_graph_relational(row: Mapping[str, Any] | None) -> bool:
    candidate, meta, metrics = _candidate_maps(row)
    if graph_feature_ids_from_candidate(candidate):
        return True
    for source in (candidate, meta, metrics):
        for key in ("graph_relational", GRAPH_RELATIONAL_GROUP, "graph_metadata"):
            value = source.get(key)
            if isinstance(value, Mapping) and value:
                return True
    model_family = str(meta.get("model_family") or metrics.get("model_family") or "").lower()
    score_source = str(meta.get("score_source") or metrics.get("score_source") or "").lower()
    return bool("graph_relational" in model_family or score_source == "graph_shadow_predictions")


def _candidate_graph_metadata(row: Mapping[str, Any] | None) -> dict[str, Any]:
    candidate, meta, metrics = _candidate_maps(row)
    for source in (candidate, meta, metrics):
        for key in ("graph_relational", GRAPH_RELATIONAL_GROUP, "graph_metadata"):
            value = source.get(key)
            if isinstance(value, Mapping) and value:
                return dict(value)
    return {}


def evaluate_graph_promotion_gate(row: Mapping[str, Any] | None) -> tuple[bool, dict[str, Any]]:
    """Fail closed when a graph candidate lacks required PIT metadata.

    The current graph layer is shadow-only, so even fully valid graph metadata
    does not grant live promotion authority.
    """

    if not candidate_uses_graph_relational(row):
        return True, {"enabled": True, "applied": False, "status": "not_graph_candidate", "passed": True}

    feature_ids = graph_feature_ids_from_candidate(row)
    graph_meta = _candidate_graph_metadata(row)
    blockers: list[str] = []
    if not graph_meta:
        blockers.append("graph_metadata_missing")
    normalized = graph_metadata_from_snapshot(graph_meta)
    graph_id = str(normalized.get("graph_id") or "")
    if graph_meta and graph_id != GRAPH_RELATIONAL_GRAPH_ID:
        blockers.append("graph_id_invalid")
    if graph_meta and int(normalized.get("snapshot_version") or 0) < int(GRAPH_RELATIONAL_SNAPSHOT_VERSION):
        blockers.append("snapshot_version_missing")
    if graph_meta and not bool(normalized.get("snapshot_available")):
        blockers.append("snapshot_unavailable")
    if graph_meta and not bool(normalized.get("pit_safe")):
        blockers.append("pit_safety_missing")
    if graph_meta and (
        _safe_int(normalized.get("max_source_ts_ms"), 0) <= 0
        or _safe_int(normalized.get("max_availability_ts_ms"), 0) <= 0
    ):
        blockers.append("pit_timestamps_missing")
    if graph_meta and bool(normalized.get("direct_trading_authority")):
        blockers.append("direct_trading_authority_not_allowed")
    if graph_meta and str(normalized.get("stage") or "shadow").lower() != "shadow":
        blockers.append("stage_not_shadow")
    meta_feature_ids = [str(fid) for fid in list(normalized.get("feature_ids") or [])]
    if graph_meta and feature_ids and not meta_feature_ids:
        blockers.append("feature_ids_missing")
    if graph_meta and feature_ids and meta_feature_ids and feature_ids != meta_feature_ids:
        blockers.append("feature_ids_mismatch")
    parity = graph_meta.get("train_serve_parity") if isinstance(graph_meta, Mapping) else None
    if isinstance(parity, Mapping) and not bool(parity.get("ok", False)):
        blockers.append("train_serve_parity_failed")

    if blockers:
        return False, {
            "enabled": True,
            "applied": True,
            "status": str(blockers[0]),
            "passed": False,
            "blockers": list(blockers),
            "feature_ids": list(feature_ids),
            "graph_metadata": dict(graph_meta),
            "shadow_only": True,
        }

    return False, {
        "enabled": True,
        "applied": True,
        "status": "graph_relational_shadow_only",
        "passed": False,
        "blockers": ["graph_relational_shadow_only"],
        "feature_ids": list(feature_ids),
        "graph_metadata": dict(normalized),
        "shadow_only": True,
    }
