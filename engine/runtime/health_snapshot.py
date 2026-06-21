"""Health snapshot scaffolding used by the runtime health facade."""

from __future__ import annotations

import copy
import os
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


WarnFn = Callable[..., None]


@dataclass
class HealthSnapshotContext:
    """Mutable state shared by registered health checks."""

    con: Any
    now_ms: int
    out: Dict[str, Any]
    scratch: Dict[str, Any] = field(default_factory=dict)
    check_failures: List[str] = field(default_factory=list)
    warn: Optional[WarnFn] = field(default=None, repr=False, compare=False)

    def table_exists(self, table: str) -> bool:
        try:
            row = self.con.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (str(table),),
            ).fetchone()
            return bool(row)
        except Exception as e:
            if self.warn is not None:
                self.warn("health.table_exists", e, table=table)
            return False


@dataclass(frozen=True)
class HealthSnapshotCheck:
    name: str
    run: Callable[[HealthSnapshotContext], None]


def pending_payload(
    *,
    now_ms: int,
    reason: str,
    cached_ts_ms: int = 0,
    environ: os._Environ[str] = os.environ,
) -> Dict[str, Any]:
    cache_age_ms = max(0, int(now_ms) - int(cached_ts_ms or 0)) if cached_ts_ms else None
    return {
        "ok": False,
        "ts_ms": int(now_ms),
        "status": "DEGRADED",
        "warming_up": True,
        "error": str(reason or "health_snapshot_pending"),
        "reasons": [str(reason or "health_snapshot_pending")],
        "db": {
            "ok": False,
            "initialized": False,
            "exists": False,
            "status": "UNKNOWN",
            "detail": str(reason or "health_snapshot_pending"),
        },
        "lifecycle": {
            "state": "WARMING_UP",
            "detail": str(reason or "health_snapshot_pending"),
            "ts_ms": int(now_ms),
        },
        "execution_barrier": {
            "ok": True,
            "allowed": False,
            "allow_execution": False,
            "allow_execution_pipeline": False,
            "allow_simulation": False,
            "real_trading_allowed": False,
            "mode": str(environ.get("EXECUTION_MODE") or environ.get("ENGINE_MODE") or "safe").strip().lower() or "safe",
            "reason": str(reason or "health_snapshot_pending"),
            "fast_path": True,
        },
        "cache": {
            "source": "runtime_health_singleflight",
            "stale": True,
            "age_ms": cache_age_ms,
            "populated": False,
            "refresh_in_flight": True,
        },
    }


def stale_payload(payload: Dict[str, Any], *, now_ms: int, cached_ts_ms: int) -> Dict[str, Any]:
    out = copy.deepcopy(payload)
    cache_age_ms = max(0, int(now_ms) - int(cached_ts_ms or 0))
    cache = dict(out.get("cache") or {})
    cache.update(
        {
            "source": "runtime_health_singleflight",
            "stale": True,
            "age_ms": int(cache_age_ms),
            "populated": True,
            "refresh_in_flight": True,
        }
    )
    out["cache"] = cache
    out.setdefault("ts_ms", int(now_ms))
    return out


def new_payload(now_ms: int, *, db_path: Path) -> Dict[str, Any]:
    return {
        "ok": False,
        "ts_ms": now_ms,
        "db_file": {
            "path": str(db_path),
            "exists": bool(db_path.exists()),
        },
        "reasons": [],
        "db": {
            "ok": True,
            "db_path": str(db_path),
            "exists": False,
            "initialized": False,
            "size_bytes": 0,
            "wal_bytes": 0,
            "quick_check": "unknown",
            "error": None,
        },
        "event_log": {
            "ok": False,
            "count": 0,
            "last_ts_ms": None,
            "age_s": None,
        },
    }


def build_context(con: Any, now_ms: int, *, db_path: Path, warn: WarnFn) -> HealthSnapshotContext:
    return HealthSnapshotContext(
        con=con,
        now_ms=now_ms,
        out=new_payload(now_ms, db_path=db_path),
        warn=warn,
    )


def run_checks(ctx: HealthSnapshotContext, checks: Iterable[HealthSnapshotCheck], *, warn: WarnFn) -> None:
    for check in checks:
        try:
            check.run(ctx)
        except Exception as e:
            warn("health.registry.check", e, check=check.name)
            ctx.check_failures.append(f"health_check_failed:{check.name}:{type(e).__name__}")
