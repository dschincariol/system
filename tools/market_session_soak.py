#!/usr/bin/env python3
"""Full market-session paper/live-data soak harness.

The harness drives the same production surfaces an operator uses:
dashboard HTTP mutations, runtime health/barrier reads, broker reconciliation,
kill-switch state transitions, provider job stop/start, and database audit
tails. It exits non-zero on missing evidence instead of silently passing.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DEFAULT_OUT_DIR = ROOT / "logs" / "market_session_soaks"
DEFAULT_SESSION_SECONDS = int(6.5 * 60 * 60)
EXIT_OK = 0
EXIT_NO_GO = 2
FAIL_PATTERNS = (
    "traceback",
    "database is locked",
    "sqlite_busy",
    "sqlite_locked",
    "attempt to write a readonly database",
    "operationalerror",
)
SAFE_BROKERS = {"", "sim", "paper", "sandbox", "broker_sim", "sim-paper", "sim_paper"}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _iso_from_ms(ts_ms: int | None = None) -> str:
    value = int(ts_ms if ts_ms is not None else _now_ms()) / 1000.0
    return datetime.fromtimestamp(value, tz=ZoneInfo("UTC")).isoformat()


def _load_dotenv(path: Path = ROOT / ".env") -> None:
    if not path.exists():
        return
    try:
        for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
    except Exception:
        return


def _canonical_json(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


def sign_report(report: dict[str, Any], signing_key: str | None = None) -> dict[str, Any]:
    payload = dict(report or {})
    payload.pop("signature", None)
    body = _canonical_json(payload)
    digest = hashlib.sha256(body).hexdigest()
    key = str(signing_key if signing_key is not None else os.environ.get("SOAK_REPORT_SIGNING_KEY", "")).strip()
    if not key:
        return {
            "status": "unsigned",
            "algorithm": "none",
            "report_sha256": digest,
            "error": "soak_report_signing_key_missing",
        }
    signature = hmac.new(key.encode("utf-8"), body, hashlib.sha256).hexdigest()
    key_id = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
    return {
        "status": "signed",
        "algorithm": "hmac-sha256",
        "key_id": key_id,
        "report_sha256": digest,
        "signature": signature,
    }


def market_session_plan(
    *,
    now: datetime | None = None,
    duration_s: int = DEFAULT_SESSION_SECONDS,
    require_full_session: bool = True,
    allow_after_hours: bool = False,
    open_grace_s: int = 120,
    wait_for_open: bool = True,
) -> dict[str, Any]:
    eastern = ZoneInfo("America/New_York")
    now_et = (now or datetime.now(tz=eastern)).astimezone(eastern)
    open_dt = datetime.combine(now_et.date(), dtime(9, 30), tzinfo=eastern)
    close_dt = datetime.combine(now_et.date(), dtime(16, 0), tzinfo=eastern)
    full_seconds = int((close_dt - open_dt).total_seconds())

    base = {
        "now_et": now_et.isoformat(),
        "open_et": open_dt.isoformat(),
        "close_et": close_dt.isoformat(),
        "duration_s": int(duration_s),
        "required_full_session_s": int(full_seconds),
        "require_full_session": bool(require_full_session),
        "allow_after_hours": bool(allow_after_hours),
    }
    if now_et.weekday() >= 5:
        return {**base, "ok": False, "reason": "market_closed_weekend"}

    if require_full_session:
        if int(duration_s) < full_seconds:
            return {**base, "ok": False, "reason": "duration_short_for_full_market_session"}
        if now_et > open_dt + timedelta(seconds=max(0, int(open_grace_s))):
            return {**base, "ok": False, "reason": "full_market_session_start_missed"}
        if now_et < open_dt:
            return {
                **base,
                "ok": bool(wait_for_open),
                "reason": "wait_for_open" if wait_for_open else "market_not_open",
                "wait_s": max(0.0, (open_dt - now_et).total_seconds()),
                "planned_end_et": (open_dt + timedelta(seconds=int(duration_s))).isoformat(),
            }
        return {
            **base,
            "ok": True,
            "reason": "market_open_full_session_window",
            "wait_s": 0.0,
            "planned_end_et": (now_et + timedelta(seconds=int(duration_s))).isoformat(),
        }

    market_open = open_dt <= now_et < close_dt
    if not market_open and not allow_after_hours:
        return {**base, "ok": False, "reason": "market_not_open"}
    if market_open and now_et + timedelta(seconds=int(duration_s)) > close_dt + timedelta(seconds=open_grace_s):
        return {**base, "ok": False, "reason": "duration_exceeds_remaining_session"}
    return {
        **base,
        "ok": True,
        "reason": "market_open_partial_session" if market_open else "after_hours_allowed",
        "wait_s": 0.0,
        "planned_end_et": (now_et + timedelta(seconds=int(duration_s))).isoformat(),
    }


@dataclass
class HttpResult:
    ok: bool
    status: int
    latency_ms: float
    payload: Any
    error: str = ""


def _http_json(
    method: str,
    url: str,
    *,
    body: dict[str, Any] | None = None,
    token: str = "",
    timeout_s: float = 15.0,
    request_id: str = "",
) -> HttpResult:
    started = time.time()
    data = None
    headers = {"Accept": "application/json"}
    if token:
        headers["X-API-Token"] = token
    if request_id:
        headers["X-Request-ID"] = request_id
        headers["X-Correlation-ID"] = request_id
    if body is not None:
        data = json.dumps(body, separators=(",", ":"), sort_keys=True).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=str(method).upper())
    try:
        with urllib.request.urlopen(req, timeout=float(timeout_s)) as response:
            raw = response.read().decode("utf-8", errors="replace")
            payload = json.loads(raw) if raw.strip() else {}
            return HttpResult(True, int(response.status), (time.time() - started) * 1000.0, payload)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        try:
            payload = json.loads(raw) if raw.strip() else {"error": str(exc)}
        except Exception:
            payload = {"error": raw[:500] or str(exc)}
        return HttpResult(False, int(exc.code), (time.time() - started) * 1000.0, payload, str(exc))
    except Exception as exc:
        return HttpResult(False, 0, (time.time() - started) * 1000.0, {"error": str(exc)}, str(exc))


class LogCursor:
    def __init__(self, path: Path) -> None:
        self.path = path
        try:
            self.offset = path.stat().st_size
        except OSError:
            self.offset = 0

    def read_new_matches(self) -> list[str]:
        try:
            with self.path.open("r", encoding="utf-8", errors="replace") as handle:
                handle.seek(self.offset)
                text = handle.read()
                self.offset = handle.tell()
        except FileNotFoundError:
            return []
        except Exception as exc:
            return [f"log_scan_failed:{self.path}:{type(exc).__name__}:{exc}"]
        return scan_fail_patterns(text)


def scan_fail_patterns(text: str) -> list[str]:
    matches: list[str] = []
    for line in str(text or "").splitlines():
        lower = line.lower()
        if any(pattern in lower for pattern in FAIL_PATTERNS):
            matches.append(line.strip()[:500])
    return matches


def _json_summary(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"payload_type": type(payload).__name__}
    health = payload
    prices = health.get("prices") if isinstance(health.get("prices"), dict) else {}
    providers = health.get("providers") if isinstance(health.get("providers"), dict) else {}
    barrier = health.get("execution_barrier") if isinstance(health.get("execution_barrier"), dict) else {}
    return {
        "ok": health.get("ok"),
        "reasons": health.get("reasons"),
        "prices_ok": prices.get("ok"),
        "prices_age_s": prices.get("age_s"),
        "providers_ok": providers.get("ok"),
        "providers_healthy": providers.get("healthy"),
        "providers_total": providers.get("total"),
        "execution_barrier_allowed": barrier.get("allowed"),
        "execution_barrier_reason": barrier.get("reason"),
    }


def provider_freshness_findings(health: dict[str, Any], *, max_price_age_s: float) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    prices = health.get("prices") if isinstance(health.get("prices"), dict) else {}
    providers = health.get("providers") if isinstance(health.get("providers"), dict) else {}
    if prices:
        try:
            age_s = float(prices.get("age_s") or 0.0)
        except Exception:
            age_s = float("inf")
        if not bool(prices.get("ok")):
            findings.append({"kind": "stale_price", "reason": "prices_not_ok", "prices": dict(prices)})
        if age_s > float(max_price_age_s):
            findings.append({"kind": "stale_price", "reason": "price_age_exceeded", "age_s": age_s})
    else:
        findings.append({"kind": "stale_price", "reason": "prices_snapshot_missing"})

    by_provider = providers.get("by_provider") if isinstance(providers.get("by_provider"), dict) else {}
    if providers and int(providers.get("healthy") or 0) <= 0:
        findings.append({"kind": "stale_source", "reason": "no_healthy_providers", "providers": dict(providers)})
    for name, row in by_provider.items():
        if not isinstance(row, dict):
            continue
        if row.get("ok") is False or str(row.get("status") or "").upper() in {"STALE", "CIRCUIT_OPEN"}:
            findings.append({"kind": "stale_source", "provider": str(name), "provider_state": dict(row)})
    return findings


def _connect_ro():
    from engine.runtime.storage import connect

    return connect(readonly=True)


def _table_exists(con, table: str) -> bool:
    try:
        row = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (str(table),)).fetchone()
        return bool(row)
    except Exception:
        try:
            row = con.execute("SELECT to_regclass(?)", (str(table),)).fetchone()
            return bool(row and row[0])
        except Exception:
            return False


def _table_columns(con, table: str) -> list[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall() or []
        return [str(row[1]) for row in rows if row and len(row) > 1]
    except Exception:
        return []


def _rows_as_dicts(cursor, columns: list[str]) -> list[dict[str, Any]]:
    return [{columns[idx]: row[idx] for idx in range(min(len(columns), len(row)))} for row in cursor.fetchall() or []]


def _query_tail(con, table: str, *, ts_col: str, start_ts_ms: int, limit: int = 25) -> list[dict[str, Any]]:
    if not _table_exists(con, table):
        return []
    columns = _table_columns(con, table)
    if not columns or ts_col not in columns:
        return []
    selected = ", ".join(columns)
    order_col = "id" if "id" in columns else ts_col
    cursor = con.execute(
        f"SELECT {selected} FROM {table} WHERE {ts_col}>=? ORDER BY {order_col} DESC LIMIT ?",
        (int(start_ts_ms), int(limit)),
    )
    return _rows_as_dicts(cursor, columns)


def audit_tail_since(start_ts_ms: int) -> dict[str, Any]:
    con = _connect_ro()
    try:
        tail: dict[str, Any] = {}
        for table, ts_col in (
            ("event_log", "ts_ms"),
            ("kill_switch_audit", "ts_ms"),
            ("position_reconcile_audit", "ts_ms"),
            ("position_reconcile_bootstrap_audit", "ts_ms"),
            ("order_commands", "ts_ms"),
            ("order_events", "ts_ms"),
            ("execution_orders", "ts_ms"),
            ("broker_order_state", "updated_ts_ms"),
            ("broker_fills", "ts_ms"),
            ("broker_fills_v2", "ts_ms"),
        ):
            tail[table] = _query_tail(con, table, ts_col=ts_col, start_ts_ms=start_ts_ms)
        return tail
    finally:
        con.close()


def mutation_audit_confirmed(start_ts_ms: int, path: str) -> bool:
    con = _connect_ro()
    try:
        if not _table_exists(con, "event_log"):
            return False
        rows = con.execute(
            """
            SELECT payload_json
            FROM event_log
            WHERE event_type='api_mutation' AND ts_ms>=?
            ORDER BY ts_ms DESC
            LIMIT 200
            """,
            (int(start_ts_ms),),
        ).fetchall() or []
        for (payload_json,) in rows:
            try:
                payload = json.loads(str(payload_json or "{}"))
            except Exception:
                payload = {}
            if str(payload.get("path") or "") == str(path):
                return True
        return False
    finally:
        con.close()


def wait_for_mutation_audit(start_ts_ms: int, path: str, *, timeout_s: float = 8.0) -> bool:
    deadline = time.time() + max(0.1, float(timeout_s))
    while time.time() < deadline:
        if mutation_audit_confirmed(start_ts_ms, path):
            return True
        time.sleep(0.25)
    return mutation_audit_confirmed(start_ts_ms, path)


def terminal_intent_count_since(start_ts_ms: int, symbol: str) -> int:
    con = _connect_ro()
    try:
        if not _table_exists(con, "portfolio_orders"):
            return 0
        columns = _table_columns(con, "portfolio_orders")
        if "ts_ms" not in columns or "symbol" not in columns:
            return 0
        return int(
            (
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM portfolio_orders
                    WHERE ts_ms>=?
                      AND UPPER(symbol)=?
                      AND COALESCE(explain_json, '') LIKE '%"source":"terminal"%'
                    """,
                    (int(start_ts_ms), str(symbol).upper()),
                ).fetchone()
                or [0]
            )[0]
            or 0
        )
    finally:
        con.close()


def _allowed_paper_brokers() -> set[str]:
    brokers = set(SAFE_BROKERS)
    broker = str(os.environ.get("BROKER") or os.environ.get("BROKER_NAME") or "").strip().lower()
    broker_name = str(os.environ.get("BROKER_NAME") or os.environ.get("BROKER") or "").strip().lower()
    base_url = str(os.environ.get("ALPACA_BASE_URL") or "").strip().lower()
    if broker in {"alpaca", "alpaca_rest"} or broker_name in {"alpaca", "alpaca_rest"}:
        if "paper-api" in base_url:
            brokers.add("alpaca")
            brokers.add("alpaca_rest")
    return brokers


def paper_broker_preflight_snapshot() -> dict[str, Any]:
    broker = str(os.environ.get("BROKER") or os.environ.get("BROKER_NAME") or "").strip().lower()
    broker_name = str(os.environ.get("BROKER_NAME") or os.environ.get("BROKER") or "").strip().lower()
    configured = broker_name or broker
    base_url = str(os.environ.get("ALPACA_BASE_URL") or "").strip()
    allowed = _allowed_paper_brokers()
    ok = bool(configured in allowed)
    reason = "ok"
    if configured in {"alpaca", "alpaca_rest"} and "paper-api" not in base_url.lower():
        reason = "alpaca_paper_endpoint_required"
    elif not ok:
        reason = "paper_broker_required"
    return {
        "ok": ok,
        "reason": reason,
        "broker": broker,
        "broker_name": broker_name,
        "configured_broker": configured,
        "allowed_brokers": sorted(allowed),
        "alpaca_base_url": base_url,
    }


def live_order_findings(start_ts_ms: int) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    allowed_brokers = _allowed_paper_brokers()
    con = _connect_ro()
    try:
        for table, ts_col in (
            ("order_commands", "ts_ms"),
            ("order_events", "ts_ms"),
            ("execution_orders", "ts_ms"),
            ("broker_order_state", "updated_ts_ms"),
            ("broker_fills", "ts_ms"),
            ("broker_fills_v2", "ts_ms"),
        ):
            if not _table_exists(con, table):
                continue
            columns = _table_columns(con, table)
            if ts_col not in columns:
                continue
            selected_cols = [col for col in ("id", ts_col, "broker", "mode", "execution_mode", "source", "status", "symbol") if col in columns]
            if not selected_cols:
                continue
            rows = con.execute(
                f"SELECT {', '.join(selected_cols)} FROM {table} WHERE {ts_col}>=? ORDER BY {ts_col} DESC LIMIT 500",
                (int(start_ts_ms),),
            ).fetchall() or []
            for row in rows:
                rec = {selected_cols[idx]: row[idx] for idx in range(len(selected_cols))}
                broker = str(rec.get("broker") or "").strip().lower()
                mode = str(rec.get("mode") or rec.get("execution_mode") or "").strip().lower()
                source = str(rec.get("source") or "").strip().lower()
                if mode == "live" or source == "live" or (broker and broker not in allowed_brokers):
                    findings.append({"table": table, "reason": "unexpected_live_order_evidence", "row": rec})
        return findings
    finally:
        con.close()


def orphan_position_findings(start_ts_ms: int, symbol: str, *, qty_tol: float = 0.0001) -> list[dict[str, Any]]:
    con = _connect_ro()
    findings: list[dict[str, Any]] = []
    try:
        if _table_exists(con, "broker_positions"):
            columns = _table_columns(con, "broker_positions")
            if {"symbol", "qty", "ts_ms"}.issubset(set(columns)):
                row = con.execute(
                    """
                    SELECT symbol, qty, ts_ms
                    FROM broker_positions
                    WHERE UPPER(symbol)=? AND ts_ms>=?
                    ORDER BY ts_ms DESC
                    LIMIT 1
                    """,
                    (str(symbol).upper(), int(start_ts_ms)),
                ).fetchone()
                if row and abs(float(row[1] or 0.0)) > float(qty_tol):
                    findings.append({"table": "broker_positions", "symbol": row[0], "qty": float(row[1]), "ts_ms": int(row[2])})
        if _table_exists(con, "exec_open_orders"):
            columns = _table_columns(con, "exec_open_orders")
            if {"symbol", "status", "ts_ms"}.issubset(set(columns)):
                rows = con.execute(
                    """
                    SELECT symbol, status, ts_ms
                    FROM exec_open_orders
                    WHERE UPPER(symbol)=? AND ts_ms>=?
                      AND LOWER(COALESCE(status,'')) NOT IN ('filled','cancelled','canceled','closed','rejected')
                    ORDER BY ts_ms DESC
                    LIMIT 25
                    """,
                    (str(symbol).upper(), int(start_ts_ms)),
                ).fetchall() or []
                for row in rows:
                    findings.append({"table": "exec_open_orders", "symbol": row[0], "status": row[1], "ts_ms": int(row[2])})
        return findings
    finally:
        con.close()


def reconciliation_findings(start_ts_ms: int) -> list[dict[str, Any]]:
    con = _connect_ro()
    try:
        if not _table_exists(con, "position_reconcile_audit"):
            return [{"reason": "position_reconcile_audit_missing"}]
        columns = _table_columns(con, "position_reconcile_audit")
        required = {"ts_ms", "ok", "status", "mismatched_n", "max_abs_qty_diff", "total_abs_qty_diff"}
        if not required.issubset(set(columns)):
            return [{"reason": "position_reconcile_audit_columns_missing", "columns": columns}]
        row = con.execute(
            """
            SELECT ts_ms, broker, ok, status, mismatched_n, max_abs_qty_diff, total_abs_qty_diff, detail_json
            FROM position_reconcile_audit
            WHERE ts_ms>=?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (int(start_ts_ms),),
        ).fetchone()
        if not row:
            return [{"reason": "position_reconcile_not_exercised"}]
        ok = bool(int(row[2] or 0) == 1)
        mismatched_n = int(row[4] or 0)
        max_abs = float(row[5] or 0.0)
        total_abs = float(row[6] or 0.0)
        if not ok or mismatched_n > 0 or max_abs > 0.0 or total_abs > 0.0:
            return [
                {
                    "reason": "reconciliation_drift",
                    "ts_ms": int(row[0] or 0),
                    "broker": str(row[1] or ""),
                    "ok": ok,
                    "status": str(row[3] or ""),
                    "mismatched_n": mismatched_n,
                    "max_abs_qty_diff": max_abs,
                    "total_abs_qty_diff": total_abs,
                    "detail_json": str(row[7] or "")[:1000],
                }
            ]
        return []
    finally:
        con.close()


class SoakRunner:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.started_ts_ms = _now_ms()
        self.correlation_id = f"market-soak-{self.started_ts_ms}"
        self.token = str(os.environ.get("DASHBOARD_API_TOKEN") or "").strip()
        self.failures: list[dict[str, Any]] = []
        self.steps: list[dict[str, Any]] = []
        self.samples: list[dict[str, Any]] = []
        self.log_cursors = [LogCursor(Path(path)) for path in self._log_paths()]

    def _log_paths(self) -> list[str]:
        paths = {str(ROOT / "logs" / "runtime.log"), str(ROOT / "logs" / "engine.log")}
        for path in glob.glob(str(ROOT / "logs" / "*.combined.log")):
            paths.add(path)
        return sorted(paths)

    def fail(self, reason: str, **detail: Any) -> None:
        self.failures.append({"reason": str(reason), **detail})

    def step(self, name: str, ok: bool, **detail: Any) -> None:
        rec = {"name": str(name), "ok": bool(ok), "ts_ms": _now_ms(), **detail}
        self.steps.append(rec)
        if not ok:
            self.fail(str(name), **detail)

    def get(self, path: str, *, base: str | None = None) -> HttpResult:
        url = (base or self.args.dashboard_url).rstrip("/") + path
        return _http_json("GET", url, timeout_s=self.args.timeout_s, request_id=self.correlation_id)

    def post(self, path: str, body: dict[str, Any]) -> HttpResult:
        url = self.args.dashboard_url.rstrip("/") + path
        return _http_json(
            "POST",
            url,
            body=body,
            token=self.token,
            timeout_s=self.args.timeout_s,
            request_id=self.correlation_id,
        )

    def sample(self, label: str) -> None:
        health = self.get("/api/health")
        barrier = self.get("/api/execution/barrier")
        broker = self.get("/api/broker")
        reconcile = self.get("/api/reconcile/broker_backtest")
        kill = self.get("/api/system/kill_switches")
        endpoint_results = {
            "health": health,
            "execution_barrier": barrier,
            "broker": broker,
            "reconcile": reconcile,
            "kill_switches": kill,
        }
        missing_snapshots = [
            {
                "endpoint": name,
                "status": result.status,
                "error": result.error,
                "payload": (
                    result.payload
                    if isinstance(result.payload, dict)
                    else {"payload_type": type(result.payload).__name__}
                ),
            }
            for name, result in endpoint_results.items()
            if not bool(result.ok and result.status == 200)
        ]
        if missing_snapshots:
            self.fail("snapshot_capture_failed", label=label, endpoints=missing_snapshots)
        matches: list[str] = []
        for cursor in self.log_cursors:
            matches.extend(cursor.read_new_matches())
        if matches:
            self.fail("log_fail_pattern", matches=matches[-20:])
        health_payload = health.payload if isinstance(health.payload, dict) else {}
        stale_findings = provider_freshness_findings(health_payload, max_price_age_s=float(self.args.max_price_age_s))
        if stale_findings and label != "provider_disconnect":
            self.fail("stale_source_or_price", label=label, findings=stale_findings[:20])
        self.samples.append(
            {
                "label": str(label),
                "ts_ms": _now_ms(),
                "health": {"http_ok": health.ok, "status": health.status, "summary": _json_summary(health.payload)},
                "execution_barrier": {"http_ok": barrier.ok, "status": barrier.status, "payload": barrier.payload},
                "broker": {"http_ok": broker.ok, "status": broker.status, "payload": broker.payload},
                "reconcile": {"http_ok": reconcile.ok, "status": reconcile.status, "payload": reconcile.payload},
                "kill_switches": {"http_ok": kill.ok, "status": kill.status, "payload": kill.payload},
                "log_matches": matches[-20:],
                "stale_findings": stale_findings[:20],
            }
        )

    def preflight(self) -> bool:
        mode = str(os.environ.get("ENGINE_MODE") or os.environ.get("EXECUTION_MODE") or "").strip().lower()
        exec_mode = str(os.environ.get("EXECUTION_MODE") or "").strip().lower()
        broker = str(os.environ.get("BROKER") or os.environ.get("BROKER_NAME") or "").strip().lower()
        if mode != "paper" and exec_mode != "paper":
            self.step("paper_mode_required", False, engine_mode=mode, execution_mode=exec_mode)
        else:
            self.step("paper_mode_required", True, engine_mode=mode, execution_mode=exec_mode)
        if not self.token:
            self.step("dashboard_api_token_required", False)
        else:
            self.step("dashboard_api_token_required", True)
        broker_snapshot = paper_broker_preflight_snapshot()
        self.step("paper_broker_required", bool(broker_snapshot.get("ok")), **broker_snapshot)
        for path in ("/api/health", "/api/system/state", "/api/execution/barrier"):
            res = self.get(path)
            self.step(f"http_get:{path}", bool(res.ok and res.status == 200), status=res.status, payload=res.payload)
        operator = self.get("/api/operator/status", base=self.args.operator_url)
        self.step("operator_status", bool(operator.ok and operator.status == 200), status=operator.status, payload=operator.payload)
        return not self.failures

    def exercise_terminal_order(self) -> None:
        res = self.post(
            "/api/terminal/order",
            {
                "symbol": self.args.symbol,
                "side": "BUY",
                "qty": self.args.qty,
                "reason": "market_session_soak",
                "confirm": "TRADE",
                "confirmation": "TRADE",
                "consequence_ack": True,
                "confirmation_hold_ms": 0,
                "actor": "market_session_soak",
                "source": "market_session_soak",
            },
        )
        audited = wait_for_mutation_audit(self.started_ts_ms, "/api/terminal/order")
        intents = terminal_intent_count_since(self.started_ts_ms, self.args.symbol)
        ok = bool(res.status == 200 and isinstance(res.payload, dict) and res.payload.get("ok") is True and audited and intents > 0)
        self.step("terminal_order", ok, status=res.status, payload=res.payload, mutation_audited=audited, terminal_intents=intents)

    def exercise_flatten(self) -> None:
        res = self.post(
            "/api/terminal/flatten",
            {
                "symbol": self.args.symbol,
                "reason": "market_session_soak",
                "confirm": "FLATTEN",
                "confirmation": "FLATTEN",
                "consequence_ack": True,
                "confirmation_hold_ms": 1500,
                "actor": "market_session_soak",
                "source": "market_session_soak",
            },
        )
        audited = wait_for_mutation_audit(self.started_ts_ms, "/api/terminal/flatten")
        payload_ok = bool(isinstance(res.payload, dict) and res.payload.get("ok") is True)
        no_position = bool(isinstance(res.payload, dict) and res.payload.get("message") in {"no_position", "already_flat"})
        ok = bool(res.status == 200 and payload_ok and audited and not no_position)
        self.step("terminal_flatten", ok, status=res.status, payload=res.payload, mutation_audited=audited)

    def exercise_rollback(self) -> None:
        res = self.post(
            "/api/champion/rollback",
            {
                "confirm": "ROLLBACK_CHAMPION",
                "justification": "market session soak rollback exercise",
                "dry_run": True,
            },
        )
        audited = wait_for_mutation_audit(self.started_ts_ms, "/api/champion/rollback")
        ok = bool(res.status == 200 and isinstance(res.payload, dict) and res.payload.get("ok") is True and audited)
        self.step("rollback", ok, status=res.status, payload=res.payload, mutation_audited=audited)

    def exercise_kill_switch(self) -> None:
        from engine.execution.kill_switch import activate, clear, snapshot

        activate("global", "global", reason="market_session_soak", actor="market_session_soak")
        active = snapshot().get("state") or []
        active_ok = any(str(row.get("scope")) == "global" and int(row.get("enabled") or 0) == 1 for row in active if isinstance(row, dict))
        clear("global", "global", reason="market_session_soak_recovery", actor="market_session_soak")
        cleared = snapshot().get("state") or []
        clear_ok = not any(str(row.get("scope")) == "global" and int(row.get("enabled") or 0) == 1 for row in cleared if isinstance(row, dict))
        self.step("kill_switch_activation_recovery", bool(active_ok and clear_ok), activated=active_ok, cleared=clear_ok)

    def exercise_provider_disconnect(self) -> None:
        if not self.args.provider_job:
            self.step("provider_disconnect_reconnect", False, reason="provider_job_not_configured")
            return
        stop = self.post(
            "/api/jobs/stop",
            {
                "name": self.args.provider_job,
                "confirm": "JOB_ACTION",
                "confirmation": "JOB_ACTION",
                "consequence_ack": True,
                "confirmation_hold_ms": 0,
                "actor": "market_session_soak",
                "source": "market_session_soak",
            },
        )
        stop_audited = wait_for_mutation_audit(self.started_ts_ms, "/api/jobs/stop")
        time.sleep(max(1.0, float(self.args.provider_disconnect_wait_s)))
        self.sample("provider_disconnect")
        start = self.post(
            "/api/jobs/start",
            {
                "name": self.args.provider_job,
                "confirm": "JOB_ACTION",
                "confirmation": "JOB_ACTION",
                "consequence_ack": True,
                "confirmation_hold_ms": 0,
                "actor": "market_session_soak",
                "source": "market_session_soak",
            },
        )
        start_audited = wait_for_mutation_audit(self.started_ts_ms, "/api/jobs/start")
        deadline = time.time() + max(1.0, float(self.args.provider_reconnect_timeout_s))
        recovered = False
        last_health: Any = {}
        while time.time() < deadline:
            health = self.get("/api/health")
            last_health = health.payload
            if isinstance(health.payload, dict) and not provider_freshness_findings(
                health.payload,
                max_price_age_s=float(self.args.max_price_age_s),
            ):
                recovered = True
                break
            time.sleep(max(1.0, float(self.args.interval_s)))
        ok = bool(stop.status == 200 and start.status == 200 and stop_audited and start_audited and recovered)
        self.step(
            "provider_disconnect_reconnect",
            ok,
            provider_job=self.args.provider_job,
            stop={"status": stop.status, "payload": stop.payload, "mutation_audited": stop_audited},
            start={"status": start.status, "payload": start.payload, "mutation_audited": start_audited},
            recovered=recovered,
            last_health_summary=_json_summary(last_health),
        )

    def exercise_broker_reconcile(self) -> None:
        from engine.execution.position_reconcile import pre_live_position_reconcile

        broker = str(self.args.reconcile_broker or os.environ.get("BROKER") or os.environ.get("BROKER_NAME") or "sim").strip().lower()
        result = pre_live_position_reconcile(broker=broker)
        ok = bool(result.get("ok") and int(result.get("mismatched_n") or 0) == 0)
        self.step("broker_reconciliation", ok, broker=broker, result=result)

    def run_loop(self) -> None:
        deadline = time.time() + max(1, int(self.args.duration_s))
        sample_no = 0
        while time.time() < deadline:
            sample_no += 1
            self.sample(f"sample_{sample_no}")
            time.sleep(max(1, int(self.args.interval_s)))

    def final_checks(self) -> dict[str, Any]:
        self.sample("final")
        live_findings = live_order_findings(self.started_ts_ms)
        orphan_findings = orphan_position_findings(self.started_ts_ms, self.args.symbol)
        reconcile_drift = reconciliation_findings(self.started_ts_ms)
        if live_findings:
            self.fail("unintended_live_order_evidence", findings=live_findings[:50])
        if orphan_findings:
            self.fail("orphan_position", findings=orphan_findings[:50])
        if reconcile_drift:
            self.fail("reconciliation_drift", findings=reconcile_drift[:50])
        return {
            "live_order_findings": live_findings,
            "orphan_position_findings": orphan_findings,
            "reconciliation_findings": reconcile_drift,
        }

    def report(self, final_evidence: dict[str, Any]) -> dict[str, Any]:
        try:
            audit_tail = audit_tail_since(self.started_ts_ms)
        except Exception as exc:
            self.fail("audit_tail_capture_failed", error=f"{type(exc).__name__}: {exc}")
            audit_tail = {
                "_capture_ok": False,
                "error": f"{type(exc).__name__}: {exc}",
            }
        return {
            "schema_version": 1,
            "report_type": "market_session_paper_live_data_soak",
            "status": "GO" if not self.failures else "NO-GO",
            "started_ts_ms": int(self.started_ts_ms),
            "started_at": _iso_from_ms(self.started_ts_ms),
            "completed_ts_ms": _now_ms(),
            "completed_at": _iso_from_ms(),
            "correlation_id": self.correlation_id,
            "config": {
                "dashboard_url": self.args.dashboard_url,
                "operator_url": self.args.operator_url,
                "symbol": self.args.symbol,
                "qty": float(self.args.qty),
                "duration_s": int(self.args.duration_s),
                "interval_s": int(self.args.interval_s),
                "provider_job": self.args.provider_job,
                "reconcile_broker": self.args.reconcile_broker,
                "engine_mode": str(os.environ.get("ENGINE_MODE") or ""),
                "execution_mode": str(os.environ.get("EXECUTION_MODE") or ""),
                "broker": str(os.environ.get("BROKER") or os.environ.get("BROKER_NAME") or ""),
                "dashboard_api_token_configured": bool(self.token),
                "soak_report_signing_key_configured": bool(str(os.environ.get("SOAK_REPORT_SIGNING_KEY") or "").strip()),
            },
            "steps": self.steps,
            "samples": self.samples,
            "final_evidence": final_evidence,
            "audit_tail": audit_tail,
            "failures": self.failures,
        }


def _default_provider_job() -> str:
    for name, enabled_key, credential_key in (
        ("stream_prices_polygon_ws", "POLYGON_WS_ENABLED", "POLYGON_API_KEY"),
        ("poll_prices", "POLYGON_REST_ENABLED", "POLYGON_API_KEY"),
        ("poll_prices", "YFINANCE_ENABLED", ""),
        ("provider_monitor", "", ""),
    ):
        enabled = True if not enabled_key else str(os.environ.get(enabled_key) or "").strip().lower() in {"1", "true", "yes", "on"}
        credential_ok = True if not credential_key else bool(str(os.environ.get(credential_key) or os.environ.get("POLYGON_KEY") or "").strip())
        if enabled and credential_ok:
            return name
    return ""


def _write_report(report: dict[str, Any], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"market_session_soak_{report['started_ts_ms']}.json"
    path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    (path.with_suffix(path.suffix + ".sha256")).write_text(f"{sha}  {path.name}\n", encoding="utf-8")
    return path


def finalize_report(report: dict[str, Any]) -> dict[str, Any]:
    finalized = dict(report)
    failures = list(finalized.get("failures") or [])
    if not str(os.environ.get("SOAK_REPORT_SIGNING_KEY") or "").strip():
        if not any(str(item.get("reason") or "") == "soak_report_signing_key_missing" for item in failures if isinstance(item, dict)):
            failures.append({"reason": "soak_report_signing_key_missing"})
        finalized["status"] = "NO-GO"
    finalized["failures"] = failures
    finalized["exit_code"] = EXIT_OK if finalized.get("status") == "GO" else EXIT_NO_GO
    ended_ts_ms = _now_ms()
    finalized["ended_ts_ms"] = ended_ts_ms
    finalized["ended_at"] = _iso_from_ms(ended_ts_ms)
    finalized["signature"] = sign_report(finalized)
    return finalized


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Full market-session paper/live-data soak")
    parser.add_argument("--dashboard-url", default=os.environ.get("SOAK_DASHBOARD_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--operator-url", default=os.environ.get("SOAK_OPERATOR_URL", "http://127.0.0.1:4001"))
    parser.add_argument("--duration-s", type=int, default=int(os.environ.get("SOAK_DURATION_S", DEFAULT_SESSION_SECONDS)))
    parser.add_argument("--interval-s", type=int, default=int(os.environ.get("SOAK_INTERVAL_S", "30")))
    parser.add_argument("--timeout-s", type=float, default=float(os.environ.get("SOAK_HTTP_TIMEOUT_S", "15")))
    parser.add_argument("--symbol", default=os.environ.get("SOAK_SYMBOL", "SPY"))
    parser.add_argument("--qty", type=float, default=float(os.environ.get("SOAK_QTY", "1")))
    parser.add_argument("--provider-job", default=os.environ.get("SOAK_PROVIDER_JOB", ""))
    parser.add_argument("--reconcile-broker", default=os.environ.get("SOAK_RECONCILE_BROKER", ""))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--max-price-age-s", type=float, default=float(os.environ.get("SOAK_MAX_PRICE_AGE_S", "120")))
    parser.add_argument("--provider-disconnect-wait-s", type=float, default=float(os.environ.get("SOAK_PROVIDER_DISCONNECT_WAIT_S", "15")))
    parser.add_argument("--provider-reconnect-timeout-s", type=float, default=float(os.environ.get("SOAK_PROVIDER_RECONNECT_TIMEOUT_S", "180")))
    parser.add_argument("--allow-partial-session", action="store_true")
    parser.add_argument("--allow-after-hours", action="store_true")
    parser.add_argument("--no-wait-for-open", action="store_true")
    parser.add_argument("--skip-provider-restart", action="store_true")
    parser.add_argument("--skip-rollback", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    _load_dotenv()
    args = parse_args(argv)
    if not args.provider_job:
        args.provider_job = _default_provider_job()

    runner = SoakRunner(args)
    plan = market_session_plan(
        duration_s=int(args.duration_s),
        require_full_session=not bool(args.allow_partial_session),
        allow_after_hours=bool(args.allow_after_hours),
        wait_for_open=not bool(args.no_wait_for_open),
    )
    runner.step("market_session_window", bool(plan.get("ok")), plan=plan)
    if bool(plan.get("ok")) and float(plan.get("wait_s") or 0.0) > 0.0:
        time.sleep(float(plan.get("wait_s") or 0.0))
        runner.started_ts_ms = _now_ms()

    try:
        if not runner.failures:
            runner.preflight()
        if not runner.failures:
            runner.sample("initial")
            runner.exercise_terminal_order()
            runner.exercise_broker_reconcile()
            runner.exercise_kill_switch()
            if not args.skip_provider_restart:
                runner.exercise_provider_disconnect()
            if not args.skip_rollback:
                runner.exercise_rollback()
            runner.exercise_flatten()
            runner.run_loop()
        final_evidence = runner.final_checks()
    except Exception as exc:
        runner.fail("soak_harness_exception", error=f"{type(exc).__name__}: {exc}")
        final_evidence = {}

    report = finalize_report(runner.report(final_evidence))
    path = _write_report(report, Path(args.out_dir))

    print(json.dumps({"status": report["status"], "report": str(path), "failures": report["failures"][:20]}, sort_keys=True))
    return int(report["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
