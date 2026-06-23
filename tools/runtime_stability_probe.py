from __future__ import annotations

import argparse
import json
import os
import shlex
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psutil


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
LOG_DIR = ROOT / "var" / "log"


def _load_repo_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ModuleNotFoundError as exc:
        if exc.name == "dotenv":
            return
        raise
    load_dotenv(ROOT / ".env", override=False)


_load_repo_dotenv()

DEFAULT_OUT = LOG_DIR / "runtime_stability_probe.ndjson"
DEFAULT_DB_PATH = Path(os.environ.get("DB_PATH") or str(ROOT / "var" / "db" / "trading.db"))
DEFAULT_DASHBOARD_URL = str(os.environ.get("PIPELINE_SMOKE_BASE") or "http://127.0.0.1:8000").rstrip("/")
DEFAULT_OPERATOR_URL = str(
    os.environ.get("PIPELINE_SMOKE_OPERATOR_BASE")
    or f"{DEFAULT_DASHBOARD_URL}/operator"
).rstrip("/")
DEFAULT_LOG_PATHS = (
    LOG_DIR / "runtime.log",
    LOG_DIR / "engine.log",
    LOG_DIR / "ingestion.stdout.log",
    LOG_DIR / "ingestion.stderr.log",
    LOG_DIR / "operator.stdout.log",
    LOG_DIR / "operator.stderr.log",
)

FAIL_PATTERNS = (
    "traceback",
    "database is locked",
    "attempt to write a readonly database",
    "engine_spawn_error",
    "dashboard_unreachable",
    "poll_prices_runtime_exit_rc",
    "operationalerror",
    "restart_guard_triggered",
    "fatal",
)


def _http_json(url: str, timeout: float) -> Tuple[bool, float, object]:
    started = time.time()
    try:
        headers = {"Accept": "application/json"}
        dashboard_token = _dashboard_api_token()
        if dashboard_token:
            headers["X-API-Token"] = dashboard_token
        operator_token = _operator_api_token()
        if operator_token and ":4001/" in str(url):
            headers["X-Operator-Token"] = operator_token
        req = urllib.request.Request(str(url), headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            payload = json.loads(raw)
            return True, (time.time() - started) * 1000.0, payload
    except Exception as exc:
        sys.stderr.write(f"[runtime_stability_probe] http_json_failed url={url!r}: {type(exc).__name__}: {exc}\n")
        sys.stderr.flush()
        return False, (time.time() - started) * 1000.0, {"error": str(exc)}


def _dashboard_api_token() -> str:
    try:
        from engine.api.auth_config import dashboard_api_token_from_env

        return dashboard_api_token_from_env()
    except Exception:
        return str(os.environ.get("DASHBOARD_API_TOKEN") or "").strip()


def _operator_api_token() -> str:
    direct_token = str(os.environ.get("PIPELINE_SMOKE_OPERATOR_TOKEN") or "").strip()
    if direct_token:
        return direct_token
    operator_token = str(os.environ.get("OPERATOR_API_TOKEN") or "").strip()
    if operator_token:
        return operator_token
    try:
        from engine.runtime.secret_sources import read_secret_text_from_env

        smoke_token = read_secret_text_from_env(
            "PIPELINE_SMOKE_OPERATOR_TOKEN",
            file_envs=("PIPELINE_SMOKE_OPERATOR_TOKEN_FILE",),
            secret_envs=("PIPELINE_SMOKE_OPERATOR_TOKEN_SECRET",),
            provider_secret_names=(),
        )
        if smoke_token:
            return str(smoke_token).strip()
        return read_secret_text_from_env("OPERATOR_API_TOKEN")
    except Exception:
        return str(os.environ.get("OPERATOR_API_TOKEN") or "").strip()


def _is_operator_bridge_url(url: str) -> bool:
    parsed = urllib.parse.urlparse(str(url))
    path = parsed.path.rstrip("/")
    return path == "/operator" or path.startswith("/operator/")


def _is_direct_operator_sidecar_url(url: str) -> bool:
    parsed = urllib.parse.urlparse(str(url))
    return parsed.port == 4001


def _operator_probe_has_auth(operator_url: str) -> bool:
    if _is_operator_bridge_url(operator_url):
        return bool(_dashboard_api_token())
    if _is_direct_operator_sidecar_url(operator_url):
        return bool(_operator_api_token())
    return bool(_dashboard_api_token() or _operator_api_token())


def _arg_supplied(name: str, argv: Iterable[str]) -> bool:
    prefix = f"{name}="
    return any(arg == name or arg.startswith(prefix) for arg in argv)


def _payload_body(payload: object) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    body = payload.get("body")
    if isinstance(body, dict):
        return dict(body)
    return dict(payload)


def _scan_log(path: Path, state: Dict[str, int]) -> List[str]:
    matches: List[str] = []
    offset = int(state.get(str(path), 0) or 0)
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            handle.seek(offset)
            chunk = handle.read()
            state[str(path)] = handle.tell()
    except FileNotFoundError:
        sys.stderr.write(f"[runtime_stability_probe] log_missing path={path}\n")
        sys.stderr.flush()
        return matches
    except Exception as exc:
        sys.stderr.write(f"[runtime_stability_probe] log_scan_failed path={path}: {type(exc).__name__}: {exc}\n")
        sys.stderr.flush()
        return [f"log_scan_failed:{type(exc).__name__}:{exc}"]

    for line in chunk.splitlines():
        text = line.strip()
        lower = text.lower()
        if any(pattern in lower for pattern in FAIL_PATTERNS):
            matches.append(text[:500])
    return matches


def _resolve_log_paths(values: Optional[Iterable[str]]) -> List[Path]:
    requested = [str(value or "").strip() for value in list(values or []) if str(value or "").strip()]
    if requested:
        return [Path(value).resolve() for value in requested]
    return [path.resolve() for path in DEFAULT_LOG_PATHS]


def _db_table_exists(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table),),
    ).fetchone()
    return bool(row)


def _db_table_snapshot(con: sqlite3.Connection, table: str, *, ts_col: str = "ts_ms") -> Dict[str, Any]:
    if not _db_table_exists(con, table):
        return {"present": False, "count": 0, "last_ts_ms": 0, "age_s": None}
    row = con.execute(
        f"SELECT COUNT(*), MAX({ts_col}) FROM {table}"
    ).fetchone() or (0, 0)
    last_ts_ms = int(row[1] or 0)
    now_ms = int(time.time() * 1000)
    age_s = round(max(0, now_ms - last_ts_ms) / 1000.0, 1) if last_ts_ms > 0 else None
    return {
        "present": True,
        "count": int(row[0] or 0),
        "last_ts_ms": last_ts_ms,
        "age_s": age_s,
    }


def _collect_db_snapshot(db_path: Path) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {
        "path": str(db_path),
        "exists": bool(db_path.exists()),
        "tables": {},
        "fresh_prices": {
            "rows": 0,
            "symbols": 0,
            "last_ts_ms": 0,
            "age_s": None,
            "null_price_rows": 0,
        },
        "provider_health": {
            "rows": 0,
            "last_ts_ms": 0,
            "age_s": None,
            "unhealthy_recent": 0,
        },
        "error": "",
    }
    if not db_path.exists():
        return snapshot

    now_ms = int(time.time() * 1000)
    price_cutoff_ms = int(now_ms - (10 * 60 * 1000))
    provider_cutoff_ms = int(now_ms - (15 * 60 * 1000))

    con = sqlite3.connect(str(db_path))
    try:
        for table, ts_col in (
            ("prices", "ts_ms"),
            ("events", "ts_ms"),
            ("predictions", "ts_ms"),
            ("alerts", "ts_ms"),
            ("runtime_metrics", "ts_ms"),
            ("job_heartbeats", "ts_ms"),
            ("job_locks", "heartbeat_ts_ms"),
            ("ingestion_pipeline_health", "updated_ts_ms"),
            ("price_provider_health", "ts_ms"),
        ):
            snapshot["tables"][table] = _db_table_snapshot(con, table, ts_col=ts_col)

        if _db_table_exists(con, "prices"):
            row = con.execute(
                """
                SELECT
                  COUNT(*),
                  COUNT(DISTINCT symbol),
                  MAX(ts_ms),
                  SUM(CASE WHEN COALESCE(price, px) IS NULL THEN 1 ELSE 0 END)
                FROM prices
                WHERE ts_ms >= ?
                """,
                (int(price_cutoff_ms),),
            ).fetchone() or (0, 0, 0, 0)
            last_ts_ms = int(row[2] or 0)
            snapshot["fresh_prices"] = {
                "rows": int(row[0] or 0),
                "symbols": int(row[1] or 0),
                "last_ts_ms": last_ts_ms,
                "age_s": round(max(0, now_ms - last_ts_ms) / 1000.0, 1) if last_ts_ms > 0 else None,
                "null_price_rows": int(row[3] or 0),
            }

        if _db_table_exists(con, "price_provider_health"):
            row = con.execute(
                """
                SELECT
                  COUNT(*),
                  MAX(ts_ms),
                  SUM(CASE WHEN ok = 0 AND ts_ms >= ? THEN 1 ELSE 0 END)
                FROM price_provider_health
                """
                ,
                (int(provider_cutoff_ms),),
            ).fetchone() or (0, 0, 0)
            last_ts_ms = int(row[1] or 0)
            snapshot["provider_health"] = {
                "rows": int(row[0] or 0),
                "last_ts_ms": last_ts_ms,
                "age_s": round(max(0, now_ms - last_ts_ms) / 1000.0, 1) if last_ts_ms > 0 else None,
                "unhealthy_recent": int(row[2] or 0),
            }
    except Exception as exc:
        snapshot["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        con.close()
    return snapshot


def _split_command(command: str) -> List[str]:
    if os.name == "nt":
        return shlex.split(command, posix=False)
    return shlex.split(command, posix=True)


def _spawn_runtime(command: str, cwd: Path) -> subprocess.Popen:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stdout_path = LOG_DIR / "runtime_stability_probe.launch.stdout.log"
    stderr_path = LOG_DIR / "runtime_stability_probe.launch.stderr.log"
    stdout_fh = stdout_path.open("ab")
    stderr_fh = stderr_path.open("ab")
    try:
        return subprocess.Popen(
            _split_command(command),
            cwd=str(cwd),
            stdout=stdout_fh,
            stderr=stderr_fh,
        )
    except Exception:
        stdout_fh.close()
        stderr_fh.close()
        raise


def _process_tree_metrics(root_pid: Optional[int]) -> Dict[str, Any]:
    if not root_pid:
        return {
            "root_pid": 0,
            "pids": [],
            "process_count": 0,
            "rss_mb": 0.0,
            "cpu_percent": 0.0,
            "thread_count": 0,
        }

    try:
        root = psutil.Process(int(root_pid))
    except Exception as exc:
        sys.stderr.write(
            f"[runtime_stability_probe] process_lookup_failed pid={root_pid!r}: {type(exc).__name__}: {exc}\n"
        )
        sys.stderr.flush()
        return {
            "root_pid": int(root_pid),
            "pids": [],
            "process_count": 0,
            "rss_mb": 0.0,
            "cpu_percent": 0.0,
            "thread_count": 0,
        }

    processes = [root]
    try:
        processes.extend(root.children(recursive=True))
    except Exception:
        # no-op-guard: allow best-effort child enumeration for transient process races
        pass

    seen = set()
    unique: List[psutil.Process] = []
    for proc in processes:
        try:
            pid = int(proc.pid)
        except Exception as exc:
            sys.stderr.write(
                f"[runtime_stability_probe] process_pid_read_failed: {type(exc).__name__}: {exc}\n"
            )
            sys.stderr.flush()
            continue
        if pid in seen:
            continue
        seen.add(pid)
        unique.append(proc)

    rss_mb = 0.0
    cpu_percent = 0.0
    thread_count = 0
    live_pids: List[int] = []
    for proc in unique:
        try:
            rss_mb += float(proc.memory_info().rss) / (1024.0 * 1024.0)
            cpu_percent += float(proc.cpu_percent(interval=None))
            thread_count += int(proc.num_threads())
            live_pids.append(int(proc.pid))
        except Exception as exc:
            sys.stderr.write(
                f"[runtime_stability_probe] process_metrics_failed pid={getattr(proc, 'pid', '?')}: "
                f"{type(exc).__name__}: {exc}\n"
            )
            sys.stderr.flush()
            continue

    return {
        "root_pid": int(root_pid),
        "pids": live_pids,
        "process_count": len(live_pids),
        "rss_mb": round(rss_mb, 2),
        "cpu_percent": round(cpu_percent, 2),
        "thread_count": int(thread_count),
    }


def _extract_db_debug(system_state: object) -> Dict[str, Any]:
    body = _payload_body(system_state)
    db_debug = dict(body.get("database_debug") or {})
    return {
        "reader_count": int(db_debug.get("reader_count") or 0),
        "writer_count": int(db_debug.get("writer_count") or 0),
        "connection_records": int(len(db_debug.get("connections") or [])),
        "long_lived_readers": int(len(db_debug.get("long_lived_readers") or [])),
        "wal_bytes": int(db_debug.get("wal_bytes") or 0),
        "db_bytes": int(db_debug.get("db_bytes") or 0),
    }


def _extract_recent_errors(system_state: object) -> List[Dict[str, Any]]:
    body = _payload_body(system_state)
    rows = body.get("recent_errors")
    if not isinstance(rows, list):
        return []
    return [dict(row or {}) for row in rows if isinstance(row, dict)]


def _extract_job_restarts(jobs_payload: object) -> Dict[str, int]:
    body = _payload_body(jobs_payload)
    rows = body.get("jobs")
    if not isinstance(rows, list):
        return {}
    out: Dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        out[name] = int(row.get("restart_count") or 0)
    return out


def _extract_provider_retries(provider_payload: object) -> Dict[str, int]:
    body = _payload_body(provider_payload)
    provider_telemetry = dict(body.get("provider_telemetry") or body)
    providers = provider_telemetry.get("providers")
    if not isinstance(providers, dict):
        return {}
    out: Dict[str, int] = {}
    for name, row in providers.items():
        if not isinstance(row, dict):
            continue
        out[str(name)] = int(row.get("manager_reconnect_attempts") or 0)
    return out


def _summarize_growth(samples: List[Dict[str, Any]], key_path: Iterable[str]) -> Dict[str, Any]:
    values: List[float] = []
    for sample in samples:
        cur: Any = sample
        for key in key_path:
            if not isinstance(cur, dict):
                cur = None
                break
            cur = cur.get(str(key))
        if cur is None:
            continue
        try:
            values.append(float(cur))
        except Exception as exc:
            sys.stderr.write(
                f"[runtime_stability_probe] growth_value_parse_failed value={cur!r}: "
                f"{type(exc).__name__}: {exc}\n"
            )
            sys.stderr.flush()
            continue
    if not values:
        return {"start": None, "end": None, "max": None, "growth": None}
    return {
        "start": round(values[0], 2),
        "end": round(values[-1], 2),
        "max": round(max(values), 2),
        "growth": round(values[-1] - values[0], 2),
    }


def _wait_for_runtime(base_url: str, timeout_s: float, *, operator_url: Optional[str]) -> bool:
    deadline = time.time() + max(5.0, float(timeout_s))
    health_url = f"{base_url.rstrip('/')}/api/health"
    while time.time() < deadline:
        ok, _, payload = _http_json(health_url, 5.0)
        body = _payload_body(payload)
        if ok and isinstance(body, dict):
            if operator_url:
                operator_ok, _, _ = _http_json(operator_url.rstrip("/") + "/api/operator/status", 5.0)
                if operator_ok:
                    return True
            else:
                return True
        time.sleep(2.0)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Extended runtime stability probe")
    parser.add_argument("--duration-s", type=int, default=600)
    parser.add_argument("--interval-s", type=int, default=30)
    parser.add_argument("--timeout-s", type=float, default=15.0)
    parser.add_argument("--warmup-s", type=int, default=45)
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument("--base-url", default=DEFAULT_DASHBOARD_URL)
    parser.add_argument("--operator-url", default=DEFAULT_OPERATOR_URL)
    parser.add_argument("--skip-operator", action="store_true")
    parser.add_argument("--require-operator", action="store_true")
    parser.add_argument("--launch-command", default="")
    parser.add_argument("--launch-cwd", default=str(ROOT))
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--log-path", action="append", default=[])
    parser.add_argument("--max-memory-growth-mb", type=float, default=64.0)
    parser.add_argument("--max-error-count", type=int, default=0)
    parser.add_argument("--max-restart-delta", type=int, default=0)
    args = parser.parse_args()

    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    db_path = Path(str(args.db_path or DEFAULT_DB_PATH)).resolve()
    log_paths = _resolve_log_paths(args.log_path)

    probe_started_ts_ms = int(time.time() * 1000)
    launched_proc: Optional[subprocess.Popen] = None
    root_pid = 0
    if str(args.launch_command or "").strip():
        launched_proc = _spawn_runtime(str(args.launch_command), Path(args.launch_cwd).resolve())
        root_pid = int(launched_proc.pid)

    # Prime cpu_percent counters before timed sampling.
    if root_pid:
        try:
            root = psutil.Process(root_pid)
            for proc in [root, *root.children(recursive=True)]:
                try:
                    proc.cpu_percent(interval=None)
                except Exception:
                    # no-op-guard: allow cpu counter priming to skip transient process exits
                    pass
        except Exception:
            # no-op-guard: allow best-effort cpu counter priming when the root process exits early
            pass

    operator_url = None if args.skip_operator else str(args.operator_url)
    operator_configured = bool(str(os.environ.get("PIPELINE_SMOKE_OPERATOR_BASE") or "").strip()) or _arg_supplied(
        "--operator-url",
        sys.argv[1:],
    )
    operator_requested = bool(args.require_operator or operator_configured)
    if operator_url and not _operator_probe_has_auth(operator_url):
        if args.require_operator or operator_configured:
            sys.stderr.write(
                "[runtime_stability_probe] operator_probe_auth_missing "
                f"url={operator_url!r}; set DASHBOARD_API_TOKEN for the dashboard bridge "
                "or PIPELINE_SMOKE_OPERATOR_TOKEN/OPERATOR_API_TOKEN for the direct sidecar\n"
            )
            sys.stderr.flush()
        else:
            sys.stderr.write(
                "[runtime_stability_probe] operator_probe_skipped reason=missing_auth "
                f"url={operator_url!r}; use --require-operator with DASHBOARD_API_TOKEN "
                "or PIPELINE_SMOKE_OPERATOR_TOKEN/OPERATOR_API_TOKEN to make this a hard check\n"
            )
            sys.stderr.flush()
            operator_url = None
    elif operator_url and not operator_requested:
        sys.stderr.write(
            "[runtime_stability_probe] operator_probe_skipped reason=not_required "
            f"url={operator_url!r}; use --require-operator or set PIPELINE_SMOKE_OPERATOR_BASE "
            "to make this a hard check\n"
        )
        sys.stderr.flush()
        operator_url = None
    if not _wait_for_runtime(str(args.base_url), float(args.warmup_s), operator_url=operator_url):
        if launched_proc is not None and launched_proc.poll() is not None:
            return int(launched_proc.returncode or 1) or 1
        return 1

    deadline = time.time() + max(60, int(args.duration_s))
    log_offsets: Dict[str, int] = {}
    for log_path in log_paths:
        if not log_path.exists():
            continue
        try:
            log_offsets[str(log_path)] = log_path.stat().st_size
        except Exception:
            log_offsets[str(log_path)] = 0
    samples: List[Dict[str, Any]] = []
    baseline_job_restarts: Optional[Dict[str, int]] = None
    baseline_provider_retries: Optional[Dict[str, int]] = None

    with out_path.open("a", encoding="utf-8") as out:
        sample_no = 0
        while time.time() < deadline:
            sample_no += 1
            ts = time.strftime("%Y-%m-%dT%H:%M:%S%z")

            health_ok, health_ms, health = _http_json(f"{args.base_url.rstrip('/')}/api/health", args.timeout_s)
            telemetry_ok, telemetry_ms, telemetry = _http_json(f"{args.base_url.rstrip('/')}/api/telemetry", args.timeout_s)
            system_ok, system_ms, system_state = _http_json(f"{args.base_url.rstrip('/')}/api/system/state", args.timeout_s)
            jobs_ok, jobs_ms, jobs = _http_json(f"{args.base_url.rstrip('/')}/api/jobs", args.timeout_s)
            provider_ok, provider_ms, provider = _http_json(
                f"{args.base_url.rstrip('/')}/api/operator/provider_telemetry",
                args.timeout_s,
            )
            operator_ok, operator_ms, operator = (True, 0.0, {})
            if operator_url:
                operator_ok, operator_ms, operator = _http_json(
                    f"{operator_url.rstrip('/')}/api/operator/status",
                    args.timeout_s,
                )

            process_metrics = _process_tree_metrics(root_pid or None)
            telemetry_body = _payload_body(telemetry)
            if int(process_metrics.get("process_count") or 0) == 0:
                process_metrics = {
                    "root_pid": 0,
                    "pids": [],
                    "process_count": 1 if telemetry_ok else 0,
                    "rss_mb": round(float(telemetry_body.get("process_rss_mb") or 0.0), 2),
                    "cpu_percent": round(float(telemetry_body.get("cpu_percent") or 0.0), 2),
                    "thread_count": int(telemetry_body.get("thread_count") or 0),
                }
            log_matches: List[str] = []
            for log_path in log_paths:
                path_matches = _scan_log(log_path, log_offsets)
                log_matches.extend([f"{log_path.name}:{line}" for line in path_matches])
            db_debug = _extract_db_debug(system_state)
            db_snapshot = _collect_db_snapshot(db_path)
            recent_errors = [
                row
                for row in _extract_recent_errors(system_state)
                if int(row.get("ts_ms") or 0) >= int(probe_started_ts_ms)
            ]
            job_restarts = _extract_job_restarts(jobs)
            provider_retries = _extract_provider_retries(provider)

            if baseline_job_restarts is None:
                baseline_job_restarts = dict(job_restarts)
            if baseline_provider_retries is None:
                baseline_provider_retries = dict(provider_retries)

            restart_delta = {
                name: int(count) - int((baseline_job_restarts or {}).get(name) or 0)
                for name, count in job_restarts.items()
            }
            provider_retry_delta = {
                name: int(count) - int((baseline_provider_retries or {}).get(name) or 0)
                for name, count in provider_retries.items()
            }

            record = {
                "ts": ts,
                "sample": sample_no,
                "ok": {
                    "health": health_ok,
                    "telemetry": telemetry_ok,
                    "system_state": system_ok,
                    "jobs": jobs_ok,
                    "provider_telemetry": provider_ok,
                    "operator_status": operator_ok,
                },
                "latency_ms": {
                    "health": round(health_ms, 1),
                    "telemetry": round(telemetry_ms, 1),
                    "system_state": round(system_ms, 1),
                    "jobs": round(jobs_ms, 1),
                    "provider_telemetry": round(provider_ms, 1),
                    "operator_status": round(operator_ms, 1),
                },
                "process": process_metrics,
                "db_debug": db_debug,
                "db_snapshot": db_snapshot,
                "recent_error_count": len(recent_errors),
                "restart_delta": restart_delta,
                "provider_retry_delta": provider_retry_delta,
                "log_matches": log_matches[-20:],
                "health": _payload_body(health),
                "telemetry": telemetry_body,
            }
            samples.append(record)
            out.write(json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n")
            out.flush()

            print(
                json.dumps(
                    {
                        "sample": sample_no,
                        "ts": ts,
                        "rss_mb": process_metrics.get("rss_mb"),
                        "cpu_percent": process_metrics.get("cpu_percent"),
                        "db_connections": db_debug.get("connection_records"),
                        "fresh_price_age_s": dict(db_snapshot.get("fresh_prices") or {}).get("age_s"),
                        "recent_errors": len(recent_errors),
                        "restart_delta": restart_delta,
                        "provider_retry_delta": provider_retry_delta,
                        "log_matches": len(log_matches),
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )
            sys.stdout.flush()
            time.sleep(max(5, int(args.interval_s)))

    summary = {
        "sample_count": len(samples),
        "memory_rss_mb": _summarize_growth(samples, ("process", "rss_mb")),
        "cpu_percent": _summarize_growth(samples, ("process", "cpu_percent")),
        "thread_count": _summarize_growth(samples, ("process", "thread_count")),
        "db_connection_records": _summarize_growth(samples, ("db_debug", "connection_records")),
        "db_reader_count": _summarize_growth(samples, ("db_debug", "reader_count")),
        "db_writer_count": _summarize_growth(samples, ("db_debug", "writer_count")),
        "db_long_lived_readers": _summarize_growth(samples, ("db_debug", "long_lived_readers")),
        "fresh_price_age_s": _summarize_growth(samples, ("db_snapshot", "fresh_prices", "age_s")),
        "fresh_price_rows": _summarize_growth(samples, ("db_snapshot", "fresh_prices", "rows")),
        "event_count": _summarize_growth(samples, ("db_snapshot", "tables", "events", "count")),
        "prediction_count": _summarize_growth(samples, ("db_snapshot", "tables", "predictions", "count")),
        "alert_count": _summarize_growth(samples, ("db_snapshot", "tables", "alerts", "count")),
        "total_recent_errors": int(sum(int(sample.get("recent_error_count") or 0) for sample in samples)),
        "total_log_matches": int(sum(len(sample.get("log_matches") or []) for sample in samples)),
        "max_restart_delta": int(
            max(
                [0]
                + [
                    int(value)
                    for sample in samples
                    for value in dict(sample.get("restart_delta") or {}).values()
                ]
            )
        ),
        "max_provider_retry_delta": int(
            max(
                [0]
                + [
                    int(value)
                    for sample in samples
                    for value in dict(sample.get("provider_retry_delta") or {}).values()
                ]
            )
        ),
    }

    print(json.dumps({"summary": summary}, indent=2, sort_keys=True))
    sys.stdout.flush()

    memory_growth = float(summary["memory_rss_mb"].get("growth") or 0.0)
    error_count = int(summary.get("total_recent_errors") or 0) + int(summary.get("total_log_matches") or 0)
    restart_delta = int(summary.get("max_restart_delta") or 0) + int(summary.get("max_provider_retry_delta") or 0)

    exit_code = 0
    if memory_growth > float(args.max_memory_growth_mb):
        exit_code = 1
    if error_count > int(args.max_error_count):
        exit_code = 1
    if restart_delta > int(args.max_restart_delta):
        exit_code = 1

    if launched_proc is not None:
        try:
            launched_proc.terminate()
            launched_proc.wait(timeout=20.0)
        except Exception:
            try:
                launched_proc.kill()
            except Exception:
                # no-op-guard: allow cleanup to ignore already-exited subprocesses
                pass

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
