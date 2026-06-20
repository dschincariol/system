import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Dict, List, Tuple


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_OUT = os.path.join(ROOT, "var", "log", "safe_mode_soak.ndjson")
RUNTIME_LOG = os.path.join(ROOT, "var", "log", "runtime.log")
DEFAULT_DASHBOARD_URL = str(os.environ.get("PIPELINE_SMOKE_BASE") or "http://127.0.0.1:8000").rstrip("/")
DEFAULT_OPERATOR_URL = str(
    os.environ.get("PIPELINE_SMOKE_OPERATOR_BASE")
    or f"{DEFAULT_DASHBOARD_URL}/operator"
).rstrip("/")

FAIL_PATTERNS = (
    "traceback",
    "database is locked",
    "attempt to write a readonly database",
    "engine_spawn_error",
    "dashboard_unreachable",
    "poll_prices_runtime_exit_rc",
    "operationalerror",
)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    details = ", ".join(f"{k}={v}" for k, v in (extra or {}).items())
    suffix = f" ({details})" if details else ""
    sys.stderr.write(f"[tools.safe_mode_soak] {code}: {type(error).__name__}: {error}{suffix}\n")
    sys.stderr.flush()
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _http_json(url: str, timeout: float) -> Tuple[bool, float, object]:
    started = time.time()
    try:
        headers = {"Accept": "application/json"}
        dashboard_token = str(os.environ.get("DASHBOARD_API_TOKEN") or "").strip()
        if dashboard_token:
            headers["X-API-Token"] = dashboard_token
        operator_token = str(
            os.environ.get("PIPELINE_SMOKE_OPERATOR_TOKEN")
            or os.environ.get("OPERATOR_API_TOKEN")
            or ""
        ).strip()
        if operator_token and ":4001/" in str(url):
            headers["X-Operator-Token"] = operator_token
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            payload = json.loads(raw)
            return True, (time.time() - started) * 1000.0, payload
    except Exception as exc:
        _warn_nonfatal("SAFE_MODE_SOAK_HTTP_JSON_FAILED", exc, once_key=f"http_json:{url}", url=str(url))
        return False, (time.time() - started) * 1000.0, {"error": str(exc)}


def _scan_log(path: str, state: Dict[str, int]) -> List[str]:
    matches: List[str] = []
    try:
        offset = int(state.get(path, 0))
    except Exception as exc:
        _warn_nonfatal("SAFE_MODE_SOAK_LOG_OFFSET_PARSE_FAILED", exc, once_key=f"log_offset:{path}", path=str(path))
        offset = 0

    try:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            handle.seek(offset)
            chunk = handle.read()
            state[path] = handle.tell()
    except FileNotFoundError as exc:
        _warn_nonfatal("SAFE_MODE_SOAK_LOG_MISSING", exc, once_key=f"log_missing:{path}", path=str(path))
        return matches
    except Exception as exc:
        _warn_nonfatal("SAFE_MODE_SOAK_LOG_SCAN_FAILED", exc, once_key=f"log_scan:{path}", path=str(path))
        return [f"log_scan_failed:{type(exc).__name__}:{exc}"]

    for line in chunk.splitlines():
        text = line.strip()
        lower = text.lower()
        for pattern in FAIL_PATTERNS:
            if pattern in lower:
                matches.append(text[:500])
                break
    return matches


def _summarize_sample(operator: object, health: object, system_state: object, jobs: object) -> Dict[str, object]:
    summary: Dict[str, object] = {}
    if isinstance(operator, dict):
        summary["operator_status"] = operator.get("status")
        summary["external_runtime"] = operator.get("externalRuntime")
        summary["restart_attempts"] = operator.get("restartAttempts")
    if isinstance(health, dict):
        summary["health_ok"] = health.get("ok")
        prices_obj = health.get("prices")
        providers_obj = health.get("providers")
        job_summary_obj = health.get("job_summary")
        ingestion_obj = health.get("ingestion_runtime")
        prices = dict(prices_obj) if isinstance(prices_obj, dict) else {}
        providers = dict(providers_obj) if isinstance(providers_obj, dict) else {}
        job_summary = dict(job_summary_obj) if isinstance(job_summary_obj, dict) else {}
        ingestion = dict(ingestion_obj) if isinstance(ingestion_obj, dict) else {}
        summary["prices_ok"] = prices.get("ok")
        summary["providers_ok"] = providers.get("ok")
        summary["jobs_ok"] = job_summary.get("ok")
        summary["ingestion_stale"] = ingestion.get("stale")
        summary["health_reasons"] = health.get("reasons")
    if isinstance(system_state, dict):
        barrier_obj = system_state.get("execution_barrier")
        barrier = dict(barrier_obj) if isinstance(barrier_obj, dict) else {}
        summary["runtime_state"] = barrier.get("runtime_state")
        summary["runtime_detail"] = barrier.get("runtime_detail")
    if isinstance(jobs, dict):
        summary["jobs_endpoint_ok"] = jobs.get("ok")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Safe-mode soak monitor")
    parser.add_argument("--duration-s", type=int, default=14400)
    parser.add_argument("--interval-s", type=int, default=30)
    parser.add_argument("--timeout-s", type=float, default=15.0)
    parser.add_argument("--out", default=DEFAULT_OUT)
    parser.add_argument("--base-url", default=DEFAULT_DASHBOARD_URL)
    parser.add_argument("--operator-url", default=DEFAULT_OPERATOR_URL)
    args = parser.parse_args()

    base_url = str(args.base_url).rstrip("/")
    operator_url = str(args.operator_url).rstrip("/")
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    log_offsets: Dict[str, int] = {}
    deadline = time.time() + max(60, int(args.duration_s))
    sample_no = 0

    with open(args.out, "a", encoding="utf-8") as out:
        while time.time() < deadline:
            sample_no += 1
            ts = time.strftime("%Y-%m-%dT%H:%M:%S%z")
            operator_ok, operator_ms, operator = _http_json(f"{operator_url}/api/operator/status", args.timeout_s)
            health_ok, health_ms, health = _http_json(f"{base_url}/api/health", args.timeout_s)
            system_ok, system_ms, system_state = _http_json(f"{base_url}/api/system/state", args.timeout_s)
            jobs_ok, jobs_ms, jobs = _http_json(f"{base_url}/api/jobs", args.timeout_s)
            log_matches = _scan_log(RUNTIME_LOG, log_offsets)

            record = {
                "ts": ts,
                "sample": sample_no,
                "latency_ms": {
                    "operator_status": round(operator_ms, 1),
                    "health": round(health_ms, 1),
                    "system_state": round(system_ms, 1),
                    "jobs": round(jobs_ms, 1),
                },
                "ok": {
                    "operator_status": operator_ok,
                    "health": health_ok,
                    "system_state": system_ok,
                    "jobs": jobs_ok,
                },
                "summary": _summarize_sample(operator, health, system_state, jobs),
                "log_matches": log_matches[-20:],
            }
            out.write(json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n")
            out.flush()

            print(json.dumps({
                "ts": ts,
                "sample": sample_no,
                "summary": record["summary"],
                "latency_ms": record["latency_ms"],
                "log_matches": len(log_matches),
            }, separators=(",", ":"), sort_keys=True))
            sys.stdout.flush()
            time.sleep(max(5, int(args.interval_s)))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
