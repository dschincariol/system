import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Iterable, Mapping
from typing import Any, Dict, List, Tuple


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_OUT = os.path.join(ROOT, "var", "log", "safe_mode_soak.ndjson")
RUNTIME_LOG = os.path.join(ROOT, "var", "log", "runtime.log")
DEFAULT_DASHBOARD_URL = str(os.environ.get("PIPELINE_SMOKE_BASE") or "http://127.0.0.1:8000").rstrip("/")
DEFAULT_OPERATOR_URL = str(
    os.environ.get("PIPELINE_SMOKE_OPERATOR_BASE")
    or f"{DEFAULT_DASHBOARD_URL}/operator"
).rstrip("/")
EXIT_OK = 0
EXIT_NO_GO = 2

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


def _parse_pid(raw: object) -> int:
    try:
        return int(str(raw or "").strip() or "0")
    except Exception:
        return 0


def _process_tree_rss_mb(root_pid: int) -> float | None:
    if int(root_pid or 0) <= 0:
        return None

    try:
        import psutil  # type: ignore
    except Exception as exc:
        _warn_nonfatal("SAFE_MODE_SOAK_PSUTIL_IMPORT_FAILED", exc, once_key="psutil_import")
        return None

    try:
        root = psutil.Process(int(root_pid))
        processes = [root]
        try:
            processes.extend(root.children(recursive=True))
        except Exception as exc:
            _warn_nonfatal(
                "SAFE_MODE_SOAK_PROCESS_CHILDREN_FAILED",
                exc,
                once_key=f"process_children:{root_pid}",
                pid=int(root_pid),
            )
        rss_bytes = 0
        for proc in processes:
            try:
                rss_bytes += int(proc.memory_info().rss)
            except Exception as exc:
                _warn_nonfatal(
                    "SAFE_MODE_SOAK_PROCESS_RSS_FAILED",
                    exc,
                    once_key=f"process_rss:{getattr(proc, 'pid', '?')}",
                    pid=getattr(proc, "pid", "?"),
                )
        return round(float(rss_bytes) / (1024.0 * 1024.0), 3)
    except Exception as exc:
        _warn_nonfatal(
            "SAFE_MODE_SOAK_PROCESS_LOOKUP_FAILED",
            exc,
            once_key=f"process_lookup:{root_pid}",
            pid=int(root_pid),
        )
        return None


def _sample_ok_failed(record: Mapping[str, Any]) -> bool:
    ok = record.get("ok")
    if not isinstance(ok, Mapping):
        return False
    return any(value is False for value in ok.values())


def _sample_log_match_count(record: Mapping[str, Any]) -> int:
    matches = record.get("log_matches")
    if isinstance(matches, list):
        return len(matches)
    if isinstance(matches, tuple):
        return len(matches)
    if isinstance(matches, str) and matches:
        return 1
    return 0


def _sample_rss_mb(record: Mapping[str, Any]) -> float | None:
    process = record.get("process")
    raw: object = None
    if isinstance(process, Mapping):
        raw = process.get("rss_mb")
    if raw is None:
        raw = record.get("process_rss_mb")
    if raw is None:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def _iter_ndjson_records(path: str) -> Iterable[Mapping[str, Any]]:
    with open(path, "r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                parsed = json.loads(text)
            except Exception as exc:
                yield {
                    "ok": {"ndjson_parse": False},
                    "log_matches": [f"invalid_ndjson:{line_no}:{type(exc).__name__}:{exc}"],
                }
                continue
            if isinstance(parsed, Mapping):
                yield parsed
            else:
                yield {
                    "ok": {"ndjson_record": False},
                    "log_matches": [f"invalid_ndjson_record:{line_no}:{type(parsed).__name__}"],
                }


def evaluate_soak_evidence(
    records: Iterable[Mapping[str, Any]],
    *,
    max_error_rate: float = 0.0,
    max_rss_growth_mb: float = 200.0,
) -> Tuple[int, Dict[str, Any]]:
    samples = 0
    error_samples = 0
    log_match_count = 0
    rss_values: List[float] = []

    for record in records:
        samples += 1
        if _sample_ok_failed(record):
            error_samples += 1
        log_match_count += _sample_log_match_count(record)
        rss_mb = _sample_rss_mb(record)
        if rss_mb is not None:
            rss_values.append(float(rss_mb))

    error_rate = float(error_samples) / float(samples) if samples else 1.0
    rss_growth_mb = None
    if rss_values:
        rss_growth_mb = round(float(rss_values[-1]) - float(rss_values[0]), 3)

    reasons: List[Dict[str, Any]] = []
    if samples <= 0:
        reasons.append({"reason": "no_soak_samples"})
    if error_rate > float(max_error_rate):
        reasons.append({
            "reason": "endpoint_error_rate_exceeded",
            "error_rate": round(error_rate, 6),
            "max_error_rate": float(max_error_rate),
            "error_samples": int(error_samples),
            "samples": int(samples),
        })
    if log_match_count > 0:
        reasons.append({"reason": "runtime_log_fail_patterns", "matches": int(log_match_count)})
    if rss_growth_mb is not None and rss_growth_mb > float(max_rss_growth_mb):
        reasons.append({
            "reason": "rss_growth_exceeded",
            "rss_growth_mb": float(rss_growth_mb),
            "max_rss_growth_mb": float(max_rss_growth_mb),
        })

    status = "GO" if not reasons else "NO-GO"
    summary = {
        "status": status,
        "samples": int(samples),
        "error_samples": int(error_samples),
        "error_rate": round(error_rate, 6),
        "log_match_count": int(log_match_count),
        "rss_growth_mb": rss_growth_mb,
        "rss_samples": int(len(rss_values)),
        "reasons": reasons,
    }
    return (EXIT_OK if status == "GO" else EXIT_NO_GO), summary


def evaluate_soak_evidence_file(
    path: str,
    *,
    max_error_rate: float = 0.0,
    max_rss_growth_mb: float = 200.0,
) -> Tuple[int, Dict[str, Any]]:
    return evaluate_soak_evidence(
        _iter_ndjson_records(path),
        max_error_rate=float(max_error_rate),
        max_rss_growth_mb=float(max_rss_growth_mb),
    )


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
    parser.add_argument(
        "--server-pid",
        type=int,
        default=_parse_pid(os.environ.get("SAFE_MODE_SOAK_PID") or os.environ.get("DASHBOARD_PID")),
    )
    parser.add_argument("--max-error-rate", type=float, default=0.0)
    parser.add_argument("--max-rss-growth-mb", type=float, default=200.0)
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Evaluate an existing NDJSON file without polling endpoints.",
    )
    args = parser.parse_args()

    base_url = str(args.base_url).rstrip("/")
    operator_url = str(args.operator_url).rstrip("/")
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    log_offsets: Dict[str, int] = {}
    deadline = time.time() + max(60, int(args.duration_s))
    sample_no = 0
    samples: List[Mapping[str, Any]] = []

    if bool(args.check_only):
        exit_code, summary = evaluate_soak_evidence_file(
            str(args.out),
            max_error_rate=float(args.max_error_rate),
            max_rss_growth_mb=float(args.max_rss_growth_mb),
        )
        print(json.dumps(summary, separators=(",", ":"), sort_keys=True))
        return exit_code

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
                # RSS is recorded when the workflow passes the dashboard PID; the gate
                # still fails closed on endpoint and log evidence if process lookup is unavailable.
                "process": {
                    "pid": int(args.server_pid or 0),
                    "rss_mb": _process_tree_rss_mb(int(args.server_pid or 0)),
                },
            }
            out.write(json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n")
            out.flush()
            samples.append(record)

            print(json.dumps({
                "ts": ts,
                "sample": sample_no,
                "summary": record["summary"],
                "latency_ms": record["latency_ms"],
                "log_matches": len(log_matches),
                "process": record["process"],
            }, separators=(",", ":"), sort_keys=True))
            sys.stdout.flush()
            time.sleep(max(5, int(args.interval_s)))

    exit_code, summary = evaluate_soak_evidence(
        samples,
        max_error_rate=float(args.max_error_rate),
        max_rss_growth_mb=float(args.max_rss_growth_mb),
    )
    print(json.dumps(summary, separators=(",", ":"), sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
