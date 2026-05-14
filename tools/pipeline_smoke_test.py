"""
FILE: pipeline_smoke_test.py

Tooling or validation script for `pipeline_smoke_test`.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error

BASE = str(os.environ.get("PIPELINE_SMOKE_BASE") or "http://127.0.0.1:8000").rstrip("/")
OPERATOR_BASE = str(
    os.environ.get("PIPELINE_SMOKE_OPERATOR_BASE")
    or f"http://127.0.0.1:{str(os.environ.get('OPERATOR_PORT') or '4001').strip() or '4001'}"
).rstrip("/")
SKIP_OPERATOR = str(os.environ.get("PIPELINE_SMOKE_SKIP_OPERATOR", "0")).strip().lower() in ("1", "true", "yes", "on")
API_TOKEN = str(os.environ.get("DASHBOARD_API_TOKEN") or "").strip()
OPERATOR_TOKEN = str(
    os.environ.get("PIPELINE_SMOKE_OPERATOR_TOKEN")
    or os.environ.get("OPERATOR_API_TOKEN")
    or ""
).strip()
REQUEST_TIMEOUT = max(5, int(float(os.environ.get("PIPELINE_SMOKE_TIMEOUT_S", "300"))))
JOB_TIMEOUT = max(5, int(float(os.environ.get("PIPELINE_SMOKE_JOB_TIMEOUT_S", str(REQUEST_TIMEOUT)))))
PRICE_WAIT_TIMEOUT = max(5, int(float(os.environ.get("PIPELINE_SMOKE_PRICE_WAIT_S", "60"))))
JOB_START_REQUEST_TIMEOUT = max(
    5,
    int(
        float(
            os.environ.get(
                "PIPELINE_SMOKE_JOB_START_TIMEOUT_S",
                str(min(30, REQUEST_TIMEOUT)),
            )
        )
    ),
)
SMOKE_JOBS = [
    job.strip()
    for job in str(
        os.environ.get(
            "PIPELINE_SMOKE_JOBS",
            "update_universe,label_due_events,compute_drift",
        )
    ).split(",")
    if job.strip()
]

def _req_to(base, path, method="GET", body=None, timeout=REQUEST_TIMEOUT):
    url = base + path
    data = None
    headers = {"Content-Type": "application/json"}
    if API_TOKEN:
        headers["X-API-Token"] = API_TOKEN
    if OPERATOR_TOKEN:
        headers["X-Operator-Token"] = OPERATOR_TOKEN
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            ct = r.headers.get("content-type", "")
            raw = r.read()
            if "application/json" in ct:
                return json.loads(raw.decode("utf-8"))
            return raw.decode("utf-8")
    except urllib.error.HTTPError as e:
        raw = e.read()
        ct = e.headers.get("content-type", "")
        text = raw.decode("utf-8", errors="replace")
        if "application/json" in ct:
            obj = json.loads(text)
            if isinstance(obj, dict):
                obj.setdefault("ok", False)
                obj.setdefault("error", f"http_{int(e.code)}")
                meta = obj.get("meta") if isinstance(obj.get("meta"), dict) else {}
                meta["status"] = int(e.code)
                obj["meta"] = meta
                return obj
            return obj
        return {"ok": False, "error": f"http_{int(e.code)}", "status": int(e.code), "body": text}


def _req(path, method="GET", body=None, timeout=REQUEST_TIMEOUT):
    return _req_to(BASE, path, method=method, body=body, timeout=timeout)


def _operator_req(path, method="GET", body=None, timeout=REQUEST_TIMEOUT):
    return _req_to(OPERATOR_BASE, path, method=method, body=body, timeout=timeout)

def _wait_for_price(timeout_s=120):
    start = time.time()
    start_ms = int(start * 1000)
    while time.time() - start < timeout_s:
        try:
            h = _req("/api/health")
            body = (h or {}).get("body") or (h or {})
            prices = body.get("prices") or {}
            last_ts_ms = int(prices.get("last_ts_ms") or 0)
            age_s = float(prices.get("age_s") or 1e9)
            if (
                last_ts_ms >= (start_ms - 5000)
                or age_s <= 10.0
            ):
                return True
        except Exception as e:
            sys.stderr.write(f"[pipeline_smoke_test.wait_for_price] {type(e).__name__}: {e}\n")
            sys.stderr.flush()
        time.sleep(2)
    return False

def _print(title, obj):
    print("\n=== {} ===".format(title))
    try:
        print(json.dumps(obj, indent=2)[:2000])
    except Exception:
        print(obj)


def _require_ok(title, obj):
    if isinstance(obj, dict) and obj.get("ok") is False:
        raise RuntimeError(f"{title}_failed:{obj.get('error') or 'request_failed'}")


def _operator_start_is_proxy_only(obj):
    if not isinstance(obj, dict):
        return False
    if bool(obj.get("disabled")) and str(obj.get("reason") or "") == "OPERATOR_DISABLE_INTERNAL_ENGINE_START":
        return True
    if str(obj.get("reason") or "") == "OPERATOR_DISABLE_INTERNAL_ENGINE_START":
        return True
    for step in list(obj.get("steps") or []):
        if not isinstance(step, dict):
            continue
        detail = step.get("detail")
        if not isinstance(detail, dict):
            continue
        if bool(detail.get("disabled")) and str(detail.get("reason") or "") == "OPERATOR_DISABLE_INTERNAL_ENGINE_START":
            return True
    return False


def _health_ready_for_smoke(obj):
    body = (obj or {}).get("body") or (obj or {})
    db = body.get("db") or {}
    prices = body.get("prices") or {}
    critical = [str(x) for x in (body.get("critical_blockers") or [])]

    db_ok = bool(db.get("ok"))
    last_ts_ms = int(prices.get("last_ts_ms") or 0)
    age_s = float(prices.get("age_s") or 1e9)
    prices_ready = bool(last_ts_ms > 0 and age_s <= 180.0)
    only_provider_blockers = bool(critical) and all(x == "providers_not_ok" for x in critical)

    return bool(db_ok and prices_ready and only_provider_blockers)


def _run_smoke_jobs():
    started = []
    for name in SMOKE_JOBS:
        started.append(_start_job_and_wait(name))
    return {"ok": True, "jobs": started, "pipeline_order": list(SMOKE_JOBS)}


def _jobs_snapshot():
    jobs = _req("/api/jobs")
    _require_ok("jobs", jobs)
    rows = (jobs or {}).get("jobs") or []
    by_name = {
        str(row.get("name") or "").strip(): row
        for row in rows
        if isinstance(row, dict) and str(row.get("name") or "").strip()
    }
    return by_name


def _job_history(name, limit=10):
    hist = _req(f"/api/jobs/history?name={name}&limit={int(limit)}")
    _require_ok(f"job_history_{name}", hist)
    return (hist or {}).get("rows") or []


def _job_started_since(name, before_event_ts=0, before_hist_ts=0):
    row = dict((_jobs_snapshot().get(name) or {}))
    event_ts = int(row.get("last_event_ts_ms") or 0)
    if bool(row.get("running")) or event_ts > int(before_event_ts):
        return row
    for hist_row in _job_history(name, limit=10):
        try:
            hist_ts = int(hist_row.get("ts_ms") or 0)
        except Exception:
            hist_ts = 0
        if hist_ts > int(before_hist_ts):
            return row
    return None


def _start_job_and_wait(name, timeout_s=JOB_TIMEOUT):
    before = dict((_jobs_snapshot().get(name) or {}))
    before_event_ts = int(before.get("last_event_ts_ms") or 0)
    before_hist_ts = 0
    for row in _job_history(name, limit=5):
        try:
            before_hist_ts = max(before_hist_ts, int(row.get("ts_ms") or 0))
        except Exception:
            continue

    start = None
    try:
        start = _req(
            "/api/jobs/start",
            method="POST",
            body={"name": name},
            timeout=JOB_START_REQUEST_TIMEOUT,
        )
        _require_ok(f"job_start_{name}", start)
    except Exception as e:
        reconcile_deadline = time.time() + max(5, min(15, int(timeout_s)))
        reconciled_state = None
        while time.time() < reconcile_deadline:
            try:
                reconciled_state = _job_started_since(
                    name,
                    before_event_ts=before_event_ts,
                    before_hist_ts=before_hist_ts,
                )
            except Exception:
                reconciled_state = None
            if reconciled_state is not None:
                start = {
                    "ok": True,
                    "job": name,
                    "status": "start_response_timeout_reconciled",
                    "request_error": f"{type(e).__name__}: {e}",
                }
                break
            time.sleep(1.0)
        if start is None:
            raise

    deadline = time.time() + max(5, float(timeout_s))
    saw_activity = False
    last_row = before

    while time.time() < deadline:
        row = dict((_jobs_snapshot().get(name) or {}))
        last_row = row
        event_ts = int(row.get("last_event_ts_ms") or 0)
        running = bool(row.get("running"))
        if running or event_ts > before_event_ts:
            saw_activity = True
        if saw_activity and not running:
            latest = None
            for hist_row in _job_history(name, limit=10):
                try:
                    hist_ts = int(hist_row.get("ts_ms") or 0)
                except Exception:
                    hist_ts = 0
                if hist_ts > before_hist_ts:
                    latest = hist_row
                    break
            if latest is None:
                time.sleep(0.5)
                continue
            event = str(latest.get("event") or "").strip().lower()
            exit_code = latest.get("exit_code")
            if event == "exit" and exit_code in (None, 0):
                return {
                    "job": name,
                    "ok": True,
                    "start": start,
                    "state": row,
                    "history": latest,
                }
            raise RuntimeError(f"job_failed:{name}:event={event}:exit_code={exit_code}")
        time.sleep(1.0)

    raise RuntimeError(
        f"job_timeout:{name}:running={bool(last_row.get('running'))}:last_exit_code={last_row.get('last_exit_code')}"
    )

def main():
    print("PIPELINE SMOKE TEST\n")

    if not SKIP_OPERATOR:
        # 1) Operator status
        st = _operator_req("/api/operator/status")
        _print("operator/status", st)
        _require_ok("operator_status", st)

        # 2) Start engine in SAFE (idempotent)
        start = _operator_req("/api/operator/start", method="POST", body={"mode": "safe"})
        _print("operator/start", start)
        if _operator_start_is_proxy_only(start):
            print("operator/start reconciled: proxy-only sidecar; runtime ownership belongs to deployment")
        else:
            _require_ok("operator_start", start)
    else:
        print("operator steps skipped: PIPELINE_SMOKE_SKIP_OPERATOR enabled")

    # 3) Wait for first price tick
    print("\nWaiting for first price tick (<=60s)...")
    ok = _wait_for_price(timeout_s=PRICE_WAIT_TIMEOUT)
    print("price_tick:", ok)

    # 4) Health snapshot
    health = _req("/api/health")
    _print("health", health)
    if not _health_ready_for_smoke(health):
        _require_ok("health", health)

    # 5) Telemetry
    tele = _req("/api/telemetry")
    _print("telemetry", tele)
    _require_ok("telemetry", tele)

    # 6) Run a bounded non-execution smoke sequence.
    pipe = _run_smoke_jobs()
    _print("pipeline_smoke_jobs", pipe)
    _require_ok("pipeline_run", pipe)

    # 7) Validate jobs
    jobs = _req("/api/jobs")
    _print("jobs", jobs)
    _require_ok("jobs", jobs)

    # 8) Strategy metrics
    try:
        strat = _req("/api/strategy_metrics")
        _print("strategy_metrics", strat)
    except urllib.error.HTTPError as e:
        print("strategy_metrics not available:", e)

    # 9) Portfolio state
    try:
        port = _req("/api/portfolio")
        _print("portfolio", port)
    except urllib.error.HTTPError as e:
        print("portfolio not available:", e)

    # 10) Operator summary
    if not SKIP_OPERATOR:
        summ = _operator_req("/api/operator_summary")
        _print("operator_summary", summ)
        _require_ok("operator_summary", summ)
    else:
        print("operator summary skipped: PIPELINE_SMOKE_SKIP_OPERATOR enabled")

    print("\nSMOKE TEST COMPLETE")

if __name__ == "__main__":
    main()
