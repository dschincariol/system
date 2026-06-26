"""Run the shadow-only graph challenger benchmark."""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

from engine.runtime.storage import acquire_job_lock, connect, init_db, put_job_heartbeat, release_job_lock, touch_job_lock
from engine.strategy.graph_challenger import run_graph_challenger_benchmark


JOB_NAME = "graph_challenger_benchmark"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))


def _csv(value: Any, *, default: str = "") -> list[str]:
    text = str(value if value is not None else default).strip()
    return [part.strip().upper() for part in text.split(",") if part.strip()]


def _int_csv(value: Any, *, default: str = "") -> list[int]:
    out: list[int] = []
    for part in str(value if value is not None else default).split(","):
        try:
            parsed = int(str(part).strip())
        except Exception:
            continue
        if parsed > 0:
            out.append(parsed)
    return out


def _opt_int(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = int(text)
    except Exception:
        return None
    return parsed if parsed > 0 else None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", default=os.environ.get("GRAPH_CHALLENGER_SYMBOLS", "SPY,QQQ"))
    parser.add_argument("--horizons-s", default=os.environ.get("GRAPH_CHALLENGER_HORIZONS_S", "300"))
    parser.add_argument("--start-ts-ms", default=os.environ.get("GRAPH_CHALLENGER_START_TS_MS", ""))
    parser.add_argument("--end-ts-ms", default=os.environ.get("GRAPH_CHALLENGER_END_TS_MS", ""))
    parser.add_argument("--max-samples", type=int, default=int(os.environ.get("GRAPH_CHALLENGER_MAX_SAMPLES", "512") or 512))
    parser.add_argument("--window-count", type=int, default=int(os.environ.get("GRAPH_CHALLENGER_WINDOW_COUNT", "3") or 3))
    parser.add_argument(
        "--window-stride-ms",
        type=int,
        default=int(os.environ.get("GRAPH_CHALLENGER_WINDOW_STRIDE_MS", "300000") or 300000),
    )
    parser.add_argument(
        "--holdout-fraction",
        type=float,
        default=float(os.environ.get("GRAPH_CHALLENGER_HOLDOUT_FRACTION", "0.25") or 0.25),
    )
    return parser


def run(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    init_db()
    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        print(json.dumps({"ok": True, "status": "locked_out", "job": JOB_NAME}, sort_keys=True))
        return 0
    con = None
    try:
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps({"phase": "start"}, separators=(",", ":"), sort_keys=True))
        con = connect(readonly=False)
        result = run_graph_challenger_benchmark(
            con=con,
            symbols=_csv(args.symbols),
            horizons_s=_int_csv(args.horizons_s, default="300"),
            start_ts_ms=_opt_int(args.start_ts_ms),
            end_ts_ms=_opt_int(args.end_ts_ms),
            max_samples=max(1, int(args.max_samples)),
            window_count=max(1, int(args.window_count)),
            window_stride_ms=max(1, int(args.window_stride_ms)),
            holdout_fraction=max(0.0, min(0.9, float(args.holdout_fraction))),
        )
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps(
                {
                    "phase": "done",
                    "run_id": result.get("run_id"),
                    "oos_prediction_count": result.get("oos_prediction_count"),
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
        print(json.dumps({"job": JOB_NAME, **result}, separators=(",", ":"), sort_keys=True, default=str))
        return 0
    finally:
        if con is not None:
            con.close()
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    raise SystemExit(run())
