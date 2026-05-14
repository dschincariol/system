"""Run the monthly backup restore drill and publish runtime metrics."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _emit(metric: str, value: Any, **tags: Any) -> None:
    try:
        from engine.runtime.metrics import emit_metric

        emit_metric(
            metric,
            value,
            metric_type="gauge",
            component="backup",
            job="monthly_restore_drill",
            extra_tags={k: v for k, v in tags.items() if v is not None},
        )
    except Exception:
        return


def _flush_metrics() -> None:
    try:
        from engine.runtime.metrics_store import flush_runtime_metrics_buffer

        flush_runtime_metrics_buffer(max_batches=16)
    except Exception:
        return


def _latest_drill_age_days(drill_dir: Path) -> float | None:
    reports = sorted(drill_dir.glob("restore_drill_*.txt"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not reports:
        return None
    age_s = max(0.0, time.time() - float(reports[0].stat().st_mtime))
    return age_s / 86400.0


def main() -> int:
    root = _repo_root()
    script = Path(os.environ.get("TS_RESTORE_DRILL_SCRIPT") or root / "ops" / "backup" / "restore_drill.sh")
    drill_dir = Path(os.environ.get("TS_RESTORE_DRILL_DIR") or "/var/backups/trading/drills")
    started = time.monotonic()

    if not script.exists():
        _emit("backup.restore_drill_exit_code", 127, outcome="missing_script")
        _flush_metrics()
        print(f"restore_drill_script_missing path={script}", file=sys.stderr)
        return 127

    proc = subprocess.run(["bash", str(script)], cwd=str(root), check=False)
    elapsed_s = time.monotonic() - started
    outcome = "pass" if proc.returncode == 0 else "fail"

    _emit("backup.restore_drill_exit_code", proc.returncode, outcome=outcome)
    _emit("backup.restore_drill_elapsed_s", elapsed_s, outcome=outcome)
    age_days = _latest_drill_age_days(drill_dir)
    if age_days is not None:
        _emit("backup.last_drill_age_days", age_days, outcome=outcome)
    _flush_metrics()
    return int(proc.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
