"""Focused subsystem probes used by runtime health checks."""

from __future__ import annotations

import time
from typing import Any, Callable, Dict

from engine.runtime.health_snapshot import HealthSnapshotContext


WarnFn = Callable[..., None]
TraceFn = Callable[..., None]


def check_runtime_hardware(
    ctx: HealthSnapshotContext,
    *,
    runtime_hardware_snapshot: Callable[[], Dict[str, Any]],
    warn: WarnFn,
    trace_section: TraceFn,
    perf_counter: Callable[[], float] = time.perf_counter,
) -> None:
    out = ctx.out
    section_started = perf_counter()
    try:
        out["runtime_hardware"] = runtime_hardware_snapshot()
    except Exception as e:
        warn("health.runtime_hardware", e)
        out["runtime_hardware"] = {
            "ok": False,
            "error": f"{type(e).__name__}: {e}",
        }
    trace_section("runtime_hardware", section_started, ok=bool((out.get("runtime_hardware") or {}).get("ok")))


def check_disk_pressure(
    ctx: HealthSnapshotContext,
    *,
    disk_pressure_snapshot: Callable[[], Dict[str, Any]],
    warn: WarnFn,
    trace_section: TraceFn,
    perf_counter: Callable[[], float] = time.perf_counter,
) -> None:
    out = ctx.out
    section_started = perf_counter()
    try:
        out["disk_pressure"] = disk_pressure_snapshot()
    except Exception as e:
        warn("health.disk_pressure", e)
        out["disk_pressure"] = {
            "ok": False,
            "status": "error",
            "critical": [f"disk_pressure_error:{type(e).__name__}:{e}"],
            "warnings": [],
            "paths": [],
        }
    trace_section("disk_pressure", section_started, ok=bool((out.get("disk_pressure") or {}).get("ok")))
