from __future__ import annotations

import subprocess
from typing import Any, Sequence

from engine.runtime.clock_health import clock_health_snapshot


def _base_env(**overrides: str) -> dict[str, str]:
    env = {
        "ENGINE_MODE": "live",
        "TRADING_CLOCK_REQUIRED_TIMEZONE": "",
        "TRADING_CLOCK_MAX_SKEW_MS": "1000",
        "TRADING_CLOCK_CHECK_TIMEOUT_S": "0.25",
    }
    env.update(overrides)
    return env


def _which(available: set[str]):
    def _lookup(name: str) -> str | None:
        return f"/usr/bin/{name}" if name in available else None

    return _lookup


def _runner(stdout_by_cmd: dict[str, str], *, timeout_cmds: set[str] | None = None):
    timeout_set = set(timeout_cmds or set())

    def _run(argv: Sequence[str], timeout_s: float) -> dict[str, Any]:
        cmd = str(argv[0])
        if cmd in timeout_set:
            raise subprocess.TimeoutExpired(cmd=list(argv), timeout=timeout_s)
        return {"returncode": 0, "stdout": stdout_by_cmd.get(cmd, ""), "stderr": ""}

    return _run


def _https_ok(url: str, timeout_s: float) -> dict[str, Any]:
    return {
        "ok": True,
        "name": "https_date",
        "url": url,
        "reason": "ok",
        "skew_ms": 25.0,
        "observed_skew_ms": 25.0,
        "round_trip_ms": 20.0,
    }


def test_clock_health_accepts_healthy_chrony_sync() -> None:
    state = clock_health_snapshot(
        engine_mode="live",
        environ=_base_env(),
        which_fn=_which({"chronyc"}),
        command_runner=_runner(
            {
                "chronyc": """
Reference ID    : 8.8.8.8
Stratum         : 3
System time     : 0.000120 seconds fast of NTP time
Last offset     : +0.000060 seconds
RMS offset      : 0.000090 seconds
Leap status     : Normal
"""
            }
        ),
    )

    assert state["ok"] is True
    assert state["blockers"] == []
    assert state["healthy_sources"] == ["chronyc"]
    assert state["skew_sources"] == ["chronyc"]


def test_clock_health_blocks_unsynchronized_host_tool() -> None:
    state = clock_health_snapshot(
        engine_mode="live",
        environ=_base_env(),
        which_fn=_which({"timedatectl"}),
        command_runner=_runner(
            {
                "timedatectl": """
SystemClockSynchronized=no
NTPSynchronized=no
Timezone=UTC
LocalRTC=no
NTP=yes
"""
            }
        ),
        https_date_reader=_https_ok,
    )

    assert state["ok"] is False
    assert "clock_unsynchronized" in state["blockers"]
    timedatectl = next(source for source in state["sources"] if source["name"] == "timedatectl")
    assert timedatectl["synchronized"] is False


def test_clock_health_blocks_excessive_chrony_skew() -> None:
    state = clock_health_snapshot(
        engine_mode="live",
        environ=_base_env(TRADING_CLOCK_MAX_SKEW_MS="500"),
        which_fn=_which({"chronyc"}),
        command_runner=_runner(
            {
                "chronyc": """
Reference ID    : 8.8.8.8
Stratum         : 3
System time     : 2.500000 seconds slow of NTP time
Last offset     : -2.500000 seconds
RMS offset      : 1.000000 seconds
Leap status     : Normal
"""
            }
        ),
    )

    assert state["ok"] is False
    assert "clock_skew_excessive" in state["blockers"]
    chronyc = next(source for source in state["sources"] if source["name"] == "chronyc")
    assert chronyc["skew_excessive"] is True


def test_clock_health_falls_back_to_https_date_when_tools_are_missing() -> None:
    state = clock_health_snapshot(
        engine_mode="live",
        environ=_base_env(),
        which_fn=_which(set()),
        command_runner=_runner({}),
        https_date_reader=_https_ok,
    )

    assert state["ok"] is True
    assert state["healthy_sources"] == ["https_date"]
    assert state["skew_sources"] == ["https_date"]


def test_clock_health_enforces_configured_system_source_requirement() -> None:
    state = clock_health_snapshot(
        engine_mode="live",
        environ=_base_env(TRADING_CLOCK_REQUIRED_SOURCES="system"),
        which_fn=_which(set()),
        command_runner=_runner({}),
        https_date_reader=_https_ok,
    )

    assert state["ok"] is False
    assert "clock_required_source_unavailable:system" in state["blockers"]


def test_clock_health_blocks_when_sources_timeout_and_fallback_unavailable() -> None:
    state = clock_health_snapshot(
        engine_mode="live",
        environ=_base_env(TRADING_CLOCK_HTTPS_TIME_URLS="https://time.invalid/"),
        which_fn=_which({"chronyc"}),
        command_runner=_runner({}, timeout_cmds={"chronyc"}),
        https_date_reader=lambda *_args: (_ for _ in ()).throw(TimeoutError("unit timeout")),
    )

    assert state["ok"] is False
    assert "clock_time_source_unavailable" in state["blockers"]
    chronyc = next(source for source in state["sources"] if source["name"] == "chronyc")
    assert chronyc["reason"] == "timeout"


def test_clock_health_blocks_stale_monotonic_sanity_sample() -> None:
    state = clock_health_snapshot(
        engine_mode="live",
        environ=_base_env(),
        which_fn=_which({"chronyc"}),
        command_runner=_runner(
            {
                "chronyc": """
System time     : 0.000100 seconds fast of NTP time
Leap status     : Normal
"""
            }
        ),
        monotonic_fn=lambda: 42.0,
    )

    assert state["ok"] is False
    assert "clock_monotonic_not_advancing" in state["blockers"]


def test_clock_health_explicitly_bypasses_non_live_modes() -> None:
    state = clock_health_snapshot(
        engine_mode="paper",
        environ=_base_env(ENGINE_MODE="paper"),
        which_fn=lambda _name: (_ for _ in ()).throw(AssertionError("tools should not be probed")),
        command_runner=lambda *_args: (_ for _ in ()).throw(AssertionError("commands should not run")),
        https_date_reader=lambda *_args: (_ for _ in ()).throw(AssertionError("https should not be probed")),
    )

    assert state["ok"] is True
    assert state["required"] is False
    assert state["skipped"] is True
    assert state["reason"] == "not_required"
