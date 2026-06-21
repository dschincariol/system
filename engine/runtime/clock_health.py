from __future__ import annotations

"""Read-only clock synchronization checks for live trading preflight."""

import email.utils
import math
import os
import re
import shutil
import subprocess
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Sequence


DEFAULT_CLOCK_MAX_SKEW_MS = 2_000
DEFAULT_CLOCK_CHECK_TIMEOUT_S = 1.5
DEFAULT_CLOCK_REQUIRED_SOURCES = "system_or_https"
DEFAULT_CLOCK_REQUIRED_TIMEZONE = "UTC"
DEFAULT_CLOCK_HTTPS_TIME_URLS = (
    "https://www.google.com/generate_204",
    "https://www.cloudflare.com/cdn-cgi/trace",
)
_TRUTHY = {"1", "true", "yes", "y", "on"}
_FALSEY = {"0", "false", "no", "n", "off"}

CommandRunner = Callable[[Sequence[str], float], Any]
WhichFn = Callable[[str], str | None]
HttpsDateReader = Callable[[str, float], Mapping[str, Any]]
TimeFn = Callable[[], float]


def _normalize_mode(value: Any) -> str:
    return str(value or "safe").strip().lower() or "safe"


def _split_csv(raw: Any) -> list[str]:
    return [part.strip() for part in re.split(r"[\s,]+", str(raw or "")) if part.strip()]


def _env_bool(raw: Any, default: bool = False) -> bool:
    text = str(raw if raw is not None else "").strip().lower()
    if not text:
        return bool(default)
    if text in _TRUTHY:
        return True
    if text in _FALSEY:
        return False
    return bool(default)


def _env_float(env: Mapping[str, Any], names: Sequence[str], default: float, *, minimum: float) -> tuple[float, str | None]:
    for name in names:
        raw = env.get(name)
        if raw is None or str(raw).strip() == "":
            continue
        try:
            value = float(str(raw).strip())
        except Exception:
            return float(default), f"{name}_invalid"
        if not math.isfinite(value) or value < float(minimum):
            return float(default), f"{name}_invalid"
        return float(value), None
    return float(default), None


def _env_int(env: Mapping[str, Any], names: Sequence[str], default: int, *, minimum: int) -> tuple[int, str | None]:
    value, issue = _env_float(env, names, float(default), minimum=float(minimum))
    if issue:
        return int(default), issue
    return int(round(value)), None


def _default_command_runner(argv: Sequence[str], timeout_s: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(argv),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=float(timeout_s),
        check=False,
    )


def _coerce_command_result(raw: Any) -> dict[str, Any]:
    if isinstance(raw, Mapping):
        return {
            "returncode": int(raw.get("returncode", raw.get("rc", 0)) or 0),
            "stdout": str(raw.get("stdout", "") or ""),
            "stderr": str(raw.get("stderr", "") or ""),
        }
    return {
        "returncode": int(getattr(raw, "returncode", 0) or 0),
        "stdout": str(getattr(raw, "stdout", "") or ""),
        "stderr": str(getattr(raw, "stderr", "") or ""),
    }


def _run_readonly_command(
    argv: Sequence[str],
    *,
    timeout_s: float,
    command_runner: CommandRunner,
) -> dict[str, Any]:
    try:
        result = _coerce_command_result(command_runner(list(argv), float(timeout_s)))
        result["timeout"] = False
        return result
    except subprocess.TimeoutExpired as exc:
        return {
            "returncode": 124,
            "stdout": str(getattr(exc, "stdout", "") or ""),
            "stderr": str(getattr(exc, "stderr", "") or "timeout"),
            "timeout": True,
        }
    except Exception as exc:
        return {
            "returncode": 127,
            "stdout": "",
            "stderr": f"{type(exc).__name__}: {exc}",
            "timeout": False,
        }


def _parse_key_values(text: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in str(text or "").splitlines():
        if "=" in line:
            key, _, value = line.partition("=")
        elif ":" in line:
            key, _, value = line.partition(":")
        else:
            continue
        fields[key.strip().lower()] = value.strip()
    return fields


def _parse_bool_text(raw: Any) -> bool | None:
    text = str(raw if raw is not None else "").strip().lower()
    if text in {"yes", "true", "1", "enabled", "active"}:
        return True
    if text in {"no", "false", "0", "disabled", "inactive"}:
        return False
    return None


def _parse_seconds(raw: Any) -> float | None:
    text = str(raw if raw is not None else "").strip().lower()
    match = re.search(r"([+-]?\d+(?:\.\d+)?)\s*seconds?", text)
    if not match:
        match = re.search(r"([+-]?\d+(?:\.\d+)?)", text)
    if not match:
        return None
    value = float(match.group(1))
    if "slow" in text and value > 0:
        value = -value
    return float(value)


def _check_chronyc(
    *,
    timeout_s: float,
    command_runner: CommandRunner,
    which_fn: WhichFn,
) -> dict[str, Any]:
    path = which_fn("chronyc")
    state: dict[str, Any] = {
        "name": "chronyc",
        "available": bool(path),
        "path": path or "",
        "ok": False,
        "synchronized": None,
        "skew_ms": None,
        "reason": "not_installed",
    }
    if not path:
        return state

    result = _run_readonly_command(["chronyc", "tracking"], timeout_s=timeout_s, command_runner=command_runner)
    state["returncode"] = int(result.get("returncode") or 0)
    state["timeout"] = bool(result.get("timeout"))
    if bool(result.get("timeout")):
        state["reason"] = "timeout"
        return state
    if int(result.get("returncode") or 0) != 0:
        state["reason"] = "command_failed"
        stderr = str(result.get("stderr") or "").strip()
        if stderr:
            state["error"] = stderr[:500]
        return state

    fields = _parse_key_values(str(result.get("stdout") or ""))
    leap_status = fields.get("leap status", "")
    system_time_s = _parse_seconds(fields.get("system time"))
    last_offset_s = _parse_seconds(fields.get("last offset"))
    rms_offset_s = _parse_seconds(fields.get("rms offset"))
    skew_s = system_time_s if system_time_s is not None else last_offset_s
    skew_ms = abs(float(skew_s) * 1000.0) if skew_s is not None else None
    synchronized = bool(leap_status.strip().lower() == "normal")
    if "not synchron" in str(result.get("stdout") or "").lower():
        synchronized = False
    state.update(
        {
            "ok": bool(synchronized and skew_ms is not None),
            "synchronized": bool(synchronized),
            "reason": "ok" if synchronized else "unsynchronized",
            "leap_status": leap_status,
            "reference_id": fields.get("reference id", ""),
            "stratum": fields.get("stratum", ""),
            "system_time_s": system_time_s,
            "last_offset_s": last_offset_s,
            "rms_offset_s": rms_offset_s,
            "skew_ms": skew_ms,
        }
    )
    if synchronized and skew_ms is None:
        state["reason"] = "skew_unavailable"
    return state


def _check_timedatectl(
    *,
    timeout_s: float,
    command_runner: CommandRunner,
    which_fn: WhichFn,
) -> dict[str, Any]:
    path = which_fn("timedatectl")
    state: dict[str, Any] = {
        "name": "timedatectl",
        "available": bool(path),
        "path": path or "",
        "ok": False,
        "synchronized": None,
        "skew_ms": None,
        "reason": "not_installed",
    }
    if not path:
        return state

    argv = [
        "timedatectl",
        "show",
        "--property=SystemClockSynchronized",
        "--property=NTPSynchronized",
        "--property=Timezone",
        "--property=LocalRTC",
        "--property=NTP",
    ]
    result = _run_readonly_command(argv, timeout_s=timeout_s, command_runner=command_runner)
    state["returncode"] = int(result.get("returncode") or 0)
    state["timeout"] = bool(result.get("timeout"))
    if bool(result.get("timeout")):
        state["reason"] = "timeout"
        return state
    if int(result.get("returncode") or 0) != 0:
        state["reason"] = "command_failed"
        stderr = str(result.get("stderr") or "").strip()
        if stderr:
            state["error"] = stderr[:500]
        return state

    fields = _parse_key_values(str(result.get("stdout") or ""))
    sync_values = [
        _parse_bool_text(fields.get("systemclocksynchronized")),
        _parse_bool_text(fields.get("ntpsynchronized")),
    ]
    sync_known = [value for value in sync_values if value is not None]
    synchronized = any(sync_known) if sync_known else _parse_bool_text(fields.get("ntp"))
    local_rtc = _parse_bool_text(fields.get("localrtc"))
    state.update(
        {
            "ok": bool(synchronized),
            "synchronized": synchronized,
            "reason": "ok" if synchronized else "unsynchronized",
            "timezone": fields.get("timezone", ""),
            "local_rtc": local_rtc,
            "ntp_enabled": _parse_bool_text(fields.get("ntp")),
        }
    )
    if synchronized is None:
        state["ok"] = False
        state["reason"] = "sync_state_unavailable"
    return state


def _read_https_date_header(url: str, timeout_s: float, *, time_fn: TimeFn = time.time) -> dict[str, Any]:
    started = float(time_fn())
    req = urllib.request.Request(
        str(url),
        headers={"User-Agent": "trading-system-clock-preflight/1.0"},
        method="HEAD",
    )
    with urllib.request.urlopen(req, timeout=float(timeout_s)) as resp:
        ended = float(time_fn())
        date_header = str(resp.headers.get("Date") or "").strip()
    if not date_header:
        return {
            "ok": False,
            "url": str(url),
            "reason": "date_header_missing",
            "started_wall_ts": started,
            "ended_wall_ts": ended,
        }
    parsed = email.utils.parsedate_to_datetime(date_header)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    server_ts = float(parsed.timestamp())
    midpoint = started + ((ended - started) / 2.0)
    observed_skew_ms = (midpoint - server_ts) * 1000.0
    rtt_ms = max(0.0, (ended - started) * 1000.0)
    adjusted_skew_ms = max(0.0, abs(observed_skew_ms) - 1000.0 - (rtt_ms / 2.0))
    return {
        "ok": True,
        "url": str(url),
        "reason": "ok",
        "date_header": date_header,
        "server_ts": server_ts,
        "started_wall_ts": started,
        "ended_wall_ts": ended,
        "round_trip_ms": rtt_ms,
        "observed_skew_ms": observed_skew_ms,
        "skew_ms": adjusted_skew_ms,
        "precision_ms": 1000,
    }


def _check_https_dates(
    urls: Sequence[str],
    *,
    timeout_s: float,
    https_date_reader: HttpsDateReader,
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    for url in urls:
        try:
            state = dict(https_date_reader(str(url), float(timeout_s)) or {})
        except Exception as exc:
            state = {
                "ok": False,
                "url": str(url),
                "reason": f"{type(exc).__name__}: {exc}",
            }
        state.setdefault("name", "https_date")
        state.setdefault("available", True)
        attempts.append(state)
        if bool(state.get("ok")) and state.get("skew_ms") is not None:
            state["attempts"] = attempts
            return state
    return {
        "name": "https_date",
        "available": bool(urls),
        "ok": False,
        "reason": "unavailable" if urls else "not_configured",
        "attempts": attempts,
        "skew_ms": None,
    }


def _python_timezone_name() -> str:
    try:
        local = datetime.now().astimezone()
        name = str(local.tzinfo or "").strip() or str(local.tzname() or "").strip()
        if name:
            return name
    except Exception:
        pass  # no-op-guard: allow fallback to time.tzname below
    try:
        return str(time.tzname[0] or "").strip()
    except Exception:
        return ""


def _timezone_matches(actual: str, required: str) -> bool:
    actual_s = str(actual or "").strip()
    required_s = str(required or "").strip()
    if not required_s or required_s.lower() in {"any", "local"}:
        return True
    if actual_s == required_s:
        return True
    actual_l = actual_s.lower()
    required_l = required_s.lower()
    if required_l in {"utc", "etc/utc", "gmt", "z"}:
        return actual_l in {"utc", "etc/utc", "gmt", "z", "+00:00", "utc+00:00", "utc-00:00"}
    return actual_l == required_l


def clock_health_snapshot(
    *,
    engine_mode: str | None = None,
    environ: Mapping[str, Any] | None = None,
    command_runner: CommandRunner | None = None,
    which_fn: WhichFn | None = None,
    https_date_reader: HttpsDateReader | None = None,
    time_fn: TimeFn | None = None,
    monotonic_fn: TimeFn | None = None,
) -> dict[str, Any]:
    """Return the live clock-health contract without mutating host state."""

    env = dict(os.environ if environ is None else environ)
    mode = _normalize_mode(engine_mode if engine_mode is not None else env.get("ENGINE_MODE"))
    required = mode == "live"
    max_skew_ms, max_skew_issue = _env_int(
        env,
        ("TRADING_CLOCK_MAX_SKEW_MS", "PREFLIGHT_CLOCK_MAX_SKEW_MS"),
        DEFAULT_CLOCK_MAX_SKEW_MS,
        minimum=1,
    )
    timeout_s, timeout_issue = _env_float(
        env,
        ("TRADING_CLOCK_CHECK_TIMEOUT_S", "PREFLIGHT_CLOCK_TIMEOUT_S", "PREFLIGHT_EXTERNAL_TIMEOUT_S"),
        DEFAULT_CLOCK_CHECK_TIMEOUT_S,
        minimum=0.05,
    )
    required_sources_raw = str(env.get("TRADING_CLOCK_REQUIRED_SOURCES") or DEFAULT_CLOCK_REQUIRED_SOURCES).strip()
    required_sources = [part.lower() for part in _split_csv(required_sources_raw)] or [DEFAULT_CLOCK_REQUIRED_SOURCES]
    endpoint_raw = str(
        env.get("TRADING_CLOCK_HTTPS_TIME_URLS")
        or env.get("TRADING_CLOCK_TIME_ENDPOINT")
        or ",".join(DEFAULT_CLOCK_HTTPS_TIME_URLS)
    )
    https_urls = _split_csv(endpoint_raw)
    required_timezone = str(env.get("TRADING_CLOCK_REQUIRED_TIMEZONE", DEFAULT_CLOCK_REQUIRED_TIMEZONE) or "").strip()

    config = {
        "max_skew_ms": int(max_skew_ms),
        "timeout_s": float(timeout_s),
        "required_sources": list(required_sources),
        "https_time_urls": list(https_urls),
        "required_timezone": required_timezone,
    }
    if not required:
        return {
            "ok": True,
            "required": False,
            "mode": mode,
            "reason": "not_required",
            "blockers": [],
            "skipped": True,
            "config": config,
            "sources": [],
        }

    blockers: list[str] = []
    if max_skew_issue or timeout_issue:
        blockers.append("clock_config_invalid")

    runner = command_runner or _default_command_runner
    which = which_fn or shutil.which
    time_reader = time_fn or time.time
    monotonic_reader = monotonic_fn or time.monotonic
    https_reader = https_date_reader or (lambda url, timeout: _read_https_date_header(url, timeout, time_fn=time_reader))

    start_wall = float(time_reader())
    start_monotonic = float(monotonic_reader())

    chronyc = _check_chronyc(timeout_s=timeout_s, command_runner=runner, which_fn=which)
    timedatectl = _check_timedatectl(timeout_s=timeout_s, command_runner=runner, which_fn=which)
    sources: list[dict[str, Any]] = [chronyc, timedatectl]

    chronyc_has_skew = bool(chronyc.get("available")) and bool(chronyc.get("ok")) and chronyc.get("skew_ms") is not None
    system_available = bool(chronyc.get("available") or timedatectl.get("available"))
    source_policy = set(required_sources)
    require_https = bool(source_policy.intersection({"https", "https_date", "external"}))
    require_chronyc = "chronyc" in source_policy
    require_timedatectl = "timedatectl" in source_policy
    require_system = bool(source_policy.intersection({"system", "systemd"}))
    require_system_or_https = bool(source_policy.intersection({"system_or_https", "any"}))
    need_https_for_skew = not chronyc_has_skew
    need_https = bool(require_https or need_https_for_skew or not system_available)
    https_state: dict[str, Any] | None = None
    if need_https:
        https_state = _check_https_dates(https_urls, timeout_s=timeout_s, https_date_reader=https_reader)
        sources.append(dict(https_state))

    end_wall = float(time_reader())
    end_monotonic = float(monotonic_reader())
    sanity = {
        "start_wall_ts": start_wall,
        "end_wall_ts": end_wall,
        "wall_elapsed_ms": (end_wall - start_wall) * 1000.0,
        "start_monotonic_ts": start_monotonic,
        "end_monotonic_ts": end_monotonic,
        "monotonic_elapsed_ms": (end_monotonic - start_monotonic) * 1000.0,
    }
    if sanity["monotonic_elapsed_ms"] <= 0:
        blockers.append("clock_monotonic_not_advancing")
    if sanity["wall_elapsed_ms"] < -100.0:
        blockers.append("clock_wall_time_moved_backwards")
    if sanity["monotonic_elapsed_ms"] > 50.0:
        drift_ms = abs(float(sanity["wall_elapsed_ms"]) - float(sanity["monotonic_elapsed_ms"]))
        sanity["wall_monotonic_drift_ms"] = drift_ms
        if drift_ms > max(1_000.0, float(max_skew_ms)):
            blockers.append("clock_wall_monotonic_sanity_failed")

    if require_chronyc and not bool(chronyc.get("ok")):
        blockers.append("clock_required_source_unavailable:chronyc")
    if require_timedatectl and not bool(timedatectl.get("ok")):
        blockers.append("clock_required_source_unavailable:timedatectl")
    if require_system and not bool(chronyc.get("ok") or timedatectl.get("ok")):
        blockers.append("clock_required_source_unavailable:system")
    if require_system_or_https and not bool(chronyc.get("ok") or timedatectl.get("ok")) and not bool((https_state or {}).get("ok")):
        blockers.append("clock_time_source_unavailable")
    if require_https and not bool((https_state or {}).get("ok")):
        blockers.append("clock_required_source_unavailable:https")

    for source in (chronyc, timedatectl):
        if bool(source.get("available")) and source.get("synchronized") is False:
            blockers.append("clock_unsynchronized")
            break

    skew_sources = [
        source
        for source in sources
        if bool(source.get("ok")) and source.get("skew_ms") is not None
    ]
    if not skew_sources:
        blockers.append("clock_time_source_unavailable")
    for source in skew_sources:
        try:
            skew_ms = abs(float(source.get("skew_ms") or 0.0))
        except Exception:
            continue
        if skew_ms > float(max_skew_ms):
            blockers.append("clock_skew_excessive")
            source["skew_excessive"] = True

    if bool(timedatectl.get("available")) and timedatectl.get("local_rtc") is True:
        blockers.append("clock_local_rtc_enabled")

    actual_timezone = str(timedatectl.get("timezone") or "").strip() or _python_timezone_name()
    timezone_state = {
        "required": bool(required_timezone and required_timezone.lower() not in {"any", "local"}),
        "required_timezone": required_timezone,
        "actual_timezone": actual_timezone,
        "ok": True,
    }
    if bool(timezone_state["required"]):
        if not actual_timezone:
            timezone_state["ok"] = False
            blockers.append("clock_timezone_unavailable")
        elif not _timezone_matches(actual_timezone, required_timezone):
            timezone_state["ok"] = False
            blockers.append("clock_timezone_misconfigured")

    healthy_sources = [str(source.get("name") or "") for source in sources if bool(source.get("ok"))]
    blockers = list(dict.fromkeys(blockers))
    return {
        "ok": not blockers,
        "required": True,
        "mode": mode,
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "config": config,
        "sources": sources,
        "healthy_sources": healthy_sources,
        "skew_sources": [str(source.get("name") or "") for source in skew_sources],
        "max_observed_skew_ms": max((abs(float(source.get("skew_ms") or 0.0)) for source in skew_sources), default=None),
        "sanity": sanity,
        "timezone": timezone_state,
    }


__all__ = [
    "DEFAULT_CLOCK_CHECK_TIMEOUT_S",
    "DEFAULT_CLOCK_HTTPS_TIME_URLS",
    "DEFAULT_CLOCK_MAX_SKEW_MS",
    "DEFAULT_CLOCK_REQUIRED_SOURCES",
    "DEFAULT_CLOCK_REQUIRED_TIMEZONE",
    "clock_health_snapshot",
]
