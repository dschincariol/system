from __future__ import annotations

import importlib
import os
from pathlib import Path

from tools import live_host_readiness_check as readiness


def _systemctl_show(
    *,
    load_state: str = "loaded",
    active_state: str = "active",
    service_type: str = "notify",
    watchdog_usec: str = "60000000",
    memory_max: str = "infinity",
    oom_score_adjust: str = "0",
    unit_file_state: str = "enabled",
) -> str:
    return "\n".join(
        [
            f"LoadState={load_state}",
            f"ActiveState={active_state}",
            f"Type={service_type}",
            f"WatchdogUSec={watchdog_usec}",
            f"MemoryMax={memory_max}",
            f"OOMScoreAdjust={oom_score_adjust}",
            f"UnitFileState={unit_file_state}",
            "",
        ]
    )


def _good_long_running_units() -> dict[str, str]:
    return {
        unit: _systemctl_show(
            memory_max=expected["memory_max"],
            oom_score_adjust=expected["oom_score_adjust"],
        )
        for unit, expected in readiness.SYSTEMD_UNITS.items()
    }


def _good_backup_units() -> dict[str, str]:
    return {
        "trading-backup.service": _systemctl_show(active_state="inactive", service_type="oneshot"),
        "trading-backup.timer": _systemctl_show(active_state="active", service_type=""),
        "trading-restore-drill.service": _systemctl_show(active_state="inactive", service_type="oneshot"),
        "trading-restore-drill.timer": _systemctl_show(active_state="active", service_type=""),
    }


def _fake_run(unit_values: dict[str, str], *, swap_bytes: int):
    def fake(args: list[str]) -> readiness.CommandResult:
        if args and args[0] == "swapon":
            return readiness.CommandResult(0, f"{swap_bytes}\n", "")
        if len(args) >= 3 and args[0] == "systemctl" and args[1] == "show":
            unit = args[2]
            return readiness.CommandResult(
                0,
                unit_values.get(unit, _systemctl_show(load_state="not-found", active_state="inactive")),
                "",
            )
        return readiness.CommandResult(127, "", f"unexpected command: {args!r}")

    return fake


def _fake_which(name: str) -> str | None:
    if name in {"systemctl", "swapon"}:
        return f"/usr/bin/{name}"
    return None


def _write_secret(path: Path, mode: int) -> None:
    path.write_text("not-a-real-secret\n", encoding="utf-8")
    path.chmod(mode)


def test_live_host_readiness_strict_returns_all_required_reason_codes(monkeypatch, tmp_path) -> None:
    backup_key = tmp_path / "backup.key"
    _write_secret(backup_key, 0o640)
    secret_value = "super-secret-value"
    env = {
        "BACKUP_EVIDENCE_HMAC_KEY_FILE": str(backup_key),
        "TRADING_MASTER_KEY": secret_value,
    }
    inactive_units = {
        "trading-engine.service": _systemctl_show(
            active_state="inactive",
            watchdog_usec="infinity",
            memory_max=readiness.SYSTEMD_UNITS["trading-engine.service"]["memory_max"],
            oom_score_adjust=readiness.SYSTEMD_UNITS["trading-engine.service"]["oom_score_adjust"],
        ),
        "trading-operator.service": _systemctl_show(
            active_state="inactive",
            watchdog_usec="infinity",
            memory_max=readiness.SYSTEMD_UNITS["trading-operator.service"]["memory_max"],
            oom_score_adjust=readiness.SYSTEMD_UNITS["trading-operator.service"]["oom_score_adjust"],
        ),
    }

    monkeypatch.setattr(readiness.shutil, "which", _fake_which)
    monkeypatch.setattr(readiness, "_run", _fake_run(inactive_units, swap_bytes=512 * 1024 * 1024))

    errors = readiness.live_host_readiness_errors(
        require_active=True,
        paid_equity_provider_names=["polygon", "ibkr"],
        environ=env,
    )

    assert any(error.startswith("swap_capacity_below_policy:") for error in errors)
    assert "trading-engine.service:active_state_invalid:inactive" in errors
    assert any(error.startswith("trading-engine.service:watchdog_not_applied:") for error in errors)
    assert "trading-backup.service:not_installed" in errors
    assert "backup_key_mode_insecure:0o640" in errors
    assert "inline_secret_present:TRADING_MASTER_KEY" in errors
    assert "no_paid_feed_credential" in errors
    assert "offsite_dest_unreachable:missing" in errors
    assert secret_value not in "\n".join(errors)


def test_live_host_readiness_strict_passes_when_all_signals_are_satisfied(monkeypatch, tmp_path) -> None:
    backup_key = tmp_path / "backup.key"
    polygon_key = tmp_path / "polygon.key"
    offsite_dir = tmp_path / "offsite"
    offsite_dir.mkdir()
    _write_secret(backup_key, 0o600)
    _write_secret(polygon_key, 0o600)
    env = {
        "BACKUP_EVIDENCE_HMAC_KEY_FILE": str(backup_key),
        "POLYGON_API_KEY_FILE": str(polygon_key),
        "TS_OFFSITE_BACKUP_DEST": str(offsite_dir),
    }
    units = {**_good_long_running_units(), **_good_backup_units()}

    monkeypatch.setattr(readiness.shutil, "which", _fake_which)
    monkeypatch.setattr(readiness, "_run", _fake_run(units, swap_bytes=readiness.MIN_SWAP_BYTES))

    assert (
        readiness.live_host_readiness_errors(
            require_active=True,
            paid_equity_provider_names=["polygon", "ibkr"],
            environ=env,
        )
        == []
    )


def test_live_host_readiness_default_preserves_not_found_unit_standalone_behavior(monkeypatch, tmp_path) -> None:
    backup_key = tmp_path / "backup.key"
    polygon_key = tmp_path / "polygon.key"
    offsite_dir = tmp_path / "offsite"
    offsite_dir.mkdir()
    _write_secret(backup_key, 0o600)
    _write_secret(polygon_key, 0o600)
    env = {
        "BACKUP_EVIDENCE_HMAC_KEY_FILE": str(backup_key),
        "POLYGON_API_KEY_FILE": str(polygon_key),
        "TS_OFFSITE_BACKUP_DEST": str(offsite_dir),
    }

    monkeypatch.setattr(readiness.shutil, "which", _fake_which)
    monkeypatch.setattr(readiness, "_run", _fake_run({}, swap_bytes=readiness.MIN_SWAP_BYTES))

    assert readiness.live_host_readiness_errors(require_active=False, environ=env) == []


def test_prod_preflight_live_host_readiness_gate_uses_strict_mode(monkeypatch) -> None:
    import engine.runtime.prod_preflight as prod_preflight

    prod_preflight = importlib.reload(prod_preflight)
    captured: dict[str, object] = {}

    def fake_errors(**kwargs):
        captured.update(kwargs)
        return ["swap_capacity_below_policy:0:expected_at_least=17179869184"]

    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setattr(readiness, "live_host_readiness_errors", fake_errors)

    notes, warnings, errors, state = prod_preflight._live_host_readiness_gate()

    assert notes == []
    assert warnings == []
    assert errors == ["swap_capacity_below_policy:0:expected_at_least=17179869184"]
    assert state["required"] is True
    assert state["ok"] is False
    assert captured["require_active"] is True
    assert captured["paid_equity_provider_names"] == prod_preflight._paid_equity_provider_names()


def test_prod_preflight_live_host_readiness_gate_skips_non_live(monkeypatch) -> None:
    import engine.runtime.prod_preflight as prod_preflight

    prod_preflight = importlib.reload(prod_preflight)
    monkeypatch.setattr(os, "environ", {"ENGINE_MODE": "safe"})

    notes, warnings, errors, state = prod_preflight._live_host_readiness_gate()

    assert notes == ["live host readiness not required mode=safe"]
    assert warnings == []
    assert errors == []
    assert state["required"] is False
    assert state["skipped"] is True
