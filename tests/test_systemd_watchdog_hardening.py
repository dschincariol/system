from __future__ import annotations

import os
import re
import socket
import stat
import subprocess
from pathlib import Path

import start_system
from engine.runtime import sd_notify


REPO_ROOT = Path(__file__).resolve().parents[1]


def _duration_seconds(value: str) -> int:
    match = re.fullmatch(r"(?P<amount>\d+)(?P<unit>s)?", value.strip())
    assert match is not None
    return int(match.group("amount"))


def _read_unit(name: str) -> dict[str, str]:
    values: dict[str, str] = {}
    path = REPO_ROOT / "deploy" / "systemd" / name
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _write_sudo_shim(bin_dir: Path) -> None:
    sudo = bin_dir / "sudo"
    sudo.write_text("#!/usr/bin/env bash\nif [[ \"$1\" == \"-n\" ]]; then shift; fi\nexec \"$@\"\n", encoding="utf-8")
    sudo.chmod(sudo.stat().st_mode | stat.S_IXUSR)


def _service_ctl_env(bin_dir: Path, log_dir: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["PATH"] = f"{bin_dir}{os.pathsep}{env.get('PATH', '')}"
    env["TRADING_LOGS"] = str(log_dir)
    return env


def test_engine_and_operator_units_have_watchdog_and_memory_bounds() -> None:
    expected = {
        "trading-engine.service": {
            "NotifyAccess": "main",
            "MemoryHigh": "24G",
            "MemoryMax": "32G",
            "OOMScoreAdjust": "600",
        },
        "trading-operator.service": {
            "NotifyAccess": "all",
            "MemoryHigh": "4G",
            "MemoryMax": "6G",
            "OOMScoreAdjust": "700",
        },
    }

    for unit_name, expected_values in expected.items():
        unit = _read_unit(unit_name)
        assert unit["Type"] == "notify"
        assert unit["WorkingDirectory"] == "/opt/trading/app"
        assert unit["EnvironmentFile"] == "/etc/trading/trading.env"
        assert _duration_seconds(unit["WatchdogSec"]) == 60
        assert unit["MemoryAccounting"] == "true"
        assert unit["Restart"] == "on-failure"
        assert _duration_seconds(unit["TimeoutStartSec"]) == 300
        assert int(unit["OOMScoreAdjust"]) > 0
        for key, value in expected_values.items():
            assert unit[key] == value

        text = (REPO_ROOT / "deploy" / "systemd" / unit_name).read_text(encoding="utf-8")
        assert "/opt/trading-system" not in text
        assert "/etc/trading-system" not in text
        assert "Environment=TS_SECRETS_PROVIDER=systemd-creds" in text
        assert "Environment=DATA_SOURCE_MASTER_KEY_SECRET=master_key" in text
        assert "Environment=DATA_SOURCE_MASTER_KEY_FILE=" in text
        assert "Environment=OBJECT_STORE_ACCESS_KEY_SECRET=object_store_access_key" in text
        assert "Environment=OBJECT_STORE_SECRET_KEY_SECRET=object_store_secret_key" in text
        assert "Environment=BACKUP_EVIDENCE_HMAC_KEY_FILE=" in text
        assert "Environment=BACKUP_EVIDENCE_HMAC_KEY_SECRET=backup_evidence_hmac_key" in text
        assert "LoadCredentialEncrypted=master_key:/etc/credstore.encrypted/master_key.cred" in text
        assert "LoadCredentialEncrypted=pg_password_app:/etc/credstore.encrypted/pg_password_app.cred" in text
        assert (
            "LoadCredentialEncrypted=dashboard_api_token:/etc/credstore.encrypted/dashboard_api_token.cred"
            in text
        )
        assert (
            "LoadCredentialEncrypted=object_store_access_key:/etc/credstore.encrypted/object_store_access_key.cred"
            in text
        )
        assert (
            "LoadCredentialEncrypted=object_store_secret_key:/etc/credstore.encrypted/object_store_secret_key.cred"
            in text
        )
        assert (
            "LoadCredentialEncrypted=backup_evidence_hmac_key:/etc/credstore.encrypted/backup_evidence_hmac_key.cred"
            in text
        )
        assert "UnsetEnvironment=OBJECT_STORE_ACCESS_KEY OBJECT_STORE_ACCESS_KEY_FILE" in text
        assert "AWS_SECRET_ACCESS_KEY_FILE" in text
        assert "StartLimitIntervalSec=300" in text
        assert "StartLimitBurst=5" in text
        assert "RestartSec=5" in text


def test_operator_notify_access_rationale_is_documented() -> None:
    unit_text = (REPO_ROOT / "deploy" / "systemd" / "trading-operator.service").read_text(encoding="utf-8")
    docs_text = (REPO_ROOT / "docs" / "FAILURE_MODES.md").read_text(encoding="utf-8")

    notify_match = re.search(
        r"(?P<comment>(?:#[^\n]*\n)+)NotifyAccess=all",
        unit_text,
    )
    assert notify_match is not None
    comment = notify_match.group("comment")
    assert "systemd-notify" in comment
    assert "child" in comment
    assert "NotifyAccess=main would reject" in comment
    assert "docs/FAILURE_MODES.md" in comment

    assert "NotifyAccess=all" in docs_text
    assert "SCM_CREDENTIALS PID" in docs_text
    assert "--pid=parent" in docs_text
    assert "child-originated fallback datagram" in docs_text
    assert "NotifyAccess=main` would reject" in docs_text


def test_linux_installer_excludes_machine_local_env_files_from_app_mirror() -> None:
    text = (REPO_ROOT / "deploy" / "install_trading_system.sh").read_text(encoding="utf-8")

    assert "--include '.env.example'" in text
    assert "--include '*.env.example'" in text
    for pattern in ("'.env'", "'.env.*'", "'*.env'", "'*.env.*'"):
        assert f"--exclude {pattern}" in text
    for pattern in (
        "'data/secrets'",
        "'data/secrets/**'",
        "'data/runtime'",
        "'data/runtime/**'",
        "'*.db'",
        "'*.sqlite'",
        "'*.sqlite-*'",
    ):
        assert f"--exclude {pattern}" in text
    assert "ENV_TEMPLATE=\"$REPO_DIR/deploy/env/trading.env.example\"" in text
    assert "ENV_TEMPLATE=\"$REPO_DIR/deploy/env/trading.env\"" not in text
    assert "! -name '.env.example'" in text
    assert "! -name '*.env.example'" in text
    assert "-exec rm -f -- {} +" in text
    assert 'rm -rf "$REPO_DIR/data/secrets" "$REPO_DIR/data/runtime"' in text
    assert 'CREDSTORE_DIR="${CREDSTORE_DIR:-/etc/credstore.encrypted}"' in text
    assert "systemd-creds encrypt --name=\"$name\"" in text
    assert 'install_systemd_credential "backup_evidence_hmac_key" "$BACKUP_EVIDENCE_KEY_FILE"' in text
    assert 'set_env_value "BACKUP_EVIDENCE_HMAC_KEY_FILE" ""' in text
    assert 'set_env_value "BACKUP_EVIDENCE_HMAC_KEY_SECRET" "backup_evidence_hmac_key"' in text


def test_engine_watchdog_ping_intervals_are_inside_systemd_budget() -> None:
    watchdog_sec = _duration_seconds(_read_unit("trading-engine.service")["WatchdogSec"])

    assert float(start_system._INGESTION_WATCHDOG_SLEEP_S) <= (watchdog_sec / 2.0)
    assert float(start_system._SYSTEMD_WATCHDOG_PING_SECONDS) <= (watchdog_sec / 2.0)


def test_operator_uses_unix_datagram_notify_with_systemd_notify_fallback() -> None:
    text = (REPO_ROOT / "boot" / "operator_server.js").read_text(encoding="utf-8")
    package = (REPO_ROOT / "package.json").read_text(encoding="utf-8")

    assert 'require("unix-dgram")' in text
    assert 'systemdNotify("READY=1")' in text
    assert 'systemdNotify("WATCHDOG=1")' in text
    assert "systemdNotifyViaCli" in text
    assert "/usr/bin/systemd-notify" in text
    assert "--pid=parent" in text
    assert '"unix-dgram"' in package


def test_sd_notify_noops_when_socket_unset(monkeypatch) -> None:
    monkeypatch.delenv("NOTIFY_SOCKET", raising=False)

    assert sd_notify.notify_ready() is False
    assert sd_notify.notify_watchdog() is False
    assert sd_notify.notify("STATUS=unit-test") is False


def test_sd_notify_delivers_ready_and_watchdog_datagrams(monkeypatch) -> None:
    socket_name = f"@sdnotify-test-{os.getpid()}-{id(monkeypatch)}"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        server.bind(b"\0" + socket_name[1:].encode("utf-8"))
        server.settimeout(1.0)
        monkeypatch.setenv("NOTIFY_SOCKET", socket_name)

        assert sd_notify.notify_ready() is True
        assert server.recv(1024) == b"READY=1"

        assert sd_notify.notify_watchdog() is True
        assert server.recv(1024) == b"WATCHDOG=1"
    finally:
        server.close()


def test_service_ctl_logs_prefers_file_sink(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    bin_dir = tmp_path / "bin"
    log_dir.mkdir()
    bin_dir.mkdir()
    (log_dir / "engine.log").write_text("\n".join(f"marker-{i}" for i in range(8)) + "\n", encoding="utf-8")
    _write_sudo_shim(bin_dir)

    result = subprocess.run(
        ["bash", str(REPO_ROOT / "deploy" / "bin" / "service_ctl.sh"), "logs", "engine", "3"],
        env=_service_ctl_env(bin_dir, log_dir),
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.splitlines() == ["marker-5", "marker-6", "marker-7"]
    assert "journalctl" not in result.stdout


def test_service_ctl_logs_since_prefers_file_sink_with_notice(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    bin_dir = tmp_path / "bin"
    log_dir.mkdir()
    bin_dir.mkdir()
    (log_dir / "engine.log").write_text("\n".join(f"attempt-{i}" for i in range(6)) + "\n", encoding="utf-8")
    _write_sudo_shim(bin_dir)

    result = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "deploy" / "bin" / "service_ctl.sh"),
            "logs_since",
            "engine",
            "2026-06-26 12:00:00",
            "4",
        ],
        env=_service_ctl_env(bin_dir, log_dir),
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.splitlines() == ["attempt-2", "attempt-3", "attempt-4", "attempt-5"]
    assert "# note: file sink has no --since filter; showing last 4 lines" in result.stderr
    assert "journalctl" not in result.stdout


def test_service_ctl_logs_falls_back_to_journald_when_file_sink_missing(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    bin_dir = tmp_path / "bin"
    log_dir.mkdir()
    bin_dir.mkdir()
    _write_sudo_shim(bin_dir)
    journalctl = bin_dir / "journalctl"
    journalctl.write_text(
        "#!/usr/bin/env bash\nprintf 'journalctl:%s\\n' \"$*\"\n",
        encoding="utf-8",
    )
    journalctl.chmod(journalctl.stat().st_mode | stat.S_IXUSR)

    env = _service_ctl_env(bin_dir, log_dir)
    engine_result = subprocess.run(
        ["bash", str(REPO_ROOT / "deploy" / "bin" / "service_ctl.sh"), "logs", "engine", "10"],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    backup_result = subprocess.run(
        ["bash", str(REPO_ROOT / "deploy" / "bin" / "service_ctl.sh"), "logs", "backup", "10"],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    assert engine_result.stdout.strip() == "journalctl:-u trading-engine.service -n 10 --no-pager"
    assert backup_result.stdout.strip() == "journalctl:-u trading-backup.service -n 10 --no-pager"


def test_service_ctl_logs_since_missing_arg_still_fails() -> None:
    result = subprocess.run(
        ["bash", str(REPO_ROOT / "deploy" / "bin" / "service_ctl.sh"), "logs_since", "engine"],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert '{"ok":false,"error":"missing_since"}' in result.stdout
