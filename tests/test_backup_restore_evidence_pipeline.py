from __future__ import annotations

import json
import os
import grp
import hashlib
import hmac
import stat
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _sign_payload(payload: dict, key: str, *, key_id: str = "test-key", signed_at: str | None = None) -> dict:
    signed = dict(payload)
    signed.pop("signature", None)
    payload_bytes = json.dumps(
        signed,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()
    algorithm = "hmac-sha256"
    signed_at = signed_at or (
        datetime.fromtimestamp(time.time(), tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
    metadata_bytes = json.dumps(
        {
            "algorithm": algorithm,
            "key_id": key_id,
            "payload_sha256": payload_sha256,
            "signed_at": signed_at,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    signed["signature"] = {
        "status": "signed",
        "algorithm": algorithm,
        "key_id": key_id,
        "signed_at": signed_at,
        "payload_sha256": payload_sha256,
        "value": hmac.new(key.encode("utf-8"), payload_bytes + b"\n" + metadata_bytes, hashlib.sha256).hexdigest(),
    }
    return signed


def _fresh_evidence_payload(now_ts: float | None = None) -> dict:
    now = float(now_ts if now_ts is not None else time.time())
    return {
        "schema_version": 1,
        "generated_at": (
            datetime.fromtimestamp(now, tz=timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        ),
        "generated_at_ts": now,
        "status": "pass",
        "base_backup": {
            "status": "pass",
            "backup_dir": "/var/backups/trading/base/base_20260617",
            "verify_log": "/var/backups/trading/base/base_20260617/pg_verifybackup.out",
            "verified_at_ts": now,
        },
        "wal_archive": {
            "status": "pass",
            "wal_file": "/var/backups/trading/wal/0000000100000000000000AA",
            "verified_at_ts": now,
        },
        "wal_archiver": {
            "status": "pass",
            "source": "pg_stat_archiver",
            "archive_mode": "on",
            "archive_command": '/opt/trading/ops/backup/wal_archive.sh "%p" "%f"',
            "archived_count": 10,
            "last_archived_wal": "0000000100000000000000AA",
            "last_archived_at_ts": now,
            "failed_count": 0,
            "last_failed_wal": "",
            "last_failed_at_ts": None,
        },
        "wal_archive_target": {
            "status": "pass",
            "source": "filesystem_repair",
            "root": "/var/backups/trading",
            "wal_dir": "/var/backups/trading/wal",
            "tmp_dir": "/var/backups/trading/wal/.tmp",
            "expected_owner_uid": 70,
            "expected_group": "trading",
            "expected_group_gid": 70,
            "expected_dir_mode": "2750",
            "repaired": False,
            "issue_count": 0,
            "verified_at_ts": now,
            "diagnosis": {
                "archive_command_failure_signature": "",
                "archive_command_exit_code": None,
                "fix": "",
            },
        },
        "restore_drill": {
            "status": "pass",
            "report": "/var/backups/trading/drills/restore_drill_20260617.txt",
            "verified_at_ts": now,
            "time_to_recover_s": 42,
        },
    }


def _write_executable(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def test_backup_restore_evidence_script_writes_signed_component_evidence(monkeypatch, tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()
    backup_root = tmp_path / "backup"
    evidence_dir = backup_root / "evidence"
    base_dir = backup_root / "base"
    wal_dir = backup_root / "wal"
    drill_dir = backup_root / "drills"
    latest_json = evidence_dir / "latest_backup_restore_evidence.json"
    wal_dir.mkdir(parents=True)
    (wal_dir / ".tmp").mkdir()
    backup_root.chmod(0o700)
    wal_dir.chmod(0o550)
    (wal_dir / ".tmp").chmod(0o550)

    _write_executable(
        scripts_dir / "base_backup.sh",
        """#!/usr/bin/env bash
set -euo pipefail
stamp="${TS_BACKUP_STAMP:-stub_base}"
mkdir -p "${TS_BACKUP_BASE_DIR}" "${TS_BACKUP_BASE_DIR}/${stamp}"
printf '{}\n' > "${TS_BACKUP_BASE_DIR}/${stamp}/backup_manifest"
printf 'backup successfully verified\n' > "${TS_BACKUP_BASE_DIR}/${stamp}/pg_verifybackup.out"
ln -sfn "${stamp}" "${TS_BACKUP_BASE_DIR}/latest"
""",
    )
    _write_executable(
        scripts_dir / "restore_drill.sh",
        """#!/usr/bin/env bash
set -euo pipefail
stamp="${TS_RESTORE_DRILL_STAMP:-stub_drill}"
mkdir -p "${TS_RESTORE_DRILL_DIR}"
report="${TS_RESTORE_DRILL_DIR}/restore_drill_${stamp}.txt"
{
  printf 'restore_drill_report_version=1\n'
  printf 'generated_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'exit_code=0\n'
  printf 'status=pass\n'
  printf 'time_to_recover_s=42\n'
} > "${report}"
ln -sfn "$(basename "${report}")" "${TS_RESTORE_DRILL_DIR}/latest_restore_drill.txt"
""",
    )
    _write_executable(scripts_dir / "restore.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(scripts_dir / "wal_archive.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(scripts_dir / "wal_archive_catchup.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(
        bin_dir / "psql",
        """#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  *pg_switch_wal*)
    mkdir -p "${TS_BACKUP_WAL_DIR}"
    printf 'wal segment\n' > "${TS_BACKUP_WAL_DIR}/0000000100000000000000AA"
    printf '0000000100000000000000AA\n'
    ;;
  *pg_stat_archiver*)
    now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    now_epoch="$(date +%s)"
    printf 'on|/opt/trading/ops/backup/wal_archive.sh "%%p" "%%f"|1|0000000100000000000000AA|%s|%s|0||||%s\n' "$now_iso" "$now_epoch" "$now_iso"
    ;;
  *)
    printf 'unexpected psql query: %s\n' "$*" >&2
    exit 1
    ;;
esac
""",
    )
    _write_executable(
        bin_dir / "sort",
        """#!/usr/bin/env bash
printf 'unexpected shell sort invocation\n' >&2
exit 97
""",
    )

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env.get('PATH', '')}",
            "TS_BACKUP_EVIDENCE_SKIP_SYSTEMD": "1",
            "TS_BASE_BACKUP_SCRIPT": str(scripts_dir / "base_backup.sh"),
            "TS_WAL_ARCHIVE_SCRIPT": str(REPO_ROOT / "ops" / "backup" / "wal_archive.sh"),
            "TS_WAL_ARCHIVE_CATCHUP_SCRIPT": str(scripts_dir / "wal_archive_catchup.sh"),
            "TS_RESTORE_SCRIPT": str(scripts_dir / "restore.sh"),
            "TS_RESTORE_DRILL_SCRIPT": str(scripts_dir / "restore_drill.sh"),
            "TS_BACKUP_BASE_DIR": str(base_dir),
            "TS_BACKUP_WAL_DIR": str(wal_dir),
            "TS_RESTORE_DRILL_DIR": str(drill_dir),
            "TS_BACKUP_EVIDENCE_DIR": str(evidence_dir),
            "TS_BACKUP_EVIDENCE_STAMP": "2026-06-17T120000Z",
            "TS_BACKUP_STAMP": "base_20260617",
            "TS_RESTORE_DRILL_STAMP": "drill_20260617",
            "TS_BACKUP_EVIDENCE_RUN_BASE_BACKUP": "1",
            "TS_BACKUP_EVIDENCE_RUN_RESTORE_DRILL": "1",
            "BACKUP_EVIDENCE_PATH": str(latest_json),
            "BACKUP_EVIDENCE_HMAC_KEY": "test-signing-key",
            "BACKUP_EVIDENCE_KEY_ID": "test-key",
            "BACKUP_EVIDENCE_REQUIRE_SIGNATURE": "1",
            "TS_BACKUP_READ_GROUP": grp.getgrgid(os.getgid()).gr_name,
        }
    )
    proc = subprocess.run(
        ["bash", str(REPO_ROOT / "ops" / "backup" / "backup_restore_evidence.sh")],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=30,
        check=False,
    )

    assert proc.returncode == 0, proc.stdout
    payload = json.loads(latest_json.read_text(encoding="utf-8"))
    assert payload["status"] == "pass"
    assert payload["base_backup"]["status"] == "pass"
    assert payload["base_backup"]["backup_dir"].endswith("base_20260617")
    assert payload["wal_archive"]["status"] == "pass"
    assert payload["wal_archive"]["wal_file"].endswith("0000000100000000000000AA")
    assert payload["wal_archiver"]["status"] == "pass"
    assert payload["wal_archiver"]["source"] == "pg_stat_archiver"
    assert payload["wal_archiver"]["archive_mode"] == "on"
    assert payload["wal_archiver"]["archive_command"].endswith('wal_archive.sh "%p" "%f"')
    assert payload["wal_archiver"]["last_archived_wal"] == "0000000100000000000000AA"
    assert payload["wal_archive_target"]["status"] == "pass"
    assert payload["wal_archive_target"]["expected_owner_uid"] == os.getuid()
    assert payload["wal_archive_target"]["expected_group_gid"] == os.getgid()
    assert payload["wal_archive_target"]["expected_dir_mode"] == "2750"
    assert payload["wal_archive_target"]["repaired"] is True
    assert payload["wal_archive_target"]["issue_count"] >= 1
    assert payload["wal_archive_target"]["verified_at"]
    assert payload["wal_archive_target"]["diagnosis"]["source"] == "wal_archive_probe"
    assert payload["wal_archive_target"]["diagnosis"]["archive_command_probe_status"] == "observed_failure"
    assert payload["wal_archive_target"]["diagnosis"]["archive_command_probe_wal_name"].endswith(
        ".diagnosis.2026-06-17T120000Z"
    )
    assert payload["wal_archive_target"]["diagnosis"]["archive_command_failure_signature"] == "archive_dir_not_writable"
    assert payload["wal_archive_target"]["diagnosis"]["archive_command_exit_code"] == 1
    assert payload["wal_archive_target"]["diagnosis"]["original_archive_command_failure_signature"] == "archive_dir_not_writable"
    assert payload["wal_archive_target"]["diagnosis"]["original_archive_command_exit_code"] == 1
    assert "event=archive_dir_not_writable" in payload["wal_archive_target"]["diagnosis"]["archive_command_probe_output"]
    assert "chmod 2750" in payload["wal_archive_target"]["diagnosis"]["fix"]
    assert stat.S_IMODE(backup_root.stat().st_mode) == 0o2750
    assert stat.S_IMODE(wal_dir.stat().st_mode) == 0o2750
    assert stat.S_IMODE((wal_dir / ".tmp").stat().st_mode) == 0o2750
    assert payload["restore_drill"]["status"] == "pass"
    assert payload["restore_drill"]["time_to_recover_s"] == 42
    assert payload["restore_drill"]["verified_at"]
    assert payload["signature"]["status"] == "signed"
    assert payload["signature"]["algorithm"] == "hmac-sha256"
    assert payload["signature"]["value"]
    assert stat.S_IMODE(latest_json.stat().st_mode) == 0o640
    assert latest_json.stat().st_gid == os.getgid()

    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(latest_json))
    monkeypatch.setenv("BACKUP_EVIDENCE_HMAC_KEY", "test-signing-key")
    monkeypatch.setenv("BACKUP_EVIDENCE_REQUIRE_SIGNATURE", "1")
    monkeypatch.setenv("BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RPO_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RTO_S", "300")

    from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

    state = backup_restore_evidence_snapshot(engine_mode="live")
    assert state["ok"] is True
    assert state["fresh"] is True
    assert state["signature"]["status"] == "verified"


def test_backup_evidence_requires_latest_json_even_when_fallback_files_exist(monkeypatch, tmp_path):
    now_ts = time.time()
    now = datetime.fromtimestamp(now_ts, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    base_dir = tmp_path / "base"
    backup_dir = base_dir / "base_20260617"
    wal_dir = tmp_path / "wal"
    drill_dir = tmp_path / "drills"
    backup_dir.mkdir(parents=True)
    wal_dir.mkdir()
    drill_dir.mkdir()
    (backup_dir / "backup_manifest").write_text("{}\n", encoding="utf-8")
    (backup_dir / "pg_verifybackup.out").write_text("backup successfully verified\n", encoding="utf-8")
    (base_dir / "latest").symlink_to(backup_dir.name)
    (wal_dir / "0000000100000000000000AA").write_text("wal segment\n", encoding="utf-8")
    (drill_dir / "restore_drill_20260617.txt").write_text(
        "\n".join(
            [
                "restore_drill_report_version=1",
                f"generated_at={now}",
                "exit_code=0",
                "status=pass",
                "time_to_recover_s=42",
                "",
            ]
        ),
        encoding="utf-8",
    )

    for path in (
        backup_dir,
        backup_dir / "backup_manifest",
        backup_dir / "pg_verifybackup.out",
        wal_dir / "0000000100000000000000AA",
        drill_dir / "restore_drill_20260617.txt",
    ):
        os.utime(path, (now_ts, now_ts))

    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(tmp_path / "missing_latest_backup_restore_evidence.json"))
    monkeypatch.setenv("TS_BACKUP_BASE_DIR", str(base_dir))
    monkeypatch.setenv("TS_BACKUP_WAL_DIR", str(wal_dir))
    monkeypatch.setenv("TS_RESTORE_DRILL_DIR", str(drill_dir))
    monkeypatch.setenv("BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RPO_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RTO_S", "300")

    from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

    state = backup_restore_evidence_snapshot(engine_mode="live", now_ts=now_ts + 60)
    assert state["ok"] is False
    assert state["fresh"] is False
    assert "backup_evidence_json_missing" in state["blockers"]
    assert state["base_backup"]["status"] == "pass"
    assert state["wal_archive"]["status"] == "pass"
    assert state["restore_drill"]["status"] == "pass"


def test_backup_evidence_requires_wal_archiver_component(monkeypatch, tmp_path):
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    payload = _fresh_evidence_payload()
    payload.pop("wal_archiver")
    evidence_path.write_text(
        json.dumps(_sign_payload(payload, "test-signing-key"), separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("BACKUP_EVIDENCE_HMAC_KEY", "test-signing-key")
    monkeypatch.setenv("BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RPO_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RTO_S", "300")

    from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

    state = backup_restore_evidence_snapshot(engine_mode="live")
    assert state["ok"] is False
    assert state["wal_archiver"]["status"] == "missing"
    assert "backup_evidence_wal_archiver_missing" in state["blockers"]


def test_backup_evidence_blocks_unrecovered_wal_archiver_failure(monkeypatch, tmp_path):
    now = time.time()
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    payload = _fresh_evidence_payload(now)
    payload["wal_archiver"].update(
        {
            "status": "failed",
            "failed_count": 1,
            "last_failed_wal": "0000000100000000000000AB",
            "last_failed_at_ts": now + 1,
        }
    )
    evidence_path.write_text(
        json.dumps(_sign_payload(payload, "test-signing-key"), separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("BACKUP_EVIDENCE_HMAC_KEY", "test-signing-key")
    monkeypatch.setenv("BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RPO_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RTO_S", "300")

    from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

    state = backup_restore_evidence_snapshot(engine_mode="live", now_ts=now + 2)
    assert state["ok"] is False
    assert "backup_evidence_wal_archiver_failed" in state["blockers"]
    assert "backup_evidence_wal_archiver_failure_unrecovered" in state["blockers"]


def test_backup_evidence_blocks_stale_wal_archiver(monkeypatch, tmp_path):
    now = time.time()
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    payload = _fresh_evidence_payload(now)
    payload["wal_archiver"]["last_archived_at_ts"] = now - 7200
    evidence_path.write_text(
        json.dumps(_sign_payload(payload, "test-signing-key"), separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("BACKUP_EVIDENCE_HMAC_KEY", "test-signing-key")
    monkeypatch.setenv("BACKUP_EVIDENCE_RPO_S", "120")

    from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

    state = backup_restore_evidence_snapshot(engine_mode="live", now_ts=now)
    assert state["ok"] is False
    assert "backup_evidence_wal_archiver_stale" in state["blockers"]


def test_backup_evidence_blocks_repaired_wal_target_without_diagnosis(monkeypatch, tmp_path):
    now = time.time()
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    payload = _fresh_evidence_payload(now)
    payload["wal_archive_target"]["repaired"] = True
    payload["wal_archive_target"]["issue_count"] = 1
    payload["wal_archive_target"]["diagnosis"] = {}
    evidence_path.write_text(
        json.dumps(_sign_payload(payload, "test-signing-key"), separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("BACKUP_EVIDENCE_HMAC_KEY", "test-signing-key")
    monkeypatch.setenv("BACKUP_EVIDENCE_RPO_S", "120")

    from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

    state = backup_restore_evidence_snapshot(engine_mode="live", now_ts=now)
    assert state["ok"] is False
    assert "backup_evidence_wal_archive_target_diagnosis_fix_missing" in state["blockers"]
    assert "backup_evidence_wal_archive_target_archive_command_exit_code_missing" in state["blockers"]


def test_backup_accounting_snapshot_reports_sizes_mount_and_retention(monkeypatch, tmp_path):
    backup_root = tmp_path / "trading"
    base_dir = backup_root / "base"
    wal_dir = backup_root / "wal"
    drill_dir = backup_root / "drills"
    backup_dir = base_dir / "base_20260617"
    backup_dir.mkdir(parents=True)
    wal_dir.mkdir(parents=True)
    drill_dir.mkdir(parents=True)
    (backup_dir / "backup_manifest").write_text("{}\n", encoding="utf-8")
    (backup_dir / "pg_verifybackup.out").write_text("verified\n", encoding="utf-8")
    (base_dir / "latest").symlink_to(backup_dir.name)
    (wal_dir / "0000000100000000000000AA").write_text("wal\n", encoding="utf-8")

    monkeypatch.setenv("TRADING_BACKUP_ROOT", str(backup_root))
    monkeypatch.setenv("TS_BACKUP_BASE_DIR", str(base_dir))
    monkeypatch.setenv("TS_BACKUP_WAL_DIR", str(wal_dir))
    monkeypatch.setenv("TS_RESTORE_DRILL_DIR", str(drill_dir))
    monkeypatch.setenv("TS_BACKUP_KEEP_RECENT_COUNT", "2")
    monkeypatch.setenv("TS_BACKUP_KEEP_DAILY_DAYS", "9")
    monkeypatch.setenv("TS_BACKUP_KEEP_WEEKLY_DAYS", "90")
    monkeypatch.setenv("TS_BACKUP_WAL_CUSHION_DAYS", "3")

    from engine.runtime.backup_evidence import backup_accounting_snapshot

    state = backup_accounting_snapshot(timeout_s=5)

    assert state["ok"] is True
    assert state["host_path"] == str(backup_root)
    assert state["container_path"] == "/var/backups/trading"
    assert state["root_size"]["apparent_bytes"] > 0
    assert state["subdir_sizes"]["base"]["apparent_bytes"] > 0
    assert state["inventory"]["base_backup_count"] == 1
    assert state["inventory"]["wal_file_count"] == 1
    assert state["container_mount_source"] == (
        state["container_mount"].get("mount_source", "") if state["container_mount"].get("available") else ""
    )
    assert state["retention_status"] == "configured"
    assert state["retention"]["status"] == "configured"
    assert state["retention"]["keep_recent_count"] == 2
    assert state["retention"]["keep_daily_days"] == 9
    assert state["retention"]["keep_weekly_days"] == 90
    assert state["retention"]["wal_cushion_days"] == 3
    assert "mount_source" in state["container_mount"] or state["container_mount"]["available"] is False


def test_live_backup_evidence_requires_signature_even_when_stale_env_disables_it(monkeypatch, tmp_path):
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    evidence_path.write_text(
        json.dumps(_fresh_evidence_payload(), separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RPO_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RTO_S", "300")
    monkeypatch.setenv("BACKUP_EVIDENCE_REQUIRE_SIGNATURE", "0")

    from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

    state = backup_restore_evidence_snapshot(engine_mode="live")
    assert state["ok"] is False
    assert state["signature"]["required"] is True
    assert "backup_evidence_unsigned" in state["blockers"]


def test_signed_backup_evidence_rejects_tampered_payload(monkeypatch, tmp_path):
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    payload = _sign_payload(_fresh_evidence_payload(), "test-signing-key")
    payload["restore_drill"]["time_to_recover_s"] = 10
    evidence_path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True), encoding="utf-8")
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("BACKUP_EVIDENCE_HMAC_KEY", "test-signing-key")
    monkeypatch.setenv("BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RPO_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RTO_S", "300")

    from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

    state = backup_restore_evidence_snapshot(engine_mode="live")
    assert state["ok"] is False
    assert state["signature"]["status"] == "invalid"
    assert "backup_evidence_signature_invalid" in state["blockers"]


def test_signed_backup_evidence_rejects_signature_metadata_tampering(monkeypatch, tmp_path):
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    payload = _sign_payload(_fresh_evidence_payload(), "test-signing-key")
    payload["signature"]["signed_at"] = (
        datetime.fromtimestamp(time.time() + 60, tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
    evidence_path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True), encoding="utf-8")
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("BACKUP_EVIDENCE_HMAC_KEY", "test-signing-key")
    monkeypatch.setenv("BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RPO_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RTO_S", "300")

    from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

    state = backup_restore_evidence_snapshot(engine_mode="live")
    assert state["ok"] is False
    assert state["signature"]["status"] == "invalid"
    assert "backup_evidence_signature_invalid" in state["blockers"]


def test_signed_backup_evidence_rejects_wrong_key(monkeypatch, tmp_path):
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    evidence_path.write_text(
        json.dumps(_sign_payload(_fresh_evidence_payload(), "correct-key"), separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("BACKUP_EVIDENCE_HMAC_KEY", "wrong-key")
    monkeypatch.setenv("BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RPO_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RTO_S", "300")

    from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

    state = backup_restore_evidence_snapshot(engine_mode="live")
    assert state["ok"] is False
    assert state["signature"]["status"] == "invalid"
    assert "backup_evidence_signature_invalid" in state["blockers"]


def test_signed_backup_evidence_rejects_malformed_signature(monkeypatch, tmp_path):
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    payload = _fresh_evidence_payload()
    payload["signature"] = {
        "status": "signed",
        "algorithm": "hmac-sha256",
        "value": "abc123",
    }
    evidence_path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True), encoding="utf-8")
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("BACKUP_EVIDENCE_HMAC_KEY", "test-signing-key")
    monkeypatch.setenv("BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RPO_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RTO_S", "300")

    from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

    state = backup_restore_evidence_snapshot(engine_mode="live")
    assert state["ok"] is False
    assert state["signature"]["status"] == "invalid"
    assert "backup_evidence_signature_malformed" in state["blockers"]


def test_signed_backup_evidence_rejects_stale_signature(monkeypatch, tmp_path):
    now = time.time()
    stale_signed_at = (
        datetime.fromtimestamp(now - 7200, tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    evidence_path.write_text(
        json.dumps(
            _sign_payload(_fresh_evidence_payload(now), "test-signing-key", signed_at=stale_signed_at),
            separators=(",", ":"),
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("BACKUP_EVIDENCE_HMAC_KEY", "test-signing-key")
    monkeypatch.setenv("BACKUP_EVIDENCE_SIGNATURE_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RPO_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RTO_S", "300")

    from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

    state = backup_restore_evidence_snapshot(engine_mode="live", now_ts=now)
    assert state["ok"] is False
    assert state["signature"]["status"] == "stale"
    assert "backup_evidence_signature_stale" in state["blockers"]


def test_signed_backup_evidence_requires_validation_key(monkeypatch, tmp_path):
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    evidence_path.write_text(
        json.dumps(_sign_payload(_fresh_evidence_payload(), "test-signing-key"), separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.delenv("BACKUP_EVIDENCE_HMAC_KEY", raising=False)
    monkeypatch.delenv("BACKUP_EVIDENCE_SIGNING_KEY", raising=False)
    monkeypatch.delenv("BACKUP_EVIDENCE_HMAC_KEY_FILE", raising=False)
    monkeypatch.delenv("BACKUP_EVIDENCE_SIGNING_KEY_FILE", raising=False)

    from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

    state = backup_restore_evidence_snapshot(engine_mode="live")
    assert state["ok"] is False
    assert state["signature"]["status"] == "unverified"
    assert "backup_evidence_signature_key_missing" in state["blockers"]


def test_signed_backup_evidence_verifies_with_key_file(monkeypatch, tmp_path):
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    key_file = tmp_path / "backup_evidence_hmac_key"
    key_file.write_text("test-signing-key\n", encoding="utf-8")
    evidence_path.write_text(
        json.dumps(_sign_payload(_fresh_evidence_payload(), "test-signing-key"), separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("BACKUP_EVIDENCE_HMAC_KEY_FILE", str(key_file))
    monkeypatch.delenv("BACKUP_EVIDENCE_HMAC_KEY", raising=False)
    monkeypatch.delenv("BACKUP_EVIDENCE_SIGNING_KEY", raising=False)

    from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

    state = backup_restore_evidence_snapshot(engine_mode="live")
    assert state["ok"] is True
    assert state["signature"]["status"] == "verified"
    assert state["signature"]["key_source"] == "file:BACKUP_EVIDENCE_HMAC_KEY_FILE"


def test_signed_backup_evidence_verifies_with_systemd_secret_ref(monkeypatch, tmp_path):
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    cred_dir = tmp_path / "credentials"
    cred_dir.mkdir()
    (cred_dir / "backup_evidence_hmac_key").write_text("test-signing-key\n", encoding="utf-8")
    evidence_path.write_text(
        json.dumps(_sign_payload(_fresh_evidence_payload(), "test-signing-key"), separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("TS_SECRETS_PROVIDER", "systemd-creds")
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(cred_dir))
    monkeypatch.setenv("BACKUP_EVIDENCE_HMAC_KEY_SECRET", "backup_evidence_hmac_key")
    monkeypatch.setenv("TS_CREDENTIAL_AUDIT_ENABLED", "0")
    monkeypatch.delenv("BACKUP_EVIDENCE_HMAC_KEY", raising=False)
    monkeypatch.delenv("BACKUP_EVIDENCE_SIGNING_KEY", raising=False)
    monkeypatch.delenv("BACKUP_EVIDENCE_HMAC_KEY_FILE", raising=False)
    monkeypatch.delenv("BACKUP_EVIDENCE_SIGNING_KEY_FILE", raising=False)

    from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

    state = backup_restore_evidence_snapshot(engine_mode="live")
    assert state["ok"] is True
    assert state["signature"]["status"] == "verified"
    assert state["signature"]["key_source"] == "secret:BACKUP_EVIDENCE_HMAC_KEY_SECRET"


def test_signed_backup_evidence_reports_unreadable_key(monkeypatch, tmp_path):
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    evidence_path.write_text(
        json.dumps(_sign_payload(_fresh_evidence_payload(), "test-signing-key"), separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    unreadable_key = tmp_path / "key-as-directory"
    unreadable_key.mkdir()
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("BACKUP_EVIDENCE_HMAC_KEY_FILE", str(unreadable_key))
    monkeypatch.delenv("BACKUP_EVIDENCE_HMAC_KEY", raising=False)
    monkeypatch.delenv("BACKUP_EVIDENCE_SIGNING_KEY", raising=False)

    from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

    state = backup_restore_evidence_snapshot(engine_mode="live")
    assert state["ok"] is False
    assert state["signature"]["status"] == "key_unreadable"
    assert "backup_evidence_signature_key_unreadable" in state["blockers"]


def test_backup_evidence_rejects_stale_wal_archive(monkeypatch, tmp_path):
    now = time.time()
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    payload = _fresh_evidence_payload(now)
    payload["wal_archive"]["verified_at_ts"] = now - 7200
    evidence_path.write_text(
        json.dumps(_sign_payload(payload, "test-signing-key"), separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("BACKUP_EVIDENCE_HMAC_KEY", "test-signing-key")
    monkeypatch.setenv("BACKUP_EVIDENCE_RPO_S", "120")

    from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

    state = backup_restore_evidence_snapshot(engine_mode="live", now_ts=now)
    assert state["ok"] is False
    assert "backup_evidence_wal_archive_stale" in state["blockers"]


def test_pg_wal_disk_risk_snapshot_blocks_large_ready_backlog(monkeypatch, tmp_path):
    class _Cursor:
        def __init__(self, row):
            self._row = row

        def fetchone(self):
            return self._row

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql):
            text = " ".join(str(sql).lower().split())
            if "from pg_ls_waldir()" in text:
                return _Cursor((128, 8))
            if "from pg_ls_dir('pg_wal/archive_status')" in text:
                return _Cursor((4,))
            raise AssertionError(f"unexpected SQL: {sql}")

    class _FakePsycopg:
        @staticmethod
        def connect(*args, **kwargs):
            return _FakeConnection()

    pg_wal = tmp_path / "pgdata" / "pg_wal"
    pg_wal.mkdir(parents=True)
    monkeypatch.setitem(sys.modules, "psycopg", _FakePsycopg)
    monkeypatch.setenv("TS_PG_DSN", "dbname=trading user=trading password=test")
    monkeypatch.setenv("TS_PG_WAL_DIR", str(pg_wal))
    monkeypatch.setenv("PREFLIGHT_PG_WAL_CRITICAL_BYTES", "64")
    monkeypatch.setenv("PREFLIGHT_PG_WAL_READY_CRITICAL_COUNT", "3")
    monkeypatch.setenv("PREFLIGHT_PG_WAL_CRITICAL_FREE_BYTES", "0")
    monkeypatch.setenv("PREFLIGHT_PG_WAL_WARN_FREE_BYTES", "0")

    from engine.runtime.backup_evidence import pg_wal_disk_risk_snapshot

    state = pg_wal_disk_risk_snapshot(engine_mode="live")

    assert state["ok"] is False
    assert state["wal_bytes"] == 128
    assert state["ready_count"] == 4
    assert "pg_wal_bytes_exceeds_budget" in state["blockers"]
    assert "pg_wal_ready_backlog_critical" in state["blockers"]


def test_pg_wal_disk_risk_snapshot_derives_local_wal_from_timescale_data(monkeypatch, tmp_path):
    class _Cursor:
        def __init__(self, row):
            self._row = row

        def fetchone(self):
            return self._row

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql):
            text = " ".join(str(sql).lower().split())
            if "from pg_ls_waldir()" in text:
                return _Cursor((0, 0))
            if "from pg_ls_dir('pg_wal/archive_status')" in text:
                return _Cursor((0,))
            raise AssertionError(f"unexpected SQL: {sql}")

    class _FakePsycopg:
        @staticmethod
        def connect(*args, **kwargs):
            return _FakeConnection()

    timescale_data = tmp_path / "timescaledb" / "data"
    pg_wal = timescale_data / "pg_wal"
    pg_wal.mkdir(parents=True)
    monkeypatch.setitem(sys.modules, "psycopg", _FakePsycopg)
    monkeypatch.setenv("TS_PG_DSN", "dbname=trading user=trading password=test")
    monkeypatch.delenv("TS_PG_WAL_DIR", raising=False)
    monkeypatch.setenv("TRADING_TIMESCALE_DATA", str(timescale_data))
    monkeypatch.setenv("PREFLIGHT_PG_WAL_CRITICAL_BYTES", str(1024 * 1024 * 1024))
    monkeypatch.setenv("PREFLIGHT_PG_WAL_READY_CRITICAL_COUNT", "100")
    monkeypatch.setenv("PREFLIGHT_PG_WAL_CRITICAL_FREE_BYTES", "0")
    monkeypatch.setenv("PREFLIGHT_PG_WAL_WARN_FREE_BYTES", "0")

    from engine.runtime.backup_evidence import pg_wal_disk_risk_snapshot

    state = pg_wal_disk_risk_snapshot(engine_mode="live")

    assert state["ok"] is True
    assert state["local_space"]["path"] == str(pg_wal)
    assert state["local_space"]["path_source"] == "TRADING_TIMESCALE_DATA"
    assert state["local_space"]["visible"] is True


def test_pg_wal_disk_risk_snapshot_blocks_missing_required_local_wal_evidence(monkeypatch):
    class _Cursor:
        def __init__(self, row):
            self._row = row

        def fetchone(self):
            return self._row

    class _FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql):
            text = " ".join(str(sql).lower().split())
            if "from pg_ls_waldir()" in text:
                return _Cursor((0, 0))
            if "from pg_ls_dir('pg_wal/archive_status')" in text:
                return _Cursor((0,))
            raise AssertionError(f"unexpected SQL: {sql}")

    class _FakePsycopg:
        @staticmethod
        def connect(*args, **kwargs):
            return _FakeConnection()

    monkeypatch.setitem(sys.modules, "psycopg", _FakePsycopg)
    monkeypatch.setenv("TS_PG_DSN", "dbname=trading user=trading password=test")
    monkeypatch.delenv("TS_PG_WAL_DIR", raising=False)
    monkeypatch.setenv("TRADING_TIMESCALE_DATA", "/not-visible/timescaledb/data")
    monkeypatch.setenv("PREFLIGHT_PG_WAL_CRITICAL_BYTES", str(1024 * 1024 * 1024))
    monkeypatch.setenv("PREFLIGHT_PG_WAL_READY_CRITICAL_COUNT", "100")

    from engine.runtime.backup_evidence import pg_wal_disk_risk_snapshot

    state = pg_wal_disk_risk_snapshot(engine_mode="live")

    assert state["ok"] is False
    assert state["local_space"]["path"] == "/not-visible/timescaledb/data/pg_wal"
    assert state["local_space"]["path_source"] == "TRADING_TIMESCALE_DATA"
    assert "pg_wal_free_space_not_visible" in state["blockers"]


def test_backup_evidence_rejects_incomplete_json(monkeypatch, tmp_path):
    now = time.time()
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    payload = {
        "schema_version": 1,
        "generated_at_ts": now,
        "status": "pass",
        "base_backup": {"status": "pass", "verified_at_ts": now},
        "wal_archive": {"status": "pass", "verified_at_ts": now},
        "wal_archiver": {"status": "pass", "archive_mode": "on", "archive_command": 'wal_archive.sh "%p" "%f"'},
        "restore_drill": {"status": "pass", "verified_at_ts": now, "time_to_recover_s": 42},
    }
    evidence_path.write_text(
        json.dumps(_sign_payload(payload, "test-signing-key"), separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("BACKUP_EVIDENCE_HMAC_KEY", "test-signing-key")

    from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

    state = backup_restore_evidence_snapshot(engine_mode="live", now_ts=now)
    assert state["ok"] is False
    assert "backup_evidence_base_backup_dir_missing" in state["blockers"]
    assert "backup_evidence_wal_file_missing" in state["blockers"]
    assert "backup_evidence_wal_archiver_last_wal_missing" in state["blockers"]
    assert "backup_evidence_restore_drill_report_missing" in state["blockers"]


def test_backup_restore_evidence_script_records_hung_base_backup_timeout(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()
    backup_root = tmp_path / "backup"
    evidence_dir = backup_root / "evidence"
    base_dir = backup_root / "base"
    wal_dir = backup_root / "wal"
    drill_dir = backup_root / "drills"
    latest_json = evidence_dir / "latest_backup_restore_evidence.json"
    wal_dir.mkdir(parents=True)

    _write_executable(
        scripts_dir / "base_backup.sh",
        """#!/usr/bin/env bash
sleep 5
""",
    )
    _write_executable(
        scripts_dir / "restore_drill.sh",
        """#!/usr/bin/env bash
set -euo pipefail
stamp="${TS_RESTORE_DRILL_STAMP:-stub_drill}"
mkdir -p "${TS_RESTORE_DRILL_DIR}"
report="${TS_RESTORE_DRILL_DIR}/restore_drill_${stamp}.txt"
{
  printf 'restore_drill_report_version=1\n'
  printf 'generated_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'exit_code=0\n'
  printf 'status=pass\n'
  printf 'time_to_recover_s=42\n'
} > "${report}"
""",
    )
    _write_executable(scripts_dir / "restore.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(scripts_dir / "wal_archive.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(scripts_dir / "wal_archive_catchup.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(
        bin_dir / "psql",
        """#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  *pg_switch_wal*)
    mkdir -p "${TS_BACKUP_WAL_DIR}"
    printf 'wal segment\n' > "${TS_BACKUP_WAL_DIR}/0000000100000000000000AA"
    ;;
  *pg_stat_archiver*)
    now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    now_epoch="$(date +%s)"
    printf 'on|/opt/trading/ops/backup/wal_archive.sh "%%p" "%%f"|1|0000000100000000000000AA|%s|%s|0||||%s\n' "$now_iso" "$now_epoch" "$now_iso"
    ;;
esac
""",
    )

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env.get('PATH', '')}",
            "TS_BACKUP_EVIDENCE_SKIP_SYSTEMD": "1",
            "TS_BASE_BACKUP_SCRIPT": str(scripts_dir / "base_backup.sh"),
            "TS_WAL_ARCHIVE_SCRIPT": str(scripts_dir / "wal_archive.sh"),
            "TS_WAL_ARCHIVE_CATCHUP_SCRIPT": str(scripts_dir / "wal_archive_catchup.sh"),
            "TS_RESTORE_SCRIPT": str(scripts_dir / "restore.sh"),
            "TS_RESTORE_DRILL_SCRIPT": str(scripts_dir / "restore_drill.sh"),
            "TS_BACKUP_BASE_DIR": str(base_dir),
            "TS_BACKUP_WAL_DIR": str(wal_dir),
            "TS_RESTORE_DRILL_DIR": str(drill_dir),
            "TS_BACKUP_EVIDENCE_DIR": str(evidence_dir),
            "TS_BACKUP_EVIDENCE_STAMP": "2026-06-17T130000Z",
            "TS_RESTORE_DRILL_STAMP": "drill_timeout",
            "TS_BACKUP_EVIDENCE_RUN_BASE_BACKUP": "1",
            "TS_BACKUP_EVIDENCE_BASE_BACKUP_TIMEOUT_S": "0.2",
            "BACKUP_EVIDENCE_PATH": str(latest_json),
            "BACKUP_EVIDENCE_HMAC_KEY": "test-signing-key",
            "BACKUP_EVIDENCE_REQUIRE_SIGNATURE": "1",
        }
    )
    proc = subprocess.run(
        ["bash", str(REPO_ROOT / "ops" / "backup" / "backup_restore_evidence.sh")],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=20,
        check=False,
    )

    assert proc.returncode != 0
    payload = json.loads(latest_json.read_text(encoding="utf-8"))
    assert payload["status"] == "fail"
    assert payload["base_backup"]["status"] == "timeout"
    assert payload["timeouts"]["base_backup_s"] == 0.2


def test_backup_restore_evidence_script_default_does_not_run_overdue_base_or_drill(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()
    backup_root = tmp_path / "backup"
    evidence_dir = backup_root / "evidence"
    base_dir = backup_root / "base"
    wal_dir = backup_root / "wal"
    drill_dir = backup_root / "drills"
    latest_json = evidence_dir / "latest_backup_restore_evidence.json"
    marker = tmp_path / "heavy_job_ran"
    base_dir.mkdir(parents=True)
    wal_dir.mkdir(parents=True)
    drill_dir.mkdir(parents=True)

    _write_executable(
        scripts_dir / "base_backup.sh",
        f"""#!/usr/bin/env bash
printf ran > "{marker}"
exit 97
""",
    )
    _write_executable(
        scripts_dir / "restore_drill.sh",
        f"""#!/usr/bin/env bash
printf ran > "{marker}"
exit 98
""",
    )
    _write_executable(scripts_dir / "restore.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(scripts_dir / "wal_archive.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(scripts_dir / "wal_archive_catchup.sh", "#!/usr/bin/env bash\nexit 99\n")
    _write_executable(
        bin_dir / "psql",
        """#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  *pg_switch_wal*)
    mkdir -p "${TS_BACKUP_WAL_DIR}"
    printf 'wal segment\n' > "${TS_BACKUP_WAL_DIR}/0000000100000000000000AA"
    ;;
  *pg_stat_archiver*)
    now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    now_epoch="$(date +%s)"
    printf 'on|/opt/trading/ops/backup/wal_archive.sh "%%p" "%%f"|1|0000000100000000000000AA|%s|%s|0||||%s\n' "$now_iso" "$now_epoch" "$now_iso"
    ;;
esac
""",
    )

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env.get('PATH', '')}",
            "TS_BACKUP_EVIDENCE_SKIP_SYSTEMD": "1",
            "TS_BASE_BACKUP_SCRIPT": str(scripts_dir / "base_backup.sh"),
            "TS_WAL_ARCHIVE_SCRIPT": str(scripts_dir / "wal_archive.sh"),
            "TS_WAL_ARCHIVE_CATCHUP_SCRIPT": str(scripts_dir / "wal_archive_catchup.sh"),
            "TS_RESTORE_SCRIPT": str(scripts_dir / "restore.sh"),
            "TS_RESTORE_DRILL_SCRIPT": str(scripts_dir / "restore_drill.sh"),
            "TS_BACKUP_BASE_DIR": str(base_dir),
            "TS_BACKUP_WAL_DIR": str(wal_dir),
            "TS_RESTORE_DRILL_DIR": str(drill_dir),
            "TS_BACKUP_EVIDENCE_DIR": str(evidence_dir),
            "TS_BACKUP_EVIDENCE_STAMP": "2026-06-17T140000Z",
            "BACKUP_EVIDENCE_PATH": str(latest_json),
            "BACKUP_EVIDENCE_HMAC_KEY": "test-signing-key",
            "BACKUP_EVIDENCE_REQUIRE_SIGNATURE": "1",
        }
    )
    proc = subprocess.run(
        ["bash", str(REPO_ROOT / "ops" / "backup" / "backup_restore_evidence.sh")],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=20,
        check=False,
    )

    assert proc.returncode != 0
    assert not marker.exists()
    payload = json.loads(latest_json.read_text(encoding="utf-8"))
    assert payload["status"] == "fail"
    assert payload["base_backup"]["status"] == "missing"
    assert payload["restore_drill"]["status"] == "missing"
    assert payload["wal_catchup"]["status"] == "skipped"


def test_backup_restore_evidence_script_fails_when_forced_restore_drill_has_no_report(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()
    backup_root = tmp_path / "backup"
    evidence_dir = backup_root / "evidence"
    base_dir = backup_root / "base"
    backup_dir = base_dir / "base_20260617"
    wal_dir = backup_root / "wal"
    drill_dir = backup_root / "drills"
    latest_json = evidence_dir / "latest_backup_restore_evidence.json"
    now = time.time()
    backup_dir.mkdir(parents=True)
    wal_dir.mkdir(parents=True)
    drill_dir.mkdir(parents=True)
    (backup_dir / "backup_manifest").write_text("{}\n", encoding="utf-8")
    (backup_dir / "pg_verifybackup.out").write_text("backup successfully verified\n", encoding="utf-8")
    (base_dir / "latest").symlink_to(backup_dir.name)
    os.utime(backup_dir / "pg_verifybackup.out", (now, now))

    _write_executable(scripts_dir / "base_backup.sh", "#!/usr/bin/env bash\nexit 97\n")
    _write_executable(scripts_dir / "restore_drill.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(scripts_dir / "restore.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(scripts_dir / "wal_archive.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(scripts_dir / "wal_archive_catchup.sh", "#!/usr/bin/env bash\nexit 99\n")
    _write_executable(
        bin_dir / "psql",
        """#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  *pg_switch_wal*)
    mkdir -p "${TS_BACKUP_WAL_DIR}"
    printf 'wal segment\n' > "${TS_BACKUP_WAL_DIR}/0000000100000000000000AA"
    ;;
  *pg_stat_archiver*)
    now_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    now_epoch="$(date +%s)"
    printf 'on|/opt/trading/ops/backup/wal_archive.sh "%%p" "%%f"|1|0000000100000000000000AA|%s|%s|0||||%s\n' "$now_iso" "$now_epoch" "$now_iso"
    ;;
esac
""",
    )

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env.get('PATH', '')}",
            "TS_BACKUP_EVIDENCE_SKIP_SYSTEMD": "1",
            "TS_BASE_BACKUP_SCRIPT": str(scripts_dir / "base_backup.sh"),
            "TS_WAL_ARCHIVE_SCRIPT": str(scripts_dir / "wal_archive.sh"),
            "TS_WAL_ARCHIVE_CATCHUP_SCRIPT": str(scripts_dir / "wal_archive_catchup.sh"),
            "TS_RESTORE_SCRIPT": str(scripts_dir / "restore.sh"),
            "TS_RESTORE_DRILL_SCRIPT": str(scripts_dir / "restore_drill.sh"),
            "TS_BACKUP_BASE_DIR": str(base_dir),
            "TS_BACKUP_WAL_DIR": str(wal_dir),
            "TS_RESTORE_DRILL_DIR": str(drill_dir),
            "TS_BACKUP_EVIDENCE_DIR": str(evidence_dir),
            "TS_BACKUP_EVIDENCE_STAMP": "2026-06-17T143000Z",
            "TS_BACKUP_EVIDENCE_RUN_RESTORE_DRILL": "1",
            "BACKUP_EVIDENCE_PATH": str(latest_json),
            "BACKUP_EVIDENCE_HMAC_KEY": "test-signing-key",
            "BACKUP_EVIDENCE_REQUIRE_SIGNATURE": "1",
        }
    )
    proc = subprocess.run(
        ["bash", str(REPO_ROOT / "ops" / "backup" / "backup_restore_evidence.sh")],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=20,
        check=False,
    )

    assert proc.returncode != 0
    payload = json.loads(latest_json.read_text(encoding="utf-8"))
    assert payload["status"] == "fail"
    assert payload["base_backup"]["status"] == "pass"
    assert payload["restore_drill"]["status"] == "fail"


def test_backup_restore_evidence_script_fails_on_unrecovered_wal_archiver_failure(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()
    backup_root = tmp_path / "backup"
    evidence_dir = backup_root / "evidence"
    base_dir = backup_root / "base"
    backup_dir = base_dir / "base_20260617"
    wal_dir = backup_root / "wal"
    drill_dir = backup_root / "drills"
    latest_json = evidence_dir / "latest_backup_restore_evidence.json"
    now = time.time()
    old = now - 60
    backup_dir.mkdir(parents=True)
    wal_dir.mkdir(parents=True)
    drill_dir.mkdir(parents=True)
    (backup_dir / "backup_manifest").write_text("{}\n", encoding="utf-8")
    (backup_dir / "pg_verifybackup.out").write_text("backup successfully verified\n", encoding="utf-8")
    (base_dir / "latest").symlink_to(backup_dir.name)
    report = drill_dir / "restore_drill_20260617.txt"
    generated_at = datetime.fromtimestamp(now, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    report.write_text(
        "\n".join(
            [
                "restore_drill_report_version=1",
                f"generated_at={generated_at}",
                "exit_code=0",
                "status=pass",
                "time_to_recover_s=42",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (drill_dir / "latest_restore_drill.txt").symlink_to(report.name)
    os.utime(backup_dir / "pg_verifybackup.out", (now, now))
    os.utime(report, (now, now))

    _write_executable(scripts_dir / "base_backup.sh", "#!/usr/bin/env bash\nexit 97\n")
    _write_executable(scripts_dir / "restore_drill.sh", "#!/usr/bin/env bash\nexit 98\n")
    _write_executable(scripts_dir / "restore.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(scripts_dir / "wal_archive.sh", "#!/usr/bin/env bash\nexit 0\n")
    _write_executable(scripts_dir / "wal_archive_catchup.sh", "#!/usr/bin/env bash\nexit 99\n")
    _write_executable(
        bin_dir / "psql",
        f"""#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  *pg_switch_wal*)
    mkdir -p "${{TS_BACKUP_WAL_DIR}}"
    printf 'wal segment\n' > "${{TS_BACKUP_WAL_DIR}}/0000000100000000000000AA"
    ;;
  *pg_stat_archiver*)
    printf 'on|/opt/trading/ops/backup/wal_archive.sh "%%p" "%%f"|1|0000000100000000000000AA|2026-06-17T12:00:00Z|{int(old)}|1|0000000100000000000000AB|2026-06-17T12:01:00Z|{int(now)}|2026-06-17T12:00:00Z\n'
    ;;
esac
""",
    )

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env.get('PATH', '')}",
            "TS_BACKUP_EVIDENCE_SKIP_SYSTEMD": "1",
            "TS_BASE_BACKUP_SCRIPT": str(scripts_dir / "base_backup.sh"),
            "TS_WAL_ARCHIVE_SCRIPT": str(scripts_dir / "wal_archive.sh"),
            "TS_WAL_ARCHIVE_CATCHUP_SCRIPT": str(scripts_dir / "wal_archive_catchup.sh"),
            "TS_RESTORE_SCRIPT": str(scripts_dir / "restore.sh"),
            "TS_RESTORE_DRILL_SCRIPT": str(scripts_dir / "restore_drill.sh"),
            "TS_BACKUP_BASE_DIR": str(base_dir),
            "TS_BACKUP_WAL_DIR": str(wal_dir),
            "TS_RESTORE_DRILL_DIR": str(drill_dir),
            "TS_BACKUP_EVIDENCE_DIR": str(evidence_dir),
            "TS_BACKUP_EVIDENCE_STAMP": "2026-06-17T150000Z",
            "BACKUP_EVIDENCE_WAL_RPO_S": "3600",
            "BACKUP_EVIDENCE_PATH": str(latest_json),
            "BACKUP_EVIDENCE_HMAC_KEY": "test-signing-key",
            "BACKUP_EVIDENCE_REQUIRE_SIGNATURE": "1",
        }
    )
    proc = subprocess.run(
        ["bash", str(REPO_ROOT / "ops" / "backup" / "backup_restore_evidence.sh")],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=20,
        check=False,
    )

    assert proc.returncode != 0
    payload = json.loads(latest_json.read_text(encoding="utf-8"))
    assert payload["status"] == "fail"
    assert payload["wal_archiver"]["status"] == "fail"
    assert payload["wal_archiver"]["last_failed_wal"] == "0000000100000000000000AB"
