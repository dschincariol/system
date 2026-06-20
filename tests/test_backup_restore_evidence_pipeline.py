from __future__ import annotations

import json
import os
import grp
import hashlib
import hmac
import stat
import subprocess
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
        "generated_at_ts": now,
        "status": "pass",
        "base_backup": {"status": "pass", "verified_at_ts": now},
        "wal_archive": {"status": "pass", "verified_at_ts": now},
        "restore_drill": {
            "status": "pass",
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
    _write_executable(
        bin_dir / "psql",
        """#!/usr/bin/env bash
set -euo pipefail
mkdir -p "${TS_BACKUP_WAL_DIR}"
printf 'wal segment\n' > "${TS_BACKUP_WAL_DIR}/0000000100000000000000AA"
printf '0000000100000000000000AA\n'
""",
    )

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env.get('PATH', '')}",
            "TS_BACKUP_EVIDENCE_SKIP_SYSTEMD": "1",
            "TS_BASE_BACKUP_SCRIPT": str(scripts_dir / "base_backup.sh"),
            "TS_WAL_ARCHIVE_SCRIPT": str(scripts_dir / "wal_archive.sh"),
            "TS_RESTORE_SCRIPT": str(scripts_dir / "restore.sh"),
            "TS_RESTORE_DRILL_SCRIPT": str(scripts_dir / "restore_drill.sh"),
            "TS_BACKUP_BASE_DIR": str(base_dir),
            "TS_BACKUP_WAL_DIR": str(wal_dir),
            "TS_RESTORE_DRILL_DIR": str(drill_dir),
            "TS_BACKUP_EVIDENCE_DIR": str(evidence_dir),
            "TS_BACKUP_EVIDENCE_STAMP": "2026-06-17T120000Z",
            "TS_BACKUP_STAMP": "base_20260617",
            "TS_RESTORE_DRILL_STAMP": "drill_20260617",
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
