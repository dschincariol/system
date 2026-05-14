from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import textwrap

import pytest

from services.credential_encryption import decrypt_credentials, encrypt_credentials
from services.secrets.rotation import re_encrypt_data_sources


class _FakeStorage:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con

    @contextlib.contextmanager
    def connect_ro_direct(self):
        yield self.con

    def run_write_txn(self, fn):
        result = fn(self.con)
        self.con.commit()
        return result


def test_re_encrypt_data_sources_moves_rows_to_new_key(monkeypatch, tmp_path):
    secret_dir = tmp_path / "secrets"
    secret_dir.mkdir()
    (secret_dir / "key_a").write_bytes(b"alpha")
    (secret_dir / "key_b").write_bytes(b"bravo")

    monkeypatch.setenv("TS_SECRETS_PROVIDER", "plaintext")
    monkeypatch.setenv("TS_DEV_SECRETS_DIR", str(secret_dir))
    monkeypatch.delenv("TS_ENV", raising=False)
    sys.modules.pop("services.secrets.providers.plaintext", None)

    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute(
        """
        CREATE TABLE data_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_key TEXT NOT NULL,
            credentials_enc TEXT,
            key_version TEXT NOT NULL
        )
        """
    )
    with pytest.warns(RuntimeWarning):
        for idx in range(5):
            con.execute(
                "INSERT INTO data_sources(source_key, credentials_enc, key_version) VALUES(?, ?, ?)",
                (
                    f"source_{idx}",
                    encrypt_credentials({"api_key": f"secret_{idx}"}, key_name="key_a"),
                    "key_a",
                ),
            )
    con.commit()

    result = re_encrypt_data_sources(
        old_key_name="key_a",
        new_key_name="key_b",
        storage_module=_FakeStorage(con),
    )

    assert result["scanned"] == 5
    assert result["rotated"] == 5
    assert result["verified"] == 5

    rows = con.execute("SELECT credentials_enc, key_version FROM data_sources ORDER BY id").fetchall()
    for idx, row in enumerate(rows):
        assert row["key_version"] == "key_b"
        assert decrypt_credentials(row["credentials_enc"], key_name="key_b") == {"api_key": f"secret_{idx}"}
        with pytest.raises(Exception):
            decrypt_credentials(row["credentials_enc"], key_name="key_a")


def test_rotation_script_exits_nonzero_and_records_metric_on_row_decryption_failure(tmp_path):
    secret_dir = tmp_path / "secrets"
    secret_dir.mkdir()
    (secret_dir / "key_a").write_bytes(b"alpha")
    (secret_dir / "key_b").write_bytes(b"bravo")
    metrics_path = tmp_path / "rotation_metric.json"
    repo_root = Path(__file__).resolve().parents[1]

    script = textwrap.dedent(
        f"""
        import contextlib
        import json
        import sqlite3
        from pathlib import Path

        from services.credential_encryption import encrypt_credentials
        from services.secrets import loader
        import services.secrets.rotation as rotation

        metrics_path = Path({str(metrics_path)!r})
        loader._insert_access_log = lambda **_kwargs: None

        def fake_emit_counter(metric, value=1, **kwargs):
            metrics_path.write_text(
                json.dumps({{"metric": metric, "value": value, "kwargs": kwargs}}),
                encoding="utf-8",
            )

        rotation.emit_counter = fake_emit_counter

        class FakeStorage:
            def __init__(self, con):
                self.con = con

            @contextlib.contextmanager
            def connect_ro_direct(self):
                yield self.con

            def run_write_txn(self, fn):
                result = fn(self.con)
                self.con.commit()
                return result

        con = sqlite3.connect(":memory:")
        con.row_factory = sqlite3.Row
        con.execute(
            '''
            CREATE TABLE data_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_key TEXT NOT NULL,
                credentials_enc TEXT,
                key_version TEXT NOT NULL
            )
            '''
        )
        bad_blob = encrypt_credentials({{"api_key": "encrypted_with_new_key"}}, key_name="key_b")
        con.execute(
            "INSERT INTO data_sources(source_key, credentials_enc, key_version) VALUES(?, ?, ?)",
            ("source_bad", bad_blob, "key_a"),
        )
        con.commit()

        rotation.re_encrypt_data_sources(
            old_key_name="key_a",
            new_key_name="key_b",
            storage_module=FakeStorage(con),
        )
        """
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root) + os.pathsep + env.get("PYTHONPATH", "")
    env["TS_SECRETS_PROVIDER"] = "plaintext"
    env["TS_DEV_SECRETS_DIR"] = str(secret_dir)
    env.pop("TS_ENV", None)

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode != 0
    metric = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert metric["metric"] == "credential_rotation_row_failures"
    assert metric["kwargs"]["component"] == "services.secrets.rotation"
    assert metric["kwargs"]["extra_tags"]["row_id"] == 1
    assert metric["kwargs"]["extra_tags"]["error_class"] == "InvalidTag"
