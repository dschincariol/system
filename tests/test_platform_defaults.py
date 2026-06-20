import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.runtime import platform as runtime_platform


def _plaintext_secret(monkeypatch, tmp_path, value="secret"):
    from services.secrets import loader

    (tmp_path / "pg_password_app").write_text(value, encoding="utf-8")
    monkeypatch.setenv("TS_SECRETS_PROVIDER", "plaintext")
    monkeypatch.setenv("TS_DEV_SECRETS_DIR", str(tmp_path))
    monkeypatch.delenv("TS_ENV", raising=False)
    monkeypatch.setattr(loader, "_record_access", lambda **kwargs: None)
    sys.modules.pop("services.secrets.providers.plaintext", None)


def test_linux_defaults(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_platform.sys, "platform", "linux")
    monkeypatch.delenv("TS_DATA_ROOT", raising=False)
    monkeypatch.delenv("TS_PG_PORT", raising=False)
    _plaintext_secret(monkeypatch, tmp_path)
    assert runtime_platform.default_pg_dsn() == (
        "host=/var/run/postgresql port=6432 user=ts_app dbname=trading password=secret"
    )
    assert runtime_platform.default_admin_pg_dsn() == (
        "host=/var/run/postgresql port=5432 user=postgres dbname=postgres"
    )
    assert runtime_platform.default_data_root() == Path("/var/lib/trading")


def test_linux_pg_port_env_override(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_platform.sys, "platform", "linux")
    monkeypatch.setenv("TS_PG_PORT", "5432")
    _plaintext_secret(monkeypatch, tmp_path)
    assert runtime_platform.default_pg_dsn() == (
        "host=/var/run/postgresql port=5432 user=ts_app dbname=trading password=secret"
    )


def test_data_root_env_override(monkeypatch, tmp_path):
    monkeypatch.setenv("TS_DATA_ROOT", str(tmp_path))
    assert runtime_platform.default_data_root() == tmp_path


def test_local_runtime_layout_defaults(monkeypatch):
    monkeypatch.delenv("TRADING_RUNTIME_ROOT", raising=False)
    monkeypatch.delenv("TS_LOCAL_RUNTIME_ROOT", raising=False)
    root = ROOT / "var"

    assert runtime_platform.default_local_runtime_root() == root
    assert runtime_platform.default_local_log_dir() == root / "log"
    assert runtime_platform.default_local_db_dir() == root / "db"
    assert runtime_platform.default_local_db_path() == root / "db" / "trading.db"
    assert runtime_platform.default_local_tmp_dir() == root / "tmp"
    assert runtime_platform.default_local_artifacts_dir() == root / "artifacts"
    assert runtime_platform.default_local_audit_dir() == root / "audit"


def test_local_runtime_root_env_override(monkeypatch, tmp_path):
    monkeypatch.setenv("TRADING_RUNTIME_ROOT", str(tmp_path))

    assert runtime_platform.default_local_runtime_root() == tmp_path
    assert runtime_platform.default_local_db_path() == tmp_path / "db" / "trading.db"
