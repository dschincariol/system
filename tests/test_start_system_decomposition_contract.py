import inspect
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import start_system


def test_start_system_facade_exports_compatibility_signatures() -> None:
    expected = {
        "_env_file_has_nonempty_value": "(env_path: pathlib.Path, key: str) -> bool",
        "_append_env_line": "(env_path: pathlib.Path, line: str) -> None",
        "_ensure_local_secret_file": "(path: pathlib.Path) -> None",
        "_strict_runtime_requires_explicit_db_path": "() -> bool",
        "_ensure_local_env_file": "() -> None",
        "_env_int": "(name: str, default: int, *, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int",
        "_env_float": "(name: str, default: float, *, minimum: Optional[float] = None, maximum: Optional[float] = None) -> float",
        "_env_bool": "(name: str, default: bool) -> bool",
        "_record_phase": "(phase: str, *, status: str = 'started', detail: str = '', extra: Optional[dict] = None) -> None",
        "_record_first_failure": "(phase: str, exc: BaseException, *, file_path: str = '', line_no: Optional[int] = None, module: str = '') -> None",
        "_pick_mode_from_argv_or_env": "() -> str",
        "_module_name_from_path": "(path_value: str) -> str",
        "_import_smoke_subprocess": "(module_name: str, abs_path: str, *, timeout_s: float) -> Dict[str, Any]",
        "_run_production_validation_gate": "() -> None",
        "_startup_validation_summary": "(snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]",
        "_redact_log_string": "(value: str) -> str",
        "_redact_for_log": "(value: Any, *, key: str = '') -> Any",
        "_persist_startup_validation": "(snapshot: Optional[Dict[str, Any]], *, stage: str, attempt: int, timeout_s: float) -> None",
        "_wait_for_dashboard_bind": "(*, host: str, port: int, timeout_s: float) -> bool",
        "_run_dashboard_server_post_bind_validation": "(run_server, *, mode: str, host: str, port: int) -> None",
        "_run_dashboard_server": "(run_server, *, mode: str) -> None",
        "_coerce_ts_ms": "(value: Any) -> int",
        "_dashboard_stop_requested": "() -> bool",
        "_dashboard_returned_after_clean_shutdown": "(lifecycle: Dict[str, Any], *, run_enter_ts_ms: int, stop_requested_at_enter: bool = False) -> bool",
        "_request_dashboard_runtime_stop": "(reason: str) -> None",
        "_handle_signal": "(signum, _frame) -> None",
        "_bootstrap_runtime_side_effects": "() -> None",
        "main": "()",
    }

    for name, signature in expected.items():
        assert hasattr(start_system, name), name
        assert str(inspect.signature(getattr(start_system, name))) == signature


def test_start_system_env_scalar_helpers_keep_existing_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("START_SYSTEM_DECOMP_INT", raising=False)
    monkeypatch.delenv("START_SYSTEM_DECOMP_FLOAT", raising=False)
    monkeypatch.delenv("START_SYSTEM_DECOMP_BOOL", raising=False)

    assert start_system._env_int("START_SYSTEM_DECOMP_INT", 99, maximum=10) == 10
    assert start_system._env_float("START_SYSTEM_DECOMP_FLOAT", 9.5, maximum=2.0) == 2.0
    assert start_system._env_bool("START_SYSTEM_DECOMP_BOOL", True) is True

    monkeypatch.setenv("START_SYSTEM_DECOMP_INT", "8.9")
    monkeypatch.setenv("START_SYSTEM_DECOMP_FLOAT", "0.25")
    monkeypatch.setenv("START_SYSTEM_DECOMP_BOOL", "off")

    assert start_system._env_int("START_SYSTEM_DECOMP_INT", 1, minimum=3, maximum=10) == 8
    assert start_system._env_float("START_SYSTEM_DECOMP_FLOAT", 1.0, minimum=0.5) == 0.5
    assert start_system._env_bool("START_SYSTEM_DECOMP_BOOL", True) is False

    monkeypatch.setenv("START_SYSTEM_DECOMP_INT", "not-an-int")
    monkeypatch.setenv("START_SYSTEM_DECOMP_FLOAT", "not-a-float")
    monkeypatch.setenv("START_SYSTEM_DECOMP_BOOL", "sometimes")

    assert start_system._env_int("START_SYSTEM_DECOMP_INT", 7, minimum=1, maximum=10) == 7
    assert start_system._env_float("START_SYSTEM_DECOMP_FLOAT", 1.5, minimum=1.0, maximum=2.0) == 1.5
    assert start_system._env_bool("START_SYSTEM_DECOMP_BOOL", False) is False


def test_start_system_env_file_helpers_keep_append_and_lookup_semantics(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"

    assert start_system._env_file_has_nonempty_value(env_path, "MISSING") is False

    start_system._append_env_line(env_path, "EMPTY=")
    start_system._append_env_line(env_path, "VALUE=present")

    assert start_system._env_file_has_nonempty_value(env_path, "EMPTY") is False
    assert start_system._env_file_has_nonempty_value(env_path, "VALUE") is True

    compact_path = tmp_path / "compact.env"
    compact_path.write_text("A=1", encoding="utf-8")
    start_system._append_env_line(compact_path, "B=2")
    assert compact_path.read_text(encoding="utf-8") == "A=1\nB=2\n"


def test_start_system_local_env_bootstrap_remains_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / ".env.example").write_text("# local template\n", encoding="utf-8")
    monkeypatch.setattr(start_system, "_BASE_DIR", str(tmp_path))

    start_system._ensure_local_env_file()
    start_system._ensure_local_env_file()

    env_text = (tmp_path / ".env").read_text(encoding="utf-8")
    assert env_text.count("DATA_SOURCE_MASTER_KEY_FILE=") == 1
    assert "DATA_SOURCE_MASTER_KEY_FILE=data/secrets/data_source_master_key" in env_text
    assert "DATA_SOURCE_MASTER_KEY=" not in env_text
    secret_path = tmp_path / "data" / "secrets" / "data_source_master_key"
    assert secret_path.is_file()
    assert secret_path.read_text(encoding="utf-8").strip()


def test_start_system_launch_mode_selection_preserves_argv_precedence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setattr(start_system.sys, "argv", ["start_system.py", "live"])
    assert start_system._pick_mode_from_argv_or_env() == "live"

    monkeypatch.setattr(start_system.sys, "argv", ["start_system.py"])
    monkeypatch.setenv("ENGINE_MODE", "shadow")
    assert start_system._pick_mode_from_argv_or_env() == "shadow"

    monkeypatch.delenv("ENGINE_MODE", raising=False)
    assert start_system._pick_mode_from_argv_or_env() == "safe"

    monkeypatch.setenv("ENGINE_MODE", "invalid")
    with pytest.raises(RuntimeError, match="invalid ENGINE_MODE: invalid"):
        start_system._pick_mode_from_argv_or_env()


def test_start_system_phase_tracking_preserves_trace_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persisted = []
    trace = {"phase": "BOOT", "phases": [], "first_failure": {}}
    monkeypatch.setattr(start_system, "_STARTUP_TRACE", trace)
    monkeypatch.setattr(start_system, "_persist_startup_trace", lambda: persisted.append(dict(trace)))
    monkeypatch.setattr(start_system.time, "time", lambda: 123.456)

    start_system._record_phase("VALIDATE", status="ok", detail="prebind", extra={"mode": "safe"})

    assert trace["phase"] == "VALIDATE"
    assert trace["phases"] == [
        {
            "phase": "VALIDATE",
            "status": "ok",
            "detail": "prebind",
            "ts_ms": 123456,
            "extra": {"mode": "safe"},
        }
    ]
    assert len(persisted) == 1

    try:
        raise ValueError("boom")
    except ValueError as exc:
        start_system._record_first_failure("VALIDATE", exc, file_path="custom.py", line_no=42, module="custom")
        start_system._record_first_failure("SHUTDOWN", exc)

    assert trace["first_failure"]["phase"] == "VALIDATE"
    assert trace["first_failure"]["type"] == "ValueError"
    assert trace["first_failure"]["error"] == "boom"
    assert trace["first_failure"]["module"] == "custom"
    assert trace["first_failure"]["file"] == "custom.py"
    assert trace["first_failure"]["line"] == 42
    assert trace["first_failure"]["ts_ms"] == 123456
    assert "ValueError: boom" in trace["first_failure"]["traceback"]
    assert len(persisted) == 2


def test_start_system_import_smoke_subprocess_preserves_command_env_and_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = {}
    monkeypatch.setenv("PYTHONPATH", "existing-path")
    monkeypatch.setattr(start_system, "_BASE_DIR", str(tmp_path))

    def _fake_run(cmd, **kwargs):
        captured["cmd"] = list(cmd)
        captured.update(kwargs)
        return SimpleNamespace(returncode=0, stdout=" ok \n", stderr="")

    monkeypatch.setattr(start_system.subprocess, "run", _fake_run)

    result = start_system._import_smoke_subprocess("pkg.mod", "/tmp/pkg/mod.py", timeout_s=0.25)

    assert result == {"ok": True}
    assert captured["cmd"][0:2] == [sys.executable, "-c"]
    assert "importlib.import_module(module_name)" in captured["cmd"][2]
    assert captured["cmd"][3:] == ["pkg.mod", "/tmp/pkg/mod.py"]
    assert captured["cwd"] == str(tmp_path)
    assert captured["capture_output"] is True
    assert captured["text"] is True
    assert captured["timeout"] == 1.0
    assert captured["env"]["TRADING_IMPORT_SMOKE_CHILD"] == "1"
    assert captured["env"]["PYTHONPATH"].startswith(str(tmp_path) + start_system.os.pathsep)
    assert captured["env"]["PYTHONPATH"].endswith("existing-path")


def test_start_system_import_smoke_subprocess_preserves_failure_shapes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(start_system, "_BASE_DIR", str(tmp_path))
    swallowed = []
    monkeypatch.setattr(start_system, "_log_swallowed", lambda event, **extra: swallowed.append((event, extra)))

    def _timeout_run(_cmd, **_kwargs):
        raise subprocess.TimeoutExpired(cmd=["unit"], timeout=2.5, output="  stdout  ", stderr="  stderr  ")

    monkeypatch.setattr(start_system.subprocess, "run", _timeout_run)
    timeout_result = start_system._import_smoke_subprocess("", "/tmp/file.py", timeout_s=2.5)

    assert timeout_result == {
        "ok": False,
        "error_type": "TimeoutError",
        "error": "import_timeout_after_2.5s",
        "stdout": "stdout",
        "stderr": "stderr",
    }
    assert swallowed[-1][0] == "IMPORT_SMOKE_SUBPROCESS_TIMEOUT"

    def _returncode_run(_cmd, **_kwargs):
        return SimpleNamespace(returncode=7, stdout="  out  ", stderr="  err  ")

    monkeypatch.setattr(start_system.subprocess, "run", _returncode_run)
    returncode_result = start_system._import_smoke_subprocess("pkg.bad", "/tmp/bad.py", timeout_s=1.0)

    assert returncode_result == {
        "ok": False,
        "error_type": "ImportError",
        "error": "import_process_exit_7",
        "stdout": "out",
        "stderr": "err",
    }


def test_start_system_validation_summary_and_persist_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trace = {}
    persisted = []
    meta_payloads = []
    monkeypatch.setattr(start_system, "_STARTUP_TRACE", trace)
    monkeypatch.setattr(start_system, "_persist_startup_trace", lambda: persisted.append(dict(trace)))
    monkeypatch.setattr(start_system, "_meta_set_json", lambda key, payload: meta_payloads.append((key, payload)))

    snapshot = {
        "ok": False,
        "mode": "live",
        "blocking_checks": ["database_reachable"],
        "critical_systems_missing": ["postgres"],
        "reasons": ["dsn password=super-secret"],
        "checks": {"database_reachable": {"ok": False}},
        "db_validation": {"ok": False},
        "ts_ms": 123,
    }

    summary = start_system._startup_validation_summary(snapshot)
    assert summary["blocking_checks"] == ["database_reachable"]
    assert summary["blocking_gates"] == ["database_reachable"]
    assert summary["gates"] == {"database_reachable": {"ok": False}}

    start_system._persist_startup_validation(snapshot, stage="poll", attempt=3, timeout_s=9.5)

    payload = trace["startup_health_validation"]
    assert payload["stage"] == "poll"
    assert payload["attempt"] == 3
    assert payload["timeout_s"] == 9.5
    assert payload["ts_ms"] == 123
    assert persisted == [{"startup_health_validation": payload}]
    assert meta_payloads == [("startup_health_validation", payload)]
    assert start_system._redact_for_log({"dsn": "postgres://user:secret@host/db"}) == {
        "dsn": "<redacted>",
    }


def test_start_system_production_validation_gate_preserves_runtime_graph_subprocess(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = {}
    trace = {}
    monkeypatch.setattr(start_system, "_BASE_DIR", str(tmp_path))
    monkeypatch.setattr(start_system, "_VALIDATION_TIMEOUT_S", 33)
    monkeypatch.setattr(start_system, "_SKIP_RUNTIME_GRAPH_CHECK", False)
    monkeypatch.setattr(start_system, "_IMPORT_SMOKE", {"ok": True, "failures": []})
    monkeypatch.setattr(start_system, "_STARTUP_TRACE", trace)
    monkeypatch.setattr(start_system, "_run_import_smoke", lambda: None)
    monkeypatch.setattr(start_system, "_persist_startup_trace", lambda: None)
    (tmp_path / "tools").mkdir(parents=True)
    script_path = tmp_path / "tools" / "runtime_graph_check.py"
    script_path.write_text("print('ok')\n", encoding="utf-8")

    def _fake_run(cmd, **kwargs):
        captured["cmd"] = list(cmd)
        captured.update(kwargs)
        return SimpleNamespace(returncode=0, stdout=" graph ok \n", stderr="")

    monkeypatch.setattr(start_system.subprocess, "run", _fake_run)

    start_system._run_production_validation_gate()

    assert captured["cmd"] == [sys.executable, str(script_path)]
    assert captured["cwd"] == str(tmp_path)
    assert captured["env"]["TRADING_VALIDATION_MODE"] == "startup"
    assert captured["capture_output"] is True
    assert captured["text"] is True
    assert captured["timeout"] == 33
    assert trace["validation_gate"]["ok"] is True
    assert trace["validation_gate"]["checks"] == ["import_smoke", "runtime_graph_check"]
    assert trace["validation_gate"]["failures"] == []


def test_start_system_dashboard_helpers_preserve_bind_and_return_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts = []
    sleeps = []

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    def _fake_create_connection(address, timeout):
        attempts.append((address, timeout))
        if len(attempts) == 1:
            raise OSError("not yet")
        return _Conn()

    values = iter([0.0, 0.1, 0.2])
    monkeypatch.setattr(start_system.time, "monotonic", lambda: next(values))
    monkeypatch.setattr(start_system.time, "sleep", lambda value: sleeps.append(value))
    monkeypatch.setattr(start_system.socket, "create_connection", _fake_create_connection)

    assert start_system._wait_for_dashboard_bind(host="127.0.0.1", port=8765, timeout_s=0.5) is True
    assert attempts == [(("127.0.0.1", 8765), 0.25), (("127.0.0.1", 8765), 0.25)]
    assert sleeps == [0.25]

    assert start_system._dashboard_returned_after_clean_shutdown(
        {"state": "LIVE", "last_clean_shutdown_ts_ms": "99"},
        run_enter_ts_ms=100,
    ) is False
    assert start_system._dashboard_returned_after_clean_shutdown(
        {"state": "LIVE", "last_clean_shutdown_ts_ms": "101"},
        run_enter_ts_ms=100,
    ) is True


def test_start_system_shutdown_helpers_preserve_signal_and_bootstrap_side_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle_calls = []
    shutdown_calls = []
    terminate_calls = []
    monkeypatch.setattr("engine.runtime.lifecycle_state.mark_clean_shutdown", lambda: lifecycle_calls.append("clean"))
    monkeypatch.setattr(start_system, "_terminate_ingestion", lambda: terminate_calls.append("terminate"))
    monkeypatch.setattr(
        start_system,
        "runtime_shutdown",
        lambda **kwargs: shutdown_calls.append(kwargs.get("shutdown_reason")),
    )
    start_system._INGESTION_WATCHDOG_STOP.clear()

    with pytest.raises(SystemExit) as exc_info:
        start_system._handle_signal(15, None)

    assert exc_info.value.code == 0
    assert start_system._INGESTION_WATCHDOG_STOP.is_set()
    assert lifecycle_calls == ["clean"]
    assert terminate_calls == ["terminate"]
    assert shutdown_calls == ["signal:15"]

    registered_atexit = []
    registered_signals = []
    monkeypatch.setattr(start_system._INGESTION_WATCHDOG_STOP, "clear", lambda: lifecycle_calls.append("clear"))
    monkeypatch.setattr(start_system.atexit, "register", lambda fn: registered_atexit.append(fn))
    monkeypatch.setattr(start_system, "_write_pid_file", lambda: lifecycle_calls.append("write_pid"))
    monkeypatch.setattr(start_system.signal, "signal", lambda sig, handler: registered_signals.append((sig, handler)))
    monkeypatch.setattr(start_system, "_run_startup_db_repair", lambda: lifecycle_calls.append("db_repair"))

    start_system._bootstrap_runtime_side_effects()

    assert registered_atexit == [start_system._terminate_ingestion, start_system._cleanup_pid_file]
    assert registered_signals == [
        (start_system.signal.SIGTERM, start_system._handle_signal),
        (start_system.signal.SIGINT, start_system._handle_signal),
    ]
    assert lifecycle_calls[-3:] == ["clear", "write_pid", "db_repair"]
