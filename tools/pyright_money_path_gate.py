"""Ratcheted pyright gate for live trading money paths."""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
BASELINE_PATH = ROOT / "tools" / "pyright_money_path_baseline.json"
PYRIGHT_CONFIG_PATH = ROOT / "pyrightconfig.json"

TARGET_PATHS = (
    "engine/data/live_prices/ccxt_live.py",
    "engine/data/live_prices/yfinance_live.py",
    "engine/data/options_poll.py",
    "engine/data/poll_prices.py",
    "engine/data/price_cache.py",
    "engine/data/provider_router.py",
    "engine/execution/broker_router.py",
    "engine/execution/broker_apply_orders.py",
    "engine/execution/execution_mode.py",
    "engine/execution/order_idempotency.py",
    "engine/execution/order_command_boundary.py",
    "engine/execution/position_reconcile.py",
    "engine/execution/kill_switch.py",
    "engine/execution/execution_ledger.py",
    "engine/risk/portfolio_risk_engine.py",
    "engine/risk/monte_carlo_risk_engine.py",
    "engine/strategy/portfolio.py",
    "engine/strategy/portfolio_risk_gate.py",
    "engine/strategy/predictor.py",
    "engine/strategy/champion_manager.py",
    "engine/runtime/gates.py",
    "engine/runtime/live_trading_preflight.py",
    "engine/runtime/live_execution_control.py",
    "engine/runtime/prod_preflight.py",
    "engine/runtime/staging_prod_preflight.py",
    "engine/runtime/price_read_router.py",
    "engine/runtime/telemetry_read_router.py",
    "engine/runtime/storage_pg.py",
    "engine/runtime/storage_pg_prices.py",
    "engine/runtime/timescale_client.py",
)

HIGH_RISK_ROOTS = (
    "engine/data",
    "engine/execution",
    "engine/risk",
    "engine/runtime",
    "engine/strategy",
)

STAGED_INCLUSION = (
    {
        "path": "engine/data",
        "current_gate": "Live price/provider polling files are included now.",
        "full_package_target": "2026-09-30",
        "justification": "Remaining historical, research, and alternative ingestion jobs need type cleanup before full-package gating.",
    },
    {
        "path": "engine/execution",
        "current_gate": "Broker routing, order apply, idempotency, kill-switch, reconciliation, and ledger files are included now.",
        "full_package_target": "2026-09-30",
        "justification": "Remaining analytics, training, and broker-adapter files are staged after the live order path.",
    },
    {
        "path": "engine/risk",
        "current_gate": "The full risk package is included now.",
        "full_package_target": None,
        "justification": "Risk contains the portfolio and Monte Carlo engines that directly size capital.",
    },
    {
        "path": "engine/runtime",
        "current_gate": "Live gates, preflight, read routers, Postgres storage, and Timescale client files are included now.",
        "full_package_target": "2026-10-31",
        "justification": "Remaining runtime modules include broad startup, health, and operator support surfaces with legacy typing debt.",
    },
    {
        "path": "engine/strategy",
        "current_gate": "Portfolio, portfolio risk gate, predictor, and champion manager files are included now.",
        "full_package_target": "2026-10-31",
        "justification": "Remaining strategy modules include research-only model families and feature discovery surfaces.",
    },
)


def _normalise_path(path: str) -> str:
    candidate = Path(path)
    try:
        return candidate.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        return str(path).replace("\\", "/")


def _normalise_message(message: object) -> str:
    return str(message or "").replace("\u00a0", " ").replace("\u2026", "...")


def _diagnostics_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    for raw in payload.get("generalDiagnostics") or []:
        raw_range = raw.get("range") or {}
        start = raw_range.get("start") or {}
        diagnostics.append(
            {
                "file": _normalise_path(str(raw.get("file") or "")),
                "severity": str(raw.get("severity") or ""),
                "rule": raw.get("rule") or None,
                "line": int(start.get("line", -1)) + 1,
                "character": int(start.get("character", -1)) + 1,
                "message": _normalise_message(raw.get("message")),
            }
        )
    return sorted(diagnostics, key=_diagnostic_key)


def _diagnostic_key(diagnostic: dict[str, Any]) -> tuple[str, str, str, int, int, str]:
    return (
        str(diagnostic.get("file") or ""),
        str(diagnostic.get("severity") or ""),
        str(diagnostic.get("rule") or ""),
        int(diagnostic.get("line") or 0),
        int(diagnostic.get("character") or 0),
        str(diagnostic.get("message") or ""),
    )


def _diagnostic_counter(diagnostics: list[dict[str, Any]]) -> Counter[tuple[str, str, str, int, int, str]]:
    return Counter(_diagnostic_key(item) for item in diagnostics)


def _expanded(counter: Counter[tuple[str, str, str, int, int, str]]) -> list[tuple[str, str, str, int, int, str]]:
    rows: list[tuple[str, str, str, int, int, str]] = []
    for key, count in counter.items():
        rows.extend([key] * count)
    return sorted(rows)


def _summary(diagnostics: list[dict[str, Any]]) -> dict[str, int]:
    severities = Counter(str(item.get("severity") or "") for item in diagnostics)
    return {
        "error_count": severities.get("error", 0),
        "warning_count": severities.get("warning", 0),
        "information_count": severities.get("information", 0),
        "diagnostic_count": len(diagnostics),
    }


def _baseline_payload(pyright_payload: dict[str, Any], diagnostics: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "tool": "pyright",
        "pyright_version": pyright_payload.get("version"),
        "ratchet": (
            "Default gate requires exact diagnostic equality. Fixes must update this "
            "baseline so resolved diagnostics cannot reappear unnoticed."
        ),
        "scope": {
            "target_paths": list(TARGET_PATHS),
            "high_risk_roots": list(HIGH_RISK_ROOTS),
            "staged_inclusion": list(STAGED_INCLUSION),
        },
        "summary": _summary(diagnostics),
        "diagnostics": diagnostics,
    }


def _pyright_command() -> list[str]:
    override = os.environ.get("PYRIGHT_BIN")
    if override:
        return override.split()
    executable = shutil.which("pyright")
    if executable:
        return [executable]
    return [sys.executable, "-m", "pyright"]


def _missing_targets() -> list[str]:
    return [path for path in TARGET_PATHS if not (ROOT / path).is_file()]


def _load_pyright_config() -> dict[str, Any]:
    try:
        return json.loads(PYRIGHT_CONFIG_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise RuntimeError(f"missing {PYRIGHT_CONFIG_PATH.relative_to(ROOT)}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid {PYRIGHT_CONFIG_PATH.relative_to(ROOT)}: {exc}") from exc


def _pattern_excludes_root(pattern: str, root: str) -> bool:
    normalised = pattern.replace("\\", "/").strip().lstrip("./").rstrip("/")
    root = root.rstrip("/")
    if normalised in {root, f"{root}/**", f"**/{root}", f"**/{root}/**"}:
        return True
    if normalised.startswith("**/") and root.endswith("/" + normalised[3:]):
        return True

    probes = (root, f"{root}/__init__.py", f"{root}/sample.py")
    return any(fnmatch.fnmatchcase(path, normalised) for path in probes)


def high_risk_exclusion_violations(config: dict[str, Any]) -> list[str]:
    violations: list[str] = []
    excludes = config.get("exclude") or []
    for raw_pattern in excludes:
        pattern = str(raw_pattern)
        for root in HIGH_RISK_ROOTS:
            if _pattern_excludes_root(pattern, root):
                violations.append(f"{pattern!r} excludes high-risk root {root}")
    return violations


def _run_pyright() -> tuple[dict[str, Any], str, str, int]:
    missing = _missing_targets()
    if missing:
        raise RuntimeError("pyright money-path targets are missing:\n" + "\n".join(f"- {path}" for path in missing))

    command = [
        *_pyright_command(),
        "--project",
        str(PYRIGHT_CONFIG_PATH),
        "--outputjson",
        *TARGET_PATHS,
    ]
    result = subprocess.run(
        command,
        check=False,
        cwd=str(ROOT),
        text=True,
        capture_output=True,
    )
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(
            "pyright did not produce JSON output. Install dev dependencies with "
            "`python -m pip install --require-hashes -r requirements-dev.txt` "
            "or set PYRIGHT_BIN. "
            f"Command: {' '.join(command)}\n{detail}"
        ) from exc
    return payload, result.stdout, result.stderr, result.returncode


def _load_baseline() -> dict[str, Any]:
    try:
        return json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"missing {BASELINE_PATH.relative_to(ROOT)}; run "
            "`python tools/pyright_money_path_gate.py --update-baseline` after reviewing diagnostics"
        ) from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid {BASELINE_PATH.relative_to(ROOT)}: {exc}") from exc


def _format_diagnostic_key(key: tuple[str, str, str, int, int, str]) -> str:
    file, severity, rule, line, character, message = key
    first_line = message.splitlines()[0] if message else ""
    rule_label = f" {rule}" if rule else ""
    return f"{file}:{line}:{character}: {severity}{rule_label}: {first_line}"


def _compare_to_baseline(baseline: dict[str, Any], current: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    baseline_diagnostics = list(baseline.get("diagnostics") or [])
    baseline_counter = _diagnostic_counter(baseline_diagnostics)
    current_counter = _diagnostic_counter(current)
    new_diagnostics = [_format_diagnostic_key(item) for item in _expanded(current_counter - baseline_counter)]
    resolved_diagnostics = [_format_diagnostic_key(item) for item in _expanded(baseline_counter - current_counter)]
    return new_diagnostics, resolved_diagnostics


def _print_diagnostic_group(title: str, diagnostics: list[str], limit: int = 12) -> None:
    if not diagnostics:
        return
    print(f"\n{title}")
    for item in diagnostics[:limit]:
        print(f"- {item}")
    if len(diagnostics) > limit:
        print(f"- ... {len(diagnostics) - limit} more")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the ratcheted pyright gate for trading money paths.")
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        help="Rewrite the pyright money-path baseline from current diagnostics.",
    )
    args = parser.parse_args(argv)

    try:
        config = _load_pyright_config()
        exclusion_violations = high_risk_exclusion_violations(config)
        if exclusion_violations:
            print("pyrightconfig.json excludes high-risk trading modules:")
            for violation in exclusion_violations:
                print(f"- {violation}")
            return 1

        payload, _, stderr, pyright_returncode = _run_pyright()
        diagnostics = _diagnostics_from_payload(payload)
    except RuntimeError as exc:
        print(str(exc))
        return 1

    if args.update_baseline:
        baseline = _baseline_payload(payload, diagnostics)
        BASELINE_PATH.write_text(json.dumps(baseline, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        summary = baseline["summary"]
        print(
            f"Updated {BASELINE_PATH.relative_to(ROOT)}: "
            f"{summary['error_count']} errors, {summary['warning_count']} warnings."
        )
        return 0

    try:
        baseline = _load_baseline()
    except RuntimeError as exc:
        print(str(exc))
        return 1

    new_diagnostics, resolved_diagnostics = _compare_to_baseline(baseline, diagnostics)
    if new_diagnostics or resolved_diagnostics:
        print("Pyright money-path gate failed.")
        _print_diagnostic_group("New diagnostics not in baseline:", new_diagnostics)
        _print_diagnostic_group("Resolved diagnostics require a ratcheted baseline update:", resolved_diagnostics)
        print("\nAfter reviewing the change, run `python tools/pyright_money_path_gate.py --update-baseline`.")
        return 1

    summary = _summary(diagnostics)
    print(
        "Pyright money-path gate passed: "
        f"{summary['error_count']} baseline errors, {summary['warning_count']} baseline warnings, "
        f"{len(TARGET_PATHS)} target files."
    )
    if stderr.strip():
        print(stderr.strip())
    return 0 if pyright_returncode in {0, 1} else pyright_returncode


if __name__ == "__main__":
    raise SystemExit(main())
