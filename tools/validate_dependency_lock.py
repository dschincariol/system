from __future__ import annotations

"""Validate first-party dependency manifests without installing packages."""

import argparse
import json
import re
import sys
import tomllib
from pathlib import Path
from typing import Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
REQ_NAME_RE = re.compile(r"^\s*([A-Za-z0-9_.-]+)")
PIN_RE = re.compile(r"(==|~=|>=|<=|<|>|===|@\s*https?://)")


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _normalize_req_name(line: str) -> str:
    match = REQ_NAME_RE.match(line)
    if not match:
        return ""
    return match.group(1).replace("_", "-").lower()


def _requirements_entries(path: Path) -> List[str]:
    entries: List[str] = []
    if not path.exists():
        return entries
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        entries.append(line)
    return entries


def _requirements_report(path: Path, *, strict: bool) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []
    if not path.exists():
        errors.append(f"{path.name}_missing")
        return errors, warnings

    seen: Dict[str, int] = {}
    unpinned: List[str] = []
    for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        name = _normalize_req_name(line)
        if not name:
            warnings.append(f"{path.name}:requirements_line_unparsed:{lineno}:{line}")
            continue
        if name in seen:
            errors.append(f"{path.name}:requirements_duplicate:{name}:lines:{seen[name]},{lineno}")
        seen[name] = lineno
        if not PIN_RE.search(line):
            unpinned.append(f"{name}:line:{lineno}")

    if unpinned:
        target = errors if strict else warnings
        target.append(f"{path.name}:requirements_unbounded:" + ",".join(unpinned))
    return errors, warnings


def _pyproject_rocm_extra_report(pyproject_path: Path, rocm_requirements_path: Path) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []
    if not pyproject_path.exists():
        errors.append("pyproject.toml_missing")
        return errors, warnings
    try:
        data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    except Exception as exc:
        errors.append(f"pyproject.toml_parse_failed:{type(exc).__name__}:{exc}")
        return errors, warnings
    optional = dict((data.get("project") or {}).get("optional-dependencies") or {})
    rocm_extra = list(optional.get("amd-rocm") or [])
    rocm_entries = _requirements_entries(rocm_requirements_path)
    missing = sorted(set(rocm_entries) - set(rocm_extra))
    unexpected = sorted(set(rocm_extra) - set(rocm_entries))
    if missing:
        errors.append("pyproject_amd_rocm_extra_missing:" + ",".join(missing))
    if unexpected:
        errors.append("pyproject_amd_rocm_extra_unexpected:" + ",".join(unexpected))
    return errors, warnings


def _npm_lock_report(package_json_path: Path, package_lock_path: Path) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []
    if not package_json_path.exists():
        errors.append("package_json_missing")
        return errors, warnings
    if not package_lock_path.exists():
        errors.append("package_lock_missing")
        return errors, warnings

    package_json = _load_json(package_json_path)
    package_lock = _load_json(package_lock_path)
    pkg_deps = dict(package_json.get("dependencies") or {})
    lock_root = dict((package_lock.get("packages") or {}).get("") or {})
    lock_deps = dict(lock_root.get("dependencies") or {})
    if pkg_deps != lock_deps:
        errors.append(
            "package_lock_root_dependencies_mismatch:"
            + json.dumps({"package_json": pkg_deps, "package_lock": lock_deps}, sort_keys=True)
        )
    if int(package_lock.get("lockfileVersion") or 0) <= 0:
        errors.append("package_lock_invalid_version")
    if "node" not in dict(package_json.get("engines") or {}):
        warnings.append("package_json_node_engine_missing")
    return errors, warnings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate dependency lock/manifest consistency.")
    parser.add_argument("--strict", action="store_true", help="Fail on unbounded requirements entries.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    args = parser.parse_args(argv)

    errors: List[str] = []
    warnings: List[str] = []
    for req_path in sorted(ROOT.glob("requirements*.txt")):
        req_errors, req_warnings = _requirements_report(req_path, strict=bool(args.strict))
        errors.extend(req_errors)
        warnings.extend(req_warnings)
    pyproject_errors, pyproject_warnings = _pyproject_rocm_extra_report(
        ROOT / "pyproject.toml",
        ROOT / "requirements-amd-rocm.txt",
    )
    npm_errors, npm_warnings = _npm_lock_report(ROOT / "package.json", ROOT / "package-lock.json")
    errors.extend(pyproject_errors)
    errors.extend(npm_errors)
    warnings.extend(pyproject_warnings)
    warnings.extend(npm_warnings)

    payload = {"ok": not errors, "errors": errors, "warnings": warnings, "strict": bool(args.strict)}
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print("Dependency lock validation:", "OK" if payload["ok"] else "FAILED")
        for warning in warnings:
            print("WARNING", warning)
        for error in errors:
            print("ERROR", error)
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
