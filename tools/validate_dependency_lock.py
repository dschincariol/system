from __future__ import annotations

"""Validate first-party dependency manifests without installing packages."""

import argparse
import json
import re
import sys
import tomllib
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
REQ_NAME_RE = re.compile(r"^\s*([A-Za-z0-9_.-]+)")
PIN_RE = re.compile(r"(==|~=|>=|<=|<|>|===)")
INCLUDE_RE = re.compile(r"^-r\s+(.+)$")
NVIDIA_ONLY_REQUIREMENTS = {"pynvml", "nvidia-ml-py"}
NVIDIA_REQUIREMENT_PREFIXES = ("nvidia-",)


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _normalize_req_name(line: str) -> str:
    match = REQ_NAME_RE.match(line)
    if not match:
        return ""
    return match.group(1).replace("_", "-").lower()


def _iter_requirement_lines(path: Path, seen: set[Path] | None = None) -> Iterable[Tuple[Path, int, str]]:
    seen = seen or set()
    resolved = path.resolve()
    if resolved in seen:
        return
    seen.add(resolved)
    if not path.exists():
        yield path, 0, "__MISSING__"
        return
    for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        include = INCLUDE_RE.match(line)
        if include:
            include_path = (path.parent / include.group(1).strip()).resolve()
            yield from _iter_requirement_lines(include_path, seen)
            continue
        yield path, lineno, raw


def _requirements_report(path: Path, *, strict: bool) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []
    if not path.exists():
        errors.append("requirements.txt_missing")
        return errors, warnings

    seen: Dict[str, str] = {}
    unpinned: List[str] = []
    names: set[str] = set()
    for source_path, lineno, raw in _iter_requirement_lines(path):
        if raw == "__MISSING__":
            errors.append(f"requirements_include_missing:{source_path}")
            continue
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        name = _normalize_req_name(line)
        if not name:
            warnings.append(f"requirements_line_unparsed:{source_path.relative_to(ROOT)}:{lineno}:{line}")
            continue
        names.add(name)
        location = f"{source_path.relative_to(ROOT)}:{lineno}"
        if name in seen:
            errors.append(f"requirements_duplicate:{name}:lines:{seen[name]},{location}")
        seen[name] = location
        if not PIN_RE.search(line):
            unpinned.append(f"{name}:line:{location}")

    if unpinned:
        target = errors if strict else warnings
        target.append("requirements_unbounded:" + ",".join(unpinned))
    nvidia_in_cpu = _nvidia_requirements(names)
    if nvidia_in_cpu:
        errors.append("requirements_cpu_profile_contains_nvidia_only:" + ",".join(nvidia_in_cpu))
    return errors, warnings


def _nvidia_requirements(names: Iterable[str]) -> List[str]:
    return sorted(
        name
        for name in names
        if name in NVIDIA_ONLY_REQUIREMENTS
        or any(name.startswith(prefix) for prefix in NVIDIA_REQUIREMENT_PREFIXES)
    )


def _requirement_names(path: Path) -> set[str]:
    names: set[str] = set()
    for _source_path, _lineno, raw in _iter_requirement_lines(path):
        if raw == "__MISSING__":
            continue
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        name = _normalize_req_name(line)
        if name:
            names.add(name)
    return names


def _profile_requirements_report() -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []
    nvidia_path = ROOT / "requirements-nvidia-cuda.txt"
    amd_path = ROOT / "requirements-amd-rocm.txt"

    if not nvidia_path.exists():
        errors.append("requirements_nvidia_cuda_missing")
    else:
        nvidia_names = _requirement_names(nvidia_path)
        missing = sorted(NVIDIA_ONLY_REQUIREMENTS - nvidia_names)
        if missing:
            errors.append("requirements_nvidia_profile_missing_diagnostics:" + ",".join(missing))

    if not amd_path.exists():
        warnings.append("requirements_amd_rocm_marker_missing")
    return errors, warnings


def _pyproject_report(path: Path) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []
    if not path.exists():
        warnings.append("pyproject_missing")
        return errors, warnings

    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        errors.append(f"pyproject_parse_failed:{type(exc).__name__}:{exc}")
        return errors, warnings

    project = dict(data.get("project") or {})
    default_names = {
        _normalize_req_name(str(item))
        for item in list(project.get("dependencies") or [])
        if str(item).strip()
    }
    default_nvidia = _nvidia_requirements(default_names)
    if default_nvidia:
        errors.append("pyproject_default_dependencies_contain_nvidia_only:" + ",".join(default_nvidia))

    optional = dict(project.get("optional-dependencies") or {})
    cpu_names = {
        _normalize_req_name(str(item))
        for item in list(optional.get("cpu-runtime") or [])
        if str(item).strip()
    }
    cpu_nvidia = _nvidia_requirements(cpu_names)
    if cpu_nvidia:
        errors.append("pyproject_cpu_runtime_extra_contains_nvidia_only:" + ",".join(cpu_nvidia))

    nvidia_names = {
        _normalize_req_name(str(item))
        for item in list(optional.get("nvidia-cuda") or [])
        if str(item).strip()
    }
    missing_nvidia = sorted(NVIDIA_ONLY_REQUIREMENTS - nvidia_names)
    if missing_nvidia:
        errors.append("pyproject_nvidia_cuda_extra_missing_diagnostics:" + ",".join(missing_nvidia))
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
    req_errors, req_warnings = _requirements_report(ROOT / "requirements.txt", strict=bool(args.strict))
    profile_errors, profile_warnings = _profile_requirements_report()
    pyproject_errors, pyproject_warnings = _pyproject_report(ROOT / "pyproject.toml")
    npm_errors, npm_warnings = _npm_lock_report(ROOT / "package.json", ROOT / "package-lock.json")
    errors.extend(req_errors)
    errors.extend(profile_errors)
    errors.extend(pyproject_errors)
    errors.extend(npm_errors)
    warnings.extend(req_warnings)
    warnings.extend(profile_warnings)
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
