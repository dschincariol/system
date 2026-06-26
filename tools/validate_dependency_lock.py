from __future__ import annotations

"""Validate first-party dependency manifests without installing packages."""

import argparse
import json
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
REQ_NAME_RE = re.compile(r"^\s*([A-Za-z0-9_.-]+)")
PIN_RE = re.compile(r"(==|~=|>=|<=|<|>|===)")
HASH_RE = re.compile(r"--hash=sha256:[A-Fa-f0-9]{64}\b")
DIRECT_REFERENCE_RE = re.compile(r"^\s*[A-Za-z0-9_.-]+(?:\[[^\]]+\])?\s*@\s*\S+")
INCLUDE_RE = re.compile(r"^-r\s+(.+)$")
CONSTRAINT_RE = re.compile(r"^(?:-c|--constraint)\s+(.+)$")
NVIDIA_ONLY_REQUIREMENTS = {"pynvml", "nvidia-ml-py"}
NVIDIA_REQUIREMENT_PREFIXES = ("nvidia-",)


@dataclass(frozen=True)
class SharedPinSource:
    profile: str
    path: str
    required: bool = True


@dataclass(frozen=True)
class SharedPinAllowlistEntry:
    package: str
    profile: str
    expected_version: str
    reason: str


SHARED_SCIENTIFIC_PINS = {
    "huggingface-hub",
    "joblib",
    "lightgbm",
    "llvmlite",
    "numba",
    "numpy",
    "pandas",
    "pyarrow",
    "pydantic",
    "pydantic-core",
    "safetensors",
    "scikit-learn",
    "scipy",
    "sentence-transformers",
    "sqlalchemy",
    "statsmodels",
    "tokenizers",
    "transformers",
}
# xgboost is intentionally excluded: CPU/base pins xgboost-cpu, while accelerator
# profiles pin xgboost, so distribution-name equality would produce false failures.
SHARED_PIN_SOURCES = (
    SharedPinSource("base", "requirements-base.txt"),
    SharedPinSource("cpu", "requirements.in"),
    SharedPinSource("cpu", "requirements.txt"),
    SharedPinSource("cpu", "requirements.lock.txt"),
    SharedPinSource("nvidia-cuda", "requirements-nvidia-cuda.txt"),
    SharedPinSource("nvidia-cuda", "requirements-nvidia-cuda.lock.txt"),
    SharedPinSource("amd-rocm", "requirements-amd-rocm.txt", required=False),
    SharedPinSource("amd-rocm", "requirements-amd-rocm-full.txt"),
    SharedPinSource("amd-rocm", "requirements-amd-rocm.lock.txt"),
)
ROCM_SHARED_PIN_DIVERGENCE_REASON = (
    "unverified base-version availability on ROCm image; pending confirmation"
)
SHARED_PIN_ALLOWLIST = (
    SharedPinAllowlistEntry("lightgbm", "amd-rocm", "4.6.0", ROCM_SHARED_PIN_DIVERGENCE_REASON),
    SharedPinAllowlistEntry("numpy", "amd-rocm", "2.4.6", ROCM_SHARED_PIN_DIVERGENCE_REASON),
    SharedPinAllowlistEntry("scikit-learn", "amd-rocm", "1.9.0", ROCM_SHARED_PIN_DIVERGENCE_REASON),
)
RUNTIME_INSTALL_MANIFESTS = {
    "requirements.txt": ("requirements.in", "requirements.lock.txt"),
}
RUNTIME_PROFILE_LOCK_CONSTRAINTS = {
    "requirements.txt": "requirements.lock.txt",
    "requirements-nvidia-cuda.txt": "requirements-nvidia-cuda.lock.txt",
    "requirements-amd-rocm-full.txt": "requirements-amd-rocm.lock.txt",
}
DEV_INSTALL_MANIFESTS = {
    "requirements-dev.txt": ("requirements-dev.in", "requirements-dev.lock.txt"),
}
DEV_TOOL_REQUIREMENTS = {
    "coverage",
    "pytest",
    "pytest-cov",
    "pytest-timeout",
    "pyright",
    "ruff",
}
FORBIDDEN_REQUIREMENTS = {
    "psycopg2": "use psycopg 3.x via psycopg[binary,pool]",
    "psycopg2-binary": "use psycopg 3.x via psycopg[binary,pool]",
}
TABULAR_CHALLENGER_OPTIONAL_REQUIREMENTS = {
    "autogluon",
    "catboost",
    "pytorch-tabnet",
    "tabm",
    "tabnet",
    "tabpfn",
}
TABULAR_CHALLENGER_DEFAULT_MANIFESTS = (
    "requirements-base.txt",
    "requirements.in",
    "requirements.txt",
    "requirements-nvidia-cuda.txt",
    "requirements-amd-rocm-full.txt",
)


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _normalize_req_name(line: str) -> str:
    match = REQ_NAME_RE.match(line)
    if not match:
        return ""
    return match.group(1).replace("_", "-").lower()


def _report_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


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


def _manifest_refs(path: Path) -> Tuple[List[Path], List[Path]]:
    includes: List[Path] = []
    constraints: List[Path] = []
    if not path.exists():
        return includes, constraints
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        include = INCLUDE_RE.match(line)
        if include:
            includes.append((path.parent / include.group(1).strip()).resolve())
            continue
        constraint = CONSTRAINT_RE.match(line)
        if constraint:
            constraints.append((path.parent / constraint.group(1).strip()).resolve())
    return includes, constraints


def _requirements_entries(path: Path) -> Dict[str, str]:
    entries: Dict[str, str] = {}
    for source_path, lineno, raw in _iter_requirement_lines(path):
        if raw == "__MISSING__":
            continue
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        name = _normalize_req_name(line)
        if not name:
            continue
        entries[name] = f"{_report_path(source_path)}:{lineno}:{line}"
    return entries


def _requirements_report(
    path: Path,
    *,
    strict: bool,
    allow_nvidia: bool = False,
) -> Tuple[List[str], List[str]]:
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
        if not PIN_RE.search(line) and not DIRECT_REFERENCE_RE.match(line):
            unpinned.append(f"{name}:line:{location}")

    if unpinned:
        target = errors if strict else warnings
        target.append("requirements_unbounded:" + ",".join(unpinned))
    if not allow_nvidia:
        nvidia_in_cpu = _nvidia_requirements(names)
        if nvidia_in_cpu:
            errors.append("requirements_cpu_profile_contains_nvidia_only:" + ",".join(nvidia_in_cpu))
    forbidden = sorted(name for name in names if name in FORBIDDEN_REQUIREMENTS)
    for name in forbidden:
        errors.append(f"requirements_forbidden:{name}:{FORBIDDEN_REQUIREMENTS[name]}")
    return errors, warnings


def _install_manifest_report(
    manifests: Dict[str, Tuple[str, str]],
) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []

    for manifest_name, (input_name, lock_name) in manifests.items():
        manifest_path = ROOT / manifest_name
        input_path = (ROOT / input_name).resolve()
        lock_path = (ROOT / lock_name).resolve()
        if not manifest_path.exists():
            errors.append(f"requirements_install_manifest_missing:{manifest_name}")
            continue
        if not input_path.exists():
            errors.append(f"requirements_input_missing:{input_name}")
        if not lock_path.exists():
            errors.append(f"requirements_lock_missing:{lock_name}")

        includes, constraints = _manifest_refs(manifest_path)
        if input_path not in includes:
            errors.append(f"requirements_install_manifest_missing_include:{manifest_name}:{input_name}")
        if lock_path not in constraints:
            errors.append(f"requirements_install_manifest_missing_constraint:{manifest_name}:{lock_name}")

        unexpected_includes = sorted(
            str(path.relative_to(ROOT))
            for path in includes
            if path != input_path and path.is_relative_to(ROOT)
        )
        if unexpected_includes:
            warnings.append(
                f"requirements_install_manifest_unexpected_include:{manifest_name}:"
                + ",".join(unexpected_includes)
            )
        unexpected_constraints = sorted(
            str(path.relative_to(ROOT))
            for path in constraints
            if path != lock_path and path.is_relative_to(ROOT)
        )
        if unexpected_constraints:
            warnings.append(
                f"requirements_install_manifest_unexpected_constraint:{manifest_name}:"
                + ",".join(unexpected_constraints)
            )
    return errors, warnings


def _runtime_profile_lock_constraint_report(
    manifests: Dict[str, str] | None = None,
) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []
    manifests = manifests or RUNTIME_PROFILE_LOCK_CONSTRAINTS

    for manifest_name, lock_name in manifests.items():
        manifest_path = ROOT / manifest_name
        lock_path = (ROOT / lock_name).resolve()
        if not manifest_path.exists():
            errors.append(f"requirements_profile_manifest_missing:{manifest_name}")
            continue
        if not lock_path.exists():
            errors.append(f"requirements_profile_lock_missing:{lock_name}")

        _includes, constraints = _manifest_refs(manifest_path)
        lock_constraints = [
            constraint
            for constraint in constraints
            if constraint.name.endswith(".lock.txt")
        ]
        if not lock_constraints:
            errors.append(f"requirements_profile_missing_lock_constraint:{manifest_name}")
        if lock_path not in constraints:
            errors.append(f"requirements_profile_missing_expected_lock_constraint:{manifest_name}:{lock_name}")

        unexpected_locks = sorted(
            str(path.relative_to(ROOT))
            for path in lock_constraints
            if path != lock_path and path.is_relative_to(ROOT)
        )
        if unexpected_locks:
            warnings.append(
                f"requirements_profile_unexpected_lock_constraint:{manifest_name}:"
                + ",".join(unexpected_locks)
            )
    return errors, warnings


def _lock_requirements_missing_hashes(lock_path: Path) -> List[str]:
    missing: List[str] = []
    if not lock_path.exists():
        return missing

    current_lineno = 0
    current_line = ""
    current_has_hash = False

    def flush_current() -> None:
        if not current_lineno or current_has_hash:
            return
        name = _normalize_req_name(current_line) or "<unparsed>"
        missing.append(f"{_report_path(lock_path)}:{current_lineno}:{name}")

    for lineno, raw in enumerate(lock_path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("--hash="):
            if current_lineno and HASH_RE.search(line):
                current_has_hash = True
            continue
        if line.startswith("-"):
            flush_current()
            current_lineno = 0
            current_line = ""
            current_has_hash = False
            continue
        if raw[:1].isspace():
            if current_lineno and HASH_RE.search(line):
                current_has_hash = True
            continue

        flush_current()
        current_lineno = lineno
        current_line = line
        current_has_hash = bool(HASH_RE.search(line))

    flush_current()
    return missing


def _lock_file_report(
    lock_path: Path,
    input_path: Path,
    *,
    require_hashes: bool = False,
    allow_nvidia: bool = False,
) -> Tuple[List[str], List[str]]:
    errors, warnings = _requirements_report(lock_path, strict=True, allow_nvidia=allow_nvidia)
    if not lock_path.exists() or not input_path.exists():
        return errors, warnings

    includes, constraints = _manifest_refs(lock_path)
    if includes:
        errors.append(f"requirements_lock_contains_include:{lock_path.relative_to(ROOT)}")
    if constraints:
        errors.append(f"requirements_lock_contains_constraint:{lock_path.relative_to(ROOT)}")

    input_names = set(_requirements_entries(input_path))
    lock_names = set(_requirements_entries(lock_path))
    missing = sorted(input_names - lock_names)
    if missing:
        errors.append(
            f"requirements_lock_missing_direct_pins:{lock_path.relative_to(ROOT)}:"
            + ",".join(missing)
        )
    if require_hashes:
        missing_hashes = _lock_requirements_missing_hashes(lock_path)
        if missing_hashes:
            errors.append("requirements_lock_missing_hashes:" + ",".join(missing_hashes))
    return errors, warnings


def _dev_runtime_separation_report() -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []

    runtime_paths = [
        ROOT / "requirements.in",
        ROOT / "requirements-base.txt",
        ROOT / "requirements.txt",
        ROOT / "requirements.lock.txt",
        ROOT / "requirements-nvidia-cuda.txt",
        ROOT / "requirements-amd-rocm.txt",
        ROOT / "requirements-amd-rocm-full.txt",
    ]
    for path in runtime_paths:
        if not path.exists():
            continue
        names = set(_requirements_entries(path))
        dev_tools = sorted(names & DEV_TOOL_REQUIREMENTS)
        if dev_tools:
            errors.append(
                f"runtime_requirements_contain_dev_tools:{path.relative_to(ROOT)}:"
                + ",".join(dev_tools)
            )

    dev_entries = _requirements_entries(ROOT / "requirements-dev.in")
    dev_lock_entries = _requirements_entries(ROOT / "requirements-dev.lock.txt")
    for tool in sorted(DEV_TOOL_REQUIREMENTS):
        line = dev_entries.get(tool)
        if line is None:
            errors.append(f"requirements_dev_missing_tool:{tool}")
        elif "==" not in line and "===" not in line:
            errors.append(f"requirements_dev_tool_not_exactly_pinned:{tool}:{line}")
        if tool not in dev_lock_entries:
            errors.append(f"requirements_dev_lock_missing_tool:{tool}")
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


def _exact_pin_version(entry: str) -> str | None:
    parts = entry.split(":", 2)
    if len(parts) < 3:
        return None
    line = parts[2]
    if "==" not in line or "===" in line:
        return None
    version = line.split("==", 1)[1].split("#", 1)[0].strip()
    version = re.split(r"[\s;]+", version, maxsplit=1)[0].rstrip("\\")
    return version or None


def _shared_scientific_pin_versions(path: Path) -> Dict[str, str]:
    pins: Dict[str, str] = {}
    for name, entry in _requirements_entries(path).items():
        if name not in SHARED_SCIENTIFIC_PINS:
            continue
        version = _exact_pin_version(entry)
        if version is not None:
            pins[name] = version
    return pins


def _shared_pin_allowlist() -> Dict[Tuple[str, str], SharedPinAllowlistEntry]:
    return {
        (entry.profile, entry.package): entry
        for entry in SHARED_PIN_ALLOWLIST
        if entry.reason.strip() and entry.expected_version.strip()
    }


def _shared_pin_reference(
    pins_by_source: Dict[str, Dict[str, str]],
    source_profiles: Dict[str, str],
    package: str,
) -> Tuple[str, str] | None:
    for source_path in (
        "requirements-base.txt",
        "requirements.in",
        "requirements.txt",
        "requirements.lock.txt",
    ):
        version = pins_by_source.get(source_path, {}).get(package)
        if version is not None:
            return source_path, version
    for source_path in sorted(pins_by_source):
        if source_profiles.get(source_path) in {"base", "cpu"}:
            version = pins_by_source[source_path].get(package)
            if version is not None:
                return source_path, version
    return None


def _shared_pin_report_metadata() -> dict:
    return {
        "allowlist": [
            {
                "expected_version": entry.expected_version,
                "package": entry.package,
                "profile": entry.profile,
                "reason": entry.reason,
            }
            for entry in SHARED_PIN_ALLOWLIST
        ],
        "packages": sorted(SHARED_SCIENTIFIC_PINS),
        "sources": [
            {
                "path": source.path,
                "profile": source.profile,
                "required": source.required,
            }
            for source in SHARED_PIN_SOURCES
        ],
    }


def _shared_scientific_pin_report() -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []
    pins_by_source: Dict[str, Dict[str, str]] = {}
    source_profiles: Dict[str, str] = {}
    allowlist = _shared_pin_allowlist()

    for source in SHARED_PIN_SOURCES:
        source_path = ROOT / source.path
        if not source_path.exists():
            if source.required:
                errors.append(f"shared_pin_source_missing:{source.path}")
            continue
        source_profiles[source.path] = source.profile
        pins_by_source[source.path] = _shared_scientific_pin_versions(source_path)

    if "requirements-base.txt" not in pins_by_source:
        errors.append("shared_pin_base_missing")
        return errors, warnings

    for package in sorted(SHARED_SCIENTIFIC_PINS):
        reference = _shared_pin_reference(pins_by_source, source_profiles, package)
        if reference is None:
            continue
        reference_source, reference_version = reference
        for source in SHARED_PIN_SOURCES:
            source_pins = pins_by_source.get(source.path)
            if not source_pins:
                continue
            profile_version = source_pins.get(package)
            if profile_version is None or profile_version == reference_version:
                continue
            allowlist_entry = allowlist.get((source.profile, package))
            if allowlist_entry and profile_version == allowlist_entry.expected_version:
                warnings.append(
                    "cross_profile_pin_allowlisted:"
                    f"{source.path}:{package}:"
                    f"reference={reference_source}:{reference_version}:"
                    f"profile={source.profile}:actual={profile_version}:"
                    f"expected={allowlist_entry.expected_version}:"
                    f"reason={allowlist_entry.reason}"
                )
                continue
            expected = ""
            if allowlist_entry:
                expected = f":allowlist_expected={allowlist_entry.expected_version}"
            errors.append(
                "cross_profile_pin_mismatch:"
                f"{source.path}:{package}:"
                f"reference={reference_source}:{reference_version}:"
                f"profile={source.profile}:actual={profile_version}"
                f"{expected}"
            )
    return errors, warnings


def _tabular_challenger_optional_dependency_report() -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []
    for manifest_name in TABULAR_CHALLENGER_DEFAULT_MANIFESTS:
        entries = _requirements_entries(ROOT / manifest_name)
        leaked = sorted(set(entries) & TABULAR_CHALLENGER_OPTIONAL_REQUIREMENTS)
        if leaked:
            errors.append(f"tabular_challenger_optional_dependency_in_default_manifest:{manifest_name}:{','.join(leaked)}")

    try:
        data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    except Exception as exc:
        warnings.append(f"tabular_challenger_pyproject_extra_unchecked:{type(exc).__name__}:{exc}")
        return errors, warnings
    optional = dict((data.get("project") or {}).get("optional-dependencies") or {})
    extra_entries = {
        _normalize_req_name(str(item))
        for item in list(optional.get("tabular-foundation") or [])
        if str(item).strip()
    }
    missing = sorted({"tabm", "tabpfn"} - extra_entries)
    if missing:
        errors.append("tabular_challenger_optional_extra_missing:" + ",".join(missing))
    return errors, warnings


def _ci_workflow_report(path: Path) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    warnings: List[str] = []
    if not path.exists():
        warnings.append("ci_validate_workflow_missing")
        return errors, warnings

    text = path.read_text(encoding="utf-8")
    if "python tools/validate_dependency_lock.py --strict" not in text:
        errors.append("ci_missing_strict_dependency_lock_validation")
    if "python -m pip install --require-hashes -r requirements-dev.txt" not in text:
        errors.append("ci_missing_hash_verified_dev_requirements_install")
    unhashed_dev_installs = [
        line.strip()
        for line in text.splitlines()
        if "python -m pip install -r requirements-dev.txt" in line
    ]
    if unhashed_dev_installs:
        errors.append("ci_installs_dev_requirements_without_hashes:" + ",".join(unhashed_dev_installs))
    forbidden_installs = [
        line.strip()
        for line in text.splitlines()
        if "python -m pip install -r requirements.txt" in line
        or "python -m pip install -r requirements-base.txt" in line
    ]
    if forbidden_installs:
        errors.append("ci_installs_runtime_requirements_for_tests:" + ",".join(forbidden_installs))
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
    dev_errors, dev_warnings = _requirements_report(ROOT / "requirements-dev.txt", strict=True)
    runtime_manifest_errors, runtime_manifest_warnings = _install_manifest_report(RUNTIME_INSTALL_MANIFESTS)
    profile_constraint_errors, profile_constraint_warnings = _runtime_profile_lock_constraint_report()
    dev_manifest_errors, dev_manifest_warnings = _install_manifest_report(DEV_INSTALL_MANIFESTS)
    runtime_lock_errors, runtime_lock_warnings = _lock_file_report(
        ROOT / "requirements.lock.txt", ROOT / "requirements.in", require_hashes=bool(args.strict)
    )
    nvidia_lock_errors, nvidia_lock_warnings = _lock_file_report(
        ROOT / "requirements-nvidia-cuda.lock.txt",
        ROOT / "requirements-nvidia-cuda.txt",
        require_hashes=bool(args.strict),
        allow_nvidia=True,
    )
    rocm_lock_errors, rocm_lock_warnings = _lock_file_report(
        ROOT / "requirements-amd-rocm.lock.txt",
        ROOT / "requirements-amd-rocm-full.txt",
        require_hashes=bool(args.strict),
        allow_nvidia=True,
    )
    dev_lock_errors, dev_lock_warnings = _lock_file_report(
        ROOT / "requirements-dev.lock.txt", ROOT / "requirements-dev.in", require_hashes=bool(args.strict)
    )
    separation_errors, separation_warnings = _dev_runtime_separation_report()
    tabular_errors, tabular_warnings = _tabular_challenger_optional_dependency_report()
    profile_errors, profile_warnings = _profile_requirements_report()
    shared_pin_errors, shared_pin_warnings = _shared_scientific_pin_report()
    pyproject_errors, pyproject_warnings = _pyproject_report(ROOT / "pyproject.toml")
    npm_errors, npm_warnings = _npm_lock_report(ROOT / "package.json", ROOT / "package-lock.json")
    ci_errors, ci_warnings = _ci_workflow_report(ROOT / ".github" / "workflows" / "validate.yml")
    errors.extend(req_errors)
    errors.extend(dev_errors)
    errors.extend(runtime_manifest_errors)
    errors.extend(profile_constraint_errors)
    errors.extend(dev_manifest_errors)
    errors.extend(runtime_lock_errors)
    errors.extend(nvidia_lock_errors)
    errors.extend(rocm_lock_errors)
    errors.extend(dev_lock_errors)
    errors.extend(separation_errors)
    errors.extend(tabular_errors)
    errors.extend(profile_errors)
    errors.extend(shared_pin_errors)
    errors.extend(pyproject_errors)
    errors.extend(npm_errors)
    errors.extend(ci_errors)
    warnings.extend(req_warnings)
    warnings.extend(dev_warnings)
    warnings.extend(runtime_manifest_warnings)
    warnings.extend(profile_constraint_warnings)
    warnings.extend(dev_manifest_warnings)
    warnings.extend(runtime_lock_warnings)
    warnings.extend(nvidia_lock_warnings)
    warnings.extend(rocm_lock_warnings)
    warnings.extend(dev_lock_warnings)
    warnings.extend(separation_warnings)
    warnings.extend(tabular_warnings)
    warnings.extend(profile_warnings)
    warnings.extend(shared_pin_warnings)
    warnings.extend(pyproject_warnings)
    warnings.extend(npm_warnings)
    warnings.extend(ci_warnings)

    payload = {
        "errors": errors,
        "ok": not errors,
        "shared_scientific_pins": _shared_pin_report_metadata(),
        "strict": bool(args.strict),
        "warnings": warnings,
    }
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
