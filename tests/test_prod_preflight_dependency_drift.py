from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_module():
    import engine.runtime.prod_preflight as prod_preflight

    return importlib.reload(prod_preflight)


def _requirements_entries_side_effect(
    direct_entries: dict[str, str],
    lock_entries: dict[str, str],
):
    def _entries(path: Path) -> dict[str, str]:
        name = Path(path).name
        if name == "requirements.in":
            return dict(direct_entries)
        if name == "requirements.lock.txt":
            return dict(lock_entries)
        return {}

    return _entries


class ProdPreflightDependencyDriftTests(unittest.TestCase):
    def test_dependency_drift_gate_accepts_matching_direct_lock_versions(self) -> None:
        with patch.dict(os.environ, {"ENGINE_MODE": "safe"}, clear=True):
            prod_preflight = _reload_module()
            from tools import validate_dependency_lock

            direct_entries = {
                "alpha-pkg": "requirements.in:1:alpha_pkg",
                "beta": "requirements.in:2:beta",
            }
            lock_entries = {
                "alpha-pkg": "requirements.lock.txt:1:alpha-pkg==1.0.0",
                "beta": "requirements.lock.txt:2:beta==2.0.0",
            }
            versions = {"alpha-pkg": "1.0.0", "beta": "2.0.0"}
            with patch.object(
                validate_dependency_lock,
                "_requirements_entries",
                side_effect=_requirements_entries_side_effect(direct_entries, lock_entries),
            ):
                with patch.object(
                    prod_preflight.importlib_metadata,
                    "version",
                    side_effect=lambda name: versions[name],
                ):
                    notes, warnings, errors, state = prod_preflight._dependency_drift_gate()

        self.assertEqual(warnings, [])
        self.assertEqual(errors, [])
        self.assertEqual(notes, ["dependency drift check: direct=2 ok=2 lock=requirements.lock.txt"])
        self.assertTrue(state["checked"])
        self.assertEqual(state["direct_count"], 2)
        self.assertEqual(state["ok_count"], 2)
        self.assertEqual(state["drift"], [])
        self.assertEqual(state["missing"], [])

    def test_dependency_drift_gate_warns_on_version_drift_in_safe_mode(self) -> None:
        with patch.dict(os.environ, {"ENGINE_MODE": "safe"}, clear=True):
            prod_preflight = _reload_module()
            from tools import validate_dependency_lock

            with patch.object(
                validate_dependency_lock,
                "_requirements_entries",
                side_effect=_requirements_entries_side_effect(
                    {"alpha-pkg": "requirements.in:1:alpha-pkg"},
                    {"alpha-pkg": "requirements.lock.txt:1:alpha-pkg==1.0.0"},
                ),
            ):
                with patch.object(prod_preflight.importlib_metadata, "version", return_value="1.1.0"):
                    notes, warnings, errors, state = prod_preflight._dependency_drift_gate()

        self.assertEqual(notes, ["dependency drift check: direct=1 ok=0 lock=requirements.lock.txt"])
        self.assertEqual(errors, [])
        self.assertEqual(
            warnings,
            [
                "dependency_drift: 1 drifted, 0 missing vs requirements.lock.txt: "
                "alpha-pkg installed=1.1.0 locked=1.0.0"
            ],
        )
        self.assertEqual(state["drift"], ["alpha-pkg installed=1.1.0 locked=1.0.0"])
        self.assertEqual(state["missing"], [])

    def test_dependency_drift_gate_fails_on_version_drift_in_live_mode(self) -> None:
        with patch.dict(os.environ, {"ENGINE_MODE": "live"}, clear=True):
            prod_preflight = _reload_module()
            from tools import validate_dependency_lock

            with patch.object(
                validate_dependency_lock,
                "_requirements_entries",
                side_effect=_requirements_entries_side_effect(
                    {"alpha-pkg": "requirements.in:1:alpha-pkg"},
                    {"alpha-pkg": "requirements.lock.txt:1:alpha-pkg==1.0.0"},
                ),
            ):
                with patch.object(prod_preflight.importlib_metadata, "version", return_value="1.1.0"):
                    _notes, warnings, errors, state = prod_preflight._dependency_drift_gate()

        self.assertEqual(warnings, [])
        self.assertEqual(
            errors,
            [
                "dependency_drift: 1 drifted, 0 missing vs requirements.lock.txt: "
                "alpha-pkg installed=1.1.0 locked=1.0.0"
            ],
        )
        self.assertTrue(state["required"])

    def test_dependency_drift_gate_fails_when_lock_match_env_is_required(self) -> None:
        with patch.dict(
            os.environ,
            {"ENGINE_MODE": "safe", "PROD_PREFLIGHT_REQUIRE_DEPENDENCY_LOCK_MATCH": "1"},
            clear=True,
        ):
            prod_preflight = _reload_module()
            from tools import validate_dependency_lock

            with patch.object(
                validate_dependency_lock,
                "_requirements_entries",
                side_effect=_requirements_entries_side_effect(
                    {"alpha-pkg": "requirements.in:1:alpha-pkg"},
                    {"alpha-pkg": "requirements.lock.txt:1:alpha-pkg==1.0.0"},
                ),
            ):
                with patch.object(prod_preflight.importlib_metadata, "version", return_value="1.1.0"):
                    _notes, warnings, errors, state = prod_preflight._dependency_drift_gate()

        self.assertEqual(warnings, [])
        self.assertTrue(errors)
        self.assertTrue(state["required"])

    def test_dependency_drift_gate_warns_on_missing_package_in_safe_mode(self) -> None:
        with patch.dict(os.environ, {"ENGINE_MODE": "safe"}, clear=True):
            prod_preflight = _reload_module()
            from tools import validate_dependency_lock

            def _missing(_name: str) -> str:
                raise prod_preflight.importlib_metadata.PackageNotFoundError("alpha-pkg")

            with patch.object(
                validate_dependency_lock,
                "_requirements_entries",
                side_effect=_requirements_entries_side_effect(
                    {"alpha-pkg": "requirements.in:1:alpha-pkg"},
                    {"alpha-pkg": "requirements.lock.txt:1:alpha-pkg==1.0.0"},
                ),
            ):
                with patch.object(prod_preflight.importlib_metadata, "version", side_effect=_missing):
                    _notes, warnings, errors, state = prod_preflight._dependency_drift_gate()

        self.assertEqual(errors, [])
        self.assertEqual(
            warnings,
            [
                "dependency_drift: 0 drifted, 1 missing vs requirements.lock.txt: "
                "alpha-pkg locked=1.0.0 not-installed"
            ],
        )
        self.assertEqual(state["missing"], ["alpha-pkg locked=1.0.0 not-installed"])

    def test_dependency_drift_gate_notes_unlocked_direct_without_failure(self) -> None:
        with patch.dict(os.environ, {"ENGINE_MODE": "live"}, clear=True):
            prod_preflight = _reload_module()
            from tools import validate_dependency_lock

            with patch.object(
                validate_dependency_lock,
                "_requirements_entries",
                side_effect=_requirements_entries_side_effect(
                    {"alpha-pkg": "requirements.in:1:alpha-pkg"},
                    {},
                ),
            ):
                with patch.object(
                    prod_preflight.importlib_metadata,
                    "version",
                    side_effect=AssertionError("unlocked direct packages are not version-compared"),
                ):
                    notes, warnings, errors, state = prod_preflight._dependency_drift_gate()

        self.assertEqual(warnings, [])
        self.assertEqual(errors, [])
        self.assertEqual(
            notes,
            [
                "dependency drift check: direct=1 ok=0 lock=requirements.lock.txt",
                "unlocked_direct:alpha-pkg",
            ],
        )
        self.assertEqual(state["unlocked_direct"], ["unlocked_direct:alpha-pkg"])

    def test_dependency_drift_gate_skip_env_bypasses_strict_live_error_for_diagnostics(self) -> None:
        with patch.dict(
            os.environ,
            {"ENGINE_MODE": "live", "PROD_PREFLIGHT_SKIP_DEPENDENCY_DRIFT": "1"},
            clear=True,
        ):
            prod_preflight = _reload_module()
            from tools import validate_dependency_lock

            with patch.object(
                validate_dependency_lock,
                "_requirements_entries",
                side_effect=AssertionError("parser should not run when skipped"),
            ) as requirements_entries:
                notes, warnings, errors, state = prod_preflight._dependency_drift_gate()

        requirements_entries.assert_not_called()
        self.assertEqual(notes, [])
        self.assertEqual(errors, [])
        self.assertEqual(
            warnings,
            [
                "dependency_drift: skipped by PROD_PREFLIGHT_SKIP_DEPENDENCY_DRIFT=1; "
                "requirements.lock.txt match not enforced"
            ],
        )
        self.assertFalse(state["checked"])
        self.assertTrue(state["required"])
        self.assertTrue(state["skipped"])

    def test_dependency_drift_gate_parser_failure_is_fail_soft(self) -> None:
        with patch.dict(os.environ, {"ENGINE_MODE": "live"}, clear=True):
            prod_preflight = _reload_module()
            from tools import validate_dependency_lock

            with patch.object(
                validate_dependency_lock,
                "_requirements_entries",
                side_effect=OSError("/tmp/secret/path/requirements.in"),
            ):
                notes, warnings, errors, state = prod_preflight._dependency_drift_gate()

        self.assertEqual(notes, [])
        self.assertEqual(warnings, [])
        self.assertEqual(errors, [])
        self.assertEqual(state, {})


if __name__ == "__main__":
    unittest.main()
