import sys
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.runtime import health


NOW_MS = 1_700_000_000_000


class DummyConnection:
    def __enter__(self):
        return object()

    def __exit__(self, exc_type, exc, tb):
        return False


class OptionsIngestionHealthDataQualityTests(unittest.TestCase):
    def test_options_snapshot_keeps_existing_keys_and_adds_data_quality(self):
        data_quality = {
            "available": True,
            "ok": True,
            "degraded": False,
            "coverage_fraction": 1.0,
            "providers": {"polygon": {"greeks_complete_fraction": 1.0}},
        }

        with mock.patch.object(health, "get_pipeline_status", return_value=None):
            with mock.patch.object(health, "_options_credentials_configured", return_value=(False, [])):
                with mock.patch.object(health, "_db_connect", return_value=DummyConnection()):
                    with mock.patch(
                        "engine.data.options_data_quality.compute_options_data_quality",
                        return_value=data_quality,
                    ):
                        with mock.patch(
                            "engine.data.options_data_quality.record_options_data_quality_observability",
                            return_value={"events": 0},
                        ):
                            snapshot = health._options_ingestion_snapshot(NOW_MS)

        for key in ("ok", "available", "degraded", "critical", "fresh_symbols"):
            self.assertIn(key, snapshot)
        self.assertIn("data_quality", snapshot)
        self.assertEqual(snapshot["data_quality"]["coverage_fraction"], 1.0)

    def test_options_snapshot_fails_soft_when_data_quality_raises(self):
        with mock.patch.object(health, "get_pipeline_status", return_value=None):
            with mock.patch.object(health, "_options_credentials_configured", return_value=(False, [])):
                with mock.patch.object(health, "_db_connect", return_value=DummyConnection()):
                    with mock.patch(
                        "engine.data.options_data_quality.compute_options_data_quality",
                        side_effect=RuntimeError("dq failed"),
                    ):
                        snapshot = health._options_ingestion_snapshot(NOW_MS)

        self.assertEqual(snapshot["detail"], "options_provider_unconfigured")
        self.assertIn("data_quality", snapshot)
        self.assertFalse(snapshot["data_quality"]["available"])
        self.assertFalse(snapshot["data_quality"]["ok"])
        self.assertTrue(snapshot["data_quality"]["degraded"])
        self.assertEqual(snapshot["data_quality"]["detail"], "options_data_quality_error")
        self.assertIn("options_data_quality_error", snapshot["data_quality"]["reason_codes"])


if __name__ == "__main__":
    unittest.main()
