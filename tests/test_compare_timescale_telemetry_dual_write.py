from __future__ import annotations

import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from tools import compare_timescale_telemetry_dual_write


class CompareTimescaleTelemetryDualWriteTests(unittest.TestCase):
    def test_main_passes_through_validation_arguments(self) -> None:
        output = io.StringIO()
        expected = {"ok": True, "detail": "validation_ok", "tables": {"runtime_metrics": {"ok": True}}}

        with (
            patch.object(
                compare_timescale_telemetry_dual_write,
                "build_telemetry_migration_validation_snapshot",
                return_value=expected,
            ) as build_mock,
            redirect_stdout(output),
        ):
            exit_code = compare_timescale_telemetry_dual_write.main(
                [
                    "--lookback-minutes",
                    "15",
                    "--max-count-delta",
                    "2",
                    "--max-last-ts-lag-ms",
                    "2500",
                    "--require-healthy-mirror",
                    "--require-healthy-timescale",
                    "--json",
                ]
            )

        self.assertEqual(exit_code, 0)
        build_mock.assert_called_once_with(
            lookback_minutes=15,
            max_count_delta=2,
            max_last_ts_lag_ms=2500,
            require_healthy_mirror=True,
            require_healthy_timescale=True,
        )
        rendered = json.loads(output.getvalue())
        self.assertEqual(rendered, expected)

    def test_main_strict_mode_fails_when_snapshot_is_not_ok(self) -> None:
        output = io.StringIO()
        snapshot = {"ok": False, "detail": "runtime_metrics_parity_out_of_bounds", "reasons": ["runtime_metrics_parity_out_of_bounds"]}

        with (
            patch.object(
                compare_timescale_telemetry_dual_write,
                "build_telemetry_migration_validation_snapshot",
                return_value=snapshot,
            ),
            redirect_stdout(output),
        ):
            exit_code = compare_timescale_telemetry_dual_write.main(["--strict"])

        self.assertEqual(exit_code, 1)
        self.assertEqual(json.loads(output.getvalue()), snapshot)

    def test_main_non_strict_mode_keeps_zero_exit_for_report_only_runs(self) -> None:
        output = io.StringIO()
        snapshot = {"ok": False, "detail": "timescale_disabled", "reasons": ["timescale_disabled"]}

        with (
            patch.object(
                compare_timescale_telemetry_dual_write,
                "build_telemetry_migration_validation_snapshot",
                return_value=snapshot,
            ),
            redirect_stdout(output),
        ):
            exit_code = compare_timescale_telemetry_dual_write.main([])

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(output.getvalue()), snapshot)


if __name__ == "__main__":
    unittest.main()
