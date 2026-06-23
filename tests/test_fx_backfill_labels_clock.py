from __future__ import annotations

import importlib
import json
import os
import sys
import unittest
import uuid
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


NY = ZoneInfo("America/New_York")


def _ms_et(year: int, month: int, day: int, hour: int, minute: int = 0) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=NY).timestamp() * 1000)


class FxBackfillLabelsClockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.backfill = importlib.reload(
            importlib.import_module("engine.data.jobs.backfill_labels_price_from_prices")
        )

    def test_fx_gap_window_skips_and_non_fx_is_unchanged(self) -> None:
        start = _ms_et(2026, 1, 9, 16, 30)
        target, meta, skip = self.backfill._label_price_eval_target("EURUSD", start, 3600)
        self.assertTrue(skip)
        self.assertEqual(target, _ms_et(2026, 1, 11, 17, 30))
        self.assertEqual(meta["naive_eval_ms"], start + 3_600_000)
        self.assertTrue(meta["fx_clock_corrected"])

        equity_target, equity_meta, equity_skip = self.backfill._label_price_eval_target("SPY", start, 3600)
        self.assertFalse(equity_skip)
        self.assertEqual(equity_target, start + 3_600_000)
        self.assertEqual(equity_meta, {})

    def test_fx_meta_payload_uses_existing_json_enrichment_path(self) -> None:
        canary = "CANARY-" + uuid.uuid4().hex
        os.environ["FX04_SECRET_SHAPED_VALUE"] = canary
        self.addCleanup(os.environ.pop, "FX04_SECRET_SHAPED_VALUE", None)
        start = _ms_et(2026, 1, 7, 10, 0)
        target, meta, skip = self.backfill._label_price_eval_target("EURUSD", start, 300)
        self.assertFalse(skip)
        self.assertEqual(target, start + 300_000)

        payload = {"entry_ts_ms": start + 1, "exit_ts_ms": target + 1}
        payload.update(meta)
        encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        decoded = json.loads(encoded)
        self.assertTrue(decoded["fx_clock_corrected"])
        self.assertEqual(decoded["naive_eval_ms"], start + 300_000)
        self.assertNotIn(canary, encoded)


if __name__ == "__main__":
    unittest.main()
