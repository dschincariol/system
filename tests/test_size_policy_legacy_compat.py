from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class SizePolicyLegacyCompatTests(unittest.TestCase):
    def test_load_latest_size_policy_falls_back_to_legacy_row_shape(self) -> None:
        import engine.strategy.size_policy as size_policy

        size_policy = importlib.reload(size_policy)

        class _FakeCursor:
            def __init__(self, rows):
                self._rows = rows

            def fetchone(self):
                return self._rows[0] if self._rows else None

            def fetchall(self):
                return list(self._rows)

        class _FakeConnection:
            def execute(self, sql, params=()):
                text = " ".join(str(sql).split()).lower()
                if "select id, ts_ms, lookback_days, buckets" in text:
                    raise RuntimeError("no such column: lookback_days")
                if "select id, ts_ms, method, params_json, metrics_json" in text:
                    return _FakeCursor(
                        [
                            (
                                5,
                                1111,
                                "bucket_sharpe_monotone",
                                '{"lookback_days":90,"buckets":10}',
                                '{"n_samples":42}',
                            )
                        ]
                    )
                if "from size_policy_points" in text:
                    return _FakeCursor([(0, 0.0, 1.0, 42, 0.1, 0.2, 0.3)])
                raise AssertionError(f"unexpected query: {sql!r} params={params!r}")

        policy = size_policy.load_latest_size_policy(con=_FakeConnection())

        self.assertIsNotNone(policy)
        assert policy is not None
        self.assertEqual(policy["policy_id"], 5)
        self.assertEqual(policy["lookback_days"], 90)
        self.assertEqual(policy["buckets"], 10)
        self.assertEqual(len(policy["points"]), 1)

    def test_position_sizing_uses_loader_points_for_factor_lookup(self) -> None:
        import engine.strategy.position_sizing as position_sizing

        position_sizing = importlib.reload(position_sizing)
        position_sizing._size_policy_cache.update({"ts": 0.0, "buckets": 0, "points": None})

        class _FakeConnection:
            def close(self):
                return None

        with patch("engine.strategy.size_policy.load_latest_size_policy", return_value={
            "policy_id": 9,
            "buckets": 2,
            "points": [
                {"conf_lo": 0.0, "conf_hi": 0.5, "factor": 0.2, "n": 1, "mean_net_ret": 0.0, "std_net_ret": 1.0},
                {"conf_lo": 0.5, "conf_hi": 1.0, "factor": 0.7, "n": 1, "mean_net_ret": 0.0, "std_net_ret": 1.0},
            ],
        }):
            with patch.object(position_sizing, "connect", return_value=_FakeConnection()):
                factor_point = position_sizing._get_size_policy_factor(0.8)

        self.assertIsNotNone(factor_point)
        assert factor_point is not None
        self.assertEqual(factor_point[0], 0.7)
        self.assertEqual(position_sizing._size_policy_cache["buckets"], 2)

    def test_train_drawdown_policy_store_uses_legacy_insert_when_metadata_columns_missing(self) -> None:
        import engine.execution.train_drawdown_policy as train_drawdown_policy

        train_drawdown_policy = importlib.reload(train_drawdown_policy)

        class _FakeCursor:
            def __init__(self, rows):
                self._rows = rows

            def fetchone(self):
                return self._rows[0] if self._rows else None

            def fetchall(self):
                return list(self._rows)

        class _FakeConnection:
            def __init__(self) -> None:
                self.calls = []

            def execute(self, sql, params=()):
                self.calls.append((sql, params))
                text = " ".join(str(sql).split()).lower()
                if "pragma table_info(size_policy)" in text:
                    return _FakeCursor(
                        [
                            (0, "id", "INTEGER", 0, None, 1),
                            (1, "ts_ms", "INTEGER", 1, None, 0),
                            (2, "method", "TEXT", 1, None, 0),
                            (3, "params_json", "TEXT", 1, None, 0),
                            (4, "metrics_json", "TEXT", 1, None, 0),
                        ]
                    )
                if "last_insert_rowid" in text:
                    return _FakeCursor([(11,)])
                return _FakeCursor([])

        fake_con = _FakeConnection()
        train_drawdown_policy._store_drawdown_policy(
            fake_con,
            ts_ms=1234,
            params={"lookback_days": 180, "dd_bins": [0.1]},
            metrics={"n_samples": 300},
        )

        insert_sql = " ".join(str(fake_con.calls[1][0]).split()).lower()
        self.assertIn("insert into size_policy(ts_ms, method, params_json, metrics_json)", insert_sql)

    def test_api_get_size_policy_maps_loader_payload_for_dashboard(self) -> None:
        import engine.api.api_read_advanced as api_read_advanced

        api_read_advanced = importlib.reload(api_read_advanced)

        class _FakeConnection:
            def close(self):
                return None

        with patch("engine.api.internal_access.db_connect", return_value=_FakeConnection()):
            with patch("engine.strategy.size_policy.load_latest_size_policy", return_value={
                "policy_id": 3,
                "ts_ms": 999,
                "lookback_days": 90,
                "buckets": 10,
                "method": "bucket_sharpe_monotone",
                "params": {"lookback_days": 90},
                "metrics": {"n_samples": 50},
                "points": [{"bucket_idx": 0, "conf_lo": 0.0, "conf_hi": 1.0, "n": 50, "mean_net_ret": 0.1, "std_net_ret": 0.2, "factor": 0.3}],
            }):
                payload = api_read_advanced.get_size_policy()

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["policy"]["id"], 3)
        self.assertEqual(payload["policy"]["buckets"], 10)
        self.assertEqual(len(payload["points"]), 1)


if __name__ == "__main__":
    unittest.main()
