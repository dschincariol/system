from __future__ import annotations

import importlib
import io
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_module():
    import engine.execution.train_size_policy as train_size_policy

    return importlib.reload(train_size_policy)


class TrainSizePolicyContractTests(unittest.TestCase):
    class _EmptyTrainingConnection:
        def __init__(self) -> None:
            self.closed = False

        def execute(self, sql, params=()):
            text = " ".join(str(sql).split()).lower()
            if "where type='table' and name=?" in text:
                table_name = str((params or [""])[0] or "")
                if table_name in {"predictions", "labels_exec"}:
                    return TrainSizePolicyContractTests._Cursor([(1,)])
                return TrainSizePolicyContractTests._Cursor([])
            if "pragma table_info(labels_exec)" in text:
                return TrainSizePolicyContractTests._Cursor([(0, "realized", "INTEGER", 0, None, 0)])
            if "select p.confidence, le.net_ret" in text:
                return TrainSizePolicyContractTests._Cursor([])
            raise AssertionError(f"unexpected query: {sql!r} params={params!r}")

        def close(self):
            self.closed = True

    class _Cursor:
        def __init__(self, rows):
            self._rows = list(rows)

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def fetchall(self):
            return list(self._rows)

    def test_main_skips_empty_dataset_during_preflight_smoke(self) -> None:
        train_size_policy = _reload_module()
        fake_con = self._EmptyTrainingConnection()
        stdout = io.StringIO()

        with patch.dict(os.environ, {"ENGINE_SUPERVISED": "1", "PREFLIGHT_SMOKE": "1"}, clear=False):
            with patch.object(train_size_policy, "init_db"):
                with patch.object(train_size_policy, "init_size_policy_schema"):
                    with patch.object(train_size_policy, "connect", return_value=fake_con):
                        with patch.object(sys, "stdout", stdout):
                            train_size_policy.main()

        self.assertTrue(fake_con.closed)
        self.assertIn("[size_policy] skip: not enough samples: 0 < 200", stdout.getvalue())

    def test_main_preserves_empty_dataset_failure_outside_preflight_smoke(self) -> None:
        train_size_policy = _reload_module()
        fake_con = self._EmptyTrainingConnection()

        with patch.dict(os.environ, {"ENGINE_SUPERVISED": "1", "PREFLIGHT_SMOKE": "0"}, clear=False):
            with patch.object(train_size_policy, "init_db"):
                with patch.object(train_size_policy, "init_size_policy_schema"):
                    with patch.object(train_size_policy, "connect", return_value=fake_con):
                        with self.assertRaises(SystemExit) as raised:
                            train_size_policy.main()

        self.assertTrue(fake_con.closed)
        self.assertIn("[size_policy] not enough samples: 0 < 200", str(raised.exception))

    def test_init_size_policy_schema_skips_write_txn_when_marker_is_ready(self) -> None:
        train_size_policy = _reload_module()
        with patch.object(train_size_policy, "_size_policy_schema_marker_ready", return_value=True):
            with patch.object(train_size_policy, "run_write_txn", side_effect=AssertionError("write txn should be skipped")):
                train_size_policy.init_size_policy_schema()

    def test_init_size_policy_schema_uses_retrying_direct_write_txn_when_marker_missing(self) -> None:
        train_size_policy = _reload_module()
        with patch.object(train_size_policy, "_size_policy_schema_marker_ready", return_value=False):
            with patch.object(train_size_policy, "run_write_txn") as run_write_txn:
                train_size_policy.init_size_policy_schema()

        run_write_txn.assert_called_once_with(
            train_size_policy._init_size_policy_schema,
            table="size_policy",
            operation="init_size_policy_schema",
            direct=True,
        )

    def test_schema_ready_accepts_readonly_schema_probe_without_marker(self) -> None:
        train_size_policy = _reload_module()

        class _FakeCursor:
            def __init__(self, rows):
                self._rows = rows

            def fetchone(self):
                return self._rows[0] if self._rows else None

            def fetchall(self):
                return list(self._rows)

        class _FakeConnection:
            def __init__(self) -> None:
                self.closed = False

            def execute(self, sql, params=()):
                text = " ".join(str(sql).split()).lower()
                if "where type='table' and name=?" in text:
                    table_name = str(params[0] or "")
                    if table_name in {"size_policy", "size_policy_points"}:
                        return _FakeCursor([(1,)])
                    return _FakeCursor([])
                if "pragma table_info(size_policy)" in text:
                    return _FakeCursor(
                        [
                            (0, "id", "INTEGER", 0, None, 1),
                            (1, "lookback_days", "INTEGER", 1, None, 0),
                            (2, "buckets", "INTEGER", 1, None, 0),
                        ]
                    )
                if "where type='index' and name=?" in text:
                    index_name = str(params[0] or "")
                    if index_name in train_size_policy._SIZE_POLICY_SCHEMA_INDEXES:
                        return _FakeCursor([(1,)])
                    return _FakeCursor([])
                raise AssertionError(f"unexpected query: {sql!r} params={params!r}")

            def close(self):
                self.closed = True

        fake_con = _FakeConnection()
        with patch.object(train_size_policy, "connect", return_value=fake_con):
            self.assertTrue(train_size_policy._size_policy_schema_marker_ready())

        self.assertTrue(fake_con.closed)

    def test_store_size_policy_inserts_every_bucket_point(self) -> None:
        train_size_policy = _reload_module()

        class _FakeCursor:
            def __init__(self, rows):
                self._rows = rows

            def fetchone(self):
                return self._rows[0] if self._rows else None

        class _FakeConnection:
            def __init__(self) -> None:
                self.calls = []

            def execute(self, sql, params=()):
                self.calls.append((sql, params))
                if "pragma table_info(size_policy)" in " ".join(str(sql).split()).lower():
                    return _FakeCursor(
                        [
                            (0, "id", "INTEGER", 0, None, 1),
                            (1, "ts_ms", "INTEGER", 1, None, 0),
                            (2, "lookback_days", "INTEGER", 1, None, 0),
                            (3, "buckets", "INTEGER", 1, None, 0),
                        ]
                    )
                if "last_insert_rowid" in str(sql).lower():
                    return _FakeCursor([(42,)])
                return _FakeCursor([])

        fake_con = _FakeConnection()
        points = [
            {
                "bucket_idx": 0,
                "conf_lo": 0.0,
                "conf_hi": 0.5,
                "n": 10,
                "mean_net_ret": 0.1,
                "std_net_ret": 0.2,
                "factor": 0.3,
            },
            {
                "bucket_idx": 1,
                "conf_lo": 0.5,
                "conf_hi": 1.0,
                "n": 11,
                "mean_net_ret": 0.2,
                "std_net_ret": 0.3,
                "factor": 0.4,
            },
        ]

        policy_id = train_size_policy._store_size_policy(
            fake_con,
            ts_ms=1234,
            params={"lookback_days": 90},
            metrics={"n_samples": 21},
            points=points,
        )

        point_inserts = [
            call
            for call in fake_con.calls
            if "insert into size_policy_points" in " ".join(str(call[0]).split()).lower()
        ]
        self.assertEqual(policy_id, 42)
        self.assertEqual(len(point_inserts), len(points))

    def test_store_size_policy_uses_legacy_insert_when_metadata_columns_are_missing(self) -> None:
        train_size_policy = _reload_module()

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
                    return _FakeCursor([(7,)])
                return _FakeCursor([])

        fake_con = _FakeConnection()
        train_size_policy._store_size_policy(
            fake_con,
            ts_ms=1234,
            params={"lookback_days": 90, "buckets": 10},
            metrics={"n_samples": 21},
            points=[],
        )

        insert_sql = " ".join(str(fake_con.calls[1][0]).split()).lower()
        self.assertIn("insert into size_policy(ts_ms, method, params_json, metrics_json)", insert_sql)


if __name__ == "__main__":
    unittest.main()
