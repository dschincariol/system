from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

from tools import storage_route_audit


class StorageRouteAuditTests(unittest.TestCase):
    def _write_file(self, root: Path, relative_path: str, contents: str) -> Path:
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(contents).lstrip(), encoding="utf-8")
        return path

    def test_scan_repo_flags_unapproved_run_write_txn_scoped_table(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_file(
                root,
                "engine/runtime/bad_writer.py",
                """
                from engine.runtime.storage import run_write_txn

                def write_price_health():
                    def _txn(con):
                        con.execute("SELECT 1")

                    return run_write_txn(_txn, table="price_provider_health", operation="unit_test")
                """,
            )

            findings = storage_route_audit.scan_repo(repo_root=root, scan_dirs=("engine",), approved_paths=set())

            self.assertEqual(len(findings), 1)
            self.assertEqual(findings[0]["rule"], "run_write_txn_scoped_table")
            self.assertEqual(findings[0]["table"], "price_provider_health")
            self.assertEqual(findings[0]["path"], "engine/runtime/bad_writer.py")

    def test_scan_repo_flags_unapproved_sql_write_call(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_file(
                root,
                "engine/data/poll_prices.py",
                """
                def persist_rows(con, row):
                    con.execute(
                        '''
                        INSERT INTO prices(ts_ms, symbol, price, px, source)
                        VALUES (?,?,?,?,?)
                        ''',
                        row,
                    )
                """,
            )

            findings = storage_route_audit.scan_repo(repo_root=root, scan_dirs=("engine",), approved_paths=set())

            self.assertEqual(len(findings), 1)
            self.assertEqual(findings[0]["rule"], "sql_write_scoped_table")
            self.assertEqual(findings[0]["table"], "prices")
            self.assertEqual(findings[0]["path"], "engine/data/poll_prices.py")

    def test_scan_repo_skips_approved_owner_module(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_file(
                root,
                "engine/runtime/price_router.py",
                """
                def write_rows(con, rows):
                    con.executemany(
                        '''
                        INSERT INTO price_quotes(ts_ms, symbol, last, bid, ask, spread, volume)
                        VALUES (?,?,?,?,?,?,?)
                        ''',
                        rows,
                    )
                """,
            )

            findings = storage_route_audit.scan_repo(
                repo_root=root,
                scan_dirs=("engine",),
                approved_paths={"engine/runtime/price_router.py"},
            )

            self.assertEqual(findings, [])

    def test_scan_repo_skips_schema_migrations(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_file(
                root,
                "engine/runtime/schema/migrations/9999_test_price_quotes.py",
                """
                def up(conn):
                    conn.execute("ALTER TABLE price_quotes ADD COLUMN IF NOT EXISTS source_latency_ms BIGINT")
                """,
            )

            findings = storage_route_audit.scan_repo(repo_root=root, scan_dirs=("engine",), approved_paths=set())

            self.assertEqual(findings, [])

    def test_allow_marker_suppresses_finding(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._write_file(
                root,
                "engine/runtime/manual_exception.py",
                f"""
                def persist_rows(con, row):
                    # {storage_route_audit.ALLOW_MARKER}
                    con.execute(
                        "INSERT INTO event_log(ts_ms, event_type, event_source, event_version, payload_json) VALUES (?,?,?,?,?)",
                        row,
                    )
                """,
            )

            findings = storage_route_audit.scan_repo(repo_root=root, scan_dirs=("engine",), approved_paths=set())

            self.assertEqual(findings, [])

    def test_write_baseline_round_trips_fingerprints(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            baseline_path = Path(tmp) / "storage_route_audit_baseline.json"
            findings = [
                {
                    "rule": "sql_write_scoped_table",
                    "path": "engine/data/poll_prices.py",
                    "line": 12,
                    "table": "prices",
                    "snippet": "INSERT INTO prices(ts_ms, symbol, price)",
                    "fingerprint": "sql_write_scoped_table|engine/data/poll_prices.py|prices|INSERT INTO prices(ts_ms, symbol, price)",
                }
            ]

            storage_route_audit.write_baseline(findings, path=baseline_path)
            loaded = storage_route_audit.load_baseline(baseline_path)

            self.assertEqual(
                loaded["allowed_fingerprints"],
                [findings[0]["fingerprint"]],
            )


if __name__ == "__main__":
    unittest.main()
