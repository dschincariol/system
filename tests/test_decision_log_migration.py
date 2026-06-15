from __future__ import annotations

import importlib
import unittest


class DecisionLogMigrationTests(unittest.TestCase):
    def test_decision_log_component_vector_migration_declares_column(self) -> None:
        migration = importlib.import_module(
            "engine.runtime.schema.migrations.0031_decision_log_component_vector"
        )

        class FakeConn:
            def __init__(self) -> None:
                self.statements: list[str] = []

            def execute(self, sql: str, params=None):
                del params
                self.statements.append(str(sql))
                return self

        conn = FakeConn()
        migration.up(conn)
        sql = "\n".join(conn.statements)

        self.assertIn("ALTER TABLE IF EXISTS decision_log", sql)
        self.assertIn("ADD COLUMN IF NOT EXISTS component_vector JSONB NULL", sql)


if __name__ == "__main__":
    unittest.main()
