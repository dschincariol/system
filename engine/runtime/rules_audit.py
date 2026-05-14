"""
FILE: rules_audit.py

Runtime subsystem module for `rules_audit`.
"""

# dev_core/rules_audit.py
"""
Persistent audit trail for rule decisions.
"""

import time
import json
from engine.runtime.storage import connect


SCHEMA = """
CREATE TABLE IF NOT EXISTS rules_audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  scope TEXT NOT NULL,
  reason TEXT NOT NULL,
  state TEXT NOT NULL,
  details_json TEXT
);
"""


def init_rules_audit_db():
    # rules_audit is append-only operational evidence, so init is lightweight
    # and idempotent.
    con = connect()
    try:
        con.execute(SCHEMA)
        con.commit()
    finally:
        con.close()


def log_rule(scope: str, reason: str, state: str, details: dict | None = None):
    con = connect()
    try:
        # Keep the payload schema intentionally loose so new rule families can
        # add details without a migration each time.
        con.execute(
            """
            INSERT INTO rules_audit(ts_ms, scope, reason, state, details_json)
            VALUES (?,?,?,?,?)
            """,
            (
                int(time.time() * 1000),
                str(scope),
                str(reason),
                str(state),
                json.dumps(details or {}, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.commit()
    finally:
        con.close()
