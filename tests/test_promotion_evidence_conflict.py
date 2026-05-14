from __future__ import annotations

import sqlite3

import pytest

from engine.strategy import promotion_audit, promotion_guard


def _create_evidence_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE promotion_statistical_evidence (
            id INTEGER PRIMARY KEY,
            ts INTEGER NOT NULL,
            model_id TEXT NOT NULL,
            feature_id TEXT,
            evidence_kind TEXT,
            test_name TEXT NOT NULL,
            t_stat REAL,
            p_value REAL,
            q_value REAL,
            bootstrap_samples INTEGER,
            decision TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            prev_hash BLOB,
            row_hash BLOB
        )
        """
    )


def test_assess_challenger_rejects_duplicate_model_evidence_unmarked() -> None:
    con = sqlite3.connect(":memory:")
    _create_evidence_table(con)
    kwargs = {
        "con": con,
        "model_id": "duplicate_AAPL_1700000000006_abcdef7",
        "model_name": "duplicate_AAPL_1700000000006_abcdef7",
        "challenger_returns": [0.02 + (idx % 5) * 0.001 for idx in range(40)],
        "champion_returns": [0.001 * (idx % 3) for idx in range(40)],
        "bootstrap_samples": 199,
        "random_state": 11,
    }

    promotion_guard.assess_challenger(**kwargs)

    with pytest.raises(promotion_audit.EvidenceConflict) as exc:
        promotion_guard.assess_challenger(**kwargs)

    assert exc.value.original_ts_ms > 0
    assert exc.value.model_id == "duplicate_AAPL_1700000000006_abcdef7"
    assert exc.value.evidence_kind == "white_reality_check"
    con.close()
