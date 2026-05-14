"""
FILE: decision_snapshot.py

Human-readable purpose:
Writes a point-in-time snapshot of the decision context used by the strategy
layer, including allocations, portfolio state, execution mode, and regime
vectors. This is primarily an audit/debug artifact.
"""

import json
import time
from typing import Dict

from engine.runtime.storage import connect, init_db, run_write_txn
from engine.execution.execution_mode import get_execution_mode
from engine.strategy.regime_stack import compute_regime_vector


def _now_ms() -> int:
    return int(time.time() * 1000)


def snapshot_decision(universe: Dict, allocation: Dict, portfolio: Dict):
    """
    Stores full decision snapshot including regime vectors per symbol.
    """

    init_db()
    ts = _now_ms()

    regime_vectors = {}
    con = connect(readonly=True)
    try:
        try:
            # Capture regime state alongside the allocation so later debugging
            # can explain not just what was chosen, but under what market regime.
            for sym in (allocation or {}).keys():
                regime_vectors[sym] = compute_regime_vector(
                    symbol=sym,
                    ts_ms=ts,
                    con=con,
                )
        except Exception:
            regime_vectors = {}
    finally:
        con.close()

    def _write(txn_con):
        txn_con.execute(
            """
            INSERT INTO trade_decision_snapshot(
            ts_ms,
            universe_json,
            allocation_json,
            portfolio_json,
            execution_mode_json,
            regime_model_version,
            regime_vectors_json
            )
            VALUES (?,?,?,?,?,?,?)

            """,
            (
                ts,
                json.dumps(universe, separators=(",", ":"), sort_keys=True),
                json.dumps(allocation, separators=(",", ":"), sort_keys=True),
                json.dumps(portfolio, separators=(",", ":"), sort_keys=True),
                json.dumps(get_execution_mode(), separators=(",", ":"), sort_keys=True),
                "regime_stack_v1",
                json.dumps(regime_vectors, separators=(",", ":"), sort_keys=True),
            ),
        )

    run_write_txn(_write)
