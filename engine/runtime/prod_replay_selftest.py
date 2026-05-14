"""
FILE: prod_replay_selftest.py

Runtime replay determinism self-test.
"""

import json
import os
import sys

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
if _SCRIPT_DIR in sys.path:
    sys.path.remove(_SCRIPT_DIR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from engine.runtime.event_replay import (
    replay_determinism_snapshot,
    replay_model_predictions_determinism_snapshot,
    replay_pipeline_chain_determinism_snapshot,
    replay_persisted_determinism_snapshot,
)
from engine.runtime.event_log import flush_event_log_buffer
from engine.runtime.storage import connect


def main() -> int:
    flush_event_log_buffer(max_batches=64)
    con = connect(readonly=True)
    try:
        row = con.execute("SELECT MAX(id) FROM event_log").fetchone()
        max_event_id = int((row or [0])[0] or 0)
    finally:
        con.close()

    after_event_id = max(0, int(max_event_id) - 5000)
    event_snap = replay_determinism_snapshot(after_event_id=after_event_id, limit=5000)
    persisted_snap = replay_persisted_determinism_snapshot(after_event_id=after_event_id, limit=5000)
    chain_snap = replay_pipeline_chain_determinism_snapshot(after_event_id=after_event_id, limit=5000)
    model_snap = replay_model_predictions_determinism_snapshot(
        after_event_id=after_event_id,
        limit_events=5,
        symbol_limit=12,
    )
    out = {
        "event_replay": event_snap,
        "persisted_outputs": persisted_snap,
        "pipeline_chain": chain_snap,
        "model_predictions": model_snap,
    }
    print(json.dumps(out, indent=2, sort_keys=True))
    ok = (
        bool(event_snap.get("ok")) and bool(event_snap.get("deterministic"))
        and bool(persisted_snap.get("ok")) and bool(persisted_snap.get("deterministic"))
        and bool(chain_snap.get("ok")) and bool(chain_snap.get("deterministic"))
        and bool(model_snap.get("ok")) and bool(model_snap.get("deterministic"))
    )
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
