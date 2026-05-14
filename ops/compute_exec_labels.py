"""
FILE: compute_exec_labels.py

Operational helper script for `compute_exec_labels`.
"""

# compute_exec_labels.py
"""
Phase 5: Compute execution-aware labels (labels_exec).

Assumptions:
- Entry at alert/prediction time (ts_ms from predictions/alerts)
- Exit at ts_ms + horizon_s
- Use last/mid price from prices table (best-effort)
- side is inferred from predicted_z sign
"""

import json
import os
import sys
import time
from typing import Optional, Dict, Any, Tuple

from engine.runtime.storage import connect, init_db
from engine.execution.execution_costs import estimate_cost_bps, apply_cost_to_return
_WARNED_NONFATAL_KEYS: set[str] = set()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    details = ", ".join(f"{k}={v}" for k, v in (extra or {}).items())
    suffix = f" ({details})" if details else ""
    sys.stderr.write(f"[ops.compute_exec_labels] {code}: {type(error).__name__}: {error}{suffix}\n")
    sys.stderr.flush()
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _get_px_at_or_before(con, symbol: str, ts_ms: int) -> Optional[Tuple[float, int]]:
    r = con.execute(
        """
        SELECT ts_ms, price
        FROM prices
        WHERE symbol = ?
        AND ts_ms <= ?
        ORDER BY ts_ms DESC
        LIMIT 1
        """,
        (symbol, ts_ms),

    ).fetchone()

    if not r:
        return None
    try:
        # r = (ts_ms, price)
        return (float(r[1]), int(r[0]))
    except Exception as e:
        _warn_nonfatal("COMPUTE_EXEC_LABELS_PRICE_ROW_PARSE_FAILED", e, once_key="price_row_parse", symbol=str(symbol), ts_ms=int(ts_ms))
        return None

def _get_quote_meta_at_or_before(con, symbol: str, ts_ms: int) -> Dict[str, Any]:
    """
    Best-effort: if you stored bid/ask/spread in prices.extra_json (recommended),
    this tries to read it. If not present, returns empty dict.
    """
    # If your prices table does NOT have extra_json, this will safely except and return {}
    try:
        r = con.execute(
            """
            SELECT extra_json
            FROM prices
            WHERE symbol=? AND ts_ms <= ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (symbol, int(ts_ms)),
        ).fetchone()
        if not r or not r[0]:
            return {}
        return json.loads(r[0])
    except Exception as e:
        _warn_nonfatal("COMPUTE_EXEC_LABELS_QUOTE_META_PARSE_FAILED", e, once_key="quote_meta_parse", symbol=str(symbol), ts_ms=int(ts_ms))
        return {}


def _table_exists(con, name: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (str(name),),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal("COMPUTE_EXEC_LABELS_TABLE_EXISTS_FAILED", e, once_key=f"table_exists:{name}", table_name=str(name))
        return False


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("options_poll must be launched by supervisor")
        raise SystemExit(1)

    init_db()
    con = connect()
    try:
        if not _table_exists(con, "predictions"):
            print("[labels_exec] skip: predictions table missing")
            return

        if not _table_exists(con, "labels_exec"):
            print("[labels_exec] skip: labels_exec table missing")
            return

        if not _table_exists(con, "prices"):
            print("[labels_exec] skip: prices table missing")
            return

        # Pull latest predictions that do not yet have exec labels
        rows = con.execute(
            """
            SELECT p.event_id, p.symbol, p.horizon_s, p.ts_ms, p.predicted_z
            FROM predictions p
            LEFT JOIN labels_exec le
              ON le.event_id=p.event_id AND le.symbol=p.symbol AND le.horizon_s=p.horizon_s
            WHERE le.event_id IS NULL
            ORDER BY p.ts_ms ASC
            LIMIT 20000
            """
        ).fetchall()

        n_ok = 0
        n_skip = 0

        for eid, sym, horizon_s, ts_ms, predicted_z in rows:
            eid = int(eid)
            sym = str(sym)
            horizon_s = int(horizon_s)
            ts_ms = int(ts_ms)
            try:
                pred = float(predicted_z)
            except Exception:
                pred = 0.0

            side = 1 if pred >= 0 else -1

            entry = _get_px_at_or_before(con, sym, ts_ms)
            exit_ = _get_px_at_or_before(con, sym, ts_ms + horizon_s * 1000)

            if not entry or not exit_:
                n_skip += 1
                continue

            px_in, ts_in = entry
            px_out, ts_out = exit_

            if px_in <= 0 or px_out <= 0:
                n_skip += 1
                continue

            # This is a synthetic execution label based on market data, not a real
            # fill-derived attribution. The fill override job can replace it later.
            # gross return in trade direction
            gross_ret = (px_out / px_in - 1.0) * float(side)

            meta = _get_quote_meta_at_or_before(con, sym, ts_ms)
            bid = meta.get("bid")
            ask = meta.get("ask")
            spr = meta.get("spread")

            costs = estimate_cost_bps(px=px_in, bid=bid, ask=ask, side=side)
            net_ret = apply_cost_to_return(gross_ret, costs["total_cost_bps"], side=side)

            extra = {
                "entry_ts_px": ts_in,
                "exit_ts_px": ts_out,
                "quote_meta": meta,
            }

            con.execute(
                """
                INSERT OR REPLACE INTO labels_exec(
                  event_id, symbol, horizon_s, ts_ms,
                  side, gross_ret, net_ret,
                  gross_z, net_z,
                  mid_in, mid_out, spread_in,
                  fees_bps, slippage_bps, spread_bps, total_cost_bps,
                  extra_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    eid, sym, horizon_s, ts_ms,
                    int(side), float(gross_ret), float(net_ret),
                    None, None,  # z computed in Phase 5D
                    float(px_in), float(px_out),
                    (float(spr) if spr is not None else None),
                    float(costs["fees_bps"]),
                    float(costs["slippage_bps"]),
                    float(costs["spread_bps"]),
                    float(costs["total_cost_bps"]),
                    json.dumps(extra),
                ),
            )
            n_ok += 1

        con.commit()
        print(f"[labels_exec] wrote={n_ok} skipped={n_skip} total={len(rows)}")

    finally:
        con.close()


if __name__ == "__main__":
    main()
