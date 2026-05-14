"""
FILE: run_dev.py

Top-level entrypoint or configuration module for `run_dev`.
"""

# run_dev.py
"""
WINDOWS-ONLY DEV MODE

Implements:
- Event ingestion
- Price ingestion
- Labeling (ground truth)
- Learning
- Prediction
- Alerting
- Walk-forward validation (STEP 7)
"""

from pathlib import Path
import os
import numpy as np

from engine.runtime.storage import init_db, put_event, put_price, connect
from engine.data.prices.csv_feed import load_prices
from engine.strategy.labeling import label_event
from engine.strategy.learning import learn_relevance_stats as train_stats_from_labels

from engine.strategy.predictor import expected_impact
from engine.runtime.alerts import emit_alert, init_alerts_db
from engine.strategy.validation import (
    init_validation_db,
    store_prediction,
    compute_validation_scores,
    get_validation_scores,
)

NEWS = [
    "Federal Reserve signals interest rates may stay higher for longer",
    "Bitcoin surges after ETF approval rumors",
    "Oil prices jump amid Middle East tensions",
    "Tech stocks fall after earnings warnings",
]

SYMBOLS = ["SPY", "BTC", "OIL"]
HORIZONS = [300, 3600]


def main() -> int:
    import torch
    from sentence_transformers import SentenceTransformer

    torch.set_num_threads(int(os.environ.get("TORCH_NUM_THREADS", "16")))
    torch.set_num_interop_threads(int(os.environ.get("TORCH_INTEROP_THREADS", "4")))

    # This script is a standalone toy/dev pipeline, not the supervised production
    # runtime. It is useful for local smoke tests and examples, not operational boot.
    # ---------------------------
    # Boot
    # ---------------------------

    dev = os.environ.get("EMBED_DEVICE", "").strip().lower()
    if not dev:
        dev = "cuda" if torch.cuda.is_available() else "cpu"
    model = SentenceTransformer("all-MiniLM-L6-v2", device=dev)

    init_db()
    init_alerts_db()
    init_validation_db()

    # ---------------------------
    # Load Prices
    # ---------------------------

    price_data = load_prices(Path("data/prices.csv"))
    for sym, series in price_data.items():
        for p in series:
            put_price(p["ts_ms"], sym, p["price"])

    # ---------------------------
    # Anchor Events
    # ---------------------------

    base_ts_ms = min(
        p["ts_ms"]
        for series in price_data.values()
        for p in series
    )

    event_rows = []
    for i, title in enumerate(NEWS):
        ts = base_ts_ms + (i + 1) * 1000
        eid = put_event(
            ts_ms=ts,
            source="dev",
            title=title,
            body="",
            url="",
            event_key=f"dev:{i}:{abs(hash(title)) % 10_000_000}",
        )
        event_rows.append((eid, ts, title))

    # ---------------------------
    # Embed + Store
    # ---------------------------

    embeddings = np.asarray(model.encode([t for _, _, t in event_rows]), dtype=np.float32)

    con = connect()
    try:
        cur = con.cursor()
        for (eid, _, _), vec in zip(event_rows, embeddings):
            cur.execute(
                """
                INSERT OR REPLACE INTO event_embeddings(event_id, dim, vec)
                VALUES (?, ?, ?)
                """,
                (eid, len(vec), vec.tobytes()),
            )
        con.commit()
    finally:
        con.close()

    # ---------------------------
    # Label events (ground truth)
    # ---------------------------

    for eid, ets, _ in event_rows:
        label_event(eid, ets, price_data)

    # ---------------------------
    # Train model
    # ---------------------------

    train_stats_from_labels()

    # ---------------------------
    # Predict → Store → Alert
    # ---------------------------

    print("\nPredictions + alerts:\n")

    for eid, _, title in event_rows:
        qvec = np.asarray(model.encode([title])[0], dtype=np.float32)

        print(f"EVENT: {title}")
        for sym in SYMBOLS:
            for h in HORIZONS:
                z, conf = expected_impact(qvec, sym, h)
                print(f"  {sym} h={h} predicted_z={z:+.3f} conf={conf:.2f}")

                store_prediction(eid, sym, h, z, conf)

                emit_alert(
                    event_title=title,
                    symbol=sym,
                    horizon_s=h,
                    expected_z=z,
                    confidence=conf,
                )

        print("-" * 60)

    # ---------------------------
    # Validate (compare predictions vs realized)
    # ---------------------------

    compute_validation_scores()

    print("\nValidation scores:")
    for sym, h, mae, rmse, n, ts in get_validation_scores():
        print(f"  {sym} h={h} MAE={mae:.3f} RMSE={rmse:.3f} n={n}")

    print("\nDEV RUN COMPLETE\n")
    return 0


if __name__ == "__main__":
    exit_code = main()
    raise SystemExit(exit_code)
