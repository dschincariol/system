"""One-shot safe/sim price ingestion job."""

from __future__ import annotations

import json
import os

from engine.data.simulated_price_ingestion import run_simulated_price_ingestion_once
from engine.runtime.storage import init_db


def main() -> None:
    init_db()
    symbols = [
        part.strip().upper()
        for part in str(os.environ.get("SIMULATED_MARKET_DATA_SYMBOLS", "") or "").split(",")
        if part.strip()
    ]
    result = run_simulated_price_ingestion_once(symbols=symbols or None)
    print(json.dumps(result, separators=(",", ":"), sort_keys=True), flush=True)
    raise SystemExit(0 if bool(result.get("ok")) else 1)


if __name__ == "__main__":
    main()
