"""
FILE: csv_feed.py

Price-series utility module for `csv_feed`.
"""

# dev_core/prices/csv_feed.py
import csv
from pathlib import Path
from typing import Dict, List

def load_prices(csv_path: Path) -> Dict[str, List[dict]]:
    """
    CSV format:
    ts_ms,symbol,price
    """
    out: Dict[str, List[dict]] = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sym = row["symbol"]
            out.setdefault(sym, []).append({
                "ts_ms": int(row["ts_ms"]),
                "price": float(row["price"]),
            })
    return out
