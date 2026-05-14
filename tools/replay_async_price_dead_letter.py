from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.runtime.storage_pg_prices import get_price_storage


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay async price-writer dead-letter batches into Postgres/Timescale.")
    parser.add_argument("path", type=Path, help="Path to async_price_writer_dead_letter.jsonl")
    parser.add_argument("--limit", type=int, default=0, help="Maximum dead-letter records to replay")
    args = parser.parse_args()

    storage = get_price_storage()
    if not bool(storage.enabled):
        raise SystemExit("Timescale/Postgres price storage is not enabled.")

    replayed = 0
    with args.path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not str(line or "").strip():
                continue
            payload = json.loads(line)
            for batch in list(payload.get("batches") or []):
                storage.write_batch(
                    prices=tuple(batch.get("prices") or ()),
                    quotes=tuple(batch.get("quotes") or ()),
                    raw=tuple(batch.get("raw") or ()),
                )
                replayed += 1
                if int(args.limit or 0) > 0 and replayed >= int(args.limit):
                    print(json.dumps({"ok": True, "replayed_batches": int(replayed)}, indent=2))
                    return 0
    print(json.dumps({"ok": True, "replayed_batches": int(replayed)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
