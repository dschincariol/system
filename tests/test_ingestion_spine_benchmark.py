from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_ingestion_spine_benchmark_skip_postgres_writes_json(tmp_path: Path) -> None:
    output = tmp_path / "ingestion_spine_benchmark.json"
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "ingestion_spine_benchmark.py"),
            "--skip-postgres",
            "--non-price-rows",
            "64",
            "--batch-size",
            "16",
            "--rows-per-table",
            "16",
            "--output",
            str(output),
        ],
        cwd=str(ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["ok"] is True
    benchmarks = list(payload.get("benchmarks") or [])
    assert [item["mode"] for item in benchmarks] == [
        "non_price_durable_spool",
        "price_row_copy_boundary",
        "price_shared_row_normalization",
    ]
    assert benchmarks[0]["rows"] == 64
    assert benchmarks[0]["dropped_rows"] == 0
    assert benchmarks[0]["pending_rows_after"] == 0
    assert benchmarks[1]["normalized_rows"] == benchmarks[1]["rows"]
    assert benchmarks[1]["row_copy_avoided_rows"] == benchmarks[1]["rows"]
    assert benchmarks[1]["row_copy_fallback_rows"] == 0
    assert benchmarks[1]["safe_float_calls_per_row"] > 0
    assert benchmarks[1]["safe_int_calls_per_row"] > 0
    assert benchmarks[2]["normalized_rows"] == benchmarks[2]["rows"]
    assert benchmarks[2]["safe_float_calls"] < benchmarks[2]["legacy_safe_float_calls"]
    assert benchmarks[2]["safe_int_calls"] < benchmarks[2]["legacy_safe_int_calls"]
    assert benchmarks[2]["datetime_conversions"] < benchmarks[2]["legacy_datetime_conversions"]
    assert benchmarks[2]["symbol_parses"] < benchmarks[2]["legacy_symbol_parses"]
    assert benchmarks[2]["safe_float_calls_saved"] > 0
    assert benchmarks[2]["safe_int_calls_saved"] > 0
