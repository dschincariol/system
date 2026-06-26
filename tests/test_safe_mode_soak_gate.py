import json

from tools import safe_mode_soak


def _write_ndjson(path, records):
    path.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )


def test_safe_mode_soak_gate_accepts_clean_evidence(tmp_path) -> None:
    evidence = tmp_path / "clean.ndjson"
    _write_ndjson(
        evidence,
        [
            {
                "sample": 1,
                "ok": {"operator_status": True, "health": True, "system_state": True, "jobs": True},
                "log_matches": [],
                "process": {"pid": 123, "rss_mb": 100.0},
            },
            {
                "sample": 2,
                "ok": {"operator_status": True, "health": True, "system_state": True, "jobs": True},
                "log_matches": [],
                "process": {"pid": 123, "rss_mb": 120.0},
            },
        ],
    )

    exit_code, summary = safe_mode_soak.evaluate_soak_evidence_file(str(evidence))

    assert exit_code == safe_mode_soak.EXIT_OK
    assert summary["status"] == "GO"
    assert summary["error_rate"] == 0.0
    assert summary["log_match_count"] == 0


def test_safe_mode_soak_gate_rejects_endpoint_error_and_fail_pattern(tmp_path) -> None:
    evidence = tmp_path / "bad.ndjson"
    _write_ndjson(
        evidence,
        [
            {
                "sample": 1,
                "ok": {"operator_status": True, "health": True, "system_state": True, "jobs": True},
                "log_matches": [],
                "process": {"pid": 123, "rss_mb": 100.0},
            },
            {
                "sample": 2,
                "ok": {"operator_status": True, "health": False, "system_state": True, "jobs": True},
                "log_matches": ["Traceback while polling health"],
                "process": {"pid": 123, "rss_mb": 101.0},
            },
        ],
    )

    exit_code, summary = safe_mode_soak.evaluate_soak_evidence_file(str(evidence))

    assert exit_code == safe_mode_soak.EXIT_NO_GO
    assert summary["status"] == "NO-GO"
    assert summary["error_samples"] == 1
    assert summary["log_match_count"] == 1
    assert {reason["reason"] for reason in summary["reasons"]} == {
        "endpoint_error_rate_exceeded",
        "runtime_log_fail_patterns",
    }


def test_safe_mode_soak_gate_rejects_rss_growth(tmp_path) -> None:
    evidence = tmp_path / "rss-growth.ndjson"
    _write_ndjson(
        evidence,
        [
            {
                "sample": 1,
                "ok": {"operator_status": True, "health": True, "system_state": True, "jobs": True},
                "log_matches": [],
                "process": {"pid": 123, "rss_mb": 100.0},
            },
            {
                "sample": 2,
                "ok": {"operator_status": True, "health": True, "system_state": True, "jobs": True},
                "log_matches": [],
                "process": {"pid": 123, "rss_mb": 350.5},
            },
        ],
    )

    exit_code, summary = safe_mode_soak.evaluate_soak_evidence_file(
        str(evidence),
        max_rss_growth_mb=200.0,
    )

    assert exit_code == safe_mode_soak.EXIT_NO_GO
    assert summary["status"] == "NO-GO"
    assert summary["rss_growth_mb"] == 250.5
    assert [reason["reason"] for reason in summary["reasons"]] == ["rss_growth_exceeded"]
