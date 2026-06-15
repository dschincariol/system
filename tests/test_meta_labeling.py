from __future__ import annotations

import numpy as np
import pytest

from engine.strategy import meta_labeling


def test_triple_barrier_profit_take_hit_first() -> None:
    outcome = meta_labeling.triple_barrier_outcome(
        [(1_000, 100.0), (2_000, 102.0), (3_000, 98.0)],
        side_sign=1,
        sigma=0.01,
        barrier_k=1.5,
    )

    assert outcome.outcome == "profit"
    assert outcome.label == 1
    assert outcome.exit_ts_ms == 2_000


def test_triple_barrier_stop_loss_hit_first() -> None:
    outcome = meta_labeling.triple_barrier_outcome(
        [(1_000, 100.0), (2_000, 98.0), (3_000, 104.0)],
        side_sign=1,
        sigma=0.01,
        barrier_k=1.5,
    )

    assert outcome.outcome == "loss"
    assert outcome.label == 0
    assert outcome.exit_ts_ms == 2_000


def test_triple_barrier_timeout_keeps_signed_outcome() -> None:
    outcome = meta_labeling.triple_barrier_outcome(
        [(1_000, 100.0), (2_000, 101.0), (3_000, 100.5)],
        side_sign=1,
        sigma=0.05,
        barrier_k=1.5,
    )

    assert outcome.outcome == "timeout_profit"
    assert outcome.label == 0
    assert outcome.timeout_sign == 1


def test_meta_label_multiplier_edges() -> None:
    assert meta_labeling.meta_label_multiplier(0.44, lower=0.45, upper=0.65) == 0.0
    assert meta_labeling.meta_label_multiplier(0.45, lower=0.45, upper=0.65) == 0.0
    assert meta_labeling.meta_label_multiplier(0.55, lower=0.45, upper=0.65) == pytest.approx(0.5)
    assert meta_labeling.meta_label_multiplier(0.65, lower=0.45, upper=0.65) == 1.0


def test_isotonic_calibration_does_not_worsen_brier_and_bins_are_ordered() -> None:
    raw = np.asarray([0.05, 0.15, 0.20, 0.35, 0.60, 0.75, 0.80, 0.90], dtype=float)
    labels = np.asarray([0, 0, 0, 1, 0, 1, 1, 1], dtype=int)

    calibrated = meta_labeling.calibrate_probabilities(raw, labels)
    assert calibrated["brier"] <= calibrated["raw_brier"] + 1.0e-12

    bins = meta_labeling.reliability_bins(calibrated["probabilities"], labels, bins=4)
    prob_means = [float(row["prob_mean"]) for row in bins]
    assert prob_means == sorted(prob_means)


def test_cpcv_calibration_uses_purged_folds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("META_LABEL_CPCV_N_SPLITS", "4")
    monkeypatch.setenv("META_LABEL_CPCV_N_TEST_SPLITS", "1")
    monkeypatch.setenv("META_LABEL_CPCV_EMBARGO_PCT", "0.0")
    X = np.asarray([[idx / 24.0, (idx % 3) / 3.0] for idx in range(24)], dtype=np.float32)
    y = np.asarray([idx % 2 for idx in range(24)], dtype=np.int8)
    rows = [
        {"ts_ms": 1_000 + idx * 1_000, "vertical_ts_ms": 1_500 + idx * 1_000}
        for idx in range(24)
    ]

    validation = meta_labeling._cpcv_calibration(X, y, rows)

    assert validation["method"] == "cpcv"
    assert validation["fold_count"] > 0
    assert validation["coverage"] == pytest.approx(1.0)
    assert validation["calibration"]["brier"] <= validation["calibration"]["raw_brier"] + 1.0e-12


def test_generate_triple_barrier_labels_propagates_commit_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FailingCommit:
        def commit(self) -> None:
            raise RuntimeError("commit failed")

    monkeypatch.setattr(meta_labeling, "ensure_schema", lambda con: None)
    monkeypatch.setattr(
        meta_labeling,
        "_candidate_rows",
        lambda con, **kwargs: [{"symbol": "SPY", "ts_ms": 1_700_000_000_000}],
    )
    monkeypatch.setattr(
        meta_labeling,
        "label_candidate",
        lambda con, candidate, **kwargs: {"ok": True},
    )

    with pytest.raises(RuntimeError, match="commit failed"):
        meta_labeling.generate_triple_barrier_labels(
            con=_FailingCommit(),
            now_ms=1_700_000_100_000,
            limit=1,
        )
