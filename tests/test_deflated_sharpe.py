import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.backtest.deflated_sharpe import deflated_sharpe_ratio


def test_deflated_sharpe_collapses_to_raw_when_single_trial():
    result = deflated_sharpe_ratio([1.25], n_trials=1)

    assert result.raw_sharpe == 1.25
    assert result.deflated_sharpe == 1.25
    assert result.expected_max_sharpe == 0.0


def test_many_trials_deflate_realized_best_sharpe():
    result = deflated_sharpe_ratio([0.2, 0.4, 0.8, 1.0, 1.2], realized_sharpe=1.2, n_trials=20)

    assert result.deflated_sharpe < result.raw_sharpe
    assert result.expected_max_sharpe > 0.0
    assert 0.0 <= result.p_value <= 1.0
