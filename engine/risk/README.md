# Risk Subsystem

The `engine/risk/` package owns the portfolio-risk engines that feed API reads, execution barriers, and operator diagnostics.

## Files

- [portfolio_risk_engine.py](portfolio_risk_engine.py)
  Additive exposure, drawdown, volatility, and budget checks that write current portfolio-risk state and snapshots.
- [monte_carlo_risk_engine.py](monte_carlo_risk_engine.py)
  Background Monte Carlo refresher that stores stressed portfolio-risk summaries in `risk_state`.

## API Surfaces

- `GET /api/risk/portfolio`
- `GET /api/risk/monte_carlo`
- `GET /api/execution/barrier`

The execution barrier can incorporate portfolio-risk blocks, so risk changes can affect whether the execution pipeline is currently allowed to run.

## Configuration Families

- `PORTFOLIO_RISK_*`
- `MC_*`

These variables are consumed directly by the risk engines and should be documented in `.env.example` and `docs/REFERENCE_CONFIGURATION_GLOSSARY.md` when their operator-facing meaning changes.
