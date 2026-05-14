"""
FILE: portfolio_rebalance.py

Job entrypoint or scheduled task for `portfolio_rebalance`.
"""

from engine.strategy.portfolio_rebalance import main

if __name__ == "__main__":
    raise SystemExit(main())
