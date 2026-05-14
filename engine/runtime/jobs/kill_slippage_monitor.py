"""
FILE: kill_slippage_monitor.py

Job entrypoint or scheduled task for `kill_slippage_monitor`.
"""

from engine.strategy.kill_slippage_monitor import main

if __name__ == "__main__":
    raise SystemExit(main())
