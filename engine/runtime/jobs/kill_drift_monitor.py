"""
FILE: kill_drift_monitor.py

Job entrypoint or scheduled task for `kill_drift_monitor`.
"""

from engine.strategy.kill_drift_monitor import main

if __name__ == "__main__":
    raise SystemExit(main())
