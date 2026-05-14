"""
FILE: kill_health_monitor.py

Job entrypoint or scheduled task for `kill_health_monitor`.
"""

from engine.strategy.kill_health_monitor import main

if __name__ == "__main__":
    raise SystemExit(main())
