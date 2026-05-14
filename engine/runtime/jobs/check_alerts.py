"""
FILE: check_alerts.py

Job entrypoint or scheduled task for `check_alerts`.
"""

from ops.check_alerts import main

if __name__ == "__main__":
    raise SystemExit(main())
