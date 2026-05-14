"""
FILE: check_events.py

Job entrypoint or scheduled task for `check_events`.
"""

from ops.check_events import main

if __name__ == "__main__":
    raise SystemExit(main())
