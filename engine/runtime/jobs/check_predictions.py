"""
FILE: check_predictions.py

Job entrypoint or scheduled task for `check_predictions`.
"""

from ops.check_predictions import main

if __name__ == "__main__":
    raise SystemExit(main())
