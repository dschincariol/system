"""
FILE: calibrate_price_confidence.py

Job entrypoint or scheduled task for `calibrate_price_confidence`.
"""

from ops.calibrate_price_confidence import main

if __name__ == "__main__":
    raise SystemExit(main())
