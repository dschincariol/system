"""
FILE: check_labels.py

Job entrypoint or scheduled task for `check_labels`.
"""

from ops.check_labels import main

if __name__ == "__main__":
    raise SystemExit(main())
