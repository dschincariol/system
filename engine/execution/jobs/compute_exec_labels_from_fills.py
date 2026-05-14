"""
FILE: compute_exec_labels_from_fills.py

Job entrypoint or scheduled task for `compute_exec_labels_from_fills`.
"""

from ops.compute_exec_labels_from_fills import main

if __name__ == "__main__":
    raise SystemExit(main())
