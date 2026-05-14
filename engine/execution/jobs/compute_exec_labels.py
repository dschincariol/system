"""
FILE: compute_exec_labels.py

Job entrypoint or scheduled task for `compute_exec_labels`.
"""

from ops.compute_exec_labels import main

if __name__ == "__main__":
    raise SystemExit(main())
