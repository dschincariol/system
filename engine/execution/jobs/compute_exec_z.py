"""
FILE: compute_exec_z.py

Job entrypoint or scheduled task for `compute_exec_z`.
"""

from ops.compute_exec_z import main

if __name__ == "__main__":
    raise SystemExit(main())
