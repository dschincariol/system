"""
FILE: import_graph_check.py

Repository maintenance script for `import_graph_check`.
"""

import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from tools.runtime_graph_check import run_canonical_validation


def main() -> int:
    return run_canonical_validation(mode="imports")


if __name__ == "__main__":
    raise SystemExit(main())