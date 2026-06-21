from __future__ import annotations

import os
from pathlib import Path

from tests.conftest import TEST_TMP_ROOT


def test_pytest_tmp_path_uses_repo_disk_backed_root(tmp_path: Path) -> None:
    expected = TEST_TMP_ROOT.resolve()
    resolved_tmp = tmp_path.resolve()

    assert os.environ["TMPDIR"] == str(expected)
    assert os.environ["PYTEST_DEBUG_TEMPROOT"] == str(expected)
    assert expected in resolved_tmp.parents
