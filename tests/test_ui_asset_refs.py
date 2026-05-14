from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tools.check_local_asset_refs import (
    find_local_asset_reference_issues,
    iter_local_asset_refs,
    resolve_local_asset_ref,
)


class UiAssetReferenceTests(unittest.TestCase):
    def test_resolve_local_asset_ref_strips_query_string(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.assertEqual(
                resolve_local_asset_ref("/ui/copilot.js?v=4", "ui/dashboard.html", root=root),
                "ui/copilot.js",
            )

    def test_resolve_local_asset_ref_handles_relative_imports(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.assertEqual(
                resolve_local_asset_ref("./tooltip.js", "ui/dashboard.js", root=root),
                "ui/tooltip.js",
            )
            self.assertEqual(
                resolve_local_asset_ref("../vendor/chart.js", "ui/panels/metrics.js", root=root),
                "ui/vendor/chart.js",
            )

    def test_resolve_local_asset_ref_ignores_node_builtin_imports(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self.assertIsNone(resolve_local_asset_ref("node:test", "tests/test_ui.mjs", root=root))

    def test_iter_local_asset_refs_ignores_external_routes(self) -> None:
        html = """
        <link rel="stylesheet" href="/ui/base.css">
        <script type="module" src="https://example.com/app.js"></script>
        <script type="module" src="/api/copilot/ask"></script>
        """
        refs = iter_local_asset_refs("ui/dashboard.html", html)
        self.assertEqual(
            refs,
            [
                ("/ui/base.css", 2, "html"),
                ("https://example.com/app.js", 3, "html"),
                ("/api/copilot/ask", 4, "html"),
            ],
        )

    def test_find_local_asset_reference_issues_flags_untracked_and_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "ui").mkdir(parents=True, exist_ok=True)
            (root / "ui" / "dashboard.html").write_text(
                '<link rel="stylesheet" href="/ui/base.css">\n'
                '<script type="module" src="/ui/copilot.js?v=4"></script>\n',
                encoding="utf-8",
            )
            (root / "ui" / "dashboard.js").write_text(
                'import { tooltip } from "./tooltip.js";\n'
                'import "./missing.js";\n',
                encoding="utf-8",
            )
            (root / "ui" / "base.css").write_text(":root {}\n", encoding="utf-8")
            (root / "ui" / "copilot.js").write_text('console.log("copilot");\n', encoding="utf-8")
            (root / "ui" / "tooltip.js").write_text('export const tooltip = true;\n', encoding="utf-8")

            tracked = {
                "ui/dashboard.html",
                "ui/dashboard.js",
                "ui/base.css",
            }
            issues = find_local_asset_reference_issues(root=root, tracked_paths=tracked)

        issue_rows = sorted(
            (issue.source_path, issue.line, issue.reason, issue.resolved_path)
            for issue in issues
        )
        self.assertEqual(
            issue_rows,
            [
                ("ui/dashboard.html", 2, "untracked", "ui/copilot.js"),
                ("ui/dashboard.js", 1, "untracked", "ui/tooltip.js"),
                ("ui/dashboard.js", 2, "missing", "ui/missing.js"),
            ],
        )

    def test_current_tracked_ui_assets_do_not_reference_untracked_files(self) -> None:
        issues = find_local_asset_reference_issues()
        issue_rows = [
            f"{issue.source_path}:{issue.line}: {issue.reason}:{issue.kind}: "
            f"{issue.raw_ref} -> {issue.resolved_path}"
            for issue in issues
        ]
        self.assertEqual(issue_rows, [])


if __name__ == "__main__":
    unittest.main()
