# ADR 0003: Docstring Standard

- Status: Accepted
- Date: 2026-04-12

## Context

The repository already signals a preference for NumPy-style docstrings compatible with Sphinx and Napoleon, but the codebase still contains mixed module headers, brief inline descriptions, and placeholder file-level docstrings. Contributors need a shared standard that improves touched code without forcing large style-only rewrites.

## Decision

The repository will use NumPy-style docstrings for public, operator-facing, and cross-module Python APIs.

- The detailed convention lives in `docs/DOCSTRING_STYLE.md`.
- Newly added or materially edited public modules, classes, and functions should follow that guide.
- Private helpers can remain lightweight unless their behavior is subtle, reused, or safety-sensitive.
- Module docstrings should describe the module's role rather than repeating the filename.

## Consequences

- Docstrings become consistent enough for future Sphinx or Napoleon-based rendering without requiring a heavy docs build now.
- Contributors improve documentation where they already work instead of doing large repository-wide rewrites.
- Safety-sensitive loaders, handlers, and control-plane functions get clearer contracts for future maintainers.
