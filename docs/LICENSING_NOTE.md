# Licensing Note

As of 2026-04-12, this repository does not contain a repo-wide `LICENSE` file.

This note is grounded in the current repo state:

- there is no root `LICENSE`, `COPYING`, or equivalent file
- `package.json` marks the package as `"private": true`
- `package.json` does not declare a repo license field
- the repository documentation does not declare external reuse terms

## Practical Consequence

Without an explicit repository license, default copyright rules apply. That blocks safe external reuse, redistribution, and derivative work assumptions for the repository as a whole.

This does not change the licensing notices carried by third-party vendored assets such as the files under `ui/vendor/`. Those upstream notices apply to those upstream components only, not to the repository as a whole.

## What To Do If A License Is Chosen Later

If the maintainers decide to license the repository, add a root `LICENSE` file and update:

- `README.md`
- `CONTRIBUTING.md`
- `CHANGELOG.md`
- this note, or remove this note if it is no longer needed
