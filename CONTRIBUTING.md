# Contributing

This repository expects documentation to move with the code. If a change alters a contract, boundary, or operating rule, update the relevant docs in the same change.

## Documentation Structure

- `README.md`
  Canonical repository entrypoint and top-level runtime map.
- `docs/DOCUMENTATION_INDEX.md`
  Canonical map of repository docs and their status.
- `docs/MAINTAINER_INDEX.md`
  Fast engineer read path into high-risk surfaces.
- `docs/REFERENCE_CONFIGURATION_GLOSSARY.md`
  Canonical configuration, environment-variable, and secret-management reference.
- `docs/REFERENCE_DATA_SOURCE_CONTROL_PLANE.md`
  Canonical contract for the data-source UI, routes, storage, and lifecycle behavior.
- Subsystem READMEs under `engine/`, plus `boot/README.md`, `services/README.md`, `ui/README.md`, `ops/README.md`, and `deploy/README.md`
  Local ownership and boundary docs for the area being changed.
- `docs/DOCSTRING_STYLE.md`
  NumPy-style docstring standard for Python modules, classes, and functions.
- `docs/openapi/openapi.yaml`
  Incremental OpenAPI source of truth for the HTTP surface.
- `docs/adr/`
  Architecture decision record collection.
- `CHANGELOG.md`
  Forward-looking notable change log from the documented baseline onward.
- `docs/handoff/`, planning docs, and redesign notes
  Supplementary context only. Do not treat them as canonical runtime truth.

## Documentation Update Expectations

Use the smallest set of docs that keeps the changed contract current.

| If your change affects... | Update at least... |
| --- | --- |
| Runtime behavior, ownership, or control flow in one subsystem | The relevant subsystem README and any cross-cutting reference it depends on |
| Operator-facing behavior, procedures, or UI contracts | The relevant subsystem README plus the affected reference doc under `docs/` |
| Environment variables, defaults, secret handling, or configuration authority | `.env.example`, `docs/REFERENCE_CONFIGURATION_GLOSSARY.md`, and the relevant subsystem README |
| Data-source routes, payloads, lifecycle, or credential handling | `docs/REFERENCE_DATA_SOURCE_CONTROL_PLANE.md` and the affected local README |
| HTTP routes, request or response shapes, auth requirements, status codes, or query parameters | `docs/openapi/openapi.yaml` and the affected API README |
| Cross-cutting architectural direction, ownership boundaries, or long-lived standards | An ADR in `docs/adr/` |
| A notable operator, API, configuration, governance, or documentation baseline change | `CHANGELOG.md` |

If a change does not need a documentation update, state why in the change summary instead of silently skipping it.

## Docstring Standard

Python code in this repository uses NumPy-style docstrings for public and operator-relevant APIs.

- Follow `docs/DOCSTRING_STYLE.md` when adding or revising docstrings.
- Prioritize touched public modules, classes, and functions over large style-only rewrites.
- Keep private helpers lightweight unless their behavior is reused, subtle, or safety-sensitive.
- Replace placeholder module docstrings such as `FILE: foo.py` with real purpose summaries when touching those modules.

## When To Add Or Update ADRs

Add a new ADR when a change introduces or formalizes a long-lived decision such as:

- a control-plane boundary or ownership rule
- a storage or configuration source of truth
- an API specification standard or contract format
- a cross-cutting documentation or governance rule
- an execution, runtime, or operator-safety architecture rule that other changes will depend on

Update an existing ADR when:

- its status changes to superseded or deprecated
- implementation materially diverges from the recorded decision
- follow-on work narrows or clarifies the accepted decision without creating a distinct new one

Do not create ADRs for one-off bug fixes, local refactors, or temporary experiments.

## When To Update OpenAPI

Update `docs/openapi/openapi.yaml` when a change adds or alters:

- an `/api/*` path
- request body fields
- query parameters
- response fields or status codes
- auth requirements
- route ownership that changes how clients are expected to use the endpoint

If the touched endpoint is not represented in the spec yet, add that path as part of the same change instead of leaving the gap for later.

## When To Update The Configuration Glossary

Update `docs/REFERENCE_CONFIGURATION_GLOSSARY.md` when a change adds or alters:

- environment variables
- configuration defaults that matter operationally
- secret ownership or bootstrap rules
- configuration source-of-truth boundaries
- runtime-set environment variables that callers now depend on

If the change also alters operator setup, update `.env.example` in the same change.

## Validation Expectations

- Run `npm run test:py` or `python -m pytest tests/ -v --tb=short` for the canonical Python test suite. Avoid the stdlib discovery runner; pytest collects the repository's `unittest.TestCase` tests.
- Run `python tools/validate_repo.py` before merging. It runs `python tools/validate_docs.py` as its `docs` sub-validator and runs pytest collection before pytest execution, so documentation and Python test gates also run in CI.
- Run `python tools/check_repo_artifact_hygiene.py --report` when broad local-output or dependency directories are present. The same guard runs in CI and fails if generated caches, virtualenvs, `node_modules/`, repo-local `var/` state, local `.env*`, or secret paths are tracked.
- Local and CI pytest runs inherit the `pyproject.toml` `pytest-timeout` policy. The default per-test timeout is 120 seconds, `timeout_method` is `thread`, and `pytest-timeout>=2.4` is a required plugin. For an intentionally slow test, add `@pytest.mark.timeout(<seconds>)` directly on that test and include a nearby comment explaining the bound. Prefer a larger explicit bound to `@pytest.mark.timeout(0)`, and never disable timeouts for a whole marker class or CI lane.
- For doc-only changes, run at minimum `python tools/validate_docs.py`.
- Keep local Markdown links valid.
- Keep `docs/adr/` numbering sequential and update `docs/adr/README.md` when adding a new ADR.
- Do not backfill speculative historical changelog or ADR entries. Record the decision or change from the point where it becomes grounded.

In addition to link, ADR-numbering, required-file, OpenAPI, and changelog checks, `tools/validate_docs.py` enforces three documentation-governance gates (see [docs/adr/0006-documentation-governance-gates.md](docs/adr/0006-documentation-governance-gates.md)):

- **Subsystem-README coverage** — every `engine/<subsystem>/` directory must have a `README.md` and a link row in `docs/DOCUMENTATION_INDEX.md`. Add both when you add a subsystem.
- **Environment-variable coverage** — every env var read in code must appear in `.env.example`, `docs/REFERENCE_CONFIGURATION_GLOSSARY.md`, or `docs/config_env_allowlist.txt`. Document new variables in the first two; the allowlist only freezes the legacy backlog and should shrink, never grow.
- **Staleness sentinels** — the map/index docs (`CLAUDE.md`, `MAINTAINER_INDEX.md`, `README_DEVELOPER_MAP.md`, `README_ARCHITECTURE.md`, `README_FUNCTION_MAP.md`, `Database_Schema.md`) must each carry a `Last verified against code` line.
