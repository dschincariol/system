# Coverage Gate

The repository enforces branch-aware Python coverage for the money-path package
roots that can affect live trading behavior:

- `engine/`
- `services/`
- `routes/`
- `ops/`

The gate is configured in `pyproject.toml` under
`tool.coverage.*` and `tool.trading_system.coverage_gate`. Coverage output is
written to `artifacts/coverage/coverage.xml` and
`artifacts/coverage/coverage.json`; those files are build artifacts and are not
version-controlled.

## Local Command

Run the same coverage gate that CI runs:

```bash
python tools/coverage_gate.py run
```

To debug a focused subset while preserving the same coverage settings:

```bash
python tools/coverage_gate.py run -- tests/test_storage_contracts.py -q
```

The command runs `pytest` with `pytest-cov`, enables branch coverage, prints a
terminal missing-line report, writes XML/JSON reports, prints package-level
coverage for the four money-path roots, and fails when total line+branch
coverage drops below the configured threshold. It also inherits the canonical
`pytest-timeout` configuration from `pyproject.toml`, including the 120 second
per-test default timeout.

## Initial Threshold

The initial gate is `52.0%`, based on a full local baseline on 2026-06-21:

- Command:
  `python -m pytest tests/ -q --tb=short --disable-warnings --cov=engine --cov=services --cov=routes --cov=ops --cov-branch --cov-report=term --cov-report=json:/tmp/trading-prelim-coverage.json --cov-fail-under=0`
- Result: `2069 passed, 101 skipped`
- Measured total line+branch coverage: `52.94%`
- Root-level baseline: `engine 53.22%`, `services 66.73%`,
  `routes 27.69%`, `ops 17.06%`

The threshold is intentionally just below the measured baseline to avoid
rounding noise while still blocking real regressions.

## Ratchet Process

Coverage owners raise the threshold after coverage improvements land and remain
stable on the default branch. The normal ratchet is one percentage point at a
time in `pyproject.toml`.

Do not lower the threshold or add omit rules to pass CI unless the pull request
also explains the production-risk tradeoff and has maintainer approval.
Money-path runtime modules must stay visible in the report even when their
coverage is low.

Acceptable exclusions are limited to non-runtime constructs such as
`TYPE_CHECKING` branches. Tests, generated reports, docs, and frontend assets
are outside the measured source roots rather than hidden with broad omit rules.

## CI Enforcement

GitHub Actions runs the `coverage` job on every push and pull request. The job
installs dev dependencies, runs `python tools/coverage_gate.py run`, emits the
terminal summary, uploads XML/JSON coverage artifacts, and fails the workflow on
test failure or threshold breach. Branch protection should require this status
before merging.
