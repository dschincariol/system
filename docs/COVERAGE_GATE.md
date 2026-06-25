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

The custom gate enforces three hard checks from the same coverage JSON:

- total line+branch coverage must meet `minimum_percent`
- configured nested package floors must pass for `engine/risk`,
  `engine/execution`, and `engine/runtime`
- no new zero-covered Python module may appear under
  `engine/risk`, `engine/execution`, or `engine/runtime` outside the committed
  zero-covered burndown allowlist

## Local Command

Run the same coverage gate that CI runs:

```bash
python tools/coverage_gate.py run
```

To debug a focused subset while preserving the same coverage settings:

```bash
python tools/coverage_gate.py run -- tests/test_storage_contracts.py -q
```

The command runs `pytest` with `pytest-cov`, enables branch coverage, suppresses
the heavyweight terminal missing-line report, writes XML/JSON reports, prints
package-level coverage for the four money-path roots, prints PASS/FAIL rows for
the nested package floors, prints the zero-covered burndown count and
importer-priority list, and fails on any threshold or ratchet breach. It also
inherits the canonical `pytest-timeout` configuration from `pyproject.toml`,
including the 120 second per-test default timeout.

`run` also stamps `artifacts/coverage/coverage_gate_metadata.json` with the
coverage JSON hash, pytest args, gate config, pytest exit code, and source/test
freshness data. `python tools/coverage_gate.py check` now requires that stamp
and rejects reports from focused pytest runs, failed pytest runs, changed gate
config, changed report bytes, or source/test files newer than the report. This
prevents a partial local report from being reused as wider-repo acceptance
evidence.

To inspect an old report without treating it as release evidence:

```bash
python tools/coverage_gate.py check --allow-unstamped artifacts/coverage/coverage.json
```

That forensic mode still evaluates thresholds, but a merge or acceptance gate
must use the stamped full-run path above.

## Current Baseline

The global gate is `52.0%`, based on a full local baseline originally taken on
2026-06-21 and rechecked after the per-package ratchet was added:

- Command:
  `python tools/coverage_gate.py run`
- Latest measured total line+branch coverage: `54.39%`
- Root-level baseline: `engine 54.65%`, `services 71.00%`,
  `routes 27.69%`, `ops 17.53%`
- Nested hard floors, rounded down to avoid display-rounding false failures:
  `engine/risk 50.81%`, `engine/execution 58.92%`,
  `engine/runtime 58.49%`
- Zero-covered critical-root burndown: `13` allowlisted modules, with no new
  zero-covered modules allowed under `engine/risk`, `engine/execution`, or
  `engine/runtime`

The threshold is intentionally just below the measured baseline to avoid
rounding noise while still blocking real regressions.

If `check` reports `stale or partial coverage report`, do not lower floors or
expand allowlists. Regenerate with `python tools/coverage_gate.py run` and use
the fresh stamped result. If the stamped full run is genuinely below a floor,
document the measured values, affected packages, owner, remediation plan, and
temporary baseline approval in the release handoff before treating the gate as
baselined.

## Ratchet Process

Coverage owners raise thresholds after coverage improvements land and remain
stable on the default branch. The normal global ratchet is one percentage point
at a time in `pyproject.toml`; nested money-path floors should be raised to the
new measured value when tests improve `engine/risk`, `engine/execution`, or
`engine/runtime`.

Do not lower the global threshold, lower a nested floor, expand the
zero-covered allowlist, or add omit rules to pass CI unless the pull request
also explains the production-risk tradeoff and has maintainer approval.
Money-path runtime modules must stay visible in the report even when their
coverage is low. The zero-covered burndown is prioritized by production importer
count in the coverage-gate output so the highest-reach uncovered modules are
visible first.

Acceptable exclusions are limited to non-runtime constructs such as
`TYPE_CHECKING` branches. Tests, generated reports, docs, and frontend assets
are outside the measured source roots rather than hidden with broad omit rules.

## CI Enforcement

GitHub Actions runs the `coverage` job on every push and pull request. The job
installs dev dependencies, runs `python tools/coverage_gate.py run`, emits the
gate summary, uploads XML/JSON coverage artifacts, and fails the workflow on
test failure, global threshold breach, nested package floor breach, or a new
zero-covered critical module. Branch protection should require this status
before merging.
