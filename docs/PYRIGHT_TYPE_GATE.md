# Pyright Type Gate

The repository has a CI-enforced pyright gate for trading money paths. It is
staged because the current codebase is not full-repo clean under pyright yet.

## Local Command

Install the locked dev tools first:

```bash
python -m pip install -r requirements-dev.txt
```

Run the gate from the repo root:

```bash
python tools/pyright_money_path_gate.py
```

The gate invokes `pyright --project pyrightconfig.json --outputjson` for a fixed
target list and compares the diagnostics to
`tools/pyright_money_path_baseline.json`.

If you intentionally fix existing diagnostics, ratchet the baseline in the same
change:

```bash
python tools/pyright_money_path_gate.py --update-baseline
```

Do not update the baseline to accept new errors. Fix the type problem instead.
Avoid broad `# type: ignore` use; prefer concrete annotations, narrower unions,
typed dictionaries, protocol boundaries, or runtime guards that match the
existing behavior.

## CI Behavior

The GitHub `Validate` workflow runs `python tools/validate_repo.py` in the step
named `Run canonical repository validation with pyright gate`. The canonical
validator runs `python tools/pyright_money_path_gate.py` after syntax validation
and before ruff and pytest. Any new pyright diagnostic, removed diagnostic
without a matching baseline ratchet, missing target file, or high-risk exclusion
in `pyrightconfig.json` fails the PR.

`requirements-dev.in` and `requirements-dev.lock.txt` pin `pyright==1.1.410`;
the gate uses that pyright version in CI. Local runs may set `PYRIGHT_BIN` if
they need to point at a specific executable.

## Current Baseline

The June 22, 2026 gate baseline records 88 existing pyright errors and 1 warning
across the first-slice target files. A full-repo `pyright --outputjson` pass was
not adopted as the initial gate because it did not complete quickly enough for a
practical PR signal on this workspace. The scoped money-path pass completes in
about 10 seconds locally.

The pyright config intentionally excludes top-level artifact directories such as
`data/**` and `logs/**`; it must not exclude high-risk code packages such as
`engine/data`. The gate checks `pyrightconfig.json` and fails if an exclusion
would hide any high-risk root.

## Money-Path Coverage

| Package | Current gate coverage | Full-package target | Justification |
| --- | --- | --- | --- |
| `engine/data` | Live price/provider polling files | 2026-09-30 | Historical, research, and alternative ingestion jobs need cleanup after live data files. |
| `engine/execution` | Broker routing, order apply, idempotency, kill switch, reconciliation, and ledger files | 2026-09-30 | Remaining analytics, training, and broker adapters are staged after live order flow. |
| `engine/risk` | Full package | Included now | Risk directly sizes capital. |
| `engine/runtime` | Live gates, preflight, read routers, Postgres storage, and Timescale client files | 2026-10-31 | Remaining startup, health, and operator support surfaces have legacy typing debt. |
| `engine/strategy` | Portfolio, portfolio risk gate, predictor, and champion manager files | 2026-10-31 | Remaining strategy modules include research-only model families and discovery surfaces. |

When expanding the gate, add paths to `TARGET_PATHS` in
`tools/pyright_money_path_gate.py`, run the gate, fix or baseline only the
pre-existing diagnostics, and update this document if package coverage or target
dates change.
