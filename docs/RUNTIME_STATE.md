# Runtime State Layout

Local runtime state belongs under the ignored repository-local `var/` tree.
Source, docs, migrations, tests, and deploy assets stay outside this tree.

## Local Layout

| Path | Contents |
| --- | --- |
| `var/log/` | Runtime, ingestion, operator, soak, and validation logs. |
| `var/db/` | Local SQLite compatibility files such as `trading.db`, liveness DBs, and Optuna studies. |
| `var/db/async_price_writer_spool.sqlite` | Local async price-writer SQLite WAL spool when `DB_PATH`/`TS_DATA_ROOT` points into the repo-local data tree. Production deployments keep the same file under the configured runtime data root unless `ASYNC_PRICE_WRITER_SPOOL_PATH` overrides it. Spool rows carry a stable shard id so `ASYNC_PRICE_WRITER_WORKERS` can replay each symbol/event-key shard in order. Failed downstream flushes leave rows in this spool for retry and startup replay; enqueue saturation is surfaced as producer-visible backpressure with rejected-row and oldest-age metrics. `ASYNC_PRICE_WRITER_SPOOL_SYNCHRONOUS` accepts `FULL`, `NORMAL`, and `EXTRA`; the default `NORMAL` keeps WAL commits fast and is safe against process crashes for re-fetchable market data, but a hard OS/power loss can lose the most recent spooled transaction. Use `FULL` on power-unreliable hosts to fsync every spool commit at a write-throughput cost. This market-data spool is never used for order, ledger, risk, capital, or audit writes. |
| `var/tmp/` | Temporary operator state, generated patches, scratch files, and transient outputs. |
| `var/artifacts/` | Local artifact-store objects, model caches, retraining datasets, preflight evidence, and generated reports. |
| `var/audit/` | Local audit-run outputs that previously landed in `.run-audit/`. |

## Legacy Inputs Still Supported

Explicit configuration always wins. Existing deployments that set
`TRADING_LOGS`, `LOG_DIR`, `TRADING_DATA`, `DATA_DIR`, `DB_PATH`,
`TS_ARTIFACTS_ROOT`, `NLP_MODEL_CACHE_DIR`, or `RL_PORTFOLIO_MODEL_ROOT`
continue to use those paths.

Production and supervised runtimes still require explicit absolute storage
roots. The repo-local `var/` defaults are for non-strict local development and
validation only.
Launcher-loaded profiles may use project-relative or `${PWD}`-anchored values
for `DB_PATH`, `TRADING_DATA`, `TRADING_LOGS`, and `SQLITE_LIVENESS_DB_PATH`;
the launchers resolve them to absolute paths before strict runtime validation.

## Safe Runtime Liveness

`start_system.py` keeps stale ingestion cleanup ownership-scoped. A process
found only by command line is eligible for termination only when its parent
chain or `ENGINE_RUNTIME_OWNER_PID` / `TRADING_RUNTIME_OWNER_PID` ties it to
the current runtime. Recorded pid files and stale liveness rows still allow
genuine orphaned ingestion processes to be reaped, while fresh rows owned by a
different live runtime are preserved.

Safe runtimes with no configured market-data feed do not exit when
`WARMUP_TIMEOUT_S` expires. The lifecycle monitor transitions from
`WARMING_UP` to `DEGRADED` with
`warmup_timeout_awaiting_first_price_tick`; the dashboard remains bound and
serving, while execution gates continue to block because the runtime is not
`LIVE`.

The startup-health thread and operator supervision use the same safe-mode
contract. In `ENGINE_MODE=EXECUTION_MODE=OPERATOR_MODE=safe`, with live
execution disabled and no live broker/feed enabled, a missing first price tick
is recorded as `safe_mode_feedless_degraded_serving` instead of requesting
dashboard shutdown. The operator treats a reachable `WARMING_UP` or `DEGRADED`
dashboard with no first tick as supervision-ready for the current safe startup
attempt, so the attempt is not counted as a pre-healthy crash loop. Structural
startup gate failures still fail closed, and live/shadow modes still stop on
late startup-health validation failure.

The legacy ignored paths remain ignored so older local runs do not become git
noise:

- `logs/`
- `tmp/`
- `.run-audit/`
- root `*.db`, `*.sqlite`, and `*.sqlite3`
- `data/runtime/`
- `data/artifacts/`
- `data/operator/`
- `data/retraining/`
- `data/*.db`, `data/*.sqlite`, and local `data/.data_source_master_key`
- `artifacts/`
- `models/`

## Source-Control Hygiene Guard

The repo permits local runtime and dependency outputs to remain on disk, but
they must stay out of the git index. The enforced check is:

```bash
python tools/check_repo_artifact_hygiene.py --report
```

That guard runs in `python tools/validate_repo.py` and in the GitHub
`Validate` workflow before dependency installation. It fails on tracked
virtualenvs (`.venv/`, `venv/`, `env/`), `node_modules/`, Python caches,
repo-local `var/` state, runtime DB/log/temp files, local secret paths, and
local `.env*` files. The only tracked env files allowed by the guard are
checked-in `*.env.example` templates.

The ops CI lane also carries self-tests for this contract. `tests/ops`
includes a planted-offender matrix for `tools/check_repo_artifact_hygiene.py`
and a discovery meta-check that verifies the `Validate` workflow runs pytest by
directory (`tests/ops`) and shell tests through `find tests/ops -name '*.sh'`.
New `tests/ops/test_*.py` and `tests/ops/test_*.sh` files are therefore picked
up by the same gate instead of requiring a workflow file list edit.

## Local Migration

To clean an existing checkout, move ignored runtime outputs into `var/`:

```bash
python tools/migrate_runtime_state.py
```

The migration is conservative: it refuses to move tracked files and will not
overwrite an existing destination.
