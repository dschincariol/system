# Runtime State Layout

Local runtime state belongs under the ignored repository-local `var/` tree.
Source, docs, migrations, tests, and deploy assets stay outside this tree.

## Local Layout

| Path | Contents |
| --- | --- |
| `var/log/` | Runtime, ingestion, operator, soak, and validation logs. |
| `var/db/` | Local SQLite compatibility files such as `trading.db`, liveness DBs, and Optuna studies. |
| `var/db/async_price_writer_spool.sqlite` | Local async price-writer SQLite WAL spool when `DB_PATH`/`TS_DATA_ROOT` points into the repo-local data tree. Production deployments keep the same file under the configured runtime data root unless `ASYNC_PRICE_WRITER_SPOOL_PATH` overrides it. |
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

## Local Migration

To clean an existing checkout, move ignored runtime outputs into `var/`:

```bash
python tools/migrate_runtime_state.py
```

The migration is conservative: it refuses to move tracked files and will not
overwrite an existing destination.
