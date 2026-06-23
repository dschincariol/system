# Hardware Reconfiguration Validation Audit — optimized for Codex (OpenAI Codex CLI)

> Run from the repo root: `cd /home/david/gitsandbox/system/system`, start Codex,
> paste everything below the line. Keep approvals on; this audit is READ-ONLY by
> design — it inspects code and host state and emits a report, it does not change
> code, config, or schema. (You can also save the prompt body as `AGENTS.md` so
> Codex loads it automatically.) Twin of the Claude-flavored
> `HARDWARE_RECONFIG_VALIDATION_DEEP_DIVE_PROMPTS.md`.

---

ROLE: Act as a senior SRE / hardware-platform auditor for a LIVE algorithmic
trading system that was just reconfigured at the host/OS level (storage moved to
NVMe ZFS pools, CPU-first compute, container limits, LAN access mode). Goal:
decide, per dimension, whether the **new host configuration now satisfies and is
optimized for what the code actually requires** — GO / NO-GO / NEEDS-EVIDENCE —
and emit a per-issue report. Be conservative: unproven == not optimized.

TARGET: `/home/david/gitsandbox/system/system` (only codebase to assess). The
repo is dirty; do not revert or touch unrelated user changes.

HARD CONSTRAINTS (do not violate):
1. READ-ONLY. Do not edit, format, reinstall, migrate, or run git-mutating
   commands. The single deliverable is the report.
2. Do NOT launch the engine or any entry point that could place orders, connect a
   live broker/exchange, send alerts, or write the production DB.
3. Allowed commands are inspection-only: read files, `grep`, `pip list`,
   `python -m py_compile`, `--help`/`--version`, and read-only host probes you can
   run without privilege. If a command might have side effects, read the code
   instead.
4. Never print secret values (DASHBOARD_API_TOKEN, DATA_SOURCE_MASTER_KEY, broker
   keys). Report location + risk, masked.
5. This shell CANNOT sudo (NoNewPrivs). Anything needing root — `zpool status`,
   `zfs list`, `lsblk`, `smartctl`, `df -hT` on host mounts, `docker stats`,
   `docker inspect`, `systemctl`, reading container `PGDATA` — is an OPERATOR
   ACTION: print the exact command and ask the user to run it, then fold their
   output into the report. Do not silently skip the evidence; mark it
   NEEDS-EVIDENCE until provided.

MACHINE PROFILE (current, post-reconfiguration — verify each fact, do not assume):
- Host `bart`, Ubuntu 25.10 (kernel ~6.17, x86_64).
- CPU: AMD Ryzen AI Max+ 395 (Strix Halo) — 16C/32T. RAM ~123 GiB; confirm swap
  size (previously ~512 MiB → an OOM kills the trading process instead of
  swapping; flag if still tiny).
- GPU/NPU: Radeon 8060S iGPU + NPU, `gfx1151`. NO CUDA. ROCm acceleration is NOT
  usable today: the installed torch+rocm wheel ships no `gfx1151` kernels, so
  live code must resolve to CPU and must not assume the accelerator. Confirm
  `torch.cuda.is_available()` is False and no path hardcodes `cuda`.
- STORAGE — the central change to validate. Intended layout: 3 NVMe drives
  (2×2TB + 1×4TB) as independent ZFS pools, `PGDATA` on the 4TB pool, backups on
  a separate 2TB pool, with `TRADING_ALLOWED_STORAGE_PREFIXES` overridden for the
  new paths. PRIOR FINDING to re-verify, not trust: `PGDATA`/`pg_wal` were still
  on the root Docker volume and the live WAL archiver was failing. VERIFY actual
  current placement, `pg_stat_archiver` health, `pg_wal` growth, and that nothing
  high-volume (DB/WAL/Redis/MinIO/backups/spool) still lands on root `/`.
- NETWORK: LAN mode added — under `TRADING_NETWORK_MODE=lan` the dashboard `:8000`
  and operator `:4001` bind `0.0.0.0`, gated by `DASHBOARD_API_TOKEN`. Verify
  bind addresses and token enforcement.
- Docker resource limits, log retention, Postgres tuning, ingestion writer knobs,
  and dependency profiles may or may not be applied — that is part of what you
  verify.

OBJECTIVE — three phases, IN ORDER:

PHASE 1 — DERIVE THE CODE'S HARDWARE/OS REQUIREMENTS (from code, not memory).
Read the repo's features and functions and extract what the system needs/assumes
from hardware and OS. Build an explicit requirements inventory (cite file:line,
note default, and whether enforced in production code vs only tests/docs):
- Storage: allowed path prefixes (`TRADING_ALLOWED_STORAGE_PREFIXES`); expected
  homes for `PGDATA`, `pg_wal`, WAL archive, Redis, MinIO, backups, runtime spool
  / async-writer SQLite spool / dead-letter; free-space + growth assumptions;
  filesystem assumptions (ZFS recordsize/compression, fsync, `synchronous_commit`
  tiers).
- CPU/threads: BLAS/OMP/MKL/OpenBLAS/NumExpr/torch thread caps per role;
  supervised child-process count vs 32 threads (oversubscription).
- Accelerator: every torch/embed/NLP/FinBERT/TS device selection and every
  NVIDIA-only import (`pynvml`, `nvidia-ml-py`); whether each degrades to CPU on
  this AMD host.
- Memory: Postgres/Timescale tuning, Redis `maxmemory`, container limits,
  `/dev/shm`, model/data-loader footprint vs RAM+swap.
- Network: bind addresses, ports, LAN-mode token/auth requirements.
- Database/IO: Timescale chunk intervals, compression, WAL/checkpoint headroom,
  per-child connection pools.

PHASE 2 — AUDIT THE ACTUAL HOST CONFIG AGAINST PHASE 1. Gather real evidence
(drives, ZFS pools, mounts, free space, container limits, log sizes, effective
Postgres settings, `pg_stat_archiver`, `pg_wal` growth, resolved torch device,
thread env, bind addresses). Map each to its Phase 1 requirement and classify:
satisfied / partial / violated / unverifiable. For anything privileged, emit the
operator command (HARD CONSTRAINT 5).

PHASE 3 — VERIFY THE RECONFIGURATION OPTIMIZED THE CODE+HARDWARE FIT. Using the
prior baseline `HARDWARE_OPTIMIZATION_DEEP_DIVE_PROMPTS.md` (disk/retention,
CPU-first device, Docker resource isolation, Postgres tuning, scoring indexes,
ingestion writer tuning, live/offline split, dependency profiles) and the repo
root `INGESTION_PERF_RELIABILITY_AUDIT.md`, grade each prior finding: closed in
production / partial / regressed-or-new-risk. Explicitly answer: is data now on
the ZFS NVMe pools (not root Docker storage); is WAL archiving real and healthy;
is the runtime deterministic CPU-first; are services resource-bounded with
internally consistent Postgres/Redis memory; is LAN exposure authenticated and
minimal.

AUDIT STEPS / COVERAGE (cover each; cite file:line + host evidence):
1. Storage placement & ZFS NVMe pool layout (PGDATA on 4TB, backups on separate
   2TB, allowed-prefix override, dataset tuning).
2. WAL archiving health + storage-placement startup gate.
3. Disk pressure, log retention, backup accounting after the move.
4. CPU-first device resolution + AMD/ROCm `gfx1151` non-availability handling.
5. BLAS/OMP/torch thread caps vs 32 threads × supervised child count.
6. Docker CPU/memory/`shm` limits + host headroom (OS, IDE, tests, diagnostics).
7. Postgres/Timescale tuning vs container limits and the 123 GiB host (and swap).
8. Ingestion writer pools/queues/backpressure sizing for this host.
9. Timescale chunk-interval/compression on the new pools.
10. Live vs offline/research resource profiles.
11. Dependency profile (CPU/default install needs no NVIDIA-only packages on the
    hot path).
12. LAN network exposure, bind addresses, token/auth enforcement.

EVIDENCE SOURCES (read; widen as needed):
- `deploy/compose/docker-compose.stack.yml`,
  `deploy/compose/docker-compose.external-services.yml`,
  `deploy/compose/.env.example`, `deploy/env/trading.env.example`,
  `deploy/compose/README.md`, `deploy/README.md`
- `engine/runtime/prod_preflight.py`, `engine/runtime/staging_prod_preflight.py`,
  `engine/runtime/external_service_readiness.py`, `engine/runtime/health.py`,
  `engine/runtime/platform.py`, `engine/runtime/config.py`
- storage-placement gate + `TRADING_ALLOWED_STORAGE_PREFIXES` handling;
  `ops/backup/`, `ops/server/bootstrap.sh`, `ops/server/config/postgres.conf.tmpl`
- `engine/runtime/torch_threads.py`; device selection in
  `engine/data/jobs/process_events*.py`, `engine/data/finbert_sentiment.py`,
  `engine/nlp/encoder.py`, `engine/strategy/models/patchtst.py`,
  `engine/strategy/models/itransformer.py`,
  `engine/strategy/ts_foundation_encoder.py`
- `engine/runtime/timescale_client.py`, `engine/runtime/storage_pg_prices.py`,
  `engine/runtime/async_writer.py`, `engine/runtime/telemetry_append_buffer.py`,
  `engine/runtime/storage_pool.py`, `engine/cache/redis_pool.py`,
  `engine/runtime/ingestion_runtime.py`
- `requirements*.txt`, `pyproject.toml`
- `dashboard_server.py` + operator-service bind/auth; `docs/LAN_ACCESS.md`
- Docs: `docs/PRODUCTION_CHECKLIST.md`, `docs/OBSERVABILITY.md`,
  `docs/README_DATABASE_MAP.md`, `docs/REFERENCE_CONFIGURATION_GLOSSARY.md`, and
  the prior prompt/audit files in this folder and the repo root.

SEVERITY + VERDICT:
- P0 (Blocker): data-loss, live-trade-safety, or DB/ingestion-outage risk — e.g.
  PGDATA/WAL/backups still on root, WAL archiver dead, a hardware "optimization"
  that weakens a trading gate. Any open P0 ⇒ that dimension is NO-GO.
- P1 (Critical): silent failure, code↔hardware mismatch, or unrecoverable state in
  normal use. NO-GO unless mitigated.
- P2 Major / P3 Minor: fix soon / polish / improvement.
- Per-dimension verdict = GO only with zero P0 and every P1 mitigated; NO-GO if
  not; NEEDS-EVIDENCE if a required host probe is still operator-pending.

OUTPUT: print the report ONLY (no step narration, no code dumps). Lead with a
one-paragraph summary, then the dimension verdict table, then every finding as a
row. Use this template:

```text
# Hardware Reconfiguration Validation Audit — system
Auditor: <model>   Date: 2026-06-22   Confidence: High | Med | Low
Summary: <one paragraph: is code+hardware now optimized? top risks?>

## Dimension verdicts
| # | Dimension | Verdict | One-line note |
|---|-----------|:------:|---------------|
| 1 | Storage placement / ZFS pools     | GO/NO-GO/NEEDS-EVIDENCE |  |
| 2 | WAL archiving health + gate       | GO/NO-GO/NEEDS-EVIDENCE |  |
| 3 | Disk pressure / log retention     | GO/NO-GO/NEEDS-EVIDENCE |  |
| 4 | CPU-first device / gfx1151        | GO/NO-GO/NEEDS-EVIDENCE |  |
| 5 | Thread caps vs 32T × children     | GO/NO-GO/NEEDS-EVIDENCE |  |
| 6 | Docker limits / host headroom     | GO/NO-GO/NEEDS-EVIDENCE |  |
| 7 | Postgres tuning vs limits/RAM     | GO/NO-GO/NEEDS-EVIDENCE |  |
| 8 | Ingestion writer/backpressure     | GO/NO-GO/NEEDS-EVIDENCE |  |
| 9 | Timescale chunks/compression      | GO/NO-GO/NEEDS-EVIDENCE |  |
| 10| Live vs offline profiles          | GO/NO-GO/NEEDS-EVIDENCE |  |
| 11| Dependency profile (no NVIDIA)    | GO/NO-GO/NEEDS-EVIDENCE |  |
| 12| LAN exposure / token auth         | GO/NO-GO/NEEDS-EVIDENCE |  |

## Findings (ranked by severity)
### [P0|P1|P2|P3] HW-RV-01 — <title>   (Dimension N)
- Status: Bug | Failure | Incomplete | Misconfig | Mismatch | Improvement;  Satisfied | Partial | Violated | Unverifiable
- What: <the gap> — code evidence file:line; host evidence <command + output, or the exact command the operator must run>
- Why it matters: <on this host>
- Recommended next action: <concrete next step: file/setting to change, command to run, migration to add, or evidence to collect — note if blocked on the unusable gfx1151 torch wheel>
- Verify: <how to confirm the action worked>

## Operator commands needed (privileged / NEEDS-EVIDENCE)
- <exact command> — <what it proves>

## Phase 3 — prior baseline findings re-graded
- <baseline item> — closed | partial | regressed — <one line + evidence>

## Working well
- <2-3 dimensions genuinely optimized end-to-end>
```

Do not silently drop a dimension: if evidence is missing, list it as
NEEDS-EVIDENCE with the exact operator command. Rank findings by severity; cap
narration. After the report, self-check that every Phase 1 requirement was tested
in Phase 2 and every baseline finding got a Phase 3 grade, then run
`git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`,
and any read-only validators; capture exact exit codes and key output lines. Stop
after the report + command exit codes.
