# Hardware Reconfiguration Validation Deep Dive Prompt

A single focused audit prompt. It first derives what the code requires from the
hardware/OS, then audits the **new** host configuration against those
requirements, verifies the recent changes actually optimized the code+hardware
fit, and emits a per-issue report. This is a **read-only audit that produces a
report** — it does not implement fixes.

---

## Prompt — Audit: does the reconfigured host now satisfy and optimize the code's hardware/OS requirements?

You are working in `/home/david/gitsandbox/system/system`. The repo is dirty; do
not revert or modify unrelated user changes. This is a **read-only audit**: do
not change production code, config, or schema. Your single deliverable is the
report described at the end. Preserve a "safe/live trading semantics before raw
performance" lens throughout — flag anything where a hardware/OS optimization
could weaken a live trading safety gate.

### Host context (current, post-reconfiguration — verify, do not assume)

Treat the target host as an **AMD Ryzen AI Max+ 395 (Strix Halo)**, 16 cores /
32 threads, ~123 GiB RAM, Radeon 8060S iGPU + NPU (`gfx1151`), Docker-based
runtime services. Recent host/OS changes that this audit must validate were made
to close prior findings:

- **Storage was moved off the pressured root disk onto NVMe ZFS pools.** Planned
  layout: 3 NVMe drives (2×2TB + 1×4TB) as independent ZFS pools, with `PGDATA`
  on the 4TB pool and backups on a separate 2TB pool. The prior audit found
  `PGDATA`/`pg_wal` still on the root Docker volume and a failing live WAL
  archiver — **verify the actual current placement and archiver health**, do not
  trust the plan.
- **CPU-first compute.** PyTorch is CPU-only on this host today: ROCm/`gfx1151`
  acceleration is **not** usable (the installed torch+rocm wheel ships no
  `gfx1151` kernels), so the iGPU/NPU must not be assumed available by live code.
- **LAN access mode** was added (dashboard `:8000` + operator `:4001` bound to
  `0.0.0.0` under `TRADING_NETWORK_MODE=lan`, gated by `DASHBOARD_API_TOKEN`).
- Docker resource limits, log retention, Postgres tuning, ingestion writer
  knobs, and dependency profiles may or may not have been applied — that is part
  of what you are verifying.

Privileged host inspection (`zpool status`, `zfs list`, `lsblk`, `smartctl`,
`df -h`, `docker stats`, `systemctl`, reading `/proc`) may require sudo, which
this agent shell cannot run. When a command needs privilege, **state the exact
command and ask the user to run it**, then incorporate their output — do not
silently skip the evidence.

### Objective — three phases, in order

**Phase 1 — Derive the code's hardware/OS requirements (from the code, not from memory).**
Read the repo's features and functions and extract what the running system
actually *needs and assumes* from the hardware and OS. Build an explicit
requirements inventory covering at least:

- **Storage:** required/allowed data paths and prefixes (e.g.
  `TRADING_ALLOWED_STORAGE_PREFIXES`), where `PGDATA`, `pg_wal`, WAL archive,
  Redis, MinIO, backups, runtime spool/dead-letter, and the durable async-writer
  SQLite spool are expected to live; free-space and growth assumptions;
  filesystem assumptions (ZFS dataset/recordsize/compression, fsync/`O_DIRECT`,
  `synchronous_commit` tiers).
- **CPU / threads:** BLAS/OMP/MKL/OpenBLAS/NumExpr/torch thread policy, per-role
  thread caps, supervised child-process count vs. core count (oversubscription).
- **Accelerator:** every place that selects a torch/embed/NLP/FinBERT/TS device
  or imports NVIDIA-only modules (`pynvml`, `nvidia-ml-py`) — what it assumes and
  whether it degrades cleanly to CPU on this AMD host.
- **Memory:** Postgres/Timescale memory tuning, Redis `maxmemory`, container
  limits, `/dev/shm`, model/data-loader footprints.
- **Network:** bind addresses, ports, and the LAN-mode auth/token requirements.
- **Database/IO:** Timescale chunk intervals, compression, WAL/checkpoint
  headroom, connection pools per child process.

For each requirement, record where in the code it is declared/enforced
(`file:line`), its default, and whether it is enforced in production code or only
in tests/docs.

**Phase 2 — Audit the actual current host/OS configuration against Phase 1.**
Gather real evidence of the host's current state (drives, ZFS pools, mounts,
free space, container resource limits, log sizes, effective Postgres settings,
WAL archiver status via `pg_stat_archiver`, `pg_wal` growth, resolved torch
device, thread env, bind addresses). Map each piece of evidence to the matching
Phase 1 requirement and classify: **satisfied**, **partially satisfied**,
**violated**, or **unverifiable (needs user-run command)**.

**Phase 3 — Verify the reconfiguration optimized the code+hardware fit.**
Using the prior baseline in
`docs/handoff/deep_dive_prompts/HARDWARE_OPTIMIZATION_DEEP_DIVE_PROMPTS.md`
(disk/retention, CPU-first device, Docker resource isolation, Postgres tuning,
scoring indexes, ingestion writer tuning, live/offline split, dependency
profiles) and `INGESTION_PERF_RELIABILITY_AUDIT.md`, determine for each prior
finding whether the recent changes **actually closed it in production code/config
on this host**, left it **partially done**, or **regressed/introduced new risk**.
Explicitly answer: is the code now placing data on the ZFS NVMe pools (not root
Docker storage), is WAL archiving real and healthy, is the runtime deterministic
CPU-first, are services resource-bounded with internally consistent
Postgres/Redis memory, and is LAN exposure authenticated and minimal.

### Coverage areas (verify each; not exhaustive)

1. Storage placement & ZFS NVMe pool layout (PGDATA on 4TB, backups on separate
   2TB, allowed-prefix overrides, dataset tuning).
2. WAL archiving health and the storage-placement startup gate.
3. Disk pressure, log retention, and backup accounting after the move.
4. CPU-first device resolution and AMD/ROCm `gfx1151` non-availability handling.
5. BLAS/OMP/torch thread caps vs. 32 threads × supervised child count.
6. Docker CPU/memory/`shm` limits and host headroom (OS, IDE, tests, diagnostics).
7. Postgres/Timescale tuning vs. container limits and the 123 GiB host.
8. Ingestion writer pools/queues/backpressure sizing for this host.
9. Timescale chunk-interval/compression placement on the new pools.
10. Live vs. offline/research resource profiles.
11. Dependency profile (CPU/default install does not require NVIDIA-only packages).
12. LAN network mode exposure, bind addresses, and token/auth enforcement.

### Suggested evidence sources (read these; widen as needed)

- `deploy/compose/docker-compose.stack.yml`,
  `deploy/compose/docker-compose.external-services.yml`,
  `deploy/compose/.env.example`, `deploy/env/trading.env.example`,
  `deploy/compose/README.md`, `deploy/README.md`
- `engine/runtime/prod_preflight.py`, `engine/runtime/staging_prod_preflight.py`,
  `engine/runtime/external_service_readiness.py`, `engine/runtime/health.py`,
  `engine/runtime/platform.py`, `engine/runtime/config.py`
- Storage/placement gate code and `TRADING_ALLOWED_STORAGE_PREFIXES` handling;
  `ops/backup/`, `ops/server/bootstrap.sh`, `ops/server/config/postgres.conf.tmpl`
- `engine/runtime/torch_threads.py`, device-selection in
  `engine/data/jobs/process_events*.py`, `engine/data/finbert_sentiment.py`,
  `engine/nlp/encoder.py`, `engine/strategy/models/patchtst.py`,
  `engine/strategy/models/itransformer.py`,
  `engine/strategy/ts_foundation_encoder.py`
- `engine/runtime/timescale_client.py`, `engine/runtime/storage_pg_prices.py`,
  `engine/runtime/async_writer.py`, `engine/runtime/telemetry_append_buffer.py`,
  `engine/runtime/storage_pool.py`, `engine/cache/redis_pool.py`,
  `engine/runtime/ingestion_runtime.py`
- `requirements*.txt`, `pyproject.toml`
- `dashboard_server.py` and operator-service bind/auth code; `docs/LAN_ACCESS.md`
- Docs: `docs/PRODUCTION_CHECKLIST.md`, `docs/OBSERVABILITY.md`,
  `docs/README_DATABASE_MAP.md`, `docs/REFERENCE_CONFIGURATION_GLOSSARY.md`,
  and the prior prompt/audit files in this folder and the repo root.

### Deliverable — the report

Produce one report. Start with a one-paragraph executive summary and a
per-dimension GO / NO-GO / NEEDS-EVIDENCE verdict table for the 12 coverage
areas. Then list every finding — bug, failure, incomplete implementation,
misconfiguration, code↔hardware mismatch, or improvement — as its own entry with
these fields:

- **ID** (e.g. `HW-RV-01`) and **area** (one of the 12).
- **Severity:** Critical / High / Medium / Low (Critical = data loss, live-trade
  safety, or DB/ingestion outage risk).
- **Status:** Bug | Failure | Incomplete | Misconfig | Mismatch | Improvement;
  and Satisfied | Partial | Violated | Unverifiable.
- **What the issue is:** the specific gap, with code evidence (`file:line`) and
  host evidence (command + output, or the exact command the user must run).
- **Why it matters** on this host.
- **Recommended next action:** the concrete next step (file/setting to change,
  command to run, migration to add, or evidence to collect) — actionable, not
  vague. If it depends on the unusable `gfx1151` torch wheel, say so explicitly.
- **How to verify** the action worked.

Rank findings by severity. Do not silently drop any coverage area — if you could
not get evidence, list it as a NEEDS-EVIDENCE finding with the exact command for
the user to run.

After writing the report, audit your own work: confirm every Phase 1 requirement
was checked in Phase 2, every prior baseline finding got a Phase 3 verdict, and
every claim cites code or host evidence. Then run
`git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`,
and any relevant read-only validators; capture exact exit codes and key output
lines. If any coverage area could not be fully assessed, say **NEEDS-EVIDENCE**
and list precisely what is missing rather than guessing.
