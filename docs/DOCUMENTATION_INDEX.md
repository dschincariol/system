# Documentation Index

This index organizes the repository documentation using Diataxis.

Definitions used here:

- `Canonical`: should be kept in sync with code and operator-facing behavior
- `Supplementary`: useful context or planning material, but not the primary source of truth

## Canonical First Read

Engineer path:

1. [README.md](../README.md)
2. [MAINTAINER_INDEX.md](MAINTAINER_INDEX.md)
3. [REFERENCE_CONFIGURATION_GLOSSARY.md](REFERENCE_CONFIGURATION_GLOSSARY.md)
4. The subsystem README for the code you will touch
5. The relevant reference or explanation doc below

Operator path:

1. [README_OPERATOR_GUIDE.md](README_OPERATOR_GUIDE.md)
2. [REFERENCE_CONFIGURATION_GLOSSARY.md](REFERENCE_CONFIGURATION_GLOSSARY.md)
3. [REFERENCE_DATA_SOURCE_CONTROL_PLANE.md](REFERENCE_DATA_SOURCE_CONTROL_PLANE.md)
4. [boot/README.md](../boot/README.md)
5. [ui/README.md](../ui/README.md)

## Tutorials

Current state:

- The repository does not yet contain true tutorial-style documentation.
- Use the read paths above as onboarding until step-by-step tutorials are authored.

Closest current substitutes:

- [README.md](../README.md)
  `Canonical`. Repo entrypoint and current documentation contract.
- [MAINTAINER_INDEX.md](MAINTAINER_INDEX.md)
  `Canonical`. Short engineer orientation path.

## How-To Guides

| Document | Status | Use When |
| --- | --- | --- |
| [README_OPERATOR_GUIDE.md](README_OPERATOR_GUIDE.md) | Canonical | You need the operator mental model for daily supervision, incident triage, or dashboard interpretation. |
| [PRODUCTION_CHECKLIST.md](PRODUCTION_CHECKLIST.md) | Canonical | You are preparing a host, validating a deployment, or checking readiness before enabling higher-risk runtime modes. |
| [PRODUCTION_BACKEND_CI.md](PRODUCTION_BACKEND_CI.md) | Canonical | You need to reproduce or audit the Postgres/Redis production-backend CI gate and staging preflight evidence. |
| [LIVE_READINESS_CHECKLIST.md](LIVE_READINESS_CHECKLIST.md) | Canonical | You are moving from safe or paper operation toward live trading. |
| [STAGING_PROD_PREFLIGHT_EVIDENCE.md](STAGING_PROD_PREFLIGHT_EVIDENCE.md) | Canonical | You need to run the staging prod-preflight harness or review redacted preflight evidence. |
| [DISK_RETENTION_RUNBOOK.md](DISK_RETENTION_RUNBOOK.md) | Canonical | You need to inspect root/Docker disk pressure, relocate Docker data-root to ZFS, run backup accounting, or use safe cleanup commands without deleting live state. |
| [MEMORY_PRESSURE_RUNBOOK.md](MEMORY_PRESSURE_RUNBOOK.md) | Canonical | You need to enforce or verify swappiness, zram, disk swap, ZFS ARC caps, deleted `/tmp` holder detection, or disk-backed pytest scratch. |
| [OS_MIGRATION_RUNBOOK.md](OS_MIGRATION_RUNBOOK.md) | Canonical | You need to move production host `bart` from Ubuntu 25.10 to a supported LTS with repo-tracked preflight/postflight gates and ZFS rollback evidence. |
| [CPU_POWER_POLICY.md](CPU_POWER_POLICY.md) | Canonical | You need to verify, revert, or audit host `bart` CPU performance-profile enforcement and ROCm/GPU thermal composition. |
| [Secrets_Rotation_Runbook.md](Secrets_Rotation_Runbook.md) | Canonical | You need to rotate production secrets or credential-encryption key material. |
| [boot/README.md](../boot/README.md) | Canonical | You are working on the local launcher, operator server, or guarded repair flow. |
| [deploy/README.md](../deploy/README.md) | Supplementary | You need the current deployment directory layout and install entrypoint. |
| [ops/README.md](../ops/README.md) | Supplementary | You need a quick orientation to ad hoc ops and offline maintenance scripts. |

## Reference

| Document | Status | Scope |
| --- | --- | --- |
| [REFERENCE_CONFIGURATION_GLOSSARY.md](REFERENCE_CONFIGURATION_GLOSSARY.md) | Canonical | Runtime configuration surfaces, environment-variable families, and secret-management boundaries. |
| [config_env_allowlist.txt](config_env_allowlist.txt) | Canonical | Frozen allowlist of environment variables read in code but not yet documented in `.env.example` or the glossary; the validator env-coverage gate blocks new undocumented vars while tolerating this legacy backlog. Shrinks as variables are documented. |
| [REFERENCE_DATA_SOURCE_CONTROL_PLANE.md](REFERENCE_DATA_SOURCE_CONTROL_PLANE.md) | Canonical | Data-source UI, routes, storage tables, mutation payloads, and runtime lifecycle rules. |
| [DATA_CONTRACTS.md](DATA_CONTRACTS.md) | Canonical | Current payload, row, and response contracts that cross subsystem boundaries. |
| [PREDICTION_MARKET_MACRO.md](PREDICTION_MARKET_MACRO.md) | Canonical | Kalshi/CME macro expectation, Polymarket event-signal, ForecastEx regulated event-contract, and optional read-only IBKR source setup, storage, feature ids, PIT policy, and promotion boundaries. |
| [OBSERVABILITY.md](OBSERVABILITY.md) | Canonical | Runtime observability signals, operator APIs, and telemetry ownership. |
| [DEPENDENCY_PROFILES.md](DEPENDENCY_PROFILES.md) | Canonical | CPU/default, NVIDIA CUDA, and reserved AMD/ROCm dependency profile selection and rollback rules. |
| [README_DATABASE_MAP.md](README_DATABASE_MAP.md) | Canonical | Runtime storage table families, key tables, and data-flow-oriented schema reference. |
| [Database_Schema.md](Database_Schema.md) | Canonical | Production Postgres/Timescale schema classification and human review register. |
| [Audit_Chain_Spec.md](Audit_Chain_Spec.md) | Canonical | Audit hash-chain serialization, ordering, and verification contract. |
| [DOCSTRING_STYLE.md](DOCSTRING_STYLE.md) | Canonical | NumPy-style docstring contract for touched Python modules, classes, and functions. |
| [DECOMPOSITION_CONVENTIONS.md](DECOMPOSITION_CONVENTIONS.md) | Canonical | Safe pattern for incrementally splitting oversized modules while preserving public entrypoints and behavior. |
| [openapi/README.md](openapi/README.md) | Canonical | Location and maintenance rule for the incremental OpenAPI source of truth. |
| [hyperparameter_inventory.md](hyperparameter_inventory.md) | Canonical | Tunable model-training parameters managed by the Optuna tuning catalog. |
| [README_FUNCTION_MAP.md](README_FUNCTION_MAP.md) | Supplementary | Function-level navigation for large Python entrypoints and major subsystems. |
| [engine/README.md](../engine/README.md) | Canonical | Top-level engine package map. |
| [engine/runtime/README.md](../engine/runtime/README.md) | Canonical | Runtime control plane ownership. |
| [engine/dashboard/README.md](../engine/dashboard/README.md) | Canonical | Extracted helper package for the root dashboard compatibility facade. |
| [engine/api/system/README.md](../engine/api/system/README.md) | Canonical | Extracted helper package for the system API compatibility facade. |
| [engine/startup/README.md](../engine/startup/README.md) | Canonical | Extracted startup helper package used by the root runtime executable facade. |
| [engine/data/README.md](../engine/data/README.md) | Canonical | Data-ingestion ownership and extension points. |
| [engine/strategy/README.md](../engine/strategy/README.md) | Canonical | Strategy, model, governance, and portfolio ownership. |
| [engine/execution/README.md](../engine/execution/README.md) | Canonical | Execution, broker, and attribution ownership. |
| [engine/api/README.md](../engine/api/README.md) | Canonical | HTTP handler modules and route-boundary ownership. |
| [engine/risk/README.md](../engine/risk/README.md) | Canonical | Portfolio-risk and Monte Carlo risk ownership. |
| [engine/terminal/README.md](../engine/terminal/README.md) | Canonical | Browser-terminal API ownership and terminal safety boundary. |
| [engine/research/README.md](../engine/research/README.md) | Canonical | Offline research and fragility tooling. |
| [engine/jobs/README.md](../engine/jobs/README.md) | Canonical | Live price-ingestion jobs, currently the Polygon websocket streamer that publishes trade/quote events into the runtime price tables. |
| [engine/audit/README.md](../engine/audit/README.md) | Canonical | Tamper-evident SHA-256 hash chain over append-only audit ledger tables: canonical serialization, the append API, and the verifier and CLI that recompute chains and record divergences. |
| [engine/cache/README.md](../engine/cache/README.md) | Canonical | Redis hot-path cache: write-through store, codec/keyspace, circuit-breaker fallback to Postgres, and typed wrappers for the cached runtime tables. |
| [engine/backtest/README.md](../engine/backtest/README.md) | Canonical | Leakage-aware backtest primitives — combinatorial purged K-fold CV (purge + embargo) and deflated Sharpe diagnostics used by the promotion gate. |
| [engine/nlp/README.md](../engine/nlp/README.md) | Canonical | Offline NLP encoders (FinBERT sentiment, sentence-transformer embeddings), the content-hash cache, and recency-weighted symbol-day aggregation for financial text. |
| [engine/causal/README.md](../engine/causal/README.md) | Canonical | Causal-plausibility scoring of features (Granger with HAC errors, DoWhy backdoor estimation, curated DAGs) and persistence of the composite [0,1] causal score. |
| [engine/artifacts/README.md](../engine/artifacts/README.md) | Canonical | Content-addressed artifact blob store: SHA256 sharded storage, aliases and reference counting, the centralized serialization facade with AST-lint enforcement, and the fsck verifier plus garbage collector. |
| [engine/rl/README.md](../engine/rl/README.md) | Canonical | Shadow-only portfolio RL: Gym environment, PPO/SAC agents, env wrappers, and the advisory shadow evaluator writing rl_shadow_decisions (no live order authority). |
| [services/README.md](../services/README.md) | Canonical | Sidecar-service ownership, especially data-source and operator-AI boundaries. |
| [ui/README.md](../ui/README.md) | Canonical | Dashboard and data-source UI module reference. |

Known reference gaps:

- The OpenAPI baseline now exists under [openapi/openapi.yaml](openapi/openapi.yaml) and covers the high-value system, jobs, data-source, broker-config, and browser-terminal contracts. The broader aggregated `/api/*` surface in [engine/api/api_ops.py](../engine/api/api_ops.py), [engine/api/api_market.py](../engine/api/api_market.py), fallback routes in [dashboard_server.py](../dashboard_server.py), and the separate Node operator server in [boot/operator_server.js](../boot/operator_server.js) still needs additional coverage.

## Governance

| Document | Status | Use When |
| --- | --- | --- |
| [../CONTRIBUTING.md](../CONTRIBUTING.md) | Canonical | You are making a change and need the doc update rules, ADR triggers, OpenAPI expectations, and validation commands. |
| [../CHANGELOG.md](../CHANGELOG.md) | Canonical | You need the forward-maintained record of notable changes from the documented baseline onward. |
| [adr/README.md](adr/README.md) | Canonical | You need the decision log format or need to add or supersede an ADR. |
| [LICENSING_NOTE.md](LICENSING_NOTE.md) | Canonical | You need the current repo licensing status and the reason external reuse is blocked. |

## Explanation

| Document | Status | Use When |
| --- | --- | --- |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Canonical | You need the system-level runtime architecture grounded in the current entrypoints and control planes. |
| [FAILURE_MODES.md](FAILURE_MODES.md) | Canonical | You need the repo's current fail-closed behaviors and the first places to inspect for common runtime failures. |
| [README_ARCHITECTURE.md](README_ARCHITECTURE.md) | Supplementary | You want a shorter architecture-oriented walkthrough alongside the canonical architecture reference. |
| [README_SEQUENCE_DIAGRAMS.md](README_SEQUENCE_DIAGRAMS.md) | Canonical | You need startup, decision, governance, or dashboard flows in sequence form. |
| [STATE_MACHINES.md](STATE_MACHINES.md) | Canonical | You need lifecycle, execution barrier, capital guard, promotion, or repair-gating state transitions. |
| [SEQUENCE_DIAGRAMS_EXTENDED.md](SEQUENCE_DIAGRAMS_EXTENDED.md) | Supplementary | You want expanded sequence diagrams beyond the canonical short set. |
| [README_DEVELOPER_MAP.md](README_DEVELOPER_MAP.md) | Supplementary | You want a narrative cross-repo navigation guide and common edit paths. |
| [System_Audit_Layer1.md](System_Audit_Layer1.md) and [System_Audit_Layer1.json](System_Audit_Layer1.json) | Supplementary | Generated static-audit outputs retained at the tool-owned path. |
| [System_Audit_Layer3.md](System_Audit_Layer3.md) and [System_Audit_Layer3.json](System_Audit_Layer3.json) | Supplementary | Generated spec-compliance outputs retained at the tool-owned path. |

## Archived And Handoff Material

These documents are preserved for context and future work, but they are not canonical runtime truth.

| Document | Status | Use When |
| --- | --- | --- |
| [archive/README.md](archive/README.md) | Supplementary | You need historical planning material, old audit triage, or binary planning artifacts. |
| [archive/Database_Production_Plan.md](archive/Database_Production_Plan.md) | Supplementary | You need historical database production-planning context. |
| [archive/STORAGE_MIGRATION_BACKLOG.md](archive/STORAGE_MIGRATION_BACKLOG.md) | Supplementary | You need the historical phased SQLite-to-Postgres or Timescale migration backlog. |
| [archive/README_UI_REDESIGN_PLAN.md](archive/README_UI_REDESIGN_PLAN.md) | Supplementary | You are evaluating forward-looking UI restructuring work, not current runtime behavior. |
| [archive/UI_CHARTING_BEST_IN_CLASS_RECOMMENDATIONS.md](archive/UI_CHARTING_BEST_IN_CLASS_RECOMMENDATIONS.md) | Supplementary | You need the advisory charting and decision-visualization roadmap. |
| [archive/Quant_Architecture_Upgrade_Document.docx](archive/Quant_Architecture_Upgrade_Document.docx) | Supplementary | You need the legacy binary planning artifact. |
| [archive/CANONICAL_REPOSITORY.md](archive/CANONICAL_REPOSITORY.md) | Supplementary | You need the one-time repository canonicalization note and removal plan. |
| [archive/LEGACY_SALVAGE_REPORT.md](archive/LEGACY_SALVAGE_REPORT.md) | Supplementary | You need the historical legacy-tree salvage review. |
| [archive/System_Audit_Layer1_P0P1_Triage.md](archive/System_Audit_Layer1_P0P1_Triage.md) | Supplementary | You need the dated static-audit triage record. |
| [handoff/README.md](handoff/README.md) | Supplementary | You need AI-session handoff context or prompt collections. |
| [handoff/TRADING_SYSTEM_HANDOFF.md](handoff/TRADING_SYSTEM_HANDOFF.md) | Supplementary | You need recent AI-session context or strategic roadmap material. |
| [handoff/QUICK_WINS.md](handoff/QUICK_WINS.md) | Supplementary | You need implementation prompts for proposed future work, not current system contracts. |
| [handoff/deep_dive_prompts/README.md](handoff/deep_dive_prompts/README.md) | Supplementary | You need the moved go-live, hardware, prediction-market, or UI deep-dive prompt bundles. |
| [handoff/codex_migration/README.md](handoff/codex_migration/README.md) | Supplementary | You need staged migration handoff records, slice prompts, or validation ledgers. |
| [codex_prompts/README.md](codex_prompts/README.md) | Supplementary | You need older standalone Codex implementation prompt sets. |

## Canonical Vs Supplementary

Canonical documentation in this repository currently consists of:

- [README.md](../README.md)
- [../CONTRIBUTING.md](../CONTRIBUTING.md)
- [../CHANGELOG.md](../CHANGELOG.md)
- [MAINTAINER_INDEX.md](MAINTAINER_INDEX.md)
- [REFERENCE_CONFIGURATION_GLOSSARY.md](REFERENCE_CONFIGURATION_GLOSSARY.md)
- [REFERENCE_DATA_SOURCE_CONTROL_PLANE.md](REFERENCE_DATA_SOURCE_CONTROL_PLANE.md)
- [DATA_CONTRACTS.md](DATA_CONTRACTS.md)
- [PREDICTION_MARKET_MACRO.md](PREDICTION_MARKET_MACRO.md)
- [OBSERVABILITY.md](OBSERVABILITY.md)
- [DEPENDENCY_PROFILES.md](DEPENDENCY_PROFILES.md)
- [DOCSTRING_STYLE.md](DOCSTRING_STYLE.md)
- [openapi/README.md](openapi/README.md)
- [LICENSING_NOTE.md](LICENSING_NOTE.md)
- [adr/README.md](adr/README.md) and the ADR files under `docs/adr/`
- [README_OPERATOR_GUIDE.md](README_OPERATOR_GUIDE.md)
- [PRODUCTION_CHECKLIST.md](PRODUCTION_CHECKLIST.md)
- [PRODUCTION_BACKEND_CI.md](PRODUCTION_BACKEND_CI.md)
- [LIVE_READINESS_CHECKLIST.md](LIVE_READINESS_CHECKLIST.md)
- [STAGING_PROD_PREFLIGHT_EVIDENCE.md](STAGING_PROD_PREFLIGHT_EVIDENCE.md)
- [DISK_RETENTION_RUNBOOK.md](DISK_RETENTION_RUNBOOK.md)
- [MEMORY_PRESSURE_RUNBOOK.md](MEMORY_PRESSURE_RUNBOOK.md)
- [OS_MIGRATION_RUNBOOK.md](OS_MIGRATION_RUNBOOK.md)
- [CPU_POWER_POLICY.md](CPU_POWER_POLICY.md)
- [Secrets_Rotation_Runbook.md](Secrets_Rotation_Runbook.md)
- [README_DATABASE_MAP.md](README_DATABASE_MAP.md)
- [Database_Schema.md](Database_Schema.md)
- [Audit_Chain_Spec.md](Audit_Chain_Spec.md)
- [hyperparameter_inventory.md](hyperparameter_inventory.md)
- [ARCHITECTURE.md](ARCHITECTURE.md)
- [FAILURE_MODES.md](FAILURE_MODES.md)
- [README_SEQUENCE_DIAGRAMS.md](README_SEQUENCE_DIAGRAMS.md)
- [STATE_MACHINES.md](STATE_MACHINES.md)
- Subsystem READMEs under `engine/`, plus [boot/README.md](../boot/README.md), [services/README.md](../services/README.md), and [ui/README.md](../ui/README.md)

Supplementary documentation should not override the canonical set when there is a conflict.

## Maintenance Rule

When code changes land:

1. Update the relevant subsystem README.
2. Update the relevant reference doc if the change modifies configuration, storage, HTTP contracts, or operator-facing behavior.
3. Update [../CHANGELOG.md](../CHANGELOG.md) when the change is notable to operators, integrators, or future maintainers.
4. Add or update an ADR when the change establishes or changes a long-lived architectural or governance rule.
5. Update this index if a document changes canonical status or a new documentation area is introduced.
