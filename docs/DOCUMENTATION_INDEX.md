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
| [boot/README.md](../boot/README.md) | Canonical | You are working on the local launcher, operator server, or guarded repair flow. |
| [deploy/README.md](../deploy/README.md) | Supplementary | You need the current deployment directory layout and install entrypoint. |
| [ops/README.md](../ops/README.md) | Supplementary | You need a quick orientation to ad hoc ops and offline maintenance scripts. |

## Reference

| Document | Status | Scope |
| --- | --- | --- |
| [REFERENCE_CONFIGURATION_GLOSSARY.md](REFERENCE_CONFIGURATION_GLOSSARY.md) | Canonical | Runtime configuration surfaces, environment-variable families, and secret-management boundaries. |
| [REFERENCE_DATA_SOURCE_CONTROL_PLANE.md](REFERENCE_DATA_SOURCE_CONTROL_PLANE.md) | Canonical | Data-source UI, routes, storage tables, mutation payloads, and runtime lifecycle rules. |
| [DATA_CONTRACTS.md](DATA_CONTRACTS.md) | Canonical | Current payload, row, and response contracts that cross subsystem boundaries. |
| [OBSERVABILITY.md](OBSERVABILITY.md) | Canonical | Runtime observability signals, operator APIs, and telemetry ownership. |
| [README_DATABASE_MAP.md](README_DATABASE_MAP.md) | Canonical | SQLite table families, key tables, and data-flow-oriented schema reference. |
| [DOCSTRING_STYLE.md](DOCSTRING_STYLE.md) | Canonical | NumPy-style docstring contract for touched Python modules, classes, and functions. |
| [openapi/README.md](openapi/README.md) | Canonical | Location and maintenance rule for the incremental OpenAPI source of truth. |
| [README_FUNCTION_MAP.md](README_FUNCTION_MAP.md) | Supplementary | Function-level navigation for large Python entrypoints and major subsystems. |
| [engine/README.md](../engine/README.md) | Canonical | Top-level engine package map. |
| [engine/runtime/README.md](../engine/runtime/README.md) | Canonical | Runtime control plane ownership. |
| [engine/data/README.md](../engine/data/README.md) | Canonical | Data-ingestion ownership and extension points. |
| [engine/strategy/README.md](../engine/strategy/README.md) | Canonical | Strategy, model, governance, and portfolio ownership. |
| [engine/execution/README.md](../engine/execution/README.md) | Canonical | Execution, broker, and attribution ownership. |
| [engine/api/README.md](../engine/api/README.md) | Canonical | HTTP handler modules and route-boundary ownership. |
| [engine/risk/README.md](../engine/risk/README.md) | Canonical | Portfolio-risk and Monte Carlo risk ownership. |
| [engine/terminal/README.md](../engine/terminal/README.md) | Canonical | Browser-terminal API ownership and terminal safety boundary. |
| [engine/research/README.md](../engine/research/README.md) | Canonical | Offline research and fragility tooling. |
| [services/README.md](../services/README.md) | Canonical | Sidecar-service ownership, especially data-source and operator-AI boundaries. |
| [ui/README.md](../ui/README.md) | Canonical | Dashboard and data-source UI module reference. |

Known reference gaps:

- The OpenAPI baseline now exists under [openapi/openapi.yaml](openapi/openapi.yaml), but the broader aggregated `/api/*` surface in [engine/api/api_ops.py](../engine/api/api_ops.py), [engine/api/api_market.py](../engine/api/api_market.py), fallback routes in [dashboard_server.py](../dashboard_server.py), and the separate Node operator server in [boot/operator_server.js](../boot/operator_server.js) still needs additional coverage.

## Governance

| Document | Status | Use When |
| --- | --- | --- |
| [../CONTRIBUTING.md](../CONTRIBUTING.md) | Canonical | You are making a change and need the doc update rules, ADR triggers, OpenAPI expectations, and validation commands. |
| [../CHANGELOG.md](../CHANGELOG.md) | Canonical | You need the forward-maintained record of notable changes from the documented baseline onward. |
| [adr/README.md](adr/README.md) | Canonical | You need the decision log format or need to add or supersede an ADR. |
| [LICENSING_NOTE.md](LICENSING_NOTE.md) | Canonical | You need the current repo licensing status and the reason external reuse is blocked. |
| [FINAL_DOCS_AUDIT.md](FINAL_DOCS_AUDIT.md) | Supplementary | You need the outcome of the latest final documentation verification pass. |

## Explanation

| Document | Status | Use When |
| --- | --- | --- |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Canonical | You need the system-level runtime architecture grounded in the current entrypoints and control planes. |
| [FAILURE_MODES.md](FAILURE_MODES.md) | Canonical | You need the repo's current fail-closed behaviors and the first places to inspect for common runtime failures. |
| [README_ARCHITECTURE.md](README_ARCHITECTURE.md) | Supplementary | You want a shorter architecture-oriented walkthrough alongside the canonical architecture reference. |
| [README_SEQUENCE_DIAGRAMS.md](README_SEQUENCE_DIAGRAMS.md) | Canonical | You need startup, decision, governance, or dashboard flows in sequence form. |
| [README_DEVELOPER_MAP.md](README_DEVELOPER_MAP.md) | Supplementary | You want a narrative cross-repo navigation guide and common edit paths. |
| [DOCS_AUDIT.md](DOCS_AUDIT.md) | Supplementary | You need the current documentation inventory, duplication analysis, and prioritized gaps. |
| [STORAGE_MIGRATION_BACKLOG.md](STORAGE_MIGRATION_BACKLOG.md) | Supplementary | You are executing or reviewing the phased SQLite to Postgres or Timescale migration backlog and need the phase gates, audits, and rollback rules. |
| [README_UI_REDESIGN_PLAN.md](README_UI_REDESIGN_PLAN.md) | Supplementary | You are evaluating forward-looking UI restructuring work, not current runtime behavior. |
| [handoff/TRADING_SYSTEM_HANDOFF.md](handoff/TRADING_SYSTEM_HANDOFF.md) | Supplementary | You need recent AI-session context or strategic roadmap material. |
| [handoff/QUICK_WINS.md](handoff/QUICK_WINS.md) | Supplementary | You need implementation prompts for proposed future work, not current system contracts. |
| [Quant_Architecture_Upgrade_Document.docx](Quant_Architecture_Upgrade_Document.docx) | Supplementary | Legacy binary planning artifact; do not treat as canonical source of truth. |

## Canonical Vs Supplementary

Canonical documentation in this repository currently consists of:

- [README.md](../README.md)
- [../CONTRIBUTING.md](../CONTRIBUTING.md)
- [../CHANGELOG.md](../CHANGELOG.md)
- [MAINTAINER_INDEX.md](MAINTAINER_INDEX.md)
- [REFERENCE_CONFIGURATION_GLOSSARY.md](REFERENCE_CONFIGURATION_GLOSSARY.md)
- [REFERENCE_DATA_SOURCE_CONTROL_PLANE.md](REFERENCE_DATA_SOURCE_CONTROL_PLANE.md)
- [DATA_CONTRACTS.md](DATA_CONTRACTS.md)
- [OBSERVABILITY.md](OBSERVABILITY.md)
- [DOCSTRING_STYLE.md](DOCSTRING_STYLE.md)
- [openapi/README.md](openapi/README.md)
- [LICENSING_NOTE.md](LICENSING_NOTE.md)
- [adr/README.md](adr/README.md) and the ADR files under `docs/adr/`
- [README_OPERATOR_GUIDE.md](README_OPERATOR_GUIDE.md)
- [PRODUCTION_CHECKLIST.md](PRODUCTION_CHECKLIST.md)
- [README_DATABASE_MAP.md](README_DATABASE_MAP.md)
- [ARCHITECTURE.md](ARCHITECTURE.md)
- [FAILURE_MODES.md](FAILURE_MODES.md)
- [README_SEQUENCE_DIAGRAMS.md](README_SEQUENCE_DIAGRAMS.md)
- Subsystem READMEs under `engine/`, plus [boot/README.md](../boot/README.md), [services/README.md](../services/README.md), and [ui/README.md](../ui/README.md)

Supplementary documentation should not override the canonical set when there is a conflict.

## Maintenance Rule

When code changes land:

1. Update the relevant subsystem README.
2. Update the relevant reference doc if the change modifies configuration, storage, HTTP contracts, or operator-facing behavior.
3. Update [../CHANGELOG.md](../CHANGELOG.md) when the change is notable to operators, integrators, or future maintainers.
4. Add or update an ADR when the change establishes or changes a long-lived architectural or governance rule.
5. Update this index if a document changes canonical status or a new documentation area is introduced.
