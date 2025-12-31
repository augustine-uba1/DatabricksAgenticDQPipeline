# DatabricksAgenticDQPipeline
Config-driven, scalable Databricks pipeline that ingests files with Auto Loader, processes them through a Medallion architecture (RAW → Bronze → Silver → Gold), and adds **autonomous data quality triage + self-healing** using a governed remediation “playbook”.


---

## TL;DR — What we’re building

- ✅ **Orchestrator Job** (top-level Databricks Workflow) that manages all other jobs
- ✅ **Config-driven ingestion**: add new file feeds by editing `src/config/sources.json`
- ✅ **Generic Auto Loader ingestion** (one job handles all sources) → **RAW Delta tables**
- ✅ **Medallion pipeline**: RAW → Bronze → Silver → Gold (Lakeflow/DLT-style)
- ✅ **DQ Quarantine**: invalid records routed to quarantine tables with reasons + metadata
- ✅ **Autonomous DQ triage agent**:
  - reads DQ signals + quarantine samples
  - proposes safe remediations
  - writes actions to a governed **DQ Playbook Delta table**
- ✅ **Self-healing Silver**: Silver applies playbook overrides deterministically (no agent editing prod code)
- ✅ **CI/CD**: Databricks Asset Bundles (DAB) + Azure DevOps for dev/test/prod promotion

---
## Why this approach

- Scales cleanly: **new sources = config change**, not new jobs/code
- Safer “agentic” behavior: agent can only write to **controlled remediation tables**
- Auditable + reversible: playbook entries are logged and can be disabled for rollback
- Production-friendly: UC governance, metadata, quarantine, and run traceability

---
## Execution Flow (high level)

- Orchestrator reads src/config/sources.json
- Orchestrator fans out ingestion (For each source)
- Ingestion job runs Auto Loader → RAW Delta
- Medallion pipeline runs RAW → Bronze → Silver → Gold
- DQ agent runs and writes remediation playbook
- Silver re-runs applying safe overrides from the playbook

---
## Roadmap (what we plan to implement step-by-step)

### Phase 1 — Foundation & Ingestion

- Create DAB bundle structure + targets (dev/test/prod)
- Implement orchestrator job
- Implement generic Auto Loader ingestion job
- Validate multi-source ingestion using sources.json

### Phase 2 — Medallion + Data Quality

- Build medallion pipeline (Bronze/Silver/Gold)
- Add DQ expectations + metrics per run
- Add quarantine outputs with reason codes
- Standardize audit/metadata fields

### Phase 3 — Agentic DQ Triage + Self-Healing

- Implement DQ playbook Delta table
- Implement triage agent to read signals + propose actions
- Apply playbook overrides in Silver deterministically
- Add audit log of agent decisions + outcomes

### Phase 4 — CI/CD + Operational Hardening

- Azure DevOps pipeline: validate, deploy, run smoke tests
- RBAC/UC hardening: least privilege for agent
- Operational runbook + troubleshooting guides

## Notes / Guardrails

- Agent does NOT edit code in production
- Agent can only write to approved tables (playbook + audit log)
- All changes are auditable and reversible via enabled=false in playbook entries
