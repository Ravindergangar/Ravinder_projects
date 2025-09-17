## HubSpot ↔ Dynamics 365 Sync on Databricks

This project provides a configuration-driven, resilient data sync between HubSpot and Microsoft Dynamics 365 using Databricks and Delta Lake.

It implements:
- Bronze ingestion from both APIs with incremental fetch
- Silver normalization with configurable field mappings
- Gold SCD2 history tables for change tracking
- Field-level sync rules (bidirectional/unidirectional, overwrite vs fill-if-missing)
- Change audit logging and idempotent outbound sync to both platforms

### Architecture
- Bronze: Raw JSON from HubSpot and Dynamics per object (e.g., contacts, companies), incrementally ingested via `updated_at`/`modifiedon` watermarks.
- Silver: Normalized, unified schemas per domain entity using config mappings and deterministic ids.
- Gold: SCD2 history tables for business-friendly consumption and downstream analytics.
- Audit: Append-only change and sync attempt logs with request/response metadata and outcomes.

### Repo Layout
```
configs/
  mappings.yaml        # Object/field mappings, directions, and policies
  settings.yaml        # Endpoints, table names, options
jobs/
  main.py              # Orchestration entrypoint for Databricks Jobs
src/
  pipelines/           # Ingestion, normalization, SCD2, outbound sync
  sync_engine/         # Common libs: config, http, secrets, delta utils, logging
```

### Running on Databricks
1) Provision a cluster (DBR 13.3+ recommended). Install the following PyPI packages on the cluster:
   - `requests`, `tenacity`, `pydantic`, `pyyaml`

2) Store secrets (recommended: Databricks Secrets):
   - HubSpot: `HUBSPOT_API_KEY` or Private App token
   - Dynamics: `D365_TENANT_ID`, `D365_CLIENT_ID`, `D365_CLIENT_SECRET`, `D365_RESOURCE_URL`

3) Upload `configs/*.yaml` and `src/` to your workspace repo or DBFS. Configure `settings.yaml`.

4) Create a Job pointing to `jobs/main.py` with task parameters, e.g.:
   - `--run all` or `--run bronze,silver,gold,sync`
   - `--objects contacts,companies`

### Incremental Strategy
- High-watermark column per source object (HubSpot: `updatedAt`; D365: `modifiedon` or object-specific).
- Persisted watermark in the target Delta table’s table properties or a control table.
- Idempotent merges using stable natural keys and deterministic ids.

### SCD2
- Tracks historical attribute changes with `effective_start_at`, `effective_end_at`, `is_current` and a composite business key.

### Field-Level Rules
- Direction: `hubspot_to_d365`, `d365_to_hubspot`, `bidirectional`
- Merge policy: `overwrite` or `fill_if_missing`
- Conflict policy: `source_of_truth: hubspot/d365` or `most_recent_wins`

### Local Development
You can run parts locally with `pyspark` if available. For production, run on Databricks.

