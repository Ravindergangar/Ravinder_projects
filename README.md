## HubSpot ↔ Dynamics 365 Sync on Databricks

This project implements a robust, auditable, and incremental bi-directional sync between HubSpot and Microsoft Dynamics 365 using Databricks and Delta Lake.

### Architecture
- **Bronze (Raw landing)**: Incremental API ingestion with watermarks
  - `bronze.hubspot_contacts_raw`
  - `bronze.dynamics_contacts_raw`
- **Silver (Cleansed/Modeled)**: Latest source snapshots, unified contacts, and ID crosswalk
  - `silver.hubspot_contacts_snapshot`
  - `silver.dynamics_contacts_snapshot`
  - `silver.id_crosswalk_contacts`
  - `silver.contacts_unified`
- **Gold (Sync/outbox)**: Per-system outboxes of field-level deltas to push
  - `gold.hubspot_outbox_contacts`
  - `gold.dynamics_outbox_contacts`
- **Meta (Control plane)**: Watermarks, audit, dead-letter
  - `meta.watermarks`
  - `meta.audit_log`
  - `meta.dead_letter`

### Incremental strategy
- Uses per-entity watermarks:
  - HubSpot filter: `hs_lastmodifieddate >= watermark` (via Search API)
  - Dynamics filter: `modifiedon ge watermark` (OData v9.2)
- Watermarks are persisted in `meta.watermarks` and advanced only after successful ingestion writes.

### Direction and update strategies
Configure per-field sync behavior in `configs/field_mappings.yaml`:
- `direction`: `bi`, `to_hubspot`, `to_dynamics`
- `strategy`: `overwrite`, `fill_if_missing`

The compute step produces per-system outboxes honoring these rules:
- `overwrite`: push when values differ
- `fill_if_missing`: push only when the target is null/empty

### Crosswalk and keys
- The canonical key for unified contacts is the normalized email (fallback to a generated UUID for records without email). A crosswalk maps HubSpot `id` and Dynamics `contactid` to `unified_contact_id`.

### Notebooks
Run these in order (also provided as a Job):
1) `notebooks/00_setup.py` — create schemas/tables
2) `notebooks/01_ingest_hubspot_bronze.py` — incremental ingest from HubSpot
3) `notebooks/02_ingest_dynamics_bronze.py` — incremental ingest from Dynamics
4) `notebooks/10_silver_unify_contacts.py` — source snapshots, crosswalk, unified contacts
5) `notebooks/20_scd2_contacts.py` — SCD2 history for unified contacts
6) `notebooks/30_compute_deltas.py` — compute field-level deltas honoring rules
7) `notebooks/40_push_hubspot.py` — push deltas to HubSpot with audit/dead-letter
8) `notebooks/41_push_dynamics.py` — push deltas to Dynamics with audit/dead-letter

### Configuration
- `configs/sources.yaml`: databases, API endpoints, properties, watermarks, and rate limits
- `configs/field_mappings.yaml`: per-field mapping, direction, strategy, and unified names

Environment/secret variables expected at runtime (Databricks secret scopes recommended):
- `HUBSPOT_TOKEN`: HubSpot Private App token
- `D365_TENANT_ID`: Azure AD tenant ID
- `D365_CLIENT_ID`: App registration client ID
- `D365_CLIENT_SECRET`: App registration client secret
- `D365_ORG_URI`: e.g. `yourorg.crm.dynamics.com`

### Dependencies
Install once per cluster (or in your repo job task):
```
%pip install -r /Workspace/.../requirements.txt
```

### Job configuration
Use `jobs/crm_sync_job.json` as a starting point. Update the cluster version, node type, and notebook paths to match your workspace path.

### Runbook (Databricks)
1) Create a Repo and add this project (or upload under Workspace)
2) Open `notebooks/00_setup.py` and run all cells (creates DBs/tables)
3) Configure secrets:
   - Create a secret scope or set env vars on the job/cluster:
     - `HUBSPOT_TOKEN`
     - `D365_TENANT_ID`, `D365_CLIENT_ID`, `D365_CLIENT_SECRET`, `D365_ORG_URI`
4) Run `01_ingest_hubspot_bronze.py` and `02_ingest_dynamics_bronze.py`
5) Run `10_silver_unify_contacts.py` then `20_scd2_contacts.py`
6) Run `30_compute_deltas.py` to stage outbox payloads
7) Run `40_push_hubspot.py` and `41_push_dynamics.py` to push changes

Notes on Dynamics OptionSets/Lookups:
- Configure OptionSets and Lookups in `configs/sources.yaml` under `dynamics.option_sets` and `dynamics.lookups`.
- The Dynamics push notebook translates display values to backend numeric codes or OData binds automatically.

### Data model details
- Bronze tables store raw payloads plus selected flattened columns and timestamps
- Silver snapshots select the latest row per source ID using `updated_at`
- Unified contacts choose a value per field using simple precedence (HubSpot first, then Dynamics) — can be refined
- SCD2 table maintains `valid_from`, `valid_to`, `is_current`, and a `checksum` across unified fields
- Outboxes store PATCH-ready payloads and a `status` (`PENDING`, `SUCCESS`, `FAILED`) with attempt counters
- Audit log records every API call result; dead-letter stores non-recoverable failures

### Running locally vs Databricks
The notebooks are designed for Databricks. They rely on Spark + Delta Lake. For development, open them in a Databricks Repo project. Ensure the configs are reachable at `../configs` relative to notebooks, or set `CONFIG_DIR`.

### Extending
- Add more entities (`companies`, `deals`, `owners`) by copying the pattern: Bronze → Silver → Deltas → Push
- Expand `configs/field_mappings.yaml` with additional properties and directions
- Add de-duplication and survivorship rules in the Silver unify step as needed

### Disclaimer
This is a production-grade scaffold. You should review and adapt mappings, secrets handling, rate limits, and retry policies before go-live.

