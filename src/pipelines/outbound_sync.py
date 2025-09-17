from __future__ import annotations

from typing import Dict, List

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from sync_engine.config import load_settings, load_mappings, FieldMapping, ObjectMapping
from sync_engine.http_client import ResilientHttpClient
from sync_engine.secrets import get_secret
from sync_engine.logging_utils import get_logger
from sync_engine.delta_utils import merge_incremental


logger = get_logger(__name__)


def _filter_fields_for_direction(fields: List[FieldMapping], direction: str, outbound_to: str) -> List[FieldMapping]:
    if outbound_to == "hubspot":
        allowed = {"d365_to_hubspot", "bidirectional"}
    else:
        allowed = {"hubspot_to_d365", "bidirectional"}
    return [f for f in fields if f.direction in allowed]


def _apply_merge_policy(existing: Dict[str, str], updates: Dict[str, str], fields: List[FieldMapping]) -> Dict[str, str]:
    result = dict(existing)
    for fm in fields:
        if fm.merge_policy == "overwrite":
            if fm.target_field in updates:
                result[fm.target_field] = updates[fm.target_field]
        elif fm.merge_policy == "fill_if_missing":
            if (not result.get(fm.target_field)) and fm.target_field in updates and updates[fm.target_field] not in (None, ""):
                result[fm.target_field] = updates[fm.target_field]
    return result


def run(objects: List[str], settings_path: str, mappings_path: str, secret_scope: str | None) -> None:
    spark = SparkSession.builder.getOrCreate()
    settings = load_settings(settings_path)
    mappings = load_mappings(mappings_path)

    hubspot_token = get_secret(secret_scope, "HUBSPOT_TOKEN", "HUBSPOT_TOKEN")
    d365_access_token = get_secret(secret_scope, "D365_ACCESS_TOKEN", "D365_ACCESS_TOKEN")
    hub_client = ResilientHttpClient(base_url=settings.hubspot_base_url, default_headers={"Authorization": f"Bearer {hubspot_token}"})
    d365_client = ResilientHttpClient(base_url=settings.d365_resource_url, default_headers={"Authorization": f"Bearer {d365_access_token}"})

    for obj in objects:
        obj_cfg: ObjectMapping = mappings[obj]
        silver_tbl = f"{settings.database}.{settings.silver_schema}.{obj}"
        audit_tbl = f"{settings.database}.{settings.audit_schema}.outbound_attempts"

        df = spark.table(silver_tbl)

        # TODO: Identify changed records since last outbound sync using audit/gold
        rows = df.limit(1000).collect()  # placeholder batching

        # Prepare outbound payloads
        to_hubspot_fields = _filter_fields_for_direction(obj_cfg.fields, direction="outbound", outbound_to="hubspot")
        to_d365_fields = _filter_fields_for_direction(obj_cfg.fields, direction="outbound", outbound_to="d365")

        for r in rows:
            src = {c: r[c] for c in df.columns}
            hs_payload = {fm.source_field: src.get(fm.target_field) for fm in to_hubspot_fields}
            d_payload = {fm.target_field: src.get(fm.target_field) for fm in to_d365_fields}

            try:
                if to_hubspot_fields:
                    hub_client.patch_json(f"crm/v3/objects/{obj}", json={"properties": hs_payload})
                if to_d365_fields:
                    d365_client.patch_json(f"api/data/v9.2/{obj}", json=d_payload)
                # audit success
                payload = {"obj": obj, "status": "success"}
                spark.createDataFrame([payload]).write.format("delta").mode("append").saveAsTable(audit_tbl)
            except Exception as e:
                logger.error(f"Outbound sync failed for {obj}: {e}")
                payload = {"obj": obj, "status": "error", "message": str(e)}
                spark.createDataFrame([payload]).write.format("delta").mode("append").saveAsTable(audit_tbl)

