from __future__ import annotations

from typing import Dict, List, Tuple
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, sha2, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from sync_engine.config import load_settings, load_mappings, ObjectMapping
from sync_engine.http_client import ResilientHttpClient
from sync_engine.secrets import get_secret
from sync_engine.delta_utils import ensure_database_and_schemas, merge_incremental
from sync_engine.logging_utils import get_logger


logger = get_logger(__name__)


def _hubspot_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _d365_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _get_hubspot_increment(
    client: ResilientHttpClient,
    object_name: str,
    watermark_field: str,
    since_ts_iso: str,
) -> List[Dict]:
    # HubSpot CRM v3 paging example (simplified). Adjust per object.
    results: List[Dict] = []
    after = None
    while True:
        params = {"limit": 100, "sorts": f"-{watermark_field}"}
        if after:
            params["after"] = after
        resp = client.get_json(f"crm/v3/objects/{object_name}", params=params)
        for row in resp.get("results", []):
            if row.get("properties", {}).get(watermark_field) and row["properties"][watermark_field] >= since_ts_iso:
                results.append(row)
        paging = resp.get("paging", {}).get("next", {})
        after = paging.get("after")
        if not after:
            break
    return results


def _get_d365_increment(
    client: ResilientHttpClient,
    entity_set: str,
    watermark_field: str,
    since_ts_iso: str,
) -> List[Dict]:
    results: List[Dict] = []
    next_link = f"api/data/v9.2/{entity_set}?$filter={watermark_field} ge {since_ts_iso}&$top=5000&$orderby={watermark_field} desc"
    while next_link:
        resp = client.get_json(next_link)
        value = resp.get("value", [])
        results.extend(value)
        next_link = resp.get("@odata.nextLink")
    return results


def run(objects: List[str], settings_path: str, mappings_path: str, secret_scope: str | None) -> None:
    spark = SparkSession.builder.getOrCreate()
    settings = load_settings(settings_path)
    mappings = load_mappings(mappings_path)

    ensure_database_and_schemas(
        spark,
        database=settings.database,
        schemas={
            "bronze": settings.bronze_schema,
            "silver": settings.silver_schema,
            "gold": settings.gold_schema,
            "audit": settings.audit_schema,
        },
    )

    hubspot_token = get_secret(secret_scope, "HUBSPOT_TOKEN", "HUBSPOT_TOKEN")
    d365_access_token = get_secret(secret_scope, "D365_ACCESS_TOKEN", "D365_ACCESS_TOKEN")

    hub_client = ResilientHttpClient(base_url=settings.hubspot_base_url, default_headers=_hubspot_headers(hubspot_token))
    d365_client = ResilientHttpClient(base_url=settings.d365_resource_url, default_headers=_d365_headers(d365_access_token))

    for obj in objects:
        obj_cfg: ObjectMapping = mappings[obj]
        # Control: get last watermark from a control table (simplified: table property or default epoch)
        control_tbl = f"{settings.database}.{settings.audit_schema}.watermarks"
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {control_tbl} (
              source STRING,
              object STRING,
              watermark_ts STRING,
              updated_at TIMESTAMP
            ) USING delta
            """
        )
        wm = spark.sql(
            f"SELECT watermark_ts FROM {control_tbl} WHERE source='hubspot' AND object='{obj}' ORDER BY updated_at DESC LIMIT 1"
        ).collect()
        since_hs = wm[0][0] if wm else "1970-01-01T00:00:00Z"

        wm2 = spark.sql(
            f"SELECT watermark_ts FROM {control_tbl} WHERE source='d365' AND object='{obj}' ORDER BY updated_at DESC LIMIT 1"
        ).collect()
        since_d = wm2[0][0] if wm2 else "1970-01-01T00:00:00Z"

        # Fetch increments
        hs_rows = _get_hubspot_increment(hub_client, obj, obj_cfg.watermark_field_hubspot, since_hs)
        d_rows = _get_d365_increment(d365_client, obj, obj_cfg.watermark_field_d365, since_d)

        # Write to bronze tables
        hs_df = spark.createDataFrame([{
            "source": "hubspot",
            "object": obj,
            "payload_json": json.dumps(r, default=str),
            "ingested_at": None,
        } for r in hs_rows], schema=StructType([
            StructField("source", StringType(), False),
            StructField("object", StringType(), False),
            StructField("payload_json", StringType(), True),
            StructField("ingested_at", TimestampType(), True),
        ])).withColumn("ingested_at", current_timestamp())

        d_df = spark.createDataFrame([{
            "source": "d365",
            "object": obj,
            "payload_json": json.dumps(r, default=str),
            "ingested_at": None,
        } for r in d_rows], schema=hs_df.schema).withColumn("ingested_at", current_timestamp())

        bronze_tbl = f"{settings.database}.{settings.bronze_schema}.{obj}"
        union_df = hs_df.unionByName(d_df, allowMissingColumns=True)
        # deduplicate on payload hash to avoid duplicates on retries
        union_df = union_df.withColumn(
            "payload_hash",
            sha2(concat_ws("||", col("source"), col("object"), col("payload_json")), 256),
        )
        merge_incremental(
            target_table=bronze_tbl,
            source_df=union_df,
            merge_condition_sql="t.payload_hash = s.payload_hash",
        )

        # Update watermarks
        if hs_rows:
            latest_hs = max(r.get("properties", {}).get(obj_cfg.watermark_field_hubspot, since_hs) for r in hs_rows)
            spark.sql(f"INSERT INTO {control_tbl} VALUES ('hubspot', '{obj}', '{latest_hs}', current_timestamp())")
        if d_rows:
            latest_d = max(r.get(obj_cfg.watermark_field_d365, since_d) for r in d_rows)
            spark.sql(f"INSERT INTO {control_tbl} VALUES ('d365', '{obj}', '{latest_d}', current_timestamp())")

        logger.info(f"Bronze ingested {obj}: hs={len(hs_rows)} d365={len(d_rows)}")

