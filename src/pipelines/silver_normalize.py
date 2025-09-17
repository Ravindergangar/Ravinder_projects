from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when, to_timestamp, current_timestamp, get_json_object

from sync_engine.config import load_settings, load_mappings, ObjectMapping, FieldMapping
from sync_engine.delta_utils import merge_incremental


def _extract_payload(df: DataFrame) -> DataFrame:
    # Payload is a map for HubSpot, and raw dict for D365; normalize keys
    return df


def _normalize_object(spark: SparkSession, obj: str, obj_cfg: ObjectMapping, database: str, bronze_schema: str, silver_schema: str) -> None:
    bronze_tbl = f"{database}.{bronze_schema}.{obj}"
    silver_tbl = f"{database}.{silver_schema}.{obj}"

    df = spark.table(bronze_tbl)

    # Explode into columns per mapping unioning hubspot and d365 rows (payload_json)
    select_exprs = []
    for fm in obj_cfg.fields:
        select_exprs.append(
            when(
                col("source") == "hubspot",
                get_json_object(col("payload_json"), f"$.properties.{fm.source_field}")
            ).otherwise(
                get_json_object(col("payload_json"), f"$.{fm.target_field}")
            ).alias(fm.target_field)
        )
    # Business key columns
    for bk in obj_cfg.business_key:
        select_exprs.append(
            when(
                col("source") == "hubspot",
                get_json_object(col("payload_json"), f"$.properties.{bk}")
            ).otherwise(
                get_json_object(col("payload_json"), f"$.{bk}")
            ).alias(bk)
        )

    select_exprs.append(col("source"))
    select_exprs.append(col("ingested_at"))

    norm = df.select(*select_exprs).withColumn("normalized_at", current_timestamp())

    # Merge on business key
    if not obj_cfg.business_key:
        raise ValueError(f"Business key required for {obj}")
    on = " AND ".join([f"t.{bk} <=> s.{bk}" for bk in obj_cfg.business_key])

    merge_incremental(
        target_table=silver_tbl,
        source_df=norm,
        merge_condition_sql=on,
    )


def run(objects: List[str], settings_path: str, mappings_path: str) -> None:
    spark = SparkSession.builder.getOrCreate()
    settings = load_settings(settings_path)
    mappings = load_mappings(mappings_path)
    for obj in objects:
        _normalize_object(spark, obj, mappings[obj], settings.database, settings.bronze_schema, settings.silver_schema)

