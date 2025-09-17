from __future__ import annotations

from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, sha2, concat_ws

from sync_engine.config import load_settings, load_mappings, ObjectMapping


def run(objects: List[str], settings_path: str, mappings_path: str) -> None:
    spark = SparkSession.builder.getOrCreate()
    settings = load_settings(settings_path)
    mappings = load_mappings(mappings_path)

    for obj in objects:
        obj_cfg: ObjectMapping = mappings[obj]
        silver_tbl = f"{settings.database}.{settings.silver_schema}.{obj}"
        gold_tbl = f"{settings.database}.{settings.gold_schema}.{obj}_scd2"

        # Minimal SCD2 using MERGE with match on natural key and change hash
        df = spark.table(silver_tbl)
        # Create a change hash from all non-key columns
        non_key_cols = [c for c in df.columns if c not in set(obj_cfg.business_key)]
        df = df.withColumn("record_hash", sha2(concat_ws("||", *[df[c].cast("string") for c in non_key_cols]), 256))

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {gold_tbl} (
              {', '.join([f'{bk} STRING' for bk in obj_cfg.business_key])},
              record_hash STRING,
              is_current BOOLEAN,
              effective_start_at TIMESTAMP,
              effective_end_at TIMESTAMP
            ) USING delta
            """
        )

        df.createOrReplaceTempView("src")
        spark.sql(
            f"""
            MERGE INTO {gold_tbl} AS t
            USING src AS s
            ON { ' AND '.join([f't.{bk} <=> s.{bk}' for bk in obj_cfg.business_key]) }
            WHEN MATCHED AND t.is_current = true AND t.record_hash <> s.record_hash THEN
              UPDATE SET t.is_current = false, t.effective_end_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT ({', '.join(obj_cfg.business_key)}, record_hash, is_current, effective_start_at, effective_end_at)
              VALUES ({', '.join([f's.{bk}' for bk in obj_cfg.business_key])}, s.record_hash, true, current_timestamp(), NULL)
            """
        )

