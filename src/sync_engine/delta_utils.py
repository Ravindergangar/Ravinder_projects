from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit


def ensure_database_and_schemas(spark: SparkSession, database: str, schemas: Dict[str, str]) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    for schema in schemas.values():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")


def merge_incremental(
    target_table: str,
    source_df: DataFrame,
    merge_condition_sql: str,
    update_all: bool = True,
) -> None:
    from delta.tables import DeltaTable  # type: ignore

    spark = source_df.sparkSession
    if spark.catalog.tableExists(target_table):
        target = DeltaTable.forName(spark, target_table)
        merge_builder = target.alias("t").merge(source_df.alias("s"), merge_condition_sql)
        if update_all:
            merge_builder.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            merge_builder.whenNotMatchedInsertAll().execute()
    else:
        source_df.write.format("delta").mode("overwrite").saveAsTable(target_table)


def upsert_change_audit(
    audit_table: str,
    df: DataFrame,
) -> None:
    merge_incremental(
        target_table=audit_table,
        source_df=df,
        merge_condition_sql="t.change_id = s.change_id",
        update_all=False,
    )

