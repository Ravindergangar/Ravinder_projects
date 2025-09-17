from typing import Optional
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


WATERMARK_TABLE_SCHEMA = StructType([
    StructField("entity", StringType(), False),
    StructField("source", StringType(), False),
    StructField("watermark_ts", TimestampType(), False),
    StructField("updated_at", TimestampType(), False),
])


def ensure_watermark_table(spark: SparkSession, meta_db: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {meta_db}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {meta_db}.watermarks (
            entity STRING,
            source STRING,
            watermark_ts TIMESTAMP,
            updated_at TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (entity)
        """
    )


def get_watermark(
    spark: SparkSession,
    meta_db: str,
    entity: str,
    source: str,
    default_iso: str,
) -> datetime:
    df = (
        spark.table(f"{meta_db}.watermarks")
        .where((F.col("entity") == entity) & (F.col("source") == source))
        .select("watermark_ts", "updated_at")
    )
    row = df.orderBy(F.col("updated_at").desc()).limit(1).collect()
    if row:
        # Access by positional index to avoid runtime issues with name-based indexing
        return row[0][0]
    return datetime.fromisoformat(default_iso.replace("Z", "+00:00"))


def set_watermark(
    spark: SparkSession,
    meta_db: str,
    entity: str,
    source: str,
    watermark_ts: datetime,
) -> None:
    now = datetime.utcnow()
    new_row = spark.createDataFrame(
        [(entity, source, watermark_ts, now)],
        schema=WATERMARK_TABLE_SCHEMA,
    )
    new_row.write.format("delta").mode("append").saveAsTable(f"{meta_db}.watermarks")

