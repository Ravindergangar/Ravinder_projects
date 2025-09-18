from datetime import datetime
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def ensure_audit_tables(spark: SparkSession, meta_db: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {meta_db}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {meta_db}.audit_log (
            ts TIMESTAMP,
            system STRING,
            entity STRING,
            operation STRING,
            request_id STRING,
            status STRING,
            http_code INT,
            message STRING,
            payload STRING
        ) USING DELTA
        PARTITIONED BY (system, entity)
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {meta_db}.dead_letter (
            ts TIMESTAMP,
            system STRING,
            entity STRING,
            key STRING,
            reason STRING,
            payload STRING
        ) USING DELTA
        PARTITIONED BY (system, entity)
        """
    )


def log_audit(
    spark: SparkSession,
    meta_db: str,
    system: str,
    entity: str,
    operation: str,
    request_id: str,
    status: str,
    http_code: Optional[int],
    message: str,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    payload_str = None
    if payload is not None:
        import json

        payload_str = json.dumps(payload, ensure_ascii=False)
    data = [
        (
            system,
            entity,
            operation,
            request_id,
            status,
            int(http_code) if http_code is not None else None,
            message,
            payload_str,
        )
    ]
    schema = StructType([
        StructField("system", StringType(), True),
        StructField("entity", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("request_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("http_code", IntegerType(), True),
        StructField("message", StringType(), True),
        StructField("payload", StringType(), True),
    ])
    df = spark.createDataFrame(data, schema=schema)
    # Enforce target types
    df = (
        df.withColumn("http_code", F.col("http_code").cast("int"))
          .withColumn("message", F.col("message").cast("string"))
          .withColumn("payload", F.col("payload").cast("string"))
          .withColumn("ts", F.current_timestamp())
    )
    df = df.select(
        F.col("ts").cast("timestamp").alias("ts"),
        F.col("system").cast("string").alias("system"),
        F.col("entity").cast("string").alias("entity"),
        F.col("operation").cast("string").alias("operation"),
        F.col("request_id").cast("string").alias("request_id"),
        F.col("status").cast("string").alias("status"),
        F.col("http_code").cast("int").alias("http_code"),
        F.col("message").cast("string").alias("message"),
        F.col("payload").cast("string").alias("payload"),
    )
    df.write.format("delta").mode("append").saveAsTable(f"{meta_db}.audit_log")


def log_dead_letter(
    spark: SparkSession,
    meta_db: str,
    system: str,
    entity: str,
    key: str,
    reason: str,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    payload_str = None
    if payload is not None:
        import json

        payload_str = json.dumps(payload, ensure_ascii=False)
    data = [(system, entity, key, reason, payload_str)]
    schema = StructType([
        StructField("system", StringType(), True),
        StructField("entity", StringType(), True),
        StructField("key", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("payload", StringType(), True),
    ])
    df = spark.createDataFrame(data, schema=schema).withColumn("ts", F.current_timestamp())
    df = df.select("ts", "system", "entity", "key", "reason", "payload")
    df.write.format("delta").mode("append").saveAsTable(f"{meta_db}.dead_letter")

