from datetime import datetime
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    StringType,
    IntegerType,
)
from pyspark.sql import Row


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
    now = datetime.utcnow()
    payload_str = None
    if payload is not None:
        import json

        payload_str = json.dumps(payload, ensure_ascii=False)
    row = Row(
        ts=now,
        system=system,
        entity=entity,
        operation=operation,
        request_id=request_id,
        status=status,
        http_code=(int(http_code) if http_code is not None else None),
        message=message,
        payload=payload_str,
    )
    df = spark.createDataFrame([row]).selectExpr(
        "cast(ts as timestamp) as ts",
        "cast(system as string) as system",
        "cast(entity as string) as entity",
        "cast(operation as string) as operation",
        "cast(request_id as string) as request_id",
        "cast(status as string) as status",
        "cast(http_code as int) as http_code",
        "cast(message as string) as message",
        "cast(payload as string) as payload",
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
    now = datetime.utcnow()
    payload_str = None
    if payload is not None:
        import json

        payload_str = json.dumps(payload, ensure_ascii=False)
    row = Row(
        ts=now,
        system=system,
        entity=entity,
        key=key,
        reason=reason,
        payload=payload_str,
    )
    df = spark.createDataFrame([row]).selectExpr(
        "cast(ts as timestamp) as ts",
        "cast(system as string) as system",
        "cast(entity as string) as entity",
        "cast(key as string) as key",
        "cast(reason as string) as reason",
        "cast(payload as string) as payload",
    )
    df.write.format("delta").mode("append").saveAsTable(f"{meta_db}.dead_letter")

