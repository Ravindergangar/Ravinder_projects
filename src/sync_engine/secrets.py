from __future__ import annotations

import os
from typing import Optional

try:
    from pyspark.dbutils import DBUtils  # type: ignore
except Exception:  # pragma: no cover
    DBUtils = None  # type: ignore


def get_secret(scope: Optional[str], key: str, fallback_env: Optional[str] = None) -> str:
    if scope and DBUtils is not None:
        try:
            # In Databricks, spark is implicitly available
            from pyspark.sql import SparkSession  # type: ignore
            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)
            return dbutils.secrets.get(scope=scope, key=key)  # type: ignore
        except Exception:
            pass
    if fallback_env:
        value = os.getenv(fallback_env)
        if value:
            return value
    value = os.getenv(key)
    if value:
        return value
    raise RuntimeError(f"Secret {key} not found in scope {scope} or environment")

