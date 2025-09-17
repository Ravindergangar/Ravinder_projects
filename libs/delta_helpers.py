from typing import Iterable, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def upsert_delta(
    spark: SparkSession,
    df: DataFrame,
    full_table_name: str,
    merge_keys: List[str],
) -> None:
    from delta.tables import DeltaTable

    if spark._jsparkSession.catalog().tableExists(full_table_name):  # type: ignore
        target = DeltaTable.forName(spark, full_table_name)
        cond = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
        (
            target.alias("target")
            .merge(df.alias("source"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)


def latest_by_ts(df: DataFrame, key_cols: List[str], ts_col: str) -> DataFrame:
    w = Window.partitionBy(*[F.col(c) for c in key_cols]).orderBy(F.col(ts_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")

