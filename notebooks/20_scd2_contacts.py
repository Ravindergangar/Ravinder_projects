# Databricks notebook source
# MAGIC %pip install pyyaml requests msal tenacity python-dateutil

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from libs.config import load_yaml_config

spark = SparkSession.getActiveSession()

cfg = load_yaml_config()
silver_db = cfg["sources"]["databases"]["silver"]

# Unified snapshot
u = spark.table(f"{silver_db}.contacts_unified")

# Create SCD2 table if not exists
spark.sql(f"CREATE TABLE IF NOT EXISTS {silver_db}.contacts_unified_scd2 (\n  unified_contact_id STRING,\n  email STRING,\n  firstname STRING,\n  lastname STRING,\n  phone STRING,\n  mobilephone STRING,\n  jobtitle STRING,\n  company STRING,\n  valid_from TIMESTAMP,\n  valid_to TIMESTAMP,\n  is_current BOOLEAN,\n  checksum STRING\n) USING DELTA")

# Compute checksum
checksum_cols = ["email","firstname","lastname","phone","mobilephone","jobtitle","company"]
chk = F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("")) for c in checksum_cols]), 256).alias("checksum")

current = u.select("unified_contact_id", *checksum_cols).withColumn("checksum", chk)

# Load existing current rows
try:
    scd = spark.table(f"{silver_db}.contacts_unified_scd2")
    current_rows = scd.where(F.col("is_current") == True).select("unified_contact_id","checksum","valid_from")
except Exception:
    current_rows = spark.createDataFrame([], "unified_contact_id string, checksum string, valid_from timestamp")

# Join to detect changes
joined = current.alias("c").join(current_rows.alias("p"), on=["unified_contact_id"], how="left")

inserts = joined.where((F.col("p.checksum").isNull()) | (F.col("p.checksum") != F.col("c.checksum"))) \
    .select("c.*") \
    .withColumn("valid_from", F.current_timestamp()) \
    .withColumn("valid_to", F.lit(None).cast("timestamp")) \
    .withColumn("is_current", F.lit(True))

# Close out previous versions for changed rows
changed_ids = inserts.select("unified_contact_id").distinct()
prev = None
try:
    prev = spark.table(f"{silver_db}.contacts_unified_scd2")
except Exception:
    prev = None

if prev is not None and prev.columns:
    to_close = prev.where(F.col("is_current") == True).join(changed_ids, on=["unified_contact_id"], how="inner") \
        .withColumn("is_current", F.lit(False)) \
        .withColumn("valid_to", F.current_timestamp())
    if to_close.count() > 0:
        # Upsert by full replace of affected keys (simplify with MERGE if needed)
        others = prev.join(changed_ids, on=["unified_contact_id"], how="left_anti")
        updated = others.unionByName(to_close.select(prev.columns))
        updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{silver_db}.contacts_unified_scd2")

# Append new versions
if inserts.count() > 0:
    inserts.select("unified_contact_id", *checksum_cols, "valid_from", "valid_to", "is_current", "checksum") \
        .write.format("delta").mode("append").saveAsTable(f"{silver_db}.contacts_unified_scd2")
