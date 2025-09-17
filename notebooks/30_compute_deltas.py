# COMMAND ----------

# Ensure map_from_entries with lambda works across runtimes by using SQL exprs for map building

from pyspark.sql import functions as F

def array_of_kv_to_map(col):
    # col is array<struct<field:string,value:...>> -> map<string, string>
    return F.map_from_arrays(
        F.transform(col, lambda x: F.col("x.field")),
        F.transform(col, lambda x: F.col("x.value"))
    )
# Databricks notebook source
# MAGIC %pip install pyyaml requests msal tenacity python-dateutil

# COMMAND ----------

import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from libs.config import load_yaml_config

spark = SparkSession.getActiveSession()

cfg = load_yaml_config()
silver_db = cfg["sources"]["databases"]["silver"]
gold_db = cfg["sources"]["databases"]["gold"]

maps = cfg["mappings"]["contacts"]
fields = maps["fields"]

# COMMAND ----------

# Load latest snapshots from source for comparison
hs = spark.table(f"{silver_db}.hubspot_contacts_snapshot").alias("h")
dy = spark.table(f"{silver_db}.dynamics_contacts_snapshot").alias("d")
cw = spark.table(f"{silver_db}.id_crosswalk_contacts").alias("x")

# Join to align sources by unified id
joined = cw.join(hs, F.col("x.hs_object_id") == F.col("h.hs_object_id"), "left") \
    .join(dy, F.col("x.contactid") == F.col("d.contactid"), "left")

# Build deltas
updates_to_hubspot = []
updates_to_dynamics = []

for f in fields:
    unified = f["unified"]
    h = f.get("hubspot")
    d = f.get("dynamics")
    direction = f.get("direction", "bi")
    strategy = f.get("strategy", "overwrite")

    if not h or not d:
        continue

    # hubspot payload generation
    h_src = F.col(f"h.{h}")
    d_src = F.col(f"d.{d}")

    # Overwrite: if different -> update target
    # Fill if missing: if target is null and source has value -> update target
    if direction in ("bi", "to_hubspot"):
        cond = (h_src.isNull() & d_src.isNotNull()) if strategy == "fill_if_missing" else (h_src != d_src)
        updates_to_hubspot.append(F.when(cond, F.struct(F.lit(h).alias("field"), d_src.alias("value"))))
    if direction in ("bi", "to_dynamics"):
        cond = (d_src.isNull() & h_src.isNotNull()) if strategy == "fill_if_missing" else (d_src != h_src)
        updates_to_dynamics.append(F.when(cond, F.struct(F.lit(d).alias("field"), h_src.alias("value"))))

# Collapse to per-record payload arrays and filter nulls
h_updates = joined.select(
    F.col("x.unified_contact_id").alias("unified_contact_id"),
    F.col("x.hs_object_id").alias("hs_object_id"),
    F.array([u for u in updates_to_hubspot if u is not None]).alias("updates")
).withColumn("updates", F.expr("filter(updates, x -> x is not null)"))

# HubSpot expects properties map
h_payload = h_updates.where(F.size("updates") > 0).select(
    "unified_contact_id",
    "hs_object_id",
    array_of_kv_to_map(F.col("updates")).alias("properties")
)

# Dynamics payloads
d_updates = joined.select(
    F.col("x.unified_contact_id").alias("unified_contact_id"),
    F.col("x.contactid").alias("contactid"),
    F.array([u for u in updates_to_dynamics if u is not None]).alias("updates")
).withColumn("updates", F.expr("filter(updates, x -> x is not null)"))

# Convert to map for JSON patch body
# For OptionSet/lookup fields, you may need to convert labels to option values or bind lookups.
d_payload = d_updates.where(F.size("updates") > 0).select(
    "unified_contact_id",
    "contactid",
    array_of_kv_to_map(F.col("updates")).alias("properties")
)

# Write outboxes
h_payload.select(
    "unified_contact_id","hs_object_id",F.to_json(F.col("properties")).alias("payload")
).withColumn("status", F.lit("PENDING")).withColumn("attempt", F.lit(0)).withColumn("last_attempt_ts", F.lit(None).cast("timestamp")) \
 .write.format("delta").mode("overwrite").saveAsTable(f"{gold_db}.hubspot_outbox_contacts")


d_payload.select(
    "unified_contact_id","contactid",F.to_json(F.col("properties")).alias("payload")
).withColumn("status", F.lit("PENDING")).withColumn("attempt", F.lit(0)).withColumn("last_attempt_ts", F.lit(None).cast("timestamp")) \
 .write.format("delta").mode("overwrite").saveAsTable(f"{gold_db}.dynamics_outbox_contacts")
