# Databricks notebook source
# MAGIC %pip install pyyaml requests msal tenacity python-dateutil

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from libs.config import load_yaml_config
from libs.helpers import normalize_email
from libs.delta_helpers import upsert_delta

spark = SparkSession.getActiveSession()

cfg = load_yaml_config()
bronze_db = cfg["sources"]["databases"]["bronze"]
silver_db = cfg["sources"]["databases"]["silver"]

mappings = cfg["mappings"]["contacts"]
fields = mappings["fields"]

# COMMAND ----------

# Latest source snapshots per source ID
hs = spark.table(f"{bronze_db}.hubspot_contacts_raw")
hs_latest = hs.withColumn(
    "_rn",
    F.row_number().over(Window.partitionBy("hs_object_id").orderBy(F.col("hs_lastmodifieddate").desc_nulls_last()))
).where(F.col("_rn") == 1).drop("_rn")

crm = spark.table(f"{bronze_db}.dynamics_contacts_raw")
crm_latest = crm.withColumn(
    "_rn",
    F.row_number().over(Window.partitionBy("contactid").orderBy(F.col("modifiedon").desc_nulls_last()))
).where(F.col("_rn") == 1).drop("_rn")

hs_latest.select(
    "hs_object_id","hs_lastmodifieddate","email","firstname","lastname","phone","mobilephone","jobtitle","company"
).write.format("delta").mode("overwrite").saveAsTable(f"{silver_db}.hubspot_contacts_snapshot")

crm_latest.select(
    "contactid","modifiedon","emailaddress1","firstname","lastname","telephone1","mobilephone","jobtitle","company"
).write.format("delta").mode("overwrite").saveAsTable(f"{silver_db}.dynamics_contacts_snapshot")

# COMMAND ----------

normalize_email_udf = F.udf(normalize_email)

# Crosswalk by normalized email
hs_keys = hs_latest.select(
    normalize_email_udf(F.col("email")).alias("email"),
    F.col("hs_object_id")
).where(F.col("email").isNotNull()).dropDuplicates(["email"]).alias("hs")

crm_keys = crm_latest.select(
    normalize_email_udf(F.col("emailaddress1")).alias("email"),
    F.col("contactid")
).where(F.col("email").isNotNull()).dropDuplicates(["email"]).alias("crm")

crosswalk = hs_keys.join(crm_keys, on=["email"], how="full")

# Generate unified_contact_id based on email (or retain previous)
from pyspark.sql.functions import sha2, concat_ws

crosswalk = crosswalk.withColumn(
    "unified_contact_id",
    sha2(concat_ws("|", F.coalesce(F.col("email"), F.lit(""))), 256)
)

crosswalk.select("unified_contact_id","email","hs_object_id","contactid").write.format("delta").mode("overwrite").saveAsTable(f"{silver_db}.id_crosswalk_contacts")

# COMMAND ----------

# Unified contacts with simple precedence: HubSpot then Dynamics
u = crosswalk.alias("x") \
    .join(hs_latest.alias("h"), F.col("x.hs_object_id") == F.col("h.hs_object_id"), "left") \
    .join(crm_latest.alias("d"), F.col("x.contactid") == F.col("d.contactid"), "left")

unified = u.select(
    F.col("x.unified_contact_id").alias("unified_contact_id"),
    F.coalesce(F.col("h.email"), F.col("d.emailaddress1")).alias("email"),
    F.coalesce(F.col("h.firstname"), F.col("d.firstname")).alias("firstname"),
    F.coalesce(F.col("h.lastname"), F.col("d.lastname")).alias("lastname"),
    F.coalesce(F.col("h.phone"), F.col("d.telephone1")).alias("phone"),
    F.coalesce(F.col("h.mobilephone"), F.col("d.mobilephone")).alias("mobilephone"),
    F.coalesce(F.col("h.jobtitle"), F.col("d.jobtitle")).alias("jobtitle"),
    F.coalesce(F.col("h.company"), F.col("d.company")).alias("company"),
)

unified.write.format("delta").mode("overwrite").saveAsTable(f"{silver_db}.contacts_unified")
