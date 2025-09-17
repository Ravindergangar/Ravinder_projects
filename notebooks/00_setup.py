# Ensure repo root is on sys.path and CONFIG_DIR is set
import sys, os
try:
    nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    repo_root = "/Workspace/" + nb_path.split("/notebooks/")[0]
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    os.environ.setdefault("CONFIG_DIR", repo_root + "/configs")
except Exception:
    pass
# Databricks notebook source
# MAGIC %pip install pyyaml requests msal tenacity python-dateutil

# COMMAND ----------

from pyspark.sql import SparkSession
from libs.config import load_yaml_config
from libs.watermarks import ensure_watermark_table
from libs.audit import ensure_audit_tables

spark = SparkSession.getActiveSession()

cfg = load_yaml_config()
bronze_db = cfg["sources"]["databases"]["bronze"]
silver_db = cfg["sources"]["databases"]["silver"]
gold_db = cfg["sources"]["databases"]["gold"]
meta_db = cfg["sources"]["databases"]["meta"]

# Create DBs
spark.sql(f"CREATE DATABASE IF NOT EXISTS {bronze_db}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_db}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_db}")
ensure_watermark_table(spark, meta_db)
ensure_audit_tables(spark, meta_db)

# COMMAND ----------

# Bronze tables (raw payload + selected fields)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bronze_db}.hubspot_contacts_raw (
  load_ts TIMESTAMP,
  hs_object_id STRING,
  hs_lastmodifieddate TIMESTAMP,
  email STRING,
  firstname STRING,
  lastname STRING,
  phone STRING,
  mobilephone STRING,
  jobtitle STRING,
  company STRING,
  lifecyclestage STRING,
  raw STRING
) USING DELTA
PARTITIONED BY (date(load_ts))
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bronze_db}.dynamics_contacts_raw (
  load_ts TIMESTAMP,
  contactid STRING,
  modifiedon TIMESTAMP,
  emailaddress1 STRING,
  firstname STRING,
  lastname STRING,
  telephone1 STRING,
  mobilephone STRING,
  jobtitle STRING,
  company STRING,
  raw STRING
) USING DELTA
PARTITIONED BY (date(load_ts))
""")

# COMMAND ----------

# Silver tables
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_db}.hubspot_contacts_snapshot (
  hs_object_id STRING,
  hs_lastmodifieddate TIMESTAMP,
  email STRING,
  firstname STRING,
  lastname STRING,
  phone STRING,
  mobilephone STRING,
  jobtitle STRING,
  company STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_db}.dynamics_contacts_snapshot (
  contactid STRING,
  modifiedon TIMESTAMP,
  emailaddress1 STRING,
  firstname STRING,
  lastname STRING,
  telephone1 STRING,
  mobilephone STRING,
  jobtitle STRING,
  company STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_db}.id_crosswalk_contacts (
  unified_contact_id STRING,
  email STRING,
  hs_object_id STRING,
  contactid STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_db}.contacts_unified (
  unified_contact_id STRING,
  email STRING,
  firstname STRING,
  lastname STRING,
  phone STRING,
  mobilephone STRING,
  jobtitle STRING,
  company STRING
) USING DELTA
""")

# COMMAND ----------

# Gold outboxes
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {gold_db}.hubspot_outbox_contacts (
  unified_contact_id STRING,
  hs_object_id STRING,
  payload STRING,
  status STRING,
  attempt INT,
  last_attempt_ts TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {gold_db}.dynamics_outbox_contacts (
  unified_contact_id STRING,
  contactid STRING,
  payload STRING,
  status STRING,
  attempt INT,
  last_attempt_ts TIMESTAMP
) USING DELTA
""")
