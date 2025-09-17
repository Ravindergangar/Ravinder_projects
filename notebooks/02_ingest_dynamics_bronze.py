# Databricks notebook source
# MAGIC %pip install pyyaml requests msal tenacity python-dateutil

# COMMAND ----------

from datetime import datetime
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from libs.config import load_yaml_config, get_secret
from libs.watermarks import get_watermark, set_watermark
from libs.audit import log_audit
from libs.dynamics_client import DynamicsClient

spark = SparkSession.getActiveSession()

cfg = load_yaml_config()
bronze_db = cfg["sources"]["databases"]["bronze"]
meta_db = cfg["sources"]["databases"]["meta"]
d_select = cfg["sources"]["dynamics"]["select"]
wm_default = cfg["sources"]["watermarks"]["contacts"]["default_start"]

# Secrets
TENANT = get_secret("D365_TENANT_ID")
CLIENT_ID = get_secret("D365_CLIENT_ID")
CLIENT_SECRET = get_secret("D365_CLIENT_SECRET")
ORG_URI = get_secret("D365_ORG_URI")

assert TENANT and CLIENT_ID and CLIENT_SECRET and ORG_URI, "Missing Dynamics credentials"

client = DynamicsClient(
    tenant_id=TENANT,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    org_uri=ORG_URI,
    api_version=cfg["sources"]["dynamics"]["api_version"],
)

# COMMAND ----------

# Watermark
wm = get_watermark(spark, meta_db, entity="contacts", source="dynamics", default_iso=wm_default)
wm_iso = wm.isoformat().replace("+00:00", "Z")

records, max_modified = client.list_contacts(select=d_select, modifiedon_gte_iso=wm_iso)

load_ts = datetime.utcnow()

rows = []
for r in records:
    rows.append(
        (
            load_ts,
            r.get("contactid"),
            r.get("modifiedon"),
            r.get("emailaddress1"),
            r.get("firstname"),
            r.get("lastname"),
            r.get("telephone1"),
            r.get("mobilephone"),
            r.get("jobtitle"),
            r.get("company"),
            json.dumps(r),
        )
    )

schema = "load_ts timestamp, contactid string, modifiedon timestamp, emailaddress1 string, firstname string, lastname string, telephone1 string, mobilephone string, jobtitle string, company string, raw string"
df = spark.createDataFrame(rows, schema=schema) if rows else spark.createDataFrame([], schema=schema)

if df.count() > 0:
    df.write.format("delta").mode("append").saveAsTable(f"{bronze_db}.dynamics_contacts_raw")

    # Advance watermark only if ingested
    if max_modified:
        max_dt = datetime.fromisoformat(max_modified.replace("Z", "+00:00"))
        set_watermark(spark, meta_db, entity="contacts", source="dynamics", watermark_ts=max_dt)

# Audit
log_audit(
    spark,
    meta_db,
    system="dynamics",
    entity="contacts",
    operation="ingest",
    request_id=f"load-{int(load_ts.timestamp())}",
    status="SUCCESS",
    http_code=200,
    message=f"Ingested {df.count()} records",
    payload={"since": wm_iso, "max": max_modified},
)
