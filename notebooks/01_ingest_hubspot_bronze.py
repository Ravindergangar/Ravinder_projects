# Optional: auto-pick scope via env var
import os
SCOPE = os.environ.get("SECRET_SCOPE")
from libs.config import get_secret
if SCOPE:
    HUBSPOT_TOKEN = get_secret("HUBSPOT_TOKEN", scope=SCOPE) or HUBSPOT_TOKEN
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

from datetime import datetime
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from libs.config import load_yaml_config, get_secret
from libs.watermarks import get_watermark, set_watermark
from libs.audit import log_audit
from libs.hubspot_client import HubSpotClient

spark = SparkSession.getActiveSession()

cfg = load_yaml_config()
bronze_db = cfg["sources"]["databases"]["bronze"]
meta_db = cfg["sources"]["databases"]["meta"]
h_props = cfg["sources"]["hubspot"]["properties"]
wm_default = cfg["sources"]["watermarks"]["contacts"]["default_start"]

# Secrets
HUBSPOT_TOKEN = get_secret("HUBSPOT_TOKEN")
assert HUBSPOT_TOKEN, "Missing HUBSPOT_TOKEN"

client = HubSpotClient(HUBSPOT_TOKEN, base_url=cfg["sources"]["hubspot"]["base_url"])

# COMMAND ----------

# Watermark
wm = get_watermark(spark, meta_db, entity="contacts", source="hubspot", default_iso=wm_default)
wm_iso = wm.isoformat().replace("+00:00", "Z")

records, max_lastmod = client.search_contacts(properties=h_props, last_modified_gte_iso=wm_iso)

load_ts = datetime.utcnow()

rows = []
for r in records:
    props = r.get("properties", {})
    rows.append(
        (
            load_ts,
            load_ts.date(),
            r.get("id"),
            props.get("hs_lastmodifieddate"),
            props.get("email"),
            props.get("firstname"),
            props.get("lastname"),
            props.get("phone"),
            props.get("mobilephone"),
            props.get("jobtitle"),
            props.get("company"),
            props.get("lifecyclestage"),
            json.dumps(r),
        )
    )

schema = "load_ts timestamp, load_date date, hs_object_id string, hs_lastmodifieddate timestamp, email string, firstname string, lastname string, phone string, mobilephone string, jobtitle string, company string, lifecyclestage string, raw string"
df = spark.createDataFrame(rows, schema=schema) if rows else spark.createDataFrame([], schema=schema)

if df.count() > 0:
    df.write.format("delta").mode("append").saveAsTable(f"{bronze_db}.hubspot_contacts_raw")

    # Advance watermark only if we ingested
    if max_lastmod:
        max_dt = datetime.fromisoformat(max_lastmod.replace("Z", "+00:00"))
        set_watermark(spark, meta_db, entity="contacts", source="hubspot", watermark_ts=max_dt)

# Audit
log_audit(
    spark,
    meta_db,
    system="hubspot",
    entity="contacts",
    operation="ingest",
    request_id=f"load-{int(load_ts.timestamp())}",
    status="SUCCESS",
    http_code=200,
    message=f"Ingested {df.count()} records",
    payload={"since": wm_iso, "max": max_lastmod},
)
