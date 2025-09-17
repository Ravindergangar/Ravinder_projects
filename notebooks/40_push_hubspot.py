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
# MAGIC %pip install -r /Workspace/Repos/ignore/requirements.txt

# COMMAND ----------

import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from libs.config import load_yaml_config, get_secret
from libs.audit import log_audit, log_dead_letter
from libs.hubspot_client import HubSpotClient, HubSpotError

spark = SparkSession.getActiveSession()

cfg = load_yaml_config()
gold_db = cfg["sources"]["databases"]["gold"]
meta_db = cfg["sources"]["databases"]["meta"]

HUBSPOT_TOKEN = get_secret("HUBSPOT_TOKEN")
assert HUBSPOT_TOKEN, "Missing HUBSPOT_TOKEN"

client = HubSpotClient(HUBSPOT_TOKEN, base_url=cfg["sources"]["hubspot"]["base_url"])

# COMMAND ----------

# Load pending outbox (batch up to 90 to be safe)
ob = spark.table(f"{gold_db}.hubspot_outbox_contacts").where(F.col("status") == "PENDING").limit(90)
rows = ob.collect()

if rows:
    inputs = []
    for r in rows:
        payload = json.loads(r["payload"]) if r["payload"] else {}
        if not r["hs_object_id"]:
            # skip if no id
            log_dead_letter(
                spark, meta_db, system="hubspot", entity="contacts", key=r["unified_contact_id"], reason="Missing hs_object_id", payload=payload
            )
            continue
        inputs.append({"id": r["hs_object_id"], "properties": payload})

    try:
        resp = client.batch_update_contacts(inputs)
        # Mark each as success via SQL update
        ids = [r["unified_contact_id"] for r in rows]
        for uid in ids:
            spark.sql(f"""
            UPDATE {gold_db}.hubspot_outbox_contacts
            SET status = 'SUCCESS', last_attempt_ts = current_timestamp(), attempt = attempt + 1
            WHERE unified_contact_id = '{uid}'
            """)
        log_audit(spark, meta_db, "hubspot", "contacts", "push", request_id=f"push-{int(datetime.utcnow().timestamp())}", status="SUCCESS", http_code=200, message=f"Pushed {len(inputs)} records")
    except HubSpotError as e:
        # Record failure but leave as PENDING for retry, log audit and dead-letter if needed
        log_audit(spark, meta_db, "hubspot", "contacts", "push", request_id=f"push-{int(datetime.utcnow().timestamp())}", status="FAILED", http_code=500, message=str(e))
