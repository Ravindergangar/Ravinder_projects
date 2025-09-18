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
# COMMAND ----------

from libs.dynamics_mapping import translate_display_to_backend

# Enhance payload translation for OptionSets/Lookups using config

cfg_full = cfg

# Databricks notebook source
# MAGIC %pip install pyyaml requests msal tenacity python-dateutil

# COMMAND ----------

import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from libs.config import load_yaml_config, get_secret
from libs.audit import log_audit, log_dead_letter
from libs.dynamics_client import DynamicsClient, DynamicsError

spark = SparkSession.getActiveSession()

cfg = load_yaml_config()
gold_db = cfg["sources"]["databases"]["gold"]
meta_db = cfg["sources"]["databases"]["meta"]

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

# Load up to 5 pending payloads for debug
ob = spark.table(f"{gold_db}.dynamics_outbox_contacts").where(F.col("status") == "PENDING").limit(5)
rows = ob.collect()

for r in rows:
    payload = json.loads(r["payload"]) if r["payload"] else {}
    if not r["contactid"]:
        log_dead_letter(spark, meta_db, system="dynamics", entity="contacts", key=r["unified_contact_id"], reason="Missing contactid", payload=payload)
        continue

    try:
        # Translate display fields (OptionSet/Lookup) to backend values before patch
        tr_payload = translate_display_to_backend(client, cfg_full, payload)
        client.patch_contact(contact_id=r["contactid"], payload=tr_payload)
        # mark success for this row with SQL update to avoid overwriting others
        spark.sql(f"""
        UPDATE {gold_db}.dynamics_outbox_contacts
        SET status = 'SUCCESS', last_attempt_ts = current_timestamp(), attempt = attempt + 1
        WHERE unified_contact_id = '{r['unified_contact_id']}'
        """)
        log_audit(spark, meta_db, "dynamics", "contacts", "push", request_id=f"push-{int(datetime.utcnow().timestamp())}", status="SUCCESS", http_code=200, message=f"Patched {r['contactid']}")
    except DynamicsError as e:
        log_audit(spark, meta_db, "dynamics", "contacts", "push", request_id=f"push-{int(datetime.utcnow().timestamp())}", status="FAILED", http_code=500, message=str(e), payload=payload)
