# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Capability probe
# MAGIC Records what this workspace actually exposes (writes `audit_curated.capability_matrix`):
# MAGIC model services and their effective owner, inference-table / rate-limit config read back from the
# MAGIC API, whether a service-policy API exists in the installed SDK, system-table access, trace table.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.157.0" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys, json
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo
from databricks.sdk import WorkspaceClient, version

w = WorkspaceClient()
p = lngdemo.get_params(dbutils)
CAT, SVC, RAW, CUR = p["catalog"], p["services_schema"], p["audit_raw_schema"], p["audit_curated_schema"]
rows = []

def add(capability, status_doc, observed, method, module):
    rows.append((capability, status_doc, str(observed)[:2000], method, module))

# COMMAND ----------

for role, svc in lngdemo.services(p).items():
    sid = svc["service"].split(".")[-1]
    name = "model-services/" + svc["service"]
    try:
        ms = w.ai_gateway.get_model_service(name)
        cfg = ms.as_dict().get("config", {})
        add(f"model service {sid}", "Beta (bundle)", f"owner={ms.effective_owner}; api_types={ms.supported_api_types}", "bundle resource", "M1")
        add(f"{sid} inference_table via API", "documented UI; schema has config.inference_table",
            json.dumps(cfg.get("inference_table")), "bundle resource (config.inference_table)", "M2")
        add(f"{sid} rate_limits via API", "documented UI; schema has config.rate_limits",
            json.dumps(cfg.get("rate_limits")), "bundle resource (config.rate_limits)", "M2")
    except Exception as e:  # noqa: BLE001
        add(f"model service {sid}", "Beta (bundle)", f"ERROR {str(e)[:300]}", "bundle resource", "M1")

policy_methods = [m for m in dir(w.ai_gateway) if "polic" in m.lower()]
add("service policy API in SDK", "UI-only during Beta (docs)", f"sdk {version.__version__}; ai_gateway policy methods={policy_methods}",
    "manual (MANUAL_STEPS.md)", "M4/M5/M7")

for t in ("system.ai_gateway.usage", "system.access.audit"):
    try:
        n = spark.sql(f"SELECT count(*) c FROM {t} WHERE event_time >= current_timestamp() - INTERVAL 1 DAY").first()["c"]
        add(f"read {t}", "GA system table; admin grant needed", f"readable by job runner; rows last 24h={n}", "SQL", "M3/M7")
    except Exception as e:  # noqa: BLE001
        add(f"read {t}", "GA system table; admin grant needed", f"NOT readable: {str(e)[:200]}", "grant by account/metastore admin", "M3/M7")

if p["trace_table"]:
    try:
        cols = [r["col_name"] for r in spark.sql(f"DESCRIBE TABLE {p['trace_table']}").collect()]
        add("unified trace table", "Beta; Enhanced Unity Gateway preview", f"{p['trace_table']} columns={cols[:40]}", "manual setup (metastore admin)", "M3/M4")
    except Exception as e:  # noqa: BLE001
        add("unified trace table", "Beta; Enhanced Unity Gateway preview", f"configured but unreadable: {str(e)[:200]}", "manual", "M3/M4")
else:
    add("unified trace table", "Beta; Enhanced Unity Gateway preview", "trace_table variable empty -> skipped", "manual setup (metastore admin)", "M3/M4")

for role, svc in lngdemo.services(p).items():
    sid = svc["service"].split(".")[-1]
    t = svc["payload"]
    add(f"inference table {svc['payload']}", "auto-created after first request (up to 1h)",
        "exists" if lngdemo.table_exists(spark, t) else "not created yet", "auto", "M2")

mode = spark.sql(f"SELECT value FROM {lngdemo.fq(CAT, CUR, 'setup_state')} WHERE key='contract_protection_mode'").collect()
add("contract-book row filter / masks", "declared in the medallion pipeline (Public Preview)", mode[0]["value"] if mode else "unknown", "pipeline definition (gold/contracts.sql)", "M4")

# COMMAND ----------

df = spark.createDataFrame(rows, "capability STRING, status_doc STRING, observed STRING, deploy_method STRING, module STRING")
df.createOrReplaceTempView("cap_new")
spark.sql(f"""
INSERT INTO {lngdemo.fq(CAT, CUR, 'capability_matrix')}
SELECT capability, status_doc, observed, deploy_method, module, current_timestamp() FROM cap_new
""")
display(df)
