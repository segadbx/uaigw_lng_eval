# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Default `system.ai` access (M1)
# MAGIC Shows who can call system-provided model services directly (bypassing `trading_copilot`).
# MAGIC **Read-only by default.** It revokes nothing unless `confirm_shared_changes = true`, and it
# MAGIC prints the exact statements first. `system.ai` is shared by every workspace user on this
# MAGIC metastore, so locking it down affects other demos too. `restore_statements` lets you undo it.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.157.0" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys, json
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
p = lngdemo.get_params(dbutils)
CAT, CUR = p["catalog"], p["audit_curated_schema"]

def grants(securable_type, name):
    try:
        resp = w.grants.get(securable_type=securable_type, full_name=name)
        return [{"principal": a.principal, "privileges": [str(x.value if hasattr(x, "value") else x) for x in (a.privileges or [])]}
                for a in (resp.privilege_assignments or [])]
    except Exception as e:  # noqa: BLE001
        return [{"error": str(e)[:300]}]

current = {
    "catalog system": grants("catalog", "system"),
    "schema system.ai": grants("schema", "system.ai"),
}
print(json.dumps(current, indent=2))

# COMMAND ----------

# Plan: remove EXECUTE for `account users` on schema system.ai, keep USE for the gateway owner.
targets = [a for a in current["schema system.ai"] if a.get("principal") == "account users" and "EXECUTE" in a.get("privileges", [])]
plan = ["REVOKE EXECUTE ON SCHEMA system.ai FROM `account users`"] if targets else []
restore = ["GRANT EXECUTE ON SCHEMA system.ai TO `account users`"] if targets else []
print("PLAN:", plan or "nothing to change (no direct EXECUTE for `account users` on system.ai)")
print("RESTORE:", restore)

if plan and lngdemo.shared_changes_confirmed(p):
    for s in plan:
        spark.sql(s)
        print("executed:", s)
elif plan:
    print("confirm_shared_changes != true -> not executed.")

spark.sql(f"""
MERGE INTO {lngdemo.fq(CAT, CUR, 'setup_state')} t
USING (SELECT 'system_ai_restore_statements' AS key, :v AS value) s ON t.key = s.key
WHEN MATCHED THEN UPDATE SET value = s.value, updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (key, value, updated_at) VALUES (s.key, s.value, current_timestamp())
""", args={"v": json.dumps({"before": current, "plan": plan, "restore": restore,
                             "executed": bool(plan and lngdemo.shared_changes_confirmed(p))})})
