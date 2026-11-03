# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Genie agent over the governed gold contract book (M10), runs AFTER 06_contact_pii_abac
# MAGIC Creates (or updates in place) one Genie space over gold `trading_data.contracts`, with curated SQL for the three
# MAGIC demo questions (`lngdemo.GENIE_QUESTIONS`): rows per desk, price terms, contacts. Genie runs every query with
# MAGIC the credentials of **the person asking**, so the same question returns different rows and masks per persona:
# MAGIC * **RBAC (grants):** only the two trader groups have SELECT on `contracts`; the others are denied even though
# MAGIC   they can open the space;
# MAGIC * **identity-bound row filter + price masks** (declared in `gold/contracts.sql`);
# MAGIC * **ABAC (tag-bound) contact-PII mask** (`06_contact_pii_abac`).
# MAGIC
# MAGIC It is a job-created object (not a bundle `genie_spaces` resource), because Genie rejects a space whose table
# MAGIC doesn't exist yet ("Failed to fetch tables for the agent", observed 2026-11-02), and gold `contracts` only exists
# MAGIC after the pipeline runs. Every persona group gets CAN_RUN, so denials come from Unity Catalog, not the space ACL.
# MAGIC The space id goes to `setup_state.genie_space_id`. Teardown trashes the space.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.157.0" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

w = WorkspaceClient()
p = lngdemo.get_params(dbutils)
CAT, TS, CUR = p["catalog"], p["trading_schema"], p["audit_curated_schema"]
TABLE = f"{CAT}.{TS}.contracts"
TITLE = p["genie_space_title"]
STATE = lngdemo.fq(CAT, CUR, "setup_state")
DESCRIPTION = ("LNG demo (synthetic data): ask about the gold contract book. Answers are governed per user by Unity "
               "Catalog: grants, desk row filter, price masks and a tag-driven PII mask. "
               "lngdemo_project=lngdemo-ai-governance lngdemo_component=genie-contracts")

# COMMAND ----------

prev = spark.sql(f"SELECT value FROM {STATE} WHERE key = 'genie_space_id'").collect()
space_id = prev[0]["value"] if prev else None
payload = lngdemo.genie_space_payload(TABLE)
if space_id:
    try:
        w.genie.get_space(space_id)
    except Exception as e:  # noqa: BLE001  (trashed by hand, or another workspace)
        print(f"recorded space {space_id} not reachable ({str(e)[:120]}); creating a new one")
        space_id = None
if space_id:
    w.genie.update_space(space_id, serialized_space=payload, title=TITLE, description=DESCRIPTION, warehouse_id=p["warehouse_id"])
    print("updated Genie space", space_id)
else:
    space_id = w.genie.create_space(warehouse_id=p["warehouse_id"], serialized_space=payload, title=TITLE,
                                    description=DESCRIPTION).space_id
    print("created Genie space", space_id)

spark.sql(f"""
MERGE INTO {STATE} t
USING (SELECT 'genie_space_id' AS key, :v AS value) s ON t.key = s.key
WHEN MATCHED THEN UPDATE SET value = s.value, updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (key, value, updated_at) VALUES (s.key, s.value, current_timestamp())
""", args={"v": space_id})

# COMMAND ----------

# MAGIC %md ### Space ACL: CAN_RUN for every persona group (the data decision stays with Unity Catalog)

# COMMAND ----------

groups = [p[k] for k in ("trader_group", "unentitled_trader_group", "developer_group", "operator_group",
                         "security_group", "auditor_group") if p.get(k)]
try:
    w.permissions.update("genie", space_id, access_control_list=[
        iam.AccessControlRequest(group_name=g, permission_level=iam.PermissionLevel.CAN_RUN) for g in groups])
    print("CAN_RUN granted to", groups)
except Exception as e:  # noqa: BLE001
    print("WARNING: could not set the space ACL:", str(e)[:300])

host = w.config.host.rstrip("/")
print(f"\nGenie space: {host}/genie/rooms/{space_id}")
print("Warehouse: the space embeds this job's compute credentials (warehouse only); data access is evaluated per asker.")
print("App tab 'Genie agent': redeploy with the space id so the app can reach it:\n"
      f"  databricks bundle deploy -t {p['bundle_target'] or 'dev'} --var genie_space_id={space_id} --profile <profile>")
