# Databricks notebook source
# MAGIC %md
# MAGIC # 90 · Teardown
# MAGIC Removes what the **jobs** created inside the bundle-owned schemas (tables, views, functions,
# MAGIC ABAC policies, auto-created inference tables) and the job-created Genie space, so that `databricks bundle destroy` can drop the
# MAGIC schemas, model services, volume, secret scope, app, dashboard and jobs afterwards.
# MAGIC
# MAGIC It never touches objects outside the bundle schemas, except for reverting shared changes
# MAGIC this demo made, and only when `confirm_shared_changes = true`. Everything else is printed as a manual step.

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
CAT, SVC, RAW, CUR, TS = p["catalog"], p["services_schema"], p["audit_raw_schema"], p["audit_curated_schema"], p["trading_schema"]
manual = []

def run(stmt):
    try:
        spark.sql(stmt)
        print("ok  ", stmt[:140])
    except Exception as e:  # noqa: BLE001
        print("skip", stmt[:140], "-", str(e)[:160])

# Read restore info before audit_curated is emptied
state = {r["key"]: r["value"] for r in spark.sql(f"SELECT key, value FROM {lngdemo.fq(CAT, CUR, 'setup_state')}").collect()} \
    if lngdemo.table_exists(spark, f"{CAT}.{CUR}.setup_state") else {}

# COMMAND ----------

# M9 Lakebase: CDF config and synced tables first (synced tables must be deleted before the registered
# catalog is removed by `bundle destroy`), then the job-created dev branch, then UC objects in the ops catalog.
if p.get("lakebase_project"):
    prod = lngdemo.lb_paths(p)
    try:
        for d in w.postgres.list_databases(prod["branch"]):
            try:
                for c in w.postgres.list_cdf_configs(d.name):
                    w.postgres.delete_cdf_config(c.name, force=True).wait(); print("deleted CDF config", c.name)
            except Exception as e:  # noqa: BLE001  (404 when none exist)
                print("no CDF config on", d.name, "-", str(e)[:80])
    except Exception as e:  # noqa: BLE001
        print("Lakebase project not reachable (already destroyed?):", str(e)[:160])
    for table in ("desk_book_summary", "fgac_probe_contracts"):
        sid = f"{p['lakebase_uc_catalog']}.{p['lakebase_reference_schema']}.{table}"
        try:
            w.postgres.delete_synced_table(f"synced_tables/{sid}").wait(); print("deleted synced table", sid)
        except Exception as e:  # noqa: BLE001
            print("synced table", sid, "-", str(e)[:100])
    dev_branch = lngdemo.lb_paths(p, p["lakebase_dev_branch"])["branch"]
    try:
        w.postgres.delete_branch(dev_branch, purge=True).wait(); print("deleted branch", dev_branch)
    except Exception as e:  # noqa: BLE001
        print("dev branch", dev_branch, "-", str(e)[:100])
    for schema in (p["ops_curated_schema"], p["ops_cdc_schema"]):
        if not spark.catalog.databaseExists(f"{p['ops_catalog']}.{schema}"):
            continue
        for t in spark.sql(f"SHOW TABLES IN {lngdemo.fq(p['ops_catalog'], schema)}").where("isTemporary = false").collect():
            kind = spark.sql(f"DESCRIBE TABLE EXTENDED {lngdemo.fq(p['ops_catalog'], schema, t['tableName'])}").where("col_name = 'Type'").collect()
            run(f"DROP {'VIEW' if kind and kind[0]['data_type'] == 'VIEW' else 'TABLE'} IF EXISTS {lngdemo.fq(p['ops_catalog'], schema, t['tableName'])}")
    manual.append(f"Lakebase project {prod['project']} is purged by `bundle destroy` (purge_on_delete: true); "
                  "verify with `databricks postgres list-projects`.")

# COMMAND ----------

# M10: Genie space (job-created, not a bundle resource) and the ABAC policy, which must go before its mask UDF.
if state.get("genie_space_id"):
    try:
        w.genie.trash_space(state["genie_space_id"]); print("trashed Genie space", state["genie_space_id"])
    except Exception as e:  # noqa: BLE001
        print("Genie space", state["genie_space_id"], "-", str(e)[:100])
run(f"DROP POLICY IF EXISTS lngdemo_mask_contact_pii ON SCHEMA {lngdemo.fq(CAT, TS)}")

# COMMAND ----------

# Job-created tables/views/functions in bundle schemas. Pipeline-managed datasets (materialized views and
# streaming tables in bronze / silver / gold) are skipped: `bundle destroy` deletes the pipeline, which drops
# them (cascade_on_destroy defaults to true). The raw landing volume is a bundle resource as well.
PIPELINE_TYPES = ("MATERIALIZED_VIEW", "STREAMING_TABLE")
for schema in (TS, CUR, RAW, SVC):
    for t in spark.sql(f"SHOW TABLES IN {lngdemo.fq(CAT, schema)}").where("isTemporary = false").collect():
        kind = spark.sql(f"DESCRIBE TABLE EXTENDED {lngdemo.fq(CAT, schema, t['tableName'])}").where("col_name = 'Type'").collect()
        kind = kind[0]["data_type"] if kind else "TABLE"
        if kind in PIPELINE_TYPES:
            print("skip (pipeline-managed):", schema, t["tableName"])
            continue
        run(f"DROP {'VIEW' if kind == 'VIEW' else 'TABLE'} IF EXISTS {lngdemo.fq(CAT, schema, t['tableName'])}")
    for full in lngdemo.list_functions(spark, CAT, schema):
        run(f"DROP FUNCTION IF EXISTS {lngdemo.fq(*full.split('.'))}")

# COMMAND ----------

# Shared changes made by this demo
if state.get("system_ai_restore_statements"):
    info = json.loads(state["system_ai_restore_statements"])
    if info.get("executed"):
        if lngdemo.shared_changes_confirmed(p):
            for s in info["restore"]:
                run(s)
        else:
            manual.append(f"Restore system.ai access: {info['restore']}")

if p["trace_table"]:
    manual.append(f"Trace table {p['trace_table']}: DROP POLICY lngdemo_ai_traces_filter ON TABLE {p['trace_table']}; "
                  f"ALTER TABLE {p['trace_table']} DROP ROW DELETION (if you don't want to keep the TTL); "
                  "disable tracing in AI Gateway > Govern > Traces if it was enabled only for this demo.")

try:
    w.tag_policies.get_tag_policy("lngdemo_sensitivity")
    if lngdemo.shared_changes_confirmed(p):
        w.tag_policies.delete_tag_policy("lngdemo_sensitivity")
        print("deleted governed tag lngdemo_sensitivity")
    else:
        manual.append("Delete governed tag `lngdemo_sensitivity` (account-level) if this demo created it: "
                      "databricks tag-policies delete-tag-policy lngdemo_sensitivity")
except Exception:  # noqa: BLE001
    pass

manual += [
    f"Detach every service policy from {p['copilot_service']} and {p['copilot_dev_service']} in AI Gateway > Models > <service> > Policies "
    "(SDD, deny_contract_terms, LLM judge, require_desk_tag, broken_policy_fail_closed).",
    "Warehouse CAN_USE granted to persona SPs (if confirm_shared_changes was used in 30_persona_matrix): remove in SQL Warehouses > Permissions.",
    "Persona SP OAuth secrets: revoke the client secrets in the account console if they were created only for this demo.",
    f"MLflow experiment {p['traces_experiment']} (app traces): delete if not needed.",
    f"Then run:  databricks bundle destroy -t {p['bundle_target'] or '<target>'} --profile <your-profile>",
    f"Finally drop the catalog if it was created only for this demo:  DROP CATALOG {CAT} CASCADE",
]
print("\nMANUAL CLEANUP:\n" + "\n".join(f"  - {m}" for m in manual))
