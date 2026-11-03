# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Tag-driven (ABAC) mask for contact PII on the gold contract book (M10), runs AFTER governance
# MAGIC Gold `contracts` already carries **identity-bound** controls declared in the pipeline: the desk row filter
# MAGIC and price masks are fixed to named columns, and the UDFs check the caller's groups. This task adds an
# MAGIC **attribute-bound** control on top of them, so the Genie agent (07) can show both mechanisms in one answer:
# MAGIC 1. governed tag `lngdemo_sensitivity` (allowed value `pii`). This is an account-level object, so it is created only
# MAGIC    when `confirm_shared_changes = true`;
# MAGIC 2. tags `contact_email` / `contact_phone` on gold `contracts` with `lngdemo_sensitivity = pii`. The tags are
# MAGIC    re-applied on every setup run, because a pipeline full refresh may recreate the dataset;
# MAGIC 3. UDF `mask_contact_pii` plus the ABAC policy `lngdemo_mask_contact_pii` on schema `trading_data`
# MAGIC    (`src/sql/08_contact_pii_abac.sql`), which masks any column with that tag for everyone except security.
# MAGIC
# MAGIC Records `contact_pii_abac_mode` in `setup_state` (`active` / `SKIPPED_...` / `FAILED_...`); the Genie matrix reads it.
# MAGIC Idempotent: `SET TAGS`, `CREATE OR REPLACE FUNCTION / POLICY`, `MERGE`.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.157.0" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import tags as t

w = WorkspaceClient()
p = lngdemo.get_params(dbutils)
CAT, TS, CUR = p["catalog"], p["trading_schema"], p["audit_curated_schema"]
GOLD = lngdemo.fq(CAT, TS, "contracts")
LOG = lngdemo.fq(CAT, CUR, "experiment_log")
TAG_KEY, TAG_VALUE = lngdemo.PII_TAG
PII_COLUMNS = ("contact_email", "contact_phone")

def log(target, stmt, ok, detail):
    print("ok  " if ok else "FAIL", target, "-", str(detail)[:200])
    spark.sql(f"INSERT INTO {LOG} VALUES (current_timestamp(), 'contact_pii_abac', :t, :s, :ok, :d)",
              args={"t": target, "s": stmt[:2000], "ok": ok, "d": str(detail)[:2000]})

def set_mode(mode):
    print("contact_pii_abac_mode =", mode)
    spark.sql(f"""
    MERGE INTO {lngdemo.fq(CAT, CUR, 'setup_state')} t
    USING (SELECT 'contact_pii_abac_mode' AS key, :mode AS value) s ON t.key = s.key
    WHEN MATCHED THEN UPDATE SET value = s.value, updated_at = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (key, value, updated_at) VALUES (s.key, s.value, current_timestamp())
    """, args={"mode": mode})

# COMMAND ----------

# MAGIC %md ### 1 · Governed tag (account-level → gated)

# COMMAND ----------

try:
    existing = w.tag_policies.get_tag_policy(TAG_KEY)
    tag_ok = TAG_VALUE in [v.name for v in (existing.values or [])]
    log(TAG_KEY, "get_tag_policy", tag_ok, f"exists; values={[v.name for v in (existing.values or [])]}")
except Exception:  # noqa: BLE001  (not found)
    tag_ok = False
    print(f"PLAN: create governed tag {TAG_KEY} with allowed value '{TAG_VALUE}' (account-level, shared)")
    if lngdemo.shared_changes_confirmed(p):
        try:
            w.tag_policies.create_tag_policy(t.TagPolicy(
                tag_key=TAG_KEY, values=[t.Value(name=TAG_VALUE)],
                description="LNG demo: column sensitivity for ABAC masks (lngdemo_project=lngdemo-ai-governance)"))
            tag_ok = True
            log(TAG_KEY, "create_tag_policy", True, "created")
        except Exception as e:  # noqa: BLE001
            log(TAG_KEY, "create_tag_policy", False, e)
    else:
        print("confirm_shared_changes != true -> governed tag not created; the ABAC policy is skipped.")

# COMMAND ----------

# MAGIC %md ### 2-3 · Column tags, mask UDF and ABAC policy

# COMMAND ----------

if not tag_ok:
    set_mode("SKIPPED_NO_GOVERNED_TAG")
    dbutils.notebook.exit("SKIPPED_NO_GOVERNED_TAG")

# Pipeline-managed materialized view: try the table form first, then the materialized-view form.
tagged = []
for col in PII_COLUMNS:
    for kind in ("TABLE", "MATERIALIZED VIEW"):
        stmt = f"ALTER {kind} {GOLD} ALTER COLUMN `{col}` SET TAGS ('{TAG_KEY}' = '{TAG_VALUE}')"
        try:
            spark.sql(stmt)
            log(f"{CAT}.{TS}.contracts.{col}", stmt, True, f"tagged via ALTER {kind}")
            tagged.append(col)
            break
        except Exception as e:  # noqa: BLE001
            log(f"{CAT}.{TS}.contracts.{col}", stmt, False, e)

if not tagged:
    set_mode("FAILED_COLUMN_TAGS")
    dbutils.notebook.exit("FAILED_COLUMN_TAGS")

try:
    for r in lngdemo.run_sql_file(spark, os.path.abspath("../sql/08_contact_pii_abac.sql"), lngdemo.sql_subs(p)):
        log(f"{CAT}.{TS}", r["statement"], r["ok"], r["error"] or "ok")
except Exception as e:  # noqa: BLE001
    log(f"{CAT}.{TS}", "08_contact_pii_abac.sql", False, e)
    set_mode("FAILED_POLICY")
    raise

set_mode("active" if len(tagged) == len(PII_COLUMNS) else f"active_partial({','.join(tagged)})")

# COMMAND ----------

# MAGIC %md ### Check: tags and policies on gold `contracts`

# COMMAND ----------

display(spark.sql(f"""SELECT column_name, tag_name, tag_value FROM {lngdemo.fq(CAT, 'information_schema', 'column_tags')}
                     WHERE schema_name = :s AND table_name = 'contracts'""", args={"s": TS}))
for pol in w.policies.list_policies(on_securable_type="SCHEMA", on_securable_fullname=f"{CAT}.{TS}"):
    print(pol.name, pol.policy_type, "to", pol.to_principals, "except", pol.except_principals)
