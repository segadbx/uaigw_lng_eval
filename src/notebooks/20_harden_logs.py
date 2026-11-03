# Databricks notebook source
# MAGIC %md
# MAGIC # 20 · Log permissions, retention, curated copy (M3), as recorded experiments
# MAGIC Every statement's success or failure is written to `audit_curated.experiment_log`, because several
# MAGIC of these are **undocumented** on auto-created inference tables (claims 7, 8):
# MAGIC
# MAGIC | Step | Target | Why it is an experiment |
# MAGIC |---|---|---|
# MAGIC | Auto-TTL `DELETE ROWS n DAYS AFTER event_time` | every `*_payload` table | Not documented for inference tables (claim 7) |
# MAGIC | Column mask on `request` / `response` | **dev** payload table only | Protects prod logging if it breaks delivery (claim 8) |
# MAGIC | Redacted curated copy + operator view | `audit_curated` | Supported pattern (plain CTAS / view) |
# MAGIC | Documented row filter + Auto-TTL | unified trace table (optional) | Shared object → needs `confirm_shared_changes=true` |
# MAGIC
# MAGIC Run this **after** the inference tables exist (first request + up to 1h delivery).

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.157.0" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo

p = lngdemo.get_params(dbutils)
CAT, RAW, CUR, SVC = p["catalog"], p["audit_raw_schema"], p["audit_curated_schema"], p["services_schema"]
DAYS = int(p["retention_days"] or 7)
SEC, OPS = p["security_group"], p["operator_group"]
PAYLOAD = {role: s["payload"] for role, s in lngdemo.services(p).items()}  # prod / dev / judge
existing = {sid: t for sid, t in PAYLOAD.items() if lngdemo.table_exists(spark, t)}
print("payload tables present:", existing or "none yet - send requests first (red_team job) and wait for delivery")

def attempt(experiment, target, stmt):
    try:
        spark.sql(stmt)
        ok, detail = True, "ok"
    except Exception as e:  # noqa: BLE001
        ok, detail = False, str(e)[:1500]
    spark.sql(f"INSERT INTO {lngdemo.fq(CAT, CUR, 'experiment_log')} VALUES (current_timestamp(), :e, :t, :s, :ok, :d)",
              args={"e": experiment, "t": target, "s": stmt[:2000], "ok": ok, "d": detail})
    print(("OK   " if ok else "FAIL ") + experiment, target, "" if ok else detail[:300])
    return ok

def q(full):  # backtick-quote a.b.c
    return lngdemo.fq(*full.split("."))

# COMMAND ----------

# MAGIC %md ### 1 · Restrict raw payloads to the security group
# MAGIC Schema `audit_raw` grants come from the bundle (security group only). The table owner is the
# MAGIC model-service creator; here we only remove any broad table-level grant and record SHOW GRANTS.

# COMMAND ----------

for sid, t in existing.items():
    attempt("raw_restrict", t, f"REVOKE ALL PRIVILEGES ON TABLE {q(t)} FROM `account users`")
    attempt("raw_restrict", t, f"GRANT SELECT ON TABLE {q(t)} TO `{SEC}`")
    display(spark.sql(f"SHOW GRANTS ON TABLE {q(t)}"))

# COMMAND ----------

# MAGIC %md ### 2 · Auto-TTL on inference tables (claim 7)

# COMMAND ----------

for sid, t in existing.items():
    attempt("auto_ttl_inference", t, f"ALTER TABLE {q(t)} DELETE ROWS {DAYS} DAYS AFTER event_time")
    props = spark.sql(f"SHOW TBLPROPERTIES {q(t)}").where("key LIKE 'autottl%'").collect()
    print(t, {r["key"]: r["value"] for r in props})

# COMMAND ----------

# MAGIC %md ### 3 · Column mask on the dev payload table (claim 8, inference-table half)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {lngdemo.fq(CAT, CUR, 'mask_payload')}(v STRING)
RETURNS STRING
COMMENT 'LNG demo: payload visible to security only'
RETURN CASE WHEN is_account_group_member('{SEC}') THEN v ELSE '[payload masked: security group only]' END
""")
dev_t = existing.get("dev")
if dev_t:
    attempt("payload_column_mask", dev_t, f"ALTER TABLE {q(dev_t)} ALTER COLUMN request SET MASK {lngdemo.fq(CAT, CUR, 'mask_payload')}")
    attempt("payload_column_mask", dev_t, f"ALTER TABLE {q(dev_t)} ALTER COLUMN response SET MASK {lngdemo.fq(CAT, CUR, 'mask_payload')}")
    spark.sql(f"""
    MERGE INTO {lngdemo.fq(CAT, CUR, 'setup_state')} t
    USING (SELECT 'dev_payload_mask_applied_at' AS key, cast(current_timestamp() AS STRING) AS value) s ON t.key = s.key
    WHEN MATCHED THEN UPDATE SET value = s.value, updated_at = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (key, value, updated_at) VALUES (s.key, s.value, current_timestamp())""")
else:
    print("dev payload table not present yet - skipped (send a request to the dev service first)")

# COMMAND ----------

# MAGIC %md ### 4 · Redacted curated copy + operator view
# MAGIC Redaction here is plain SQL (`regexp_replace`) on a copy, so regex *is* available
# MAGIC (unlike inside service policy functions). Operators get no raw payload columns.

# COMMAND ----------

if existing:
    union = "\nUNION ALL\n".join(
        f"SELECT '{sid}' AS service, * FROM {q(t)}" for sid, t in existing.items())
    red = lambda col: (f"regexp_replace(regexp_replace(regexp_replace(regexp_replace({col}, "
                       r"'[\\w.+-]+@[\\w-]+\\.[\\w.]+', '[EMAIL]'), "
                       r"'\\+?\\d[\\d\\s().-]{8,}\\d', '[PHONE]'), "
                       r"'(?i)counterparty [a-h]\\b', '[COUNTERPARTY]'), "
                       r"'(?i)[-+]?\\d+(\\.\\d+)?\\s*(%\\s*(of\\s*)?(brent|hh)|x\\s*(hh|henry hub|jkm|ttf|brent)|(usd|\\$)\\s*/\\s*mmbtu)', '[PRICE]')")
    attempt("curated_copy", f"{CAT}.{CUR}.inference_redacted", f"""
    CREATE OR REPLACE TABLE {lngdemo.fq(CAT, CUR, 'inference_redacted')}
    COMMENT 'LNG demo: inference payloads with emails, phones, counterparty aliases and price terms redacted'
    AS SELECT service, request_id, invocation_id, event_time, requester, status_code, latency_ms,
              destination_model, request_tags, logging_error_codes,
              {red('request')} AS request_redacted,
              {red('response')} AS response_redacted
    FROM ({union})""")
    attempt("auto_ttl_curated", f"{CAT}.{CUR}.inference_redacted",
            f"ALTER TABLE {lngdemo.fq(CAT, CUR, 'inference_redacted')} DELETE ROWS {DAYS} DAYS AFTER event_time")
    attempt("operator_view", f"{CAT}.{CUR}.v_inference_ops", f"""
    CREATE OR REPLACE VIEW {lngdemo.fq(CAT, CUR, 'v_inference_ops')}
    COMMENT 'LNG demo: operator view - metadata only, no payloads'
    AS SELECT service, request_id, event_time, requester, status_code, latency_ms, destination_model,
              request_tags, logging_error_codes
    FROM ({union})""")
    attempt("operator_view", f"{CAT}.{CUR}.v_inference_ops", f"GRANT SELECT ON TABLE {lngdemo.fq(CAT, CUR, 'v_inference_ops')} TO `{OPS}`")

# COMMAND ----------

# MAGIC %md ### 5 · Unified trace table (optional, shared object)

# COMMAND ----------

tt = p["trace_table"]
if not tt:
    print("trace_table empty -> skipped")
else:
    plan = [
        f"ALTER TABLE {q(tt)} DELETE ROWS {DAYS} DAYS AFTER time",
        f"CREATE OR REPLACE FUNCTION {lngdemo.fq(CAT, CUR, 'ai_traces_filter')}(svc STRING) RETURNS BOOLEAN "
        f"RETURN is_account_group_member('{p["admin_group"]}') OR is_account_group_member('{SEC}') OR is_account_group_member(svc || '-owners')",
        f"ALTER TABLE {q(tt)} ALTER COLUMN service_name SET TAGS ('lngdemo_trace_service' = '')",
        f"CREATE OR REPLACE POLICY lngdemo_ai_traces_filter ON TABLE {q(tt)} COMMENT 'LNG demo: trace visibility' "
        f"ROW FILTER {lngdemo.fq(CAT, CUR, 'ai_traces_filter')} TO `account users` FOR TABLES "
        f"MATCH COLUMNS has_tag('lngdemo_trace_service') AS svc USING COLUMNS (svc)",
    ]
    print("PLAN (shared trace table):\n  " + "\n  ".join(plan))
    if lngdemo.shared_changes_confirmed(p):
        for s in plan:
            attempt("trace_table", tt, s)
    else:
        print("confirm_shared_changes != true -> not executed")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {lngdemo.fq(CAT, CUR, 'experiment_log')} ORDER BY run_ts DESC LIMIT 50"))
