# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Unity Catalog governance on the gold contract book (M4), runs AFTER the medallion pipeline
# MAGIC The gold `contracts` materialized view is owned by the Lakeflow pipeline `lngdemo-medallion`. Its desk
# MAGIC row filter and price masks are **declared in the pipeline definition**
# MAGIC (`src/pipelines/medallion/transformations/gold/contracts.sql`), because pipeline-managed datasets can't take
# MAGIC `ALTER TABLE ... SET ROW FILTER / SET MASK` afterwards. This task:
# MAGIC 1. verifies the declared protection is present on the gold table (recorded as `contract_protection_mode`);
# MAGIC 2. creates the aggregate-only tool function (it reads gold `contracts`, so it must follow the pipeline);
# MAGIC 3. applies object grants on the gold datasets.
# MAGIC
# MAGIC Idempotent: `CREATE OR REPLACE FUNCTION`, `GRANT`, `MERGE`.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.157.0" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo

p = lngdemo.get_params(dbutils)
CAT, TS, CUR = p["catalog"], p["trading_schema"], p["audit_curated_schema"]
SQL = os.path.abspath("../sql")
subs = lngdemo.sql_subs(p)
GOLD = lngdemo.fq(CAT, TS, "contracts")

# COMMAND ----------

# MAGIC %md ### 1 · Verify the pipeline-declared row filter and masks on gold `contracts`

# COMMAND ----------

desc = spark.sql(f"DESCRIBE TABLE EXTENDED {GOLD}").collect()
text = "\n".join(f"{r['col_name']}|{r['data_type']}|{r['comment']}" for r in desc)
typ = next((r["data_type"] for r in desc if r["col_name"] == "Type"), "")
has_filter = "desk_filter" in text
has_masks = "mask_price_text" in text and "mask_price_num" in text
mode = "pipeline_declared" if has_filter and has_masks else "MISSING_PROTECTION"
print(f"gold contracts type={typ}; row filter present={has_filter}; price masks present={has_masks} -> {mode}")
if mode != "pipeline_declared":
    print("WARNING: expected the row filter / masks declared in gold/contracts.sql. Inspect DESCRIBE TABLE EXTENDED output:")
    print(text[-3000:])

spark.sql(f"""
MERGE INTO {lngdemo.fq(CAT, CUR, 'setup_state')} t
USING (SELECT 'contract_protection_mode' AS key, :mode AS value) s ON t.key = s.key
WHEN MATCHED THEN UPDATE SET value = s.value, updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (key, value, updated_at) VALUES (s.key, s.value, current_timestamp())
""", args={"mode": mode})

# COMMAND ----------

# MAGIC %md ### 2-3 · Tool function and grants

# COMMAND ----------

log = []
for name in ("05_tool_function.sql", "06_object_grants.sql"):
    for r in lngdemo.run_sql_file(spark, f"{SQL}/{name}", subs):
        log.append({**r, "file": name})

import pandas as pd
display(pd.DataFrame(log))
print("Check: two traders see different rows/columns -> run as each persona:")
print(f"  SELECT desk, counterparty_alias, price_formula, slope FROM {CAT}.{TS}.contracts LIMIT 10")
