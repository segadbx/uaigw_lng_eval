# Databricks notebook source
# MAGIC %md
# MAGIC # 01a · Governance prerequisites (runs BEFORE the medallion pipeline)
# MAGIC The gold `contracts` table declares its row filter and price masks inside the pipeline definition, so the
# MAGIC UDFs it references must exist first. This task creates:
# MAGIC 1. catalog access for personas (`01_catalog_access.sql`);
# MAGIC 2. the desk-entitlement row filter and price-mask UDFs (`02_entitlement_and_mask_functions.sql`);
# MAGIC 3. the `audit_curated` result tables (`07_audit_curated_tables.sql`) and the claim-ledger seed.
# MAGIC
# MAGIC Idempotent: `GRANT`, `CREATE OR REPLACE FUNCTION`, `CREATE TABLE IF NOT EXISTS`, insert-only `MERGE`.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.157.0" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys, yaml
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo

p = lngdemo.get_params(dbutils)
CAT, CUR = p["catalog"], p["audit_curated_schema"]
SQL = os.path.abspath("../sql")
subs = lngdemo.sql_subs(p)
log = []
for name in ("01_catalog_access.sql", "02_entitlement_and_mask_functions.sql", "07_audit_curated_tables.sql"):
    for r in lngdemo.run_sql_file(spark, f"{SQL}/{name}", subs):
        log.append({**r, "file": name})

# COMMAND ----------

# MAGIC %md ### Claim ledger seed (insert-only; job results overwrite status later)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {lngdemo.fq(CAT, CUR, 'claim_ledger')} (
  claim_id INT, claim STRING, source STRING, test_performed STRING,
  observed_result STRING, status STRING, tested_at TIMESTAMP)
""")
claims = yaml.safe_load(open(os.path.abspath("../../tests/claims.yaml")))["claims"]
for c in claims:
    spark.sql(f"""
    MERGE INTO {lngdemo.fq(CAT, CUR, 'claim_ledger')} t
    USING (SELECT :id AS claim_id) s ON t.claim_id = s.claim_id
    WHEN NOT MATCHED THEN INSERT (claim_id, claim, source, test_performed, observed_result, status, tested_at)
      VALUES (:id, :claim, :source, :test, NULL, :status, NULL)
    """, args={"id": c["id"], "claim": c["claim"], "source": c["source"], "test": c["test"], "status": c["initial_status"]})

import pandas as pd
display(pd.DataFrame(log))
