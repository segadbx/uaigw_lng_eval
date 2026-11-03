# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Service-policy SQL functions (M4, M7)
# MAGIC Creates the custom policy functions in `<catalog>.services`. **Attaching** them to a model
# MAGIC service is UI-only during the Beta → see `docs/MANUAL_STEPS.md` (P2, P5, P6).
# MAGIC
# MAGIC Unsupported constructs are rejected when the policy is *attached*, not when the function is
# MAGIC created, so this notebook also prints a static check against the documented allow-list.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.157.0" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, re, sys, glob
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo

p = lngdemo.get_params(dbutils)
POL = os.path.abspath("../policies")
subs = lngdemo.sql_subs(p)

# Documented allow-list (policy function reference, 2026-10-28). Anything else that looks like a
# function call is flagged for review before attaching.
ALLOWED = {
    "concat", "length", "char_length", "upper", "lower", "substring", "trim", "ltrim", "rtrim",
    "replace", "startswith", "endswith", "contains", "coalesce", "nullif", "ifnull", "nvl", "abs",
    "mod", "isnull", "isnotnull", "named_struct", "to_variant_object", "cast", "if",
}
SQL_WORDS = {"variant", "in", "and", "or", "not", "returns", "function", "exists"}

for path in sorted(glob.glob(f"{POL}/*.sql")):
    name = os.path.basename(path)
    body = open(path).read()
    if "RETURN" in body and "LANGUAGE SQL" in body:
        expr = body.split("LANGUAGE SQL", 1)[1]
        calls = {m.lower() for m in re.findall(r"\b([A-Za-z_]+)\s*\(", expr)}
        unknown = sorted(calls - ALLOWED - SQL_WORDS - {"comment"})
        print(f"{name}: functions used={sorted(calls)} -> {'OK' if not unknown else 'REVIEW ' + str(unknown)}")
    out = lngdemo.run_sql_file(spark, path, subs)
    print("   created/granted:", len(out), "statement(s)")

# COMMAND ----------

S = f"{p['catalog']}.{p['services_schema']}"
print("functions in", S, lngdemo.list_functions(spark, p["catalog"], p["services_schema"]))
print(f"Attach in the UI: {S}.deny_contract_terms (prod), {S}.broken_policy_fail_closed (dev only), {S}.require_desk_tag (optional, Log mode)")
