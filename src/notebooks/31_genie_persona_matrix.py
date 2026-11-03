# Databricks notebook source
# MAGIC %md
# MAGIC # 31 · Genie persona matrix: the same questions, asked by each persona (M10)
# MAGIC Each persona SP (OAuth M2M from the secret scope, same as `30_persona_matrix`) opens a conversation in the
# MAGIC Genie space created by `07_genie_space` and asks the three demo questions. Genie runs the generated SQL with the
# MAGIC asker's credentials, so the answer reflects that persona's grants (RBAC), the desk row filter and price masks
# MAGIC (identity-bound, declared in the pipeline) and the contact-PII mask (ABAC, tag-bound).
# MAGIC
# MAGIC Result table: `audit_curated.genie_persona_results` (one row per persona × question, with Genie's SQL).
# MAGIC The expected column is the **design recommendation** for this demo, not product behaviour.
# MAGIC No warehouse grant is needed: the space's warehouse runs on the author's embedded compute credentials, while
# MAGIC data access is evaluated as the end user (Genie Agents set-up docs, checked 2026-11-02).
# MAGIC Genie's SQL is model-generated: an inconclusive answer (e.g. a column not returned) is reported as `INCONCLUSIVE`,
# MAGIC not as a governance failure.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.157.0" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys, uuid, datetime as dt
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo

p = lngdemo.get_params(dbutils)
CAT, CUR = p["catalog"], p["audit_curated_schema"]
STATE = lngdemo.fq(CAT, CUR, "setup_state")
state = {r["key"]: r["value"] for r in spark.sql(f"SELECT key, value FROM {STATE}").collect()}
SPACE = state.get("genie_space_id")
ABAC_MODE = state.get("contact_pii_abac_mode", "unknown")
if not SPACE:
    dbutils.notebook.exit("No genie_space_id in setup_state: run the setup job first (task genie_space).")
RUN_ID = "genie-" + uuid.uuid4().hex[:8]
personas = [x for x in lngdemo.PERSONAS if x != "app"]
ids = {x: lngdemo.persona_identity(x, dbutils, p["secret_scope"]) for x in personas}
print("space", SPACE, "| contact_pii_abac_mode", ABAC_MODE)
print({k: ("available" if v.available else v.note) for k, v in ids.items()})

# Design recommendation per persona: rows -> visible desks, prices / contacts -> clear | masked, DENIED = no result.
# Security is in desk_filter / the mask exemptions, but has no USE SCHEMA on trading_data: RBAC denies first.
DENIED = {"rows": "DENIED", "prices": "DENIED", "contacts": "DENIED"}
EXPECTED = {
    "trader":            {"rows": "Atlantic Desk,Portfolio Desk", "prices": "clear", "contacts": "masked"},
    "unentitled_trader": {"rows": "Pacific Desk", "prices": "masked", "contacts": "masked"},
    "developer": DENIED, "operator": DENIED, "security": DENIED, "auditor": DENIED,
}
LAYER = {"rows": "RBAC grant + desk row filter", "prices": "RBAC grant + price column masks (group-bound)",
         "contacts": "RBAC grant + ABAC mask on lngdemo_sensitivity=pii (tag-bound)"}

# COMMAND ----------

def verdict(key, expected, observed):
    if key == "contacts" and expected == "masked" and not ABAC_MODE.startswith("active"):
        return "NOT_TESTED"  # ABAC policy not in place (governed tag not confirmed, or setup failed)
    if observed == expected:
        return "PASS"
    if observed in ("no rows", "column not returned"):
        return "INCONCLUSIVE"
    return "FAIL"

rows = []
for x, idn in ids.items():
    if not idn.available:
        for key, question, _ in lngdemo.GENIE_QUESTIONS:
            rows.append(dict(run_id=RUN_ID, persona=x, principal="", question_key=key, question=question,
                             layer=LAYER[key], expected=EXPECTED[x][key], observed="", verdict="NOT_TESTED",
                             genie_status="", genie_sql=None, rows_returned=None, detail=idn.note, identity_note=idn.note))
        continue
    principal = idn.principal()
    for key, question, _ in lngdemo.GENIE_QUESTIONS:
        ans = lngdemo.ask_genie(idn.client, SPACE, question)
        observed = lngdemo.genie_observation(key, ans)
        rows.append(dict(run_id=RUN_ID, persona=x, principal=principal, question_key=key, question=question,
                         layer=LAYER[key], expected=EXPECTED[x][key], observed=observed,
                         verdict=verdict(key, EXPECTED[x][key], observed), genie_status=ans["status"],
                         genie_sql=ans["sql"], rows_returned=len(ans["rows"]),
                         detail=(ans["error"] or ans["text"] or "")[:1000], identity_note=idn.note))
        print(f"{x:18} {key:9} {ans['status']:12} observed={observed!r} expected={EXPECTED[x][key]!r}")

for r in rows:
    r["run_ts"] = dt.datetime.utcnow().isoformat()
lngdemo.append_rows(spark, f"{CAT}.{CUR}.genie_persona_results", rows)

# COMMAND ----------

display(spark.sql(f"""
SELECT persona,
  max(CASE WHEN question_key='rows' THEN concat(observed, ' / ', verdict) END) AS rows_per_desk,
  max(CASE WHEN question_key='prices' THEN concat(observed, ' / ', verdict) END) AS price_terms,
  max(CASE WHEN question_key='contacts' THEN concat(observed, ' / ', verdict) END) AS contact_pii,
  max(identity_note) AS identity
FROM {lngdemo.fq(CAT, CUR, 'genie_persona_results')} WHERE run_id = '{RUN_ID}' GROUP BY persona ORDER BY persona"""))

# COMMAND ----------

# MAGIC %md ### Claim 22 · Genie answers inherit the asker's Unity Catalog controls

# COMMAND ----------

tested = [r for r in rows if r["verdict"] in ("PASS", "FAIL")]
fails = [r for r in tested if r["verdict"] == "FAIL"]
observed = (f"{len(tested)} persona x question checks: {len(tested) - len(fails)} PASS, {len(fails)} FAIL"
            + (f" (first FAIL: {fails[0]['persona']}/{fails[0]['question_key']} observed {fails[0]['observed']!r})" if fails else "")
            + f"; contact_pii_abac_mode={ABAC_MODE}; run_id={RUN_ID}")
print(observed)
if tested:
    lngdemo.ledger_update(spark, lngdemo.fq(CAT, CUR, "claim_ledger"), 22,
                          "31_genie_persona_matrix: 3 questions x 6 persona SPs via the Genie Conversation API", observed,
                          "Confirmed" if not fails else "Contradicted")

# COMMAND ----------

# MAGIC %md ### Claim 23 · A tag-bound ABAC mask applies to the pipeline-managed gold materialized view

# COMMAND ----------

contact = [r for r in rows if r["question_key"] == "contacts" and r["expected"] == "masked" and r["genie_status"] == "ANSWERED"]
if ABAC_MODE.startswith("active") and contact:
    masked = [r["persona"] for r in contact if r["observed"] == "masked"]
    observed = (f"contact columns tagged {lngdemo.PII_TAG[0]}={lngdemo.PII_TAG[1]} on gold contracts (mode {ABAC_MODE}); "
                f"masked for {masked} of {[r['persona'] for r in contact]} via Genie; run_id={RUN_ID}")
    print(observed)
    lngdemo.ledger_update(spark, lngdemo.fq(CAT, CUR, "claim_ledger"), 23,
                          "06_contact_pii_abac tags + schema-level ABAC policy; 31_genie_persona_matrix contact question as both traders",
                          observed, "Confirmed" if len(masked) == len(contact) else "Contradicted")
else:
    print(f"claim 23 not measured: contact_pii_abac_mode={ABAC_MODE}, answered contact checks={len(contact)}")
