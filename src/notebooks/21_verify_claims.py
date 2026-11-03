# Databricks notebook source
# MAGIC %md
# MAGIC # 21 · Verify claims against observed data (M2, M3, M5, claim ledger)
# MAGIC Reads `red_team_results`, the payload tables and `experiment_log`, then updates
# MAGIC `audit_curated.claim_ledger`. A claim is only marked **Confirmed** or **Contradicted** from an
# MAGIC observation; "no row yet" stays *Not yet tested*. Any **Contradicted** row is printed at the end:
# MAGIC fix the customer deck before the session.

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
CAT, RAW, CUR = p["catalog"], p["audit_raw_schema"], p["audit_curated_schema"]
LEDGER = lngdemo.fq(CAT, CUR, "claim_ledger")
PROD = p["copilot_payload"]
DEV = p["copilot_dev_payload"]
RT = lngdemo.fq(CAT, CUR, "red_team_results")

def latest(test_id):
    r = spark.sql(f"SELECT * FROM {RT} WHERE test_id = :t AND outcome <> 'NOT_TESTED' ORDER BY run_ts DESC LIMIT 1", args={"t": test_id}).collect()
    return r[0].asDict() if r else None

def payload_rows(table, marker):
    if not lngdemo.table_exists(spark, table):
        return None
    return spark.sql(f"SELECT request, response, logging_error_codes, event_time FROM {lngdemo.fq(*table.split('.'))} WHERE request LIKE :m",
                     args={"m": f"%{marker}%"}).collect()

def age_minutes(row):
    return spark.sql("SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(to_timestamp(:ts))) / 60 AS m",
                     args={"ts": row["run_ts"]}).first()["m"]

# COMMAND ----------

# MAGIC %md ### M2 evidence: payload table schema and logging errors (no hard-coded column list)

# COMMAND ----------

if lngdemo.table_exists(spark, PROD):
    display(spark.sql(f"DESCRIBE TABLE {lngdemo.fq(*PROD.split('.'))}"))
    display(spark.sql(f"""SELECT logging_error_codes, status_code, count(*) n
                          FROM {lngdemo.fq(*PROD.split('.'))} GROUP BY ALL ORDER BY n DESC"""))
else:
    print(PROD, "not created yet")

# COMMAND ----------

# Claim 1 - request column holds original vs. redacted text
for tid in ("m4_sdd_email_phone", "m2_redacted"):
    r = latest(tid)
    if not r:
        continue
    rows = payload_rows(PROD, r["request_marker"])
    if rows is None or not rows:
        print(f"claim 1: no payload row yet for {tid} (age {age_minutes(r):.0f} min)")
        continue
    req = rows[0]["request"] or ""
    raw_email = "example.com" in req
    placeholder = "[EMAIL_ADDRESS]" in req or "[EMAIL]" in req
    observed = f"{tid}: request column contains raw email={raw_email}, placeholder={placeholder}; model reply preview: {r['content_preview'][:200]}"
    status = "Confirmed" if raw_email and not placeholder else ("Contradicted" if placeholder else "Partially confirmed")
    lngdemo.ledger_update(spark, LEDGER, 1, f"Looked up marker {r['request_marker']} in {PROD}", observed, status)
    print("claim 1:", status, observed)
    break

# COMMAND ----------

# Claim 2 - blocked input leaves no row (only judged once a same-run allowed request has landed)
blk, ok = latest("m2_blocked_input"), latest("m2_normal")
if blk and ok and blk["outcome"] == "BLOCKED_BY_POLICY":
    b_rows, ok_rows = payload_rows(PROD, blk["request_marker"]), payload_rows(PROD, ok["request_marker"])
    if ok_rows:
        status = "Confirmed" if not b_rows else "Contradicted"
        observed = (f"allowed request landed ({len(ok_rows)} row); blocked request rows={len(b_rows or [])}"
                    + (f"; logged response={b_rows[0]['response'][:300]}" if b_rows else ""))
        lngdemo.ledger_update(spark, LEDGER, 2, "m2_blocked_input vs m2_normal markers in payload table", observed, status)
        print("claim 2:", status, observed)
    else:
        print("claim 2: control row not delivered yet; re-run later")
elif blk:
    print("claim 2: m2_blocked_input outcome was", blk["outcome"], "- attach P2 first")

# COMMAND ----------

# Claim 3 - HTTP 200 + databricks_service_policy payload shape
blocked = spark.sql(f"SELECT test_id, http_status, policy_json FROM {RT} WHERE outcome = 'BLOCKED_BY_POLICY' ORDER BY run_ts DESC LIMIT 20").collect()
if blocked:
    statuses = sorted({b["http_status"] for b in blocked})
    shape = json.loads(blocked[0]["policy_json"])
    keys = sorted(shape.keys()) if isinstance(shape, dict) else type(shape).__name__
    status = "Confirmed" if statuses == ["200"] and isinstance(shape, dict) and "reason" in json.dumps(shape) else "Partially confirmed"
    observed = f"{len(blocked)} blocked calls; http_status={statuses}; databricks_service_policy keys={keys}; example={json.dumps(shape)[:600]}"
    lngdemo.ledger_update(spark, LEDGER, 3, "red_team BLOCKED_BY_POLICY rows", observed, status)
    print("claim 3:", status, observed)

# Claim 4 - fail closed (job lngdemo-gov-05-fail-closed)
fc = latest("m7_fail_closed")
if fc:
    status = "Confirmed" if fc["outcome"] in ("BLOCKED_BY_POLICY",) else ("Contradicted" if fc["outcome"] == "ALLOWED" else "Partially confirmed")
    observed = f"broken policy on dev: outcome={fc['outcome']} http={fc['http_status']} policy={fc['policy_json']} error={fc['error']}"
    lngdemo.ledger_update(spark, LEDGER, 4, f"broken_policy_fail_closed attached to {p['copilot_dev_service']}", observed, status)
    print("claim 4:", status, observed)

# COMMAND ----------

# Claims 7 / 8 - Auto-TTL and column mask on inference tables vs. continued delivery
def exp(name, target):
    r = spark.sql(f"SELECT ok, detail, run_ts FROM {lngdemo.fq(CAT, CUR, 'experiment_log')} WHERE experiment=:e AND target=:t ORDER BY run_ts DESC LIMIT 1",
                  args={"e": name, "t": target}).collect()
    return r[0] if r else None

def rows_after(table, ts):
    if not lngdemo.table_exists(spark, table):
        return 0
    return spark.sql(f"SELECT count(*) c FROM {lngdemo.fq(*table.split('.'))} WHERE event_time > :ts", args={"ts": ts}).first()["c"]

ttl = exp("auto_ttl_inference", PROD)
if ttl:
    if not ttl["ok"]:
        lngdemo.ledger_update(spark, LEDGER, 7, f"ALTER TABLE {PROD} DELETE ROWS ...", f"ALTER failed: {ttl['detail'][:800]}", "Contradicted")
    else:
        n = rows_after(PROD, ttl["run_ts"])
        props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {lngdemo.fq(*PROD.split('.'))}").collect() if r["key"].startswith("autottl")}
        status = "Confirmed" if n > 0 else "Partially confirmed"
        lngdemo.ledger_update(spark, LEDGER, 7, f"Auto-TTL on {PROD}; count rows delivered after ALTER",
                            f"TTL props={props}; rows delivered after TTL set={n}" + ("" if n else " (send a request and re-run)"), status)
        print("claim 7:", status, props, n)

mask = exp("payload_column_mask", DEV)
if mask:
    if not mask["ok"]:
        lngdemo.ledger_update(spark, LEDGER, 8, f"SET MASK on {DEV}.request/response", f"mask failed: {mask['detail'][:800]}", "Contradicted")
    else:
        n = rows_after(DEV, mask["run_ts"])
        lngdemo.ledger_update(spark, LEDGER, 8, f"Column mask on {DEV} (dev only); rows delivered after mask",
                            f"rows delivered after mask={n}; trace-table row filter documented, tested only if trace_table set",
                            "Partially confirmed" if n > 0 else "Not yet tested")
        print("claim 8: rows after mask", n)

# COMMAND ----------

# Claim 11 - provider credentials are not returned on read
try:
    leaks = []
    for s in w.ai_gateway.list_model_provider_services():
        d = json.dumps(s.as_dict())
        if '"plaintext": "' in d and '"plaintext": ""' not in d:
            leaks.append(s.name)
    n = sum(1 for _ in w.ai_gateway.list_model_provider_services())
    lngdemo.ledger_update(spark, LEDGER, 11, f"GET/list {n} model provider services visible to the job runner; searched for plaintext values",
                        f"services with a returned plaintext credential: {leaks or 'none'}. Rotating direct provider keys stays a customer action outside Databricks.",
                        "Confirmed" if not leaks and n else ("Contradicted" if leaks else "Not yet tested"))
    print("claim 11:", leaks or "no credential values returned", f"({n} services checked)")
except Exception as e:  # noqa: BLE001
    print("claim 11 check failed:", e)

# COMMAND ----------

# Claim 12 - fragile patterns: did price-in-words / indirect references get through?
for tid in ("m5_price_in_words", "m5_indirect_counterparty"):
    r = latest(tid)
    if r:
        print(f"{tid}: outcome={r['outcome']}")
words = latest("m5_price_in_words")
if words:
    lngdemo.ledger_update(spark, LEDGER, 12, "m5_price_in_words against SDD + deny_contract_terms",
                        f"price written in words -> {words['outcome']}. Policy functions cannot use regex; only LIKE/CONTAINS.",
                        "Confirmed" if words["outcome"] == "ALLOWED" else "Partially confirmed")

# COMMAND ----------

display(spark.sql(f"SELECT claim_id, status, observed_result, tested_at FROM {LEDGER} ORDER BY claim_id"))
contra = spark.sql(f"SELECT claim_id, claim, observed_result FROM {LEDGER} WHERE status = 'Contradicted'").collect()
if contra:
    print("!!! CONTRADICTED CLAIMS - correct the customer deck before the session:")
    for c in contra:
        print(f"  #{c['claim_id']}: {c['claim']}\n     observed: {c['observed_result']}")
