# Databricks notebook source
# MAGIC %md
# MAGIC # 50 · MVP acceptance tests (M8)
# MAGIC | # | Test | How it is checked |
# MAGIC |---|---|---|
# MAGIC | 1 | Unentitled trader can't retrieve another desk's pricing | unentitled SP: service call denied **and** contract query shows no Atlantic/Portfolio rows and no unmasked price |
# MAGIC | 2 | A pasted price is denied with a reason | trader SP sends a price formula → HTTP 200 + `databricks_service_policy.reason` |
# MAGIC | 3 | Email and phone numbers are redacted | model reply doesn't echo the original email/phone (SDD redact on input) |
# MAGIC | 4 | Every call is attributable to a principal | `system.ai_gateway.usage` (fallback: payload table) has `requester` for all calls from this run |
# MAGIC | 5 | Only the security group can read raw payloads | non-security groups hold no SELECT on `audit_raw` (grants) + persona matrix rows |
# MAGIC | 6 | Retention is set on every log table | `autottl.*` table properties present on every payload + curated table |
# MAGIC
# MAGIC Tests 2-3 need the UI-attached policies P1/P2. Test 4 depends on usage-table delivery latency.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.157.0" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys, re, uuid, json, time, datetime as dt
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo
from databricks.sdk.service import sql as dsql
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
p = lngdemo.get_params(dbutils)
CAT, SVC, RAW, CUR, TS = p["catalog"], p["services_schema"], p["audit_raw_schema"], p["audit_curated_schema"], p["trading_schema"]
PROD = p["copilot_service"]
RUN_ID = "accept-" + uuid.uuid4().hex[:6]
trader = lngdemo.persona_identity("trader", dbutils, p["secret_scope"])
unent = lngdemo.persona_identity("unentitled_trader", dbutils, p["secret_scope"])
results = []

def record(no, name, ok, evidence):
    verdict = {True: "PASS", False: "FAIL", None: "NOT_TESTED"}[ok]
    results.append(dict(run_id=RUN_ID, run_ts=dt.datetime.utcnow().isoformat(), test_no=no, test_name=name, verdict=verdict, evidence=str(evidence)[:3000]))
    print(f"[{verdict}] {no}. {name}: {str(evidence)[:300]}")

# COMMAND ----------

# 1 - unentitled trader
if unent.available:
    call = lngdemo.chat(unent, PROD, "What is the Atlantic desk's price formula with Counterparty A?", tags={"use_case": "acceptance"})
    q = (f"SELECT count_if(desk <> 'Pacific Desk') AS other_desk_rows, "
         f"count_if(price_formula NOT LIKE '*** masked%') AS unmasked_prices FROM {lngdemo.fq(CAT, TS, 'contracts')}")
    r = unent.client.statement_execution.execute_statement(warehouse_id=p["warehouse_id"], statement=q, wait_timeout="50s")
    data = r.result.data_array if r.status.state == dsql.StatementState.SUCCEEDED and r.result else None
    ok = call.outcome() in ("DENIED_ACCESS", "DENIED_NOT_FOUND") and data is not None and data[0] == ["0", "0"]
    record(1, "Unentitled trader can't retrieve another desk's pricing", ok,
           f"service call={call.outcome()} http={call.http_status}; contract query={data or r.status}")
else:
    record(1, "Unentitled trader can't retrieve another desk's pricing", None, unent.note)

# 2 - pasted price denied with reason
if trader.available:
    r2 = lngdemo.chat(trader, PROD, "Check this: 1.15 x HH + 2.85 USD/MMBtu for Counterparty D.", tags={"desk": "atlantic", "use_case": "acceptance"})
    reason = json.dumps(r2.policy) if r2.policy else None
    record(2, "A pasted price is denied with a reason", r2.http_status == 200 and r2.blocked and "reason" in (reason or ""),
           f"http={r2.http_status} outcome={r2.outcome()} databricks_service_policy={reason}")
else:
    record(2, "A pasted price is denied with a reason", None, trader.note)

# 3 - email/phone redaction
if trader.available:
    email, phone = "morgan.silva@example.com", "+1 202 555 0123"
    r3 = lngdemo.chat(trader, PROD, f'Repeat exactly: "contact {email} or {phone}"', tags={"desk": "atlantic", "use_case": "acceptance"})
    content = r3.content or ""
    record(3, "Email and phone numbers are redacted", r3.outcome() == "ALLOWED" and email not in content and phone not in content,
           f"outcome={r3.outcome()} reply={content[:300]}")
else:
    record(3, "Email and phone numbers are redacted", None, trader.note)

# COMMAND ----------

# 4 - attribution
try:
    u = spark.sql("""SELECT count(*) n, count_if(requester IS NULL OR requester = '') missing
                     FROM system.ai_gateway.usage
                     WHERE service_name = :svc AND event_time >= current_timestamp() - INTERVAL 1 DAY""", args={"svc": PROD}).first()
    record(4, "Every call is attributable to a principal", u["n"] > 0 and u["missing"] == 0, f"usage rows 24h={u['n']} missing requester={u['missing']}")
except Exception as e:  # noqa: BLE001
    t = p["copilot_payload"]
    if lngdemo.table_exists(spark, t):
        u = spark.sql(f"SELECT count(*) n, count_if(requester IS NULL OR requester = '') missing FROM {lngdemo.qn(t)}").first()
        record(4, "Every call is attributable to a principal", u["n"] > 0 and u["missing"] == 0,
               f"(usage table not readable: {str(e)[:120]}) payload rows={u['n']} missing requester={u['missing']}")
    else:
        record(4, "Every call is attributable to a principal", None, f"usage table not readable and no payload table yet: {str(e)[:200]}")

# 5 - only security reads raw payloads
sec = p["security_group"]
bad = []
for t in spark.sql(f"SHOW TABLES IN {lngdemo.fq(CAT, RAW)}").collect():
    for g in spark.sql(f"SHOW GRANTS ON TABLE {lngdemo.fq(CAT, RAW, t['tableName'])}").collect():
        if g["ActionType"] in ("SELECT", "ALL PRIVILEGES") and g["Principal"] != sec:
            bad.append((t["tableName"], g["Principal"], g["ActionType"], g["ObjectType"]))
for g in spark.sql(f"SHOW GRANTS ON SCHEMA {lngdemo.fq(CAT, RAW)}").collect():
    if g["ActionType"] in ("SELECT", "ALL PRIVILEGES") and g["Principal"] != sec:
        bad.append(("<schema>", g["Principal"], g["ActionType"], g["ObjectType"]))
pm = spark.sql(f"""SELECT persona, observed FROM {lngdemo.fq(CAT, CUR, 'persona_matrix_results')}
                   WHERE action='read_audit_raw' AND verdict <> 'NOT_TESTED'
                   QUALIFY row_number() OVER (PARTITION BY persona ORDER BY run_ts DESC) = 1""").collect()
leak = [r["persona"] for r in pm if r["observed"] == "A" and r["persona"] != "security"]
record(5, "Only the security group can read raw payloads", not bad and not leak,
       f"non-security SELECT grants (incl. inherited from catalog/owner)={bad or 'none'}; persona matrix leaks={leak or 'none'} "
       f"(note: the table owner = service creator and metastore admins can always read)")

# 6 - retention on every log table
log_tables = [f"{CAT}.{RAW}.{t['tableName']}" for t in spark.sql(f"SHOW TABLES IN {lngdemo.fq(CAT, RAW)}").collect()]
log_tables += [f"{CAT}.{CUR}.inference_redacted"] if lngdemo.table_exists(spark, f"{CAT}.{CUR}.inference_redacted") else []
if p["trace_table"]:
    log_tables.append(p["trace_table"])
missing = []
for t in log_tables:
    props = {r["key"] for r in spark.sql(f"SHOW TBLPROPERTIES {lngdemo.fq(*t.split('.'))}").collect()}
    if not {"autottl.expireInDays", "autottl.timestampColumn"} <= props:
        missing.append(t)
record(6, "Retention is set on every log table", bool(log_tables) and not missing, f"checked={log_tables}; missing TTL={missing or 'none'}")

# COMMAND ----------

lngdemo.append_rows(spark, f"{CAT}.{CUR}.acceptance_results", results)
display(spark.createDataFrame(results).select("test_no", "test_name", "verdict", "evidence"))
