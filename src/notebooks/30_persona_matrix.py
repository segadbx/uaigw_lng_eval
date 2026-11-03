# Databricks notebook source
# MAGIC %md
# MAGIC # 30 · Separation-of-duties matrix (M6)
# MAGIC Each persona is a **service principal placed in the persona's account group** (credentials in the
# MAGIC secret scope). Human user tokens are not used; the output says so on every row.
# MAGIC
# MAGIC The expected column is the **design recommendation** for this demo, not product behaviour.
# MAGIC Also measures grant propagation (claim 9) by revoking/re-granting EXECUTE on `trading_copilot_dev`.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.157.0" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys, time, uuid, json, datetime as dt
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam, sql as dsql, catalog as c

w = WorkspaceClient()
p = lngdemo.get_params(dbutils)
CAT, SVC, RAW, CUR, TS = p["catalog"], p["services_schema"], p["audit_raw_schema"], p["audit_curated_schema"], p["trading_schema"]
WH = p["warehouse_id"]
PROD, DEV = p["copilot_service"], p["copilot_dev_service"]
RUN_ID = uuid.uuid4().hex[:10]
personas = [x for x in lngdemo.PERSONAS if x != "app"]
ids = {x: lngdemo.persona_identity(x, dbutils, p["secret_scope"]) for x in personas}
print({k: ("available" if v.available else v.note) for k, v in ids.items()})

# Design recommendation (A = allow, D = deny)
EXPECTED = {
    #                 query_prod query_dev read_raw read_curated read_contracts change_service change_grant
    "trader":            ["A", "D", "D", "D", "A", "D", "D"],
    "unentitled_trader": ["D", "D", "D", "D", "A", "D", "D"],  # reads only own desk rows, masked prices
    "developer":         ["D", "A", "D", "D", "D", "D", "D"],
    "operator":          ["D", "D", "D", "A", "D", "D", "D"],
    "security":          ["D", "D", "A", "A", "D", "A", "D"],
    "auditor":           ["D", "D", "D", "A", "D", "D", "D"],
}
ACTIONS = ["query_prod", "query_dev", "read_audit_raw", "read_audit_curated", "read_contracts", "change_service", "change_grant"]

# COMMAND ----------

# MAGIC %md ### Warehouse access for persona SPs (shared object → gated)

# COMMAND ----------

sp_ids = {}
for x, idn in ids.items():
    if idn.available:
        sp_ids[x] = idn.client.config.client_id
plan = [f"CAN_USE on warehouse {WH} for SP {a} ({x})" for x, a in sp_ids.items()]
print("PLAN:", plan or "no persona credentials")
if plan and lngdemo.shared_changes_confirmed(p):
    w.permissions.update("warehouses", WH, access_control_list=[
        iam.AccessControlRequest(service_principal_name=a, permission_level=iam.PermissionLevel.CAN_USE) for a in sp_ids.values()])
    print("granted")
elif plan:
    print("confirm_shared_changes != true -> not granted; SQL actions will fail with a warehouse permission error unless already granted.")

# COMMAND ----------

def sql_as(idn, stmt):
    try:
        r = idn.client.statement_execution.execute_statement(warehouse_id=WH, statement=stmt, wait_timeout="50s")
        st = r.status.state
        if st == dsql.StatementState.SUCCEEDED:
            data = (r.result.data_array if r.result else None) or []
            return "A", json.dumps(data[:3])[:300]
        return "D", f"{st}: {(r.status.error.message if r.status.error else '')[:300]}"
    except Exception as e:  # noqa: BLE001
        return "D", f"{type(e).__name__}: {str(e)[:300]}"

def call_as(idn, svc):
    res = lngdemo.chat(idn, svc, "Reply with the single word OK.", tags={"use_case": "persona_matrix", "run_id": RUN_ID}, max_tokens=5)
    return ("A" if res.outcome() in ("ALLOWED", "BLOCKED_BY_POLICY") else "D"), f"{res.outcome()} http={res.http_status}"

from databricks.sdk.common.types.fieldmask import FieldMask

def change_service_as(idn):
    name = f"model-services/{PROD}"
    try:
        orig = w.ai_gateway.get_model_service(name).comment
        idn.client.ai_gateway.update_model_service(name=name, update_mask=FieldMask(["comment"]),
                                                   model_service=c.ModelService(comment=(orig or "") + " [persona-matrix probe]"))
        w.ai_gateway.update_model_service(name=name, update_mask=FieldMask(["comment"]), model_service=c.ModelService(comment=orig))
        return "A", "comment updated and reverted"
    except Exception as e:  # noqa: BLE001
        return "D", f"{type(e).__name__}: {str(e)[:300]}"

def change_grant_as(idn):
    a, d = sql_as(idn, f"GRANT SELECT ON TABLE {lngdemo.fq(CAT, TS, 'contracts')} TO `account users`")
    if a == "A":
        spark.sql(f"REVOKE SELECT ON TABLE {lngdemo.fq(CAT, TS, 'contracts')} FROM `account users`")
        d += " (reverted)"
    return a, d

RAW_T = lngdemo.qn(p["copilot_payload"])
CUR_T = lngdemo.fq(CAT, CUR, "red_team_results")
CON_T = lngdemo.fq(CAT, TS, "contracts")

rows = []
for x, idn in ids.items():
    if not idn.available:
        for a, e in zip(ACTIONS, EXPECTED[x]):
            rows.append(dict(run_id=RUN_ID, persona=x, principal="", action=a, expected=e, observed="", verdict="NOT_TESTED", detail=idn.note, identity_note=idn.note))
        continue
    principal = idn.principal()
    results = [
        call_as(idn, PROD), call_as(idn, DEV),
        sql_as(idn, f"SELECT count(*) FROM {RAW_T}"),
        sql_as(idn, f"SELECT count(*) FROM {CUR_T}"),
        sql_as(idn, f"SELECT desk, count(*), max(price_formula) FROM {CON_T} GROUP BY desk"),
        change_service_as(idn), change_grant_as(idn),
    ]
    for a, e, (o, d) in zip(ACTIONS, EXPECTED[x], results):
        rows.append(dict(run_id=RUN_ID, persona=x, principal=principal, action=a, expected=e, observed=o,
                         verdict="PASS" if o == e else "FAIL", detail=d, identity_note=idn.note))

for r in rows:
    r["run_ts"] = dt.datetime.utcnow().isoformat()
lngdemo.append_rows(spark, f"{CAT}.{CUR}.persona_matrix_results", rows)

# COMMAND ----------

display(spark.sql(f"""
SELECT persona,
  max(CASE WHEN action='query_prod' THEN concat(observed, ' / ', verdict) END) AS query_prod,
  max(CASE WHEN action='query_dev' THEN concat(observed, ' / ', verdict) END) AS query_dev,
  max(CASE WHEN action='read_audit_raw' THEN concat(observed, ' / ', verdict) END) AS read_audit_raw,
  max(CASE WHEN action='read_audit_curated' THEN concat(observed, ' / ', verdict) END) AS read_audit_curated,
  max(CASE WHEN action='read_contracts' THEN concat(observed, ' / ', verdict) END) AS read_contracts,
  max(CASE WHEN action='change_service' THEN concat(observed, ' / ', verdict) END) AS change_service,
  max(CASE WHEN action='change_grant' THEN concat(observed, ' / ', verdict) END) AS change_grant,
  max(identity_note) AS identity
FROM {lngdemo.fq(CAT, CUR, 'persona_matrix_results')} WHERE run_id = '{RUN_ID}' GROUP BY persona ORDER BY persona"""))

# COMMAND ----------

# MAGIC %md ### Claim 9 · grant propagation (revoke → deny, re-grant → allow) on trading_copilot_dev
# MAGIC Temporarily drifts from the bundle's grant; it is restored here (and on the next `bundle deploy`).

# COMMAND ----------

dev_idn = ids["developer"]
def poll(want, limit_s=600, every=10):
    t0 = time.time()
    while time.time() - t0 < limit_s:
        o, _ = call_as(dev_idn, DEV)
        if o == want:
            return round(time.time() - t0)
        time.sleep(every)
    return None

if dev_idn.available and call_as(dev_idn, DEV)[0] == "A":
    g = p["developer_group"]
    w.grants.update("model_service", DEV, changes=[c.PermissionsChange(principal=g, remove=[c.Privilege.EXECUTE])])
    revoke_s = poll("D")
    w.grants.update("model_service", DEV, changes=[c.PermissionsChange(principal=g, add=[c.Privilege.EXECUTE])])
    grant_s = poll("A")
    observed = f"revoke EXECUTE -> denied after {revoke_s}s; re-grant -> allowed after {grant_s}s (10s polling; None = not within 600s)"
    print(observed)
    lngdemo.ledger_update(spark, lngdemo.fq(CAT, CUR, "claim_ledger"), 9, f"Revoke/re-grant EXECUTE on {DEV} for the developer group, poll every 10s",
                        observed, "Confirmed" if revoke_s is not None and grant_s is not None else "Partially confirmed")
else:
    print("claim 9 not measured: developer persona credentials missing or developer cannot call dev service")
