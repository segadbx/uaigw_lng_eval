# Databricks notebook source
# MAGIC %md
# MAGIC # 60 · Evidence pack (M7)
# MAGIC Writes `/Volumes/<catalog>/audit_curated/evidence/evidence_<UTC timestamp>.md` with:
# MAGIC grants (services, schemas, log tables) · policy inventory (ABAC via API; service policies:
# MAGIC manual capture, since no API is available during the Beta) · red-team, persona-matrix and acceptance results
# MAGIC · retention settings · audit-log changes in the demo window · claim ledger.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.157.0" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys, json, datetime as dt
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
p = lngdemo.get_params(dbutils)
CAT, SVC, RAW, CUR, TS = p["catalog"], p["services_schema"], p["audit_raw_schema"], p["audit_curated_schema"], p["trading_schema"]
now = dt.datetime.utcnow()
md = [f"# LNG demo AI governance demo - evidence pack\n", f"Generated {now:%Y-%m-%d %H:%M} UTC by `{w.current_user.me().user_name}`. "
      "All data is synthetic.\n"]

def table_md(rows, cols=None, limit=200):
    rows = [r.asDict() if hasattr(r, "asDict") else r for r in rows][:limit]
    if not rows:
        return "_no rows_\n"
    cols = cols or list(rows[0].keys())
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in rows:
        out.append("| " + " | ".join(str(r.get(c, "")).replace("|", "\\|").replace("\n", " ")[:300] for c in cols) + " |")
    return "\n".join(out) + "\n"

def section(title, body):
    md.append(f"\n## {title}\n\n{body}")

def try_sql(q, args=None):
    try:
        return spark.sql(q, args=args or {}).collect()
    except Exception as e:  # noqa: BLE001
        return [{"error": str(e)[:300]}]

# COMMAND ----------

# Grants on model services (UC grants API, securable type model_service)
g_rows = []
for role, svc in lngdemo.services(p).items():
    sid = svc["service"].split(".")[-1]
    full = svc["service"]
    try:
        ms = w.ai_gateway.get_model_service(f"model-services/{full}")
        owner = ms.effective_owner
        for a in (w.grants.get("model_service", full).privilege_assignments or []):
            g_rows.append({"service": full, "owner": owner, "principal": a.principal,
                           "privileges": ", ".join(str(x.value if hasattr(x, "value") else x) for x in (a.privileges or []))})
    except Exception as e:  # noqa: BLE001
        g_rows.append({"service": full, "owner": "?", "principal": "error", "privileges": str(e)[:200]})
section("Grants - model services", table_md(g_rows) + "\n_ABAC GRANT-policy privileges, if any, do not appear in SHOW GRANTS (documented)._\n")

for s in (SVC, RAW, CUR, TS):
    section(f"Grants - schema {CAT}.{s}", table_md(try_sql(f"SHOW GRANTS ON SCHEMA {lngdemo.fq(CAT, s)}")))

for t in try_sql(f"SHOW TABLES IN {lngdemo.fq(CAT, RAW)}"):
    if "tableName" in (t.asDict() if hasattr(t, "asDict") else t):
        section(f"Grants - log table {CAT}.{RAW}.{t['tableName']}", table_md(try_sql(f"SHOW GRANTS ON TABLE {lngdemo.fq(CAT, RAW, t['tableName'])}")))

# COMMAND ----------

# Policy inventory
abac = []
try:
    # include_inherited: the contact-PII ABAC policy (M10) is defined on the trading_data schema
    for pol in w.policies.list_policies(on_securable_type="TABLE", on_securable_fullname=f"{CAT}.{TS}.contracts", include_inherited=True):
        abac.append({"name": pol.name, "type": str(pol.policy_type), "to": pol.to_principals, "except": pol.except_principals,
                     "match_columns": str(pol.match_columns)[:200]})
except Exception as e:  # noqa: BLE001
    abac.append({"name": "error", "type": str(e)[:200]})
mode = try_sql(f"SELECT value FROM {lngdemo.fq(CAT, CUR, 'setup_state')} WHERE key='contract_protection_mode'")
section("Policy inventory - Unity Catalog ABAC (contracts table)",
        f"Protection mode: `{mode[0]['value'] if mode and 'value' in mode[0] else 'unknown'}`\n\n" + table_md(abac))
section("Policy inventory - Unity Gateway service policies",
        "No API exposes service-policy attachments during the Beta (SDK "
        f"`ai_gateway` methods: {[m for m in dir(w.ai_gateway) if not m.startswith('_')]}).\n\n"
        "**Manual capture:** screenshot each service's *Policies* tab ({p['copilot_service']}, {p['copilot_dev_service']}) and paste below.\n\n"
        "| Service | Policy | Type | Phase | Rank | Mode |\n|---|---|---|---|---|---|\n| | | | | | |\n")

# COMMAND ----------

section("Red-team results (latest run per test)", table_md(try_sql(f"""
SELECT module, test_id, persona, expected, outcome, verdict, http_status, left(policy_json, 200) AS policy
FROM {lngdemo.fq(CAT, CUR, 'red_team_results')}
QUALIFY row_number() OVER (PARTITION BY test_id ORDER BY run_ts DESC) = 1 ORDER BY module, test_id""")))
section("Persona matrix (latest run)", table_md(try_sql(f"""
SELECT persona, action, expected, observed, verdict, left(detail, 150) AS detail, identity_note
FROM {lngdemo.fq(CAT, CUR, 'persona_matrix_results')}
WHERE run_id = (SELECT run_id FROM {lngdemo.fq(CAT, CUR, 'persona_matrix_results')} ORDER BY run_ts DESC LIMIT 1)
ORDER BY persona, action""")))
section("Acceptance tests (latest run)", table_md(try_sql(f"""
SELECT test_no, test_name, verdict, left(evidence, 300) AS evidence FROM {lngdemo.fq(CAT, CUR, 'acceptance_results')}
QUALIFY row_number() OVER (PARTITION BY test_no ORDER BY run_ts DESC) = 1 ORDER BY test_no""")))

# Retention
ret = []
tables = [f"{CAT}.{RAW}.{t['tableName']}" for t in try_sql(f"SHOW TABLES IN {lngdemo.fq(CAT, RAW)}") if hasattr(t, "asDict")]
tables += [f"{CAT}.{CUR}.inference_redacted"] + ([p["trace_table"]] if p["trace_table"] else [])
for t in tables:
    props = {r["key"]: r["value"] for r in try_sql(f"SHOW TBLPROPERTIES {lngdemo.fq(*t.split('.'))}") if hasattr(r, "asDict") and r["key"].startswith("autottl")}
    ret.append({"table": t, "autottl.expireInDays": props.get("autottl.expireInDays", "NOT SET"), "autottl.timestampColumn": props.get("autottl.timestampColumn", "")})
section("Retention (Auto-TTL)", table_md(ret))
section("Log-table experiments", table_md(try_sql(f"SELECT run_ts, experiment, target, ok, left(detail, 200) AS detail FROM {lngdemo.fq(CAT, CUR, 'experiment_log')} ORDER BY run_ts DESC LIMIT 60")))

# COMMAND ----------

# Audit log: grant / policy / model-service changes touching the demo catalog in the last 7 days
section("Audit log - grant and configuration changes (last 7 days)", table_md(try_sql("""
SELECT event_time, user_identity.email AS actor, service_name, action_name,
       left(to_json(request_params), 250) AS request_params, response.status_code AS status
FROM system.access.audit
WHERE event_time >= current_timestamp() - INTERVAL 7 DAYS
  AND (action_name ILIKE '%permission%' OR action_name ILIKE '%grant%' OR action_name ILIKE '%policy%'
       OR action_name ILIKE '%modelservice%' OR action_name ILIKE '%model_service%')
  AND to_json(request_params) ILIKE :cat
ORDER BY event_time DESC LIMIT 200""", {"cat": f"%{CAT}%"})) +
"\n_action_name values for Unity Gateway operations are matched by pattern; verify exact names in system.access.audit for this workspace._\n")

section("Claim ledger", table_md(try_sql(f"SELECT claim_id, status, claim, left(observed_result, 250) AS observed, tested_at FROM {lngdemo.fq(CAT, CUR, 'claim_ledger')} ORDER BY claim_id")))

# COMMAND ----------

path = f"/Volumes/{CAT}/{CUR}/evidence/evidence_{now:%Y%m%dT%H%M%SZ}.md"
with open(path, "w") as f:
    f.write("\n".join(md))
print("wrote", path, f"({sum(len(x) for x in md)} chars)")
displayHTML(f"<pre>{chr(10).join(md)[:20000]}</pre>")
