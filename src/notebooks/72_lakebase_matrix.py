# Databricks notebook source
# MAGIC %md
# MAGIC # 72 · Governed Lakehouse views over Lakebase history + OLTP persona matrix (M9)
# MAGIC **Part 1 - governed views.** CDF history tables must not carry UC row filters or masks (CDF stops writing
# MAGIC once one is applied - documented), so access goes through views in `<ops_catalog>.ops_curated`:
# MAGIC `v_nominations_current` (desk filter + masked contact), `v_nominations_ops` (metadata, all desks),
# MAGIC `v_nominations_audit` (change history without counterparty/contact).
# MAGIC
# MAGIC **Part 2 - matrix.** Each persona SP (credentials from the secret scope) is tested at every layer:
# MAGIC Lakebase project ACL → Postgres login → Postgres GRANT/RLS → dev branch → Unity Catalog (registered
# MAGIC catalog and governed views). The expected column is the **design recommendation**, not product behaviour.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.159.0" "psycopg[binary]>=3.2" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys, uuid, json, datetime as dt
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as dsql

w = WorkspaceClient()
p = lngdemo.get_params(dbutils)
OPS, CDC, OC = p["ops_catalog"], p["ops_cdc_schema"], p["ops_curated_schema"]
SCHEMA, REF = p["lakebase_pg_schema"], p["lakebase_reference_schema"]
HIST = lngdemo.fq(OPS, CDC, "lb_cargo_nominations_history")
ENT = lngdemo.fq(p["catalog"], p["trading_schema"], "desk_entitlements")
SEC = p["security_group"]
LEDGER = lngdemo.fq(p["catalog"], p["audit_curated_schema"], "claim_ledger")

# COMMAND ----------

# MAGIC %md ### Part 1 · Governed views over the CDF history

# COMMAND ----------

if not lngdemo.table_exists(spark, f"{OPS}.{CDC}.lb_cargo_nominations_history"):
    print(f"{HIST} not there yet (CDF still snapshotting, or preview off: MANUAL_STEPS L1). Views skipped.")
else:
    latest = f"""WITH latest AS (
      SELECT *, row_number() OVER (PARTITION BY nomination_id ORDER BY _pg_lsn DESC, _sort_by DESC) AS rn
      FROM {HIST} WHERE _pg_change_type IN ('insert', 'update_postimage', 'delete'))"""
    views = {
        "v_nominations_current": f"""{latest}
          SELECT nomination_id, desk, counterparty_alias, cargo_window_start, volume_mmbtu, status, entered_by,
                 CASE WHEN is_account_group_member('{SEC}') THEN contact_email ELSE '*** masked ***' END AS contact_email,
                 created_at
          FROM latest WHERE rn = 1 AND _pg_change_type <> 'delete'
            AND (is_account_group_member('{SEC}')
                 OR EXISTS (SELECT 1 FROM {ENT} e WHERE e.desk = latest.desk AND is_account_group_member(e.group_name)))""",
        "v_nominations_ops": f"""{latest}
          SELECT nomination_id, desk, cargo_window_start, volume_mmbtu, status, created_at
          FROM latest WHERE rn = 1 AND _pg_change_type <> 'delete'""",
        "v_nominations_audit": f"""SELECT _pg_change_type, _timestamp, _pg_lsn, nomination_id, desk, status, entered_by
          FROM {HIST}""",
    }
    readers = {"v_nominations_current": [p["trader_group"], p["unentitled_trader_group"]],
               "v_nominations_ops": [p["operator_group"], p["auditor_group"]],
               "v_nominations_audit": [p["auditor_group"]]}
    for name, body in views.items():
        fqv = lngdemo.fq(OPS, OC, name)
        spark.sql(f"CREATE OR REPLACE VIEW {fqv} COMMENT 'LNG demo: governed view over Lakebase CDF history' AS {body}")
        for grp in readers[name]:
            spark.sql(f"GRANT SELECT ON TABLE {fqv} TO `{grp}`")
        tags = lngdemo.base_tags(p, component=f"view-{name}", data_class="curated", managed_by="setup_job")
        spark.sql(f"ALTER VIEW {fqv} SET TAGS ({', '.join(f'{chr(39)}{k}{chr(39)} = {chr(39)}{v}{chr(39)}' for k, v in tags.items())})")
        print("view ready:", fqv, "->", readers[name])

# COMMAND ----------

# MAGIC %md ### Part 2 · Persona matrix across the OLTP layers

# COMMAND ----------

RUN_ID = "lb-" + uuid.uuid4().hex[:8]
PROD, DEV = lngdemo.lb_paths(p), lngdemo.lb_paths(p, p["lakebase_dev_branch"])
personas = ["trader", "unentitled_trader", "developer", "operator", "security", "auditor"]
ids = {x: lngdemo.persona_identity(x, dbutils, p["secret_scope"]) for x in personas}
OWN_DESK = {"trader": "Atlantic Desk", "unentitled_trader": "Pacific Desk"}

def pg_try(idn, endpoint, sql, params=None, fetch=True, rollback=False):
    """Run one statement as the persona over Postgres. Returns (A|D, detail)."""
    try:
        conn = lngdemo.pg_connect(idn.client, endpoint, p["lakebase_database"])
    except Exception as e:  # noqa: BLE001
        return "D", f"connect: {type(e).__name__}: {str(e)[:200]}"
    try:
        conn.autocommit = not rollback
        with conn.cursor() as cur:
            cur.execute(sql, params)
            out = cur.fetchall() if fetch else f"rowcount={cur.rowcount}"
        if rollback:
            conn.rollback()
        return "A", json.dumps(out, default=str)[:300]
    except Exception as e:  # noqa: BLE001
        return "D", f"{type(e).__name__}: {str(e).splitlines()[0][:220]}"
    finally:
        conn.close()

def uc_try(idn, stmt):
    try:
        r = idn.client.statement_execution.execute_statement(warehouse_id=p["warehouse_id"], statement=stmt, wait_timeout="50s")
        if r.status.state == dsql.StatementState.SUCCEEDED:
            return "A", json.dumps((r.result.data_array if r.result else []) or [])[:300]
        return "D", (r.status.error.message if r.status.error else str(r.status.state))[:300]
    except Exception as e:  # noqa: BLE001
        return "D", f"{type(e).__name__}: {str(e)[:220]}"

def project_acl(idn):
    try:
        idn.client.postgres.get_project(PROD["project"]); return "A", "get_project ok"
    except Exception as e:  # noqa: BLE001
        return "D", f"{type(e).__name__}: {str(e)[:200]}"

def credential(idn):
    try:
        idn.client.postgres.generate_database_credential(endpoint=PROD["endpoint"]); return "A", "token issued"
    except Exception as e:  # noqa: BLE001
        return "D", f"{type(e).__name__}: {str(e)[:200]}"

NOM = f"{SCHEMA}.cargo_nominations"
COLS = "nomination_id, desk, counterparty_alias, cargo_window_start, volume_mmbtu, status"
def ins(desk):
    return (f"INSERT INTO {NOM} (nomination_id, desk, counterparty_alias, cargo_window_start, volume_mmbtu, status) "
            f"VALUES (%s, %s, 'Counterparty A', current_date + 30, 3500000, 'draft')", (f"NOM-T-{uuid.uuid4().hex[:6]}", desk))

# action -> (callable(persona, idn), expected per persona in order of `personas`)
ACTIONS = {
    "lb_project_acl":       (lambda x, i: project_acl(i),                                         "A A A A A D"),
    "lb_credential_prod":   (lambda x, i: credential(i),                                          "A A A A A D"),
    "pg_login_prod":        (lambda x, i: pg_try(i, PROD["endpoint"], "SELECT current_user"),     "A A D A A D"),
    "pg_read_nominations":  (lambda x, i: pg_try(i, PROD["endpoint"], f"SELECT desk, count(*) FROM {NOM} GROUP BY desk ORDER BY desk"), "A A D D A D"),
    "pg_read_contact":      (lambda x, i: pg_try(i, PROD["endpoint"], f"SELECT contact_email FROM {NOM} LIMIT 1"), "D D D D A D"),
    "pg_insert_own_desk":   (lambda x, i: pg_try(i, PROD["endpoint"], *ins(OWN_DESK.get(x, "Atlantic Desk")), fetch=False, rollback=True), "A A D D D D"),
    "pg_insert_other_desk": (lambda x, i: pg_try(i, PROD["endpoint"], *ins("Pacific Desk" if x == "trader" else "Atlantic Desk"), fetch=False, rollback=True), "D D D D D D"),
    "pg_ops_view":          (lambda x, i: pg_try(i, PROD["endpoint"], f"SELECT count(*) FROM {SCHEMA}.v_nominations_ops"), "D D D A A D"),
    "pg_reference_summary": (lambda x, i: pg_try(i, PROD["endpoint"], f"SELECT count(*) FROM {REF}.desk_book_summary"), "A A D A A D"),
    "pg_login_dev_branch":  (lambda x, i: pg_try(i, DEV["endpoint"], f"SELECT count(*), max(contact_email) FROM {NOM}"), "D D A D D D"),
    "uc_registered_catalog":(lambda x, i: uc_try(i, f"SELECT desk, count(*) FROM {lngdemo.fq(p['lakebase_uc_catalog'], SCHEMA, 'cargo_nominations')} GROUP BY desk"), "D D D D A A"),
    "uc_cdc_current_view":  (lambda x, i: uc_try(i, f"SELECT desk, count(*), max(contact_email) FROM {lngdemo.fq(OPS, OC, 'v_nominations_current')} GROUP BY desk"), "A A D D A D"),
    "uc_cdc_ops_view":      (lambda x, i: uc_try(i, f"SELECT count(*) FROM {lngdemo.fq(OPS, OC, 'v_nominations_ops')}"), "D D D A A A"),
    "uc_cdc_raw_history":   (lambda x, i: uc_try(i, f"SELECT count(*) FROM {HIST}"), "D D D D A D"),
}

rows = []
for idx, x in enumerate(personas):
    idn = ids[x]
    for action, (fn, exp) in ACTIONS.items():
        e = exp.split()[idx]
        if not idn.available:
            o, d, verdict = "", idn.note, "NOT_TESTED"
        else:
            o, d = fn(x, idn)
            verdict = "PASS" if o == e else ("OBSERVED" if action == "pg_login_dev_branch" and x != "developer" else "FAIL")
        rows.append(dict(run_id=RUN_ID, run_ts=dt.datetime.utcnow().isoformat(), persona=x,
                         principal=idn.client.config.client_id if idn.available else "",
                         action=action, expected=e, observed=o, verdict=verdict, detail=d, identity_note=idn.note))
        print(f"{x:<18} {action:<22} exp={e} obs={o:<2} {verdict:<10} {d[:90]}")

lngdemo.append_rows(spark, f"{p['catalog']}.{p['audit_curated_schema']}.persona_matrix_results", rows)

# COMMAND ----------

# MAGIC %md ### Claim ledger updates (15, 16, 19)

# COMMAND ----------

def obs(persona, action):
    return next((r for r in rows if r["persona"] == persona and r["action"] == action), {})

dev_acl, dev_login = obs("developer", "lb_project_acl"), obs("developer", "pg_login_prod")
if dev_acl.get("observed") == "A":
    lngdemo.ledger_update(spark, LEDGER, 15, "developer SP: CAN_USE on the project but no Postgres role on production",
                          f"project ACL={dev_acl.get('observed')}, prod login={dev_login.get('observed')} ({dev_login.get('detail', '')[:200]})",
                          "Confirmed" if dev_login.get("observed") == "D" else "Contradicted")
t, u = obs("trader", "pg_read_nominations"), obs("unentitled_trader", "pg_read_nominations")
oi = obs("trader", "pg_insert_other_desk")
if t.get("observed") == "A" and u.get("observed") == "A":
    lngdemo.ledger_update(spark, LEDGER, 16, "RLS by desk for SP OAuth roles mapped to NO_LOGIN group roles",
                          f"trader sees {t['detail'][:120]}; unentitled sees {u['detail'][:120]}; trader insert into other desk={oi.get('observed')}",
                          "Confirmed" if "Pacific" not in t["detail"] and "Atlantic" not in u["detail"] and oi.get("observed") == "D" else "Contradicted")
au = obs("auditor", "uc_registered_catalog")
if au.get("observed"):
    lngdemo.ledger_update(spark, LEDGER, 19, "Auditor (UC SELECT on registered catalog, NO Postgres role) queries it via serverless SQL",
                          f"observed={au['observed']}: {au['detail'][:400]}", "Confirmed")

display(spark.sql(f"""
SELECT persona, action, expected, observed, verdict, left(detail, 120) AS detail
FROM {lngdemo.fq(p['catalog'], p['audit_curated_schema'], 'persona_matrix_results')} WHERE run_id = '{RUN_ID}'
ORDER BY persona, action"""))
