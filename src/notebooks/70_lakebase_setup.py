# Databricks notebook source
# MAGIC %md
# MAGIC # 70 · Lakebase OLTP store: schema, row-level security, roles, dev branch (M9)
# MAGIC Runs as the bundle deployer, who owns the Lakebase project (CAN_MANAGE + `databricks_superuser`).
# MAGIC 1. **Production branch:** `trade_ops` schema, RLS by desk, operator view, seed nominations, GRANTs
# MAGIC    (persona SP OAuth roles → NO_LOGIN group roles; both role kinds come from `resources/lakebase.yml`).
# MAGIC 2. **Dev branch:** copy-on-write branch of production *after* seeding, contact data scrubbed, an OAuth role
# MAGIC    for the developer SP that exists **only on this branch**.
# MAGIC 3. **Unity Catalog:** grants on the catalog that registers the Lakebase database.
# MAGIC
# MAGIC Idempotent: `IF NOT EXISTS`, `ON CONFLICT DO NOTHING`, `GRANT`, create-if-missing for branch/endpoint/role.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.159.0" "psycopg[binary]>=3.2" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys, random, datetime as dt
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import postgres as pg
from google.protobuf.duration_pb2 import Duration

w = WorkspaceClient()
p = lngdemo.get_params(dbutils)
SQL = os.path.abspath("../lakebase")
subs = lngdemo.lb_subs(p)
PROD = lngdemo.lb_paths(p)
SCHEMA = p["lakebase_pg_schema"]
CUR = lngdemo.fq(p["catalog"], p["audit_curated_schema"], "experiment_log")
print("project", PROD["project"], "| endpoint", PROD["endpoint"])

def log(experiment, target, ok, detail):
    spark.sql(f"INSERT INTO {CUR} VALUES (current_timestamp(), :e, :t, :s, :ok, :d)",
              args={"e": experiment, "t": target, "s": "", "ok": ok, "d": str(detail)[:1500]})
    print(("OK   " if ok else "FAIL ") + experiment, target, "" if ok else str(detail)[:300])

# COMMAND ----------

# MAGIC %md ### 1 · Production branch: schema, RLS, seed, grants

# COMMAND ----------

conn = lngdemo.pg_connect(w, PROD["endpoint"], p["lakebase_database"])
lngdemo.pg_run_file(conn, f"{SQL}/01_schema.sql", subs)

desk_access = [(p["role_trader"], "Atlantic Desk"), (p["role_trader"], "Portfolio Desk"),
               (p["role_unentitled_trader"], "Pacific Desk")]
rng = random.Random(20261102)
desks = ["Atlantic Desk", "Pacific Desk", "Portfolio Desk"]
with conn.cursor() as cur:
    cur.executemany(f"INSERT INTO {SCHEMA}.desk_access (role_name, desk) VALUES (%s, %s) ON CONFLICT DO NOTHING", desk_access)
    rows = []
    for i in range(36):
        desk = desks[i % 3]
        rows.append((f"NOM-{2000 + i}", desk, f"Counterparty {'ABCDEFGH'[i % 8]}",
                     dt.date(2026, 12, 1) + dt.timedelta(days=7 * (i % 12)), rng.randint(30, 38) * 100_000,
                     rng.choice(["draft", "submitted", "confirmed"]), "seed", f"scheduler{i:02d}@example.com"))
    cur.executemany(f"""INSERT INTO {SCHEMA}.cargo_nominations
        (nomination_id, desk, counterparty_alias, cargo_window_start, volume_mmbtu, status, entered_by, contact_email)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (nomination_id) DO NOTHING""", rows)
lngdemo.pg_run_file(conn, f"{SQL}/02_grants.sql", subs)
with conn.cursor() as cur:
    cur.execute(f"SELECT desk, count(*) FROM {SCHEMA}.cargo_nominations GROUP BY desk ORDER BY desk")
    print("production rows by desk (owner view, RLS bypassed):", cur.fetchall())
conn.close()
log("lakebase_setup", PROD["branch"], True, "schema + RLS + seed + grants")

# COMMAND ----------

# MAGIC %md ### 2 · Developer branch (copy-on-write, created after seeding)

# COMMAND ----------

DEV = lngdemo.lb_paths(p, p["lakebase_dev_branch"])
branches = {b.name for b in w.postgres.list_branches(PROD["project"])}
if DEV["branch"] not in branches:
    w.postgres.create_branch(
        parent=PROD["project"], branch_id=p["lakebase_dev_branch"],
        branch=pg.Branch(spec=pg.BranchSpec(source_branch=PROD["branch"], ttl=Duration(seconds=14 * 24 * 3600)))).wait()
    print("created branch", DEV["branch"])
endpoints = {e.name for e in w.postgres.list_endpoints(DEV["branch"])}
if not endpoints:
    w.postgres.create_endpoint(
        parent=DEV["branch"], endpoint_id=p["lakebase_dev_endpoint"],
        endpoint=pg.Endpoint(spec=pg.EndpointSpec(endpoint_type=pg.EndpointType.ENDPOINT_TYPE_READ_WRITE,
                                                  autoscaling_limit_min_cu=0.5, autoscaling_limit_max_cu=1))).wait()
    print("created endpoint", DEV["endpoint"])
elif DEV["endpoint"] not in endpoints:
    DEV["endpoint"] = sorted(endpoints)[0]  # the platform auto-created one; use it
roles = {r.spec.postgres_role if r.spec else None for r in w.postgres.list_roles(DEV["branch"])}
if p["developer_sp"] not in roles:
    w.postgres.create_role(
        parent=DEV["branch"], role_id="sp-developer",
        role=pg.Role(spec=pg.RoleRoleSpec(postgres_role=p["developer_sp"],
                                          identity_type=pg.RoleIdentityType.SERVICE_PRINCIPAL,
                                          auth_method=pg.RoleAuthMethod.LAKEBASE_OAUTH_V1))).wait()
    print("created developer OAuth role on", DEV["branch"])

dconn = lngdemo.pg_connect(w, DEV["endpoint"], p["lakebase_database"])
lngdemo.pg_run_file(dconn, f"{SQL}/10_dev_branch.sql", subs)
with dconn.cursor() as cur:
    cur.execute(f"SELECT count(*), count(DISTINCT contact_email) FROM {SCHEMA}.cargo_nominations")
    print("dev branch rows / distinct contacts after scrub:", cur.fetchone())
    cur.execute("SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)",
                ([p["trader_sp"], p["operator_sp"], p["security_sp"], p["developer_sp"]],))
    inherited = [r[0] for r in cur.fetchall()]
dconn.close()
# Branches copy production roles too: record which persona roles exist on the dev branch (M9 limit / claim).
log("lakebase_dev_branch_roles", DEV["branch"], True, f"persona roles present on dev branch: {inherited}")

# COMMAND ----------

# MAGIC %md ### 3 · Unity Catalog grants on the registered Lakebase catalog
# MAGIC The registered catalog is a read-only mirror queried with serverless compute. Only security and auditors
# MAGIC are granted it; `72_lakebase_matrix` records which Postgres identity a UC query actually uses.

# COMMAND ----------

UC_LB = lngdemo.fq(p["lakebase_uc_catalog"])
for grp in (p["security_group"], p["auditor_group"]):
    for stmt in (f"GRANT USE CATALOG ON CATALOG {UC_LB} TO `{grp}`",
                 f"GRANT USE SCHEMA ON CATALOG {UC_LB} TO `{grp}`",
                 f"GRANT SELECT ON CATALOG {UC_LB} TO `{grp}`"):
        try:
            spark.sql(stmt)
            log("lakebase_uc_grant", p["lakebase_uc_catalog"], True, stmt)
        except Exception as e:  # noqa: BLE001
            log("lakebase_uc_grant", p["lakebase_uc_catalog"], False, f"{stmt}: {e}")
