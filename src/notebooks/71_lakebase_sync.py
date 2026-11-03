# Databricks notebook source
# MAGIC %md
# MAGIC # 71 · Data movement between Lakehouse and Lakebase, and what governance travels with it (M9)
# MAGIC | Direction | Mechanism | Governance question |
# MAGIC |---|---|---|
# MAGIC | Lakehouse → Lakebase | **Synced table** of the aggregate-only `desk_book_summary` | Only curated data leaves the lakehouse |
# MAGIC | Lakehouse → Lakebase | **Experiment:** synced table of the row-filtered, masked `contracts` table | Are UC row filters / masks carried into Postgres? (claim 17) |
# MAGIC | Lakebase → Lakehouse | **Lakebase Change Data Feed** of `trade_ops` into `<ops_catalog>.lakebase_cdc` (`lb_<table>_history`) | History lands in UC, where UC grants apply (claims 18, 20) |
# MAGIC
# MAGIC Every outcome goes to `audit_curated.experiment_log`. Lakebase CDF is Public Preview: if the workspace
# MAGIC preview is off, this notebook records the error and MANUAL_STEPS L1 applies.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.159.0" "psycopg[binary]>=3.2" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys, time
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import postgres as pg

w = WorkspaceClient()
p = lngdemo.get_params(dbutils)
PROD = lngdemo.lb_paths(p)
SQL = os.path.abspath("../lakebase")
LOG = lngdemo.fq(p["catalog"], p["audit_curated_schema"], "experiment_log")
LEDGER = lngdemo.fq(p["catalog"], p["audit_curated_schema"], "claim_ledger")
REF = p["lakebase_reference_schema"]

def log(experiment, target, ok, detail):
    spark.sql(f"INSERT INTO {LOG} VALUES (current_timestamp(), :e, :t, '', :ok, :d)",
              args={"e": experiment, "t": target, "ok": ok, "d": str(detail)[:1500]})
    print(("OK   " if ok else "FAIL ") + experiment, target, str(detail)[:300])

def synced_table(source: str, table: str, pk: list[str]) -> str:
    """Create (if missing) a SNAPSHOT synced table <uc_lakebase_catalog>.<reference>.<table>; return its id."""
    sid = f"{p['lakebase_uc_catalog']}.{REF}.{table}"
    try:
        w.postgres.get_synced_table(f"synced_tables/{sid}")
        print("exists:", sid)
        return sid
    except Exception:  # noqa: BLE001  (not found -> create)
        pass
    w.postgres.create_synced_table(synced_table_id=sid, synced_table=pg.SyncedTable(spec=pg.SyncedTableSyncedTableSpec(
        source_table_full_name=source, primary_key_columns=pk,
        scheduling_policy=pg.SyncedTableSyncedTableSpecSyncedTableSchedulingPolicy.SNAPSHOT,
        branch=PROD["branch"], postgres_database=p["lakebase_database"], create_database_objects_if_missing=True,
        new_pipeline_spec=pg.NewPipelineSpec(storage_catalog=p["ops_catalog"], storage_schema=p["ops_pipeline_schema"]))))
    print("created:", sid)
    return sid

def wait_online(sid: str, timeout_s: int = 1200) -> str:
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        st = w.postgres.get_synced_table(f"synced_tables/{sid}").status
        state = str(st.detailed_state) if st else "UNKNOWN"
        if "ONLINE" in state or "FAILED" in state or "OFFLINE" in state:
            return state + (f" | {st.message}" if st and st.message else "")
        time.sleep(20)
    return "TIMEOUT"

# COMMAND ----------

# MAGIC %md ### 1 · Lakehouse → Lakebase: aggregate-only reference table

# COMMAND ----------

src = f"{p['catalog']}.{p['trading_schema']}.desk_book_summary"
try:
    sid = synced_table(src, "desk_book_summary", ["summary_id"])
    state = wait_online(sid)
    log("lakebase_synced_table", sid, "ONLINE" in state, state)
    conn = lngdemo.pg_connect(w, PROD["endpoint"], p["lakebase_database"])
    lngdemo.pg_run_file(conn, f"{SQL}/03_reference_grants.sql", lngdemo.lb_subs(p))
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {REF}.desk_book_summary")
        print("rows in Postgres reference table:", cur.fetchone()[0])
    conn.close()
except Exception as e:  # noqa: BLE001
    log("lakebase_synced_table", src, False, e)

# COMMAND ----------

# MAGIC %md ### 2 · Experiment: do UC row filters / column masks travel with a synced table? (claim 17)
# MAGIC Source = `contracts` (desk row filter + price masks). The sync pipeline runs as its creator (the job
# MAGIC runner, who is in no persona group). Whatever lands in Postgres is then readable through Postgres GRANTs
# MAGIC alone. The probe table is removed at the end; nothing is granted on it.

# COMMAND ----------

probe_src = f"{p['catalog']}.{p['trading_schema']}.contracts"
probe_id = None
try:
    probe_id = synced_table(probe_src, "fgac_probe_contracts", ["contract_id"])
    state = wait_online(probe_id)
    detail = f"state={state}"
    if "ONLINE" in state:
        conn = lngdemo.pg_connect(w, PROD["endpoint"], p["lakebase_database"])
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*), count(DISTINCT desk), max(price_formula) FROM {REF}.fgac_probe_contracts")
            n, ndesk, sample = cur.fetchone()
        conn.close()
        detail += f"; rows in Postgres={n}, desks={ndesk}, sample price_formula={sample!r}"
    log("lakebase_fgac_probe", probe_id, True, detail)
    lngdemo.ledger_update(spark, LEDGER, 17, f"SNAPSHOT synced table from row-filtered/masked {probe_src}", detail,
                          "Partially confirmed" if "ONLINE" in state else "Not yet tested")
except Exception as e:  # noqa: BLE001
    log("lakebase_fgac_probe", probe_src, False, e)
    lngdemo.ledger_update(spark, LEDGER, 17, f"SNAPSHOT synced table from row-filtered/masked {probe_src}",
                          f"create failed: {str(e)[:800]}", "Partially confirmed")
finally:
    if probe_id:
        try:
            w.postgres.delete_synced_table(f"synced_tables/{probe_id}").wait()
            conn = lngdemo.pg_connect(w, PROD["endpoint"], p["lakebase_database"])
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {REF}.fgac_probe_contracts")
            conn.close()
            print("probe removed")
        except Exception as e:  # noqa: BLE001
            print("probe cleanup failed (see MANUAL_STEPS L3):", e)

# COMMAND ----------

# MAGIC %md ### 3 · Lakebase → Lakehouse: Change Data Feed into the ops catalog

# COMMAND ----------

dbs = list(w.postgres.list_databases(PROD["branch"]))
db = next((d.name for d in dbs if d.status and d.status.postgres_database == p["lakebase_database"]), None)
print("database resource:", db)
cdf_id = f"{p['lakebase_project']}-{p['lakebase_pg_schema']}".replace("_", "-")
try:
    existing = [c.name for c in w.postgres.list_cdf_configs(db)]
except Exception:  # noqa: BLE001  (documented: 404 when none exist)
    existing = []
try:
    if not any(n.endswith("/" + cdf_id) for n in existing):
        w.postgres.create_cdf_config(parent=db, cdf_config_id=cdf_id, cdf_config=pg.CdfConfig(
            catalog=p["ops_catalog"], schema=p["ops_cdc_schema"], postgres_schema=p["lakebase_pg_schema"])).wait()
    statuses = []
    for _ in range(30):
        try:
            statuses = [(s.postgres_table, str(s.state), s.uc_table) for s in w.postgres.list_cdf_statuses(db)]
        except Exception:  # noqa: BLE001
            statuses = []
        if statuses and all("ONLINE" in s[1] or "ACTIVE" in s[1] for s in statuses):
            break
        time.sleep(20)
    log("lakebase_cdf", f"{p['ops_catalog']}.{p['ops_cdc_schema']}", bool(statuses), statuses or "no status yet")
    lngdemo.ledger_update(spark, LEDGER, 20, f"create_cdf_config into non-default-storage catalog {p['ops_catalog']}",
                          f"statuses={statuses}", "Confirmed" if statuses else "Partially confirmed")
except Exception as e:  # noqa: BLE001
    log("lakebase_cdf", f"{p['ops_catalog']}.{p['ops_cdc_schema']}", False, e)
    print("If this is a preview/feature error, enable the Lakebase CDF preview (MANUAL_STEPS L1) and re-run.")
