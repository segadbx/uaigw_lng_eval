# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Apply `lngdemo_*` tags to Unity Catalog assets
# MAGIC The bundle can tag jobs (`presets.tags` + job `tags`), but schemas, volumes, model services and
# MAGIC job-created tables/views/functions have **no tag field** in the bundle schema. This notebook applies
# MAGIC the same tag keys with SQL `SET TAGS`, so every asset carries:
# MAGIC
# MAGIC | Key | Value |
# MAGIC |---|---|
# MAGIC | `lngdemo_project` | `lngdemo-ai-governance` |
# MAGIC | `lngdemo_env` | bundle target |
# MAGIC | `lngdemo_data_class` | `synthetic` · `sensitive_payload` (raw logs) · `curated` |
# MAGIC | `lngdemo_component` | schema / object role |
# MAGIC | `lngdemo_owner` | deploying / running identity |
# MAGIC | `lngdemo_managed_by` | `dab` (bundle resource) · `setup_job` · `gateway_auto` (auto-created inference table) |
# MAGIC
# MAGIC Idempotent (`SET TAGS` overwrites values). Runs at the end of `setup` and again in `open_questions`
# MAGIC (after inference tables exist). Every result goes to `audit_curated.experiment_log`
# MAGIC (`experiment = 'apply_tags'`), including the object types that can't be tagged.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.157.0" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo

p = lngdemo.get_params(dbutils)
CAT, SVC, RAW, CUR, TS = p["catalog"], p["services_schema"], p["audit_raw_schema"], p["audit_curated_schema"], p["trading_schema"]
LOG = lngdemo.fq(CAT, CUR, "experiment_log")
OWNER = spark.sql("SELECT current_user() AS u").first()["u"]  # same meaning as lngdemo_owner on jobs
results = []

def tag_sql(kind, full, tg):
    tg = {**tg, lngdemo.TAG_PREFIX + "owner": OWNER}
    pairs = ", ".join(f"'{k}' = '{v}'" for k, v in tg.items())
    stmt = f"ALTER {kind} {lngdemo.fq(*full.split('.'))} SET TAGS ({pairs})"
    try:
        spark.sql(stmt)
        ok, detail = True, "ok"
    except Exception as e:  # noqa: BLE001
        ok, detail = False, str(e)[:800]
    results.append((kind, full, ok, detail[:160]))
    spark.sql(f"INSERT INTO {LOG} VALUES (current_timestamp(), 'apply_tags', :t, :s, :ok, :d)",
              args={"t": full, "s": stmt[:2000], "ok": ok, "d": detail})

# COMMAND ----------

# (catalog, schema) -> (role label, data class). The ops catalog (M9) holds Lakebase CDF history and its views.
SCHEMA_ROLE = {
    (CAT, SVC): ("services", "synthetic"),
    (CAT, RAW): ("audit-raw", "sensitive_payload"),
    (CAT, CUR): ("audit-curated", "curated"),
    (CAT, TS): ("trading-data", "synthetic"),
}
if p.get("bronze_schema"):  # medallion stages (raw = landing volume; gold = trading_data)
    SCHEMA_ROLE.update({
        (CAT, p["raw_schema"]): ("raw", "sensitive_payload"),
        (CAT, p["bronze_schema"]): ("bronze", "sensitive_payload"),
        (CAT, p["silver_schema"]): ("silver", "sensitive_payload"),
    })
if p.get("ops_catalog"):
    SCHEMA_ROLE.update({
        (p["ops_catalog"], p["ops_cdc_schema"]): ("lakebase-cdc", "sensitive_payload"),
        (p["ops_catalog"], p["ops_curated_schema"]): ("ops-curated", "curated"),
        (p["ops_catalog"], p["ops_pipeline_schema"]): ("pipeline-meta", "synthetic"),
    })
SCHEMA_ROLE = {k: v for k, v in SCHEMA_ROLE.items() if spark.catalog.databaseExists(f"{k[0]}.{k[1]}")}

# Bundle-managed catalogs, schemas + volume
if p.get("ops_catalog"):
    tag_sql("CATALOG", p["ops_catalog"], lngdemo.base_tags(p, component="catalog-ops", managed_by="dab"))
if p.get("lakebase_uc_catalog"):  # registered Lakebase catalog (read-only mirror); outcome is logged either way
    tag_sql("CATALOG", p["lakebase_uc_catalog"], lngdemo.base_tags(p, component="catalog-lakebase-registered", managed_by="dab"))
for (cat, schema), (role, dclass) in SCHEMA_ROLE.items():
    tag_sql("SCHEMA", f"{cat}.{schema}", lngdemo.base_tags(p, component=f"schema-{role}", data_class=dclass, managed_by="dab"))
tag_sql("VOLUME", f"{CAT}.{CUR}.evidence", lngdemo.base_tags(p, component="volume-evidence", data_class="curated", managed_by="dab"))
if p.get("raw_schema"):
    tag_sql("VOLUME", f"{CAT}.{p['raw_schema']}.{p['landing_volume']}",
            lngdemo.base_tags(p, component="volume-raw-landing", data_class="sensitive_payload", managed_by="dab"))

# Tables and views created by jobs, auto-created by the gateway (_payload) or by Lakebase CDF (lb_*_history).
# Tags are safe on CDF history tables; only row filters / column masks stop CDF (documented).
for (cat, schema), (role, dclass) in SCHEMA_ROLE.items():
    for t in spark.sql(f"SHOW TABLES IN {lngdemo.fq(cat, schema)}").where("isTemporary = false").collect():
        name = t["tableName"]
        typ = spark.sql(f"DESCRIBE TABLE EXTENDED {lngdemo.fq(cat, schema, name)}").where("col_name = 'Type'").collect()
        t_type = typ[0]["data_type"] if typ else "TABLE"
        kind = {"VIEW": "VIEW", "MATERIALIZED_VIEW": "MATERIALIZED VIEW", "STREAMING_TABLE": "STREAMING TABLE"}.get(t_type, "TABLE")
        managed_by = ("gateway_auto" if name.endswith("_payload")
                      else "lakebase_cdf" if name.startswith("lb_") and name.endswith("_history")
                      else "pipeline_medallion" if t_type in ("MATERIALIZED_VIEW", "STREAMING_TABLE") else "setup_job")
        tag_sql(kind, f"{cat}.{schema}.{name}",
                lngdemo.base_tags(p, component=f"{kind.lower().replace(' ', '-')}-{name}", data_class=dclass, managed_by=managed_by))

# UC functions (row filters, masks, tool, policy functions). Tagging functions needs DBR 18.1+ per the
# tags docs, so failures are recorded rather than raised.
for (cat, schema) in SCHEMA_ROLE:
    for full in lngdemo.list_functions(spark, cat, schema):
        tag_sql("FUNCTION", full, lngdemo.base_tags(p, component=f"function-{full.split('.')[-1]}", managed_by="setup_job"))

# COMMAND ----------

# MAGIC %md ### Model services
# MAGIC `system.ai_gateway.usage` exposes `service_tags` ("tags on the Unity Catalog securable"), but the
# MAGIC bundle schema has no tag field for model services, the entity-tag-assignment API documents
# MAGIC catalogs/schemas/tables/columns/volumes only, and SQL management of model services isn't
# MAGIC supported. So nothing is guessed here: services are recorded as **manual** (tag them in Catalog
# MAGIC Explorer if your workspace offers it, see MANUAL_STEPS.md step G1). Every request is still
# MAGIC tagged at call time with `lngdemo_*` request tags.

# COMMAND ----------

for role, svc in lngdemo.services(p).items():
    sid = svc["service"].split(".")[-1]
    full = svc["service"]
    results.append(("MODEL SERVICE", full, False, "no tagging API -> manual (MANUAL_STEPS G1); requests carry lngdemo_* request tags"))
    spark.sql(f"INSERT INTO {LOG} VALUES (current_timestamp(), 'apply_tags', :t, 'n/a', false, 'no documented tagging API for model services; manual step G1')",
              args={"t": full})

display(spark.createDataFrame(results, "kind STRING, object STRING, ok BOOLEAN, detail STRING"))
