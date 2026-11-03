# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Judge non-determinism (M5)
# MAGIC Sends the same judge-triggering prompt `judge_runs` times (default 10) to `trading_copilot`
# MAGIC with the LLM-judge policy attached in **Log** mode (MANUAL_STEPS P3), then reads verdicts from the
# MAGIC evaluator's inference table (`audit_raw.trading_judge_payload`).
# MAGIC
# MAGIC Inference rows can take **up to an hour** to land. If the distribution below is empty, re-run
# MAGIC only the last cell later (or run job `lngdemo-gov-03-open-questions`, which repeats this analysis).

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.157.0" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys, uuid, yaml
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo

p = lngdemo.get_params(dbutils)
CAT, SVC, CUR = p["catalog"], p["services_schema"], p["audit_curated_schema"]
spec = yaml.safe_load(open(os.path.abspath("../../tests/red_team_prompts.yaml")))["judge_repeat"]
N = int(p["judge_runs"] or 10)
service = lngdemo.services(p)[spec["service"]]["service"]
idn = lngdemo.persona_identity(spec["persona"], dbutils, p["secret_scope"])
RUN_ID = "judge-" + uuid.uuid4().hex[:8]

rows = []
if not idn.available:
    print("NOT TESTED:", idn.note)
else:
    for i in range(N):
        res = lngdemo.chat(idn, service, spec["prompt"], tags={"desk": "pacific", "use_case": "m5_judge_repeat", "run_id": RUN_ID})
        rows.append(lngdemo.result_row(res, run_id=RUN_ID, module="M5", test_id=f"m5_judge_repeat_{i:02d}",
                                     prompt=spec["prompt"], expected="OBSERVE", verdict="OBSERVED",
                                     notes=f"identity: {idn.note}"))
        print(i, res.outcome(), (res.content or "")[:80].replace("\n", " "))
    lngdemo.append_rows(spark, f"{CAT}.{CUR}.red_team_results", rows)

# COMMAND ----------

# MAGIC %md ### Verdict distribution (evaluator inference table)
# MAGIC The judge sees the caller's message, which contains the unique `[lngdemo:…]` marker, so evaluator
# MAGIC rows can be joined back to each request. `verdict_json` is the judge's raw JSON (`flagged`,
# MAGIC `confidence`, `reason`). The `reason` column often **restates the flagged content** (M5 point).

# COMMAND ----------

judge_tbl = p["judge_payload"]
if not lngdemo.table_exists(spark, judge_tbl):
    print(f"{judge_tbl} not created yet (first judge call creates it; delivery can take up to 1h).")
else:
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW judge_runs AS
    SELECT r.test_id, r.request_marker, j.event_time,
           get_json_object(j.response, '$.choices[0].message.content') AS verdict_json
    FROM {lngdemo.fq(CAT, CUR, 'red_team_results')} r
    LEFT JOIN {lngdemo.qn(judge_tbl)} j ON j.request LIKE concat('%', r.request_marker, '%')
    WHERE r.run_id LIKE 'judge-%'
    """)
    display(spark.sql("""
    SELECT coalesce(get_json_object(verdict_json, '$.flagged'), 'no evaluator row yet') AS flagged,
           count(*) AS n,
           round(avg(cast(get_json_object(verdict_json, '$.confidence') AS DOUBLE)), 2) AS avg_confidence
    FROM judge_runs GROUP BY 1 ORDER BY 1"""))
    display(spark.sql("SELECT test_id, get_json_object(verdict_json, '$.reason') AS judge_reason FROM judge_runs WHERE verdict_json IS NOT NULL"))
