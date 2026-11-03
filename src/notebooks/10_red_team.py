# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Red-team run (M1, M2, M4, M5)
# MAGIC Sends every enabled prompt in `tests/red_team_prompts.yaml` as the listed persona and stores
# MAGIC the raw outcome in `audit_curated.red_team_results`.
# MAGIC
# MAGIC * Persona identities are SP OAuth credentials from the secret scope (`<persona>_client_id/_secret`).
# MAGIC   Missing credentials → the test is recorded as `NOT_TESTED` (never silently run as someone else).
# MAGIC * Outcomes: `ALLOWED`, `BLOCKED_BY_POLICY` (HTTP 200 + `databricks_service_policy`), `DENIED_ACCESS` (401/403), …
# MAGIC * Expectations that need a UI-attached policy (P1/P2/P3) only hold after `MANUAL_STEPS.md`.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.157.0" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys, uuid, json, re, yaml
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo

p = lngdemo.get_params(dbutils)
CAT, SVC, CUR = p["catalog"], p["services_schema"], p["audit_curated_schema"]
SERVICES = {
    "prod": p["copilot_service"],
    "dev": p["copilot_dev_service"],
    "system": f"system.ai.{p['system_model']}",
}
spec = yaml.safe_load(open(os.path.abspath("../../tests/red_team_prompts.yaml")))
modules = None if p["module_filter"] in ("", "all") else {m.strip() for m in p["module_filter"].split(",")}
RUN_ID = uuid.uuid4().hex[:10]
EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
PHONE = re.compile(r"\+?\d[\d\s().-]{8,}\d")
print("run_id", RUN_ID, "services", SERVICES)

# COMMAND ----------

identities = {}
def ident(persona):
    if persona not in identities:
        identities[persona] = lngdemo.persona_identity(persona, dbutils, p["secret_scope"])
    return identities[persona]

rows = []
for t in spec["tests"]:
    if t.get("enabled", True) is False or (modules and t["module"] not in modules):
        continue
    prompt = t.get("prompt") or (t["prompt_repeat"]["text"] * t["prompt_repeat"]["times"])
    idn = ident(t["persona"])
    base = dict(run_id=RUN_ID, module=t["module"], test_id=t["id"], persona=t["persona"],
                service=SERVICES[t["service"]], prompt=prompt[:2000], expected=t["expected"], notes=t.get("notes", ""))
    if not idn.available:
        rows.append({**base, "outcome": "NOT_TESTED", "verdict": "NOT_TESTED", "error": idn.note})
        continue
    tags = {**spec["defaults"]["tags"], "test_id": t["id"], "run_id": RUN_ID}
    res = lngdemo.chat(idn, SERVICES[t["service"]], prompt, tags=tags, max_tokens=spec["defaults"]["max_tokens"])
    outcome = res.outcome()
    checks_failed = []
    for chk in t.get("check", []):
        src_vals = EMAIL.findall(prompt) if chk == "no_email" else PHONE.findall(prompt)
        if any(v in (res.content or "") for v in src_vals):
            checks_failed.append(chk)
    if t["expected"] == "OBSERVE":
        verdict = "OBSERVED"
    elif (outcome == t["expected"] or (t["expected"] == "DENIED_ACCESS" and outcome == "DENIED_NOT_FOUND")) and not checks_failed:
        verdict = "PASS"
    else:
        verdict = "FAIL" + (f" (checks failed: {checks_failed})" if checks_failed else "")
    rows.append(lngdemo.result_row(res, **{**base, "verdict": verdict,
                                         "notes": (t.get("notes", "") + f" | identity: {idn.note}").strip(" |")}))
    print(f"{t['id']:<28} {t['persona']:<18} {outcome:<18} {verdict}")

lngdemo.append_rows(spark, f"{CAT}.{CUR}.red_team_results", rows)

# COMMAND ----------

display(spark.sql(f"""
SELECT module, test_id, persona, service, expected, outcome, verdict, http_status, policy_json, content_preview
FROM {lngdemo.fq(CAT, CUR, 'red_team_results')} WHERE run_id = '{RUN_ID}' ORDER BY module, test_id
"""))
dbutils.jobs.taskValues.set("run_id", RUN_ID)
