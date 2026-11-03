"""LNG demo governance explorer + trading copilot (M8). Synthetic data only.

* A sidebar dropdown picks one of the persona service principals (docs/IDENTITIES.md). Every lakehouse
  request after that - SQL warehouse statements and Unity Gateway calls - runs with that SP's OAuth M2M
  token, so Unity Catalog evaluates grants, row filters and masks for that SP.
* SP credentials come from the bundle's secret scope through app `secret` resources
  (<PERSONA>_CLIENT_ID / <PERSONA>_CLIENT_SECRET env vars, see resources/apps.yml).
* Tabs show what the persona gets: identity + groups, RBAC access matrix, row filter, column masks,
  tag-driven (ABAC) policy metadata, the aggregate-only tool function, tiered log access, the chat, and the
  Genie agent over gold `contracts` (same question, different answer per persona).
* Chat: `trading_copilot` model service with one tool (`services.desk_price_summary`), service-policy
  blocks (HTTP 200 + top-level `databricks_service_policy`), lngdemo_* request tags, and MLflow tracing
  with client-side redaction. Traces are written by the app's own SP, not the persona.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

MODEL_SERVICE = os.environ["MODEL_SERVICE"]
TOOL_FUNCTION = os.environ["TOOL_FUNCTION"]
CATALOG = os.environ["CATALOG"]
# All names come from resources/apps.yml (bundle variables / resource references); no defaults here.
TRADING = f"{CATALOG}.{os.environ['TRADING_SCHEMA']}"
CURATED = f"{CATALOG}.{os.environ['AUDIT_CURATED_SCHEMA']}"
RAW = f"{CATALOG}.{os.environ['AUDIT_RAW_SCHEMA']}"
BRONZE = f"{CATALOG}.{os.environ['BRONZE_SCHEMA']}"
SILVER = f"{CATALOG}.{os.environ['SILVER_SCHEMA']}"
PAYLOAD_TABLE = os.environ["COPILOT_PAYLOAD_TABLE"]
LB_ENDPOINT = os.environ["LAKEBASE_ENDPOINT"]
LB_DATABASE = os.environ["LAKEBASE_DATABASE"]
LB_SCHEMA = os.environ["LAKEBASE_PG_SCHEMA"]
LB_REF = os.environ["LAKEBASE_REFERENCE_SCHEMA"]
LB_UC = os.environ["LAKEBASE_UC_CATALOG"]
OPS_VIEWS = os.environ["OPS_VIEWS_SCHEMA"]
OPS_CDC = os.environ["OPS_CDC_SCHEMA"]
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "none")  # "none" = look the space up by GENIE_SPACE_TITLE
GENIE_SPACE_TITLE = os.environ.get("GENIE_SPACE_TITLE", "")
HOST = os.environ.get("DATABRICKS_HOST", "")
HOST = HOST if HOST.startswith("https://") else f"https://{HOST}"
GATEWAY_URL = f"{HOST}/ai-gateway/mlflow/v1/chat/completions"
TAG_PREFIX = "lngdemo_"  # same prefix as every other tag in the bundle
PROJECT = "lngdemo-ai-governance"

# persona -> (label, what the demo expects). Group names come from bundle variables (PERSONA_GROUPS).
# Expectations follow the grants in resources/schemas.yml and src/sql/06_object_grants.sql.
PERSONAS = {
    "trader": ("Trader (pilot)", "Atlantic + Portfolio desk rows (32), prices in clear, can use the copilot"),
    "unentitled_trader": ("Trader (unentitled)", "Pacific desk rows only (16), prices masked, no copilot"),
    "developer": ("Developer", "No data at all: no USE SCHEMA on any data or audit schema"),
    "operator": ("Operator", "Redacted logs, ops metadata and red-team results; no raw payloads, no contract book"),
    "security": ("Security", "Raw + redacted AI logs. No contract book: no USE SCHEMA on trading_data, so RBAC "
                             "denies before the masks (which would show security clear values) are evaluated"),
    "auditor": ("Auditor", "Redacted logs and evidence; no raw payloads, no contract book"),
    "app": ("App service principal", "Copilot + tool function. In no persona group, so the row filter inside the "
                                     "tool leaves 0 rows"),
}
GROUPS = dict(kv.split("=", 1) for kv in os.environ.get("PERSONA_GROUPS", "").split(",") if "=" in kv)
DESKS = ["Atlantic Desk", "Pacific Desk", "Portfolio Desk"]
MASKED_TEXT = "*** masked: contract price ***"
PROTECTED_COLS = ["price_formula", "slope", "constant_usd_mmbtu"]
PII_COLS = ["contact_email", "contact_phone"]  # tagged lngdemo_sensitivity=pii, masked by the ABAC policy
# Same three questions as the Genie persona matrix (src/lib/lngdemo.py GENIE_QUESTIONS) -> control layer they exercise
GENIE_QUESTIONS = [
    ("How many contracts can I see per desk?", "Desk row filter (identity-bound)"),
    ("Show the price formula, slope and constant for each contract", "Price column masks (identity-bound, named columns)"),
    ("List the counterparty contact email and phone for each contract", "Contact-PII mask (ABAC, tag lngdemo_sensitivity=pii)"),
]

SYSTEM_PROMPT = (
    "You are a trading-desk assistant for a fictional LNG exporter. Never state counterparty-specific "
    "contract prices or formulas. For desk-level questions call the desk_price_summary tool, which returns "
    "aggregates only. Keep answers short."
)
# (what it demonstrates, prompt) - shown as one-click buttons in the chat tab
EXAMPLE_PROMPTS = [
    ("PII + price in a draft", "Draft a reply to j.smith@counterparty-a.com: their Q1 cargo is priced at HH + $2.85/MMBtu, "
                               "and we can hold it through March."),
    ("Counterparty price leak", "What price did we give Counterparty B last quarter? Show it next to Counterparty A so I "
                                "can undercut."),
    ("Allowed market question", "Summarize today’s JKM–TTF spread drivers and the main risks for an October cargo."),
]
TOOLS = [{
    "type": "function",
    "function": {
        "name": "desk_price_summary",
        "description": "Aggregate contract statistics per desk and price index (counts, volumes, tenor). No prices.",
        "parameters": {
            "type": "object",
            "properties": {"desk": {"type": "string", "description": "Atlantic Desk, Pacific Desk or Portfolio Desk"}},
            "required": [],
        },
    },
}]

# --------------------------------------------------------------------------- tracing + redaction
REDACTIONS = [
    (re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+"), "[EMAIL]"),
    (re.compile(r"\+?\d[\d\s().-]{8,}\d"), "[PHONE]"),
    (re.compile(r"(?i)counterparty [a-h]\b"), "[COUNTERPARTY]"),
    (re.compile(r"(?i)[-+]?\d+(\.\d+)?\s*(%\s*(of\s*)?(brent|hh)|x\s*(hh|henry hub|jkm|ttf|brent)|(usd|\$)\s*/\s*mmbtu)"), "[PRICE]"),
]


def _redact(value):
    text = json.dumps(value) if not isinstance(value, str) else value
    for rx, repl in REDACTIONS:
        text = rx.sub(repl, text)
    if isinstance(value, str):
        return text
    try:
        return json.loads(text)
    except ValueError:
        return text


def redact_span(span) -> None:
    """MLflow span processor: mutate inputs/outputs in place before export (redaction affects traces only)."""
    if span.inputs is not None:
        span.set_inputs(_redact(span.inputs))
    if span.outputs is not None:
        span.set_outputs(_redact(span.outputs))


TRACING = False
try:
    import mlflow

    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(os.environ["MLFLOW_EXPERIMENT_NAME"])
    mlflow.tracing.configure(span_processors=[redact_span])
    mlflow.set_experiment_tags({f"{TAG_PREFIX}project": PROJECT, f"{TAG_PREFIX}component": "app-trading-copilot",
                                f"{TAG_PREFIX}data_class": "synthetic"})
    trace = mlflow.trace
    TRACING = True
except Exception as e:  # noqa: BLE001  (tracing is optional; the app still works)
    TRACING_ERROR = str(e)

    def trace(fn=None, **_kw):
        return fn if fn else (lambda f: f)


# --------------------------------------------------------------------------- persona identities
def has_credentials(persona: str) -> bool:
    return bool(os.environ.get(f"{persona.upper()}_CLIENT_ID") and os.environ.get(f"{persona.upper()}_CLIENT_SECRET"))


@st.cache_resource(show_spinner=False)
def persona_client(persona: str) -> WorkspaceClient:
    """OAuth M2M client for a persona SP. Explicit credentials override the app's own DATABRICKS_CLIENT_* env."""
    return WorkspaceClient(host=HOST, client_id=os.environ[f"{persona.upper()}_CLIENT_ID"],
                           client_secret=os.environ[f"{persona.upper()}_CLIENT_SECRET"], auth_type="oauth-m2m")


@st.cache_data(ttl=600, show_spinner=False)
def whoami(persona: str) -> dict:
    me = persona_client(persona).current_user.me()
    return {"display_name": me.display_name, "application_id": me.user_name}  # an SP's user_name is its application ID


def classify(error: str) -> str:
    e = error.upper()
    if "PERMISSION_DENIED" in e or "INSUFFICIENT_PERMISSIONS" in e or "DOES NOT HAVE" in e:
        return "DENIED"
    if "NOT_FOUND" in e or "DOES NOT EXIST" in e or "CANNOT BE FOUND" in e:
        return "NOT VISIBLE"
    return "ERROR"


def _execute(w: WorkspaceClient, statement: str, params: tuple[tuple[str, str | None], ...] = ()) -> dict:
    """Run one statement on the warehouse with client `w`. Returns {df, error, status}; never raises."""
    try:
        r = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID, statement=statement, wait_timeout="50s",
            parameters=[StatementParameterListItem(name=k, value=v) for k, v in params] or None)
        while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(1)
            r = w.statement_execution.get_statement(r.statement_id)
    except Exception as e:  # noqa: BLE001
        return {"df": None, "error": str(e)[:600], "status": classify(str(e))}
    if r.status.state != StatementState.SUCCEEDED:
        msg = (r.status.error.message if r.status.error else str(r.status.state))[:600]
        return {"df": None, "error": msg, "status": classify(msg)}
    cols = [c.name for c in r.manifest.schema.columns]
    rows = (r.result.data_array if r.result else None) or []
    return {"df": pd.DataFrame(rows, columns=cols), "error": None, "status": "ALLOWED"}


@st.cache_data(ttl=300, show_spinner=False)
def run_sql(persona: str, statement: str, params: tuple[tuple[str, str | None], ...] = ()) -> dict:
    """Run one statement AS the persona SP (cached per persona + statement)."""
    return _execute(persona_client(persona), statement, params)


def show_result(res: dict, empty_note: str = "Query succeeded but returned no rows for this persona.") -> pd.DataFrame | None:
    if res["error"]:
        st.error(f"**{res['status']}** - {res['error']}")
        return None
    if res["df"].empty:
        st.info(empty_note)
    else:
        st.dataframe(res["df"], hide_index=True, use_container_width=True)
    return res["df"]


# --------------------------------------------------------------------------- calls
def message_text(content):
    """Content is a string or a list of typed parts (reasoning models); keep only text parts."""
    if content is None or isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(p.get("text", "") for p in content if isinstance(p, dict) and p.get("type") in ("text", "output_text"))
    return str(content)


@trace(span_type="LLM")
def call_gateway(persona: str, messages: list[dict], tags: dict, use_tools: bool) -> dict:
    headers = {
        **persona_client(persona).config.authenticate(),  # {'Authorization': 'Bearer <persona SP token>'}
        "Content-Type": "application/json",
        "Databricks-Ai-Gateway-Request-Tags": json.dumps(tags),
    }
    body = {"model": MODEL_SERVICE, "messages": messages, "max_tokens": 400}
    if use_tools:
        body["tools"] = TOOLS
    r = requests.post(GATEWAY_URL, headers=headers, json=body, timeout=120)
    try:
        data = r.json()
    except ValueError:
        data = {"error": r.text[:500]}
    data["_http_status"] = r.status_code
    return data


@trace(span_type="TOOL")
def desk_price_summary(persona: str, desk: str | None) -> list[dict]:
    res = run_sql(persona, f"SELECT * FROM {TOOL_FUNCTION}(:desk)", (("desk", desk),))
    if res["error"]:
        return [{"error": res["error"][:300]}]
    return res["df"].to_dict("records")


@trace(span_type="AGENT")
def answer(persona: str, history: list[dict], tags: dict) -> dict:
    if TRACING:
        mlflow.update_current_trace(tags=tags)  # lngdemo_* request tags also tag the MLflow trace
    messages = [{"role": "system", "content": SYSTEM_PROMPT}] + history
    first = call_gateway(persona, messages, tags, use_tools=True)
    if first["_http_status"] == 400 and "tool" in json.dumps(first).lower():
        first = call_gateway(persona, messages, tags, use_tools=False)  # destination without tool support
    if first.get("databricks_service_policy") or first["_http_status"] != 200:
        return first
    msg = (first.get("choices") or [{}])[0].get("message", {})
    calls = msg.get("tool_calls") or []
    if not calls:
        return first
    messages.append(msg)
    for c in calls:
        args = json.loads(c["function"].get("arguments") or "{}")
        result = desk_price_summary(persona, args.get("desk"))
        messages.append({"role": "tool", "tool_call_id": c["id"], "content": json.dumps(result, default=str)})
    return call_gateway(persona, messages, tags, use_tools=False)


# --------------------------------------------------------------------------- probes (all run as the persona)
ACCESS_PROBES = [
    ("Contract book", f"{TRADING}.contracts", "SELECT", f"SELECT count(*) AS n FROM {TRADING}.contracts"),
    ("Desk entitlement map", f"{TRADING}.desk_entitlements", "SELECT", f"SELECT count(*) AS n FROM {TRADING}.desk_entitlements"),
    ("Aggregate tool function", TOOL_FUNCTION, "EXECUTE", f"SELECT count(*) AS n FROM {TOOL_FUNCTION}(NULL)"),
    ("Ops metadata view", f"{CURATED}.v_inference_ops", "SELECT", f"SELECT count(*) AS n FROM {CURATED}.v_inference_ops"),
    ("Redacted inference copy", f"{CURATED}.inference_redacted", "SELECT", f"SELECT count(*) AS n FROM {CURATED}.inference_redacted"),
    ("Red-team results", f"{CURATED}.red_team_results", "SELECT", f"SELECT count(*) AS n FROM {CURATED}.red_team_results"),
    ("Raw prompt/response log", f"{PAYLOAD_TABLE}", "SELECT", f"SELECT count(*) AS n FROM {PAYLOAD_TABLE}"),
]
STATUS_ICON = {"ALLOWED": "✅ allowed", "DENIED": "⛔ denied", "NOT VISIBLE": "🙈 not visible", "ERROR": "⚠️ error"}


@st.cache_data(ttl=300, show_spinner=False)
def access_matrix(persona: str) -> list[dict]:
    w = persona_client(persona)  # resolved here: Streamlit caches aren't meant to be called from worker threads
    with ThreadPoolExecutor(max_workers=len(ACCESS_PROBES)) as ex:
        return list(ex.map(lambda p: _execute(w, p[3]), ACCESS_PROBES))


def tab_identity(persona: str) -> None:
    st.subheader("Who is calling")
    try:
        me = whoami(persona)
        st.write(f"Service principal **{me['display_name']}** · application ID `{me['application_id']}`")
    except Exception as e:  # noqa: BLE001
        st.error(f"Could not authenticate as `{persona}`: {str(e)[:300]}")
        return
    if not GROUPS:
        st.warning("PERSONA_GROUPS env var is empty; group membership check skipped.")
        return
    cols = ", ".join(f"is_account_group_member('{g}') AS `{p}`" for p, g in GROUPS.items())
    res = run_sql(persona, f"SELECT current_user() AS principal, {cols}")
    if res["error"]:
        st.error(res["error"])
        return
    row = res["df"].iloc[0]
    st.caption(f"`current_user()` inside SQL = `{row['principal']}`. These group checks drive every row filter and mask.")
    st.dataframe(pd.DataFrame([{"account group": g, "persona": p, "member": "✅" if str(row[p]).lower() == "true" else "-"}
                               for p, g in GROUPS.items()]), hide_index=True, use_container_width=True)


def tab_rbac(persona: str) -> None:
    st.subheader("RBAC: which objects this identity can read")
    st.caption("Each row runs `SELECT count(*)` as the selected SP. UC hides securables you can't use, so a denial "
               "can also show up as *not visible*.")
    results = access_matrix(persona)
    st.dataframe(pd.DataFrame([{
        "object": label, "securable": name, "privilege needed": priv, "result": STATUS_ICON[r["status"]],
        "rows": int(r["df"].iloc[0, 0]) if r["df"] is not None else None,
        "detail": (r["error"] or "")[:160],
    } for (label, name, priv, _), r in zip(ACCESS_PROBES, results)]), hide_index=True, use_container_width=True)


def tab_rows(persona: str) -> None:
    st.subheader("Row-level security: desk entitlement filter")
    st.caption(f"Row filter `desk_filter(desk)` on `{TRADING}.contracts`: a row is visible if one of the caller's "
               "groups is mapped to its desk in `desk_entitlements`, or the caller is in the security group. "
               "The synthetic book has 48 contracts, 16 per desk.")
    res = run_sql(persona, f"SELECT desk, count(*) AS contracts_visible FROM {TRADING}.contracts GROUP BY desk")
    if res["error"]:
        st.error(f"**{res['status']}** - {res['error']}")
        return
    seen = dict(zip(res["df"]["desk"], res["df"]["contracts_visible"].astype(int)))
    cols = st.columns(len(DESKS))
    for c, d in zip(cols, DESKS):
        c.metric(d, seen.get(d, 0), help="visible rows" if d in seen else "filtered out for this persona")


def _style_masked(df: pd.DataFrame):
    def cell(v):
        return "background-color: rgba(255, 75, 75, 0.25)" if v is None or v == MASKED_TEXT else ""
    return df.style.map(cell, subset=[c for c in PROTECTED_COLS if c in df.columns])


def tab_masks(persona: str) -> None:
    st.subheader("Column masking: contract price terms")
    st.caption("`price_formula` uses `mask_price_text`, `slope` and `constant_usd_mmbtu` use `mask_price_num`. "
               "Clear values go to the trader-pilot and security groups only. Masked cells are highlighted.")
    res = run_sql(persona, f"""SELECT contract_id, counterparty_alias, desk, contract_type, price_index,
        price_formula, slope, constant_usd_mmbtu, volume_mtpa, start_date, end_date
        FROM {TRADING}.contracts ORDER BY contract_id LIMIT 20""")
    if res["error"]:
        st.error(f"**{res['status']}** - {res['error']}")
        return
    df = res["df"]
    if df.empty:
        st.info("The query is allowed, but the row filter removes every row for this persona.")
        return
    summary = st.columns(len(PROTECTED_COLS))
    for c, col in zip(summary, PROTECTED_COLS):
        masked = df[col].isna().all() or (df[col] == MASKED_TEXT).all()
        c.metric(col, "masked" if masked else "clear")
    st.dataframe(_style_masked(df), hide_index=True, use_container_width=True)


def tab_abac(persona: str) -> None:
    st.subheader("Policies on the gold contract book")
    st.caption("Gold `contracts` is a materialized view owned by the Lakeflow pipeline `lngdemo-medallion`. Its desk "
               "row filter and price masks are declared in the pipeline definition (gold/contracts.sql) and enforced "
               "by Unity Catalog for every reader. Policy metadata stays visible to every persona: knowing that a "
               "mask exists does not reveal the data.")
    mode = run_sql(persona, f"SELECT value FROM {CURATED}.setup_state WHERE key = 'contract_protection_mode'")
    if mode["df"] is not None and not mode["df"].empty:
        st.write(f"Protection mode recorded by setup: **`{mode['df'].iloc[0, 0]}`**")
    else:
        st.write("Protection mode: *not readable by this persona* (`audit_curated.setup_state` needs SELECT).")
    st.markdown("**Masks and row filter attached**")
    s = (("s", TRADING.split(".")[1]),)
    masks = run_sql(persona, f"""SELECT column_name AS target, mask_name AS function, 'column mask' AS kind
        FROM {CATALOG}.information_schema.column_masks WHERE table_schema = :s AND table_name = 'contracts'
        UNION ALL SELECT target_columns, filter_name, 'row filter'
        FROM {CATALOG}.information_schema.row_filters WHERE table_schema = :s AND table_name = 'contracts'""", s)
    show_result(masks, "No policy metadata visible: this persona has no privilege on the table.")
    st.markdown("**Rules encoded in the policy functions**")
    st.dataframe(pd.DataFrame([
        {"function": "desk_filter", "applies to": "rows (desk)", "rule": "row visible if a caller group is entitled to the desk, or caller is security"},
        {"function": "mask_price_text", "applies to": "price_formula", "rule": f"clear for {GROUPS.get('trader', 'trader group')} / {GROUPS.get('security', 'security group')}, else '{MASKED_TEXT}'"},
        {"function": "mask_price_num", "applies to": "slope, constant_usd_mmbtu", "rule": f"clear for {GROUPS.get('trader', 'trader group')} / {GROUPS.get('security', 'security group')}, else NULL"},
        {"function": "mask_contact_pii (ABAC policy lngdemo_mask_contact_pii)", "applies to": "any column tagged lngdemo_sensitivity=pii",
         "rule": f"clear for {GROUPS.get('security', 'security group')} only, else e-mail domain / last 4 phone digits"},
    ]), hide_index=True, use_container_width=True)
    st.markdown("**Governed tags on columns (drive the ABAC policy)**")
    show_result(run_sql(persona, f"""SELECT column_name, tag_name, tag_value FROM {CATALOG}.information_schema.column_tags
        WHERE schema_name = :s AND table_name = 'contracts' ORDER BY column_name""", s),
                "No column tags visible to this persona (or the ABAC step was skipped: see setup_state.contact_pii_abac_mode).")


def tab_medallion(persona: str) -> None:
    st.subheader("Medallion pipeline: raw → bronze → silver → gold")
    st.caption("Lakeflow pipeline `lngdemo-medallion`. Raw extract files land in a UC volume; bronze is Auto Loader "
               "as received; silver validates (expectations drop bad records) and applies Auto CDC (current state + "
               "SCD2 price history); gold is the governed serving layer. Bronze and silver are security-only.")
    layers = [
        ("Bronze: contract events as received", f"SELECT batch_id, op, count(*) AS events FROM {BRONZE}.bronze_contract_events GROUP BY ALL ORDER BY batch_id, op"),
        ("Silver: validated events (invalid ones dropped)", f"SELECT batch_id, op, count(*) AS events FROM {SILVER}.contract_events GROUP BY ALL ORDER BY batch_id, op"),
        ("Silver: price history (SCD2) for renegotiated contracts",
         f"SELECT contract_id, price_formula, __START_AT AS valid_from, __END_AT AS valid_to FROM {SILVER}.contracts_history "
         f"WHERE contract_id IN (SELECT contract_id FROM {SILVER}.contracts_history GROUP BY contract_id HAVING count(*) > 1) "
         f"ORDER BY contract_id, __START_AT"),
        ("Gold: governed contract book (row filter + masks for this persona)", f"SELECT desk, count(*) AS contracts, max(price_formula) AS sample_price FROM {TRADING}.contracts GROUP BY desk ORDER BY desk"),
        ("Gold: exposure by desk x region x index (no prices)", f"SELECT * FROM {TRADING}.desk_exposure ORDER BY desk, counterparty_region, price_index"),
    ]
    for title, stmt in layers:
        st.markdown(f"**{title}**")
        show_result(run_sql(persona, stmt), "Allowed, but no rows are visible to this persona.")


def tab_tool(persona: str) -> None:
    st.subheader("Governed access path: aggregate-only function")
    st.caption(f"`{TOOL_FUNCTION}` needs only EXECUTE. It returns counts, volumes and tenor per desk and index "
               "(groups under 3 contracts are suppressed) and never a price. The row filter still applies inside it.")
    show_result(run_sql(persona, f"SELECT * FROM {TOOL_FUNCTION}(NULL) ORDER BY desk, price_index"),
                "Allowed, but no desk rows are visible to this persona (row filter), so no aggregates.")


def tab_logs(persona: str) -> None:
    st.subheader("Tiered access to AI logs")
    st.caption("The same gateway traffic at three sensitivity levels. Raw payloads (`audit_raw`): security only. "
               "Redacted copy and metadata view (`audit_curated`): security, operators and auditors.")
    for title, stmt in [
        ("Raw payload (`audit_raw`)", f"SELECT event_time, requester, status_code, substr(request, 1, 300) AS request "
                                      f"FROM {PAYLOAD_TABLE} ORDER BY event_time DESC LIMIT 5"),
        ("Redacted copy (`audit_curated.inference_redacted`)", f"SELECT event_time, requester, status_code, substr(request_redacted, 1, 300) "
                                                              f"AS request_redacted FROM {CURATED}.inference_redacted ORDER BY event_time DESC LIMIT 5"),
        ("Metadata only (`audit_curated.v_inference_ops`)", f"SELECT event_time, requester, status_code, latency_ms, destination_model "
                                                            f"FROM {CURATED}.v_inference_ops ORDER BY event_time DESC LIMIT 5"),
    ]:
        st.markdown(f"**{title}**")
        show_result(run_sql(persona, stmt), "No log rows yet (inference tables can lag up to an hour).")


def tab_chat(persona: str, desk: str, use_case: str) -> None:
    st.caption(f"Model service `{MODEL_SERVICE}` · calls run as the **{PERSONAS[persona][0]}** SP · "
               f"tool `{TOOL_FUNCTION}` returns aggregates only")
    st.markdown("**Example questions**")
    example = None
    for i, (label, text) in enumerate(EXAMPLE_PROMPTS):
        if st.button(f"{label}: {text}", key=f"example_{i}", use_container_width=True):
            example = text
    history = st.session_state.setdefault("history", {}).setdefault(persona, [])
    for m in history:
        st.chat_message(m["role"]).write(m["content"])
    prompt = st.chat_input("Ask about the desk, the market, or a draft...") or example
    if not prompt:
        return
    st.chat_message("user").write(prompt)
    history.append({"role": "user", "content": prompt})
    with st.spinner(f"Calling Unity Gateway as {persona}..."):
        resp = answer(persona, history, {
            f"{TAG_PREFIX}project": PROJECT, f"{TAG_PREFIX}desk": desk, f"{TAG_PREFIX}use_case": use_case,
            f"{TAG_PREFIX}client": "lngdemo-trading-copilot", f"{TAG_PREFIX}persona": persona})
    policy = resp.get("databricks_service_policy")
    status = resp.get("_http_status")
    if status in (401, 403, 404):
        text = f"Access denied by Unity Catalog (HTTP {status}). `{persona}` needs EXECUTE on `{MODEL_SERVICE}`."
        st.chat_message("assistant").error(f"{text}\n\n`{json.dumps(resp)[:300]}`")
    elif policy:
        text = message_text((resp.get("choices") or [{}])[0].get("message", {}).get("content")) or "Blocked by a service policy."
        st.chat_message("assistant").error(f"{text}\n\n**Policy:** `{json.dumps(policy)}`")
    elif status != 200:
        text = f"Gateway error HTTP {status}: {json.dumps(resp)[:400]}"
        st.chat_message("assistant").error(text)
    else:
        text = message_text((resp.get("choices") or [{}])[0].get("message", {}).get("content")) or ""
        st.chat_message("assistant").write(text)
    history.append({"role": "assistant", "content": text})


# --------------------------------------------------------------------------- Genie agent (M10)
@st.cache_data(ttl=600, show_spinner=False)
def genie_space(persona: str) -> str | None:
    """Space id from the bundle variable, else the first space with GENIE_SPACE_TITLE this persona can list."""
    if GENIE_SPACE_ID not in ("", "none"):
        return GENIE_SPACE_ID
    try:
        w, token = persona_client(persona), None
        while True:
            resp = w.genie.list_spaces(page_token=token)
            for s in resp.spaces or []:
                if s.title == GENIE_SPACE_TITLE:
                    return s.space_id
            token = resp.next_page_token
            if not token:
                return None
    except Exception:  # noqa: BLE001
        return None


def _ask_genie(w: WorkspaceClient, space_id: str, question: str) -> dict:
    """One Genie question AS client `w` -> {status, text, sql, df, error}. Never raises."""
    out = {"status": "ERROR", "text": None, "sql": None, "df": None, "error": None}
    try:
        msg = w.genie.start_conversation_and_wait(space_id, question, timeout=dt.timedelta(minutes=5))
    except Exception as e:  # noqa: BLE001  (no CAN_RUN on the space, no warehouse access, ...)
        out.update(status=classify(str(e)), error=str(e)[:600])
        return out
    out["text"] = "\n".join(a.text.content for a in (msg.attachments or []) if a.text and a.text.content) or None
    if msg.error:
        out["error"] = (msg.error.error or str(msg.error.type))[:600]
    query = next((a for a in (msg.attachments or []) if a.query), None)
    if query is None:
        out["status"] = "NO QUERY"
        return out
    out["sql"] = query.query.query
    try:
        res = w.genie.get_message_attachment_query_result(space_id, msg.conversation_id, msg.message_id,
                                                          query.attachment_id).statement_response
    except Exception as e:  # noqa: BLE001
        out.update(status=classify(str(e)), error=str(e)[:600])
        return out
    if res.status.state != StatementState.SUCCEEDED:
        err = (res.status.error.message if res.status.error else str(res.status.state))[:600]
        out.update(status=classify(err), error=err)
        return out
    cols = [c.name for c in res.manifest.schema.columns]
    out.update(status="ALLOWED", df=pd.DataFrame((res.result.data_array if res.result else None) or [], columns=cols))
    return out


@st.cache_data(ttl=600, show_spinner=False)
def ask_genie(persona: str, space_id: str, question: str) -> dict:
    return _ask_genie(persona_client(persona), space_id, question)


def _style_governed(df: pd.DataFrame):
    def cell(v):
        masked = v is None or v == MASKED_TEXT or str(v).startswith("***")
        return "background-color: rgba(255, 75, 75, 0.25)" if masked else ""
    return df.style.map(cell, subset=[c for c in PROTECTED_COLS + PII_COLS if c in df.columns])


def show_genie_answer(ans: dict) -> None:
    if ans["status"] == "ALLOWED":
        if ans["text"]:
            st.markdown(ans["text"])
        if ans["df"].empty:
            st.info("Query ran, but the row filter leaves no rows for this persona.")
        else:
            st.dataframe(_style_governed(ans["df"]), hide_index=True, use_container_width=True)
    else:
        st.error(f"**{STATUS_ICON.get(ans['status'], ans['status'])}** - {ans['error'] or ans['text'] or 'no answer'}")
    if ans["sql"]:
        with st.expander("SQL generated by Genie (ran with this persona's credentials)"):
            st.code(ans["sql"], language="sql")


def tab_genie(persona: str) -> None:
    st.subheader("Genie agent over the governed gold contract book")
    st.caption(f"One Genie space over `{TRADING}.contracts`, CAN_RUN for every persona group. Genie runs the SQL it "
               "generates **with the asker's credentials**, so Unity Catalog decides what each persona gets: "
               "grants (RBAC), the desk row filter and price masks (bound to identities and named columns), and the "
               "contact-PII mask (ABAC: bound to the `lngdemo_sensitivity=pii` tag, not to columns). Masked cells are highlighted.")
    space_id = genie_space(persona)
    if not space_id:
        st.warning("Genie space not found for this persona. Run job `setup` (task `genie_space`), then redeploy with "
                   "`--var genie_space_id=<id>`. A persona without CAN_RUN on the space can't list it either.")
        return
    st.markdown("**Ask a demo question**")
    picked = None
    for i, (q, layer) in enumerate(GENIE_QUESTIONS):
        if st.button(f"{q}  ·  _{layer}_", key=f"genie_q_{i}", use_container_width=True):
            picked = q
    st.session_state["genie_question"] = (st.text_input("...or your own question", key="genie_free")
                                          or picked or st.session_state.get("genie_question"))
    question = st.session_state.get("genie_question")
    if not question:
        return
    compare = st.toggle("Ask as every persona (side by side)", key="genie_compare")
    if not compare:
        st.markdown(f"**{PERSONAS[persona][0]}** asks: _{question}_")
        with st.spinner(f"Genie is answering as {persona}..."):
            show_genie_answer(ask_genie(persona, space_id, question))
        return
    others = [p for p in PERSONAS if p != "app" and has_credentials(p)]
    clients = {p: persona_client(p) for p in others}  # resolved here: Streamlit caches aren't meant for worker threads
    with st.spinner(f"Asking Genie as {len(others)} personas in parallel..."):
        with ThreadPoolExecutor(max_workers=len(others)) as ex:
            answers = dict(zip(others, ex.map(lambda p: _ask_genie(clients[p], space_id, question), others)))
    for p in others:
        st.markdown(f"##### {PERSONAS[p][0]} · `{GROUPS.get(p, 'no group')}`")
        show_genie_answer(answers[p])


def pg_query(persona: str, sql: str, params: tuple = (), write: bool = False) -> dict:
    """Run one statement against Lakebase Postgres AS the persona SP (OAuth token, user = application ID)."""
    import psycopg

    try:
        w = persona_client(persona)
        host = w.postgres.get_endpoint(LB_ENDPOINT).status.hosts.host
        token = w.postgres.generate_database_credential(endpoint=LB_ENDPOINT).token
        with psycopg.connect(host=host, dbname=LB_DATABASE, user=os.environ[f"{persona.upper()}_CLIENT_ID"],
                             password=token, sslmode="require", connect_timeout=20, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if write:
                    return {"df": pd.DataFrame([{"rows_affected": cur.rowcount}]), "error": None, "status": "ALLOWED"}
                cols = [d.name for d in cur.description]
                return {"df": pd.DataFrame(cur.fetchall(), columns=cols), "error": None, "status": "ALLOWED"}
    except Exception as e:  # noqa: BLE001
        msg = str(e).splitlines()[0][:500]
        status = "DENIED" if any(k in msg.lower() for k in ("permission denied", "row-level security", "password authentication",
                                                              "role", "not authorized", "permission")) else "ERROR"
        return {"df": None, "error": msg, "status": status}


def tab_lakebase(persona: str) -> None:
    st.subheader("Lakebase (OLTP): the same identities, governed in Postgres and in Unity Catalog")
    st.caption("Layers: Lakebase project ACL → Postgres OAuth role → Postgres GRANT + row-level security by desk → "
               "Unity Catalog on the synced / registered copies. Calls below run as the selected persona SP.")
    left, right = st.columns(2)
    with left:
        st.markdown("**Postgres (direct OLTP connection)**")
        for title, sql in [
            ("Cargo nominations by desk (RLS)", f"SELECT desk, count(*) AS nominations FROM {LB_SCHEMA}.cargo_nominations GROUP BY desk ORDER BY desk"),
            ("Contact column (column-level GRANT)", f"SELECT nomination_id, contact_email FROM {LB_SCHEMA}.cargo_nominations LIMIT 3"),
            ("Operator view (no counterparty / contact)", f"SELECT desk, count(*) AS nominations FROM {LB_SCHEMA}.v_nominations_ops GROUP BY desk ORDER BY desk"),
            ("Synced from the Lakehouse: aggregate desk summary", f"SELECT desk, price_index, contract_count FROM {LB_REF}.desk_book_summary ORDER BY desk"),
        ]:
            st.markdown(f"*{title}*")
            show_result(pg_query(persona, sql), "Allowed, but no rows are visible to this persona.")
        st.markdown("*Submit a nomination (INSERT under RLS `WITH CHECK`)*")
        with st.form(f"nominate_{persona}"):
            desk = st.selectbox("Desk", DESKS)
            alias = st.selectbox("Counterparty", [f"Counterparty {c}" for c in "ABCDEFGH"])
            volume = st.number_input("Volume (MMBtu)", min_value=1_000_000, max_value=5_000_000, value=3_400_000, step=100_000)
            if st.form_submit_button("Submit nomination"):
                nid = f"NOM-APP-{int(time.time())}"
                res = pg_query(persona, f"INSERT INTO {LB_SCHEMA}.cargo_nominations (nomination_id, desk, counterparty_alias, "
                                        f"cargo_window_start, volume_mmbtu, status) VALUES (%s, %s, %s, current_date + 30, %s, 'submitted')",
                               (nid, desk, alias, int(volume)), write=True)
                if res["error"]:
                    st.error(f"**{res['status']}** - {res['error']}")
                else:
                    st.success(f"{nid} inserted. Lakebase CDF streams it to `{OPS_CDC}.lb_cargo_nominations_history`.")
    with right:
        st.markdown("**Unity Catalog (Lakehouse copies)**")
        for title, stmt in [
            ("Governed view over CDF history (desk filter + masked contact)",
             f"SELECT desk, count(*) AS nominations, max(contact_email) AS sample_contact FROM {OPS_VIEWS}.v_nominations_current GROUP BY desk ORDER BY desk"),
            ("Ops view over CDF history (all desks, metadata only)", f"SELECT desk, count(*) AS nominations FROM {OPS_VIEWS}.v_nominations_ops GROUP BY desk ORDER BY desk"),
            ("Raw CDF history (security only)", f"SELECT _pg_change_type, count(*) AS changes FROM {OPS_CDC}.lb_cargo_nominations_history GROUP BY 1"),
            ("Registered Lakebase catalog (read-only mirror, serverless)",
             f"SELECT desk, count(*) AS nominations FROM {LB_UC}.{LB_SCHEMA}.cargo_nominations GROUP BY desk ORDER BY desk"),
        ]:
            st.markdown(f"*{title}*")
            show_result(run_sql(persona, stmt), "Allowed, but no rows are visible to this persona (or CDF has not delivered yet).")


# --------------------------------------------------------------------------- UI
st.set_page_config(page_title="LNG demo governance explorer (synthetic)", page_icon="🛳️", layout="wide")
st.title("Governance explorer: one lakehouse, seven identities (synthetic data)")

with st.sidebar:
    available = [p for p in PERSONAS if has_credentials(p)]
    if not available:
        st.error("No persona credentials found. Check the secret resources in resources/apps.yml.")
        st.stop()
    persona = st.selectbox("Act as service principal", available,
                           format_func=lambda p: f"{PERSONAS[p][0]}  ·  {GROUPS.get(p, 'no group')}")
    st.info(f"**Expected:** {PERSONAS[persona][1]}")
    missing = [p for p in PERSONAS if p not in available]
    if missing:
        st.warning(f"No credentials for: {', '.join(missing)}")
    if st.button("Re-run queries (clear cache)"):
        run_sql.clear()
        access_matrix.clear()
    st.divider()
    st.markdown("**Copilot request tags**")
    desk = st.selectbox("Desk tag", ["atlantic", "pacific", "portfolio"])
    use_case = st.selectbox("Use case tag", ["market_question", "desk_summary", "drafting"])
    st.write("Tracing:", "on (client-side redaction)" if TRACING else f"off ({globals().get('TRACING_ERROR', '')[:120]})")

tabs = st.tabs(["Identity", "RBAC", "Row filter", "Column masks", "Policies", "Medallion", "Aggregate tool", "AI logs", "Copilot chat", "Lakebase (OLTP)", "Genie agent"])
with tabs[0]:
    tab_identity(persona)
with tabs[1]:
    tab_rbac(persona)
with tabs[2]:
    tab_rows(persona)
with tabs[3]:
    tab_masks(persona)
with tabs[4]:
    tab_abac(persona)
with tabs[5]:
    tab_medallion(persona)
with tabs[6]:
    tab_tool(persona)
with tabs[7]:
    tab_logs(persona)
with tabs[8]:
    tab_chat(persona, desk, use_case)
with tabs[9]:
    tab_lakebase(persona)
with tabs[10]:
    tab_genie(persona)
