"""Shared helpers for the LNG demo AI governance demo notebooks.

Kept dependency-light: databricks-sdk, requests, pyyaml. Every helper that talks to
Unity Gateway uses the documented MLflow Chat Completions route
`https://<workspace>/ai-gateway/mlflow/v1/chat/completions` and the documented
`Databricks-Ai-Gateway-Request-Tags` header (JSON object of string tags). Every tag key the
demo writes (request tags, UC tags, job tags) uses the TAG_PREFIX `lngdemo_`.
"""

from __future__ import annotations

import datetime as dt
import json
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import requests
from databricks.sdk import WorkspaceClient

TAG_PREFIX = "lngdemo_"
PROJECT = "lngdemo-ai-governance"
# Governed tag (key, value) that the ABAC contact-PII mask matches (06_contact_pii_abac, src/sql/08_contact_pii_abac.sql)
PII_TAG = (TAG_PREFIX + "sensitivity", "pii")

PERSONAS = [
    "trader",
    "unentitled_trader",
    "developer",
    "operator",
    "security",
    "auditor",
    "app",
]

PARAM_NAMES = [
    "catalog",
    "services_schema",
    "audit_raw_schema",
    "audit_curated_schema",
    "trading_schema",
    "trader_group",
    "unentitled_trader_group",
    "developer_group",
    "operator_group",
    "security_group",
    "auditor_group",
    "app_sp_application_id",
    "secret_scope",
    "retention_days",
    "trace_table",
    "confirm_shared_changes",
    "warehouse_id",
    "bundle_files_root",
    "bundle_target",
    "module_filter",
    "judge_runs",
    "system_model",
    "copilot_service",
    "copilot_dev_service",
    "judge_service",
    "copilot_payload",
    "copilot_dev_payload",
    "judge_payload",
    "traces_experiment",
    "admin_group",
    # medallion pipeline
    "raw_schema",
    "landing_volume",
    "bronze_schema",
    "silver_schema",
    # M9 Lakebase
    "ops_catalog",
    "ops_cdc_schema",
    "ops_curated_schema",
    "ops_pipeline_schema",
    "lakebase_project",
    "lakebase_branch",
    "lakebase_endpoint",
    "lakebase_dev_branch",
    "lakebase_dev_endpoint",
    "lakebase_database",
    "lakebase_pg_schema",
    "lakebase_reference_schema",
    "lakebase_uc_catalog",
    "role_trader",
    "role_unentitled_trader",
    "role_operator",
    "role_security",
    "trader_sp",
    "unentitled_trader_sp",
    "developer_sp",
    "operator_sp",
    "security_sp",
    "auditor_sp",
    # M10 Genie agent
    "genie_space_title",
]


# --------------------------------------------------------------------------- params


def get_params(dbutils) -> dict[str, str]:
    """Read job parameters (exposed to notebooks as widgets). Missing ones become ''."""
    out = {}
    for name in PARAM_NAMES:
        try:
            out[name] = dbutils.widgets.get(name)
        except Exception:  # widget not defined for this job
            out[name] = ""
    return out


def services(p: dict) -> dict[str, dict[str, str]]:
    """Model services and their inference tables, passed in by the bundle (no hard-coded names).
    Keys: prod / dev / judge -> {"service": catalog.schema.name, "payload": catalog.schema.table}."""
    return {
        "prod": {"service": p["copilot_service"], "payload": p["copilot_payload"]},
        "dev": {"service": p["copilot_dev_service"], "payload": p["copilot_dev_payload"]},
        "judge": {"service": p["judge_service"], "payload": p["judge_payload"]},
    }


def qn(full_name: str) -> str:
    """Backtick-quote a dotted full name ('a.b.c' -> `a`.`b`.`c`)."""
    return fq(*full_name.split("."))


def fq(*parts: str) -> str:
    """Backtick-quote a dotted UC name."""
    return ".".join(f"`{p}`" for p in parts)


def shared_changes_confirmed(p: dict) -> bool:
    return str(p.get("confirm_shared_changes", "")).strip().lower() == "true"


# --------------------------------------------------------------------------- tags


def tags(**kv: str) -> dict[str, str]:
    """Prefix every key with TAG_PREFIX (idempotent): tags(desk='atlantic') -> {'lngdemo_desk': 'atlantic'}."""
    return {(k if k.startswith(TAG_PREFIX) else TAG_PREFIX + k): str(v) for k, v in kv.items() if v is not None}


def base_tags(p: dict, **extra: str) -> dict[str, str]:
    """Standard asset tags (same keys as presets.tags in databricks.yml) plus object-specific ones."""
    return tags(**{"project": PROJECT, "env": p.get("bundle_target") or "dev", "data_class": "synthetic", **extra})


def request_tags(**kv: str) -> dict[str, str]:
    """Gateway request tags (Databricks-Ai-Gateway-Request-Tags header), always carrying the project."""
    return tags(project=PROJECT, **kv)


# --------------------------------------------------------------------------- SQL files


def render_sql(path: str, subs: dict[str, str]) -> list[str]:
    """Load a numbered SQL file, substitute {{name}} placeholders, split on ';' at line end."""
    text = open(path).read()
    for k, v in subs.items():
        text = text.replace("{{" + k + "}}", v)
    missing = set(re.findall(r"\{\{(\w+)\}\}", text))
    if missing:
        raise ValueError(f"{path}: unresolved placeholders {sorted(missing)}")
    stmts = [s.strip() for s in re.split(r";\s*(?:\n|$)", text)]
    return [s for s in stmts if s and not all(l.strip().startswith("--") or not l.strip() for l in s.splitlines())]


def run_sql_file(spark, path: str, subs: dict[str, str], tolerate: tuple[str, ...] = ()) -> list[dict]:
    """Run each statement; return a log. Errors containing any `tolerate` substring are recorded, not raised."""
    log = []
    for stmt in render_sql(path, subs):
        first = stmt.splitlines()[0][:120]
        try:
            spark.sql(stmt)
            log.append({"statement": first, "ok": True, "error": None})
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            if any(t in msg for t in tolerate):
                log.append({"statement": first, "ok": False, "error": msg[:500]})
            else:
                raise
    return log


def sql_subs(p: dict) -> dict[str, str]:
    return {k: v for k, v in p.items() if v is not None}


# --------------------------------------------------------------------------- identities


@dataclass
class Identity:
    persona: str
    client: WorkspaceClient | None
    note: str = ""

    @property
    def available(self) -> bool:
        return self.client is not None

    def headers(self) -> dict[str, str]:
        return self.client.config.authenticate()  # {'Authorization': 'Bearer ...'}

    def principal(self) -> str:
        try:
            me = self.client.current_user.me()
            return me.user_name or me.application_id or me.display_name or "?"
        except Exception as e:  # noqa: BLE001
            return f"unknown ({type(e).__name__})"


def runner_identity() -> Identity:
    return Identity("job_runner", WorkspaceClient(), "job run-as identity")


def persona_identity(persona: str, dbutils, scope: str) -> Identity:
    """OAuth M2M identity for a persona SP from the secret scope (<persona>_client_id/_client_secret)."""
    if persona == "job_runner":
        return runner_identity()
    try:
        cid = dbutils.secrets.get(scope, f"{persona}_client_id")
        secret = dbutils.secrets.get(scope, f"{persona}_client_secret")
    except Exception:  # noqa: BLE001
        return Identity(persona, None, f"no credentials in scope '{scope}' -> NOT TESTED")
    host = WorkspaceClient().config.host
    w = WorkspaceClient(host=host, client_id=cid, client_secret=secret, auth_type="oauth-m2m")
    return Identity(persona, w, "service principal placed in the persona group (not a human user token)")


# --------------------------------------------------------------------------- gateway calls


@dataclass
class GatewayResult:
    service: str
    persona: str
    http_status: int | None
    latency_ms: int
    content: str | None
    policy: Any | None  # top-level databricks_service_policy object, if present
    error: str | None
    raw: str
    request_marker: str
    sent_at: str = field(default_factory=lambda: dt.datetime.utcnow().isoformat())

    @property
    def blocked(self) -> bool:
        return self.policy is not None

    def outcome(self) -> str:
        if self.http_status is None:
            return "ERROR"
        if self.http_status in (401, 403):
            # A workspace-entitlement rejection happens before any UC grant check, so it must not
            # count as evidence that UC denied the caller (observed in the demo workspace, 2026-10-28).
            if "entitlement" in (self.raw or "").lower():
                return "DENIED_ENTITLEMENT"
            return "DENIED_ACCESS"
        if self.http_status == 404 and "does not exist" in (self.raw or ""):
            # UC hides securables the caller has no privilege on: a denied caller gets NOT_FOUND.
            # Only meaningful when an entitled caller resolves the same name (compare in the matrix).
            return "DENIED_NOT_FOUND"
        if self.http_status == 429:
            return "RATE_LIMITED"
        if self.http_status >= 400:
            return f"HTTP_{self.http_status}"
        return "BLOCKED_BY_POLICY" if self.blocked else "ALLOWED"


def message_text(content) -> str | None:
    """Assistant content may be a string or a list of typed parts (reasoning models return
    [{'type': 'reasoning', ...}, {'type': 'text', 'text': ...}]). Keep only the text parts."""
    if content is None or isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(p.get("text", "") for p in content if isinstance(p, dict) and p.get("type") in ("text", "output_text"))
    return str(content)


def chat(
    identity: Identity,
    service: str,
    prompt: str,
    *,
    tags: dict[str, str] | None = None,
    max_tokens: int = 200,
    system: str | None = None,
) -> GatewayResult:
    """Send one chat-completions request to a model service and keep the raw response.

    A unique marker is appended so the request can be found later in inference/evaluator tables.
    """
    marker = f"[lngdemo:{uuid.uuid4().hex[:12]}]"
    host = identity.client.config.host.rstrip("/")
    url = f"{host}/ai-gateway/mlflow/v1/chat/completions"
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": f"{prompt}\n\n{marker}"})
    headers = {"Content-Type": "application/json", **identity.headers()}
    # Keys are prefixed with TAG_PREFIX here (desk -> lngdemo_desk) and lngdemo_project is always set.
    headers["Databricks-Ai-Gateway-Request-Tags"] = json.dumps(request_tags(**(tags or {})))
    body = {"model": service, "messages": messages, "max_tokens": max_tokens}
    t0 = time.time()
    try:
        r = requests.post(url, headers=headers, json=body, timeout=120)
    except Exception as e:  # noqa: BLE001
        return GatewayResult(service, identity.persona, None, int((time.time() - t0) * 1000), None, None, str(e), "", marker)
    latency = int((time.time() - t0) * 1000)
    raw = r.text
    content, policy, error = None, None, None
    try:
        j = r.json()
        policy = j.get("databricks_service_policy")
        choices = j.get("choices") or []
        if choices:
            content = message_text((choices[0].get("message") or {}).get("content"))
        if r.status_code >= 400:
            error = json.dumps(j)[:1000]
    except ValueError:
        error = raw[:1000]
    return GatewayResult(service, identity.persona, r.status_code, latency, content, policy, error, raw[:20000], marker)


def result_row(res: GatewayResult, **extra) -> dict:
    row = {
        "run_ts": res.sent_at,
        "service": res.service,
        "persona": res.persona,
        "http_status": res.http_status,
        "outcome": res.outcome(),
        "latency_ms": res.latency_ms,
        "policy_json": json.dumps(res.policy) if res.policy is not None else None,
        "content_preview": (res.content or "")[:500],
        "error": res.error,
        "raw_response": res.raw,
        "request_marker": res.request_marker,
    }
    row.update(extra)
    return row


# --------------------------------------------------------------------------- tables


def append_rows(spark, table: str, rows: list[dict]) -> None:
    if not rows:
        return
    import pandas as pd

    df = spark.createDataFrame(pd.DataFrame(rows).astype(str).replace({"None": None, "nan": None}))
    df.write.mode("append").option("mergeSchema", "true").saveAsTable(table)


def ledger_update(spark, ledger_table: str, claim_id: int, test: str, observed: str, status: str) -> None:
    """Upsert one row of the claim ledger (audit_curated.claim_ledger)."""
    spark.sql(
        f"""
        MERGE INTO {ledger_table} t
        USING (SELECT {int(claim_id)} AS claim_id) s ON t.claim_id = s.claim_id
        WHEN MATCHED THEN UPDATE SET
          test_performed = :test, observed_result = :observed, status = :status, tested_at = current_timestamp()
        """,
        args={"test": test[:2000], "observed": observed[:4000], "status": status},
    )


def list_functions(spark, catalog: str, schema: str) -> list[str]:
    """Full names of user functions in a schema. (`SHOW USER FUNCTIONS IN cat.schema` is rejected on
    serverless with CROSS_CATALOG_SCHEMA_REFERENCE_NOT_SUPPORTED, so use information_schema.)"""
    rows = spark.sql(
        f"SELECT routine_name FROM {fq(catalog, 'information_schema', 'routines')} WHERE routine_schema = :s",
        args={"s": schema}).collect()
    return [f"{catalog}.{schema}.{r['routine_name']}" for r in rows]


def table_exists(spark, full_name: str) -> bool:
    return spark.catalog.tableExists(full_name)


def show(df_or_rows, display_fn=None):
    """display() when available (notebook), else print."""
    if display_fn is not None:
        display_fn(df_or_rows)
    else:
        print(df_or_rows)


# --------------------------------------------------------------------------- Lakebase (M9)

def lb_paths(p: dict, branch: str | None = None) -> dict[str, str]:
    """Lakebase resource paths built from job parameters (project/branch/endpoint come from the bundle)."""
    b = branch or p["lakebase_branch"]
    branch_path = f"projects/{p['lakebase_project']}/branches/{b}"
    ep = p["lakebase_endpoint"] if b == p["lakebase_branch"] else p["lakebase_dev_endpoint"]
    return {"project": f"projects/{p['lakebase_project']}", "branch": branch_path,
            "endpoint": f"{branch_path}/endpoints/{ep}"}


def pg_connect(client: WorkspaceClient, endpoint_path: str, database: str, user: str | None = None):
    """Open a psycopg connection as the identity behind `client` (OAuth token from
    generate_database_credential; the Postgres user is the user name or SP application ID)."""
    import psycopg

    host = client.postgres.get_endpoint(endpoint_path).status.hosts.host
    token = client.postgres.generate_database_credential(endpoint=endpoint_path).token
    if user is None:
        me = client.current_user.me()
        user = me.user_name  # for an SP this is its application ID
    return psycopg.connect(host=host, dbname=database, user=user, password=token, sslmode="require",
                           connect_timeout=30, autocommit=True)


def pg_run_file(conn, path: str, subs: dict[str, str]) -> list[dict]:
    """Run a numbered Postgres SQL file statement by statement; returns a log, raises on error."""
    log = []
    with conn.cursor() as cur:
        for stmt in render_sql(path, subs):
            cur.execute(stmt)
            log.append({"statement": stmt.splitlines()[0][:120], "ok": True})
    return log


def lb_subs(p: dict) -> dict[str, str]:
    """Placeholders for src/lakebase/*.sql."""
    return {
        "pg_schema": p["lakebase_pg_schema"], "reference_schema": p["lakebase_reference_schema"],
        "database": p["lakebase_database"],
        "role_trader": p["role_trader"], "role_unentitled_trader": p["role_unentitled_trader"],
        "role_operator": p["role_operator"], "role_security": p["role_security"],
        "trader_sp": p["trader_sp"], "unentitled_trader_sp": p["unentitled_trader_sp"],
        "operator_sp": p["operator_sp"], "security_sp": p["security_sp"], "developer_sp": p["developer_sp"],
    }


# --------------------------------------------------------------------------- Genie agent (M10)

MASKED_PRICE_TEXT = "*** masked: contract price ***"  # mask_price_text output (src/sql/02_*.sql)

# (key, question, curated SQL with {table}) - the demo questions. Each one exercises one control layer on gold
# `contracts`: rows -> desk row filter, prices -> group-bound price masks, contacts -> tag-bound ABAC mask.
GENIE_QUESTIONS = [
    ("rows", "How many contracts can I see per desk?",
     "SELECT desk, count(*) AS contracts FROM {table} GROUP BY desk ORDER BY desk"),
    ("prices", "Show the price formula, slope and constant for each contract",
     "SELECT contract_id, counterparty_alias, desk, price_formula, slope, constant_usd_mmbtu FROM {table} "
     "ORDER BY contract_id LIMIT 20"),
    ("contacts", "List the counterparty contact email and phone for each contract",
     "SELECT contract_id, counterparty_alias, desk, contact_email, contact_phone FROM {table} "
     "ORDER BY contract_id LIMIT 20"),
]


def _gid(*parts: str) -> str:
    """Stable 32-hex id (Genie requires lowercase hex ids; stable ids keep re-runs idempotent)."""
    return uuid.uuid5(uuid.NAMESPACE_URL, "lngdemo-genie/" + "/".join(parts)).hex


def genie_space_payload(table: str) -> str:
    """serialized_space (version 2) for the governed contract-book Genie agent over one gold table."""
    by_id = lambda items: sorted(items, key=lambda x: x["id"])  # noqa: E731  (id-keyed arrays must be sorted)
    payload = {
        "version": 2,
        "config": {"sample_questions": by_id([{"id": _gid("sq", k), "question": [q]} for k, q, _ in GENIE_QUESTIONS])},
        "data_sources": {"tables": [{
            "identifier": table,
            "column_configs": sorted([
                {"column_name": "contact_email", "description": ["Counterparty contact e-mail (PII, tag lngdemo_sensitivity=pii)."]},
                {"column_name": "contact_phone", "description": ["Counterparty contact phone (PII, tag lngdemo_sensitivity=pii)."]},
                {"column_name": "desk", "enable_entity_matching": True,
                 "description": ["Trading desk: Atlantic Desk, Pacific Desk or Portfolio Desk."]},
                {"column_name": "price_formula", "synonyms": ["price", "pricing", "formula"],
                 "description": ["Contract price formula. Masked for non-pilot traders."]},
                {"column_name": "slope", "description": ["Price slope vs the index. NULL when masked."]},
                {"column_name": "constant_usd_mmbtu", "synonyms": ["constant", "adder"],
                 "description": ["Price constant in USD/MMBtu. NULL when masked."]},
            ], key=lambda c: c["column_name"]),
        }]},
        "instructions": {
            "text_instructions": [{"id": _gid("ti"), "content": [
                "All data is synthetic. Answer only from the contracts table.\n",
                "Unity Catalog enforces access for the person asking: a desk row filter, column masks on price terms, "
                "and an attribute-based (tag-driven) mask on contact columns. Results differ by user on purpose.\n",
                f"Values equal to '{MASKED_PRICE_TEXT}', NULL slope/constant, or starting with '***' are masked by policy. "
                "Say they are masked by Unity Catalog policy. Never guess, reconstruct or estimate masked values.\n",
                "If only some desks appear, say the user can see only the desks they are entitled to; do not claim the "
                "other desks have no contracts.",
            ]}],
            "example_question_sqls": by_id([
                {"id": _gid("eq", k), "question": [q], "sql": [sql.format(table=table)]} for k, q, sql in GENIE_QUESTIONS]),
        },
    }
    return json.dumps(payload)


def find_genie_space(client: WorkspaceClient, title: str) -> str | None:
    """Space id of the first space with this title that `client` can see (None if none)."""
    token = None
    while True:
        resp = client.genie.list_spaces(page_token=token)
        for s in resp.spaces or []:
            if s.title == title:
                return s.space_id
        token = resp.next_page_token
        if not token:
            return None


def ask_genie(client: WorkspaceClient, space_id: str, question: str, timeout_s: int = 300) -> dict:
    """Ask one question AS `client` and return {status, text, sql, columns, rows, error}. Never raises.

    status: ANSWERED (a query ran and returned a result) | QUERY_FAILED | TEXT_ONLY | FAILED | ERROR."""
    out = {"status": "ERROR", "text": None, "sql": None, "columns": [], "rows": [], "error": None, "conversation_id": None}
    try:
        msg = client.genie.start_conversation_and_wait(space_id, question, timeout=dt.timedelta(seconds=timeout_s))
    except Exception as e:  # noqa: BLE001  (e.g. no CAN_RUN on the space, warehouse access)
        out["error"] = f"{type(e).__name__}: {str(e)[:600]}"
        return out
    out["conversation_id"] = msg.conversation_id
    if msg.error:
        out["error"] = (msg.error.error or str(msg.error.type))[:600]
    texts = [a.text.content for a in (msg.attachments or []) if a.text and a.text.content]
    out["text"] = "\n".join(texts) or None
    query = next((a for a in (msg.attachments or []) if a.query), None)
    if query is None:
        out["status"] = "FAILED" if str(msg.status).endswith("FAILED") else "TEXT_ONLY"
        return out
    out["sql"] = query.query.query
    try:
        res = client.genie.get_message_attachment_query_result(space_id, msg.conversation_id, msg.message_id,
                                                               query.attachment_id).statement_response
    except Exception as e:  # noqa: BLE001
        out.update(status="QUERY_FAILED", error=f"{type(e).__name__}: {str(e)[:600]}")
        return out
    state = str(res.status.state) if res and res.status else ""
    if not state.endswith("SUCCEEDED"):
        err = res.status.error.message if res and res.status and res.status.error else state
        out.update(status="QUERY_FAILED", error=(err or "")[:600])
        return out
    out["columns"] = [c.name for c in res.manifest.schema.columns] if res.manifest and res.manifest.schema else []
    out["rows"] = (res.result.data_array if res.result else None) or []
    out["status"] = "ANSWERED"
    return out


def genie_observation(key: str, ans: dict) -> str:
    """Reduce one Genie answer to what the governance layer under test decided.

    rows -> visible desks ('Atlantic Desk,Portfolio Desk'); prices / contacts -> 'clear' | 'masked';
    'DENIED' when no query result came back; 'no rows' / 'column not returned' when inconclusive."""
    if ans["status"] != "ANSWERED":
        return "DENIED"
    if not ans["rows"]:
        return "no rows"
    col = {c: i for i, c in enumerate(ans["columns"])}
    values = lambda name: [r[col[name]] for r in ans["rows"]]  # noqa: E731
    if key == "rows":
        return ",".join(sorted({v for v in values("desk") if v})) if "desk" in col else "column not returned"
    if key == "prices":
        if "price_formula" not in col:
            return "column not returned"
        return "masked" if all(v in (None, MASKED_PRICE_TEXT) for v in values("price_formula")) else "clear"
    if key == "contacts":
        if "contact_email" not in col:
            return "column not returned"
        return "masked" if all(v is None or str(v).startswith("***") for v in values("contact_email")) else "clear"
    raise ValueError(key)
