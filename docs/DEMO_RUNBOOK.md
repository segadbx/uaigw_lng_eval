# Demo runbook: Unity Gateway governance for an LNG trading copilot

Audience: an enterprise architect at an LNG exporter. Core concern: counterparty contract pricing
must never be exposed, regurgitated or logged where it shouldn't be.

**Talk-track rule.** Sentences in *italics* are talk track. They may only state **Confirmed** rows from
[VERIFICATION.md](VERIFICATION.md) as fact. Everything else is phrased "documented as…" or "we
observed…". Before the session, run `21_verify_claims` and update the phrasing. Recommendations are
marked **[design recommendation]**, not product behaviour (claim 14).

`<cat>` = `lngdemo_gov`. Pre-reqs: setup, P1-P3 attached, red_team run ≥ 60 min earlier,
open_questions run, persona_matrix run, dashboard open.

---

## M0 · Data foundation: medallion pipeline (raw → bronze → silver → gold), 6 min

The governed contract book is not a hand-made table: a Lakeflow Spark Declarative Pipeline (`lngdemo-medallion`)
builds it from mock source-system extracts, and governance is declared on the gold layer.

| Min | Do this | Visible result |
|---|---|---|
| 0-1 | Catalog Explorer → volume `<cat>.raw.landing` | RAW: `contracts/batch_0001_initial.json`, `batch_0002_amendments.json`, `counterparties/counterparties.csv` |
| 1-3 | Jobs & Pipelines → `lngdemo-medallion` → graph | BRONZE (Auto Loader streaming tables) → SILVER (`contract_events` with expectations, `contracts_current` SCD1, `contracts_history` SCD2, `counterparties`) → GOLD (`trading_data.contracts`, `desk_book_summary`, `desk_exposure`) |
| 3-4 | Pipeline → `contract_events` → **Data quality** | 3 records dropped by expectations (missing desk, negative volume, unknown index) |
| 4-5 | App → **Medallion** tab as security, then as trader | Security sees every layer, including SCD2 price history for 6 renegotiated contracts. The trader can't read bronze/silver, and sees only entitled desks in gold |
| 5-6 | App → **Policies** tab | Row filter + masks attached to gold `contracts`, declared in the pipeline (`gold/contracts.sql`) |

```sql
-- 2 cancellations removed, 6 amendments applied, 3 invalid records dropped
SELECT (SELECT count(*) FROM <cat>.bronze.bronze_contract_events) AS bronze_events,
       (SELECT count(*) FROM <cat>.silver.contract_events)        AS silver_valid_events,
       (SELECT count(*) FROM <cat>.silver.contracts_current)      AS current_contracts;
SELECT contract_id, price_formula, __START_AT, __END_AT FROM <cat>.silver.contracts_history
WHERE contract_id IN (SELECT contract_id FROM <cat>.silver.contracts_history GROUP BY 1 HAVING count(*) > 1)
ORDER BY contract_id, __START_AT;
```

*"Protection is declared with the data product: the gold table's row filter and masks are part of the pipeline
definition, and Unity Catalog enforces them for every reader."* (claim 21, test pending)

**[design recommendation]:** keep bronze/silver security-only and build aggregates from silver; expose only gold.
**Limits:** row filters / masks on pipeline datasets are Public Preview, and they can't be added with `ALTER TABLE`
afterwards. Aggregates must read silver, because the gold row filter applies to the pipeline's own identity too.

---

## M1 · Control plane: who may call which model? (Q1), 6 min

| Min | Do this | Visible result |
|---|---|---|
| 0-1 | AI Gateway → Models → `trading_copilot` → **Permissions** | EXECUTE: trader pilot group + app SP only; MANAGE: security |
| 1-3 | Show `red_team` rows m1_* (query below) | entitled trader `ALLOWED`; unentitled `DENIED_ACCESS` (401/403); app SP `ALLOWED` |
| 3-4 | Show m1_system_ai_* rows + `03_system_ai_access` output | Whether default `system.ai` access lets users bypass the governed service (as observed) |
| 4-6 | Definer's privileges: show `04_capability_probe` `effective_owner` | The caller has no grant on the destination model and the call still succeeds, because the owner's access is used |

```sql
SELECT test_id, persona, service, outcome, http_status, verdict
FROM <cat>.audit_curated.red_team_results
WHERE module = 'M1' QUALIFY row_number() OVER (PARTITION BY test_id ORDER BY run_ts DESC) = 1;
```

*"Access to the copilot is a Unity Catalog grant on the service. Model services are documented as
using the owner's privileges on the destination, so traders never get direct model access."*

**Limit:** `system.ai` services are callable by whoever holds EXECUTE on them. Unless you lock that
down (a shared, metastore-wide change), users can go around the governed service.

## M2 · Inference tables: what gets logged? (Q2), 7 min

| Min | Do this | Visible result |
|---|---|---|
| 0-1 | `DESCRIBE TABLE <cat>.audit_raw.trading_copilot_payload` | Full column list straight from the table (not hard-coded) |
| 1-4 | Query by marker for m2_normal / m2_blocked_input / m2_redacted | Which requests produced rows, and whether `request` holds original or redacted text |
| 4-6 | `logging_error_codes` breakdown | Empty for normal calls; `MAX_REQUEST_SIZE_EXCEEDED` only if m2_oversized was enabled |
| 6-7 | Show `trading_judge_payload` | The evaluator's table holds the judged content (see M5) |

```sql
SELECT r.test_id, r.outcome, p.event_time, p.status_code, p.requester, p.logging_error_codes,
       left(p.request, 200) AS request_start
FROM <cat>.audit_curated.red_team_results r
LEFT JOIN <cat>.audit_raw.trading_copilot_payload p ON p.request LIKE concat('%', r.request_marker, '%')
WHERE r.test_id IN ('m2_normal','m2_blocked_input','m2_redacted','m4_sdd_email_phone')
QUALIFY row_number() OVER (PARTITION BY r.test_id ORDER BY r.run_ts DESC) = 1;
```

*"Inference tables are documented as holding the raw request and response JSON. We observed
<fill from claim 1> for redacted requests and <fill from claim 2> for blocked ones."*

**Limit:** delivery is documented as taking up to an hour, payloads over 10 MiB aren't logged, and
401/403/429/500 responses may not be logged.

## M3 · Who can read the logs, for how long, and how do we monitor? (Q3), 7 min

| Min | Do this | Visible result |
|---|---|---|
| 0-2 | `SHOW GRANTS ON SCHEMA <cat>.audit_raw` + persona matrix `read_audit_raw` column | Only the security group reads raw payloads |
| 2-3 | `SELECT * FROM <cat>.audit_curated.inference_redacted LIMIT 5` | `[EMAIL]`, `[PHONE]`, `[COUNTERPARTY]`, `[PRICE]` placeholders |
| 3-4 | `SELECT * FROM <cat>.audit_curated.v_inference_ops LIMIT 5` (operator view) | Metadata only, no payloads |
| 4-5 | `experiment_log` for Auto-TTL / column mask + `SHOW TBLPROPERTIES` | Whether TTL / mask could be set on auto-created tables, and whether delivery continued |
| 5-7 | Dashboard → **Gateway usage** page | Requests by requester, `lngdemo_desk` / `lngdemo_use_case` request tags, latency, status codes |

```sql
SELECT run_ts, experiment, target, ok, left(detail, 160) AS detail
FROM <cat>.audit_curated.experiment_log ORDER BY run_ts DESC;
SHOW TBLPROPERTIES <cat>.audit_raw.trading_copilot_payload;
```

*"Raw payloads sit in a schema only security can read; operators get a redacted copy."*
**[design recommendation]:** 7-day TTL on raw payloads, a longer TTL on curated metadata.

**Limit:** Auto-TTL and column masks on auto-created inference tables are not documented. Show the
ledger status (claims 7, 8). `system.ai_gateway.usage` has no policy-outcome columns.

## M4 · Masking and redaction (Q4), 8 min

| Min | Do this | Visible result |
|---|---|---|
| 0-2 | As trader vs. unentitled trader: `SELECT desk, counterparty_alias, price_formula, slope FROM <cat>.trading_data.contracts` | Different desks, prices masked for the unentitled trader (row filter + masks declared in the medallion pipeline's gold `contracts`; see `setup_state`) |
| 2-3 | `SELECT * FROM <cat>.services.desk_price_summary('Atlantic Desk')` | Aggregates only, no price columns, groups < 3 suppressed |
| 3-5 | App or red-team m4_sdd_email_phone | Email/phone replaced by placeholders |
| 5-6 | m4_sql_deny_alias / m4_sql_deny_formula | HTTP 200 + `databricks_service_policy` reason |
| 6-7 | m4_judge_log + trace/evaluator query | Judge verdict recorded in Log mode, request not blocked |
| 7-8 | MLflow experiment `/Shared/lngdemo-trading-copilot-traces` | App traces with `[EMAIL]`/`[PRICE]` redacted client-side |

```sql
SELECT test_id, outcome, http_status, policy_json, content_preview
FROM <cat>.audit_curated.red_team_results WHERE module = 'M4'
QUALIFY row_number() OVER (PARTITION BY test_id ORDER BY run_ts DESC) = 1;
```

*"Custom service policies are SQL functions that return ALLOW or DENY. They can't rewrite content;
only Sensitive Data Detection redacts."* (claim 5, Confirmed from documentation)

**Limit:** policy functions only have LIKE/CONTAINS-style string matching (no regex), so the price
detector is a fixed pattern list.

## M5 · What the controls do and don't do (Q5), 7 min

| Min | Do this | Visible result |
|---|---|---|
| 0-2 | m5_sdd_ignores_alias (dev, SDD only) | SDD lets "Counterparty B" through (no name/org category) |
| 2-3 | m5_price_in_words | Price written in words passes the SQL deny |
| 3-5 | `11_judge_repeat` distribution | Verdict split across N identical prompts |
| 5-6 | Judge `reason` column | Reasons can restate the flagged content |
| 6-7 | `trading_judge_payload` | The evaluator table holds the evaluated text, so it needs the same protection as audit_raw |

```sql
SELECT get_json_object(get_json_object(j.response, '$.choices[0].message.content'), '$.flagged') AS flagged, count(*)
FROM <cat>.audit_curated.red_team_results r
JOIN <cat>.audit_raw.trading_judge_payload j ON j.request LIKE concat('%', r.request_marker, '%')
WHERE r.test_id LIKE 'm5_judge_repeat_%' GROUP BY 1;
```

*"We observed <x/10> flags for the same prompt. Judge verdicts are documented as non-deterministic."*

**Text-only limits (not built):**
* On model provider services, passthrough requests ("forward all URL paths") are documented as **not evaluated** by service policies.
* Agent Services are not supported by service policies (documented).
* `ai_query` against `system.ai` gets usage tracking only. Rate limits, guardrails and inference tables
  are documented as not applying.
* Request tags aren't passed to LLM judges or external policies (documented).

**Limit:** pattern lists miss paraphrases and judges vary. **[design recommendation]** layer SDD + SQL + judge (Log first) + log access control.

## M6 · Separation of duties (Q6), 5 min

| Min | Do this | Visible result |
|---|---|---|
| 0-4 | Persona matrix query (below) | Pass/fail per persona × action |
| 4-5 | Point at `identity` column | "SP placed in persona group", not a human token |

```sql
SELECT persona, action, expected, observed, verdict, left(detail, 120) AS detail
FROM <cat>.audit_curated.persona_matrix_results
WHERE run_id = (SELECT run_id FROM <cat>.audit_curated.persona_matrix_results ORDER BY run_ts DESC LIMIT 1)
ORDER BY persona, action;
```

**[design recommendation]:** the persona matrix itself (who should have what).
**Limit:** service policies apply to all account users of a service (documented), so per-persona
behaviour comes from grants and separate services, not from policy scoping.

## M7 · Operating model and evidence (Q7), 6 min

| Min | Do this | Visible result |
|---|---|---|
| 0-2 | Open the latest `/Volumes/<cat>/audit_curated/evidence/evidence_*.md` | Grants, policy inventory (manual capture), red-team, retention, audit log, ledger |
| 2-5 | Fail-closed: P5 on dev → `fail_closed_check` → show raw response → detach | What the caller receives when policy evaluation errors |
| 5-6 | Audit-log section of the pack | Grant / service changes in the window |

```sql
SELECT outcome, http_status, policy_json, error FROM <cat>.audit_curated.red_team_results
WHERE test_id = 'm7_fail_closed' ORDER BY run_ts DESC LIMIT 1;
```

*"Service policies are documented as failing closed: evaluation errors, timeouts and missing fields
result in DENY. We observed <fill from claim 4>."*

**Limit:** no API lists service-policy attachments during the Beta, so the policy inventory is a
manual screenshot.

## M8 · Governance explorer + copilot (Q8), 10 min

Open app `lngdemo-trading-copilot`. The sidebar dropdown picks a persona **service principal**; every SQL
statement and gateway call then runs with that SP's OAuth M2M token (credentials: app `secret` resources
from scope `lngdemo_gov`). Observed on FE-SANDBOX-AWS 2026-10-29:

| Persona SP | RBAC tab | Row filter | Column masks | AI logs | Aggregate tool |
|---|---|---|---|---|---|
| Trader (pilot) | contracts + tool | Atlantic 16, Portfolio 16 | clear | denied | 7 rows |
| Trader (unentitled) | contracts + tool | Pacific 16 | masked text / NULL | denied | 3 rows |
| Developer | nothing | denied | denied | denied | denied |
| Operator | `audit_curated` only | denied | denied | redacted + metadata | denied |
| Security | `audit_curated` + `audit_raw` | denied (no USE SCHEMA on `trading_data`) | denied | raw + redacted + metadata | denied |
| Auditor | `audit_curated` only | denied | denied | redacted + metadata | denied |
| App SP | tool only | denied | denied | denied | 0 rows (row filter inside the function) |

| Min | Do this | Visible result |
|---|---|---|
| 0-3 | **Trader** → Row filter, Column masks tabs | 32 rows, price formulas in clear |
| 3-5 | Switch to **Trader (unentitled)** | Only Pacific rows; price columns highlighted as masked |
| 5-6 | **Policies** tab | Attached row filter and masks on gold `contracts` (pipeline-declared), protection mode |
| 6-7 | **Security** → AI logs; then **Operator** | Raw payloads vs redacted/metadata only |
| 7-9 | **Trader** → Copilot chat: "How is the Atlantic desk book split by index?", then paste a price | Tool answer with aggregates; then a red policy block |
| 9-10 | **Trader (unentitled)** → same question | Access denied (no EXECUTE on the service) |

Acceptance-test results:

```sql
SELECT test_no, test_name, verdict, left(evidence, 200) AS evidence
FROM <cat>.audit_curated.acceptance_results
QUALIFY row_number() OVER (PARTITION BY test_no ORDER BY run_ts DESC) = 1 ORDER BY test_no;
```

**Limit:** the app acts as persona SPs, not as the signed-in human, so gateway usage and audit logs attribute
calls to the SP. `lngdemo_*` request-tag values (including `lngdemo_persona`) are caller-supplied, so they don't prove identity.

## M9 · Governing the OLTP store: Lakebase (Q1, Q4, Q6 applied to operational data), 8 min

Scenario: traders enter **cargo nominations** in a Lakebase Postgres database (`trade_ops`). The same seven identities
and the same evidence model as the AI modules govern it, across separate enforcement points.
Pre-reqs: jobs `lakebase` and `lakebase_matrix` run; Lakebase CDF preview on (MANUAL_STEPS L1).

| Min | Do this | Visible result |
|---|---|---|
| 0-1 | Lakebase App → project `lngdemo-trading-ops` → **Settings › Project permissions**, then **Roles & Databases** | Platform ACL (security CAN_MANAGE; traders/operators/developers CAN_USE; auditors nothing) is separate from Postgres roles (one OAuth role per persona SP + `lngdemo_*_role` group roles) |
| 1-3 | App → **Lakebase (OLTP)** tab, switch trader → unentitled trader → operator → security | Trader sees Atlantic + Portfolio rows only, unentitled Pacific only (RLS); contact column denied except security (column GRANT); operator sees all desks without counterparty/contact |
| 3-4 | As trader, submit a nomination for **Pacific Desk**, then for **Atlantic Desk** | Pacific rejected by RLS `WITH CHECK`; Atlantic accepted |
| 4-5 | Same tab, right column (Unity Catalog), after ~1-2 min | The new nomination appears in `lngdemo_ops.lakebase_cdc.lb_cargo_nominations_history` (CDF). Traders read it only through `v_nominations_current` (desk-filtered, contact masked) |
| 5-6 | Postgres `reference.desk_book_summary` | Lakehouse → Lakebase synced table: aggregate-only (no prices) |
| 6-7 | `experiment_log` row `lakebase_fgac_probe` | What happened when the masked, row-filtered `contracts` table was synced into Postgres (claim 17) |
| 7-8 | Persona matrix (query below) | 14 actions × 6 personas, PASS/FAIL against the design |

```sql
SELECT persona, action, expected, observed, verdict, left(detail, 120) AS detail
FROM lngdemo_gov.audit_curated.persona_matrix_results
WHERE run_id = (SELECT run_id FROM lngdemo_gov.audit_curated.persona_matrix_results
                WHERE run_id LIKE 'lb-%' ORDER BY run_ts DESC LIMIT 1)
ORDER BY persona, action;

SELECT run_ts, experiment, target, ok, left(detail, 200) AS detail
FROM lngdemo_gov.audit_curated.experiment_log WHERE experiment LIKE 'lakebase%' ORDER BY run_ts DESC;
```

*"Lakebase project permissions are documented as not granting database access. Database access is Postgres roles and
GRANTs. On the lakehouse side, Unity Catalog grants and governed views apply to the synced history. We observed
<fill from claims 15, 16, 19>."*

**[design recommendation]:** sync only curated / aggregate data into Lakebase; give developers a scrubbed
copy-on-write branch instead of production access; keep CDF history in a security-only schema and expose it through views.

**Limits:**
- Governance does **not** travel with the data: Postgres RLS doesn't apply to the CDF copy, and UC row filters/masks
  don't apply to synced tables (claim 17). Each copy needs its own controls.
- A branch copies production **data and roles**. The job scrubs contacts on `dev`, and records which persona roles
  exist there (`lakebase_dev_branch_roles`).
- Lakebase CDF is Public Preview; history tables can't carry masks/filters, and default-storage catalogs aren't
  supported as the destination.

## M10 · Genie agent: one question, different answers per user (RBAC vs ABAC on gold `contracts`), 7 min

Scenario: a business user asks the **Genie agent** "LNG Demo - Governed contract book (Genie)" about the gold book.
Every persona group has CAN_RUN on the space; Genie runs the SQL it generates **as the person asking**, so Unity Catalog,
not the agent, decides what comes back. Three control layers, one per demo question:

| Question | Control layer | Trader (pilot) | Trader (unentitled) | Developer / operator / security / auditor |
|---|---|---|---|---|
| How many contracts can I see per desk? | RBAC grant + desk row filter (identity-bound) | Atlantic + Portfolio | Pacific only | denied: no SELECT / USE SCHEMA |
| Show the price formula, slope and constant… | price masks bound to named columns, group check in the UDF | clear | masked | denied |
| List the counterparty contact email and phone… | **ABAC**: policy bound to tag `lngdemo_sensitivity=pii`, not to columns | masked | masked | denied |

Pre-reqs: setup run with `confirm_shared_changes=true` (creates the governed tag; otherwise the ABAC row reads
`NOT_TESTED`), job `genie_matrix` run, app redeployed with `--var genie_space_id=<id>`.

| Min | Do this | Visible result |
|---|---|---|
| 0-1 | Open the Genie space (URL printed by setup task `genie_space`) → **Configure › Data / Instructions** | One table, curated SQL for the three questions, instruction "never guess masked values" |
| 1-3 | App → **Genie agent** tab as trader, click the three questions | Two desks; prices in clear; contacts `***@example.com`. Expand "SQL generated by Genie": no masking logic in it, because UC applied it |
| 3-4 | Same tab, toggle **Ask as every persona** on the price question | Unentitled trader: Pacific rows, prices masked; the four others: denied, even though they can open the agent |
| 4-5 | Catalog Explorer → `trading_data` → **Policies**; `contracts` → contact columns → tags | `lngdemo_mask_contact_pii` (TO account users EXCEPT security) matches the tag; the price masks sit on the columns themselves |
| 5-6 | *Talk track:* tag one more column `lngdemo_sensitivity=pii` (e.g. `counterparty_alias`, any desk-owned table in the schema) and re-ask | Masked at once, with no pipeline or table change. That is the scaling argument for ABAC **[design recommendation]** |
| 6-7 | Matrix query below | 3 questions × 6 personas, PASS / FAIL / INCONCLUSIVE against the design, with Genie's SQL |

```sql
SELECT persona, question_key, layer, expected, observed, verdict, genie_status, left(genie_sql, 160) AS genie_sql
FROM lngdemo_gov.audit_curated.genie_persona_results
WHERE run_id = (SELECT max_by(run_id, run_ts) FROM lngdemo_gov.audit_curated.genie_persona_results)
ORDER BY persona, question_key;
```

*"Genie Agents are documented as evaluating data access with each end user's own Unity Catalog permissions; the
warehouse runs on the author's embedded credentials. We observed <fill from claims 22, 23>."*

**Limits:**
- Genie's SQL is model-generated. A question that doesn't select the protected column shows nothing about its mask
  (`INCONCLUSIVE`); the curated example SQL keeps the three demo questions stable.
- Only one distinct column mask can resolve per column and user: don't tag a column that already has a
  pipeline-declared mask (`price_formula`, `slope`, `constant_usd_mmbtu`), or queries fail for users that both cover.
- Security is exempt from every mask but has no USE SCHEMA on `trading_data`, so no persona sees contacts in clear.
  To show the exemption, add a human user to `lngdemo-security` **and** grant that group SELECT on `contracts`, or
  change `EXCEPT` in `src/sql/08_contact_pii_abac.sql`.
- Pipeline full refreshes can recreate gold `contracts`; setup re-applies the column tags on every run.

