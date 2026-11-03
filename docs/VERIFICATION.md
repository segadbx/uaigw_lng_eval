# Claim ledger

> ## ⚠ Flags for the customer deck (read first)
> No rehearsal claim has been **Contradicted** yet. The live tests haven't run, because the bundle is
> built but not deployed. The documentation review on **2026-10-28** found these corrections:
>
> 1. **No regex in service-policy functions.** The policy function reference lists LIKE / CONTAINS /
>    STARTSWITH-style functions only. If the deck says "a regex policy blocks prices", change it to
>    "a SQL pattern-match policy". Regex is available only in post-hoc SQL (the curated log copy).
> 2. **`system.ai_gateway.usage` has no policy-outcome columns.** Policy outcomes come from the trace
>    table's `policy_evaluated` events, the evaluator inference table, or our red-team results.
> 3. **The demo workspace is AWS (us-west-2), not Azure.** Use AWS doc links, and don't present Azure-only
>    features (Entra, Azure OpenAI terms) as tested here.
> 4. **Inference-table config and rate limits *are* settable through the API/bundle** (`config.inference_table`,
>    `config.rate_limits` in the bundle schema). The model-services page only says they are configured "from the UI".
> 5. **ai_query is a bypass path for guardrails:** against `system.ai` it gets usage tracking only. Rate limits,
>    guardrails and inference tables don't apply (query-model-services docs).
>
> 6. **Observed on FE-SANDBOX-AWS (2026-10-28, M1 run):** a principal with **no grant** on `trading_copilot` called
>    `system.ai.claude-haiku-5-5` directly and got HTTP 200. Default `system.ai` access is a bypass around the governed
>    service until it's locked down. The same caller got **HTTP 404 "does not exist"** (not 403) for `trading_copilot`,
>    because UC hides securables from unentitled callers.
> 7. **Observed:** inference tables need a catalog on **default storage**. On a metastore-root catalog the service create
>    failed with `Failed to initialize inference table storage`. Gateway calls also need the caller to have the
>    `workspace-access` (or SQL/consumer) entitlement, otherwise HTTP 403 before any UC check.
>
> 8. **Lakebase (M9, from docs 2026-11-02):** Lakebase CDF (Lakebase → Lakehouse) is **Public Preview** and needs a
>    workspace preview. Its destination **cannot be on default storage**, the opposite of inference tables, so the demo uses
>    two catalogs. UC row filters/masks **break CDF** if applied to the history tables. Postgres OAuth roles for
>    **groups only support workspace-level groups**. Don't present "one grant covers Postgres and the lakehouse": they're
>    separate enforcement points (project ACL, Postgres roles/RLS, UC).
>
> The live jobs update `<cat>.audit_curated.claim_ledger`. Copy any **Contradicted** row here and fix the deck before the session.

Status values: **Confirmed**, **Contradicted**, **Partially confirmed**, **Not testable here**,
**Not yet tested**. A documentation-only confirmation is labelled "(documentation)".
The talk track may only state rows marked **Confirmed** as fact.

| # | Claim | Source | Test performed | Observed result | Status | Date |
|---|---|---|---|---|---|---|
| 1 | Inference-table `request` holds the original caller text, not the SDD-redacted text | rehearsal inference (open question; inference-tables docs are silent) | `red_team` m2_redacted / m4_sdd_email_phone → `21_verify_claims` finds the request marker in `trading_copilot_payload` and checks for the raw email vs a placeholder | pending live run | Not yet tested | 2026-10-28 |
| 2 | An input-denied request leaves no row in the protected service's inference table | [create-service-policy](https://docs.databricks.com/aws/en/data-governance/unity-catalog/service-policies/create-service-policy) ("blocked input requests **may not** reach the protected service") | m2_blocked_input vs the m2_normal control marker in the payload table (judged only once the control row has landed) | pending live run | Not yet tested | 2026-10-28 |
| 3 | Blocked calls return HTTP 200 with a top-level `databricks_service_policy` object carrying the reason | [service policies](https://docs.databricks.com/aws/en/data-governance/unity-catalog/service-policies/) (documented; no JSON example published) | Every BLOCKED red-team row stores `http_status` + `policy_json`; `21_verify_claims` reports the keys | documented; payload shape not yet observed | Partially confirmed | 2026-10-28 |
| 4 | Service policies fail closed on evaluation errors, timeouts and missing fields | [service policies](https://docs.databricks.com/aws/en/data-governance/unity-catalog/service-policies/) ("any error during evaluation results in DENY") | `broken_policy_fail_closed` (missing-field access) on `trading_copilot_dev`, job `lngdemo-gov-05-fail-closed` | documented; live test pending | Partially confirmed | 2026-10-28 |
| 5 | Custom policies support only `LANGUAGE SQL` and can't rewrite content; only SDD redacts | [create-service-policy](https://docs.databricks.com/aws/en/data-governance/unity-catalog/service-policies/create-service-policy) Limitations + overview ("except `system.ai.detect_sensitive_data`") | Documentation re-check | Both pages state it (2026-10-28) | Confirmed (documentation) | 2026-10-28 |
| 6 | Attachment is UI-only, and a policy applies to all account users of the service | create-service-policy Limitations ("Applied to: All account users") | Docs + SDK 0.157.0 `w.ai_gateway` / CLI 1.19.0 `databricks ai-gateway` inspection | No policy attach/list method in SDK or CLI; docs state UI-only, all account users | Confirmed (documentation + SDK/CLI) | 2026-10-28 |
| 7 | Auto-TTL works on the auto-created inference table without breaking logging | not documented ([Auto-TTL](https://docs.databricks.com/aws/en/tables/operations/auto-ttl) covers managed Delta/Iceberg; [inference tables](https://docs.databricks.com/aws/en/ai-gateway/inference-tables) warn that altering the table can stop logging) | `20_harden_logs`: `ALTER TABLE … DELETE ROWS 7 DAYS AFTER event_time`, `SHOW TBLPROPERTIES`; `21_verify_claims` counts rows delivered afterwards | pending live run | Not yet tested | 2026-10-28 |
| 8 | ABAC masks/filters can be applied to inference and trace tables without breaking delivery | [unified trace table](https://docs.databricks.com/aws/en/ai-gateway/unified-trace-table) documents the trace-table row filter only | Column mask on the **dev** payload table (protects prod logging); trace row filter if `trace_table` is set | trace-table filter documented; inference table untested | Partially confirmed | 2026-10-28 |
| 9 | Grant and policy changes take a few minutes to propagate | create-service-policy ("a minute or two to propagate") | `30_persona_matrix` revoke/re-grant EXECUTE with 10 s polling; policy propagation timed manually (MANUAL_STEPS P2) | documented for policies; not yet measured | Partially confirmed | 2026-10-28 |
| 10 | Model Serving doesn't train on customer inputs/outputs (paid accounts); FMAPI may retain data up to 30 days for abuse detection | [Model Serving](https://docs.databricks.com/aws/en/machine-learning/model-serving/) / Foundation Model APIs data protection (link only) | Documentation only. Doesn't extend to Azure OpenAI (Microsoft's terms) | not tested by design | Not testable here | 2026-10-28 |
| 11 | "The gateway holds the credentials, so no app ever sees a key" | partly inference; bundle schema marks credential `plaintext` fields INPUT_ONLY | GET of an existing provider service on a different workspace returned `api_key_value: {}` (2026-10-28); re-check on FE-SANDBOX-AWS; `21_verify_claims` checks every visible provider service | No credential returned for the one service checked. Key rotation for direct provider keys stays a **customer action outside Databricks** | Partially confirmed | 2026-10-28 |
| 12 | "A regex for prices is fragile; layer it with a judge" | design opinion | M5: m5_price_in_words, m5_indirect_counterparty, judge repeat | Policy functions have **no regex at all** (LIKE/CONTAINS only); demonstrate rather than assert | Not yet tested | 2026-10-28 |
| 13 | "At scale, use ABAC GRANT policies on governed tags" | [GRANT policies](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/grant-policies): model services EXECUTE/MANAGE **GA**; SQL DDL needs classic DBR 18 LTS+; privileges don't show in `SHOW GRANTS` | Documentation check; not built | Capability exists for model services (GA); the recommendation is design advice | Confirmed (documentation) | 2026-10-28 |
| 14 | Retention, ownership and persona recommendations are design recommendations, not product behaviour | design recommendation | Labelled **[design recommendation]** in DEMO_RUNBOOK.md | n/a | Not testable here | 2026-10-28 |
| 15 | Lakebase project permissions (CAN_USE / CAN_MANAGE) don't grant database access | [manage-project-permissions](https://docs.databricks.com/aws/en/oltp/projects/manage-project-permissions) | `72_lakebase_matrix`: developer SP has CAN_USE but no Postgres role on production, and tries to log in | documented; live test pending | Partially confirmed | 2026-11-02 |
| 16 | Postgres RLS enforces desk entitlement for Databricks SPs logging in with OAuth | design; OAuth group roles support only **workspace** groups ([manage-roles](https://docs.databricks.com/aws/en/oltp/projects/manage-roles)), so SP roles map into NO_LOGIN group roles | `72_lakebase_matrix`: trader vs unentitled reads, insert into another desk | pending live run | Not yet tested | 2026-11-02 |
| 17 | UC row filters / column masks are **not** carried into Lakebase synced tables | skill reference ("FGAC not propagated"); not on the pages checked | `71_lakebase_sync`: synced table from the filtered/masked `contracts`, inspect Postgres, remove probe | pending live run | Not yet tested | 2026-11-02 |
| 18 | CDF history tables must not carry UC row filters / masks (CDF stops writing), so govern with views | [lakebase-cdf](https://docs.databricks.com/aws/en/oltp/projects/lakebase-cdf) | Documentation; demo uses `ops_curated` views instead | documented | Confirmed (documentation) | 2026-11-02 |
| 19 | Queries through the registered Lakebase UC catalog are governed by UC grants, independent of Postgres roles | [register-uc](https://docs.databricks.com/aws/en/oltp/projects/register-uc) (identity used not stated) | `72_lakebase_matrix`: auditor (UC SELECT, no PG role) vs security (both) | pending live run | Not yet tested | 2026-11-02 |
| 20 | Lakebase CDF can't write to default-storage catalogs while inference tables need default storage, so two catalogs are required | lakebase-cdf docs + inference-table failure observed 2026-10-28 | `71_lakebase_sync` CDF into the bundle-created `lngdemo_ops` (metastore root) | inference-table half observed | Partially confirmed | 2026-11-02 |
| 21 | On a pipeline-managed gold table, row filters / column masks are declared in the pipeline and enforced for every reader | skill reference (pipeline datasets: `WITH ROW FILTER`, `MASK`, Public Preview) | `01_setup_governance` inspects `DESCRIBE TABLE EXTENDED` on gold `contracts`; `persona_matrix` trader vs unentitled reads | pending live run | Not yet tested | 2026-11-02 |
| 22 | A Genie agent answers with the asker's UC controls: same question, different rows / masks per persona | [Genie Agents set-up](https://docs.databricks.com/aws/en/genie-agents/set-up): "Data access is always evaluated using each end user's own Unity Catalog permissions"; warehouse uses the author's embedded credentials | `31_genie_persona_matrix`: 3 questions × 6 persona SPs through the Conversation API | documented; Genie payload + Conversation API helpers exercised on a scratch table 2026-11-02 (single user) | Partially confirmed | 2026-11-02 |
| 23 | A tag-bound ABAC column mask applies to a pipeline-managed gold materialized view, for the reader at query time | [ABAC requirements](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/requirements) ("ABAC policies on materialized views and streaming tables") | `06_contact_pii_abac` (column tags + schema policy), `31_genie_persona_matrix` contact question | documented; whether `SET TAGS` is accepted on the pipeline MV is logged in `experiment_log` (`contact_pii_abac`) | Partially confirmed | 2026-11-02 |

## How to refresh this table

```sql
SELECT claim_id, status, observed_result, tested_at
FROM lngdemo_gov.audit_curated.claim_ledger ORDER BY claim_id;
```
