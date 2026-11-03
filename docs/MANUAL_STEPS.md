# Manual (UI-only) steps

Each step says **why it isn't automated**, gives the exact settings, and a **check** that confirms it
took effect. Documentation was checked on 2026-10-28 against `docs.databricks.com/aws/...`.
Replace `<cat>` with the bundle catalog (default `lngdemo_gov`).

> Service policies take **a minute or two to propagate** (documented). Wait before running checks.
> Every service policy applies to **all account users** of the service (documented; not configurable in the Beta).

## A. Previews and admin settings

| # | Step | Who | Why manual |
|---|---|---|---|
| A0 | Workspace **Previews** → enable **Unity Gateway Service Policies** | Workspace admin | Preview toggle |
| A0b | Account console **Previews** → enable **Enhanced Unity Gateway** (needed for the unified trace table; also enables service policies when the compliance security profile is off) | Account admin | Account preview |
| A1 | Not needed any more: the app runs as persona SPs (secret resources in `resources/apps.yml`), not on behalf of the user | - | - |
| A2 | Grant `system.ai_gateway.usage` and `system.access.audit` to the security and auditor groups (by default only account + metastore admins can read them) | Account/metastore admin | System-table grants are outside the bundle |
| A3 | Create the six persona account groups and add one SP per persona (`scripts/create_persona_groups.sh <account-profile> <workspace-profile>`), then assign the groups to the workspace (secret-scope ACLs need workspace-assigned principals) | Account admin | Account-level identities |
| A4 | Create catalog `lngdemo_gov` in Catalog Explorer with **Storage: Default storage** | Metastore admin / CREATE CATALOG | No CLI/API field selects default storage. A catalog on the metastore root made inference tables fail with `Failed to initialize inference table storage` (observed 2026-10-28) |

**Check A0:** AI Gateway → Models → `<cat>.services.trading_copilot` shows a **Policies** tab.
**Check A2:** as a security-group member, run `SELECT count(*) FROM system.ai_gateway.usage`.

## T. Unified trace table (optional, Beta)

Why manual: setup is UI-only and needs a metastore admin.

1. AI Gateway → **Govern** → **Traces** → **Set up tracing**.
2. Catalog/schema: a dedicated one is recommended because the creator owns the table. For this demo use `<cat>.audit_raw`.
3. **Save.** Then set the bundle variable `trace_table` to the table's full name (docs example: `<catalog>.<schema>.unity_gateway_otel_spans`)
   and re-run `open_questions` with `confirm_shared_changes=true` to apply the documented row filter and Auto-TTL.

**Check:**
```sql
SELECT name, service_name, time FROM <trace_table>
WHERE time >= current_timestamp() - INTERVAL 1 HOUR AND service_name LIKE '<cat>.services.%' LIMIT 20;
```

## P. Service policies (Beta, UI-only attachment)

Why manual: the docs state attachment is UI-only during the Beta. SDK 0.157.0 (`w.ai_gateway`) and
CLI 1.19.0 (`databricks ai-gateway`) have no policy attach/list methods (inspected 2026-10-28).
Required privilege: `MANAGE` on the target service (the bundle grants it to the security group). Custom
SQL policies also need `EXECUTE` on the function, and judge policies need `EXECUTE` on the evaluator service (both granted by the bundle/setup).

Navigation for all P-steps: **AI Gateway** (sidebar) → **Models** → select the service → **Policies** → **New policy**.

### P1 · Sensitive Data Detection, redact (trading_copilot)

| Field | Value |
|---|---|
| Name | `lngdemo-sdd-redact-contact` |
| Guardrail type | Sensitive Data Detection |
| Classification tags | `class.email_address`, `class.phone_number` (optionally also `class.credit_card`, `class.iban_code`) |
| Action | **Redact** |
| Phase | **Input** and **Output** |
| Rank | `10` (runs first on input, so later policies see redacted text) |
| Mode | Enforce (SDD supports enforce only) |

Redaction replaces each match with a bracketed category token (the documented example is `[US_SSN]`).
SDD does **not** detect names, organisations or prices (documented limitation), which M5 shows.

**Check:** `databricks bundle run red_team --params module_filter=M4`. Then:
```sql
SELECT test_id, outcome, verdict, content_preview FROM <cat>.audit_curated.red_team_results
WHERE test_id IN ('m4_sdd_email_phone','m2_redacted') ORDER BY run_ts DESC LIMIT 4;
```
Expected: `verdict = PASS`, and the reply shows placeholders instead of `@example.com` / `555`.

**P1-dev (M5 only):** attach the same SDD policy to `trading_copilot_dev` (no other policies) to run
`m5_sdd_ignores_alias`.

### P2 · Custom SQL deny: contract terms (trading_copilot)

| Field | Value |
|---|---|
| Name | `lngdemo-deny-contract-terms` |
| Guardrail type | Custom |
| Implementation | **Custom function** → `<cat>.services.deny_contract_terms` |
| Phase | *(none: custom SQL functions run on input and output and branch on `event:type`)* |
| Rank | `20` |
| Mode | Enforce |

If attaching fails with **"SQL cannot be transpiled to CEL"**, attach `<cat>.services.deny_contract_terms_minimal`
instead (request phase only, letters-only patterns, same shape as the documented `block_codenames` example), and record
which one attached in VERIFICATION.md. The first version (COALESCE + LIKE + IN + COMMENT) was rejected on 2026-10-28.

**Check:**
```sql
SELECT test_id, http_status, outcome, policy_json FROM <cat>.audit_curated.red_team_results
WHERE test_id IN ('m2_blocked_input','m4_sql_deny_alias','m4_sql_deny_formula') ORDER BY run_ts DESC LIMIT 6;
```
Expected: `http_status = 200`, `outcome = BLOCKED_BY_POLICY`, and `policy_json` contains the reason.
Note the time from attach to the first block in VERIFICATION.md (claim 9).

### P3 · LLM-as-a-judge, Log mode (trading_copilot)

| Field | Value |
|---|---|
| Name | `lngdemo-judge-counterparty-terms` |
| Guardrail type | Custom |
| Implementation | **LLM-as-a-judge** |
| Phase | **Input** |
| Rank | `30` |
| Evaluator model service | `<cat>.services.trading_judge` |
| Prompt | the *Prompt* section of `src/policies/judge_counterparty_terms.md` (no ALLOW/DENY or output format) |
| Conversation window | `1` |
| Mode | **Log** |

**Check:** after running `red_team`, wait up to 60 min:
```sql
SELECT event_time, get_json_object(response, '$.choices[0].message.content') AS verdict
FROM <cat>.audit_raw.trading_judge_payload ORDER BY event_time DESC LIMIT 10;
```
With the trace table enabled, look for `policy_evaluated` events with `policy.dry_run_action` /
`policy.dry_run_reason` (the field layout isn't published in a schema we could verify, so search as text):
```sql
SELECT trace_id, name, time FROM <trace_table>
WHERE time >= current_timestamp() - INTERVAL 2 HOURS AND to_json(struct(*)) ILIKE '%dry_run%' LIMIT 20;
```

### P5 · Broken policy for the fail-closed demo (trading_copilot_dev ONLY)

| Field | Value |
|---|---|
| Name | `lngdemo-broken-fail-closed` |
| Guardrail type | Custom |
| Implementation | Custom function → `<cat>.services.broken_policy_fail_closed` |
| Rank | `10` |
| Mode | **Enforce** |

1. Attach it. Wait 1-2 min.
2. `databricks bundle run fail_closed_check`.
3. **Detach it** (Policies tab → policy → Delete/Detach).

If the UI rejects the function at attach time, record that in VERIFICATION.md (claim 4): it means
validation caught it before runtime, which is a different behaviour from fail-closed at runtime.

**Check:** `SELECT outcome, http_status, policy_json, error FROM <cat>.audit_curated.red_team_results WHERE test_id='m7_fail_closed' ORDER BY run_ts DESC LIMIT 1;`

### P6 · Require desk tag, Log mode (optional, trading_copilot)

| Field | Value |
|---|---|
| Name | `lngdemo-require-desk-tag` |
| Guardrail type | Custom |
| Implementation | Custom function → `<cat>.services.require_desk_tag` |
| Rank | `40` |
| Mode | **Log** |

The function checks the `lngdemo_desk` request tag. `lngdemo.chat()` and the app send it as `lngdemo_desk`. Request tags are caller-supplied: they help attribution, but they **don't establish identity** (documented).

## I. Inference tables

Not manual. Inference tables are configured by the bundle (`config.inference_table` in
`resources/model_services.yml`), and the API returns that config (`04_capability_probe`). The table is
auto-created after the first request and rows can take **up to an hour** to appear.

**Check:** `SHOW TABLES IN <cat>.audit_raw` lists `trading_copilot_payload`, `trading_copilot_dev_payload`, `trading_judge_payload`.

## L. Lakebase (M9)

| # | Step | Who | Why manual | Check |
|---|---|---|---|---|
| L1 | Workspace **Previews** → enable **Lakebase Change Data Feed** (Lakebase → Lakehouse sync) | Workspace admin | Public Preview toggle | `SELECT target, ok, left(detail,200) FROM lngdemo_gov.audit_curated.experiment_log WHERE experiment = 'lakebase_cdf' ORDER BY run_ts DESC LIMIT 1` shows `ok = true` |
| L2 | Confirm the metastore has a **root storage location** (the bundle creates `lngdemo_ops` without `storage_root`, so it lands on the metastore root; Lakebase CDF doesn't support default-storage catalogs). If there's no metastore root, set `storage_root` on `resources.catalogs.ops` to an external location | Metastore admin | Storage layout is environment-specific | `DESCRIBE CATALOG EXTENDED lngdemo_ops` shows a non-default storage root |
| L3 | If `71_lakebase_sync` failed half-way, remove the probe: `databricks postgres delete-synced-table synced_tables/lngdemo_trading_ops.reference.fgac_probe_contracts`, then `DROP TABLE IF EXISTS reference.fgac_probe_contracts` in Postgres | Project owner | Cleanup after a failed experiment | `databricks postgres get-synced-table …` returns not found |
| L4 | (Demo) Lakebase App → project → **Settings › Project permissions** and **Roles & Databases**, to show the two layers side by side | Presenter | UI walkthrough | — |

Developer access: the job creates the `dev` branch **after** seeding production (a branch is a copy-on-write snapshot
taken at creation), scrubs contacts there, and creates the developer's OAuth role only on that branch. Re-run job
`lakebase` to recreate the branch after the 14-day TTL.

## G. Tagging gaps

All tag keys use the `lngdemo_` prefix (docs/README.md#tagging). These assets can't be tagged by the bundle:

| # | Asset | Why | What to do |
|---|---|---|---|
| G1 | Model services `trading_copilot`, `trading_copilot_dev`, `trading_judge` | No `tags` field in the bundle schema. The entity-tag API documents catalogs/schemas/tables/columns/volumes only, and SQL management of model services isn't supported | If Catalog Explorer shows **Tags** on the service page, add `lngdemo_project = lngdemo-ai-governance`, `lngdemo_env = <target>`, `lngdemo_component = model-service-<name>`. They should then appear in `system.ai_gateway.usage.service_tags`. Otherwise rely on request tags (always sent) |
| G2 | App `lngdemo-trading-copilot` | No tag field. Only `budget_policy_id` / `usage_policy_id` | For cost attribution, have an account admin create a serverless usage policy with tag `lngdemo_project = lngdemo-ai-governance`, and set `usage_policy_id` on the app (and jobs) in the bundle |
| G3 | Dashboard | No tag field | Identified by its display name prefix "LNG Demo AI Governance" |
| G4 | Secret scope `lngdemo_gov` | Secret scopes don't support tags | Identified by name |

**Check G1:**
```sql
SELECT DISTINCT service_name, service_tags FROM system.ai_gateway.usage
WHERE service_name LIKE '%.services.trading_%' AND event_time >= current_timestamp() - INTERVAL 1 DAY;
```

## Cleanup (manual part of teardown)

1. Detach all P-policies from both services.
2. Disable tracing (Govern → Traces) if it was enabled only for this demo.
3. Revoke persona SP OAuth secrets that were created only for the demo.
4. Run `databricks bundle run teardown`, then `databricks bundle destroy`.
