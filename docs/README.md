# LNG demo AI governance demo bundle (Unity Gateway + Unity Catalog)

A Declarative Automation Bundle that lets a Solutions Architect show, one capability at a time,
how Unity Catalog and Unity Gateway govern **customer-specific contract pricing** in an LLM-powered
LNG trading-desk copilot, and how the same identities and evidence model govern the **OLTP store** (Lakebase, M9). All data is synthetic (Counterparty A-H, invented desks and formulas).

| Doc | Purpose |
|---|---|
| [CAPABILITY_MATRIX.md](CAPABILITY_MATRIX.md) | What is GA/Beta, how each piece is deployed, which module uses it |
| [MANUAL_STEPS.md](MANUAL_STEPS.md) | UI-only steps (service policies, previews, trace table) with exact settings and check queries |
| [DEMO_RUNBOOK.md](DEMO_RUNBOOK.md) | Minute-by-minute script per module (M0-M10) |
| [VERIFICATION.md](VERIFICATION.md) | Claim ledger: 23 claims (14 rehearsal + 6 Lakebase + 1 pipeline + 2 Genie/ABAC), how each is tested, status |

## Assumptions (inputs not yet confirmed by the SA)

These defaults were chosen because the inputs weren't provided. Override them with `--var` or in `databricks.yml`.

| Input | Default used | Notes |
|---|---|---|
| Workspace / profile | `FE-SANDBOX-AWS` → `https://dbc-206b78ef-29e7.cloud.databricks.com` | **AWS us-west-2 (metastore `oregon-metastore`), not Azure.** Doc links point to `docs.databricks.com/aws/...` |
| Catalog | `lngdemo_gov` | Must be on **Default storage** (create it in the UI, see step 1). A catalog created with `databricks catalogs create` landed on the metastore root, and inference tables failed with `Failed to initialize inference table storage` (observed 2026-10-28) |
| Persona groups | `lngdemo-trader-pilot`, `lngdemo-trader-unentitled`, `lngdemo-developers`, `lngdemo-operators`, `lngdemo-security`, `lngdemo-auditors` | **Must exist as account groups** (account admin). Not created by the bundle |
| App / caller SP | `lngdemo-sp-app`, resolved by display name with a bundle `lookup` | Granted EXECUTE on `trading_copilot`. All persona SPs: [IDENTITIES.md](IDENTITIES.md) |
| Chat model | `databricks-claude-sonnet-5-5` (prod), `databricks-claude-haiku-5-5` (dev) | Seen in `serving-endpoints list` on FE-SANDBOX-AWS, 2026-10-28 |
| Judge model | `databricks-claude-haiku-5-5` | Behind the `trading_judge` evaluator service |
| Previews | Unknown | Service policies need **Unity Gateway Service Policies** (workspace) or **Enhanced Unity Gateway** (account). The trace table needs Enhanced Unity Gateway + a metastore admin |
| Retention | 7 days | `retention_days` |
| Copilot app | Included | Delete `resources/apps.yml` to leave it out |
| Warehouse | `Serverless Starter Warehouse`, resolved by name with a bundle `lookup` | `warehouse_id` |
| Azure OpenAI provider | Off | Commented out in `resources/model_provider_services.yml` |
| Lakebase (M9) | project `lngdemo-trading-ops` (PG 17, 0.5–2 CU, scale-to-zero 5 min); ops catalog `lngdemo_ops`; registered catalog `lngdemo_trading_ops` | `lakebase_*` / `ops_catalog` variables. Lakebase CDF needs the workspace preview (MANUAL_STEPS L1) |
| Shared changes | `confirm_shared_changes=false` | Gates system.ai grants, governed tags, warehouse ACLs and the trace table. Without it the M10 ABAC contact mask is skipped (governed tag `lngdemo_sensitivity` not created) |

## Prerequisites

1. Databricks CLI **≥ 1.17.0** (checked: 1.19.0). `model_services` in bundles is Beta and needs the
   **direct** deployment engine; `databricks.yml` sets `bundle.engine: direct` (the CLI default).
2. The six account groups above exist and contain the right members / persona SPs.
3. Unity Gateway previews enabled (see [MANUAL_STEPS.md](MANUAL_STEPS.md#a-previews-and-admin-settings)).
4. **M9:** Lakebase available in the workspace; **Lakebase Change Data Feed** preview enabled (MANUAL_STEPS L1);
   a metastore root storage location for the bundle-created `lngdemo_ops` catalog (L2); `CREATE CATALOG` for the deployer.
5. The deployer has `CREATE SCHEMA` on the catalog, and `USE CATALOG` / `USE SCHEMA` / `EXECUTE` on
   `system.ai` and the chosen models. Through definer's privileges the **owner's** access to `system.ai` is what callers use.

## Deploy and run order

```bash
P=FE-SANDBOX-AWS
# 1. catalog (once): Catalog Explorer -> Create catalog -> name lngdemo_gov -> Storage: "Default storage".
#    Do NOT use `databricks catalogs create`: on this metastore it puts the catalog on the metastore root,
#    and inference tables then fail with "Failed to initialize inference table storage".
#    An existing default-storage catalog also works: --var catalog=<name>

# 2. identities (once): persona SPs, then account groups (account admin), then deploy
scripts/create_persona_sps.sh $P
scripts/create_persona_groups.sh <account-profile> $P
databricks bundle validate --strict --profile $P   # SP and warehouse IDs are resolved by lookup
databricks bundle deploy -t dev --profile $P

# 3. persona credentials (see below), then setup (idempotent - safe to re-run).
#    setup = raw extract files -> governance UDFs -> medallion pipeline -> gold grants -> policy functions -> ...
scripts/create_persona_oauth_secrets.sh $P
databricks bundle run setup -t dev --profile $P

# 4. UI steps P1-P3 (+P6 optional) in MANUAL_STEPS.md

# 5. demo traffic, then wait >= 60 min for inference-table delivery
databricks bundle run red_team -t dev --profile $P

# 6. log hardening + claim checks, persona matrix, acceptance tests, evidence
databricks bundle run open_questions   -t dev --profile $P
databricks bundle run persona_matrix   -t dev --profile $P
databricks bundle run acceptance_tests -t dev --profile $P
databricks bundle run evidence_pack    -t dev --profile $P

# 7. fail-closed demo: attach P5 to trading_copilot_dev, run, detach
databricks bundle run fail_closed_check -t dev --profile $P

# 8. M9 Lakebase: OLTP schema/RLS/roles/dev branch, synced table + FGAC probe + CDF; then views + matrix
databricks bundle run lakebase        -t dev --profile $P
databricks bundle run lakebase_matrix -t dev --profile $P   # re-run once CDF history has landed

# 9. app (if included)
databricks bundle run trading_copilot_app -t dev --profile $P

# 10. M10 Genie agent: setup already created the space (task genie_space) and printed its id.
#     Ask the same questions as every persona, then give the app the space id for its "Genie agent" tab.
databricks bundle run genie_matrix -t dev --profile $P
databricks bundle deploy -t dev --var genie_space_id=<id printed by setup> --profile $P
```

Pass shared-change confirmation explicitly when you want it, for example:
`databricks bundle run setup --params confirm_shared_changes=true`.

### Persona credentials

M6/M8 call the gateway and SQL as **service principals placed in the persona groups** (human user
tokens are not used in jobs). For each persona `trader, unentitled_trader, developer, operator,
security, auditor, app`, create an OAuth secret for its SP and store it in the bundle's scope:

```bash
databricks secrets put-secret lngdemo_gov trader_client_id     --string-value <application-id> --profile $P
databricks secrets put-secret lngdemo_gov trader_client_secret --profile $P   # prompts for the value
```

`scripts/create_persona_oauth_secrets.sh <workspace-profile>` creates and stores them for all personas (`scripts/put_persona_secrets.sh <scope> <profile>` if you already have secrets). Missing credentials make the affected tests
report `NOT_TESTED`. The jobs never silently fall back to another identity.

### Ownership

Model-service calls are authorised with the **owner's** privileges (definer's privileges). The
owner is whoever creates the service, so deploy the `demo` target **authenticated as the owner SP**
(for example an OAuth M2M profile for that SP). `04_capability_probe` records `effective_owner` for
each service. Inference tables are owned by the service creator too.

## What gets deployed

| Resource | Name |
|---|---|
| Schemas | `services`, `audit_raw`, `audit_curated`, `trading_data` (gold), `raw`, `bronze`, `silver` (+ volumes `audit_curated.evidence`, `raw.landing`) |
| Pipeline | `lngdemo-medallion`: Lakeflow SDP, serverless, triggered by job `setup` (raw → bronze → silver → gold) |
| Model services | `services.trading_copilot`, `services.trading_copilot_dev`, `services.trading_judge` (each with an inference table in `audit_raw` and rate limits) |
| Secret scope | `lngdemo_gov` (empty; values added manually) |
| Jobs | `lngdemo-gov-01-setup`, `-02-red-team`, `-03-open-questions`, `-04-persona-matrix`, `-05-fail-closed`, `-06-acceptance-tests`, `-07-evidence-pack`, `-08-lakebase`, `-09-lakebase-matrix`, `-10-genie-matrix`, `-99-teardown` |
| Lakebase (M9) | project `lngdemo-trading-ops` with project ACLs; Postgres roles (4 NO_LOGIN group roles + 4 persona SP OAuth roles); registered UC catalog `lngdemo_trading_ops`; catalog `lngdemo_ops` with schemas `lakebase_cdc`, `ops_curated`, `pipeline_meta` |
| Dashboard | "LNG Demo AI Governance - Gateway evidence" |
| App | `lngdemo-trading-copilot` (optional) |

Created by the pipeline: bronze/silver streaming tables and materialized views, and gold `trading_data.contracts`,
`desk_book_summary`, `desk_exposure`. Objects created by jobs (all inside bundle schemas): raw extract files in `raw.landing`, `desk_entitlements`,
row-filter/mask UDFs, `services.desk_price_summary` + policy functions,
`audit_curated.*` result tables and views, column masks / Auto-TTL on payload tables,
M9 jobs also create the Postgres
`trade_ops` / `reference` schemas, the `dev` branch with its endpoint and developer role, the synced table, the CDF
config, and governed views in `lngdemo_ops.ops_curated`. M10 (setup tasks `contact_pii_abac`, `genie_space`): `lngdemo_sensitivity=pii`
tags on `contracts.contact_email` / `contact_phone`, UDF `trading_data.mask_contact_pii`, ABAC policy
`lngdemo_mask_contact_pii` on schema `trading_data`, and the Genie space "LNG Demo - Governed contract book (Genie)"
(id in `audit_curated.setup_state`; job-created because Genie rejects a space whose table doesn't exist yet).

## Tagging

Every tag the demo writes uses the prefix **`lngdemo_`**, whatever the tag type: job tags, UC tags,
schema properties, request tags or MLflow tags.

| Key | Value | Where |
|---|---|---|
| `lngdemo_project` | `lngdemo-ai-governance` | everything |
| `lngdemo_env` | bundle target (`dev` / `demo`) | jobs, schemas, UC objects |
| `lngdemo_owner` | deploying / running identity | jobs, UC objects |
| `lngdemo_data_class` | `synthetic`, `sensitive_payload` (raw logs), `curated` | jobs, schemas, UC objects |
| `lngdemo_managed_by` | `dab`, `setup_job`, `gateway_auto` | jobs, schemas, UC objects |
| `lngdemo_component` | e.g. `job-setup`, `schema-audit-raw`, `table-contracts`, `app-trading-copilot` | per asset |
| `lngdemo_module` | `M1`…`M8` | jobs |
| `lngdemo_desk`, `lngdemo_use_case`, `lngdemo_test_id`, `lngdemo_run_id`, `lngdemo_client` | per request | gateway request tags + MLflow trace tags |
| `lngdemo_trace_service` | `''` | trace-table `service_name` column (optional) |

How each asset gets its tags:

| Asset | Mechanism |
|---|---|
| Jobs (8) | `presets.tags` in `databricks.yml` + per-job `tags` (native bundle field) |
| Schemas (4) | `properties` (native bundle field) **and** UC tags via `05_apply_tags.py` |
| Volume, tables, views, functions | UC `SET TAGS` via `05_apply_tags.py` (runs at the end of `setup`, and again in `open_questions` after inference tables exist). Function tagging needs DBR 18.1+, so failures are logged in `audit_curated.experiment_log` |
| Inference (payload) tables | UC tags via `05_apply_tags.py` (`lngdemo_managed_by = gateway_auto`) |
| Gateway requests | `lngdemo.chat()` and the app prefix every request tag and always add `lngdemo_project` |
| MLflow experiment / traces (app) | `mlflow.set_experiment_tags` / `mlflow.update_current_trace(tags=...)` |
| Model services, app, dashboard, secret scope | **No tag field in the bundle schema** and no documented tagging API → named with the `lngdemo` prefix, labelled in the description, see [MANUAL_STEPS G](MANUAL_STEPS.md#g-tagging-gaps) |

Check:
```sql
SELECT schema_name, tag_name, tag_value FROM lngdemo_gov.information_schema.schema_tags WHERE tag_name LIKE 'lngdemo\_%';
SELECT table_schema, table_name, tag_name, tag_value FROM lngdemo_gov.information_schema.table_tags WHERE tag_name LIKE 'lngdemo\_%';
SELECT target, ok, detail FROM lngdemo_gov.audit_curated.experiment_log WHERE experiment = 'apply_tags' AND NOT ok;
```

## Teardown

```bash
databricks bundle run teardown -t dev --profile $P     # drops job-created objects, prints manual cleanup
# do the printed manual steps (detach service policies first!)
databricks bundle destroy -t dev --profile $P
```

`scripts/teardown.sh` runs both. Teardown reverts shared changes (system.ai grant, governed tag)
only with `confirm_shared_changes=true`, and otherwise prints them.

## Cost drivers

* **Model tokens:** about 20 red-team calls + 10 judge repeats + about 15 matrix/acceptance calls per run, with
  `max_tokens` ≤ 400. With the LLM judge attached, **every** prod request also makes one evaluator call.
* **Serverless jobs:** each notebook task spends about 1-2 min on `%pip install`. The full run is about 9 tasks.
* **SQL warehouse:** dashboard, app tool queries and the persona matrix (serverless, auto-stop).
* **App compute:** the app runs until stopped (`databricks apps stop lngdemo-trading-copilot`).
* **Logging:** inference tables (Zerobus delivery) and Delta storage are negligible at demo volume.
  The optional oversized test (`m2_oversized`, disabled) sends about 11 MiB.
* **Lakebase (M9):** two endpoints (production 0.5–2 CU, dev 0.5–1 CU) with scale-to-zero after 5 minutes; one SNAPSHOT
  synced-table pipeline run (plus the short-lived FGAC probe); Lakebase CDF streaming at demo volume; the dev branch
  expires after 14 days.
* **Medallion pipeline:** serverless SDP updates (triggered from `setup`; a few minutes of compute per run at demo volume).
