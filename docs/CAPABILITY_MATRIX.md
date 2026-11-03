# Capability matrix

Checked on 2026-10-28 against CLI 1.19.0 (`databricks bundle schema`, `databricks ai-gateway --help`),
SDK 0.157.0 and `docs.databricks.com/aws`. Workspace: `FE-SANDBOX-AWS` (AWS us-west-2). Schema/SDK/doc checks are workspace-independent; live values (warehouse, models) re-checked on FE-SANDBOX-AWS 2026-10-28.
`04_capability_probe` re-checks the live workspace and writes `audit_curated.capability_matrix`.

| Capability | Status (docs) | Deploy method | Evidence of programmability | Module |
|---|---|---|---|---|
| Model services (`trading_copilot`, `_dev`, `trading_judge`) | Beta in bundles; direct engine | **Bundle** `resources.model_services` | Schema keys: `parent, model_service_id, comment, config, grants, lifecycle` | M1 |
| Routing to pay-per-token model | — | Bundle `config.routing.destinations[].pay_per_token_config.model = models/system.ai.<model>` | docs bundle example + existing services | M1 |
| EXECUTE / MANAGE grants on services | — | Bundle `grants` | docs bundle example (`privileges: [EXECUTE]`) | M1, M6 |
| Inference table config | UI per model-services page | **Bundle** `config.inference_table {parent, table_name_prefix}` | Schema + existing services return it on GET | M2 |
| Rate limits (QPM/TPM, user default, service) | UI per docs | **Bundle** `config.rate_limits[]` | Schema enums `RATE_LIMIT_KEY_*`, `RATE_LIMIT_RENEWAL_PERIOD_*` | M2 |
| Model provider services (Azure OpenAI) | Beta in bundles | Bundle (commented out, optional) | Schema `resources.ModelProviderService`; credentials INPUT_ONLY | M1/M5 |
| Service policies: SDD, custom SQL, LLM judge | Beta; preview toggle | **Manual** (MANUAL_STEPS P1-P6) | No attach/list API in SDK/CLI | M4, M5, M7 |
| Policy SQL functions | Beta | Setup job (SQL) | `CREATE FUNCTION … (event VARIANT) RETURNS VARIANT LANGUAGE SQL` | M4, M7 |
| Request tags | — | Caller header `Databricks-Ai-Gateway-Request-Tags: {"lngdemo_project":"lngdemo-ai-governance","lngdemo_desk":"atlantic"}` | docs example | M3, M8 |
| Unified trace table | Beta; Enhanced Unity Gateway preview; metastore admin | **Manual** setup (MANUAL_STEPS T) + job for filter/TTL (gated) | UI-only setup in docs | M3, M4 |
| `system.ai_gateway.usage` | GA system table; admin grant | Dashboard + SQL | docs column list (no policy columns) | M3, M8 |
| `system.access.audit` | GA | Evidence-pack SQL | — | M7 |
| Row filter + column masks on gold `contracts` | **Public Preview** (row filters / masks on pipeline datasets) | **Declared in the pipeline** (`gold/contracts.sql`: `WITH ROW FILTER`, `MASK`); UDFs created first by `01a_governance_prereqs` | Pipeline-managed tables can't take `ALTER TABLE SET ROW FILTER / SET MASK` afterwards | M4 |
| ABAC GRANT policies for model services | GA (EXECUTE/MANAGE) | Not built (claim 13) | docs | — |
| ABAC column mask (governed tag `lngdemo_sensitivity=pii`) on gold `contracts` | GA (ABAC); policies apply to pipeline MVs per the ABAC requirements page | Setup job (`06_contact_pii_abac`: governed tag (gated), column `SET TAGS`, `src/sql/08_contact_pii_abac.sql`) | `CREATE POLICY … ON SCHEMA … COLUMN MASK … MATCH COLUMNS has_tag_value(…)` | M10 |
| Genie agent (space) over gold `contracts` | GA (Genie Agents) | **Job** (`07_genie_space`: SDK `create_space` / `update_space`, ACL `CAN_RUN` per persona group). Bundle `genie_spaces` exists (CLI 1.19), but create fails while the table is missing | "Failed to fetch tables for the agent" on create with a missing table (2026-11-02) | M10 |
| Genie Conversation API as persona SPs | GA | `31_genie_persona_matrix`, app tab "Genie agent" (`start_conversation_and_wait`, `get_message_attachment_query_result`) | SDK 0.159 `w.genie` | M10 |
| Auto-TTL | GA for managed Delta; **undocumented on inference tables** | Job experiment (`20_harden_logs`) | `ALTER TABLE … DELETE ROWS n DAYS AFTER col` | M3 |
| Column mask on inference tables | Undocumented | Job experiment on the dev payload table only | — | M3 |
| Redacted curated copy / operator view | Standard SQL | Job (`20_harden_logs`) | — | M3 |
| Aggregate-only UC tool function | GA | Setup job (SQL) | — | M4, M8 |
| Databricks App with user authorization | Apps GA; user-auth scopes `ai-gateway`, `sql` | **Bundle** `resources.apps` (`user_api_scopes`) + possibly admin enablement | Schema `user_api_scopes`; apps-auth docs list scopes | M8 |
| MLflow client-side trace redaction | MLflow ≥ 3.14 | App code `mlflow.tracing.configure(span_processors=[...])` | govern-redact docs | M4, M8 |
| Evidence volume | GA | Bundle `resources.volumes` | — | M7 |
| Secret scope for persona credentials | GA | Bundle `resources.secret_scopes` (values manual) | — | M6, M8 |
| Dashboard | GA | Bundle `resources.dashboards` | — | M3 |
| `lngdemo_*` tags on jobs | GA | Bundle `presets.tags` + job `tags` | Schema `presets.tags`, `resources.Job.tags` | all |
| `lngdemo_*` labels/tags on schemas, volume, tables, views, functions | GA (function tags need DBR 18.1+) | Bundle schema `properties` + setup job (`05_apply_tags.py`, `SET TAGS`) | Tags docs | all |
| Tags on model services / app / dashboard / secret scope | No bundle field; no documented API | Manual or naming only (MANUAL_STEPS G) | Schema inspection 2026-10-28 | — |
| Lakebase Autoscaling project (PG 17), project ACL, custom tags | GA (Lakebase); bundle fields Beta | **Bundle** `postgres_projects` (`permissions`, `custom_tags`, `default_endpoint_settings`) | Schema `resources.PostgresProject` | M9 |
| Postgres OAuth roles for persona SPs + NO_LOGIN group roles | Beta (bundle) | **Bundle** `postgres_roles` (`identity_type: SERVICE_PRINCIPAL`, `auth_method`); membership via SQL GRANT | `membership_roles` only accepts `DATABRICKS_SUPERUSER`; OAuth group roles only for workspace groups | M9 |
| Postgres GRANTs, column privileges, row-level security | Postgres | Job `lakebase` (`src/lakebase/*.sql`) | — | M9 |
| Developer branch (copy-on-write) + endpoint + branch-only role | GA | Job `lakebase` (SDK `create_branch` / `create_endpoint` / `create_role`), not bundle: must be created after seeding | SDK 0.159 `w.postgres` | M9 |
| Register Lakebase DB in Unity Catalog | GA; serverless only | **Bundle** `postgres_catalogs`; UC grants by job | register-uc docs | M9 |
| Synced table Lakehouse → Lakebase | GA | Job `lakebase` (SDK `create_synced_table`); bundle `postgres_synced_tables` exists, but the source table is created after deploy | Schema `resources.PostgresSyncedTable` | M9 |
| Lakebase CDF Lakebase → Lakehouse (`lb_<table>_history`) | **Public Preview**; workspace preview toggle | Job `lakebase` (SDK `create_cdf_config`); no bundle resource | lakebase-cdf docs | M9 |
| Ops catalog on non-default storage | GA | **Bundle** `catalogs` (direct engine), no `storage_root` → metastore root | CDF rejects default-storage destinations | M9 |
| Governed views over CDF history | GA | Job `lakebase_matrix` (dynamic views) | Masks/filters on history tables stop CDF | M9 |
| Lakeflow Spark Declarative Pipeline (serverless, triggered) | GA | **Bundle** `pipelines.medallion` (`configuration`, `tags`, `permissions`); run as task `medallion` of job `setup` | Schema `resources.Pipeline` | M0, M4, M9 |
| Raw landing zone (UC volume) | GA | **Bundle** `volumes.landing` in schema `raw`; files written by `00_setup_data` | — | M0 |
| Auto Loader (`STREAM read_files`) into bronze streaming tables | GA | Pipeline SQL (`bronze/*.sql`) | — | M0 |
| Expectations (DROP ROW / warn) | GA | Pipeline SQL (`silver/contract_events.sql`) | Drop counts in the pipeline event log | M0 |
| Auto CDC SCD1 + SCD2 | GA | Pipeline SQL (`silver/contracts_current.sql`, `silver/contracts_history.sql`) | `APPLY AS DELETE WHEN`, `TRACK HISTORY ON` | M0 |
