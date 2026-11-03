-- 07 · Result tables in audit_curated (idempotent: CREATE IF NOT EXISTS).
CREATE TABLE IF NOT EXISTS `{{catalog}}`.`{{audit_curated_schema}}`.red_team_results (
  run_id STRING, run_ts STRING, module STRING, test_id STRING, persona STRING, service STRING,
  prompt STRING, expected STRING, outcome STRING, verdict STRING, http_status STRING,
  latency_ms STRING, policy_json STRING, content_preview STRING, error STRING,
  raw_response STRING, request_marker STRING, notes STRING
);

CREATE TABLE IF NOT EXISTS `{{catalog}}`.`{{audit_curated_schema}}`.persona_matrix_results (
  run_id STRING, run_ts STRING, persona STRING, principal STRING, action STRING,
  expected STRING, observed STRING, verdict STRING, detail STRING, identity_note STRING
);

CREATE TABLE IF NOT EXISTS `{{catalog}}`.`{{audit_curated_schema}}`.acceptance_results (
  run_id STRING, run_ts STRING, test_no STRING, test_name STRING, verdict STRING, evidence STRING
);

CREATE TABLE IF NOT EXISTS `{{catalog}}`.`{{audit_curated_schema}}`.experiment_log (
  run_ts TIMESTAMP, experiment STRING, target STRING, statement STRING, ok BOOLEAN, detail STRING
);

CREATE TABLE IF NOT EXISTS `{{catalog}}`.`{{audit_curated_schema}}`.capability_matrix (
  capability STRING, status_doc STRING, observed STRING, deploy_method STRING, module STRING, checked_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `{{catalog}}`.`{{audit_curated_schema}}`.setup_state (
  key STRING, value STRING, updated_at TIMESTAMP
);
