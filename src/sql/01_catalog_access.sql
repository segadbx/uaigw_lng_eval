-- 01 · Catalog-level access for personas (idempotent).
-- The catalog is the demo catalog created for this bundle (README step 1); schema-level
-- grants are managed by the bundle in resources/schemas.yml.
GRANT USE CATALOG ON CATALOG `{{catalog}}` TO `{{trader_group}}`;
GRANT USE CATALOG ON CATALOG `{{catalog}}` TO `{{unentitled_trader_group}}`;
GRANT USE CATALOG ON CATALOG `{{catalog}}` TO `{{developer_group}}`;
GRANT USE CATALOG ON CATALOG `{{catalog}}` TO `{{operator_group}}`;
GRANT USE CATALOG ON CATALOG `{{catalog}}` TO `{{security_group}}`;
GRANT USE CATALOG ON CATALOG `{{catalog}}` TO `{{auditor_group}}`;
GRANT USE CATALOG ON CATALOG `{{catalog}}` TO `{{app_sp_application_id}}`;
