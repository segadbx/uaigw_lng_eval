-- 03 · Read access to synced (Lakehouse -> Lakebase) reference tables. Run after the synced table is ONLINE.
-- Synced tables are read-only in Postgres (modifications break the sync pipeline).
GRANT USAGE ON SCHEMA {{reference_schema}} TO "{{role_trader}}", "{{role_unentitled_trader}}", "{{role_operator}}", "{{role_security}}";
GRANT SELECT ON ALL TABLES IN SCHEMA {{reference_schema}} TO "{{role_trader}}", "{{role_unentitled_trader}}", "{{role_operator}}", "{{role_security}}";
