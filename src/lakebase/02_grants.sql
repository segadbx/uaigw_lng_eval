-- 02 · Postgres privileges (layer 3). Idempotent. Runs as the project owner.
-- Persona SP OAuth roles (created by the bundle) are made members of NO_LOGIN group roles; privileges are
-- granted to the group roles only. Developer and auditor have no role on the production branch.
GRANT "{{role_trader}}" TO "{{trader_sp}}";
GRANT "{{role_unentitled_trader}}" TO "{{unentitled_trader_sp}}";
GRANT "{{role_operator}}" TO "{{operator_sp}}";
GRANT "{{role_security}}" TO "{{security_sp}}";

GRANT CONNECT ON DATABASE {{database}} TO "{{role_trader}}", "{{role_unentitled_trader}}", "{{role_operator}}", "{{role_security}}";
GRANT USAGE ON SCHEMA {{pg_schema}} TO "{{role_trader}}", "{{role_unentitled_trader}}", "{{role_operator}}", "{{role_security}}";

-- Traders: own-desk rows only (RLS); every column except contact_email; insert/update within their desk.
GRANT SELECT ON {{pg_schema}}.desk_access TO "{{role_trader}}", "{{role_unentitled_trader}}";
GRANT SELECT (nomination_id, desk, counterparty_alias, cargo_window_start, volume_mmbtu, status, entered_by, created_at)
  ON {{pg_schema}}.cargo_nominations TO "{{role_trader}}", "{{role_unentitled_trader}}";
GRANT INSERT (nomination_id, desk, counterparty_alias, cargo_window_start, volume_mmbtu, status, contact_email)
  ON {{pg_schema}}.cargo_nominations TO "{{role_trader}}", "{{role_unentitled_trader}}";
GRANT UPDATE (status) ON {{pg_schema}}.cargo_nominations TO "{{role_trader}}", "{{role_unentitled_trader}}";

-- Operators: metadata view only.
GRANT SELECT ON {{pg_schema}}.v_nominations_ops TO "{{role_operator}}";

-- Security: read everything in the schema (all desks, contact included); no writes.
GRANT SELECT ON ALL TABLES IN SCHEMA {{pg_schema}} TO "{{role_security}}";
