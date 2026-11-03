-- 01 · Lakebase (Postgres) schema for cargo nominations. Idempotent. Runs as the project owner.
-- Placeholders ({{name}}) are filled by src/notebooks/70_lakebase_setup.py from job parameters.
-- Synthetic data only: counterparty aliases, example.com contacts.
CREATE SCHEMA IF NOT EXISTS {{pg_schema}};

-- Which Postgres group role may see / enter which desk (input to row-level security).
CREATE TABLE IF NOT EXISTS {{pg_schema}}.desk_access (
  role_name text NOT NULL,
  desk      text NOT NULL,
  PRIMARY KEY (role_name, desk)
);

CREATE TABLE IF NOT EXISTS {{pg_schema}}.cargo_nominations (
  nomination_id      text PRIMARY KEY,
  desk               text NOT NULL,
  counterparty_alias text NOT NULL,
  cargo_window_start date NOT NULL,
  volume_mmbtu       numeric(14,0) NOT NULL,
  status             text NOT NULL DEFAULT 'draft',
  entered_by         text NOT NULL DEFAULT current_user,
  contact_email      text,
  created_at         timestamptz NOT NULL DEFAULT now()
);

-- Lakebase CDF (Lakebase -> Lakehouse) requires REPLICA IDENTITY FULL on every source table.
ALTER TABLE {{pg_schema}}.desk_access REPLICA IDENTITY FULL;
ALTER TABLE {{pg_schema}}.cargo_nominations REPLICA IDENTITY FULL;

-- Row-level security by desk. The table owner (project owner) bypasses RLS, as Postgres does by default.
ALTER TABLE {{pg_schema}}.cargo_nominations ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS desk_read ON {{pg_schema}}.cargo_nominations;
CREATE POLICY desk_read ON {{pg_schema}}.cargo_nominations FOR SELECT
  USING (
    pg_has_role(current_user, '{{role_security}}', 'MEMBER')
    OR EXISTS (SELECT 1 FROM {{pg_schema}}.desk_access a
               WHERE a.desk = cargo_nominations.desk AND pg_has_role(current_user, a.role_name, 'MEMBER'))
  );

DROP POLICY IF EXISTS desk_insert ON {{pg_schema}}.cargo_nominations;
CREATE POLICY desk_insert ON {{pg_schema}}.cargo_nominations FOR INSERT
  WITH CHECK (
    EXISTS (SELECT 1 FROM {{pg_schema}}.desk_access a
            WHERE a.desk = cargo_nominations.desk AND pg_has_role(current_user, a.role_name, 'MEMBER'))
  );

DROP POLICY IF EXISTS desk_update ON {{pg_schema}}.cargo_nominations;
CREATE POLICY desk_update ON {{pg_schema}}.cargo_nominations FOR UPDATE
  USING (
    EXISTS (SELECT 1 FROM {{pg_schema}}.desk_access a
            WHERE a.desk = cargo_nominations.desk AND pg_has_role(current_user, a.role_name, 'MEMBER'))
  );

-- Operator view: every desk, but no counterparty and no contact (runs with the owner's rights, so RLS
-- does not narrow it; access is controlled by GRANT on the view only).
CREATE OR REPLACE VIEW {{pg_schema}}.v_nominations_ops AS
  SELECT nomination_id, desk, cargo_window_start, volume_mmbtu, status, created_at
  FROM {{pg_schema}}.cargo_nominations;

COMMENT ON TABLE {{pg_schema}}.cargo_nominations IS 'LNG demo: SYNTHETIC cargo nominations. lngdemo_project=lngdemo-ai-governance';
