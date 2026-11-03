-- 10 · Runs on the DEV branch only, right after it is branched from production.
-- The branch is a full copy of production (data AND roles), so contact data is scrubbed here and the
-- developer gets read/write on the copy. Production is untouched (copy-on-write).
UPDATE {{pg_schema}}.cargo_nominations SET contact_email = 'scrubbed@example.com' WHERE contact_email IS NOT NULL;
GRANT CONNECT ON DATABASE {{database}} TO "{{developer_sp}}";
GRANT USAGE ON SCHEMA {{pg_schema}} TO "{{developer_sp}}";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {{pg_schema}} TO "{{developer_sp}}";
