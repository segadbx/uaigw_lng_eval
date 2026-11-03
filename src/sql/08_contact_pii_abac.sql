-- 08 · Tag-driven (ABAC) column mask for counterparty contact PII on the gold contract book (idempotent).
-- Unlike the price masks (bound to named columns in gold/contracts.sql), this policy binds to any column in
-- the trading_data schema tagged lngdemo_sensitivity = 'pii' (06_contact_pii_abac tags contact_email and
-- contact_phone). ABAC policies on pipeline materialized views are evaluated for the reader at query time.
-- Exempt: the security group only, so even the fully entitled trader sees masked contacts.
CREATE OR REPLACE FUNCTION `{{catalog}}`.`{{trading_schema}}`.mask_contact_pii(v STRING)
RETURNS STRING
COMMENT 'LNG demo: ABAC mask for lngdemo_sensitivity=pii columns (keeps e-mail domain / last 4 phone digits)'
RETURN CASE
  WHEN v IS NULL THEN NULL
  WHEN v LIKE '%@%' THEN concat('***', substring(v, instr(v, '@')))
  ELSE concat('***-', right(regexp_replace(v, '[^0-9]', ''), 4))
END;

CREATE OR REPLACE POLICY lngdemo_mask_contact_pii
ON SCHEMA `{{catalog}}`.`{{trading_schema}}`
COMMENT 'LNG demo: mask every column tagged lngdemo_sensitivity=pii for everyone except security'
COLUMN MASK `{{catalog}}`.`{{trading_schema}}`.mask_contact_pii
TO `account users`
EXCEPT `{{security_group}}`
FOR TABLES
MATCH COLUMNS has_tag_value('lngdemo_sensitivity', 'pii') AS pii_col
ON COLUMN pii_col;
