-- SILVER · typed, validated contract events. Expectations DROP invalid records (the amendment batch carries
-- three on purpose: missing desk, negative volume, unknown price index) and record the counts in the
-- pipeline event log. COALESCE(..., false) makes NULL inputs count as violations.
CREATE OR REFRESH STREAMING TABLE ${lngdemo.catalog}.${lngdemo.silver_schema}.contract_events (
  CONSTRAINT valid_contract_id EXPECT (contract_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_desk        EXPECT (COALESCE(desk IN ('Atlantic Desk', 'Pacific Desk', 'Portfolio Desk'), false)) ON VIOLATION DROP ROW,
  CONSTRAINT valid_price_index EXPECT (COALESCE(price_index IN ('HH', 'JKM', 'TTF', 'Brent'), false)) ON VIOLATION DROP ROW,
  CONSTRAINT positive_volume   EXPECT (op = 'DELETE' OR COALESCE(volume_mtpa > 0, false)) ON VIOLATION DROP ROW,
  CONSTRAINT known_operation   EXPECT (COALESCE(op IN ('UPSERT', 'DELETE'), false)) ON VIOLATION DROP ROW,
  CONSTRAINT dated_contract    EXPECT (start_date IS NOT NULL AND end_date > start_date)
)
COMMENT 'LNG demo SILVER: validated contract events (typed). Synthetic data.'
TBLPROPERTIES ('lngdemo_layer' = 'silver', 'lngdemo_project' = 'lngdemo-ai-governance')
AS SELECT
  contract_id,
  counterparty_alias,
  desk,
  contract_type,
  price_index,
  CAST(slope AS DOUBLE)              AS slope,
  CAST(constant_usd_mmbtu AS DOUBLE) AS constant_usd_mmbtu,
  price_formula,
  CAST(volume_mtpa AS DOUBLE)        AS volume_mtpa,
  CAST(start_date AS DATE)           AS start_date,
  CAST(end_date AS DATE)             AS end_date,
  contact_email,
  contact_phone,
  upper(op)                          AS op,
  CAST(event_ts AS TIMESTAMP)        AS event_ts,
  batch_id,
  _source_file,
  _ingested_at
FROM STREAM(bronze_contract_events);
