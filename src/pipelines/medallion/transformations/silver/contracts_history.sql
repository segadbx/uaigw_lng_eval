-- SILVER · price history of every contract (Auto CDC, SCD Type 2). A new version is opened only when a
-- commercial term changes; __START_AT / __END_AT bound each version (current rows: __END_AT IS NULL).
CREATE OR REFRESH STREAMING TABLE ${lngdemo.catalog}.${lngdemo.silver_schema}.contracts_history
COMMENT 'LNG demo SILVER: contract term history (Auto CDC SCD2). Synthetic data.'
TBLPROPERTIES ('lngdemo_layer' = 'silver', 'lngdemo_project' = 'lngdemo-ai-governance');

CREATE FLOW contracts_history_cdc AS AUTO CDC INTO ${lngdemo.catalog}.${lngdemo.silver_schema}.contracts_history
FROM STREAM(${lngdemo.catalog}.${lngdemo.silver_schema}.contract_events)
KEYS (contract_id)
APPLY AS DELETE WHEN op = 'DELETE'
SEQUENCE BY event_ts
COLUMNS * EXCEPT (op, batch_id, _source_file, _ingested_at)
STORED AS SCD TYPE 2
TRACK HISTORY ON (slope, constant_usd_mmbtu, price_formula, volume_mtpa, end_date);
