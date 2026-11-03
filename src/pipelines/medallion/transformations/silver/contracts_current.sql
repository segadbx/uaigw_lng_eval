-- SILVER · current state of every contract (Auto CDC, SCD Type 1): amendments overwrite, cancellations
-- (op = 'DELETE') remove the contract. Ordered by the source event time.
CREATE OR REFRESH STREAMING TABLE ${lngdemo.catalog}.${lngdemo.silver_schema}.contracts_current
COMMENT 'LNG demo SILVER: current contract book (Auto CDC SCD1). Synthetic data.'
TBLPROPERTIES ('lngdemo_layer' = 'silver', 'lngdemo_project' = 'lngdemo-ai-governance');

CREATE FLOW contracts_current_cdc AS AUTO CDC INTO ${lngdemo.catalog}.${lngdemo.silver_schema}.contracts_current
FROM STREAM(${lngdemo.catalog}.${lngdemo.silver_schema}.contract_events)
KEYS (contract_id)
APPLY AS DELETE WHEN op = 'DELETE'
SEQUENCE BY event_ts
COLUMNS * EXCEPT (op, batch_id, _source_file, _ingested_at)
STORED AS SCD TYPE 1;
