-- GOLD · the governed contract book that personas, the copilot tool and the Lakebase probe read.
-- Unity Catalog protection is DECLARED HERE (pipeline-managed tables can't take ALTER TABLE SET ROW FILTER /
-- SET MASK afterwards): desk-entitlement row filter + price masks, using the UDFs the setup job creates
-- before the pipeline runs (src/sql/02_entitlement_and_mask_functions.sql). Row filters / column masks on
-- pipeline datasets are Public Preview.
CREATE OR REFRESH MATERIALIZED VIEW ${lngdemo.catalog}.${lngdemo.gold_schema}.contracts (
  contract_id         STRING NOT NULL,
  counterparty_alias  STRING,
  counterparty_region STRING,
  desk                STRING,
  contract_type       STRING,
  price_index         STRING,
  slope               DOUBLE MASK ${lngdemo.mask_num_fn},
  constant_usd_mmbtu  DOUBLE MASK ${lngdemo.mask_num_fn},
  price_formula       STRING MASK ${lngdemo.mask_text_fn},
  volume_mtpa         DOUBLE,
  start_date          DATE,
  end_date            DATE,
  contact_email       STRING,
  contact_phone       STRING,
  last_event_ts       TIMESTAMP
)
COMMENT 'LNG demo GOLD: SYNTHETIC LNG contract book (row filter + price masks declared in the pipeline).'
TBLPROPERTIES ('lngdemo_layer' = 'gold', 'lngdemo_project' = 'lngdemo-ai-governance')
WITH ROW FILTER ${lngdemo.desk_filter_fn} ON (desk)
AS SELECT
  c.contract_id, c.counterparty_alias, cp.region AS counterparty_region, c.desk, c.contract_type, c.price_index,
  c.slope, c.constant_usd_mmbtu, c.price_formula, c.volume_mtpa, c.start_date, c.end_date,
  c.contact_email, c.contact_phone, c.event_ts AS last_event_ts
FROM ${lngdemo.catalog}.${lngdemo.silver_schema}.contracts_current c
LEFT JOIN ${lngdemo.catalog}.${lngdemo.silver_schema}.counterparties cp
  ON c.counterparty_alias = cp.counterparty_alias;
