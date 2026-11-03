-- GOLD · exposure by desk, counterparty region and price index (business dimensions kept for slicing;
-- no counterparty names, no prices; groups < 3 suppressed).
CREATE OR REFRESH MATERIALIZED VIEW ${lngdemo.catalog}.${lngdemo.gold_schema}.desk_exposure
COMMENT 'LNG demo GOLD: volume exposure by desk x region x index (no prices). Synthetic data.'
TBLPROPERTIES ('lngdemo_layer' = 'gold', 'lngdemo_project' = 'lngdemo-ai-governance')
AS SELECT
  c.desk,
  COALESCE(cp.region, 'Unknown') AS counterparty_region,
  c.price_index,
  count(*)                       AS contract_count,
  round(sum(c.volume_mtpa), 2)   AS total_volume_mtpa,
  min(c.start_date)              AS first_start_date,
  max(c.end_date)                AS last_end_date
FROM ${lngdemo.catalog}.${lngdemo.silver_schema}.contracts_current c
LEFT JOIN ${lngdemo.catalog}.${lngdemo.silver_schema}.counterparties cp
  ON c.counterparty_alias = cp.counterparty_alias
GROUP BY c.desk, COALESCE(cp.region, 'Unknown'), c.price_index
HAVING count(*) >= 3;
