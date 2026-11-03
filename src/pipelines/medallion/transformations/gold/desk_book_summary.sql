-- GOLD · aggregate-only desk book summary (no prices; groups < 3 suppressed). Source of the Lakebase synced
-- table (M9). Built from SILVER, not from the governed gold table: the gold row filter is evaluated for the
-- pipeline's run-as identity, which is in no persona group and would aggregate zero rows.
CREATE OR REFRESH MATERIALIZED VIEW ${lngdemo.catalog}.${lngdemo.gold_schema}.desk_book_summary
COMMENT 'LNG demo GOLD: aggregate-only desk book summary (no prices). Synthetic data.'
TBLPROPERTIES ('lngdemo_layer' = 'gold', 'lngdemo_project' = 'lngdemo-ai-governance')
AS SELECT
  concat(desk, ' | ', price_index)                                AS summary_id,
  desk,
  price_index,
  count(*)                                                        AS contract_count,
  round(sum(volume_mtpa), 2)                                      AS total_volume_mtpa,
  round(avg(months_between(end_date, current_date()) / 12), 1)    AS avg_remaining_tenor_years
FROM ${lngdemo.catalog}.${lngdemo.silver_schema}.contracts_current
GROUP BY desk, price_index
HAVING count(*) >= 3;
