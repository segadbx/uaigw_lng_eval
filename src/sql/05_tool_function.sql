-- 05 · Aggregate-only UC function exposed to the copilot as a tool (idempotent).
-- Returns counts, volumes, tenor and index mix per desk; never a price, slope or constant.
-- Groups with fewer than 3 contracts are suppressed (small-cell suppression).
CREATE OR REPLACE FUNCTION `{{catalog}}`.`{{services_schema}}`.desk_price_summary(p_desk STRING)
RETURNS TABLE (
  desk STRING,
  price_index STRING,
  contract_count BIGINT,
  total_volume_mtpa DOUBLE,
  avg_remaining_tenor_years DOUBLE
)
COMMENT 'LNG demo tool: aggregate-only view of the contract book. No per-counterparty or price terms.'
RETURN
  SELECT desk,
         price_index,
         COUNT(*) AS contract_count,
         ROUND(SUM(volume_mtpa), 2) AS total_volume_mtpa,
         ROUND(AVG(months_between(end_date, current_date()) / 12), 1) AS avg_remaining_tenor_years
  FROM `{{catalog}}`.`{{trading_schema}}`.contracts
  WHERE p_desk IS NULL OR lower(desk) = lower(p_desk)
  GROUP BY desk, price_index
  HAVING COUNT(*) >= 3;
