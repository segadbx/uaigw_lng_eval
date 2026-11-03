-- SILVER · cleaned counterparty master: latest record per alias.
CREATE OR REFRESH MATERIALIZED VIEW ${lngdemo.catalog}.${lngdemo.silver_schema}.counterparties (
  CONSTRAINT valid_alias EXPECT (counterparty_alias IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'LNG demo SILVER: counterparty master, latest per alias. Synthetic data.'
TBLPROPERTIES ('lngdemo_layer' = 'silver', 'lngdemo_project' = 'lngdemo-ai-governance')
AS SELECT counterparty_alias, region, credit_rating, relationship_contact, CAST(updated_at AS DATE) AS updated_at
FROM bronze_counterparties
QUALIFY row_number() OVER (PARTITION BY counterparty_alias ORDER BY updated_at DESC, _ingested_at DESC) = 1;
