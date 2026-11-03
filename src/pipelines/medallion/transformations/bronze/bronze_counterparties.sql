-- BRONZE · counterparty master extract (CSV) as received, ingested incrementally with Auto Loader.
CREATE OR REFRESH STREAMING TABLE bronze_counterparties
COMMENT 'LNG demo BRONZE: raw counterparty master (Auto Loader, CSV). Synthetic data.'
TBLPROPERTIES ('lngdemo_layer' = 'bronze', 'lngdemo_project' = 'lngdemo-ai-governance')
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${lngdemo.landing_path}/counterparties',
  format => 'csv',
  header => true,
  schemaHints => 'counterparty_alias STRING, region STRING, credit_rating STRING, relationship_contact STRING, updated_at STRING'
);
