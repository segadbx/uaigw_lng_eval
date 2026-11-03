-- BRONZE · contract events exactly as received from the (mock) CTRM extract, ingested incrementally with
-- Auto Loader from the raw landing volume. Adds file lineage; no cleansing here.
-- Configuration keys (resources/pipelines.yml): lngdemo.landing_path
CREATE OR REFRESH STREAMING TABLE bronze_contract_events
COMMENT 'LNG demo BRONZE: raw contract events (Auto Loader, JSON). Synthetic data.'
TBLPROPERTIES ('lngdemo_layer' = 'bronze', 'lngdemo_project' = 'lngdemo-ai-governance')
AS SELECT
  *,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _file_modified_at,
  current_timestamp()              AS _ingested_at
FROM STREAM read_files(
  '${lngdemo.landing_path}/contracts',
  format => 'json',
  schemaHints => 'contract_id STRING, desk STRING, price_index STRING, slope DOUBLE, constant_usd_mmbtu DOUBLE, volume_mtpa DOUBLE, start_date STRING, end_date STRING, op STRING, event_ts STRING, batch_id STRING'
);
