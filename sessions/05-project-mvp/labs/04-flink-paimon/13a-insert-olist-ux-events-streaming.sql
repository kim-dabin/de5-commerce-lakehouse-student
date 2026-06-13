-- Submitted with:
--   sql-client.sh -i 00-init-flink-session.sql -f this_file.sql
--
-- Restore placeholder:
--   SET 'execution.savepoint.path' = '<savepoint-path>';
SET 'pipeline.name' = 'ingest-ux-events';

CREATE TEMPORARY TABLE ux_events_kafka_raw (
  raw_json STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'ux-events',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-olist-ux-bronze-streaming',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'raw'
);

CREATE TABLE IF NOT EXISTS paimon_lake.bronze.ux_events_bronze (
  event_id STRING,
  event_type STRING,
  event_time_text STRING,
  order_id STRING,
  product_id BIGINT,
  source_product_id STRING,
  catalog_id STRING,
  category_id BIGINT,
  category_code STRING,
  brand STRING,
  price DECIMAL(12, 2),
  user_id BIGINT,
  source_customer_id STRING,
  session_id STRING,
  is_synthetic_ux BOOLEAN,
  raw_json STRING,
  ingested_at TIMESTAMP_LTZ(3)
) WITH (
  'bucket' = '3',
  'bucket-key' = 'event_id',
  'metadata.iceberg.storage' = 'rest-catalog',
  'metadata.iceberg.rest.uri' = 'http://iceberg-rest:8181',
  'metadata.iceberg.rest.warehouse' = 's3://warehouse/',
  'metadata.iceberg.rest.clients' = '1'
);

INSERT INTO paimon_lake.bronze.ux_events_bronze
SELECT
  JSON_VALUE(raw_json, '$.event_id') AS event_id,
  JSON_VALUE(raw_json, '$.event_type') AS event_type,
  JSON_VALUE(raw_json, '$.event_time') AS event_time_text,
  JSON_VALUE(raw_json, '$.order_id') AS order_id,
  CAST(JSON_VALUE(raw_json, '$.product_id') AS BIGINT) AS product_id,
  JSON_VALUE(raw_json, '$.source_product_id') AS source_product_id,
  JSON_VALUE(raw_json, '$.catalog_id') AS catalog_id,
  CAST(JSON_VALUE(raw_json, '$.category_id') AS BIGINT) AS category_id,
  JSON_VALUE(raw_json, '$.category_code') AS category_code,
  JSON_VALUE(raw_json, '$.brand') AS brand,
  CAST(JSON_VALUE(raw_json, '$.price') AS DECIMAL(12, 2)) AS price,
  CAST(JSON_VALUE(raw_json, '$.user_id') AS BIGINT) AS user_id,
  JSON_VALUE(raw_json, '$.source_customer_id') AS source_customer_id,
  JSON_VALUE(raw_json, '$.session_id') AS session_id,
  CASE JSON_VALUE(raw_json, '$.is_synthetic_ux') WHEN 'true' THEN TRUE ELSE FALSE END AS is_synthetic_ux,
  raw_json,
  CURRENT_TIMESTAMP AS ingested_at
FROM ux_events_kafka_raw
WHERE JSON_VALUE(raw_json, '$.event_id') IS NOT NULL;
