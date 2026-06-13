-- Submitted with:
--   sql-client.sh -i 00-init-flink-session.sql -f this_file.sql
--
-- Restore placeholder:
--   SET 'execution.savepoint.path' = '<savepoint-path>';
SET 'pipeline.name' = 'ingest-review-current';

CREATE TEMPORARY TABLE review_events_kafka_raw (
  raw_json STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'review-events',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-olist-review-current-streaming',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'raw'
);

CREATE TABLE IF NOT EXISTS paimon_lake.bronze.review_current (
  review_id STRING,
  last_event_type STRING,
  updated_at_text STRING,
  order_id STRING,
  product_id BIGINT,
  source_product_id STRING,
  catalog_id STRING,
  category_id BIGINT,
  category_code STRING,
  rating INT,
  sentiment STRING,
  is_used BOOLEAN,
  matched_product_id BIGINT,
  review_title STRING,
  review_text STRING,
  raw_json STRING,
  ingested_at TIMESTAMP_LTZ(3),
  PRIMARY KEY (review_id) NOT ENFORCED
) WITH (
  'bucket' = '3',
  'metadata.iceberg.storage' = 'rest-catalog',
  'metadata.iceberg.rest.uri' = 'http://iceberg-rest:8181',
  'metadata.iceberg.rest.warehouse' = 's3://warehouse/',
  'metadata.iceberg.rest.clients' = '1',
  'full-compaction.delta-commits' = '1'
);

INSERT INTO paimon_lake.bronze.review_current
SELECT
  JSON_VALUE(raw_json, '$.review_id') AS review_id,
  JSON_VALUE(raw_json, '$.event_type') AS last_event_type,
  JSON_VALUE(raw_json, '$.updated_at') AS updated_at_text,
  JSON_VALUE(raw_json, '$.order_id') AS order_id,
  CAST(JSON_VALUE(raw_json, '$.product_id') AS BIGINT) AS product_id,
  JSON_VALUE(raw_json, '$.source_product_id') AS source_product_id,
  JSON_VALUE(raw_json, '$.catalog_id') AS catalog_id,
  CAST(JSON_VALUE(raw_json, '$.category_id') AS BIGINT) AS category_id,
  JSON_VALUE(raw_json, '$.category_code') AS category_code,
  CAST(JSON_VALUE(raw_json, '$.rating') AS INT) AS rating,
  JSON_VALUE(raw_json, '$.sentiment') AS sentiment,
  CASE JSON_VALUE(raw_json, '$.is_used') WHEN 'true' THEN TRUE ELSE FALSE END AS is_used,
  CAST(JSON_VALUE(raw_json, '$.matched_product_id') AS BIGINT) AS matched_product_id,
  JSON_VALUE(raw_json, '$.review_title') AS review_title,
  JSON_VALUE(raw_json, '$.review_text') AS review_text,
  raw_json,
  CURRENT_TIMESTAMP AS ingested_at
FROM review_events_kafka_raw
WHERE JSON_VALUE(raw_json, '$.review_id') IS NOT NULL;
