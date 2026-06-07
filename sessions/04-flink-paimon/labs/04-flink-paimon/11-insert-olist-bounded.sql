SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE TEMPORARY TABLE ux_events_kafka_raw (
  raw_json STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'ux-events',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-olist-ux-bronze',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'raw'
);

CREATE TEMPORARY TABLE review_events_kafka_raw (
  raw_json STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'review-events',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-olist-review-current',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'raw'
);

CREATE TEMPORARY TABLE order_status_events_kafka_raw (
  raw_json STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'order-status-events',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-olist-order-current',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'raw'
);

CREATE CATALOG paimon_lake WITH (
  'type' = 'paimon',
  'warehouse' = 's3://paimon/warehouse',
  's3.endpoint' = 'http://minio:9000',
  's3.access-key' = 'minioadmin',
  's3.secret-key' = 'minioadmin',
  's3.path.style.access' = 'true'
);

CREATE DATABASE IF NOT EXISTS paimon_lake.bronze;

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
  'metadata.iceberg.storage' = 'hadoop-catalog'
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
  'metadata.iceberg.storage' = 'hadoop-catalog',
  'full-compaction.delta-commits' = '1'
);

CREATE TABLE IF NOT EXISTS paimon_lake.bronze.order_current (
  order_id STRING,
  last_event_type STRING,
  updated_at_text STRING,
  order_status STRING,
  user_id BIGINT,
  source_customer_id STRING,
  session_id STRING,
  raw_json STRING,
  ingested_at TIMESTAMP_LTZ(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'bucket' = '3',
  'metadata.iceberg.storage' = 'hadoop-catalog',
  'full-compaction.delta-commits' = '1'
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

INSERT INTO paimon_lake.bronze.order_current
SELECT
  JSON_VALUE(raw_json, '$.order_id') AS order_id,
  JSON_VALUE(raw_json, '$.event_type') AS last_event_type,
  JSON_VALUE(raw_json, '$.event_time') AS updated_at_text,
  JSON_VALUE(raw_json, '$.order_status') AS order_status,
  CAST(JSON_VALUE(raw_json, '$.user_id') AS BIGINT) AS user_id,
  JSON_VALUE(raw_json, '$.source_customer_id') AS source_customer_id,
  JSON_VALUE(raw_json, '$.session_id') AS session_id,
  raw_json,
  CURRENT_TIMESTAMP AS ingested_at
FROM order_status_events_kafka_raw
WHERE JSON_VALUE(raw_json, '$.order_id') IS NOT NULL;
