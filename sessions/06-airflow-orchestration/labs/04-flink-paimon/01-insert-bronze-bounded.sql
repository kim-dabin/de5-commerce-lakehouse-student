SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE TEMPORARY TABLE commerce_events_kafka_raw (
  raw_json STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'commerce-events',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-paimon-bronze',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'raw'
);

CREATE CATALOG paimon_lake WITH (
  'type' = 'paimon',
  'warehouse' = 'file:/warehouse/paimon'
);

CREATE DATABASE IF NOT EXISTS paimon_lake.bronze;

CREATE TABLE IF NOT EXISTS paimon_lake.bronze.commerce_events_bronze (
  event_id STRING,
  event_type STRING,
  event_time_text STRING,
  product_id BIGINT,
  category_id BIGINT,
  category_code STRING,
  brand STRING,
  price DECIMAL(12, 2),
  user_id BIGINT,
  user_session STRING,
  raw_json STRING,
  ingested_at TIMESTAMP_LTZ(3),
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'bucket' = '3'
);

INSERT INTO paimon_lake.bronze.commerce_events_bronze
SELECT
  JSON_VALUE(raw_json, '$.id') AS event_id,
  JSON_VALUE(raw_json, '$.event_type') AS event_type,
  JSON_VALUE(raw_json, '$.event_time') AS event_time_text,
  CAST(JSON_VALUE(raw_json, '$.product_id') AS BIGINT) AS product_id,
  CAST(JSON_VALUE(raw_json, '$.category_id') AS BIGINT) AS category_id,
  JSON_VALUE(raw_json, '$.category_code') AS category_code,
  JSON_VALUE(raw_json, '$.brand') AS brand,
  CAST(JSON_VALUE(raw_json, '$.price') AS DECIMAL(12, 2)) AS price,
  CAST(JSON_VALUE(raw_json, '$.user_id') AS BIGINT) AS user_id,
  JSON_VALUE(raw_json, '$.user_session') AS user_session,
  raw_json,
  CURRENT_TIMESTAMP AS ingested_at
FROM commerce_events_kafka_raw
WHERE JSON_VALUE(raw_json, '$.id') IS NOT NULL;
