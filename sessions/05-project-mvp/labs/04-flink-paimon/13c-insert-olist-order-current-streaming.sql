SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE TEMPORARY TABLE order_status_events_kafka_raw (
  raw_json STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'order-status-events',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-olist-order-current-streaming',
  'scan.startup.mode' = 'earliest-offset',
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
  'metadata.iceberg.storage' = 'rest-catalog',
  'metadata.iceberg.rest.uri' = 'http://iceberg-rest:8181',
  'metadata.iceberg.rest.warehouse' = 's3://warehouse/',
  'metadata.iceberg.rest.clients' = '1',
  'full-compaction.delta-commits' = '1'
);

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
