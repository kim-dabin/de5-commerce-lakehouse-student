SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG paimon_lake WITH (
  'type' = 'paimon',
  'warehouse' = 'file:/warehouse/paimon'
);

USE CATALOG paimon_lake;

CREATE DATABASE IF NOT EXISTS bronze;

USE bronze;

CREATE TABLE IF NOT EXISTS commerce_events_bronze (
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
