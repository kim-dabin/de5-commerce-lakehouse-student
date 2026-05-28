CREATE STORAGE VOLUME IF NOT EXISTS de5_starrocks_volume
TYPE = S3
LOCATIONS = ("s3://starrocks/")
PROPERTIES
(
  "enabled" = "true",
  "aws.s3.region" = "us-east-1",
  "aws.s3.endpoint" = "minio:9000",
  "aws.s3.access_key" = "AAAAAAAAAAAAAAAAAAAA",
  "aws.s3.secret_key" = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.use_aws_sdk_default_behavior" = "false",
  "aws.s3.enable_path_style_access" = "true"
);

SET de5_starrocks_volume AS DEFAULT STORAGE VOLUME;

CREATE DATABASE IF NOT EXISTS de5_realtime_olap;

USE de5_realtime_olap;

ADMIN SET FRONTEND CONFIG("tablet_create_timeout_second" = "300");
ADMIN SET FRONTEND CONFIG("enable_statistic_collect" = "false");
ADMIN SET FRONTEND CONFIG("enable_statistic_collect_on_first_load" = "false");
ADMIN SET FRONTEND CONFIG("enable_collect_full_statistic" = "false");

DROP VIEW IF EXISTS commerce_minute_event_type_realtime;
DROP VIEW IF EXISTS commerce_category_realtime;
DROP VIEW IF EXISTS commerce_event_type_realtime;
DROP VIEW IF EXISTS commerce_events_rt_typed;
DROP TABLE IF EXISTS commerce_events_rt;

CREATE TABLE commerce_events_rt (
  id VARCHAR(64) NOT NULL,
  event_time VARCHAR(32),
  event_type VARCHAR(32),
  product_id BIGINT,
  category_id BIGINT,
  category_code VARCHAR(256),
  brand VARCHAR(128),
  price DECIMAL(12, 2),
  user_id BIGINT,
  user_session VARCHAR(128),
  ingested_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

CREATE VIEW commerce_events_rt_typed AS
SELECT
  id AS event_id,
  STR_TO_DATE(REPLACE(REPLACE(event_time, 'T', ' '), 'Z', ''), '%Y-%m-%d %H:%i:%s') AS event_time_ts,
  DATE_TRUNC('minute', STR_TO_DATE(REPLACE(REPLACE(event_time, 'T', ' '), 'Z', ''), '%Y-%m-%d %H:%i:%s')) AS event_minute,
  event_type,
  product_id,
  category_id,
  COALESCE(category_code, 'unknown') AS category_code,
  COALESCE(brand, 'unknown') AS brand,
  price,
  user_id,
  user_session,
  ingested_at
FROM commerce_events_rt;

CREATE VIEW commerce_event_type_realtime AS
SELECT
  event_type,
  COUNT(*) AS event_count,
  COUNT(DISTINCT user_id) AS user_count,
  COUNT(DISTINCT user_session) AS session_count,
  COUNT(DISTINCT product_id) AS product_count,
  SUM(IF(event_type = 'purchase', price, 0)) AS revenue,
  MIN(event_time_ts) AS first_event_at,
  MAX(event_time_ts) AS last_event_at
FROM commerce_events_rt_typed
GROUP BY event_type;

CREATE VIEW commerce_category_realtime AS
SELECT
  category_code,
  COUNT(*) AS event_count,
  SUM(IF(event_type = 'view', 1, 0)) AS view_count,
  SUM(IF(event_type = 'cart', 1, 0)) AS cart_count,
  SUM(IF(event_type = 'purchase', 1, 0)) AS purchase_count,
  COUNT(DISTINCT user_id) AS user_count,
  COUNT(DISTINCT user_session) AS session_count,
  SUM(IF(event_type = 'purchase', price, 0)) AS revenue,
  MAX(event_time_ts) AS last_event_at
FROM commerce_events_rt_typed
GROUP BY category_code;

CREATE VIEW commerce_minute_event_type_realtime AS
SELECT
  event_minute,
  event_type,
  COUNT(*) AS event_count,
  COUNT(DISTINCT user_id) AS user_count,
  SUM(IF(event_type = 'purchase', price, 0)) AS revenue
FROM commerce_events_rt_typed
GROUP BY event_minute, event_type;
