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

ADMIN SET FRONTEND CONFIG("tablet_create_timeout_second" = "300");
ADMIN SET FRONTEND CONFIG("enable_statistic_collect" = "false");
ADMIN SET FRONTEND CONFIG("enable_statistic_collect_on_first_load" = "false");
ADMIN SET FRONTEND CONFIG("enable_collect_full_statistic" = "false");

DROP CATALOG IF EXISTS paimon_olist;
DROP CATALOG IF EXISTS iceberg_olist;

CREATE EXTERNAL CATALOG paimon_olist
PROPERTIES
(
  "type" = "paimon",
  "paimon.catalog.type" = "filesystem",
  "paimon.catalog.warehouse" = "file:///warehouse/paimon"
);

CREATE EXTERNAL CATALOG iceberg_olist
PROPERTIES
(
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "http://iceberg-rest:8181",
  "iceberg.catalog.warehouse" = "s3://warehouse/",
  "aws.s3.enable_ssl" = "false",
  "aws.s3.enable_path_style_access" = "true",
  "aws.s3.endpoint" = "http://minio:9000",
  "aws.s3.access_key" = "minioadmin",
  "aws.s3.secret_key" = "minioadmin",
  "aws.s3.region" = "us-east-1"
);

CREATE DATABASE IF NOT EXISTS de5_realtime_olap;

USE de5_realtime_olap;

DROP VIEW IF EXISTS order_status_realtime;
DROP VIEW IF EXISTS review_sentiment_realtime;
DROP VIEW IF EXISTS review_risk_category_realtime;
DROP VIEW IF EXISTS review_risk_product_realtime;
DROP VIEW IF EXISTS review_impact_summary_realtime;
DROP VIEW IF EXISTS commerce_minute_event_type_realtime;
DROP VIEW IF EXISTS commerce_category_realtime;
DROP VIEW IF EXISTS commerce_event_type_realtime;
DROP VIEW IF EXISTS commerce_events_rt_typed;
DROP TABLE IF EXISTS commerce_events_rt;

CREATE VIEW commerce_events_rt_typed AS
SELECT
  event_id,
  STR_TO_DATE(REPLACE(REPLACE(event_time_text, 'T', ' '), 'Z', ''), '%Y-%m-%d %H:%i:%s') AS event_time_ts,
  DATE_TRUNC('minute', STR_TO_DATE(REPLACE(REPLACE(event_time_text, 'T', ' '), 'Z', ''), '%Y-%m-%d %H:%i:%s')) AS event_minute,
  event_type,
  product_id,
  category_id,
  COALESCE(category_code, 'unknown') AS category_code,
  COALESCE(brand, 'unknown') AS brand,
  price,
  user_id,
  session_id AS user_session,
  ingested_at
FROM paimon_olist.bronze.ux_events_bronze;

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
  SUM(IF(event_type = 'product_view', 1, 0)) AS view_count,
  SUM(IF(event_type = 'add_to_cart', 1, 0)) AS cart_count,
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

CREATE VIEW review_sentiment_realtime AS
SELECT
  COALESCE(sentiment, 'unknown') AS sentiment,
  COUNT(*) AS review_count,
  COUNT(DISTINCT product_id) AS product_count,
  AVG(rating) AS avg_rating
FROM paimon_olist.bronze.review_current
GROUP BY COALESCE(sentiment, 'unknown');

CREATE VIEW order_status_realtime AS
SELECT
  COALESCE(order_status, 'unknown') AS order_status,
  COUNT(*) AS order_count,
  COUNT(DISTINCT user_id) AS user_count
FROM paimon_olist.bronze.order_current
GROUP BY COALESCE(order_status, 'unknown');

CREATE VIEW review_impact_summary_realtime AS
WITH session_product AS (
  SELECT
    user_session,
    product_id,
    MIN(IF(event_type = 'product_view', event_time_ts, NULL)) AS product_view_at,
    MIN(IF(event_type IN ('review_impression', 'review_expand'), event_time_ts, NULL)) AS review_seen_at,
    MIN(IF(event_type = 'review_expand', event_time_ts, NULL)) AS review_expand_at,
    MIN(IF(event_type = 'add_to_cart', event_time_ts, NULL)) AS add_to_cart_at,
    MIN(IF(event_type = 'purchase', event_time_ts, NULL)) AS purchase_at
  FROM commerce_events_rt_typed
  WHERE product_id IS NOT NULL
  GROUP BY user_session, product_id
),
counts AS (
  SELECT
    COUNT(*) AS session_product_pairs,
    SUM(IF(product_view_at IS NOT NULL, 1, 0)) AS product_view_pairs,
    SUM(IF(review_seen_at IS NOT NULL, 1, 0)) AS review_seen_pairs,
    SUM(IF(review_expand_at IS NOT NULL, 1, 0)) AS review_expand_pairs,
    SUM(IF(
      review_seen_at IS NOT NULL
      AND add_to_cart_at IS NOT NULL
      AND add_to_cart_at >= review_seen_at,
      1,
      0
    )) AS cart_click_after_review_pairs,
    SUM(IF(
      review_seen_at IS NOT NULL
      AND purchase_at IS NOT NULL
      AND purchase_at >= review_seen_at,
      1,
      0
    )) AS purchase_after_review_pairs,
    SUM(IF(
      product_view_at IS NOT NULL
      AND add_to_cart_at IS NULL
      AND purchase_at IS NULL,
      1,
      0
    )) AS pdp_exit_pairs
  FROM session_product
)
SELECT
  session_product_pairs,
  product_view_pairs,
  review_seen_pairs,
  review_expand_pairs,
  cart_click_after_review_pairs,
  purchase_after_review_pairs,
  pdp_exit_pairs,
  IF(review_seen_pairs = 0, 0, ROUND(cart_click_after_review_pairs * 100.0 / review_seen_pairs, 2)) AS cart_click_after_review_rate,
  IF(review_seen_pairs = 0, 0, ROUND(purchase_after_review_pairs * 100.0 / review_seen_pairs, 2)) AS purchase_after_review_rate,
  IF(product_view_pairs = 0, 0, ROUND(pdp_exit_pairs * 100.0 / product_view_pairs, 2)) AS pdp_exit_rate
FROM counts;

CREATE VIEW review_risk_product_realtime AS
WITH session_product AS (
  SELECT
    product_id,
    MAX(category_code) AS category_code,
    user_session,
    MIN(IF(event_type = 'product_view', event_time_ts, NULL)) AS product_view_at,
    MIN(IF(event_type = 'review_expand', event_time_ts, NULL)) AS review_expand_at,
    MIN(IF(event_type = 'add_to_cart', event_time_ts, NULL)) AS add_to_cart_at,
    MIN(IF(event_type = 'purchase', event_time_ts, NULL)) AS purchase_at
  FROM commerce_events_rt_typed
  WHERE product_id IS NOT NULL
  GROUP BY product_id, user_session
),
product_behavior AS (
  SELECT
    product_id,
    MAX(category_code) AS category_code,
    SUM(IF(product_view_at IS NOT NULL, 1, 0)) AS product_view_sessions,
    SUM(IF(
      product_view_at IS NOT NULL
      AND add_to_cart_at IS NULL
      AND purchase_at IS NULL,
      1,
      0
    )) AS pdp_exit_sessions,
    SUM(IF(add_to_cart_at IS NOT NULL, 1, 0)) AS add_to_cart_sessions,
    SUM(IF(purchase_at IS NOT NULL, 1, 0)) AS purchase_sessions
  FROM session_product
  GROUP BY product_id
),
product_reviews AS (
  SELECT
    product_id,
    MAX(COALESCE(category_code, 'unknown')) AS category_code,
    COUNT(*) AS review_count,
    SUM(IF(sentiment = 'negative', 1, 0)) AS negative_review_count,
    AVG(rating) AS avg_rating
  FROM paimon_olist.bronze.review_current
  WHERE product_id IS NOT NULL
  GROUP BY product_id
)
SELECT
  behavior.product_id,
  COALESCE(behavior.category_code, reviews.category_code, 'unknown') AS category_code,
  reviews.review_count,
  reviews.negative_review_count,
  IF(reviews.review_count = 0, 0, ROUND(reviews.negative_review_count * 100.0 / reviews.review_count, 2)) AS negative_review_ratio,
  reviews.avg_rating,
  behavior.product_view_sessions,
  behavior.pdp_exit_sessions,
  IF(behavior.product_view_sessions = 0, 0, ROUND(behavior.pdp_exit_sessions * 100.0 / behavior.product_view_sessions, 2)) AS pdp_exit_rate,
  behavior.add_to_cart_sessions,
  behavior.purchase_sessions,
  IF(behavior.product_view_sessions = 0, 0, ROUND(behavior.purchase_sessions * 100.0 / behavior.product_view_sessions, 2)) AS purchase_rate
FROM product_behavior behavior
JOIN product_reviews reviews
  ON behavior.product_id = reviews.product_id;

CREATE VIEW review_risk_category_realtime AS
SELECT
  category_code,
  COUNT(*) AS product_count,
  SUM(review_count) AS review_count,
  SUM(negative_review_count) AS negative_review_count,
  IF(SUM(review_count) = 0, 0, ROUND(SUM(negative_review_count) * 100.0 / SUM(review_count), 2)) AS negative_review_ratio,
  SUM(product_view_sessions) AS product_view_sessions,
  SUM(pdp_exit_sessions) AS pdp_exit_sessions,
  IF(SUM(product_view_sessions) = 0, 0, ROUND(SUM(pdp_exit_sessions) * 100.0 / SUM(product_view_sessions), 2)) AS pdp_exit_rate,
  SUM(add_to_cart_sessions) AS add_to_cart_sessions,
  SUM(purchase_sessions) AS purchase_sessions,
  IF(SUM(product_view_sessions) = 0, 0, ROUND(SUM(purchase_sessions) * 100.0 / SUM(product_view_sessions), 2)) AS purchase_rate
FROM review_risk_product_realtime
GROUP BY category_code;
