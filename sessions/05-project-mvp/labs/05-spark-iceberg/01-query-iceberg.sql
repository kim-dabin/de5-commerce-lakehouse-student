SHOW NAMESPACES IN iceberg_lake;

SHOW TABLES IN iceberg_lake.analytics;

SELECT COUNT(*) AS olist_ux_clean_row_count
FROM iceberg_lake.analytics.olist_ux_events_clean;

SELECT COUNT(*) AS olist_review_current_count
FROM iceberg_lake.analytics.olist_review_current;

SELECT COUNT(*) AS olist_order_current_count
FROM iceberg_lake.analytics.olist_order_current;

SELECT
  event_date,
  event_type,
  event_count,
  user_count,
  session_count,
  product_count,
  revenue
FROM iceberg_lake.analytics.olist_event_type_daily
ORDER BY event_date, event_type;

SELECT
  event_date,
  sessions,
  product_view_sessions,
  review_impression_sessions,
  review_expand_sessions,
  add_to_cart_sessions,
  purchase_sessions,
  revenue
FROM iceberg_lake.analytics.olist_funnel_daily
ORDER BY event_date;

SELECT
  event_date,
  category_code,
  product_view_count,
  add_to_cart_count,
  purchase_count,
  review_impression_count,
  review_expand_count,
  revenue
FROM iceberg_lake.analytics.olist_category_daily
ORDER BY event_date, revenue DESC;

SELECT
  category_code,
  sentiment,
  review_count,
  avg_rating
FROM iceberg_lake.analytics.olist_review_sentiment_by_category
ORDER BY category_code, sentiment;

SELECT
  event_id,
  event_time_ts,
  event_type,
  category_code,
  brand,
  price,
  user_id,
  session_id
FROM iceberg_lake.analytics.olist_ux_events_clean
ORDER BY event_time_ts, event_id
LIMIT 50;
