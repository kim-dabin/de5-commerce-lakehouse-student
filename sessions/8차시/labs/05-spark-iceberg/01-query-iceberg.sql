SHOW NAMESPACES IN iceberg_lake;

SHOW TABLES IN iceberg_lake.analytics;

SELECT COUNT(*) AS clean_row_count
FROM iceberg_lake.analytics.commerce_events_clean;

SELECT
  event_date,
  event_type,
  event_count,
  user_count,
  session_count,
  product_count,
  revenue
FROM iceberg_lake.analytics.commerce_event_type_daily
ORDER BY event_date, event_type;

SELECT
  event_date,
  category_code,
  view_count,
  cart_count,
  purchase_count,
  revenue
FROM iceberg_lake.analytics.commerce_category_daily
ORDER BY event_date, revenue DESC;

SELECT
  event_id,
  event_time_ts,
  event_type,
  category_code,
  brand,
  price,
  user_id,
  user_session
FROM iceberg_lake.analytics.commerce_events_clean
ORDER BY event_id;
