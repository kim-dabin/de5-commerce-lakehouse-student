USE de5_realtime_olap;

SELECT
  COUNT(*) AS total_events,
  COUNT(DISTINCT user_id) AS users,
  COUNT(DISTINCT user_session) AS sessions,
  COUNT(DISTINCT product_id) AS products,
  SUM(IF(event_type = 'purchase', price, 0)) AS revenue,
  MIN(event_time_ts) AS first_event_at,
  MAX(event_time_ts) AS last_event_at
FROM commerce_events_rt_typed;

SELECT *
FROM commerce_event_type_realtime
ORDER BY event_type;

SELECT *
FROM commerce_category_realtime
ORDER BY revenue DESC, event_count DESC
LIMIT 10;

SELECT *
FROM commerce_minute_event_type_realtime
ORDER BY event_minute, event_type
LIMIT 20;
