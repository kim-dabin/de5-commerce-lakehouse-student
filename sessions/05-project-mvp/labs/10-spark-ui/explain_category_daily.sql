-- 8차시 실무 보강: 실행 플랜 읽기용 EXPLAIN 쿼리.
-- category_daily 집계(스캔 → 셔플(Exchange) → HashAggregate)의 물리 플랜을 본다.
EXPLAIN FORMATTED
SELECT
  event_date,
  category_code,
  COUNT(*) AS event_count,
  SUM(CASE WHEN event_type = 'purchase' THEN price ELSE CAST(0 AS DECIMAL(12,2)) END) AS revenue
FROM iceberg_lake.analytics.olist_ux_events_clean
WHERE event_date >= DATE '2017-12-01'
GROUP BY event_date, category_code
ORDER BY event_date, category_code;
