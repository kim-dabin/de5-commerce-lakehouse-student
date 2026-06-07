SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG paimon_lake WITH (
  'type' = 'paimon',
  'warehouse' = 's3://paimon/warehouse',
  's3.endpoint' = 'http://minio:9000',
  's3.access-key' = 'minioadmin',
  's3.secret-key' = 'minioadmin',
  's3.path.style.access' = 'true'
);

USE CATALOG paimon_lake;
USE bronze;

SELECT COUNT(*) AS ux_row_count
FROM ux_events_bronze;

SELECT event_type, COUNT(*) AS event_count
FROM ux_events_bronze
GROUP BY event_type
ORDER BY event_type;

SELECT COUNT(*) AS review_current_count
FROM review_current;

SELECT sentiment, COUNT(*) AS review_count
FROM review_current
GROUP BY sentiment
ORDER BY sentiment;

SELECT COUNT(*) AS order_current_count
FROM order_current;

SELECT last_event_type, COUNT(*) AS order_count
FROM order_current
GROUP BY last_event_type
ORDER BY last_event_type;

SELECT review_id, last_event_type, updated_at_text, rating, sentiment, category_code
FROM review_current
ORDER BY updated_at_text DESC
LIMIT 20;
