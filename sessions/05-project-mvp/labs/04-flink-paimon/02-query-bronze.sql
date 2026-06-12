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

SELECT COUNT(*) AS row_count
FROM commerce_events_bronze;

SELECT event_type, COUNT(*) AS event_count
FROM commerce_events_bronze
GROUP BY event_type
ORDER BY event_type;

SELECT event_id, event_time_text, event_type, category_code, brand, price, user_id, user_session
FROM commerce_events_bronze
ORDER BY event_id;
