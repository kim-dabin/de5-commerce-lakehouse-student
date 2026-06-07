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

CREATE DATABASE IF NOT EXISTS paimon_lake.bronze;

DROP TABLE IF EXISTS paimon_lake.bronze.ux_events_bronze;
DROP TABLE IF EXISTS paimon_lake.bronze.review_current;
DROP TABLE IF EXISTS paimon_lake.bronze.order_current;
