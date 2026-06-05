SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG paimon_lake WITH (
  'type' = 'paimon',
  'warehouse' = 'file:/warehouse/paimon'
);

CREATE DATABASE IF NOT EXISTS paimon_lake.bronze;

DROP TABLE IF EXISTS paimon_lake.bronze.ux_events_bronze;
DROP TABLE IF EXISTS paimon_lake.bronze.review_current;
