SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG paimon_lake WITH (
  'type' = 'paimon',
  'warehouse' = 'file:/warehouse/paimon'
);

DROP TABLE IF EXISTS paimon_lake.bronze.commerce_events_bronze;
