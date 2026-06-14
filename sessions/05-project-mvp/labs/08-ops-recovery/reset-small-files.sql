-- R7 cleanup: drop the small-files demo table (the opsdemo namespace is kept).
DROP TABLE IF EXISTS iceberg_lake.opsdemo.smallfiles_demo;
