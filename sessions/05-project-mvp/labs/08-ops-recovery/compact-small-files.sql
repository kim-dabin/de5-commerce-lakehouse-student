-- R7 recover: compact the many small files into fewer larger files.
-- rewrite_data_files is the Iceberg maintenance procedure that bin-packs small files.
SELECT COUNT(*) AS data_files_before, SUM(file_size_in_bytes) AS bytes_before
FROM iceberg_lake.opsdemo.smallfiles_demo.files;

CALL iceberg_lake.system.rewrite_data_files(
  table => 'opsdemo.smallfiles_demo',
  options => map('min-input-files', '2')
);

SELECT COUNT(*) AS data_files_after, SUM(file_size_in_bytes) AS bytes_after
FROM iceberg_lake.opsdemo.smallfiles_demo.files;

SELECT COUNT(*) AS row_count_unchanged FROM iceberg_lake.opsdemo.smallfiles_demo;
