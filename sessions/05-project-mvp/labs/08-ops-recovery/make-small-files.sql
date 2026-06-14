-- R7 inject: simulate the streaming "small files" problem.
-- Each INSERT is a separate Iceberg commit, so 20 inserts => 20 tiny data files.
-- This is the shrunk version of "frequent checkpoints / many small commits create
-- thousands of small files and degrade reads".
CREATE NAMESPACE IF NOT EXISTS iceberg_lake.opsdemo;
DROP TABLE IF EXISTS iceberg_lake.opsdemo.smallfiles_demo;
CREATE TABLE iceberg_lake.opsdemo.smallfiles_demo (id INT, payload STRING) USING iceberg;

INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (1, 'event-0001');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (2, 'event-0002');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (3, 'event-0003');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (4, 'event-0004');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (5, 'event-0005');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (6, 'event-0006');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (7, 'event-0007');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (8, 'event-0008');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (9, 'event-0009');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (10, 'event-0010');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (11, 'event-0011');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (12, 'event-0012');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (13, 'event-0013');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (14, 'event-0014');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (15, 'event-0015');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (16, 'event-0016');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (17, 'event-0017');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (18, 'event-0018');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (19, 'event-0019');
INSERT INTO iceberg_lake.opsdemo.smallfiles_demo VALUES (20, 'event-0020');

SELECT COUNT(*) AS row_count FROM iceberg_lake.opsdemo.smallfiles_demo;
SELECT COUNT(*) AS data_file_count, SUM(file_size_in_bytes) AS total_bytes
FROM iceberg_lake.opsdemo.smallfiles_demo.files;
