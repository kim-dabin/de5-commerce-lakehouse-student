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

-- 1. Primary key table snapshot 흐름: APPEND / COMPACT 확인
SELECT
  snapshot_id,
  commit_kind,
  commit_time,
  total_record_count,
  delta_record_count
FROM `review_current$snapshots`
ORDER BY snapshot_id DESC
LIMIT 20;

-- 2. Commit 종류 요약: 최근까지 어떤 commit이 쌓였는지 확인
SELECT
  commit_kind,
  COUNT(*) AS snapshots,
  MIN(commit_time) AS first_seen,
  MAX(commit_time) AS last_seen
FROM `review_current$snapshots`
GROUP BY commit_kind
ORDER BY snapshots DESC;

-- 3. 파일 level별 분포: L0 파일이 많으면 compaction이 밀리는지 의심
SELECT
  level,
  COUNT(*) AS file_count,
  SUM(record_count) AS records,
  ROUND(CAST(SUM(file_size_in_bytes) AS DOUBLE) / 1024 / 1024, 2) AS size_mb,
  ROUND(CAST(AVG(file_size_in_bytes) AS DOUBLE) / 1024 / 1024, 2) AS avg_file_mb
FROM `review_current$files`
GROUP BY level
ORDER BY level;

-- 4. Bucket x level 분포: 특정 bucket에 변경 파일이 몰리는지 확인
SELECT
  bucket,
  level,
  COUNT(*) AS files,
  SUM(record_count) AS records,
  ROUND(CAST(SUM(file_size_in_bytes) AS DOUBLE) / 1024 / 1024, 2) AS size_mb
FROM `review_current$files`
GROUP BY bucket, level
ORDER BY bucket, level;

-- 5. L0 파일 상세: 오래된 L0 파일이 계속 남는지 확인
SELECT
  bucket,
  record_count,
  ROUND(CAST(file_size_in_bytes AS DOUBLE) / 1024 / 1024, 2) AS size_mb,
  min_sequence_number,
  max_sequence_number,
  creation_time,
  file_path
FROM `review_current$files`
WHERE level = 0
ORDER BY creation_time DESC
LIMIT 20;

-- 6. Append table과 비교: UXLog는 current-state merge가 아니라 계속 쌓이는 사실 로그
SELECT
  snapshot_id,
  commit_kind,
  commit_time,
  total_record_count,
  delta_record_count
FROM `ux_events_bronze$snapshots`
ORDER BY snapshot_id DESC
LIMIT 20;

SELECT
  level,
  COUNT(*) AS file_count,
  SUM(record_count) AS records,
  ROUND(CAST(SUM(file_size_in_bytes) AS DOUBLE) / 1024 / 1024, 2) AS size_mb
FROM `ux_events_bronze$files`
GROUP BY level
ORDER BY level;
