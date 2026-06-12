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
  commit_user,
  commit_kind,
  commit_time,
  total_record_count,
  delta_record_count,
  UNIX_TIMESTAMP(CAST(commit_time AS STRING))
    - UNIX_TIMESTAMP(CAST(LAG(commit_time) OVER (ORDER BY snapshot_id) AS STRING)) AS gap_sec
FROM `review_current$snapshots`
ORDER BY snapshot_id DESC
LIMIT 20;

-- 1-1. 특정 장애/배포 시점 직전의 정상 snapshot 후보 찾기
-- 실무에서는 '현재 row_count가 맞나?'보다 먼저
-- '어느 snapshot까지 정상으로 볼 수 있나?'를 찾습니다.
-- 수업 중에는 CURRENT_TIMESTAMP 기준으로 실행하고,
-- 실제 장애 분석에서는 아래 조건을 원하는 시각으로 바꿉니다.
SELECT
  snapshot_id,
  commit_kind,
  commit_time,
  total_record_count
FROM `review_current$snapshots`
WHERE commit_time <= CURRENT_TIMESTAMP
ORDER BY commit_time DESC
LIMIT 5;

-- 예시:
-- WHERE commit_time <= TIMESTAMP '2026-06-07 21:00:00'

-- 1-2. snapshot id 기준 time travel 예시
-- 위 쿼리에서 확인한 snapshot_id로 특정 시점의 테이블 상태를 읽습니다.
-- reset 직후 처음 적재하면 snapshot_id=1이 만들어지는 경우가 많습니다.
-- 만약 snapshot 1이 만료되었거나 없는 환경이면 위 결과의 snapshot_id로 바꿔 실행하세요.
SELECT
  COUNT(*) AS review_current_rows_at_snapshot_1
FROM review_current /*+ OPTIONS('scan.snapshot-id' = '1') */;

-- Paimon 1.4 + Flink 1.18+ 에서는 timestamp 기반 time travel도 가능합니다.
-- 실제 수업에서는 시각을 현재 snapshot commit_time 중 하나로 바꿔 사용합니다.
-- SELECT COUNT(*)
-- FROM review_current FOR SYSTEM_TIME AS OF TIMESTAMP '2026-06-07 21:00:00';

-- rollback은 운영에서 매우 신중하게 실행해야 하므로 수업에서는 실행하지 않습니다.
-- 기준점 찾기와 time travel 조회까지만 실습합니다.
-- CALL sys.rollback_to(`table` => 'bronze.review_current', snapshot_id => 1);

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
  commit_user,
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

-- 7. Iceberg-compatible metadata 관점
-- Paimon table DDL에 metadata.iceberg.storage=rest-catalog를 켜두면
-- Paimon commit 이후 Iceberg REST Catalog 쪽에도 reader 호환용 metadata가 생성됩니다.
-- 이 SQL Client는 Paimon catalog를 보고 있으므로 Iceberg metadata table 조회는
-- Jupyter/PySpark의 iceberg_lake catalog에서 별도로 확인합니다.
--
-- PySpark 예시:
-- spark.sql("""
-- SELECT snapshot_id, committed_at, operation
-- FROM iceberg_lake.bronze.review_current.snapshots
-- ORDER BY committed_at DESC
-- LIMIT 5
-- """).show(truncate=False)
--
-- Iceberg와 Paimon 모두 snapshot을 통해 '문제 직전 정상 기준점'을 찾습니다.
-- 차이는 Iceberg는 committed_at/operation, Paimon은 commit_time/commit_kind를 본다는 점입니다.
