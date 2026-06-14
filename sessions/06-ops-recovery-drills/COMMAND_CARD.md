# 6차시 명령 카드

모든 명령은 아래 위치에서 실행합니다.

```bash
cd sessions/05-project-mvp
```

## 0. 기준 상태 만들기

```bash
git pull origin main
docker compose -f docker-compose.lite.yml up -d --build
docker compose -f docker-compose.lite.yml ps

./scripts/reset-olist-kafka-topics.sh
./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon-streaming.sh all
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh
./scripts/start-airflow.sh
./scripts/trigger-airflow-pipeline.sh
```

Airflow UI에서 `de5_olist_project_mvp_pipeline`이 성공한 뒤 baseline을 확인합니다.

```bash
./scripts/ops-baseline-evidence.sh
```

## R1. TaskManager 장애

이 라운드는 로컬 Docker 축소판입니다.

실제 운영 사슬은 아래와 같았습니다.

```text
ephemeral-storage/DiskPressure -> kubelet pod eviction -> TaskManager slot 상실 -> 소비 중단 -> 입력이 계속 오면 backlog/lag 증가
```

로컬에는 kubelet eviction이 없으므로, `flink-taskmanager`를 직접 멈춰서 **eviction 이후 TaskManager가 사라진 효과**만 재현합니다.

주입:

```bash
./scripts/ops-r1-stop-taskmanager.sh
```

관찰:

```bash
docker compose -f docker-compose.lite.yml ps flink-jobmanager flink-taskmanager
./scripts/ops-flink-list-jobs.sh
./scripts/ops-kafka-topic-offsets.sh ux-events
```

선택 관찰:

TaskManager가 멈춘 동안 offset/backlog가 늘어나는 모습을 보고 싶다면 아래를 추가로 실행합니다.

```bash
./scripts/produce-olist-ux-events.sh --rate-per-second 20 --max-events 300
./scripts/ops-kafka-topic-offsets.sh ux-events
```

주의: 이 선택 관찰을 실행하면 복구 후 `ux_events_bronze` count가 baseline보다 증가할 수 있습니다. 깨끗한 기준값이 필요하면 다음 라운드 전에 reset하세요.

복구:

```bash
./scripts/ops-r1-start-taskmanager.sh
./scripts/ops-flink-list-jobs.sh
./scripts/query-olist-paimon.sh
```

포인트:

```text
RUNNING/RESTARTING 상태만으로는 끝이 아닙니다.
TaskManager 상태, Flink job 상태, checkpoint/restart 로그, Paimon count를 교차검증합니다.
lag/offset은 하나의 신호일 뿐이고, 단독으로 원인을 확정하지 않습니다.
```

## R2. savepoint 기반 재배포

UX append job 하나만 savepoint로 멈춥니다.

```bash
./scripts/ops-r2-stop-with-savepoint.sh ingest-ux-events
```

스크립트가 savepoint 경로를 `.ops-r2-last-savepoint`에 저장합니다. 그래서 restore 명령에는 path를 직접 붙이지 않아도 됩니다.
Flink UI에서 job 이름이 `insert-into_paimon_lake.bronze.ux_events_bronze`처럼 보여도, 스크립트가 `ux_events_bronze` 테이블명으로 같이 매칭합니다.

```bash
./scripts/ops-r2-restore-job-from-savepoint.sh ingest-ux-events
./scripts/ops-flink-list-jobs.sh
./scripts/query-olist-paimon.sh
```

멘토 시연:

```bash
./scripts/ops-r2-restore-bad-savepoint-demo.sh ingest-ux-events
```

포인트:

```text
운영형 복구는 "다시 실행"이 아니라 "어느 상태에서 이어갈지"를 명시하는 일입니다.
다만 이어받을 state가 깨져 있으면 resume은 복구가 아니라 같은 실패 반복입니다.
학생 핸즈온은 KEEP 경로, 멘토 시연은 DISCARD 판단을 보여줍니다.
```

## R3. Kafka ISR 부족 (acks=all 쓰기 실패)

전제: olist topic은 `RF=2 + min.insync.replicas=2`이고 broker가 `kafka`, `kafka2` 두 대 떠 있어야 합니다(기준 상태에서 자동 구성). 확인:

```bash
docker compose -f docker-compose.lite.yml exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic ux-events
# ReplicationFactor: 2, Isr: 1,2 이면 정상
```

주입:

```bash
./scripts/ops-r3-break-kafka-isr.sh            # kafka2 정지 -> ISR 2->1
./scripts/produce-olist-ux-events.sh --max-events 200
```

관찰할 것:

```text
kafka2가 내려가면 ISR이 1로 줄어 min.insync.replicas=2를 못 채웁니다.
acks=all producer는 NotEnoughReplicasException으로 실패하고, producer는 "delivery failure"로 비0 종료합니다.
controller/leader는 kafka에 남아 consumer(Flink)는 계속 읽힙니다 -> 읽기는 정상, 쓰기 경로만 막힙니다.
즉 "코드 문제가 아니라 topic 내구성 설정 + broker 가용성" 문제임을 증거로 봅니다.
```

복구:

```bash
./scripts/ops-r3-fix-kafka-isr.sh              # kafka2 재기동 -> ISR 1->2
./scripts/produce-olist-ux-events.sh --max-events 200
```

주의:

```text
단일 broker(RF=1)에서는 min.insync.replicas=2를 걸어도 쓰기가 그냥 성공합니다. ISR이 항상 1뿐이라 기준이 적용되지 않기 때문입니다.
그래서 이 라운드는 kafka2(두 번째 broker)가 반드시 떠 있어야 성립합니다.
복구 후 replay를 한 번 더 하면 ux_events_bronze는 append라 count가 증가할 수 있습니다. 깨끗한 기준값이 필요하면 reset 후 다시 시작합니다.
```

## R4. 잘못된 payload 주입

주입:

```bash
./scripts/ops-r4-produce-bad-ux-event.sh
```

관찰:

```bash
docker compose -f docker-compose.lite.yml logs --tail=120 flink-taskmanager
./scripts/ops-flink-list-jobs.sh
```

복구:

```bash
./scripts/reset-olist-kafka-topics.sh
./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon-streaming.sh all
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh
./scripts/query-olist-paimon.sh
```

포인트:

```text
Kafka에 이미 들어간 잘못된 메시지는 수정되지 않습니다. 원천을 다시 만들거나, 처리 로직에서 격리/필터링해야 합니다.
현재 UX job은 price를 DECIMAL로 엄격하게 CAST합니다. 그래서 bad record가 Paimon raw_json까지 남기 전에 Flink operator에서 실패할 수 있습니다.
이 라운드의 증거는 Kafka raw payload와 Flink log입니다. 운영형 fix는 전체 리셋이 아니라 quarantine/tolerant parsing입니다.
아래 리셋은 수업용으로 기준 상태를 빨리 회복하기 위한 편의 조치입니다.
```

## R5. Iceberg mart empty/누락

주입:

```bash
./scripts/ops-r5-empty-iceberg-mart.sh
./scripts/query-iceberg-tables.sh
```

복구 기준점 확인:

```bash
./scripts/ops-r5-find-iceberg-recovery-point.sh
```

복구 (방법 A — rebuild, 권위 있는 정답):

```bash
./scripts/trigger-airflow-pipeline.sh
./scripts/query-iceberg-tables.sh
./scripts/query-bi-metrics.sh
```

복구 (방법 B — R5b: snapshot rollback, 즉시):

```bash
# 위 find-recovery-point가 출력한 RECOVERY_POINT_CANDIDATE snapshot_id 사용
./scripts/ops-r5b-rollback-iceberg-mart.sh <snapshot_id>
./scripts/query-iceberg-tables.sh
./scripts/query-bi-metrics.sh
```

UI:

```text
Airflow: http://localhost:8080
ID/PW: admin / admin
```

포인트:

```text
Flink/Paimon이 정상이고 DAG가 초록이어도 Iceberg mart가 비어 있으면 BI는 실패합니다.
과거에는 이 검증이 없어 방치됐고, 현재 프로젝트는 validate_bi_metric_counts로 이 문제를 잡도록 보강했습니다.
drop이 아니라 empty table을 만드는 이유는 실제 사례가 "테이블이 없음"이 아니라 "테이블은 있는데 데이터가 없음"에 가까웠기 때문입니다.
복구는 두 갈래입니다. (A) Airflow rebuild는 원천(Paimon)에서 다시 derive하는 권위 있는 복구이고,
(B) rollback(R5b)은 metadata 포인터를 직전 정상 snapshot으로 되돌리는 즉시 복구입니다.
rollback은 빠르지만 그 snapshot 이후 commit을 전부 폐기하므로, "그래도 되는 상황"에서만 씁니다.
time travel/find-recovery-point는 먼저 "어느 snapshot까지 정상인가"를 확정하는 단계이고, rollback은 그 다음 결정/실행입니다.
```

## R6. StarRocks external metadata refresh

R6는 독립적인 파괴 주입이 아니라, R5/Airflow rebuild 이후 serving 계층을 다시 맞추는 검증 라운드입니다.

refresh 전 관찰:

```bash
./scripts/query-bi-metrics.sh
```

복구:

```bash
./scripts/ops-r6-refresh-starrocks-iceberg.sh
./scripts/query-bi-metrics.sh
```

포인트:

```text
데이터 파일과 catalog가 정상이어도 조회 계층의 metadata cache가 stale할 수 있습니다.
refresh 전후 숫자가 같으면 cache가 이미 fresh한 것입니다. 그 경우도 실패가 아니라 "조회 계층은 별도로 확인해야 한다"는 결론은 같습니다.
가능하면 Paimon/Iceberg native count, StarRocks count, BI metric을 3자 비교합니다.
```

## R7. (확장) Small file 문제와 컴팩션

시간이 남을 때 진행하는 확장 라운드입니다. baseline은 건드리지 않고 `iceberg_lake.opsdemo` 데모 테이블만 씁니다.

주입(작은 commit 다수 → small file):

```bash
./scripts/ops-r7-make-small-files.sh
```

복구(컴팩션):

```bash
./scripts/ops-r7-compact-small-files.sh
```

정리:

```bash
./scripts/ops-r7-reset-small-files.sh
```

포인트:

```text
INSERT 1건마다 Iceberg commit 1개 = 작은 데이터 파일 1개. 20번이면 행 20개에 파일 20개입니다(read amplification).
rewrite_data_files가 작은 파일을 큰 파일로 bin-pack합니다. 행 수는 그대로, 파일 수/오버헤드만 감소합니다.
스트리밍/빈번 commit 환경에서 컴팩션은 선택이 아니라 정기 유지보수입니다.
```

## R8. (멘토 시연) Spark driver OOM

학생 핸즈온이 아니라 멘토 시연입니다. 데이터는 바뀌지 않습니다(컴퓨트 자원 장애).

```bash
./scripts/ops-r8-spark-driver-oom-demo.sh
```

포인트:

```text
--driver-memory를 작게(512m) 준 spark-submit에서 큰 결과를 collect()하면 driver heap이 터집니다(java.lang.OutOfMemoryError).
OOM은 이 spark-submit JVM 안에 갇혀 다른 컨테이너에는 영향이 없습니다(공유 16GB 보호).
운영형 fix는 메모리를 무작정 늘리는 게 아니라 collect 회피/집계·limit/스큐 점검입니다.
주의: Mac mini는 메모리 무제한 컨테이너 + 16GB 공유라, 캡 없는 OOM 주입은 전체 스택을 위협하므로 하지 않습니다.
```

## 마지막 정리

```bash
./scripts/ops-baseline-evidence.sh
```
