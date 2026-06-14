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

출력에서 `file:/...` 또는 `file:///...`로 시작하는 savepoint 경로를 복사합니다.

```bash
./scripts/ops-r2-restore-job-from-savepoint.sh ingest-ux-events '<savepoint-path>'
./scripts/ops-flink-list-jobs.sh
./scripts/query-olist-paimon.sh
```

포인트:

```text
운영형 복구는 "다시 실행"이 아니라 "어느 상태에서 이어갈지"를 명시하는 일입니다.
```

## R3. Kafka ISR 설정 오류

주입:

```bash
TOPIC=ux-events ./scripts/ops-r3-break-kafka-isr.sh
./scripts/produce-olist-ux-events.sh
```

관찰할 것:

```text
acks=all producer가 실패합니다. 브로커 1대인데 min.insync.replicas=2를 요구하기 때문입니다.
```

복구:

```bash
TOPIC=ux-events ./scripts/ops-r3-fix-kafka-isr.sh
./scripts/produce-olist-ux-events.sh
```

주의:

```text
복구 후 replay를 한 번 더 하면 ux_events_bronze는 append라 count가 증가할 수 있습니다.
깨끗한 기준값이 필요하면 reset 후 다시 시작합니다.
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
```

## R5. Iceberg mart empty/누락

주입:

```bash
./scripts/ops-r5-drop-iceberg-mart.sh
./scripts/query-iceberg-tables.sh
```

복구:

```bash
./scripts/trigger-airflow-pipeline.sh
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
```

## R6. StarRocks external metadata refresh

관찰:

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
```

## 마지막 정리

```bash
./scripts/ops-baseline-evidence.sh
```
