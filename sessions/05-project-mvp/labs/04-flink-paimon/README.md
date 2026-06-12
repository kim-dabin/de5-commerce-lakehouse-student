# 04 Flink to Paimon Bronze

## 목표

Kafka의 Olist 이벤트를 Flink SQL streaming job으로 계속 읽어 Paimon Bronze/current-state 테이블에 저장합니다.

이번 차시의 핵심은 "Flink job이 실행됐다"가 아니라, Kafka -> Flink -> Paimon 각 계층에서 정상이라고 말할 증거를 남기는 것입니다.

```text
ux-events
  -> Flink SQL
  -> paimon_lake.bronze.ux_events_bronze
  -> append 행동 로그

review-events
  -> Flink SQL
  -> paimon_lake.bronze.review_current
  -> review_id primary key current-state

order-status-events
  -> Flink SQL
  -> paimon_lake.bronze.order_current
  -> order_id primary key current-state
```

## 왜 Paimon을 쓰는가

UXLog 자체는 append로 쌓아도 됩니다. 같은 사용자가 같은 상품을 두 번 보면 두 번 모두 의미 있는 행동입니다.

하지만 review와 order는 다릅니다.

- `review_current`: 같은 `review_id`에 리뷰 생성, 감성 분석, 답변 상태가 나중에 붙습니다.
- `order_current`: 같은 `order_id`에 주문 생성, 승인, 배송, 완료/취소 상태가 붙습니다.

따라서 이 데이터는 이벤트처럼 들어오지만 저장 모델은 current-state입니다. Paimon primary key table은 이런 "계속 보강되는 엔티티 상태"를 Lakehouse 안에서 다루기 위한 좋은 실습 대상입니다.

또한 Bronze에는 parsed column과 `raw_json`을 같이 남깁니다. 파싱 로직이 틀렸거나 스키마가 바뀌면 parsed column만으로는 원인을 찾기 어렵습니다. raw payload를 남겨야 나중에 다시 해석할 수 있습니다.

## 실행

```bash
docker compose -f docker-compose.lite.yml up -d --build \
  kafka kafka-init kafka-ui \
  minio minio-init \
  iceberg-postgres iceberg-rest \
  flink-jobmanager flink-taskmanager

./scripts/reset-olist-kafka-topics.sh
./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon-streaming.sh

./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh

./scripts/query-olist-paimon.sh
```

## 기대 결과

샘플 기준 검증 결과입니다.

```text
ux_events_bronze      16,693 rows
review_events input    5,943 rows
review_current         1,971 rows
order_events input     7,886 rows
order_current          2,000 rows
```

`review_events`가 5,943건인데 `review_current`가 1,971건인 이유는 같은 `review_id`에 대해 `review_created`, `sentiment_scored`, `review_answered`가 들어오고, Paimon primary key table이 이를 최신 상태로 접기 때문입니다.

`order_status_events`가 7,886건인데 `order_current`가 2,000건인 이유도 같습니다. 같은 `order_id`의 상태 변경 이력이 하나의 최신 주문 상태로 접힙니다.

## Streaming ingestion 주의점

이번 실습의 기준 경로는 streaming job입니다.

```text
Flink job은 Kafka topic을 계속 구독합니다.
Flink UI에서 ingestion job이 RUNNING이면 정상입니다.
새로 replay한 메시지는 job이 살아 있는 동안 계속 Paimon에 반영됩니다.
```

Streaming SQL은 job별로 분리되어 있습니다.

```text
13a-insert-olist-ux-events-streaming.sql      -> ux_events_bronze
13b-insert-olist-review-current-streaming.sql -> review_current
13c-insert-olist-order-current-streaming.sql  -> order_current
```

이렇게 나눈 이유는 운영 관점에서 각 ingestion path를 별도의 Flink job으로 다루기 위해서입니다.
나중에 savepoint/restore를 실습할 때도 UX job, review job, order job을 따로 멈추고 복구할 수 있습니다.

저장 모델 기준으로도 나누어 실행할 수 있습니다.

```bash
# append fact: 발생 사실을 계속 쌓는 UX 행동 로그
./scripts/run-flink-olist-paimon-streaming.sh append

# upsert current-state: 같은 key의 최신 상태를 유지하는 리뷰/주문
./scripts/run-flink-olist-paimon-streaming.sh upsert
```

기존 `13-insert-olist-streaming.sql`는 레거시 안내 파일입니다. 직접 실행하지 말고 아래 스크립트를 사용합니다.

```bash
./scripts/run-flink-olist-paimon-streaming.sh
```

환경 문제를 분리하거나 빠르게 count만 재현해야 할 때는 보조 경로로 bounded job을 사용할 수 있습니다.

```bash
./scripts/run-flink-olist-paimon.sh
```

bounded job은 job 시작 시점의 latest offset까지만 읽고 종료합니다. 그래서 Flink UI에서 `FINISHED`가 정상입니다. 반대로 streaming job은 운영형 경로이므로 "job이 끝났는가"보다 "checkpoint가 계속 정상적으로 도는가"가 더 중요합니다.

## Iceberg-compatible metadata

현재 Paimon 테이블은 Iceberg-compatible metadata 생성을 켜둔 상태로 생성합니다.

```text
metadata.iceberg.storage = rest-catalog
metadata.iceberg.rest.uri = http://iceberg-rest:8181
```

Paimon snapshot commit 이후 Iceberg-compatible metadata를 생성하고, 같은 테이블을 Iceberg REST Catalog에도 등록합니다. 그래서 Spark/BI 쪽에서는 `iceberg_lake.bronze.*` 경로로 Paimon table을 Iceberg reader 관점에서 확인할 수 있습니다.

`ux_events_bronze` 같은 append table은 Iceberg reader가 그대로 읽기 쉽습니다. 반면 `review_current`, `order_current` 같은 primary key table은 Paimon의 LSM 구조 때문에 Iceberg reader가 항상 최신 상태를 즉시 merge해서 읽는 것은 아닙니다. 그래서 local demo에서는 full compaction을 빠르게 발생시키도록 `full-compaction.delta-commits = 1`을 둡니다. 운영에서는 테이블 크기와 compaction 비용을 보고 더 긴 주기로 조정합니다.

주의할 점은 Iceberg-compatible metadata의 `operation`이 Paimon의 `commit_kind`와 1:1로 대응하지 않는다는 것입니다. Paimon에서는 snapshot 2가 `COMPACT`로 보일 수 있지만, Iceberg metadata table에서는 같은 시점이 `operation = append`로 보일 수 있습니다. Iceberg 쪽 `operation`은 Iceberg reader가 보는 metadata snapshot의 갱신 방식이고, Paimon 내부 compaction 여부를 진단하려면 반드시 Paimon의 `$snapshots`, `$files`를 봐야 합니다.

## 운영 관찰 확장: snapshot / files / compaction

row count는 결과를 보여주고, snapshot은 commit 이력을 보여주고, files는 내부 저장 상태를 보여줍니다.
운영에서는 셋을 같이 봐야 "데이터가 들어왔다"에서 끝나지 않고 "정상적으로 쌓이고 정리되고 있는가"까지 판단할 수 있습니다.

```bash
./scripts/query-olist-paimon-ops.sh
```

이 스크립트는 아래 내용을 확인합니다.

- `review_current$snapshots`: `APPEND`, `COMPACT` commit 흐름
- 특정 시점 직전의 snapshot 후보: 장애/배포/데이터 이상 직전 정상 기준점
- snapshot id 기준 time travel: 과거 테이블 상태를 직접 읽는 방법
- `review_current$files`: level별 파일 분포와 L0 파일 상태
- bucket x level 분포: 특정 bucket에 변경 파일이 몰리는지
- `ux_events_bronze$snapshots`, `ux_events_bronze$files`: append table과 primary key table의 차이

실무에서 snapshot은 "멋있는 time travel 기능"이라기보다 운영 기준점입니다. 문제가 생기면 현재 row count만 보지 않고 아래 질문부터 던집니다.

```text
문제가 생기기 직전 정상 snapshot은 무엇인가?
잘못된 commit은 APPEND였나 COMPACT였나?
특정 시점 이후 row 수가 갑자기 달라졌는가?
time travel로 문제 전 상태를 읽을 수 있는가?
rollback이 필요하다면 어느 snapshot으로 돌아가야 하는가?
```

수업에서 볼 핵심은 아래입니다.

```text
review_current/order_current 같은 primary key table은 upsert를 처리하기 위해
내부적으로 LSM 구조를 사용합니다.

L0에 작은 변경 파일이 많이 쌓이면 읽을 때 병합해야 할 것이 많아지고,
compaction은 이 파일들을 더 읽기 좋은 level로 정리합니다.
```

작은 로컬 실습에서는 운영처럼 극단적인 파일 분포가 나오지 않을 수 있습니다. 그래도 system table을 통해 "Paimon은 그냥 파일을 저장하는 것이 아니라 commit 이력과 파일 상태를 함께 관리한다"는 점을 확인할 수 있습니다.

local demo에서는 `review_current`, `order_current`에 `full-compaction.delta-commits = 1`을 두었기 때문에 L0 파일이 비어 있고 더 높은 level 파일만 보일 수 있습니다. 이 경우는 "compaction이 빠르게 따라잡았다"는 관찰 포인트로 설명합니다.

JupyterLab의 `notebooks/de5-spark-starter.ipynb`에는 같은 내용을 PySpark로 따라가는 셀이 들어 있습니다. 수업에서는 아래 순서로 확인합니다.

1. `paimon_lake.bronze.review_current$snapshots`로 Paimon commit 기준점 확인
2. `VERSION AS OF <snapshot_id>`로 Paimon 과거 상태 조회
3. `iceberg_lake.bronze.review_current.snapshots`로 Iceberg-compatible metadata 확인
4. `review_current$files`로 compaction 이후 level 파일 분포 확인

Jupyter에서 보는 증거는 아래 네 가지로 나눕니다.

```text
row_count         : 현재 결과
Paimon $snapshots : commit 이력과 정상 기준점
Paimon $files     : level, bucket, file_count로 보는 저장 상태
Iceberg snapshots : reader 호환 metadata 이력
```

Paimon 파일은 snapshot에서 시작해 manifest를 거쳐 실제 data file을 읽는 구조입니다.

```text
snapshot
  -> manifest list
  -> manifest
  -> data-*.parquet

schema
  -> 테이블 구조 버전
```

따라서 `data-*.parquet`만 보는 것이 아니라, snapshot/manifest/files를 함께 봐야 "어떤 시점에 어떤 파일을 읽는가"를 설명할 수 있습니다. small file 문제는 checkpoint 간격, write buffer, bucket 수, compaction 정책과 연결해서 봅니다.

Paimon과 Iceberg snapshot 조회는 비슷하지만 보는 컬럼 이름이 다릅니다.

```text
Paimon  : table$snapshots / commit_time   / commit_kind  / compaction 진단 가능
Iceberg : table.snapshots  / committed_at / operation    / reader metadata 기준
```

따라서 수업에서는 이렇게 정리합니다.

```text
Paimon COMPACT가 Iceberg operation=compact로 보일 거라고 기대하면 안 됩니다.
compaction은 Paimon 내부 저장 상태 진단이고,
Iceberg-compatible metadata는 다른 엔진이 읽기 위한 호환 메타데이터입니다.
```

## 검증 체인

Paimon row count 하나만으로 전체 정상이라고 말하지 않습니다.

| 계층 | 내가 확인할 증거 | 정상 기준 | 이상할 때 다음 확인 |
|---|---|---|---|
| Kafka topic | topic/message/offset | 메시지 존재, offset 증가 | producer log, listener, topic |
| Flink job | SQL Client output, Flink UI/log | job 성공 또는 실패 원인 확인 | source/sink connector, JSON parsing |
| Paimon Bronze | row count, event type, raw_json | 기대 count와 분포 일치 | PK, warehouse path, raw payload |

## 수업 중 사용할 질문

- `ux_events_bronze`는 append인데 `review_current`는 upsert인 이유는 무엇일까요?
- 같은 review 데이터를 한 번 더 replay하면 row count는 어떻게 될까요?
- Paimon row count가 0이면 Kafka, Flink, Paimon 중 어디부터 확인해야 할까요?
- `raw_json`을 남기는 비용과 얻는 이점은 무엇일까요?
