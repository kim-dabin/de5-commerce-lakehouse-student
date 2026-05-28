# 07 Realtime OLAP

## 목표

신선한 커머스 이벤트를 StarRocks 기반 realtime OLAP layer에 적재하고 빠르게 조회합니다.

```text
Kafka commerce-events
  -> StarRocks commerce_events_rt
  -> realtime event/category/minute views
  -> Realtime Ops BI
```

이 로컬 실습은 StarRocks를 realtime OLAP engine으로 사용합니다. Iceberg batch analytics 경로와 의도적으로 분리해, 수업에서 "지금 무슨 일이 일어나고 있는가?"와 "믿을 수 있는 일 단위 결과는 무엇인가?"를 비교할 수 있게 합니다.

## 실행

```bash
docker compose -f docker-compose.lite.yml up -d --build starrocks-fe starrocks-cn
./scripts/reset-realtime-olap.sh
./scripts/load-realtime-olap-from-kafka.sh
./scripts/query-realtime-olap.sh
./scripts/query-realtime-olap-metrics.sh
```

## Shared-data mode

이 실습은 StarRocks shared-data quickstart 구조를 따릅니다.

```text
StarRocks FE
StarRocks CN
MinIO object storage
```

이전 all-in-one shared-nothing container는 일부 Apple Silicon Docker Desktop 환경에서 FE는 뜨지만 BE가 안정적으로 유지되지 않는 문제가 있었습니다. Shared-data mode는 table data를 MinIO에 저장하고 CN을 compute node로 사용하므로 이 lakehouse 실습에 더 잘 맞습니다.

Kafka production을 먼저 실행합니다.

```bash
./scripts/reset-kafka-topic.sh
./scripts/produce-kafka.sh
```

## Table과 view

- `de5_realtime_olap.commerce_events_rt`: realtime OLAP용 raw-ish event table
- `de5_realtime_olap.commerce_event_type_realtime`: 현재 event type 집계
- `de5_realtime_olap.commerce_category_realtime`: 현재 category 집계
- `de5_realtime_olap.commerce_minute_event_type_realtime`: minute/event type 집계

## BI 실행

```bash
./scripts/start-streamlit-bi.sh
```

브라우저에서 아래 주소로 접속합니다.

```text
http://127.0.0.1:8501
```

대시보드는 두 관점을 나누어 보여줍니다.

- `Realtime Ops · StarRocks`: 지금 들어온 이벤트 기준
- `Daily Business · Iceberg`: batch로 정리한 기준 결과

## 수업 중 사용할 질문

- Realtime OLAP은 "지금 무슨 일이 일어나고 있는가?"에 답합니다.
- Batch Lakehouse BI는 "믿을 수 있는 비즈니스 결과는 무엇인가?"에 답합니다.
- 이 데이터의 purchase event는 행동 이벤트입니다. 프로덕션 수준의 매출을 만들려면 order, payment, refund CDC가 추가로 필요할 수 있습니다.
