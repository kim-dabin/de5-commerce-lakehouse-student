# 07 Realtime OLAP and BI

## 목표

Olist UXLog와 Review/Order current-state를 StarRocks Paimon external catalog로 조회하고, Streamlit BI에서 lakehouse 운영 지표와 batch 지표를 비교합니다.

```text
Kafka ux/review/order events
  -> Flink
  -> Paimon ux_events_bronze / review_current / order_current
  -> StarRocks Paimon external catalog
  -> realtime event/category/minute views
  -> Streamlit Lakehouse Ops BI

Iceberg analytics tables
  -> StarRocks Iceberg external catalog
  -> Streamlit Daily Business BI
```

이 실습은 같은 Olist 기반 데이터를 두 가지 질문으로 나누어 봅니다.

- Realtime OLAP: 지금 무슨 일이 일어나고 있는가?
- Batch Lakehouse BI: 믿을 수 있는 기준 결과는 무엇인가?

## 실행

전체 Olist BI 파이프라인을 한 번에 실행할 수 있습니다.

```bash
./scripts/run-olist-bi-pipeline.sh
```

Realtime OLAP 구간만 다시 실행하려면 아래 명령을 사용합니다.

```bash
docker compose -f docker-compose.lite.yml up -d --build \
  kafka kafka-init \
  flink-jobmanager flink-taskmanager \
  starrocks-fe starrocks-cn
./scripts/reset-olist-kafka-topics.sh
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh
./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon.sh
./scripts/reset-realtime-olap.sh
./scripts/query-realtime-olap.sh
./scripts/query-realtime-olap-metrics.sh
```

이 로컬 실습은 StarRocks internal table에 UXLog를 다시 적재하지 않습니다. StarRocks는 Paimon external catalog와 Iceberg external catalog를 만들고, Paimon Bronze/current table과 Iceberg Analytics table을 직접 조회하는 OLAP serving/query layer로 동작합니다.

현재 StarRocks view 이름은 기존 스크립트 호환을 위해 `commerce_events_rt_typed`, `commerce_event_type_realtime` 형식을 유지합니다. 물리 데이터는 Paimon `ux_events_bronze`, `review_current`, `order_current`에 있습니다.

## 기대 결과

External catalog/view setup 성공 메시지입니다.

```text
reset=starrocks_catalogs catalogs=paimon_olist,iceberg_olist views=de5_realtime_olap
```

대표 realtime 지표입니다.

```text
total_events       16,693
users               2,875
sessions            2,875
products            1,470
revenue           265,036.00
```

리뷰 영향 분석 대표 지표입니다.

```text
review_seen_pairs                 2,940
cart_click_after_review_rate      73.91%
purchase_after_review_rate        68.98%
pdp_exit_rate                     25.90%
```

## Shared-data mode

이 실습은 StarRocks shared-data quickstart 구조를 따릅니다.

```text
StarRocks FE
StarRocks CN
MinIO object storage
```

Apple Silicon Docker Desktop에서 all-in-one shared-nothing container가 불안정할 수 있어, 수업용 로컬 스택은 shared-data mode로 구성했습니다.

## BI 실행

```bash
./scripts/start-streamlit-bi.sh
```

브라우저에서 아래 주소로 접속합니다.

```text
http://127.0.0.1:8501
```

대시보드는 두 관점을 나누어 보여줍니다.

- `Lakehouse Ops · StarRocks(Paimon)`: Paimon Bronze/current table을 StarRocks로 직접 조회
- `Daily Business · Iceberg`: Iceberg Analytics table을 StarRocks Iceberg external catalog로 직접 조회

## 수업 중 사용할 질문

- Realtime OLAP은 왜 빠른 조회를 위해 StarRocks 같은 serving layer를 둘까요?
- 같은 Paimon 데이터를 StarRocks external catalog로 조회할 때와 StarRocks internal table로 복사할 때의 차이는 무엇일까요?
- Batch BI와 Realtime BI가 같은 데이터를 다르게 보여줄 때 어떤 기준으로 해석해야 할까요?
- UXLog의 purchase는 행동 이벤트입니다. 공식 매출 지표로 쓰려면 어떤 주문/결제/환불 데이터가 더 필요할까요?
