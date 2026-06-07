# Olist 기반 실무형 Lakehouse 데이터 설계

## 방향

이번 프로젝트는 Olist 공개 이커머스 데이터셋을 기반으로 한다. 원본에는 주문, 상품, 리뷰의 실제 연결 관계가 있지만 클릭스트림 UXLog는 없다. 따라서 주문 발생 전 사용자 행동 흐름은 `order_id`, `customer_id`, `product_id`, `review_id` 관계를 기반으로 교육용 append 이벤트로 재구성한다.

핵심 메시지는 아래와 같다.

```text
UXLog = 사용자가 무엇을 했는가 = append fact
Review/Order = 현재 상태가 어떻게 바뀌었는가 = upsert/current-state entity
```

## 원본 데이터

Kaggle dataset: `olistbr/brazilian-ecommerce`

CSV 파서 기준 row count:

| 파일 | row count |
|---|---:|
| `olist_customers_dataset.csv` | 99,441 |
| `olist_orders_dataset.csv` | 99,441 |
| `olist_order_items_dataset.csv` | 112,650 |
| `olist_order_reviews_dataset.csv` | 99,224 |
| `olist_products_dataset.csv` | 32,951 |

주의: 리뷰 텍스트에는 줄바꿈이 들어갈 수 있으므로 `wc -l`로 리뷰 row 수를 세면 실제 CSV row보다 크게 보일 수 있다. 수업에서는 이 지점을 raw 데이터 파싱/검증 사례로 사용할 수 있다.

## 생성 데이터

생성 스크립트:

```bash
./data/generate_olist_lakehouse_events.py \
  --input-dir data/raw/olist \
  --output-dir data/derived/olist
```

전체 Olist 기준 생성 결과:

| 출력 | row count | 역할 |
|---|---:|---|
| `ux_events.jsonl` | 660,225 | Kafka append UXLog |
| `order_status_events.jsonl` | 393,481 | order current-state 변경 이벤트 |
| `review_events.jsonl` | 297,672 | review current-state 변경 이벤트 |
| `product_xref.csv` | 32,951 | 원본 product_id와 수업용 product_id 매핑 |

## Topic 설계

```text
ux-events
  key: session_id
  storage model: append

review-events
  key: review_id
  storage model: upsert/current-state

order-status-events
  key: order_id
  storage model: upsert/current-state
```

## Paimon 설계

```text
ux_events_bronze
  - append table
  - event_id 단위 행동 사실 보관
  - raw_json 보관

review_current
  - primary key: review_id
  - review_created -> sentiment_scored -> review_answered 이벤트가 같은 review_id로 접힘
  - Paimon을 쓰는 주된 명분

order_current
  - primary key: order_id
  - order_created -> order_approved -> order_shipped -> order_delivered/canceled 이벤트가 같은 order_id로 접힘
  - 주문 상태 기준 BI와 UXLog purchase 해석의 보조 기준
```

수업에서 강조할 문장:

> UXLog는 같은 사용자가 같은 상품을 두 번 보면 두 번 모두 의미 있는 행동이다. 그래서 append로 쌓는다. 반면 review는 같은 `review_id`에 분석 결과와 답변 상태가 나중에 붙는다. 그래서 Paimon primary key table로 최신 상태를 유지한다.

## 검증된 로컬 실행 흐름

```bash
docker compose -f docker-compose.lite.yml up -d --build kafka kafka-init kafka-producer flink-jobmanager flink-taskmanager
./scripts/reset-olist-kafka-topics.sh
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh
./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon.sh
./scripts/query-olist-paimon.sh
```

샘플 기준 검증 결과:

```text
ux_events_bronze row count = 16,693
review_current row count = 1,971
order_current row count = 2,000

review_events input row count = 5,943
review_current row count = 1,971
order_status_events input row count = 7,886
order_current row count = 2,000
```

해석:

```text
review_events는 변경 이벤트 로그다.
review_current는 review_id 기준 최신 상태 테이블이다.
따라서 이벤트 수보다 current row 수가 작아지는 것이 정상이다.

order_status_events도 변경 이벤트 로그다.
order_current는 order_id 기준 최신 상태 테이블이다.
따라서 주문 상태 변경 이벤트 수보다 current row 수가 작아지는 것이 정상이다.
```

## 전체 BI 파이프라인

전체 파이프라인은 아래 명령으로 검증한다.

```bash
./scripts/run-olist-bi-pipeline.sh
```

검증된 샘플 기준 결과:

| 계층 | table/metric | 값 |
|---|---|---:|
| Paimon | `ux_events_bronze` | 16,693 |
| Paimon | `review_current` | 1,971 |
| Paimon | `order_current` | 2,000 |
| Iceberg | `olist_ux_events_clean` | 16,693 |
| Iceberg | `olist_review_current` | 1,971 |
| Iceberg | `olist_order_current` | 2,000 |
| StarRocks | Paimon catalog query rows | 16,693 |
| BI | total events | 16,693 |
| BI | revenue | 265,036.00 |
| BI | average review rating | 3.93 |
| BI | negative reviews | 367 |
| BI | review seen pairs | 2,940 |
| BI | PDP exit rate after review seen | 25.90% |

## 수업에서 사용할 해석

```text
UXLog는 append fact다.
동일 사용자가 동일 상품을 여러 번 본 것은 모두 별도 행동이다.

Review와 Order는 current-state entity다.
동일 review_id/order_id에 대해 나중에 상태나 분석 결과가 보강된다.

따라서 이 프로젝트는 append와 upsert를 한 파이프라인 안에서 같이 보여준다.
Paimon은 이 두 모델을 Bronze 계층에서 함께 다루는 역할을 한다.
Iceberg는 Spark batch 결과를 기준 BI 테이블로 남긴다.
StarRocks는 Paimon external catalog를 통해 Bronze/current table을 OLAP 방식으로 빠르게 조회한다.
```
