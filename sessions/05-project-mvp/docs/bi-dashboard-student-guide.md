# BI 대시보드 학생 가이드

이 문서는 수강생이 최종 프로젝트에서 BI 화면을 어떻게 이해하고, 포트폴리오에 어떤 메시지로 가져갈지 정리한 가이드입니다.

## 한 줄 요약

이번 프로젝트의 BI는 "리뷰를 본 사용자가 실제로 장바구니/구매로 이어졌는가"와 "부정 리뷰가 많은 상품/카테고리에서 이탈이 커지는가"를 확인하는 커머스 운영/분석 화면입니다.

## 전체 데이터 흐름

```text
Olist CSV
  -> Event Generator
  -> Kafka ux/review/order topics
  -> Flink bounded ingestion
  -> Paimon Bronze/current-state
  -> StarRocks Paimon external catalog
  -> Streamlit Lakehouse Ops BI

Paimon Bronze/current-state
  -> Spark batch transform
  -> Iceberg Analytics tables
  -> StarRocks Iceberg external catalog
  -> Streamlit Daily Business BI
```

StarRocks는 raw UXLog를 내부 테이블로 복사하지 않습니다. Paimon external catalog와 Iceberg external catalog를 통해 Lakehouse table을 직접 조회하는 OLAP serving/query layer로 사용합니다.

## 왜 BI가 중요한가

Kafka, Flink, Paimon, Spark, Iceberg, StarRocks를 각각 실행하는 것만으로는 포트폴리오 메시지가 약합니다.

BI 화면은 아래 질문에 답하기 위해 존재합니다.

- 이 파이프라인이 어떤 비즈니스 질문에 답하는가?
- append fact와 current-state entity를 왜 같이 다루는가?
- Realtime OLAP과 Batch Lakehouse BI는 어떻게 다르게 해석하는가?
- 데이터가 어디까지 정상인지 어떤 지표로 확인하는가?

## 탭 1. Lakehouse Ops · StarRocks(Paimon)

이 탭은 Paimon `ux_events_bronze`, `review_current`, `order_current`를 StarRocks Paimon external catalog로 직접 조회합니다.

해석 관점은 "지금 Lakehouse에 들어온 데이터가 운영적으로 어떻게 보이는가"입니다.

### 상단 KPI

| 지표 | 의미 |
|---|---|
| Events | 들어온 UXLog 이벤트 수 |
| Users | 이벤트에 등장한 사용자 수 |
| Sessions | 사용자 세션 수 |
| Revenue | purchase 행동 이벤트 기준 금액 합계 |

주의: 여기서 purchase는 행동 이벤트입니다. 공식 정산 매출로 쓰려면 주문/결제/환불 데이터 기준 검증이 추가로 필요합니다.

### Minute-level Event Flow

분 단위로 이벤트가 어떻게 흐르는지 봅니다.

수업 메시지:

```text
Kafka/Flink/Paimon이 잘 동작하면 이벤트가 시간 흐름에 따라 관찰 가능해야 한다.
```

### Realtime Revenue by Category

카테고리별 purchase 행동 금액을 빠르게 봅니다.

수업 메시지:

```text
운영자는 지금 어떤 카테고리에서 이벤트와 매출 행동이 많이 발생하는지 빠르게 확인한다.
```

### Paimon Current-state Tables via StarRocks

`review_current`, `order_current`를 조회합니다.

| 패널 | 의미 |
|---|---|
| Review Current by Sentiment | 리뷰 감성 상태 분포 |
| Order Current by Status | 주문 현재 상태 분포 |

수업 메시지:

```text
UXLog는 append로 쌓이고, Review/Order는 같은 key의 최신 상태로 접힌다.
Paimon은 이 두 모델을 같은 Lakehouse 계층에서 보여준다.
```

### Review Impact

가장 중요한 분석 영역입니다.

질문:

```text
리뷰를 본 사용자가 장바구니나 구매로 이어지는가?
부정 리뷰가 많은 상품/카테고리에서 상세페이지 이탈률이 높아지는가?
```

대표 지표:

| 지표 | 의미 |
|---|---|
| Review Seen Pairs | 리뷰를 본 session-product 조합 수 |
| Cart Click After Review | 리뷰를 본 뒤 장바구니 행동으로 이어진 비율 |
| Purchase After Review | 리뷰를 본 뒤 구매 행동으로 이어진 비율 |
| PDP Exit Rate | 상품 상세를 본 뒤 다음 전환 행동 없이 이탈한 비율 |

샘플 기준 대표 값:

```text
review_seen_pairs                 2,940
cart_click_after_review_rate      73.91%
purchase_after_review_rate        68.98%
pdp_exit_rate                     25.90%
```

### Negative Review Ratio vs PDP Exit by Category

카테고리별 부정 리뷰 비율과 PDP 이탈률을 비교합니다.

수업 메시지:

```text
부정 리뷰가 많은 카테고리에서 사용자가 상세페이지를 보고 이탈하는지 가설을 세운다.
```

### Products to Inspect

부정 리뷰 비율, 평균 평점, 상품 조회 세션, PDP 이탈률, 구매율을 함께 보여주는 상품 후보 테이블입니다.

수업 메시지:

```text
실무에서는 이 테이블을 보고 상품 상세, 리뷰 품질, 배송/재고 이슈를 같이 확인한다.
```

## 탭 2. Daily Business · Iceberg

이 탭은 Spark가 Paimon 데이터를 정리해 Iceberg Analytics table에 쓰고, StarRocks Iceberg external catalog로 조회합니다.

해석 관점은 "빠른 운영 화면"이 아니라 "믿을 수 있는 기준 결과"입니다.

### 상단 KPI

| 지표 | 의미 |
|---|---|
| Events | batch 기준 전체 이벤트 수 |
| Users | batch 기준 사용자 수 |
| Sessions | batch 기준 세션 수 |
| Revenue | batch 기준 purchase 행동 금액 |

샘플 기준 대표 값:

```text
total_events       16,693
users               2,875
sessions            2,875
orders              1,968
revenue           265,036.00
reviews             1,971
avg_rating             3.93
negative_reviews     367
```

### Batch Decision KPIs

| 지표 | 의미 |
|---|---|
| View -> Cart | 상품 조회 후 장바구니로 이어진 비율 |
| View -> Purchase | 상품 조회 후 구매로 이어진 비율 |
| Review Seen -> Purchase | 리뷰 노출 후 구매로 이어진 비율 |
| PDP Exit | 상세페이지 이탈률 |

수업 메시지:

```text
이 지표는 실시간 운영 알림보다 다음날 회고/의사결정에 가까운 기준 지표다.
```

### Daily Revenue and Conversion Trend

일자별 revenue와 전환율 흐름을 봅니다.

수업 메시지:

```text
Iceberg batch mart는 시간 단위 회고와 재처리에 유리하다.
```

### Batch Funnel Sessions

상품 조회, 리뷰 노출, 리뷰 확장, 장바구니, 구매 세션을 funnel로 봅니다.

수업 메시지:

```text
UXLog append fact를 분석 가능한 funnel 지표로 정리한다.
```

### Trusted Review Impact

실시간 탭에서 본 리뷰 영향 지표를 batch 기준으로 다시 계산합니다.

수업 메시지:

```text
Realtime 화면의 빠른 숫자가 batch 기준으로도 설명 가능한지 확인한다.
```

### Category Business Health

카테고리별 매출, 리뷰 커버리지, 부정 리뷰 비율을 함께 봅니다.

수업 메시지:

```text
매출이 높은데 부정 리뷰 비율과 이탈률도 높다면 운영적으로 먼저 점검할 후보가 된다.
```

### Analytics Table

일자/카테고리별 집계 테이블입니다.

수업 메시지:

```text
BI 차트는 결국 검증 가능한 집계 테이블에서 나온다.
차트가 이상하면 원천 집계 테이블부터 확인한다.
```

## 수강생이 가져갈 포트폴리오 메시지

1. 공개 Olist 데이터를 수업용 UXLog/Review/Order 이벤트로 재구성했다.
2. UXLog는 append fact로, Review/Order는 current-state entity로 모델링했다.
3. Kafka로 이벤트를 replay하고 Flink로 Paimon Lakehouse table에 적재했다.
4. Spark로 Paimon 데이터를 정리해 Iceberg Analytics table을 만들었다.
5. StarRocks는 Paimon/Iceberg external catalog를 통해 Lakehouse table을 OLAP 방식으로 조회했다.
6. Streamlit BI에서 Realtime Ops 화면과 Batch Business 화면을 분리해 보여줬다.
7. 최종 질문은 "리뷰 경험이 구매/이탈에 어떤 영향을 주는가"이다.

## 학생 발표에서 쓰기 좋은 한 문장

```text
이 프로젝트는 단순히 Kafka/Flink/Paimon을 실행하는 것이 아니라, 커머스 UXLog와 리뷰 상태 데이터를 결합해 리뷰 노출 이후의 장바구니/구매 전환과 부정 리뷰 기반 이탈 리스크를 Lakehouse + OLAP 구조로 분석하는 프로젝트입니다.
```

## 실행 명령

전체 파이프라인:

```bash
./scripts/run-olist-bi-pipeline.sh
```

BI 실행:

```bash
./scripts/start-streamlit-bi.sh
```

브라우저:

```text
http://127.0.0.1:8501
```

현재 3차시 Kafka 실습 스택만 띄운 상태라면 BI가 바로 뜨지 않을 수 있습니다. BI는 Kafka -> Flink -> Paimon -> Spark -> Iceberg -> StarRocks 경로를 준비한 뒤 확인합니다.

