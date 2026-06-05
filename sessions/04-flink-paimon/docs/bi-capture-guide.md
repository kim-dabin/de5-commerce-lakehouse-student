# BI 캡처 가이드

이 문서는 최종 BI 화면을 포트폴리오와 과제 제출용으로 캡처하는 방법을 정리합니다.

## 1. 전체 BI 파이프라인 실행

4차시 폴더에서 실행합니다.

```bash
cd de5-commerce-lakehouse-student/sessions/04-flink-paimon

./scripts/run-olist-bi-pipeline.sh
./scripts/start-streamlit-bi.sh
```

대시보드 주소:

```text
http://127.0.0.1:8501
```

## 2. 캡처해야 하는 화면

### 필수 1. Lakehouse Ops · StarRocks(Paimon)

이 탭은 StarRocks가 Paimon external catalog로 `ux_events_bronze`, `review_current`, `order_current`를 조회한 화면입니다.

캡처에 보이면 좋은 것:

- 상단 KPI: Events, Users, Sessions, Revenue
- `Minute-level Event Flow`
- `Realtime Revenue by Category`
- `Paimon Current-state Tables via StarRocks`

해석 문장:

```text
Paimon Bronze/current-state table을 StarRocks external catalog로 조회해 운영성 지표를 확인한다.
```

### 필수 2. Daily Business · Iceberg

이 탭은 Spark가 만든 Iceberg Analytics table을 StarRocks Iceberg external catalog로 조회한 화면입니다.

캡처에 보이면 좋은 것:

- 상단 KPI: Events, Users, Sessions, Revenue
- 리뷰 KPI: Reviews, Reviewed Products, Avg Rating, Negative Reviews
- `Batch Decision KPIs`
- `Daily Revenue and Conversion Trend`
- `Batch Funnel Sessions`

해석 문장:

```text
Spark batch transform이 만든 Iceberg 기준 테이블을 StarRocks external catalog로 조회해 일 단위 의사결정 지표를 확인한다.
```

## 3. macOS 캡처 방법

브라우저에서 대시보드를 연 뒤:

```text
Command + Shift + 4
```

영역을 선택해 캡처합니다.

전체 화면을 캡처하려면:

```text
Command + Shift + 5
```

브라우저 창 캡처를 선택합니다.

## 4. Windows 캡처 방법

브라우저에서 대시보드를 연 뒤:

```text
Windows + Shift + S
```

영역을 선택해 캡처합니다.

## 5. 제출/포트폴리오에 적을 설명

캡처만 제출하지 말고 아래 3문장을 함께 적습니다.

```text
1. Lakehouse Ops 탭은 Paimon table을 StarRocks Paimon external catalog로 조회한 운영성 BI입니다.
2. Daily Business 탭은 Spark가 만든 Iceberg Analytics table을 StarRocks Iceberg external catalog로 조회한 기준 BI입니다.
3. UXLog는 append fact이고, review/order는 current-state entity라서 같은 파이프라인 안에서 저장 모델이 다르게 해석됩니다.
```

## 6. 자주 헷갈리는 점

### StarRocks가 Kafka에서 직접 읽나요?

아니요. 이번 프로젝트의 BI 경로는 Kafka 직접 적재가 아닙니다.

```text
Kafka -> Flink -> Paimon -> StarRocks external catalog -> BI
Paimon -> Spark -> Iceberg -> StarRocks external catalog -> BI
```

### purchase는 공식 매출인가요?

아니요. `ux-events`의 `purchase`는 구매 행동 이벤트입니다.

공식 주문 상태는 `order_current`에서 보고, 기준 집계는 Iceberg batch mart에서 봅니다.

### 왜 두 탭의 숫자가 비슷하거나 같나요?

같은 샘플 데이터를 다른 경로로 조회하기 때문입니다.

해석은 다릅니다.

- Lakehouse Ops: 빠른 운영 관찰
- Daily Business: batch 정리 후 기준 결과
