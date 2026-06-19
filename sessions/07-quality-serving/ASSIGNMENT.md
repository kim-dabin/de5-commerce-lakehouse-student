# 7차시 과제 - 품질 게이트와 serving 해석을 발표 자료로

이번 주 과제도 "제출하고 끝"이 아니라 최종 발표 자료로 쌓아갑니다.

7차시 과제는 **(1) 데이터 품질 규칙 한 개를 직접 추가/검증하고, (2) serving 지표 한 개를 grain까지 설명하고, (3) 최종 발표 방향을 1차로 정리**하는 것입니다.

## 제출물

아래 중 편한 형식으로 제출하세요.

- Markdown / Notion 링크 / PDF / 이미지 1-2장

형식보다, 최종 발표에서 그대로 설명할 수 있는 구조가 중요합니다.

## 필수 내용

### A. 데이터 품질 (택1 이상)

1. 기준 테이블 1개를 골라 **품질 규칙 1개를 추가**합니다.
   - 예: `olist_review_current`에 `rating`이 1~5 범위 + not-null, 또는 `olist_order_current.order_id` 유일성.
   - `labs/09-data-quality/data_quality_checks.py`의 `expectations_for()`에 한 줄 추가하거나, 노트북에서 직접 작성.
2. 그 규칙이 **통과**하는 결과(PASS)를 캡처합니다.
3. **의도적 실패 데모**를 한 번 변형해, 일부러 깨뜨린 행을 만들어 그 규칙이 **FAIL로 잡는** 것을 캡처합니다.
   - 무엇을 어떻게 오염시켰고, 어떤 expectation이 어떤 `unexpected_count`로 잡았는지 한 줄로 기록.

### B. Serving 해석 (필수)

4. **realtime 지표 1개와 batch 지표 1개**를 고릅니다.
   - 예: realtime `total_events=16,693`(Paimon current-state) vs batch `category_daily` 합계(Iceberg mart).
5. 각 지표가 **어떤 테이블·어떤 grain**에서 계산됐는지 설명합니다.
6. 두 숫자가 같거나 다른 이유를 한 문장으로 적습니다. (grain/시점/append vs current-state 관점)

### C. 마무리 (필수)

7. 발견한 **품질 또는 serving 리스크 1개**와 개선 아이디어 1개.
8. 최종 발표에서 말할 **한 문장**.

선택: OpenMetadata 화면을 확인했다면, 품질 실패가 났을 때 **어떤 table/column/lineage를 먼저 볼지** 한 줄로 추가해도 됩니다. OpenMetadata는 자동 게이트라기보다 품질 결과를 metadata와 lineage 맥락에서 설명하는 도구로 보면 됩니다.

### D. 최종 프로젝트 방향 초안 (필수)

9. 내가 집중할 비즈니스 질문 1개.
10. 최종 발표에 넣을 핵심 지표 1개와 출처 테이블.
11. 내가 강조할 운영 리스크 1개.
12. 내가 추가하거나 바꿔보고 싶은 것 1개.

예시:

```text
비즈니스 질문:
부정 리뷰가 많은 카테고리에서 상세페이지 이탈이 높아지는가?

핵심 지표:
olist_category_daily의 review_impression_count, product_view_count, purchase_count

운영 리스크:
review/order를 current-state로만 관리하면 상태 변화 이력이 사라질 수 있음

확장 아이디어:
review_events_bronze를 append 테이블로 추가해 "어떻게 변했는가"까지 분석
```

## 제출 구조 예시

```text
제목: "DAG는 초록인데 mart가 비면? — 품질 게이트로 막기"

A. 품질 규칙
   - 추가한 규칙: olist_category_daily.event_count >= 1 (validity)
   - 통과: OVERALL PASS (4/4)
   - 실패 데모: category_code=NULL + event_count=0 행 1개 주입
     → expect_column_values_to_not_be_null(category_code) FAIL, unexpected_count=1

B. serving 해석
   - realtime: total_events 16,693 (Paimon ux current-state, 이벤트 grain)
   - batch: category_daily revenue 합 (Iceberg mart, 일자×카테고리 grain)
   - 차이: realtime은 행동 이벤트 전체, batch는 집계 후 결과라 grain이 다름

C. 마무리
   - 리스크: validate가 모든 mart를 검사하진 않음 → 빈 mart가 통과할 수 있음
   - 개선: run-data-quality-checks.sh를 DAG validate 다음 task로 게이트화
   - 한 문장: "정상은 숫자가 아니라 통과한 규칙으로 말한다."

D. 최종 프로젝트 방향
   - 비즈니스 질문: 부정 리뷰가 많은 카테고리에서 구매 전환이 낮아지는가?
   - 핵심 지표: review_impression_count 대비 purchase_count
   - 운영 리스크: 조회 계층 metadata cache가 stale하면 BI 숫자가 늦게 바뀔 수 있음
   - 확장 아이디어: StarRocks refresh task를 DAG에 추가
```

## 평가 기준

정답을 맞히는 과제가 아닙니다. 좋은 제출물은 아래를 만족합니다.

- 품질을 **차원(완전성/유일성/유효성/일관성/신선도)**으로 나눠 설명한다.
- 규칙이 통과하는 경우와 **잡아내는 경우**를 둘 다 보여준다.
- serving 지표를 **grain까지** 설명한다(숫자만 읽지 않는다).
- 발견한 리스크와 개선이 1개 이상 있다.
- 최종 발표에서 쓸 수 있는 한 문장으로 정리되어 있다.

## 마감

다음 차시 시작 3시간 전까지 Discord 7차시 스레드에 제출합니다.

규칙 추가나 실패 데모가 막혀도 괜찮습니다. 그 경우 "어디까지 했고 어디에서 막혔는지"를 그대로 제출하세요.
