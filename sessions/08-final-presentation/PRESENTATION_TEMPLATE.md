# 최종 발표 슬라이드 템플릿 (7장)

이 파일을 복사해 `[ ]` 부분을 본인 내용으로 채우세요. 발표 시간이 짧으면 장수를 줄이되 **"질문 → 구조 → 증거 → 해석 → 개선"** 흐름은 유지합니다.
각 슬라이드의 _핵심 메시지_ 한 줄을 청중이 가져가게 하는 것이 목표입니다.

---

## 슬라이드 1 — 프로젝트 한 줄 소개
_핵심 메시지: 무엇을 만든 프로젝트인가_

- 제목: `[ 예: Olist 커머스 행동·리뷰·주문 데이터로 만든 lakehouse + OLAP 파이프라인 ]`
- 한 줄 소개: `[ 이 프로젝트가 무엇인지 한 문장 ]`
- 사용 스택: Kafka → Flink → Paimon → Spark/Iceberg → StarRocks/BI → Airflow

## 슬라이드 2 — 내가 선택한 질문
_핵심 메시지: 왜 이 프로젝트가 필요한가_

- 내 질문: `[ 예: 리뷰를 본 사용자가 실제 구매로 이어지는가? / 부정 리뷰가 많은 카테고리는 이탈이 높은가? ]`
- 왜 이 질문인가: `[ 비즈니스 관점에서 한 줄 ]`
- 이 질문에 답하려면 어느 테이블/지표가 필요한가: `[ ... ]`

## 슬라이드 3 — 전체 아키텍처
_핵심 메시지: 데이터가 어디서 어디로 흐르는가_

- 아키텍처 그림(직접 그리거나 캡처): `[ Kafka → Flink → Paimon → Spark/Iceberg → StarRocks/BI → Airflow ]`
- 내 질문이 이 흐름의 **어느 지점**에서 답해지는지 표시: `[ 예: Iceberg L2 mart(category_daily) ]`

## 슬라이드 4 — 데이터 모델과 mart 설계
_핵심 메시지: count를 읽는 기준은 grain이다_

- L0 Paimon: `[ ux_events_bronze(append) / review_current·order_current(upsert) ]`
- L1 clean/current: `[ olist_ux_events_clean / review_current / order_current ]`
- L2 aggregate mart: `[ 내가 쓴 mart: 예 category_daily(일자×카테고리) / funnel_daily(일자) ]`
- 내가 읽은 지표의 grain: `[ 예: category_daily는 일자×카테고리 1행 ]`

## 슬라이드 5 — BI 결과와 해석
_핵심 메시지: 숫자가 어떤 질문에 답하는가_

- 대시보드 캡처: `[ Streamlit BI 또는 query 결과 ]`
- 내 핵심 지표: `[ 예: realtime total_events 16,693 / revenue 265,036 / category별 전환 ]`
- 해석(숫자를 그대로 읽지 않기): `[ 이 숫자가 내 질문에 어떻게 답하는가 ]`
- grain/시점 주의: `[ 예: purchase는 행동 이벤트라 공식 매출과 다름 ]`

## 슬라이드 6 — 계층별 검증 증거
_핵심 메시지: 어디까지 정상이라고 말할 수 있는가_

> `EVIDENCE_CARD.md`로 직접 수집한 것만, 본인이 확인한 계층만 적습니다.

| 계층 | 내가 확인한 증거 |
|---|---|
| Kafka | `[ topic 3개 / offset / message count ]` |
| Flink | `[ 3 job RUNNING / checkpoint ]` |
| Paimon | `[ 16,693 / 1,971 / 2,000 ]` |
| Iceberg | `[ mart 7개 / category_daily 759 ]` |
| 데이터 품질 | `[ run-data-quality-checks.sh OVERALL PASS (32+3) ]` |
| StarRocks/BI | `[ external catalog 조회 / mart와 일치 ]` |
| Airflow | `[ DAG run success / 어느 task까지 ]` |

## 슬라이드 7 — 발견한 리스크와 개선 로드맵
_핵심 메시지: 실무형 포트폴리오의 설계 판단_

- 발견한 리스크 1~2개: `[ 예: validate가 모든 mart를 검사하지 않음 / current-state라 이력 소실 / cache stale ]`
- 개선 우선순위: `[ 1) DQ 게이트를 serving 전으로 2) append bronze 추가 3) StarRocks refresh task ... ]`
- 내 한 문장 마무리: `[ 예: "정상은 숫자가 아니라 통과한 규칙으로 말한다." ]`

---

### 마무리 체크
- [ ] 첫 슬라이드에 **내 질문**이 명확한가
- [ ] BI 캡처에 **어느 mart/grain**인지 설명이 붙었는가
- [ ] 검증 증거가 **계층별로** 한 장에 모였는가
- [ ] 남은 갭이 **우선순위 있는 개선 로드맵**으로 표현됐는가 (숨기지 않기)
