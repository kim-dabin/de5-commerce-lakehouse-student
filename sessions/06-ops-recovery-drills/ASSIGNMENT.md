# 6차시 과제 - 최종 발표용 장애 대응 슬라이드 만들기

이번 주부터 과제는 "제출하고 끝나는 문서"가 아니라 최종 발표 자료로 쌓아갑니다.

6차시 과제는 수업에서 직접 본 장애/운영 상황 1개를 골라, 최종 발표에 그대로 붙일 수 있는 1-2장짜리 슬라이드 초안으로 정리하는 것입니다.

## 제출물

아래 중 편한 형식으로 제출하세요.

- PPT / Keynote / PDF
- Notion 링크
- Markdown
- 이미지 1-2장

중요한 것은 형식이 아니라, 최종 발표에서 바로 설명할 수 있는 구조로 정리하는 것입니다.

## 필수 내용

1. 선택한 장애 라운드 1개
   - R1 TaskManager 장애
   - R2 checkpoint/savepoint 복구
   - R3 Kafka ISR 설정 오류
   - R4 잘못된 payload
   - R5 Iceberg mart empty/누락
   - R6 StarRocks metadata refresh
2. 장애 전 정상 기준
3. 장애 후 관찰한 증상
4. 내가 본 증거 3개
   - Kafka / Flink / Paimon / Iceberg / StarRocks / BI / Airflow 중 해당되는 계층
5. 복구 명령과 복구 후 증거
6. 재발 방지 또는 개선 아이디어 1개
7. 최종 발표에서 말할 한 문장

템플릿은 [INCIDENT_NOTE_TEMPLATE.md](./INCIDENT_NOTE_TEMPLATE.md)를 참고하세요.

## 최종 발표 슬라이드 구조 예시

```text
제목: Flink는 RUNNING이었지만 Paimon count가 틀어졌던 상황

1. 증상
   - Flink job은 RUNNING
   - ux_events_bronze count가 기대값 16,693과 다름

2. 증거 체인
   - Kafka: ux-events offset 증가
   - Flink: job RUNNING, sink records 증가
   - Paimon: append table이라 replay 시 row count 누적

3. 복구
   - Kafka/Paimon reset
   - Flink job 재기동
   - produce 재실행 후 count 재검증

4. 배운 점
   - RUNNING은 정상의 증거가 아니다.
   - append table은 replay 중복을 downstream에서 별도로 해석해야 한다.
```

## 평가 기준

정답을 맞히는 과제가 아닙니다.

좋은 제출물은 아래를 만족합니다.

- 장애를 계층별로 나눠 설명한다.
- "어디까지 정상인지"를 증거로 말한다.
- 복구 후 다시 확인한 숫자나 로그가 있다.
- 최종 발표에서 사용할 수 있는 문장으로 정리되어 있다.
- "다음에는 어떻게 개선할지"가 1개 이상 들어 있다.

## 마감

다음 차시 시작 3시간 전까지 Discord 6차시 스레드에 제출합니다.

복구에 실패해도 괜찮습니다. 그 경우에는 "어디까지 확인했고, 어디에서 막혔는지"를 그대로 제출하세요.
