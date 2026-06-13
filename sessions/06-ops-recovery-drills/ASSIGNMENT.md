# 6차시 과제 - 장애 대응 시나리오를 포트폴리오 문장으로 정리하기

이번 과제는 긴 보고서가 아닙니다. 수업에서 직접 본 장애 1개를 골라, "내가 어떤 증거로 원인을 좁혔는지"를 정리합니다.

## 필수 제출

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
   - Kafka/Flink/Paimon/Iceberg/StarRocks/BI 중 해당되는 계층
5. 복구 명령과 복구 후 증거
6. 포트폴리오에 넣을 3-5줄 요약

템플릿은 [INCIDENT_NOTE_TEMPLATE.md](./INCIDENT_NOTE_TEMPLATE.md)를 사용하세요.

## 평가 기준

정답을 맞히는 과제가 아니라, 계층별 증거를 분리해서 설명하는 과제입니다.

좋은 제출 예시는 아래처럼 씁니다.

```text
Flink job은 RUNNING이었지만 Paimon count가 기대값과 달랐다.
그래서 Flink 상태만으로는 정상이라고 말할 수 없고, sink table count를 별도로 확인했다.
원인은 reset 없이 append topic을 한 번 더 replay한 것이었고, clean baseline이 필요해 Kafka/Paimon reset 후 재실행했다.
```

## 마감

다음 차시 시작 3시간 전까지 Discord 6차시 스레드에 제출합니다.
