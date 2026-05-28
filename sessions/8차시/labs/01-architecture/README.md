# 01 아키텍처

1차시는 구현보다 전체 지도를 그리는 시간입니다. 우리가 만들 파이프라인이 어떤 질문에 답하는지 먼저 이해합니다.

## 목표

- 커머스 이벤트 데이터가 어떤 성격인지 이해합니다.
- Realtime OLAP과 Batch Lakehouse BI가 답하는 질문을 구분합니다.
- Kafka, Flink, Paimon, Spark, Iceberg, StarRocks, BI의 역할을 한 장의 그림으로 설명합니다.

## 핵심 흐름

```text
Commerce Events
  -> Kafka
  -> Flink
  -> Paimon Bronze
  -> Spark
  -> Iceberg Analytics
  -> StarRocks
  -> BI
```

## 과제 방향

수업 자료를 그대로 복사하기보다 본인 언어로 다시 그리는 것이 중요합니다.

1. 전체 아키텍처 그림을 그립니다.
2. 각 기술의 역할을 한 문장으로 설명합니다.
3. Realtime OLAP과 Batch Lakehouse BI의 차이를 두 줄로 정리합니다.
4. 가장 궁금한 장애 시나리오를 하나 적습니다.

## 수업 중 사용할 질문

- 같은 커머스 이벤트를 왜 realtime 관점과 batch 관점으로 나누어 볼까요?
- Paimon과 Iceberg는 둘 다 lakehouse table format인데 왜 함께 사용할까요?
- 면접에서 이 프로젝트를 설명한다면 첫 문장을 어떻게 시작할 수 있을까요?
