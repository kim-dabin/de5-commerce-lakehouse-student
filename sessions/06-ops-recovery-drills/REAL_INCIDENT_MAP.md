# 6차시 실무 인시던트 맵

이 문서는 6차시 드릴을 실제 운영 사례와 연결하기 위한 멘토/학생 공용 참고 자료입니다. 회사 내부 서비스명과 테이블명은 공개 문서에 맞게 익명화했습니다.

## 한 줄 요약

운영 장애 대응의 핵심은 "실패한 도구 이름"을 맞히는 것이 아니라, Kafka/Flink/Paimon/Iceberg/StarRocks 중 어느 계층까지 정상인지 증거로 자르는 것입니다.

## R1. Flink 리소스/TaskManager 장애

실제 형태:

- 컨테이너가 ephemeral-storage limit을 넘거나 노드가 DiskPressure 상태가 됨.
- kubelet이 pod를 evict하고, TaskManager slot이 사라짐.
- 소비가 멈춘 동안 입력이 계속 들어오면 backlog/lag가 증가함.
- Flink job 상태만 보면 원인을 놓칠 수 있었고, 노드/컨테이너/로그/lag를 같이 봐야 했음.

수업 축소판:

- 로컬 Docker에는 kubelet eviction이 없으므로 `flink-taskmanager`를 직접 중지한다.
- 이 stop은 "디스크가 찬 원인"을 재현하는 것이 아니라, "eviction 이후 TaskManager가 사라진 효과"를 흉내낸 것이다.
- Flink UI, `flink list -r`, TaskManager 컨테이너 상태, Paimon count를 같이 본다.
- lag/offset은 하나의 신호일 뿐이며, TaskManager 상태와 checkpoint/restart 로그, Paimon count와 교차검증해야 한다.
- 복구 후 RUNNING만 보지 말고 count까지 다시 확인한다.

핵심 문장:

> 컴퓨트는 내 코드와 무관한 이유로 사라질 수 있습니다. 이번 랩은 eviction 이후 효과를 축소 재현하고, 실제 운영에서는 디스크/메모리/노드/쿼터 같은 오케스트레이션 리소스까지 같이 봐야 합니다.

## R2. checkpoint/last-state가 복구를 막은 경우

실제 형태:

- Flink job이 checkpoint/last-state로 복구하려 했지만, 그 state가 더 이상 유효하지 않은 metadata pointer를 참조함.
- 대표 증상은 Iceberg-compatible metadata JSON 파일을 읽으려 했는데 실제 파일이 사라진 상태.
- 로그 형태는 `Failed to read Iceberg metadata from path ... vNNNNN.metadata.json` 또는 `FileNotFoundException`.
- 같은 checkpoint/last-state로 계속 올리면 같은 실패를 반복하므로, stateless 재기동과 원천 재처리/검증/cutover가 필요했음.

수업 축소판:

- 학생 핸즈온은 savepoint로 멈춘 뒤 같은 savepoint에서 복구하는 KEEP 경로를 보여준다.
- 멘토 시연은 잘못된 savepoint path로 restore가 실패하는 DISCARD 판단을 보여준다.
- 이어받기 자체는 좋은 패턴이지만, "이어받을 state가 신뢰 가능한가?"라는 판단이 별도로 필요하다는 점을 설명한다.

핵심 문장:

> checkpoint는 복구 도구이지만, 깨진 metadata pointer나 잘못된 offset/state를 들고 있으면 실패를 반복시키는 원인이 됩니다.

## R3. Kafka ISR 부족 (acks=all 쓰기 실패)

실제 형태:

- `acks=all` producer는 in-sync replica 수가 `min.insync.replicas` 이상이어야 ack를 받는다.
- broker 장애/재기동, 디스크 문제, ISR 축소가 겹치면 `ISR < min.insync.replicas`가 되어 쓰기가 거부된다(`NotEnoughReplicasException`).
- 데이터가 유실되는 게 아니라 "내구성 기준을 못 맞춰 쓰기를 막는" 안전장치다.

수업 축소판:

- olist topic을 `RF=2 + min.insync.replicas=2`로 만든다(`reset-olist-kafka-topics.sh`). 즉 두 broker가 모두 살아 있어야 acks=all 쓰기가 성공한다.
- 주입은 `kafka2`(두 번째 broker)를 정지시키는 것이다. ISR이 2→1로 줄어 `1 < 2`가 되고, `acks=all` producer는 `NotEnoughReplicasException`으로 실패한다. controller/leader는 `kafka`에 남아 consumer(Flink)는 기존 데이터를 계속 읽는다 — 즉 **읽기는 정상, 쓰기 경로만 막힌다.**
- 우리 실습 producer는 `acks=all`, `enable.idempotence=true`, `retries=5`라서 재시도 후 delivery 실패를 보고하고 비0으로 종료한다.
- 주의: 단일 broker(RF=1)에서는 `min.insync.replicas=2`를 걸어도 쓰기가 그냥 성공한다(ISR이 항상 1뿐이라 기준 자체가 적용되지 않음). 그래서 이 라운드는 **두 번째 broker가 반드시 필요**하다.
- R1과 R3는 downstream에서 모두 "count가 멈춤"처럼 보일 수 있다. R1은 Kafka offset은 늘고 Flink가 못 따라가는 케이스이고, R3는 producer가 실패해 Kafka offset 자체가 늘지 않는 케이스다.

핵심 문장:

> producer 장애처럼 보여도 실제 원인은 topic 설정일 수 있습니다.

## R4. schema/payload 오류

실제 형태:

- schemaless source에서 특정 batch부터 필드가 없거나 타입이 달라짐.
- parsed column만 보고는 원인을 찾기 어려웠고, raw payload를 남긴 것이 복구 단서가 됨.
- MongoDB 같은 소스에서는 batch 1과 batch 50의 schema가 다를 수 있음.

수업 축소판:

- `price`가 숫자가 아닌 UX event를 Kafka에 주입한다.
- Flink log와 Paimon count를 보며 "Kafka에 들어갔다"와 "Paimon에 정상 적재됐다"가 다름을 확인한다.
- 현재 UX job은 `price`를 `DECIMAL`로 엄격히 `CAST`하므로 bad record가 Paimon row/raw_json으로 남기 전에 Flink operator에서 실패할 수 있다.
- 따라서 이 라운드의 primary evidence는 Kafka raw payload와 Flink log이다. production fix는 전체 리셋이 아니라 bad record quarantine 또는 tolerant parsing이다.

핵심 문장:

> raw payload와 raw_json 설계는 낭비가 아니라 장애 분석용 증거입니다. 다만 엄격한 parser 앞에서 실패한 레코드는 sink table에 남기 전에 멈출 수 있으므로 Kafka 원문과 Flink log를 같이 봐야 합니다.

## R5. Iceberg mart empty / dependency 누락

실제 형태:

- source와 bronze는 정상이고 Airflow DAG도 이상 없이 끝난 것처럼 보였지만, Iceberg mart table에 실제 데이터가 없었음.
- DAG가 "성공"으로 끝났기 때문에 장애가 없다고 판단하고 방치했고, 나중에 BI 지표가 비거나 깨지면서 문제가 드러남.
- 이 사건의 핵심은 "DAG success"가 "serving-ready data"를 보장하지 않는다는 점.
- task 성공 여부뿐 아니라 output table row count, freshness, BI metric까지 검증해야 함.

수업 축소판:

- Iceberg mart 하나를 drop하지 않고 비운 상태로 만든다.
- `snapshots`와 time travel 조회로 문제 직전 정상 snapshot 후보를 찾는다.
- Airflow DAG를 다시 돌려 mart를 재생성하고, `query_iceberg_tables`와 `validate_bi_metric_counts` 로그로 복구를 확인한다.
- 현재 프로젝트에는 이 실무 사고를 반영해 BI metric validation task를 넣어 두었다. 즉, 과거라면 방치됐을 문제를 지금은 DAG 안에서 잡게 만든 구조다.

핵심 문장:

> DAG가 초록이라는 말은 BI가 정상이라는 뜻이 아닙니다. output table count와 freshness가 검증돼야 합니다.

> Time travel은 "멋있는 과거 조회"가 아니라, 어느 snapshot까지 정상으로 볼 수 있는지 찾는 복구 기준점 도구입니다.

## R6. StarRocks external metadata/cache stale

실제 형태:

- native Paimon count는 맞는데 StarRocks/Iceberg-compatible view 쪽이 최신 상태를 못 보는 문제가 있었음.
- 이 경우 primary data failure가 아니라 reflection/cache/metadata refresh 문제로 분리해야 함.

수업 축소판:

- Iceberg mart를 재생성한 뒤 StarRocks external metadata refresh를 실행한다.
- 이 라운드는 별도의 파괴 주입이 아니라 R5 이후 serving 계층을 확인하는 post-check이다.
- StarRocks external catalog는 자동 갱신될 수 있으므로 refresh 전후 count가 같을 수 있다. 이 경우도 "이미 fresh했다"는 결론이지 라운드 실패는 아니다.
- 데이터 파일/REST catalog/StarRocks cache가 서로 다른 계층이라는 점을 설명한다.

핵심 문장:

> 조회 계층이 stale하다고 해서 곧바로 원천 데이터 유실이라고 판단하면 안 됩니다.

## Blind drill 후보. Kafka lag가 거짓말하는 경우

실제 형태:

- consumer group lag가 크거나 `no active members`처럼 보여도 실제 Flink job은 정상 처리 중일 수 있음.
- 이유는 Flink Kafka Source가 offset을 Kafka consumer group에 commit하지 않고 checkpoint에 저장하는 구조일 수 있기 때문.

수업에서 던질 질문:

```text
Kafka UI에서 consumer group이 비어 있거나 lag가 이상해 보이면, Flink가 죽었다고 말할 수 있을까요?
```

기대 답:

```text
아니요. Flink job 상태, checkpoint, source/sink records, Paimon count를 같이 봐야 합니다.
```
