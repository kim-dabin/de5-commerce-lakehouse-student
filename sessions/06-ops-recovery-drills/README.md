# 6차시 - 장애 대응과 운영 복구 드릴

이번 차시는 설명을 길게 듣는 시간이 아니라, 같은 파이프라인을 직접 망가뜨리고 다시 정상으로 돌려보는 시간입니다.

목표는 한 가지입니다.

> "어디까지 정상이고, 어디부터 의심해야 하는지 증거로 말한다."

## 오늘 사용할 위치

6차시 드릴은 5차시에 완성한 프로젝트 스택을 그대로 사용합니다.

```bash
cd sessions/05-project-mvp
```

## 시작 전 기준 상태

수업 시작 전 또는 첫 10분 동안 아래 상태를 맞춥니다.

```bash
git pull origin main
docker compose -f docker-compose.lite.yml up -d --build
docker compose -f docker-compose.lite.yml ps

./scripts/reset-olist-kafka-topics.sh
./scripts/reset-olist-paimon.sh
./scripts/run-flink-olist-paimon-streaming.sh all
./scripts/produce-olist-ux-events.sh
./scripts/produce-olist-review-events.sh
./scripts/produce-olist-order-events.sh
./scripts/start-airflow.sh
./scripts/trigger-airflow-pipeline.sh
# Airflow UI에서 de5_olist_project_mvp_pipeline 성공 확인 후:
./scripts/ops-baseline-evidence.sh
```

정상 기준입니다.

| 계층 | 확인 증거 | 기준 |
|---|---|---|
| Kafka | broker / topic | broker 2대(`de5-kafka`, `de5-kafka2`)가 Up이고, olist topic은 `RF=2 / Isr: 1,2`. `ux-events` 등에 메시지 존재 (R3 ISR 드릴 전제) |
| Flink | UI 또는 `flink list -r` | `ingest-ux-events`, `ingest-review-current`, `ingest-order-current` RUNNING |
| Paimon | count | `16,693 / 1,971 / 2,000` |
| Iceberg | query 로그 | mart 7개 조회 가능 |
| StarRocks/BI | BI 또는 SQL | Iceberg external catalog로 mart 조회 가능 |

Airflow UI는 `http://localhost:8080`, 기본 계정은 `admin / admin`입니다.

## 2시간 진행 방식

먼저 지난주 과제 발표를 짧게 진행합니다.

```text
1인 60-90초
1. 내가 확인한 증거 1개
2. 아직 헷갈리는 지점 1개
3. 최종 발표에 넣고 싶은 포인트 1개
```

이번 주부터 과제는 "제출하고 끝"이 아니라 최종 발표 자료로 쌓아갑니다. 같은 공통 프로젝트를 진행하더라도, 각자가 어떤 증거를 중요하게 봤고 어떤 운영 리스크를 발견했는지가 개인 포트폴리오의 차별점이 됩니다.

각 라운드는 같은 리듬으로 진행합니다.

```text
장애 주입 -> 증상 관찰 -> 원인 가설 -> 복구 -> 증거 캡처
```

수업 중 개인 환경에서 막히면 멈춰서 고치기보다, 실패한 명령어와 에러 메시지를 남깁니다. 실패 로그도 이번 차시의 정상 산출물입니다.

| 시간 | 라운드 | 핵심 질문 |
|---|---|---|
| 20:00-20:05 | Open | 오늘 목표와 규칙 |
| 20:05-20:20 | 지난주 과제 발표 | 증거 1개, 질문 1개, 최종 발표 포인트 1개 |
| 20:20-20:30 | Baseline | 지금 정상이라고 말할 증거가 있는가? |
| 20:30-20:43 | R1 TaskManager 장애 | Flink RUNNING/RESTARTING만 보고 정상이라고 말할 수 있는가? |
| 20:43-20:58 | R2 checkpoint/savepoint 복구 | 상태를 이어받아야 할 때와 버려야 할 때는 어떻게 다를까? |
| 20:58-21:08 | R3 Kafka 토픽 내구성 설정 오류·ISR 데모 | producer 실패가 코드 문제가 아니라 토픽 설정/broker 가용성 문제일 수 있는가? |
| 21:08-21:15 | 휴식 | 로그/캡처 정리 |
| 21:15-21:28 | R4 잘못된 payload | Kafka에 들어간 메시지는 항상 downstream에 안전한가? |
| 21:28-21:43 | R5 Iceberg mart 누락 | BI 장애를 Airflow DAG로 어떻게 복구하고, 어느 snapshot을 복구 기준점으로 잡는가? |
| 21:43-21:53 | R6 StarRocks metadata refresh | 데이터는 있는데 조회 계층만 stale할 수 있는가? |
| 21:53-22:00 | 정리 | 최종 발표용 장애 대응 슬라이드 선택 |

## 실제 운영 사례와 연결

오늘 드릴은 임의로 만든 장난감 장애가 아니라, 실제 데이터 플랫폼 운영에서 겪는 패턴을 수업용으로 축소한 것입니다.

자세한 사례 맵은 [REAL_INCIDENT_MAP.md](./REAL_INCIDENT_MAP.md)에 정리했습니다.

| 드릴 | 실제 운영에서의 형태 | 오늘 실습에서 보는 증상 |
|---|---|---|
| R1 TaskManager 장애 | 디스크/리소스 압박으로 kubelet이 TaskManager pod를 evict하고, 입력이 계속 오면 backlog/lag가 증가 | 로컬에서는 TaskManager 중지로 eviction 이후 효과만 축소 재현, checkpoint/restart, count 재검증 |
| R2 checkpoint/savepoint | 기존 checkpoint/last-state가 깨진 메타데이터를 계속 참조해 stateless 재기동이 필요했던 복구 | 학생은 clean savepoint KEEP, 멘토는 bad savepoint DISCARD 판단 시연 |
| R3 Kafka 토픽 내구성 설정 오류·ISR 데모 | 토픽 설정값 오입력으로 `min.insync.replicas`가 잘못 커지거나, broker 장애·ISR 축소로 기준을 못 채워 `acks=all` producer가 `NotEnoughReplicasException`으로 실패 | (설정 오입력 사고에서 착안한 **재현**) olist topic `RF=2 + min.insync.replicas=2`에서 `kafka2` 정지 → ISR 2→1 → acks=all producer 실패. 읽기는 정상, 쓰기만 막힘 |
| R4 payload/schema 오류 | schemaless source에서 특정 batch부터 타입/필드가 달라져 parser 또는 sink가 실패 | 잘못된 `price` 타입 이벤트를 Kafka에 주입하고 Kafka raw payload/Flink log 확인 |
| R5 Iceberg mart empty/누락 | DAG는 성공처럼 보였지만 Iceberg mart가 비어 BI에서 장애가 드러난 사건 | Iceberg mart 하나를 drop하지 않고 empty로 만들고, snapshot/time travel로 복구 기준점 후보를 확인 |
| R6 metadata/cache stale | native table은 정상인데 StarRocks/Iceberg-compatible view가 최신 상태를 못 보는 문제 | R5 이후 StarRocks external metadata refresh 전후 비교 |
| R5b (확장) snapshot rollback | bad write 이후 직전 정상 snapshot으로 즉시 되돌려 serving을 복구 | R5 empty 후 find-recovery-point가 준 snapshot_id로 rollback_to_snapshot 실행 → count 즉시 복구 (rebuild와 대비) |
| R7 (확장) small file 문제 | 스트리밍/빈번 commit이 작은 파일을 양산해 read amplification·메타데이터 증가 | opsdemo 데모 테이블에 small commit 다수 → .files로 파일 수 확인 → rewrite_data_files 컴팩션 |
| R8 (멘토 시연) compute OOM | collect 과다/스큐로 driver·executor가 OOM | --driver-memory 작게 준 Spark job이 collect()로 driver heap OOM (JVM 내 격리, 데이터 불변) |

특히 R2는 중요합니다. 운영에서는 checkpoint가 항상 정답이 아닙니다. checkpoint가 이미 잘못된 metadata pointer나 잘못된 offset/state를 들고 있으면, "이어받기"가 복구가 아니라 실패 반복이 됩니다. 이때는 상태를 버리고 stateless로 다시 시작한 뒤, 원천에서 재처리하거나 staging 검증 후 cutover하는 판단이 필요합니다. 이번 수업에서는 clean savepoint 복구는 전원 핸즈온으로, 깨진 savepoint/path 실패는 멘토 시연으로 분리합니다.

## 수업에서 남길 증거

각 라운드마다 아래 네 줄만 남기면 됩니다.

```text
장애:
관찰한 증상:
확인한 증거:
복구 명령/결과:
```

R5에서는 한 줄을 더 남깁니다.

```text
복구 기준점 후보 snapshot_id:
```

제출 템플릿은 [INCIDENT_NOTE_TEMPLATE.md](./INCIDENT_NOTE_TEMPLATE.md)를 사용합니다.

## 6차시 과제 방향

이번 과제는 인시던트 노트를 최종 발표용 슬라이드 1-2장으로 바꾸는 것입니다.

최종 발표에서는 아래 구조로 말할 수 있어야 합니다.

```text
1. 어떤 장애/운영 리스크를 봤는가?
2. 어느 계층까지 정상이라고 확인했는가?
3. 어떤 증거로 원인을 좁혔는가?
4. 어떻게 복구했고, 복구 후 무엇을 다시 확인했는가?
5. 다음에는 어떤 개선으로 같은 문제를 줄일 것인가?
```

자세한 제출 기준은 [ASSIGNMENT.md](./ASSIGNMENT.md)를 확인하세요.

## 빠른 명령 카드

수업 중에는 [COMMAND_CARD.md](./COMMAND_CARD.md)를 열어두세요.
