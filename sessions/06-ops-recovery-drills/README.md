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
| Kafka | topic offset/message | `ux-events`, `review-events`, `order-status-events`에 메시지 존재 |
| Flink | UI 또는 `flink list -r` | `ingest-ux-events`, `ingest-review-current`, `ingest-order-current` RUNNING |
| Paimon | count | `16,693 / 1,971 / 2,000` |
| Iceberg | query 로그 | mart 7개 조회 가능 |
| StarRocks/BI | BI 또는 SQL | Iceberg external catalog로 mart 조회 가능 |

Airflow UI는 `http://localhost:8080`, 기본 계정은 `admin / admin`입니다.

## 2시간 진행 방식

각 라운드는 같은 리듬으로 진행합니다.

```text
장애 주입 -> 증상 관찰 -> 원인 가설 -> 복구 -> 증거 캡처
```

수업 중 개인 환경에서 막히면 멈춰서 고치기보다, 실패한 명령어와 에러 메시지를 남깁니다. 실패 로그도 이번 차시의 정상 산출물입니다.

| 시간 | 라운드 | 핵심 질문 |
|---|---|---|
| 20:00-20:10 | Baseline | 지금 정상이라고 말할 증거가 있는가? |
| 20:10-20:25 | R1 TaskManager 장애 | Flink RUNNING/RESTARTING만 보고 정상이라고 말할 수 있는가? |
| 20:25-20:45 | R2 checkpoint/savepoint 복구 | 상태를 이어받아야 할 때와 버려야 할 때는 어떻게 다를까? |
| 20:45-21:00 | R3 Kafka ISR 설정 오류 | producer 실패가 코드 문제가 아니라 topic 설정 문제일 수 있는가? |
| 21:00-21:10 | 휴식 | 로그/캡처 정리 |
| 21:10-21:25 | R4 잘못된 payload | Kafka에 들어간 메시지는 항상 downstream에 안전한가? |
| 21:25-21:45 | R5 Iceberg mart 누락 | BI 장애를 Airflow DAG로 어떻게 복구하는가? |
| 21:45-21:55 | R6 StarRocks metadata refresh | 데이터는 있는데 조회 계층만 stale할 수 있는가? |
| 21:55-22:00 | 정리 | 내 포트폴리오에 넣을 장애 대응 시나리오 1개 선택 |

## 실제 운영 사례와 연결

오늘 드릴은 임의로 만든 장난감 장애가 아니라, 실제 데이터 플랫폼 운영에서 겪는 패턴을 수업용으로 축소한 것입니다.

자세한 사례 맵은 [REAL_INCIDENT_MAP.md](./REAL_INCIDENT_MAP.md)에 정리했습니다.

| 드릴 | 실제 운영에서의 형태 | 오늘 실습에서 보는 증상 |
|---|---|---|
| R1 TaskManager 장애 | 노드 디스크/리소스 문제로 Flink 컨테이너가 evict되고 lag가 급증 | TaskManager 중지, checkpoint/restart, count 재검증 |
| R2 checkpoint/savepoint | 기존 checkpoint/last-state가 깨진 메타데이터를 계속 참조해 stateless 재기동이 필요했던 복구 | savepoint 복원은 이어읽기, 상태 폐기는 중복/누락 위험 |
| R3 Kafka ISR 설정 오류 | retention 값을 다른 설정에 넣거나 ISR 설정을 잘못 넣어 acks=all producer가 실패 | `min.insync.replicas=2`를 단일 broker topic에 주입 |
| R4 payload/schema 오류 | schemaless source에서 특정 batch부터 타입/필드가 달라져 parser 또는 sink가 실패 | 잘못된 `price` 타입 이벤트를 Kafka에 주입 |
| R5 Iceberg mart empty/누락 | DAG는 성공처럼 보였지만 Iceberg mart가 비어 BI에서 장애가 드러난 사건 | Iceberg mart 하나를 drop하고 DAG 검증으로 재생성/검출 |
| R6 metadata/cache stale | native table은 정상인데 StarRocks/Iceberg-compatible view가 최신 상태를 못 보는 문제 | StarRocks external metadata refresh |

특히 R2는 중요합니다. 운영에서는 checkpoint가 항상 정답이 아닙니다. checkpoint가 이미 잘못된 metadata pointer나 잘못된 offset/state를 들고 있으면, "이어받기"가 복구가 아니라 실패 반복이 됩니다. 이때는 상태를 버리고 stateless로 다시 시작한 뒤, 원천에서 재처리하거나 staging 검증 후 cutover하는 판단이 필요합니다.

## 수업에서 남길 증거

각 라운드마다 아래 네 줄만 남기면 됩니다.

```text
장애:
관찰한 증상:
확인한 증거:
복구 명령/결과:
```

제출 템플릿은 [INCIDENT_NOTE_TEMPLATE.md](./INCIDENT_NOTE_TEMPLATE.md)를 사용합니다.

## 빠른 명령 카드

수업 중에는 [COMMAND_CARD.md](./COMMAND_CARD.md)를 열어두세요.
