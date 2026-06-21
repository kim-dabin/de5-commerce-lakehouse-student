# OpenMetadata optional stack

이 디렉터리는 7차시에서 OpenMetadata를 선택적으로 확인하기 위한 별도 Docker 스택입니다.

OpenMetadata는 Kafka/Flink/Paimon/Spark/Iceberg 파이프라인의 필수 실행 경로가 아닙니다. 데이터 품질 결과, 테이블 설명, owner, lineage를 한곳에서 보는 metadata/observability 계층입니다.

## 권장 사용 방식

수강생 전원 필수 실행이 아니라, 시간이 남거나 개인 PC 리소스가 충분할 때 선택적으로 실행합니다. Docker 메모리가 부족하면 수업 핵심 실습인 5차시 프로젝트 스택을 우선하세요.

## 실행

```bash
cd sessions/07-quality-serving/openmetadata
./start-openmetadata.sh
```

UI:

```text
http://localhost:8585
admin@open-metadata.org / admin
```

데모 lineage/asset 주입:

```bash
./seed-openmetadata-demo.sh
```

데모 Data Quality 결과 주입:

```bash
./seed-openmetadata-dq-demo.sh
```

이 스크립트는 `commerce_category_daily` 테이블에 대표 Test Suite/Test Case/Test Result를 넣습니다. 수업에서는 OpenMetadata의 Data Quality 탭에서 품질 규칙과 PASS 결과가 테이블 메타데이터, 컬럼, lineage 옆에 붙는 모습을 확인합니다.

FAIL 결과를 일부러 보여주려면 아래처럼 실행합니다. 다시 PASS 상태로 되돌리려면 옵션 없이 한 번 더 실행합니다.

```bash
DQ_DEMO_INCLUDE_FAILURE=true ./seed-openmetadata-dq-demo.sh
./seed-openmetadata-dq-demo.sh
```

## 스크립트별 역할

| 스크립트 | 언제 실행하나 | 하는 일 | 수업에서 확인할 것 |
|---|---|---|---|
| `start-openmetadata.sh` | OpenMetadata UI를 처음 켤 때 | OpenMetadata Docker Compose 스택을 실행합니다. 서버가 이미 떠 있으면 새로 띄우지 않고 “already running”으로 통과합니다. | `http://localhost:8585` UI 접속, `http://localhost:8586/healthcheck` 정상 여부 |
| `seed-openmetadata-demo.sh` | UI가 뜬 뒤, lineage/asset 데모를 넣을 때 | OpenMetadata API로 Kafka topic, Flink pipeline, Spark pipeline, Lakehouse table, Streamlit dashboard 예시 자산을 생성하고 서로 lineage로 연결합니다. | table 상세 화면, lineage graph, pipeline/dashboard 연결 |
| `seed-openmetadata-dq-demo.sh` | Data Quality 탭을 보여주고 싶을 때 | `commerce_category_daily` 테이블에 대표 Test Suite 1개, Test Case 4개, Test Result를 생성합니다. 기본 실행은 모두 PASS입니다. | Data Quality/Test 화면에서 품질 규칙과 PASS 결과 |
| `DQ_DEMO_INCLUDE_FAILURE=true ./seed-openmetadata-dq-demo.sh` | FAIL 결과까지 보여주고 싶을 때 | 같은 Test Suite에 custom SQL 성격의 규칙 하나를 일부러 FAIL로 기록합니다. 실제 데이터를 망가뜨리는 것이 아니라 OpenMetadata에 실패 결과를 publish하는 데모입니다. | PASS/FAIL 차이, 실패 규칙 확인, incident/alert 확장 설명 |
| `stop-openmetadata.sh` | 수업/실습 후 OpenMetadata를 내릴 때 | OpenMetadata 컨테이너를 중지합니다. 기본은 볼륨을 남겨서 다음 실행 때 같은 상태를 다시 볼 수 있습니다. | Docker 리소스 회수 |
| `DELETE_OPENMETADATA_VOLUMES=true ./stop-openmetadata.sh` | 완전 초기화가 필요할 때 | OpenMetadata 컨테이너와 함께 MySQL/Elasticsearch 볼륨까지 삭제합니다. 기존 seed 데이터도 사라집니다. | 깨끗한 초기 상태에서 다시 시작 |

### 수업에서 설명할 때의 구분

`seed-openmetadata-demo.sh`는 “어떤 데이터 자산이 있고 서로 어떻게 연결되는가”를 보여주는 lineage/metadata 데모입니다.

`seed-openmetadata-dq-demo.sh`는 “품질 규칙과 실행 결과가 데이터 자산 옆에 어떻게 붙는가”를 보여주는 Data Quality 데모입니다.

즉 첫 번째 seed는 **지도 만들기**, 두 번째 seed는 **지도 위에 품질 신호 붙이기**입니다.

실무에서는 이 두 과정을 사람이 seed 스크립트로 넣지 않습니다. Kafka/Flink/Spark/Iceberg/BI metadata는 connector나 ingestion workflow로 수집하고, 품질 결과는 Great Expectations, OpenMetadata Data Quality as Code, Airflow task, custom SQL test 결과를 자동으로 publish합니다.

중지:

```bash
./stop-openmetadata.sh
```

볼륨까지 삭제:

```bash
DELETE_OPENMETADATA_VOLUMES=true ./stop-openmetadata.sh
```

## 포트

| 서비스 | URL |
|---|---|
| OpenMetadata UI | http://localhost:8585 |
| OpenMetadata admin health | http://localhost:8586/healthcheck |
| OpenMetadata ingestion Airflow | http://localhost:18080 |
| MySQL host port | 13306 |
| Elasticsearch host port | 19200 |

## 수업에서 볼 포인트

- Table: 테이블 설명, 컬럼, owner, tag
- Lineage: topic/table/pipeline/dashboard가 어떻게 연결되는지
- Test/Quality: 품질 규칙과 실행 결과를 데이터 자산과 함께 보는 방식

이번 seed 스크립트는 OpenMetadata 사용법을 보여주기 위한 작은 데모 lineage를 생성합니다. 최신 Olist 프로젝트의 전체 자산을 자동 수집하는 운영 설정은 아니므로, 실제 품질 게이트는 `sessions/05-project-mvp/scripts/run-data-quality-checks.sh` 결과를 기준으로 봅니다.
`seed-openmetadata-dq-demo.sh`는 GE 결과 자동 연동의 완성형 구현이 아니라, OpenMetadata의 Data Quality 화면 구조를 수업 중 보여주기 위한 대표 결과 publish 데모입니다. 운영에서는 GE action, OpenMetadata Data Quality as Code, Airflow task로 이 publish 과정을 자동화합니다.

## 메모리 부족 시

OpenMetadata는 Elasticsearch와 ingestion Airflow를 같이 띄우므로 무겁습니다. UI만 보면 되는 경우 ingestion Airflow를 내려도 됩니다.

```bash
docker compose -p de5-openmetadata -f docker-compose.openmetadata.yml stop ingestion
```

검색 오류가 나면 Elasticsearch가 죽었을 수 있습니다.

```bash
docker compose -p de5-openmetadata -f docker-compose.openmetadata.yml ps
docker compose -p de5-openmetadata -f docker-compose.openmetadata.yml restart elasticsearch openmetadata-server
```
