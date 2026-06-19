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
