# 발표 증거 수집 카드

발표 슬라이드 6("계층별 검증 증거")에 넣을 증거를 스택에서 직접 수집하는 명령입니다.
**본인이 직접 실행해 확인한 것만** 발표에 넣으세요. 모든 명령은 아래에서 실행합니다.

```bash
cd sessions/05-project-mvp
docker compose -f docker-compose.lite.yml up -d        # 스택 기동
```

> 기준 baseline을 새로 만들어야 하면 6·7차시 README의 셋업(토픽/Paimon reset → Flink → produce → Airflow trigger)을 따르세요.

## 한 번에 계층 상태 보기

```bash
./scripts/ops-baseline-evidence.sh     # Kafka/Flink/Paimon/Iceberg/StarRocks 상태 한 번에
```

## 계층별 증거

| 계층 | 명령 | 캡처할 것 (기준값) |
|---|---|---|
| Kafka | `./scripts/ops-kafka-topic-offsets.sh ux-events` | topic offset/누적 message |
| Flink | `./scripts/ops-flink-list-jobs.sh` | 3 job RUNNING (ingest-ux/review/order) |
| Paimon | `./scripts/query-olist-paimon.sh` | 16,693 / 1,971 / 2,000 |
| Iceberg | `./scripts/query-iceberg-tables.sh` | mart 7개, category_daily 759 |
| 데이터 품질 | `./scripts/run-data-quality-checks.sh 2>/dev/null` | OVERALL PASS (32 expectation + 3 custom) |
| StarRocks (realtime) | `./scripts/query-realtime-olap-metrics.sh` | total_events 16,693 / users 2,875 / revenue 265,036 |
| BI (batch) | `./scripts/query-bi-metrics.sh` | Iceberg mart 지표 (category_daily 759 등) |
| Airflow | `http://localhost:8080` (admin/admin) | `de5_olist_project_mvp_pipeline` run success / task log |

## 품질 게이트 증거 (7차시 산출물 재사용)

```bash
./scripts/run-data-quality-checks.sh 2>/dev/null    # PASS 캡처
echo "exit=$?"                                       # 0 = 게이트 통과
```

대화형 + 의도적 실패 데모는 `notebooks/de5-data-quality.ipynb` (JupyterLab `http://localhost:8888`). "검증이 없으면 오염이 조용히 통과한다"는 리스크 슬라이드의 좋은 근거입니다.

## 증거를 슬라이드로 옮길 때

- 명령 출력을 **그대로 나열하지 말고**, "이 숫자가 어느 테이블·어느 grain에서 나왔는가"를 한 줄 붙입니다.
- count 불일치는 **유실로 단정하지 말고** append/upsert·current-state 관점으로 해석합니다.
- 직접 확인하지 못한 계층은 비워 두는 게 낫습니다(거짓 증거 금지).

## 자주 보는 화면

```text
Streamlit BI   http://localhost:8501   (./scripts/start-streamlit-bi.sh)
Airflow        http://localhost:8080   (admin/admin)
Flink UI       http://localhost:8081
JupyterLab     http://localhost:8888
```
