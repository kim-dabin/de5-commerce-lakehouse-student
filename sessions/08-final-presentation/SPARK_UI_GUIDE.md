# 보너스 (8차시) — Spark UI · 실행 플랜 읽기

발표가 끝나고, 실무에서 **자주 들여다보는 영역**을 한 번 같이 봅니다. UI 화면 소개가 목적이 아니라,
**잡이 느릴 때 어디를 보는지 / stage·task에서 어떤 이상 신호를 잡는지 / 실패하거나 리소스를 많이 먹을 때 원인을 어떻게 좁히는지**가 목적입니다.

## 0. Spark UI 여는 법

- spark-client에서 Spark job이 돌면 Spark UI가 **4040**에 뜹니다 → `http://localhost:4040`
- UI는 **driver가 살아있는 동안만** 보입니다. 한 번에 안 뜨면 job이 이미 끝난 것.
- 오래 들여다보려면 **대화형 세션**을 띄워 두세요(아래 §2). 끝난 job을 나중에 보려면 **History Server `localhost:18080`**(아래 §2.5) — 이 스택에 추가됨.

## 1. 실행 플랜 읽기 — `EXPLAIN`

```bash
cd sessions/05-project-mvp
./scripts/run-spark-iceberg-sql.sh labs/10-spark-ui/explain_category_daily.sql
```

실제 출력(category_daily 집계의 물리 플랜):

```text
== Physical Plan ==
Sort (6)
+- Exchange (5)            rangepartitioning(...)        ← ORDER BY 용 셔플
   +- HashAggregate (4)    final                          ← 최종 집계
      +- Exchange (3)      hashpartitioning(..., 200)     ← GROUP BY 용 셔플 (파티션 200)
         +- HashAggregate (2)  partial                    ← map-side 부분 집계
            +- BatchScan olist_ux_events_clean (1)        ← Iceberg 스캔
+ AdaptiveSparkPlan (AQE)
```

읽는 법:
- **아래에서 위로** 읽는다: 스캔 → 부분집계 → 셔플 → 최종집계 → (정렬 셔플) → Sort.
- **Exchange = 셔플 = 비싼 구간.** 여기선 2번(GROUP BY + ORDER BY). ORDER BY를 빼면 셔플 1번을 아낀다.
- `hashpartitioning(..., 200)` = shuffle partition 기본값 **200**. 작은 데이터엔 빈 task 200개 = **과분할** 신호 → `spark.sql.shuffle.partitions` 조정 대상.
- `AdaptiveSparkPlan`(AQE) = 런타임에 파티션 합치기/조인 전략 변경. `isFinalPlan=false`는 아직 실행 전 플랜.
- `BatchScan`에 `PushedFilters`가 보이면 = **predicate pushdown**(스캔 단계에서 미리 거름)이 먹은 것.

## 2. 느릴 때 — Spark UI 어디를 보나

오래 보려면 대화형 세션을 띄우면 4040이 계속 살아 있습니다.

```bash
docker compose -f docker-compose.lite.yml exec spark-client /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg_lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg_lake.type=rest \
  --conf spark.sql.catalog.iceberg_lake.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.iceberg_lake.warehouse=s3://warehouse/ \
  --conf spark.sql.catalog.iceberg_lake.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.iceberg_lake.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.iceberg_lake.s3.path-style-access=true \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///workspace/data/spark-events
-- 세션 유지 중엔 http://localhost:4040 라이브 UI, 종료 후엔 http://localhost:18080(History Server)에서 같은 잡을 다시 본다.
```

- **SQL / DataFrame 탭**: 쿼리별 플랜 + 노드별 소요시간·행수. 어느 노드가 오래 걸리나.
- **Stages 탭**: 느린 stage 클릭 → Task 분포를 본다.
  - task duration **max ≫ median** = **skew(편향)**. 한 task만 오래 = 데이터 쏠림.
  - **Shuffle Read/Write**가 큰 stage = 셔플 병목.
  - **Spill (Memory/Disk)**가 있으면 = 메모리 부족으로 디스크에 흘림 = 느려짐.
- **Jobs 탭**: 어느 job/stage에 시간이 쌓이나(전체 그림).

## 2.5 History Server — 끝난 잡 다시 보기 (`localhost:18080`)

4040은 driver가 사는 동안만 뜨고, 이 스택은 데이터가 작아 잡이 수 초면 끝나 창을 열 새가 없다. **History Server**는 `spark.eventLog`로 남긴 기록을 읽어 **끝난 잡의 UI(SQL plan·Stages·Task·skew)를 나중에 차분히** 보여준다.

```text
http://localhost:18080        # 완료된 application 목록 → 클릭하면 4040과 동일한 UI
```

- 잡에 `--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file:///workspace/data/spark-events` 가 있으면(위 §2 명령처럼) 자동 기록된다.
- 잡이 너무 빨라 4040을 놓쳤어도 18080에서 같은 plan·stages·task를 본다 — 데모/디버깅에 이게 4040보다 실용적이다.

## 3. 실패하거나 리소스를 많이 먹을 때

데모(멘토): 6차시 R8 — collect 직전 **일부러 멈춰서 UI를 볼 시간**을 줍니다.

```bash
OOM_UI_PAUSE_SECONDS=40 ./scripts/ops-r8-spark-driver-oom-demo.sh
```

- pause 동안 **Executors 탭**: driver/executor 메모리·GC time·active task를 본다.
- 그 뒤 `collect()`로 큰 결과를 driver로 모으면 → `java.lang.OutOfMemoryError: Java heap space`.
- 증상 신호: OOM, **GC time 과다**(연산보다 GC가 김), executor lost.
- 원인 좁히기: **메모리부터 늘리지 말고** → `collect()` 회피 / `limit`·집계로 줄이기 / skew 점검 / shuffle partition 조정.

## 한 장 체크리스트

```text
느릴 때    : SQL탭 느린 노드 → Stages 탭 task skew(max vs median) → Shuffle Read/Write·Spill
실패할 때  : 에러 로그(OOM / GC / executor lost) → Executors 탭 → driver냐 executor냐
리소스 과다 : collect() / 큰 셔플 / skew / 과분할(shuffle partition 200) 의심
플랜 볼 때  : 아래→위, Exchange(셔플) 수 세기, PushedFilters 확인, AQE 여부
```

> 핵심: "잡이 느리거나 실패하면 **메모리부터 늘리지 말고**, 플랜의 셔플 수 · stage의 task 편향 · spill부터 본다."
