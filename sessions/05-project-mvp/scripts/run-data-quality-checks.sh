#!/usr/bin/env bash
# DE5 7차시 · Iceberg analytics 기준 테이블 데이터 품질 검증 실행기.
#
# jupyter 컨테이너(Great Expectations 설치됨) 안에서 Spark 드라이버로 GE 스위트를
# 실행한다. notebook 커널(kernel.json)과 동일한 PYSPARK_SUBMIT_ARGS(=iceberg_lake
# REST 카탈로그 등)를 재사용하므로, 노트북과 같은 카탈로그를 헤드리스로 검증한다.
#
# 사용:
#   ./scripts/run-data-quality-checks.sh
set -euo pipefail

CONTAINER="${JUPYTER_CONTAINER:-de5-jupyter}"
KERNEL_JSON="${KERNEL_JSON:-docker/jupyter/kernel.json}"
SCRIPT="${DQ_SCRIPT:-/workspace/labs/09-data-quality/data_quality_checks.py}"

if [[ ! -f "${KERNEL_JSON}" ]]; then
  echo "kernel.json을 찾을 수 없습니다: ${KERNEL_JSON} (05-project-mvp 디렉터리에서 실행하세요)" >&2
  exit 2
fi

# kernel과 동일한 카탈로그/패키지 설정을 재사용한다.
ARGS="$(python3 -c "import json; print(json.load(open('${KERNEL_JSON}'))['env']['PYSPARK_SUBMIT_ARGS'])")"

echo "container=${CONTAINER}"
echo "script=${SCRIPT}"

exec docker exec \
  -e PYSPARK_SUBMIT_ARGS="${ARGS}" \
  -e GX_ANALYTICS_ENABLED=false \
  -e ICEBERG_CATALOG="${ICEBERG_CATALOG:-iceberg_lake}" \
  -e ICEBERG_NAMESPACE="${ICEBERG_NAMESPACE:-analytics}" \
  -e SPARK_LOG_LEVEL="${SPARK_LOG_LEVEL:-ERROR}" \
  "${CONTAINER}" python3 "${SCRIPT}"
