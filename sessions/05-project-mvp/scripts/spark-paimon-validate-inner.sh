#!/usr/bin/env bash
set -euo pipefail

PAIMON_VERSION="${PAIMON_VERSION:-1.4.1}"
ICEBERG_VERSION="${ICEBERG_VERSION:-1.11.0}"
SPARK_PACKAGES="${SPARK_PACKAGES:-org.apache.paimon:paimon-spark-3.5_2.12:${PAIMON_VERSION},org.apache.paimon:paimon-s3:${PAIMON_VERSION},org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}}"

/opt/spark/bin/spark-submit \
  --packages "${SPARK_PACKAGES}" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions" \
  --conf "spark.sql.catalog.paimon_lake=org.apache.paimon.spark.SparkCatalog" \
  --conf "spark.sql.catalog.paimon_lake.warehouse=s3://paimon/warehouse" \
  --conf "spark.sql.catalog.paimon_lake.s3.endpoint=http://minio:9000" \
  --conf "spark.sql.catalog.paimon_lake.s3.access-key=minioadmin" \
  --conf "spark.sql.catalog.paimon_lake.s3.secret-key=minioadmin" \
  --conf "spark.sql.catalog.paimon_lake.s3.path.style.access=true" \
  --conf "spark.sql.catalog.iceberg_lake=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.iceberg_lake.type=rest" \
  --conf "spark.sql.catalog.iceberg_lake.uri=http://iceberg-rest:8181" \
  --conf "spark.sql.catalog.iceberg_lake.warehouse=s3://warehouse/" \
  --conf "spark.sql.catalog.iceberg_lake.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
  --conf "spark.sql.catalog.iceberg_lake.s3.endpoint=http://minio:9000" \
  --conf "spark.sql.catalog.iceberg_lake.s3.path-style-access=true" \
  --conf "spark.sql.catalog.iceberg_lake.client.region=us-east-1" \
  --conf "spark.sql.session.timeZone=UTC" \
  /workspace/labs/05-spark-iceberg/validate_paimon_counts.py
