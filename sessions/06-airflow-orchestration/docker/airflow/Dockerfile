ARG AIRFLOW_BASE_IMAGE=apache/airflow:2.10.5-python3.11
FROM ${AIRFLOW_BASE_IMAGE}

ARG CONFLUENT_KAFKA_VERSION=2.14.0
ARG OPENMETADATA_INGESTION_VERSION=1.12.6.4

USER root

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl docker.io; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    "confluent-kafka==${CONFLUENT_KAFKA_VERSION}" \
    "openmetadata-ingestion==${OPENMETADATA_INGESTION_VERSION}" \
    "protobuf==4.25.8" \
    "googleapis-common-protos==1.72.0" \
    "google-api-core==2.28.1" \
    "grpc-google-iam-v1==0.14.3" \
    "proto-plus==1.26.1" \
    "botocore==1.41.5"
