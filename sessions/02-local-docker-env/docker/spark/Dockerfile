ARG SPARK_BASE_IMAGE=apache/spark:3.5.6
FROM ${SPARK_BASE_IMAGE}

USER root

RUN set -eux; \
    mkdir -p /workspace /warehouse/paimon /home/spark/.ivy2; \
    chown -R spark:spark /workspace /warehouse /home/spark/.ivy2

USER spark
WORKDIR /workspace

