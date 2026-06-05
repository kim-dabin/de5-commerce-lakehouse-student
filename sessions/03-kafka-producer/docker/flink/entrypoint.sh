#!/usr/bin/env bash
set -euo pipefail

if [ "$(id -u)" = "0" ]; then
  mkdir -p /warehouse/paimon /opt/flink/checkpoints /opt/flink/log
  chown -R flink:flink /warehouse /opt/flink/checkpoints /opt/flink/log
fi

exec /docker-entrypoint.sh "$@"
