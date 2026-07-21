#!/bin/sh
# Kafka Connect REST API가 뜨면 정형 CDC 파이프라인의 소스/싱크 커넥터를
# 등록(PUT은 멱등적)한다: Oracle(LogMiner) -> Kafka -> Postgres(target-db).
set -eu

CONNECT_URL="http://kafka-connect:8083"

echo "[register-connector] Waiting for Kafka Connect REST API at ${CONNECT_URL} ..."
until curl -sf "${CONNECT_URL}/connectors" >/dev/null 2>&1; do
  sleep 3
done

envsubst < "/connectors/oracle-cdc-source.json.template" > "/tmp/oracle-cdc-source.json"
echo "[register-connector] Registering oracle-cdc-source connector ..."
curl -sf -X PUT "${CONNECT_URL}/connectors/oracle-cdc-source/config" \
  -H "Content-Type: application/json" \
  -d @"/tmp/oracle-cdc-source.json"

envsubst < "/connectors/postgres-cdc-sink.json.template" > "/tmp/postgres-cdc-sink.json"
echo "[register-connector] Registering postgres-cdc-sink connector ..."
curl -sf -X PUT "${CONNECT_URL}/connectors/postgres-cdc-sink/config" \
  -H "Content-Type: application/json" \
  -d @"/tmp/postgres-cdc-sink.json"

envsubst < "/connectors/postgres-cdc-sink-clob-test-data.json.template" > "/tmp/postgres-cdc-sink-clob-test-data.json"
echo "[register-connector] Registering postgres-cdc-sink-clob-test-data connector ..."
curl -sf -X PUT "${CONNECT_URL}/connectors/postgres-cdc-sink-clob-test-data/config" \
  -H "Content-Type: application/json" \
  -d @"/tmp/postgres-cdc-sink-clob-test-data.json"

echo "[register-connector] Done."
