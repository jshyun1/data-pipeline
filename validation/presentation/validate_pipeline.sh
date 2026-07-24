#!/usr/bin/env bash
set -euo pipefail

# Presentation evidence runner.
# It intentionally leaves the Kafka topic and this script in place so the
# validation can be repeated and inspected after the presentation is generated.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RESULT_DIR="${ROOT_DIR}/validation/presentation/results"
RUN_ID="${1:-$(date '+%Y%m%d%H%M%S')}"
REPORT_FILE="${RESULT_DIR}/validation-${RUN_ID}.md"
KAFKA_TOPIC="cerebroetl.presentation.validation.v1"
CDC_TOPIC="ppt-validation-cdc.APPUSER.PPT_VALIDATION_EVENTS"
CDC_SOURCE_CONNECTOR="ppt-validation-oracle-source"
CDC_SINK_CONNECTOR="ppt-validation-postgres-sink"
CDC_INITIAL_VALUE="INSERT_${RUN_ID}"
CDC_UPDATED_VALUE="UPDATE_${RUN_ID}"

mkdir -p "${RESULT_DIR}"

declare -a RESULT_ROWS=()

now_ms() {
  date +%s%3N
}

add_result() {
  local area="$1"
  local check="$2"
  local result="$3"
  local detail="$4"
  RESULT_ROWS+=("| ${area} | ${check} | ${result} | ${detail} |")
}

connector_running() {
  local connector="$1"
  local status
  status="$(curl -fsS "http://localhost:18083/connectors/${connector}/status")"
  [[ "${status}" == *'"connector":{"state":"RUNNING"'* ]] &&
    [[ "${status}" == *'"tasks":[{"id":0,"state":"RUNNING"'* ]]
}

wait_for_target_value() {
  local event_id="$1"
  local event_value="$2"
  local timeout_seconds="$3"
  local started="$4"
  local deadline=$((SECONDS + timeout_seconds))
  local found=""
  while (( SECONDS <= deadline )); do
    found="$(docker exec target-db sh -lc \
      "psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -Atc \"select event_value from cdc_landing.ppt_validation_events where event_id='${event_id}' limit 1;\"")"
    if [[ "${found}" == "${event_value}" ]]; then
      printf '%s\n' "$(( $(now_ms) - started ))"
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_for_target_absence() {
  local event_id="$1"
  local timeout_seconds="$2"
  local started="$3"
  local deadline=$((SECONDS + timeout_seconds))
  local found=""
  while (( SECONDS <= deadline )); do
    found="$(docker exec target-db sh -lc \
      "psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -Atc \"select event_id from cdc_landing.ppt_validation_events where event_id='${event_id}';\"")"
    if [[ -z "${found}" ]]; then
      printf '%s\n' "$(( $(now_ms) - started ))"
      return 0
    fi
    sleep 1
  done
  return 1
}

cd "${ROOT_DIR}"

COMPOSE_VALIDATION="$(docker compose config --quiet 2>&1)" || {
  add_result "구성" "docker compose config" "FAIL" "구성 오류: ${COMPOSE_VALIDATION}"
  exit 1
}
add_result "구성" "docker compose config" "PASS" "Compose 구문 및 변수 치환 성공"

SERVICE_SUMMARY="$(docker compose ps --format '{{.Service}}|{{.State}}|{{.Health}}')"
REQUIRED_SERVICES=(
  kafka kafka-connect oracle-db target-db nifi
  pipeline-api metadata-db keycloak cerebroetl-ui
  airflow-apiserver airflow-scheduler airflow-dag-processor
)
UNHEALTHY_SERVICES=()
for service in "${REQUIRED_SERVICES[@]}"; do
  line="$(printf '%s\n' "${SERVICE_SUMMARY}" | awk -F'|' -v name="${service}" '$1 == name {print; exit}')"
  state="$(printf '%s' "${line}" | cut -d'|' -f2)"
  health="$(printf '%s' "${line}" | cut -d'|' -f3)"
  if [[ "${state}" != "running" ]] || [[ "${health}" != "healthy" ]]; then
    UNHEALTHY_SERVICES+=("${service}:${state:-missing}/${health:-unknown}")
  fi
done
if (( ${#UNHEALTHY_SERVICES[@]} == 0 )); then
  add_result "런타임" "핵심 12개 서비스" "PASS" "모두 running/healthy"
else
  add_result "런타임" "핵심 12개 서비스" "FAIL" "${UNHEALTHY_SERVICES[*]}"
fi

CONNECTORS=(
  source-13-oracle-appuser-customers
  sink-13-postgresql-cdc_landing-customers
  sink-8-postgresql-log_landing-app_log
)
FAILED_CONNECTORS=()
for connector in "${CONNECTORS[@]}"; do
  if ! connector_running "${connector}"; then
    FAILED_CONNECTORS+=("${connector}")
  fi
done
if (( ${#FAILED_CONNECTORS[@]} == 0 )); then
  add_result "Kafka Connect" "등록 커넥터 3개" "PASS" "connector/task 모두 RUNNING"
else
  add_result "Kafka Connect" "등록 커넥터 3개" "FAIL" "${FAILED_CONNECTORS[*]}"
fi

AIRFLOW_HEALTH="$(curl -fsS http://localhost:8090/api/v2/monitor/health)"
if [[ "${AIRFLOW_HEALTH}" == *'"metadatabase":{"status":"healthy"}'* ]] &&
   [[ "${AIRFLOW_HEALTH}" == *'"scheduler":{"status":"healthy"'* ]] &&
   [[ "${AIRFLOW_HEALTH}" == *'"dag_processor":{"status":"healthy"'* ]]; then
  add_result "Airflow" "메타DB·스케줄러·DAG Processor" "PASS" "공식 health API 모두 healthy"
else
  add_result "Airflow" "메타DB·스케줄러·DAG Processor" "FAIL" "health API 응답 확인 필요"
fi

PIPELINE_API_RESPONSE="$(curl -fsS http://localhost:18086/api/pipelines)"
if [[ "${PIPELINE_API_RESPONSE}" == *'"success":true'* ]] &&
   [[ "${PIPELINE_API_RESPONSE}" == *'"id":13'* ]]; then
  add_result "Cerebro ETL API" "파이프라인 목록 조회" "PASS" "배포된 CDC 파이프라인 ID 13 확인"
else
  add_result "Cerebro ETL API" "파이프라인 목록 조회" "FAIL" "업무 API 응답 확인 필요"
fi

docker exec -i oracle-db sqlplus -s / as sysdba >/dev/null <<'SQL'
SET PAGESIZE 0 FEEDBACK OFF VERIFY OFF HEADING OFF ECHO OFF
ALTER SESSION SET CONTAINER = XEPDB1;
BEGIN
  EXECUTE IMMEDIATE '
    CREATE TABLE appuser.ppt_validation_events (
      event_id VARCHAR2(64) PRIMARY KEY,
      event_value VARCHAR2(200) NOT NULL,
      updated_at TIMESTAMP DEFAULT SYSTIMESTAMP
    )';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -955 THEN RAISE; END IF;
END;
/
BEGIN
  EXECUTE IMMEDIATE '
    ALTER TABLE appuser.ppt_validation_events
    ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE NOT IN (-32588, -32589) THEN RAISE; END IF;
END;
/
EXIT;
SQL

docker exec target-db sh -lc \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -c "
    CREATE TABLE IF NOT EXISTS cdc_landing.ppt_validation_events (
      event_id VARCHAR(64) PRIMARY KEY,
      event_value VARCHAR(200) NOT NULL,
      updated_at TIMESTAMP
    );"' >/dev/null

SOURCE_CONFIG="$(
  curl -fsS http://localhost:18083/connectors/source-13-oracle-appuser-customers/config |
    python3 -c '
import json, sys
c = json.load(sys.stdin)
c.pop("name", None)
c["topic.prefix"] = "ppt-validation-cdc"
c["table.include.list"] = "APPUSER.PPT_VALIDATION_EVENTS"
c["schema.history.internal.kafka.topic"] = "schema-changes.ppt-validation-oracle-source"
print(json.dumps(c, separators=(",", ":")))
'
)"
SINK_CONFIG="$(
  curl -fsS http://localhost:18083/connectors/sink-13-postgresql-cdc_landing-customers/config |
    python3 -c '
import json, sys
c = json.load(sys.stdin)
c.pop("name", None)
c["topics"] = "ppt-validation-cdc.APPUSER.PPT_VALIDATION_EVENTS"
c["table.name.format"] = "cdc_landing.ppt_validation_events"
print(json.dumps(c, separators=(",", ":")))
'
)"

curl -fsS -X PUT \
  -H 'Content-Type: application/json' \
  --data "${SOURCE_CONFIG}" \
  "http://localhost:18083/connectors/${CDC_SOURCE_CONNECTOR}/config" >/dev/null
curl -fsS -X PUT \
  -H 'Content-Type: application/json' \
  --data "${SINK_CONFIG}" \
  "http://localhost:18083/connectors/${CDC_SINK_CONNECTOR}/config" >/dev/null

CONNECTOR_DEADLINE=$((SECONDS + 120))
while (( SECONDS <= CONNECTOR_DEADLINE )); do
  if connector_running "${CDC_SOURCE_CONNECTOR}" &&
     connector_running "${CDC_SINK_CONNECTOR}"; then
    break
  fi
  sleep 2
done
if connector_running "${CDC_SOURCE_CONNECTOR}" &&
   connector_running "${CDC_SINK_CONNECTOR}"; then
  add_result "전용 CDC 프로세스" "Source/Sink 배포" "PASS" "전용 테이블·커넥터·토픽을 보존"
else
  add_result "전용 CDC 프로세스" "Source/Sink 배포" "FAIL" "120초 내 connector/task RUNNING 미확인"
fi

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic "${KAFKA_TOPIC}" \
  --partitions 1 \
  --replication-factor 1 >/dev/null

KAFKA_VALUE="{\"runId\":\"${RUN_ID}\",\"purpose\":\"presentation-validation\",\"status\":\"PASS\"}"
printf '%s|%s\n' "${RUN_ID}" "${KAFKA_VALUE}" |
  docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic "${KAFKA_TOPIC}" \
    --property parse.key=true \
    --property key.separator='|' >/dev/null

KAFKA_CONSUMED="$(
  docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic "${KAFKA_TOPIC}" \
    --from-beginning \
    --timeout-ms 5000 \
    --property print.key=true 2>/dev/null || true
)"
if [[ "${KAFKA_CONSUMED}" == *"${RUN_ID}"* ]] &&
   [[ "${KAFKA_CONSUMED}" == *'"status":"PASS"'* ]]; then
  add_result "Kafka" "고정 검증 토픽 produce/consume" "PASS" "topic=${KAFKA_TOPIC}, key=${RUN_ID}"
else
  add_result "Kafka" "고정 검증 토픽 produce/consume" "FAIL" "생산한 메시지를 재소비하지 못함"
fi

INSERT_STARTED="$(now_ms)"
docker exec -i oracle-db sqlplus -s / as sysdba >/dev/null <<SQL
SET PAGESIZE 0 FEEDBACK OFF VERIFY OFF HEADING OFF ECHO OFF
ALTER SESSION SET CONTAINER = XEPDB1;
DELETE FROM appuser.ppt_validation_events WHERE event_id = '${RUN_ID}';
INSERT INTO appuser.ppt_validation_events (event_id, event_value, updated_at)
VALUES ('${RUN_ID}', '${CDC_INITIAL_VALUE}', SYSTIMESTAMP);
COMMIT;
EXIT;
SQL

if INSERT_LATENCY_MS="$(wait_for_target_value "${RUN_ID}" "${CDC_INITIAL_VALUE}" 60 "${INSERT_STARTED}")"; then
  add_result "CDC E2E" "Oracle INSERT → Kafka → PostgreSQL" "PASS" "event_id=${RUN_ID}, ${INSERT_LATENCY_MS}ms"
else
  add_result "CDC E2E" "Oracle INSERT → PostgreSQL" "FAIL" "60초 내 타깃 반영 없음"
fi

UPDATE_STARTED="$(now_ms)"
docker exec -i oracle-db sqlplus -s / as sysdba >/dev/null <<SQL
SET PAGESIZE 0 FEEDBACK OFF VERIFY OFF HEADING OFF ECHO OFF
ALTER SESSION SET CONTAINER = XEPDB1;
UPDATE appuser.ppt_validation_events
SET event_value = '${CDC_UPDATED_VALUE}', updated_at = SYSTIMESTAMP
WHERE event_id = '${RUN_ID}';
COMMIT;
EXIT;
SQL
if UPDATE_LATENCY_MS="$(wait_for_target_value "${RUN_ID}" "${CDC_UPDATED_VALUE}" 60 "${UPDATE_STARTED}")"; then
  add_result "CDC E2E" "Oracle UPDATE → PostgreSQL UPSERT" "PASS" "event_id=${RUN_ID}, ${UPDATE_LATENCY_MS}ms"
else
  add_result "CDC E2E" "Oracle UPDATE → PostgreSQL UPSERT" "FAIL" "60초 내 타깃 반영 없음"
fi

DELETE_STARTED="$(now_ms)"
docker exec -i oracle-db sqlplus -s / as sysdba >/dev/null <<SQL
SET PAGESIZE 0 FEEDBACK OFF VERIFY OFF HEADING OFF ECHO OFF
ALTER SESSION SET CONTAINER = XEPDB1;
DELETE FROM appuser.ppt_validation_events WHERE event_id = '${RUN_ID}';
COMMIT;
EXIT;
SQL
if DELETE_LATENCY_MS="$(wait_for_target_absence "${RUN_ID}" 60 "${DELETE_STARTED}")"; then
  add_result "CDC E2E" "Oracle DELETE → PostgreSQL DELETE" "PASS" "event_id=${RUN_ID}, ${DELETE_LATENCY_MS}ms"
else
  add_result "CDC E2E" "Oracle DELETE → PostgreSQL DELETE" "FAIL" "60초 내 타깃 삭제 없음"
fi

PASS_COUNT="$(printf '%s\n' "${RESULT_ROWS[@]}" | grep -c '| PASS |' || true)"
FAIL_COUNT="$(printf '%s\n' "${RESULT_ROWS[@]}" | grep -c '| FAIL |' || true)"

{
  printf '# Cerebro ETL 발표자료 검증 결과\n\n'
  printf -- '- 실행 시각: %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')"
  printf -- '- Run ID: `%s`\n' "${RUN_ID}"
  printf -- '- 결과: **PASS %s / FAIL %s**\n' "${PASS_COUNT}" "${FAIL_COUNT}"
  printf -- '- 보존된 Kafka 검증 토픽: `%s`\n\n' "${KAFKA_TOPIC}"
  printf -- '- 보존된 CDC 토픽: `%s`\n' "${CDC_TOPIC}"
  printf -- '- 보존된 커넥터: `%s`, `%s`\n\n' "${CDC_SOURCE_CONNECTOR}" "${CDC_SINK_CONNECTOR}"
  printf '| 영역 | 검증 항목 | 결과 | 근거 |\n'
  printf '|---|---|:---:|---|\n'
  printf '%s\n' "${RESULT_ROWS[@]}"
  printf '\n## 재실행\n\n'
  printf '```bash\n'
  printf 'cd %s\n' "${ROOT_DIR}"
  printf './validation/presentation/validate_pipeline.sh\n'
  printf '```\n'
} > "${REPORT_FILE}"

printf '%s\n' "${REPORT_FILE}"
printf 'PASS=%s FAIL=%s\n' "${PASS_COUNT}" "${FAIL_COUNT}"

if (( FAIL_COUNT > 0 )); then
  exit 1
fi
