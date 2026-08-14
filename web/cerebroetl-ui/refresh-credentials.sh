#!/bin/sh
# NiFi/Airflow 공유 서비스계정으로 주기적으로 재로그인해서 최신 토큰을 받아
# nginx가 include하는 conf 파일에 기록하고 nginx를 리로드한다.
# 실패해도 기존 파일은 건드리지 않는다(오래된 토큰이라도 만료 전까지는 살아있는 게
# 갱신 실패로 서비스가 통째로 끊기는 것보다 낫다).
set -eu

# crond가 컨테이너 environment를 물려주지 않아서, 기동 시 떠둔 스냅샷을 직접 불러온다.
# (컨테이너를 직접 기동한 최초 1회 호출 때는 docker-entrypoint-credentials.sh가 아직
# 이 파일을 안 만들었을 수 있어 없으면 조용히 건너뛴다 - 그때는 현재 프로세스 env를 그대로 씀)
[ -f /docker-entrypoint-credentials/env.sh ] && . /docker-entrypoint-credentials/env.sh

OUT_DIR=/etc/nginx/generated
OUT_FILE="$OUT_DIR/shared-credentials.conf"
TMP_FILE="$OUT_FILE.tmp"
COOKIE_JAR=/tmp/airflow-refresh-cookies.txt

mkdir -p "$OUT_DIR"

log() { echo "[$(date -u +%FT%TZ)] $*"; }

# 한 시스템의 로그인이 실패해도 다른 시스템의 자격증명은 갱신할 수 있도록 기존 줄을
# 각각 보존한다. 컨테이너 최초 기동이라 파일이 없으면 빈 값으로 시작한다.
existing_credential_line() {
  grep -F "set \$$1 " "$OUT_FILE" 2>/dev/null || printf 'set $%s "";\n' "$1"
}

NIFI_LINE=$(existing_credential_line nifi_shared_bearer)
AIRFLOW_LINE=$(existing_credential_line airflow_shared_cookie)

# --- NiFi: /nifi-api/access/token (username/password -> JWT 원문) ---
NIFI_TOKEN=$(curl -sk -X POST "https://nifi:8443/nifi-api/access/token" \
  -H "Host: ${NIFI_HOST_HEADER:-localhost:8443}" \
  --data-urlencode "username=${NIFI_USERNAME}" \
  --data-urlencode "password=${NIFI_PASSWORD}") || true

if [ -z "$NIFI_TOKEN" ]; then
  log "ERROR: NiFi 토큰 발급 실패, 기존 NiFi 자격증명 유지"
else
  NIFI_LINE="set \$nifi_shared_bearer \"Bearer $NIFI_TOKEN\";"
fi

# --- Airflow: CSRF 토큰 확보 -> Referer 포함 로그인 -> _token 쿠키 추출 ---
rm -f "$COOKIE_JAR"
LOGIN_PAGE=$(curl -sk -c "$COOKIE_JAR" "http://airflow-apiserver:8080/airflow/auth/login/" || true)
CSRF=$(echo "$LOGIN_PAGE" | grep -oE 'name="csrf_token" type="hidden" value="[^"]+"' | sed -E 's/.*value="([^"]+)"/\1/')

if [ -z "$CSRF" ]; then
  log "ERROR: Airflow CSRF 토큰 추출 실패, 기존 Airflow 자격증명 유지"
else
  curl -sk -o /dev/null -b "$COOKIE_JAR" -c "$COOKIE_JAR" \
    -H "Referer: http://airflow-apiserver:8080/airflow/auth/login/" \
    --data-urlencode "csrf_token=$CSRF" \
    --data-urlencode "username=${AIRFLOW_USERNAME}" \
    --data-urlencode "password=${AIRFLOW_PASSWORD}" \
    "http://airflow-apiserver:8080/airflow/auth/login/" || true

  AIRFLOW_TOKEN=$(grep -E "_token" "$COOKIE_JAR" | awk '{print $7}')
  if [ -z "$AIRFLOW_TOKEN" ]; then
    log "ERROR: Airflow 로그인 실패(_token 쿠키 없음), 기존 Airflow 자격증명 유지"
  else
    AIRFLOW_LINE="set \$airflow_shared_cookie \"_token=$AIRFLOW_TOKEN\";"
  fi
fi

cat > "$TMP_FILE" <<EOF
# 자동 생성 파일 - refresh-credentials.sh가 주기적으로 덮어씀. 직접 수정하지 말 것.
$NIFI_LINE
$AIRFLOW_LINE
EOF
mv "$TMP_FILE" "$OUT_FILE"

# 컨테이너 최초 기동 시(nginx 시작 전)엔 reload 대상이 없어 실패하는 게 정상이다 -
# 이때는 nginx가 곧이어 기동하며 방금 쓴 파일을 그대로 읽으니 문제 없다.
nginx -s reload 2>/dev/null || log "nginx reload 스킵(아직 미기동 - 최초 기동 시 정상)"
log "자격증명 갱신 완료"
