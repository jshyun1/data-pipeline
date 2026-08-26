#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# P5b(NiFi 개인계정 대행인증) 인프라 셋업 자동화.
#
# 폐쇄망 서버마다 손으로 하던 것 — 인증서 발급, NiFi truststore 등록, NiFi 재기동,
# cerebro-proxy 사용자/정책 생성, 검증 — 을 한 번에 수행한다. 런북
# docs/p5b-personal-accounts-runbook.md 의 §2·§3 을 그대로 옮긴 것이며, 거기 적힌
# 함정 1·2·3·4·7·8 을 코드로 강제한다.
#
#   사용:  ./deploy/p5b-setup.sh              # 전체 셋업(멱등 - 이미 된 단계는 건너뜀)
#          ./deploy/p5b-setup.sh --verify-only # 현재 상태만 점검
#          ./deploy/p5b-setup.sh --help
#
# 멱등이다. 여러 번 돌려도 안전하고, 중간에 실패하면 고치고 다시 돌리면 된다.
# ---------------------------------------------------------------------------
set -euo pipefail

NIFI_CONTAINER="${NIFI_CONTAINER:-nifi}"
CA_ALIAS="cerebro-proxy-ca"
PROXY_CN="CN=cerebro-proxy"
CERT_DAYS="${CERT_DAYS:-3650}"
VERIFY_ONLY=0
FORCE_CERT=0
SKIP_RESTART=0

log()  { printf '\033[1;36m==\033[0m %s\n' "$*"; }
ok()   { printf '   \033[1;32m✔\033[0m %s\n' "$*"; }
skip() { printf '   \033[1;33m•\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m✘ %s\033[0m\n' "$*" >&2; exit 1; }

usage() {
  sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'
  cat <<'USAGE'

옵션:
  --verify-only    아무것도 바꾸지 않고 현재 상태만 점검한다.
  --force-cert     인증서가 이미 있어도 새로 발급한다(기존 파일은 .bak 으로 보존).
  --skip-restart   NiFi 재기동을 하지 않는다. truststore 를 새로 넣었다면 나중에
                   반드시 직접 재기동해야 한다(NiFi 는 truststore 를 자동 재로드하지 않음).
  --help
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --verify-only)  VERIFY_ONLY=1 ;;
    --force-cert)   FORCE_CERT=1 ;;
    --skip-restart) SKIP_RESTART=1 ;;
    --help|-h)      usage; exit 0 ;;
    *)              die "알 수 없는 옵션: $1 (--help 참고)" ;;
  esac
  shift
done

# ---------------------------------------------------------------------------
# 0. 사전 점검
# ---------------------------------------------------------------------------
log "0/7  사전 점검"

[ -f .env ] || die ".env 가 없습니다. 리포 루트에서 실행하세요."
command -v docker >/dev/null || die "docker 명령을 찾을 수 없습니다."
command -v python3 >/dev/null || die "python3 가 필요합니다(JSON 파싱)."

# .env 를 통째로 source 하면 주석·따옴표에서 깨질 수 있어 필요한 키만 뽑는다.
# 값이 없으면 빈 문자열. grep 실패가 set -e 로 스크립트를 죽이지 않도록 || true.
envval() { grep -m1 "^$1=" .env 2>/dev/null | cut -d= -f2- | sed 's/^"//;s/"$//' || true; }

TRUSTSTORE_PASSWORD="$(envval NIFI_TRUSTSTORE_PASSWORD)"
NIFI_USER="$(envval NIFI_SINGLE_USER_USERNAME)"
NIFI_PASS="$(envval NIFI_SINGLE_USER_PASSWORD)"
CERT_DIR="$(envval P5B_CERT_DIR)"
CERT_DIR="${CERT_DIR:-./deploy/p5b-certs}"

[ -n "$TRUSTSTORE_PASSWORD" ] || die ".env 에 NIFI_TRUSTSTORE_PASSWORD 가 없습니다."
[ -n "$NIFI_USER" ] || die ".env 에 NIFI_SINGLE_USER_USERNAME 이 없습니다."
[ -n "$NIFI_PASS" ] || die ".env 에 NIFI_SINGLE_USER_PASSWORD 가 없습니다."

docker inspect "$NIFI_CONTAINER" >/dev/null 2>&1 \
  || die "'$NIFI_CONTAINER' 컨테이너가 없습니다. 먼저 스택을 기동하세요."
[ "$(docker inspect -f '{{.State.Running}}' "$NIFI_CONTAINER")" = "true" ] \
  || die "'$NIFI_CONTAINER' 컨테이너가 실행 중이 아닙니다."

ok "docker / python3 / .env / $NIFI_CONTAINER 컨테이너 확인"
ok "인증서 디렉터리: $CERT_DIR"

# 함정 #9 — 배포가 소스 트리를 통째로 교체하면 gitignore 대상인 인증서가 사라져
# nginx 가 재시작 루프에 빠진다. 트리 안이면 경고만 하고 진행한다(로컬 개발은 정상).
case "$(cd "$(dirname "$CERT_DIR")" 2>/dev/null && pwd)/$(basename "$CERT_DIR")" in
  "$PWD"/*)
    printf '   \033[1;33m⚠\033[0m 인증서가 소스 트리 안(%s)에 있습니다.\n' "$CERT_DIR"
    printf '     운영 서버에서는 배포가 deploy/ 를 교체하며 인증서를 지운 장애가 있었습니다.\n'
    printf '     .env 에 P5B_CERT_DIR=/트리/밖/경로 를 설정하는 것을 권합니다.\n'
    ;;
esac

TRUSTSTORE=/opt/nifi/nifi-current/conf/truststore.p12

# NiFi 컨테이너 안에서 REST API 를 호출한다(호스트에 curl/네트워크 의존 안 함).
nifi_api() {   # nifi_api <METHOD> <PATH> [BODY_JSON]
  local method="$1" path="$2" body="${3:-}"
  if [ -n "$body" ]; then
    docker exec -e TOKEN="$NIFI_TOKEN" -e BODY="$body" "$NIFI_CONTAINER" sh -c \
      "curl -sk -X $method 'https://localhost:8443/nifi-api$path' \
         -H \"Authorization: Bearer \$TOKEN\" -H 'Content-Type: application/json' -d \"\$BODY\""
  else
    docker exec -e TOKEN="$NIFI_TOKEN" "$NIFI_CONTAINER" sh -c \
      "curl -sk -X $method 'https://localhost:8443/nifi-api$path' -H \"Authorization: Bearer \$TOKEN\""
  fi
}

nifi_token() {
  NIFI_TOKEN="$(docker exec -e U="$NIFI_USER" -e P="$NIFI_PASS" "$NIFI_CONTAINER" sh -c \
    'curl -sk -X POST https://localhost:8443/nifi-api/access/token -d "username=$U&password=$P"')"
  case "$NIFI_TOKEN" in
    ey*) : ;;
    *)   die "NiFi 토큰 발급 실패. NIFI_SINGLE_USER_USERNAME/PASSWORD 를 확인하세요." ;;
  esac
}

truststore_has_ca() {
  docker exec "$NIFI_CONTAINER" keytool -list -alias "$CA_ALIAS" \
    -keystore "$TRUSTSTORE" -storetype PKCS12 -storepass "$TRUSTSTORE_PASSWORD" >/dev/null 2>&1
}

# ---------------------------------------------------------------------------
# 검증 전용 모드
# ---------------------------------------------------------------------------
if [ "$VERIFY_ONLY" = 1 ]; then
  log "현재 상태 점검 (변경 없음)"
  [ -f "$CERT_DIR/ca.crt" ] && ok "CA 인증서 있음" || skip "CA 인증서 없음 → 미설정"
  [ -f "$CERT_DIR/cerebro-proxy.crt" ] && ok "프록시 인증서 있음" || skip "프록시 인증서 없음"
  if [ -f "$CERT_DIR/cerebro-proxy.crt" ]; then
    if openssl x509 -in "$CERT_DIR/cerebro-proxy.crt" -noout -ext extendedKeyUsage 2>/dev/null \
         | grep -q "TLS Web Client Authentication"; then
      ok "clientAuth EKU 있음 (함정 #2)"
    else
      die "프록시 인증서에 clientAuth EKU 가 없습니다 → NiFi 가 거부합니다(함정 #2)."
    fi
    openssl x509 -in "$CERT_DIR/cerebro-proxy.crt" -noout -enddate \
      | sed 's/notAfter=/   만료: /'
  fi
  truststore_has_ca && ok "NiFi truststore 에 $CA_ALIAS 등록됨" \
                    || skip "NiFi truststore 에 CA 없음 → 미설정"
  nifi_token
  users_json="$(nifi_api GET /tenants/users)"
  echo "$users_json" | python3 -c "
import json,sys
d=json.load(sys.stdin)
hit=[u for u in d.get('users',[]) if u['component']['identity']=='$PROXY_CN']
print('   \033[1;32m✔\033[0m cerebro-proxy 사용자 있음' if hit else '   \033[1;33m•\033[0m cerebro-proxy 사용자 없음')
"
  pol="$(nifi_api GET /policies/write/proxy)"
  echo "$pol" | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except Exception: print('   \033[1;33m•\033[0m /proxy write 정책 없음'); raise SystemExit
us=[u['component']['identity'] for u in d.get('component',{}).get('users',[])]
print('   \033[1;32m✔\033[0m /proxy write 정책에 cerebro-proxy 포함' if '$PROXY_CN' in us
      else '   \033[1;31m✘\033[0m /proxy 정책은 있으나 cerebro-proxy 미포함: %s' % us)
"
  exit 0
fi

# ---------------------------------------------------------------------------
# 1. nifi-conf 볼륨 백업 (truststore 오조작 시 유일한 복구 수단)
# ---------------------------------------------------------------------------
log "1/7  nifi-conf 볼륨 백업"
CONF_VOLUME="$(docker inspect -f \
  '{{range .Mounts}}{{if eq .Destination "/opt/nifi/nifi-current/conf"}}{{.Name}}{{end}}{{end}}' \
  "$NIFI_CONTAINER")"
[ -n "$CONF_VOLUME" ] || die "nifi conf 볼륨을 찾지 못했습니다."
mkdir -p backup
BACKUP_FILE="backup/nifi-conf-$(date +%Y%m%d-%H%M%S).tgz"
docker run --rm -v "$CONF_VOLUME":/from -v "$PWD/backup":/to alpine:3.20 \
  tar czf "/to/$(basename "$BACKUP_FILE")" -C /from . 2>/dev/null
ok "백업: $BACKUP_FILE  (truststore 사고 시 여기서 복원)"

# ---------------------------------------------------------------------------
# 2. 인증서 발급 (함정 #2 - clientAuth EKU)
# ---------------------------------------------------------------------------
log "2/7  mTLS 인증서 발급"
mkdir -p "$CERT_DIR"
if [ -f "$CERT_DIR/ca.crt" ] && [ -f "$CERT_DIR/cerebro-proxy.crt" ] && [ "$FORCE_CERT" = 0 ]; then
  skip "이미 있음 — 건너뜀 (--force-cert 로 재발급)"
else
  if [ "$FORCE_CERT" = 1 ]; then
    for f in ca.crt ca.key cerebro-proxy.crt cerebro-proxy.key; do
      [ -f "$CERT_DIR/$f" ] && mv "$CERT_DIR/$f" "$CERT_DIR/$f.bak.$(date +%Y%m%d-%H%M%S)"
    done
    ok "기존 인증서를 .bak 으로 보존"
  fi
  openssl req -x509 -newkey rsa:2048 -nodes -days "$CERT_DAYS" \
    -keyout "$CERT_DIR/ca.key" -out "$CERT_DIR/ca.crt" -subj "/CN=cerebro-proxy-ca" 2>/dev/null
  openssl req -newkey rsa:2048 -nodes \
    -keyout "$CERT_DIR/cerebro-proxy.key" -out "$CERT_DIR/cerebro-proxy.csr" \
    -subj "/$PROXY_CN" 2>/dev/null
  # EKU clientAuth 가 없으면 NiFi 가 거부한다 - 반드시 넣는다.
  printf 'extendedKeyUsage=clientAuth\nkeyUsage=digitalSignature\n' > "$CERT_DIR/.ext"
  openssl x509 -req -in "$CERT_DIR/cerebro-proxy.csr" -days "$CERT_DAYS" \
    -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -CAcreateserial \
    -out "$CERT_DIR/cerebro-proxy.crt" -extfile "$CERT_DIR/.ext" 2>/dev/null
  rm -f "$CERT_DIR/.ext" "$CERT_DIR/cerebro-proxy.csr"
  chmod 600 "$CERT_DIR"/*.key
  ok "발급 완료 (유효기간 ${CERT_DAYS}일)"
fi

openssl x509 -in "$CERT_DIR/cerebro-proxy.crt" -noout -ext extendedKeyUsage 2>/dev/null \
  | grep -q "TLS Web Client Authentication" \
  || die "프록시 인증서에 clientAuth EKU 가 없습니다(함정 #2). --force-cert 로 재발급하세요."
ok "clientAuth EKU 확인"

# ---------------------------------------------------------------------------
# 3. NiFi truststore 에 CA 등록 (함정 #1 - 반드시 keytool importcert)
# ---------------------------------------------------------------------------
log "3/7  NiFi truststore 에 CA 등록"
TRUSTSTORE_CHANGED=0
if truststore_has_ca; then
  skip "$CA_ALIAS 가 이미 등록돼 있음 — 건너뜀"
else
  docker cp "$CERT_DIR/ca.crt" "$NIFI_CONTAINER:/tmp/p5b-ca.crt"
  # openssl 로 만든 cert-only PKCS12 는 Java 가 0 entries 로 읽는다 → keytool importcert 필수.
  docker exec "$NIFI_CONTAINER" keytool -importcert -trustcacerts -noprompt \
    -alias "$CA_ALIAS" -file /tmp/p5b-ca.crt \
    -keystore "$TRUSTSTORE" -storetype PKCS12 -storepass "$TRUSTSTORE_PASSWORD" >/dev/null
  docker exec "$NIFI_CONTAINER" rm -f /tmp/p5b-ca.crt
  truststore_has_ca || die "CA 등록에 실패했습니다. 백업($BACKUP_FILE)에서 복원하세요."
  TRUSTSTORE_CHANGED=1
  ok "$CA_ALIAS 등록 완료 (trustedCertEntry)"
fi

# ---------------------------------------------------------------------------
# 4. NiFi 재기동 (함정 #8 - truststore 는 자동 재로드되지 않는다)
# ---------------------------------------------------------------------------
log "4/7  NiFi 재기동"
if [ "$TRUSTSTORE_CHANGED" = 0 ]; then
  skip "truststore 변경 없음 — 재기동 불필요"
elif [ "$SKIP_RESTART" = 1 ]; then
  printf '   \033[1;33m⚠\033[0m --skip-restart 지정됨. truststore 를 새로 넣었으므로\n'
  printf '     반드시 나중에 NiFi 를 재기동해야 적용됩니다.\n'
else
  docker restart "$NIFI_CONTAINER" >/dev/null
  printf '   NiFi 기동 대기'
  for _ in $(seq 1 60); do
    if [ "$(docker inspect -f '{{.State.Health.Status}}' "$NIFI_CONTAINER" 2>/dev/null)" = "healthy" ]; then
      printf '\n'; ok "healthy"; break
    fi
    printf '.'; sleep 5
  done
  [ "$(docker inspect -f '{{.State.Health.Status}}' "$NIFI_CONTAINER" 2>/dev/null)" = "healthy" ] \
    || die "NiFi 가 healthy 가 되지 않았습니다. docker logs $NIFI_CONTAINER 를 확인하세요."
fi

# ---------------------------------------------------------------------------
# 5. cerebro-proxy 사용자
# ---------------------------------------------------------------------------
log "5/7  NiFi 사용자 '$PROXY_CN'"
nifi_token
USERS_JSON="$(nifi_api GET /tenants/users)"
PROXY_USER_ID="$(printf '%s' "$USERS_JSON" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for u in d.get('users',[]):
    if u['component']['identity']=='$PROXY_CN':
        print(u['id']); break
")"
if [ -n "$PROXY_USER_ID" ]; then
  skip "이미 존재 (id=$PROXY_USER_ID)"
else
  CREATED="$(nifi_api POST /tenants/users \
    "{\"revision\":{\"version\":0},\"component\":{\"identity\":\"$PROXY_CN\"}}")"
  PROXY_USER_ID="$(printf '%s' "$CREATED" | python3 -c "
import json,sys
try: print(json.load(sys.stdin).get('id',''))
except Exception: pass
")"
  [ -n "$PROXY_USER_ID" ] || die "사용자 생성 실패: $CREATED"
  ok "생성 완료 (id=$PROXY_USER_ID)"
fi

# ---------------------------------------------------------------------------
# 6. /proxy write 정책 (함정 #7 - 기존 정책을 POST 로 덮으면 기존 사용자가 날아간다)
# ---------------------------------------------------------------------------
log "6/7  /proxy write 정책"
POLICY_JSON="$(nifi_api GET /policies/write/proxy)"
ACTION="$(printf '%s' "$POLICY_JSON" | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except Exception: print('CREATE'); raise SystemExit
if 'component' not in d: print('CREATE'); raise SystemExit
us=[u['component']['identity'] for u in d['component'].get('users',[])]
print('DONE' if '$PROXY_CN' in us else 'APPEND')
")"
case "$ACTION" in
  DONE)
    skip "이미 cerebro-proxy 가 포함돼 있음" ;;
  CREATE)
    RES="$(nifi_api POST /policies \
      "{\"revision\":{\"version\":0},\"component\":{\"resource\":\"/proxy\",\"action\":\"write\",\"users\":[{\"id\":\"$PROXY_USER_ID\"}]}}")"
    printf '%s' "$RES" | grep -q '"id"' || die "정책 생성 실패: $RES"
    ok "정책 신규 생성" ;;
  APPEND)
    # 기존 users 를 보존한 채 추가해서 PUT — POST 로 덮으면 기존 사용자가 사라진다.
    BODY="$(printf '%s' "$POLICY_JSON" | PROXY_ID="$PROXY_USER_ID" python3 -c "
import json,os,sys
d=json.load(sys.stdin)
d['component'].setdefault('users',[]).append({'id':os.environ['PROXY_ID']})
print(json.dumps({'revision':d['revision'],'component':d['component']}))
")"
    POLICY_ID="$(printf '%s' "$POLICY_JSON" | python3 -c "import json,sys;print(json.load(sys.stdin)['id'])")"
    RES="$(nifi_api PUT "/policies/$POLICY_ID" "$BODY")"
    printf '%s' "$RES" | grep -q '"id"' || die "정책 갱신 실패: $RES"
    ok "기존 정책에 cerebro-proxy 추가(기존 사용자 보존)" ;;
esac

# ---------------------------------------------------------------------------
# 7. 검증
# ---------------------------------------------------------------------------
log "7/7  검증"
truststore_has_ca && ok "truststore CA 등록" || die "truststore CA 미등록"
nifi_token
printf '%s' "$(nifi_api GET /policies/write/proxy)" | python3 -c "
import json,sys
d=json.load(sys.stdin)
us=[u['component']['identity'] for u in d['component'].get('users',[])]
if '$PROXY_CN' in us: print('   \033[1;32m✔\033[0m /proxy write 정책에 cerebro-proxy 포함')
else: sys.exit('   /proxy 정책에 cerebro-proxy 가 없습니다: %s' % us)
"
openssl x509 -in "$CERT_DIR/cerebro-proxy.crt" -noout -enddate | sed 's/notAfter=/   프록시 인증서 만료: /'

cat <<NEXT

────────────────────────────────────────────────────────────────────
 인프라 셋업 완료. 남은 것은 애플리케이션 쪽입니다.

 1) .env 에 플래그를 켠다 (⚠️ 함정 #6 — nginx 는 auth_request 를 무조건 걸므로
    새 nginx.conf 를 쓰는 환경은 플래그도 반드시 같이 켜야 한다):
       AUTHZ_PROXY_ENABLED=true
       AUTHZ_IDENTITY_SYNC_ENABLED=true
       P5B_CERT_DIR=$CERT_DIR

 2) 재기동:
       docker compose up -d --force-recreate cerebroetl-ui pipeline-api
    (--force-recreate 는 함정 #10 — 마운트 타입이 바뀐 경우 up -d 만으로는 안 살아난다)

 3) 검증: 조회자 계정으로 로그인 → ETL>관리(NiFi 캔버스)가 "보기는 되고 수정은 막힘",
    NiFi 감사 로그에 공유계정이 아니라 실명이 남는지 확인.

 되돌리기: 플래그 off + 재기동. NiFi 문제 시 $BACKUP_FILE 복원.
────────────────────────────────────────────────────────────────────
NEXT
