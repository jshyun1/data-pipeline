#!/bin/sh -e
# nifi.properties에 우리 운영 원칙을 강제하는 값들을 기동할 때마다 적용한다.
#
# 왜 Dockerfile이 아니라 여기서 하는가: conf 디렉터리는 nifi-conf 볼륨이라, 이미지에
# 구워 넣은 값은 "볼륨이 처음 만들어질 때"만 반영되고 이미 쓰고 있는 볼륨에는 적용되지
# 않는다. 그래서 컨테이너가 뜰 때마다 여기서 다시 써준다(같은 값이면 실질 변화 없음).
#
# 왜 docker-compose 환경변수로 안 되는가: NiFi 공식 이미지의 start.sh는 미리 정해둔
# 일부 속성만 환경변수와 연결해두었고 여기서 다루는 속성들은 그 목록에 없다.

NIFI_PROPS=/opt/nifi/nifi-current/conf/nifi.properties
NIFI_USERS=/opt/nifi/nifi-current/conf/users.xml
NIFI_AUTHORIZATIONS=/opt/nifi/nifi-current/conf/authorizations.xml
NIFI_FLOW_JSON=/opt/nifi/nifi-current/conf/flow.json.gz

ensure_tls_stores() {
    keystore=${KEYSTORE_PATH:-/opt/nifi/nifi-current/conf/keystore.p12}
    truststore=${TRUSTSTORE_PATH:-/opt/nifi/nifi-current/conf/truststore.p12}

    if [ -f "${keystore}" ] && [ -f "${truststore}" ]; then
        return
    fi

    if [ -z "${KEYSTORE_PASSWORD:-}" ] || [ -z "${TRUSTSTORE_PASSWORD:-}" ]; then
        echo "NiFi TLS 저장소가 없으며 KEYSTORE_PASSWORD/TRUSTSTORE_PASSWORD도 설정되지 않았습니다." >&2
        exit 1
    fi

    cert_file=$(mktemp /tmp/nifi-cert.XXXXXX.pem)
    trap 'rm -f "${cert_file}"' EXIT HUP INT TERM

    rm -f "${keystore}" "${truststore}"
    keytool -genkeypair \
        -alias nifi-key \
        -keyalg RSA \
        -keysize 3072 \
        -validity 825 \
        -dname "CN=localhost, OU=Cerebro ETL, O=Data Pipeline, L=Seoul, ST=Seoul, C=KR" \
        -ext "SAN=dns:localhost,dns:nifi,ip:127.0.0.1" \
        -keystore "${keystore}" \
        -storetype PKCS12 \
        -storepass "${KEYSTORE_PASSWORD}" \
        -keypass "${KEYSTORE_PASSWORD}" \
        -noprompt
    keytool -exportcert \
        -alias nifi-key \
        -keystore "${keystore}" \
        -storetype PKCS12 \
        -storepass "${KEYSTORE_PASSWORD}" \
        -rfc \
        -file "${cert_file}"
    keytool -importcert \
        -alias nifi-ca \
        -file "${cert_file}" \
        -keystore "${truststore}" \
        -storetype PKCS12 \
        -storepass "${TRUSTSTORE_PASSWORD}" \
        -noprompt

    chmod 600 "${keystore}" "${truststore}"
    rm -f "${cert_file}"
    trap - EXIT HUP INT TERM
    echo "NiFi TLS keystore/truststore 자동 생성 완료"
}

set_prop() {
    key=$1
    value=$2
    if grep -q "^${key}=" "${NIFI_PROPS}"; then
        sed -i "s|^${key}=.*|${key}=${value}|" "${NIFI_PROPS}"
    else
        echo "${key}=${value}" >> "${NIFI_PROPS}"
    fi
    echo "nifi.properties 적용: ${key}=${value}"
}

ensure_read_policy() {
    resource=$1
    policy_id=$2
    username=${SINGLE_USER_CREDENTIALS_USERNAME:-admin}

    if [ ! -f "${NIFI_USERS}" ] || [ ! -f "${NIFI_AUTHORIZATIONS}" ]; then
        return
    fi

    user_id=$(sed -n "s/.*<user identifier=\"\\([^\"]*\\)\" identity=\"${username}\".*/\\1/p" "${NIFI_USERS}" | head -n 1)
    if [ -z "${user_id}" ]; then
        echo "NiFi 권한 보정 건너뜀: users.xml에서 ${username} 사용자를 찾지 못함"
        return
    fi

    if grep -q "resource=\"${resource}\" action=\"R\"" "${NIFI_AUTHORIZATIONS}"; then
        POLICY_RESOURCE=${resource} USER_ID=${user_id} perl -0pi -e '
            my $res = $ENV{"POLICY_RESOURCE"};
            my $uid = $ENV{"USER_ID"};
            s{(<policy[^>]*resource="\Q$res\E"[^>]*action="R"[^>]*>)(.*?)(\s*</policy>)}{
                index($2, $uid) >= 0 ? "$1$2$3" : "$1$2            <user identifier=\"$uid\"/>\n$3"
            }gse;
        ' "${NIFI_AUTHORIZATIONS}"
    else
        POLICY_ID=${policy_id} POLICY_RESOURCE=${resource} USER_ID=${user_id} perl -0pi -e '
            my $id = $ENV{"POLICY_ID"};
            my $res = $ENV{"POLICY_RESOURCE"};
            my $uid = $ENV{"USER_ID"};
            s{    </policies>}{
                "        <policy identifier=\"$id\" resource=\"$res\" action=\"R\">\n"
                . "            <user identifier=\"$uid\"/>\n"
                . "        </policy>\n"
                . "    </policies>"
            }e;
        ' "${NIFI_AUTHORIZATIONS}"
    fi
    echo "NiFi 권한 적용: ${username} -> ${resource} R"
}

ensure_root_group_name() {
    if [ ! -f "${NIFI_FLOW_JSON}" ]; then
        return
    fi

    tmp_flow=$(mktemp /tmp/nifi-flow.XXXXXX.json)
    trap 'rm -f "${tmp_flow}" "${tmp_flow}.gz"' EXIT HUP INT TERM

    gzip -cd "${NIFI_FLOW_JSON}" > "${tmp_flow}"
    if grep -q '"rootGroup"[^{]*:{[^{}]*"name"[[:space:]]*:[[:space:]]*"NiFi Flow"' "${tmp_flow}"; then
        perl -0pi -e 's{("rootGroup"\s*:\s*\{[^{}]*"name"\s*:\s*)"NiFi Flow"}{$1"ETL Root"}s' "${tmp_flow}"
        gzip -n -c "${tmp_flow}" > "${tmp_flow}.gz"
        cp "${tmp_flow}.gz" "${NIFI_FLOW_JSON}"
        echo "NiFi 루트 그룹 이름 적용: NiFi Flow -> ETL Root"
    fi

    rm -f "${tmp_flow}" "${tmp_flow}.gz"
    trap - EXIT HUP INT TERM
}

ensure_tls_stores

# 적재 시작은 오직 Airflow만 지시한다는 원칙을 컨테이너 레벨에서 강제한다.
#
# 기본값(true)이면 NiFi는 재시작할 때 직전에 RUNNING이던 프로세서를 자동으로 다시
# 켠다. 그러면 아무도 지시하지 않았는데 컨테이너 재시작만으로 대량 적재가 시작된다 -
# 실제로 이것 때문에 두 번 사고가 났다. OOM으로 죽은 프로세서가 재시작마다 되살아나
# 같은 자리에서 다시 죽는 무한 루프가 한 번, 정지시키지 않고 둔 배치 그룹이 재시작과
# 함께 전량 재적재를 다시 돌려 적재 집계가 부풀려진 것이 한 번이다.
#
# false면 무슨 이유로 재시작되든 전부 STOPPED로 올라오므로, 시작 시점을 Airflow가
# 온전히 통제한다. 상시 대기가 필요한 그룹(logfile, http-ingest)도 각자의 제어 DAG가
# 있으므로 그쪽에서 켜면 된다.
set_prop nifi.flowcontroller.autoResumeState "${NIFI_AUTO_RESUME_STATE:-false}"

# cerebroetl-ui nginx가 NiFi 콘솔을 iframe으로 프록시할 때 X-ProxiedEntitiesChain
# 헤더로 실제 포털 사용자를 전달한다. NiFi는 이 헤더를 아무 클라이언트에게나
# 허용하지 않으므로, nginx 클라이언트 인증서의 DN을 대행 프록시 주체로 등록한다.
set_prop nifi.security.user.authorized.proxy.identities "${NIFI_AUTHORIZED_PROXY_IDENTITIES:-CN=cerebroetl-ui}"

# pipeline-api의 ETL 처리 이력 수집기는 /nifi-api/counters를 읽어
# PutDatabaseRecord의 "INSERT updates performed" 증가분을 관측한다. 기존 conf 볼륨에는
# Initial Admin Identity가 다시 적용되지 않아 /counters 정책이 빠질 수 있으므로,
# 기동 때마다 현재 single-user 계정에 읽기 권한을 보정한다.
ensure_read_policy "/counters" "a1b2c3d4-1111-3333-8888-999999999999"

# Controller Services 화면의 최상위 Scope는 루트 프로세스 그룹 이름을 그대로 보여준다.
# 기본 이름인 "NiFi Flow" 대신 이 프로젝트의 ETL 최상위 영역명으로 고정한다.
ensure_root_group_name

# 원래 이미지의 기동 스크립트로 넘긴다.
exec /opt/nifi/scripts/start.sh "$@"
