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

# 원래 이미지의 기동 스크립트로 넘긴다.
exec /opt/nifi/scripts/start.sh "$@"
