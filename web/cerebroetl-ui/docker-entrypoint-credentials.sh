#!/bin/sh
# nginx 기동 전에 자격증명을 한 번 확보해두고(콜드 스타트 때 include 대상 파일이
# 아예 없어서 nginx가 기동 실패하는 것 방지), crond로 주기 갱신을 건 뒤 nginx를
# 포그라운드로 넘긴다.
#
# crond는 컨테이너의 environment(docker-compose에서 준 NIFI_USERNAME 등)를 작업에
# 물려주지 않으므로, 지금 프로세스가 보고 있는 env를 파일로 떠서 refresh-credentials.sh가
# 실행될 때마다 직접 source하게 한다.
set -eu

env | sed -E "s/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/export \\1='\\2'/" > /docker-entrypoint-credentials/env.sh

/docker-entrypoint-credentials/refresh-credentials.sh || echo "초기 자격증명 확보 실패 - crond가 재시도함"

crond -b -l 8

exec nginx -g "daemon off;"
