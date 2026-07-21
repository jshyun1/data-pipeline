#!/usr/bin/env bash
# 이미 설치된 폐쇄망 서버에서 서비스를 (재)기동한다. 최초 설치는 install.sh 사용.
set -euo pipefail

cd "$(dirname "$0")/.."

PROFILE_ARGS=()
[ "${1:-}" = "--with-poc" ] && PROFILE_ARGS=(--profile poc)

docker compose "${PROFILE_ARGS[@]}" up -d --no-build
echo "완료. 상태 확인: ./offline/healthcheck.sh"
