#!/usr/bin/env bash
# 폐쇄망 서버에서 실행. 이미지 load가 끝난 뒤 서비스를 기동한다.
# --no-build로 명시해서, 혹시라도 이미지가 안 맞아 docker compose가 조용히
# 빌드를 시도하는 일이 없도록 한다 (설계서 §13.7: 폐쇄망에서는 build 금지).
set -euo pipefail

cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
  echo ".env가 없습니다. .env.example을 복사해서 폐쇄망 환경에 맞게 값을 채워주세요:" >&2
  echo "  cp .env.example .env && vi .env" >&2
  echo "(TARGET_DB_HOST 등을 실제 회사 DB 주소로, APP_VERSION을 반입한 이미지 버전으로 - CDC 연결정보는 Cerebro ETL 웹 화면에서 등록)" >&2
  exit 1
fi

PROFILE_ARGS=()
if [ "${1:-}" = "--with-poc" ]; then
  PROFILE_ARGS=(--profile poc)
  echo "== --with-poc: 로컬 Target DB 컨테이너도 함께 기동 =="
fi

echo "== docker compose up -d --no-build =="
docker compose "${PROFILE_ARGS[@]}" up -d --no-build

echo
echo "완료. 상태 확인: ./offline/healthcheck.sh"
