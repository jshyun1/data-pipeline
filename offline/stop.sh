#!/usr/bin/env bash
# 폐쇄망 서버에서 서비스를 정지한다. 컨테이너/네트워크를 지우는 'down'이 아니라
# 'stop'을 쓴다 - 볼륨은 물론이고 컨테이너 자체도 남겨둬서 재기동이 빠르고,
# 실수로 데이터 볼륨까지 건드릴 위험이 없다.
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose stop
echo "완료. 다시 기동하려면: ./offline/start.sh"
