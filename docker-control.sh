#!/usr/bin/env bash
# 로컬/운영 서버 공통 진입점. data-pipeline은 dw-app0X-svr류와 달리 Eureka에
# 등록되는 단일 JAR 서비스가 아니라, Oracle/Kafka/NiFi/Airflow까지
# 포함한 자체완결형 docker-compose 스택이라 이미지 하나를 레지스트리에 올려
# pull하는 방식이 아니라 이 스크립트가 그 스택을 통째로 기동/종료한다.
set -euo pipefail

ACTION="${1:-}"
TARGET="${2:-}"

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd "${SCRIPT_DIR}"

BASE_COMPOSE="docker-compose.yml"

usage() {
    echo "Usage:"
    echo "  ./docker-control.sh up dev    # --profile poc 포함: 로컬 Oracle/target-db POC 컨테이너까지 기동"
    echo "  ./docker-control.sh up prod   # 사내 실 Oracle/타란툴라DB 연결 전제, POC 컨테이너 제외"
    echo "  ./docker-control.sh down"
    echo "  ./docker-control.sh logs"
    echo "  ./docker-control.sh ps"
}

ensure_env_file() {
    if [ ! -f .env ]; then
        [ -f .env.example ] || { echo "ERROR: .env.example not found" >&2; exit 1; }
        echo "==> .env가 없어 .env.example을 복사합니다. 배포 전 실제 값으로 반드시 채워주세요: ${SCRIPT_DIR}/.env"
        cp .env.example .env
    fi
}

compose_up() {
    if ! docker compose "$@" -f "$BASE_COMPOSE" up -d --build; then
        echo "ERROR: docker compose up failed. Recent pipeline-api logs:" >&2
        docker compose -f "$BASE_COMPOSE" logs --tail=200 pipeline-api >&2 || true
        echo "ERROR: docker compose status:" >&2
        docker compose -f "$BASE_COMPOSE" ps >&2 || true
        exit 1
    fi
}

case "$ACTION" in
    up)
        ensure_env_file
        case "$TARGET" in
            dev)
                compose_up --profile poc
                ;;
            prod)
                compose_up
                ;;
            *)
                usage
                exit 1
                ;;
        esac
        ;;
    down)
        # POC 프로필 컨테이너가 떠 있었을 수도 있으니 항상 같이 내린다.
        docker compose --profile poc -f "$BASE_COMPOSE" down
        ;;
    logs)
        docker compose -f "$BASE_COMPOSE" logs -f
        ;;
    ps)
        docker compose -f "$BASE_COMPOSE" ps
        ;;
    *)
        usage
        exit 1
        ;;
esac
