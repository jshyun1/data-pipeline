#!/usr/bin/env bash
# 폐쇄망 배포 후 상태 확인 - 모든 서비스가 running/healthy인지 확인하고,
# 하나라도 아니면 비정상 종료 코드를 반환한다. 설계서 §13.7이 이 스크립트의
# 통과를 운영 승인 기준으로 삼으라고 명시하고 있다.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ -f .env ]; then
  # docker compose용 .env는 값에 공백이 그대로 올 수 있어(KAFKA_HEAP_OPTS=-Xms256m -Xmx512m
  # 같은 값) 그냥 source하면 깨진다 - 첫 '='만 구분자로 써서 한 줄씩 직접 export한다.
  # 이미 설정된 환경변수가 .env 값보다 우선하도록(docker compose와 동일한 우선순위) 건너뛴다.
  set -a
  while IFS='=' read -r key value; do
    [[ -z "$key" || "$key" == \#* ]] && continue
    [ -n "${!key+x}" ] && continue
    export "$key=$value"
  done < .env
  set +a
fi

echo "== 컨테이너 상태 =="
docker compose ps

FAILED=0
while IFS=$'\t' read -r name state health; do
  [ "$name" = "NAME" ] && continue
  if [ "$state" != "running" ]; then
    echo "  X ${name}: ${state} (기동 실패)" >&2
    FAILED=1
    continue
  fi
  if [ -n "$health" ] && [ "$health" != "healthy" ]; then
    echo "  X ${name}: running / ${health}" >&2
    FAILED=1
  fi
done < <(docker compose ps --format '{{.Name}}\t{{.State}}\t{{.Health}}')

if [ "$FAILED" -eq 1 ]; then
  echo >&2
  echo "일부 서비스가 정상 기동되지 않았습니다. 위 목록과 'docker compose logs <서비스명>'을 확인하세요." >&2
  exit 1
fi

echo
echo "== pipeline-api API 응답 확인 =="
if curl -sf "http://localhost:${PIPELINE_API_PORT:-8081}/api/pipelines" >/dev/null; then
  echo "  정상"
else
  echo "  경고: pipeline-api API에 응답이 없습니다." >&2
  exit 1
fi

echo
echo "모든 서비스가 정상 기동되었습니다."
