#!/usr/bin/env bash
# 외부망(인터넷 되는) 빌드 머신에서 실행. docs/kafka-webservice-design.md §13.2~§13.4.
# 1) APP_VERSION으로 자체 빌드 이미지를 빌드하고 2) 전부 하나의 tar로 묶는다.
# 폐쇄망 서버에서는 이 스크립트를 실행하지 않는다 - 여기서 나온 tar를 반입해서
# load-images.sh로 불러오기만 한다.
#
# 사용법: APP_VERSION=1.0.0 ./offline/save-images.sh [--with-poc]
#   --with-poc: 로컬 Oracle POC 컨테이너 이미지(gvenzl/oracle-xe)까지 같이 저장
#               (실제 회사 DB에 연결하는 운영 배포라면 필요 없음)
set -euo pipefail

cd "$(dirname "$0")/.."

if [ -f .env ]; then
  # docker compose용 .env는 값에 공백이 그대로 올 수 있어(KAFKA_HEAP_OPTS=-Xms256m -Xmx512m
  # 같은 값) 그냥 source하면 깨진다 - 첫 '='만 구분자로 써서 한 줄씩 직접 export한다.
  # docker compose와 동일한 우선순위: 호출 시 이미 넘긴 환경변수(예: APP_VERSION=1.0.0
  # ./save-images.sh)가 .env 파일 값보다 우선해야 하므로, 이미 설정된 변수는 건너뛴다.
  set -a
  while IFS='=' read -r key value; do
    [[ -z "$key" || "$key" == \#* ]] && continue
    [ -n "${!key+x}" ] && continue
    export "$key=$value"
  done < .env
  set +a
fi

APP_VERSION="${APP_VERSION:-latest}"
if [ "$APP_VERSION" = "latest" ]; then
  echo "경고: APP_VERSION이 'latest'입니다. 폐쇄망 반입용 릴리스라면" >&2
  echo "      APP_VERSION=1.0.0 $0 처럼 버전을 명시하세요 (설계서 §13.7: latest 태그 금지)." >&2
fi

RELEASE_DIR="release/data-pipeline-${APP_VERSION}"
IMAGE_DIR="${RELEASE_DIR}/images"
IMAGE_TAR="${IMAGE_DIR}/data-pipeline-images-${APP_VERSION}.tar"
mkdir -p "${IMAGE_DIR}"

echo "== 자체 빌드 이미지 빌드 (APP_VERSION=${APP_VERSION}) =="
APP_VERSION="${APP_VERSION}" docker compose build kafka-connect connect-init nifi pipeline-api pipeline-ui cerebroetl-ui

IMAGES=(
  "data-pipeline-kafka-connect:${APP_VERSION}"
  "data-pipeline-connect-init:${APP_VERSION}"
  "data-pipeline-nifi:${APP_VERSION}"
  "data-pipeline-pipeline-api:${APP_VERSION}"
  "data-pipeline-pipeline-ui:${APP_VERSION}"
  "data-pipeline-cerebroetl-ui:${APP_VERSION}"
  "apache/kafka:3.8.0"
  "postgres:16-alpine"
  "docker.elastic.co/beats/filebeat:8.15.3"
  "apache/airflow:3.2.2"
)

if [ "${1:-}" = "--with-poc" ]; then
  IMAGES+=("gvenzl/oracle-xe:21-slim")
  echo "== --with-poc: 로컬 POC DB 이미지(gvenzl/oracle-xe)도 함께 저장 =="
fi

echo "== 다음 이미지를 ${IMAGE_TAR} 로 저장 =="
printf '  - %s\n' "${IMAGES[@]}"

docker save "${IMAGES[@]}" -o "${IMAGE_TAR}"

echo "완료: ${IMAGE_TAR} ($(du -h "${IMAGE_TAR}" | cut -f1))"
echo
echo "다음 단계: offline/image-manifest.json의 releaseVersion을 ${APP_VERSION}으로 맞추고,"
echo "docker-compose.yml / .env.example / offline/*.sh / docs를 ${RELEASE_DIR}에 모아"
echo "반입 패키지를 구성하세요 (offline/README.md 참고)."
