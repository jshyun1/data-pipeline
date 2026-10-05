#!/usr/bin/env bash
# 외부망(인터넷 되는) 빌드 머신에서 실행. docs/kafka-webservice-design.md §13.2~§13.4.
# 1) APP_VERSION으로 자체 빌드 이미지를 빌드하고 2) 전부 하나의 tar로 묶는다.
# 폐쇄망 서버에서는 이 스크립트를 실행하지 않는다 - 여기서 나온 tar를 반입해서
# load-images.sh로 불러오기만 한다.
#
# 사용법: APP_VERSION=1.0.0 ./offline/save-images.sh
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

# NiFi 화면은 Cerebro 판(nifi/ui-custom, 2026-10-05 채택)을 반드시 이미지에 넣는다 - 빌드 PC 의 .env 값(.env.example 기본 off)과
# 상관없이 켠다. 공식 화면 NAR 사본도 늘 함께 들어가므로, 폐쇄망에서 공식 화면이 필요하면 .env 의 CEREBRO_NIFI_UI=official 로 고른다.
# 이 빌드는 GitHub · npm 접속과 메모리 2GB 이상이 필요하다(8GB PC 는 스택을 멈추고 실행).
export CEREBRO_NIFI_UI_BUILD=on

echo "== 자체 빌드 이미지 빌드 (APP_VERSION=${APP_VERSION}, CEREBRO_NIFI_UI_BUILD=on) =="
APP_VERSION="${APP_VERSION}" docker compose build kafka-connect nifi pipeline-api cerebroetl-ui

# Cerebro NiFi 화면이 실제로 들어갔는지 확인 - 빠진 채 반입하면 폐쇄망에서 기존 화면으로 뜬다.
if ! docker run --rm --network none --entrypoint sh "data-pipeline-nifi:${APP_VERSION}" \
    -c 'ls /opt/nifi/cerebro-ui/custom/nifi-server-nar-*.nar && ls /opt/nifi/cerebro-ui/official/nifi-server-nar-*.nar' > /dev/null; then
  echo "오류: data-pipeline-nifi:${APP_VERSION} 에 Cerebro NiFi 화면(/opt/nifi/cerebro-ui/custom)이 없습니다 - 반입 패키지를 만들지 않습니다." >&2
  exit 1
fi
echo "확인: data-pipeline-nifi:${APP_VERSION} 에 Cerebro NiFi 화면 · 공식 화면 사본이 들어 있음"

IMAGES=(
  "data-pipeline-kafka-connect:${APP_VERSION}"
  "data-pipeline-nifi:${APP_VERSION}"
  "data-pipeline-pipeline-api:${APP_VERSION}"
  "data-pipeline-cerebroetl-ui:${APP_VERSION}"
  "apache/kafka:3.8.0"
  "postgres:16-alpine"
  "docker.elastic.co/beats/filebeat:8.15.3"
  "apache/airflow:3.2.2"
)

echo "== 다음 이미지를 ${IMAGE_TAR} 로 저장 =="
printf '  - %s\n' "${IMAGES[@]}"

docker save "${IMAGES[@]}" -o "${IMAGE_TAR}"

echo "완료: ${IMAGE_TAR} ($(du -h "${IMAGE_TAR}" | cut -f1))"
echo
echo "다음 단계: offline/image-manifest.json의 releaseVersion을 ${APP_VERSION}으로 맞추고,"
echo "docker-compose.yml / .env.example / offline/*.sh / docs를 ${RELEASE_DIR}에 모아"
echo "반입 패키지를 구성하세요 (offline/README.md 참고)."
