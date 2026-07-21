#!/usr/bin/env bash
# 로컬 개발 도구용 JDK 21 설치 (커스텀 NiFi 프로세서 / Kafka Connect 커넥터 개발 시 필요).
# 컨테이너 자체는 각 이미지에 필요한 JRE가 내장되어 있어 이 스크립트가 없어도 동작합니다.
set -euo pipefail

if ! command -v sdk >/dev/null 2>&1; then
  echo "[setup-local-dev] Installing SDKMAN..."
  curl -s "https://get.sdkman.io" | bash
fi

# shellcheck disable=SC1090
source "${HOME}/.sdkman/bin/sdkman-init.sh"

echo "[setup-local-dev] Installing Temurin JDK 21..."
sdk install java 21.0.4-tem || true
sdk use java 21.0.4-tem

java -version
echo "[setup-local-dev] Done. JAVA_HOME=${JAVA_HOME:-unset}"
