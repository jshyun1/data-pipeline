#!/bin/sh
# Cerebro NiFi 화면(②안, 2026-10-05) 빌드 - nifi/Dockerfile 의 nifi-ui-on 단계가 부른다.
#
#   1) NiFi 공식 소스에서 화면 폴더(nifi-frontend)만 같은 버전 태그로 받는다.
#   2) patches/*.patch(Cerebro 수정분)를 번호 순서대로 적용한다 - 하나라도 안 맞으면 빌드를 멈춘다.
#   3) Pretendard 글꼴 파일을 넣고 화면을 빌드한다.
#   4) 공식 nifi-server-nar 안의 화면(nifi-ui war)만 빌드 결과로 바꾼 NAR 을 만든다(repack_nar.py).
#
# 사용: build.sh <NiFi 버전> <공식 nifi-server-nar 경로> <출력 폴더>
#
# 메모리: 화면 빌드는 2GB 이상을 쓴다. 로컬(8GB)에서는 스택을 멈추고 빌드해야 한다 - 스택과 같이 돌리면
# WSL 이 메모리 부족으로 멈춘다(2026-10-05 실측).
set -eu

NIFI_VERSION=$1
OFFICIAL_NAR=$2
OUT=$3
HERE=$(cd "$(dirname "$0")" && pwd)
# Cerebro 화면(web/cerebroetl-ui/src/assets/fonts)과 같은 Pretendard 가변 글꼴 - 1.3.9 의 파일이 그것과 바이트까지 같다(2026-10-05 확인).
# 패키지가 72MB 라 npm 캐시(빌드 캐시 마운트)에 두고 다시 쓴다.
PRETENDARD_VERSION=1.3.9

git clone --quiet --depth 1 --filter=blob:none --sparse --branch "rel/nifi-${NIFI_VERSION}" \
    https://github.com/apache/nifi.git /build/src
cd /build/src
git sparse-checkout set nifi-frontend

for patch in "${HERE}"/patches/*.patch; do
    git apply --whitespace=nowarn "${patch}"
    echo "Cerebro 수정분 적용: $(basename "${patch}")"
done

cd nifi-frontend/src/main/frontend

font_dir=libs/shared/src/assets/fonts/pretendard
mkdir -p "${font_dir}"
npm pack --silent "pretendard@${PRETENDARD_VERSION}" > /dev/null
tar -xzf "pretendard-${PRETENDARD_VERSION}.tgz" -C "${font_dir}" --strip-components=5 \
    package/dist/web/variable/woff2/PretendardVariable.woff2
tar -xzf "pretendard-${PRETENDARD_VERSION}.tgz" -O package/dist/LICENSE.txt > "${font_dir}/LICENSE.txt"
rm -f "pretendard-${PRETENDARD_VERSION}.tgz"
test -s "${font_dir}/PretendardVariable.woff2"

npm ci --no-audit --no-fund
# nx 데몬은 빌드 한 번에는 필요 없다(NiFi 의 maven 빌드와 같은 설정).
NX_DAEMON=false npx nx build nifi --configuration=production --skip-nx-cache

dist=dist/nifi/browser
[ -f "${dist}/index.html" ] || dist=dist/nifi
mkdir -p "${OUT}/custom"
python3 "${HERE}/repack_nar.py" "${OFFICIAL_NAR}" "${dist}" "${OUT}/custom/$(basename "${OFFICIAL_NAR}")"
