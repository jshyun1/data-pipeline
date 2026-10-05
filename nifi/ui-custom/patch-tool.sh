#!/bin/sh
# Cerebro NiFi 화면 수정분(patches/*.patch)을 고칠 때 쓰는 도구.
#
#   patch-tool.sh checkout <작업 폴더> [NiFi 버전]
#       NiFi 공식 소스의 화면 폴더(nifi-frontend)만 그 버전 태그로 받고 수정분을 모두 적용한다. 버전을 안 주면 .env 의 NIFI_IMAGE_TAG.
#       고칠 파일은 <작업 폴더>/nifi-frontend/src/main/frontend/ 아래에 있다.
#
#   patch-tool.sh save <작업 폴더>
#       고친 내용을 수정분 파일로 다시 쓴다. 파일은 원래 속해 있던 수정분으로 들어간다(각 .patch 의 «+++ b/경로» 기준).
#       새로 만든 파일은 어느 수정분에 넣을지 몰라서 알려주기만 한다 - 해당 .patch 에 한 번 손으로 넣으면(git diff 로 뽑아 붙이기)
#       다음부터는 save 가 알아서 챙긴다.
#
# 수정분을 고친 뒤 반영: 로컬은 스택을 멈추고 `docker compose build nifi` → `docker compose up -d nifi`(README 참고).
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
PATCHES="${HERE}/patches"
cmd=${1:-}
dir=${2:-}
[ -n "${cmd}" ] && [ -n "${dir}" ] || { sed -n '2,13p' "$0"; exit 1; }

case "${cmd}" in
checkout)
    version=${3:-$(grep -E '^NIFI_IMAGE_TAG=' "${HERE}/../../.env" 2>/dev/null | cut -d= -f2)}
    [ -n "${version}" ] || { echo "NiFi 버전을 알 수 없습니다 - 세 번째 인자로 주십시오(예: 2.2.0)" >&2; exit 1; }
    [ ! -e "${dir}" ] || { echo "이미 있는 폴더입니다: ${dir}" >&2; exit 1; }
    git clone --quiet --depth 1 --filter=blob:none --sparse --branch "rel/nifi-${version}" https://github.com/apache/nifi.git "${dir}"
    git -C "${dir}" sparse-checkout set nifi-frontend
    for patch in "${PATCHES}"/*.patch; do
        git -C "${dir}" apply --whitespace=nowarn "${patch}"
        echo "적용: $(basename "${patch}")"
    done
    # 수정분이 만든 새 파일도 git diff 에 잡히게 한다.
    git -C "${dir}" add -N .
    echo "준비 완료: ${dir}/nifi-frontend/src/main/frontend (NiFi ${version})"
    ;;
save)
    git -C "${dir}" add -N .
    claimed=$(mktemp)
    for patch in "${PATCHES}"/*.patch; do
        paths=$(sed -n 's|^+++ b/||p' "${patch}")
        printf '%s\n' ${paths} >> "${claimed}"
        # 해시 길이가 저장소마다 달라지지 않게 전체 해시로 쓴다(짧은 해시는 저장소 크기에 따라 7·8자리로 바뀐다).
        # shellcheck disable=SC2086
        git -C "${dir}" diff --full-index -- ${paths} > "${patch}.new"
        if cmp -s "${patch}" "${patch}.new"; then
            rm -f "${patch}.new"
        else
            mv "${patch}.new" "${patch}"
            echo "갱신: $(basename "${patch}")"
        fi
    done
    git -C "${dir}" diff --name-only | sort > "${claimed}.all"
    sort -u "${claimed}" > "${claimed}.sorted"
    orphans=$(comm -23 "${claimed}.all" "${claimed}.sorted")
    rm -f "${claimed}" "${claimed}.all" "${claimed}.sorted"
    if [ -n "${orphans}" ]; then
        echo "어느 수정분에도 속하지 않은 변경(손으로 알맞은 .patch 에 넣을 것):"
        echo "${orphans}" | sed 's/^/  /'
    fi
    ;;
*)
    sed -n '2,13p' "$0"
    exit 1
    ;;
esac
