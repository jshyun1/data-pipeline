#!/usr/bin/env bash
# 폐쇄망 서버에서 실행. images/ 아래 tar 파일을 전부 docker load한다.
# docs/kafka-webservice-design.md §13.6.
set -euo pipefail

cd "$(dirname "$0")/.."

IMAGE_DIR="${1:-images}"
if [ ! -d "$IMAGE_DIR" ]; then
  echo "이미지 디렉터리를 찾을 수 없습니다: $IMAGE_DIR" >&2
  echo "사용법: $0 [이미지_tar_디렉터리]  (기본값: ./images)" >&2
  exit 1
fi

shopt -s nullglob
TARS=("$IMAGE_DIR"/*.tar)
if [ ${#TARS[@]} -eq 0 ]; then
  echo "$IMAGE_DIR 안에 .tar 파일이 없습니다." >&2
  exit 1
fi

for tar in "${TARS[@]}"; do
  echo "== docker load -i $tar =="
  docker load -i "$tar"
done

echo
echo "== 로드된 이미지 확인 =="
docker images | grep -E 'data-pipeline|apache/kafka|postgres|elastic/beats|oracle-xe' || true
echo
echo "offline/image-manifest.json과 위 목록을 대조해서 빠진 이미지가 없는지 확인하세요."
