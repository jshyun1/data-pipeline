#!/bin/sh
# Jenkins entry point: locate the data-pipeline checkout and bring the whole
# docker-compose stack up in place. Unlike a single-JAR/single-image service,
# this project has no separate build-server/registry-push step to delegate
# to (make.sh/docker.sh) - docker-control.sh builds and starts everything.
set -eu

fail() {
    echo "ERROR: $1" >&2
    exit 1
}

: "${WORKSPACE:?WORKSPACE environment variable is required}"

if [ -n "${PROJECT_DIR:-}" ]; then
    project_dir=${PROJECT_DIR}
elif [ -f "${WORKSPACE}/docker-compose.yml" ] && [ -f "${WORKSPACE}/docker-control.sh" ]; then
    project_dir=${WORKSPACE}
elif [ -f "${WORKSPACE}/data-pipeline/docker-compose.yml" ] && [ -f "${WORKSPACE}/data-pipeline/docker-control.sh" ]; then
    project_dir=${WORKSPACE}/data-pipeline
else
    fail "Cannot locate data-pipeline under WORKSPACE=${WORKSPACE}"
fi

[ -f "${project_dir}/docker-compose.yml" ] || fail "docker-compose.yml not found: ${project_dir}/docker-compose.yml"
[ -f "${project_dir}/docker-control.sh" ] || fail "docker-control.sh not found: ${project_dir}/docker-control.sh"

echo "WORKSPACE=${WORKSPACE}"
echo "PROJECT_DIR=${project_dir}"

if git -C "${WORKSPACE}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "GIT_COMMIT=$(git -C "${WORKSPACE}" rev-parse --short HEAD)"
else
    echo "WARNING: WORKSPACE is not a Git worktree; continuing with the checked-out files." >&2
fi

# dev: --profile poc로 로컬 Oracle/target-db POC 컨테이너까지 포함, prod: 사내 실 DB 연결 전제.
deploy_target=${DEPLOY_TARGET:-prod}
exec sh "${project_dir}/docker-control.sh" up "${deploy_target}"
