#!/usr/bin/env bash
# 도커/호스트 재시작 뒤 깨진 바인드 마운트를 복구한다.
#
# 왜 필요한가 (2026-08~09 네 번 같은 일을 손으로 했다)
#   WSL2 + Docker Desktop 에서 호스트를 재시작하면 compose 의 바인드 마운트가 두 가지 모양으로 깨진다.
#     ① 단일 파일 마운트(nginx.conf · filebeat.yml 등)가 «파일 없음» 이 되어 컨테이너가 exit 127 로 죽는다.
#     ② 디렉터리 마운트(airflow/dags)가 «빈 폴더» 로 떠서, 컨테이너는 healthy 인데 DAG 가 하나도 안 보인다.
#        Airflow 는 파일이 사라진 DAG 를 stale 로 표시하고 화면에는 «DAG 준비 중» 만 남는다.
#   둘 다 `docker compose up -d --force-recreate --no-deps <서비스>` 로 낫는다. 이 스크립트가 그걸 찾아서 한다.
#
# 쓰는 법
#   scripts/docker-recover.sh            # 진단 + 복구
#   scripts/docker-recover.sh --check    # 진단만(아무것도 바꾸지 않는다)
#   scripts/docker-recover.sh --yes      # 확인 질문 없이 복구(재부팅 직후 자동 실행용)
#
# 하지 않는 것
#   - 멈춰 있는 배치 · 커넥터를 켜지 않는다. 일시 중지된 DAG 도 풀지 않는다(밀린 회차가 돌 수 있다).
#   - 볼륨을 지우지 않는다. `docker compose down -v` 는 이 스크립트 어디에도 없다.

set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

CHECK_ONLY=0
ASSUME_YES=0
for arg in "$@"; do
  case "$arg" in
    --check) CHECK_ONLY=1 ;;
    --yes|-y) ASSUME_YES=1 ;;
    -h|--help) sed -n '2,25p' "$0"; exit 0 ;;
    *) echo "모르는 옵션: $arg (--check · --yes · --help)"; exit 2 ;;
  esac
done

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
info()  { printf '%s\n' "$*"; }

# DAG 파일이 들어 있어야 하는 컨테이너(디렉터리 마운트가 빈 폴더로 뜨는 쪽).
AIRFLOW_DAG_SERVICES=(airflow-scheduler airflow-dag-processor airflow-apiserver airflow-triggerer)
DAG_DIR_IN_CONTAINER=/opt/airflow/dags

broken_exit127=()
broken_dags=0

info "== 1) 마운트가 깨져 못 뜬 컨테이너"
# exited(127) - 마운트된 파일이 없어 진입점이 실행되지 못한 경우(가장 흔한 모양).
# created     - 바인드 소스가 파일이 아니라 디렉터리가 되어 아예 시작조차 못 한 경우(실측 2026-09-27).
# dead        - 드물게 남는 좀비.
# 일부러 내려 둔 것(target-db · airflow-init 같은 exited(0))은 건드리지 않는다.
while read -r svc state status; do
  [ -z "${svc:-}" ] && continue
  case "$state" in
    exited) [[ "$status" == *"(127)"* ]] || continue ;;
    created|dead) ;;
    *) continue ;;
  esac
  red "   ✗ $svc — $state / $status"
  broken_exit127+=("$svc")
done < <(docker compose ps -a --format '{{.Service}} {{.State}} {{.Status}}' 2>/dev/null)
[ ${#broken_exit127[@]} -eq 0 ] && green "   정상 — 마운트로 못 뜬 컨테이너 없음"

info "== 2) Airflow DAG 디렉터리 (빈 폴더로 마운트되면 DAG 가 stale 이 된다)"
host_dags=$(find airflow/dags -maxdepth 1 -name '*.py' 2>/dev/null | wc -l | tr -d ' ')
info "   호스트 airflow/dags: ${host_dags}개"
if [ "$host_dags" -eq 0 ]; then
  red "   ✗ 호스트에 DAG 파일이 없다 - 마운트 문제가 아니라 파일 문제다. 저장소 상태를 먼저 확인할 것."
else
  for svc in "${AIRFLOW_DAG_SERVICES[@]}"; do
    if ! docker compose ps --format '{{.Service}}' 2>/dev/null | grep -qx "$svc"; then
      continue   # 안 띄우는 구성일 수 있다
    fi
    n=$(docker compose exec -T "$svc" sh -c "ls $DAG_DIR_IN_CONTAINER/*.py 2>/dev/null | wc -l" 2>/dev/null | tr -d ' \r')
    n=${n:-0}
    if [ "$n" -eq 0 ]; then
      red "   ✗ $svc — 컨테이너 안 DAG 0개"
      broken_dags=1
    else
      green "   ✓ $svc — ${n}개"
    fi
  done
fi

if [ ${#broken_exit127[@]} -eq 0 ] && [ "$broken_dags" -eq 0 ]; then
  green "== 복구할 것이 없다."
  exit 0
fi

if [ "$CHECK_ONLY" -eq 1 ]; then
  info "== --check 라 여기서 멈춘다. 복구하려면 옵션 없이 다시 실행."
  exit 1
fi

targets=("${broken_exit127[@]}")
if [ "$broken_dags" -eq 1 ]; then
  # DAG 를 읽는 쪽을 함께 되살린다. apiserver 가 127 목록에 이미 있으면 중복은 아래에서 걸러진다.
  targets+=(airflow-dag-processor airflow-scheduler airflow-triggerer)
fi
# 중복 제거
mapfile -t targets < <(printf '%s\n' "${targets[@]}" | awk '!seen[$0]++')

info "== 3) 재생성 대상: ${targets[*]}"
if [ "$ASSUME_YES" -eq 0 ]; then
  read -r -p "   이 컨테이너들을 --force-recreate --no-deps 로 다시 만든다. 진행할까? [y/N] " answer
  case "$answer" in [yY]*) ;; *) info "   취소."; exit 1 ;; esac
fi

docker compose up -d --force-recreate --no-deps "${targets[@]}" || {
  red "   재생성 실패. 위 메시지를 확인할 것."
  exit 1
}

info "== 4) 확인"
sleep 10
docker compose ps --format '{{.Service}}\t{{.Status}}' | sort
if [ "$broken_dags" -eq 1 ]; then
  for svc in airflow-scheduler airflow-dag-processor; do
    n=$(docker compose exec -T "$svc" sh -c "ls $DAG_DIR_IN_CONTAINER/*.py 2>/dev/null | wc -l" 2>/dev/null | tr -d ' \r')
    info "   $svc DAG 파일: ${n:-0}개"
  done
  info "   DAG 파싱에는 30초~1분쯤 걸린다. 그 뒤에도 화면이 «DAG 준비 중» 이면 dag-processor 로그를 볼 것:"
  info "     docker compose logs --since 5m airflow-dag-processor | grep -iE 'error|traceback'"
fi
green "== 복구 완료. 멈춰 있던 배치 · 커넥터 · 일시 중지된 DAG 는 일부러 건드리지 않았다."
