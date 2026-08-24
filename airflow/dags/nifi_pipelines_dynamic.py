"""
NiFi의 최상위(루트 바로 아래) 프로세스 그룹을 "파이프라인 1개"로 취급해서,
그룹마다 start/stop을 수행하는 DAG를 동적으로 생성한다.

kafka_pipelines_dynamic.py와 동일한 패턴 - NiFi에 REST API로 토큰을 발급받아
프로세스 그룹의 실행 상태를 변경한다(캔버스에서 프로세스 그룹을 직접 만드는 것은
여전히 사람이 NiFi 화면에서 함, 이 DAG는 켜고 끄는 것만 담당).

NiFi 2.x는 프로세스 그룹 단위 상태 변경 API가 RUNNING/STOPPED 두 가지만 지원해서
(Kafka 커넥터의 pause와 달리 별도의 "일시정지" 개념 없음) action은 start/stop만 둔다.

Host 헤더 관련 주의: NiFi(Jetty)는 HTTPS 요청의 Host 헤더를 nifi.web.proxy.host
허용 목록과 비교해서 다르면 "Invalid SNI" 400을 반환한다. docker-compose 네트워크
안에서 이 컨테이너(nifi:8443)로 직접 붙으면 그 값이 허용 목록에 있어도 왜인지
거부되는 게 확인돼서(원인 미상), Host 헤더를 이미 허용된 "localhost:8443"으로
명시적으로 덮어써서 우회한다(TCP 연결 자체는 nifi 컨테이너로 정상적으로 감 -
Host 헤더만 바꾸는 것이라 안전).
각 DAG의 스케줄은 코드에 고정하지 않고 Airflow Variable로 뺐다 - Admin > Variables
화면에서 "{dag_id}__schedule" 키에 크론 표현식/프리셋(예: "@hourly", "0 */6 * * *")을
넣으면 다음 DAG 파싱 주기(최대 5분)에 반영된다. Variable이 없으면 기존과 동일하게
schedule=None(수동 트리거 전용)으로 동작한다.
apply_action 뒤의 verify_action이 프로세스 그룹의 컴포넌트 상태 집계
(runningCount/invalidCount 등)를 다시 조회해서 액션이 실제로 반영됐는지 확인한다
- 예전에 FetchFile이 invalid로 고착돼 시작이 안 되던 결함 같은 것이 있으면
호출 자체는 성공해도 이 검증 단계에서 실패로 잡힌다.

verify_action 다음의 verify_target_db_landing은 한 단계 더 나아가 NiFi 컴포넌트
상태가 아니라 실제 타겟 DB(target-db의 Postgres)에 "이번 실행이" 데이터를
적재했는지 확인한다. NiFi 프로세스 그룹은 배치 잡과 달리 한 번 시작하면 계속
도는 구조라 "이번 실행이 만든 건수"라는 경계가 원래 없다 - 대신 dag_run이
시작된 시각(dag_run.start_date) 이후로 적재된 행만 세는 방식으로 근사한다
(타겟 테이블 전체 row count를 그냥 세면 이전에 이미 쌓인 데이터 때문에 이번
실행이 실제로 아무것도 안 넣었어도 통과해버려서 안 됨).

더 정확한 대안으로 NiFi 자체 Provenance API(특정 프로세서가 실제로 처리한
FlowFile 이력 조회)도 검토했으나 기각했다: NiFi의 provenance 인덱스는
nifi.provenance.repository.rollover.time(기본 10분) 주기로만 커밋되기 때문에
방금 일어난 이벤트가 최대 10분간 검색이 안 될 수 있어서, "실행 직후 바로
검증"이라는 이 태스크의 목적과 맞지 않는다(실측: 로그 라인을 넣고 20초 후
조회해도 0건). 그래서 타겟 테이블에 직접 타임스탬프로 필터링하는 방식을 쓴다.

이 방식의 알려진 한계: UPSERT로 적재하는 파이프라인(예: employees-batch-sync)은
NiFi PutDatabaseRecord가 레코드 스키마에 없는 컬럼(synced_at 등)은 INSERT
시점의 DEFAULT로만 채워지고 UPDATE(충돌) 경로에서는 건드리지 않는다 - 즉 이번
실행이 기존 행만 UPDATE하고 새로 INSERT한 행이 없으면 이 검증이 놓칠 수 있다.
INSERT 전용 파이프라인(http-ingest, logfile)에는 이 한계가 없다.

count 조회는 verify_action과 동일하게 VERIFY_ATTEMPTS/VERIFY_INTERVAL_SECONDS로
재시도한다 - NiFi 프로세서 자체가 폴링 주기를 갖는 경우가 있어서(예: logfile의
ListFile은 1분 주기로만 새 파일을 확인 - 실측으로 확인함) start 직후 바로
확인하면 아직 아무것도 안 왔을 뿐인데 실패로 오탐될 수 있다.
재시도 끝까지 count=0이어도 그 자체를 실패로 보지 않는다 - 이번 주기에 소스
쪽에 신규/변경 데이터가 없었을 뿐일 수 있기 때문이다(특히 QueryDatabaseTable
같은 증분 소스). 대신 NiFi 프로세스 그룹 자체에 실제 문제 신호(ERROR bulletin,
invalid 컴포넌트)가 있는지 추가로 확인해서, 신호가 있으면 그때 진짜 실패로
처리하고, 없으면 skipped로 끝낸다(=Asset 이벤트도 발행 안 됨 - 실제로 아무것도
적재되지 않았으니 맞는 동작).

어떤 process group이 어느 schema.table/타임스탬프 컬럼으로 적재하는지는 NiFi
캔버스 안에만 있고 이 DAG가 알 방법이 없어서, schedule과 동일한 패턴으로
Admin > Variables의 "{dag_id}__target_schema"/"{dag_id}__target_table"/
"{dag_id}__target_timestamp_column"에서 읽는다 - 셋 다 설정 안 하면 이
태스크는 skipped로 끝난다(실패 아님). action이 start가 아니면(stop) 검증할
대상이 없으므로 마찬가지로 skip.
검증에 성공하면(=태스크가 success로 끝나면) outlets로 등록해둔 Asset 이벤트가
자동 발행된다 - skipped인 경우엔 Asset 이벤트가 발행되지 않는다(Airflow의
표준 동작: outlets는 태스크가 success로 끝났을 때만 이벤트를 만든다).
"""
import os
import re
import time
from datetime import datetime

import requests

import _pipeline_svc_auth  # noqa: F401  # import 만으로 pipeline-api 서비스 토큰 자동주입(P4 §7.6)
import urllib3
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset, Param
from airflow.sdk.exceptions import AirflowSkipException

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # 자체 서명 인증서

NIFI_BASE_URL_DEFAULT = "https://nifi:8443"
NIFI_HOST_HEADER_DEFAULT = "localhost:8443"
ACTIONS = ["start", "stop"]
VERIFY_ATTEMPTS = 6
VERIFY_INTERVAL_SECONDS = 5

# 완료 대기(wait_for_group_completion) 튜닝값. 실측 근거: 테이블 하나의 추출
# 쿼리가 56초, 1,565만 건 전체 적재가 약 20분 걸렸다. 폴링을 너무 촘촘히 하면
# NiFi API에 부담만 주고, 타임아웃이 짧으면 정상 적재를 실패로 만든다.
WAIT_POLL_INTERVAL_SECONDS = 15
# 연속 몇 번 유휴여야 완료로 볼지. 프로세서 사이를 넘어가는 짧은 순간에도
# queued/active가 0으로 보일 수 있어서 한 번만 보고 끝내면 안 된다.
IDLE_SETTLE_CHECKS = 3
# 시작 후 이 시간까지 아무 활동도 없으면 "처리할 데이터가 없었다"로 보고 끝낸다.
WAIT_START_GRACE_SECONDS = 180
WAIT_COMPLETION_TIMEOUT_SECONDS = 7200

# docker-compose.yml의 AIRFLOW_CONN_TARGET_DB_POSTGRES 환경변수로 등록되는 Connection.
TARGET_DB_CONN_ID = "target_db_postgres"
TARGET_DB_NAME = os.environ.get("TARGET_DB_NAME", "tarantula")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PIPELINE_API_BASE_URL = "http://pipeline-api:8081"


def _as_int(value) -> int:
    """NiFi status 응답의 건수는 정수일 때도 있고 "1,234" 같은 문자열일 때도 있다."""
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    return int(str(value).replace(",", "").strip() or 0)


INSERT_COUNTER_NAME = "INSERT updates performed"
_PROCESSOR_ID_IN_CONTEXT = re.compile(r"\(([0-9a-fA-F-]{36})\)\s*$")


def _walk_processor_snapshots(snapshot: dict):
    """status(recursive=true) 응답에서 하위 그룹까지 훑어 프로세서 스냅샷을 모은다."""
    for entry in snapshot.get("processorStatusSnapshots") or []:
        processor = entry.get("processorStatusSnapshot")
        if processor:
            yield processor
    for entry in snapshot.get("processGroupStatusSnapshots") or []:
        child = entry.get("processGroupStatusSnapshot")
        if child:
            yield from _walk_processor_snapshots(child)


def _insert_counters_by_processor(base_url: str, host_header: str, token: str) -> dict[str, int]:
    """PutDatabaseRecord가 올리는 "적재 행수" 카운터를 프로세서별로 뽑는다.

    FlowFile 건수(flowFilesOut)는 "파일 몇 개"라 행수가 아니다 - 155 MB짜리
    FlowFile 하나가 166만 행인 식이라, 실제 적재 건수는 이 카운터로만 알 수 있다.
    """
    try:
        response = requests.get(
            f"{base_url}/nifi-api/counters",
            headers={"Authorization": f"Bearer {token}", "Host": host_header},
            verify=False,
            timeout=15,
        )
        response.raise_for_status()
        counters = response.json()["counters"]["aggregateSnapshot"]["counters"]
    except Exception as exc:  # 카운터는 부가 정보라 못 읽어도 대기 자체는 계속한다
        print(f"카운터 조회 실패(적재 건수 표시 생략): {exc}")
        return {}
    result = {}
    for counter in counters:
        if counter.get("name") != INSERT_COUNTER_NAME:
            continue
        matched = _PROCESSOR_ID_IN_CONTEXT.search(counter.get("context") or "")
        if matched:
            result[matched.group(1)] = _as_int(counter.get("valueCount"))
    return result


def _collect_new_bulletins(pg_id: str, pg_name: str, base_url: str, host_header: str,
                           token: str, seen: set) -> list[str]:
    """그룹 안에서 새로 올라온 경고/에러를 찍고, 그중 ERROR만 요약해서 돌려준다.

    NiFi의 bulletin은 5분 남짓만 메모리에 남는 링버퍼라, 적재가 실패해도 그 자리에
    없으면 어디에도 안 남는다. 대기하는 동안 계속 긁어서 태스크 로그에 박아둔다.

    커서(after 파라미터)를 쓰지 않는 이유: bulletin id는 NiFi 프로세스 안에서만
    단조 증가하고 재시작하면 1부터 다시 시작한다. 커서를 쓰면 재시작 직후의 실패가
    통째로 걸러진다 - 백엔드 수집기가 실제로 이것 때문에 사고를 놓쳤다(2026-07-29).
    링버퍼 자체가 작으니 매번 전체를 가져와서 이 태스크 안에서 본 id로만 거른다.
    """
    try:
        response = requests.get(
            f"{base_url}/nifi-api/flow/bulletin-board",
            params={"groupId": pg_id},
            headers={"Authorization": f"Bearer {token}", "Host": host_header},
            verify=False,
            timeout=15,
        )
        response.raise_for_status()
        bulletins = response.json()["bulletinBoard"]["bulletins"]
    except Exception as exc:
        print(f"[{pg_name}] bulletin 조회 실패(이번 주기 건너뜀): {exc}")
        return []
    errors = []
    for entry in bulletins:
        bulletin = entry.get("bulletin") or {}
        # id는 재시작하면 재사용되므로 메시지까지 묶어야 같은 태스크 안에서 안전하다.
        key = (bulletin.get("id"), bulletin.get("sourceId"), bulletin.get("message"))
        if key in seen:
            continue
        seen.add(key)
        level = bulletin.get("level")
        source = bulletin.get("sourceName")
        message = bulletin.get("message")
        print(f"[{pg_name}]   !! {level} {source}: {message}")
        if level == "ERROR":
            errors.append(f"{source}: {message}")
    return errors


def _fail_if_errors(pg_name: str, errors: list[str]):
    """대기 중 ERROR bulletin을 하나라도 봤으면 태스크를 실패시킨다.

    "큐 0 + 활성 스레드 0"만으로 완료를 판정하면 실패도 완료로 보인다 - NiFi 쪽
    failure 관계가 자동종료(폐기)라 실패한 FlowFile이 큐에 남지 않기 때문이다.
    실제로 원천 Oracle이 안 떠 있어 추출이 전부 실패한 날, truncate만 먼저 커밋돼
    DZ 5개 테이블이 통째로 비었는데도 DAG는 초록불로 끝났다(2026-07-29).

    WARNING은 평상시에도 올라오므로 실패로 보지 않는다. ERROR만 본다.
    """
    if not errors:
        return
    preview = "\n  - ".join(errors[:5])
    more = f"\n  ... 외 {len(errors) - 5}건" if len(errors) > 5 else ""
    raise RuntimeError(
        f"[{pg_name}] 그룹은 유휴가 됐지만 처리 중 ERROR가 {len(errors)}건 발생했습니다. "
        f"적재가 누락됐을 수 있으니 반드시 확인하십시오.\n  - {preview}{more}"
    )


def _print_load_summary(pg_name: str, base_url: str, host_header: str, token: str,
                        baseline: dict[str, int], processor_names: dict[str, str]):
    """이번 실행에서 프로세서별로 몇 행을 넣었는지 표로 남긴다."""
    final = _insert_counters_by_processor(base_url, host_header, token)
    rows = []
    for processor_id, name in processor_names.items():
        if processor_id not in final:
            continue
        before = baseline.get(processor_id, 0)
        after = final[processor_id]
        # NiFi가 중간에 재시작되면 카운터가 0부터 다시 오른다(그땐 현재값이 곧 이번분).
        delta = after - before if after >= before else after
        if delta > 0:
            rows.append((name, delta))
    if not rows:
        print(f"[{pg_name}] 이번 실행 적재 건수: 없음 (적재 프로세서가 없거나 새로 넣은 행이 0)")
        return
    total = sum(delta for _, delta in rows)
    print(f"[{pg_name}] 이번 실행 적재 건수 (총 {total:,}행)")
    for name, delta in sorted(rows, key=lambda r: -r[1]):
        print(f"[{pg_name}]   {name:<24} {delta:>12,} 행")


def _downstream_group_ids(pg_id: str, connections: list[dict]) -> set[str]:
    """pg_id의 출력포트가 곧바로 이어지는 다른 그룹들의 id."""
    result = set()
    for entry in connections:
        component = entry.get("component", entry)
        source = component.get("source", {})
        destination = component.get("destination", {})
        if source.get("groupId") == pg_id and source.get("type") == "OUTPUT_PORT":
            dest_group = destination.get("groupId")
            if dest_group and dest_group != pg_id:
                result.add(dest_group)
    return result


def build_group_chain(pg_id: str, connections: list[dict]) -> list[str]:
    """pg_id에서 출력포트를 따라 도달하는 그룹들을 상류->하류 순서로 반환.

    NiFi의 커넥션은 데이터를 넘겨줄 뿐 실행 상태를 전파하지 않는다. 그래서 DZ만
    켜면 DW는 STOPPED 그대로이고, DZ가 보낸 FlowFile은 DW 입력포트 앞에 쌓이기만
    한다. 이 함수로 연결된 그룹을 찾아 한 DAG가 체인 전체를 제어하게 한다.
    """
    ordered = [pg_id]
    visited = {pg_id}
    frontier = [pg_id]
    # 캔버스에서 실수로 순환을 만들 수 있으므로 visited로 끊는다.
    while frontier:
        current = frontier.pop(0)
        for nxt in sorted(_downstream_group_ids(current, connections)):
            if nxt in visited:
                continue
            visited.add(nxt)
            ordered.append(nxt)
            frontier.append(nxt)
    return ordered


def _get_nifi_token() -> str:
    # NiFi가 Keycloak/OIDC를 제거하고 Single User 인증(공유 서비스계정)으로 전환됨에 따라,
    # /nifi-api/access/token에 username/password를 보내 JWT를 직접 발급받는다.
    # 이 엔드포인트는 응답 본문이 JSON이 아니라 JWT 문자열 그대로임에 주의.
    base_url = os.environ.get("NIFI_BASE_URL", NIFI_BASE_URL_DEFAULT)
    username = os.environ.get("NIFI_USERNAME", "")
    password = os.environ.get("NIFI_PASSWORD", "")
    response = requests.post(
        f"{base_url}/nifi-api/access/token",
        data={"username": username, "password": password},
        headers={"Host": os.environ.get("NIFI_HOST_HEADER", NIFI_HOST_HEADER_DEFAULT)},
        verify=False,
        timeout=10,
    )
    response.raise_for_status()
    return response.text


def _set_group_state(pg_id: str, state: str, base_url: str, host_header: str, token: str):
    response = requests.put(
        f"{base_url}/nifi-api/flow/process-groups/{pg_id}",
        json={"id": pg_id, "state": state},
        headers={"Authorization": f"Bearer {token}", "Host": host_header},
        verify=False,
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def apply_process_group_action(pg_ids: list[str], base_url: str, host_header: str, **context):
    """체인에 속한 그룹을 한꺼번에 제어한다.

    체인 전체를 "동시에" 켜는 것이 중요하다. DZ -> DW처럼 출력포트로 이어진
    구조에서 상류만 켜고 하류를 나중에 켜면, 상류가 내보낸 FlowFile이 인계 큐에
    쌓이다가 백프레셔 임계치(기본 1GB)에 걸려 상류까지 통째로 멈춘다(실측으로
    확인함 - to-dw 큐가 1GB에 도달하자 DZ 40개 프로세서가 전부 정지).
    """
    action = context["params"]["action"]
    if action not in ACTIONS:
        raise ValueError(f"알 수 없는 action: {action}")
    state = "RUNNING" if action == "start" else "STOPPED"

    token = _get_nifi_token()
    for pg_id in pg_ids:
        result = _set_group_state(pg_id, state, base_url, host_header, token)
        print(f"NiFi 프로세스 그룹 {pg_id} {action}({state}) 완료: {result}")


def wait_for_group_completion(
    pg_id: str, pg_name: str, base_url: str, host_header: str, **context
):
    """프로세스 그룹이 이번 작업을 다 끝낼 때까지 기다린다.

    NiFi에는 배치 잡 같은 "실행 완료" 개념이 없어서(Provenance는 이 환경에서 조회
    불가, 프로세서 단위 Status History는 항상 비어 있음), 그룹 전체가 유휴 상태
    - 대기 중인 FlowFile이 0이고 돌고 있는 스레드도 0 - 가 되는 것으로 완료를
    판정한다.

    함정: 시작 직후에도 이 두 값이 0이다(아직 아무것도 안 흘렀으니). 그대로 보면
    즉시 완료로 오판하므로, 한 번이라도 활동을 관측한 뒤부터 판정을 시작하고,
    그마저도 연속 IDLE_SETTLE_CHECKS회 유휴여야 완료로 인정한다. 시작 자체가
    늦는 경우를 위해 활동을 못 본 채 WAIT_START_GRACE_SECONDS가 지나면 "이번엔
    처리할 게 없었다"로 보고 정상 종료한다.
    """
    action = context["params"]["action"]
    if action != "start":
        raise AirflowSkipException(f"action={action}이라 완료 대기를 건너뜀 (start일 때만 대기)")

    token = _get_nifi_token()
    deadline = time.time() + WAIT_COMPLETION_TIMEOUT_SECONDS
    started_at = time.time()
    idle_streak = 0
    seen_activity = False

    # 시작 시점의 적재 카운터를 기준점으로 잡아둔다. 카운터는 NiFi 재시작 전까지
    # 누적이라, 이번 실행분만 보려면 끝값에서 이걸 빼야 한다.
    baseline_counters = _insert_counters_by_processor(base_url, host_header, token)
    seen_bulletins: set = set()
    errors: list[str] = []
    processor_names: dict[str, str] = {}

    while time.time() < deadline:
        response = requests.get(
            f"{base_url}/nifi-api/flow/process-groups/{pg_id}/status",
            params={"recursive": "true"},
            headers={"Authorization": f"Bearer {token}", "Host": host_header},
            verify=False,
            timeout=15,
        )
        if response.status_code == 401:  # 장시간 대기 중 토큰 만료 시 재발급
            token = _get_nifi_token()
            continue
        response.raise_for_status()
        snapshot = response.json()["processGroupStatus"]["aggregateSnapshot"]
        queued = _as_int(snapshot.get("queuedCount"))
        active = _as_int(snapshot.get("activeThreadCount"))

        if queued > 0 or active > 0:
            seen_activity = True
            idle_streak = 0
        else:
            idle_streak += 1

        print(
            f"[{pg_name}] queued={queued} activeThreads={active} "
            f"idle={idle_streak}/{IDLE_SETTLE_CHECKS} activity={seen_activity}"
        )

        # 합계만으로는 "뭔가 돌고 있다"까지밖에 모른다. 같은 응답에 프로세서별
        # 스냅샷이 이미 들어있으니, 지금 스레드를 잡고 있는 놈을 이름으로 찍어준다.
        for processor in _walk_processor_snapshots(snapshot):
            processor_names[processor["id"]] = processor["name"]
            if _as_int(processor.get("activeThreadCount")) > 0:
                print(
                    f"[{pg_name}]   > {processor['name']} ({processor['type']}) "
                    f"스레드={processor.get('activeThreadCount')} "
                    f"입력={processor.get('input')} 출력={processor.get('output')}"
                )
        errors.extend(_collect_new_bulletins(
            pg_id, pg_name, base_url, host_header, token, seen_bulletins
        ))

        if seen_activity and idle_streak >= IDLE_SETTLE_CHECKS:
            print(f"[{pg_name}] 완료 - 대기 FlowFile 0, 활성 스레드 0")
            _print_load_summary(pg_name, base_url, host_header, token,
                                baseline_counters, processor_names)
            _fail_if_errors(pg_name, errors)
            return
        if not seen_activity and time.time() - started_at > WAIT_START_GRACE_SECONDS:
            print(f"[{pg_name}] 유예시간 내 아무 활동이 없어 처리할 데이터가 없었던 것으로 봄")
            _fail_if_errors(pg_name, errors)
            return
        time.sleep(WAIT_POLL_INTERVAL_SECONDS)

    raise RuntimeError(
        f"[{pg_name}] {WAIT_COMPLETION_TIMEOUT_SECONDS}초 안에 완료되지 않음 "
        f"(queued={queued}, activeThreads={active}). 백프레셔로 막혔거나 하류 그룹이 "
        f"소비하지 못하고 있을 수 있음 - NiFi 캔버스에서 큐 상태를 확인할 것"
    )


def stop_process_group_after_run(pg_ids: list[str], base_url: str, host_header: str, **context):
    # 배치성 그룹(DZ 등)은 "하루 1번 돌고 다음날 그 시각에 다시" 동작이 되려면
    # 실행이 끝난 뒤 반드시 STOPPED로 돌아가야 한다 - NiFi의 GenerateFlowFile은
    # STOPPED -> RUNNING으로 전환되는 순간 주기와 무관하게 즉시 1회 실행되는
    # 것으로 관측됐고(2026-07-26), 이 특성을 이용해 "다음 Airflow 스케줄 실행
    # = 다음 날의 1회 실행"이 되도록 만든다. 계속 RUNNING으로 남겨두면 그룹이
    # 이미 실행 중이라 다음날 apply_action(start)이 아무 효과가 없다.
    # context["params"]["action"]과 무관하게 무조건 STOPPED로 보낸다 - 이 태스크
    # 자체가 "실행 후 정리" 목적이라 수동 stop 트리거일 때도 정지 상태를 재확인하는
    # 것뿐이라 안전하다.
    token = _get_nifi_token()
    # 하류부터 거꾸로 멈춘다. 상류를 먼저 멈추면 하류가 아직 처리 중인 FlowFile을
    # 남긴 채 인계 큐만 붙잡고 있게 된다.
    for pg_id in reversed(pg_ids):
        result = _set_group_state(pg_id, "STOPPED", base_url, host_header, token)
        print(f"NiFi 프로세스 그룹 {pg_id} 실행 후 자동 정지 완료: {result}")


def verify_process_group_action(pg_ids: list[str], base_url: str, host_header: str, **context):
    action = context["params"]["action"]
    token = _get_nifi_token()

    pending = list(pg_ids)
    counts = {}
    for attempt in range(VERIFY_ATTEMPTS):
        still_pending = []
        for pg_id in pending:
            response = requests.get(
                f"{base_url}/nifi-api/process-groups/{pg_id}",
                headers={"Authorization": f"Bearer {token}", "Host": host_header},
                verify=False,
                timeout=15,
            )
            response.raise_for_status()
            entity = response.json()
            counts = {
                key: entity.get(key, 0)
                for key in ("runningCount", "stoppedCount", "invalidCount", "disabledCount")
            }
            if action == "start":
                # invalid가 하나라도 있으면 그 프로세서는 시작되지 못한 것 (호출은 성공했어도)
                ok = counts["invalidCount"] == 0 and counts["stoppedCount"] == 0 and counts["runningCount"] > 0
            else:
                ok = counts["runningCount"] == 0
            if ok:
                print(f"검증 통과: {pg_id} {action} 반영 확인 {counts}")
            else:
                still_pending.append(pg_id)
        pending = still_pending
        if not pending:
            return
        time.sleep(VERIFY_INTERVAL_SECONDS)

    raise RuntimeError(f"{action} 후에도 상태가 기대와 다른 그룹이 있음: {pending} (마지막 관측 {counts})")


def verify_target_db_landing(
    dag_id: str,
    pg_id: str,
    base_url: str,
    host_header: str,
    target_schema: str | None,
    target_table: str | None,
    target_timestamp_column: str | None,
    **context,
):
    action = context["params"]["action"]
    if action != "start":
        raise AirflowSkipException(f"action={action}이라 타겟 DB 적재 검증은 건너뜀 (start일 때만 검증)")
    if not (target_schema and target_table and target_timestamp_column):
        raise AirflowSkipException(
            f"Admin > Variables에 '{dag_id}__target_schema'/'{dag_id}__target_table'/"
            f"'{dag_id}__target_timestamp_column'이 모두 설정되지 않아 타겟 DB 적재 검증을 건너뜀"
        )
    if not all(_IDENTIFIER_RE.match(v) for v in (target_schema, target_table, target_timestamp_column)):
        raise ValueError(
            f"target 설정 형식이 올바르지 않습니다: {target_schema}.{target_table}.{target_timestamp_column}"
        )

    # dag_run 시작 시각 이후로 적재된 행만 센다 (테이블 전체 count는 이전에
    # 이미 쌓인 데이터 때문에 이번 실행이 아무것도 안 넣었어도 통과해버림).
    run_start = context["dag_run"].start_date
    hook = PostgresHook(postgres_conn_id=TARGET_DB_CONN_ID)
    sql = f"SELECT COUNT(*) FROM {target_schema}.{target_table} WHERE {target_timestamp_column} >= %s"

    count = 0
    for attempt in range(VERIFY_ATTEMPTS):
        count = hook.get_first(sql, parameters=(run_start,))[0]
        if count > 0:
            print(
                f"타겟 DB 적재 확인: {target_schema}.{target_table} "
                f"({target_timestamp_column} >= {run_start.isoformat()}) = {count}건"
            )
            # 대시보드가 GET /api/v2/assets/events로 조회할 수 있도록 실제 적재
            # 건수를 Asset 이벤트의 extra로 남긴다. outlets=[...]에 넘긴 것과
            # 동일한 Asset을 다시 만들어서 키로 써야 한다(build_dag의 target_outlets
            # 표현식과 반드시 일치해야 함).
            target_asset = Asset(f"postgres://target-db/{TARGET_DB_NAME}/{target_schema}/{target_table}")
            context["outlet_events"][target_asset].extra = {"count": count}
            return
        time.sleep(VERIFY_INTERVAL_SECONDS)

    # 재시도 끝까지 count=0. 이 자체는 실패가 아니다 - 이번 주기에 신규/변경된
    # 소스 데이터가 없었을 뿐일 수 있다(특히 QueryDatabaseTable류 증분 소스).
    # NiFi 쪽에 실제 문제 신호(ERROR bulletin, invalid 컴포넌트)가 있을 때만
    # 진짜 실패로 처리한다.
    token = _get_nifi_token()
    response = requests.get(
        f"{base_url}/nifi-api/process-groups/{pg_id}",
        headers={"Authorization": f"Bearer {token}", "Host": host_header},
        verify=False,
        timeout=15,
    )
    response.raise_for_status()
    entity = response.json()
    error_bulletins = [
        b.get("bulletin", {}).get("message", "")
        for b in entity.get("bulletins", [])
        if b.get("bulletin", {}).get("level") == "ERROR"
    ]
    invalid_count = entity.get("invalidCount", 0)

    if error_bulletins or invalid_count > 0:
        raise RuntimeError(
            f"{target_schema}.{target_table}에 적재된 데이터가 없고(count=0), NiFi 쪽에서도 "
            f"문제 신호가 확인됩니다 (invalidCount={invalid_count}, error bulletins={error_bulletins})"
        )

    raise AirflowSkipException(
        f"{target_schema}.{target_table}에 이번 실행({run_start.isoformat()}) 이후 적재된 데이터는 "
        "없지만(count=0), NiFi 프로세스 자체는 정상입니다(ERROR bulletin/invalid 컴포넌트 없음) - "
        "신규/변경 소스 데이터가 없었던 것으로 보고 실패 처리하지 않음"
    )


def sanitize(name: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in name).strip("_").lower()


def build_dag(
    pg_id: str,
    pg_name: str,
    base_url: str,
    host_header: str,
    chain: list[str] | None = None,
    group_names: dict[str, str] | None = None,
) -> DAG:
    # dag_id에는 그룹 "이름"을 넣지 않는다 - 캔버스에서 이름을 바꾸면 dag_id가
    # 통째로 바뀌어서 스케줄 Variable과 실행 이력이 조용히 끊어지기 때문.
    # 프로세스 그룹 id(앞 8자리)는 불변이라 이것만으로 dag_id를 만들고,
    # 사람이 읽을 이름은 dag_display_name(화면 표시 전용)으로만 쓴다.
    # (구 형식 nifi_pipeline_{이름}_{id8}_control 에서 2026-07-22 전환)
    dag_id = f"nifi_pipeline_{pg_id[:8]}_control"
    # 스케줄은 코드가 아니라 Airflow Variable로 설정한다 - Admin > Variables에서
    # "{dag_id}__schedule" 키에 크론/프리셋을 넣으면 다음 파싱 주기에 반영됨.
    # Variable이 없으면 기존과 동일하게 수동 트리거 전용(schedule=None)으로 동작.
    # 자동(스케줄) 실행 시엔 params가 없어 action은 항상 Param 기본값("start")으로 동작한다.
    schedule = Variable.get(f"{dag_id}__schedule", default_var=None)
    # 타겟 DB 적재 검증 대상. Admin > Variables에서 "{dag_id}__target_schema"/
    # "{dag_id}__target_table"/"{dag_id}__target_timestamp_column"으로 설정 -
    # 셋 다 있어야 Asset을 만들고 검증 태스크를 의미 있게 돈다(없으면 태스크는
    # 만들어지되 매 실행마다 skipped로 끝남).
    target_schema = Variable.get(f"{dag_id}__target_schema", default_var=None)
    target_table = Variable.get(f"{dag_id}__target_table", default_var=None)
    target_timestamp_column = Variable.get(f"{dag_id}__target_timestamp_column", default_var=None)
    target_outlets = (
        [Asset(f"postgres://target-db/{TARGET_DB_NAME}/{target_schema}/{target_table}")]
        if target_schema and target_table
        else []
    )
    # 배치성 그룹(예: DZ)을 "그 시각에 한 번 돌고 다음날 같은 시각에 다시" 동작시키려면
    # Admin > Variables에서 "{dag_id}__auto_stop_after_run"을 "true"로 설정한다.
    # 기본값(미설정)은 기존과 동일하게 실행 후에도 계속 RUNNING으로 남겨두는 상시 동작
    # (logfile/http-ingest처럼 항상 대기해야 하는 그룹에 적합).
    auto_stop_after_run = Variable.get(f"{dag_id}__auto_stop_after_run", default_var="false").lower() == "true"
    # 출력포트로 이어진 하류 그룹까지 이 DAG가 함께 제어한다(build_group_chain).
    # chain[0]은 항상 자기 자신. NiFi 조회가 실패해 체인을 못 구했으면 자기 자신만.
    chain = chain or [pg_id]
    group_names = group_names or {}
    downstream = chain[1:]
    with DAG(
        dag_id=dag_id,
        dag_display_name=f"ETL_{sanitize(pg_name)}",
        description=f'ETL 프로세스 그룹 "{pg_name}"({pg_id}) 시작/중지 제어'
        + (
            " / 연결된 하류 그룹 함께 제어: "
            + ", ".join(group_names.get(g, g[:8]) for g in downstream)
            if downstream
            else ""
        )
        + (f" (스케줄: {schedule})" if schedule else " (수동 트리거 전용)"),
        schedule=schedule,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["nifi", "pipeline-control"],
        params={"action": Param("start", enum=ACTIONS, description="수행할 동작을 선택하세요")},
    ) as dag:
        _chain_kwargs = {
            "pg_ids": chain,
            "base_url": base_url,
            "host_header": host_header,
        }
        apply = PythonOperator(
            task_id="apply_action",
            python_callable=apply_process_group_action,
            op_kwargs=_chain_kwargs,
        )
        verify = PythonOperator(
            task_id="verify_action",
            python_callable=verify_process_group_action,
            op_kwargs=_chain_kwargs,
        )
        # 그룹마다 완료 대기 태스크를 따로 둔다 - Airflow 화면에서 "어느 그룹이
        # 얼마나 걸렸는지"가 그대로 보이게 하기 위함. task_id에는 이름 대신
        # 그룹 id를 쓴다(dag_id와 같은 이유: 캔버스에서 이름을 바꿔도 이력이
        # 끊기지 않도록). 사람이 읽을 이름은 task_display_name으로만 준다.
        waits = []
        for member in chain:
            member_name = group_names.get(member, member[:8])
            waits.append(
                PythonOperator(
                    task_id=f"wait_{member[:8]}",
                    task_display_name=f"wait_{sanitize(member_name)}",
                    python_callable=wait_for_group_completion,
                    op_kwargs={
                        "pg_id": member,
                        "pg_name": member_name,
                        "base_url": base_url,
                        "host_header": host_header,
                    },
                )
            )
        verify_target = PythonOperator(
            task_id="verify_target_db_landing",
            python_callable=verify_target_db_landing,
            op_kwargs={
                "dag_id": dag_id,
                "pg_id": pg_id,
                "base_url": base_url,
                "host_header": host_header,
                "target_schema": target_schema,
                "target_table": target_table,
                "target_timestamp_column": target_timestamp_column,
            },
            outlets=target_outlets,
        )
        # apply -> verify -> (상류부터 순서대로 완료 대기) -> 적재 검증 -> 정지.
        # 완료 대기가 verify_target보다 앞에 오는 것이 중요하다. 예전에는 시작
        # 직후 바로 적재를 확인해서, 실제로는 20분 넘게 걸리는 작업을 DAG가 14초
        # 만에 success로 끝내버렸다(그 사이 실패해도 아무도 모름).
        chain_tail = verify
        for wait_task in waits:
            chain_tail >> wait_task
            chain_tail = wait_task
        chain_tail >> verify_target
        if auto_stop_after_run:
            stop_after = PythonOperator(
                task_id="stop_after_run",
                python_callable=stop_process_group_after_run,
                op_kwargs=_chain_kwargs,
                trigger_rule="all_done",
            )
            verify_target >> stop_after
        apply >> verify
    return dag


_base_url = os.environ.get("NIFI_BASE_URL", NIFI_BASE_URL_DEFAULT)
_host_header = os.environ.get("NIFI_HOST_HEADER", NIFI_HOST_HEADER_DEFAULT)

# 마지막으로 성공한 프로세스 그룹 조회 결과를 담는 Variable 키.
# NiFi가 잠깐 죽어있는 동안 파싱이 돌면 예전엔 DAG가 통째로 사라져서
# (= 그 사이 스케줄 실행이 조용히 누락) 마지막 성공 조회를 캐시로 재사용한다.
PG_CACHE_VARIABLE_KEY = "nifi_process_groups_cache"
# 그룹 사이 연결(출력포트 -> 입력포트)도 같은 이유로 캐시한다. 연결을 못 읽으면
# 체인을 모르는 채 DAG가 만들어져서, 어제까지처럼 상류만 켜고 하류는 방치하는
# 동작으로 조용히 되돌아가기 때문에 캐시가 특히 중요하다.
CONN_CACHE_VARIABLE_KEY = "nifi_root_connections_cache"

try:
    _token = _get_nifi_token()
    _resp = requests.get(
        f"{_base_url}/nifi-api/flow/process-groups/root",
        headers={"Authorization": f"Bearer {_token}", "Host": _host_header},
        verify=False,
        timeout=10,
    )
    _resp.raise_for_status()
    _flow = _resp.json()["processGroupFlow"]
    _root_id = _flow["id"]
    _process_groups = [
        {"id": pg["component"]["id"], "name": pg["component"]["name"]}
        for pg in _flow["flow"]["processGroups"]
    ]
    _conn_resp = requests.get(
        f"{_base_url}/nifi-api/process-groups/{_root_id}/connections",
        headers={"Authorization": f"Bearer {_token}", "Host": _host_header},
        verify=False,
        timeout=10,
    )
    _conn_resp.raise_for_status()
    # 체인 계산에 필요한 필드만 남겨서 캐시한다(전체 응답은 크고 자주 바뀜).
    _connections = [
        {
            "component": {
                "source": {
                    "groupId": c["component"]["source"].get("groupId"),
                    "type": c["component"]["source"].get("type"),
                },
                "destination": {
                    "groupId": c["component"]["destination"].get("groupId"),
                    "type": c["component"]["destination"].get("type"),
                },
            }
        }
        for c in _conn_resp.json().get("connections", [])
    ]
    # 매 파싱(기본 30초)마다 DB에 쓰지 않도록 내용이 바뀐 경우에만 캐시 갱신
    _cached = Variable.get(PG_CACHE_VARIABLE_KEY, default_var=None, deserialize_json=True)
    if _cached != _process_groups:
        Variable.set(PG_CACHE_VARIABLE_KEY, _process_groups, serialize_json=True)
    _cached_conn = Variable.get(CONN_CACHE_VARIABLE_KEY, default_var=None, deserialize_json=True)
    if _cached_conn != _connections:
        Variable.set(CONN_CACHE_VARIABLE_KEY, _connections, serialize_json=True)
except Exception as exc:  # NiFi가 잠시 안 뜬 상태라도 DAG 파싱 전체가 죽지 않게
    _process_groups = Variable.get(PG_CACHE_VARIABLE_KEY, default_var=[], deserialize_json=True)
    _connections = Variable.get(CONN_CACHE_VARIABLE_KEY, default_var=[], deserialize_json=True)
    print(
        f"NiFi 조회 실패, 캐시된 프로세스 그룹 {len(_process_groups)}개로 DAG를 유지함"
        f" (스케줄 누락 방지, 그룹 추가/삭제는 NiFi 복구 후 반영): {exc}"
    )

_group_names = {pg["id"]: pg["name"] for pg in _process_groups}
# 하류 그룹은 상류 DAG가 통째로 제어하므로 자기 이름의 DAG를 따로 만들지 않는다.
# (DW용 DAG를 남겨두면 DZ가 이미 켠 그룹을 다시 켜려 하거나, 사람이 DW만 단독
#  실행해서 dz_* 테이블이 아직 안 찬 상태로 dw_*를 덮어쓰는 사고가 난다.)
_chains = {pg["id"]: build_group_chain(pg["id"], _connections) for pg in _process_groups}
_downstream_only = {m for head, ch in _chains.items() for m in ch[1:]}

for _pg in _process_groups:
    if _pg["id"] in _downstream_only:
        print(f"프로세스 그룹 {_pg['name']}({_pg['id'][:8]})은 상류 DAG가 함께 제어하므로 단독 DAG를 만들지 않음")
        continue
    globals()[f"nifi_pg_{_pg['id'][:8]}_control_dag"] = build_dag(
        _pg["id"], _pg["name"], _base_url, _host_header,
        chain=_chains[_pg["id"]], group_names=_group_names,
    )

# 예전엔 여기서 nifi_pipelines_metrics_collector DAG가 1시간마다 모든 파이프라인의
# 타겟 DB를 타임스탬프로 훑어서 대시보드 적재 건수를 반영했다. 이 DAG의 태스크가
# 호스트 메모리 부족(OOM)으로 계속 실패로 찍히는 문제가 있었고, 근본적으로도
# NiFi 프로세스 그룹의 상태를 Airflow가 대신 폴링하는 간접적인 방식이었다.
# 이제 pipeline-api의 NifiPipelineMetricScheduler가 NiFi 자신의 Counters API
# (PutDatabaseRecord의 "INSERT updates performed")를 60초마다 직접 조회해서 같은
# 롤업 테이블(pipeline_daily_load_metric)에 반영하므로 이 DAG는 제거했다.
