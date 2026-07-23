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

# docker-compose.yml의 AIRFLOW_CONN_TARGET_DB_POSTGRES 환경변수로 등록되는 Connection.
TARGET_DB_CONN_ID = "target_db_postgres"
TARGET_DB_NAME = os.environ.get("TARGET_DB_NAME", "tarantula")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PIPELINE_API_BASE_URL = "http://pipeline-api:8081"


def _get_nifi_token() -> str:
    # NiFi가 OIDC 전용 인증으로 전환되며 username/password 토큰 발급(/nifi-api/access/token)이
    # 막혀서, Keycloak에서 client_credentials로 직접 토큰을 받아 NiFi에 Bearer로 제시한다.
    # NiFi는 신뢰하는 realm이 서명한 토큰이면 발급 클라이언트를 가리지 않고 인증을 통과시키고,
    # 이후 sub 클레임을 identity로 authorizers.xml/users.xml에 등록된 권한을 확인한다
    # (pipeline-api의 NifiClient.getToken()과 동일한 방식).
    backchannel_url = os.environ.get("KEYCLOAK_BACKCHANNEL_URL", "https://host.docker.internal:8543")
    realm = os.environ.get("KEYCLOAK_REALM", "cerebro")
    client_id = os.environ.get("KEYCLOAK_PIPELINE_SERVICE_CLIENT_ID", "")
    client_secret = os.environ.get("KEYCLOAK_PIPELINE_SERVICE_CLIENT_SECRET", "")
    response = requests.post(
        f"{backchannel_url}/realms/{realm}/protocol/openid-connect/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=10,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def apply_process_group_action(pg_id: str, base_url: str, host_header: str, **context):
    action = context["params"]["action"]
    if action not in ACTIONS:
        raise ValueError(f"알 수 없는 action: {action}")
    state = "RUNNING" if action == "start" else "STOPPED"

    token = _get_nifi_token()
    response = requests.put(
        f"{base_url}/nifi-api/flow/process-groups/{pg_id}",
        json={"id": pg_id, "state": state},
        headers={"Authorization": f"Bearer {token}", "Host": host_header},
        verify=False,
        timeout=15,
    )
    response.raise_for_status()
    print(f"NiFi 프로세스 그룹 {pg_id} {action}({state}) 완료: {response.json()}")


def verify_process_group_action(pg_id: str, base_url: str, host_header: str, **context):
    action = context["params"]["action"]
    token = _get_nifi_token()

    counts = {}
    for attempt in range(VERIFY_ATTEMPTS):
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
            print(f"검증 통과: {action} 반영 확인 {counts}")
            return
        time.sleep(VERIFY_INTERVAL_SECONDS)

    raise RuntimeError(f"{action} 후에도 프로세스 그룹 상태가 기대와 다름: {counts}")


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


def build_dag(pg_id: str, pg_name: str, base_url: str, host_header: str) -> DAG:
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
    with DAG(
        dag_id=dag_id,
        dag_display_name=f"nifi_pipeline_{sanitize(pg_name)}_control",
        description=f'NiFi 프로세스 그룹 "{pg_name}"({pg_id}) 시작/중지 제어'
        + (f" (스케줄: {schedule})" if schedule else " (수동 트리거 전용)"),
        schedule=schedule,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["nifi", "pipeline-control"],
        params={"action": Param("start", enum=ACTIONS, description="수행할 동작을 선택하세요")},
    ) as dag:
        _op_kwargs = {
            "pg_id": pg_id,
            "base_url": base_url,
            "host_header": host_header,
        }
        apply = PythonOperator(
            task_id="apply_action",
            python_callable=apply_process_group_action,
            op_kwargs=_op_kwargs,
        )
        verify = PythonOperator(
            task_id="verify_action",
            python_callable=verify_process_group_action,
            op_kwargs=_op_kwargs,
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
        apply >> verify >> verify_target
    return dag


_base_url = os.environ.get("NIFI_BASE_URL", NIFI_BASE_URL_DEFAULT)
_host_header = os.environ.get("NIFI_HOST_HEADER", NIFI_HOST_HEADER_DEFAULT)

# 마지막으로 성공한 프로세스 그룹 조회 결과를 담는 Variable 키.
# NiFi가 잠깐 죽어있는 동안 파싱이 돌면 예전엔 DAG가 통째로 사라져서
# (= 그 사이 스케줄 실행이 조용히 누락) 마지막 성공 조회를 캐시로 재사용한다.
PG_CACHE_VARIABLE_KEY = "nifi_process_groups_cache"

try:
    _token = _get_nifi_token()
    _resp = requests.get(
        f"{_base_url}/nifi-api/flow/process-groups/root",
        headers={"Authorization": f"Bearer {_token}", "Host": _host_header},
        verify=False,
        timeout=10,
    )
    _resp.raise_for_status()
    _process_groups = [
        {"id": pg["component"]["id"], "name": pg["component"]["name"]}
        for pg in _resp.json()["processGroupFlow"]["flow"]["processGroups"]
    ]
    # 매 파싱(기본 30초)마다 DB에 쓰지 않도록 내용이 바뀐 경우에만 캐시 갱신
    _cached = Variable.get(PG_CACHE_VARIABLE_KEY, default_var=None, deserialize_json=True)
    if _cached != _process_groups:
        Variable.set(PG_CACHE_VARIABLE_KEY, _process_groups, serialize_json=True)
except Exception as exc:  # NiFi가 잠시 안 뜬 상태라도 DAG 파싱 전체가 죽지 않게
    _process_groups = Variable.get(PG_CACHE_VARIABLE_KEY, default_var=[], deserialize_json=True)
    print(
        f"NiFi 조회 실패, 캐시된 프로세스 그룹 {len(_process_groups)}개로 DAG를 유지함"
        f" (스케줄 누락 방지, 그룹 추가/삭제는 NiFi 복구 후 반영): {exc}"
    )

for _pg in _process_groups:
    globals()[f"nifi_pg_{_pg['id'][:8]}_control_dag"] = build_dag(
        _pg["id"], _pg["name"], _base_url, _host_header
    )


def check_all_pipelines_landing(**context):
    # 사용자가 Airflow에서 수동으로 start를 트리거하지 않아도(NiFi 프로세스 그룹은
    # 배포되면 알아서 계속 도는 구조라 보통 그렇다) 대시보드에 적재 건수가 반영되도록,
    # 이 DAG가 1분마다 모든 파이프라인을 훑어서 확인한다. dag_run.start_date 대신
    # data_interval_start/end(이번 1분 스케줄 구간)를 기준으로 세서 "직전 확인 이후
    # 새로 늘어난 만큼"만 잡는다 - verify_target_db_landing(수동 트리거 전용)과
    # 동일한 검증 로직을 재사용하되 재시도/skip-vs-fail 판정 없이 훨씬 가볍게 돈다.
    interval_start = context["data_interval_start"]
    interval_end = context["data_interval_end"]

    for pg in _process_groups:
        dag_id = f"nifi_pipeline_{pg['id'][:8]}_control"
        target_schema = Variable.get(f"{dag_id}__target_schema", default_var=None)
        target_table = Variable.get(f"{dag_id}__target_table", default_var=None)
        target_timestamp_column = Variable.get(f"{dag_id}__target_timestamp_column", default_var=None)
        if not (target_schema and target_table and target_timestamp_column):
            continue
        if not all(_IDENTIFIER_RE.match(v) for v in (target_schema, target_table, target_timestamp_column)):
            continue

        try:
            hook = PostgresHook(postgres_conn_id=TARGET_DB_CONN_ID)
            sql = (
                f"SELECT COUNT(*) FROM {target_schema}.{target_table} "
                f"WHERE {target_timestamp_column} >= %s AND {target_timestamp_column} < %s"
            )
            count = hook.get_first(sql, parameters=(interval_start, interval_end))[0]
            if count > 0:
                requests.post(
                    f"{PIPELINE_API_BASE_URL}/api/metrics/daily-load/increment",
                    json={
                        "pipelineSource": "NIFI",
                        "pipelineKey": pg["id"],
                        "taskKey": "verify_target_db_landing",
                        "pipelineLabel": pg["name"],
                        "count": count,
                    },
                    timeout=10,
                )
                print(f"파이프라인 {pg['id']}({pg['name']}) 적재 {count}건 반영")
        except Exception as exc:
            # 파이프라인 하나가 일시적으로 응답 안 해도(타겟 DB 재시작 등) 다음 1분
            # 스케줄에 다시 시도되므로, 여기서 죽지 않고 나머지 파이프라인을 계속 확인한다.
            print(f"파이프라인 {pg['id']} 적재 건수 체크 실패(다음 스케줄에 재시도): {exc}")


with DAG(
    dag_id="nifi_pipelines_metrics_collector",
    description="모든 NiFi 파이프라인의 실제 적재 건수를 1분마다 확인해서 대시보드 롤업 테이블에 반영",
    schedule="* * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["nifi", "metrics"],
) as nifi_pipelines_metrics_collector_dag:
    PythonOperator(
        task_id="check_all_pipelines_landing",
        python_callable=check_all_pipelines_landing,
    )
