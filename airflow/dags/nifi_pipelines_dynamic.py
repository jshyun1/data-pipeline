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
"""
import os
import time
from datetime import datetime

import requests
import urllib3
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # 자체 서명 인증서

NIFI_BASE_URL_DEFAULT = "https://nifi:8443"
NIFI_HOST_HEADER_DEFAULT = "localhost:8443"
ACTIONS = ["start", "stop"]
VERIFY_ATTEMPTS = 6
VERIFY_INTERVAL_SECONDS = 5


def _get_nifi_token(base_url: str, host_header: str, username: str, password: str) -> str:
    response = requests.post(
        f"{base_url}/nifi-api/access/token",
        data={"username": username, "password": password},
        headers={"Host": host_header},
        verify=False,
        timeout=10,
    )
    response.raise_for_status()
    return response.text


def apply_process_group_action(
    pg_id: str, base_url: str, host_header: str, username: str, password: str, **context
):
    action = context["params"]["action"]
    if action not in ACTIONS:
        raise ValueError(f"알 수 없는 action: {action}")
    state = "RUNNING" if action == "start" else "STOPPED"

    token = _get_nifi_token(base_url, host_header, username, password)
    response = requests.put(
        f"{base_url}/nifi-api/flow/process-groups/{pg_id}",
        json={"id": pg_id, "state": state},
        headers={"Authorization": f"Bearer {token}", "Host": host_header},
        verify=False,
        timeout=15,
    )
    response.raise_for_status()
    print(f"NiFi 프로세스 그룹 {pg_id} {action}({state}) 완료: {response.json()}")


def verify_process_group_action(
    pg_id: str, base_url: str, host_header: str, username: str, password: str, **context
):
    action = context["params"]["action"]
    token = _get_nifi_token(base_url, host_header, username, password)

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


def sanitize(name: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in name).strip("_").lower()


def build_dag(pg_id: str, pg_name: str, base_url: str, host_header: str, username: str, password: str) -> DAG:
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
            "username": username,
            "password": password,
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
        apply >> verify
    return dag


_base_url = os.environ.get("NIFI_BASE_URL", NIFI_BASE_URL_DEFAULT)
_host_header = os.environ.get("NIFI_HOST_HEADER", NIFI_HOST_HEADER_DEFAULT)
_username = os.environ.get("NIFI_USERNAME", "")
_password = os.environ.get("NIFI_PASSWORD", "")

# 마지막으로 성공한 프로세스 그룹 조회 결과를 담는 Variable 키.
# NiFi가 잠깐 죽어있는 동안 파싱이 돌면 예전엔 DAG가 통째로 사라져서
# (= 그 사이 스케줄 실행이 조용히 누락) 마지막 성공 조회를 캐시로 재사용한다.
PG_CACHE_VARIABLE_KEY = "nifi_process_groups_cache"

try:
    _token = _get_nifi_token(_base_url, _host_header, _username, _password)
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
        _pg["id"], _pg["name"], _base_url, _host_header, _username, _password
    )
