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
"""
import os
from datetime import datetime

import requests
import urllib3
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # 자체 서명 인증서

NIFI_BASE_URL_DEFAULT = "https://nifi:8443"
NIFI_HOST_HEADER_DEFAULT = "localhost:8443"
ACTIONS = ["start", "stop"]


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


def sanitize(name: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in name).strip("_").lower()


def build_dag(pg_id: str, pg_name: str, base_url: str, host_header: str, username: str, password: str) -> DAG:
    dag_id = f"nifi_pipeline_{sanitize(pg_name)}_{pg_id[:8]}_control"
    with DAG(
        dag_id=dag_id,
        description=f'NiFi 프로세스 그룹 "{pg_name}"({pg_id}) 시작/중지 제어',
        schedule=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["nifi", "pipeline-control"],
        params={"action": Param("start", enum=ACTIONS, description="수행할 동작을 선택하세요")},
    ) as dag:
        PythonOperator(
            task_id="apply_action",
            python_callable=apply_process_group_action,
            op_kwargs={
                "pg_id": pg_id,
                "base_url": base_url,
                "host_header": host_header,
                "username": username,
                "password": password,
            },
        )
    return dag


_base_url = os.environ.get("NIFI_BASE_URL", NIFI_BASE_URL_DEFAULT)
_host_header = os.environ.get("NIFI_HOST_HEADER", NIFI_HOST_HEADER_DEFAULT)
_username = os.environ.get("NIFI_USERNAME", "")
_password = os.environ.get("NIFI_PASSWORD", "")

try:
    _token = _get_nifi_token(_base_url, _host_header, _username, _password)
    _resp = requests.get(
        f"{_base_url}/nifi-api/flow/process-groups/root",
        headers={"Authorization": f"Bearer {_token}", "Host": _host_header},
        verify=False,
        timeout=10,
    )
    _resp.raise_for_status()
    _process_groups = _resp.json()["processGroupFlow"]["flow"]["processGroups"]
except Exception as exc:  # NiFi가 잠시 안 뜬 상태라도 DAG 파싱 전체가 죽지 않게
    print(f"NiFi 조회 실패, 이번 파싱 주기엔 NiFi 파이프라인 DAG를 생성하지 않음: {exc}")
    _process_groups = []

for _pg in _process_groups:
    _component = _pg["component"]
    globals()[f"nifi_pg_{_component['id'][:8]}_control_dag"] = build_dag(
        _component["id"], _component["name"], _base_url, _host_header, _username, _password
    )
