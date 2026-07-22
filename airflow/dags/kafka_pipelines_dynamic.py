"""
Kafka 파이프라인 웹 서비스(pipeline-api)에 등록된 모든 파이프라인을 조회해서,
파이프라인마다 start/stop/restart를 수행하는 DAG를 동적으로 생성한다.

새 파이프라인이 웹 화면/API로 생성되면 dag-processor가 dags 폴더를 다시 스캔하는
주기(bundle refresh, 기본 5분 - 파일 재파싱 주기인 refresh_interval 300s와는 별개
설정인 bundle_refresh_check_interval로 스캔 자체 여부를 5초마다 체크)에 맞춰
자동으로 DAG 목록에 나타난다 - 파이프라인마다 DAG 코드를 직접 만들 필요 없음.
(실측: 새 파일 추가 후 최대 5분 가까이 걸릴 수 있음 - 즉시 반영 아님, 유의할 것)
DAG는 실제 데이터 흐름을 만들지 않고, 이미 배포된 파이프라인의 Kafka Connect
커넥터를 켜고 끄는 리모컨 역할만 한다(파이프라인 생성/배포/삭제는 여전히 웹에서).

트리거할 때 Params의 action(start/stop/restart)을 선택해서 실행한다.
apply_action 뒤의 verify_action이 Kafka Connect의 "라이브" 상태(메타데이터 DB의
저장값이 아님 - 그건 낡아있을 수 있음)를 조회해서 액션이 실제로 반영됐는지
확인한다: start/restart면 커넥터/태스크 전부 RUNNING, stop이면 PAUSED.
"""
import time
from datetime import datetime

import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

PIPELINE_API_BASE_URL = "http://pipeline-api:8081"
ACTIONS = ["start", "stop", "restart"]
VERIFY_ATTEMPTS = 6
VERIFY_INTERVAL_SECONDS = 5


def call_pipeline_action(pipeline_id: int, **context):
    action = context["params"]["action"]
    if action not in ACTIONS:
        raise ValueError(f"알 수 없는 action: {action}")
    response = requests.post(
        f"{PIPELINE_API_BASE_URL}/api/pipelines/{pipeline_id}/{action}", timeout=30
    )
    response.raise_for_status()
    print(f"파이프라인 {pipeline_id} {action} 완료: {response.json()}")


def verify_pipeline_action(pipeline_id: int, **context):
    action = context["params"]["action"]
    # Kafka Connect의 "stop"은 내부적으로 pause라 기대 상태는 PAUSED (백엔드 주석과 동일)
    expected = "PAUSED" if action == "stop" else "RUNNING"

    resp = requests.get(f"{PIPELINE_API_BASE_URL}/api/pipelines/{pipeline_id}", timeout=30)
    resp.raise_for_status()
    connector_names = [c["connectorName"] for c in resp.json()["data"]["connectors"]]
    if not connector_names:
        raise RuntimeError(f"파이프라인 {pipeline_id}에 배포된 커넥터가 없어 검증할 수 없습니다")

    problems = []
    for attempt in range(VERIFY_ATTEMPTS):
        problems = []
        for name in connector_names:
            status_resp = requests.get(
                f"{PIPELINE_API_BASE_URL}/api/connect/connectors/{name}/status", timeout=30
            )
            status_resp.raise_for_status()
            data = status_resp.json()["data"]
            connector_state = data["connector"]["state"]
            task_states = [t["state"] for t in data["tasks"]]
            if connector_state != expected:
                problems.append(f"{name}: connector={connector_state} (기대: {expected})")
            elif expected == "RUNNING" and any(s != "RUNNING" for s in task_states):
                traces = [t.get("trace", "")[:200] for t in data["tasks"] if t.get("trace")]
                problems.append(f"{name}: tasks={task_states} trace={traces}")
        if not problems:
            print(f"검증 통과: 커넥터 {len(connector_names)}개 전부 {expected}")
            return
        time.sleep(VERIFY_INTERVAL_SECONDS)

    raise RuntimeError(f"{action} 후에도 커넥터가 기대 상태({expected})가 아님: {problems}")


def build_dag(pipeline_id: int, pipeline_name: str) -> DAG:
    with DAG(
        dag_id=f"kafka_pipeline_{pipeline_id}_control",
        description=f'Kafka 파이프라인 "{pipeline_name}"(id={pipeline_id}) 시작/중지/재시작 제어',
        schedule=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["kafka", "pipeline-control", f"pipeline-{pipeline_id}"],
        params={"action": Param("start", enum=ACTIONS, description="수행할 동작을 선택하세요")},
    ) as dag:
        apply = PythonOperator(
            task_id="apply_action",
            python_callable=call_pipeline_action,
            op_kwargs={"pipeline_id": pipeline_id},
        )
        verify = PythonOperator(
            task_id="verify_action",
            python_callable=verify_pipeline_action,
            op_kwargs={"pipeline_id": pipeline_id},
        )
        apply >> verify
    return dag


try:
    _resp = requests.get(f"{PIPELINE_API_BASE_URL}/api/pipelines", timeout=10)
    _resp.raise_for_status()
    _pipelines = _resp.json().get("data", [])
except Exception as exc:  # pipeline-api가 잠시 안 뜬 상태라도 DAG 파싱 전체가 죽지 않게
    print(f"pipeline-api 조회 실패, 이번 파싱 주기엔 Kafka 파이프라인 DAG를 생성하지 않음: {exc}")
    _pipelines = []

for _pipeline in _pipelines:
    globals()[f"kafka_pipeline_{_pipeline['id']}_control_dag"] = build_dag(
        _pipeline["id"], _pipeline["name"]
    )
