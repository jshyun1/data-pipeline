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
"""
from datetime import datetime

import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

PIPELINE_API_BASE_URL = "http://pipeline-api:8081"
ACTIONS = ["start", "stop", "restart"]


def call_pipeline_action(pipeline_id: int, **context):
    action = context["params"]["action"]
    if action not in ACTIONS:
        raise ValueError(f"알 수 없는 action: {action}")
    response = requests.post(
        f"{PIPELINE_API_BASE_URL}/api/pipelines/{pipeline_id}/{action}", timeout=30
    )
    response.raise_for_status()
    print(f"파이프라인 {pipeline_id} {action} 완료: {response.json()}")


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
        PythonOperator(
            task_id="apply_action",
            python_callable=call_pipeline_action,
            op_kwargs={"pipeline_id": pipeline_id},
        )
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
