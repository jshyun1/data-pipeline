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

verify_action 다음의 verify_target_db_landing은 실제로 타겟에 데이터가 적재됐는지
확인한다. NiFi 쪽(nifi_pipelines_dynamic.py)은 타겟 테이블의 타임스탬프 컬럼으로
세지만, Kafka CDC는 그 방식이 안 통한다: Debezium이 그대로 복제하는 타겟 테이블에는
감사용 타임스탬프 컬럼이 없을 수 있고, 타겟 커넥션은 Oracle/Postgres가 섞여 있으며
비밀번호는 pipeline-api에서만 복호화 가능해서 Airflow가 직접 타겟 DB에 붙어 셀 수
없다. 대신 **싱크 커넥터의 컨슈머 그룹 committed offset**을 쓴다 - 그 값이 늘어난
만큼 실제로 소비(=타겟에 적재)된 레코드다. pipeline-api의
POST /api/pipelines/{id}/metrics/snapshot을 두 번 호출(베이스라인 → 재시도 대기 후
재조회)해서 그 차이를 "이번 실행에서 새로 적재된 건수"로 취급한다.
재시도 끝까지 델타가 0이어도 그 자체는 실패가 아니다(소스 쪽에 변경분이 없었을
뿐일 수 있음 - 특히 증분 CDC) - 커넥터 상태/trace에 실제 문제가 있을 때만 진짜
실패로 처리하고, 없으면 skipped로 끝낸다(NiFi 쪽과 동일한 3단계 판정).
검증에 성공하면 Asset outlet 이벤트의 extra에 건수를 남겨서 대시보드가
GET /api/v2/assets/events로 조회할 수 있게 한다.
"""
import time
from datetime import datetime

import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset, Param
from airflow.sdk.exceptions import AirflowSkipException

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


def _record_metric_snapshot(pipeline_id: int) -> int:
    resp = requests.post(
        f"{PIPELINE_API_BASE_URL}/api/pipelines/{pipeline_id}/metrics/snapshot", timeout=30
    )
    resp.raise_for_status()
    return resp.json()["data"]["committedOffset"]


def verify_target_db_landing(
    pipeline_id: int,
    target_schema: str | None,
    target_table: str | None,
    **context,
):
    action = context["params"]["action"]
    if action != "start":
        raise AirflowSkipException(f"action={action}이라 타겟 DB 적재 검증은 건너뜀 (start일 때만 검증)")

    baseline = _record_metric_snapshot(pipeline_id)
    count = 0
    for attempt in range(VERIFY_ATTEMPTS):
        time.sleep(VERIFY_INTERVAL_SECONDS)
        current = _record_metric_snapshot(pipeline_id)
        count = current - baseline
        if count > 0:
            print(f"타겟 DB 적재 확인: 파이프라인 {pipeline_id} 신규 {count}건(오프셋 {baseline} -> {current})")
            if target_schema and target_table:
                asset = Asset(f"kafka-cdc://pipeline-{pipeline_id}/{target_schema}/{target_table}")
                context["outlet_events"][asset].extra = {"count": count}
            return

    # 재시도 끝까지 델타=0. NiFi 쪽과 동일하게 커넥터 상태/trace로 실제 문제인지 확인
    # (verify_pipeline_action과 같은 방식 - 소스 쪽 변경분이 없었을 뿐이면 실패 아님).
    resp = requests.get(f"{PIPELINE_API_BASE_URL}/api/pipelines/{pipeline_id}", timeout=30)
    resp.raise_for_status()
    connector_names = [c["connectorName"] for c in resp.json()["data"]["connectors"]]

    problems = []
    for name in connector_names:
        status_resp = requests.get(
            f"{PIPELINE_API_BASE_URL}/api/connect/connectors/{name}/status", timeout=30
        )
        status_resp.raise_for_status()
        data = status_resp.json()["data"]
        if data["connector"]["state"] != "RUNNING":
            problems.append(f"{name}: connector={data['connector']['state']}")
            continue
        traces = [t.get("trace", "")[:200] for t in data["tasks"] if t.get("trace")]
        if traces:
            problems.append(f"{name}: trace={traces}")

    if problems:
        raise RuntimeError(f"파이프라인 {pipeline_id}에 적재된 데이터가 없고(delta=0), 커넥터 문제 확인됨: {problems}")

    raise AirflowSkipException(
        f"파이프라인 {pipeline_id}에 이번 실행 이후 적재된 데이터는 없지만(delta=0), 커넥터 자체는 "
        "정상입니다 - 신규/변경 소스 데이터가 없었던 것으로 보고 실패 처리하지 않음"
    )


def sanitize(name: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in name).strip("_").lower()


def build_dag(
    pipeline_id: int,
    pipeline_name: str,
    target_schema: str | None,
    target_table: str | None,
) -> DAG:
    with DAG(
        dag_id=f"kafka_pipeline_{pipeline_id}_control",
        # dag_id는 파이프라인 삭제 후 같은 id가 재사용될 일이 없어 안정적이지만, 사람이
        # 읽을 이름은 NiFi DAG와 동일하게 dag_display_name(화면 표시 전용)으로 분리한다 -
        # Airflow 화면에서 "kafka_pipeline_13_control" 대신 실제 파이프라인명이 보인다.
        dag_display_name=f"CDC_{sanitize(pipeline_name)}",
        description=f'Kafka 파이프라인 "{pipeline_name}"(id={pipeline_id}) 시작/중지/재시작 제어',
        schedule=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["kafka", "pipeline-control", f"pipeline-{pipeline_id}"],
        params={"action": Param("start", enum=ACTIONS, description="수행할 동작을 선택하세요")},
    ) as dag:
        target_outlets = (
            [Asset(f"kafka-cdc://pipeline-{pipeline_id}/{target_schema}/{target_table}")]
            if target_schema and target_table
            else []
        )
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
        verify_target = PythonOperator(
            task_id="verify_target_db_landing",
            python_callable=verify_target_db_landing,
            op_kwargs={
                "pipeline_id": pipeline_id,
                "target_schema": target_schema,
                "target_table": target_table,
            },
            outlets=target_outlets,
        )
        apply >> verify >> verify_target
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
        _pipeline["id"],
        _pipeline["name"],
        _pipeline.get("targetSchema"),
        _pipeline.get("targetTable"),
    )
