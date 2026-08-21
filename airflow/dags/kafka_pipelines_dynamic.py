"""
Kafka 파이프라인 웹 서비스(pipeline-api)에 등록된 모든 파이프라인을 조회해서,
파이프라인마다 start/stop을 수행하는 DAG를 동적으로 생성한다.

새 파이프라인이 웹 화면/API로 생성되면 dag-processor가 dags 폴더를 다시 스캔하는
주기(bundle refresh, 기본 5분 - 파일 재파싱 주기인 refresh_interval 300s와는 별개
설정인 bundle_refresh_check_interval로 스캔 자체 여부를 5초마다 체크)에 맞춰
자동으로 DAG 목록에 나타난다 - 파이프라인마다 DAG 코드를 직접 만들 필요 없음.
(실측: 새 파일 추가 후 최대 5분 가까이 걸릴 수 있음 - 즉시 반영 아님, 유의할 것)
DAG는 실제 데이터 흐름을 만들지 않고, 생성 시 STOPPED로 준비된 Kafka Connect
커넥터를 안전하게 시작하거나 Sink만 중지하는 제어 역할을 한다.

트리거할 때 Params의 action(start/stop)을 선택해서 실행한다.
apply_action 뒤의 verify_action이 Kafka Connect의 "라이브" 상태(메타데이터 DB의
저장값이 아님 - 그건 낡아있을 수 있음)를 조회해서 액션이 실제로 반영됐는지
확인한다: start면 Source/Sink와 태스크가 모두 RUNNING, stop이면 Source/태스크는
RUNNING을 유지하고 Sink만 STOPPED여야 한다.

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

start 실행은 monitor_cdc_runtime reschedule Sensor를 마지막 leaf task로 유지한다.
Sensor는 worker를 점유한 채 sleep하지 않고 30초마다 잠깐 실행되어 pipeline-api가
동기화한 런타임 상태를 확인한다. DEPLOYED 동안 DAG Run은 RUNNING으로 남고, Airflow의
별도 stop 실행이 STOPPED를 만들면 성공으로 끝난다. 백엔드 런타임 모니터가 Kafka
Connect의 연속 장애를 FAILED로 확정하면 Sensor도 실패해 DAG Run을 FAILED로 만든다.
"""
import time
from datetime import datetime

import requests

import _pipeline_svc_auth  # noqa: F401  # import 만으로 pipeline-api 서비스 토큰 자동주입(P4 §7.6)
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import Asset, Param
from airflow.sdk.exceptions import AirflowException, AirflowSkipException

PIPELINE_API_BASE_URL = "http://pipeline-api:8081"
# deploy(배포)까지 여기에 두는 이유: 화면에서 버튼으로 배포하면 "아무도 시작을
# 지시하지 않았는데 적재가 시작되는" 경로가 생긴다. 파이프라인의 생성/삭제만 화면이
# 맡고 실행 계통(배포·시작·중지)은 전부 Airflow가 지시한다 - NiFi 쪽에
# autoResumeState=false로 강제해둔 것과 같은 규칙이다.
ACTIONS = ["deploy", "start", "stop"]
VERIFY_ATTEMPTS = 6
VERIFY_INTERVAL_SECONDS = 5
RUNTIME_MONITOR_INTERVAL_SECONDS = 30


def call_pipeline_action(pipeline_id: int, **context):
    action = context["params"]["action"]
    if action not in ACTIONS:
        raise ValueError(f"알 수 없는 action: {action}")
    response = requests.post(
        f"{PIPELINE_API_BASE_URL}/api/pipelines/{pipeline_id}/{action}",
        # 백엔드는 Oracle Debezium Source task가 RUNNING이 될 때까지 최대 60초 검증한다.
        # 연결 제한과 응답 제한을 분리해 정상 초기화를 Airflow가 먼저 끊지 않게 한다.
        timeout=(10, 90),
    )
    response.raise_for_status()
    print(f"파이프라인 {pipeline_id} {action} 완료: {response.json()}")


def verify_pipeline_action(pipeline_id: int, **context):
    action = context["params"]["action"]

    resp = requests.get(f"{PIPELINE_API_BASE_URL}/api/pipelines/{pipeline_id}", timeout=30)
    resp.raise_for_status()
    connectors = resp.json()["data"]["connectors"]
    roles = {c["connectorRole"] for c in connectors}
    # 로그 파이프라인의 "소스"는 filebeat라서 Kafka Connect 커넥터가 아니다 - SINK만
    # 있는 것이 정상 구성이다. 반대로 CDC는 Source/Sink가 둘 다 있어야 한다.
    if "SINK" not in roles:
        raise RuntimeError(
            f"파이프라인 {pipeline_id}에 Sink Connector가 없습니다: {sorted(roles)}"
        )

    problems = []
    for attempt in range(VERIFY_ATTEMPTS):
        problems = []
        for connector in connectors:
            name = connector["connectorName"]
            role = connector["connectorRole"]
            # deploy는 "만들되 실행하지 않는" 단계라 전부 STOPPED가 정상이다.
            # stop은 CDC에서 Source를 계속 돌려둔다(원천 변경분을 놓치지 않기 위해).
            if action == "deploy":
                expected = "STOPPED"
            elif action == "start" or role == "SOURCE":
                expected = "RUNNING"
            else:
                expected = "STOPPED"
            status_resp = requests.get(
                f"{PIPELINE_API_BASE_URL}/api/connect/connectors/{name}/status", timeout=30
            )
            status_resp.raise_for_status()
            data = status_resp.json()["data"]
            connector_state = data["connector"]["state"]
            task_states = [t["state"] for t in (data.get("tasks") or [])]
            if connector_state != expected:
                problems.append(f"{role} {name}: connector={connector_state} (기대: {expected})")
            elif expected == "RUNNING" and (
                not task_states or any(s != "RUNNING" for s in task_states)
            ):
                traces = [t.get("trace", "")[:200] for t in (data.get("tasks") or []) if t.get("trace")]
                problems.append(f"{name}: tasks={task_states} trace={traces}")
        if not problems:
            expected_summary = (
                "Source/Sink RUNNING"
                if action == "start"
                else "Source RUNNING + Sink STOPPED"
            )
            print(f"검증 통과: {expected_summary}")
            return
        time.sleep(VERIFY_INTERVAL_SECONDS)

    raise RuntimeError(f"{action} 후 Kafka Connect 상태가 기대값과 다름: {problems}")


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


def monitor_cdc_runtime(pipeline_id: int, **context) -> bool:
    """
    start DagRun을 CDC 수명과 맞춘다.

    실제 Kafka Connect 상태의 연속 실패 판정과 metadata-db 갱신은 백엔드의
    KafkaPipelineStateSynchronizer가 담당한다. Sensor가 직접 한 번의 REST 실패나
    Connect 재조정을 장애로 확정하지 않으므로, Airflow 감시 자체가 Kafka 파이프라인을
    임의로 중지시키지 않는다.
    """
    action = context["params"]["action"]
    if action == "stop":
        return True
    if action == "deploy":
        # 배포는 커넥터를 STOPPED로 만들어두는 단계라 감시할 런타임이 없다.
        # 실제 실행은 이후 start DagRun이 지시하고, 그 Run이 감시를 맡는다.
        return True

    try:
        response = requests.get(
            f"{PIPELINE_API_BASE_URL}/api/pipelines/{pipeline_id}", timeout=30
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        # pipeline-api의 일시적인 재시작/네트워크 오류 때문에 이미 동작 중인 CDC를
        # 실패로 오판하지 않는다. 다음 reschedule 주기에 다시 확인한다.
        print(f"파이프라인 {pipeline_id} 런타임 상태 조회 일시 실패: {exc}")
        return False

    status = response.json()["data"]["status"]
    if status == "DEPLOYED":
        return False
    if status == "STOPPED":
        print(f"파이프라인 {pipeline_id} Sink 정상 중지 확인 - 런타임 감시 종료")
        return True
    if status == "FAILED":
        raise AirflowException(
            f"파이프라인 {pipeline_id}의 Source/Sink 런타임 장애가 확정되어 FAILED로 동기화됨"
        )
    raise AirflowException(
        f"파이프라인 {pipeline_id}가 실행 중 예상 상태를 벗어남: status={status}"
    )


def sanitize(name: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in name).strip("_").lower()


def build_dag(
    pipeline_id: int,
    pipeline_name: str,
    pipeline_type: str,
    target_schema: str | None,
    target_table: str | None,
) -> DAG:
    with DAG(
        dag_id=f"kafka_pipeline_{pipeline_id}_control",
        # dag_id는 파이프라인 삭제 후 같은 id가 재사용될 일이 없어 안정적이지만, 사람이
        # 읽을 이름은 NiFi DAG와 동일하게 dag_display_name(화면 표시 전용)으로 분리한다 -
        # Airflow 화면에서 "kafka_pipeline_13_control" 대신 실제 파이프라인명이 보인다.
        dag_display_name=f"{'LOG' if pipeline_type == 'LOG_FILE' else 'CDC'}_{sanitize(pipeline_name)}",
        description=(
            f'Kafka {"로그" if pipeline_type == "LOG_FILE" else "CDC"} 파이프라인 '
            f'"{pipeline_name}"(id={pipeline_id}) 배포/시작/중지 제어'
        ),
        schedule=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        # start DagRun이 감시 중이어도 두 번째 stop DagRun이 실행될 수 있어야 한다.
        # 백엔드는 DEPLOYED 상태의 중복 start를 거부해 감시 Run이 둘 생기는 것을 막는다.
        max_active_runs=2,
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
        monitor_runtime = PythonSensor(
            task_id="monitor_cdc_runtime",
            python_callable=monitor_cdc_runtime,
            op_kwargs={"pipeline_id": pipeline_id},
            mode="reschedule",
            poke_interval=RUNTIME_MONITOR_INTERVAL_SECONDS,
            # CDC는 장기 실행이므로 사실상 무기한 감시한다. 명시적인 상한을 두어
            # 잘못 생성된 영구 고아 Task가 영원히 남는 것만 방지한다(10년).
            timeout=60 * 60 * 24 * 3650,
        )
        apply >> verify
        verify >> [verify_target, monitor_runtime]
    return dag


try:
    _resp = requests.get(f"{PIPELINE_API_BASE_URL}/api/pipelines", timeout=10)
    _resp.raise_for_status()
    _pipelines = _resp.json().get("data", [])
except Exception as exc:  # pipeline-api가 잠시 안 뜬 상태라도 DAG 파싱 전체가 죽지 않게
    print(f"pipeline-api 조회 실패, 이번 파싱 주기엔 Kafka 파이프라인 DAG를 생성하지 않음: {exc}")
    _pipelines = []

# LOG_FILE도 포함한다. 예전에는 TABLE_CDC만 DAG를 만들어서 로그 파이프라인은
# Airflow에서 제어할 수단이 아예 없었고, 그래서 화면에만 "배포" 버튼이 따로 남아
# 있었다 - 실행 계통을 Airflow로 일원화하려면 여기부터 열어야 한다.
for _pipeline in _pipelines:
    if _pipeline.get("pipelineType") not in ("TABLE_CDC", "LOG_FILE"):
        continue
    globals()[f"kafka_pipeline_{_pipeline['id']}_control_dag"] = build_dag(
        _pipeline["id"],
        _pipeline["name"],
        _pipeline.get("pipelineType"),
        _pipeline.get("targetSchema"),
        _pipeline.get("targetTable"),
    )
