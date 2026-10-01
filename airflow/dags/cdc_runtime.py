"""워크플로우 CDC 노드가 쓰는 실행 헬퍼(2026-10-01, 제어 DAG 폐지).

예전에는 CDC 노드가 파이프라인마다 따로 있던 제어 DAG(`kafka_pipeline_{id}_control`)를 트리거만 하고
결과를 기다리지 않았다. 그래서 제어 DAG 가 502 로 실패해도 워크플로우는 성공이었다(2026-09-27 실측) -
«워크플로우는 성공인데 CDC 는 안 돈다». 제어 DAG 의 마지막 센서(monitor_cdc_runtime)는 CDC 가 도는 내내
Run 을 «실행 중»으로 붙잡아 두었지만, 실제 상태 판정은 백엔드(KafkaPipelineStateSynchronizer)가 하고
있었고 센서는 그 결과를 DAG 색으로 옮겨 적을 뿐이었다.

이제 노드가 직접 지시하고, 그 상태에 도달했는지 확인하고, 끝낸다.
    apply  : 이번 실행에서 이 노드가 받을 지시를 고르고(없으면 건너뜀), 이미 그 상태면 건너뛰고, 백엔드에 지시
    verify : Kafka Connect 의 «라이브» 상태가 기대값이 될 때까지 확인 - 안 되면 실패(= 워크플로우 실패)
끝난 뒤 «계속 잘 도는지»는 DAG 가 아니라 파이프라인 런타임 상태 + 알림(CONNECTOR_FAILED)이 맡는다.

노드별 지시: 워크플로우 실행 conf 의 `node_actions`({"노드키": "start", ...}). 비어 있으면 `action` 이
모든 CDC 노드에 간다(예전 «일괄 지시»와 같다). node_actions 에 없는 노드는 이번 실행에서 건너뛴다 -
«B 하나만 중지»가 워크플로우 실행 한 번으로 된다. 어떤 지시든 워크플로우 실행 이력에 남는다.
"""
import time

import requests

import _pipeline_svc_auth  # noqa: F401  # import 만으로 pipeline-api 서비스 토큰 자동주입
from airflow.sdk.exceptions import AirflowSkipException

PIPELINE_API_BASE_URL = "http://pipeline-api:8081"

# 노드가 받을 수 있는 지시. "none" 은 «이번엔 그대로 둔다»(화면의 노드별 선택 «그대로»).
ACTIONS = ["start", "stop", "restart", "deploy"]

VERIFY_ATTEMPTS = 12
VERIFY_INTERVAL_SECONDS = 5


def resolve_action(node_key: str, params: dict) -> str | None:
    """이번 실행에서 이 노드가 받을 지시. None 이면 대상이 아니다."""
    node_actions = params.get("node_actions") or {}
    if node_actions:
        action = node_actions.get(node_key) or node_actions.get("*")
    else:
        action = params.get("action")
    if not action or action == "none":
        return None
    if action not in ACTIONS:
        raise ValueError(f"[{node_key}] 알 수 없는 지시: {action} (가능: {', '.join(ACTIONS)})")
    return action


def skip_reason(action: str, status: str) -> str | None:
    """이미 기대 상태라 «할 일이 없는» 경우의 사유. 없으면 None(=지시를 보낸다).

    백엔드는 이미 그 상태인 요청을 400 으로 거부한다. 그대로 두면 «문제가 없는데 화면만 빨간» 실행이
    남는다 - 실패가 아니라 건너뜀으로 끝내야 «진짜 실패»가 눈에 띈다(옛 제어 DAG 와 같은 규칙).
    """
    if action == "start" and status == "DEPLOYED":
        return "이미 실행 중입니다(DEPLOYED)"
    if action == "stop" and status in ("STOPPED", "READY", "CREATED"):
        return f"이미 멈춰 있습니다({status})"
    # 재시작은 돌고 있거나 실패한 것만 대상이다(백엔드 restart 와 같은 규칙). 멈춰 둔 것을 켜면 «끈 의도»를 뒤집는다.
    if action == "restart" and status not in ("DEPLOYED", "FAILED"):
        return f"재시작 대상이 아닙니다({status}) - 실행 중이거나 실패한 것만 다시 띄웁니다"
    return None


def _pipeline(pipeline_id: int) -> dict:
    response = requests.get(f"{PIPELINE_API_BASE_URL}/api/pipelines/{pipeline_id}", timeout=30)
    response.raise_for_status()
    return response.json()["data"]


def _error_text(response: requests.Response) -> str:
    """백엔드 오류 응답({"error": {"code", "message"}})에서 사람이 읽을 사유를 꺼낸다."""
    try:
        body = response.json()
    except ValueError:
        return response.text[:500]
    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict):
        text = error.get("message") or error.get("code")
    else:
        text = error or (body.get("message") if isinstance(body, dict) else None)
    return str(text or response.text)[:500]


def apply(node: dict, **context) -> str:
    """지시를 보낸다. 대상이 아니거나 이미 그 상태면 건너뛴다. 보낸 지시를 돌려준다(XCom → verify)."""
    pipeline_id = node.get("pipeline_id")
    name = node.get("name") or node["key"]
    if not pipeline_id:
        raise RuntimeError(f"[{name}] 가리키는 CDC 파이프라인이 없습니다 - 워크플로우에서 노드를 다시 지정하십시오")

    action = resolve_action(node["key"], context["params"])
    if action is None:
        raise AirflowSkipException(f"[{name}] 이번 실행의 지시 대상이 아닙니다")

    status = _pipeline(pipeline_id).get("status")
    reason = skip_reason(action, status)
    if reason:
        raise AirflowSkipException(f"[{name}] {action} 건너뜀 - {reason}")

    print(f"[{name}] 파이프라인 {pipeline_id} 에 {action} 지시 (현재 {status})")
    response = requests.post(
        f"{PIPELINE_API_BASE_URL}/api/pipelines/{pipeline_id}/{action}",
        # 백엔드는 Oracle Debezium Source task 가 RUNNING 이 될 때까지 최대 60초 검증한다.
        timeout=(10, 120),
    )
    if response.status_code >= 400:
        # 백엔드의 거절 사유(예: «Kafka Connect가 실패 상태입니다»)가 로그와 실패 창에 그대로 보여야 한다.
        raise RuntimeError(f"[{name}] {action} 실패(HTTP {response.status_code}): {_error_text(response)}")
    print(f"[{name}] {action} 요청 완료 - 상태 확인으로 넘어갑니다")
    return action


def _expected_state(action: str, role: str) -> str:
    # deploy 는 «만들되 실행하지 않는» 단계라 전부 STOPPED 가 정상이다.
    # stop 은 CDC 에서 Source 를 계속 돌려둔다(원천 변경분을 놓치지 않기 위해) - Sink 만 멈춘다.
    if action == "deploy":
        return "STOPPED"
    if action in ("start", "restart") or role == "SOURCE":
        return "RUNNING"
    return "STOPPED"


def verify(node: dict, **context) -> None:
    """Kafka Connect 의 라이브 상태가 기대값인지 확인한다. 끝까지 아니면 실패 = 워크플로우 실패."""
    pipeline_id = node.get("pipeline_id")
    name = node.get("name") or node["key"]
    action = context["task_instance"].xcom_pull(task_ids=f"{node['key']}.apply", key="return_value")
    if not action:
        raise AirflowSkipException(f"[{name}] 보낸 지시가 없어 확인을 건너뜁니다")

    connectors = _pipeline(pipeline_id).get("connectors") or []
    roles = {c["connectorRole"] for c in connectors}
    # 로그 파이프라인의 «소스»는 filebeat 라 Kafka Connect 커넥터가 아니다 - SINK 만 있는 것이 정상이다.
    if "SINK" not in roles:
        raise RuntimeError(f"[{name}] Sink Connector 가 없습니다: {sorted(roles)}")

    problems: list = []
    for attempt in range(1, VERIFY_ATTEMPTS + 1):
        problems = []
        for connector in connectors:
            connector_name, role = connector["connectorName"], connector["connectorRole"]
            expected = _expected_state(action, role)
            response = requests.get(
                f"{PIPELINE_API_BASE_URL}/api/connect/connectors/{connector_name}/status", timeout=30)
            response.raise_for_status()
            data = response.json()["data"]
            state = data["connector"]["state"]
            tasks = data.get("tasks") or []
            task_states = [t["state"] for t in tasks]
            if state != expected:
                problems.append(f"{role} {connector_name}: connector={state} (기대 {expected})")
            elif expected == "RUNNING" and (not task_states or any(s != "RUNNING" for s in task_states)):
                traces = [t.get("trace", "")[:300] for t in tasks if t.get("trace")]
                problems.append(f"{role} {connector_name}: tasks={task_states} {traces}")
        if not problems:
            summary = {"start": "Source·Sink RUNNING", "restart": "Source·Sink RUNNING",
                       "stop": "Source RUNNING · Sink STOPPED", "deploy": "전부 STOPPED"}[action]
            print(f"[{name}] 확인 완료({attempt}회차): {summary}")
            return
        print(f"[{name}] 확인 {attempt}/{VERIFY_ATTEMPTS} - 아직 기대 상태가 아님: {problems}")
        time.sleep(VERIFY_INTERVAL_SECONDS)

    raise RuntimeError(f"[{name}] {action} 후 {VERIFY_ATTEMPTS * VERIFY_INTERVAL_SECONDS}초 안에 "
                       f"기대 상태가 되지 않았습니다: {problems}")
