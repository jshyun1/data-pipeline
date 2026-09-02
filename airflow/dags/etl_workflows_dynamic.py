"""워크플로우 캔버스에서 게시한 spec으로 DAG를 만든다(설계서 2026-08-26 §6 Part C).

기존 `nifi_pipelines_dynamic.py`와 결정적으로 다른 점 두 가지:

1. **NiFi를 조회하지 않는다.** top-level에서 Airflow Variable(spec)만 읽는다.
   기존 팩토리는 파싱할 때마다 NiFi REST를 불러서, NiFi가 잠깐이라도 흔들리면
   DAG 파싱이 통째로 영향을 받았다(그래서 캐시 Variable을 따로 둬야 했다).

2. **잡 사이의 순서를 NiFi 출력포트로 추론하지 않는다.** 순서는 워크플로우 캔버스가
   유일한 원장이고, 그 결과가 spec의 edges로 내려온다. 그래서 NiFi 캔버스에는
   job(프로세스 그룹) 하나만 그리면 되고, 그룹끼리 연결할 필요가 없다.

DAG 1개 = 워크플로우 1개 = 스케줄 1개. 그 안의 job은 TaskGroup으로 내려간다:
    open_run -> start_pg -> await -> stop_pg -> verify
국면을 나눈 이유는 실패 지점을 화면에서 바로 알아보게 하기 위해서다.
"""
import json

import etl_job_runtime as runtime
import _pipeline_svc_auth  # noqa: F401  # import 만으로 pipeline-api 서비스 토큰 자동주입
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Asset, Param, TaskGroup

INDEX_VARIABLE = "etl_wf_index"
SPEC_PREFIX = "etl_wf_spec__"

# 캔버스의 조건 표기 -> Airflow trigger_rule.
# FAILURE 링크는 "앞이 실패했을 때만" 도는 보상/알림 경로다.
_CONDITION_RULE = {
    "SUCCESS": "all_success",
    "FAILURE": "one_failed",
    "ALWAYS": "all_done",
}


def _load_spec(workflow_key: str):
    raw = Variable.get(f"{SPEC_PREFIX}{workflow_key}", default_var=None)
    if not raw:
        return None
    try:
        return json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError as exc:
        print(f"[{workflow_key}] spec 파싱 실패 - 이 워크플로우는 건너뜁니다: {exc}")
        return None


def _trigger_rule(node: dict, spec: dict) -> str:
    """들어오는 엣지의 조건이 하류 노드의 trigger_rule이 된다.

    노드에 명시된 trigger_rule이 있으면 그것을 우선한다(캔버스 속성 패널에서
    JOIN 노드에 all_done 같은 것을 직접 고를 수 있으므로).
    """
    explicit = (node.get("trigger_rule") or "").lower()
    if explicit and explicit != "all_success":
        return explicit
    incoming = [e for e in spec.get("edges", []) if e.get("to") == node["key"]]
    conditions = {e.get("condition", "SUCCESS") for e in incoming}
    if conditions == {"FAILURE"}:
        return "one_failed"
    if "ALWAYS" in conditions:
        return "all_done"
    return "all_success"


def _build_job_group(node: dict, spec: dict) -> TaskGroup:
    """job 1개 = TaskGroup 1개."""
    rule = _trigger_rule(node, spec)
    with TaskGroup(group_id=node["key"]) as group:
        open_run = PythonOperator(
            task_id="open_run",
            python_callable=runtime.open_run,
            op_kwargs={"node": node},
            trigger_rule=rule,          # 그룹의 진입 조건은 첫 태스크가 갖는다
        )
        start_pg = PythonOperator(
            task_id="start_pg",
            python_callable=runtime.start_pg,
            op_kwargs={"node": node},
            retries=int(node.get("retries") or 0),
        )
        await_pg = PythonOperator(
            task_id="await",
            python_callable=runtime.await_completion,
            op_kwargs={"node": node},
        )
        stop_pg = PythonOperator(
            task_id="stop_pg",
            python_callable=runtime.stop_pg,
            op_kwargs={"node": node},
            trigger_rule="all_done",    # 실패해도 반드시 정지시킨다
        )
        verify = PythonOperator(
            task_id="verify",
            python_callable=runtime.verify_landing,
            op_kwargs={"node": node},
        )
        # 정지는 성패와 무관하게 도는 «곁가지»다. 본줄기에 두면(… >> stop_pg >> verify)
        # await가 실패해도 all_done인 stop_pg가 성공하면서 그 뒤가 정상으로 이어지고,
        # 결국 workflow_done까지 성공해 DAG 전체가 성공으로 끝난다(실측 2026-08-31).
        # 실패를 세탁하지 않도록 verify는 await에 직접 잇는다.
        open_run >> start_pg >> await_pg >> verify
        await_pg >> stop_pg
    return group


def _build_subworkflow(node: dict, spec: dict) -> TaskGroup:
    """워크플로우 노드. 자식 워크플로우 DAG를 띄우고 <b>끝날 때까지 기다린다</b>.

    이게 있어야 «daily = monthly 끝나면 years» 같은 조립이 성립한다. 기다리지 않으면
    자식이 아직 도는데 뒤 노드가 시작해버려서 그린 그림과 실제가 어긋난다.

    자식이 실패하면 이 노드도 실패한다(failed_states). 그래야 상위 워크플로우가
    «다 됐다»고 완료 신호를 내보내지 않는다.
    """
    sub_dag_id = node.get("sub_dag_id")
    with TaskGroup(group_id=node["key"]) as group:
        if not sub_dag_id:
            # 참조가 끊긴 노드(대상 워크플로우 삭제 등). 조용히 통과시키면 «돌았다»가
            # 되어버리므로 실패로 세운다.
            PythonOperator(
                task_id="missing",
                python_callable=lambda **_: (_ for _ in ()).throw(
                    RuntimeError(f"[{node['key']}] 가리키는 워크플로우가 없습니다")),
                trigger_rule=_trigger_rule(node, spec),
            )
            return group
        TriggerDagRunOperator(
            task_id="run",
            trigger_dag_id=sub_dag_id,
            # 부모 실행마다 자식 run_id가 달라야 한다(같으면 두 번째 실행이 충돌한다).
            #
            # ⚠️ 부모 run_id를 그대로 앞에 붙이면 안 된다. TriggerDagRunOperator가 만드는 건
            # «수동(manual)» 실행인데, Airflow 3은 scheduled__ / backfill__ 접두사를 각각
            # 스케줄·백필 전용으로 예약해 두고 그 밖의 실행이 쓰면 거부한다:
            #   ValueError: A manual DAG run cannot use ID 'scheduled__...' since it is
            #               reserved for scheduled runs
            # 그래서 부모를 «수동»으로 돌릴 때(manual__…)는 통과하고 «스케줄»로 돌 때만
            # 500으로 죽는, 재현이 헷갈리는 형태로 나타났다.
            # manual__ 접두사를 우리가 직접 붙여 예약어 충돌을 없앤다(부모 run_id는 뒤에
            # 그대로 남으므로 어느 부모 실행에서 나온 자식인지는 계속 추적된다).
            trigger_run_id="manual__{{ dag_run.run_id }}__" + node["key"],
            wait_for_completion=True,
            poke_interval=30,
            allowed_states=["success"],
            failed_states=["failed"],
            # 자식이 이미 같은 run_id로 있으면 그걸 기다린다(재실행 대비).
            reset_dag_run=True,
            deferrable=False,
            trigger_rule=_trigger_rule(node, spec),
        )
    return group


def _build_marker(node: dict, spec: dict) -> TaskGroup:
    """JOB/SUBWF가 아닌 노드(START/END/JOIN 등)는 자리만 잡는 빈 그룹으로 둔다.

    BRANCH의 실제 동작은 후속 단계에서 붙인다 - 지금 반쪽으로 만들어두면
    "그린 대로 안 도는" 그래프가 생겨서 오히려 위험하다.
    """
    with TaskGroup(group_id=node["key"]) as group:
        PythonOperator(
            task_id="marker",
            python_callable=lambda **_: print(f"[{node['key']}] {node['type']} 노드(통과)"),
            trigger_rule=_trigger_rule(node, spec),
        )
    return group


def _schedule_of(spec: dict):
    """시간 스케줄 또는 선행 워크플로우의 Asset.

    선행이 있으면 Asset로 건다 - "DZ 일배치가 끝나면 DW 일배치" 같은 연결이다.
    Airflow는 schedule=[a, b] 를 AND(둘 다 갱신돼야)로 해석하므로, ANY 는 명시적으로
    AssetAny 로 감싼다.
    """
    consumes = spec.get("consumes_assets") or []
    if not consumes:
        return spec.get("schedule")             # None이면 수동 트리거 전용
    assets = [Asset(uri) for uri in consumes]
    if len(assets) == 1:
        return assets
    if (spec.get("upstream_mode") or "ALL").upper() == "ANY":
        from airflow.sdk import AssetAny
        return AssetAny(*assets)
    return assets                               # 기본 AND


def build_workflow_dag(spec: dict) -> DAG:
    dag_id = spec["dag_id"]
    with DAG(
        dag_id=dag_id,
        # 화면에는 dag_id가 아니라 사용자가 지은 이름이 보여야 한다(실행 현황 트리·카탈로그).
        dag_display_name=(spec.get("name") or dag_id),
        description=(spec.get("description") or spec.get("name") or dag_id),
        schedule=_schedule_of(spec),
        catchup=bool(spec.get("catchup", False)),
        max_active_runs=int(spec.get("max_active_runs") or 1),
        start_date=None,
        tags=["etl", "workflow", spec.get("workflow_key", "")],
        params={"workflow_key": Param(spec.get("workflow_key", ""), type="string")},
        default_args={"owner": "cerebro-etl"},
    ) as dag:
        groups = {}
        for node in spec.get("nodes", []):
            if node.get("type") == "JOB" and node.get("nifi_pg_id"):
                groups[node["key"]] = _build_job_group(node, spec)
            elif node.get("type") == "SUBWF":
                groups[node["key"]] = _build_subworkflow(node, spec)
            else:
                groups[node["key"]] = _build_marker(node, spec)
        for edge in spec.get("edges", []):
            source, target = groups.get(edge.get("from")), groups.get(edge.get("to"))
            if source is not None and target is not None:
                source >> target

        # 워크플로우 "전체 완료"를 알리는 종단 태스크.
        #
        # Asset 이벤트는 태스크가 성공할 때 발행되므로, 중간 태스크에 outlet을 달면
        # 워크플로우가 아직 돌고 있는데 후행 워크플로우가 시작돼버린다. 모든 리프를
        # 이 태스크로 모아서 "정말 다 끝났을 때"만 신호가 나가게 한다.
        produces = spec.get("produces_asset")
        if produces:
            targets = {e.get("to") for e in spec.get("edges", [])}
            leaves = [g for key, g in groups.items() if key not in
                      {e.get("from") for e in spec.get("edges", [])}]
            done = PythonOperator(
                task_id="workflow_done",
                python_callable=lambda **_: print(f"[{spec.get('name')}] 워크플로우 완료"),
                outlets=[Asset(produces)],
                trigger_rule="all_success",   # 하나라도 실패하면 신호를 보내지 않는다
            )
            for leaf in (leaves or list(groups.values())):
                leaf >> done
            _ = targets
    return dag


# --- top-level: Variable만 읽는다(NiFi/백엔드 가용성과 무관) -------------------
_index_raw = Variable.get(INDEX_VARIABLE, default_var="[]")
try:
    _keys = json.loads(_index_raw) if isinstance(_index_raw, str) else (_index_raw or [])
except json.JSONDecodeError:
    print(f"{INDEX_VARIABLE} 파싱 실패 - 워크플로우 DAG를 만들지 않습니다: {_index_raw!r}")
    _keys = []

# 먼저 모든 spec을 읽어 «누가 누구의 자식인지»를 모은다.
#
# 상위 워크플로우가 품고 있는 워크플로우는 <b>자체 스케줄이 돌면 안 된다</b>.
# 예: AA(매일 1시)가 BB(매일 2시) -> CC(매일 3시)를 품고 있으면, BB·CC가 각자
# 2시·3시에 또 도는 순간 같은 적재가 하루 두 번 일어난다. 상위가 지시할 때만 돈다.
_specs = {}
for _key in _keys:
    _loaded = _load_spec(_key)
    if _loaded and _loaded.get("dag_id"):
        _specs[_key] = _loaded

_child_dag_ids = set()
for _loaded in _specs.values():
    for _node in _loaded.get("nodes", []):
        if _node.get("type") == "SUBWF" and _node.get("sub_dag_id"):
            _child_dag_ids.add(_node["sub_dag_id"])

for _key, _spec in _specs.items():
    if _spec["dag_id"] in _child_dag_ids and _spec.get("schedule"):
        print(f"[{_key}] 상위 워크플로우가 있어 자체 스케줄({_spec['schedule']})은 끕니다 "
              f"- 상위가 지시할 때만 돕니다")
        _spec = {**_spec, "schedule": None, "schedule_suppressed_by_parent": True}
    # 파싱은 워크플로우 단위로 격리한다 - 하나가 깨져도 나머지는 살아야 한다.
    try:
        globals()[_spec["dag_id"]] = build_workflow_dag(_spec)
    except Exception as exc:  # noqa: BLE001
        print(f"[{_key}] DAG 생성 실패 - 건너뜁니다: {exc}")
