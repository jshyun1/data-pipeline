"""워크플로우 DAG의 job TaskGroup이 쓰는 실행 헬퍼.

핵심은 완료 판정을 **콜백 우선 + 유휴 추측 fallback** 이중 구조로 둔 것이다.

- 지금(범위 A): NiFi가 아직 콜백을 보내지 않으므로 항상 fallback(유휴 판정)으로 끝난다.
  기존 `wait_for_group_completion`과 같은 기준이라 동작이 달라지지 않는다.
- 나중(플랜 B-1): NiFi 잡 종단에 InvokeHTTP 2개를 붙여 `/complete`·`/fail`을 호출하면
  1순위 경로가 켜진다. **이 파일도 백엔드도 고칠 필요가 없다.**

그래서 `completion_source`가 신뢰도 지표가 된다 - `CALLBACK`이면 확정, `OBSERVED`면 추정.
"""
import time

import requests

import etl_nifi_ops as nifi

API = nifi.PIPELINE_API_BASE_URL


def _post(path: str, payload: dict | None = None) -> dict:
    response = requests.post(f"{API}{path}", json=payload or {}, timeout=15)
    response.raise_for_status()
    body = response.json()
    return (body or {}).get("data") or {}


def _get(path: str) -> dict:
    response = requests.get(f"{API}{path}", timeout=15)
    response.raise_for_status()
    body = response.json()
    return (body or {}).get("data") or {}


def open_run(node: dict, **context) -> str:
    """실행 원장을 연다(open-or-adopt).

    관측기(NifiProcessorRunTracker)가 이미 run을 열어뒀을 수 있으므로 백엔드가
    새로 만들지 않고 그 run을 입양한다. 그래야 한 실행이 두 행으로 갈라지지 않는다.
    """
    dag_run = context["dag_run"]
    payload = {
        "jobId": node.get("job_id"),
        "nifiPgId": node.get("nifi_pg_id"),
        "workflowKey": context["params"].get("workflow_key"),
        "nodeKey": node.get("key"),
        "dagRunId": dag_run.run_id,
        "taskId": context["task_instance"].task_id,
    }
    try:
        run = _post("/api/etl/job-runs", payload)
        token = run.get("runToken")
        print(f"[{node.get('name')}] 실행 원장 open runToken={token}")
        return token
    except Exception as exc:
        # 원장이 없어도 NiFi 실행 자체는 가능해야 한다(관측기가 뒤늦게 열어줄 수도 있다).
        # 원장 없이 가면 완료 판정은 fallback만 쓰게 된다.
        print(f"[{node.get('name')}] 실행 원장 open 실패(무시하고 진행): {exc}")
        return ""


def start_pg(node: dict, **_) -> dict:
    """그룹을 켠다. 켜기 <b>직전</b>의 적재 카운터를 기준선으로 남긴다(XCom).

    기준선을 await 시작 시점에 뜨면, await가 늦게 붙었을 때 그 사이에 끝난 작업이
    통째로 안 보인다. 실측(2026-08-31): max_active_tasks_per_dag=2 환경에서 job 4개짜리
    워크플로우의 3·4번 await가 start_pg보다 3분 늦게 시작했고, 그동안 NiFi가 99,594행을
    다 옮겨버려 await는 «활동 없음 / 적재 건수 없음»으로 판정했다. 데이터는 들어왔는데
    원장에는 0건으로 남았다.

    켜기 전에 떠 두면 스케줄이 얼마나 밀리든 «이번 실행이 옮긴 양»이 정확해진다.
    """
    token = nifi.get_token()
    baseline = nifi.insert_counters_by_processor(token)
    nifi.start_group(node["nifi_pg_id"], node.get("name", ""))
    nifi.verify_started(node["nifi_pg_id"], node.get("name", ""))
    return baseline


def stop_pg(node: dict, **_) -> None:
    """실패해도 반드시 정지시켜야 다음 주기에 다시 트리거된다(trigger_rule=all_done)."""
    try:
        nifi.stop_group(node["nifi_pg_id"], node.get("name", ""))
    except Exception as exc:
        print(f"[{node.get('name')}] 그룹 정지 실패(다음 실행에 영향 가능): {exc}")


def _callback_state(run_token: str) -> tuple[str, str]:
    """백엔드 원장 상태. 콜백(플랜 B-1)이나 관측 마감이 닫았으면 여기서 잡힌다.

    <b>completion_source가 있는 마감만 신뢰한다.</b> 원장에는 유휴 정리
    (EtlJobRunService.closeIdleRuns)가 닫는 경로도 있는데, 그쪽은 이 실행의 결과를 아는
    주체가 아니라 "오래 조용하니 닫는다"일 뿐이다. 실제로 NiFi 추출이 DB 접속 실패로
    한 건도 못 옮긴 실행이 유휴 정리에 SUCCESS로 닫혔고, 그걸 «콜백 확정»으로 받아
    태스크가 성공으로 끝난 사고가 있었다(2026-08-31).
    """
    if not run_token:
        return "", ""
    try:
        run = _get(f"/api/etl/job-runs/{run_token}")
        if not (run.get("completionSource") or ""):
            return "", ""
        return (run.get("status") or ""), (run.get("errorMessage") or "")
    except Exception as exc:
        print(f"원장 조회 실패(이번 주기 건너뜀): {exc}")
        return "", ""


def await_completion(node: dict, **context) -> None:
    """완료 대기. 1순위 콜백, 2순위 유휴 추측.

    유휴 판정 기준은 기존 로직 그대로다 - 시작 직후에도 queued/active가 0이라
    바로 완료로 오판하지 않도록, 활동을 한 번 본 뒤부터 연속 IDLE_SETTLE_CHECKS회
    유휴여야 완료로 본다. 활동을 못 본 채 유예시간이 지나면 "이번엔 처리할 게
    없었다"로 정상 종료한다.
    """
    run_token = context["task_instance"].xcom_pull(
        task_ids=f"{node['key']}.open_run", key="return_value") or ""
    try:
        _await_completion(node, run_token, **context)
    except Exception as exc:
        _report_failed(run_token, str(exc))
        raise


def _await_completion(node: dict, run_token: str, **context) -> None:
    pg_id = node["nifi_pg_id"]
    pg_name = node.get("name", pg_id)
    timeout = int(node.get("await_timeout_sec") or 7200)

    token = nifi.get_token()
    deadline = time.time() + timeout
    started_at = time.time()
    idle_streak = 0
    seen_activity = False
    # 기준선은 start_pg가 «그룹을 켜기 직전»에 떠 둔 것을 쓴다. 없으면(과거 실행분 재시도 등)
    # 지금 뜬다 - 그 경우 이 시점 이전의 적재는 세지 못한다.
    baseline = context["task_instance"].xcom_pull(
        task_ids=f"{node['key']}.start_pg", key="return_value")
    if not isinstance(baseline, dict):
        print(f"[{pg_name}] start_pg 기준선이 없어 지금 시점으로 잡습니다(이전 적재분은 집계 제외)")
        baseline = nifi.insert_counters_by_processor(token)
    seen_bulletins: set = set()
    errors: list = []
    processor_names: dict = {}
    queued = active = 0

    while time.time() < deadline:
        # 1순위: 콜백/관측기가 원장을 닫았는가
        status, error_message = _callback_state(run_token)
        if status == "FAILED":
            raise RuntimeError(f"[{pg_name}] job 실패(콜백): {error_message}")
        if status == "SUCCESS":
            print(f"[{pg_name}] 완료(콜백 확정)")
            nifi.print_load_summary(pg_name, token, baseline, processor_names)
            # 콜백이 성공이라 해도 이번 실행에서 ERROR bulletin을 봤으면 실패다.
            # 유휴 경로에만 이 검사가 있어서, 콜백 경로로 빠지면 오류가 묻혔다.
            nifi.fail_if_errors(pg_name, errors)
            return

        # 2순위: 유휴 추측(fallback)
        response = requests.get(
            f"{nifi.base_url()}/nifi-api/flow/process-groups/{pg_id}/status",
            params={"recursive": "true"},
            headers={"Authorization": f"Bearer {token}", "Host": nifi.host_header()},
            verify=False,
            timeout=15,
        )
        if response.status_code == 401:      # 장시간 대기 중 토큰 만료
            token = nifi.get_token()
            continue
        response.raise_for_status()
        snapshot = response.json()["processGroupStatus"]["aggregateSnapshot"]
        queued = nifi.as_int(snapshot.get("queuedCount"))
        active = nifi.as_int(snapshot.get("activeThreadCount"))

        if queued > 0 or active > 0:
            seen_activity = True
            idle_streak = 0
        else:
            idle_streak += 1
        print(f"[{pg_name}] queued={queued} activeThreads={active} "
              f"idle={idle_streak}/{nifi.IDLE_SETTLE_CHECKS} activity={seen_activity}")

        for processor in nifi._walk_processor_snapshots(snapshot):
            processor_names[processor["id"]] = processor["name"]
            if nifi.as_int(processor.get("activeThreadCount")) > 0:
                print(f"[{pg_name}]   > {processor['name']} ({processor['type']}) "
                      f"스레드={processor.get('activeThreadCount')}")
        errors.extend(nifi.collect_new_bulletins(pg_id, pg_name, token, seen_bulletins))

        done = (seen_activity and idle_streak >= nifi.IDLE_SETTLE_CHECKS) or (
            not seen_activity and time.time() - started_at > nifi.WAIT_START_GRACE_SECONDS)
        if done:
            reason = "유휴 관측" if seen_activity else "유예시간 내 활동 없음"
            print(f"[{pg_name}] 완료({reason}, 추정)")
            if not seen_activity:
                # 감시가 늦게 붙어 이미 끝나 있었을 수 있다. 기준선 대비 증가로 가려낸다.
                print(f"[{pg_name}] 감시 구간에는 활동이 없었습니다 - "
                      f"아래 적재 건수가 0이 아니면 감시 전에 이미 끝난 실행입니다")
            rows = nifi.print_load_summary(pg_name, token, baseline, processor_names)
            # ERROR bulletin이 있었으면 완료가 아니라 실패다(2026-07-29 사고 방지).
            nifi.fail_if_errors(pg_name, errors)
            _report_observed(run_token, rows)
            return
        time.sleep(nifi.WAIT_POLL_INTERVAL_SECONDS)

    raise RuntimeError(
        f"[{pg_name}] {timeout}초 안에 완료되지 않음 (queued={queued}, activeThreads={active}). "
        "백프레셔로 막혔거나 하류가 소비하지 못하고 있을 수 있음")


def _report_failed(run_token: str, message: str) -> None:
    """실패로 끝났음을 원장에 남긴다.

    없으면 실행이 원장에 열린 채 남고, 유휴 정리가 나중에 SUCCESS로 닫아버린다.
    Airflow는 실패인데 원장만 성공인 상태가 되어 이력을 믿을 수 없게 된다.
    """
    if not run_token:
        return
    try:
        _post(f"/api/etl/job-runs/{run_token}/observe-failed", {"error": message[:500]})
    except Exception as exc:
        print(f"원장 실패 보고 실패(무시): {exc}")


def _report_observed(run_token: str, rows: int) -> None:
    """추정으로 끝났음을 원장에 남긴다(completion_source=OBSERVED).

    화면이 '확정(콜백)'과 '추정(관측)'을 구분해 보여줄 수 있게 하는 근거다.
    """
    if not run_token:
        return
    try:
        _post(f"/api/etl/job-runs/{run_token}/observe-complete", {"rows": rows})
    except Exception as exc:
        print(f"원장 마감 보고 실패(무시): {exc}")


def verify_landing(node: dict, **context) -> None:
    """spec의 target_tables로 적재를 확인한다.

    대상 테이블을 Airflow Variable이 아니라 spec에서 읽는 것이 기존과의 차이다.
    예전에는 `{dag_id}__target_schema/table/timestamp_column`을 손으로 넣어야 했고,
    셋 중 하나만 빠져도 검증이 조용히 skip됐다(실측: 이 환경엔 그 Variable이
    하나도 없어서 검증이 매번 건너뛰어지고 있었다).
    """
    tables = node.get("target_tables") or []
    if not tables:
        print(f"[{node.get('name')}] 적재 대상 테이블이 없어 검증 생략(추출 전용 job일 수 있음)")
        return

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id="target_db_postgres")
    for table in tables:
        try:
            count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
            print(f"[{node.get('name')}] {table} 총 {count:,}행")
        except Exception as exc:
            # 검증 실패가 적재 자체를 실패로 만들면 안 된다(권한/스키마 문제일 수 있음).
            print(f"[{node.get('name')}] {table} 조회 실패(검증 생략): {exc}")
