"""워크플로우 DAG가 NiFi를 다루는 공용 연산.

기존 `nifi_pipelines_dynamic.py`에 있던 NiFi 제어·완료판정·실패감지 로직을 그대로
옮겨 온 것이다(로직 변경 없음). 옮긴 이유는 두 가지다.

1. 새 팩토리(`etl_workflows_dynamic.py`)와 기존 팩토리가 공존하는 기간 동안 같은
   코드를 두 벌 두지 않기 위해서.
2. 기존 팩토리에서 폐기하는 것은 **잡 사이의 순서를 NiFi 출력포트로 추론하던 부분**
   (`build_group_chain`)뿐이고, NiFi를 켜고 끄고 지켜보는 로직 자체는 계속 필요하다.
   순서는 이제 워크플로우 캔버스(메타DB)가 유일한 원장이다.

`_fail_if_errors`는 특히 그대로 가져와야 한다. "큐 0 + 스레드 0"만으로 완료를 보면
실패도 완료로 보이기 때문이다(2026-07-29 사고: 추출이 전부 실패했는데 truncate만
커밋돼 DZ 5개 테이블이 비었고 DAG는 초록불이었다).
"""
import os
import re
import time

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # 자체 서명 인증서

NIFI_BASE_URL_DEFAULT = "https://nifi:8443"
NIFI_HOST_HEADER_DEFAULT = "localhost:8443"
PIPELINE_API_BASE_URL = os.environ.get("PIPELINE_API_BASE_URL", "http://pipeline-api:8081")

VERIFY_ATTEMPTS = 6
VERIFY_INTERVAL_SECONDS = 5

# 완료 판정 튜닝값. 실측 근거: 테이블 하나 추출이 56초, 1,565만 건 적재가 약 20분.
WAIT_POLL_INTERVAL_SECONDS = 15
# 프로세서 사이를 넘어가는 짧은 순간에도 queued/active가 0으로 보일 수 있어서
# 한 번만 보고 끝내면 안 된다.
IDLE_SETTLE_CHECKS = 3
# 시작 후 이 시간까지 아무 활동이 없으면 "처리할 데이터가 없었다"로 본다.
WAIT_START_GRACE_SECONDS = 180

# PutDatabaseRecord 는 statement type 마다 <b>다른 이름</b>의 카운터를 올린다
# ("INSERT updates performed", "UPSERT updates performed", "UPDATE updates performed" …).
# 예전에는 INSERT 하나만 세서, UPSERT 로 적재하는 job 은 실제로 10만 행이 들어가도
# 실행 이력에 «적재 건수: 없음»(0행)으로 남았다. dz_*(INSERT)는 제대로 나오는데
# dw_*(UPSERT)만 전부 0 으로 보이던 원인이다(2026-09-03 확인. 그날 MSA NiFi 누계는
# INSERT 16,637,720 / UPSERT 2,116,460 이었다).
#
# 이름을 하나씩 나열하면 새 statement type 이 생길 때 또 0 이 되므로 접미사로 받는다.
# "Records Written"/"Records Processed"/"Batches Executed" 는 행수가 아니거나 중복이라
# 걸러진다.
LOAD_COUNTER_NAME_SUFFIX = "updates performed"
_PROCESSOR_ID_IN_CONTEXT = re.compile(r"\(([0-9a-fA-F-]{36})\)\s*$")


def base_url() -> str:
    return os.environ.get("NIFI_BASE_URL", NIFI_BASE_URL_DEFAULT)


def host_header() -> str:
    return os.environ.get("NIFI_HOST_HEADER", NIFI_HOST_HEADER_DEFAULT)


def as_int(value) -> int:
    """NiFi status 응답의 건수는 정수일 때도 있고 "1,234" 같은 문자열일 때도 있다."""
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    return int(str(value).replace(",", "").strip() or 0)


def get_token() -> str:
    """NiFi Single User 인증. 이 엔드포인트는 응답 본문이 JSON이 아니라 JWT 문자열이다."""
    response = requests.post(
        f"{base_url()}/nifi-api/access/token",
        data={
            "username": os.environ.get("NIFI_USERNAME", ""),
            "password": os.environ.get("NIFI_PASSWORD", ""),
        },
        headers={"Host": host_header()},
        verify=False,
        timeout=10,
    )
    response.raise_for_status()
    return response.text


def _headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Host": host_header()}


def set_group_state(pg_id: str, state: str, token: str) -> dict:
    response = requests.put(
        f"{base_url()}/nifi-api/flow/process-groups/{pg_id}",
        json={"id": pg_id, "state": state},
        headers=_headers(token),
        verify=False,
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def start_group(pg_id: str, pg_name: str = "") -> None:
    token = get_token()
    set_group_state(pg_id, "RUNNING", token)
    print(f"[{pg_name or pg_id}] NiFi 그룹 START")


def stop_group(pg_id: str, pg_name: str = "") -> None:
    """배치성 그룹은 끝난 뒤 STOPPED로 돌아가야 다음 주기에 다시 1회 트리거된다.

    GenerateFlowFile은 STOPPED -> RUNNING 전환 순간 주기와 무관하게 즉시 1회
    실행되므로, 계속 RUNNING이면 다음 실행의 start가 아무 효과가 없다.
    """
    token = get_token()
    set_group_state(pg_id, "STOPPED", token)
    print(f"[{pg_name or pg_id}] NiFi 그룹 STOP")


def group_entity(pg_id: str, token: str | None = None) -> dict:
    token = token or get_token()
    response = requests.get(
        f"{base_url()}/nifi-api/process-groups/{pg_id}",
        headers=_headers(token),
        verify=False,
        timeout=15,
    )
    response.raise_for_status()
    return response.json()


def verify_started(pg_id: str, pg_name: str = "") -> None:
    """start 호출이 실제로 반영됐는지. invalid가 있으면 그 프로세서는 시작되지 못한 것이다."""
    token = get_token()
    counts = {}
    for _ in range(VERIFY_ATTEMPTS):
        entity = group_entity(pg_id, token)
        counts = {
            key: entity.get(key, 0)
            for key in ("runningCount", "stoppedCount", "invalidCount", "disabledCount")
        }
        if counts["invalidCount"] == 0 and counts["stoppedCount"] == 0 and counts["runningCount"] > 0:
            print(f"[{pg_name or pg_id}] start 반영 확인 {counts}")
            return
        time.sleep(VERIFY_INTERVAL_SECONDS)
    raise RuntimeError(f"[{pg_name or pg_id}] start 후에도 상태가 기대와 다릅니다: {counts}")


def _walk_processor_snapshots(snapshot: dict):
    """status(recursive=true) 응답에서 하위 그룹까지 훑어 프로세서 스냅샷을 모은다."""
    for entry in snapshot.get("processorStatusSnapshots") or []:
        processor = entry.get("processorStatusSnapshot")
        if processor:
            yield processor
    for entry in snapshot.get("processGroupStatusSnapshots") or []:
        child = entry.get("processGroupStatusSnapshot")
        if child:
            yield from _walk_processor_snapshots(child)


def insert_counters_by_processor(token: str) -> dict:
    """PutDatabaseRecord가 올리는 "적재 행수" 카운터를 프로세서별로 뽑는다.

    FlowFile 건수는 "파일 몇 개"라 행수가 아니다 - 155MB짜리 FlowFile 하나가
    166만 행인 식이라, 실제 적재 건수는 이 카운터로만 알 수 있다.

    INSERT/UPSERT 등 statement type 별 카운터를 <b>프로세서 단위로 합산</b>한다
    (자세한 배경은 LOAD_COUNTER_NAME_SUFFIX 주석).
    """
    try:
        response = requests.get(
            f"{base_url()}/nifi-api/counters",
            headers=_headers(token),
            verify=False,
            timeout=15,
        )
        response.raise_for_status()
        counters = response.json()["counters"]["aggregateSnapshot"]["counters"]
    except Exception as exc:  # 카운터는 부가 정보라 못 읽어도 진행한다
        print(f"카운터 조회 실패(적재 건수 표시 생략): {exc}")
        return {}
    result: dict = {}
    for counter in counters:
        if not (counter.get("name") or "").endswith(LOAD_COUNTER_NAME_SUFFIX):
            continue
        matched = _PROCESSOR_ID_IN_CONTEXT.search(counter.get("context") or "")
        if matched:
            # 한 프로세서가 여러 statement type 을 올릴 수 있어 대입이 아니라 누적이다.
            result[matched.group(1)] = result.get(matched.group(1), 0) + as_int(counter.get("valueCount"))
    return result


def collect_new_bulletins(pg_id: str, pg_name: str, token: str, seen: set) -> list:
    """그룹 안에서 새로 올라온 경고/에러를 찍고, 그중 ERROR만 요약해 돌려준다.

    커서(after)를 쓰지 않는다: bulletin id는 NiFi 프로세스 안에서만 단조 증가하고
    재시작하면 1부터 다시 시작한다. 커서를 쓰면 재시작 직후의 실패가 통째로
    걸러진다 - 백엔드 수집기가 실제로 이것 때문에 사고를 놓쳤다(2026-07-29).
    """
    try:
        response = requests.get(
            f"{base_url()}/nifi-api/flow/bulletin-board",
            params={"groupId": pg_id},
            headers=_headers(token),
            verify=False,
            timeout=15,
        )
        response.raise_for_status()
        bulletins = response.json()["bulletinBoard"]["bulletins"]
    except Exception as exc:
        print(f"[{pg_name}] bulletin 조회 실패(이번 주기 건너뜀): {exc}")
        return []
    errors = []
    for entry in bulletins:
        bulletin = entry.get("bulletin") or {}
        key = (bulletin.get("id"), bulletin.get("sourceId"), bulletin.get("message"))
        if key in seen:
            continue
        seen.add(key)
        level = bulletin.get("level")
        source = bulletin.get("sourceName")
        message = bulletin.get("message")
        print(f"[{pg_name}]   !! {level} {source}: {message}")
        if level == "ERROR":
            errors.append(f"{source}: {message}")
    return errors


def fail_if_errors(pg_name: str, errors: list) -> None:
    """대기 중 ERROR bulletin을 하나라도 봤으면 실패시킨다.

    WARNING은 평상시에도 올라오므로 실패로 보지 않는다. ERROR만 본다.
    """
    if not errors:
        return
    preview = "\n  - ".join(errors[:5])
    more = f"\n  ... 외 {len(errors) - 5}건" if len(errors) > 5 else ""
    raise RuntimeError(
        f"[{pg_name}] 그룹은 유휴가 됐지만 처리 중 ERROR가 {len(errors)}건 발생했습니다. "
        f"적재가 누락됐을 수 있으니 반드시 확인하십시오.\n  - {preview}{more}"
    )


def print_load_summary(pg_name: str, token: str, baseline: dict, processor_names: dict) -> int:
    """이번 실행에서 프로세서별로 몇 행을 넣었는지 표로 남기고 합계를 돌려준다."""
    final = insert_counters_by_processor(token)
    rows = []
    for processor_id, name in processor_names.items():
        if processor_id not in final:
            continue
        before = baseline.get(processor_id, 0)
        after = final[processor_id]
        # NiFi가 중간에 재시작되면 카운터가 0부터 다시 오른다(그땐 현재값이 곧 이번분).
        delta = after - before if after >= before else after
        if delta > 0:
            rows.append((name, delta))
    if not rows:
        print(f"[{pg_name}] 이번 실행 적재 건수: 없음")
        return 0
    total = sum(delta for _, delta in rows)
    print(f"[{pg_name}] 이번 실행 적재 건수 (총 {total:,}행)")
    for name, delta in sorted(rows, key=lambda r: -r[1]):
        print(f"[{pg_name}]   {name:<24} {delta:>12,} 행")
    return total
