# ETL 워크플로우 캔버스 — 범위 A 상세 구현 계획 (개정 1판)

> **대상**: `docs/2026-08-26-etl-workflow-design.md` **설계안 C(비주얼 컴파일러형)** 의 **범위 A**
> **브랜치**: `feature/etl-workflow-canvas` (복귀 태그 `pre-workflow-redesign` → `52cbd61`)
> **복구**: `docs/2026-08-30-workflow-redesign-rollback.md`
> **원칙**: NiFi 캔버스 무변경. 단 **"NiFi가 나중에 붙을 자리"(완료 콜백 수신부)는 미리 판다.**

> **개정 1판에서 바뀐 것** (코드 재검증 결과 반영):
> 1. **완료 콜백을 run_token 방식 → pg_id 방식으로 변경** — NiFi는 실행별 토큰을 알 수 없다(Parameter Context를 매 실행 갱신하는 건 비현실적). 열린 run은 job당 1개뿐이므로 pg_id로 유일하게 특정된다.
> 2. **`EtlJobRunService` 공존 전략 명시** — 기존 관측기(`NifiProcessorRunTracker` → `openOrAttach`)가 이미 "열린 run에 붙기"로 동작함을 확인. Airflow `open_run`은 같은 시맨틱(open-or-adopt)으로 만들고, 유니크 인덱스 경합은 재조회로 흡수.
> 3. **sensor는 `mode="reschedule"`** — deferrable은 커스텀 Trigger 구현이 필요해 범위 A에서 제외(후속 최적화, 플랜 C).
> 4. **job 단위(granularity) 절 신설** — 미러가 중첩 PG를 이미 재귀 동기화함을 확인. 현재 job이 굵어도(DW=체인 15스텝) 그대로 쓸 수 있고, 체인별 job 분리는 사용자의 NiFi 재구성(플랜 B-3)으로 자연 해소.
> 5. 기존 팩토리 공존 필터의 판정 키를 spec의 `governed_root_pg_ids`로 구체화.

---

## 0. 스코프

| 포함 (범위 A) | 제외 (나중) |
|---|---|
| 워크플로우 캔버스 화면(설계) | ⏸ NiFi `InvokeHTTP` 완료 콜백 추가 → **플랜 B-1** |
| 메타 테이블 `etl_workflow`/`_node`/`_edge` | ⏸ staging-swap 트랜잭션 → **플랜 B-2** |
| 워크플로우 CRUD + 컴파일러 + spec publish | ⏸ NiFi 캔버스 재구성(체인→자식 PG) → **플랜 B-3** |
| **완료 콜백 수신부**(백엔드 API + Airflow sensor) | ⏸ 최상위 일괄 실행(`TRIGGER_WF`) → **플랜 C-1** |
| Airflow 새 팩토리(spec → DAG) | ⏸ Asset 워크플로우 체이닝 → **플랜 C-2** |
| 기존 `nifi_pipelines_dynamic.py` 전환·제거 | ⏸ deferrable trigger 최적화 → **플랜 C-3** |

**핵심 설계 결정**: `await` 태스크를 **콜백 확인 + 유휴 추측 fallback 이중 구조**로 만든다.
→ 플랜 B-1에서 NiFi에 `InvokeHTTP` 2개만 추가하면 **백엔드·Airflow 코드 수정 0으로** 정확한 완료 판정으로 전환된다.

---

## 1. 아키텍처 & 데이터 흐름

```
[설계 타임]
 ETL>생성(NiFi)     워크플로우>설계(신규)          백엔드                Airflow
   job 생성   ──▶   팔레트에서 드래그·연결   ──▶  컴파일·검증    ──▶  Variable(spec)
                    스케줄 지정 → [게시]          spec JSON            └─▶ 팩토리가 DAG 생성

[런타임]
 스케줄 도래 ─▶ DAG 실행 ─▶ TaskGroup(job) ─▶ NiFi PG START ─▶ 완료대기 ─▶ STOP ─▶ verify
                                                   │
                                    (범위A) 유휴 추측 fallback / (플랜B) NiFi 콜백
```

**원장 분리**: NiFi = job만 / 메타DB = 오케스트레이션만 / Airflow = 실행만.

### 1.1 job 단위(granularity) — 재검증으로 확정된 사실

- `NifiJobMirrorService`는 **root부터 재귀**로 모든 PG를 `etl_job`으로 미러한다(`collectGroupsForMirror`, `parent_pg_id` 보존). 실측: `비정형`(top) 안의 `unstructured-csv`/`-image`/… 가 **각각 etl_job 행**으로 존재.
- 따라서 워크플로우 노드는 **어느 깊이의 PG든** `etl_job(id)`로 참조할 수 있다 — 스키마·코드 변경 불필요.
- **현재 상태**: DW/DZ 같은 top PG는 체인들이 프로세서로 직접 들어있어(job 1개 = 체인 여러 개, DZ=20스텝) 굵다. 범위 A에서는 이 굵기 그대로 쓴다(워크플로우 노드 1개 = DZ 전체).
- **목표 상태**(사용자의 NiFi 재구성, 플랜 B-3): `IMP > IMP_DAILY > [COM001M PG][COM002L PG]…` 처럼 체인을 자식 PG로 분리 → 미러가 자동으로 job화 → 팔레트에 개별 job으로 등장 → 워크플로우에서 체인 단위 조합 가능. **범위 A 산출물은 재구성 전후 모두 동작한다.**

---

## 2. 메타 스키마 (V57 ~ V58)

> 현재 최신 마이그레이션 **V56**. 신규는 V57부터.
> 기존 `etl_job`(id, `nifi_pg_id`, `parent_pg_id`, `job_name`, `airflow_dag_id` …)과
> `etl_job_run`(id, job_id, `airflow_dag_run_id`, `trigger_source`, status, `total_inserted` …)을 **재사용**한다.

### V57 — 워크플로우 캔버스

```sql
CREATE TABLE etl_workflow (
    id                BIGSERIAL PRIMARY KEY,
    workflow_key      VARCHAR(80)  NOT NULL,   -- 불변. dag_id 축 (이름 변경돼도 이력 유지)
    name              VARCHAR(200) NOT NULL,
    description       TEXT,
    nifi_group_pg_id  VARCHAR(100),            -- NULL 허용 = 논리 워크플로우(여러 그룹 job 조합)
    schedule_cron     VARCHAR(120),            -- 스케줄 단일 소스. NULL이면 수동 전용
    timezone          VARCHAR(64)  NOT NULL DEFAULT 'Asia/Seoul',
    catchup           BOOLEAN      NOT NULL DEFAULT false,
    max_active_runs   INTEGER      NOT NULL DEFAULT 1,
    suspend_on_error  BOOLEAN      NOT NULL DEFAULT true,
    published_spec    JSONB,                   -- 컴파일 성공본. 팩토리는 이것만 읽는다
    published_at      TIMESTAMP,
    published_by      VARCHAR(100),
    dag_id_override   VARCHAR(200),            -- 레거시 dag_id 연속성
    deleted_at        TIMESTAMP,
    created_at        TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at        TIMESTAMP    NOT NULL DEFAULT now());
CREATE UNIQUE INDEX uq_etl_workflow_key ON etl_workflow (workflow_key) WHERE deleted_at IS NULL;

CREATE TABLE etl_workflow_node (
    id              BIGSERIAL PRIMARY KEY,
    workflow_id     BIGINT       NOT NULL REFERENCES etl_workflow (id),
    node_key        VARCHAR(80)  NOT NULL,     -- 캔버스 내 불변 key = TaskGroup id 축
    node_type       VARCHAR(20)  NOT NULL DEFAULT 'JOB',   -- JOB/BRANCH/JOIN/START/END/SUBWF
    job_id          BIGINT       REFERENCES etl_job (id),  -- node_type=JOB
    sub_workflow_id BIGINT       REFERENCES etl_workflow (id), -- node_type=SUBWF
    trigger_rule    VARCHAR(40)  NOT NULL DEFAULT 'ALL_SUCCESS',
    branch_expr     TEXT,                      -- node_type=BRANCH
    retries         INTEGER      NOT NULL DEFAULT 0,
    retry_delay_sec INTEGER      NOT NULL DEFAULT 60,
    display_x       DOUBLE PRECISION,
    display_y       DOUBLE PRECISION,
    deleted_at      TIMESTAMP,
    created_at      TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP    NOT NULL DEFAULT now());
CREATE UNIQUE INDEX uq_etl_wf_node ON etl_workflow_node (workflow_id, node_key) WHERE deleted_at IS NULL;
CREATE INDEX ix_etl_wf_node_job ON etl_workflow_node (job_id) WHERE deleted_at IS NULL;  -- 참조 무결성 가드용

CREATE TABLE etl_workflow_edge (
    id             BIGSERIAL PRIMARY KEY,
    workflow_id    BIGINT       NOT NULL REFERENCES etl_workflow (id),
    from_node_key  VARCHAR(80)  NOT NULL,
    to_node_key    VARCHAR(80)  NOT NULL,
    condition_type VARCHAR(20)  NOT NULL DEFAULT 'SUCCESS',  -- SUCCESS/FAILURE/ALWAYS/EXPR
    condition_expr TEXT,
    created_at     TIMESTAMP    NOT NULL DEFAULT now());
CREATE UNIQUE INDEX uq_etl_wf_edge ON etl_workflow_edge (workflow_id, from_node_key, to_node_key);
```

### V58 — 실행 원장 확장 (완료 콜백 수신부)

```sql
ALTER TABLE etl_job_run
    ADD COLUMN run_token         VARCHAR(64),    -- Airflow↔백엔드 상관관계 식별자(폴링용)
    ADD COLUMN workflow_id       BIGINT REFERENCES etl_workflow (id),
    ADD COLUMN node_key          VARCHAR(80),
    ADD COLUMN airflow_task_id   VARCHAR(250),
    ADD COLUMN completion_source VARCHAR(20),    -- CALLBACK / OBSERVED / TIMEOUT
    ADD COLUMN rows_processed    BIGINT,         -- 콜백이 싣는 확정 건수(없으면 total_inserted=관측치 사용)
    ADD COLUMN error_message     TEXT;
CREATE UNIQUE INDEX uq_etl_job_run_token ON etl_job_run (run_token) WHERE run_token IS NOT NULL;
-- 한 job에 열린 run은 하나만 (동시 실행 혼선 차단; 기존 openOrAttach 가정을 DB로 강제)
CREATE UNIQUE INDEX uq_etl_job_run_open ON etl_job_run (job_id) WHERE ended_at IS NULL;
```

### 2.1 기존 관측기와의 공존 (개정 1판 핵심)

`etl_job_run`은 이미 **`NifiProcessorRunTracker` → `EtlJobRunService.openOrAttach`** 가 쓴다
(NiFi 활동을 보면 run을 열고, 유휴면 `closeIdleRuns`로 닫는 관측기. `trigger_source='OBSERVED'`).
충돌이 아니라 **협업**으로 설계한다:

| 상황 | 동작 |
|---|---|
| Airflow `open_run`이 먼저 | run 생성(`trigger_source='WORKFLOW'`, run_token 발급). 관측기는 기존 시맨틱대로 **그 run에 붙는다**(openOrAttach가 이미 그렇게 동작) |
| 관측 run이 이미 열려 있음 | `open_run`은 새로 만들지 않고 **그 run을 입양(adopt)** — workflow_id/node_key/run_token/airflow_dag_run_id를 채움 |
| 경합(동시 생성) | `uq_etl_job_run_open` 위반 → **재조회 후 adopt**로 흡수 (`openOrAttach`에도 같은 방어 1줄 추가) |
| 관측기가 유휴로 먼저 닫음 | `closeIdleRuns`가 `completion_source='OBSERVED'`를 찍도록 1줄 추가. sensor는 "닫힌 run"을 보면 누가 닫았든 종료 |

→ 기존 코드 수정은 `EtlJobRunService` **2줄 내외**(unique 경합 재조회 + completion_source 스탬프)로 최소화.

`completion_source`가 신뢰도 지표다: 범위 A에서는 전부 `OBSERVED`, 플랜 B-1 이후 `CALLBACK`.
→ **화면에서 "확정 완료 vs 추정 완료"를 구분해 보여줄 수 있다.**

---

## 3. 백엔드 API

### 3.1 워크플로우 CRUD — `/api/etl/workflows`

| 메서드 | 경로 | 설명 |
|---|---|---|
| GET | `/api/etl/workflows` | 목록(요약: key, name, 스케줄, 게시상태) |
| GET | `/api/etl/workflows/{id}` | 상세(노드·엣지 포함 = 캔버스 로드) |
| POST | `/api/etl/workflows` | 생성 |
| PUT | `/api/etl/workflows/{id}` | 속성 수정(이름·스케줄·타임존 등) |
| **PUT** | `/api/etl/workflows/{id}/graph` | **캔버스 저장(draft)** — 노드·엣지 통째 교체 |
| POST | `/api/etl/workflows/{id}/validate` | **검증만** (게시 안 함) |
| POST | `/api/etl/workflows/{id}/publish` | **검증 + 컴파일 + spec 게시** |
| POST | `/api/etl/workflows/{id}/unpublish` | 게시 취소(index에서 제거 + spec Variable 삭제 → DAG 소멸) |
| DELETE | `/api/etl/workflows/{id}` | 소프트 삭제 (게시 중이면 거부 → 먼저 unpublish) |

권한: `@RequirePermission(system = NIFI)` — 조회 READ / 변경 WRITE.
(검토 리포트 S-1~S-3의 "무방비 컨트롤러" 전철을 밟지 않도록 **처음부터 애노테이션을 단다**.)

### 3.2 팔레트용 (기존 재사용)
`GET /api/etl/jobs` — **이미 존재**(`listEtlJobs()` 프론트 클라이언트 포함). 추가 개발 없음.
새 job이 팔레트에 안 보이면: 미러 주기 대기 또는 기존 `POST /api/etl/jobs/sync` 수동 동기화.

### 3.3 실행 원장 & 완료 콜백 수신부 — `/api/etl/job-runs`

| 메서드 | 경로 | 호출자 | 설명 |
|---|---|---|---|
| POST | `/api/etl/job-runs` | Airflow | **open-or-adopt** (§2.1). body: job_id, workflow_id, node_key, dag_run_id, task_id → run_token 반환 |
| GET | `/api/etl/job-runs/{token}` | Airflow | sensor 폴링용 상태 조회 |
| **POST** | `/api/etl/job-runs/by-pg/{pgId}/complete` | **NiFi(플랜B)** | 성공 콜백. pg→job 해석 후 **열린 run**을 닫음(`completion_source='CALLBACK'`, rows 선택). 이미 닫혔으면 200 no-op(멱등) |
| **POST** | `/api/etl/job-runs/by-pg/{pgId}/fail` | **NiFi(플랜B)** | 실패 콜백 + error_message |

> **개정**: 콜백을 `{token}`이 아니라 **`by-pg/{pgId}`** 로 받는다. NiFi는 실행별 토큰을 알 수 없지만
> 자기 PG id는 Parameter Context로 항상 안다. `uq_etl_job_run_open`이 "job당 열린 run 1개"를
> 보장하므로 pg_id만으로 대상 run이 유일하게 특정된다. run_token은 Airflow↔백엔드 폴링
> 식별자로만 쓴다(URL 로그 노출 우려도 pg_id가 더 낮음).

콜백 2개는 사람 세션이 없으므로 **서비스 토큰**(`X-Service-Token`, 기존 `PermissionAspect`
바이패스 재사용)으로 인증한다. 범위 A에서는 아무도 호출하지 않는 "예약된 자리"다.

---

## 4. 컴파일러 — 검증 규칙 & spec 포맷

### 4.1 검증 규칙 (publish 시 전부 통과해야 함)

| # | 규칙 | 실패 메시지 예 |
|---|---|---|
| V1 | **사이클 없음** (DAG여야 함) | "순환 참조: A → B → A" |
| V2 | **고아 노드 없음** (진입 엣지도 진출 엣지도 없는 고립 노드 금지 — 단일 노드 워크플로우는 허용) | "연결되지 않은 노드: 집계" |
| V3 | JOB 노드는 `job_id` 필수 + 그 job이 살아있어야(`deleted_at IS NULL`) + `nifi_pg_id` 보유 | "삭제된 job 참조: COM001M" |
| V4 | BRANCH 노드는 `branch_expr` 필수 + 출력 엣지 ≥ 2 | |
| V5 | 엣지의 from/to `node_key` 실존 + 자기 자신 금지 | |
| V6 | `schedule_cron` 유효(크론 5필드/프리셋) 또는 NULL | "크론 표현식 오류" |
| V7 | 같은 job을 여러 노드가 참조 → **경고**(동시 실행은 `uq_etl_job_run_open`에 걸림) | 경고(차단 아님) |
| V8 | SUBWF 순환 참조 없음 + 참조 워크플로우 실존 | |
| V9 | **다른 게시된 워크플로우와 job 중복 → 경고** (두 스케줄이 같은 job을 다투는 상황 예방) | 경고 |

### 4.2 spec JSON (Airflow가 읽는 계약)

```json
{
  "workflow_key": "impdaily",
  "dag_id": "etl_wf_impdaily",
  "name": "IMP 일배치",
  "schedule": "0 2 * * *",
  "timezone": "Asia/Seoul",
  "catchup": false,
  "max_active_runs": 1,
  "governed_root_pg_ids": ["8ef671b6-…"],
  "nodes": [
    { "key": "com001m", "type": "JOB", "job_id": 12,
      "nifi_pg_id": "ad0c9365-...", "name": "COM001M",
      "target_tables": ["dz_com001m"],
      "trigger_rule": "all_success", "retries": 1, "retry_delay_sec": 60 }
  ],
  "edges": [ { "from": "com001m", "to": "aggregate", "condition": "SUCCESS" } ],
  "compiled_at": "2026-08-30T16:00:00+09:00",
  "spec_version": 1
}
```

- **`governed_root_pg_ids`**: 각 노드 job의 조상 체인을 미러(`parent_pg_id`)로 거슬러 올라가
  구한 **root 직하 PG id 집합**. 기존 팩토리의 중복 방지 필터가 이 키만 보고 건너뛴다(§7①).
- **`target_tables`**: `etl_job_step.target_table`(PutDatabaseRecord 스텝)에서 컴파일 시 추출.
  → verify가 **Airflow Variable 없이** 적재 검증 대상을 안다(현행 `__target_*` Variable 5종 의존 제거).

### 4.3 게시 경로
```
publish → 검증 → spec JSON 생성 → etl_workflow.published_spec 저장
       → Airflow Variable 갱신 (REST API v2, 서비스 admin Bearer — AirflowDagRunClient 패턴 재사용):
            etl_wf_index          = ["impdaily","impmonthly"]
            etl_wf_spec__impdaily = {...}
unpublish → index에서 제거 + spec Variable 삭제 → 다음 파싱 주기에 DAG 소멸
```

---

## 5. Airflow 새 팩토리 — `airflow/dags/etl_workflows_dynamic.py`

### 5.1 골격

```python
# top-level: NiFi를 조회하지 않는다. Variable(spec)만 읽는다 → 파싱이 NiFi 가용성에 안 묶임
_index = Variable.get("etl_wf_index", default_var=[], deserialize_json=True)
for _key in _index:
    _spec = Variable.get(f"etl_wf_spec__{_key}", default_var=None, deserialize_json=True)
    if not _spec:
        continue
    _dag = build_workflow_dag(_spec)
    globals()[_dag.dag_id] = _dag


def build_workflow_dag(spec: dict) -> DAG:
    with DAG(
        dag_id=spec["dag_id"],
        schedule=spec.get("schedule"),          # None이면 수동 전용
        catchup=spec.get("catchup", False),
        max_active_runs=spec.get("max_active_runs", 1),
        default_args={"owner": "cerebro-etl"},
    ) as dag:
        groups = {n["key"]: build_job_group(n) for n in spec["nodes"]}
        for e in spec["edges"]:
            groups[e["from"]] >> groups[e["to"]]
    return dag


def build_job_group(node: dict) -> TaskGroup:
    """job 1개 = TaskGroup 1개. 실패 지점을 국면별로 구분한다."""
    with TaskGroup(group_id=node["key"]) as tg:
        open_run = PythonOperator(task_id="open_run", python_callable=_open_run)    # POST /job-runs → run_token(XCom)
        start_pg = PythonOperator(task_id="start_pg", python_callable=_start_pg)    # NiFi PG START
        await_pg = PythonSensor(task_id="await", python_callable=_poke,
                                mode="reschedule", poke_interval=15,                # ★ 이중 구조(5.2)
                                timeout=node.get("await_timeout_sec", 3600))
        stop_pg  = PythonOperator(task_id="stop_pg", python_callable=_stop_pg,
                                  trigger_rule="all_done")                          # 실패해도 반드시 정지
        verify   = PythonOperator(task_id="verify", python_callable=_verify)        # spec.target_tables 기반
        open_run >> start_pg >> await_pg >> stop_pg >> verify
    return tg
```

> **개정**: sensor는 `mode="reschedule"`(poke 사이 워커 슬롯 반납). deferrable(Triggerer 오프로드)은
> 커스텀 `BaseTrigger` 구현이 필요해 범위 A에서 제외 — 플랜 C-3에서 최적화.

### 5.2 ★ `_poke` — 콜백 확인 + 유휴 추측 fallback (핵심)

```python
def _poke(node, token, **_):
    # 1순위: 백엔드 원장 — 콜백(플랜 B-1) 또는 관측기(closeIdleRuns)가 run을 닫았는가
    run = get_json(f"/api/etl/job-runs/{token}")
    if run["status"] in ("SUCCEEDED", "FAILED"):
        if run["status"] == "FAILED":
            raise AirflowException(f"job 실패: {run.get('errorMessage')}")   # 즉시 태스크 실패
        return True                              # completion_source는 원장이 이미 기록

    # 2순위(fallback): 기존 유휴 판정 로직 이관 (queued==0 and activeThreads==0, 안정화 대기 포함)
    if _observed_idle(node["nifi_pg_id"]):
        post(f"/api/etl/job-runs/{token}/observe-complete")   # OBSERVED로 닫기 요청
        return True
    return False
```

**이 구조 덕분에**: 플랜 B-1에서 NiFi에 `InvokeHTTP` 2개만 추가하면 1순위 경로가 활성화되고,
**Airflow·백엔드 코드는 그대로**다. 실패 콜백은 sensor를 **즉시 실패**시켜 후행을 정확히 막는다
(현행 "실패도 유휴로 수렴 → 초록불" 오판 제거의 첫 단추).

### 5.3 기존 파일에서 **이관해야 할** 로직
`nifi_pipelines_dynamic.py`(781줄)에서 아래를 `etl_nifi_ops.py`(공용 모듈)로 추출해 재사용:

| 함수 | 용도 | 비고 |
|---|---|---|
| `_get_nifi_token`, `_set_group_state` | NiFi 인증·상태 변경 | 필수 |
| `apply_process_group_action` | PG START/STOP | 필수 |
| `verify_process_group_action` | 상태 검증 | 필수 |
| `verify_target_db_landing` | 적재 건수 검증 | 필수 — 대상 테이블은 spec `target_tables`로(Variable 의존 제거) |
| **`_collect_new_bulletins`, `_fail_if_errors`** | **컴포넌트별 실패 감지** | **필수** — 빠지면 프로세서 실패를 못 잡음 |
| `wait_for_group_completion` | 유휴 판정 | sensor **fallback**(`_observed_idle`)으로 이관 |
| `_print_load_summary`, `_insert_counters_by_processor` | 건수 집계 | 이관 |

**폐기**: `build_group_chain`(출력포트 기반 잡 순서 추론) — **워크플로우 캔버스가 대체.**
**불필요해짐**: `nifi_process_groups_cache` / `nifi_root_connections_cache` (spec Variable이 대신함).

---

## 6. 프론트 화면 — `워크플로우 > 설계`

### 6.1 캔버스 라이브러리 결정

**`@xyflow/react` (React Flow) 채택.**

| 근거 | |
|---|---|
| 노드 에디터 사실상 표준 (드래그·연결·줌·팬·미니맵 기본 제공), MIT, React 19 지원 | |
| **폐쇄망 안전**: npm 접근은 **빌드 스테이지에서만** 발생, 폐쇄망은 `docker load`만(`offline/README.md` 원칙). 런타임 영향 없음 | |
| 대안(자체 SVG 구현)은 개발량 수 배 | |

→ 작업: `package.json` 의존성 추가 + `package-lock.json` 갱신 + 이미지 재빌드.
→ `offline/image-manifest.json` 변경 불필요(커스텀 빌드 이미지 태그 유지).

### 6.2 라우트 & 메뉴
- 라우트: `/workflows/design` (목록), `/workflows/design/:id` (캔버스)
- 메뉴: `AppLayout.tsx` `워크플로우` 그룹 맨 위에 `설계` (`system: "NIFI"`, 조회 READ/버튼 WRITE 게이트)

### 6.3 컴포넌트 분해

```
WorkflowDesignPage.tsx          목록 + 새 워크플로우 생성
WorkflowCanvasPage.tsx          3분할 레이아웃 컨테이너
├─ JobPalette.tsx               좌: job 목록(listEtlJobs) + 검색 + 드래그 소스 + [동기화]
├─ WorkflowCanvas.tsx           중: ReactFlow 래퍼(노드·엣지·연결 규칙)
│   ├─ JobNode.tsx              JOB 노드(이름·상태·아이콘)
│   ├─ BranchNode.tsx / JoinNode.tsx
│   └─ ConditionEdge.tsx        조건별 색/라벨(성공=회색, 실패=빨강, 항상=점선)
├─ PropertyPanel.tsx            우: 워크플로우 속성 / 선택 노드 속성
└─ PublishBar.tsx               하단: 저장·검증·게시 + 상태 배지(draft/게시됨/게시본과 다름/검증실패)
api/workflows.ts                CRUD·validate·publish 클라이언트
```

### 6.4 상태 & UX 규칙
- **저장(draft) ≠ 게시** 명확 분리. 게시 전에는 DAG가 생기지 않음을 배지로 표시
- 게시 후 수정 시 **"게시본과 다름"** 배지 → 재게시 유도
- 검증 실패 시 **문제 노드를 캔버스에서 하이라이트**
- 게시 직후 안내: *"최대 5분 내 Airflow에 반영됩니다"* (dag_processor `refresh_interval=300`)
- 실행 현황 화면(기존)에서 job 완료 배지에 `completion_source` 노출: `확정(콜백)` / `추정(관측)`

---

## 7. 기존 팩토리 전환 절차 (순서 엄수)

```
① 새 팩토리 추가 (기존 파일 그대로 둠)
   → 공존 기간 중복 DAG 방지: 기존 팩토리 top-level에 필터 추가 —
     etl_wf_index의 각 spec에서 governed_root_pg_ids 합집합을 만들어,
     그 안에 든 root 직하 PG는 기존 팩토리가 DAG를 만들지 않는다
② 워크플로우 1개 게시 → 새 DAG 생성 확인 → 수동 실행 → 정상 동작 검증
③ 나머지 워크플로우 게시 · 도메인별 컷오버
④ 기존 파일 제거: airflow/dags/nifi_pipelines_dynamic.py → airflow/dags_archive/
   (⚠️ dags 폴더에 .py로 두면 계속 파싱됨)
⑤ 정리:
   - 캐시 Variable 2개 삭제(nifi_process_groups_cache, nifi_root_connections_cache)
   - {dag_id}__* Variable: 실측 0개 → 정리 대상 없음
   - airflow_dag_catalog에서 옛 dag_id 행 정리 (유령 DAG 방지)
   - (선택) etl_workflow.dag_id_override로 레거시 dag_id 승계 시 catalog 행 재매핑
```

**절대 금지**: ①②를 건너뛰고 기존 파일부터 제거 → spec 미게시 상태면 **DAG 0개 = 배치 전면 중단.**

---

## 8. 구현 순서 (각 단계 독립 검증 가능)

| 단계 | 산출물 | 완료 판정 |
|---|---|---|
| **P0** | 전제 확인 | 미러가 대상 job들을 정확히 갖고 있는지(`GET /api/etl/jobs`), Airflow Variable REST 쓰기 스모크 |
| **P1** | V57 + 워크플로우 CRUD API | `curl`로 워크플로우·노드·엣지 생성/조회 성공 |
| **P2** | 컴파일러(검증 9종) + publish(spec → Variable) | 사이클 그래프 **거부** / 정상 그래프 spec 게시 확인 |
| **P3** | `etl_nifi_ops.py` 추출 + 새 팩토리 + sensor(fallback만) | 게시한 워크플로우가 **DAG로 생성**, 수동 실행 시 job이 순서대로 NiFi에서 실행 |
| **P4** | V58 + job-runs API 4종 + `EtlJobRunService` 공존 2줄 | `curl`로 by-pg complete/fail 호출 시 run 닫힘·sensor 1순위 종료, 관측기 공존 확인 |
| **P5** | 프론트 캔버스 화면 | 화면에서 드래그 → 연결 → 스케줄 → 게시 → DAG 생성 E2E |
| **P6** | 전환·정리 (§7) | 기존 파일 제거 후에도 전 워크플로우 정상, 유령 DAG 없음 |

**P1~P4는 화면 없이 API/CLI로 검증 가능** — 프론트(P5)와 병행 가능.

---

## 9. 검증 계획

**단위/통합**
- 컴파일러: 사이클·고아·삭제 job 참조·잘못된 크론·BRANCH 엣지 부족 → 각각 거부 (테스트 9건)
- job-runs: open-or-adopt(관측 run 입양) / by-pg complete 멱등 / 경합 시 재조회
- sensor: 원장 닫힘(성공) → 종료 / 원장 닫힘(실패) → 태스크 즉시 실패 / 미도착 → fallback / 타임아웃

**E2E 시나리오**
1. job 3개 fan-in 워크플로우 → 게시 → DAG 생성 확인
2. 수동 실행 → 캔버스 순서대로 NiFi START/STOP
3. **중간 job 강제 실패** → 후행 `upstream_failed` 정지, 무관한 가지는 계속
4. 실패 job 수정 후 **"이 지점부터 재시작"** → 선행 유지, 실패 지점부터 후행 재개
5. 스케줄(`*/10 * * * *`) 자동 실행 확인 후 원복
6. 워크플로우 이름 변경 → **dag_id 불변**(이력 유지)
7. 관측기 공존: Airflow 실행 중 `etl_job_run`이 **1행**으로 유지(입양)되는지

**회귀**: CDC(`kafka_pipelines_dynamic.py`) 무영향 / 대시보드·알림·권한 정상

---

## 10. 리스크 & 대응

| # | 리스크 | 대응 |
|---|---|---|
| R1 | **전환 중 배치 공백** | §7 순서 엄수. 롤백은 옛 파일 복원 |
| R2 | 중복 DAG(공존기) | `governed_root_pg_ids` 필터(§7①) + 컷오버 후 즉시 제거 |
| R3 | job 삭제 → 노드 깨짐 | `ix_etl_wf_node_job` 참조 확인 → **삭제 가드**(참조 중이면 거부/경고). 미러의 소프트삭제(`deleted_at`)도 검증 V3에 걸림 |
| R4 | 같은 job 동시 실행 | `uq_etl_job_run_open` + 검증 V7/V9 경고 |
| R5 | **완료 판정이 여전히 추측**(범위 A) | `completion_source=OBSERVED` 화면 노출로 구분. 플랜 B-1에서 해소 |
| R6 | spec Variable 유실 | `published_spec`(메타DB)이 원본, Variable은 파생 → 재게시로 복구 |
| R7 | React Flow 신규 의존성 | 빌드 시점 전용(폐쇄망 무영향). 락파일 커밋 필수 |
| R8 | 게시 후 최대 5분 지연 | UX 안내 + 필요 시 `refresh_interval` 하향 검토 |
| R9 | 관측기·Airflow 원장 경합 | §2.1 open-or-adopt + unique 재조회. E2E 7번으로 검증 |
| R10 | 굵은 job(DW=체인 15개)의 부분 실패 | 범위 A 한계로 명시 — job 내부 체인 구분은 사용자의 NiFi 재구성(플랜 B-3) 후 자연 해소 |

---

## 11. 착수 전 체크리스트

- [x] 복귀 태그 `pre-workflow-redesign` / 브랜치 `feature/etl-workflow-canvas`
- [x] `pipeline_meta`·`airflow` DB 덤프, Variable export
- [ ] 미푸시 커밋 4개 push (유실 위험 잔존)
- [ ] P0 착수
