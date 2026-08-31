# ETL 워크플로우 캔버스 — 범위 A 구현 진행 상황 (2026-08-30)

> **브랜치**: `feature/etl-workflow-canvas` (복귀 태그 `pre-workflow-redesign` → `52cbd61`)
> **상태**: **범위 A 완료 + Asset 체이닝 완료 · NiFi 재구성(B-3) 완료 · P6 메커니즘 완료(최종 컷오버 보류)**. 전부 **미커밋**.
> **최종 갱신**: 2026-08-30 (명명 인식·엣지 조건 UI·재시작 검증·Asset 체이닝·DW/DZ 재구성 반영)
> **관련 문서**: 설계 `2026-08-26-etl-workflow-design.md` / 계획 `-impl-plan-A|B|C.md` / 복구 `-rollback.md`

---

## 1. 한 줄 요약

**화면에서 job을 끌어와 연결하고 게시하면 Airflow DAG가 만들어지고, 워크플로우가 끝나면 다음
워크플로우가 자동으로 이어 도는 경로까지 전 구간 동작한다**(브라우저 E2E + 실제 실행으로 실증).
NiFi 캔버스도 "1 프로세스그룹 = 1 job" 구조로 재구성을 마쳤다. 남은 것은 최종 컷오버(P6 ④⑤)와
확장 기능(BRANCH/SUBWF/일괄실행)뿐이다.

---

## 2. 단계별 상태

| 단계 | 내용 | 상태 |
|---|---|---|
| P0 | 전제 확인(미러 job, Airflow Variable 쓰기) | ✅ job 13개 / Variable POST 201·DELETE 204 |
| P1 | V57 스키마 + 워크플로우 CRUD API | ✅ 13항목 스모크 통과 |
| P2 | 컴파일러(검증 9종) + spec 게시 | ✅ 사이클·고아·삭제job·크론 거부, Variable 게시 확인 |
| P3 | 공용 모듈 + 새 팩토리 + 실행 원장 | ✅ DAG 생성·태스크 10개·의존성 확인 |
| P4 | 완료 콜백 수신부 | ✅ **P3에 통합**(팩토리가 원장 API에 의존해 앞당김) |
| P5 | 프론트 캔버스 화면 | ✅ 브라우저 E2E 성공 |
| P6 | 컷오버 | ⏸ **메커니즘 완료·검증됨 / 파일 제거는 보류**(§5) |
| 추가 | **명명 인식**(`etl_wf_*`) 4곳 | ✅ 실행 현황 ETL 탭 표시·감시 대상 등록 |
| 추가 | **엣지 조건 편집 UI** | ✅ 성공 시/실패 시/완료 시 선택 |
| 추가 | **실패 지점 재실행** | ✅ 실제 실행으로 검증(선행 유지·후행 재개) |
| 추가 | **Asset 체이닝**(워크플로우→워크플로우) | ✅ V59, 실제 자동 트리거 확인 |
| B-3 | **NiFi 캔버스 재구성**(DW·DZ) | ✅ 각 5체인을 자식 PG로 분리 |

---

## 3. 최종 검증 (브라우저 E2E, admin 로그인)

```
로그인 → 워크플로우>설계 → 새 워크플로우(impdaily)
  → DW·DZ 팔레트 클릭 배치 → DW ──성공 시──▶ DZ 연결
  → 저장 → 검증 → 게시
      ↓ 5초
Airflow: etl_wf_impdaily (owner cerebro-etl, schedule 0 2 * * *)
  dw.open_run → dw.start_pg → dw.await → dw.stop_pg → dw.verify
                                                         ↓
  dz.open_run → dz.start_pg → dz.await → dz.stop_pg → dz.verify
```
JS 오류 0건. DB 게시본 `published_by=admin`, nodes 2 / edges 1.

---

## 4. 구현 중 발견해 고친 것 (계획서와 달랐던 점)

| # | 발견 | 조치 |
|---|---|---|
| 1 | `uq_etl_job_run_open` 인덱스가 **V18에 이미 존재**(정의 동일). 계획서는 신규 추가로 잘못 잡음 | V58에서 제거. **오히려 좋은 소식** — "한 job에 열린 실행은 하나"가 처음부터 보장돼 있어, NiFi 콜백이 pg_id만으로 대상을 특정할 수 있다는 설계 전제가 실증됨 |
| 2 | 완료 콜백을 `run_token`으로 받는 설계는 **비현실적** — NiFi는 실행마다 바뀌는 토큰을 모름 | `by-pg/{pgId}` 방식으로 변경(위 인덱스가 근거) |
| 3 | jsonb 컬럼에 `@JdbcTypeCode(SqlTypes.JSON)` 누락 → INSERT 실패 | 기존 `EtlJobSnapshot` 패턴에 맞춤 |
| 4 | 없는 job 참조 시 FK 위반이 500으로 노출 | 저장 단계 검사 추가(`VALIDATION_ERROR`). 단 **이미 삭제된 job은 저장 허용 + 게시 차단** — 화면에서 깨진 노드를 보여주고 고칠 기회를 줘야 하므로 경로를 다르게 둠 |
| 5 | **게시 버튼 영구 비활성** (치명적) — 저장 → 재조회 → `setNodes` → React Flow가 `dimensions` 변경 발생 → 사용자 편집으로 오인해 dirty 재설정 | `isUserEdit()`로 실제 편집(이동 끝·추가·삭제)만 dirty 처리 |
| 6 | **노드 연결 불가** (치명적) — `style` prop이 antd 전역 CSS에 밀려 노드 폭이 부풀고 핸들이 겹침 | `index.css`의 `.wf-node` 클래스로 크기 확정(150×44px) |
| 7 | `TaskGroup` deprecated import | `airflow.sdk`로 교체 |
| 8 | 인프라: `airflow-dag-processor`/`scheduler`가 dags 마운트를 **빈 디렉터리로** 인식(WSL2) | `--force-recreate`로 복구. 복구 후 DAG 반영이 5분→**5초** |

> 5·6번은 API 테스트로는 잡히지 않고 **브라우저 검증에서만** 드러났다. 빌드 통과를 완성으로 착각했으면 "게시가 안 되는 화면"을 납품할 뻔했다.

---

## 4-A. 추가 구현분 (2026-08-30 후반)

### 명명 인식 — `etl_wf_*`
새 팩토리가 만드는 DAG 이름을 기존 코드 4곳이 몰라서 **"기타"로 분류**됐다. 그래서 워크플로우를
게시해도 실행 현황 화면에 나타나지 않았다(만들기만 되고 운영이 안 되는 상태).

| 위치 | 조치 |
|---|---|
| `dagHistory.ts` | `etl_wf_*` → ETL 분류 |
| `AirflowDagCatalogSyncService` | 카탈로그 등록(폴더 "워크플로우") → 이상 감지 대상 |
| `AirflowDagCatalogController` | 스케줄 변경을 **차단**하고 캔버스로 유도(스케줄 단일 소스 보호) |
| `AirflowDagCatalogDeletionService` | 삭제를 **차단**하고 게시 취소로 유도(NiFi 자산 보호) |

뒤 두 개는 단순 차단이 아니라 **설계 원칙을 지키는 가드**다. 실행 현황에서 Variable을 덮어쓰면
`schedule_cron`과 싸우고, 그 삭제는 NiFi 그룹 자체를 지우는 동작이라 조합만 없애려다 실제 자산이 날아간다.

### 실패 지점 재실행 — 실측
DAG를 실제로 돌리고 중간 태스크를 실패시킨 뒤 `clearTaskInstances(include_downstream)` 호출:
`open_run`·`start_pg`는 **success 유지**, 실패한 `await`부터 후행만 재실행. 하류 워크플로우도 초기화.

### Asset 체이닝 (V59)
"DZ 일배치가 끝나면 DW 일배치" 를 **워크플로우 단위**로 잇는다.

- 스키마: `produces_asset_uri`, `upstream_workflow_ids`(jsonb), `upstream_mode`(ALL/ANY)
- 게시 시 Asset URI 자동 확정(`cerebro://etl/{key}`) — 사용자는 URI를 볼 일이 없다
- 팩토리: 선행이 있으면 `schedule`이 시간이 아니라 `[Asset(...)]`(AND) 또는 `AssetAny`(OR)
- **종단 태스크 `workflow_done`**: 모든 리프를 모아 `trigger_rule=all_success`로 outlet 발행.
  중간 태스크에 달면 워크플로우가 아직 도는데 후행이 시작되고, 실패해도 신호가 나간다.
- 검증 V10(순환·실존·미게시), V11(선행+스케줄 동시 설정 경고)
- **실측**: DZ 6/6 성공 → `workflow_done` → Asset 이벤트(11:42:08) → DW가 `asset_triggered`로
  **지연 0초** 자동 시작 → 6/6 성공

**사용자 조작**: 캔버스 우측 속성 패널의 `선행 워크플로우` 드롭다운 하나.

### NiFi 재구성 (B-3)
DW·DZ 각각의 5개 체인을 자식 PG로 분리해 "1 프로세스그룹 = 1 job"으로 만들었다.
그룹 간 순서용 연결·포트는 제거(순서는 캔버스가 원장). DW는 trigger가 없어
`GenerateFlowFile` 5개를 신설했다. 활성 job 13 → 21개.

부수 효과로 **DW/DZ 아래 체인 이름이 겹쳐**(COM001M ×2) 팔레트·노드 라벨·TaskGroup id가
충돌했다. 백엔드가 `parentGroupName`을 내려주고 화면이 `DW / COM001M`으로 표시하며,
노드 키에 그룹 접두어를 붙여 해결했다(`dz_com001m` / `dw_com001m`).

---

## 5. P6 컷오버 — 현재 상태와 보류 근거

### 완료: 공존 필터 (메커니즘)
`nifi_pipelines_dynamic.py`에 필터 추가. 게시된 spec의 `governed_root_pg_ids`에 든 root 직하 PG는
기존 팩토리가 DAG를 만들지 않는다.

**실측:**
| 상태 | 기존 팩토리 | 새 팩토리 | 중복 |
|---|---|---|---|
| 게시 전 | 5개 | 0 | — |
| DZ·비정형 게시 후 | **3개** | `etl_wf_impdaily` | **0건** |
| 게시 취소 후 | **5개 자동 복구** | 0 | — |

→ 컷오버가 **가역적**임이 검증됐다. `airflow_dag_catalog` 미러도 자동으로 따라온다(넘어간 DAG를 `enabled=f`로 내림).

### 보류: 기존 파일 제거
root 직하 7개 그룹 중 워크플로우로 옮길 대상이 아직 정해지지 않았다.

| 그룹 | 자식그룹 | 자식job | 성격 |
|---|---|---|---|
| DZ / 비정형 / DW / dfdf | 0~ | 0~1 | 운영 후보 |
| test | 3 | 1 | 실험용 추정 |
| ddd | 0 | 0 | 실험용 추정 |
| **Template** | 3 (`Initial`/`Incremental`/`truncate_initial`) | 0 | **템플릿 보관소** |

**`Template` 결정**: 운영 파이프라인이 아니라 **job 생성 시 복사해 쓰는 원본 보관 그룹**이다
(`NifiClient.java:56-60, 801`). 애초에 DAG가 있어야 할 그룹이 아니므로 **워크플로우로 옮기지 않고
현 상태로 둔다.** 컷오버가 끝나 기존 팩토리가 사라지면 이 그룹의 DAG(`nifi_pipeline_1cf387f7_control`)도
함께 사라지는데, 그게 오히려 올바른 상태다.

**보류 이유(플랜 B 선택)**: 지금 컷오버하면 굵은 job(DZ = 체인 5개) 기준으로 워크플로우를 짜게 되어,
NiFi 재구성 후 다시 손봐야 한다. 메커니즘이 이미 완성·검증됐으므로 서두를 이유가 없다.

---

## 5-A. 남은 기능 (나중에)

| 기능 | 무엇 | 대체 수단 |
|---|---|---|
| **BRANCH** | 데이터·변수 값으로 갈래 선택("월말이면 월마감") | 워크플로우를 분리해 각각 다른 스케줄 |
| **SUBWF** | 여러 워크플로우가 공유하는 재사용 묶음(worklet) | 같은 job을 각 워크플로우에 중복 배치 |
| **TRIGGER_WF** | 상위 DAG가 하위 워크플로우들을 일괄 실행 | 워크플로우를 하나씩 수동 트리거 |
| **P6 ④⑤** | 기존 팩토리 제거 + 캐시 Variable 정리 | 공존 필터로 안전하게 병행 중 |
| **B-1/B-2** | NiFi 완료 콜백 · staging-swap 트랜잭션 | 수신부는 이미 구현됨(§4-A), NiFi 배선만 남음 |

조건 링크(성공/실패/완료 시)는 BRANCH와 다르며 **이미 구현**되어 있다.

## 6. 다음 순서

```
[사용자] 플랜 B-3: NiFi 캔버스 재구성
   DW/DZ 안의 체인을 자식 PG로 분리 (COM001M, COM002L, …)
   그룹 간 순서용 연결 제거 (순서는 캔버스가 원장)
        ↓ 미러가 자동 동기화 → 팔레트에 체인별 job 등장
[사용자] 워크플로우 캔버스에서 체인 단위로 조합 → 게시
        ↓ 도메인별 점진 컷오버 (필터가 중복을 막아줌)
[선택]   플랜 B-1 완료 콜백 / B-2 staging-swap
   → completion_source 가 OBSERVED → CALLBACK 으로 바뀌며 판정이 확정으로
[마지막] P6 ④⑤: nifi_pipelines_dynamic.py → dags_archive/ + 캐시 Variable 2개 정리
```

---

## 7. 변경 파일 (미커밋)

**신규 (14)**
- `web/backend/.../workflow/` — 엔티티 3, 레포 3, DTO 3, 서비스 4, 컨트롤러 2
- `db/migration/V57__create_etl_workflow.sql`, `V58__extend_etl_job_run_for_workflow.sql`
- `airflow/dags/etl_nifi_ops.py`, `etl_job_runtime.py`, `etl_workflows_dynamic.py`
- `web/cerebroetl-ui/src/api/workflows.ts`, `pages/WorkflowDesignPage.tsx`, `pages/WorkflowCanvasPage.tsx`
- 문서 4종(계획 A/B/C, 복구 런북) + 이 문서

**수정 (7)**
- `airflow/dags/nifi_pipelines_dynamic.py` — 공존 필터
- `web/backend/.../jobcatalog/EtlJobRun.java` — 워크플로우 연동 필드·메서드
- `web/cerebroetl-ui/` — `App.tsx`(라우트), `AppLayout.tsx`(메뉴), `index.css`(노드 스타일), `package.json`·`package-lock.json`(@xyflow/react)

**미해결로 남긴 것**: 미푸시 커밋 4개(세션 이전 작업, 유실 위험).

---

## 8. 후속 UI 보완 (2026-08-30)

### 8-1. 노드 연결점 4방향

기본 React Flow 노드는 위/아래에만 점이 있어, 좌우로 배치하면 선이 크게 돌아가고
점 자체가 작아 "두 job을 어떻게 잇느냐"는 질문이 나올 만큼 눈에 띄지 않았다.

- 커스텀 노드 `JobNode` 도입 (`nodeTypes={{ job: JobNode }}`)
  - 들어오는 쪽: 위(`t-top`), 왼쪽(`t-left`) — target
  - 나가는 쪽: 아래(`s-bottom`), 오른쪽(`s-right`) — source
  - 한 변에 source/target을 겹쳐 두면 React Flow가 어느 점을 잡았는지 가려내지 못해
    연결이 성립하지 않는다. 그래서 변마다 한 종류만 둔다.
- 점 크기 6px → 11px, 파란 테두리, 노드 hover 시 확대. target은 회색 테두리로 구분.
- 저장된 엣지에는 어느 변에서 나가는지가 없다. 두 노드의 상대 위치로 추정한다
  (가로로 늘어놓으면 오른쪽→왼쪽, 세로로 쌓으면 아래→위).

**막혔던 지점**: `.wf-node { overflow: hidden }` 이 노드 밖으로 6px 나간 점을 잘라내
점이 보이지도 눌리지도 않았다(마우스 지점이 캔버스 배경에 닿음). 줄임표 처리를
안쪽 라벨(`span`)로 옮기고 노드는 `overflow: visible` 로 되돌려 해결.

### 8-2. 스케줄 입력을 프리셋 방식으로

설계 화면은 크론 문자열을 그대로 받는 입력 한 칸이었다. 실행 현황 화면의 위저드와
같은 UI로 맞췄다: 수동/정기 선택 → 프리셋(매시간·매일·매주) → 시각·분 →
또는 직접 입력 → 미리보기 → 다음 5회 실행.

- `src/utils/schedulePreset.ts` 신설. `presetCron` / `presetDescription` /
  `nextPresetRuns` / `validCron` / `scheduleDescription` 을 두 화면이 공유한다.
  각자 계산하면 "화면마다 다음 실행 시각이 다르다"가 되기 때문이다.
- `parseCronToPreset()` 추가 — 저장된 크론을 프리셋으로 되돌려 폼을 채운다.
  프리셋으로 표현할 수 없는 식(예: `*/15 * * * *`)은 직접 입력 칸에만 남는다.

### 8-3. 브라우저 검증 결과

| 항목 | 결과 |
|---|---|
| 노드당 연결점 | 4개(상·하·좌·우), 서버에서 불러온 노드도 동일 |
| 오른쪽 → 왼쪽 연결 | 성립 (엣지 1) |
| 아래 → 위 연결 | 성립 (엣지 2) |
| 설계 화면 스케줄 | 프리셋·미리보기·다음 5회 표시, 저장 후 재조회에도 유지 |
| 실행 현황 위저드 | 공용 모듈 전환 후에도 동일하게 동작 |
| JS 오류 | 없음 |

검증용 워크플로우(`handleqa`)와 노드·엣지는 삭제했고, Airflow Variable은 생성되지 않았다.

---

## 9. 그룹별 워크플로우 + DW/DZ 컷오버 (2026-08-30)

### 9-1. 워크플로우가 그룹에 속한다

`etl_workflow.nifi_group_pg_id`는 V57부터 있었지만 화면에서 쓰지 않아 전부 NULL이었다.
이제 만들 때 그룹을 고르고, 그 그룹 안에서만 job을 집는다. Informatica의 폴더와 같은 자리다.

- **설계 목록**: 왼쪽에 ETL 그룹 트리(GROUPING만), 고르면 그 그룹 하위 워크플로우만 보인다.
  그룹을 고른 채로 "새 워크플로우"를 누르면 그 그룹이 기본값으로 들어간다.
- **캔버스 팔레트**: 소속 그룹 하위로 좁힌다. DW 워크플로우를 그리다 DZ의 같은 이름 job을
  집는 사고를 막는다(두 그룹 다 COM001M을 갖고 있다). 필요하면 "전체 보기"로 넓힌다.
- **V12 경고**: 소속 그룹 밖 job을 참조하면 경고. 여러 그룹을 모으는 워크플로우도 정당해서
  막지는 않는다.

### 9-2. workflowKey 자동 생성

사용자는 키를 입력하지 않는다. 서버가 그룹 이름 + 워크플로우 이름에서 만든다.

- `DW` + `DW 일배치` → `dw` (이름이 그룹으로 시작하면 접두어를 겹쳐 붙이지 않는다)
- 한글만 있는 이름은 슬러그가 비므로 그룹 이름만 쓴다
- 겹치면 뒤에 번호(`dw_2`)

키가 dag_id(`etl_wf_{key}`)의 축이라 전역 유일해야 하는데, 그룹별로 그리면 이름이 겹치기
마련이다. 그룹 이름을 앞에 두어 충돌을 구조적으로 없앴다. 마이그레이션은 필요 없었다.

### 9-3. V2(고아 노드)를 오류에서 경고로

컷오버를 해보고 나서야 드러난 문제다. 옛 DAG는 **그룹 전체를 켜고 잡들이 병렬로 돈다**
(그룹 간 연결이 0개다). 그대로 옮기면 연결 없는 노드 5개가 되는데, V2가 이걸 오류로 보고
게시를 막았다. 연결이 하나도 없는 워크플로우는 "한꺼번에 돌린다"는 뜻이므로 문제로 보지
않고, 순서를 그려놓고 한 노드만 떨어진 경우만 경고한다.

### 9-4. 실행 현황 트리를 워크플로우 기준으로

실행되는 단위는 DAG = 워크플로우다. job은 그 안의 TaskGroup이라 트리에 둘 이유가 없었다.

- JOB 그룹을 트리에서 뺐다(눌러도 아무 일이 없는 항목이 절반이었다)
- 워크플로우를 소속 그룹 아래에 붙인다(`nifi_group_pg_id` 기준). 예전에는 매칭이 안 돼
  "기타" 목록으로 빠졌다
- 아직 안 옮긴 그룹은 옛 DAG가 그 자리를 지킨다(점진 컷오버)
- 돌릴 것이 하나도 없는 그룹은 감춘다
- 오른쪽 숫자는 그룹 수가 아니라 **워크플로우 수**
- `dag_display_name`을 지정해 트리에 dag_id가 아니라 사용자가 지은 이름이 나온다

### 9-5. 컷오버 결과

| 그룹 | 이전 | 이후 |
|---|---|---|
| DW | `nifi_pipeline_ad0c9365_control` | `etl_wf_dw` (DW 일배치, job 5) — 옛 DAG는 stale |
| DZ | `nifi_pipeline_ad0c12a1_control` | `etl_wf_dz` (DZ 일배치, job 5) — 옛 DAG는 stale |
| Template | `nifi_pipeline_1cf387f7_control` | **그대로** |

- 게시만으로 전환됐다. 파일은 건드리지 않았고, 게시를 내리면 옛 DAG가 되돌아온다
- 옛 DAG는 전부 `timetable_summary = None`(수동 전용)이었고 새 워크플로우도 같아서
  **스케줄 손실 없음**
- 생성된 DAG는 job당 TaskGroup 1개(`open_run`/`start_pg`/`await`/`verify`/`stop_pg`)

**Template을 못 옮기는 이유**: 잡 미러가 Template 하위를 의도적으로 제외한다
([NifiJobMirrorService](../web/backend/src/main/java/com/company/pipeline/jobcatalog/NifiJobMirrorService.java) —
"Template 하위가 아니면서 직계 프로세서를 가진 그룹만 미러링"). 템플릿은 실행 대상이 아니라
복제 원본이라서다. 그래서 `nifi_pipelines_dynamic.py`는 아직 보관 이동(P6 ④)을 할 수 없다.
Template에 DAG가 필요한지부터 정해야 한다.

### 9-6. 정리한 것 / 남은 것

- 지웠음: 검증 잔여물 `etl_wf_dwdaily`, `etl_wf_dzdaily`, `etl_wf_impdaily`
  (Airflow 레코드 + 카탈로그 행)
- 남음: stale DAG 8개(`nifi_pipeline_*` 6개 + 이번에 stale이 된 DW/DZ 2개), `kafka_pipeline_1_control`.
  **돌지 않는다.** 지우면 Airflow 실행 이력이 함께 사라지므로 사용자 판단이 필요하다

---

## 10. 컷오버 완료 — 옛 팩토리 보관 (2026-08-30)

Template에는 DAG가 필요 없다고 정리됐다(템플릿은 실행 대상이 아니라 복제 원본이다).
DW·DZ가 이미 워크플로우로 넘어간 상태라 `nifi_pipelines_dynamic.py`가 만들 DAG가
하나도 남지 않았고, 그래서 P6 ④⑤를 마무리했다.

- `airflow/dags/nifi_pipelines_dynamic.py` → `airflow/dags_archive/`로 이동(지우지 않았다).
  되돌리려면 파일 하나를 `airflow/dags/`로 옮기면 된다. 보관 폴더에 README를 뒀다.
- 이 파일만 쓰던 Variable 2개 삭제: `nifi_process_groups_cache`, `nifi_root_connections_cache`.
  없으면 NiFi에서 다시 만들어지는 캐시라 되돌림에 지장이 없다.

### 결과

활성 DAG (`is_stale = false`)

| DAG | 정체 |
|---|---|
| `etl_wf_dw` | DW 일배치 (job 5) |
| `etl_wf_dz` | DZ 일배치 (job 5) |
| `kafka_pipeline_2_control`, `kafka_pipeline_3_control` | CDC |
| `test_hello`, `test_pipeline_api_read` | 테스트 |

실행 현황 업무별 트리

```
CDC
  CDC_aaa_tb_imp018m
  CDC_aaa_tb_imp060l
ETL
  ETL Root (2)
    DW (1) → DW 일배치
    DZ (1) → DZ 일배치
```

### stale DAG는 지우지 않는다

`nifi_pipeline_*` 8개는 `is_stale = true`로 남는다. **돌지 않고, 화면에도 안 나온다.**
카탈로그 동기화가 stale DAG를 후보에서 빼고 기존 행을 `enabled = FALSE`로 내리며,
화면은 `enabled = TRUE`만 읽기 때문이다([AirflowDagCatalogSyncService:36,64](../web/backend/src/main/java/com/company/pipeline/airflowdashboard/AirflowDagCatalogSyncService.java#L36)).

`airflow dags delete`는 dag_run·task_instance까지 지우므로 실행 이력이 사라진다.
남겨두는 쪽이 비용이 없고 이력도 지킨다.

### 연결 없는 job 5개는 병렬로 돈다

옛 DAG 동작(그룹 전체를 켜고 잡들이 동시에 돈다)과 같다. 생성된 DAG의 의존 관계:

```
dw_com_com001m ─┐
dw_com_com002l ─┤
dw_com_com003m ─┼→ workflow_done  (trigger_rule=all_success)
dw_com_com004m ─┤
dw_pop_pop003l ─┘
```

달라진 점은 job별로 성패가 따로 보이고, 실패한 job만 골라 후행까지 재실행할 수 있다는 것이다.
실제 선후 관계가 있다면 이제 캔버스에서 그리면 된다.

---

## 11. 워크플로우를 하위 그룹 단위로 + 실행 현황 개편 (2026-08-31)

### 11-1. 워크플로우 단위를 한 단계 내렸다

DW·DZ 그룹 하나에 워크플로우 하나였던 것을 잎 그룹 단위로 바꿨다.

| 그룹 | 워크플로우 | DAG | job |
|---|---|---|---|
| DW_COM | dw_com_daily | `etl_wf_dw_com_daily` | 4 |
| DW_POP | dw_pop_daily | `etl_wf_dw_pop_daily` | 1 |
| DZ_COM | dz_com_daily | `etl_wf_dz_com_daily` | 4 |
| DZ_POP | dz_pop_daily | `etl_wf_dz_pop_daily` | 1 |

키는 서버가 만든다(그룹 이름이 이름 앞에 이미 있으면 겹쳐 붙이지 않는다).

### 11-2. 실행 현황 = Informatica의 모니터링 계층

**상단 카드**
- 숫자는 모두 **당일 누적**이다. 예전에는 실패만 당일 기준이고 대기는 "지금 큐에 있는 DAG 수"라
  기준이 섞여 있었다. "실행 중"만 성격상 현재 값이다.
- **신규**를 더했다 — 오늘 카탈로그에 처음 잡힌 DAG 수. CDC는 대기 오른쪽, ETL은 실패 오른쪽.
- 숫자를 누르면 가운데 목록이 그 대상으로 좁혀진다.
- **조치 필요 버튼은 없앴다.** 경보는 이제 속성창의 «알림 규칙»에서 본다.

**좌측 트리**
- CDC 가지를 CDC 관리 화면과 같은 계층으로 바꿨다: 전체 파이프라인 → 연결 → 스키마 → 파이프라인.
  예전에는 `kafka_pipeline_N_control`을 평평하게 늘어놓아 어느 원천인지 알 수 없었다.
- ETL 가지는 NiFi 그룹 계층 → 워크플로우.

**가운데 목록** — 트리에서 무엇을 골랐는지에 따라 성격이 달라진다.
- CDC: 이름 · 유형 · 소스→타깃 · Topic · 상태 · 지연 · 마지막 처리 (CDC 관리 화면과 같은 열)
- ETL: 워크플로우 · 시작 시간 · 종료 시간 · 소요 시간 · 상태
- ETL DAG를 고르면 그 아래에 task 표가 붙는다

**우측 속성창**
- CDC 파이프라인: CDC 관리 화면 상세의 «기본정보» 그대로
- ETL 워크플로우: 이름 · DAG ID · 업무 구분/폴더 · 스케줄 · 상태 · 설명
- 버튼은 «실행 설정» + «실행 이력» 둘. **스케줄 현황은 뺐다**(스케줄은 설계 화면에서 정한다).
- **«이상 감지 설정»을 없애고 «알림 규칙»으로 대체했다.** DAG마다 따로 감지 설정을 두면
  알림 규칙과 판정 기준이 두 벌이 되어 어느 쪽이 실제로 울리는지 알 수 없었다.
  이제 이 job/파이프라인을 감시 범위에 넣은 규칙을 읽어서 보여준다.
- 연결 리소스는 그대로 뒀다.

**실행 이력**은 버튼을 눌러 창으로 연다. 기본이 당일이고, 기간을 직접 넓힐 수 있다
(오늘 / 최근 7일 / 최근 30일 / 직접 선택).

**task 재실행 두 가지**
- «이 작업만 실행» — `include_downstream: false`. 값 하나만 고쳐 다시 넣을 때
- «이 작업부터 실행» — `include_downstream: true`. 중간이 막혀 뒤까지 다시 돌려야 할 때

### 11-3. 새로 만든 것

- `GET /api/admin/alert-rules/watching?target=CDC|ETL&ids=…`
  — 이 대상을 감시 범위에 넣은 규칙. `AlertEngine.ScopeFilter`와 같은 판정을 쓴다
  (ALL / INCLUDE / EXCLUDE, `idKind=CHAIN`이면 `etl_job_step.id`로 비교).
- `src/utils/cdcPresentation.tsx` — 상태·지연·목록 열을 CDC 관리 화면과 공유한다.
  각자 그리면 "화면마다 상태 표기가 다르다"가 된다.
- `airflow_dag_catalog.created_at`을 응답에 실었다("오늘 신규" 판정 근거).

### 11-4. 브라우저 확인

| 항목 | 결과 |
|---|---|
| 상단 카드 | CDC 전체2/실행중0/실패0/대기0/신규0, ETL 전체4/…/신규4 |
| 지표 클릭 | 가운데 제목이 "CDC · 전체 작업"으로 바뀌고 목록이 좁혀짐 |
| CDC 트리 | 전체 파이프라인 → oracle-source → CSB → 파이프라인 2건 |
| CDC 목록 | 이름·유형·소스→타깃·Topic·상태·지연·마지막 처리 |
| CDC 속성 | 기본정보 전 항목 + 알림 규칙 3건 + 연결 리소스 |
| ETL 그룹 클릭 | "DW 워크플로우", 워크플로우·시작·종료·소요·상태 2행 |
| ETL 속성 | 기본정보 + 알림 규칙 4건(CHAIN 범위 규칙 포함) + 연결 리소스 |
| 실행 이력 | 기간 선택 + 오늘/7일/30일 |
| JS 오류 | 없음 |

**아직 못 본 것**: task 표와 재실행 버튼은 DAG 실행 기록이 있어야 내용이 찬다. 새 워크플로우
4개는 아직 한 번도 돌리지 않아 "실행 단계가 없습니다"로 나온다. 표 자체와 버튼은 렌더링되고,
«이 작업부터 실행»(`include_downstream: true`)은 이전 회차에서 실제 실행으로 확인한 경로다.

---

## 12. 실행 검증 — 결함 4건 발견·수정 (2026-08-31)

워크플로우를 실제로 돌려봤다. **첫 실행이 성공으로 끝났는데 실제로는 아무것도 적재되지
않았다.** 거기서 시작해 결함 4개를 찾아 고쳤다.

### 무엇이 잘못됐었나

NiFi 로그:
```
!! ERROR extract-dz-POP003L: ExecuteSQL ... Unable to execute SQL select query
   [SELECT * FROM dz_pop003l] ... Cannot create PoolableConnectionFactory
   Caused by: java.net.SocketTimeoutException: Connect timed out
[POP003L] 완료(콜백 확정)
[POP003L] 이번 실행 적재 건수: 없음
```
그런데 Airflow는 6/6 성공, DAG도 success, 원장도 SUCCESS였다.

| # | 결함 | 수정 |
|---|---|---|
| D1 | `await`의 «콜백 확정» 경로가 `fail_if_errors`를 건너뛴다. ERROR bulletin을 보고도 성공 처리 | 콜백 경로에도 같은 검사를 넣었다 |
| D2 | `_callback_state`가 «누가 닫았는지»를 구분하지 않는다. 유휴 정리(`closeIdleRuns`)가 닫은 행을 «콜백 확정»으로 신뢰 | `completion_source`가 있는 마감만 확정으로 본다 |
| D3 | `… >> stop_pg >> verify` 배치 탓에 `await` 실패가 세탁된다. `all_done`인 `stop_pg`가 성공하면서 뒤가 정상으로 이어지고 `workflow_done`까지 성공 → **DAG 전체 success, asset 이벤트까지 발행** | 정지를 곁가지로 뺐다: `await >> verify`, `await >> stop_pg` |
| D4 | 실패해도 원장에 실패가 안 남는다. 유휴 정리가 먼저 SUCCESS로 닫고, 뒤늦은 실패 보고는 `completeBy`의 «이미 닫힘» 가드에 막힌다 | 실패 보고 경로(`observe-failed`)를 만들고, 판정이 없는 마감(`completion_source` 없음) 위에는 실제 판정을 덮어쓰게 했다 |

D3은 특히 위험했다. 실패한 워크플로우가 완료 신호를 내보내 **후행 워크플로우가 잘못된
데이터 위에서 돌기 시작한다.**

### 수정 후 (같은 조건으로 재실행)

```
dw_pop_pop003l.open_run   success
dw_pop_pop003l.start_pg   success
dw_pop_pop003l.await      failed        <- ERROR bulletin 감지
dw_pop_pop003l.stop_pg    success       <- 정리는 그대로 돈다
dw_pop_pop003l.verify     upstream_failed
workflow_done             upstream_failed  <- asset 이벤트 없음
dag_run                   failed
```
원장: `status=FAILED`, `completion_source=OBSERVED`,
`error_message="[POP003L] 그룹은 유휴가 됐지만 처리 중 ERROR가 1건 발생했습니다 … extract-dz-POP003L: ExecuteSQL…"`

### 화면 검증

| 항목 | 결과 |
|---|---|
| 상단 카드 | ETL 전체4 / 실행중0 / 오늘성공3 / 실패3 / 신규4 — 당일 누적 반영 |
| task 표 | 6개 task, `await` 실패 + 사유 `SocketTimeoutException: Connect timed out` |
| 선행 실패 표기 | `verify`·`workflow_done`이 "선행 작업 실패로 미실행" |
| 이 작업만 실행 | `await` try_number 1→2, 후행은 그대로 `upstream_failed` |
| 이 작업부터 실행 | `await` try 2→3, `stop_pg`·`verify`·`workflow_done`까지 초기화 |
| 실행 이력 | 5건, 기간(오늘/7일/30일) 전환 동작 |
| 4-job 병렬 | `dz_com_daily` 4개 job이 동시에 시작해 각각 독립적으로 실패 |
| JS 오류 | 없음 |

### 환경 조건 — 데이터는 실제로 이동하지 못했다

NiFi가 붙는 DB가 이 네트워크에서 응답하지 않는다.

```
cdc-2-oracle-source   jdbc:oracle:thin:@119.204.108.98:11521/XEPDB1
cdc-5-tarantula       jdbc:postgresql://119.204.108.98:7432/postgres
target-postgreDB      jdbc:postgresql://data-world.net:15432/portal
cdc-3-target-db       jdbc:postgresql://target-db:5432/tarantula   (poc 프로파일, 미기동)
```
`Connect timed out` — 라우팅은 되는데 응답이 없다(방화벽/VPN). 2026-08-24에는 같은 잡이
성공했고 1,660,590행이 적재됐으므로 설정이 아니라 **지금의 망 조건** 문제다.

또한 Airflow의 `target_db_postgres` 연결이 `target-db` 호스트를 가리키는데 그 컨테이너가
`poc` 프로파일이라 떠 있지 않다. `verify_landing`은 예외를 삼키도록 되어 있어 조용히
건너뛴다 — 설계된 동작이지만, **이 환경에서는 적재 검증이 사실상 꺼져 있는 셈**이다.

**따라서 성공 경로(실제 적재·건수 확인)는 아직 검증하지 못했다.** DB가 닿는 순간
같은 워크플로우를 한 번 돌리면 바로 확인된다.

---

## 13. 실행 현황 후속 보완 (2026-08-31)

| # | 지적 | 원인 | 수정 |
|---|---|---|---|
| 1 | 화면에 들어가면 엉뚱한 DAG 하나가 떠 있다 | 초기 선택이 없어 «선택 작업» 상태로 시작 | 처음엔 **CDC 전체 파이프라인**을 보여준다(URL로 dagId가 들어오면 그 DAG) |
| 2 | 그룹을 접어도 DAG가 그룹 밖에 보인다 | `placedDagIds`를 «그려진 노드»에서만 모아서, 접힌 가지의 DAG가 «미배치»로 남아 트리 맨 아래 고아 목록에 떴다 | 접기와 무관하게 원본 트리 전체를 훑어 배치 목록을 먼저 만든다. 접힌 그룹의 DAG도 감춘다 |
| 3 | «알림 규칙 관리»가 엉뚱한 데로 간다 | `/settings/alerts` — 없는 경로 | `/settings?tab=rules` (설정 화면의 «알림 규칙» 탭) |
| 4 | 스텝별 task가 21줄 | 태스크 인스턴스를 그대로 나열 | **job 1개 = 1줄**로 접었다. 줄을 누르면 단계(open_run/start_pg/await/stop_pg/verify)가 펼쳐진다. 재실행도 job 통째로 |
| 5 | 실패 사유가 `https://docs.oracle.com/error-help/db/ora-12170/` | 사유 추출이 대소문자 무시로 URL 속 `ora-12170`을 원인 코드로 착각했고, 역순 탐색이라 URL 줄이 걸렸다 | URL 줄은 제외하고, 원인 코드(`ORA-12170:`)를 만나면 **그 지점부터** 잘라 앞에 오게 한다 |

5번은 로그가 이상했던 게 아니다. 실제 로그는
`ORA-12170: Cannot connect. TCP connect timeout of 20000ms for host 192.168.50.91 port 1521.`
이고, 화면이 같은 줄에서 잘못된 조각을 골랐던 것이다. Airflow 로그가 JSON 한 줄에 메시지
전체를 담아서 줄 단위로 고르면 원인이 뒤로 밀린다.

### 확인

| 항목 | 결과 |
|---|---|
| 초기 화면 | "전체 파이프라인" + CDC 열 |
| DW 접기 | 트리에 `DW (2)`만, `dw_*_daily`가 밖으로 새지 않음 |
| 알림 규칙 관리 | `/settings?tab=rules`, 활성 탭 «알림 규칙» |
| task 표 | 21줄 → **5줄**(job 4 + 워크플로우 종단), 펼치면 10줄 |
| 실패 사유 | `ORA-12170: Cannot connect. TCP connect timeout of 20000ms for host 192.168.50.91 port 1521.` |
| JS 오류 | 없음 |

`workflow_done`은 job이 아니라 워크플로우 종단 신호라 «워크플로우 종단»으로 따로 표기하고
펼치기·재실행 버튼을 주지 않는다.

---

## 14. 실행 현황 3차 보완 (2026-08-31)

### 확인 요청 두 가지

**하단 task 표는 «마지막 실행»이 맞다.** `groupRuns`가 DAG별 실행을 시각 역순으로 정렬하고
화면은 그 첫 항목을 쓴다([AirflowDashboardPage.tsx:153-165](../web/cerebroetl-ui/src/pages/AirflowDashboardPage.tsx#L153-L165)).

**상단 숫자는 DAG 기준이 아니었다.** 성공·실패·대기가 «실행 건수»였다. 한 작업이 오늘 세 번
실패하면 «실패 3»으로 세면서, 바로 옆 «전체 작업»은 DAG 수라 단위가 섞여 있었다.
«실행 중»은 날짜 필터도 없었다.

### 바꾼 것

| 지적 | 수정 |
|---|---|
| 같은 DAG를 다시 눌러도 이력이 안 접힘 | DAG 클릭을 토글로. 같은 것을 다시 누르면 접힌다 |
| 다른 DAG·그룹·지표를 골라도 이전 이력이 남음 | DAG를 고른 게 아닌 모든 선택에서 이력을 닫는다 |
| «선택 해제» 버튼 | 삭제했다. 초기 범위가 항상 있어서(«CDC 전체») 해제하면 오히려 아무것도 안 고른 어정쩡한 상태가 된다 |
| 상단 숫자 단위 | **당일 · 작업(DAG) 기준**으로 통일. 각 DAG의 «오늘 마지막 실행» 상태로 한 번씩만 센다. «실행 중»만 날짜 무관(지금 도는 작업) |
| «오늘 성공» | → «성공». 카드 전체가 당일 기준이라 한 항목에만 «오늘»이 붙을 이유가 없다 |
| «연결 리소스» / «DAG 보기» | → «바로가기» / «상세보기» (CDC·ETL 속성창 둘 다) |

지표를 누르면 가운데 목록도 같은 기준(오늘 마지막 실행)으로 좁혀진다. 카드 숫자와 목록
건수가 어긋나지 않게 맞췄다.

### 확인

| 항목 | 결과 |
|---|---|
| DAG 클릭 → 재클릭 → 재클릭 | task 5행 → 0행 → 5행 |
| 다른 그룹 선택 | 0행 (접힘) |
| 지표 클릭 | 0행 (접힘), 제목 «ETL · 실패» |
| 선택 해제 버튼 | 없음 |
| 상단 카드 | ETL 전체4 · 실행중0 · **성공0 · 실패2** · 신규4 (이전 run 기준 성공3/실패3 → DAG 기준으로 정정) |
| 속성창 | «바로가기 / 상세보기» — CDC·ETL 둘 다 |
| JS 오류 | 없음 |

---

## 15. 알림 규칙 «사용여부» 토글 고장 (2026-08-31)

### 증상

목록의 «사용» 열 스위치를 눌러도 아무 일이 없다. 오류도 안 뜬다.

### 원인

`PUT /api/admin/alert-rules/{id}` 가 400을 냈고, 그 원인이 백엔드에서 삼켜지고 있었다.
로그를 남겨 보니 실제 오류는

```
ERROR: could not determine data type of parameter $10
```

수정 SQL이 부분 수정을 지원하려고 전부 `COALESCE(?, 컬럼)` / `CASE WHEN ? IS NULL …` 로
돼 있는데, **`CASE WHEN ? IS NULL` 의 맨 물음표에는 캐스팅이 없었다.** 값을 보내는 경로
(설정 모달)는 항상 시각을 채워 보내서 Postgres가 타입을 추론할 수 있었지만, 토글은
`{"enabled": false}` 하나만 보내므로 그 파라미터가 «타입 없는 NULL»이 되어 문장 전체가 실패했다.

즉 **점검 스케줄 기능이 들어온 뒤로 부분 수정이 전부 깨져 있었다.**

### 수정

- `CASE WHEN ?::time IS NULL …` 로 네 군데 모두 캐스팅을 붙였다.
- 실패 원인을 `log.warn`으로 남긴다. 삼키면 화면에는 «값이 올바르지 않습니다»만 남는다.
- 화면의 `toggle()`에 try/catch를 넣었다. 예전에는 요청이 실패해도 스위치만 슬쩍
  되돌아가고 사용자는 «왜 안 되지»만 남았다.

### 함께 점검한 것

| 항목 | 결과 |
|---|---|
| 사용여부 토글 끄기/켜기 | PUT 200, «비활성화» 메시지, 스위치 상태 반영 |
| 부분 수정이 스케줄을 건드리지 않는지 | 규칙 11: `enabled`만 바뀌고 `schedule_enabled/03:15`는 그대로 |
| 사용여부 필터 | 사용 6 / 미사용 5 / 전체 11(페이지당 10) — **정상이었다** |
| 설정 모달 | 열림 + 저장 PUT 200 |
| 수신자 모달 | 열림, 현재 1명 |
| 로그(평가 이력) 모달 | 열림 |
| JS 오류 | 없음 |

«사용여부 필터가 안 된다»고 보인 것은 필터 문제가 아니라 이 토글 실패였다. 필터 자체는
정상이고, «전체»에서 11건 중 10건만 보이는 것은 페이지당 10건이라 그렇다.

---

## 16. CDC DAG 팩토리를 Variable 기반으로 (2026-08-31)

### 배경 — 왜 CDC DAG가 «실패»로 남아 있었나

CDC 파이프라인은 계속 RUNNING인데 DAG는 8-24부터 «실패»였다. 원인은 CDC가 아니라 감시 장치다.

`monitor_cdc_runtime`은 30초마다 깨어나는 reschedule 센서다. 06:10에 깨어나려는 순간
`pipeline-api`가 죽어 있었고, DAG 파일이 파싱 때 백엔드를 직접 호출하는 구조라
**그 파싱 주기에 CDC DAG가 통째로 사라졌다.**

```
pipeline-api 조회 실패, 이번 파싱 주기엔 Kafka 파이프라인 DAG를 생성하지 않음
Dag not found during start up
Startup reschedule limit exceeded
```

poke 안에는 "일시적 재시작을 장애로 오판하지 않는다"는 방어가 이미 있었지만,
이 실패는 poke 밖(파싱·기동 단계)이라 그 방어가 닿지 않았다.

### 고친 것

**1. 팩토리가 Variable만 읽는다** — ETL 워크플로우 팩토리와 같은 방식.

- `CdcDagSpecPublisher`(신규): `pipeline_definition`을 읽어 `cdc_pipeline_index` /
  `cdc_pipeline_spec__{id}`에 내보낸다. 60초 주기 + 내용이 같으면 쓰지 않는다.
  변경 지점마다 갱신을 심지 않고 주기 동기화로 맞춘다 — 생성·수정·삭제가 여러 경로에
  흩어져 있어 한 곳만 빠져도 조용히 어긋나는데, 주기 방식은 스스로 회복한다.
- `kafka_pipelines_dynamic.py`: 최상위 `requests.get(/api/pipelines)` 제거, Variable 읽기로 교체.

**2. `monitor` 액션 추가** — 커넥터를 건드리지 않고 감시만 다시 붙인다.

감시 Run을 잃으면 되살릴 방법이 `stop → start` 뿐이었다. 그런데 `start`는 멱등이 아니다 —
TABLE_CDC는 `startTableCdc`를 타고, 이미 DEPLOYED면 **400으로 거부**한다
([PipelineDeployService:323-329](../web/backend/src/main/java/com/company/pipeline/pipeline/PipelineDeployService.java#L323-L329)).
그래서 Sink를 한 번 멈췄다 켜야만 했다. `monitor`는 `apply_action`·`verify_action`을
건너뛰고 센서만 붙인다(적재 검증은 원래 start에서만 돌아 자동 skip).
실행 설정 모달에 «감시 재개 (Monitor)»로 노출했다.

### 검증

| 항목 | 결과 |
|---|---|
| Variable 게시 | `cdc_pipeline_index`, `cdc_pipeline_spec__2/3` |
| **백엔드를 내린 채 파싱** | 18:17:47 파싱, `is_stale = false` — **DAG가 살아남았다** (예전엔 여기서 사라졌다) |
| monitor 트리거 | 두 DAG 모두 `running`, `monitor_cdc_runtime` = `up_for_reschedule` |
| 커넥터 영향 | 없음 — Source/Sink 모두 `RUNNING`, status `DEPLOYED` 유지 |
| 화면 | CDC 카드 «실행 중 2 · 실패 0», 목록 상태 «실행 중» |
| 실행 설정 모달 | 배포 / 시작 / 중지 / **감시 재개** |

### 남는 참고

CDC DAG는 `running`이 정상 상태다. `monitor_cdc_runtime`은 타임아웃 10년이고 파이프라인이
DEPLOYED인 동안 계속 대기한다. `success`는 «CDC가 정상 중지됐다»는 뜻이지 «잘 돌고 있다»가 아니다.

---

## 17. 사내망 성공 경로 검증 (2026-08-31)

원천/타깃 DB가 닿는 망에서 미검증으로 남겨뒀던 «성공 경로»를 돌렸다. 결함 3건이 더 나왔다.

### 실행 결과 — 데이터가 실제로 옮겨졌다

| 워크플로우 | 결과 | 적재 |
|---|---|---|
| `dw_pop_daily` (job 1) | 성공 | POP003L **1,660,590행** |
| `dz_com_daily` (job 4) | 성공 | COM001M 43 · COM002L 4,598 · COM003M 99,594 · COM004M 0행 |

DAG run·태스크 전부 success, `workflow_done`까지 도달(= asset 이벤트 발행).

### D5 — 적재 건수가 0으로 기록됐다 (첫 실행)

첫 `dz_com_daily`는 성공했는데 원장에 `rows_processed = 0`이었다. 로그를 보니

```
[COM003M] queued=0 activeThreads=0 idle=13/3 activity=False
[COM003M] 완료(유예시간 내 활동 없음)
[COM003M] 이번 실행 적재 건수: 없음
```

`activity=False`인데 `total_inserted`에는 99,594행. 태스크 시각을 보니 원인이 나왔다.

```
start_pg  00:06:33 ~ 00:06:37
await     00:09:44 ~ 00:12:50     <- start_pg보다 3분 7초 늦게 시작
```

`AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=2`(WSL2 메모리 때문에 일부러 낮춰 둔 값)라
job 4개 중 3·4번의 `await`가 슬롯을 3분 기다렸고, **그 사이 NiFi가 일을 다 끝냈다.**
기준선(적재 카운터)을 await 시작 시점에 뜨고 있었으니 그 전 적재는 통째로 안 보였다.

**수정**: 기준선을 `start_pg`가 «그룹을 켜기 직전»에 떠서 XCom으로 넘긴다.
스케줄이 얼마나 밀리든 «이번 실행이 옮긴 양»이 정확해진다. 재실행 결과
`rows_processed`가 `total_inserted`와 정확히 일치했다(43 / 4,598 / 99,594 / 0).

### D6 — CDC 팩토리 Variable 전환의 부작용

Variable 기반으로 바꾼 뒤 CDC 감시 태스크가 죽었다.

```
Access denied for variable 'cdc_pipeline_spec__3' by Execution API;
refusing to fall back to a less-restrictive secrets backend.
```

Airflow 3은 태스크 실행 컨텍스트에서 그 태스크와 무관한 Variable 접근을 막는다.
파이프라인 2의 태스크가 파일을 다시 파싱하면서 «남의» spec(3번)을 읽다 걸렸다.

**수정**: spec 읽기를 파이프라인 단위로 try/except 격리. 남의 spec을 못 읽으면 그것만
건너뛰고 자기 DAG는 계속 만든다. 두 CDC 감시 모두 `up_for_reschedule`로 복구됐다.

### D7 — 화면의 적재 건수가 늘 «0건»

로그는 «건»이 아니라 **«행»**으로 찍는데(`이번 실행 적재 건수 (총 99,594행)`) 화면
정규식이 `([\d,]+)\s*건`을 찾고 있었다. 게다가 job 줄은 마지막 단계(verify/stop_pg)
로그를 보고 있어서 건수가 있는 `await` 로그를 읽지도 않았다.

**수정**: `await` 단계 로그에서 `총 N행` 합계를 읽는다. 뒤따르는 프로세서별 줄도 같은
모양이라 «마지막»이 아니라 «총계»를 집는다. 화면에 `43행 / 4,598행 / 99,594행 / 0행` 표시.

### 남은 설정 사항 — 적재 검증이 여전히 꺼져 있다 (→ M2-12로 기입)

`verify_landing`이 쓰는 Airflow 연결 `target_db_postgres`가 `target-db`(포c 프로파일
컨테이너, 이 환경엔 없음)를 가리킨다. 그래서 «타겟 테이블 총 N행»을 못 찍고 조용히 건너뛴다.

- `docker-compose.yml`의 하드코딩 `"host": "target-db"`를 `"${TARGET_DB_HOST}"`로 바꿨다.
- 다만 `.env`의 `TARGET_DB_HOST`가 `target-db`이고, 그 자격증명(`tarantula_app`)은
  NiFi가 실제로 적재하는 DB와 다르다(`data-world.net:15432`에 그 계정으로 붙지 않는다).
- **NiFi의 load 프로세서가 쓰는 DB에 맞춰 `.env`의 `TARGET_DB_*`를 채워야** 적재 검증이 켜진다.

### COM004M이 0행인 점

`truncate → extract → load` 구성인데 두 번 다 0행이다. 원천에 데이터가 없으면 정상이지만,
truncate가 이미 돌았으므로 타깃 테이블은 비워진 상태다. 위 적재 검증이 켜지면
«타겟 테이블 총 N행»이 로그에 남아 바로 판별된다.

### 추적 — 검증 결함은 백로그에 기입했다

이 건은 «적재는 정상 동작, 검증만 불능»이라 즉시 조치하지 않고
[통합검증 문서](2026-08-20-integration-verification.md)의 **M2-12**로 남겼다.
핵심은 셋이다.

1. `verify_landing`이 **고정 연결 하나**(`target_db_postgres`)로 모든 job을 조회한다 —
   실제로는 타깃 DB가 이미 2종(`192.168.50.12:7432/postgres`, `data-world.net:15432/portal`)
2. 그 하나마저 실제 적재처가 아니다(`target-db`, 미기동 poc 컨테이너)
3. 예외를 삼켜서 **«검증 정상»과 «검증 못 함»이 구분되지 않는다** — 지금까지 매 실행 조용히 skip

필요한 정보(`etl_job_step.dbcp_service_id`)는 이미 미러링돼 있고 spec·검증에서 안 쓰일 뿐이다.
방향은 M2-12의 «수정 제안» 참고 — 최소 조치(거짓 안심 제거) 후 NiFi 쪽 검증으로 옮기는 순서를 권한다.
