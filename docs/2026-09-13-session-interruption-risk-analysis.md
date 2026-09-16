# 작업 도중 세션·네트워크가 끊길 때의 위험 분석 — 워크플로우 · ETL · CDC

웹서비스로 제공하는 상황에서, 사용자가 작업하는 도중 세션이 끊기거나(로그인 만료) 네트워크·브라우저가
끊겼을 때 무엇이 남는지 코드로 분석한 문서다. **분석만 했고 아무것도 수정하지 않았다.**
실제로 끊어 보는 재현은 하지 않았다 - 코드 근거로 판단했고, 확인하지 못한 것은 문서 끝에 따로 적었다.

경로 약칭: `BE` = `web/backend/src/main/java/com/company/pipeline`, `UI` = `web/cerebroetl-ui/src`

---

## 1. 결론

- **서버에 이미 도착한 요청은 브라우저가 끊겨도 끝까지 실행된다**(동기식 Spring MVC).
  문제는 **화면이 결과를 못 받아 «실패»로 보고 사용자가 다시 시도할 때** 생긴다.
- 요청 하나가 **여러 외부 시스템(NiFi · Kafka Connect · Airflow)을 차례로 호출하는 곳**에는 롤백이 없다.
  중간에 실패하면 반쯤 만들어진 자원이 남는다.
- **클라이언트가 요청을 여러 개 이어 보내는 곳**(워크플로우 초안 생성, 재실행)은 중간에 끊기면 반쪽 상태가 남는다.
- **낙관적 락(버전 확인)이 백엔드 전체에 없다**(`@Version` 0건). 같은 대상을 동시에 고치면 나중 저장이 이긴다.
- **멱등키·이름 중복 사전 확인이 없는 생성 API**가 있어, 재시도하면 중복이 생긴다.

---

## 2. 공통 — 로그인·세션

### 2.1 토큰

| 항목 | 값 | 근거 |
|---|---|---|
| 앱 JWT 수명 | 30일(`2592000000` ms, 하드코딩) | `application.yml:70` |
| 프록시 쿠키 `cetl_session` | 30일 | `application.yml:95`, `AuthController.java:260-270` |
| Airflow API JWT | 30일 | `docker-compose.yml:589` |
| refresh token · 슬라이딩 갱신 · idle timeout | 없음 | `SecurityConfig.java:145`(STATELESS) |
| 만료 전 경고 | 없음 | |
| 브라우저 저장 위치 | localStorage `cerebro_token`, `cerebro_user` | `UI/auth/session.ts:5-11` |

토큰 수명이 길어 **실제로는 «만료»보다 네트워크 끊김·응답 지연이 주된 원인**이다.

### 2.2 401을 받았을 때

```ts
// UI/api/client.ts:41-45
if (error?.response?.status === 401) {
  clearSession();
  if (window.location.pathname !== "/login") window.location.assign("/login");
}
```

- 안내 없이 **전체 새로고침으로 로그인 화면**에 간다. 원래 경로를 기억하지 않아 로그인 뒤 **대시보드**로 간다(`LoginPage.tsx:30-32`).
- React 메모리에 있던 입력은 모두 사라진다.
- `apiClient` 를 거치지 않는 요청(`/airflow/api/v2`, `/nifi-api`, `/kafka-connect-api` - `api/platform.ts`)은
  이 처리를 받지 않는다. 401이어도 그 조회가 에러가 될 뿐이다.
- NiFi·Airflow iframe 은 준비 확인을 `status < 500` 으로 해서 **401 페이지가 iframe 안에 그대로 보일 수 있다**(`ConsoleFramePage.tsx:3249-3257`).
- 부수 발견: 비밀번호 변경에서 현재 비밀번호를 틀리면 백엔드가 401(`INVALID_CREDENTIALS`)을 내서
  **세션 만료처럼 로그아웃된다**(`ErrorCode.java:9`, `AuthController.java:292-293`, 코드 추론).
- 부수 발견: 로그아웃은 localStorage 만 지우고 HttpOnly 쿠키 `cetl_session` 은 남아,
  **로그아웃 뒤에도 NiFi·Airflow 프록시 인증이 통과될 수 있다**(`AuthContext.tsx:62-68`, 코드 추론).

### 2.3 입력 보존과 이탈 확인

| 화면 | 끊긴 뒤 입력 | 근거 |
|---|---|---|
| 워크플로우 캔버스 | **사라짐**(React state 만) | `WorkflowCanvasPage.tsx:160-161, 404` |
| CDC 생성 | **사라짐** | `CdcCreatePage.tsx:82-95` |
| ETL 생성 마법사 | 같은 탭이면 복원(sessionStorage), **첨부 파일은 사라짐** | `EtlCreatePage.tsx:185, 208-240, 2075-2079` |

- 이탈 확인창(`utils/navigationGuard.ts`)은 **워크플로우 캔버스에서 메뉴로 이동할 때만** 뜬다.
- 새로고침 · 탭 닫기(`beforeunload` 없음) · 브라우저 뒤로가기 · 401 강제 이동에서는 **확인 없이 사라진다**.

### 2.4 응답 대기 시간

- nginx `/api/` 블록에 `proxy_read_timeout` 이 없다 → **기본 60초**(`UI/../nginx.conf:26-35`).
  60초가 넘는 요청은 **서버는 성공, 화면은 504** 가 된다(CDC 생성이 해당될 수 있음 - 실측 안 함).
- 백엔드 → NiFi 읽기 타임아웃 **5초**(`BE/nifi/NifiClient.java:126`), → Airflow **10초**(`AirflowDagRunClient.java:47`, `AirflowVariableClient.java:52`).
  외부가 실제로는 처리했는데 응답만 늦으면 백엔드는 실패로 본다.

---

## 3. 워크플로우

### 3.1 작업별 구조

| 작업 | 요청 수 | 트랜잭션 |
|---|---|---|
| 초안 «저장»(생성) | 최대 3개: 생성 → 속성 → 그래프 (`WorkflowCanvasPage.tsx:439-451`) | 요청마다 따로. 묶는 트랜잭션 없음 |
| 기존 워크플로우 «저장» | 2개: 속성 → 그래프 | 요청마다 따로 |
| 그래프 저장 | 1개 | 한 트랜잭션(삭제 후 삽입) - **중간에 끊겨도 이전 그래프 유지** |
| 게시 / 게시 취소 | 1개(서버가 Airflow Variable 2번 호출) | DB 트랜잭션 안에서 외부 호출(`WorkflowPublishService.java:66-117`) |
| 삭제 | 1개 | 한 트랜잭션, 소프트 삭제 - 안전 |
| 재실행(선행 성공 처리 포함) | N+1개: 선행마다 성공 표시 → clear (`AirflowDashboardPage.tsx:917-922`) | 브라우저가 Airflow 로 직접, 트랜잭션 없음 |
| 수동 실행 | 1개, 요청마다 새 실행 생성(`AirflowDagRunClient.java:168-178`) | 멱등하지 않음 |

동적 DAG 는 API 가 아니라 Airflow Variable `etl_wf_index` · `etl_wf_spec__{key}` 를 읽는다(`airflow/dags/etl_workflows_dynamic.py:40-41`).

### 3.2 위험

| # | 상황 | 남는 상태 | 위험 |
|---|---|---|---|
| W1 | 초안 생성 중 2·3번째 요청 실패, 또는 생성 응답 유실 뒤 다시 저장 | 화면은 계속 초안이라 다시 만든다 → **그래프 없는 워크플로우 + `_2` 복사본**(`WorkflowService.java:147-150`) | 높음 |
| W2 | 서로 다른 워크플로우를 동시에 게시·게시 취소 | index 를 커밋된 DB 기준으로 **통째로 다시 계산해 덮어써서**(`WorkflowPublishService.java:88, 110, 148-165`) 한쪽이 빠짐 → «게시됨»인데 DAG 없음. 자동 복구 없음 | 높음 |
| W3 | 두 탭·두 사용자가 같은 워크플로우 편집 | 나중 저장이 앞 저장을 경고 없이 덮음(그래프·속성 모두) | 중~높음 |
| W4 | 재실행 «선행 성공 처리» 도중 끊김 | 선행이 적재 없이 success, 대상은 미실행. 다시 누르면 «실행할 수 없습니다»로 막힘(`AirflowDashboardPage.tsx:970-973`) - Airflow UI 에서 직접 clear 해야 함 | 중~높음 |
| W5 | 게시 중 Airflow 가 실제로 썼는데 응답 10초 초과, 또는 DB 커밋 실패 | Airflow 에는 DAG, DB 는 미게시 → 미게시로 보고 삭제 허용 → **유령 DAG** | 중 |
| W6 | 재게시 중 index 쓰기 실패 | Airflow 는 새 spec, DB 는 옛 spec(재게시로 수렴) | 중 |
| W7 | 수동 실행·clear 응답 유실 뒤 다시 누름 | 실행 2개 → **두 번 적재**, 또는 실행 중 작업 재시작 | 중 |
| W8 | 새로고침·뒤로가기·탭 닫기·401 | 저장 안 한 캔버스 편집 소실, 복구 수단 없음 | 중 |
| W9 | 게시 취소 중 spec 삭제 실패 | DB 는 게시 중, DAG 는 사라짐(재시도로 수렴) | 낮음 |

관련 별건: 게시 중인 상위 워크플로우가 부르는 하위 워크플로우를 **게시 취소·삭제해도 막지 않는다**
→ 상위 실행 시점에 실패(2026-09-09 total 실패가 이 형태).

---

## 4. ETL(NiFi)

### 4.1 생성 구조

- 브라우저 호출은 (파일적재면 업로드) → 생성 API 1회 → 동기화 1회(`EtlCreatePage.tsx:2262-2316`).
- 백엔드는 **Template 그룹을 snippet 으로 복제**한 뒤 이름·프로세서 속성을 고친다(`NifiClient.java:236-269`).
  파일적재 1건에 NiFi 호출 약 20회(변경 7회). **트랜잭션·보상 없음**(`NifiController.java:120-147`).
- `etl_job` 행은 생성 API 가 아니라 **5분 주기 동기화**가 NiFi 를 보고 만든다(`NifiJobMirrorService.java:89-98, 182-186`).
  동기화는 NiFi → DB 단방향이라 **반쯤 된 그룹도 정상 job 으로 등록**한다.
- `etl_job` 유니크는 `nifi_pg_id` 뿐, `job_name` 은 중복 허용(`V17__create_etl_job_catalog.sql:47`). 생성 전 이름 중복 확인 없음.

### 4.2 위험

| # | 상황 | 남는 상태 | 위험 |
|---|---|---|---|
| E1 | 생성 요청 중 브라우저 끊김 → 실패로 보고 재시도 | 같은 이름 NiFi 그룹 2개 + `etl_job` 2행. 파일적재는 같은 입력 폴더를 봐 **중복 적재** | 높음 |
| E2 | 복제 뒤 이름·속성 수정 도중 실패, 또는 NiFi 응답 5초 초과 | 이름이 템플릿명(`File`/`Initial`…)이거나 설정이 반쯤 된 그룹 → 동기화가 job 으로 등록 | 높음 |
| E3 | 파일 업로드 뒤 생성 실패 → 다른 파일로 재시도 | 이전 파일이 `/opt/etl_repo/file/{userId}_{jobName}` 에 남아 **함께 적재**(`NifiController.java:149-214`) | 높음 |
| E4 | 같은 부모 그룹에서 동시 생성 + 응답에 새 그룹 id 없음 | «새로 생긴 자식»을 차집합으로 찾아 **남의 그룹을 수정**할 수 있음(`NifiClient.java:249-251`) | 중(드묾) |
| E5 | 관리 화면 파일 추가 도중 실패 | 일부 파일만 저장. 운영 폴더에 직접 써서 쓰는 중인 파일을 읽을 가능성(미확인) | 중 |
| E6 | 편집 중 45초 넘게 끊김 | 편집 잠금(TTL 45초, heartbeat 20초)이 넘어가지만 **잠금은 알림 문구일 뿐 NiFi 편집을 막지 않음**(`ConsoleFramePage.tsx:2398`, `NifiProcessorEditLockService.java:18`) | 중 |
| E7 | 파라미터 컨텍스트 동시 저장 | 직전에 GET 한 revision 으로 PUT 해서 낙관적 락 무력 → 나중 저장이 전체 덮음(`NifiClient.java:585-605`) | 중 |
| E8 | 카탈로그 삭제 중 Airflow DAG 삭제 실패 | NiFi 그룹 없음, DAG·카탈로그 잔존. 재시도는 NiFi 단계에서 실패해 **끝까지 못 지움**(`AirflowDagCatalogDeletionService.java:37-59`) | 중 |
| E9 | 연결 등록 중 DBCP ENABLE 실패 | DB 는 롤백, NiFi 루트에 비활성 컨트롤러 서비스 고아(`ConnectionService.java:55-80`) | 낮음~중 |
| E10 | 시작·중지 중 세션 끊김 | 영향 없음 - Airflow 가 서버에서 처리, stop 은 `all_done` | 없음 |

---

## 5. CDC

### 5.1 생성·배포·삭제 구조

- 생성: UI 호출 1회(`POST /api/pipelines` 또는 `/batch`, `CdcCreatePage.tsx:248-258`).
  서버는 정의 행을 **먼저 커밋**(CREATED)한 뒤, 트랜잭션 없이 배포를 이어서 한다
  (DEPLOYING → Source 생성·대기 → Sink 생성·대기 → READY, 실패 시 FAILED - `PipelineDeployService.java:119-150, 258-283`).
- 단건 생성은 **보상 없음**. 배치는 역순 삭제 보상이 있다(`PipelineBatchCreateService.java:24-54`).
- 시작·중지·배포는 Airflow 제어 DAG 로 지시하고, 트리거는 요청마다 새 실행을 만든다.
- 삭제는 한 트랜잭션이지만 **외부 정리 실패를 모두 무시하고** DB 행을 지운다(`PipelineService.java:213-242`).

### 5.2 위험

| # | 상황 | 남는 상태 | 위험 |
|---|---|---|---|
| C1 | 삭제 중 Kafka Connect 커넥터 삭제 실패·타임아웃(예외 무시) | DB 행 없음, **커넥터(Source 실행 중일 수 있음)·토픽이 추적 불가 고아**. «Connect 에만 있는 커넥터» 감지 없음(`DashboardController.java:70-84`) | 높음 |
| C2 | 삭제 중 PostgreSQL 복제 슬롯 DROP 실패(예외 무시) | 소스 DB 에 고아 슬롯 → WAL 누적, 디스크 고갈 위험(`PostgresReplicationCleanupService.java:68`). **현재 파이프라인은 소스가 Oracle 이라 해당 없음** | 높음(PG 소스 한정) |
| C3 | 실행 중인 파이프라인에 deploy 가 중복·재시도로 들어감 | deploy 에 상태 선행조건이 없어 기존 커넥터를 stop → READY 로 내려가 **CDC 중단**(`PipelineDeployService.java:119-129, 259-266`) | 중~높음 |
| C4 | 단건 생성 중 Sink 준비 실패 | FAILED 정의 + Source 커넥터(STOPPED) + (델타면) 타깃 DDL 잔존. **같은 이름으로 재생성 불가** | 중 |
| C5 | 커넥터 생성 요청 타임아웃 뒤 상태 대기 실패 | Connect 에는 커넥터가 있는데 `pipeline_connector` 행 없음 → **삭제로도 정리 안 됨**(`PipelineDeployService.java:268-282`) | 중 |
| C6 | 배포 중 백엔드 재기동 | DEPLOYING 영구 잔존. 상태 동기화는 DEPLOYED·STOPPED·FAILED 만 봄(`KafkaPipelineStateSynchronizer.java:78-85`) | 중 |
| C7 | 생성이 60초 초과 | 화면은 504, 서버는 완료 → 재시도하면 «이미 사용 중인 이름» 오류 | 중 |
| C8 | 삭제 커밋 전 서버 사망 | 외부 자원은 지워졌는데 DB 는 DEPLOYED 로 계속 표시(동기화가 404 를 판정 보류, `:106-113`) | 중 |
| C9 | DAG 트리거 응답 유실 뒤 재클릭 | 실행 중복. start 는 백엔드가 막아 실패 이력만 남음 | 낮음~중 |
| C10 | 배치 생성 중 서버 사망 | 보상 미실행, CREATED·READY 행 혼재 | 낮음~중 |
| C11 | 두 탭에서 같은 이름 동시 생성 | 이름 유니크 제약이 DB 에 없어 중복 행(`V2__create_pipeline_definition.sql`) | 낮음 |
| C12 | 그룹 생성·삭제·드리프트 무시 재시도 | 성공했는데 오류로 보이거나 이력 1행 더 | 낮음 |

안전한 부분: 중지는 몇 번 반복해도 결과가 같다. 그룹 생성·이름 변경·삭제는 한 트랜잭션이다.

---

## 6. 권장 조치(우선순위)

1. **생성의 중복 방지** — 워크플로우 초안 생성 · ETL 생성 · CDC 생성에 요청 고유 키(멱등키) 또는 이름 중복 사전 확인.
   실패 응답을 받았을 때 «이미 만들어졌는지» 조회해 이어가는 흐름. 워크플로우 초안은 생성 직후 id 를 받으면 바로 그 화면으로 전환.
2. **실패 시 정리** — ETL 생성 도중 실패하면 만든 그룹 삭제(또는 «정리 필요» 표시).
   CDC 생성 실패 시 만든 커넥터 제거, CDC 삭제에서 외부 정리 실패를 무시하지 말고 «정리 대기» 상태로 남겨 재시도.
3. **워크플로우 게시 목록** — 게시·게시 취소를 직렬화(DB 잠금)하거나, DB 기준으로 index 를 주기적으로 다시 맞추는 작업 추가.
4. **중간 상태 복구** — CDC DEPLOYING 에 제한 시간과 복구. deploy 에 상태 선행조건. Connect 에만 있는 고아 커넥터 감지.
5. **재실행 흐름** — «선행 성공 처리 + clear» 를 서버 한 요청으로 옮기거나, 중간 상태에서도 다시 진행할 수 있게.
6. **동시 편집** — 워크플로우·파이프라인 엔티티에 버전 컬럼(낙관적 락), 충돌 시 «다른 사용자가 먼저 저장했습니다».
7. **화면 보호** — 새로고침·탭 닫기 확인창(`beforeunload`), 401 뒤 원래 화면 복귀, 워크플로우 캔버스·CDC 생성 입력 임시 저장,
   오래 걸리는 API 의 nginx 대기 시간 상향, 비밀번호 오류를 401 이 아닌 코드로.

---

## 7. 확인하지 못한 것

- 클라이언트가 끊겨도 백엔드 요청이 끝까지 실행되는지(동기식 MVC 특성으로 판단, 실측 안 함)
- nginx 기본 60초에서 CDC 생성이 실제로 504 가 나는지
- 토큰 만료 뒤 iframe 안 NiFi·Airflow 화면의 실제 모습
- 운영(MSA) 서버의 `AUTHZ_*` 설정값(로컬 `.env` 만 확인)
- 게시 취소 시점에 돌고 있던 실행의 처리, 삭제된 CDC 제어 DAG 의 실행 중 센서 처리
- NiFi 2.x 에서 파라미터 컨텍스트 직접 PUT 허용 여부, ListFile 최소 파일 나이 설정
- 업로드 크기 제한이 환경변수로 덮어써지는지
- 비밀번호 변경 401 로그아웃, 로그아웃 뒤 쿠키 잔존(둘 다 코드 추론)
