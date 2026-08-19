# 사용자 / 권한 관리 설계서

> 작성일: 2026-08-13 (결정 반영 3차 개정)
> **대상: Cerebro ETL 솔루션 단독** — 통합웹(cerebroetl-ui + pipeline-api)과 그 아래 NiFi / Airflow / Kafka
> 근거: (1) 우리 소스·컨테이너 실측, (2) 참고 솔루션 Narae DataM(nd_suite, 192.168.50.15) **읽기 전용** 분석
> 짝 문서: [user-permission-decisions.md](user-permission-decisions.md) — 결정 이력과 남은 선택지

---

## 0. 확정된 방향

| 결정 | 선택 | 뜻 |
|---|---|---|
| **D1** 저장 위치 | **metadata-db 단독** | 계정·역할·권한·메뉴를 전부 우리 Postgres에 둔다 |
| **D2** 외부 MySQL(`mydb`) | **사용 안 함** | 연결 설정과 조회 코드를 제거한다 |
| **D3** 권한 단계 | **2단계 (읽기/쓰기)** | 저장은 3비트, 화면은 체크박스 2개. 쓰기 = 변경+실행 |
| **D4** NiFi/Airflow 차단 | **개인 계정 전환** | 공유 계정 주입을 걷어내고 사람마다 별도 신원 |
| **D5** 자체 계정 관리 | **구현** | 계정 생성·비밀번호 변경·실패 잠금 (D1에 따라 필연) |
| **D6** 초기 역할 | **2개** | `ETL 관리자`(전체 쓰기) / `ETL 조회자`(전체 읽기) |
| D7 배포 분할 | **미정** | 테이블 설계에 영향 없음 — P1 착수 가능 |

### 이 설계의 경계 — 외부 의존 0

```
┌──────────────────── Cerebro ETL 솔루션 ─────────────────────┐
│                                                             │
│   cerebroetl-ui ──▶ pipeline-api ──▶ metadata-db            │
│        │                  │          (계정·역할·권한·메뉴)  │
│        │                  │                                 │
│        ▼                  ▼                                 │
│   NiFi / Airflow / Kafka                                    │
│   (개인 계정, 백엔드가 동기화)                               │
│                                                             │
└─────────────────────────────────────────────────────────────┘
                     외부 DB 연결 없음
```

기존에 로그인 검증용으로 쓰던 외부 MySQL(`mydb.ST_USER`)은 **P1에서 계정을 이관한 뒤 연결을 끊습니다.** 이후 이 솔루션은 자기 DB만으로 완결됩니다.

### D1이 자동으로 해결한 것

- **상용 판매** — 고객사에 계정 연동 작업이 필요 없습니다. 패키지가 그대로 완성됩니다.
- **`PermissionProvider` 추상화 불필요** — 초안에 있던 "사내 모드 / 내장 모드" 스위치 계층을 삭제했습니다.
- **NiFi·Airflow 동기화 단순화** — 계정 원장이 한 곳이라 "비활성화 → 두 시스템 반영"이 한 트랜잭션에서 끝납니다.

### D1이 새로 만든 부담

- **계정 수명주기가 우리 책임**입니다. 입사자 등록·퇴사자 정지를 관리 화면에서 직접 합니다.
- 회사 계정이 정지돼도 자동으로 알지 못합니다. 관리자가 `use_yn='N'`으로 바꿔야 합니다.

---

## 1. 현재 상태 (실측)

### 1.1 로그인 — P1에서 바뀐다

```
[지금]
사용자 → POST /api/auth/login
        → AccountLookupService: SELECT ... FROM ST_USER WHERE USER_ID = ?   (외부 MySQL)
        → BCrypt 대조 → USE_YN / USE_STRT_DTTM / USE_END_DTTM 유효성
        → UserService.provisionAndGet(): metadata-db의 app_user에 없으면 자동 생성
        → PipelineJwtService.issue(): sub, userNm, email 만 담은 자체 JWT (30일)

[P1 이후]
사용자 → POST /api/auth/login
        → app_user 조회 (metadata-db)
        → BCrypt 대조 → use_yn / use_strt_dttm / use_end_dttm / locked_until 유효성
        → 실패 시 login_fail_count 증가, 5회면 locked_until 설정
        → PipelineJwtService.issue()   (변화 없음)
```

관련 파일: [AuthController.java](../web/backend/src/main/java/com/company/pipeline/user/AuthController.java), [AccountLookupService.java](../web/backend/src/main/java/com/company/pipeline/user/AccountLookupService.java)(삭제 예정), [PipelineJwtService.java](../web/backend/src/main/java/com/company/pipeline/user/security/PipelineJwtService.java)(변화 없음)

`app_user`(V7/V8)에 필요한 컬럼이 이미 거의 다 있습니다.

```sql
app_user(user_id PK, user_nm, user_pw, email, tel_no, hq_cd, position_cd,
         admin_yn, use_yn, last_login_dt, reg_dt, upd_dt,
         use_strt_dttm, use_end_dttm)
```

- `user_pw` — V7에서 `NOT NULL` BCrypt 컬럼으로 만들었다가 V8(Keycloak 도입)에서 nullable로 풀고 안 쓰게 된 컬럼. **이걸 되살립니다.**
- `admin_yn` — 값을 채우는 코드가 없는 죽은 컬럼. 6.4에서 용도를 확정합니다.
- 추가할 것은 `login_fail_count`, `locked_until` 두 개뿐입니다.

### 1.2 인가 — 사실상 없음

[SecurityConfig.java](../web/backend/src/main/java/com/company/pipeline/user/security/SecurityConfig.java):

```java
.requestMatchers("/api/auth/login").permitAll()
.requestMatchers("/api/auth/me").authenticated()
.anyRequest().permitAll()          // ← 나머지 전부 개방
```

지금은 **토큰 없이도** 파이프라인 생성/삭제/기동, NiFi 프로세스 그룹 생성이 전부 가능합니다. 의도된 임시 상태입니다(주석에 "서비스 토큰 도입 후 별도 단계"로 남아 있음).

> ⚠️ **인가를 켜는 순간 Airflow DAG의 서비스 호출이 401로 죽습니다.** 실측한 호출부는 아래 5개뿐이고, 모두 [kafka_pipelines_dynamic.py](../airflow/dags/kafka_pipelines_dynamic.py)와 [test_pipeline_api_read.py](../airflow/dags/test_pipeline_api_read.py)에 있습니다.
>
> | 호출 | 용도 |
> |---|---|
> | `GET /api/pipelines`, `GET /api/pipelines/{id}` | 파이프라인 상태 확인 |
> | `POST /api/pipelines/{id}/{deploy\|start\|pause\|stop\|restart}` | 제어 |
> | `POST /api/pipelines/{id}/metrics/snapshot` | 지표 스냅샷 |
> | `GET /api/connect/connectors/{name}/status` | 커넥터 상태 |
>
> `nifi_pipelines_dynamic.py`는 `PIPELINE_API_BASE_URL` 상수를 선언만 하고 쓰지 않으며, NiFi(`https://nifi:8443`)를 직접 호출합니다. 7.6의 서비스 계정 처리를 **인가 전환과 같은 단계에서** 해야 합니다.

### 1.3 메뉴 — 프론트 하드코딩

[AppLayout.tsx](../web/cerebroetl-ui/src/components/AppLayout.tsx)의 `NAV_ITEMS` 상수. 4개 그룹 / 8개 리프이고, **이미 엔진별로 깔끔하게 갈려 있습니다.** 이 점이 설계를 단순하게 만듭니다 — 메뉴 권한과 엔진 권한이 거의 1:1로 대응합니다.

| 그룹 | 리프 | 경로 | 실제 대상 |
|---|---|---|---|
| 대시보드 | — | `/dashboard` | 공통 |
| AirFlow | 생성/관리 | `/airflow/manage` | Airflow (iframe) |
| ETL | 생성 / 관리 / 로그 | `/etl/create`, `/etl/manage`, `/etl/logs` | NiFi (생성은 우리 화면, 관리는 iframe) |
| CDC | 파이프라인 / 연결정보 / 처리 로그 | `/cdc/pipelines`, `/cdc/connections`, `/cdc/logs` | Kafka Connect |

### 1.4 NiFi / Airflow — 공유 계정 대리 로그인

- NiFi: `nifi.security.user.login.identity.provider=single-user-provider` → **계정 1개만 로그인 가능**
- Airflow: `AIRFLOW__CORE__AUTH_MANAGER=...FabAuthManager` + `airflow users create`로 만든 관리자 1개
- nginx가 `$nifi_shared_bearer` / `$airflow_shared_cookie`를 서버사이드로 주입하고, `refresh-credentials.sh`가 crond로 30분마다 갱신
- 결과: **통합웹을 통과한 사람은 전원 NiFi/Airflow 관리자**이고, 두 시스템의 감사 로그에는 공유 계정 이름만 남습니다.

### 1.5 ★ NiFi는 이미 다중 사용자 인가가 켜져 있다

D4의 실현 가능성을 좌우하는 실측 결과입니다.

```properties
nifi.security.user.authorizer          = managed-authorizer      # ← SingleUserAuthorizer 아님
nifi.security.allow.anonymous.authentication = false
nifi.security.user.login.identity.provider   = single-user-provider   # ← 여기만 1인용
```

`authorizers.xml` 활성 구성:

```
file-user-group-provider    (FileUserGroupProvider, ./conf/users.xml)
  Initial User Identity 1 = cerebro-admin
file-access-policy-provider (FileAccessPolicyProvider, ./conf/authorizations.xml)
  Initial Admin Identity  = cerebro-admin
managed-authorizer          (StandardManagedAuthorizer)
```

현재 `users.xml`에 이미 3개 신원이 있습니다: `cerebro-admin`, `admin`, 노드 UUID 하나.

**즉 "사용자별 정책"을 담을 그릇은 이미 있고, 부족한 건 "사람마다 다르게 로그인시키는 방법" 하나뿐입니다.** 이게 7.7의 설계 근거입니다.

---

## 2. 참고 솔루션(nd_suite) 분석

> 192.168.50.15 / `pgsql-ndata`(TimescaleDB, PostgreSQL 12, 포트 **15432**) DB `postgres`, 스키마 `ndata`.
> **조회만 수행**했습니다(세션 `default_transaction_read_only = on`, 원격 서버에 파일 생성 없음).
> jar의 클래스 시그니처·MyBatis 매퍼·프론트 소스맵을 읽기만 했고, 이 문서에는 **구조와 개념만** 옮깁니다.

### 2.1 테이블 5개

```
tb_user(user_id PK, pwd, nm, email, adm_yn, cret_dt, updt_dt,
        block_time, login_fail_count, del_yn)

tb_user_role(role_nm PK, proj_access_rights smallint, tmpl_access_rights smallint, ...)

tb_user_asgn_role(user_id, role_nm) PK(user_id, role_nm)     -- N:M

tb_user_role_authority(role_nm, a_obj_id, a_obj_tp, a_obj_depth, all_subs_yn, ...)
             PK(role_nm, a_obj_id, a_obj_tp)                 -- 역할이 만질 수 있는 "객체"

tb_object(obj_id PK, p_obj_id, obj_tp)                       -- 프로젝트/태스크 공통 트리
```

> **이 솔루션도 계정을 자기 DB 안에 갖고 있습니다**(`tb_user.pwd` — `1000:<salt>:<hash>` 형태의 PBKDF2, 1000회). D1 이후 우리 구조와 같은 모양이 됩니다.

### 2.2 권한 = 비트마스크 ★ 우리가 채택한 부분

`RoleVo` 상수와 실제 데이터로 확인:

| 비트 | 값 | 의미 |
|---|---|---|
| `CAN_VIEW` | 1 | 보기 |
| `CAN_MANIPULATE` | 2 | 변경 |
| `CAN_EXECUTE` | 4 | 실행 |
| `CAN_ALL` | 7 | 전부 |

판정은 한 줄입니다: `isAllow(rights, required) = (rights & required) == required`

실제 값도 규칙과 정확히 맞습니다:

| 역할 | proj | 비트 | 해석 | tmpl | 해석 |
|---|---|---|---|---|---|
| PowerUser | 7 | 111 | 보기+변경+실행 | 3 | 보기+변경 |
| PowerUser2/3 | 3 | 011 | 보기+변경 | 3 | 보기+변경 |
| ReadUser2 | 5 | 101 | 보기+**실행**(변경 불가) | 1 | 보기만 |
| ReadUser3 | 1 | 001 | 보기만 | 1 | 보기만 |

> 첨부 스크린샷의 `PowerUser` 행에서 프로젝트 3개·데이터템플릿 2개가 모두 체크된 상태가 `7 / 3`과 일치합니다.
>
> 우리는 D3=2단계라 당분간 값이 **1(읽기) 또는 7(쓰기)** 두 가지만 나옵니다. 그래도 저장을 비트로 하는 이유는, 나중에 `5`(조회+실행, 수정 불가) 같은 운영자 역할이 필요해질 때 **마이그레이션 없이 화면만 고치면 되기 때문**입니다.

### 2.3 "어떤 객체에" 는 별도 테이블

`tb_user_role_authority` 한 행 = "이 역할은 이 객체를 만질 수 있다".

- `a_obj_tp`: `P`(프로젝트) / `D`(데이터템플릿) / `*`(모든 객체)
- `a_obj_id = '*'` + `a_obj_tp = '*'` → 화면의 "모든 객체" 체크박스
- `all_subs_yn = true` → 하위 전체 포함 (자식을 일일이 행으로 넣지 않아도 됨)
- `a_obj_depth` → 트리 렌더링용 깊이 (판정에는 미사용)

판정 로직(요지):

```
허용 = 아래 중 하나라도 만족하는 authority가 있고, 그 역할의 accessRights가 요구 비트를 포함할 때
   ① authority.objId == '*'
   ② authority.objId == 대상.부모ID  AND  authority.allSubs == true
   ③ authority.objId == 대상.ID
```

> ⚠️ **한계 발견**: ②가 **부모 1단계만** 봅니다. 조부모에 `allSubs`를 걸어도 손자에는 상속되지 않습니다. nd_suite는 트리가 2단이라 문제가 없지만, 우리가 3단 이상 폴더를 만들 계획이면 **재귀 상속으로 설계해야 합니다.**

### 2.4 강제 지점 — 서비스 계층

`AuthorityManager`의 static 메서드를 각 서비스 구현체 첫 줄에서 호출합니다.

```
ProjectServiceImpl      → checkProjAuthority(), getAccessibleProjects()
TaskServiceImpl         → checkProjAuthority(), checkTaskAuthority()
DataTmplServiceImpl     → checkTmplAuthority(), getAccessibleDataTmpls()
Load/Unload/Transform/Join/Sort/Flow/Xsql/Xjob/Aggregation ServiceImpl → checkTaskAuthority()
```

두 종류로 나뉜 게 참고할 만합니다.

- `check...Authority(userId, objectId, 요구비트)` — **단건 접근 차단** (실패 시 예외)
- `getAccessible...(userId, 전체목록, 요구비트)` — **목록 필터링** (권한 없는 건 안 보임)

### 2.5 메뉴는 관리자 플래그 하나로만 갈린다

프론트(Vue)는 `GET /api/login/isAdmin?user={id}` 결과 하나로 메뉴를 켜고 끕니다. **메뉴 단위 권한 테이블이 없습니다.**

| 메뉴 | 노출 조건 |
|---|---|
| 데이터 소스 | 관리자만 |
| 데이터 템플릿 | 전원 |
| 사용자/권한 > 비밀번호 변경 | 전원 |
| 사용자/권한 > 사용자 추가/수정, 역할 및 권한 | 관리자만 |
| 환경 설정 (연결정보/환경변수/언어/접근제어/공휴일) | 관리자만 |
| 백업/복구, 제품 정보 | 전원 |

우리는 메뉴 카탈로그를 DB에 두므로(6.1) 이보다 세밀하게 갑니다.

### 2.6 ⚠️ 따라 하면 안 되는 부분

`WebSecurityConfiguration`을 디컴파일한 결과, 아래가 전부 **`permitAll()`** 체인입니다:

```
/login, /auth/**, /sockjs/**, /api/login/checkSession, /api/login/existAdmin,
/api/login/isAdmin, /api/login/createAdmin, /api/login/changePassword,
/api/report/v1/**, /api/service/processMessage, /api/job/**, /api/log/**,
/*.html, /api/admin/**            ← 사용자 생성·삭제·역할 부여 API
.anyRequest().authenticated()
```

`POST /api/admin/createUser`, `updateRole`, `removeUser`가 **인증 없이 호출 가능**합니다. 메뉴만 감췄지 API는 열려 있습니다.

**교훈: 메뉴 숨김은 UX, 서버 검사는 보안. 반드시 둘 다.**

### 2.7 계정 관리에서 그대로 가져올 것 (D5 확정으로 적용 가능해짐)

계정이 우리 DB로 오면서 참고 솔루션의 계정 기능을 그대로 구현할 수 있게 됐습니다.

| 기능 | nd_suite | 우리 적용 |
|---|---|---|
| 계정 잠금 | `login_fail_count` 5회 → `block_time` 설정, 성공 시 초기화 | `app_user.login_fail_count` / `locked_until` (6.1) |
| 비밀번호 정책 | 대소문자+숫자+특수문자 9~16자 | 동일 수준 적용 |
| 비밀번호 해시 | PBKDF2 1000회 (`1000:salt:hash`) | **BCrypt** — 기존 `PasswordEncoder` 그대로 |
| 소프트 삭제 | `del_yn` | `app_user.use_yn='N'` |
| 시스템 계정 숨김 | 목록에서 `space` 계정 제외 | 서비스 계정을 사용자 목록에서 제외 |
| 감사 로그 | `tb_event_log`에 로그인 성공/실패까지 기록 | 7.8 |
| IP 접근 제어 | `tb_setting`의 `IP_ACCESS_CONTROL_YN` / `IP_ACCESS_ALLOWS`(CIDR) | 범위 밖 |

---

## 3. 설계 원칙

1. **자기 DB만으로 완결.** 계정·역할·권한·메뉴가 전부 metadata-db에 있고, 외부 DB 의존이 없습니다.
2. **서버가 최종 방어선.** 메뉴/버튼 숨김은 편의이고, 모든 상태 변경 API는 서버에서 권한을 검사합니다.
3. **거부는 두 얼굴로.** 단건 접근은 403, 목록 조회는 "안 보임". (2.4의 두 API 패턴)
4. **권한 없음이 기본값.** 역할이 없는 사용자는 로그인은 되지만 아무 메뉴도 안 보입니다. 관리자가 역할을 줘야 씁니다.
5. **NiFi·Airflow의 신원은 사람 단위.** 공유 계정을 걷어내고, 각 시스템의 감사 로그에 실명이 남게 합니다. (D4)
6. **비밀번호는 해시만.** 평문 저장·로깅 금지. 관리자도 남의 비밀번호를 볼 수 없고 초기화만 가능합니다.

---

## 4. 권한 모델

### 4.1 3축 모델

```
  사용자 ──N:M── 역할 ──┬── 시스템 권한 : {COMMON, NIFI, AIRFLOW, KAFKA, ADMIN} × 비트마스크
                        └── 메뉴 노출   : app_menu (기본은 시스템 권한에서 파생, 필요 시 개별 재정의)
```

- **시스템 권한**이 실제 보안입니다 (API/프록시/NiFi 정책에서 검사).
- **메뉴 노출**은 화면용입니다. 기본값은 시스템 권한에서 자동으로 계산되고, "권한은 있는데 메뉴는 감추고 싶다"가 필요할 때만 재정의 행을 넣습니다.

### 4.2 시스템(리소스) 코드

| 코드 | 범위 | 대응 메뉴 | 대응 API / 프록시 |
|---|---|---|---|
| `COMMON` | 대시보드, 인프라 상태 | 대시보드 | `/api/dashboard/**`, `/api/infra/**`, `/api/metrics/**` |
| `NIFI` | ETL 생성/관리/로그 | ETL 3종 | `/api/nifi/**`, `/api/etl/jobs/**`, `/nifi/`, `/nf/`, `/nifi-api/`, `/nifi-content-viewer/`, `/nifi-docs/`, `/nifi-extensions/`, `/nifi-websocket/` |
| `AIRFLOW` | 스케줄 관리 | AirFlow | `/airflow/**` (iframe + `/airflow/api/v2/**` 직접 호출) |
| `KAFKA` | CDC 파이프라인/연결정보/로그 | CDC 3종 | `/api/pipelines/**`, `/api/connections/**`, `/api/connect/**`, `/api/cdc/**`, **`/kafka-connect-api/**`** |
| `ADMIN` | 계정·역할·권한 관리 화면 | 설정 4종 (신규) | `/api/admin/**` |

> ⚠️ **대시보드는 `COMMON`만으로 완성되지 않습니다.** 대시보드 화면이 브라우저에서 Airflow·NiFi·Kafka API를 직접 호출하기 때문입니다(10.0-①). 권한이 부분적인 사용자에게는 해당 위젯을 **에러 대신 숨겨서** 축소된 대시보드를 보여줍니다.

### 4.3 동작(비트)

| 비트 | 값 | 이름 | 2단계에서의 취급 |
|---|---|---|---|
| 1 | 1 | `READ` | **읽기** 체크박스 |
| 2 | 2 | `WRITE` | **쓰기** 체크박스에 묶임 |
| 3 | 4 | `EXECUTE` | **쓰기** 체크박스에 묶임 |

판정: `(granted & required) == required`

D3=2단계이므로 화면에서 만들 수 있는 값은 **`0`(권한 없음) / `1`(읽기) / `7`(쓰기)** 세 가지입니다. 저장은 3비트라 나중에 실행을 분리해도 스키마 변경이 없습니다.

| 시스템 | READ가 허용하는 것 | WRITE가 추가로 허용하는 것 |
|---|---|---|
| NIFI | 캔버스 조회, 실행 로그·이력 조회 | 프로세스 그룹 생성/수정/삭제, 프로세서 시작/정지 |
| AIRFLOW | DAG 목록·실행 이력·로그 조회 | 변수·스케줄 수정, DAG 트리거/일시정지 |
| KAFKA | 파이프라인·연결정보·상태 조회 | 생성/수정/삭제, 커넥터 배포·기동·정지 |
| COMMON | 대시보드·인프라 조회 | (해당 없음 — 항상 READ만) |
| ADMIN | 계정·역할·권한·감사로그 조회 | 계정 생성, 역할 생성, 권한 부여, 사용자 배정 |

### 4.4 객체 단위 권한은 이번 범위 밖

nd_suite는 "프로젝트 A는 되고 B는 안 됨"까지 갑니다(2.3). 우리도 언젠가 "DZ 그룹만 접근" 요구가 올 수 있고, 그때 쓸 자리는 이미 있습니다 — `etl_job` 테이블(V17)의 잡 단위, 그리고 이전에 논의한 계층형 폴더 구조.

**이번에는 만들지 않습니다.** 다만 테이블은 나중에 붙이기 쉽게 설계하고(6.1 주석), 판정 함수 시그니처에 객체 파라미터 자리를 비워 둡니다.

### 4.5 최종 권한 계산

```
1. app_user_role 에서 사용자의 역할 목록 R = {r1, r2, ...}
2. 시스템 권한:  granted(system) = OR( bits(r, system) for r in R )      ← 역할 여러 개면 합집합
3. 메뉴 노출:    app_menu 중 granted(menu.system) 가 menu.required_bits 를 포함하는 것
                 단, app_role_menu_override 에 행이 있으면 그 값이 우선
4. 부트스트랩:   app_user.admin_yn='Y' → 전 시스템 7 (비상용, 6.4)
5. 역할 없음:    granted = 0, 메뉴 = ∅  → 로그인은 되지만 안내 화면
```

---

## 5. 남은 결정 — D7 (배포 분할)

17.5일치 작업을 몇 번에 나눠 배포할지. **테이블 설계에 영향이 없어 P1 착수를 막지 않습니다.**

가장 위험한 세 지점을 서로 다른 배포로 분리하는 것이 핵심입니다.

| 위험 지점 | 무엇이 깨질 수 있나 |
|---|---|
| P1 로그인 전환 | 아무도 로그인 못 함 |
| P4 API 인가 | Airflow DAG 401 → 운영 적재 중단 |
| P5b NiFi 개인 계정 | NiFi 접속 자체 불가 |

---

## 6. 테이블 설계 (metadata-db)

### 6.1 신규 마이그레이션 `V21__create_authz.sql`

```sql
-- 사용자/권한 체계. 계정 원장이 이 DB로 들어오면서(D1) 외부 계정 DB 의존이 사라진다.

-- ---------------------------------------------------------------------------
-- 계정 - 기존 app_user(V7/V8)를 인증원으로 되살린다.
--
-- V8에서 "자격증명은 Keycloak이 보유"라며 user_pw를 nullable로 풀었는데, Keycloak을
-- 제거한 뒤로 비어 있었다. 이제 이 컬럼이 다시 유일한 자격증명 저장소가 된다.
-- (nullable은 유지한다 - 이관 중 비밀번호 미설정 상태를 표현해야 하고, NOT NULL로
--  되돌리면 기존 행 때문에 마이그레이션이 실패한다. 로그인 시 null이면 거부한다.)
-- ---------------------------------------------------------------------------
ALTER TABLE app_user ADD COLUMN login_fail_count SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE app_user ADD COLUMN locked_until     TIMESTAMP;
ALTER TABLE app_user ADD COLUMN pw_updated_at    TIMESTAMP;

-- ---------------------------------------------------------------------------
-- 역할
-- ---------------------------------------------------------------------------
CREATE TABLE app_role (
    role_id    VARCHAR(40)  PRIMARY KEY,          -- ROLE_ETL_ADMIN 등
    role_nm    VARCHAR(100) NOT NULL,
    role_desc  VARCHAR(300),
    -- 마이그레이션이 심는 기본 역할은 삭제/개명을 막는다(운영 중 지워지면 전원이 권한을 잃는다).
    built_in   BOOLEAN      NOT NULL DEFAULT false,
    use_yn     CHAR(1)      NOT NULL DEFAULT 'Y',
    created_at TIMESTAMP    NOT NULL DEFAULT now(),
    created_by VARCHAR(100),
    updated_at TIMESTAMP    NOT NULL DEFAULT now(),
    updated_by VARCHAR(100)
);

-- ---------------------------------------------------------------------------
-- 사용자 → 역할 (N:M)
-- 계정이 같은 DB에 있으므로 FK를 건다(고아 행 걱정이 사라졌다).
-- ---------------------------------------------------------------------------
CREATE TABLE app_user_role (
    user_id     VARCHAR(255) NOT NULL REFERENCES app_user (user_id) ON DELETE CASCADE,
    role_id     VARCHAR(40)  NOT NULL REFERENCES app_role (role_id) ON DELETE CASCADE,
    assigned_at TIMESTAMP    NOT NULL DEFAULT now(),
    assigned_by VARCHAR(100),
    PRIMARY KEY (user_id, role_id)
);
CREATE INDEX idx_app_user_role_user ON app_user_role (user_id);

-- ---------------------------------------------------------------------------
-- 역할 → 시스템 권한
-- access_bits 비트마스크: 1=READ, 2=WRITE, 4=EXECUTE  (nd_suite와 동일 규칙)
-- 2단계 UI에서는 0 / 1 / 7 만 생성된다.
-- ---------------------------------------------------------------------------
CREATE TABLE app_role_system_permission (
    role_id     VARCHAR(40) NOT NULL REFERENCES app_role (role_id) ON DELETE CASCADE,
    system_code VARCHAR(20) NOT NULL,             -- COMMON | NIFI | AIRFLOW | KAFKA | ADMIN
    access_bits SMALLINT    NOT NULL DEFAULT 0,
    updated_at  TIMESTAMP   NOT NULL DEFAULT now(),
    updated_by  VARCHAR(100),
    PRIMARY KEY (role_id, system_code),
    CONSTRAINT ck_app_role_system_permission_bits CHECK (access_bits BETWEEN 0 AND 7)
);

-- ---------------------------------------------------------------------------
-- 메뉴 카탈로그
--
-- 화면 라우트와 1:1인 고정 목록이라 마이그레이션이 심고, 관리자는 "노출/숨김"만 바꾼다
-- (관리자가 임의로 메뉴를 만들 수 있으면 존재하지 않는 라우트가 생긴다).
-- required_bits 는 "이 메뉴를 보려면 해당 시스템에 최소 이만큼 필요" 를 뜻한다.
-- ---------------------------------------------------------------------------
CREATE TABLE app_menu (
    menu_id       VARCHAR(40)  PRIMARY KEY,
    parent_id     VARCHAR(40)  REFERENCES app_menu (menu_id),
    menu_nm       VARCHAR(100) NOT NULL,
    menu_url      VARCHAR(200),                   -- 그룹 노드는 NULL
    icon          VARCHAR(50),
    system_code   VARCHAR(20)  NOT NULL,
    required_bits SMALLINT     NOT NULL DEFAULT 1,
    sort_ord      INTEGER      NOT NULL DEFAULT 0,
    use_yn        CHAR(1)      NOT NULL DEFAULT 'Y'
);

-- 역할별 메뉴 재정의. 비워두면 4.5의 규칙대로 시스템 권한에서 자동 계산된다.
-- "권한은 주되 메뉴는 감추고 싶다" 같은 예외에만 행이 생긴다.
CREATE TABLE app_role_menu_override (
    role_id VARCHAR(40) NOT NULL REFERENCES app_role (role_id) ON DELETE CASCADE,
    menu_id VARCHAR(40) NOT NULL REFERENCES app_menu (menu_id) ON DELETE CASCADE,
    visible BOOLEAN     NOT NULL,
    PRIMARY KEY (role_id, menu_id)
);

-- ---------------------------------------------------------------------------
-- 감사 로그 - "언제 누가 누구에게 무슨 권한을 줬나"는 사후에 반드시 질문이 들어온다.
-- ---------------------------------------------------------------------------
CREATE TABLE permission_audit_log (
    id           BIGSERIAL PRIMARY KEY,
    occurred_at  TIMESTAMP    NOT NULL DEFAULT now(),
    actor_id     VARCHAR(100) NOT NULL,           -- 수행자 (서비스 호출은 'svc:airflow')
    action       VARCHAR(40)  NOT NULL,           -- LOGIN_SUCCESS / LOGIN_FAIL / ACCOUNT_LOCKED /
                                                  -- CREATE_USER / DISABLE_USER / RESET_PASSWORD /
                                                  -- CREATE_ROLE / DELETE_ROLE / GRANT_SYSTEM /
                                                  -- REVOKE_SYSTEM / ASSIGN_ROLE / UNASSIGN_ROLE /
                                                  -- SET_MENU / DENIED
    target_type  VARCHAR(20),                     -- USER | ROLE | MENU | API
    target_id    VARCHAR(200),
    before_value VARCHAR(200),
    after_value  VARCHAR(200),
    detail       TEXT,
    client_ip    VARCHAR(45)
);
CREATE INDEX idx_permission_audit_log_time  ON permission_audit_log (occurred_at DESC);
CREATE INDEX idx_permission_audit_log_actor ON permission_audit_log (actor_id, occurred_at DESC);

-- ---------------------------------------------------------------------------
-- (나중에) 객체 단위 권한 - 이번에는 만들지 않고 자리만 남긴다.
-- CREATE TABLE app_role_object_permission (
--     role_id VARCHAR(40), obj_type VARCHAR(20),        -- ETL_JOB | FOLDER | PIPELINE | '*'
--     obj_id  VARCHAR(100),                             -- '*' 가능
--     include_descendants BOOLEAN NOT NULL DEFAULT false,   -- nd_suite의 all_subs_yn (단, 재귀로)
--     PRIMARY KEY (role_id, obj_type, obj_id));
```

### 6.2 초기 데이터 (같은 마이그레이션에서 seed)

**역할 2개** (D6)

```sql
INSERT INTO app_role (role_id, role_nm, role_desc, built_in, created_by) VALUES
  ('ROLE_ETL_ADMIN',  'ETL 관리자', 'NiFi·Airflow·CDC 전체 쓰기 및 계정/권한 관리', true, 'system'),
  ('ROLE_ETL_VIEWER', 'ETL 조회자', '전체 조회 전용',                                true, 'system');
```

**시스템 권한**

| role_id | COMMON | NIFI | AIRFLOW | KAFKA | ADMIN |
|---|---|---|---|---|---|
| `ROLE_ETL_ADMIN` | 1 | 7 | 7 | 7 | 7 |
| `ROLE_ETL_VIEWER` | 1 | 1 | 1 | 1 | 0 |

**메뉴 카탈로그** (현재 라우트 + 신규 관리 메뉴 4개)

| menu_id | parent | 이름 | url | system | required |
|---|---|---|---|---|---|
| `m_dashboard` | — | 대시보드 | `/dashboard` | COMMON | 1 |
| `m_airflow` | — | AirFlow | — | AIRFLOW | 1 |
| `m_airflow_manage` | `m_airflow` | 생성/관리 | `/airflow/manage` | AIRFLOW | 1 |
| `m_etl` | — | ETL | — | NIFI | 1 |
| `m_etl_create` | `m_etl` | 생성 | `/etl/create` | NIFI | **7** |
| `m_etl_manage` | `m_etl` | 관리 | `/etl/manage` | NIFI | 1 |
| `m_etl_logs` | `m_etl` | 로그 | `/etl/logs` | NIFI | 1 |
| `m_cdc` | — | CDC | — | KAFKA | 1 |
| `m_cdc_pipelines` | `m_cdc` | 파이프라인 | `/cdc/pipelines` | KAFKA | 1 |
| `m_cdc_connections` | `m_cdc` | 연결정보 | `/cdc/connections` | KAFKA | 1 |
| `m_cdc_logs` | `m_cdc` | 처리 로그 | `/cdc/logs` | KAFKA | 1 |
| `m_admin` | — | 설정 | — | ADMIN | 1 |
| `m_admin_users` | `m_admin` | 계정 관리 | `/admin/users` | ADMIN | 1 |
| `m_admin_roles` | `m_admin` | 역할 및 권한 | `/admin/roles` | ADMIN | 1 |
| `m_admin_assign` | `m_admin` | 사용자 역할 배정 | `/admin/assign` | ADMIN | 1 |
| `m_admin_audit` | `m_admin` | 감사 로그 | `/admin/audit` | ADMIN | 1 |

> `ETL 생성`만 `required_bits = 7`입니다. 조회자에게 "생성" 메뉴를 보여줄 이유가 없기 때문입니다. 나머지는 조회 화면이라 `1`입니다.

### 6.3 계정 이관 (P1 1회성)

기존 사용자가 **같은 아이디·비밀번호로 그대로 로그인**하도록 BCrypt 해시를 옮깁니다. 두 테이블 모두 BCrypt라 재입력이 필요 없습니다.

```
1) 대상 선별 — Cerebro ETL을 실제로 쓸 인원만 (외부 계정 전체를 옮길 필요 없음)
2) 복사 — ST_USER(USER_ID, USER_NM, USER_PW, EMAIL, TEL_NO, USE_YN, USE_STRT_DTTM, USE_END_DTTM)
          → app_user(user_id, user_nm, user_pw, email, tel_no, use_yn, use_strt_dttm, use_end_dttm)
   · 이미 app_user에 행이 있으면(자동 프로비저닝된 사용자) user_pw만 채운다
3) 관리자 지정 — 지정 계정에 ROLE_ETL_ADMIN 부여
4) 로그인 로직 전환 → 실제 로그인 검증
5) 검증 완료 후 ACCOUNT_DB_* 설정과 조회 코드 제거
```

- 이 스크립트는 **저장소에 커밋하지 않습니다**(운영 계정 데이터가 들어가므로). 1회 실행 후 폐기.
- 4)에서 문제가 생기면 로그인 로직만 되돌리면 즉시 복구됩니다. 그래서 5)를 별도 커밋으로 분리합니다.

### 6.4 `app_user.admin_yn`의 용도 확정

지금은 아무도 채우지 않는 죽은 컬럼입니다. 두 가지로 씁니다.

1. **비상 백도어** — `app_user_role`이 비었거나 잘못 설정돼 전원이 잠겼을 때, DB에서 이 값만 `Y`로 바꾸면 들어올 수 있습니다.
2. **판정 단락** — `admin_yn='Y'` 이면 역할과 무관하게 전 시스템 `7`.

관리 화면에서는 노출하지 않습니다(DB 직접 수정 전용).

---

## 7. 기능 구현 설계

### 7.1 백엔드 구성

```
com.company.pipeline.user/          (기존 - 인증)
├─ AuthController.java              (변경) app_user 기준 로그인 + 실패 카운트/잠금
├─ AccountLookupService.java        (삭제) 외부 MySQL 조회
├─ security/AccountDataSourceConfig (삭제) 외부 MySQL 커넥션
├─ AccountService.java              (신규) 계정 CRUD, 비밀번호 변경/초기화, 정책 검증
└─ ...

com.company.pipeline.authz/         (신규 - 인가)
├─ SystemCode.java                  enum COMMON, NIFI, AIRFLOW, KAFKA, ADMIN
├─ AccessBits.java                  READ=1, WRITE=2, EXECUTE=4, ALL=7  +  allows(granted, required)
├─ AppRole / AppUserRole / AppRoleSystemPermission / AppMenu / AppRoleMenuOverride   (엔티티)
├─ ...Repository                    (5종)
├─ PermissionService.java           resolve(userId) → UserPermissions (Caffeine 캐시 TTL 5분)
│                                   check(userId, system, bits) / invalidate(userId)
├─ UserPermissions.java             record(userId, roles, Map<SystemCode,Integer>, List<MenuNode>, admin)
├─ RequirePermission.java           @interface (system, bits)
├─ PermissionAspect.java            @Around — 위반 시 BusinessException(FORBIDDEN) + 감사로그(DENIED)
├─ AuthzController.java             GET /api/authz/me, /api/authz/menus, /api/authz/proxy-credential
├─ AdminController.java             계정/역할/권한/배정 CRUD — 전부 @RequirePermission(ADMIN, WRITE)
├─ PermissionAuditService.java      감사 로그 기록
└─ provisioning/
   ├─ NifiTenantSyncService.java    NiFi 사용자·정책 동기화 (7.7)
   └─ AirflowUserSyncService.java   Airflow 계정·역할 동기화 (7.7)
```

**JWT에는 권한을 넣지 않습니다.** 토큰 수명이 30일이라, 권한을 토큰에 담으면 권한을 회수해도 30일간 유효합니다. 대신 요청마다 `PermissionService`가 캐시에서 읽고, 권한 변경 시 해당 사용자 캐시를 즉시 무효화합니다.

**애노테이션 사용 예**

```java
@GetMapping
@RequirePermission(system = KAFKA, bits = READ)
public ApiResponse<List<PipelineResponse>> list() { ... }

@PostMapping("/{id}/start")
@RequirePermission(system = KAFKA, bits = WRITE)      // 2단계이므로 실행도 WRITE
public ApiResponse<PipelineResponse> start(@PathVariable Long id) { ... }
```

### 7.2 엔드포인트 → 권한 매핑 (전수)

| 엔드포인트 | 시스템 | 요구 |
|---|---|---|
| `GET /api/dashboard/summary`, `/api/infra/**`, `/api/metrics/**` | COMMON | READ |
| `POST /api/metrics/daily-load/increment` | COMMON | WRITE (실사용 호출부 없음 — 내부 스케줄러는 서비스 빈 직접 호출) |
| `GET /api/nifi/process-group-tree`, `/execution-logs`, `/processor-runs` | NIFI | READ |
| `POST /api/nifi/process-groups`, `/api/nifi/etl/initial-db-to-db` | NIFI | WRITE |
| `GET /api/etl/jobs`, `/{id}`, `/{id}/runs` | NIFI | READ |
| `POST /api/etl/jobs/sync` | NIFI | WRITE |
| `GET /api/pipelines`, `/{id}`, `/{id}/history` | KAFKA | READ |
| `POST /api/pipelines`, `/log-file`, `DELETE /api/pipelines/{id}` | KAFKA | WRITE |
| `POST /api/pipelines/{id}/{deploy,start,pause,stop,restart}` | KAFKA | WRITE |
| `POST /api/pipelines/{id}/dismiss-drift`, `/metrics/snapshot` | KAFKA | WRITE |
| `GET /api/connections/**` | KAFKA | READ |
| `POST/PUT/DELETE /api/connections/**`, `/{id}/test` | KAFKA | WRITE |
| `GET /api/connect/**`, `/api/cdc/logs/**` | KAFKA | READ |
| `/api/admin/**` | ADMIN | WRITE |
| `GET /api/authz/**`, `POST /api/auth/password` (본인 비밀번호) | — | 인증만 |
| `POST /api/auth/login` | — | 공개 |

### 7.3 SecurityConfig 변경

```java
.requestMatchers("/api/auth/login").permitAll()
.requestMatchers("/api/internal/**").access(serviceTokenOnly)   // 7.6
.anyRequest().authenticated()                                    // permitAll → authenticated
```

세부 권한은 `@RequirePermission`이 담당합니다. URL 패턴으로 권한을 표현하면 라우팅이 바뀔 때마다 조용히 깨집니다.

### 7.4 프론트엔드

```
src/auth/
├─ AuthContext.tsx        (변경) 로그인 후 GET /api/authz/me 로 권한·메뉴 로드
├─ permissions.ts         (신규) can(system, 'READ'|'WRITE') 훅
└─ RequirePermission.tsx  (신규) 라우트 가드 — 권한 없으면 안내 후 /dashboard

src/components/AppLayout.tsx    NAV_ITEMS 하드코딩 → 서버 메뉴로 교체
src/pages/admin/
├─ UserAccountPage.tsx          계정 관리          (/admin/users)
├─ RolePermissionPage.tsx       역할 및 권한       (/admin/roles)
├─ UserRoleAssignPage.tsx       사용자 역할 배정   (/admin/assign)
└─ AuditLogPage.tsx             감사 로그          (/admin/audit)
src/pages/ChangePasswordPage.tsx  본인 비밀번호 변경 (헤더 메뉴)
```

**버튼 처리 원칙**: 숨기지 말고 **비활성화 + 툴팁**("쓰기 권한이 필요합니다"). 버튼이 사라지면 사용자는 "기능이 없다"고 오해하고, 비활성화면 "권한을 받아야 한다"를 압니다.

### 7.5 화면 설계

#### (1) 설정 > 계정 관리

```
┌─ 계정 관리 ─────────── [검색: ______ ] [ ]비활성 포함 ──── [계정 추가] [비활성화] ─┐
│ ┌─────────────────────────────────────────────────────────────────────────────┐  │
│ │ □ │ 계정      │ 이름   │ 이메일          │ 상태   │ 최종로그인       │ 잠금  │  │
│ │ ☑ │ cktnqhd15 │ 차수봉 │ ...@...         │ 사용   │ 2026-08-13 09:12 │ —     │  │
│ │ □ │ dev01     │ 홍길동 │ ...@...         │ 사용   │ 2026-08-12 17:40 │ —     │  │
│ │ □ │ dev02     │ 김철수 │ ...@...         │ 사용   │ —                │ 🔒 5회│  │
│ │ □ │ leaver01  │ 이퇴사 │ ...@...         │ 비활성 │ 2026-06-30 11:02 │ —     │  │
│ └─────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                  │
│ ── 선택: 김철수(dev02) ─────────────────────────────────────────────────────────│
│   이름   [ 김철수          ]     이메일 [ ...@...        ]                       │
│   전화   [ 010-...         ]     사용여부 (●사용 ○비활성)                        │
│   사용기간 [2026-08-13] ~ [2026-11-13]                                           │
│                                                                                  │
│   [비밀번호 초기화]  [잠금 해제]                                                 │
│   ⓘ 관리자도 기존 비밀번호를 볼 수 없습니다. 초기화하면 임시 비밀번호가 발급됩니다.│
│                                                                                  │
│   ⚠ 비활성화하면 NiFi·Airflow 계정도 함께 비활성화됩니다.                        │
│                                                          [취소]  [저장]          │
└──────────────────────────────────────────────────────────────────────────────────┘
```

- 참고 솔루션의 **계정 잠금(5회 실패)** 을 그대로 가져왔습니다. 잠긴 계정은 목록에 자물쇠로 표시되고, 관리자가 즉시 해제할 수 있습니다.
- **비밀번호는 관리자도 볼 수 없습니다.** 초기화만 가능하고, 초기화하면 사용자는 다음 로그인 때 변경을 요구받습니다.
- 삭제 대신 **비활성화**(`use_yn='N'`)를 기본으로 둡니다. 실행 이력·감사 로그가 계정을 참조하기 때문입니다.

#### (2) 설정 > 역할 및 권한

```
┌─ 역할 및 권한 ──────────────────────────── [역할 추가] [역할명 변경] [삭제] ─┐
│ ┌───────────────────────────────────────────────────────────────────────┐  │
│ │ □ │ 역할명       │ 설명                    │ 사용자 │ 최종수정        │  │
│ │ ☑ │ ETL 관리자   │ 전체 쓰기 + 계정/권한   │   2    │ 2026-08-13      │  │
│ │ □ │ ETL 조회자   │ 전체 조회 전용          │   3    │ 2026-08-13      │  │
│ └───────────────────────────────────────────────────────────────────────┘  │
│                                                                            │
│ ── 선택한 역할: ETL 관리자 ─────────────────────────────────────────────── │
│                                                                            │
│  ☑ 모든 시스템 쓰기 권한                                                   │
│                                                                            │
│  ┌── 시스템 권한 ─────────────────────────────────────────────────────┐    │
│  │ 시스템          │ 읽기 │ 쓰기 │ 설명                              │    │
│  │ 공통/대시보드   │  ☑   │  —   │ 대시보드·인프라 상태              │    │
│  │ NiFi (ETL)      │  ☑   │  ☑   │ 캔버스 편집·프로세서 기동         │    │
│  │ Airflow         │  ☑   │  ☑   │ 스케줄 변경·DAG 실행              │    │
│  │ Kafka (CDC)     │  ☑   │  ☑   │ 파이프라인 편집·커넥터 기동       │    │
│  │ 계정/권한       │  ☑   │  ☑   │ 계정 관리, 이 화면 자체           │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│    ⓘ '쓰기'는 변경과 실행을 함께 허용합니다.                                │
│                                                                            │
│  ┌── 메뉴 노출 (기본값: 시스템 권한에서 자동) ────────────────────────┐    │
│  │ ☑ 대시보드                                                         │    │
│  │ ☑ AirFlow > 생성/관리                                              │    │
│  │ ☑ ETL > 생성 / 관리 / 로그                                         │    │
│  │ ☑ CDC > 파이프라인 / 연결정보 / 처리 로그                          │    │
│  │ ☑ 설정 > 계정 관리 / 역할 및 권한 / 사용자 역할 배정 / 감사 로그   │    │
│  │                                        [기본값으로 되돌리기]       │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                                     [취소]  [저장]         │
└────────────────────────────────────────────────────────────────────────────┘
```

참고 솔루션과 다르게 만든 점:

- **시스템 권한 / 메뉴 노출 2블록으로 분리**했습니다. nd_suite는 객체 트리와 권한 체크박스가 한 화면에 섞여 "무엇에 대한 권한인지"가 헷갈립니다.
- **메뉴 블록은 기본값이 자동 계산**입니다. 관리자가 매번 체크할 필요가 없고, 예외가 필요할 때만 손댑니다.
- 저장 시 검증: 시스템 권한이 전부 0이면 경고 (nd_suite도 "보기/변경 중 하나 이상"을 강제합니다).
- `built_in` 역할은 삭제·개명 버튼이 비활성화됩니다.

#### (3) 설정 > 사용자 역할 배정

```
┌─ 사용자 역할 배정 ──────────────── [검색: ______ ] [ ] 역할 없는 사용자만 ──┐
│ ┌──────────────────────────────────────────────────────────────────────┐   │
│ │ □ │ 계정        │ 이름   │ 이메일           │ 역할                    │   │
│ │ ☑ │ cktnqhd15   │ 차수봉 │ ...              │ ETL 관리자              │   │
│ │ □ │ dev01       │ 홍길동 │ ...              │ (없음)                  │   │
│ └──────────────────────────────────────────────────────────────────────┘   │
│                                                                            │
│ ── 선택: 차수봉(cktnqhd15) ──────────────────────────────────────────────  │
│   할당된 역할              역할 목록                                        │
│   ┌──────────────┐  ◀추가  ┌──────────────┐                                │
│   │ ETL 관리자   │  제거▶  │ ETL 조회자   │                                │
│   └──────────────┘         └──────────────┘                                │
│                                                                            │
│   ⓘ 역할을 바꾸면 NiFi·Airflow 계정 권한도 함께 조정됩니다.                 │
│                                                     [취소]  [저장]         │
└────────────────────────────────────────────────────────────────────────────┘
```

**좌/우 이동식 배정**은 참고 솔루션(`할당된 역할` / `역할 목록` / `선택 역할 할당`)의 UX를 차용했습니다. 익숙하고 실수가 적습니다.

#### (4) 권한 없는 화면 진입 / 역할 없는 사용자

- 라우트 가드가 막고 토스트: "이 메뉴에 접근할 권한이 없습니다. 관리자에게 문의하세요."
- 역할이 하나도 없는 사용자는 빈 사이드바 대신 **안내 페이지**를 봅니다: "아직 권한이 부여되지 않았습니다. 관리자에게 권한을 요청하세요." + 관리자 목록.

> 참고 솔루션에는 이 안내가 없어서 "메뉴가 왜 없지?" 문의가 반복될 구조입니다.

### 7.6 서비스 계정 (인가 전환과 같은 단계)

Airflow DAG는 사람이 아닙니다. 인가를 켜면 이들이 먼저 죽습니다.

```
방식: 고정 서비스 토큰 (헤더 X-Service-Token)
  · .env: PIPELINE_SERVICE_TOKEN=<랜덤 48자>
  · Airflow Variable 로 주입 → DAG의 requests 헤더에 추가
  · SecurityConfig: 이 헤더가 유효하면 전 시스템 권한(7)으로 통과
  · 대상: /api/pipelines/**, /api/connect/**
  · 감사 로그 actor_id = 'svc:airflow'
```

호출부는 1.2의 표 5개로 확인했습니다. **동적 생성 DAG이라 파일이 늘 수 있으므로, 전환 직전에 `grep -rn "pipeline-api:8081" airflow/dags/`로 한 번 더 확인합니다.**

### 7.7 ★ NiFi / Airflow 개인 계정 전환 (D4)

가장 큰 작업입니다. 두 시스템의 사정이 다릅니다.

#### 공통 구조 — nginx가 사용자별 자격증명을 받아 주입

기존의 "고정된 공유 자격증명 주입"을 "**요청마다 백엔드에 물어봐서 그 사용자의 자격증명을 주입**"으로 바꿉니다.

```
[브라우저] ──세션 쿠키──▶ [nginx]
                            │  auth_request → GET /api/authz/proxy-credential?system=NIFI
                            │                  (백엔드가 쿠키로 사용자 식별 + 권한 확인)
                            │  ◀── 200 + 응답 헤더에 그 사용자의 자격증명
                            │      (거부 시 403 → nginx가 요청 자체를 차단)
                            ▼
                         [NiFi / Airflow]
```

```nginx
# 로그인 시 백엔드가 세션 쿠키도 함께 내려준다.
#   Set-Cookie: cetl_session=<JWT>; Path=/; SameSite=Lax; HttpOnly
#   (iframe 요청은 Authorization 헤더를 실을 수 없으므로 쿠키가 필요하다)

location = /internal/authz-nifi {
    internal;
    proxy_pass http://pipeline-api:8081/api/authz/proxy-credential?system=NIFI;
    proxy_pass_request_body off;
    proxy_set_header Content-Length "";
    proxy_set_header Cookie $http_cookie;
    proxy_set_header X-Original-Method $request_method;   # GET이면 READ, 아니면 WRITE로 판정
}

location /nifi-api/ {
    auth_request /internal/authz-nifi;
    auth_request_set $proxied_user $upstream_http_x_proxied_entity;
    proxy_set_header X-ProxiedEntitiesChain $proxied_user;   # ← 공유 Bearer 대신 이것
    ...
}
```

`auth_request_set`으로 **인가 판정과 자격증명 발급을 한 번의 서브요청으로** 처리합니다.

> ⚠️ **쿠키 확인 필요**: 이 솔루션 화면이 다른 사이트의 iframe 안에서 열리는 배포 형태라면, `SameSite=Lax` 쿠키가 전송되지 않습니다(교차 사이트 iframe). 그 경우 `SameSite=None; Secure`가 필요합니다. **P5 착수 전 실제 접속 형태를 확인**해서 값을 정합니다.

#### NiFi — 프록시 대행 인증

NiFi는 신뢰하는 프록시가 최종 사용자 신원을 대신 주장하는 방식을 지원합니다. `managed-authorizer`가 이미 켜져 있으므로(1.5) 정책 체계는 그대로 씁니다.

| 단계 | 내용 |
|---|---|
| 1 | nginx용 클라이언트 인증서 발급(`CN=cerebro-proxy`), NiFi truststore에 등록 |
| 2 | nginx가 NiFi로 갈 때 `proxy_ssl_certificate` 사용 (현재는 `proxy_ssl_verify off`에 인증서 없음) |
| 3 | NiFi에 `CN=cerebro-proxy` 사용자 생성 + **"Proxy User Requests"** 정책 부여 |
| 4 | 사람마다 NiFi 사용자 생성 — 백엔드 `NifiTenantSyncService`가 `/nifi-api/tenants/users`로 |
| 5 | 역할별 정책 부여 — 조회자: root PG의 `view the component` / `view the data`<br>관리자: + `modify the component` / `operate the component` |
| 6 | nginx에서 `proxy_set_header Authorization $nifi_shared_bearer` 제거 |

> ⚠️ **검증 필요**: NiFi 2.2.0에서 `X-ProxiedEntitiesChain`의 정확한 동작(헤더 포맷, mTLS 필수 여부)을 **스파이크로 먼저 확인**해야 합니다. 이게 안 되면 대안은 LDAP 서버 추가뿐이고, 그건 Keycloak을 뺀 이유와 충돌합니다. **P5 착수 전 0.5일 스파이크를 반드시 넣습니다.**
>
> ⚠️ truststore·인증서를 잘못 건드리면 NiFi 접속 자체가 끊깁니다. 작업 전 `nifi-conf` 볼륨 백업 필수.

#### Airflow — 개인 FAB 계정

Airflow는 훨씬 간단합니다. `AIRFLOW__CORE__AUTH_MANAGER=FabAuthManager`가 이미 설정돼 있어 계정·역할 체계를 바로 씁니다.

| 단계 | 내용 |
|---|---|
| 1 | 백엔드 `AirflowUserSyncService`가 계정 생성/역할변경 시 Airflow 계정을 만들거나 갱신 (비밀번호는 무작위 — 사람은 직접 로그인하지 않음) |
| 2 | 역할 매핑: `ETL 관리자` → FAB `Admin`, `ETL 조회자` → FAB `Viewer` |
| 3 | 백엔드가 사용자별 세션 쿠키를 획득·캐시 → `auth_request` 응답 헤더로 전달 |
| 4 | nginx에서 `$airflow_shared_cookie` 제거 |
| 5 | `refresh-credentials.sh`의 Airflow 부분과 crond 항목 정리 |

#### 계정 수명주기 동기화

D1로 계정 원장이 한 곳이 되면서 이 부분이 단순해졌습니다.

| 우리 화면에서 | NiFi | Airflow |
|---|---|---|
| 계정 생성 | 사용자 생성 (정책은 역할 배정 시) | 계정 생성 (역할은 배정 시) |
| 역할 변경 | 정책 재부여 | FAB 역할 교체 |
| 비활성화(`use_yn='N'`) | 사용자 삭제 또는 정책 회수 | 계정 비활성화 |

동기화 실패는 **저장을 막지 않되 경고를 띄우고 재시도 큐에 넣습니다** — NiFi가 잠깐 죽었다고 계정 관리가 막히면 안 됩니다.

### 7.8 감사 로그

`permission_audit_log`에 최소 아래를 남깁니다. nd_suite가 `tb_event_log`에 로그인 성공/실패까지 남기는 것을 참고했습니다.

| action | 시점 |
|---|---|
| `LOGIN_SUCCESS` / `LOGIN_FAIL` / `ACCOUNT_LOCKED` | 로그인 시도 (client_ip 포함) |
| `CREATE_USER` / `DISABLE_USER` / `RESET_PASSWORD` | 계정 관리 |
| `CREATE_ROLE` / `DELETE_ROLE` | 역할 생성·삭제 |
| `GRANT_SYSTEM` / `REVOKE_SYSTEM` | 시스템 권한 변경 (before/after 비트 기록) |
| `ASSIGN_ROLE` / `UNASSIGN_ROLE` | 사용자 역할 배정 변경 |
| `SET_MENU` | 메뉴 노출 재정의 |
| `DENIED` | 403 발생 (누가 어떤 API를 시도했는지) |

---

## 8. 실행 계획

각 단계는 **독립 배포 가능**하고, 그 시점까지의 동작이 깨지지 않아야 합니다.

| 단계 | 내용 | 산출물 | 통과 조건 | 예상 |
|---|---|---|---|---|
| **P1** | 권한 모델 + 로그인 독립 | `V21` 마이그레이션, `authz` 패키지, 계정 이관, `app_user` 기준 로그인, `GET /api/authz/me` | **기존 사용자가 같은 비밀번호로 로그인**, 화면 전부 무영향 | 3일 |
| **P2** | 관리 화면 4개 | 계정 관리 / 역할·권한 / 역할 배정 / 비밀번호 변경, `/api/admin/**` | 계정 생성 → 역할 부여 → 그 계정으로 로그인이 화면에서 완결 | 4일 |
| **P3** | 메뉴 동적화 | `AppLayout` 서버 메뉴 연동, 라우트 가드, 안내 페이지 | 조회자 계정으로 로그인 시 `ETL 생성`·`설정` 메뉴가 사라짐 | 1일 |
| **P4** | **서비스 토큰 + API 인가** ★ | `X-Service-Token`, `@RequirePermission` 전 엔드포인트, `anyRequest().authenticated()` | **DAG 정상 동작이 통과 조건.** 조회자 토큰으로 `POST /api/pipelines` → 403 | 3일 |
| **P5a** | NiFi 프록시 인증 스파이크 | 검증 보고 | `X-ProxiedEntitiesChain`으로 개인 신원이 NiFi 로그에 남는 것 확인 | 0.5일 |
| **P5b** | NiFi·Airflow 개인 계정 | 인증서, `NifiTenantSyncService`, `AirflowUserSyncService`, nginx `auth_request` | 조회자로 캔버스 편집 시도 → 거부, 조회는 정상. NiFi 로그에 실명 | 5일 |
| **P6** | 감사 로그 + 조회 화면 | `permission_audit_log`, `AuditLogPage` | 권한 변경·거부가 전부 기록 | 1일 |

**P1과 P4가 특히 위험합니다.**

```
P1 — 로그인 전환
  1) V21 + 계정 이관 (아직 로그인은 기존 경로)
  2) 이관 결과 검증 (대상 인원 전원의 해시가 옮겨졌는지)
  3) 로그인 로직 전환 → 실제 로그인 확인
  4) 확인 후 별도 커밋으로 외부 DB 설정·코드 제거
     ※ 3)에서 문제 시 그 커밋만 되돌리면 즉시 복구

P4 — API 인가
  1) X-Service-Token 통과 로직 배포 (아직 permitAll 유지 — 아무것도 안 깨짐)
  2) DAG에 토큰 주입 → 정상 동작 확인
  3) 그 다음에 anyRequest().authenticated() 전환
```

---

## 9. 위험 요소

| 위험 | 영향 | 대응 |
|---|---|---|
| **P1에서 로그인 전환 실패** | 아무도 로그인 못 함 | 8장의 4단계 순서 준수. 외부 DB 제거를 별도 커밋으로 분리해 즉시 롤백 가능하게 |
| 계정 이관 누락 | 특정 사용자만 로그인 불가 | 이관 후 대상 인원 전원 목록 대조 + 실제 로그인 1회씩 확인 |
| P4에서 Airflow DAG 401 | 운영 적재 중단 | 8장의 3단계 순서 준수 + 롤백 커밋 준비 |
| **P5b에서 NiFi 접속 불가** | ETL 관리 전면 중단 | P5a 스파이크 선행, `nifi-conf` 볼륨 백업, 롤백용 `nginx.conf` 보관 |
| `X-ProxiedEntitiesChain`이 2.2.0에서 기대대로 안 됨 | D4 경로 자체가 막힘 | P5a에서 조기 발견. 대안(LDAP 추가 / nginx 메서드 차단으로 후퇴)을 그 시점에 재결정 |
| NiFi UI가 읽기 동작에도 POST를 씀 | 조회자가 캔버스를 못 봄 | P5b 전 읽기 전용 계정으로 실제 클릭 테스트, 예외 경로 수집 |
| iframe 배포 시 세션 쿠키 미전송 | 프록시 인가가 전부 실패 | P5 전 접속 형태 확인 후 `SameSite` 값 결정 (7.7) |
| 부트스트랩 관리자 누락 | **아무도 권한을 못 줌** | 이관 시 관리자 지정 필수 + `app_user.admin_yn` 백도어(6.4) |
| 퇴사자 계정이 살아 있음 | 권한 회수 누락 | **회사 계정 정지가 자동 반영되지 않는다.** 관리 화면에서 비활성화하는 운영 절차 필요 |
| 역할 없는 사용자가 빈 화면을 장애로 신고 | 문의 증가 | 안내 페이지 (7.5-(4)) |

---

## 10. 다른 기능 개선 작업에 미치는 영향

> AIRFLOW / ETL / CDC를 각각 다른 개발자가 개선 중이므로, 이 작업이 그 작업들과 **어디서 겹치는지**를 정리한다.
> 실측 기준: 프론트 `src/api/platform.ts`, `nginx.conf`, `airflow/dags/`, 백엔드 컨트롤러 9개.

### 10.0 ★ 이 조사에서 발견한 설계 구멍 2개

**① 대시보드가 세 엔진 API를 브라우저에서 직접 호출한다**

`src/api/platform.ts`는 우리 백엔드를 거치지 않고 nginx 프록시로 **직접** 나갑니다.

| 함수 | 호출 대상 | 쓰는 화면 |
|---|---|---|
| `listAirflowDags`, `listAirflowDagRuns`, `listAllAirflowDagRuns`, `listAirflowTaskInstances`, `getAirflowTaskLog` | `/airflow/api/v2/**` | 대시보드, ETL 로그 |
| `getNifiRootStatus`, `listRootNifiControllerServices`, `getNifiBulletins` | `/nifi-api/**` | 대시보드, ETL 생성 |
| `listKafkaConnectorsWithStatus`, `getKafkaConnectorTrace` | `/kafka-connect-api/**` | 대시보드 |

문제: 대시보드 메뉴는 `COMMON=READ`만 요구하는데(6.2), 그 화면이 여는 API는 `AIRFLOW`/`NIFI`/`KAFKA` 권한을 요구하게 됩니다. **`COMMON`만 가진 사용자는 대시보드가 반쯤 깨집니다.**

해결(설계에 반영):

- 프록시 인가는 **해당 시스템 READ**를 요구한다.
- 프론트는 권한 없는 위젯을 **에러 대신 숨긴다**(`can('AIRFLOW','READ')` 등으로 렌더 자체를 건너뜀).
- 즉 조회 권한이 부분적인 사용자는 "대시보드가 축소되어" 보인다. 깨진 화면이 아니라 의도된 축소다.

**② `/kafka-connect-api/`가 인가 대상에서 빠져 있었다**

nginx의 이 location은 **자격증명 주입도, 인증도 없이** Kafka Connect REST(`kafka-connect:8083`)로 그대로 나갑니다. NiFi(7개 location, 공유 Bearer)·Airflow(2개 location, 공유 쿠키)와 달리 아무 보호가 없습니다.

→ 4.2의 `KAFKA` 대응 경로에 `/kafka-connect-api/`를 추가하고, P5에서 `auth_request`로 함께 게이팅합니다. Kafka Connect 자체에는 사용자 개념이 없으므로 **프록시 게이팅이 유일한 통제 수단**입니다.

### 10.1 세 영역 공통 — 어디서 파일이 겹치는가

| 단계 | 건드리는 파일 | 충돌 위험 |
|---|---|---|
| P1 | `AuthController`, `AppUser`, `UserService`, `SecurityConfig`, `.env` | **낮음** — 인증 계층에만 국한 |
| P2 | `src/pages/admin/*`(신규), `AdminController`(신규) | **없음** — 전부 신규 파일 |
| P3 | **`AppLayout.tsx`**, **`App.tsx`**, 모든 페이지의 버튼 | **높음** — 세 팀 전부와 겹침 |
| P4 | 컨트롤러 9개에 애노테이션 1줄씩, `SecurityConfig` | 중간 — 메서드 선언부 바로 위 |
| P5b | **`nginx.conf`**(9개 location), NiFi 컨테이너 설정 | **높음** — 아래 10.3 참조 |

P3에서 버튼에 권한 분기가 들어가는 규모(변경 호출 개수 기준):

```
PipelinesPage.tsx   19곳   ← CDC
DashboardPage.tsx   18곳   ← 공통
ConnectionsPage.tsx 12곳   ← CDC
ConsoleFramePage    9곳    ← ETL/AIRFLOW 공용 iframe 래퍼
EtlCreatePage.tsx   8곳    ← ETL
```

`.env` 변경(P1: `ACCOUNT_DB_*` 제거 / P4: `PIPELINE_SERVICE_TOKEN` 추가)은 **각 개발자가 로컬 `.env`를 갱신**해야 컨테이너가 뜹니다. 브랜치를 받는 시점에 공지가 필요합니다.

### 10.2 AIRFLOW 담당에게

| 영향 | 시점 | 내용 |
|---|---|---|
| 직접 호출 경로가 인가를 탄다 | P5b | `platform.ts`의 Airflow 함수 5개가 `/airflow/api/v2/**` → `auth_request` 통과 필요. 전부 GET이라 `AIRFLOW=READ`면 그대로 동작 |
| iframe 자격증명 교체 | P5b | `nginx.conf`의 `location /airflow/`, `location ~ ^/airflow/static/...` 2곳에서 `$airflow_shared_cookie` 제거 → 개인 쿠키 |
| **FAB 역할이 기능을 막는다** | P5b | 개인 계정이 `Viewer`면 Airflow UI에서 **DAG 트리거·일시정지·변수 수정이 막힙니다.** 그 기능을 개선 중이라면 `ETL 관리자` 역할 계정으로 테스트해야 함 |
| DAG 파일 | P4 | `nifi_pipelines_dynamic.py`는 우리 API를 호출하지 않으므로 **영향 없음**(`PIPELINE_API_BASE_URL` 상수를 선언만 하고 미사용) |
| 새 API 호출 추가 시 | P4 이후 | DAG에서 우리 API를 새로 호출하면 **401**. 7.6의 서비스 토큰 헤더를 붙여야 함 |

**요청 사항**: P4 착수 전에 "Airflow DAG에서 `pipeline-api`를 호출하는 코드를 추가했는지" 확인 필요합니다. 동적 생성 DAG이라 파일이 늘 수 있습니다.

### 10.3 ETL(NiFi) 담당에게 — 영향이 가장 큽니다

| 영향 | 시점 | 내용 |
|---|---|---|
| **NiFi 컨테이너 재기동** | P5b | truststore에 프록시 인증서 등록 + `users.xml`/`authorizations.xml` 변경 → **재기동 필요** |
| ⚠️ 캔버스 유실 위험 | P5b | 이 환경은 메모리 압박 하 재기동 시 캔버스가 사라진 이력이 있습니다. **작업 직전 flow 백업 필수**이고, 캔버스 작업 중이면 타이밍을 반드시 조율해야 합니다 |
| nginx location 7개 수정 | P5b | `/nifi/`, `/nf/`, `/nifi-api/`, `/nifi-content-viewer/`, `/nifi-docs/`, `/nifi-extensions/`, `/nifi-websocket/` 전부 `$nifi_shared_bearer` 제거 |
| 개인 계정에 정책 부여 필요 | P5b | 로컬에서 캔버스를 편집하려면 자기 NiFi 사용자에 `modify the component` 정책이 있어야 함. `ETL 관리자` 역할이면 자동 부여 |
| **백엔드 `NifiClient`는 영향 없음** | — | `NIFI_USERNAME`/`NIFI_PASSWORD`(서비스 계정)로 토큰을 받아 쓰므로 그대로 동작합니다. **ETL 생성 마법사, 잡 미러링(V17), 실행 이력(V18), 캔버스 상태 라벨 전부 무영향** |
| 직접 호출 경로 | P5b | `getNifiRootStatus`, `listRootNifiControllerServices`, `getNifiBulletins`가 `/nifi-api/` 직접 호출 → GET이라 `NIFI=READ`면 동작 |
| ETL 생성 화면 | P3 | 메뉴 `required_bits=7` — **조회자에게는 메뉴 자체가 안 보입니다** |

**요청 사항**: P5b 일정을 잡을 때 캔버스 작업이 없는 시점을 골라야 합니다. NiFi 접속 불가가 이 작업의 최대 위험입니다(9장).

### 10.4 CDC(Kafka) 담당에게

| 영향 | 시점 | 내용 |
|---|---|---|
| **DAG 파일 충돌** | P4 | `kafka_pipelines_dynamic.py`가 우리 API를 5곳에서 호출 → 전부 `X-Service-Token` 헤더 추가. **이 파일을 개선 중이면 충돌** |
| 버튼 권한 분기가 가장 많음 | P3 | `PipelinesPage.tsx` 19곳 + `ConnectionsPage.tsx` 12곳 |
| `/kafka-connect-api/` 게이팅 | P5 | 10.0-② — 지금 무인증 개방인 경로가 `KAFKA` 권한을 타게 됨 |
| API 인가 | P4 | `POST /api/pipelines/{id}/{deploy,start,pause,stop,restart}`가 `KAFKA=WRITE` 필요 |
| Kafka Connect 자체 계정 | — | Connect REST에는 사용자 개념이 없습니다. **프록시와 우리 API 경유 통제가 전부**입니다 |

**요청 사항**: P4 착수 전 `kafka_pipelines_dynamic.py`의 최신 상태를 확인해야 합니다. 호출부가 늘었으면 토큰 주입 대상도 늘어납니다.

### 10.5 충돌을 줄이는 진행 순서

| 단계 | 세 팀과의 관계 |
|---|---|
| **P1 · P2** | 파일이 겹치지 않음 → **언제 해도 무방** |
| **P3** | 모든 페이지를 건드림 → 세 팀 작업이 소강일 때, 또는 팀별로 나눠서 순차 적용 |
| **P4** | 컨트롤러 애노테이션 + DAG 파일 → CDC 담당과 사전 조율 |
| **P5b** | NiFi 재기동 → **ETL 담당과 반드시 일정 조율** |

P3을 한 번에 하지 않고 **영역별로 쪼개는 것**도 가능합니다(메뉴 필터만 먼저 → 버튼 분기는 팀별로). 다만 그 사이 기간에는 "메뉴는 안 보이는데 버튼은 눌리는" 상태가 생깁니다.

---

## 부록. 조사 방법 (재현용)

```bash
# nd_suite DB (읽기 전용 — 세션을 read-only로 고정하고 로컬 SQL을 stdin으로 전달)
ssh root@192.168.50.15
docker exec -i pgsql-ndata psql -U admin -d postgres -p 15432 -f -    # ← 포트 15432 주의

# NiFi 현재 인가 설정
docker exec nifi sh -lc 'grep -E "^nifi.security" /opt/nifi/nifi-current/conf/nifi.properties'
docker exec nifi sh -lc 'cat /opt/nifi/nifi-current/conf/users.xml'

# Airflow 인증 관리자
grep -nE 'AUTH_MANAGER|ADMIN_USERNAME' docker-compose.yml
```

nd_suite 코드는 컨테이너 안 `python`의 `zipfile`로 jar 엔트리를 stdout으로만 꺼내고(원격에 파일 생성 없음), 로컬에서 `javap`로 시그니처만 읽었습니다. 프론트는 `manager.jar` 안 `app.*.js.map`의 `sourcesContent`를 확인했습니다.
