# ETL 워크플로우 — 플랜 B: NiFi 보강 (완료 콜백 · staging-swap · 캔버스 재구성)

> **전제**: 플랜 A(범위 A) 완료 — 콜백 **수신부**(`/api/etl/job-runs/by-pg/*`)와 sensor 이중 구조가 이미 준비돼 있다.
> **성격**: 대부분 **NiFi 캔버스 작업**(사용자 주도). 백엔드/Airflow는 원칙적으로 **코드 수정 0**.
> 플랜 A의 R5(완료 추측)·설계서의 원자성 구멍(2026-07-29 사고 클래스)을 닫는 단계다.
> **적용 순서 자유**: B-1/B-2/B-3은 서로 독립이며 job 하나씩 점진 적용 가능하다(전체 일괄 전환 불필요).

---

## B-0. 사전 준비 (백엔드 설정 2가지 — 한 번만)

| 항목 | 내용 |
|---|---|
| **서비스 토큰** | `.env`에 `AUTHZ_SERVICE_TOKEN=<랜덤64>` 설정 (`openssl rand -hex 32`). 콜백은 사람 세션이 없어 `X-Service-Token` 헤더로 인증(기존 `PermissionAspect` 바이패스 경로). 현재 이 값이 **공란**이라 반드시 채워야 함 |
| **NiFi Parameter Context** | 콜백용 파라미터 3개를 공용(또는 job별) 컨텍스트에 등록: `etl.callback.base` = `http://pipeline-api:8081`, `etl.service.token` = (sensitive), `job.pg.id` = 해당 job PG의 id |

> `job.pg.id`를 넣는 이유: NiFi Expression Language에는 "내 그룹 id"를 주는 함수가 없다.
> job PG마다 자기 id를 파라미터로 들고 있어야 InvokeHTTP URL을 조립할 수 있다.
> **수고를 줄이는 선택지**: 백엔드가 NiFi REST로 프로세서를 만들 수 있으므로(마법사에서 이미 사용),
> `POST /api/etl/jobs/{id}/wire-callback` **배선 도우미 API**를 추가하면 아래 B-1 수작업을
> job당 클릭 1번으로 대체할 수 있다(선택 구현 — 백엔드 1개 엔드포인트).

---

## B-1. 완료 콜백 배선 — "추측 → 확정" 전환

### 목표 상태 (job 체인 종단)

```
… → load-dz(PutDatabaseRecord) ──success──▶ InvokeHTTP(콜백:성공)
        │                                        POST #{etl.callback.base}/api/etl/job-runs/by-pg/#{job.pg.id}/complete
        └──failure──▶ (오류 퍼널) ──▶ InvokeHTTP(콜백:실패)
                                         POST …/by-pg/#{job.pg.id}/fail
```

### 수작업 절차 (job 1개당 ~10분)

1. job PG를 연다 → 체인 **마지막 프로세서**(보통 `load-dz-*` PutDatabaseRecord)를 확인
2. **InvokeHTTP "job-complete"** 추가:
   - HTTP Method=`POST`, URL=`#{etl.callback.base}/api/etl/job-runs/by-pg/#{job.pg.id}/complete`
   - Request Headers: `X-Service-Token` = `#{etl.service.token}`, `Content-Type: application/json`
   - Send Message Body=false (건수를 실으려면 body `{"rows": ...}` — 선택)
   - Relationships: `Original`/`Response` 자동종료, `Failure`/`Retry`는 **자동종료 금지** → 재시도 셀프루프(패널티 30s) — 콜백 유실 방지
3. 마지막 프로세서의 `success`를 job-complete에 연결 (기존 success 자동종료 해제)
4. **InvokeHTTP "job-fail"** 추가 (URL만 `/fail`, body에 `{"error": "${...}"}` 선택):
   - 체인 각 프로세서의 `failure` 관계(현재 자동종료 상태)를 퍼널로 모아 job-fail에 연결
5. NiFi 화면에서 job 수동 1회 실행 → 백엔드 로그/`etl_job_run` 확인

### 도착 후 동작 (이미 플랜 A에서 준비됨 — 확인만)

- `complete` 도착 → 열린 run 닫힘(`completion_source='CALLBACK'`) → Airflow sensor 1순위 경로로 즉시 종료
- `fail` 도착 → run FAILED + error_message → **sensor가 태스크를 즉시 실패**시켜 후행 정확히 차단
- 열린 run이 없을 때(순수 NiFi 수동 실행) → 닫힌 run을 새로 만들어 기록(수동 실행도 이력에 남음)
- 중복 콜백 → 200 no-op(멱등)

### 검증
- [ ] Airflow로 실행 → run의 `completion_source='CALLBACK'`, sensor 종료가 poke 1~2회 내
- [ ] 마지막 프로세서를 강제 실패 → `/fail` 도착 → Airflow 태스크 즉시 실패 + 사유 표시
- [ ] pipeline-api를 잠시 내리고 실행 → InvokeHTTP Retry 루프가 콜백을 **유실 없이** 재전송, 그 사이 sensor는 fallback으로도 동작(이중화 확인)

---

## B-2. staging-swap — job 원자성 (2026-07-29 사고 클래스 종결)

### 문제 (설계서 §4)
`truncate-dz`(PutSQL)와 `load-dz`(PutDatabaseRecord)는 **별개 프로세서 = 별개 트랜잭션**.
truncate 커밋 후 load가 실패하면 **실테이블이 빈 채 확정**된다. NiFi는 체인 전체 트랜잭션을 제공하지 않는다.

### 처방: 원자성 지점을 "swap 한 문장"으로 축소

**체인 변경 (전량 적재형)**:
```
현행:  trigger → extract → truncate-dz → load-dz(실테이블)
목표:  trigger → extract → load-stg(dz_x_stg) → swap(PutSQL 1문) → [B-1 콜백]
```

**Postgres (현 싱크) swap — 단일 문장 = 단일 트랜잭션 (DO 블록)**:
```sql
DO $$
BEGIN
  TRUNCATE dz_com001m;
  INSERT INTO dz_com001m SELECT * FROM dz_com001m_stg;
END $$;
```
- DO 블록은 **한 문장**이라 PutSQL 1회 실행 = 전부 성공 or 전부 롤백. 실테이블은 항상 all-or-nothing
- 사전 1회: `CREATE TABLE dz_x_stg (LIKE dz_x INCLUDING ALL);` (job마다)
- load-stg 시작 전에 `TRUNCATE dz_x_stg` 프로세서 1개(스테이징 초기화 — 실패해도 실테이블 무해)
- 증분형이면 TRUNCATE+INSERT 대신 **MERGE**(PG15+) / `INSERT … ON CONFLICT` 를 DO 블록에

**⚠ Oracle 타깃이 생기면** (현재 싱크는 Postgres뿐 — 참고용):
- Oracle의 `TRUNCATE`는 DDL이라 **암묵 커밋** → DO 블록식 원자화 불가
- 대안: ① `ALTER TABLE … EXCHANGE PARTITION`(단일 DDL, 원자적·대용량 권장) ② PL/SQL 익명블록에 `DELETE`+`INSERT`(커밋 없이 — DML만이라 원자적) ③ `MERGE`

**Rollback On Failure 설정 방침** (혼동 주의 — 관계 라우팅과 상충):
| 프로세서 | RoF | 이유 |
|---|---|---|
| **swap(PutSQL)** | **true** | 일시 DB 오류 시 flowfile을 큐에 보존하고 재시도(yield). 실테이블 이미 안전(원자 swap)이라 재시도가 정답 |
| load-stg(PutDatabaseRecord) | false 유지 | failure → B-1 오류 퍼널로 라우팅해야 **실패 콜백**이 나감. (RoF=true면 failure 관계로 안 흘러 콜백 불가 — bulletins로만 감지) |
| DBCP | `Validation query = SELECT 1` | 끊긴 커넥션 재사용 방지 |

### 재실행 안전(멱등) 정리
- 전량형(stg→swap): 재실행 = stg 재구축 + 재swap → **완전 멱등**
- 실패 시: 실테이블은 이전 스냅샷 그대로 → **"오류 나면 원복돼 있고, 다시 돌리면 된다"** 성립
- INSERT 전용 append 경로만 예외 — 유니크 제약/`ON CONFLICT` 필수

### 검증
- [ ] load-stg 도중 NiFi 강제 재시작 → **실테이블 무손상**, 재실행으로 정상화
- [ ] swap 도중 DB 커넥션 절단 → flowfile 큐 보존·재시도, 실테이블 이전 상태 유지
- [ ] 같은 job 2회 연속 실행 → 결과 동일(멱등)

---

## B-3. NiFi 캔버스 재구성 — 체인 → 자식 PG (job 세분화)

### 목적
현재 DW/DZ처럼 **체인 여러 개가 한 PG에 평면 배치**된 구조를, 사용자가 구상한
`IMP > IMP_DAILY > [COM001M][COM002L]…` 계층으로 재구성 → **체인 1개 = job 1개**가 되어
워크플로우 캔버스에서 체인 단위 조합·부분 재시작이 가능해진다(플랜 A의 R10 해소).

### 절차 (그룹 1개당)
1. 상위 그룹(예: `IMP`) 안에 자식 PG `IMP_DAILY` 생성(계층은 자유 — 미러가 어느 깊이든 동기화)
2. 체인별 자식 PG 생성(`COM001M` 등) → 기존 프로세서들을 **선택 → Group** 으로 이동
   (Parameter Context 상속 확인, Controller Service 스코프는 상위에 두면 공유됨)
3. 미러 반영 확인: `POST /api/etl/jobs/sync` → 팔레트에 새 job들 등장
4. 워크플로우 캔버스에서 굵은 노드(DZ 전체)를 지우고 체인 job 노드들로 재배선 → 재게시
5. 옛 굵은 job 행은 미러가 정리(스텝 이동으로 step_count 갱신/그룹 성격 변화)

### 주의
- 이동 중 해당 그룹은 **STOPPED 상태**에서 작업(운영 시간 회피)
- B-1 콜백 파라미터(`job.pg.id`)는 **새 PG id**로 갱신 필요(배선 도우미를 쓰면 자동)
- 재구성 전후로 워크플로우 게시본이 옛 pg_id를 참조하지 않는지 검증 V3가 걸러줌

---

## 적용 로드맵 (권장)

```
job 1개 파일럿:  B-2(stg+swap) → B-1(콜백) → Airflow 실행 검증(CALLBACK 확인)
        ↓ 패턴 확정 후
나머지 job 순차 적용 (신규 job은 처음부터 이 패턴으로 생성 — 마법사 템플릿 갱신 검토)
        ↓ 필요 시
B-3 재구성(체인 세분화)은 업무 단위로 점진
```

## 완료 기준
- [ ] 전체 job의 `completion_source`가 `CALLBACK`으로 집계됨(OBSERVED 잔존 0)
- [ ] 파괴 테스트(중단·절단·재실행)에서 실테이블 무손상·멱등 확인
- [ ] "실패 → 원인 표시 → 수정 → 실패 지점부터 재시작 → 후행 완주" E2E 통과
