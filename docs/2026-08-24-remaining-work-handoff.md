# 남은 작업 핸드오프 (2026-08-24)

다른 PC에서 이어서 작업하기 위한 문서. 브랜치 `msa-integration` 기준.

---

## 0. 먼저 확인할 것

| 항목 | 상태 |
|---|---|
| 브랜치 | `msa-integration` |
| 미푸시 커밋 | `3be2eec front-proxy: 설정 템플릿 없이 기동하면 시끄럽게 죽도록 안전장치 추가` **1개** |
| 작업트리 | 깨끗함 (uncommitted 없음) |
| 로컬 스택 | 전 컨테이너 healthy, 최신 소스로 재빌드·재배포 완료 |

> ⚠️ **push 먼저.** 이 커밋이 원격에 없으면 다른 PC에서 받을 수 없다.
> ```bash
> git push gitlab msa-integration
> ```

### 다른 PC에서 시작하기

```bash
git clone https://gitlab.com/cktnqhd15/datapipeline.git
cd datapipeline
git checkout msa-integration
```

**`.env`는 gitignore 대상이라 리포에 없다.** `.env.example`을 복사한 뒤 아래 값들을 채운다
(현재 작업 PC의 `.env`에서 그대로 옮기는 게 가장 확실하다):

```
ACCOUNT_DB_PASSWORD        AIRFLOW_ADMIN_PASSWORD     AIRFLOW_DB_PASSWORD
AIRFLOW_JWT_SECRET         AIRFLOW_WEBSERVER_SECRET_KEY
METADATA_DB_PASSWORD       NIFI_KEYSTORE_PASSWORD     NIFI_SINGLE_USER_PASSWORD
NIFI_TRUSTSTORE_PASSWORD   PIPELINE_CRYPTO_SECRET     PIPELINE_CRYPTO_SALT
PIPELINE_JWT_SECRET        PIPELINE_SERVICE_TOKEN     TARGET_DB_PASSWORD
```

현재 켜져 있는 플래그(옮길 것):

```env
APP_VERSION=latest
AUTHZ_PROXY_ENABLED=true          # NiFi/Airflow per-user 대행 인증(P5b)
AUTHZ_IDENTITY_SYNC_ENABLED=true  # 역할→NiFi/Airflow 계정 동기화
AUTHZ_ENFORCEMENT_ENABLED=true    # @RequirePermission 인가 강제
```

`AUTHZ_PROXY_ENABLED=true`면 **P5b mTLS 인증서가 필요**하다
(`deploy/p5b-certs/cerebro-proxy.crt|key` — **키는 gitignore라 리포에 없다**).
없으면 cerebroetl-ui의 nginx가 기동 실패한다. 발급/설치 절차는
`docs/p5b-personal-accounts-runbook.md` 참고. 그게 번거로우면 로컬에서는
세 플래그를 `false`로 두고 시작해도 된다(기능 대부분은 그대로 동작).

```bash
docker compose build          # 최초 1회 (이미지 4종)
docker compose up -d
```

---

## 1. MSA 포털 정합 (P4.5) — 🔴 가장 시급

**서버 배포 시 즉시 장애가 나는 유일한 항목.**

### 문제

MSA 포털(192.168.50.30) app02가 `pipeline-api(:18086)`를 **무인증 서버사이드 프록시**로
호출한다. 지금 `AUTHZ_ENFORCEMENT_ENABLED=true`라서 이대로 서버에 올리면
**포털 ETL 섹션 6개 도메인이 전부 401**로 깨진다:
`/etls/connections`, `/dashboard`, `/pipelines`, `/platform`, `/cdc`, `/infra`.

app02의 `PipelineSvc` 주석도 "포털 JWT는 pipeline-api에서 안 먹힌다(다른 issuer)"고
인정하고 있다.

### 확정된 방안 ② (신규 컨테이너 0)

- `POST /api/authz/portal-exchange` 신설 — app02 신뢰(내부망 + 공유 서명키).
  포털 JWT subject → `app_user` **lazy 프로비저닝** → Cerebro JWT 발급.
  프로필(userNm/email)은 교환 토큰 claim으로 받는다
  (Cerebro가 MySQL `ST_USER`에 직접 붙지 않음 → 폐쇄망 단독 배포 유지).
- **포털 역할 → Cerebro 역할 매핑 정책**: 포털은 자체 RBAC(`ST_ROLE`)를 갖고 있어
  역할이 두 벌 병존한다. 권장 — 프로비저닝 시 기본 **ETL조회자**, 쓰기 권한은
  Cerebro 관리자가 승격. 주권 경계: 포털 역할 = ETL 섹션 진입 가부까지,
  리소스 read/write 세부 판단은 Cerebro RBAC가 최종.
- `portal.integration.enabled` 플래그로 on/off. 미설정이면 무해
  (로컬·타 폐쇄망 단독 배포에 영향 없음).

### 주의

- **app02 쪽 토큰 부착 수정과 같은 배포로 묶어야 한다** (cross-repo 조율, 리드타임 필요).
- 전제: `ST_USER.USER_ID == app_user.user_id` (userId 공간 일치).
- 현재 코드에 `portal-exchange`는 **없다**(미착수 확인함).
- MSA 서버는 **읽기 전용 조사만** 허용. 라이브 MSA 서버를 수정하지 말 것.

---

## 2. 통합검증 42건 백로그 (권한과 별개)

출처: `docs/2026-08-20-integration-verification.md`. 데이터정합·안정성 이슈.
권한 관련 항목(S-1/S-2/S-3, M2-10)은 P4로 이미 해소됨.

### 🔴 Tier 0 — 즉시

| # | 항목 | 규모 | 위치 |
|---|---|---|---|
| 1 | **DLQ 무통보 유실** — `errors.tolerance=all`로 실패 레코드를 DLQ로 보내는데 DLQ 감시·알림이 **0건**. 7일 retention 넘기면 복구 불가 | M | `JdbcSinkTemplate.java:83-86`, `AlertEngine` |
| 2 | **알림 채널 토글 ↔ 발송 단절** — 화면에서 EMAIL/SMS를 켜도 실제 발송은 `@Value` 기본값(false)만 본다 → **CRITICAL 메일이 안 나감** | S | `NotificationAdminController` vs `NotificationService.java:39-57,161,163` |
| 3 | 마스킹 대소문자 불일치 → **PII 평문 적재** *(추정 — 착수 전 E2E 확증)* | S | `Debezium*Template` |
| 4 | 마스킹 컬럼 rename 시 조용한 해제 → 평문 주민번호 적재 | M | |
| 5 | 삭제 실패한 **replication slot 고아** → 원천 DB WAL 무한 보유·디스크 포화 | M | `PipelineService.java:195-223` |

**#1, #2 먼저 권장** — 규모 S/M인데 "안전망이 도는 줄 알았는데 안 돌던" 것을 복구하는 효과가 크다.
**#3, #8**은 "추정" 건이라 착수 전 감사문서 §5 회차A E2E로 확증할 것.

### 🟠 Tier 1 — 높음
6. log retention 7일 + 역압 · 7. Kafka RF=1(다중 브로커 전제) · 8. 동시 start+delete 무가드(`@Version` 부재) ·
9. Filebeat 봉투↔타겟 컬럼 암묵 계약 · 10. NiFi skip=성공 오염(추정) · 11. `daily_load_metric` 고아 · 12. 유령 잡

### 🟡 Tier 2 — 중복 (⚠️ MSA 병행배포와 연관)
13. **분산락(ShedLock) 전무** → 2대 이상 배포 시 **이중 발송/이중 제어/이중 적재**.
    포털이 2대 프록시하면 현실화 → **P4.5와 같은 시점에 처리 권장** (현재 코드에 ShedLock 없음, 확인함)
14. renotify 무력 · 15. 억제 후 발송 · 16. 로그 재적재 중복 · 17. 적재 건수 이중 출처

### 🟢 Tier 3 — 관측 사각 / 정지
18~24. 배치 상주 RUNNING(매일 적재 소실) · target 부분 설정 조용한 skip · `max_active_runs` 미설정 ·
verify 오탐 · 스케줄러 collect 집중 · Hikari 풀 고갈 · 죽은 DDL

### ⚪ Tier 4 — 낮음
L 9건(시각 필드·고아 누적·`min_severity` 모순·메뉴 트리 미사용 등) + M2-11 프론트 라우트 가드
(백엔드가 게이팅하므로 실제 우회는 아님 — 방어 심화)

---

## 3. front-proxy 서버 적용 — 코드 완료, 켜기만 남음

감사 로그에 **실제 클라이언트 IP**를 남기는 기능. 로컬에서 검증까지 끝났다.

서버(리눅스)에서 `.env`에 한 줄:

```env
COMPOSE_FILE=docker-compose.yml:docker-compose.realip.yml
```

그리고 평소처럼 `docker compose up -d`. 포트 번호·주소는 그대로(13001 / 18086).

> ⚠️ **Docker Desktop(Windows/Mac/WSL)에서는 켜지 말 것.** host 네트워크 컨테이너의
> 포트를 호스트로 노출하지 않아 공개 포트 접속이 막힌다. **리눅스 서버 전용.**

자세한 배경·검증 기록·되돌리는 법: `docs/audit-client-ip-front-proxy.md`

---

## 4. 설계서 잔여 (커토버 시점)

- **`ST_USER` → `app_user` 1회 이관 스크립트** — MSA 커토버 시 필요. 아직 미작성
- **EXTERNAL 인증자 코드 제거** — 지금은 `authz.provider=LOCAL`이라 휴면 상태.
  안전 롤백용으로 남겨 뒀고, 커토버 검증 후 제거
- **§4.4 객체 단위 권한**(개별 NiFi 프로세서 / 개별 DAG) — **이번 범위 밖**.
  현재는 "시스템 전체 읽기/쓰기"까지만. 테이블 자리만 남겨 둠
- **NiFi 전용 서비스 계정 분리** — 사용자가 **"현행 유지"로 결정**한 항목.
  `admin` 이름 충돌 시 캔버스 감사에서 사람/서비스를 구분 못 하는 좁은 갭

---

## 권장 착수 순서

1. **push** (미푸시 커밋 1개)
2. **P4.5 포털 정합** — 서버 배포 시 바로 깨지는 유일한 항목 + app02 조율 리드타임
3. **DLQ 감시(#1) + 알림 채널 토글(#2)** — 위험 대비 비용이 가장 싸다
4. **분산락(#13)** — MSA 병행배포 전에
5. 나머지 42건은 별개 워크스트림으로

---

## 참고 문서

| 문서 | 내용 |
|---|---|
| `WORK_LOG.md` | 작업 이력 (착수 전 관련 섹션부터 읽을 것) |
| `docs/2026-08-20-integration-verification.md` | 42건 감사 원본 |
| `docs/user-permission-design.md` / `user-permission-decisions.md` | RBAC 설계서·결정사항 |
| `docs/p5b-personal-accounts-runbook.md` | NiFi/Airflow 개인계정·mTLS 배포 런북 |
| `docs/audit-client-ip-front-proxy.md` | front-proxy(실 IP 기록) |
| `docs/cdc-processing-log.md` | CDC 처리 로그 측정·관리·데이터 흐름 |
| `docs/cheatsheet.md` | 포트·접속 주소 요약 |
