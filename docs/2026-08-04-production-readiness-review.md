# 운영 전환 준비 점검 (2026-08-04)

상용 ETL 제품(Informatica / Talend / Fivetran / Airbyte / NiFi Registry)이 공통으로 갖는
기능을 기준으로 이 프로젝트에 없는 것, 고쳐야 할 것을 정리했다.

**이 문서의 범위**: 인증·HTTPS는 이번 논의에서 제외했다(별도 과제로 미룸). 그 외
아키텍처·운영 기능·컨테이너/UI 구성·NiFi↔Airflow 연계를 다룬다.

## 점검 시점의 실측 현황

| 항목 | 값 |
|---|---|
| 컨테이너 | 12개 (NiFi 1.5GB, kafka-connect 1.0GB가 메모리 대부분) |
| UI | 2개 분리 - `cerebroetl-ui`(8페이지), `pipeline-ui`(2페이지) |
| 메타 테이블 | 12개 (Flyway V1~V16) |
| 실패 알림 | 없음 |
| DLQ | 없음 |
| 커넥터 시크릿 | 평문 (`config.providers` 미설정) |
| 감사 기록 | `pipeline_command_history.requested_by` 컬럼은 있으나 전부 NULL |
| 메모리 상한 | 13개 서비스 전부 설정됨 |

---

## 1. 상용 ETL 대비 격차

### 없으면 운영이 안 되는 것

#### 실패 알림 - 가장 시급

`on_failure_callback` / SMTP / Slack / 웹훅 어느 것도 없다. 지금은 **사람이 화면을 봐야만**
실패를 안다. 새벽에 DAG가 죽으면 아침에 발견한다.

상용 제품이 예외 없이 갖는 기능이고, 작업량 대비 효과가 가장 크다.

- Airflow `default_args`에 `on_failure_callback`
- 백엔드 `RUNTIME_MONITOR`가 FAILED를 기록할 때 같이 발송
- 채널은 사내 메신저 웹훅이면 충분

#### DLQ (오류 레코드 격리)

지금은 싱크에서 레코드 하나가 깨지면 **커넥터 태스크 전체가 죽는다.**

`JdbcSinkTemplate`에 4줄이면 된다.

```java
config.put("errors.tolerance", "all");
config.put("errors.deadletterqueue.topic.name", "dlq-" + connectorName);
config.put("errors.deadletterqueue.context.headers.enable", "true");
config.put("errors.log.enable", "true");
```

DLQ 조회 화면까지 만들면 "왜 이 행이 안 들어왔나"에 답할 수 있다.

#### 데이터 정합성 검증 (reconciliation)

**원천과 타겟 건수가 맞는지 확인하는 장치가 전혀 없다.** 지금은 "적재됐다"만 알고
"맞게 적재됐다"는 모른다.

ORA-12170으로 DZ 166만 건을 날렸던 사례가 정확히 이 공백이다.

- 배치: 적재 후 `source count` vs `target count` 비교 태스크
- CDC: 주기적 샘플링 비교
- 불일치 시 알림
- 신규 테이블 `pipeline_reconciliation` 필요

#### 시크릿 외부화

`GET /connectors/{name}/config`가 DB 비밀번호를 평문으로 돌려준다.
`config.providers`(FileConfigProvider 또는 Vault)를 붙이면 커넥터 config에는
`${file:/secrets:pw}` 참조만 남는다. **인증 과제와 무관하게 지금 처리 가능하다.**

### 있으면 운영이 크게 편해지는 것

#### 파이프라인 정의 버전 관리

`pipeline_definition`은 현재 상태만 있고 이력이 없다. "누가 언제 이 매핑을 바꿨나"를
못 본다. `pipeline_definition_history`(변경 전/후 JSON + 변경자 + 시각)가 필요하다.
사고 원인 추적에 결정적이다.

#### 감사 로그 실제 기록

`requested_by` 컬럼은 이미 있는데 전부 NULL이다. 인증을 미루더라도 **Airflow 호출은
`SERVICE`, 화면 호출은 세션 사용자**를 넣는 정도는 지금도 가능하다.

#### 재처리 / 백필

실패하면 처음부터 다시 돌리는 것 외에 방법이 없다. 특정 기간만 다시 적재하는 기능이 없다.

- 배치: `GenerateTableFetch` + 기준값 주입으로 구간 지정
- CDC: 특정 SCN/LSN부터 재시작

#### 데이터 계보(lineage)

"이 타겟 테이블은 어디서 왔나", "이 원천을 바꾸면 뭐가 영향받나"에 답할 수 없다.
`pipeline_definition`에 소스·타겟이 이미 있으므로 **화면만 만들면 되는 수준**이다.

### 이미 잘 되어 있는 것

- **동적 DAG 생성** - 파이프라인 생성 시 DAG가 자동 생성되는 구조는 상용 제품에 밀리지 않는다
- **커넥터 상태 드리프트 자동 복구** - `RUNTIME_MONITOR`의 자동 복구는 꽤 성숙한 설계다
- **실행 구간 추적** - `nifi_processor_run`으로 시작/종료/처리량을 남기는 건 NiFi에 없는
  기능을 직접 만든 것이다

---

## 2. NiFi ↔ Airflow 연계의 구조적 한계

현재 동작:

```
Airflow DAG  --REST-->  NiFi 프로세서 start/stop
             --폴링-->  activeThreadCount 로 "끝났는지" 판단
             --폴링-->  카운터 증가분으로 "몇 건인지" 추정
```

### (1) 완료 판정이 추측이다

NiFi에는 "이 작업이 끝났다"는 개념이 없어서 **스레드 수가 0이 되면 끝난 것으로 간주**한다.

- 처리 중 일시적으로 0이 될 수 있음 -> **조기 완료 오판**
- 큐에 남아 있는데 스레드만 0일 수 있음

`nifi_processor_run` 주석에도 "최대 15초 오차가 있는 추정값"이라고 적혀 있다.

**대안**: NiFi 플로우 마지막에 완료 신호 프로세서를 둔다.

```
마지막 PutDatabaseRecord -> InvokeHTTP -> POST /api/pipelines/{id}/complete
```

Airflow는 추측 대신 명시적 신호를 기다린다. 폴링도 없어지고 정확해진다.

### (2) 건수가 카운터 증가분 추정이다

`INSERT updates performed` 카운터의 델타를 재는 방식이라 NiFi 재시작이나 동시 실행 시
어긋난다. 실제로 델타 계산 버그로 0건이 잡힌 적이 있다.
위 완료 신호에 건수를 실어 보내면 추정이 아니라 실측이 된다.

### (3) 스케줄이 Airflow Variable에 흩어져 있다

화면에서 만든 파이프라인의 스케줄이 `"{dag_id}__schedule"` Variable에 따로 저장된다.
파이프라인을 지워도 Variable은 남고, 어떤 파이프라인이 언제 도는지 한눈에 안 보인다.

**대안**: `pipeline_definition`에 `schedule_cron` 컬럼을 추가하고 DAG가 그걸 읽게 해서
단일 소스로 만든다.

---

## 3. 컨테이너 / UI 통합

### UI 통합 - 하는 게 맞다

```
cerebroetl-ui   8페이지  (대시보드·연결·파이프라인·ETL로그·CDC로그·생성·로그인·콘솔)
pipeline-ui     2페이지  (연결·파이프라인)   <- 전부 중복
```

`pipeline-ui`(`web/frontend`)는 `cerebroetl-ui`의 부분집합이다. 유지할 이유가 없고 오히려:

- 13000 포트가 인증 없이 열려 있어 보안 구멍
- 같은 기능을 두 곳에서 고쳐야 함

**제거를 권한다.** 컨테이너 1개, 이미지 1개, 포트 1개가 줄어든다.

### 컨테이너 통합 - 신중해야 한다

| 대상 | 판단 |
|---|---|
| `pipeline-ui` 제거 | 즉시 가능 - 중복이고 위험 요소 |
| `airflow-init` | 이미 1회성 종료 컨테이너 (문제없음) |
| `target-db` | `profiles: ["poc"]`라 운영에선 안 뜸. 확인만 |
| airflow 3개 분리 | **합치면 안 됨** - Airflow 3.x 공식 아키텍처 |
| kafka + kafka-connect | 분리 유지 - 재시작 영향 범위가 다름 |
| metadata-db | 유지 - 이미 database만 분리(airflow/pipeline_meta)해서 절약 중 |

**실질적으로 줄일 수 있는 건 `pipeline-ui` 하나다.** 나머지는 이미 잘 정리돼 있다.

메모리는 NiFi 1.5GB + kafka-connect 1GB가 지배적이다. 운영에서 부담되면 컨테이너 병합이
아니라 **JVM 힙 튜닝**이 답이다.

---

## 4. 권장 우선순위

| 순위 | 항목 | 규모 | 근거 |
|---|---|---|---|
| 1 | 실패 알림 | 소 | 없으면 장애를 모름 |
| 2 | DLQ + 오류 조회 화면 | 소~중 | 레코드 하나로 전체가 멈춤 |
| 3 | `pipeline-ui` 제거 | 소 | 중복 + 보안 구멍 |
| 4 | 정합성 검증 | 중 | 데이터 유실 재발 방지 |
| 5 | NiFi 완료 신호 방식 전환 | 중 | 추측 -> 확정. 연계의 근본 개선 |
| 6 | 스케줄을 DB로 일원화 | 소~중 | Variable 분산 해소 |
| 7 | 시크릿 외부화 | 중 | 인증과 별개로 가능 |
| 8 | 정의 이력 + 감사 기록 | 중 | 추적성 |
| 9 | 재처리/백필 | 대 | 있으면 좋지만 후순위 |

1~3번은 각각 하루 이내이고 효과가 즉각적이다. **4~6번이 이번 운영 전환의 핵심.**

---

## 5. 필요한 신규 테이블

여유 DB를 쓸 수 있다는 전제.

```sql
pipeline_definition_history   -- 정의 변경 이력 (before/after JSON, 변경자)
pipeline_reconciliation       -- 정합성 검증 결과 (source_cnt, target_cnt, diff, checked_at)
pipeline_alert_rule           -- 알림 대상/채널/조건
pipeline_dlq_record           -- DLQ 토픽 소비 결과 적재
```

`pipeline_definition`에 컬럼 2개 추가:

```sql
schedule_cron   varchar(100)   -- Airflow Variable 대체
owner_group     varchar(100)   -- 알림 수신자 결정용
```

---

## 별도 과제 (이번 범위 밖)

- **인증/인가** - 프록시에 인증 가드가 없어 로그인 없이 Airflow/NiFi 접근 가능.
  API도 `anyRequest().permitAll()`. 선행 조건은 DAG용 서비스 토큰 도입
- **HTTPS** - nginx가 `listen 80`/`8080`만. JWT와 비밀번호가 평문으로 흐름
- **NiFi Single User** - 계정이 물리적으로 하나뿐이라 사용자별 권한 분리·감사 추적 불가
