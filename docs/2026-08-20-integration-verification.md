# 전체 통합 검증 결과 (2026-08-20)

> 대상 브랜치: `msa-integration` (HEAD `d39e2a6`, 미커밋 authz WIP 포함)
> 요청서: `docs/2026-08-20-integration-verification-request.md`
> 방법: **1단계 정적 대조**(스택 기동 없이 코드·DDL·compose·DAG·설정 전수 대조). 2단계 실기동 E2E는 §5 후보 목록으로 산출.
> 근거는 모두 실제 파일을 열어 확인한 `file:line`. 정적으로 단정 불가한 것은 **(추정)** 표기.

---

## 0. 요약

정적 대조에서 **42건**을 확인했다. 데이터 관점 심각도 기준(H1 유실 / H2 오염 / M1 중복 / M2 정지 / L 지연) + 보안 우회.

| 심각도 | 건수 | 대표 |
|---|---|---|
| **H1 유실** | 3 | DLQ 무통보 유실 · RF=1 볼륨손상 · retention 기본7일 |
| **보안 H(우회)** | 3 | `/api/admin/users` 무방비(관리자 생성) · DLQ 재주입 자기승인 · Airflow 컨트롤러 무방비 |
| **H2 오염** | 10 | 마스킹 대소문자/rename 조용한 해제 · Filebeat 봉투 계약 · 삭제 실패 slot 고아 · 동시 start+delete · 미러 유령잡 · 타입 불일치 2건 |
| **M1 중복** | 6 | 분산락 부재(2대 이중발송) · renotify 무력 · 억제 후발송 · 로그 재적재 중복 · 건수 이중출처 · V24 이력손상 |
| **M2 정지** | 13 | **Airflow 대시보드·콘솔 화면 전면 불능(per-user 세션 429 스톰)** · **백엔드 Airflow 동기화·알림(nginx 401, 조치완료)** · 채널 토글↔발송 단절 · 스케줄러 collect- 집중 · verify 오탐/조용한 skip · Variable 오타 배치소실 · 포털 401 · 죽은 DDL |
| **L 지연·기타** | 9 | 시각 필드 · 고아 누적 · min_severity 모순 · 메뉴트리 미사용 등 |

### 가장 위험한 5건 (즉시 판단 필요)
1. **DLQ 무통보 유실 (H1)** — `errors.tolerance=all`인데 DLQ를 감시·알림하는 코드가 전무. 싱크는 초록불인데 타겟 데이터가 조용히 비고, 기본 7일 retention을 넘기면 복구 불가.
2. **`/api/admin/users` 완전 무방비 (보안 H1)** — 인가를 켜도 `@RequirePermission` 미부착이라, 무토큰 호출자가 관리자 계정을 생성/탈취할 수 있음.
3. **마스킹 조용한 해제 (H2)** — 대소문자 불일치 또는 컬럼 rename 시 Debezium 마스킹 술어가 매칭 실패 → PII가 평문으로 타겟에 적재. 오류 없이 진행.
4. **파이프라인 삭제 실패 시 replication slot 고아 (H2)** — 외부 정리 예외를 삼키고 메타를 먼저 지워, 남은 slot이 원천 WAL을 무한 보유 → 원천 디스크 포화.
5. **알림 채널 토글이 실제 발송과 단절 (M2)** — 화면에서 EMAIL을 켜도 dispatch는 앱 프로퍼티(기본 false)만 봐서 CRITICAL 메일이 아무에게도 안 감. 관측 사각지대.

### 요청서 가정 정정 (대조 중 확인)
- **`@Scheduled`는 21개가 아니라 17개**, 스케줄러 미지정은 8개가 아니라 **12개**(§본문 M2-2 표). 폴백은 controlPlane이 아니라 `collect-`(taskScheduler)로 몰림 → "느린 작업이 알림 발송(5초)을 밀어낸다"는 가설은 코드상 **불성립**(알림 발송은 명시적 controlPlane). 대신 `collect-` 4스레드에 12작업이 몰려 **장애 파이프라인 복구(`KafkaPipelineStateSynchronizer`)가 지연**되는 것이 실제 위험.
- **V24 중복은 라이브 DB에는 없음** — `flyway_schema_history`는 installed_rank 24=V24 단일이고 V47/V48은 rank 41/42로 정상. 중복은 `9e34dea`~`8740c4f^` **중간 커밋 창**에만 존재했고 `8740c4f`의 개번으로 현 HEAD에서는 해소됨. 위험은 그 창에서 배포한 환경에 한정.
- 2026-08-04 리뷰의 3대 지적(실패알림/DLQ/정합성)은 이후 V30~V46으로 **구현됨**. 단 **완결성에 구멍**: DLQ는 적재는 되나 감시·알림이 없고(§H1-1), NiFi 완료 판정은 여전히 카운터 추측(§H2-9, 리뷰 (1)(2) 미개선), 스케줄은 여전히 Airflow Variable 산재(§M2-6, 리뷰 (3) 미개선 — `pipeline_definition.schedule_cron` 미도입).

---

## 1. H1 — 유실 (소스에 있는데 타겟에 영영 없음)

| # | 항목 | 축 | 근거 (file:line) | 실패 시나리오 | 심각도 | 수정 제안 |
|---|---|---|---|---|---|---|
| H1-1 | **DLQ 무통보 유실** | B | `JdbcSinkTemplate.java:83-89` (`errors.tolerance=all`, `dlq.pipeline-{id}`), `AlertEngine.java:37-161` (규칙 10종에 DLQ 감지 0건, grep 확인), DLQ는 `DlqReadService`가 UI 요청 시 임시 컨슈머로만 읽음, DLQ retention 미지정 | 제약위반·타입깨짐 행 1건 → 싱크는 안 멈추고 DLQ로 우회, 커넥터 RUNNING 유지 → 대시보드 초록불 → 7일 내 아무도 DLQ 탭을 안 열면 브로커 retention이 삭제 → 원천엔 있고 타겟엔 영영 없는 행, 복구 수단 소멸. `errors.log.include.messages=false`(:89)라 로그에도 원문 없음 | **H1** | DLQ end-offset 증가분을 주기 수집해 `DLQ_GROWTH` 알림 규칙 추가 + DLQ 토픽 retention을 replay 승인주기보다 길게 명시 |
| H1-2 | **Kafka RF=1 단일 브로커 볼륨 손상** | B | `docker-compose.yml:92`(OFFSETS RF=1), `:144-146`(`_connect-configs/offsets/status` RF=1), `JdbcSinkTemplate.java:85`(dlq RF=1), `:77-90`(KRaft 1노드) | `kafka-data` 단일 볼륨 손상 시 ① `_connect-offsets` 소실 → Debezium SCN/LSN 위치 상실 → 재스냅샷(중복) 또는 건너뜀(유실) ② `_connect-configs` 소실 → 커넥터 통째로 사라짐(메타DB엔 남아 상태 불일치) ③ `__consumer_offsets` 소실 → 싱크 재소비/중복. 복제본 없어 복구 불가 | **H1** | 다중 브로커 전제로 RF≥3·`min.insync.replicas=2`, 최소한 브로커 볼륨 스냅샷/백업 정책 문서화 |
| H1-3 | **log retention 기본 7일 의존 + 역압** | B | `docker-compose.yml:82-102`(kafka env에 `log.retention.*`·`retention.bytes`·`min.insync.replicas`·`unclean.leader.election` 전부 부재), CDC 소스는 sink STOP 중에도 계속 적재(`PipelineDeployService.java:289-291`), kafka-data 볼륨 디스크 상한 없음 | 타겟 DB 장기 장애로 sink/NiFi가 7일 이상 정지 → 미소비 CDC 세그먼트가 브로커에서 만료 삭제 → 그 구간 영구 유실. 또는 소스 폭주로 consumer lag → 토픽이 디스크를 무한 점유 → 호스트 포화 → 브로커 다운(전 파이프라인 정지). `CDC_LAG`(임계 5만, `AlertEngine.java:151`)·`SERVER_DISK`(80%, :149)는 관측만 제공하고 흐름 제어 없음 | **H1**(조건부) | 데이터 토픽 `retention.ms/bytes` 상한을 소비지연 허용치 기준으로 명시, 디스크 임계 시 source 자동 pause |

---

## 2. 보안 H — 인가 우회 (인가를 켜도 남는 진짜 결함)

> 전제(정지 조건, 결함 아님): `application.yml:71-75` `authz.enforcement.enabled` 기본 **false**, `service-token` 공란 → off면 `PermissionAspect.java:68-70`이 즉시 return하여 모든 `@RequirePermission` 무효 + `SecurityConfig` permitAll. 아래 3건은 **스위치를 켜도 무방비 컨트롤러라 고쳐지지 않는다**. (권한 코드 집합 자체는 3곳 일치 — `SystemCode.java:17-23` = `V47:111-120` = `authz.ts:4`.)

| # | 항목 | 축 | 근거 (file:line) | 실패 시나리오 | 심각도 | 수정 제안 |
|---|---|---|---|---|---|---|
| S-1 | **`/api/admin/users` 완전 무방비 (관리자 생성/탈취)** | A9 | `AdminUserController.java:24-26`(`@RequestMapping("/api/admin/users")`에 `@RequirePermission` 없음), `SecurityConfig.java:53`(`anyRequest().permitAll()`). 프론트는 게이트된 `/admin/accounts`만 호출(`authz.ts:184-217`), `/api/admin/users`는 호출 코드 없는 레거시 잔존 | 인가를 켜도 애스펙트 미부착이라 무토큰 호출자가 `POST /api/admin/users {adminYn:"Y"}`로 관리자 생성(`:45-66`), `PUT /{id}/password`로 임의 계정 비번 재설정+잠금해제(`:68-81`), `DELETE /{id}`로 삭제 가능. `AdminAccountController`(ADMIN WRITE)를 잠근 의미 무력화 | **보안 H1** | 클래스에 `@RequirePermission(system=ADMIN, bits=WRITE)` 부착 또는 중복 컨트롤러 폐기 |
| S-2 | **DLQ 재처리가 KAFKA WRITE 아닌 "인증만"으로 열림 + 자기승인** | A9 | `CdcLogController.java:27`(`@RequirePermission` 없음), `SecurityConfig.java:51`(`/dlq/replay-requests/**`를 `authenticated`로만). `POST /dlq/replay-requests`(:72-76), `POST /{id}/approve`(:78-82) | `ROLE_ETL_VIEWER`(KAFKA=READ, `V47:118`)가 DLQ 재처리를 **요청하고 스스로 승인**해 죽은편지 레코드를 재주입 → 중복/오염. 요청·승인 동일인이라 직무분리도 깨짐 | **H2** | 재처리/승인 메서드에 `@RequirePermission(system=KAFKA, bits=WRITE)`(승인은 별도 역할 권장) |
| S-3 | **Airflow 컨트롤러 전부 무방비 → AIRFLOW 권한이 강제단에서 고아** | A9 | `AirflowDagCatalogController.java:19-21`, `AirflowDagAlertController` 둘 다 `@RequirePermission` 없음(AIRFLOW 애노테이션 grep 0건). `POST /sync`(:46), `DELETE /{dagId}`(:60), `PATCH /{dagId}/monitoring`(:71) | DB(`V47:113,118`)·프론트(`authz.ts:4`)·메뉴(`V47:126-127`)에 AIRFLOW 권한이 정의됐으나 검사하는 백엔드 컨트롤러가 0 → 무권한/무토큰이 DAG 카탈로그 삭제·모니터링 임계 변경으로 알림을 무력화 | **H(우회)** | 두 컨트롤러에 `@RequirePermission(system=AIRFLOW)` 부착 |

---

## 3. H2 — 오염 (타겟에 틀린 값 / 사실상 유실)

| # | 항목 | 축 | 근거 (file:line) | 실패 시나리오 | 심각도 | 수정 제안 |
|---|---|---|---|---|---|---|
| H2-1 | **소스 마스킹·제외 컬럼이 카탈로그 대소문자와 안 맞으면 조용히 무효** | A5 | `DebeziumOracleTemplate.java:52,73,76-85`(include/mask/exclude 모두 입력 대소문자 그대로), `DebeziumPostgresTemplate.java:25,42,45-54` 동일. 대조: `ConnectorNaming.topicName:31-40`은 Oracle→대문자/PG→소문자 정규화, `PipelineConsistencyService.java:58-59`도 규칙을 앎 — 소스 템플릿만 누락 | 카탈로그와 다른 대소문자로 컬럼 지정(Oracle에 소문자 등) → 마스킹 술어 `schema.table.column`이 카탈로그와 매칭 실패 → 마스킹 조용히 미적용, **PII 평문으로 타겟에 흐름**. exclude도 미적용되어 배제 컬럼 유출. 술어 불일치를 Debezium이 오류로 안 봐 무증상 (술어 대소문자 민감도 **추정**, 코드 비대칭 확인함) | **H2** | 소스 템플릿에서 schema/table/column을 `ConnectorNaming`과 동일 규칙으로 정규화 후 목록 생성 |
| H2-2 | **마스킹/제외 컬럼 rename 시 조용한 해제** | B8 | `DebeziumPostgresTemplate.java:50-54`, `DebeziumOracleTemplate.java:81-82`(배포 시점 문자열로 고정, 이후 재대조 없음), sink `schema.evolution=basic`(`JdbcSinkTemplate.java:39`)로 신규 컬럼 자동 추가 | 마스킹 대상 `ssn`→`social_no` rename → Debezium이 `...ssn`을 못 찾아 마스킹 미적용, 새 컬럼이 평문으로 CDC → sink가 컬럼 자동 추가 → **평문 주민번호가 타겟에 적재**. 오류 없이 진행 | **H2** | 배포/주기적으로 원천 `information_schema`와 mask/exclude 목록을 대조, 미매칭 시 파이프라인 정지+알림(fail-closed) |
| H2-3 | **Filebeat 로그 봉투 ↔ 타겟 테이블 컬럼: 암묵 계약** | A6 | `FilebeatConfigRenderer.java:44-63`(고정 4필드 `{message, log_timestamp, source_file, agent_host}`), 싱크 `JdbcSinkTemplate.java:70-73`(`insert.mode=insert`, `primary.key.mode=none`, `table.name.format=targetSchema.targetTable`), **타겟 컬럼 생성·검증 코드 어디에도 없음**(grep) | 타겟 테이블 컬럼이 4필드와 다르거나 봉투에 없는 NOT NULL 컬럼이 있거나 싱크 계정에 DDL 권한이 없으면 → 모든 레코드가 조용히 `dlq.pipeline-{id}`로 빠짐, 커넥터는 RUNNING이라 UI상 정상. 계약이 렌더러 주석 외 명세 없음 | **H2** | 4필드 봉투 계약을 문서화하고 로그 파이프라인 deploy 시 타겟 컬럼 존재/타입 검증(또는 사전 DDL) |
| H2-4 | **삭제 실패 시 추적 불가 고아 (replication slot → 원천 WAL 무한보유)** | B4 | `PipelineService.java:198-223` — `kafkaConnectClient.delete` 예외 삼킴(:205-208), `PostgresReplicationCleanupService.cleanup` `SQLException` 삼킴(:68-70), 직후 :217·:222에서 메타(connector 행·definition) 무조건 삭제 | 삭제 순간 Kafka Connect 일시 무응답 → 커넥터 delete 실패(무시) → 메타 삭제 → 재시도 근거 소멸 → 남은 Postgres replication slot이 active로 남아 **원천 WAL을 계속 붙잡아 원천 디스크 서서히 포화**. WORK_LOG §6은 첫 시도 성공만 검증 | **H2** | 외부 정리 성공을 확인한 뒤에만 메타 삭제, 또는 `cleanup_pending` 원장에 남겨 재시도 |
| H2-5 | **동시 start + delete 무가드** | B5 | `withPipelineLock`은 `PipelineDeployService.java:613-621`의 JVM 로컬 `ReentrantLock`이며 deploy/resume/stop만 사용, `PipelineService.delete()`(:198)는 미취득. `@Version` 0건(엔티티 전체 grep), `pipeline_flow_state`(V28)는 KPI 표시용 read-only | resume과 delete 동시 도착 → resume이 source를 RUNNING으로 올린 직후 delete가 커넥터/토픽/메타 제거 → **메타 없는 RUNNING source 커넥터(데이터가 추적 없이 흐름)** 또는 삭제된 definition을 resume save가 되살리는 유령행. 다중 인스턴스면 deploy/resume/stop 상호배제도 붕괴 | **H2** | 상태 전이를 DB 낙관적 락(`@Version`)+허용 전이 검사로 강제하고 delete도 동일 락 경로에 편입 |
| H2-6 | **파이프라인 삭제 시 `pipeline_daily_load_metric` 고아 → 대시보드 오염** | A7 | `V9__create_pipeline_daily_load_metric.sql:6-16`(pipeline_key VARCHAR, FK 없음), `PipelineService.java:199-223`(삭제 경로 미포함), `RetentionService.java:38-44`(화이트리스트 없음) | 파이프라인 삭제해도 일자별 적재 row가 영구 잔존, 보존정리 대상도 아님 → 대시보드 "일자별/파이프라인별 적재 건수"에 삭제된 파이프라인이 계속 표시 | **H2** | pipeline_key로 삭제 훅 추가 또는 retention 화이트리스트 편입 |
| H2-7 | **한 번에 3개 초과 잡 소멸 시 유령 잡이 대시보드 오염** | A8 | `NifiJobMirrorService.java:368-374`(missing>3이면 삭제표시 보류), 대시보드는 `EtlJobRepository.java:13` `findByDeletedAtIsNull...`로 읽음 | 정당한 대량 삭제(운영자가 4개 잡 정리)를 캔버스 유실과 구분 못 해, 실체 없는 잡이 `deleted_at=NULL`로 영구 잔존 → 대시보드에 유령 잡 노출, 자동 해소 경로 없음(수동 DB 수정) | **H2** | 임계 초과 시 관리자 확인 큐로 승격, 또는 연속 N주기 미검출 시 삭제 확정 |
| H2-8 | **NiFi 완료 추측 오염 (skip=성공)** | B9 | `nifi_pipelines_dynamic.py:481-557` `verify_target_db_landing`(타겟 rowcount 근사, count=0 & ERROR bulletin/invalid 없으면 `AirflowSkipException`=실패 아님), UPSERT UPDATE 경로 적재는 카운트 누락(`:42-46`) | 원천 장애로 데이터가 failure 관계 자동종료(폐기)돼 ERROR·invalid 안 남는 조용한 드롭 → landing count=0 → **skip=성공으로 기록** → `etl_job_run`·대시보드 적재건수가 "정상인데 0건"으로 오염, 알림 미발화 | **H2 (추정)** | skip과 "검증 불가"를 분리해 count=0 케이스를 별도 상태로 |
| H2-9 | **프론트 `DlqRecordDetail.pipelineName`을 백엔드가 안 줌** | A12 | `cdcLogs.ts:47`(`DlqRecordDetail extends DlqRecordEntry`→`pipelineName:string` :37 상속), 백엔드 `DlqRecordDetailResponse`에 `pipelineName` 필드 없음 | DLQ 상세 모달의 파이프라인명이 `undefined`(빈칸). 타입이 non-null `string`이라 컴파일 경고도 없음 | **H2** | `DlqRecordDetailResponse`에 `pipelineName` 추가 또는 프론트 상세 타입에서 상속 제외 |
| H2-10 | **NiFi 실행로그 `rootGroupName`이 항상 `jobName` 값** | A12 | `NifiExecutionLogResponse.from(entity, jobName)`이 `jobName`을 (jobName, rootGroupName) 두 위치에 전달, 프론트는 둘을 별도 필드로 구분(`platform.ts:11-12`) | root 그룹명을 기대하는 화면 자리에 잡 이름이 표시(타입 일치, 값 오류) | **H2** | `from` 매핑에서 rootGroupName에 실제 그룹명 전달 |

---

## 4. M1 중복 · M2 정지 · L 지연

### M1 — 중복 (재처리로 중복 발생)

| # | 항목 | 축 | 근거 (file:line) | 실패 시나리오 | 심각도 | 수정 제안 |
|---|---|---|---|---|---|---|
| M1-1 | **`@Scheduled` 17개 분산락(ShedLock) 전무 → 2대 시 이중발송/제어/적재** | B | `build.gradle`에 ShedLock 없음, 분산락 0건. **알림 outbox에 `FOR UPDATE SKIP LOCKED` 없음** `NotificationService.java:145-149`. 이중제어 `KafkaPipelineStateSynchronizer.java:75`, 이중적재 `KafkaPipelineMetricScheduler:50`·`NifiPipelineMetricScheduler:121,186` | 포털이 공유 pipeline-api를 프록시하며 가용성 위해 2대 기동 시 17개 스케줄러가 양쪽 동시 발화 → EMAIL/SMS **이중 발송**, 커넥터 이중 restart(상태 플래핑), 지표 2배 적재(대시보드 오염) | **M1**(외부채널 H2) | ShedLock(metadata-db) 도입 또는 리더 인스턴스 1대만 스케줄 활성화, outbox에 `SKIP LOCKED` |
| M1-2 | **renotify 정책이 시드 규칙 전체에서 무력** | A11 | `AlertEngine.java:228`(`notify_max` 기본 1), `:229`(`if notified<max`), 시드 규칙 params에 `notify_max` 없음(`:91,108,127,150-161`), `notify_count`는 최초 발화 시 이미 1(`:68`) → `1<1=false` | 지속 CRITICAL이 미확인으로 계속 열려 있어도 최초 1회만 통지, 30분 재알림이 영원히 안 옴. 첫 통지를 놓치면 안전망 없음 | **M1** | 시드/기본 `notify_max`를 2 이상, 또는 `renotify_seconds>0`이면 무제한 |
| M1-3 | **연쇄억제가 최초 통지 폭주는 못 막음 (억제가 발송 이후)** | A11 | `evaluate()` 순서: 규칙이 먼저 `notifyFired`(`:798`)로 아웃박스 적재 → 그 다음 `evaluateChainSuppression()`(`:198,241-288`)이 `suppressed_by`만 세팅 | 브로커 장애로 커넥터 50개 동시 FAILED → 50건이 각각 이미 발송 적재된 뒤에야 대표 1건 남기고 접힘 → 대기열 화면만 정리, 통지 채널로는 50건 그대로 | **M1**(기본 EMAIL off라 IN_APP 50건 한정) | 억제 대상의 미발송(PENDING) delivery를 SUPPRESSED로 취소, 또는 `notifyFired` 전에 억제 판정 |
| M1-4 | **로그 파이프라인 재적재 시 중복 행** | B1 | 로그 sink `insert.mode=insert`+`primary.key.mode=none`(`JdbcSinkTemplate.java:70-71`). (대조: CDC sink는 `upsert`+`record_key` `:37-38`로 멱등) | Kafka/Connect/Filebeat 재시작 시 at-least-once 재전송이 로그 테이블에 **중복 행**으로 남음. + WORK_LOG line 293의 알려진 제약(ListFile+FetchFile가 파일 수정 시 전체 재적재)과 겹침 | **M1** | 로그 파이프라인에 `message+offset` 기반 dedup 키/PK 도입 |
| M1-5 | **적재건수 이중 출처 (DAG asset vs 백엔드 롤업)** | A1 | DAG가 `outlet_events[asset].extra={count}`로 건수 기록 + docstring이 `/api/v2/assets/events` 조회 주장(`kafka_pipelines_dynamic.py:31-32,157-158`), 그러나 이를 읽는 코드 0건(grep). 실제 대시보드는 `pipeline_daily_load_metric`(`KafkaPipelineMetricScheduler.java:50-85` delta / NiFi 카운터) 독립 경로 | NiFi UPSERT 실행 → DAG는 UPDATE분 못 세어 count=0 skip, 대시보드는 INSERT-counter라 다른 수치 → DAG 로그와 대시보드 건수가 어긋나고 DAG 계산값은 사장 | **M1**(분기) | 대시보드 건수의 단일 출처를 백엔드 롤업으로 못박고 DAG asset 주장 제거, 또는 `/assets/events` 소비자 구현 |
| M1-6 | **V24 이중 버전이 만든 Flyway 이력 손상** | A7 | `V24__local_auth.sql`(8/18) + `V24__add_pipeline_column_policy.sql`(9e34dea, 8/19) 동시 존재, `8740c4f`가 개번(→V43~V46). 현 HEAD·라이브 DB는 해소됨 | `9e34dea`~`8740c4f^` 창에서 배포한 환경은 "duplicate version 24"로 Flyway 기동 거부, 그 창에서 거버넌스 마이그레이션을 V22~V25로 이미 적용했다면 개번 후 version/checksum 불일치로 다음 migrate 실패 | **M1** | 해당 환경은 `flyway repair` 또는 history rank 수동 정정 (현 HEAD 신규 배포는 무관) |

### M2 — 정지 (멈췄는데 아무도 모름 — 관측 사각지대)

| # | 항목 | 축 | 근거 (file:line) | 실패 시나리오 | 심각도 | 수정 제안 |
|---|---|---|---|---|---|---|
| **M2-0** | **Airflow 대시보드 DAG 동기화·알림이 nginx 401로 전면 불능 (라이브 재현·조치 완료)** | A2/A8 | `AirflowDagRunClient.java:22`(base URL 기본값 `http://cerebroetl-ui/airflow` = 브라우저 nginx 프록시), `:26-29`(인증 헤더 없음). 그 경로 `nginx.conf:95-115`는 P5b에서 `auth_request /internal/authz-airflow`(`:112`)로 **브라우저 사용자 세션 쿠키**를 요구하도록 바뀜(`:47-57`). 백엔드 서버-투-서버 호출은 세션 쿠키가 없어 auth_request 단계에서 막힘 → Airflow 도달 전 nginx가 401. **라이브 확인**: `GET /airflow/api/v2/dags`(쿠키없음)→401, pipeline-api 로그 `AirflowDagAlertScheduler … 401 Unauthorized … nginx/1.31.4` 매 주기 반복 | 같은 `AirflowDagRunClient`를 쓰는 두 기능이 동시에 죽음: ① `AirflowDagCatalogSyncService.java:45` 대시보드 DAG 동기화 실패 → 미러 DB(마지막 갱신 8/20)만 조회 → 새로 만든/삭제한 DAG가 화면에 반영 안 됨 ② `AirflowDagAlertScheduler`(V42) 매 주기 401 → **Airflow Job 실패/연속실패/미실행 알림이 전면 미동작**(관측 사각지대) | **M2** (알림 전면 불능이라 실질 상향) | 백엔드는 브라우저 per-user 프록시 대신 `airflow-apiserver:8080` 직접 호출 + 서비스 admin Bearer JWT(`/airflow/auth/token`, `AirflowUserSyncService.adminToken()`와 동일 방식) 부착. **본 검증에서 조치 적용함(§7)** |
| **M2-0b** | **Airflow 대시보드·콘솔 화면이 per-user 세션 429 스톰으로 전면 불능 (라이브 재현, 미조치)** | A2/A10 | **경로가 M2-0과 다름**: 대시보드 `listAirflowDags()`/`listAllAirflowDagRuns()`는 `/airflow/api/v2/*`를 **브라우저가 직접**(백엔드 아님) 호출(`platform.ts:342-348`). nginx `/airflow/`는 **요청마다** `auth_request`(`nginx.conf:112`) → `ProxyCredentialController`(`:66`) → `AirflowUserSyncService.userSessionToken`(`:163`) → **FAB 폼로그인 GET+POST /auth/login/**(`:185-199`). `userSessionToken`에 **single-flight 없음**(`sessionCache`만, lock 부재 `:52,168-175`). Airflow FAB 로그인 제한 기본 **5 per 40s**(compose 미오버라이드). 세션 실패 시 `ProxyCredentialController.java:70` **502** | 콘솔 iframe(생성/관리) 또는 대시보드를 열면 `/airflow/*` 요청 수십 개가 동시 발생 → 각자 폼로그인 → 429 → 토큰 캐시 안 됨 → **영구 429 스톰** → 502 → nginx auth_request 실패 → 콘솔 "준비 중"·대시보드 "불러올 수 없습니다". **라이브 확인**: 로그 `userSessionToken … 429 Too Many Requests: 5 per 40 second` 폭주. 2차: (a) `admin`의 Airflow 비번은 airflow-init `AIRFLOW_ADMIN_PASSWORD`인데 `userSessionToken`은 **파생비번**(`:173`)으로 로그인 → 불일치 가능, (b) REST API v2는 Bearer JWT 요구인데 브라우저는 `_token` 쿠키로 `/api/v2` 호출 → 세션 살아도 실패 가능 | **M2** (화면 전면 불능이라 실질 상향) | ① 대시보드 Airflow 조회를 **백엔드 경유**(M2-0으로 고친 `AirflowDagRunClient`, Bearer)로 라우팅 ② `userSessionToken`에 per-user single-flight + 실패 음성캐시 ③ admin 파생비번/REST v2 쿠키 인증 검증. **미조치(사용자 지시로 진단만)** |
| M2-1 | **채널 활성 토글(DB)이 실제 발송 게이트(앱 프로퍼티)와 단절** | A11 | `NotificationAdminController.java:46-58`(PUT `/channels/{type}`이 DB `enabled` 갱신) vs `NotificationService.java:161,163`(dispatch가 EMAIL/SMS 발송을 오직 `@Value emailEnabled/smsEnabled`(기본 false)로만 판정, DB `enabled` 안 읽음) | 운영자가 화면에서 EMAIL을 «활성»으로 켬 → DB·`GET /channels`는 활성 표시, 그러나 dispatch는 정적 프로퍼티만 봐서 모든 EMAIL 발송이 DEAD → **CRITICAL 메일이 «채널 켜짐»에도 아무에게도 안 감**, 연결 테스트도 항상 DEAD | **M2** | dispatch 게이트를 `notification_channel_config.enabled` 조회로, 최소한 「앱 프로퍼티 미설정 시 발송 불가」 화면 노출 |
| M2-2 | **스케줄러 미지정 12개가 `collect-` 4스레드에 집중 → 복구·관제 지연** | B | `PlatformSchedulerConfig.java:15` 주석은 "8개"라 하나 실측 12개(§부록 목록), 폴백은 `taskScheduler`=collect-(poolSize 4, `:30-38`) | 느린 작업(`NifiJobMirrorService:384` 일 1회 전량 재동기화, `DiskBreakdownService:75` 5분 디스크 walk, 캔버스 오버레이)이 4스레드를 점유하면 같은 풀의 **`KafkaPipelineStateSynchronizer:75`(FAILED 파이프라인 복구 주체)와 지표 수집이 지연** → 장애 복구/관제 갱신이 밀림 | **M2** | 주석 현행화(12), `KafkaPipelineStateSynchronizer`를 별도 풀로 분리 또는 collect- poolSize 상향 |
| M2-3 | **Hikari `maximum-pool-size` 미지정(기본 10) + `connection-timeout:3000`** | B | `application.yml:10-14`(pool-size 없음, socketTimeout 10s) | 스케줄러 10스레드(collect4+ctrl3+batch2+watchdog1)==풀 10, HTTP 요청과 공유 → batch 느린 쿼리가 커넥션을 최대 10초 쥔 사이 collect 버스트 겹치면 풀 고갈 → 관제 API가 3초 만에 500 | **M2/L** | `maximum-pool-size`를 스케줄러 총합+HTTP 여유분(예 20)으로 명시 또는 스케줄러 전용 데이터소스 분리 |
| M2-4 | **verify 태스크가 일시 HTTP 오류를 즉시 하드 실패로 처리(재시도 안 함)** | A1 | `kafka_pipelines_dynamic.py:103-106,163-176`(`raise_for_status`가 재시도 루프 안이나 try/except 없음), `nifi:452-458`. 404 창 실재(`PipelineDeployService.java:504` 주석, `KafkaConnectClient.getStatus:54-59`가 404를 예외로 감쌈) | `deploy` 200 직후 verify가 곧바로 status 조회 → 아직 404 창 → 예외가 두 루프를 뚫고 태스크 즉시 FAIL(정상 배포인데 오탐) | **M2** | status 조회를 try/except로 감싸 HTTP 오류도 `problems`에 넣어 재시도에 포함 |
| M2-5 | **NiFi `__target_{schema,table,timestamp_column}` 부분설정/오타 시 적재검증 조용히 skip** | A3 | `nifi_pipelines_dynamic.py:494-498`(셋 중 하나라도 없으면 `AirflowSkipException`), `:587-589`(`default_var=None`) | 운영자가 `..._target_timestamp_column`을 오타(또는 셋 중 둘만 설정) → 매 실행 landing 검증이 조용히 skip → 실제 0건/고장이어도 DAG는 매번 success. 세 키가 반드시 함께여야 한다는 강제 없음(암묵 계약) | **M2** | 일부만 설정된 경우는 skip이 아니라 설정 오류로 fail |
| M2-6 | **NiFi `__auto_stop_after_run` 오타/미설정 시 배치가 상주 RUNNING → 매일 재실행 소실** | A3 | `nifi_pipelines_dynamic.py:599`(`default_var="false"`, 오타·오값 전부 false로 흡수), `:425-431`(계속 RUNNING이면 다음날 `start` 무효과) | `..._auto_stop_after_run` 오타 → 첫 실행 후 그룹이 RUNNING 유지 → 다음 스케줄의 `start`가 무효 → 그날치 배치 미실행, 그런데 apply/verify/wait 모두 통과해 매일 success. **첫날 이후 적재 없음.** (2026-08-04 리뷰 (3) "스케줄 Variable 산재" 미개선의 구체적 발현) | **M2** | 값이 `{true,false}` 밖이면 경고/실패, 배치형 그룹엔 필수화 |
| M2-7 | **NiFi 제어 DAG `max_active_runs` 미설정(기본 16) + 그룹 상태 전역 공유 → 겹친 run이 in-flight 배치 중지** | A2 | `nifi_pipelines_dynamic.py:605-621`(DAG에 `max_active_runs` 없음, 대조로 kafka는 `:259`에서 2 명시), `stop_process_group_after_run`가 `trigger_rule="all_done"`(`:686`)로 전역 그룹 STOPPED(`:438-440`) | 배치 적재가 스케줄 간격 초과(@hourly인데 90분) → run2가 그룹 start, 아직 도는 run1의 `stop_after_run`이 그룹을 STOPPED → run2 배치 중도 절단, 부분 적재 (**NiFi 체인 중간실패/재실행 안정성 직접 관련**) | **M2** | 배치형 DAG에 `max_active_runs=1` |
| M2-8 | **`airflow_dag_run`·`airflow_sync_cursor` = 코드 없는 죽은 DDL** | A8 | `V29:8,33` 테이블 생성, 자바에서 INSERT/DELETE/read 전무(grep). V29 헤더는 "maxRuns 절삭 성공률 해소" 목적 명시 | 미러가 영구 비어 있음 → 이 테이블 기반 성공률 개선은 실제로 작동한 적 없음(브라우저 직접 호출로 유지), 아무도 안 읽어 조용한 no-op | **M2** | 싱크 서비스 구현 또는 미사용 DDL 제거 |
| M2-9 | **MSA 포털 `/etls` 프록시가 인가를 켜면 401 (미결)** | A10 | 인가 on 시 NIFI 게이트(`EtlJobController.java:26-27`, `NifiController.java:37-39`) → `PermissionAspect.enforce`, `service-token` 공란(`application.yml:75`)이라 바이패스 미적용(`:74`), 포털은 무인증 프록시라 principal null → `PermissionAspect.java:89-92` → 401. `application.yml:68-70` 주석과 일치 | 인가를 켜는 순간 포털 경유 ETL 조회가 전부 401 | **M2**(미결) | 포털이 `X-Service-Token`(app02) 부착(`:74-77` 통과) 또는 Cerebro JWT 발급/전달. (포털 `/etls`→`/api/etl/jobs` 매핑 자체는 리포에 포털 설정이 없어 **추정**) |
| M2-10 | **CDC 조회 로그가 permitAll, KAFKA READ 미검사** | A9 | `CdcLogController` GET `/processing,/events,/dlq,/dlq/detail`(:40-65) 애노테이션 없음 + `authenticated` 지정은 `replay-requests/**`뿐. 대조로 `PipelineController.java:28`·`ConnectionController.java:28`은 KAFKA 게이트 | KAFKA=0/무토큰 호출이 CDC 처리·이벤트·DLQ 로그 열람. 메뉴 `m_cdc_logs`는 KAFKA READ 요구하나 백엔드는 안 막음 | **M2**(읽기 노출) | `CdcLogController` 클래스에 `@RequirePermission(system=KAFKA)` |
| M2-11 | **프론트 라우트 가드가 ADMIN에만 존재** | A9 | `App.tsx:35-40` `RequirePermissionRoute`가 `/admin/*`만 감쌈, `/etl/*·/cdc/*·/airflow/*`는 무가드. 사이드바(`AppLayout.tsx:106-136`)는 `can()`으로 숨기나 URL 직접 진입은 로드됨 | 뷰어가 `/etl/create` 직접 진입 → 페이지 로드(게이트된 컨트롤러엔 실질 우회 아님이나 S-2/S-3 무방비 컨트롤러와 결합 시 동작까지 성공) | **M2** | ETL/CDC/AIRFLOW 라우트에도 `RequirePermissionRoute` 적용 |

### L — 지연·잔여·미완성

| # | 항목 | 축 | 근거 (file:line) | 요지 | 심각도 |
|---|---|---|---|---|---|
| L-1 | `log_timestamp`가 로그 시각 아닌 수집 시각(UTC) | A6 | `FilebeatConfigRenderer.java:58`(`new Date().toISOString()`) | 백로그 재적재 시 실제 이벤트 시각과 어긋남(요청서 §5 TZ 이슈와 동류) | L |
| L-2 | 소스 토픽명 vs 싱크 `topics` 새니타이즈 비대칭 | A5 | `ConnectorNaming.topicName:31-40`(정규화만), 소스는 Debezium이 `[a-zA-Z0-9._-]` 외 `_` 치환(**추정**) | topicPrefix에 특수문자·대소문자 불일치 시 싱크가 없는 토픽 구독 → 무흐름(happy-path에선 잠복) | L(잠복 M2) |
| L-3 | EMAIL/SMS 구독 min_severity=INFO 허용되나 INFO는 외부 채널에서 하드 차단 | A11 | `NotificationService.java:93`(`rank>=2 return`) vs 구독 허용 min_severity INFO 포함(`V36:73`) | 설정과 라우팅 모순(메일로 INFO 받겠다 설정해도 안 옴) | L |
| L-4 | `alert_rule_type.min_severity` 고아 컬럼 | A11 | `V30:36` 정의+시드 채움, 엔진·발송 어디서도 안 읽음 | 규칙 severity를 min 이하로 낮춰도 강제 안 됨 | L |
| L-5 | `nifi_counter_snapshot` 고아 무한 누적 | A8 | `NifiPipelineMetricScheduler.java:235,260`(save만), retention 미포함 | 프로세서 삭제/재생성(UUID 변경) 시 옛 행 영구 잔존(스토리지 누수) | L |
| L-6 | `airflow_dag_catalog` 소프트-disable 행 미제거 | A8 | `AirflowDagCatalogSyncService.java:31-33,70-73`(enabled=FALSE만) | 삭제된 DAG 카탈로그 행이 영구 잔존(화면엔 안 나오나 미러가 실체보다 커짐) | L |
| L-7 | 삭제 시 `metric_snapshot`(V5)·`command_history`(V4) 고아(시간 소멸) | A7 | `PipelineService.java:199-223`(정리 안 함), retention 화이트리스트로 시간 경과 소멸 | 보존창 동안 유령 pipeline_id가 조인/대시보드에 노출 | L |
| L-8 | 서버 메뉴트리(`app_menu`/override, `/authz/menus`)가 프론트에서 미사용 | A9 | 사이드바는 하드코딩 `NAV_ITEMS`(`AppLayout.tsx:68-113`), `getMenus()`(`authz.ts:129`)·`app_role_menu_override` 미소비, DB 카탈로그(`V47:124-140`)엔 `/airflow/dashboard`·`/cdc/create` 없음 | 역할별 메뉴 노출 설정이 UI에 반영 안 됨(보안결함 아닌 **미완성**, three-place 불일치) | L/미완성 |
| L-9 | `metrics/snapshot` 이중 호출자 → 스냅샷 행 노이즈 | A1 | DAG가 start당 최대 7회 삽입(`kafka:148-153`) + 스케줄러 20초 주기 | `observation_count` 부풀림+불필요 왕복(일별 SUM은 telescoping으로 보존, 데이터 손상 아님) | L |

> 추정 1건: `cdcLogs.ts`의 `listDlqRecords(from,to)`는 백엔드가 `Instant`(ISO.DATE_TIME) 기대(`CdcLogController.java:56-57`), `listCdcProcessingLogs`는 `LocalDate`(ISO.DATE, `:42-43`) — 같은 시그니처라 호출부가 포맷 혼동 시 400. 호출부 미확인이라 추정.

---

## 5. 2단계 실기동 E2E 후보 (회차 A / 회차 B)

정적 대조에서 나온 의심 지점 중 **실기동으로만 확증되는 것**만 추렸다. WSL2 7.6GiB 제약(요청서 §1) 때문에 경량 5개 → 확장 순으로 분할.

### 회차 A — 경량 (kafka + kafka-connect + target-db + metadata-db + pipeline-api)
| E2E | 검증 대상 | 확증할 결함 | 방법(요지) |
|---|---|---|---|
| A-1 | **DLQ 무통보 유실** | H1-1 | 타겟에 제약(NOT NULL/타입) 걸어두고 위반 행 주입 → 싱크 RUNNING 유지·타겟 누락·알림 0 확인 → 7일 retention은 `retention.ms` 축소로 가속 재현 |
| A-2 | **마스킹 조용한 해제** | H2-1, H2-2 | ① 카탈로그와 다른 대소문자로 마스킹 컬럼 지정 ② 원천 컬럼 rename 후 CDC → 타겟에 평문 유입 여부 확인 |
| A-3 | **Kafka 중단 후 재적재** (사용자 강조) | M1-4, H1-3 | Kafka stop→start. CDC 파이프라인=무손실·무중복(upsert 멱등) / 로그 파이프라인=중복 행 발생 확인. 장기 정지(retention 초과) 시 유실 재현 |
| A-4 | **삭제 전파 부분 실패** | H2-4 | delete 순간 kafka-connect 일시 정지 → 메타는 삭제되는데 replication slot이 원천에 active로 남는지 확인(`SELECT * FROM pg_replication_slots`) |
| A-5 | **동시 start+delete** | H2-5 | 같은 파이프라인에 resume/delete 동시 호출 → 메타 없는 RUNNING 커넥터 또는 유령 definition 재생성 재현 |
| A-6 | **채널 토글 ↔ 발송 단절** | M2-1 | 앱 프로퍼티 EMAIL off 상태에서 화면으로 채널 «활성» → CRITICAL 유발 → 메일 발송 0 확인 |
| A-7 | **분산락 부재(2대)** | M1-1 | pipeline-api를 2 인스턴스로 → 알림 이중 발송·지표 이중 적재 재현 |

### 회차 B — NiFi·Airflow·Oracle 추가
| E2E | 검증 대상 | 확증할 결함 | 방법(요지) |
|---|---|---|---|
| B-1 | **NiFi 체인 중간 실패 재실행** (사용자 강조) | M2-7, H2-8 | 체인 중간 프로세서를 강제 실패시킨 뒤 재실행 → 처음부터 재적재(중복)인지/이어서인지, 겹친 run이 in-flight 배치를 STOPPED로 끊는지 확인 |
| B-2 | **Variable 오타 배치 소실** | M2-5, M2-6 | `__auto_stop_after_run`/`__target_*` 키를 오타로 설정 → 매 run success인데 실제 적재 0 재현 |
| B-3 | **NiFi 완료 추측 오염** | H2-8 | UPSERT 파이프라인 + UPDATE-only 적재 → DAG count=0 skip=success로 기록되는지, 대시보드 건수와 어긋나는지 |
| B-4 | **Oracle 소스 브로커 주소 하드코딩** | (agent2 M2) | `KAFKA_BOOTSTRAP_SERVERS`를 비-기본으로 바꿔 Oracle CDC 소스 기동 실패 재현(`DebeziumOracleTemplate.java:23,68`) |
| B-5 | **인가 ON 시 포털 401** | M2-9 | `authz.enforcement.enabled=true` + service-token 공란에서 무인증 프록시 호출 → 401 확인 |
| B-6 | **인가 우회(무방비 컨트롤러)** | S-1, S-2, S-3 | 인가 ON 상태에서 무토큰으로 `POST /api/admin/users`, DLQ 자기승인, Airflow DAG 삭제가 통과하는지 확인 |
| B-7 | **미러 유령 잡** | H2-7 | NiFi 잡 4개 동시 삭제 → 대시보드에 유령 잡이 계속 남는지(`missing>3` 보류) 재현 |

> 주의: 회차 A/B의 파괴적 시나리오(브로커 중단, 커넥터 삭제, 2 인스턴스)는 **현재 사용 중인 로컬 스택을 흔든다.** 요청서 §1이 2단계를 "후보 목록만"으로 잡았고 이 문서도 실행하지 않았다. 실행하려면 별도 승인 필요.

---

## 부록 — 결함 없음으로 확인한 항목 (재보고 아님, 대조 확인)

- **REST 계약**: `POST /api/pipelines/{id}/{deploy|start|stop}`(`PipelineController:110-128`), `GET /api/pipelines/{id}`의 connectors/status 스키마, `GET /api/connect/connectors/{name}/status`(`MonitoringController:32`), `POST /{id}/metrics/snapshot`의 `committedOffset`(primitive long) — 엔드포인트·메서드·스키마 일치.
- **커넥터 이름 일관성**: 생성 시 `ConnectorNaming`(결정적)으로 계산·저장, 조회/pause/stop/삭제는 저장값 재사용(`PipelineService.java:204`) — 세 경로 일관.
- **Filebeat 경로**: `/filebeat-inputs`(compose:359,376) = 마운트(compose:465) = glob `*.yml`(filebeat.yml:6), 파일명 `pipeline-{id}.yml`(`FilebeatInputFileService.java:47`) 일관.
- **재시작 무손실(CDC)**: sink `insert.mode=upsert`+`primary.key.mode=record_key`로 at-least-once 재전송을 멱등 흡수 — CDC 경로는 견고.
- **부분 실패 방어(create-stopped)**: 커넥터를 STOPPED로 생성(`prepareStoppedConnector:207-228`)해 고아 커넥터가 데이터를 흘리는 위험 차단.
- **억제↔재발송 정합**: `evaluateRenotify`가 `suppressed_by IS NULL`로 억제 인스턴스를 재발송에서 제외(`AlertEngine.java:220`), 자동 ack-on-resolve(`:608-617`)와 정합.
- **NiFi bulletin 커서 버그**: 두 호출자 모두 `afterId=0L`(`ProcessHealthService.java:223`, `NifiPipelineMetricScheduler.java:125`)이라 현재 비활성(live 결함 아님).
- **타입 정합 표본**: `pipelines.ts`/`connections.ts`/`platform.ts`/대부분 `cdcLogs.ts`·`alerts.ts` ↔ 백엔드 DTO 필드명·nullability·enum 일치.
- **etl_job 자식 FK**: 소프트-삭제 패턴이라 `V17/V18`의 FK가 실제로 깨지지 않음.

### `@Scheduled` 17개 전수 (파일:행 | 스케줄러)
1 `NotificationService:140` ctrl / 2 `AlertEngine:167` ctrl / 3 `RetentionService:53` batch / 4 `PartitionMaintenanceService:42` batch / 5 `WatchdogService:29` watchdog / 6 `DiskBreakdownService:75` 미지정 / 7 `ResourceSampleScheduler:58` 미지정 / 8 `authz/PermissionAuditRetentionService:29` 미지정 / 9 `AirflowDagAlertScheduler:31` 미지정 / 10-11 `NifiJobMirrorService:83,384` 미지정 / 12 `KafkaPipelineMetricScheduler:50` 미지정 / 13 `KafkaPipelineStateSynchronizer:75` 미지정 / 14-15 `NifiPipelineMetricScheduler:121,186` 미지정 / 16 `NifiProcessorRunTracker:100` 미지정 / 17 `NifiCanvasStatusOverlayService:61` 미지정

### 결번 사유 (참고)
- V25 = 개번(`V25__add_pipeline_masking_policy.sql`→`V46`, `8740c4f`).
- V34·V35·V37·V38·V39 = 예약 대역 내 미사용 슬롯(어떤 커밋에도 파일로 존재한 적 없음, 스쿼시/롤백 흔적 없음 — **추정**).
- V24 = 위 M1-6(중복, 개번으로 해소).

---

## 7. 조치(remediation) 적용 이력

검토 후 사용자 지시로 **M2-0(Airflow 동기화·알림 nginx 401)**을 실제 수정·검증했다. 나머지 41건은 미조치(검토만).

### 7.1 M2-0 — Airflow 백엔드 연동 인증 수정 (완료)
- **변경 파일**: `web/backend/.../airflowdashboard/AirflowDagRunClient.java` (1개 파일, 미커밋 작업트리 수정)
- **변경 내용**:
  1. base URL을 브라우저용 nginx 프록시(`http://cerebroetl-ui/airflow`, per-user 세션 auth_request)에서 **`airflow-apiserver:8080` 직접 호출**(`airflow.base-url`)로 전환 → 세션쿠키 요구 경로를 우회.
  2. 모든 호출에 **서비스 admin Bearer JWT** 부착 — `/airflow/auth/token`을 `airflow.admin-username/password`(pipeline-api에 이미 주입됨: `AIRFLOW_ADMIN_USERNAME=admin`)로 받아 20분 캐시, 401 시 1회 재발급 재시도. `AirflowUserSyncService.adminToken()`과 동일한 검증된 방식.
  3. Airflow(uvicorn)가 JDK 기본 HTTP/2를 거부하므로 **HTTP/1.1 고정**.
- **검증**:
  - 컨테이너 내부에서 내 코드와 동일 경로 재현: 토큰 발급 성공 → `GET /airflow/api/v2/dags` → **HTTP 200**, DAG 9개 정상 조회.
  - 재기동 후 `AirflowDagAlertScheduler` 로그 **401 카운트 0**(수정 전 매 주기 401 폭주 → 해소).
  - pipeline-api 재빌드·재기동 정상(healthy), 기존 기능 무영향.
- **주의**: 이 수정은 **작업트리 수정만**(커밋 안 함). 또한 이 환경은 `AUTHZ_ENFORCEMENT_ENABLED=true`로 인가 강제가 켜져 있어, 백엔드 `/api/airflow/dag-catalog/sync`는 로그인 JWT가 있어야 호출된다(정상).
- **⚠️ 조치 범위 정정**: M2-0 수정이 고친 것은 **백엔드가 Airflow를 호출하는 경로**뿐이다 — `AirflowDagCatalogSyncService`(대시보드 접속 시 백엔드가 도는 카탈로그 동기화)와 `AirflowDagAlertScheduler`(V42 알림). 이 둘은 이제 서비스 Bearer JWT로 정상 동작(401 해소, 알림 재가동). **그러나 `AirflowDashboardPage` 화면 자체와 콘솔(생성/관리) iframe은 브라우저가 `/airflow/*`를 직접 치는 별개 경로**라 M2-0 수정과 무관하며, **여전히 실패**한다 — 그 원인은 별건 **M2-0b(per-user 세션 429 스톰)**. 즉 화면상의 "불러올 수 없습니다"는 M2-0b이며 미조치.
- **후속(미조치)**: M2-0b(대시보드·콘솔 화면), S-1/S-2/S-3(무방비 컨트롤러), 포털 401(M2-9).
