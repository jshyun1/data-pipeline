# 서비스 수준 점검 - 임시방편 설계 전수 확인과 대안 (2026-10-06)

대상: Cerebro ETL 을 개발 · 운영하는 개발자. 사용자 지적(«NiFi 캔버스 덧칠, 워크플로우의 NiFi 감시 방식처럼 서비스하기 부적합한 설계가 더 있나»)에 따라
백엔드 · Airflow · 화면 · CDC/운영 네 영역을 코드 기준으로 점검했다. 각 항목의 핵심 근거는 코드에서 직접 다시 확인했다(파일:행).

판단 기준(상용 ETL 솔루션 수준):
- 다른 제품 화면을 밖에서 고치지 않는다 → 공식 확장점을 쓴다.
- 추측으로 상태를 정하지 않는다 → 권위 있는 이벤트로 정한다.
- 대상 수에 비례해 도는 주기 전체 조회를 하지 않는다 → 묶음 조회 · 변경분 · 푸시를 쓴다.
- 여러 대로 띄워도 안전해야 한다.
- 비밀은 드러나지 않아야 하고, 데이터는 조용히 잃지 않아야 한다.

## 0. 먼저 바로잡을 것 - 어제 «해결했다»고 한 완료 판정

NiFi 확장(NAR) 완료 감시는 «화면 활동이 멈추면 끝»(45초/180초)보다 빠르고 정확해졌지만, 판정 근거는 여전히 **«3초 동안 조용하면 완료»**다
(`nifi/extensions/.../CompletionDetector.java:106-118`, `JobCompletionReportingTask.java:70-76`).
처리기가 시간 간격으로 끊어 도는 흐름(타이머 · CRON 일정, Wait/Notify, ControlRate)은 중간에 «완료»로 보일 수 있다. 그러면 Airflow 는 그것을
«콜백 확정»으로 받아 그룹을 멈춘다 → **덜 적재된 채 성공**. 이벤트 기반으로 바꾸는 방법은 2-1.

## 1. 지금 바로 고쳐야 할 것 (보안 · 데이터 손실) - P0

| # | 문제 | 근거 | 왜 위험한가 | 대안 |
|---|---|---|---|---|
| 1 | **DB 비밀번호가 로그인 없이 보인다** | 커넥터 설정에 원천/대상 DB 비밀번호 평문(`DebeziumOracleTemplate`, `JdbcSinkTemplate`) → `pipeline_connector.connector_config_json` 저장 → `GET /api/pipelines` 응답에 그대로(`PipelineConnectorSummary.java:14,26`). 업무 API 전부 `anyRequest().permitAll()`(`SecurityConfig.java:57`), 인가 강제 기본 꺼짐 | 연결정보 화면에서 공들여 암호화(AES-GCM)한 비밀번호가 이 경로로 새어 나간다 | Kafka Connect ConfigProvider(`${file:…}` / Vault)로 비밀을 설정 밖으로, API 응답에서 비밀 가리기, 인증을 기본값으로 |
| 2 | **Kafka Connect 를 인증 없이 화면 포트로 노출** | `nginx.conf:142-151` `/kafka-connect-api/` 에 `auth_request` 없음, 메서드 제한 없음 | 화면 주소에 닿는 누구나 커넥터 설정(비밀번호) 조회 · 삭제 · 변경 | 경로 삭제, 필요한 상태만 pipeline-api 가 권한 확인 후 좁힌 DTO 로 제공 |
| 3 | **비밀 파일이 git 에 커밋돼 있다** | `nifi-conf-backup/jsh_260811/conf/{nifi.properties,keystore.p12,truststore.p12}`, `web/cerebroetl-ui/certs/localhost.key` | 저장소에 접근하면 NiFi 암호화 키 · 키스토어 비밀번호 · 인증서 비밀키를 얻는다 | 파일 삭제 + **키 · 비밀번호 교체**(삭제만으로는 이력에 남는다) + 이력 정리(별도 결정) |
| 4 | **CDC 가 숫자 정밀도를 잃는다** | `decimal.handling.mode=double`(`DebeziumOracleTemplate.java:88`, `DebeziumMysqlTemplate.java:43`) | NUMBER(금액 등)가 실수로 바뀌어 끝자리가 틀어진다 - 조용히 | `precise`(또는 `string`) |
| 5 | **CDC 가 실패한 이벤트를 건너뛴다** | `event.processing.failure.handling.mode=warn`(Oracle `:92`, MySQL `:44`), `skip.unparseable.ddl=true`(`:91`) | 원천 쪽 처리 실패 · 해석 못 한 DDL 을 경고만 남기고 넘어간다 → 대상과 어긋남(대상 쪽은 «오류 시 멈춤» 정책인데 원천은 반대) | `fail`, DDL 건너뛰기는 알림을 동반할 때만 |
| 6 | **보관 정리가 최근 행을 지울 수 있다** | 파티션 테이블(`pipeline_metric_snapshot` V27, `infra_resource_sample` V41)도 `DELETE … WHERE ctid IN (SELECT ctid … LIMIT n)`(`RetentionService.java:94-119`). 파티션 삭제(DROP) 코드는 없음 | ctid 는 파티션 안에서만 유일하다 → 다른(최근) 파티션의 같은 ctid 행까지 지워질 수 있다. 문서에는 «파티션 삭제 동작»으로 적혀 있음 | 오래된 파티션 DETACH/DROP(pg_partman 등), 그 전까지는 기본키로 배치 삭제 |
| 7 | 기본 비밀값을 그대로 써도 뜬다 | Airflow 사용자 비밀번호 HMAC 키 기본값이 소스에 있음(`AirflowUserSyncService.java:70,240-246`), `.env.example` 의 `ChangeMe…` · `PIPELINE_CRYPTO_SECRET` 예시값, Airflow DB 비밀번호가 SQL 에 고정(`db/metadata-init/02_create_airflow_db.sql:12`) | 코드를 본 사람이면 사용자별 Airflow 비밀번호를 계산할 수 있다 | 기본/예시 값이면 기동 거부, 비밀은 Docker secrets/Vault |

## 2. 결과가 틀리는 것 (정확성) - P1

### 2-1. 워크플로우의 NiFi job 실행 판정

| 문제 | 근거 |
|---|---|
| 완료가 «3초 조용» 추측(0절) | NAR `CompletionDetector` |
| 옛 관측기(45초 무활동)가 아직 실행을 열고 닫는다 - 콜백과 경쟁해 한 실행이 두 행이 되거나, Airflow 가 연 실행을 45초 뒤 «성공»으로 닫는다 | `NifiProcessorRunTracker.java:61,103-174`, `EtlJobRunService.java:117-136` |
| 같은 job 을 두 실행이 동시에 써도 막지 않는다 - 경고만(«한쪽이 대기합니다»라는 안내와 달리 대기 장치 없음) | `WorkflowValidator.java:436-461`, `JobRunLedgerService.openOrAdopt` |
| NiFi 정지 실패를 숨긴다 → 그룹이 계속 돌고, 다음 실행은 시작이 무시돼 «활동 없음 180초 = 성공» | `etl_job_runtime.py:80-85`, `etl_nifi_ops.py:38`, `:192-205` |
| 완료 기다리는 태스크가 Airflow 작업 칸을 최대 2시간 붙잡는다(동시 실행 4칸) | `etl_job_runtime.py:126-224`, `docker-compose.yml:569-575` |
| 원장 조회가 실행 기록 전체를 읽는다(Airflow 가 실행 중 job 마다 3초마다 호출) | `JobRunLedgerService.java:84-90` `findAll().stream()`, `resolveJobId:195-209` |

**대안(상용 방식)**
1. **실행 경계를 NiFi 가 알게 한다**: job 그룹을 «배치» 단위로 실행(프로세스 그룹 `FlowFile Concurrency = Single Batch Per Node` + `Outbound Policy = Batch Output`,
   또는 NiFi 2 의 Stateless 실행 엔진). 들어간 묶음이 다 나가면 끝 - 시간 추측이 없다.
2. **실행 번호(run token)를 NiFi 에 넘긴다**(파라미터 · FlowFile 속성) → 마지막 단계(성공/실패 출구)가 그 번호로 보고. 보고는 NiFi 가 꺼져도 잃지 않게(재시도 가능한 큐 · Kafka 토픽).
3. **Airflow 는 기다리면서 칸을 잡지 않는다**: 하나의 deferrable 연산자(시작 → `defer(트리거)` → 완료 시 재개). 트리거는 원장 변경을 비동기로 기다린다(PostgreSQL LISTEN/NOTIFY 또는 상태 API).
4. **관측기는 지표만 기록**하고 실행을 열고 닫지 않는다. 콜백이 안 오면 «성공»이 아니라 «시간 초과/불명».
5. **같은 job 동시 실행 금지**: 시작할 때 job 단위 임대(lease)를 원자적으로 잡고, 이미 잡혀 있으면 기다리거나 거절. 또는 Airflow pool(그룹당 1칸).
6. **정지 실패는 실패로**, 태스크가 죽거나 사람이 실패 처리해도 그룹을 멈추게(Airflow listener / `on_kill`), «실행 기록 없이 RUNNING 인 그룹»을 정리하는 조정기.
7. 원장 조회는 인덱스 조회로(`uq_etl_job_run_token` 이미 있음) - 작은 수정.

### 2-2. 화면 · 알림 숫자가 틀린다

| 문제 | 근거 | 대안 |
|---|---|---|
| 대시보드가 DAG 100개까지만 받아 그 밖 실패를 안 센다, 실행 5,000건에서 자른다, 조회 실패를 «0건»으로 보인다 | `api/platform.ts:345-350,381-408`, `DashboardPage.tsx:112-117` | 서버 집계 API(기간 · 상태 · 종류별 건수), 오류 · «마지막 갱신 시각» 표시 |
| 워크플로우 알림 규칙이 전체 DAG 의 최근 200건만 본다 → 하루 1회 DAG 의 성공이 2시간이면 창 밖으로 밀려 «미완료» 오탐, Airflow 오류 시 빈 목록 = 모든 경보 해제 | `AlertEngine.java:1617-1633`, `AirflowDagRunClient.java:139` | DAG 별 최근 실행 조회 또는 Airflow 가 이벤트를 밀어 주는 방식, 조회 실패는 «신호 없음» |
| 적재 건수를 태스크 로그 전문을 받아 «총 N 행» 한글 정규식으로 뽑는다 | `workflowLogV2/logV2Hooks.tsx:31-37`, `AirflowDashboardPage.tsx:998-1016` | 원장의 구조화된 값(`totalInserted`) · NAR 보고값 사용 |
| NiFi 실패를 5분짜리 알림판(bulletin) 글자를 30초마다 긁어 판정, 시각은 «긁은 시각» | `NifiPipelineMetricScheduler.java:121-172` | NAR 보고에 실패 · 건수를 실어 보냄(이미 bulletin 저장소 접근 가능) 또는 NiFi Site-to-Site 보고 |
| 두 대 이상 띄우면 적재 건수 · 경보 타이머가 두 배로 센다, 재알림(유료 문자 · 카카오)이 두 번 나간다 | `NifiPipelineMetricScheduler.java:239-261`, `AlertEngine.java:182,222-243`, `NotificationService.java:116` | 4-1 |

### 2-3. 워크플로우 관리(Airflow) 화면에 NiFi 덧칠이 걸려 있다

`/airflow/manage` 도 `ConsoleFramePage` 를 쓰고, NiFi 전용 덧칠(640줄 CSS 주입, 1초 전체 훑기 번역, 감시 4개)이 Airflow 화면에서 돈다
(`ConsoleFramePage.tsx` handleFrameLoad). NiFi 사전이 Airflow 의 «Name · Cancel · Apply …»를 바꿔 한/영이 섞이고, «Operation» 글자가 든 상자를 38px 로 접고
마우스를 가로챌 수 있다. **대안**: 덧칠을 NiFi iframe 에만(작은 로고 규칙만 남김), Airflow 는 공식 테마 · 플러그인 · 다국어(3.1+) 또는 링크로.

## 3. 커지면 무너지는 것 (규모) - P2

수치는 그룹 500개 · CDC 파이프라인 500개 · 사용자 50명 가정의 추정이다.

| # | 문제 | 근거 | 규모에서 | 대안 |
|---|---|---|---|---|
| 1 | NiFi 를 부를 때마다 새로 로그인(토큰 발급) | `NifiClient.java:1597-1619` `getToken()` 28곳, 재사용 없음 | 호출이 늘 두 배, NiFi 가 매번 비밀번호 확인 | 토큰을 만료까지 재사용 · 401 시 재발급(Airflow 쪽은 이미 그렇게 함), 장기적으로 클라이언트 인증서(mTLS) |
| 2 | 30초마다 NiFi 그룹 트리를 그룹마다 따로 조회 | `NifiProcessGroupTreeService.java:76,114` `getFlow(groupId)` | 30초에 약 1,000건(토큰 포함), 한 바퀴가 30초보다 길어짐 | 재귀 상태 응답 1회로 트리 구성, 변경분만(`/flow/history`) |
| 3 | job 카탈로그 동기화가 5분마다 그룹 · 파라미터 컨텍스트 · 처리기마다 조회 | `NifiJobMirrorService.java:55,89-190` | 수천 건/5분 | NiFi Registry 버전 흐름 또는 변경 이벤트로 |
| 4 | Kafka Connect 를 파이프라인마다 10초 · 20초 주기로 조회 | `KafkaPipelineStateSynchronizer.java:75-130`, `KafkaPipelineMetricScheduler` | 초당 약 100건 + 오프셋 조회 | `GET /connectors?expand=status` 한 번 또는 `connect-status` 토픽 구독, 지연은 JMX/kafka-lag-exporter |
| 5 | 주기 작업 25개가 스레드 4개를 나눠 씀 | `PlatformSchedulerConfig.java:30-38` | 트리 · 카탈로그 순회가 15초 관측 · 30초 알림판 수집을 굶긴다 | 위 1~4 로 일량을 줄이고 풀 분리 |
| 6 | 화면 폴링: 대시보드 1탭이 분당 약 68건, 50명이면 약 2,700건/분(NiFi 재귀 상태만 분당 약 300건) | `DashboardPage.tsx:351-481` 등 | 사용자 수에 비례해 NiFi/백엔드 부하 | 서버 «대시보드 스냅샷» 1개(서버 캐시) + SSE/WebSocket 푸시 |
| 7 | 그룹 클릭마다 워크플로우 전부 조회(N+1), 화면 열 때마다 트리 · 카탈로그 동기화 요청 | `ConsoleFramePage.tsx:3983-3984`, `:2314`, `AirflowDashboardPage.tsx:2034-2041` | 워크플로우 500개면 클릭당 500건 | 역색인 API(`/etl/jobs/{id}/workflows`), 동기화는 서버 주기에만 |
| 8 | 목록을 기간 전체로 받아 브라우저에서 거르고 쪽 나누기 | `EtlLogsPage.tsx:223-232`, `CdcLogsPage.tsx:290-298`, `AuditLogPage.tsx:97-101`, `CdcLogService.java:55-70`(31일치 원시 행 메모리 적재) | 기록이 쌓이면 화면 · 서버 메모리 한계 | 서버 페이지네이션 · 정렬(알림 이력 탭이 이미 그렇게 함), 집계 테이블 |
| 9 | 요청 처리 중 수십 초~분 대기: CDC 배포/시작이 커넥터 상태를 요청 안에서 기다림, 일괄 생성 50건을 한 요청에서 차례로 | `PipelineDeployService.java:58-62,717-757`, `PipelineBatchCreateService.java:28-29` | Airflow 120초 읽기 제한 초과, 요청 스레드 고갈 | 비동기 명령(작업 id) + 상태 조회 · SSE |
| 10 | DAG 파일 해석 때마다 Airflow Variable 을 워크플로우 수만큼 읽고, 모든 태스크 시작 때도 다시 해석 | `etl_workflows_dynamic.py:346-379` | 워크플로우 300개 × 하루 태스크 3,000이면 하루 약 90만 회 | `get_parsing_context()` 로 필요한 DAG 만, 워크플로우별 DAG 번들(버전 관리) |
| 11 | 게시가 Variable 색인을 통째로 덮어써 동시 게시 시 하나가 빠진다 | `WorkflowPublishService.java:86-88,148-171` | 조용히 DAG 가 사라짐 | 게시 직렬화(잠금) · 커밋 뒤 반영 · 조정기 |
| 12 | NiFi 편집 잠금이 화면 글자로 버튼을 끄는 «권고형», 선택은 NiFi 화면 글자에서 UUID 를 긁어 냄 | `ConsoleFramePage.tsx:2070-2182, 1907-1975, 2518-2564` | NiFi REST 로는 그대로 수정 가능, 이름이 겹치면 다른 그룹 선택 | ②안 화면 소스에서 `postMessage` 로 선택 이벤트 보내기, 잠금은 서버/화면 소스에서 강제 |

## 4. 판매 · 서비스 전제의 운영 기반 - P3

### 4-1. 여러 대로 띄울 수 없다
주기 작업 25개에 분산 잠금이 없고(ShedLock 0건), 파이프라인 잠금 · 실패 횟수 · 마지막 카운터 · 감사 읽기 위치가 각 JVM 메모리에 있다
(`PipelineDeployService.java:77,853-861`, `KafkaPipelineStateSynchronizer.java:60`, `NifiProcessorRunTracker.java:69`, `NifiCanvasAuditService.java:36`).
NAR 감시 등록도 «확인 후 생성»이라 두 대면 두 개 등록(`NifiCompletionMonitorProvisioner.java:47-90`).
**대안**: ShedLock(JDBC) 또는 PostgreSQL advisory lock 리더, 상태는 DB(조건부 갱신), 명령은 DB 잠금 · 상태 전이 검사.

### 4-2. 단일 장애점 · 백업 없음
Kafka 1대(브로커 겸 컨트롤러) · 복제 1, Connect 작업자 1대, NiFi 1대(Registry 없음), metadata-db 1대(Airflow DB 와 공유, 384MB 제한), Airflow LocalExecutor,
백업 도구 없음(`docker-compose.yml:84-92,148-150`). Kafka 볼륨을 잃으면 SCN/LSN 위치 · 커넥터 설정 · 스키마 이력이 한꺼번에 사라진다.
**대안**: Kafka 3+3(브로커/컨트롤러) · RF=3 · `min.insync.replicas=2`, Connect 2대 이상, NiFi 클러스터 + Registry, PostgreSQL 복제 + PITR(pgBackRest/WAL-G),
RPO/RTO 문서화. 고객 규모별 «최소/권장 구성» 두 가지로 제시.

### 4-3. CDC 구조: 테이블마다 커넥터 하나
커넥터 이름이 `source-{id}-{db}-{schema}-{table}`(`ConnectorNaming.java:16-22`), 템플릿마다 테이블 하나.
- Oracle: 테이블 N개 = 고객 운영 DB 에 LogMiner 세션 N개(각자 redo 전체를 캔다).
- PostgreSQL: 테이블마다 복제 슬롯 · publication → 기본 `max_replication_slots=10` 에서 11번째 실패. heartbeat 설정이 없어 조용한 테이블의 슬롯이 WAL 을 붙잡아 원천 디스크를 채울 수 있다.
- topic.prefix 기본값이 모든 Oracle 파이프라인에 같다(`CdcCreatePage.tsx:396`).
**대안**: 원천 DB(또는 스키마 묶음)당 커넥터 하나 + 여러 테이블, 테이블 추가는 signal 테이블 증분 스냅샷, PostgreSQL heartbeat + `heartbeat.action.query`,
커넥터별 고유 prefix. 재스냅샷 · 오프셋 초기화 API(`DELETE /connectors/{n}/offsets`). 기존 파이프라인 이전 계획이 필요하다(가장 큰 작업).

### 4-4. 관측
상태를 REST 폴링으로만 보고, Micrometer/JMX/Prometheus 가 없다. 원천 지연(Debezium `MilliSecondsBehindSource`)을 재지 않아 «RUNNING 인데 멈춘» 원천을 못 잡는다.
감시 · 경보 · 알림이 감시 대상과 같은 JVM · DB 안에 있어 백엔드나 DB 가 죽으면 아무도 알리지 않는다.
**대안**: Kafka/Connect/Debezium/NiFi JMX → Prometheus, 외부 Alertmanager(`up` · `absent()` 경보), 백엔드에 actuator 준비 상태(readiness).

### 4-5. TLS · 계정
인증서 검증 끔(NiFi 클라이언트 전부 신뢰 `NifiClient.java:1632-1670`, Airflow `verify=False`, nginx `proxy_ssl_verify off`), NiFi 자체 서명 825일(갱신 없음),
NiFi single-user 관리자 · Airflow 관리자를 모든 구성요소가 공유, 서비스 토큰 하나가 모든 권한(평문 비교), 로그인 JWT 30일 · 철회 불가, Kafka PLAINTEXT 를 호스트 포트로 공개.
**대안**: 내부 CA + 검증 + 갱신, 구성요소별 신원(mTLS/OIDC), 토큰 범위 · 수명 축소, Kafka SASL/TLS + ACL, 화면 포트 외 공개 금지.

### 4-6. 데이터 증가 · 업그레이드
- 보관 정책 없는 테이블: `etl_job_run`, `pipeline_flow_state_event`, `airflow_dag_run`, `pipeline_consistency_check`, `llm_usage_log`, `airflow_dag_alert`.
  `pipeline_metric_snapshot` 은 파이프라인당 하루 4,320행.
- 이미 적용된 마이그레이션을 고친 이력(V63 · V65→V66 · V70)과 기동 때 자동 `flyway.repair()`(`FlywayV70RepairStrategy.java:17-29`)가 차이를 숨긴다.
- 업그레이드 전 백업 단계가 없다.
- 폐쇄망 패키지: 이미지 목록 0.1.0 · 이미 없는 서비스 기재 · digest 없음, 기본 이미지 태그 고정 안 됨, jar 체크섬 검증 없음.
- **대안**: 파티션 + 실제 DROP, 모든 성장 테이블 보관 정책, 적용된 마이그레이션 수정 금지 · 자동 repair 제거, 이미지 digest · SBOM · 체크섬, 업그레이드 전 백업 · 되돌리기 절차.

## 5. 잘 돼 있는 것 (유지)

- 알림 아웃박스: 집기 · 고유 발송 키 · 만료 회수로 여러 대에서도 안전(재알림 횟수 키는 2-2 참고).
- 원장 콜백: 트랜잭션 advisory lock, «열린 실행은 job 당 하나» 유일 인덱스, 재전송 감지.
- Kafka Connect 상태 동기화: «조회 실패»와 «상태 불일치»를 구분하고, 3회 확인 뒤 FAILED, 자동 회복.
- 커넥터는 멈춘 상태로 만들고 Airflow 만 시작, NiFi `autoResumeState=false`. 싱크는 오류 시 멈춤.
- 화면: React Query 폴링은 숨은 탭에서 멈추고, 화면을 나가면 정리. 알림 이력은 서버 페이지네이션.
- 하위 워크플로우 대기는 deferrable, Asset 기반 연결.
- 폐쇄망 설치는 `--no-build`, NiFi 화면 포함 검사. 실행 중 외부 요청 0건(2026-10-05 확인).

## 6. 진행 순서 (제안)

| 단계 | 기간(추정) | 내용 |
|---|---|---|
| **1단계 - 보안 · 데이터 손실 · 작은 수정** | 1~2주 | P0 1~7(비밀 노출 · 커밋된 비밀 교체 · CDC 정밀도/실패 정책 · 보관 정리), 원장 인덱스 조회, NiFi 토큰 재사용, Airflow 화면 덧칠 제한, NiFi 정지 실패 노출 · 180초 성공 제거, 대시보드 오류 표시 |
| **2단계 - 실행 판정 · 수집 구조** | 3~6주 | job 실행을 배치 경계 + 실행 번호 보고로(2-1), deferrable 연산자 · 동시 실행 임대, 관측기는 지표만, Connect `expand=status` · NiFi 트리 1회 조회, ShedLock/리더, 서버 집계 · 스냅샷 API · SSE, 서버 페이지네이션, 비동기 CDC 명령 |
| **3단계 - 판매 구성** | 1분기+ | HA 기준 구성(Kafka 3+3 · Connect 2+ · NiFi 클러스터 + Registry · PostgreSQL 복제/PITR) · RPO/RTO, CDC 를 DB 당 커넥터로 이전, Prometheus/Alertmanager, mTLS/OIDC · 비밀 저장소, 릴리스 digest · SBOM · 업그레이드 절차 |

1단계는 대부분 설정 · 몇 줄 수정이라 바로 시작할 수 있다. 2 · 3단계는 각 항목마다 설계안(상용 제품 방식 비교 · 실측 계획)을 먼저 만들고 승인 후 진행한다.

항목별 상용 제품 비교 · 채택안 · 실측 계획(합격 기준 포함)은 `docs/2026-10-06-production-improvement-design.md`.
