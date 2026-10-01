# WORK LOG — Kafka 파이프라인 웹서비스 구축

> 갱신: 2026-07-26 (Keycloak 완전 제거 → 계정 테이블 기반 자체 JWT 인증 전환, cerebroetl-ui HTTPS→HTTP 전환 — 18절 참고. 그 이전 갱신: 2026-07-22, Keycloak 기반 포털/NiFi/Airflow SSO + Airflow 통합 웹 iframe + GitLab CI 보안 테스트 수정 — 16~17절 참고)
> 상세 증분 이력/검증 로그의 원본은 `/home/user/.claude/plans/peppy-plotting-quasar.md` (계속 갱신됨, 이 파일과 같이 볼 것).
> 설계서 원문: `docs/kafka-webservice-design.md`

## 1. 지금까지 진행 상황 요약

기존에 동작하던 Oracle→Kafka(Debezium/LogMiner)→Kafka Connect→PostgreSQL CDC 파이프라인(+NiFi 비정형/배치 플로우) 위에,
사용자가 화면/API로 소스·타겟 DB나 로그 파일을 지정하면 Kafka Connect 커넥터(+ 로그 파이프라인은 Filebeat)를
동적으로 생성/배포/모니터링해주는 Spring Boot + React 웹서비스를 얹는 작업 중. NiFi는 완전히 별개 트랙으로 유지.

증분(순차 진행) 7개 완료:

| # | 내용 | 상태 |
|---|------|------|
| 1 | `web/backend` Spring Boot 스캐폴딩 + `pipeline_connection` CRUD | ✅ 완료 |
| 2 | `web/backend/Dockerfile` + docker-compose에 `metadata-db`/`pipeline-api` 추가 | ✅ 완료 |
| 3 | `KafkaConnectClient` + `ConnectorConfigRenderer`(Oracle/Postgres/JDBC Sink 템플릿) + `MonitoringController` | ✅ 완료 |
| 4 | `pipeline_definition`/`pipeline_connector` CRUD + `PipelineDeployService`(배포 오케스트레이션) | ✅ 완료, 실 Oracle/Postgres E2E 검증 |
| 5 | 파이프라인 라이프사이클(start/pause/stop/restart) | ✅ 완료 |
| 6 | `web/frontend`(React+Vite+AntD) — 연결정보/파이프라인 관리 화면 2개 | ✅ 완료 (사용자가 직접 화면에서 실사용 검증까지 함) |
| 7 | **로그파일 실시간 적재 (Filebeat)** — `LOG_FILE` 파이프라인 타입, Filebeat→Kafka→기존 JdbcSinkConnector 재사용 | ✅ 완료, 실 e2e 검증(따옴표 포함 로그 라인 정확히 landing) |

**6차 증분 이후 사용자가 화면에서 직접 파이프라인을 만들어보며 발견한 버그 4건도 모두 수정·커밋 완료**(커밋 `242f280`):
Oracle CDC 소스는 반드시 c## 공통 사용자여야 함(검증 로직 추가), `debezium-connector-postgres` 플러그인 누락, JDBC Sink 플러그인 디렉터리에 Oracle 드라이버 누락, `target-db`의 `wal_level` 미설정, Oracle 소스/타겟 계정 분리 필요성. 자세한 건 계획 파일 참고.

현재 스택 9개 서비스 기동 중: `oracle-db, target-db, kafka, kafka-connect, nifi, metadata-db, pipeline-api, pipeline-ui, filebeat`.
기존 운영 커넥터 3개 + 오늘 만든 CDC/로그 파이프라인 커넥터들 전부 정상. 메타데이터 DB에 테스트로 만든 파이프라인/커넥션이 몇 개 더 있음(정리는 사용자 판단).

## 2. 파일 현황 / 커밋 상태

전부 **로컬 커밋 완료**, push는 아직 (사용자가 요청하면 진행). `git log --oneline -12`로 전체 확인 가능. 주요 커밋:
- `242f280` — `web/`(6차까지), `docs/`, `connect-init/`, Oracle/Postgres 버그 수정 4건
- `f6aa298` — 7차 증분(Filebeat) 백엔드/인프라 전체
- `d2abc93` — 프론트엔드 로그 파이프라인 생성 UI (`PipelinesPage.tsx`의 타입 스위치 모달, `types/pipeline.ts` nullable 수정, `api/pipelines.ts`에 `createLogFilePipeline`)
- `5f949b5` — 대시보드 + 파이프라인 상세(Connector 설정/이력) 탭 — 5절 참고
- `fa80fa8` — Postgres publication/replication slot 정리 로직 (파이프라인 삭제 시) — 6절 참고
- `3bc0f95` — `filebeat` 서비스 healthcheck 추가 — 9절 참고
- `ef2a338` — 폐쇄망 배포 패키징 스크립트(`offline/`) — 7절 참고
- `8b96ccb` — `.gitlab-ci.yml`에 `test:backend` 스테이지 추가 (실제 러너 미검증, 10절 참고)
- (그 사이사이 "Update work log for..." 커밋 3개는 이 문서 갱신용, 코드 변경 없음)

## 3. web/backend, web/frontend 구조 (1~6차 증분 결과)

### web/backend (Spring Boot)
- `build.gradle.kts`, `gradlew`/`gradlew.bat`/`gradle/wrapper/*` (Gradle Wrapper 커밋됨, JDK 21만 있으면 빌드 가능)
- `Dockerfile`(멀티스테이지: `eclipse-temurin:21-jdk` build → `21-jre` runtime), `.dockerignore`
- `src/main/resources/application.yml` — 전부 환경변수 기반, `PIPELINE_CRYPTO_SECRET/SALT`는 기본값 없음(fail-fast)
- `src/main/resources/db/migration/V1~V5__*.sql` — Flyway, 설계서 §7 DDL 그대로(FK 없음). V6은 4절 참고.
- 패키지: `common`(ApiResponse/ErrorCode/BusinessException/GlobalExceptionHandler/crypto), `connection`(PipelineConnection CRUD 전체),
  `pipeline`(PipelineDefinition/PipelineDeployService/lifecycle), `connector`(KafkaConnectClient/ConnectorConfigRenderer/템플릿들),
  `monitoring`(읽기전용 MonitoringController), `logpipeline`(4절 참고)
- 테스트: `ConnectionServiceTest`, `ConnectionControllerTest`, `ConnectionRepositoryIT`(Testcontainers, 이 환경에서 실행 불가 — 9절 참고),
  `DebeziumOracleTemplateTest`, `DebeziumPostgresTemplateTest`, `JdbcSinkTemplateTest`, `KafkaConnectClientTest`,
  `PipelineDeployServiceTest`, `PipelineServiceTest`

### web/frontend (React)
- `package.json`(react19, antd^6.5.0, axios, react-router-dom^7, @tanstack/react-query^5), `vite.config.ts`(dev proxy `/api`→`localhost:8081`)
- `src/pages/ConnectionsPage.tsx`, `PipelinesPage.tsx`(테이블 CDC/로그 파이프라인 생성 모두 지원, 상세 Drawer 포함), `DashboardPage.tsx`(4/5절 참고) — 화면 3개.
- `src/api/`, `src/types/` — 백엔드 DTO와 1:1 매칭
- `Dockerfile`(node:20-alpine build → nginx:alpine serve), `nginx.conf`(`/api/` → `pipeline-api:8081` 리버스 프록시, SPA fallback)

## 4. 7차 증분: 로그파일 실시간 적재 (Filebeat) 구조

**아키텍처**: `[로그 파일] → Filebeat(output.kafka, Kafka Connect 커넥터 아님) → Kafka Topic → 기존 io.debezium.connector.jdbc.JdbcSinkConnector(새 클래스 아님) → Oracle/Postgres 랜딩 테이블`. 백엔드는 컨트롤 플레인만 맡는다는 기존 원칙(§6.2)을 지키려고 "백엔드 안에 커스텀 Consumer" 방식 대신 이 방식을 택함.

**리스크였던 지점과 해결**: Kafka Connect worker 기본 컨버터(`schemas.enable=true`)를 안 건드리려면 Filebeat가 Kafka Connect의 `{"schema":..,"payload":..}` 봉투를 직접 만들어 보내야 하는데, 이게 실제로 동작하는지가 이 증분에서 가장 불확실한 지점이었음 → **구현 전에 curl+kafka-console-producer로 수동 스파이크 검증**을 먼저 해서 통한다는 것을 확인한 후 착수(자세한 내용 계획 파일 "7차 증분" 섹션).

- `web/backend/src/main/resources/db/migration/V6__create_log_pipeline_source.sql` — 설계서 §7.6 DDL 그대로(FK 없음)
- `PipelineDefinition.java` — `source_connection_id`/`target_connection_id`를 `nullable=true`로 수정(LOG_FILE은 소스가 DB 연결이 아니라 파일이라 NULL)
- `com.company.pipeline.logpipeline` 패키지(신규): `LogPipelineSource`(엔티티), `LogPipelineSourceRepository`, `FilebeatConfigRenderer`(Filebeat input YAML 렌더링 — script 프로세서로 JSON.stringify를 이용해 스키마 봉투를 만듦), `FilebeatInputFileService`(유일하게 파일시스템을 만지는 클래스, `filebeat-inputs` 공유 볼륨에 파이프라인별 YAML 쓰기/삭제), `FilebeatProperties`, `dto/LogPipelineCreateRequest`
- `connector/JdbcSinkTemplate.renderForLogPipeline` + `connector/dto/LogSinkConnectorRequest` — CDC 싱크와 달리 `primary.key.mode=none`/`insert.mode=insert`(append-only)/`key.converter=StringConverter`(커넥터 단위 오버라이드) 사용
- `connector/ConnectorConfigRenderer.renderLogSink` — 위임만
- `pipeline/PipelineService.createLogFilePipeline` — `parseType`은 이번 버전 `PLAIN`만 허용(그 외 `VALIDATION_ERROR`), `multilineEnabled=true`도 거절. `delete()`에 LOG_FILE 분기 추가(Filebeat 입력 파일 + `log_pipeline_source` row 정리)
- `pipeline/PipelineDeployService` — `deploy()`가 `pipelineType`으로 `deployTableCdcPipeline`/`deployLogFilePipeline` 분기. LOG_FILE은 싱크 커넥터 1개만 등록(소스 커넥터 없음 — Filebeat가 소스 역할) + Filebeat YAML 씀. `pause`/`resume`/`stop`/`restart`는 수정 없이 그대로 동작(싱크 하나만 제어, Filebeat 자체는 계속 돎 — 알려진 제약, 버그 아님)
- `pipeline/PipelineController` — `POST /api/pipelines/log-file` 신규(다른 라이프사이클 엔드포인트는 타입 무관하게 이미 id 기반이라 수정 불필요)
- `docker-compose.yml` — `filebeat` 서비스(`docker.elastic.co/beats/filebeat:${FILEBEAT_IMAGE_TAG}`), `filebeat-inputs`(pipeline-api↔filebeat 공유, YAML 파일 전달용)/`filebeat-registry`(tail 위치 영속화) 볼륨, `./log-sources`(호스트 바인드 마운트, filebeat 컨테이너 안에서 `/var/log/app`) — 테스트용 로그 파일이 실제로 쌓이는 곳
- `filebeat/filebeat.yml` — 정적 base config: `filebeat.config.inputs`(`reload.enabled: true, reload.period: 10s` — 이게 동적 파이프라인 관리의 핵심, 컨테이너 재시작 없이 파일 추가/삭제만으로 반영됨), `output.kafka`(topic은 `fields.kafka_topic` 템플릿, 메시지 바디는 각 input의 script 프로세서가 만든 `kafka_value_json` 필드를 `codec.format.string`으로 그대로 내보냄)

**발견/수정한 버그**: Filebeat가 바인드 마운트된 `filebeat.yml`의 소유권(호스트 사용자, root 아님)을 이유로 부팅을 거부(`config file must be owned by ... uid=0 or root`) → `command: ["--strict.perms=false"]`로 이 검사를 끔(공식 문서가 권장하는 표준 우회법 — chown으로는 바인드 마운트 특성상 근본 해결 안 됨).

**e2e 검증 결과**: `POST /api/pipelines/log-file`로 파이프라인 생성(id=8) → `/deploy`로 SINK 커넥터 1개만 등록(설계대로) → `./log-sources/app.log`에 따옴표 포함 라인 추가 → 10초 내 Filebeat reload → 스키마 봉투가 정확히 `log-8` 토픽에 실림(따옴표 이스케이프 확인) → `log_landing.app_log`에 4컬럼(message/log_timestamp/source_file/agent_host) 그대로 landing. 기존 운영 커넥터 7개 전부 무영향.

## 5. 대시보드 + 파이프라인 상세(Connector 설정/이력) 탭

설계서 §9.1(대시보드)/§9.4(파이프라인 상세 화면) 구현. 기존 코드 스타일/패턴을 그대로 따르고 새 인프라(Kafka AdminClient, 컨테이너 로그 스트리밍 등)는 추가하지 않는 선에서 스코프를 잡음 — 그래서 설계서의 "지연 발생 수"/"Kafka Broker 상태"/"Kafka Topic 탭"/원문 그대로의 "로그 탭"(컨테이너 stdout)은 이번에 구현 안 함(아래 참고).

**백엔드**(전부 기존 리포지토리/엔티티 재사용, 새 서비스 클래스 없음 — `MonitoringController`가 원래 그랬듯 컨트롤러가 직접 리포지토리를 씀):
- `pipeline/dto/PipelineCommandHistoryResponse.java`(신규) — `pipeline_command_history` 응답 DTO.
- `pipeline/PipelineCommandHistoryRepository.java` — `findByPipelineIdOrderByRequestedAtDesc`, `findTop10ByResultOrderByRequestedAtDesc`, `findTop10ByCommandOrderByRequestedAtDesc` 추가(전부 Spring Data 파생 쿼리, 직접 JPQL 안 씀).
- `pipeline/PipelineController.java` — `GET /api/pipelines/{id}/history` 추가.
- `pipeline/dto/PipelineConnectorSummary.java` — `connectorConfigJson`/`lastStatusJson` 필드 추가(이미 엔티티에 있던 값을 노출만 함, 새 쿼리 없음) → 기존 `GET /api/pipelines/{id}` 응답에 자동으로 포함됨.
- `monitoring/dto/DashboardSummaryResponse.java` + `monitoring/DashboardController.java`(신규) — `GET /api/dashboard/summary`: 전체/RUNNING(=DEPLOYED)/FAILED/PAUSED 파이프라인 수(전체 목록을 가져와 스트림으로 카운트, 데이터 양이 적은 POC라 이렇게 함), `kafkaConnectClient.listConnectors()` 호출 성공 여부로 `kafkaConnectHealthy`, 최근 오류 10건/최근 배포 이력 10건(`pipeline_command_history` 기준).

**프론트엔드**:
- `types/dashboard.ts`, `api/dashboard.ts`(신규, 기존 `api/connections.ts`와 동일 패턴) — `getDashboardSummary()`.
- `types/pipeline.ts` — `PipelineConnectorSummary`에 `connectorConfigJson`/`lastStatusJson` 추가, `PipelineCommandHistoryResponse` 인터페이스 추가.
- `api/pipelines.ts` — `getPipelineHistory(id)` 추가.
- `pages/DashboardPage.tsx`(신규) — `Statistic` 카드 4개(전체/RUNNING/FAILED/PAUSED) + Kafka Connect 상태 뱃지 + 최근 오류/최근 배포 이력 테이블 2개. 15초 폴링(`refetchInterval`).
- `pages/PipelinesPage.tsx` — 각 파이프라인 행에 "상세" 버튼 추가, 클릭 시 `Drawer`(탭 3개: 기본정보/Connector/이력)를 염. 선택 상태는 `detailPipelineId`(숫자)만 들고 있다가 매 렌더링마다 최신 `pipelines` 목록에서 `find`로 파생시킴 — 객체 스냅샷을 따로 안 들고 있어서 배포/상태변경 후에도 자동으로 최신값이 반영됨. Connector 탭은 기존 커넥터 목록 테이블 아래에 `Collapse`로 커넥터별 `connectorConfigJson`/`lastStatusJson` 원문을 보여줌(별도 파싱/포맷팅 없이 `<pre>`로 그대로).
- `App.tsx`/`components/AppLayout.tsx` — `/dashboard` 라우트 추가, 인덱스 리다이렉트를 `/pipelines`에서 `/dashboard`로 변경, 사이드바에 "대시보드" 메뉴 추가.

**이번에 의도적으로 안 만든 것**: Kafka Broker 자체 헬스체크(→ 8차 증분에서 구현, 5.2절 참고), 컨슈머 랙 기반 "지연 발생 수"(새 AdminClient 조회 로직 필요), Kafka Topic 탭(토픽 목록/오프셋 조회 API가 없음), 원문 그대로의 실시간 로그 스트리밍 탭(컨테이너 stdout에 접근하는 방법이 이 프로젝트에 없음 — "이력" 탭이 사실상 이 역할의 상당 부분을 대신함).

**검증**: `GET /api/dashboard/summary`/`GET /api/pipelines/8/history`/`GET /api/pipelines/8`(connectorConfigJson·lastStatusJson 포함 확인) 전부 실제 스택에서 curl로 확인. 프론트 빌드 통과, 번들에 새 문자열 포함 확인, nginx `/api` 프록시로 대시보드 데이터 정상 수신 확인. 백엔드 전체 테스트 33개 중 32개 통과(Testcontainers 1건은 기존부터 실패, 무관). 기존 커넥터/파이프라인 전부 무영향.

### 5.1. `test:backend` CI 스테이지가 처음으로 잡아낸 실버그: `filebeat.inputs-dir` 누락 (수정 완료)

CI에 처음 올라간 `test:backend` 파이프라인이 `ConnectionRepositoryIT`에서 실패(`NullPointerException at Objects.java:233`, `BeanInstantiationException at BeanUtils.java:221`). 처음엔 9절에 적힌 "로컬 WSL2 Testcontainers 안 됨" 문제의 연장인 줄 알았으나, 로컬에서 그 문제(정확히는 `IllegalStateException: Could not find a valid Docker environment`)와 스택트레이스 시그니처가 완전히 다르다는 걸 확인하고 별도로 조사함.

- **재현**: 로컬에서 Testcontainers 없이(플레인 `docker run postgres`) `@SpringBootTest` + `@DynamicPropertySource`만으로 CI와 정확히 동일한 스택트레이스(`IllegalStateException → BeanCreationException → BeanInstantiationException → NullPointerException at Objects.java:233`)를 재현 — 즉 Testcontainers와 무관한 순수 앱 버그였음.
- **근본 원인**: `FilebeatInputFileService` 생성자의 `Path.of(properties.inputsDir())`에서 `properties.inputsDir()`가 null. `application.yml`에 `filebeat.inputs-dir` 항목 자체가 빠져 있었음(7차 증분 때 추가했다고 WORK_LOG에 적었지만 실제로는 누락). `docker-compose.yml`/로컬 `bootRun`에서는 항상 `FILEBEAT_INPUTS_DIR` 환경변수를 직접 넘겨서 Spring Boot relaxed binding으로 우연히 동작했고, 그 환경변수가 없는 곳(CI의 `@SpringBootTest`)에서만 null이 되어 터짐 — 이번이 `@SpringBootTest`가 실제로 빈 생성 단계까지 도달한 첫 사례라 지금까지 발견 못 했음.
- **수정**: `application.yml`에 `filebeat.inputs-dir: ${FILEBEAT_INPUTS_DIR:/filebeat-inputs}` 추가(`kafka-connect.base-url`과 동일하게 기본값 명시).
- **검증**: 동일한 로컬 재현 테스트로 수정 확인(BUILD SUCCESSFUL), 백엔드 전체 테스트 재실행해서 36개 중 35개 통과(남은 1건은 여전히 로컬 WSL2 Testcontainers 환경 이슈, 무관) 확인.

### 5.2. Kafka Broker 헬스체크 (8차 증분)

5절에서 "새 AdminClient 의존성 필요"라는 이유로 스킵했던 항목 중 Kafka Broker 자체 헬스체크만 별도로 진행(Topic 탭/컨슈머 랙은 여전히 범위 밖 — 아래 10절 참고).

- `build.gradle.kts` — `org.apache.kafka:kafka-clients:3.8.0` 추가(docker-compose의 `apache/kafka:3.8.0`과 버전 통일).
- `monitoring/KafkaBrokerProperties.java`(신규) — `kafka.bootstrap-servers` (`KAFKA_BOOTSTRAP_SERVERS` 환경변수, `KafkaConnectProperties`와 동일 패턴).
- `monitoring/KafkaBrokerHealthChecker.java`(신규) — `AdminClient`를 빈 생성 시점에 한 번만 만들어 재사용(매 폴링마다 새로 안 만듦), `describeCluster().nodes().get(3, SECONDS)`로 확인, 타임아웃/에러는 `boolean isHealthy()`로 흡수(예외 안 던짐 — `KafkaConnectClient`처럼 실제 조작에도 쓰이는 클라이언트가 아니라 헬스체크 전용이라 굳이 예외 패턴을 안 씀). `@PreDestroy`로 정리.
- `monitoring/DashboardController.java`/`dto/DashboardSummaryResponse.java` — `kafkaBrokerHealthy` 필드 추가(`kafkaConnectHealthy` 옆).
- `docker-compose.yml` — `pipeline-api`에 `KAFKA_BOOTSTRAP_SERVERS: kafka:9092` 추가.
- 프론트엔드: `types/dashboard.ts`에 필드 추가, `DashboardPage.tsx`에 "Kafka Broker" 뱃지 카드 추가(기존 4개 통계 카드 + Kafka Connect 뱃지 옆, Row 컬럼 span을 5/5/5/5/4에서 전부 4로 재조정해서 6개가 들어가게 함).
- **검증**: 백엔드 컴파일 통과, 프론트 빌드 통과, `pipeline-api` 재빌드+재기동 후 실제 스택에서 `GET /api/dashboard/summary`로 `kafkaBrokerHealthy: true` 확인. Kafka 브로커를 실제로 죽여서 false 케이스는 검증 안 함(운영 중인 CDC 파이프라인들에 영향 줄 수 있어서 의도적으로 생략 — 타임아웃/예외 처리 로직 자체는 3초로 제한돼 있어 리스크 낮다고 판단). 백엔드 테스트 36개 중 35개 통과(`ConnectionRepositoryIT` 1건은 9절의 기존 환경 이슈, 무관).

## 6. Postgres replication slot/publication 정리 로직

Debezium Postgres 소스 커넥터를 등록하면 소스 DB에 replication slot/publication이 자동 생기는데(`DebeziumPostgresTemplate` 참고), 이건 Kafka Connect 리소스가 아니라 순수 Postgres 리소스라 커넥터를 지워도 Kafka Connect REST API로는 안 지워진다 — 5차/7차 증분 검증 중 발견한 잔여 이슈였다.

- `connector/PostgresReplicationCleanupService.java`(신규) — 파이프라인 삭제 시(`PipelineService.delete()`) 소스가 Postgres인 CDC 파이프라인이면 소스 Postgres에 직접 JDBC로 접속해서 `DROP PUBLICATION`/`pg_drop_replication_slot`을 실행. Postgres JDBC 드라이버는 이미 metadata-db 연결용으로 `runtimeOnly` 의존성이 있어서 새 의존성 추가 없이 `DriverManager`로 접속 가능.
- `connector/ConnectorNaming.postgresSlotAndPublicationName()`(신규) — slot/publication 이름 계산 공식(`"dbz_" + sanitize(connectorName)`)이 원래 `DebeziumPostgresTemplate`에만 있었는데, 정리 로직도 똑같은 이름을 재계산해야 해서(별도 컬럼으로 안 저장하고 항상 결정적으로 유도) 공유 메서드로 뽑음.
- 실패해도(권한 부족, DB 응답 없음, slot이 아직 active 등) 예외를 던지지 않고 조용히 넘어감 — 파이프라인 삭제 자체를 막을 이유가 없어서.
- **검증**: 전용 테스트 파이프라인(Postgres 소스 → Oracle 타겟) 배포 → publication/slot 실제 생성 확인(`pg_publication`/`pg_replication_slots` 조회) → 파이프라인 삭제 → 둘 다 즉시 정리됨 확인(첫 시도에 바로 성공, slot이 active 상태였는데도 타이밍 문제 없었음 — Kafka Connect가 커넥터 삭제 시 DB 연결을 먼저 끊고 응답하는 듯). 기존 커넥터/데이터 전부 무영향.

## 7. 폐쇄망 배포 패키징 (`offline/`)

`docs/kafka-webservice-design.md` §13을 이 프로젝트의 실제 서비스/이미지 이름에 맞게 구현. 설계서 예시는 `data-pipeline/kafka-connect:1.0.0` 같은 placeholder 이름을 쓰는데, 실제로는 `docker compose build`가 자체 태그 없이 `{COMPOSE_PROJECT_NAME}-{service}:latest`로 이름 짓기 때문에 그대로 못 씀 — 아래처럼 조정.

- `docker-compose.yml`의 자체 빌드 서비스 5개(kafka-connect/connect-init/nifi/pipeline-api/pipeline-ui)에 `image: data-pipeline-{service}:${APP_VERSION:-latest}` 추가 — `APP_VERSION`을 지정해서 빌드하면 `latest` 대신 실제 버전 태그가 붙음(설계서 §13.7: latest 태그 금지). `.env`/`.env.example`에 `APP_VERSION=latest`(로컬 개발 기본값) 추가.
- `offline/image-manifest.json` — coreImages(운영 배포에 항상 필요) / pocOnlyImages(로컬 Oracle/Target DB 데모 컨테이너, `--with-poc`로만 필요) 분리.
- `offline/save-images.sh`(외부망 전용) — `APP_VERSION`으로 5개 서비스 빌드 + `apache/kafka`/`postgres`/`filebeat` 포함해서 tar 하나로 `docker save`.
- `offline/load-images.sh`/`install.sh`(`--no-build`로 빌드 시도 자체를 차단)/`start.sh`/`stop.sh`(`compose down` 대신 `stop` — 볼륨 안 건드림)/`healthcheck.sh`(모든 서비스 running+healthy 확인 후 pipeline-api API까지 호출 — 운영 승인 기준).
- `offline/README.md` — 설계서가 문서 3개(설치/운영/트러블슈팅)로 나누자고 한 걸 POC 단계라 내용이 적어서 하나로 합침.

**실제로 실행해서 검증**(이 세션의 다른 작업들과 같은 방식 — 스크립트만 작성하고 안 돌려본 게 아님): `save-images.sh`로 실제 ~3GB tar 생성 → 방금 태깅된 이미지 5개를 전부 삭제해서 "폐쇄망 서버"를 흉내 → `load-images.sh`로 tar에서 전부 복원 확인 → `healthcheck.sh`를 지금 떠 있는 실제 스택에 돌려서 정상 통과 확인.

**검증 중 실제로 발견/수정한 버그 2건**: (1) `.env`를 bash `source`로 직접 읽으면 `KAFKA_HEAP_OPTS=-Xms256m -Xmx512m`처럼 값에 공백이 있는 줄에서 깨짐(`-Xmx512m: command not found`) → 첫 `=`만 구분자로 쓰는 while-read 루프로 직접 파싱하도록 수정. (2) 그렇게 고친 뒤에도 `APP_VERSION=test-verify ./save-images.sh`로 넘긴 값이 `.env`의 `APP_VERSION=latest`에 조용히 덮어써지는 문제 발견(이미 있는 값도 무조건 export해버려서) → docker compose와 동일한 우선순위(호출 시 환경변수 > `.env`)가 되도록 이미 설정된 키는 건너뛰게 수정.

## 8. 핵심 설계/구현 결정 사항 (재확인 없이 유지할 것)

- 메타데이터 DB는 **`target-db`와 완전히 분리된 별도 컨테이너**(`metadata-db`, 포트 5434).
- `PipelineDeployService`의 배포/라이프사이클 메서드에는 **`@Transactional`을 걸지 않음** — Kafka Connect REST 호출/Filebeat 파일 쓰기는 되돌릴 수 없는 외부 부작용이라, 실패 시 FAILED 상태 기록까지 롤백되면 안 되기 때문.
- `command_history` 기록은 별도 빈 `PipelineCommandHistoryRecorder`의 `@Transactional(REQUIRES_NEW)` 메서드로 분리 — self-invocation은 Spring AOP 프록시를 우회해서 트랜잭션 전파가 무시되는 문제 때문.
- 백엔드는 **컨트롤 플레인만** 맡는다 — 실제 DB에 데이터를 쓰는 건 항상 Kafka Connect(CDC 커넥터, 로그는 JdbcSinkConnector)가 하고 백엔드가 직접 JDBC로 타겟 DB에 쓰지 않는다. 로그 적재 설계할 때도 이 원칙 때문에 "백엔드 안 커스텀 Consumer" 방식을 기각했음. **유일한 예외가 6절의 `PostgresReplicationCleanupService`** — Kafka Connect에 대응 API가 없어서 불가피했고, 비즈니스 데이터는 안 건드리고 관리용 DDL만 실행함.
- Oracle은 **역할별로 계정을 분리**해야 함: CDC 소스는 c## 공통 사용자(LogMiner는 CDB 레벨 인증 필요), 타겟(싱크)은 스키마 소유자 일반 계정. 하나의 커넥션을 양쪽에 재사용하면 안 됨.
- 커넥터 이름 규칙: `source-{pipelineId}-{sourceDbType}-{schema}-{table}` / `sink-...`, CDC 토픽: `{topicPrefix}.{schema}.{table}`. 로그 파이프라인 토픽은 `log_pipeline_source.topic_name`을 그대로 씀(스키마.테이블 조합 아님).
- 프론트엔드는 백엔드 주소를 번들에 하드코딩하지 않고 상대경로 `/api/*`만 사용.
- 폐쇄망 배포 원칙: 모든 의존성 다운로드는 **빌드 시점에만** 발생, 런타임은 `docker load`+`docker compose up`만.

## 9. 알려진 환경 이슈 (재조사 불필요, 그냥 우회해서 쓸 것)

- **Testcontainers가 이 WSL2+Docker Desktop 환경에서 동작 안 함**(`ConnectionRepositoryIT` 실패). 일반 `docker` CLI는 정상. → 로컬 검증은 `docker run postgres:16-alpine` 수동 기동 + `./gradlew bootRun`으로 대체.
- `pipeline-ui`/기타 헬스체크는 `http://127.0.0.1:...`로 — 컨테이너 안 `localhost`가 IPv6로만 풀려서 IPv4 바인딩 서비스는 연결 거부남.
- 로컬 셸에 Node/Gradle 없음 → `docker run --user "$(id -u):$(id -g)" -e HOME=/tmp node:20-alpine ...`로 빌드.
- WSL2 docker 네트워크에서 가끔/때로 지속적으로 `error getting credentials`/`UtilAcceptVsock` 오류 발생. 재시도로 안 풀리면 `~/.docker/config.json`의 `credsStore`를 임시로 `{}`로 비우면 우회됨(원본은 `.bak`으로 백업해둘 것) — 공개 이미지만 받으므로 인증 자체가 불필요.
- Filebeat 컨테이너는 바인드 마운트된 config 파일의 소유권 검사 때문에 `command: ["--strict.perms=false"]` 필요(5절 참고). 이미지에 `wget`이 없고 `curl`은 있음(다른 서비스들과 반대) — 헬스체크 작성 시 주의.
- WSL2 docker 네트워크 문제가 이번엔 credsStore 우회로도 안 풀리고 아예 `docker version`까지 완전히 응답 없어지는 상태까지 간 적이 있었음 — 이땐 재시도/우회로 해결 안 되고 **Windows에서 Docker Desktop을 직접 재시작**해야 했음(재시작 후 `restart: unless-stopped`인 컨테이너들은 자동으로 다시 떴음).
- **WSL2 VM 메모리가 7.6GiB로 부족**(호스트 16GB인데 `.wslconfig`에 `memory` 미지정 → WSL2 기본값 50% 적용). 컨테이너 13개(다수 JVM) 동시 기동 시 스왑 압박으로 응답 없음 현상 발생 — 15.3절 참고. `.wslconfig`에 `memory=11GB` 이상 명시 권장(아직 미적용). 임시 완화책: 안 쓸 때 `oracle-db`/`target-db`를 `docker compose stop`으로 내려두기.

## 10. 남은 작업 (다음에 이어서 할 것들, 우선순위 순서 아님 — 사용자가 고를 것)

1. (완료됨) ~~`git push`~~ — dev에 푸시 완료. **2026-07-21에 dev 히스토리 전체를 단일 커밋(`ea7ac2c`)으로 재구성**해서 원격에 강제 푸시했음(사용자 요청, 협업자 없는 개인 저장소라 영향 없음 확인 후 진행) — 이 문서의 2/4~7/이 절에서 언급하는 이전 커밋 해시(`242f280` 등)는 더 이상 dev 히스토리에 존재하지 않으니 `git show`로 찾으려 하지 말 것. 코드 자체는 `ea7ac2c`에 전부 반영돼 있음.
2. **`QueryDatabaseTable-employees → batch_landing.employees` e2e 최종 검증** — 15.2절 참고, `oracle-db`/`target-db` 재기동 후 Oracle에 테스트 행 삽입 → Postgres UPSERT 반영 확인 필요.
3. WSL2 `.wslconfig`에 `memory` 값 상향 적용 — 15.3절 참고, 권장값(`memory=11GB`)만 전달했고 사용자가 아직 적용 여부 결정 안 함.
4. 로그 `parse_type` JSON/REGEX/DELIMITER 지원 (지금은 PLAIN만).
5. 사용자가 브라우저로 `http://localhost:13000` 직접 열어서 프론트엔드 시각 확인 (대시보드/로그 파이프라인 생성 모달/상세 Drawer 포함 — 클릭 인터랙션은 이 세션에서 검증 못 함).
6. 컨슈머 랙 기반 "지연 발생 수"/Kafka Topic 탭 — 5.2절과 같은 AdminClient를 재사용할 수 있음(Broker 헬스체크는 완료, 5.2절 참고).
7. 통합 셸 앱(사이드바 대시보드/Airflow/NiFi/Kafka 4개 메뉴, 중앙 패널 전환) — 13.3절 다음 단계, 아직 착수 안 함.
8. (완료됨) ~~폐쇄망 패키징 스크립트(`offline/` 디렉터리, 설계서 §13)~~ — 7절 참고.
9. (완료됨, **미검증**) ~~`.gitlab-ci.yml`에 `test:backend` 스테이지 추가~~ — `.gitlab-ci.yml`이 이미 `validate`/`build` 스테이지로 존재하고 있어서(이전 세션 작업) 거기에 `test` 스테이지 + `test:backend` job 추가(기존 `docker:26-dind`/rules 패턴 그대로 맞춤). YAML 문법은 검증했지만 **실제 GitLab 러너에서 돌려본 적은 없음** — 실제로 파이프라인이 도는지, Testcontainers가 CI dind 환경에서 정말 통과하는지는 아직 미확인.
10. (완료됨) ~~`filebeat` 서비스에 healthcheck 추가~~ — `filebeat.yml`에 `http.enabled` 내장 모니터링 엔드포인트(포트 5066, 컨테이너 내부 전용) 켜고 `curl`로 헬스체크(이 이미지엔 wget이 없어서 다른 서비스와 다르게 curl 사용).

## 11. 재개 시 바로 쓸 수 있는 명령어

```bash
# 전체 스택 상태 확인
cd /home/user/data-pipeline
docker compose ps
git status -s

# 백엔드 재빌드 후 재기동 (코드 변경 반영)
docker compose up -d --build pipeline-api

# 로그 파이프라인 생성/배포/검증 예시
curl -s -X POST http://localhost:8081/api/pipelines/log-file -H "Content-Type: application/json" -d '{
  "name": "app-log-ingest", "agentHost": "filebeat", "filePath": "/var/log/app/app.log",
  "readFrom": "BEGINNING", "encoding": "UTF-8",
  "targetConnectionId": 1, "targetSchema": "log_landing", "targetTable": "app_log"
}' | jq
curl -s -X POST http://localhost:8081/api/pipelines/{id}/deploy | jq
echo "test line" >> ./log-sources/app.log   # filebeat가 10초 내 reload

# 커넥터/파이프라인 상태 확인
curl -s localhost:8083/connectors | jq
curl -s localhost:8081/api/pipelines | jq

# 백엔드 테스트
cd web/backend && ./gradlew test   # ConnectionRepositoryIT만 실패하면 정상(9절 참고)
```

## 12. 참고 자산 위치

- 계획/증분 이력 원본: `/home/user/.claude/plans/peppy-plotting-quasar.md`
- 설계서: `docs/kafka-webservice-design.md`
- 기존 CDC 파이프라인 설명 문서: `docs/kafka-cdc-pipeline.md`
- 폐쇄망 배포 가이드: `offline/README.md`
- GitLab 리포지토리: `https://gitlab.com/cktnqhd15/datapipeline.git`

## 13. 9차 증분: Airflow 설치 (통합 "Dataworld ETL" 웹의 선행 작업) — ✅ 완료 (2026-07-14)

사용자가 좌측 사이드바(대시보드/Airflow/NiFi/Kafka 4개 대메뉴, 중앙 패널 스위칭)로 구성된 통합 웹을 구상 중이라, 셸 앱을 만들기 전에 **Airflow 자체를 이 프로젝트의 docker-compose 스택에 새로 설치**했다. 장기적으로는 Kafka 웹의 파이프라인 실행/중지/재시작을 Airflow DAG가 `POST /api/pipelines/{id}/{start,pause,stop,restart}`를 호출하는 방식으로 담당하게 될 예정(제어 플레인은 계속 Kafka 웹이 담당).

**컨테이너 구성**: webserver/scheduler 분리(단일 `airflow standalone`이 아님) — 사용자가 "일회성 데모가 아니라 계속 서비스할 계획"이라고 확인해서, 이 프로젝트의 기존 "역할별 컨테이너 분리" 관례(kafka-connect/connect-init 등)를 그대로 따름.

- `docker-compose.yml`: `airflow-db`(postgres:16-alpine 재사용, `metadata-db`와 완전히 별개 — §14 결정과 동일 패턴), `airflow-init`(1회성, `connect-init` 패턴 — `airflow db migrate` + admin 계정 생성 후 종료), `airflow-webserver`/`airflow-scheduler`(YAML 앵커 `&airflow-common-env`로 환경변수 공유). `./airflow/{dags,logs,plugins}` 바인드 마운트(기존 `./kafka-connect/connectors` 관례). webserver/scheduler에 `user: "${AIRFLOW_UID:-50000}:0"` 추가 — 기본 이미지 uid(50000)로 뜨면 호스트 소유 바인드 마운트(`./airflow/logs`)에 쓰기 권한이 없어서, 호스트 UID로 맞춰줌(공식 Airflow docker-compose 관례).
- `.env`/`.env.example`: `AIRFLOW_IMAGE_TAG=2.10.4`, `AIRFLOW_UID`, `AIRFLOW_DB_*`, `AIRFLOW_WEBSERVER_PORT=8090`, `AIRFLOW_ADMIN_USERNAME/PASSWORD`, `AIRFLOW_FERNET_KEY`/`AIRFLOW_WEBSERVER_SECRET_KEY`(기본값 없음 — `PIPELINE_CRYPTO_SECRET`과 동일한 fail-fast 원칙, 실제로 `Fernet.generate_key()`/`openssl rand -hex 32`로 새로 발급해서 넣음).
- `airflow/dags/`(신규): 검증용 DAG 2개만 — `test_hello.py`(BashOperator, 스케줄러가 바인드 마운트를 실제로 읽어서 실행하는지 배선 검증), `test_pipeline_api_read.py`(PythonOperator+`requests`로 `GET http://pipeline-api:8081/api/pipelines` 호출 — Airflow가 Kafka 웹 API에 네트워크로 닿는지 확인하는 읽기 전용 스파이크. 실제 start/stop 호출은 다음 증분).
- `offline/image-manifest.json`/`save-images.sh`: `apache/airflow:2.10.4`를 coreImages/IMAGES에 추가(커스텀 빌드 아니라 공식 이미지 그대로라 `docker compose build` 대상엔 안 넣음).
- `.gitignore`: `airflow/logs/*`(`.gitkeep`으로 디렉터리만 유지) 추가 — `dags`/`plugins`는 커밋 대상.

**검증**: `docker compose config`로 YAML 앵커가 3개 서비스에 동일하게 풀리는지 먼저 확인 → `airflow-db` healthy → `airflow-init` 실행 후 exit 0(DB 마이그레이션 + admin 계정 생성 로그 확인) → `airflow-webserver`/`airflow-scheduler` healthy, `/health` 엔드포인트로 metadatabase/scheduler 둘 다 healthy 확인 → `airflow dags list`로 DAG 2개 다 인식(`is_paused=False`) 확인 → 둘 다 수동 트리거해서 `state=success` 확인, `test_pipeline_api_read`는 태스크 로그에서 실제로 "4개 파이프라인 조회됨"이 찍힌 것까지 확인(진짜 데이터가 온 것, 우연히 통과한 게 아님). 전체 스택 12개 컨테이너 전부 healthy, 기존 `pipeline-api`/`kafka-connect` API 응답도 무영향 확인.

**발견한 별개 이슈 → ✅ 해결됨**: 검증 중 `kafka-connect`의 커넥터가 3개(`connect-init`이 등록하는 최초 3개)뿐이고, 웹 화면으로 동적 생성했던 커넥터 7개(source-6/7/10, sink-6/7/8/10, 로그 파이프라인 sink-8 포함)가 사라져 있음을 확인했다. `kafka`/`kafka-connect` 컨테이너 시작 시각(오늘 04:33)이 이번 Airflow 작업 시작보다 이르고 `kafka-data` 볼륨 자체(생성일 07-02)는 재생성되지 않은 것으로 봐서, 이번 작업과는 무관 — `_connect-configs`(compacted topic) 로그 오프셋 이력을 보면 07-09~07-10 사이 세션에서 Kafka Connect REST API를 직접 curl로 조작하며 디버깅하던 중 이 커넥터들이 웹 앱 정식 삭제 흐름을 안 거치고 지워진 것으로 추정(`metadata-db`엔 그대로 남아있었던 게 근거). `POST /api/pipelines/{6,7,8,10}/deploy`로 전부 재배포해서 커넥터 10개(기존 3 + 재배포 7) 전부 RUNNING 확인 완료.

### 13.1. Airflow 2.10.4 → 3.2.2 교체 (같은 날, 2026-07-14)

사용자가 "Airflow 3.1부터 한국어 UI를 공식 지원한다"고 확인 요청 → 웹서치로 사실 확인(3.1.0에서 17개 언어 i18n 도입, 한국어 100% 번역 완료 — [Airflow 3.1.0 릴리즈 노트](https://airflow.apache.org/blog/airflow-3.1.0/)). 2.x vs 3.x 장단점을 비교해서 설명하고 "지금이 전환 비용이 최저인 시점(아직 실 운영 DAG가 없음)"이라는 이유로 3.x를 추천 → 사용자가 3.2.2(2026-05 기준 최신 안정 버전)로 확정.

**3.x 아키텍처 변경점** (공식 예제 `docker-compose.yaml`을 WebFetch로 직접 확인 후 반영):
- `webserver` → `api-server`로 개명(`command: ["api-server"]`), 헬스체크 엔드포인트도 `/health` → `/api/v2/monitor/health`(REST API v1→v2 전환의 일부).
- DAG 파싱이 스케줄러에서 분리돼 **`airflow-dag-processor`가 별도 필수 컨테이너**로 추가됨(`command: ["dag-processor"]`, 헬스체크는 기존 스케줄러처럼 `airflow jobs check --job-type DagProcessorJob`).
- 태스크가 DB에 직접 접근하지 않고 api-server를 거쳐 통신하는 구조(AIP-72 Task Execution API)로 바뀌어서, `AIRFLOW__CORE__EXECUTION_API_SERVER_URL: "http://airflow-apiserver:8080/execution/"`를 **컨테이너 이름으로 명시**해야 함(공식 문서/커뮤니티 글 다수가 지적하는 흔한 실수 — `localhost`로 두면 태스크가 전부 connection refused로 실패).
- 컴포넌트 간 인증용 `AIRFLOW__API_AUTH__JWT_SECRET` 신규 필요(기본값 없음 — 다른 시크릿들과 동일한 fail-fast 원칙, `openssl rand -hex 32`로 발급).
- 스케줄러 헬스체크가 CLI(`airflow jobs check`) 대신 내장 헬스체크 서버(`AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=true`, `curl http://localhost:8974/health`)로 바뀜(공식 권장 방식).
- `AIRFLOW__CORE__AUTH_MANAGER`를 `FabAuthManager`로 명시해야 기존과 동일한 `airflow users create` 기반 아이디/비밀번호 로그인이 유지됨(3.x는 auth manager가 플러그블해짐).
- `AIRFLOW__WEBSERVER__SECRET_KEY` → `AIRFLOW__API__SECRET_KEY`로 이동(구 키도 deprecation 경고와 함께 동작은 하지만 새 키로 맞춤).
- `airflow dags list-runs`의 `-d` 플래그가 없어지고 `dag_id`가 위치 인자로 바뀜(재검증하다 발견, CLI 사용 시 주의).

**적용**: 기존 2.10.4용 `airflow-db-data` 볼륨(검증용 DAG 실행 이력 2건뿐이라 보존 가치 없음)은 지우고 완전히 새로 설치 — 마이그레이션이 아니라 재설치. `.env`에 `AIRFLOW_IMAGE_TAG=3.2.2`, `AIRFLOW_JWT_SECRET`(신규 발급) 추가. `offline/image-manifest.json`/`save-images.sh`의 이미지 태그도 갱신.

**검증**: 새 스택(`airflow-db`→`airflow-init`→`airflow-apiserver`/`airflow-scheduler`/`airflow-dag-processor`) 전부 healthy, `/api/v2/monitor/health`로 metadatabase/scheduler/dag_processor 전부 healthy 확인. 검증 DAG 2개 재트리거해서 `state=success` 확인(`test_pipeline_api_read`는 새 Task Execution API 구조에서도 pipeline-api 응답 "4개 파이프라인 조회됨"이 로그에 찍히는 것까지 재확인 — 아키텍처가 바뀌어도 우리 HTTP 기반 DAG는 그대로 동작함을 검증). 컨테이너 안 번들 파일에서 `i18n/locales/ko` 디렉터리 실존 확인(한국어 번역이 실제로 포함돼 있음, 클레임이 아니라 확인된 사실). 전체 스택 13개 컨테이너 전부 healthy, 기존 Kafka Connect 커넥터 10개/파이프라인 4개 무영향 확인.

### 13.2. Kafka 파이프라인 Airflow DAG 동적 생성 (같은 날, 2026-07-14) — ✅ 완료

사용자가 "웹에서 NiFi/Kafka 파이프라인을 만들면 Airflow에서 DAG로 조회되게 하고 싶다"는 요구 확인 → Airflow의 "파싱 시점에 외부 API를 조회해서 DAG를 코드로 동적 생성" 패턴으로 가능하다고 설명하고, Kafka부터 먼저 구현.

- `airflow/dags/kafka_pipelines_dynamic.py`(신규) — 파일이 파싱될 때마다 `GET http://pipeline-api:8081/api/pipelines`를 호출해서, 반환된 파이프라인마다 `kafka_pipeline_{id}_control` DAG를 동적으로 만든다. 각 DAG는 태스크 1개(`apply_action`)만 갖고, **DAG `params`에 `action`(enum: start/stop/restart) 파라미터**를 둬서 트리거 시 어떤 동작을 할지 고르게 함(하나의 DAG 안에 태스크 3개를 두면 트리거할 때마다 셋 다 실행돼버리는 문제가 있어서 이 방식을 택함). `pipeline-api` 호출 실패 시 예외를 삼켜서 그 파싱 주기엔 DAG 생성을 건너뛰고 넘어가게 함(다른 정적 DAG 파싱까지 막히지 않도록).
- **DAG 생성 자체가 Kafka Connect 커넥터를 켜고 끄는 게 아님** — 파이프라인 생성/배포/삭제는 여전히 웹에서, DAG는 이미 배포된 파이프라인의 라이프사이클(`POST /api/pipelines/{id}/{start,stop,restart}`)만 리모컨처럼 조작한다는 원칙 유지.
- **검증 중 발견**: dag-processor가 dags 폴더에 새 파일이 생긴 걸 인식하는 주기(bundle refresh)가 기존 파일을 재파싱하는 주기(`refresh_interval`, 300초)와 별개 설정(`bundle_refresh_check_interval`, 5초마다 "지금이 refresh 타이밍인지"만 체크)이라, **새 DAG 파일을 추가해도 최대 5분 가까이 걸릴 수 있음**(즉시 반영 아님) — 이번 검증에서 실제로 새 파일 추가 후 약 4분 뒤에 인식됨. 코드 주석에 명시해둠.
- **검증**: (1) 현재 파이프라인 4개(6/7/8/10)에 대해 `kafka_pipeline_{id}_control` DAG 4개가 자동 생성된 것 확인. (2) 파이프라인 8로 `action=stop` 트리거 → 실제 Kafka Connect 커넥터가 PAUSED로 바뀐 것 curl로 확인 → `action=start` 트리거 → 다시 RUNNING으로 복귀 확인(왕복 검증). (3) 새 파이프라인(id=11, 테스트용)을 `POST /api/pipelines/log-file`로 실제 생성 → 코드 수정 없이 `kafka_pipeline_11_control` DAG가 자동으로 나타나는 것 확인 → 테스트 종료 후 파이프라인 삭제로 정리.

### 13.3. NiFi 파이프라인 Airflow DAG 동적 생성 (같은 날, 2026-07-14) — ✅ 완료 (메커니즘 검증), 기존 플로우 자체 결함 발견

사용자가 "NiFi는 최상위 프로세스 그룹을 파이프라인 1개로 취급"하기로 확정 → Kafka와 동일한 동적 DAG 생성 패턴을 NiFi REST API 대상으로 구현.

- `airflow/dags/nifi_pipelines_dynamic.py`(신규) — 파싱 시점에 NiFi에서 토큰 발급 후 `GET /nifi-api/flow/process-groups/root`로 최상위 프로세스 그룹 목록을 조회해서, 그룹마다 `nifi_pipeline_{이름}_{id앞8자리}_control` DAG를 생성. Kafka와 달리 NiFi 프로세스 그룹엔 "일시정지" 개념이 없어서 `action`은 `start`/`stop` 둘만 지원(RUNNING/STOPPED로 매핑).
- `docker-compose.yml`의 `&airflow-common-env`에 `NIFI_BASE_URL`/`NIFI_USERNAME`/`NIFI_PASSWORD` 추가(새 비밀 발급 아니고 `nifi` 서비스가 이미 쓰는 `.env`의 `NIFI_SINGLE_USER_USERNAME`/`PASSWORD` 재사용).
- **스파이크 검증 중 발견/해결한 버그 1**: 컨테이너 간 통신(`https://nifi:8443`)에서 NiFi가 "HTTP ERROR 400 Invalid SNI"로 거부함. `.env`의 `NIFI_WEB_PROXY_HOST`에 `nifi:8443`을 추가했는데도(실제로 `nifi.properties`에 반영된 것까지 확인) 여전히 거부됨 — `openssl s_client`로 직접 TLS 핸드셰이크를 떠서 원인을 좁혀보니, TLS SNI 값 자체가 아니라 **HTTP `Host` 헤더가 Jetty의 허용 목록 검사 대상**이었고, 어떤 이유에서인지 `nifi.web.proxy.host`에 새로 추가한 `nifi:8443` 항목은 반영이 안 되는데 기존부터 있던 `localhost:8443`은 통과함(원인 미상, 더 깊이 파지 않고 실용적으로 우회). **해결**: 요청의 `Host` 헤더를 명시적으로 `localhost:8443`으로 오버라이드(TCP 연결 자체는 `nifi` 컨테이너로 정상적으로 감, 헤더값만 바꾸는 것이라 안전) — `openssl s_client`로 Host 헤더만 바꿔가며 재현·확인 후 적용.
- **검증**: (1) 현재 유일한 최상위 프로세스 그룹("logfile")에 대해 `nifi_pipeline_logfile_59d6f6f7_control` DAG 자동 생성 확인. (2) `action=stop` 트리거 → 성공(`state=success`). (3) `action=start` 트리거 → **실패** — 원인 확인해보니 NiFi 응답 바디에 `FetchFile[id=...] cannot be started because it is not stopped. Current state is STARTING`. 이건 저희 DAG/API 호출 메커니즘의 문제가 아니라 **NiFi의 "logfile" 플로우 안 `FetchFile` 프로세서가 이미 invalid(5개 invalid, 0 running)한 채로 멈춰있는 기존 결함** — stop→start를 다시 해봐도 동일하게 재현되어 일시적 문제가 아님을 확인. NiFi 화면에서 그 플로우 자체를 고쳐야 하는 별개 사안이라 이번 범위에서는 손대지 않음("NiFi 플로우 내용은 별개 트랙" 원칙 유지). 최종적으로 프로세스 그룹은 테스트 시작 전과 동일한 상태(`running:0, stopped:0, invalid:5`)로 남겨둠 — 부작용 없음.
- **결론**: Airflow↔NiFi 제어 메커니즘(인증, DAG 동적 생성, 상태변경 API 호출) 자체는 완전히 검증됨. "logfile" 플로우가 시작이 안 되는 건 별도로 고쳐야 할 기존 NiFi 결함.

### 다음 증분에서 할 일 (Airflow 관련, 참고용)
- NiFi "logfile" 플로우의 `FetchFile` 프로세서 invalid/STARTING 고착 문제 — NiFi 화면에서 직접 확인/수정 필요(별개 트랙, 사용자 판단 필요).
- 통합 셸 앱 자체(사이드바 4개 대메뉴 + 중앙 패널 스위칭, NiFi/Airflow는 리버스 프록시로 iframe 임베딩 — NiFi는 clickjacking 방지 헤더 벗겨내는 작업 필요).
- Airflow 웹 화면에서 실제로 한국어 UI가 브라우저상 정상 표시되는지 시각 확인(이 세션은 도구 한계로 API/파일 레벨까지만 확인, 화면 클릭 확인은 사용자 몫).
- DAG 코드의 deprecation 경고 정리(`airflow.models.param.Param` → `airflow.sdk.Param`, `airflow.operators.python.PythonOperator` → `airflow.providers.standard.operators.python.PythonOperator`) — 지금은 동작에 지장 없어 방치.

주간보고 작성 과정에서 "NiFi/Kafka 수집 파이프라인이 정합성 검증까지 끝났다고 봐도 되냐"는 질문을 계기로 NiFi 플로우 상태를 재점검. 13.3절에서 발견했던 "logfile" 프로세스 그룹 결함(FetchFile invalid 고착)을 실제로 조사해보니, 원인이 하나가 아니라 **서로 다른 세 가지 문제**가 겹쳐 있었음이 드러남.

### 15.1. 근본 원인 조사

- DB 각 테이블의 타임스탬프 컬럼(`ingested_at`/`synced_at`)을 NiFi 볼륨 영속화 버그 수정일(10절 참고, 해당 커밋 시점)과 교차 대조하는 방식으로 원인을 특정:
  - `ListenHTTP → unstructured_landing.file_objects` 플로우와 `QueryDatabaseTable-employees → batch_landing.employees` 플로우(둘 다 FLOW_RUNBOOK.md에 "테스트 완료"로 기록돼 있던 것)는 **볼륨 영속화 버그로 인해 NiFi 재기동 시 플로우 정의 자체가 유실**된 것으로 확인(버그 수정 이전엔 있었고, 이후 사라짐).
  - "logfile" 프로세스 그룹은 반대로 **애초에 끝까지 배선된 적이 없는 미완성 스켈레톤**이었음(ListFile은 있으나 이후 단계가 연결 안 됨) — 13.3절에서 관찰한 "FetchFile invalid"는 유실이 아니라 원래부터 미완성이었던 것.

### 15.2. 복구한 것

1. **"logfile" 플로우 신규 완성**: 로그 포맷(예: access log/error log)별로 파싱하지 않고 "한 줄 = 한 필드"로 통째로 적재하는 **단일 리딩 통합 방식**(사용자 선택)으로 구성. `GrokReader`(패턴 `%{GREEDYDATA:message}`) 사용. 착지 테이블 신규 생성: `unstructured_landing.log_file_lines(id, message, source_file, ingested_at)` — DDL은 `db/target-init/01_schema.sql`에 추가, 커밋 완료(현재 dev 히스토리 재구성으로 `ea7ac2c`에 포함).
   - `ListFile → FetchFile → GrokReader/QueryRecord → PutDatabaseRecord(cp-target-db)` 로 구성, 실 로그 라인 투입 → `log_file_lines`에 정확히 적재되는 것까지 e2e 검증 완료.
   - **알려진 한계(수정 안 함, 문서화만)**: NiFi의 `ListFile`+`FetchFile` 조합은 파일을 바이트 오프셋 기준으로 tail하지 않는다. 이미 처리한 파일이 수정(줄 추가)되면 **파일 전체를 처음부터 다시 읽어서 중복 적재**된다(Filebeat의 진짜 tailing과 다른 점). 운영 수준 증분 처리가 필요해지면 `TailFile` 프로세서로 교체하거나 DB 레벨 dedup 제약이 필요 — 이번 범위에서는 우회하지 않고 알려진 제약으로만 남김.
2. **`ListenHTTP → file_objects` 복구**: FLOW_RUNBOOK.md 0-1절 그대로 재구성, `curl -X POST http://localhost:${NIFI_LISTENHTTP_PORT}/contentListener`로 e2e 재검증(row landed).
3. **`QueryDatabaseTable-employees → batch_landing.employees` 복구(부분)**: FLOW_RUNBOOK.md 0-2절 그대로 재구성, 두 프로세서 모두 VALID/RUNNING까지 확인. **다만 Oracle에 테스트 행을 넣어 실제 UPSERT 전파까지 확인하는 마지막 단계는 미완료** — 아래 15.3절 WSL2 이슈로 중단됨. `oracle-db`/`target-db`는 현재 정지된 상태라, 재기동 후 이어서 검증 필요(11절 "남은 작업" 참고).

### 15.3. WSL2 불안정 현상 진단 (별개 이슈, 수정 아님 — 진단만)

작업 중 WSL2가 반복적으로 응답 없어지는 현상 발생(`sqlplus` 로컬 SYSDBA 연결조차 멈춤 등). `free -h`/`docker stats`로 조사한 결과:

- 이 WSL2 VM의 총 메모리는 7.6GiB뿐이었음(호스트 PC 실제 RAM은 16GB — `.wslconfig`에 `memory` 값이 없어 WSL2 기본값인 "호스트의 50%"가 적용된 상태였음).
- `oracle-db`/`target-db`를 뺀 상태에서도 이미 5.6GiB 사용 중, 여유 109MiB, 스왑 75%(1.5/2.0GiB) 사용 — Oracle까지 얹으면 항상 스왑에 크게 의존하게 되는 구조.
- 컨테이너별 메모리: kafka-connect 1.01GiB, nifi 871MiB, kafka 349MiB, airflow 4개 컴포넌트 합계 ~480MiB, pipeline-api 214MiB 등 — 이 프로젝트가 커지면서 동시 기동 컨테이너 수(13개, 다수가 JVM 기반)가 7.6GiB VM의 한계를 넘어선 것으로 결론.
- **조치**: 사용자 요청으로 `oracle-db`/`target-db`를 `docker compose stop`(볼륨은 보존)으로 정지. `.wslconfig`에 `memory=11GB` 등으로 상향할 것을 권장했으나 **아직 사용자가 직접 적용 여부를 결정하지 않음** — 이 리포 밖(Windows 호스트 설정)이라 자동 반영 안 됨.

## 14. 프론트엔드 버그 수정: 파이프라인 삭제 성공인데 오류 알람 뜸 (2026-07-14) — ✅ 완료

## 15. NiFi 유실/미완성 플로우 3건 복구 + WSL2 안정성 이슈 진단 (2026-07-21)

사용자가 화면에서 파이프라인 7을 삭제했는데 "실제로는 삭제됐지만 오류 알람이 떴다"고 보고. 백엔드 로그/Kafka Connect 커넥터 상태/파이프라인 목록을 확인해보니 **서버 쪽은 완전히 정상**이었음(커넥터 2개 정상 삭제, 로그에 에러 없음) — 프론트엔드만의 버그였다.

- **원인**: `web/frontend/src/api/client.ts`의 `unwrap()` — `if (!response.success || response.data === null)`로 되어 있어서, `success: true`인데 `data: null`인 정상 응답(반환값이 없는 `DELETE`가 대표적)도 무조건 에러로 던짐. 백엔드가 `DELETE /api/pipelines/{id}` 성공 시 `{"success":true,"data":null,"error":null}`을 돌려주는데, 이게 항상 이 조건에 걸려 "요청 처리 중 오류가 발생했습니다"를 던졌던 것.
- **수정**: 조건을 `!response.success`만으로 판단하도록 변경(`data` null 여부는 더 이상 에러 판정에 안 씀) — `success` 플래그가 백엔드의 유일한 성공/실패 신호라는 원칙에 맞춤. `deletePipeline`(파이프라인)뿐 아니라 `connections.ts`의 삭제 API도 같은 `unwrap()`을 써서 동일 버그를 갖고 있었는데 이번 수정으로 같이 해결됨.
- **검증**: 프론트 빌드 통과 → `pipeline-ui` 재빌드/재기동 → nginx `/api` 프록시(실제 프론트엔드가 쓰는 것과 동일 경로)로 테스트 파이프라인 생성 후 삭제해서 실제 응답 `{"success":true,"data":null,"error":null}`을 캡처 → 그 실제 응답을 고친 `unwrap()` 로직에 그대로 넣어서 예외 없이 반환되는 것 확인(수정 전 로직이면 이 응답에서 무조건 던졌을 것). **다만 브라우저에서 실제로 토스트/알람 UI가 안 뜨는지 시각적으로는 확인 못 함**(도구 한계) — 로직 자체는 확실히 고쳐졌으니 사용자가 화면에서 재확인 필요.

## 16. 통합 SSO 2단계: Keycloak 기반 포털/NiFi/Airflow OIDC (2026-07-22) — ✅ 2-A~2-D 완료

목표: 통합 웹(Cerebro ETL) 계정 하나로 포털/NiFi/Airflow를 다 쓰는 SSO. 진행 순서를
재검토해서 **Kafka는 사용자 SSO 대상이 아니라 데이터 플레인 보안(SASL)이라 별개 트랙**으로
빼고, IdP는 **Keycloak** 도입으로 확정. 포털 로그인 자체도 Keycloak으로 통일하기로 함
(1단계에서 잠깐 만든 자체 JWT 로그인은 이 단계에서 제거됨 — 진짜 SSO엔 Keycloak 브라우저
세션이 필요하기 때문).

### 2-A. Keycloak 배포
- `keycloak` 컨테이너(quay.io/keycloak/keycloak) + `cerebro` realm import(`keycloak/realm-export.json`).
  클라이언트 3개: `cerebro-portal`(public+PKCE), `nifi`/`airflow`(confidential). 시크릿은
  `${ENV}` 치환으로 파일에 안 남김. 시드 관리자 `cerebro-admin`.
- DB는 새 컨테이너 대신 `metadata-db`의 별도 `keycloak` database 재사용(메모리 절약,
  `db/metadata-init/01_create_keycloak_db.sql`).

### issuer/URL 전략 (이 단계 핵심 난제)
- **split-horizon 문제**: 브라우저(Windows)는 `localhost:8543`만 도달 가능하고
  `host.docker.internal`(→LAN IP)은 브라우저에서 타임아웃. 반대로 컨테이너는
  `host.docker.internal:8543`만 도달 가능(`localhost`는 컨테이너 자신). 하나의 issuer 호스트로
  둘 다 만족 불가.
- **해결**: Keycloak `KC_HOSTNAME=https://localhost:8543`(프론트/토큰 iss) +
  `KC_HOSTNAME_BACKCHANNEL_DYNAMIC=true`. 그러면 discovery의 authorization/logout은
  localhost(브라우저용), token/jwks는 요청 호스트(host.docker.internal, 컨테이너용)로 분리되고
  issuer는 localhost로 일치. 실측으로 확인함.
- **HTTPS 필수**: 포털이 HTTPS라 HTTP Keycloak은 mixed-content로 차단됨 → Keycloak을 자체
  서명 인증서(SAN: host.docker.internal/localhost/keycloak)로 HTTPS 제공(`keycloak/certs/`).
  cerebroetl-ui 인증서 커밋 관례와 동일하게 인증서/키 커밋.

### 2-B. 포털 OIDC 전환
- **백엔드(pipeline-api)**: 자체 JWT 발급 제거 → `spring-boot-starter-oauth2-resource-server`로
  Keycloak 토큰 검증. `JwtDecoderConfig`가 iss는 localhost로 검증하되 JWKS는 컨테이너가 도달
  가능한 백채널 URL(host.docker.internal)에서 가져옴(자체 서명 인증서는 trust-all로 신뢰, POC).
  Keycloak realm role → `ROLE_*` 매핑, `portal_admin`→관리자.
- **app_user 역할 전환(V8)**: 인증은 Keycloak이 담당하므로 `user_pw`/`hq_cd`/`position_cd`를
  nullable로. `GET /api/auth/me`가 첫 로그인 시 토큰 클레임으로 app_user를 자동 프로비저닝
  (프로필/권한 저장소로만 사용).
- **프론트(cerebroetl-ui)**: `oidc-client-ts`로 Authorization Code + PKCE 리다이렉트. 로그인
  화면은 "CEREBRO ETL" 브랜딩 랜딩 + [로그인] 버튼(→Keycloak). `/auth/callback` 라우트에서
  코드↔토큰 교환. 자체 로그인 폼/등록신청은 제거(등록은 Keycloak 몫).
- **검증**: 백엔드는 실 토큰으로 e2e(프로비저닝/401/위조토큰 거부) 확인. **브라우저 클릭
  로그인 왕복도 사용자가 실제로 성공 확인**(cerebro-admin으로 로그인 → 대시보드 복귀).
  전제: 브라우저에서 `https://localhost:8543` 자체 서명 인증서 1회 수락 필요(NiFi처럼).
- 브랜치: `feature/unified-web-login`(MR !5). 커밋 다수(8e56127 Keycloak, 9596225 리소스서버,
  0579fd7 HTTPS, 8495bf2 프론트 OIDC, 3d4eb7e backchannel dynamic 등).

### 2-C. NiFi OIDC 전환 — ✅ 완료

- `docker-compose.yml`의 NiFi 인증을 single-user에서 이미지 네이티브 `AUTH=oidc`로 전환.
  기존 `nifi-conf` 영속 볼륨의 keystore/truststore를 그대로 사용하면서 Keycloak 자체 서명 인증서를
  truststore에 추가했고, `NIFI_SECURITY_USER_OIDC_TRUSTSTORE_STRATEGY=NIFI`로 discovery/JWKS TLS를
  신뢰하게 함.
- OIDC discovery는 컨테이너가 도달 가능한 `${KEYCLOAK_BACKCHANNEL_URL}`을 사용하고, 사용자 identity는
  `preferred_username` claim으로 고정. 초기 관리자 identity는 `${KEYCLOAK_SEED_USER_ID}`
  (`cerebro-admin`)로 설정.
- 통합 웹 프록시 callback을 위해 `NIFI_WEB_PROXY_HOST`와 Keycloak `nifi` client redirect URI에
  `https://localhost:13001/*`를 허용. Keycloak 로그인 화면을 포털 iframe 안에서 표시할 수 있도록
  realm CSP의 `frame-ancestors`에 `https://localhost:13001` 추가.
- **검증**: NiFi가 `oidcAuthorizationCode` provider로 정상 기동하고 discovery/인증서 오류 없이 OIDC
  로그인을 광고하는 것, 통합 웹 프록시 기준 callback URL을 생성하는 것 확인. 이후 사용자가 시크릿
  브라우저에서 `cerebro-admin`으로 `ETL → 관리`에 접속해 **NiFi UI가 정상 표시되는 것까지 확인**.
- 기존 일반 브라우저에서 `Insufficient Permissions`가 한 번 발생했으나, NiFi 정책 파일에는
  `cerebro-admin`의 `/flow`, `/controller`, `/tenants`, `/policies` 권한이 정상 존재했고 시크릿 창에서는
  정상 접속됨. 설정 결함이 아니라 이전 NiFi/Keycloak 쿠키 또는 서로 다른 사용자 세션 충돌로 판단.
- 현재는 단일 관리자 운영이므로 `cerebro-admin` 한 명에게만 NiFi 관리자 정책을 부여. 향후 다중 사용자
  단계에서는 Keycloak 그룹과 NiFi user group/access policy를 별도로 동기화해야 함(Keycloak role이
  NiFi 정책으로 자동 변환되지는 않음).
- 커밋 `27be89c` (`feature/nifi-oidc`, 이후 `dev`에 반영).

### 2-D. Airflow OIDC 전환 — ✅ 완료

- `airflow/webserver_config.py` 신규: FAB `AUTH_OAUTH`, Keycloak provider, 사용자 자동 등록 및 로그인할
  때마다 역할 동기화. Keycloak realm role을 `portal_admin → Admin`, `portal_user → Viewer`로 매핑하는
  `KeycloakSecurityManager` 구현.
- 서버 측 metadata/token/JWKS 요청은 `host.docker.internal` 백채널, 브라우저 authorize/logout은
  Keycloak dynamic backchannel discovery가 반환하는 `localhost:8543` 주소를 사용. Airflow 컨테이너에는
  Keycloak 인증서와 `REQUESTS_CA_BUNDLE`을 주입해 자체 서명 TLS를 검증하게 함.
- **검증**: 로그인 화면의 Keycloak 로그인 진입, authorize 302 redirect, `client_id=airflow`, 등록된
  callback URI 생성까지 서버 측 확인. 커밋 `0211d89` (`feature/airflow-oidc`, 이후 `dev`에 반영).

### 2-E. Airflow 콘솔을 Cerebro ETL iframe에 통합 — ✅ 완료

- 기존 `AirFlow → 생성/관리` 메뉴는 `http://localhost:8090`을 새 탭으로 여는 외부 링크였음.
  `/airflow/manage` 내부 라우트와 `ConsoleFramePage`로 변경해 중앙 패널 iframe으로 표시.
- `cerebroetl-ui/nginx.conf`에 `/airflow/` reverse proxy 추가. URI prefix를 rewrite하지 않고 Airflow에
  그대로 전달하고 `X-Forwarded-Proto/Host/Prefix`를 설정. `X-Frame-Options`/원래 CSP를 제거한 뒤
  동일 출처만 허용하는 `Content-Security-Policy: frame-ancestors 'self'`로 제한.
- Airflow 3 공식 하위 경로 방식에 맞춰
  `AIRFLOW__API__BASE_URL=https://localhost:${CEREBROETL_UI_PORT}/airflow` 설정. OAuth callback을 HTTPS로
  생성하도록 api-server `--proxy-headers`와 `AIRFLOW__FAB__ENABLE_PROXY_FIX=true` 적용.
- Keycloak `airflow` client에 `https://localhost:13001/airflow/*` redirect URI 추가. realm import 파일뿐
  아니라 이미 운영 중인 realm에도 `kcadm`으로 즉시 반영.
- 구현 중 브랜치 전환 이후 실행 중 Keycloak 컨테이너의 인증서 bind mount가 빈 디렉터리로 보이는 stale
  mount 현상 발견 → Keycloak DB는 보존한 채 컨테이너만 재생성해 정상 복구(기존 realm/사용자 유지).
- **검증**: 프론트 TypeScript/Vite production build 통과, Compose config/diff check 통과,
  `/airflow/` HTML 200 + `<base href="/airflow/">`, 정적 JS 200, Airflow UI 내부 API 요청 다수 200 확인.
  OAuth 최종 redirect URI가
  `https://localhost:13001/airflow/oauth-authorized/keycloak`으로 생성되는 것과 iframe CSP 확인.
  Keycloak/Airflow/Cerebro ETL 재배포 후 healthy. 커밋 `b0e53c7`, `dev` push 완료.
- **후속 수정 — FAB 확인 화면 CSS/JS 404**: 신규 React UI 자원(`/airflow/static/assets`, i18n)은
  정상이었지만, 최초 iframe 로그인 때 FAB의 `User confirmation needed` 페이지가 참조하는
  `/airflow/static/appbuilder/*`와 `/airflow/static/dist/*`는 모두 404여서 버튼·Airflow 로고만 크게
  보이는 무스타일 화면이 표시됨. Airflow 3.2.2에서 FAB 정적 라우트의 실제 위치가
  `/airflow/auth/static/{appbuilder,dist}/*`인 것을 컨테이너 파일/HTTP 조합으로 확인. Nginx에서 새 UI
  자원은 그대로 두고 FAB 전용 두 경로만 실제 라우트로 rewrite하도록 수정. 대표 CSS/JS 3개를 통합
  웹 경유로 재검증해 모두 200 및 올바른 `text/css`/`text/javascript` Content-Type 확인.
- **통합 로그인 직후 플랫폼 세션 선연결**: Airflow 메뉴에 처음 들어갔을 때 FAB의
  `Sign In with keycloak` 버튼을 다시 눌러야 하는 UX를 제거. 포털의 Keycloak 토큰 검증이 끝나면
  `PlatformSessionBootstrap`이 숨김 same-origin iframe으로 Airflow의 Keycloak 직접 로그인 경로와 NiFi
  UI를 즉시 열어 두 도구의 브라우저 세션 쿠키를 미리 발급한다. 동시에 Airflow `/ui/auth/me`, NiFi
  `/flow/process-groups/root/status`, Kafka Connect `/connectors`를 1초 간격으로 확인하고 세 서비스가 모두 준비된 뒤에만
  통합 대시보드를 표시한다. 45초 안에 준비되지 않으면 지연된 서비스명과 재시도 버튼을 표시.
  Kafka Connect는 사용자 로그인 개념이 없으므로 별도 SSO가 아니라 REST 연결 가능 여부를 선확인한다.
  이후 Airflow/NiFi 메뉴 iframe은 이미 발급된 세션을 재사용하므로 추가 로그인 입력 없이 바로 콘솔을
  표시한다. production TypeScript/Vite build 및 `cerebroetl-ui` 재배포 완료. 최종 브라우저 로그인 왕복은
  사용자가 새 로그인 세션에서 확인 필요.
- **선연결 대기 화면 정체 수정**: 최초 구현의 NiFi 확인 URL `/nifi-api/access/config`는 현재 NiFi 2.x에서
  404를 반환해 준비 상태가 영원히 완료되지 않았고, Airflow는 Keycloak OAuth callback 뒤 FAB의
  `User confirmation needed` 단계에서 멈춰 `/ui/auth/me`가 계속 401을 반환했다. NiFi 확인을 인증이
  필요한 실제 Flow status API로 교체하고, same-origin 숨김 iframe의 Airflow 확인 화면에서 `OK`를
  자동 실행해 사용자 클릭 없이 세션 발급이 끝나도록 수정.
- **Airflow 자동 확인 후속 수정**: 실제 브라우저 로그에서 NiFi Flow status와 Kafka Connect는 200이지만
  Airflow `/ui/auth/me`만 401인 것을 확인. FAB 확인 페이지에는 같은 문구의 `OK` 링크와 form submit
  버튼이 함께 있는데, 첫 구현이 링크를 선택해 `/api/v2/auth/login` → `/auth/login/`으로 되돌아가는
  인증 순환이 발생했다. 자동화 대상을 form의 submit control로 한정해 OAuth 승인을 실제 제출하도록 수정.
- **Airflow 3 세션 발급 흐름 최종 교정**: 위 `User confirmation needed` 문구는 OAuth 승인 페이지가 아니라
  FAB base layout에 항상 숨겨 포함되는 공통 modal임을 설치 패키지의 `confirm.html`로 확인했다. 따라서
  해당 DOM을 클릭하는 접근은 폐기. 기존 `/auth/login/keycloak` 직접 호출은 FAB 세션만 시작해 Airflow 3
  UI가 요구하는 API JWT 세션 발급을 건너뛰므로 `/ui/auth/me`가 401인 채 `/api/v2/auth/login`과
  `/auth/login/` 사이를 순환했다. 숨김 iframe을 Airflow 3 공식 `/api/v2/auth/login?next=/airflow/`에서
  시작하고, 이어지는 FAB provider 선택 화면에서 Keycloak 링크만 자동 클릭하도록 변경. 이후 OAuth
  callback의 `next` 체인이 API 로그인으로 돌아와 UI 세션 쿠키를 발급하게 구성.
- **Airflow provider 자동 선택 selector 수정**: 실제 FAB 로그인 HTML을 확인한 결과 Keycloak 선택 요소는
  `href`가 있는 링크가 아니라 `id="btn-signin-keycloak"`인 anchor에 inline script가 click handler를
  등록하는 구조였다. 최신 요청도 `/auth/login/` 200 이후 `/auth/login/keycloak` 요청이 없어 이 단계에서
  정지한 것을 확인. href selector를 실제 element id selector로 교체.
- **Airflow OAuth callback mount 경로 수정**: 자동 provider 선택과 Keycloak 왕복 이후에도 callback이
  `/airflow/oauth-authorized/keycloak`으로 생성됐는데, Airflow 3의 FAB 인증 앱은 `/airflow/auth` 아래에
  mount되어 있어 이 URL은 OAuth handler가 아니라 React UI fallback으로 처리되고 있었다(응답 200 뒤
  `/ui/auth/me` 401 반복). `CustomAuthOAuthView` 기반 view에서 외부 callback을
  `/airflow/auth/oauth-authorized/keycloak`으로 명시해 실제 FAB callback handler로 연결. 설정 문법/import
  검증 후 API server 재시작, authorize 302의 `redirect_uri`가 새 callback으로 생성되는 것까지 확인.

### 현재 SSO 보안 범위와 다음 단계

- 현재 `pipeline-api`는 `/api/auth/me`만 Keycloak 토큰을 필수로 요구하고, 기존 Airflow DAG와 레거시
  `pipeline-ui` 호환 때문에 파이프라인 등 업무 API는 아직 `permitAll`이다.
- 최종 운영 모델은 Keycloak 개인 계정/그룹 + Cerebro ETL의 프로젝트별 역할(admin/developer/operator/
  viewer/auditor) + 리소스 `project_id` + append-only 통합 감사 이벤트로 확장해야 한다.
- Airflow/NiFi 직접 접속은 관리자·장애 대응자 중심으로 제한하고, 일반 작업은 Cerebro ETL API를 단일
  관문으로 두는 것이 목표. 이를 위해 Airflow용 service account/token을 먼저 도입한 뒤 업무 API를
  인증 필수로 전환해야 한다.

### 2-F. 포털 일체형 로그인 화면 — ✅ 완료

- 포털 화면에서 Keycloak password grant로 자격증명을 직접 받는 방식은 브라우저 SSO 쿠키가 생성되지
  않아 Airflow/NiFi 자동 로그인이 깨지고, SPA가 비밀번호를 직접 취급하게 되므로 적용하지 않음.
- Authorization Code + PKCE 흐름은 유지하면서 Keycloak 26용 `cerebro` 로그인 테마를 추가. 기본
  Keycloak 브랜딩을 제거하고 CEREBRO ETL 카드 안에 아이디·비밀번호와 로그인 버튼만 표시하도록 구성.
- 포털 `/login`은 별도 로그인 버튼 클릭 없이 미인증 확인 즉시 OIDC 인증 화면으로 이동하도록 변경.
  사용자는 CEREBRO 화면에서 바로 자격증명을 입력하지만 비밀번호는 계속 Keycloak에만 전달되며,
  생성된 Keycloak 브라우저 세션은 Airflow와 NiFi 선연결에도 그대로 사용됨.
- Compose에 `./keycloak/themes` read-only mount 추가, realm export와 실행 중 realm 모두
  `loginTheme=cerebro`, 기본 언어 `ko`(`supportedLocales=ko,en`) 적용.
- **검증**: 프론트 production build 및 재배포 통과, Keycloak 재생성/healthy 확인, authorize 응답에서
  CEREBRO CSS와 `username`/`password` 입력 및 한국어 `로그인` submit 버튼 렌더링 확인.
- **테마 반응형 레이아웃 수정**: Keycloak v2의 기본 grid/padding과 언어 utility가 남아 모바일 폭에서
  카드가 약 190px로 축소되고 `CEREBRO ETL` 및 입력란이 깨지는 문제 수정. 로그인 container를
  `min(390px, 100vw - 16px)`로 고정하고 header/body 기본 grid를 제거했으며, 기본 언어를 한국어로
  고정해 언어 selector를 숨김. 제목 nowrap, 폼/input/password group/button을 100% 폭으로 보정.
  POC 개발 환경에서는 테마 변경이 즉시 반영되도록 Keycloak theme cache와 static max-age를 비활성화.
  Keycloak 재생성 후 healthy 및 테마 CSS `Cache-Control: no-cache` 응답 확인.

### 2-G. 로그인 화면에 등록신청 버튼 추가 — ✅ 완료

- 첨부된 목업(카드 안에 아이디/비밀번호 + 빨간 로그인 버튼 + 아웃라인 등록신청 버튼)과 비교했을 때
  기존 `cerebro` 테마는 `#kc-registration`을 `display: none`으로 숨기고 있었고, realm도
  `registrationAllowed: false`라 그 링크 자체가 렌더링되지 않는 상태였음.
- Keycloak 기본 템플릿(`keycloak.v2`의 `login.ftl`)을 직접 열어 실제 마크업을 확인
  (`#kc-registration-container` > `#kc-registration` > `<span>{noAccount} <a>{doRegister}</a></span>`).
  이걸 기반으로: `noAccount` 메시지를 빈 문자열로 비우고 `doRegister`를 "등록신청"(en: "Request Access")으로
  덮어써서 안내 문구 없이 링크만 남긴 뒤, 그 링크를 로그인 버튼과 동일한 폭의 아웃라인 버튼(흰 배경, 빨간
  테두리/글자)으로 CSS 스타일링. `#kc-registration-container`는 ID 선택자라 기존의
  `.pf-v5-c-login__main-footer-band { display:none }` 규칙보다 우선 적용되어 다른 footer 요소는
  계속 숨겨진 채로 유지됨.
- realm의 `registrationAllowed`를 `true`로 전환(실행 중 Keycloak API + `realm-export.json` 둘 다).
- **알려진 제약**: Keycloak 기본 자기등록은 계정을 즉시 `enabled=true`로 생성한다 — 이전에 정한
  "등록신청 후 관리자 승인 대기" 정책은 아직 여기 반영되지 않음. 승인 대기 상태로 만들려면 커스텀
  Registration 인증 플로우(또는 Required Action)가 추가로 필요하며, 이번 범위는 목업과 동일한 화면
  구성(버튼 노출/스타일)까지만 처리함.
- **검증**: 로그인 페이지 실제 응답(HTML/CSS)에서 `kc-registration-container` 렌더링, "등록신청" 텍스트,
  갱신된 CSS 규칙(`#kc-registration a` 등) 전부 확인. 테마가 bind mount + 캐시 비활성화 상태라 별도
  재기동 없이 즉시 반영됨.

## 17. GitLab CI 보안 테스트 실패 수정 + dev 직접 CI 활성화 (2026-07-22) — ✅ 완료

- MR !5(`feature/unified-web-login`, commit `087cc6b`)의 `test:backend`가 36개 중
  `ConnectionControllerTest` 4개 실패. Runner/Docker/Testcontainers 문제가 아니라 Keycloak Resource
  Server 의존성 추가 후 `@WebMvcTest`가 사용자 정의 `SecurityConfig` 대신 Spring 기본 보안 체인을
  적용한 것이 원인.
- 실제 응답: POST/DELETE는 CSRF로 403, GET은 미인증으로 401. 운영 설정은 업무 API `permitAll` +
  CSRF disabled인데 MVC slice가 이를 불러오지 않아 테스트와 운영 설정이 달라졌음.
- `ConnectionControllerTest`에 `@Import(SecurityConfig.class)`를 추가해 운영 보안 체인을 명시적으로
  로드. 실패했던 4개 테스트 로컬 재실행 전부 통과.
- 전체 로컬 테스트는 36개 중 35개 통과. 남은 `ConnectionRepositoryIT` 1개는 기존 WSL2 Docker
  discovery 문제(`DockerClientProviderStrategy`)이고, 동일 GitLab DinD 실행에서는 이 통합 테스트가
  통과했으므로 코드 회귀가 아님.
- `.gitlab-ci.yml` rules가 `main || develop`만 대상으로 하고 실제 공유 브랜치 `dev`를 빠뜨려,
  `dev` 직접 push가 CI를 우회하는 문제도 발견. validate/test/build 네 job 모두 `dev`를 포함하도록 수정.

## 18. Keycloak 완전 제거 → 계정 테이블 기반 자체 JWT 인증 전환 (2026-07-26) — ✅ 완료

- **배경**: Keycloak SSO의 크로스오리진 iframe 로그인 실패(`KC_STATE_CHECKER` 쿠키 SameSite=Strict가
  크로스사이트 iframe 임베딩과 근본적으로 불호환)를 조사하다, 우회 대신 Keycloak 자체를 제거하기로 결정.
- **결정된 아키텍처**: 인증은 실제 회사 공유 계정 테이블(MySQL `ST_USER`, host 192.168.50.30)을
  조회해 pipeline-api가 자체 서명 JWT(jjwt)를 발급하는 방식으로 전환. NiFi/Airflow는 개인 계정 대신
  공유 서비스계정(NiFi Single User, Airflow FAB DB auth)으로 바꾸고, 그 자격증명은 cerebroetl-ui의
  nginx가 서버사이드에서 모든 프록시 요청에 주입(브라우저는 자격증명을 들고 다니지 않음 - JWT 검증
  없이 무조건 통과, 사용자 명시 결정). **ST_USER는 여러 시스템이 공유하는 실제 사용자 디렉터리라
  절대 쓰기(INSERT/UPDATE/DELETE) 없이 조회만 한다** - 이 제약은 향후에도 반드시 지킬 것.
- **NiFi**: OIDC → Single User 전환(`nifi.sh set-single-user-credentials`), `users.xml`/
  `authorizations.xml`에 기존 admin의 13개 정책을 새 identity로 수동 복제(Initial Admin Identity
  부트스트랩은 최초 1회만 동작해서 재사용 불가였음).
- **Airflow**: FAB `AUTH_OAUTH`(Keycloak) → `AUTH_DB`. `_get_nifi_token()`(DAG)과 pipeline-api의
  `NifiClient`도 NiFi Bearer 토큰 발급 방식이 바뀌어 함께 수정 필요했음(원래 7개 작업 항목에
  없었던 필수 동반 수정).
- **cerebroetl-ui**: `oidc-client-ts`/`PlatformSessionBootstrap` 제거, `ST_USER` 기반 로그인 폼 +
  로그인 성공 시 JWT를 localStorage에 저장해 `Authorization` 헤더로 붙이는 방식으로 재작성
  (web-client-svr과 동일 패턴). 외부 포트도 이 작업 직후 HTTPS→HTTP로 전환(내부 NiFi HTTPS는 유지 -
  MSA 포털도 http라 scheme 통일 목적, 브라우저의 Secure 쿠키 제약이 사라져서 가능해짐).
- **소스 정리**: `keycloak` 서비스 블록/`extra_hosts`/전 서비스의 `KEYCLOAK_*` 환경변수·볼륨마운트를
  `docker-compose.yml`에서 전부 제거, `.env`/`.env.example`의 `KEYCLOAK_*`·`PORTAL_PUBLIC_HOST` 변수
  전부 삭제(`.env.example`엔 새로 필요해진 `PIPELINE_JWT_SECRET`/`ACCOUNT_DB_*` 자리도 추가),
  `db/metadata-init/01_create_keycloak_db.sql`과 `keycloak/`(realm-export.json/certs/themes) 디렉토리
  삭제. 코드 내 "Keycloak 제거"를 근거로 남긴 설명용 주석(왜 필드가 nullable인지 등)은 그대로 둠.
- **미반영 상태로 남겨둔 것**: `docs/poc-presentation.md`, `docs/project-demo-guide.md`는 여전히
  Keycloak 기반 로그인 흐름/데모 스크립트를 현재형으로 서술 중 - 발표 자료 성격이라 재작성 방향은
  사용자 확인 후 진행하기로 함. `db/metadata-init`는 볼륨 최초 생성 시에만 실행되므로, 이미 만들어진
  로컬 metadata-db의 `keycloak` DB 자체는 orphan으로 남아있음(운영 영향 없음, 필요시 수동 DROP).
- **범위 제약**: 이번 작업은 로컬 docker-compose 환경에 한정 - MSA 서버(192.168.50.30) 배포/빌드는
  일절 하지 않음(사용자 지시, 별도 요청 시에만 진행).
- 커밋 `2c0bbd4` 후 `feature/airflow-oidc`를 `dev`에 fast-forward 병합하고 원격 push 완료.

## 19. 통합 운영 대시보드 기능개선 착수 (2026-08-17) — 🚧 진행 중 (Phase 0)

**기반 설계서**: `/home/user/기능개선/대시보드_구현설계.md`(4,715줄). 관제/장애/알림 중심의 통합
운영 대시보드로 개편. 신규 Flyway 대역 **V22~V42**(현재 V21 다음, 충돌 없음), 전부
`metadata-db`(Postgres) 대상. MySQL(`ST_USER`)에는 마이그레이션이 나가지 않는다(§18 원칙 유지,
Postgres `@Primary`라 Flyway가 계정 DB를 집어가지 않음).

### 19.0. 재택 개발 환경 구성 (사내 MySQL 접근 불가 대응)
- 사내망에서만 닿는 `ST_USER`(MySQL, 192.168.50.30)는 로그인 검증 전용이라, 재택에서는 로컬
  목업으로 대체. `docker-compose.override.yml`에 `account-db-local`(mysql:8.0) 서비스를 추가하고
  `local-dev/st_user-init.sql`로 `ST_USER` + 테스트 계정 1개(`admin`/`admin`, bcrypt)만 시드.
- **커밋 무영향 보장**: override 파일과 `local-dev/`는 `.git/info/exclude`(이 클론에서만 무시,
  `.gitignore`와 달리 커밋 안 됨)에 등록. `.env`의 `ACCOUNT_DB_HOST`만 `account-db-local`로 바꿈
  (`.env`는 원래 gitignore). 사무실 복귀 시 `192.168.50.30`으로 되돌리면 원복.
- 실제 로그인 API(`POST /api/auth/login`)로 `admin`/`admin` 통과 + 오답 `INVALID_CREDENTIALS` 확인.

### 19.1. 착수 전 결정 — D-5 (성능테스트 잔여물 처리) → "삭제 안 함 + env_kind 생략"
- 설계서 D-5(9-2절)는 `etl_job`의 `PERF-*` 5행 + `pipeline_daily_load_metric` 100만~204만 건을
  **삭제할지 / `env_kind=TEST`로 가릴지** 결정하라고 남겨둠. 설계 기본안은 후자(env-tag).
- **실측 확인**: 이 PERF 데이터는 **로컬 `metadata-db`에 존재하지 않는다**(`etl_job` 실제 4행은
  NiFi PG 미러, `pipeline_daily_load_metric` 0건). 설계서 수치는 사내 인스턴스 기준이고, 사용자
  확인상 **고객사에도 없는 개발 전용 데이터**.
- **결정**: 삭제도 하지 않고, `env_kind`/`pipeline_env_tag`도 만들지 않는다.
  - 근거: `env_kind`(PROD/TEST/EXCLUDED)의 유일한 실제 근거가 PERF 분리(U7 DoD, 설계서 line 4289)
    뿐이고 `EXCLUDED`는 enum 정의에만 있고 의존 쿼리가 없음. 고객사에도 있는 노이즈 DAG(NiFi 카운터
    수집)는 env_kind가 아니라 신선도/판정불가 로직으로 별도 처리 → 제거해도 고객 배포판 무영향.
    개발 로컬 대시보드에 PERF 노이즈가 섞이는 건 수용.
  - **반영**: 신규 테이블 **29개 → 28개**(`pipeline_env_tag` 제외). `pipeline_load_rollup`(V26)·
    `airflow_dag_run`(V29)에서 `env_kind` 컬럼·CHECK·인덱스 제거. `/api/env-tags*` API·`^PERF-`
    SettingKey 시드 제거. (두 테이블 모두 아직 미존재라 ALTER가 아니라 "처음부터 그 컬럼 없이 CREATE".)
  - **되돌리기**: 향후 "잡별 KPI 제외"가 필요하면 `ALTER TABLE ... ADD COLUMN env_kind DEFAULT 'PROD'`로
    안전하게 재도입 가능(기존 데이터 무해한 추가형).

### 19.2. 기존 스키마 영향 요약 (착수 전 확인)
- **DROP(테이블 삭제): 없음.**
- **기존 테이블 변경: 2개뿐** — ① `app_user`: V24에서 로컬 인증 컬럼 4개 ADD(순수 추가, 안전)
  ② `pipeline_metric_snapshot`: V27에서 일 RANGE 파티션으로 전환하되 신규 파티션 테이블 생성 →
  INSERT SELECT → 원자적 RENAME 교체 + 구 테이블 `_v0` 보존(데이터 손실 0, 현재 39,961행이라 전환비 ~0).
- **기존 데이터: 지우지 않음**(D-5 결정).

### 19.3. Phase 0 스키마 마이그레이션 적용 완료 (V22~V24)
로컬 `metadata-db`에 실제 적용·검증 완료(Flyway `now at version v24`).
- **V22** `create_app_setting`: `app_setting`(override-only 설정 저장, CHECK 제약 7종 value_type),
  `app_setting_alias`(설정 키 rename 이관표). 신규 테이블은 설계 §3-3대로 전부 TIMESTAMPTZ.
- **V23** `create_platform_runtime`: `system_heartbeat`(수집기 생존, 컴포넌트당 1행),
  `collector_outage`(결측 구간, `system_heartbeat` FK + 열린구간 부분 UNIQUE),
  `retention_policy`(보존정책, **시드 5행** - metric_snapshot은 PARTITION_DROP 예약),
  `install_info`(싱글턴, `gen_random_uuid()`로 install_id 시드, display_zone Asia/Seoul).
- **V24** `local_auth`: `app_user`에 `pw_updated_at`/`pw_must_change`/`login_fail_count`/`locked_until`
  4컬럼 ADD(하위호환 규칙 2대로 NOT NULL엔 DEFAULT). `account_kind`는 R1대로 제외.
- **번호 정합성**: 설계서는 baseline을 V20으로 가정하고 V21을 권한 문서에 예약했으나, 실제 로컬은
  V21이 `create_nifi_canvas_status_label`로 채워져 있었음. 대시보드 대역(V22~)은 그대로 유효해
  재번호 불필요. 각 마이그레이션 실행 전 `BEGIN…ROLLBACK` 문법 사전검증으로 부팅차단형 실패 예방.
- **빌드 환경**: §9의 WSL2 `error getting credentials`(credsStore=desktop.exe)로 `--build`가 실패 →
  `~/.docker/config.json`을 `{}`로 비워 우회(원본은 `config.json.bak`, 공개 이미지만 받으므로 무해).
  이후 빌드 정상. 빌드 중 메모리 확보 위해 `target-db`를 잠시 stop 후 복구.
- **미완(Phase 0 코드 단위)**: SettingKey enum/SettingService, 스레드풀 격리, HeartbeatComponentRegistry,
  보존 배치 잡 등은 아직. 이번 증분은 스키마까지.

### 19.4. U1 · 스케줄러 격리 + 클라이언트 타임아웃 + 시계 정합 — ✅ 코드 완료·검증
설계서 §3-2/§3-3. 모든 신규 주기작업의 실행 기반이라 Phase 0 맨 앞.
- **스레드풀 격리** — `common/config/PlatformSchedulerConfig`(신규): `taskScheduler`(collect- pool4)/
  `controlPlaneScheduler`(ctrl- 3)/`batchScheduler`(batch- 2)/`watchdogScheduler`(watchdog- 1).
  `TaskScheduler` 빈이 2개 이상이면 이름 폴백(`DEFAULT_TASK_SCHEDULER_BEAN_NAME="taskScheduler"`)이
  발동하는 성질을 이용해, scheduler 미지정 기존 @Scheduled 8개를 collect- 풀에 자동 배선.
  `common/config/AlertSchedulerErrorHandler`(신규)로 관제/배치 루프의 조용한 죽음 방지.
- **외부 클라이언트 타임아웃**(§3-2 표) — `NifiClient`/`KafkaConnectClient` connect 2s·read 5s
  (JdkClientHttpRequestFactory, AirflowHealthClient 패턴 계승), `KafkaBrokerHealthChecker`에
  `default.api.timeout.ms=5000`, `application.yml`에 Hikari connection-timeout 3s + postgres
  socketTimeout 10s(기존엔 타임아웃 전무).
- **시계 정합** — `common/config/ClockSkewChecker`(신규): ApplicationReadyEvent에서 `SELECT now()`를
  Instant로 받아 JVM 시각과 비교(TZ 표시차가 아니라 절대시각차만 검출), 60초 초과 시 WARN.
  자가진단 상시노출(CLOCK_SKEW)은 U5에서 결과를 읽어 연결. `docker-compose.yml`의 metadata-db에
  `TZ` 추가하되 **재생성은 보류**(기존 naive 컬럼 DEFAULT now() 불연속 방지 위해 U7과 함께 적용).
- **검증**: 빌드 성공·기동 healthy(10.8s). `/proc/1/task/*/comm`에서 `collect-1..4` 스레드 4개 확인
  (기존 수집 태스크가 격리 풀에 배선됨). ctrl-/batch-/watchdog-는 할당 작업이 아직 없어 지연 생성
  대기(정상). ClockSkewChecker 로그 "차이 0초". 클라이언트 타임아웃은 코드 반영·배포까지 확인
  (강제 타임아웃 재현은 운영 커넥터 영향 우려로 생략 - §5.2와 동일 판단).
- **미반영**: ScheduledTaskHolder 통합테스트(Testcontainers 로컬 불가, §9)는 미작성 - procfs 스레드명
  확인으로 대체. `install.sh` TZ 불일치 검사는 배포툴이라 이번 로컬 범위 밖.

### 19.5. U2 · 설정 카탈로그 + SettingService + 설정 API — ✅ 코드 완료·검증
설계서 §4-2. U3~U9가 전부 임계값을 읽으므로 하드코딩보다 먼저.
- **신규 패키지 `settings/`**: `SettingValueType`(app_setting CHECK와 일치), `SettingKey`(45키 카탈로그 =
  코드 기본값 단일 원천, key/type/default/min/max), `SettingService`, `SettingsController`.
- **SettingService**: TTL 30초 전체-override 캐시 + **3단 폴백**(캐시히트 / DB SELECT / DB실패시 만료캐시
  or 코드기본값) — **어떤 경우에도 예외를 던지지 않는다**(알림 평가 루프가 죽으면 안 됨). getInt/getLong/
  getBytes/getDecimal/getBoolean/getString. 부팅 시 `ApplicationReadyEvent`로 warn/crit 상호관계
  재검증 → 역전 시 안전방향 자동보정(UPSERT) + WARN(SETTING_AUTO_CORRECT), 부팅은 막지 않음.
- **설정 API** `/api/admin/settings`(permitAll): GET(전 키 실효값+메타), PUT(벌크, 전건 검증 통과 후에만
  저장 - 부분저장 방지), DELETE(재정의 제거=기본값 초기화). 타입/범위 위반은 `VALIDATION_ERROR` 400.
- **하드코딩 이관**: `ProcessHealthService`의 COLLECTOR_STALE_AFTER(3분)/DEAD_AFTER(10분) →
  `HEALTH_STALE/DEAD_SECONDS_NIFI`(180/600). SettingService 생성자 주입.
- **D-5 반영**: `metrics.exclude.label.patterns` 키는 두지 않음(PERF 제외 기능 미구현, WORK_LOG §19.1).
- **검증**: 빌드 성공(1차는 jdbc.query 람다 RowCallbackHandler/ResultSetExtractor 모호성으로 실패 →
  블록 람다로 해소 후 성공). GET 45키, PUT 200 왕복(overridden=true·DB 실제저장·updatedBy=admin),
  범위위반(5<min60)→400+메시지, DELETE→기본값 복귀, 부팅 재검증 "보정 0건"(빈 app_setting이라 정상).
- **미이관(플래그)**: `KafkaPipelineStateSynchronizer.FAILURE_THRESHOLD=3`,
  `NifiProcessorRunTracker.IDLE_CLOSE_SECONDS=45`, `NifiPipelineMetricScheduler.BULLETIN_DEDUPE=30`,
  `CdcLogService.DELAYED_LAG=1000` — **설계서 카탈로그에 대응 키가 없다.** 알림엔진(U9)에서 재설계될
  여지가 있어 임의 키 신설을 보류. U9 착수 시 매핑 확정 필요.
- **미반영**: CI "임계 상수 잔존 시 빌드 경고"(gradle 설정 작업), DB 정지 예외무발생은 폴백 코드로 커버하되
  실제 정지 테스트는 생략(운영 커넥터 영향 우려, §5.2 판단).

### 19.6. U3 · 로컬 인증 복원 + /api/health — ✅ 코드 완료·검증
설계서 §3-4 C9. **판매 차단 요소 해소** — ST_USER 없는 고객사가 부팅·로그인 하도록.
- **authz.provider**(application.yml, 기본 `LOCAL`): LOCAL=app_user bcrypt / EXTERNAL=사내 ST_USER.
  `AccountDataSourceConfig`·`AccountLookupService`를 `@ConditionalOnProperty(authz.provider=EXTERNAL)`로
  격리. LOCAL 에선 MySQL 빈이 안 생기고, 그러면 DataSource 모호성도 사라져 Spring Boot 자동설정이
  metadata-db(Postgres)를 단독 구성한다(그래서 @Primary 재선언도 EXTERNAL 전용으로 옮김).
- **인증 추상화**: `CredentialAuthenticator` 인터페이스 + `LocalCredentialAuthenticator`(LOCAL,
  matchIfMissing) / `ExternalCredentialAuthenticator`(EXTERNAL). `AuthController`가 인터페이스에 의존하도록
  리팩터(AccountLookupService·PasswordEncoder 직접의존 제거). authz.provider 로 정확히 1개 빈만 등록.
- **AppUser +4필드**: pw_updated_at·locked_until(TIMESTAMPTZ→`OffsetDateTime` 매핑, LocalDateTime 이면
  ddl validate 실패)·login_fail_count(int)·pw_must_change(boolean). V24 컬럼과 매핑.
- **실패 잠금**: 연속 실패 임계/잠금시간은 `SettingKey(auth.login.*)`. 임계 도달 시 locked_until 설정.
- **사용자 CRUD** `/api/admin/users`: POST 생성(bcrypt), GET 목록, PUT `/{id}/password`, DELETE.
- **/api/health**(신규, permitAll) + `docker-compose.yml` healthcheck 를 `/api/connections`→`/api/health`
  로 교체(인가 켜져도 항상 열림). SecurityConfig 에 `/api/health` permitAll 명시. ErrorCode `ACCOUNT_LOCKED`(423).
- **검증 중 버그 발견·수정**: `LocalCredentialAuthenticator.authenticate`가 `@Transactional`이라 실패 시
  INVALID_CREDENTIALS(RuntimeException) 던지면 방금 올린 login_fail_count 증가까지 **롤백**되어 잠금이
  영원히 안 걸렸다(1차 검증에서 6회차 성공·count=0으로 잡음). @Transactional 제거 → 각 save() 독립 커밋.
- **검증(재빌드 후)**: LOCAL 부팅 healthy(account-db-pool 로그 0건 = MySQL 빈 미생성), `/api/health` 200,
  기존 admin 행에 비번 세팅 후 admin/admin 로그인 성공, 틀린 비번 401, **실패 5회 → count=5·locked=t →
  6회차 올바른 비번도 423 ACCOUNT_LOCKED**, 사용자 CRUD 왕복.
- **동작 변화**: 기본 인증원이 LOCAL(app_user)로 바뀜 → 재택 로컬은 이제 account-db-local(ST_USER 목업)을
  안 쓴다(사무실과 동일하게 놀기만). EXTERNAL 로 돌리려면 `.env`에 `AUTHZ_PROVIDER=EXTERNAL`.
- **미반영**: 상용 아티팩트에서 mysql-connector 제외(D-17, build.gradle 프로파일 작업), 온보딩 최초 관리자
  생성은 U19(Phase 3). 지금은 permitAll 이라 `/api/admin/users`로 직접 생성 가능.

### 19.7. U5 · 하트비트 + 결측 구간 + 자가진단 — ✅ 코드 완료·검증
설계서 §4-3/§6-5/§6-7-2. 신규 패키지 `heartbeat/`.
- **HeartbeatComponentRegistry**(코드가 기대 컴포넌트 원천): kafka-metrics(20s)/nifi-counter(60s)/
  nifi-processor(15s). 리소스 수집기는 U36, 알림 엔진 컴포넌트는 U9/U11 에서 키 추가.
- **HeartbeatService**(JdbcTemplate): onReady 에서 3행 seed(last_beat_at NULL - now()로 시드하면
  UTC/KST 차로 즉시 오탐) + 재기동 결측(STARTUP) 기록. beat(주기 완주 시 UPSERT, observed_interval
  = 직전 beat 와의 실측 차) / beatFailed(연속실패++). 하트비트 기록 실패가 수집을 막지 않게 흡수.
- **WatchdogService**(watchdogScheduler 전용 스레드 60s): 실효임계 max(expected,observed)x3 초과 시
  collector_outage GAP 개시, 재개 시 종료. 보존정리가 수백만 행 돌 때도 살아있게 배치 풀과 분리.
- **SelfCheckController** `GET /api/admin/self-check`: 수집기 상태(UP/DEGRADED/DOWN/PENDING)+시계정합.
  "모름을 정상으로 안 칠함"(원칙 A): last_beat 없고 uptime>interval x5 면 PENDING 아니라 DOWN.
- **수집기 3곳 배선**: KafkaPipelineMetricScheduler/NifiPipelineMetricScheduler(checkCounters)/
  NifiProcessorRunTracker 가 주기 완주 시 beat. **NifiPipelineMetricScheduler 의 volatile
  lastCounterCheckAt 필드 제거** → DB 하트비트로 대체(재기동 넘어 이력 유지). ProcessHealthService 의
  "적재 지표 수집"이 metricScheduler.getLastCounterCheckAt() 대신 heartbeat.lastBeat("nifi-counter")
  (OffsetDateTime, KST 변환 표시)를 읽도록 이관.
- **검증 중 버그 발견·수정(크래시 루프)**: NifiProcessorRunTracker 에 HeartbeatService 필드를 추가하며
  기존 4-arg 생성자의 @Autowired 가 필드 위로 밀려나 고아가 됐다(final 필드 @Autowired 무효) → 생성자
  둘 다 @Autowired 없어져 Spring 이 no-arg 를 찾다 실패, RestartCount 26 크래시 루프. @Autowired 를
  주입 생성자로 되돌려 해소. (NifiProcessorRunTrackerTest 도 HeartbeatService mock 추가로 갱신.)
- **검증**: 빌드 성공·RestartCount 0·healthy. system_heartbeat 3행 seed → 65초 후 전부 beat(OK,
  observed 20/15/nifi-counter는 2회차 전이라 null). self-check 3수집기 UP·clockSkew 0. **pipeline-api
  75초 정지 후 재기동 → collector_outage STARTUP 2행(kafka-metrics/nifi-processor, nifi-counter 는
  임계 180초 미달로 제외 - 정확)** → 55초 후 워치독이 watchdog-1 스레드에서 결측 2건 종료 확인.
- **미반영**: 자가진단의 알림경로/저장소 섹션(U6/U11~ 이후), 프론트 자가진단 화면(백엔드 API까지만 -
  프론트는 §9 헤드리스 제약으로 이번 범위 밖), KafkaPipelineStateSynchronizer/NifiJobMirror 하트비트(핵심
  3수집기만 배선).

### 19.8. U6 · 보존 엔진 — ✅ RetentionService 완료·검증 / ⏸ V27 파티셔닝 보류
설계서 §4-3/C10. 신규 패키지 `retention/`.
- **RetentionService**(batchScheduler, 매시간): retention_policy 를 읽어 정리. 시간예산(기본 20분,
  SettingKey)까지 잔량 0 반복. DELETE_BATCH 는 ctid 서브쿼리 배치삭제(batch_rows). 정리대상은
  코드 화이트리스트(ALLOWED_TABLES)가 원천, 식별자는 quote_ident(동적 SQL이라 바인딩 불가).
  last_run_at/last_deleted_rows/last_duration_ms/backlog_rows/last_error 기록.
  **PARTITION_DROP-safe**: pipeline_metric_snapshot 이 아직 파티션 테이블이 아니면(V27 전) DROP
  PARTITION 을 시도하지 않고 backlog 로만 남긴다 - 일반 테이블에 매시간 예외를 내지 않기 위함.
- **RetentionController** `/api/admin/retention`: GET(정책+상태), PUT(보존일/배치/활성 - 하한은 DB
  CHECK 가 지켜 위반 시 400), POST `/run`(수동 트리거).
- **검증**: 빌드 성공·healthy. 정책 5개 조회. collector_outage 에 200일 지난 6000행 삽입 → POST /run
  → 잔량 0, last_deleted_rows=6000, backlog 0, 예외 없음(5000+1000 = 2배치, DoD "2번째 배치 예외 없음"
  통과). pipeline_metric_snapshot 정책은 "PARTITION_DROP 대기(V27 전) - 정리 보류"로 우아하게 스킵.
- **⏸ V27 파티셔닝 전환 보류(의도적, 신중)**: pipeline_metric_snapshot 을 일단위 RANGE 파티션으로
  전환하는 것은 **라이브 데이터 테이블 교체 + 일단위 파티션 수명관리(선생성/DROP)** 라, 파티션이
  롤오버 시점에 없으면 **적재 INSERT 자체가 실패해 지표 수집이 끊긴다**(무증상 아닌 즉각 장애). 로컬은
  현재 0행이라 전환 급하지 않고, 이 고위험 마이그레이션은 pg_partman 부재 하에 create-ahead 잡까지
  포함해 전용으로 신중히 다뤄야 한다. 서둘러 넣으면 "문제없이" 원칙을 어긴다 → 별도 착수로 분리.
  그동안 RetentionService 가 이 테이블을 안전하게 스킵하므로 매시간 예외는 없다. (V27 착수 시 DoD:
  파티션 전환 + create-ahead + DROP PARTITION 동작 + JPA validate 통과 + 롤오버 무중단 확인.)

### 19.9. Phase 1~2 데이터모델 전체 (V26~V41) — ✅ 적용·검증·커밋 (d08fef4)
설계서 4-5~4-8절. 판정·발송·리소스 시계열의 스키마 22테이블을 전부 깔았다(서비스는 후속).
- **적용·검증**: 각 마이그레이션 BEGIN…ROLLBACK 사전검증(설계서 DDL 오류 1건 발견·수정 - alert_rule
  인덱스가 없는 eval_priority 참조) → Flyway 실제 적용 `now at v41` → 22테이블 생성 확인 →
  파티션 테이블(metric_snapshot/infra_sample) 실제 INSERT 검증(DEFAULT 안전망 작동, 적재 무중단).
- **V27 파티션 스왑**: 앞서 미뤘던 고위험 항목을 DEFAULT 파티션 안전망(일 파티션 없어도 INSERT
  실패 안 함)으로 해소. 구 테이블 pipeline_metric_snapshot_v0 보존(롤백 안전).
- **파티션 관리 미완**: 일 파티션 선생성(PartitionMaintenanceJob)과 DROP PARTITION 은 서비스 후속.
  현재는 DEFAULT 파티션이 전부 받고 RetentionService 가 DELETE_BATCH 로 정리(동작하나 DROP 효율 미달).
- **미완(서비스 계층)**: U7 롤업 기록/U8·U36 신호·리소스 수집/U9·U37·U10 판정엔진·상태기계/
  U11~U15 발송/U16~U19 프론트 화면. 이번은 데이터모델까지.

### 19.10. U7 · 적재 롤업 기록 + KPI API — ✅ 코드 완료·검증
설계서 4-5절. 신규 패키지 rollup/.
- **RollupService**(JdbcTemplate): recordObservation 이 MIN5/HOUR/DAY 3해상도로 UPSERT. **delta==0
  에도 호출**해 observation_count 를 올린다("0건 관측"과 "미관측" 구분, 사고1 데이터모델 뿌리). 버킷
  경계는 install_info.display_zone 로컬 정시(저장은 절대시각 TIMESTAMPTZ).
- **수집부 가드 제거**: KafkaPipelineMetricScheduler.checkOne / NifiPipelineMetricScheduler.checkOne 의
  `if(delta>0)` 를 걷어내고 rollupService.recordObservation(...)로 매 관측 호출. 기존
  dailyLoadMetricService(pipeline_daily_load_metric)/nifi_execution_log 는 delta>0 에만 유지(하위호환).
- **KpiController** `/api/dashboard/kpi/timeline`(프리셋별 MIN5/HOUR/DAY 버킷) + `/summary`(소스별 합계).
- **검증**: 빌드·healthy(배선 성공). recordObservation UPSERT 로직 직접 검증(관측 2회 delta 0→100 →
  observation_count=2, loaded=100). KPI API 읽기 확인. (로컬 배포 파이프라인 0·NiFi 적재프로세서 없어
  실데이터 누적은 없음 - SQL 로직/배선/API 로 검증.)
- **미완**: 주기당 배치 flush 최적화(현재 직접 UPSERT), RollupBackfillJob(과거 백필), timeseries 차트 3종.

### 19.11. U36 · 리소스 시계열 수집(호스트) — ✅ 코드 완료·검증
설계서 4-8절. "서버 리소스 이력 저장소가 0곳"이던 것을 채운다.
- **ResourceSampleScheduler**(collect- 풀 60초): HostResourceService.collect() 재사용 →
  infra_resource_sample 에 HOST(MEMORY/CPU/LOAD1) + FILESYSTEM(DISK) 적재(분 단위 절삭, 충돌 DO
  NOTHING). infra_resource_series UPSERT(HOST=P0/FILESYSTEM=P1), infra-host 하트비트.
  HeartbeatComponentRegistry 에 infra-host 키 추가.
- **검증**: 빌드·healthy. 45초 후 infra_resource_sample 4행(DISK 8.1%/CPU 19%·8코어/LOAD1 2.75/
  MEMORY 60.2%) 실측 적재, series 4행, infra-host beat 확인. 파티션 테이블(DEFAULT)로 적재됨.
- **미완**: 컨테이너 cgroup·PSI·적응형 주기·롤업 다운샘플·마운트별 deadline(infraProbeExecutor).

### 19.12. U8/U9 · 알림 판정 엔진 수직 슬라이스 — ✅ 코드 완료·검증
설계서 6-1절. 이 설계 최대 단위의 동작하는 최소 골격. 신규 패키지 alert/.
- **AlertEngine**(controlPlaneScheduler 20초, 수집과 격리): 신호(infra_resource_sample)만 읽고 외부
  호출 안 함(원칙 B). 부팅 시 내장 규칙 시드(SERVER_MEMORY, HOST:host). 상태기계
  PENDING→FIRING→RESOLVED 를 관측 누적 초(true/false_observed_sec)로 판정(벽시계 아님). alert_instance
  UPSERT + alert_instance_event 기록. 낙관적 락(version).
- **AlertQueueController** `GET /api/dashboard/queue`: 열린 인스턴스를 심각도순으로. counts(critical/
  warning/info/unknown/acked/snoozed/suppressed) + items. RESOLVED 는 closed 라 대기열 제외.
- **검증(end-to-end)**: 임계 낮춰(30%) 현재 메모리 58.7% → FIRING 인스턴스 생성(이벤트 CREATED+FIRED),
  대기열 API critical=1 확인. 이어 clear 70·clear_seconds 20 으로 → RESOLVED(closed_at·display_until·
  reason=CONDITION_CLEARED, 이벤트 RESOLVED), 대기열 totalOpen=0. 규칙 운영 기본값 복원.
- **미완(엔진 확장)**: 규칙 카탈로그 다종(JOB/SERVICE/DATA 등)·params 정식 파서·UNKNOWN(신선도)·
  백프레셔 4단계·연쇄억제(suppressed_by)·플래핑·베이스라인·ack/스누즈 API·발송 연동(U11~).

### 19.13. U11 · 알림 발송 아웃박스 + 디스패처 — ✅ 코드 완료·검증
설계서 4-7절/6-2절. 신규 패키지 notification/.
- **NotificationService**: enqueueForInstance(FIRING 시 AlertEngine 이 호출) - 구독한 수신자별
  IN_APP/EMAIL notification_delivery 행 INSERT(심각도 x min_severity 필터, dedup_key=sha256, 수신자
  없으면 IN_APP 브로드캐스트 1행). dispatch(controlPlaneScheduler 5초) - PENDING/RETRY 를
  severity_rank 순으로 claim: IN_APP 즉시 SENT, EMAIL 은 릴레이 미설정이라 RETRY→DEAD.
- **AlertEngine 연동**: FIRING 확정 시 notifyFired() → notify_count++ + enqueue + NOTIFIED 이벤트.
- **검증(end-to-end 전체 파이프라인)**: 임계 낮춰 발화 → alert_instance FIRING → notification_delivery
  IN_APP 적재(ALERT-2-1, CRITICAL) → 디스패처 SENT, notify_count=1, NOTIFIED 이벤트 확인.
  즉 신호→규칙→상태기계→아웃박스→발송이 통째로 동작.
- **미완**: 실제 SMTP 발송(JavaMailSender)·심각도별 레이트리밋·서킷브레이커·배치 요약·재시도 백오프
  (U14)·데드맨(U15)·자기알림·수신자/구독 관리 API·IN_APP 종 배지 조회 API.

### 19.14. U16 · 조치 대기열 화면(프론트) — ✅ 빌드 검증
설계서 5-2 ①/6-3. 기존 화면을 안 건드리고 새 페이지로 추가(안전).
- **api/alerts.ts**: getAlertQueue() → /api/dashboard/queue.
- **pages/AlertQueuePage.tsx**: 심각도 칩(위험/경고/판단불가) + 항목 테이블(심각도/대상/내용/상태/지속/
  확인), 20초 폴링(useQuery). App.tsx 라우트 /alerts + AppLayout 사이드바 "조치 대기열" 메뉴.
- **검증**: docker node(§9)로 npm ci + tsc -b + vite build 성공(✓ 2.67s), dist 생성, 번들에 "조치
  대기열" 포함 확인. 헤드리스라 클릭 검증은 불가(§10) - 빌드+배선까지.
- **미완(프론트)**: KPI 2축 카드(현재 대시보드 요약카드 개편, U17)·확인/스누즈 모달(U18)·온보딩
  위저드(U19)·딥링크 이동·자가진단 화면. 백엔드 API 는 대기열/KPI 까지 준비됨.

### 19.15. 설정 API (알림 규칙·조치·발송 채널·수신자) — ✅ 코드 완료·검증
설계서 5-2 ⑤⑥⑨. "알림/발송을 어디서 설정하나"의 백엔드.
- **AlertAdminController**: GET/PUT /api/admin/alert-rules(규칙 목록·enabled/severity/params/for/clear
  수정), POST /api/alerts/{id}/ack·unack·snooze(조치 + alert_instance_event 기록).
- **NotificationAdminController**: GET/PUT /api/admin/notification/channels(on/off·config·레이트,
  시크릿은 secretSet 불리언만 노출), 수신자 CRUD + 구독 UPSERT(전화 마스킹), POST
  /api/notifications/channels/{type}/test(CHANNEL_TEST 아웃박스 적재).
- **검증**: 규칙 목록/수정(for_seconds 90), 채널 목록(시크릿 미노출), 수신자 생성+구독+마스킹
  (010-****-5678), 테스트 발송 200. 전부 확인 후 테스트 데이터 정리·규칙 복원.
- **알려진 소소한 이슈**: recipients 응답의 subscriptions(json_agg)가 PGobject 래퍼로 직렬화됨
  (값은 정상, 프론트 파싱 필요). 후속에서 ::text 캐스팅으로 정리 가능.

### 19.16. 설정 화면(프론트) — ✅ 빌드·배포 검증
설계서 6-7. /settings 새 페이지 + 사이드바 "알림/발송 설정" 메뉴.
- **api/config.ts** + **pages/SettingsPage.tsx**: 탭 3개(알림 규칙 on/off·심각도·발화/해제초 표시,
  발송 채널 on/off + 테스트발송 버튼, 수신자 추가/삭제 + 구독). useQuery + invalidate.
- **검증**: docker node 로 tsc+vite build 통과(2.29s), cerebroetl-ui 재빌드·배포, 새 번들에
  "알림/발송 설정" 포함(YES), 13001 경유 alert-rules/channels API 200. antd App 래퍼 부재로
  정적 message 사용(루트 무수정).

### 19.17. 확장①(규칙 breadth) · JOB 실패 규칙(이벤트 기반) — ✅ 코드 완료·검증
설계서 알림 규칙 다종화의 첫 규칙. 메모리(임계 기반)와 달리 **이벤트 기반**.
- **AlertEngine**: `JOB_FAILURE:all` 내장 규칙 시드(WARNING, mandatory, for_seconds=0). `evaluate()`가
  controlPlaneScheduler(20s)로 `evaluateJobRules()` 호출. 최근 10분 내 status='FAILED' 인 etl_job_run
  마다 target `JOB:{jobId}` 로 FIRING alert_instance 생성(last_failed_run_id 추적), 새 run 실패 시
  RECURRED 재알림, 마지막 전이 후 30분간 새 실패 없으면 자동 해소. notifyFired→발송 아웃박스 적재.
- **검증(수직 슬라이스 end-to-end)**: 실패 etl_job_run 삽입 → `[진단] JOB평가 ruleId=2 failed=1` →
  "JOB 실패 알림 생성 job=test" → 대기열 WARNING "ETL Job 실패: test"(JOB:1) →
  notification_delivery(IN_APP, **SENT**) → `/api/alerts/{id}/ack` 200, 대기열 acked=1 반영. 확인 후 진단로그 제거.
- **⚠ 검증 함정(타임존, 코드버그 아님)**: etl_job_run.ended_at 은 `timestamp without time zone`.
  앱 DB 세션 TZ=Asia/Seoul(JVM 기본, 로그 +09:00)이라 `ended_at > now()-10min` 비교가 KST로 수행됨.
  검증 시 psql **UTC 세션**으로 now() 삽입하면 ended_at 이 9시간 과거로 읽혀 10분 창 밖으로 밀려
  `failed=0`. 실데이터는 앱(KST)이 기록하므로 자기정합적 — 문제 없음. **검증용 삽입은 반드시
  `SET TIME ZONE 'Asia/Seoul'`(또는 앱 경유)로**. 이 함정 진단에 리빌드 2회 소요됨(교훈 기록).
- **잔여(로컬 런타임)**: 검증용 etl_job_run FAILED 행 + 그 알림 인스턴스가 로컬 metadata-db 에 남음
  (커밋과 무관한 런타임 데이터, 30분 후 자동 해소, 이미 ack). DELETE 는 분류기 차단으로 미실행.

### 19.18. 확장③(프론트) · KPI 2축 카드(U17) — ✅ 빌드·배포 검증
설계서 U17. 대시보드 상단에 "적재 KPI · 처리량·관측 2축" 카드 추가. 백엔드 KPI API(U7)는 기존 것 재사용.
- **api/kpi.ts**: getKpiSummary/getKpiTimeline + 타입. summary 는 Map(snake_case), timeline 은
  record(camelCase) 직렬화 차이 주석화.
- **components/KpiLoadTrend.tsx**: 프리셋(1h/24h/7d/30d, antd Segmented) + 소스별 Statistic(총 적재/관측)
  + DualAxes(@ant-design/plots v2) 2축 차트 — 시각별 합계로 접어 적재량(막대, 좌축)·관측수(선, 우축).
  useQuery 30s 갱신. 데이터 없으면 Empty.
- **DashboardPage.tsx**: import + dashboard-main 최상단 `<section className="dashboard-kpi-region">` 삽입
  (기존 로직 무수정, 저위험).
- **검증**: 롤업 시드(HOUR 24h×NIFI/CDC 48행) 후 KPI API 채워짐(NIFI 39,840·CDC 26,100), cerebroetl-ui
  재빌드(tsc -b+vite 통과, exit 0)·배포, 번들에 "처리량·관측 2축" 포함, nginx(13001) index 200 +
  /api/dashboard/kpi/summary 200(데이터). 사용자 최종 시각 인수 대기.
- **주의**: DualAxes 는 저장소 첫 사용. `{...config}`(children 배열=지오메트리 스펙) 스프레드 패턴 채택,
  un-annotated 로 tsc 통과 확인(ComponentProps 제약은 children:ReactNode 충돌 위험 있어 미사용).
- **검증용 롤업 데이터**: pipeline_load_rollup HOUR 48행이 로컬에 남음(커밋 무관 런타임, KPI 카드 표시용).

### 19.19. 확장③(프론트) · 자가진단 화면 — ✅ 빌드·배포 검증
설계서 자가진단. 기존 SelfCheckController(/api/admin/self-check) 재사용, 화면만 신설.
- **api/selfcheck.ts**: getSelfCheck() + 타입(CollectorStatus/SelfCheckResponse).
- **pages/SelfCheckPage.tsx**: 수집기별 상태표(라벨·지표원·상태·마지막완주·주기 기대/관측·결과·오류),
  15s 갱신, 상단 정상/지연/중단 집계 + 시계오차. 오차 30s 초과 시 Alert 경고. "판정이 아니라
  판정기 자체가 살아있는가"를 보는 화면(설계 원칙). 여기선 상태색 사용 허용(상태 판정 화면이므로).
- **App.tsx/AppLayout.tsx**: /self-check 라우트 + 사이드바 "자가진단"(MonitorOutlined) 메뉴.
- **검증**: 재빌드(exit 0)·배포, 번들에 "자가진단"·"self-check" 포함, nginx(13001) 경유
  /api/admin/self-check 200(수집기 4: kafka-metrics/nifi-counter/nifi-processor/infra-host, 전부 UP,
  skew 0). 사용자 최종 시각 인수 대기.

### 19.20. 확장①(규칙 breadth) · DATA 신선도(적재 정체) 규칙 — ✅ 코드 완료·검증
설계서 규칙 다종화 2번째. JOB(이벤트)·메모리(임계 누적)과 달리 **상태 기반 재조정**.
- **AlertEngine**: `DATA_FRESHNESS:all` 시드(WARNING, non-mandatory, params staleness_minutes=30).
  `evaluateDataFreshness()`: 최근 24h HOUR 롤업 활동이 있었으나 MAX(last_observed_at)이 임계(분)보다
  오래된 소스마다 `DATA:{source}` FIRING 생성. 정체 해제(신선 관측 재개)된 소스의 열린 인스턴스는
  자동 RESOLVED(CONDITION_CLEARED). notifyFired 로 발송 아웃박스 연결.
- **검증(fire+resolve end-to-end)**: AIRFLOW를 40분 정체로 시드(NIFI/CDC는 신선화) → 다음 사이클
  "DATA 신선도 알림 생성 source=AIRFLOW age=40m" → 큐 WARNING "적재 정체: AIRFLOW (40분 무입력)"
  (AIRFLOW만, NIFI/CDC 제외) → AIRFLOW 신선 버킷 재삽입 → "알림 해소 7" → RESOLVED·closed·큐에서 제거.
- **신호 TZ 안전**: last_observed_at 은 timestamptz 라 세션 TZ 무관(JOB 규칙의 timestamp-without-tz
  함정 없음). RollupService.recordObservation 이 last_observed_at=now() 갱신하는 것을 신호로 사용.
- **잔여(로컬 런타임)**: 검증용 AIRFLOW/신선화 롤업 행이 남음(커밋 무관 런타임 데이터).

### 19.21. 확장②(발송) · 실 SMTP 릴레이(JavaMailSender) — ✅ 코드 완료·검증
설계서 발송. 기존 EMAIL=NO_RELAY(RETRY→DEAD)를 실제 SMTP 발송으로 교체. 미설정 환경은 폴백 유지.
- **build.gradle.kts**: spring-boot-starter-mail 추가.
- **application.yml**: `spring.mail`(host 미지정 — 있을 때만 자동설정), `notification.email.enabled/from`.
  기본 enabled=false → 고객사 SMTP 없는 환경은 기존과 동일(NO_RELAY). 무회귀.
- **NotificationService**: `ObjectProvider<JavaMailSender>`(빈 부재 허용) 주입. dispatch() EMAIL 분기가
  enabled && 빈 존재 시 relayEmail()로 SimpleMailMessage 발송 → SENT, 실패 시 SMTP_ERROR RETRY/DEAD,
  주소 없으면 NO_ADDRESS DEAD. 빈 없으면(호스트 미설정) markNoRelay 폴백.
- **로컬 검증 배선(커밋 안 함)**: docker-compose.override.yml(.git/info/exclude)에 mailhog 서비스 +
  pipeline-api 에 SPRING_MAIL_HOST=mailhog/PORT=1025/NOTIFICATION_EMAIL_ENABLED=true env 주입.
- **검증(end-to-end)**: EMAIL delivery 행 INSERT → dispatch → status=SENT, "EMAIL 발송 완료 to=o***@…"
  → MailHog API(:8025) 수신 1건(to=ops@example.com, subj="[관제] SMTP 릴레이 검증"). 
- **후속(미구현)**: 심각도별 레이트리밋·서킷브레이커·배치요약(U12/U14 잔여).

### 19.22. 확장①(규칙 breadth) · 수집기 중단(데드맨, U15) — ✅ 코드 완료·검증
설계서 필수 알림 3종 완성(JOB 실패·메모리 압박·수집기 중단). WatchdogService 의 결측 감지를 알림화.
- **AlertEngine**: `COLLECTOR_DOWN:all` 시드(CRITICAL, mandatory). `evaluateCollectorOutage()`:
  WatchdogService 가 연 collector_outage(ended_at IS NULL)마다 `COLLECTOR:{key}` FIRING 생성
  (deep_link=/self-check 로 자가진단 화면 연결), outage 종료 시 자동 RESOLVED. 상태 기반 재조정.
- **역할 분리**: 결측 "감지"는 기존 WatchdogService(collector_outage 기록), 이 규칙은 "알림화"만.
  중복 없음. DATA 신선도와 동일하게 신호 테이블만 읽는다.
- **검증(fire+resolve)**: 가짜 수집기(test-collector) 히트비트+열린 outage 삽입 → CRITICAL
  "수집기 중단: 테스트 수집기 (5분 결측)"(tgt COLLECTOR:test-collector, link /self-check) →
  outage 종료 → "알림 해소" → 큐/열린outage 0. **덤**: 리빌드 중 앱 다운으로 실제 수집기
  (kafka-metrics·nifi-processor·nifi-counter) 결측이 워치독에 잡혀 함께 발화됐고, 복구 후 워치독이
  outage 를 닫자 규칙이 자동 해소 — 실환경 재기동 시나리오까지 검증됨.
- **잔여(로컬 런타임)**: test-collector 히트비트/종료된 outage 행이 남음(커밋 무관).

### 19.23. 파티션 유지잡(미래 월파티션 사전 생성) — ✅ 코드 완료·검증
설계서 파티션 유지. RetentionService 는 PARTITION_DROP(삭제)만 했고 **미래 파티션 생성이 없어**
새 데이터가 계속 DEFAULT 로 쌓이던 문제를 해결.
- **retention/PartitionMaintenanceService**: batchScheduler 로 6시간마다(기동 45s 뒤 1회) RANGE 파티션
  테이블(infra_resource_sample=sampled_at, pipeline_metric_snapshot=collected_at)에 "다음 달부터"
  3개월치 월파티션을 `CREATE TABLE IF NOT EXISTS … PARTITION OF … FOR VALUES` 로 확보.
- **안전 원칙**: 현재/과거 월은 DEFAULT 에 이미 행이 있어 생성 시 겹침 오류 → 오직 미래 구간만 만든다
  (미래엔 DEFAULT 행 없음). 기존 DEFAULT 백로그 재분배(일회성 이관)는 이 잡의 책임 아님(별도 마이그).
- **검증**: 기동 후 로그 "파티션 유지잡 완료: 2 테이블, 2026-09-01부터 3개월 확보(6건)", 두 테이블에
  _p202609/_p202610/_p202611 생성 확인. infra(timestamptz)는 KST 월경계(2026-08-31 15:00Z=09-01 00:00
  KST), snapshot(timestamp)은 리터럴 경계 — 컬럼 타입별로 정확. 재실행 시 IF NOT EXISTS 로 멱등.
- **효과**: 9월부터 신규 데이터가 월파티션에 떨어져 RetentionService 의 PARTITION_DROP 이 유효해짐.

## 20. 이번 세션(2026-08-18 새벽) 확장 요약 — 7유닛 검증·커밋
설계서 확장 3분야 전부 착수, 각 유닛 build→deploy→test 검증 후 개별 커밋:
- **규칙 breadth(①)**: JOB 실패(480d611)·DATA 신선도(71e29be)·수집기 중단/데드맨(9a66932).
  필수 알림 3종(JOB·메모리·수집기 중단) 완성.
- **발송(②)**: 실 SMTP 릴레이 JavaMailSender(a995bed), MailHog end-to-end 검증.
- **프론트(③)**: KPI 2축 카드(c8b4686)·자가진단 화면(d35536d).
- **인프라**: 파티션 유지잡(이번 커밋).
- **미구현(후속)**: 알림 레이트리밋·서킷브레이커·배치요약(U12/U14), 온보딩(U19), DEFAULT 백로그 일회 이관.
- **로컬 검증 배선(커밋 제외)**: docker-compose.override.yml 에 mailhog + SMTP env(.git/info/exclude).

## 21. PDF 검토(Cerebro_ETL_기능개선_검토) 대조 후 갭 수정 — 연쇄억제·심각도분기·대기열
PDF 5-1~5-7 요구사항과 병합본(내 작업 + 다른 PC pull) 대조 → 사용자 지정으로 2·4번만 수정
(1번 Oracle/요청건수 규칙, 3번 웹훅은 제외). build→deploy→test 검증 완료.

### 21.1 연쇄억제(5-7c) — ✅ 검증
- **AlertEngine.evaluateChainSuppression()**: evaluate() 끝에서 실행. (a) SERVICE_UNREACHABLE(브로커
  응답없음)가 FIRING 이면 열린 CONNECTOR_FAILED 를 그 아래로 suppressed_by 접기. (b) 상위원인 없이
  CONNECTOR_FAILED 가 MASS_SUPPRESS_THRESHOLD(3) 이상 대량이면 가장 오래된 대표만 남기고 접기.
  (c) 억제자가 닫히면 자동 해제. 대기열/카운트는 suppressed_by 로 이미 걸러짐.
- **검증**: SERVICE + CONNECTOR×3 삽입(memory rule_id 로 Candidate reconcile 회피) → "SERVICE 상위원인
  아래 CONNECTOR 3건 접기", suppressed=3, 대표만 노출 → SERVICE 닫기 → 해제 후 "대량 대표접기 2건"
  전환(대표1+억제2). 인과·해제·대량 3동작 확인.

### 21.2 심각도 발송분기(5-7d) — ✅ 검증
- **NotificationService.enqueueForInstance()**: 정보(INFO)=화면내(IN_APP)만, 외부채널 차단
  (rank>=2 early return). 위험(CRITICAL)=EMAIL 즉시(next_attempt_at now), 경고(WARNING)=배치 창
  지연(WARNING_BATCH_SECONDS=120s). insertDelivery 에 delaySeconds 파라미터 추가.
- **부수 버그 2건 수정**(같은 경로): ① min_severity 필터 **역전**(`? >= CASE` → `? <= CASE`) —
  전엔 min=WARNING 구독자가 CRITICAL 을 못 받는 반대 동작이었음. ② EMAIL target_address 에
  **마스킹된 주소**를 저장하던 것 → 실주소 저장(실 SMTP 발송이 마스킹주소로 실패하던 문제). 마스킹은
  표시 계층 전용.
- **검증**: CRITICAL→EMAIL 지연0 SENT / WARNING→EMAIL 지연120 PENDING / INFO 3건→IN_APP 3건·EMAIL 0
  (구독 min=INFO 로 매칭시켜도 코드가 차단). min 역전 수정으로 min=WARNING 이 CRITICAL 도 정상 수신.

### 21.3 조치대기열 2건(5-1 c·f) — ✅ 검증
- **AlertQueueController**: 큐 items 쿼리에 `AND ack_at IS NULL AND (snooze_until IS NULL OR
  snooze_until<=now())` 추가 — 확인(ack)/스누즈한 항목은 목록에서 제거(카운트·이력엔 유지).
- **ActionQueuePanel**: 그룹 내 정렬을 지속시간 내림차순(오래된순)→오름차순(최신순)으로 수정.
- **검증**: ack=7건 카운트되나 items 에 0건(제거 확인). 정렬은 tsc+vite 빌드 통과(컴포넌트 반영).

### 21.4 발견 사항(수정 안 함, 사용자 판단)
- **DATA_FRESHNESS 규칙이 현재 enabled=false** 상태(내 71e29be 는 enabled 로 시드; pull 이후 어딘가서
  비활성됨). 내 세션 검증 위해 잠시 켰다가 **발견 당시 상태(비활성)로 원복**함. 신선도 알림을 쓰려면
  설정 화면에서 켜야 함.

## 22. 사용자 요청 7건 반영 — 이력탭·수신자토글·채널라벨·KPI제거·대기열접힘·SMS/EMAIL발송
사용자 지정 7가지를 구현하고 build→deploy→test 검증. 로컬 검증 배선(mailhog·sms-echo, override)은 커밋 제외.

1. **알림 이력 → 설정 탭(최좌측) + 페이징·조회조건**: 사이드바 "알림 이력" 메뉴 제거, 헤더 종아이콘은
   /settings?tab=history 로 이동. `AlertHistoryTab`(신규) — antd Table 서버 페이징 + 조회조건(기간
   1/7/30/90일, 심각도 ALL/위험/경고/정보, 확인여부, 내용·대상 검색). 백엔드 history API 확장
   (severity·q·page·pageSize·total). 기존 AlertHistoryPage.tsx 삭제, /alerts/history 라우트 제거.
   검증: page/pageSize/severity=CRITICAL(총 44, 3건), q="수집기"(39건) 필터 동작.
2. **수신자 사용여부 on/off**: RecipientsTab 전화 오른쪽에 enabled Switch 컬럼. updateRecipient 에
   enabled 파라미터 추가(프론트/백엔드). 검증: PUT {enabled:false↔true} 반영.
3. **발송채널 IN_APP → «화면알림»**: IN_APP 이 조치대기열/화면 알림이라 사용자 표기를 화면알림으로
   (EMAIL→이메일, SMS→SMS(문자)). 표시 라벨만 변경(내부 코드값 유지).
4. **대시보드 KPI 2축 차트 제거**: 대시보드가 벅차다는 지적 반영, KpiLoadTrend 제외(파일·api/kpi 삭제).
   하단 KPI 지표·차트는 유지.
5. **조치대기열 기본 접힘**: 대시보드 진입 시 기본 접힘(open = manuallyOpen ?? false). 배지로 건수는 보임.
6. **SMS·EMAIL 테스트발송 실동작**: (원인) 테스트발송이 수신자 주소 없이 INSERT → EMAIL 은 NO_ADDRESS,
   SMS 는 릴레이 자체 없음. (조치) 테스트발송을 «등록된 활성 수신자의 실제 이메일/전화»로 각각 보내게
   변경. SMS 릴레이 신설(NotificationService.relaySms) — notification.sms.gateway.url 로 {to,text} JSON
   POST, 미설정이면 NO_RELAY(EMAIL 의 SMTP 와 동일 구조). 검증: EMAIL 2건→MailHog 수신, SMS 2건→
   sms-echo 게이트웨이 POST 200(01099998888/01046550716). **운영 주의**: 실제 문자 발송은 사내/사업자
   SMS 게이트웨이 URL(NOTIFICATION_SMS_GATEWAY_URL)이, 이메일은 SMTP(SPRING_MAIL_HOST)가 있어야 함.

## 23. 설정/대시보드 추가 수정 — 페이징·필터·대기열메뉴제거·KPI명·카운트일치
사용자 추가 요청 반영. build→deploy→test 검증.

1. **알림 이력 기본 페이징 10**: AlertHistoryTab pageSize 기본 20→10(옵션 10/20/50/100).
2. **알림규칙·수신자 사용여부 필터 + 페이징**: RulesTab/RecipientsTab 에 사용여부(전체/사용/미사용)
   Select 필터 + 클라이언트 페이징 기본 10. (소규모 목록이라 클라이언트 측 처리.)
3. **조치 대기열 메뉴 제거 + 모두보기 팝업**: 사이드바 "조치 대기열" 메뉴 삭제(/alerts 라우트·
   AlertQueuePage.tsx 제거). 이력은 알림 이력 탭에 모두 있음. 대시보드 상단 대기열 패널의
   «모두 보기»는 이제 페이지 이동 대신 Modal 팝업으로 전체 목록 조회(renderItem 공용화).
4. **대시보드 KPI 명칭·라벨**: 「스트림 · CDC/플로우 · ETL/배치 · Airflow」→「CDC/ETL/Airflow」,
   KPI 카드 내 구분선 "지금"→"현재".
5. **대기열 배지 vs 목록 건수 불일치 수정**: 배지(counts.critical/warning/info)가 acked/snoozed/
   suppressed 를 포함해 목록(조치대상만)과 어긋났다(위험1/경고3 배지 ↔ 경고2 목록). counts 와
   totalOpen 을 목록과 같은 «조치가능» 필터로 맞춤. 검증: 배지==목록(위험1/경고2), WARNING ack 시
   배지·목록 동시 감소 + acked 카운트로 이동.
6. **자가진단 필요성 검토(코드 변경 없음, 분석·보고)**: 수집기 4종(kafka-metrics/nifi-counter/
   nifi-processor/infra-host)은 pipeline-api 내부 @Scheduled 로 Kafka/NiFi/호스트를 폴링, 성공 beat()/
   실패 beatFailed(). 외부 시스템(Kafka/NiFi) 장애 시 해당 수집기만 DOWN 가능. 가치는 "감시기가
   거짓 초록을 내는" 상황 포착(Debezium형). COLLECTOR_DOWN 알림이 이미 대기열/이력에 이를 노출하고
   /self-check 딥링크로 연결. 결론: 워치독+알림은 유지 가치 있으나 «자가진단 메뉴»는 선택(사용자
   판단 대기) — 제거해도 알림 딥링크로 상세 접근 가능.

## 24. 설정 메뉴 재편·화면알림 채널화 제거·대시보드 차트 정리·자가진단 메뉴 제거
사용자 추가 요청. build→deploy→test 검증.

1. **화면알림(IN_APP) 채널 관리 제거**: 발송채널 목록에서 IN_APP 제외. 화면알림은 별도 토글이 아니라
   «알림 규칙을 사용으로 켜면 자동 표시»(안내문 추가). 외부발송(이메일·SMS)만 채널로 관리.
2. **대기능 메뉴 재편**: 「알림/발송 설정」 최상위 메뉴 → 「설정」 그룹으로, 위치를 CDC 밑으로.
   하위: 알림/발송 관리(/settings), 사용자 관리(/users), 권한 관리(/permissions). 사용자·권한은
   플레이스홀더 페이지(준비 중). 그룹 active 로직을 하위 경로까지 인식하도록 보강.
3. **대시보드 로컬 기간칩 제거**: TrendCharts 의 기간(1h/24h/7d/30d)·자동갱신 토글 삭제 —
   차트는 상단 RangePicker(appliedRange) 기간조건을 따른다.
4. **중단 추이차트 삭제**: 실패 건수 추이·지연(미처리) 추이·Top5 3종(TrendCharts) 통째 제거
   (레이아웃 과밀 해소). TrendCharts.tsx 삭제.
5. **하단 ETL Job Top 5 탭화**: 기존 건수 전용 → 건수/소요시간/실패 탭(JobTop5Card 신규).
   백엔드 /dashboard/top5 에 from/to(YYYY-MM-DD) 파라미터 추가 → 상단 RangePicker 날짜범위로 집계.
   검증: metric=duration&from&to → 3건(test job 30s) 정상.
6. **자가진단 메뉴 제거(+분석)**: 사이드바 자가진단 메뉴 삭제. /self-check 라우트·화면은 유지
   (COLLECTOR_DOWN 알림의 딥링크 대상). 분석: "워치독+COLLECTOR_DOWN"은 화면이 아니라 백엔드
   메커니즘(수집기 결측을 감지해 대기열 알림화). 수집기 4종은 pipeline-api 내부에서 Kafka/NiFi/호스트를
   폴링 → 외부 시스템 장애 시 해당 수집기만 DOWN. 조치는 외부 시스템/인프라 쪽이라 화면에서 직접 못
   고침 → 메뉴 존치 실익 낮아 제거. 완전 삭제(라우트·알림규칙 포함) 원하면 후속.

## 25. 대시보드 레이아웃 조정 + SMS 실제 알림 발송 추가
사용자 요청. build→deploy→test 검증.

1. **Job Top5 설명(ⓘ) 제거**: JobTop5Card 하단 힌트 문구 삭제 — 차트 영역을 가리던 설명 제거.
2. **KPI 카드 압축 → 차트 확대**: 요약카드 패딩/값 폰트/now-divider/metric-row/details/links 여백을
   전반 축소(index.css). summary-grid 높이가 줄어 stats-grid(차트 4칸)가 그만큼 커진다.
3. **인프라 영역**: "시스템 리소스" 헤딩 + 가동시간 제거(InfraRegion) → 리소스 타일이 위로.
   주요 프로세스 현황 카드 min-height 120→190px 로 키워 가시성 향상. (formatUptime/system 정리.)
4. **SMS 실제 알림 발송 추가**: enqueueForInstance 에 SMS 블록 신설 — 그동안 실제 알림은 IN_APP/EMAIL
   만 만들고 SMS 는 «테스트 발송»만 됐다. 이제 SMS 구독(전화번호)+심각도 라우팅에 따라 실제 알림도
   SMS 발송건 생성. 검증: CRITICAL 수집기중단 → SMS 발송행 SENT → sms-echo 수신 확인.
   - **심각도 라우팅 재확인**: INFO=화면알림(IN_APP)만, WARNING/CRITICAL=EMAIL+SMS(WARNING 배치지연/
     CRITICAL 즉시). 각 수신자 구독 min_severity 로 추가 필터. «심각도 무관 전부 발송»이 아니다.
     (단, 발송채널 «테스트 발송» 버튼은 채널 점검용이라 심각도와 무관하게 즉시 발송.)

## 26. 스누즈 제거 · 해소 시 자동확인 · 알림이력 상세 한글화
사용자 요청. build→deploy→test 검증.

1. **스누즈 기능 제거**: ActionQueuePanel 스누즈 드롭다운·핸들러·옵션 삭제, 헤더 "스누즈 N" 카운트 제거,
   api/alerts snoozeAlert 삭제, 백엔드 AlertAdminController snooze 엔드포인트·SnoozeRequest 제거.
   (alert_instance.snooze_* 컬럼과 큐의 스누즈 필터는 무해하게 잔존 — 항상 null이라 no-op.)
2. **해소 시 자동 확인(ack)**: AlertEngine.resolve() 가 closed 처리 시 ack_by=COALESCE(ack_by,'SYSTEM'),
   ack_at=COALESCE(ack_at,now()) 도 세팅 → 해소된 건이 «미확인»으로 남아 종 배지를 부풀리지 않음.
   기존 해소+미확인 60건은 일회 백필(63→3 미확인). 사람이 이미 확인한 건은 그 기록 유지.
   검증: DATA 정체 발화→재개→RESOLVED+ack_by=SYSTEM 확인.
3. **알림 이력 상세 한글화**: EventTimeline 이벤트/상태 코드를 한글 라벨로(감지·생성/발화·알림시작/
   발송·알림전송/재발생/확인/확인취소/해소·조건해제, 상태 대기·발생중·해소·판단불가, actor SYSTEM→자동).
   상세 상단에 정보 블록 추가(내용·대상·유형(한글)·관측값/임계·상태·확인자·해소사유).

## 27. 규칙 재발송 정책(5) + ETL Job/CDC 파이프라인 감시범위(6·7)
사용자 요청. alert_rule 의 기존 컬럼(scope_json, renotify_seconds) 활용 + 로직·API·폼 신설.

**5. 재발송 정책** — ✅ 검증
- AlertEngine.evaluateRenotify(): 진행 중(FIRING)·미확인·미종료 알림 중, 규칙 renotify_seconds 간격을
  지났고 아직 최대(params.notify_max) 미만이면 notifyFired()로 재발송(notify_count++·아웃박스 재적재).
  확인/해소되면 자동 중단.
- 규칙 폼에 «재발송 간격(분)» + «최대 발송 횟수». (예: 즉시1 + 5분마다 + 총5회)
- 검증: JOB_FAILURE renotify 30s·max 3 → notify_count 1→2→3, "재발송 (3/3회)"에서 멈춤(4 안 감).

**6·7. 감시 범위(scope)** — ✅ 6 검증 / 7 대칭 구현
- scope_json = {"kind":"ALL"} | {"kind":"INCLUDE","ids":[...]} | {"kind":"EXCLUDE","ids":[...]}.
  ids = JOB규칙이면 etl_job.id, CDC규칙이면 pipeline_definition.id.
- AlertEngine: evaluateJobRules + consecutiveFailure/notRun 신호는 job_id로, cdcLag/connectorFailed
  신호는 pipeline_id로 scope.allows() 필터. ScopeFilter 헬퍼(Jackson 파싱).
- API: 규칙 생성/수정에 scopeJson·renotifySeconds 추가, GET /admin/alert-scope-targets?category=ETL|CDC
  (잡/파이프라인 목록).
- 폼: JOB/CDC 유형일 때 «감시 범위(전체/포함/제외) + 대상 다중선택» 노출(SCOPE_TYPES).
- 검증: JOB_FAILURE 스코프=EXCLUDE test(job1) → test 실패는 알림 없음, DW 실패는 알림 발생.
  CDC(파이프라인)는 동일 코드 경로 + 대상 API 동작 확인(로컬에 pipeline_definition 데이터가 없어
  실발화 테스트는 생략, JOB 스코프와 대칭).

## 28. 감사 로그 실제 클라이언트 IP 기록 (front-proxy)
사용자 요청. 감사 로그 `client_ip`에 작업 PC IP 대신 `172.18.0.1`(브리지 게이트웨이)이 남는 문제.
원인은 앱이 아니라 docker 포트 매핑(docker-proxy)의 SNAT — 컨테이너가 보는 `$remote_addr`
자체가 이미 게이트웨이라 헤더로 복원 불가.

**선택** — front-proxy 컨테이너(`network_mode: host`)를 공개 포트 앞단에 둔다.
- 버린 안①: docker daemon `userland-proxy: false` → 호스트 전역 설정이라 같은 호스트의 다른
  솔루션 컨테이너까지 영향 + docker 재시작(전 컨테이너 재시작) 필요.
- 버린 안②: cerebroetl-ui 자체를 host 네트워크로 → nginx.conf가 도커 내장 DNS
  (`resolver 127.0.0.11`)와 서비스명 upstream에 의존, kafka-connect는 호스트 포트가 없음,
  NiFi mTLS의 Host/SNI 전제도 깨짐 → 성립 불가.

**구현**
- `deploy/front-proxy/default.conf.template` 신설. 공개 UI 포트(13001)와 pipeline-api 직접
  포트(18086) 두 개를 받아 각각 루프백의 13101/18186으로 넘긴다. 경계선이므로
  `X-Forwarded-For $remote_addr`로 **덮어쓴다**(append하면 위조 헤더가 체인 맨 앞에 남음).
  websocket 업그레이드·`client_max_body_size 1024m`(NiFi 업로드)·`Host $http_host` 보존
  (뒤쪽 `return 308 http://$http_host/nifi/`가 공개 주소로 나오게).
- 이미지는 `data-pipeline-cerebroetl-ui` **재사용** + 엔트리포인트만 nginx:alpine 원래 것으로
  되돌림(envsubst가 템플릿 치환) → `offline/image-manifest.json` 반입 목록 불변.
- UI/API 컨테이너는 `127.0.0.1`에만 바인딩 → 공개 포트 우회로 XFF 위조하는 경로 차단.
- 켜고 끄기는 `docker-compose.realip.yml` 오버라이드 한 장으로 끝난다. 서버 `.env`에
  `COMPOSE_FILE=docker-compose.yml:docker-compose.realip.yml` 한 줄만 넣고 평소처럼
  `docker compose up -d`. 그 파일이 front-proxy 추가 + UI/API 포트를 루프백으로 비키는
  것(`ports: !override`)을 한꺼번에 한다. 기본(한 줄 없음)은 기존과 완전히 동일.
  공개 포트 번호(13001/18086)는 그대로라 문서·MSA 포털 설정 변경 불필요.
  Docker Desktop(WSL)은 host 네트워크 포트를 호스트로 노출하지 않으므로 리눅스 서버 전용.

- 안전장치: 템플릿을 이미지에 굽지 않고 마운트하므로, 서버에 파일이 없으면 도커가 빈
  디렉터리를 만들어 nginx 가 UI 설정으로 떠 버린다(공개 포트 안 열림 + 호스트 80/8080 점유).
  `create_host_path: false` 로는 못 막는 걸 실측 확인 → 엔트리포인트에서 파일 존재를 먼저
  검사하고 없으면 메시지 남기고 exit 1(포트 미점유, 크래시 루프로 즉시 눈에 띔).

**검증** — ✅ 브리지 컨테이너(172.18.0.13)에서 호출:
front-proxy 액세스로그 `172.18.0.13`(수정 전 `172.18.0.1`), cerebroetl-ui가 받은 XFF
`172.18.0.13`, `permission_audit_log.client_ip=172.18.0.13` (UI 경유 :13001·API 직접 :18086
양쪽 모두). 오버라이드로 단순화한 뒤 재검증(`__iptest2_*`), 오버라이드 해제 후 로컬 기본
동작(13001/18086, NiFi·Airflow 프록시) 회귀 없음 확인.
문서: `docs/audit-client-ip-front-proxy.md`.

## 29. 알림 발송(이메일·SMS) 폐쇄망 반입 준비

폐쇄망은 `docker compose build`가 불가능해(의존성 다운로드·베이스 이미지 pull) 현장에서
코드를 못 고친다. 그런데 SMTP 릴레이 주소도, 문자 게이트웨이 규격도 **가 봐야 안다**.
"가서 확인하고 코드 고쳐 재반입"을 없애려고, 현장에서 달라질 수 있는 지점을 전부 설정으로
뺐다. 반입 후 절차는 `.env` 작성 → 재기동 → 화면에서 수신자 등록 → [발송] 테스트로 끝난다.

1. **compose 전달 라인 신설**: `docker-compose.yml`의 pipeline-api에 알림 환경변수가 **한 줄도
   없었다**. compose는 명시한 것만 컨테이너로 넘기므로, `.env`에 SMTP를 적어도 앱까지 가지
   않아 조용히 NO_RELAY가 됐다(AUTHZ_PROVIDER에서 겪은 것과 같은 함정). SMTP 6개 +
   NOTIFICATION_* 7개를 열었다.
2. **`.env.example`에 알림 블록 추가**: 폐쇄망 운영자가 복사해 쓰는 파일에 알림 항목이 아예
   없었다. 이메일·SMS 각각 최소 설정과 선택 설정을 주석으로 구분해 넣었다.
3. **SMS 게이트웨이 규격 흡수**(`NotificationService.relaySms`): 기존엔 `{to,text}` JSON 고정에
   인증 헤더가 없어, 사내 게이트웨이가 다른 필드명이나 API 키를 요구하면 그 자리에서 막혔다.
   `gateway.headers`(세미콜론 구분)·`gateway.content-type`·`gateway.body-template`·`sms.sender`
   4개를 추가. form-urlencoded면 URL 인코딩, JSON이면 JSON 이스케이프로 자동 분기한다.
   - **치환자는 `{to} {text} {sender}`**. 처음엔 `${to}`로 만들었는데 **docker compose가 자기
     변수로 먼저 치환해 빈 문자열이 들어갔다**(실측: `{"receiver":"","msg":"","sender":""}`).
     `$` 없는 형태를 기본으로 바꾸고 `${...}`도 계속 받도록 남겼다.
4. **채널 on/off 토글이 실제로 동작**: `notification_channel_config.enabled`를 디스패처가 아무도
   안 봐서, 화면에서 껐는데 계속 나가고 켰는데 안 나가는 게 둘 다 가능했다. 이제 EMAIL/SMS는
   토글을 보고, 꺼진 채널의 건은 `CHANNEL_OFF`로 즉시 종료한다(재시도 무의미). 표를 못 읽으면
   종전 동작으로 폴백 - DB가 흔들렸다고 알림이 멈추는 쪽이 더 위험하다.
5. **«발송 경로» 진단 표시**: 채널 API에 `relay_configured`(서버에 SMTP/게이트웨이가 설정됐는지)
   추가. 발송 채널 화면에 열로 표시하고, «사용은 켜졌는데 경로가 없는» 조합이면 경고 배너를
   띄운다. 폐쇄망에서 "켰는데 왜 안 와요"를 화면에서 바로 판별하려는 것.
6. **`offline/README.md` §2-1 신설**: 알림 켜는 절차와 `failure_reason` 해석표
   (NO_RELAY/CHANNEL_OFF/NO_ADDRESS/SMTP_ERROR).

**검증** — ✅ 로컬(MailHog·sms-echo): 기본 `{to,text}` JSON 발송 SENT 회귀 없음 / 커스텀
템플릿 `{"receiver","msg","sender"}` + 인증 헤더 2개 전달 확인(sms-echo 수신 본문·헤더) /
form-urlencoded 모드에서 한글 URL 인코딩 확인 / SMS 채널 토글 off → `DEAD, CHANNEL_OFF` /
채널 API `relay_configured` 응답 확인.

**남은 것** — 실제 폐쇄망에 SMTP·문자 게이트웨이가 있는지는 미확인. 있으면 위 설정만으로
붙고, 없으면 화면 알림(IN_APP)만 동작한다.

7. **외부 발송을 «위험»만으로 축소**(사용자 결정 2026-09-02): 메일·문자는 CRITICAL 만 즉시
   나가고, 경고·정보는 화면 알림에만 남는다. 예전엔 경고도 120초 모아 발송했는데
   (`WARNING_BATCH_SECONDS`), 경고는 «봐야 하지만 지금 뛰어갈 일은 아닌» 등급이라 문자로 오면
   곧 무시하게 되고 그러면 정작 위험도 같이 묻힌다. 상수와 배치 지연 경로 제거.
   - 화면에서 등급이 읽히도록 조치 대기열 각 행에 «위험/경고/정보» 태그 추가(기존엔 색 아이콘만).
     대기열 상단 건수 태그·심각도 정렬은 이미 있던 것.
   - **검증**(로컬, 규칙 5 디스크 임계를 임시로 임계 1% 로 낮춰 강제 발화 후 원복):
     WARNING 발화 → `IN_APP` 1건만 생성(EMAIL·SMS 구독이 살아 있는데도 안 만들어짐) /
     CRITICAL 발화 → `IN_APP`+`EMAIL`+`SMS` 3건 즉시 생성, 모두 `SENT`, sms-echo 본문에
     실제 알림 문구 확인. 검증용 인스턴스·발송행은 삭제하고 규칙은 원래대로(미사용/경고/80%) 복원.
   - 로컬 수신자(id=3)에 EMAIL·SMS 구독이 하나도 없어 그동안 실제 알림은 IN_APP 만 만들어지고
     있었다(테스트 발송은 구독을 보지 않아 성공했다). 검증하며 구독을 추가해 뒀다.

8. **수신자 채널 구독을 화면에 노출**(같은 날 이어서): 실제 발송은 `notification_subscription`
   을 보고 대상을 고르는데, **그 구독을 켜는 화면이 아예 없었다**. API(`PUT
   /recipients/{id}/subscriptions`)만 있고 프론트에서 부르는 곳이 없어서, 수신자 탭에 이메일·
   전화를 등록해도 실제 알림은 화면에만 남았다(로컬 수신자도 구독 0건이었다). 폐쇄망에서는
   DB 를 직접 고치지 않는 한 손쓸 방법이 없어 재반입을 부르는 구멍이라 지금 막았다.
   - 수신자 탭에 «이메일 수신»·«문자 수신» 스위치 추가. 연락처가 없는 채널은 비활성(툴팁으로
     이유 표시)이고, 서버도 켜기를 거절한다 - 켜 두면 발송 시점에 NO_ADDRESS 로 죽을 뿐이다.
   - 구독 API 가 켜기만 되던 것을 `enabled` 로 끄기까지 지원하게 확장.
   - **신규 수신자는 적어 넣은 연락처의 채널을 자동 구독**한다. 등록해 놓고 «왜 안 오지»가
     기본값이면 안 된다 - 받기 싫은 채널은 스위치로 끈다.
   - 목록 API 의 `subscriptions` 가 드라이버 PGobject 라 `{"type":"json","value":"[...]"}` 로
     내려가 화면이 배열로 못 읽었다. `::text` + Jackson 파싱으로 실제 배열로 내린다.
   - **검증**(로컬): 끄기→`EMAIL=false` / 켜기→`EMAIL=true` / 이메일만 있는 수신자 생성 시
     `EMAIL` 만 자동 구독 / 전화번호 없이 문자 수신 켜기 → «전화번호가 등록되지 않아…» 거절.
     검증용 수신자는 삭제.

## 30. CDC 델타 적재(DELTA_APPEND) - 변경분을 구분컬럼과 함께 임시 테이블에 쌓기 (2026-09-03)

**요구**: CDC 기능이 없는 데이터분석 솔루션이 변경분을 받아 가도록, 소스 `AA_table`(컬럼 N개)의
변경 이벤트를 타깃 `AA_table_delta`(맨 앞 구분컬럼 1 + 소스 컬럼 N)에 **한 이벤트 = 한 행**으로
append 한다. 솔루션이 주기적으로 읽어 구분값(insert/update/delete)에 맞게 반영하는 건 저쪽 몫.
Oracle·PostgreSQL·MySQL 셋 다 소스/타깃 어느 쪽으로도 가능해야 함.

**기존 CDC 에 영향 없음**: 적재 방식 기본값 `UPSERT` 면 렌더링되는 커넥터 설정이 종전과 동일.
델타는 싱크 커넥터 하나의 설정 분기이고 소스 커넥터·워커 전역 설정·토픽 메시지 형식은 안 건드림.

**구성(Debezium 3.6, 실행 중 Connect 의 `config/validate` 로 옵션명 확인함)**
- 싱크 = Debezium JDBC Sink + `ExtractNewRecordState` SMT(싱크 쪽에 걺 → 같은 토픽에 upsert 싱크
  병행 가능). `add.fields=op:<구분컬럼>` 별칭 문법, `add.fields.prefix=`(빈값), 
  `delete.tombstone.handling.mode=rewrite`(**기본값 tombstone 이면 delete 가 사라짐**),
  `__deleted` 는 `ReplaceField$Value exclude` 로 제거. `insert.mode=insert`/`primary.key.mode=none`/
  `delete.enabled=false`/`schema.evolution=basic`.
- 구분값은 Debezium op 코드 그대로 **c/u/d/r**. 문자열로 바꿔 주는 내장 SMT 는 없음(커스텀 SMT 필요).
- **구분컬럼을 맨 앞에**: 싱크가 만들면 SMT 추가 필드가 맨 뒤에 붙는다. 그래서 배포(PREPARE) 시
  `DeltaTargetTableService` 가 **`cdc_seq`(자동증가 PK) + 구분컬럼** 두 열짜리 뼈대를 먼저 만들고,
  소스 컬럼은 첫 이벤트에서 싱크가 ALTER ADD 한다(세 DB 간 타입 매핑을 앱이 흉내 내지 않기 위함).
  테이블이 이미 있으면 구분컬럼 존재만 확인(순번 컬럼 강제 안 함 - «구분 1 + 소스 N» 정확히
  맞추고 싶으면 직접 만들면 됨). 식별자는 인용 없이 - 싱크(quote.identifiers=false)와 같은 접힘.
- `cdc_seq` 는 같은 행의 insert→update→delete 순서 판단용. 토픽 단일 파티션·싱크 태스크 1 이라
  삽입 순서 = 이벤트 순서.

**변경 파일**
- DB: `V65__add_pipeline_load_mode.sql` (`load_mode` NOT NULL DEFAULT 'UPSERT', `delta_op_column`).
- 백엔드: `pipeline/PipelineLoadMode`(enum + 구분컬럼명 검증), `pipeline/DeltaTargetTableService`(신규),
  `PipelineDefinition`·`PipelineCreateRequest`·`PipelineResponse`·`SinkConnectorRequest`(필드 추가, 구 생성자
  호환 유지), `JdbcSinkTemplate.renderDeltaAppend`, `PipelineDeployService`(델타면 뼈대 생성 후 커넥터 등록),
  `PipelineService.buildDefinition`(적재 방식·구분컬럼 저장), `PipelineConsistencyService`(델타는 UNSUPPORTED -
  append-only 라 행 수 비교 무의미).
- 프론트: `types/pipeline.ts`, `CdcCreatePage`(실행 옵션에 «적재 방식» + 구분컬럼명, 델타면 DELETE 반영
  숨김, 소스 컬럼과 이름 충돌 검사, 검토 요약), `PipelinesPage`·`AirflowDashboardPage` 상세에 «적재 방식».
- 테스트: `JdbcSinkTemplateTest`(델타 3건), `PipelineServiceTest`(델타 3건), `DeltaTargetTableServiceTest`(DDL 3 DB).

**주의(운영)**
- 타깃 연결 계정에 CREATE TABLE 권한 필요(뼈대 생성). 없으면 PREPARE 가 FAILED 로 떨어지고 커넥터는 안 만들어짐.
- PostgreSQL 소스: delete 행에 PK 외 컬럼까지 담으려면 소스 테이블 `REPLICA IDENTITY FULL` 필요
  (기존 파이프라인의 WAL 양도 늘어남). Oracle LogMiner/MySQL(binlog_row_image=FULL)은 기본으로 전체 before 제공.
- 초기 스냅샷 행은 `r` 로 쌓임. 필요 없으면 스냅샷 모드 NO_DATA.
- 실 데이터 end-to-end 는 사용자가 UI 로 파이프라인 생성해 직접 검증 예정(이번 커밋은 단위 테스트 + Connect
  config/validate 까지).

### 30.1. 세 번째 적재 방식 DELTA_UPSERT - PK당 한 행(마지막 상태 + 작업 종류) (같은 날 이어서)

사용자 확인: 분석 솔루션은 "주기적으로 최신 상태만 동기화"하는 구조라 이 방식을 주로 쓴다.
- 싱크: `insert.mode=upsert`/`primary.key.mode=record_key`, SMT 는 APPEND 와 같되
  `add.fields=op:<구분>,ts_ms:cdc_ts`. delete.enabled 는 여전히 false(delete 도 d 로 덮어씀).
  insert→update = u 한 행(최신 값), insert→delete = d 한 행(삭제 직전 값).
- **"가져간 만큼 삭제" 기준이 바뀜**: 순번(cdc_seq)은 INSERT 때만 채번돼 덮어써도 안 바뀌므로 범위
  삭제에 갱신분이 같이 지워진다. 그래서 덮어쓸 때마다 갱신되는 `cdc_ts`(epoch ms)를 기준으로
  `DELETE … WHERE cdc_ts <= 읽을 때의 MAX(cdc_ts)`.
- 소비 규칙: c/u/r → merge(있으면 update, 없으면 insert - u 인데 대상에 없는 경우가 정상), d → 있으면 delete.
- **타깃 뼈대는 안 만든다**: upsert 는 소스 PK 와 같은 키가 타깃에 있어야 하는데(Postgres ON CONFLICT·
  MySQL ON DUPLICATE KEY) PK 컬럼을 앱이 타입 매핑해 만드는 건 추측이라, 없으면 싱크가 PK 포함해
  만들게 두고(구분·시각 컬럼은 뒤쪽), 있으면 `DeltaTargetTableService.verifyUpsertKey` 가 구분컬럼 존재 +
  타깃 PK/UNIQUE(getPrimaryKeys + getIndexInfo(unique)) 중 소스 PK 와 컬럼 집합이 같은 것이 있는지 확인.
  소스 PK 조회 실패 시엔 경고만 남기고 통과(싱크에서 드러남). `cdc_ts` 는 없으면 싱크가 ALTER ADD.
- 구분컬럼명 예약어에 `cdc_ts` 추가. 정합성 검증은 델타 두 방식 모두 UNSUPPORTED.
- 프론트: `loadModeLabel()` 공용(types/pipeline.ts), 생성 화면 선택지 3개·모드별 안내, 상세 2곳.
- 테스트: 싱크 템플릿 DELTA_UPSERT 1건, keyMatches 1건, 예약어 1건, 생성 서비스 1건 추가.

## 31. 화면 정리 - 실시간 모니터링 레이아웃 · 캔버스/팔레트 군더더기 제거 (2026-09-03)

사용자가 화면을 보며 지적한 것들을 이어서 반영했다. 기능 변경보다 «화면이 화면답게
보이는가»에 대한 손질이다.

1. **실시간 모니터링 세로 스크롤 제거**: workspace 높이가 `calc(100vh - 330px)` 라는 «헤딩+
   상태카드가 이만큼일 것»이라는 추측이라 실제와 20px 쯤 어긋나 페이지가 스크롤됐다.
   `.airflow-dashboard-page` 를 뷰포트에 못박고(헤더 61 + 패딩 48 = 109) workspace 는
   flex 로 남은 높이를 가져가게 바꿔 추측 자체를 없앴다. 상태 카드도 74→58px 로 축소.
2. **가로 스크롤 제거**: CDC 목록은 `scroll={{x:1130}}` 이 패널(약 1040px)보다 넓어 무조건
   스크롤이 생겼다 - 860 으로 낮추고 열 폭을 줄였으며 «소스 → 타깃» 은 폭을 아예 안 줘서
   남는 자리를 흡수하게 했다. ETL task 표는 실패 사유가 nowrap 으로 늘어나 표를 밀어냈다 -
   5번째 열을 200px 에서 자르고 전문은 title 로.
3. **CDC 목록 «커넥터 상태» 열 삭제**: 상태 열이 둘이면 어느 쪽을 봐야 하는지 헷갈린다.
   목록은 «상태»(제어 DAG 기준) 하나, 커넥터 상태는 속성창에서 «커넥터 상태»로 본다.
4. **실행 이력 «실행 ID» 열 삭제**: dag_run_id 는 사람이 쓸 일이 없고 문자열이 길어 팝업을
   가로로 밀어냈다. TaskRows 에 `auxColumn` 을 둬서 열이 하나 적은 표에서도 칸이 안 밀린다.
5. **캔버스 «저장하지 않은 변경» 배너 제거**: 제목 옆 태그(«저장 안 됨»·«게시본과 다름»)가
   같은 말을 이미 하고 있었다.
6. **이탈 확인의 범위 확대**: 확인창이 속성창의 «바로가기» 버튼 두 개에서만 떴다 - ← 버튼·
   사이드 메뉴·로고 이동까지 걸었다. 데이터 라우터가 아니라 useBlocker 를 못 써서
   `utils/navigationGuard.ts`(모듈 단일 슬롯)로 만들었고, 새 탭 열기는 가로채지 않는다.
7. **캔버스 위 메모 상자 제거**: 속성창에 같은 메모 칸이 있어 두 곳에 떠 있었고 좌상단을
   늘 가렸다. 입력 창구를 속성창 하나로 합쳤다.
8. **job 노드 속성에서 «대상»·«로그» 제거**: 캔버스에서는 배치와 연결이 관심사다. 로그 구역
   때문에 노드를 누를 때마다 NiFi 실행이력 두 벌을 더 불러오던 것도 같이 사라졌다.
9. **워크플로우 노드 속성에 그 워크플로우 요약 추가(읽기 전용)**: 스케줄·다음 2회 실행·상위
   워크플로우·메모. 값은 그 워크플로우의 것이라 여기서 고치지 않는다(고치려면 바로가기).
10. **팔레트 정리**: 안내 문구 2개와 검색창 제거, 위쪽 트리에 «태스크» 머리글 추가
    (아래 «워크플로우»와 짝).
11. **스케줄링 화면 높이**: 목록이 짧으면 카드 아래가 통째로 빈 띠였다. 실시간 모니터링과
    같은 방식으로 뷰포트에 못박고 트리·목록이 각자 안에서 스크롤한다. 좌측 업무 그룹
    트리는 최초 진입 시 최상위만 펼친다(전부 펼침 → 1단계).

## 31. Oracle 테이블별 보충 로깅 미설정으로 UPDATE 가 조용히 유실되던 문제 (2026-09-05~06)

**증상**: MSA 운영의 델타 파이프라인(pipeline 20, `CSB.INS_PYM_STT` → `CSB.INS_PYM_STT_DELTA`)에서
`UPDATE ... WHERE INS_PYM_STT_ID='3'` 을 했는데 델타 테이블에 **PK 가 0** 이고 바뀐 컬럼(`hspt_clsf`)
외 전부 NULL 인 행이 들어감. INSERT·DELETE 는 멀쩡했고 커넥터도 계속 RUNNING.

**원인 — 우리 코드가 아니라 소스 Oracle 설정**. 토픽 `oracle-cdc.CSB.INS_PYM_STT` offset 8713 원본을
직접 까 보니 **Kafka 레코드 키부터 `{"INS_PYM_STT_ID": 0.0}`**, before/after 모두 PK 가 0 이었다.
`all_log_groups` 에 CSB 의 CDC 소스 3개 테이블 모두 행이 없다 = 테이블별 보충 로깅 미설정.
Oracle redo 는 UPDATE 때 **바뀐 컬럼만** 남기므로(INSERT/DELETE 는 행 전체) LogMiner 가 PK 값을
못 읽고, PK 는 NOT NULL 숫자 필드라 null 대신 **기본값 0** 으로 채워진다. 그 0 이 그대로 키가 되어
`primary.key.mode=record_key` 싱크가 **모든 UPDATE 를 PK=0 한 행에 덮어썼다**(4건 → 1행).

**조치 ①(소스 DB, 앱 밖)**: `ALTER TABLE CSB.<표> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;`
적용 이후 발생분부터 정상. 이미 발행된 이벤트는 불변이라 복구 불가 - PK=0 행은 수동 정리.

**조치 ②(이번 커밋) — 제품이 이걸 못 막고 있었다**
`CdcPrerequisiteService.checkOracle()` 의 `ORACLE_TABLE_SUPPLEMENTAL` 은 "생성 마법사에서 선택한
테이블 기준으로 다시 점검합니다" 라고 **안내만 하고 그 재점검 코드가 어디에도 없었다**(전체 검색에서
`all_log_groups` 조회 0건). 그래서 조건이 빠진 채로 파이프라인이 만들어지고 조용히 틀린 데이터가 쌓였다.
- **신규 `CdcPrerequisiteService.checkTable(connectionId, schema, table)`** + `CdcTableReadinessResponse`:
  Oracle = `ALL_LOG_GROUPS` 에 `ALL COLUMN LOGGING` 있는지(DB 전체 `SUPPLEMENTAL_LOG_DATA_ALL=YES` 면 통과) → 없으면 **FAIL**,
  Postgres = `pg_class.relreplident` 가 FULL 아니면 **WARN**(미러링엔 지장 없고 델타의 삭제 직전 값만 제한),
  MySQL = `binlog_row_image` 가 서버 단위라 테이블 조건 없음 → PASS.
- **`GET /api/connections/{id}/cdc-table-readiness?schema=&table=`** 신설.
- **`PipelineDeployService.verifySourceTableCaptureReady()`**: Oracle 소스가 FAIL 이면 **커넥터를 만들기 전에
  배포 차단**(조치 SQL 을 메시지로). WARN/UNKNOWN 은 로그만 - 점검 권한이 없다고 배포를 막으면 안 된다.
  ※ 기존 Oracle 파이프라인도 보충 로깅 없으면 **재배포가 막힌다**(실제로 깨져 있는 상태라 의도된 동작).
- **화면**: 대상 선택 표의 소스 테이블 칸에 «변경분 캡처 불가 / 삭제 전 값 제한» 태그 + 상단 오류 배너,
  FAIL 이 하나라도 있으면 다음 단계로 못 넘어간다(PK 없음 차단과 같은 자리).
- **테스트**: `CdcPrerequisiteServiceTableTest` 7건(ALL/없음/PK만/DB전체/PG FULL·DEFAULT/MySQL),
  `PipelineDeployServiceTest` 에 차단 1건 + 비 Oracle 건너뜀 1건.

**미완**: 델타 타깃 테이블 컬럼 타입 정리(CLOB→VARCHAR2 등)는 SQL 만 준비하고 미적용 -
원격 DB DDL 이 자동 승인 정책에 막혀 실행 못 함(`delta-table-rebuild.sql`).
`column.propagate.source.type` 으로 싱크가 애초에 VARCHAR2(n) 으로 만들게 하는 건 검증 필요.

### 31.1. MSA 운영 환경 실제 조치 및 복구 (2026-09-06) — ✅ 완료·검증

코드 수정(31절)과 별개로, MSA 운영 Oracle/PostgreSQL 에 실제 조치를 적용했다.

**① 보충 로깅 적용** — `ALTER TABLE CSB.<표> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS` 를
INS_PYM_STT / TB_IMP018M / TB_IMP060L 3개에 적용. `all_log_groups` 에서 세 건 모두
`ALL COLUMN LOGGING` 확인. 커넥터 재시작 불필요(redo 내용만 바뀜).

**② 델타 타깃 테이블 재생성** — `CSB.INS_PYM_STT_DELTA` 를 소스와 같은 타입으로 교체.
Oracle 은 CLOB→VARCHAR2 직접 변경 불가(ORA-22858) 라 새 테이블 생성 → 3행 복사 →
이름 교체(이전 것은 `INS_PYM_STT_DELTA_BAK` 로 보존) → PK 재생성 순서로 진행.
문자 13개 CLOB→VARCHAR2(40~800 CHAR), FLOAT→NUMBER(15,0)/(15,2), TIMESTAMP(9)→DATE,
CDC_OP 는 CLOB→VARCHAR2(10). CDC_OP·CDC_TS 를 **맨 앞**으로 배치(최초 설계안).
앞뒤로 sink-20 pause → resume → `restart?includeTasks=true`(싱크의 테이블 메타 캐시 비움).
※ `SqlRunner` 가 JDBC auto-commit 인데 스크립트에 `COMMIT` 이 있어 ORA-17273 으로 한 번
중단됐다 - auto-commit 이라 이미 반영된 상태였고 남은 문장만 이어서 실행했다.

**③ 검증** — 소스 3번 행 UPDATE → 델타에 `CDC_OP=u, INS_PYM_STT_ID=3` 과 **전체 컬럼**이
정상 도착(이전에는 PK 가 0 이고 나머지 NULL). 검증 후 소스 값은 원래대로 되돌렸고,
델타의 `INS_PYM_STT_ID=0` 유령 행은 삭제.

**④ 미러링 파이프라인 피해 규모가 예상보다 컸다** — `tb_imp018m` 타깃에 PK 가 빈 문자열인
유령 행 1건이 있었는데(text PK 라 0 이 아니라 `''`), 소스와 대조해 보니 **실제로는
232건의 UPDATE 가 통째로 유실**돼 있었다. `primary.key.mode=record_key` upsert 라
키를 잃은 232건이 **전부 같은 유령 행 하나에 덮어써진** 것. (델타에서 UPDATE 4건이
ID=0 한 행이 됐던 것과 같은 원리 - 미러링에서는 소스/타깃이 조용히 어긋난다.)
- 소스 `TEST00000087100~87356` 구간 257행이 `clrp_dvsn_cd='08'` 인데 타깃은 원래 값(00~09) 유지.
- 복구: 유령 행 삭제 후 소스에서 **값을 그대로 다시 쓰는 무해한 UPDATE**(`SET CLRP_DVSN_CD =
  CLRP_DVSN_CD`)로 257행을 재전파 → 타깃 257행 모두 `08` 로 일치 확인. 행 수도 100,000 일치.
- `tb_imp060l` 은 유령 행 0건(그동안 UPDATE 가 없었음).

**최종 상태** — 커넥터 6개 전부 RUNNING, 델타 테이블 3행(정상), 유령 행 0건.

**교훈**: 이 조건이 빠지면 INSERT/DELETE 는 정상이고 커넥터도 RUNNING 이라 **화면상 아무
문제가 없어 보인다**. 유일한 신호가 «타깃에 PK 가 0/빈 문자열인 행 1건» 이었고, 그 뒤에
232건 유실이 숨어 있었다. 31절의 사전점검·배포 차단이 필요한 이유가 이것이다.

## 32. 전체 디자인 적용 1단계 - 상단 메뉴(GNB) · 통합 운영 대시보드 · 로그인 (2026-09-10)

디자인 초안 `/home/user/CEREBRO ETL_UI DESIGN`(README · index.html · login.html · css/ · js/)을 기존 React
화면에 입혔다. **조회·판정·권한·이동 로직은 그대로 두고 마크업과 스타일만** 바꿨다.

1. **초안의 js/ 와 정적 HTML 은 가져오지 않았다**: 드롭다운·모달·달력·ECharts 를 흉내 내는 데모 스크립트라
   React/antd 가 이미 하는 일이고, DOM 을 직접 만지면 React 와 충돌한다. css/ 의 토큰·모양만 index.css 로
   옮겼고 차트는 @ant-design/plots 그대로다(ECharts 로 바꾸면 기능 변경이다).
2. **왼쪽 사이드바 → 상단 메뉴(AppLayout)**: 메뉴 구성·권한 게이팅(leafVisible/itemVisible)·이탈 확인
   (guardedNavigate)은 같다. 하위 메뉴 열림은 CSS `:hover` 가 아니라 `.open` 상태로 준다 - `:hover` 만 쓰면
   하위 메뉴를 고른 뒤에도 커서가 그 자리에 있는 동안 목록이 떠서 방금 연 화면을 가린다. 포커스·ESC 로도
   열고 닫힌다. 사이드바 접기 버튼은 사이드바와 함께 없어졌다.
3. **뷰포트 높이 상수 `109px` → `--app-offset`**: 헤더 56 + 콘텐츠 패딩(12+14). 높이를 못박는 화면
   (실시간 모니터링·스케줄링·콘솔·NiFi 설정·캔버스)의 calc 를 변수로 바꿔 한 곳에서 맞춘다.
4. **Pretendard 는 번들에 넣었다(src/assets/fonts, SIL OFL)**: 초안은 jsDelivr CDN 스타일시트인데, 폐쇄망
   설치에서는 `<head>` 의 외부 스타일시트가 첫 화면을 붙잡는다. antd 는 body 글꼴을 물려받지 않으므로
   ConfigProvider 토큰(src/theme/cerebro.ts 의 appTheme)에도 넣었다.
5. **강조색(빨강)은 화면 단위**: 대시보드·로그인만 `cerebroBrandTheme` 로 감쌌다. 전역으로 바꾸면 아직
   디자인하지 않은 화면의 파란 버튼까지 한꺼번에 바뀐다.
6. **대시보드**: 조치 대기열을 머리줄의 «위험·경고·정보 알약 + 상세보기»로 접었다(초안 그대로) - 페이지에
   펼치던 상위 5건 목록 대신, 누르면 전체 목록 팝업(항목별 이동·확인 동일, 확인됨·억제 건수는 팝업 상단).
   KPI 카드의 현재 지표는 작은 칸 한 줄로, «현재» 구분선·기간 배지·전일 대비·흐름 판정은 유지.
   Airflow 카드에 «성공» 칸 추가(이미 있던 값). 당일/전일·검색은 native 버튼(검색은 antd loading 과 같게
   첫 조회 중 누름을 무시). 오류·실행 상세 팝업(HistoryModal)은 초안의 빨간 접기 버튼 + 어두운 콘솔 상자.
7. **로그인**: 초안의 «자동 로그인» 체크박스는 로그인 유지 기능이 없어 뺐다(눌러도 아무 일 없는 칸이 된다).
   입력칸 제목은 placeholder 로 보이고 aria-label 로 남긴다. 필수값 검증·오류 문구·로딩은 그대로.

**검증**: `tsc -b`, 변경 파일 oxlint(새 경고 없음 - ActionQueuePanel 의 기존 exhaustive-deps 경고 1건만),
`vite build`(글꼴 자산 해시 포함) 통과. compose 네트워크(data-pipeline_pipeline-net)에 vite dev 서버를 띄워
qa-playwright 로 스크린샷 확인 - 로그인 필수값·오답 문구·성공 이동, 메뉴 5개·드롭다운 열림/선택 후 닫힘·
화면별 활성 탭, 조치 대기열 팝업, 리소스 상세 펼치기, 날짜 팝업 강조색, 1920/1366 에서 대시보드·실시간
모니터링·스케줄링·CDC·연결정보 세로 스크롤 없음. 오류 상세 팝업은 실패 건이 없는 데이터라 화면으로는 못 봤다.
운영 컨테이너 반영(`docker compose up -d --build cerebroetl-ui`)은 하지 않았다.

**남은 것**: 워크플로우(workflow-*.html)·ETL·CDC·설정 화면.

## 33. 전체 디자인 적용 2단계 - 워크플로우 스케줄링 · 실시간 모니터링 (2026-09-10)

초안 `workflow-scheduling.html` · `workflow-monitoring.html`(css/workflow.css)을 두 화면에 입혔다. 32절과 같은 원칙 -
조회·판정·권한·실행/재실행 로직은 그대로 두고 마크업과 스타일만 바꿨다.

1. **공통 머리줄 `components/WorkflowPageHeader`**: 화면 제목 + 하위 탭(스케줄링·실시간 모니터링·관리) + 오른쪽 조작.
   31절 무렵 «왼쪽 메뉴가 어느 화면인지 말해 준다»며 제목을 뺐는데, 사이드바가 상단 메뉴로 올라가면서
   그 역할을 이 머리줄이 다시 맡는다. 탭의 권한 규칙은 AppLayout 메뉴와 같게 둔다(안 보이는 화면이 탭에만 뜨면 403).
2. **스케줄링**: 왼쪽 트리 카드 + 오른쪽 목록 카드. antd Tree·Table·Popconfirm 은 그대로 쓰고 셀과 버튼만 초안 모양.
   초안의 트리 제목 «업무 그룹»은 쓰지 않았다 - 이 트리는 NiFi 그룹이 아니라 워크플로우 상하 계층이라 «워크플로우».
   초안에 있는 목록 검색·게시 상태 필터·«전체 접기»는 기능이 없어 넣지 않았고, 대신 «지금 표가 무엇을 보여주는지»
   칩(전체 워크플로우 / ○○ · 하위 포함)만 둔다. 동기화는 native 버튼(진행 중 누름 무시 + 아이콘 회전 = antd loading 과 같음).
3. **실시간 모니터링**: CDC·ETL 상태 카드를 위아래 두 줄에서 **나란히 한 줄**로(도메인 배지 + 아이콘 지표, 0 은 흐리게).
   목록·트리·선택 행 색을 CDC 파랑 / ETL 빨강으로 가른다. `airflow-row-selected` 는 그동안 CSS 가 없어 고른 행이
   표시되지 않았다 - 이번에 바탕색 + 왼쪽 띠를 줬다. 속성창 바로가기 링크는 링크 그대로 두고 보조 버튼 모양만.
   3열 폭은 초안대로 230 · 1fr · 320, 1440px 이하에서는 예전 220 · 280 으로 줄여 가운데 표 자리를 지킨다.
4. **antd v6 구조 주의**: Table 래퍼 아래는 `.ant-spin > .ant-spin-container` 다(v5 의 `.ant-spin-nested-loading` 아님) -
   표만 스크롤하고 페이지 이동을 카드 바닥에 붙이려면 이 경로로 flex 를 준다. Tree 노드는 기본이 위쪽 정렬이라
   접기 화살표가 떠 보여 `.ant-tree-treenode{align-items:center}` 를 줬다.
5. 두 화면 모두 `cerebroBrandTheme` 로 감싸 확인/실행 버튼·페이지 번호·확인창이 빨간 강조색이 된다.

**검증**: `tsc -b` 통과, 변경 파일 oxlint 새 경고 없음(AirflowDashboardPage 의 기존 exhaustive-deps 1건). 배포본
(`docker compose up -d --build --no-deps cerebroetl-ui`)에서 qa-playwright 로 - 메뉴→스케줄링·하위 탭→모니터링 이동과
활성 표시, 트리 선택 시 목록 좁혀짐·«전체» 복귀·검색, 삭제 확인창 열기(실행 안 함), 새 워크플로우 팝업, ETL 지표 필터,
CDC 파이프라인 선택 → 트리·행 강조·속성창, 1920/1366 세로·가로 스크롤 없음, HTTP 4xx/5xx·페이지 오류 없음.
로컬 데이터에 Airflow DAG 가 0건이라 ETL 워크플로우 행 선택·task 표·실행 이력 팝업은 화면으로 못 봤다(코드 경로는 그대로).

**33절 추가 (같은 날, 사용자 피드백)** — 스케줄링 트리가 초안과 크기·폴더 모양이 달랐다.
- antd Tree 를 걷어내고 초안 트리 마크업(`tree-node` · `tree-item-row` · `tree-toggle-arrow` ▶ · `tree-sub-group` 점선)을
  그대로 쓰는 `WorkflowTreeView` 로 바꿨다. CSS 도 초안 workflow.css 트리 규칙을 `.wf-tree-scroll` 안으로 한정해 옮김.
  동작은 antd Tree 때와 같다: 줄을 누르면 선택·다시 누르면 해제, 화살표는 접기만, 처음엔 최상단만 펼침, 검색어가 바뀌면
  다시 마운트해 기본 펼침으로. Enter/Space 로도 고른다.
- 줄 높이가 27px(초안 31px)이었던 이유: 초안 base.css 는 전체 `line-height: 1.5`, 우리 body 는 기본값. 트리 줄에만 1.5.
  수치 비교(qa-playwright, 초안 vs 배포본): 줄 31/31px, 글자 12.5px, 여백 6·8px, 아이콘 15px, 점선·들여쓰기 18px 일치.
- 트리 카드의 «워크플로우» 제목 줄 삭제(사용자 요청). 거기 있던 «전체»(선택 해제)는 목록 칩 옆 «전체 보기»로 옮겼다.
- 실시간 모니터링 «업무별 트리»도 같은 마크업·같은 CSS 로 맞췄다(사용자 요청). 트리 규칙 선택자를
  `:is(.wf-tree-scroll, .airflow-tree-body) .tree-*` 로 넓혀 두 트리가 한 벌을 쓴다. 모니터링은 고른 줄 색만 CDC 파랑으로 덮는다.
  동작은 그대로 옮겼다: 폴더 줄은 누르면 접기 + 목록 범위 변경, 하위가 있는 게시 워크플로우는 접기 화살표가 따로 눌림,
  파이프라인·워크플로우 줄 우클릭 메뉴(제어 DAG 가 있을 때), «DAG 준비 중» 흐림, 검색 필터. 이름은 한 줄로 자르고 전체는 title.
  «업무별 트리» 제목 줄 삭제. 수치(qa-playwright): 두 트리 모두 줄 31px·12.5px·아이콘 15px·점선 18px.

## 34. 워크플로우 로그2 - 인포매티카 Monitor 식 로그 화면 시안을 별도 메뉴로 (2026-09-16)

**요청**: 로그 화면을 인포매티카 Monitor 처럼 다시 짠 목업(`docs/mockups/2026-09-15-workflow-log-redesign.html`)을
기존 «로그»에 덮지 말고 «로그2» 메뉴로 따로 구현. 관리자와 둘을 비교해 하나를 채택한다.

**기존 화면은 손대지 않았다**: `AirflowDashboardPage.tsx` · `TaskFailureLogModal.tsx` 변경 없음. 판정 규칙(오늘 기준 지표,
상태 문구, 적재 건수 읽기, job 묶기·정렬, 재실행·«선행을 성공 처리하고 실행» 확인, 실행 설정 잠금)은
`pages/workflowLogV2/logV2Data.ts` · `logV2Hooks.tsx` · `LogV2Parts.tsx` 로 **복사**했다 - 채택 전에 규칙을 고치면 두 곳을
같이 고칠 것. 채택이 끝나면 버리는 쪽 파일과 index.css 블록(`lv2-` 접두어 한 덩어리)만 지우면 된다.

**추가/변경**
- 라우트 `/workflows/logs2`(App.tsx) - nginx 가 `/airflow/` 직접 주소를 가로채므로 그 밖에 둠. 상단 메뉴(AppLayout)와
  워크플로우 하위 탭(WorkflowPageHeader)에 «로그2»(AIRFLOW READ) 추가.
- 화면 구성(목업 ①~⑥):
  1. 상단 CDC·ETL 상태 카드(기존과 같은 마크업·숫자)를 커튼으로 접기 - 접으면 한 줄 요약(실패는 빨강), 상태는 localStorage.
  2. 왼쪽 트리는 기존과 같은 계층·검색·우클릭 메뉴 + 줄마다 «오늘 마지막 실행» 상태 점. CDC/ETL 이름 줄을 누르면 그 업무 전체.
  3. 컨텍스트 바: 경로 · 실행 목록/타임라인 · 기간(오늘/어제/7일/30일/지정 → `listAllAirflowDagRuns` 기간 조회) · 상태 칩 · 검색.
  4. 실행 → job → 단계를 한 표에서 펼침. 하위 워크플로우 실행은 **run id 규칙 `manual__{상위 run}__{노드키}`** 로 부른 job
     아래에 정확히 붙인다(시각 추정 아님). 범위 안에 상위 실행이 있으면 하위 실행은 맨 위 목록에서 뺀다.
     펼치거나 고른 실행만 task 를 받는다(30일로 넓혀도 줄마다 요청이 나가지 않게).
  5. 타임라인: 가장 최근 날짜의 실행(최대 30건)과 job 막대, 선행 실패 미실행 줄무늬, 지금 선.
  6. 아래 상세 창(끌어서 높이 조절·접기): 요약 / 작업·단계 / 로그 / 실행 이력 / 알림 규칙 / 속성. 예전 오른쪽 속성창·
     실행 이력 창·DAG 상세 창이 여기로 모였다. «로그» 탭은 실패 단계 목록 + parseFailureLog 결과 + Airflow 기록(state
     mismatch 등)을 바로 보여 주고, 이전 시도·하위 실행 안쪽까지는 «전체 로그 창»(기존 TaskFailureLogModal)으로 연다.
- CDC 범위는 기존과 같은 파이프라인 표(cdcPipelineColumns) + 동작(실행 설정·실행 이력), 상세는 요약(제어 DAG·커넥터·지연·
  처리율·마지막 처리) / 실행 이력(제어 DAG) / 알림 규칙 / 속성.

**검증**: tsc 통과, 새 파일·변경 파일 oxlint 0건. 배포본에서 qa-playwright(1920·1366) - 로그 화면 정상·탭 순서
«생성|관리|로그|로그2|상세 관리», 로그2 CDC 목록·상세 탭, 커튼 접기/펼치기, ETL 7일 목록(실행 6·호출 2) → 호출 행 펼침으로
하위 실행 job 표시(dz_com_daily 4,641행 = 43 + 4,598), job → 단계 5개 → 단계 상세, 30일 실패 필터 → 로그 탭(Airflow 기록 표시)
→ 전체 로그 창, 실행 이력 기간 변경, 타임라인, 지표 클릭, 상세 창 접기·끌기, 가로 넘침 0, 페이지 오류 0.
재실행·실행 설정은 실제로 누르지 않았다(운영 DAG 에 영향). 로컬 Airflow 의 오래된 task 로그는 «Could not read served logs»
라 로그 본문은 두 화면 모두 이 한 줄만 나온다(환경 문제, 화면 문제 아님).

## 35. GPT 2단계 - 워크플로우 · CDC 로그 오류 분석 (2026-09-16)

**요청**: GPT 설정 설계서(docs/2026-09-13-gpt-settings-design.md)의 다음 단계 착수. ETL 로그 · ETL 속성창은 ETL 담당 작업자가
맡으므로 워크플로우 · CDC 쪽만. 진행 상황 · 설계와 달라진 점은 설계서 12절에 적었다.

**추가/변경**
- V74 `llm_usage_log` - 본문 없이 누가 · 대상 · LLM · 전송 글자 수 · 토큰 · 소요 · 결과.
- 백엔드 `llm/client/LlmChatClient`(제공자 4종 SSE 스트리밍, JSON 한 덩어리로 답하는 호환 서버도 받음, 첫 응답 120초 · 전체 300초),
  `llm/context`(대상 종류 · 수집기 인터페이스 · 우선순위 자르기 · 개인정보 마스킹), 수집기 `WorkflowTaskContextCollector`
  (워크플로우 · 상위 경로 · 실패 단계 설명 · 노드 구성 · job 단계 SQL · 최근 실행) · `CdcPipelineContextCollector`
  (파이프라인 구성 · 실측 상태 · 실패 태스크 trace 요약 · 최근 제어 명령), `llm/agent/ErrorAnalysisAgent`(역할 지시문 · 답변 형식),
  `llm/chat`(`POST /api/llm/chat` SSE, `POST /api/llm/chat/preview`). ErrorCode `LLM_NOT_ENABLED` · `LLM_TARGET_NOT_FOUND`.
- nginx `location /api/llm/` - 버퍼링 끔, 읽기 제한 330초.
- 화면: `components/gpt/GptErrorAnalysis`(자동 분석 → 추가 질문 · 중지 · 보내는 정보 보기) · `useGptEnabled`.
  워크플로우 실패 로그 창(`TaskFailureLogModal`)의 task 머리에 [GPT 분석] - 로그 · 로그2 화면이 같은 창을 쓴다.
  CDC 로그 펼친 실패 줄에 [GPT 분석]. 선택된 LLM 이 없으면 버튼을 그리지 않는다.

**ETL 담당에게** - `LlmContextCollector` 를 구현한 빈(`targetType()` = `ETL_JOB`)을 추가하면 `/api/llm/chat` 이 자동으로 쓴다.
화면은 `GptErrorAnalysis` 에 `targetType="ETL_JOB"` 과 대상 id · 화면 오류 문구만 넘기면 된다. job 단계 · 최근 실행을 읽는 방식은
`WorkflowTaskContextCollector` 의 `steps` · `recentRuns` 를 참고.

**검증**: LLM 단위 테스트 34건(스트리밍 파싱 4종 · HTTP 오류 분류 · 리스너 중단 · 마스킹 오탐 · 자르기 · trace 요약 · 메시지 순서).
로컬 gemma4(KT Cloud 프록시, OpenAI 호환)로 nginx 경유 스트리밍 - 첫 글자 2.4초. 화면(puppeteer 1600px): dz_com_daily 09-05 실패
(PutSQL 접속 거부, 4회 시도) → 실패 로그 창 → [GPT 분석] 네 제목 답 · 추가 질문 · 보내는 정보 보기, CDC 로그 09-04 ORA-12541 실패 줄 →
[GPT 분석], 성공 줄에는 버튼 없음, 페이지 오류 0. 거절 경로: 선택 해제 409 · 없는 파이프라인 404 · 미로그인 401 · ETL_JOB 400.
권한 없는 사용자로는 확인하지 않았다. 테스트를 위해 로컬 gemma4 를 «선택» 상태로 켰다(해제 → 409 확인 후 다시 선택).

## 36. GPT 3단계 - 속성창 GPT 탭(CDC · 워크플로우 노드) + 대화 길이 관리 (2026-09-17)

**요청**: 로그 GPT 분석의 추가 질문이 길어질 때 대비(오류 분석은 오래된 문답 빼기, 속성창은 자동 요약)와 속성창 GPT 탭을 CDC ·
워크플로우 둘 다 진행. 상세는 설계서 12절.

**추가/변경**
- 백엔드: `llm/agent/JobAnalysisAgent` · `ConversationSummarizer` · `AgentPrompts`(공용 지시문), `llm/chat/ConversationFitter`
  (문답 쌍 단위로 오래된 것부터 고름), `LlmChatService`(입력 길이 60% 대상 정보 / 40% 대화, 오류 분석 DROPPED · 속성창 SUMMARIZED,
  요약 실패 시 빼기, 요약 호출도 호출 기록), SSE `compacted` 이벤트, 요청에 `summary`. `llm/context/WorkflowNodeContextCollector`
  (WORKFLOW_NODE) · `EtlJobContextText` · `WorkflowGraphText` 로 워크플로우 task 수집기와 공유.
  속성창은 질문 필수(`prepareChat`), [보기] 는 질문 없이(`prepare`).
- 화면: `components/gpt/useGptConversation`(대화 상태 · 요약 경계 · 중지 · 새 대화) · `GptConversationView` · `GptChatPanel`.
  `GptErrorAnalysis` 는 같은 훅으로 옮김(동작 같음). CDC 관리 «파이프라인 상세» 창에 GPT 탭, 워크플로우 캔버스 노드 속성창을
  [속성] [GPT] 탭으로(속성 내용은 들여쓰기만 바뀜). 선택된 LLM 이 없으면 탭 없음.

**검증**: LLM 단위 테스트 41건. API · 화면 검증 내용은 설계서 12절 «3단계 검증». [보기] 가 속성창에서 400 이던 것(질문 필수 검사에 걸림)을
화면 검증에서 찾아 고쳤다. 요약이 일어날 만큼 긴 대화를 화면에서 직접 이어 가 보지는 않았다.

## 37. GPT - 확인 못 했던 3가지 확인 (2026-09-17)

- **화면 요약 표시**: 한도를 잠시 6,000자로 낮춰 CDC GPT 탭에서 확인(설계서 12절 «추가 검증»). 이 과정에서 역할 지시문 길이를
  한도 계산에서 빼먹은 것을 찾아 `LlmChatService` 에서 지시문 · 머리말(`PROMPT_WRAPPER_CHARS`)까지 세도록 고쳤다.
- **권한 거절**: `permtest` + 임시 역할로 확인 후 원복.
- **통합 테스트 `ConnectionRepositoryIT`**: 코드 문제가 아니라 실행 환경 문제 세 가지였다. 통과시킨 실행 방법 -
  ① Docker 29 는 최소 API 1.40 인데 Testcontainers(docker-java)가 더 낮은 버전으로 물어 400 → `~/.docker-java.properties` 에
  `api.version=1.44` ② 빌드 컨테이너 안에서 테스트 컨테이너 포트를 `localhost` 로 못 찾음 → `--add-host=host.docker.internal:host-gateway`
  + `TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal`, `TESTCONTAINERS_RYUK_DISABLED=true`, 도커 소켓 마운트
  ③ 기본값 없는 환경변수 `PIPELINE_JWT_SECRET` · `ACCOUNT_DB_*` 필요(테스트용 아무 값). 새 PostgreSQL 에 마이그레이션 68개가
  V74 까지 적용되는 것도 함께 확인됐다. 전체 238건 통과.

## 38. 하위 워크플로우 교착 대비 - 트리거러 추가 + deferrable 대기 (2026-09-17)

**요청**: 하위 워크플로우 노드가 자식이 끝날 때까지 워커 슬롯을 쥐어(PARALLELISM=4) 상위가 겹치면 «상위는 자식을 기다리고 자식은 슬롯이
없어 못 뜨는» 교착이 가능하던 것을 막는다. `deferrable=False` 였던 이유는 트리거러 서비스가 없어서였다(그 밖의 기록 없음).

**변경**
- `docker-compose.yml` `airflow-triggerer` 추가(같은 Airflow 이미지, dags · logs · plugins 마운트, 0.5 CPU / 640m).
- `etl_workflows_dynamic.py` SUBWF 노드 `TriggerDagRunOperator(deferrable=True)` - 대기 중 슬롯 반납, 트리거러가 자식 상태를 지켜본다.
- 로그 · 로그2 화면: 태스크 `deferred`(하위 대기) · `up_for_reschedule`(센서 대기)를 «실행 중»으로 표시 · job 집계. 전에는 영문 그대로 · 회색.
- 실패 로그 창 잡음에 `Pausing task as DEFERRED` 추가. 하위 추적 조건(`Dag Run triggered successfully` + trigger_dag_id)은 deferrable 에서도 그대로 찍혀 동작.
- `AirflowHealthResponse` 주석(트리거러 항상 null → 이제 있음). 대시보드 상태 점검은 트리거러가 있으면 «트리거러» 항목을 자동 표시.

**트리거러 메모리**: 384m 로 시작했더니 트리거를 붙잡은 뒤 헬스체크(`airflow jobs check`, 자체로 ~200MB 파이썬 프로세스)와 겹치는 순간
OOM(137)으로 35초마다 재시작됐다(`docker events` 에 oom, 로그에는 흔적 없음). 640m + 헬스체크 30초로 해소. 평소 ~270MB.

**검증**(실제 ETL 을 돌리지 않는 임시 DAG · 1슬롯 풀, 끝나고 DAG · 풀 · 로그 · 카탈로그 삭제): 상위 → 중간 → 말단(20초) 3단 중첩.
- blocking(기존): 상위 대기가 슬롯을 쥐고 중간이 3분 넘게 `scheduled` - **교착 재현**
- deferrable: 두 대기 태스크 `deferred` → 말단 실행 → 성공 체인 36초. 같은 1슬롯에서 실패 체인을 동시에 돌려도 교착 없이 말단 실패가 상위까지 failed 로 전파
- 실제 `total` 의 하위 노드 2개 `deferrable=True` 파싱 확인(import 오류 없음). **실제 워크플로우 실행은 다음 정기 실행(10:30)이 처음**이다.

## 39. CDC 실패 정책 전환 + DLQ 백엔드 정리 (2026-09-17)

설계 · 결과 · 운영 절차 · 운영 반영 순서는 `docs/2026-09-09-cdc-error-policy-dlq.md` 9절에 모두 적었다. 요약:

- `JdbcSinkTemplate`: CDC 테이블 싱크 `errors.tolerance=none`(DLQ 설정 제거), 로그 파이프라인 싱크는 `all` 유지 · DLQ 만 제거.
- `AlertEngine.connectorFailedSignals`: 커넥터 상태 FAILED 에 더해 `pipeline_connector.last_status_json` 의 **태스크 FAILED** 도 잡는다.
- 로컬 `CONNECTOR_FAILED` · `SERVICE_UNREACHABLE` 켬(관리 API, 사람이 끈 설정이라 마이그레이션으로 강제하지 않음). 로컬 활성 수신자 0명(차수봉 비활성).
- Kafka 보존 실측 7일 → `KAFKA_LOG_RETENTION_HOURS: 336`, kafka 재생성(토픽 유지 확인).
- `PipelineDeployService.refreshSinkConfig` + `POST /api/pipelines/{id}/sink-config/refresh` + CDC 관리 «상세 › Connector» 정책 표시 · «싱크 설정 재적용».
  준비(PREPARE)와 같은 설정이 나오게 싱크 설정 렌더링을 함수로 뺐다. 명령 이력 `SINK_CONFIG_REFRESH`(CDC 로그 «싱크 설정 재적용»).
- DLQ 백엔드 삭제(API 5 · 서비스 2 · 엔티티 · DTO 4 · 보안 규칙), V75 `dlq_replay_request` 삭제(로컬 · 운영 0건), 화면 API 함수 삭제,
  파이프라인 삭제 시 `dlq.pipeline-{id}` 토픽도 정리.

**실측으로 바뀐 전제**: Debezium JDBC 싱크는 `all` 이어도 **DB 거부(길이 초과 등)는 버리지 않고 태스크를 멈췄다**(DLQ 비어 있음). `all` 이
조용히 버린 것은 변환 오류(깨진 메시지)뿐이었다. 그리고 그 멈춤을 `CONNECTOR_FAILED` 가 못 잡고 있었다 - 알림 보완이 실질 효과가 컸다.

**검증**(로컬 target-db 안의 실제 CDC 파이프라인을 앱 API 로 생성 · 준비 · 시작, 끝나고 삭제 API + 잔여 기록 · 연결정보 · 스키마 정리, target-db 는 다시 정지):
옛 설정 흉내 → 긴 값에 멈춤(DLQ 0) · 알림 FIRING 20초 내 · 파이프라인 FAILED → 컬럼 확장 + 재시작 → 멈춘 건부터 적재 · 자동 복구 · 알림 300초 뒤 해소 →
깨진 메시지는 옛 설정에서 DLQ 로 조용히 빠짐 → 화면에서 «싱크 설정 재적용»(경고 → 멈춤, Connect 실제 설정 none) → 같은 깨진 메시지에 멈춤 →
오프셋 한 칸 넘기기 + 재시작으로 탈출(소스 6 = 타깃 6) → 삭제 API 가 커넥터 · 토픽 · `dlq.pipeline-4` · 복제 슬롯까지 정리.
백엔드 테스트 246건 통과(통합 테스트 포함, `JdbcSinkTemplateTest` 로그 파이프라인 정책 1건 추가).

## 40. CDC 복구 수단 3가지 - 재시작 · 멈춘 상태 배포 · 멈춘 한 건 건너뛰기 (2026-09-17)

**배경**: 실패 정책을 «멈춤»으로 바꾼 뒤(39절) 확인해 보니 **멈춘 파이프라인을 화면에서 되살릴 방법이 없었다** - 런타임 감시가 FAILED 로
바꾸면 «실행 설정»의 시작 · 중지는 상태 검사에서 거절되고, 배포는 «Kafka Connect가 실패 상태입니다»로 실패했다(실측). 재시작 API 는 있었지만
화면 · 제어 DAG 어디에도 없었다. 사용자 요청으로 ② → ① → ③ 순서로 만들었다. 화면 안내는 docs/2026-09-17-cdc-failure-and-subworkflow-guide.md 5절.

**② 멈춘 상태에서 배포되게** — `awaitConnectorState` 가 STOPPED · PAUSED 를 기다리는 동안에는 «태스크 FAILED» 를 확정 실패로 보지 않는다.
멈추면 태스크는 어차피 사라지는데 그 전 순간을 실패로 단정하고 있었다. RUNNING 을 기다릴 때만 즉시 중단한다.

**① 재시작** — `PipelineDeployService.restart` 재작성: DEPLOYED · FAILED 에서만 허용(READY · STOPPED 는 «시작»이 맞다),
일부러 멈춰 둔 커넥터는 건드리지 않음, Connect 의 «실패한 것만 재시작»(`restartFailed` = `POST /connectors/{name}/restart?includeTasks=true&onlyFailed=true`)
사용, 재시작 뒤 RUNNING 확인(원인이 그대로면 실패로 끝난다). 재시작은 비동기라 요청 직후 옛 FAILED 가 보이므로 **6초 유예**를 둔다
(유예 없이 만들었더니 실제로는 복구됐는데 apply_action 이 실패로 끝났다 - 실측). 제어 DAG `kafka_pipelines_dynamic.py` 에 `restart` 동작 추가
(검증 기대값은 start 와 같고, 적재 검증은 건너뛴다), 로그 화면 «실행 설정»에 «재시작 (Restart)» 선택지.

**③ 멈춘 한 건 건너뛰기** — `SinkStuckRecordService` + `GET /api/pipelines/{id}/sink-stuck`(멈춘 위치 · 밀린 건수 · 레코드 내용(마스킹 · 2천자) ·
원인 trace) · `POST …/sink-stuck/skip`(사유 필수, 화면에서 본 위치와 실제가 같을 때만, 싱크가 멈춰 있을 때만 → 오프셋 +1 → 자동 재시작).
CDC 관리 «상세 › Connector» 에 빨간 안내와 «이 한 건 건너뛰기» + 사유 입력 창. 이력 `SKIP_RECORD` 에 위치 · 사유 · **요청자**가 남는다
(`PipelineCommandHistoryRecorder` 에 requested_by 오버로드 추가 - 되돌릴 수 없는 동작은 누가 했는지 남아야 한다).

**검증**(로컬 target-db 안 실제 CDC 파이프라인, 끝나고 삭제 · 기록 정리 · target-db 정지):
- ① 고치지 않고 재시작 → 502 «taskStates=[FAILED]»(확인이 실제로 동작). 제약 위반으로 멈춤 → 제약 제거 → 제어 DAG restart →
  apply_action · verify_action success, 이력 «재시작: SINK / 그대로: SOURCE(정상)», 밀린 2건 적재. 화면 «실행 설정»에 선택지 5개 확인
- ② FAILED 에서 배포 → READY(전에는 502) → 시작 → 밀린 8 · 9번 적재
- ③ 깨진 메시지 주입 → 멈춤 → 화면에서 내용 · 원인 확인 → 사유 없이는 확인 버튼 비활성 → 건너뛰기 → 다음 건 적재, 소스 10 = 타깃 10.
  API 거절 경로: 사유 없음 400, 위치 불일치 400(«화면 16, 지금 11»)
- 백엔드 테스트 248건(재시작 규칙 3건 추가). 통합 테스트는 단독 실행 시 통과 - 빌드 · 브라우저 검증과 겹치면 메모리 부족으로 테스트용
  Postgres 컨테이너가 제때 못 떠 실패한다(코드 문제 아님, 로컬 가용 메모리 약 1.8GB)

## 41. 디자인 작업물(2026-09-17) 적용 - 대시보드 · 워크플로우 · CDC · 설정 · GPT 설정 (2026-09-17)

**요청**: `/home/user/CEREBRO_ETL_UI_DESIGN_20260917`(«CEREBRO Enterprise UI Standard v2.0», 20화면)을 대시보드 · 워크플로우 · CDC ·
설정 4개 메뉴에 적용. 기준은 현재 프로젝트(기능 변경 금지), 너무 다른 화면은 목록으로 보고. GPT 설정은 디자인이 없으므로 표준 느낌만 맞춤.
ETL 메뉴는 다른 작업자 담당이라 제외.

**공통 틀(새로 만듦)**
- `theme/cerebro.ts` `cerebroStandardTheme`: 표(머리 #fafbfc · 12px 14px · 본문 12.5px) · 버튼(32px/표 안 24px, 보조 회색 테두리) ·
  탭 · 페이지 번호(지금 페이지 빨간 바탕) · Segmented · 모달 토큰. CSS 로 antd 를 덮지 않고 토큰으로 준다.
- `components/StdPage`: 위 테마 + 화면 안 모달 · 서랍에 `std-modal` · `std-drawer` 클래스(ConfigProvider modal/drawer classNames) +
  머리줄(제목 + 같은 메뉴 하위 탭). `WorkflowPageHeader` 를 메뉴별 탭(`MenuPageHeader menu=workflow|cdc|gpt|settings`)으로 일반화 -
  탭 권한 규칙은 AppLayout NAV_ITEMS 와 같게(연결정보 anySystems 포함). 아이콘 9종 추가.
- `components/StdWizard`: 생성 위저드 윗 카드(모드 전환 · 안내 · 가로 단계 표시) · 단계 머리(번호 원) · 상태 알약(작성 중/완료/잠김/대기).
- index.css «표준 화면» 블록: `.std-card` · `.std-filter-card` · `.std-toolbar` · `.std-date-preset`(당일/전일, 고른 것 빨간 바탕) ·
  배지 알약 7색(antd Tag 상태/프리셋 색 이름에 매핑) · Alert 배너 · 모달 머리/바닥 띠 · 페이지 번호 배치(총 N건 왼쪽) ·
  입력 표(`.std-form-table`, 위저드 본문의 직계 Form.Item) · 빈 상태. 전부 `.std-page/.std-modal/.std-drawer` 안으로 한정.

**화면**
- 설정 5: 알림/발송(탭 → 필터 띠 카드 → 표 카드) · 연결정보 · 계정 · 역할 및 권한(두 카드) · 감사. 계정의 [비활성화]/[활성화] 버튼은
  디자인처럼 «계정활성화» 스위치 열로 옮김 - 끌 때 같은 확인(Popconfirm), 켤 때 바로 활성화(동작 동일).
- CDC 3: 관리(트리 카드 + 필터·표 카드, 화면 바닥까지) · 생성(탭 → 모드 버튼, 단계 표시 추가 - 단계 클릭은 접이식 머리와 같은 규칙,
  단계는 떨어진 카드 · 지금 단계 빨간 띠, 본문 직계 입력칸은 왼쪽 회색 이름 칸 표 모양) · 로그(필터 띠 · 표 카드, 윗단 지표 커튼은
  ETL 로그와 같은 마크업이라 ETL 로그 화면 규칙을 `.cdc-log-page` 범위로 복사 - ETL CSS 는 그대로).
- GPT 2: 관리(툴바 · 표 카드) · 등록/수정(입력 표 카드 + 바닥 동작 띠).
- 정적 `Modal.confirm` 두 곳(CDC 그룹 삭제 · GPT 외부 제공자 선택)을 `Modal.useModal` 로 - 정적 호출은 화면 테마를 못 받는다(내용 · 동작 동일).
- 이미 적용된 화면(§32 · §33)은 새 디자인과 달라진 치수만: 하위 탭 바탕 #f1f5f9, 워크플로우 관리 · 로그 표 여백 12px 14px · 본문 12.5px,
  «전체 N건» → «총 N건», 두 표의 페이지 번호 빨간 바탕, 대시보드 오류 상세 모달 표 머리.

**적용하지 않은 것(화면이 다르거나 기능이 없음)** — 워크플로우 «상세 관리»(Airflow 원본 iframe ↔ 디자인은 Pool · DAGs · DAG Runs ·
Task Instances 자체 화면 4개), 워크플로우 «생성»(캔버스, 디자인 없음), «로그2»(디자인 없음), CDC 로그 디자인의 Sink Lag 추이 차트 ·
처리 이력/오류·상태 이력/실패 데이터 탭 · 상태 필터 · 갱신 주기(현재 화면은 명령 · 처리 통합 이력이라 틀만 적용), CDC 관리 «도움말 가이드»,
연결정보 «사용여부» 필터, 헤더 비밀번호 변경 모달(현재는 별도 화면), 페이지 번호 가운데 정렬.

**검증**: tsc 통과, 변경 파일 oxlint 새 경고 없음(기존 3건). 배포본 qa-playwright 1920 - 디자인 HTML 과 나란히 캡처 비교, 동작:
알림 이력 프리셋 · 분류 · 규칙 추가/설정/로그/수신자 창 · 채널 · 수신자 수정 창, 연결정보 상세/신규 창, 계정 역할 배정/수정(값 채움)/비번 초기화 창 ·
스위치 확인창 취소 시 상태 유지, 역할 선택 → 권한 표 · 저장 버튼, 감사 전일 · 새로고침, CDC 관리 트리 필터 · 이름 필터 · 상세 서랍 6탭 ·
그룹 우클릭 메뉴, CDC 생성 필수값 검사 · 2단계 이동 · 단계 표시 상태 · 단계 클릭 복귀(입력 유지) · 모드 전환(입력 비움), CDC 로그 지표 필터 ·
펼치기 · 전일, GPT 수정 값 불러오기, ETL 화면에 표준 틀 없음. 모달/서랍 root 에 std-modal/std-drawer 확인. 저장 · 실행 · 삭제는 누르지 않았다.

**발견한 기존 문제(고치지 않음)**: ① GPT 수정 화면(`/gpt/register?id=`)에서 «등록» 탭 · 상단 메뉴로 옮기면 같은 컴포넌트가 남아 입력값이
그대로이고, 저장하면 새 등록을 시도한다(검증 중 이름 중복 409 로 거절 - DB 변화 없음 확인). 탭이 생기며 이 경로가 쉬워졌다.
② CDC 생성 1단계에서 필수값을 비우고 «다음»을 누르면 `validateFields` 거절이 onClick 밖으로 새어 콘솔에 처리 안 된 오류 1건(화면 동작 영향 없음).

**41절 추가 (같은 날, 사용자 요청으로 위 두 문제 수정)**
- ① `GptRegisterPage` 가 `?id` 를 읽어 `<GptRegisterForm key={id ?? "new"} />` 를 그린다 - 대상이 바뀌면 폼을 새로 띄워 입력값 · 테스트 결과 ·
  «직접 바꾼 값» 표시가 비워진다. 확인: 수정(gemma4) → «등록» 탭 → 이름 칸 빈칸 · 제목 «GPT 등록», 저장 → 필수값 오류 3건만(요청 없음),
  뒤로 가기 → 다시 gemma4 값.
- ② CDC 생성 두 위저드의 «다음» · 생성 버튼(8곳)이 `passes(fields)`(검증 통과 여부만 돌려줌)로 막고 돌아간다. 같은 모양이던 연결정보
  신규 등록 «연결 테스트» 버튼도 함께 고침. 확인: 필수값 비우고 누르면 칸 오류만 표시 · 콘솔 오류 없음, 입력 후 «다음» → 2단계 정상,
  확인 중 GET 이외 API 요청 0건.

**41절 추가 2 (같은 날, 사용자 요청)** — CDC 생성 화면
- 폭: ETL 생성(`.etl-create-page` 최대 1280px 가운데)과 같게 `.std-page.cdc-create-page` 최대 1280px(머리줄 포함). 1920 화면 실측 둘 다 x=320 · 폭 1280.
- 단계 이동: 윗 단계 표시 · 단계 머리를 누르면 앞 단계를 끝내지 않았어도 그 단계로 간다(ETL 생성과 같음, «잠김» 없음 → «대기»). 각 단계 «다음»의
  검사(필수값 · 연결 테스트 · PK · 캡처 조건 · 구분컬럼 등)는 그대로.
- 생성: CDC · 로그파일 모두 1~4단계가 «다음»으로 완료돼 있어야 한다 - 아니면 «○○ 단계를 먼저 완료하세요» 경고와 함께 첫 미완료 단계로 옮긴다.
  앞 단계 값을 바꾸면 뒤 단계 완료가 풀리는 기존 규칙(invalidateFrom)도 그대로라 생성 전에 다시 확인하게 된다. 윗 안내 문구도 이에 맞춰 고침.
- 확인(qa-playwright): 5단계 바로 이동 → 생성 → 1단계로 이동 · 경고, 머리로 3단계 열기, 1단계 필수값 검사 유지, 1단계 완료 후 생성 → 2단계로 이동,
  로그파일 위저드 동일. GET 이외 API 요청 0건 · 페이지 오류 0.


## 42. 워크플로우 속성창 보강 + GPT 등록 화면 가운데 배치 (2026-09-17)

**요청**: 워크플로우 노드 속성창의 틀린 스케줄 표시 수정(설계 대상 워크플로우 속성창도 같이), «최근 실행 · 게시 상태 · 구성 요약» 추가.
GPT 등록 화면이 왼쪽에 치우침 → 가운데.

- **틀린 표시**: DAG 생성기는 <b>게시된</b> 상위 워크플로우가 노드로 품은 워크플로우의 자체 스케줄을 끈다(`etl_workflows_dynamic.py` 294행).
  그런데 속성창은 꺼진 스케줄과 «다음 2회 실행»을 그대로 보여 줘서 그 시각에 도는 것처럼 읽혔다. 이제 «상위 워크플로우가 부를 때만 실행됩니다 ·
  자체 스케줄(…)은 꺼져 있습니다 · 최상위 ○○ 기준 다음 실행»으로 보인다. 상위가 모두 미게시면 «위 스케줄대로 단독 실행». 워크플로우 속성창은
  스케줄 입력은 그대로 두고(상위에서 빠지면 다시 쓰임) 안내만 더했고, 카드 제목 옆 안내 아이콘도 «게시된 상위» 기준으로 고쳤다.
- **추가 구역**(`components/WorkflowInfoSections`, 두 속성창 공통, 읽기 전용): 게시 상태(게시 일시 · 게시자 · 게시본과 다름, 노드에서 미게시면
  «상위 실행 시 이 노드에서 실패» 경고) · 구성 요약(job · 하위 워크플로우 · CDC 노드 개수와 이름, 사라진 job 경고) · 최근 실행 5회(상태 · 시작 ·
  소요 · 스케줄/상위 호출/수동, 실패는 «로그» → 로그 화면과 같은 실패 로그 창, «로그 화면에서 보기»). 노드 속성창은 이 캔버스 워크플로우가
  <b>이 노드로</b> 부른 실행만 - 상위 실행 목록으로 `manual__{상위 run}__{노드키}` 를 만들어 정확히 맞춘다. `publishedParentsOf` 는 utils/workflowTree.
- **GPT 등록 · 수정**: 머리줄 포함 최대 980px 가운데(`.std-page.gpt-register-page`). 1920 실측 좌우 여백 470 · 470.
- **확인**(qa-playwright, 저장 · 게시 안 누름, GET 이외 요청 0 · 페이지 오류 0): dz_com_daily 워크플로우 속성창 · total 속성창 · total 캔버스의
  dz_com_daily 노드 속성창 문구 확인(total 은 상위 없음 → 기존 «다음 2회 실행» 유지, 실패 실행 3건에 «로그» 버튼).

**배포 때 함께 처리한 것**: 19:40 Docker Desktop 재기동으로 cerebroetl-ui · airflow-apiserver · filebeat · account-db-local 이 exit 127
(단일 파일 마운트, 메모리 기록과 같은 증상) → `up -d --build --force-recreate --no-deps` 로 복구. airflow-scheduler · dag-processor · triggerer 는
떠 있지만 `/opt/airflow/dags` 가 **0개**(호스트 8개) - 재생성하면 밀린 회차가 곧바로 돌 수 있어 사용자 확인 전까지 두었다.
→ 같은 날 19:51 «CDC 워크플로우 노드» 세션이 세 서비스를 `--force-recreate --no-deps` 로 재생성(그 세션 확인: 이후 scheduled/backfill 회차 0건 ·
  밀린 회차 실행 없음, 다음 정기 실행 total 09-18 10:30). 이 세션에서 dags 파일 수 복구를 다시 확인했다.
이번 UI 빌드에는 작업 트리에 있던 다른 작업(«CDC 워크플로우 노드», 미커밋 · 백엔드 미배포 · V77 미적용)의 화면 코드도 함께 들어갔다(타입 검사는 통과).


## 43. 워크플로우 CDC 노드 - 부르고 끝낸다(방안 B) + 한 개만 실패했을 때의 조치 (2026-09-17)

**요청**: 워크플로우 캔버스에 CDC 노드(설계 `docs/2026-09-10-cdc-workflow-node-design.md` 방안 B). 사용자 고민 - «CDC 여러 개 중 하나가 실패하면
어떻게 조치하나» → 노드별 상태 · 바로가기 + 일괄 restart(실패한 것만 되살림) + 이미 그 상태면 실패 대신 건너뜀.

- **저장·검증**: V77 `etl_workflow_node.pipeline_id`(FK `pipeline_definition`). `node_type='CDC'`. «CDC 워크플로우»는 따로 두지 않고 노드 구성에서
  읽는다(CDC 노드가 하나라도 있으면). 게시 검증 V12 - CDC 노드는 파이프라인 필수·실재 / 연결선 금지 / CDC 노드만(job·하위 워크플로우와 섞지 않음) /
  스케줄 금지 / 같은 파이프라인 중복은 경고. 없는 파이프라인 참조는 저장에서 400. 캔버스에 얹힌 파이프라인은 삭제 거부(«워크플로우에서 사용 중»).
- **컴파일·DAG**: 노드 spec `pipeline_id · cdc_dag_id(kafka_pipeline_{id}_control) · name`, 최상위 `kind=CDC`. 팩토리 `_build_cdc` =
  `TriggerDagRunOperator(conf={"action": "{{ params.action }}"}, wait_for_completion=False)`. CDC 워크플로우는 스케줄·완료 신호(workflow_done) 없음,
  `params.action`(start·stop·restart·deploy), `max_active_runs=2`, 태그 `cdc-workflow`(«cdc» 만 쓰면 키가 cdc 인 ETL 워크플로우와 겹침 - 검증 중 실제로 겹쳤다).
- **제어 DAG**: `skip_reason` - start 인데 DEPLOYED, stop 인데 STOPPED/READY/CREATED, restart 인데 DEPLOYED·FAILED 가 아님 → `AirflowSkipException`
  (실패가 아니라 건너뜀). **감시는 파이프라인당 하나** - 감시 센서가 `GET /api/airflow/dag-catalog/{dagId}/watch-runs`(running 만 조회, 먼저 뜬 순)로
  «나보다 먼저 뜬 감시»가 있으면 물러난다. 백엔드·화면의 «감시 중» 판정에 restart 포함(원래 start·monitor 뿐).
- **화면**: 캔버스 CDC 팔레트 끌어 놓기(청록 `cdc` 노드, 연결점 없음, 연결 시도 경고), CDC 노드 속성창(파이프라인 · 실행 방식 · CDC 파이프라인 바로가기,
  실행 조건 칸 없음, GPT 탭은 CDC_PIPELINE 수집기), CDC 워크플로우면 스케줄 칸 대신 안내. 실시간 모니터링 - 실행 설정 선택지 4개, 속성창
  «CDC 파이프라인 (N) · 실행 상태(집계: 하나라도 FAILED→실패 / 전부 실행→실행 중 / 전부 멈춤→중지 / 섞임→일부 실행)» + 파이프라인별 실제 상태 +
  이름 누르면 `/cdc/pipelines?pipelineId=` 로 그 상세 창이 바로 열림. 태스크 유형 «CDC 지시», 성공 문구 «지시 보냄», 업무 구분 «CDC 워크플로우».

**검증**
- 단위: `WorkflowCdcNodeTest` 8건(V12 5 + 컴파일 3), `PipelineServiceTest` 삭제 가드 1건 추가 - 13건 전부 통과. 프런트 tsc 통과.
- DAG 팩토리(컨테이너 안 직접 생성): 크론이 실려 와도 NullTimetable, workflow_done 없음, 노드별 send(wait=False, 서로 무관), 사라진 파이프라인 노드는
  missing 실패 태스크, ETL 워크플로우는 그대로.
- API: 연결선 → V12 거부, 없는 파이프라인 저장 → 400, 올바른 그래프 valid · 게시 → DAG 등록.
- **E2E(target-db 로컬 전용 테스트 파이프라인 7·8, 검증 후 삭제)**: 워크플로우 deploy → 둘 다 READY / start → 둘 다 DEPLOYED · 초기 2건씩 적재 →
  B 타깃에 CHECK 제약 + 양쪽 소스에 amount=5000 → **A 3건 적재, B 싱크 태스크만 FAILED**(커넥터 RUNNING) → 런타임 상태 7=RUNNING · 8=FAILED,
  8번 제어 DAG 감시만 failed · 파이프라인 8=FAILED → 화면 속성창 «실행 상태 실패 · e2e-wf-b 실패», 눌러서 상세 창 열림(캡처 `~/qa-gpt/wfcdc-fail-*`) →
  제약 제거 후 워크플로우 restart → 7번 «다시 띄울 커넥터가 없습니다(SOURCE(정상), SINK(정상))», 8번 «재시작: SINK / 그대로: SOURCE(정상)»,
  **B 에 거부됐던 3:5000 과 밀린 4:400 까지 유실 없이 적재**.
- READY 두 개에 stop → 두 제어 DAG 모두 apply_action skipped(«이미 멈춰 있습니다»), Run 은 success, 상태 변화 없음.
- **검증 중 발견해 고친 것**
  1. 일괄 restart 가 이미 감시받던 7번에 감시 Run 을 하나 더 붙여 max_active_runs=2 가 꽉 참(이후 stop 이 큐에 갇힘) → 위 «감시는 하나».
     배포 후 실측: 7번의 늦게 뜬 restart 감시가 «이미 먼저 뜬 Run(…)이 감시 중입니다 - 감시를 겹쳐 붙이지 않습니다»로 skipped, 감시 1개로 복귀 →
     워크플로우 stop → 7·8 STOPPED, 감시 Run 모두 종료.
  2. 워크플로우를 지워도 파이프라인 삭제가 막힘 - 워크플로우는 소프트 삭제라 노드가 남는데 가드가 그걸 셈. job 가드처럼 지운 워크플로우는 빼고,
     남은 노드 참조는 `clearPipelineReference` 로 비운 뒤 삭제(안 비우면 FK 위반). 단위 테스트 1건 추가(PipelineServiceTest 14건), 실측 삭제 200.
  3. CDC 워크플로우 식별 태그 «cdc» → «cdc-workflow»(키가 cdc 인 워크플로우와 겹침).
- 화면 표기: 태스크 유형 «CDC 지시», 성공 문구 «지시 보냄», 업무 구분 «CDC 워크플로우», 바로가기 «CDC 파이프라인». 캔버스 캡처 `~/qa-gpt/wfcanvas-*`
  (cdc 노드 연결점 0, 팔레트 «(배치됨)», 노드 속성창, 스케줄 안내, 쓰기 요청 0 · 페이지 오류 0).
- **정리**: 테스트 워크플로우 37·38 게시취소·삭제, 파이프라인 7·8 삭제, target-db 테스트 스키마·테이블 삭제, 커넥터·토픽 0, 제어 DAG stale,
  target-db 는 원래대로 중지.
- **고치지 않고 남긴 것(기존 코드)**: 파이프라인 삭제 때 Postgres 복제 슬롯 정리(`PostgresReplicationCleanupService`)가 소스 커넥터가 아직 슬롯을
  잡고 있으면 실패하는데 예외를 삼켜 **고아 슬롯이 남는다** - 이번에 8번 슬롯이 남아 수동으로 지웠다(7번은 지워짐, 타이밍 경합). 소스 DB 에
  WAL 이 계속 쌓이는 위험이라 별도 처리 필요.

**배포 때 함께 처리한 것**: 19:51 airflow-scheduler · dag-processor · triggerer 를 `--force-recreate --no-deps` 로 재생성(DAG 0개 상태).
42절이 «밀린 회차가 돌 수 있어 보류»로 적어 둔 것을 읽기 전이었다 - 재생성 뒤 scheduled/backfill 회차 0건 확인(정기 DAG 는 root_total 하나, 다음 09-18 10:30).


## 44. 워크플로우 CDC 노드 - 섞어 설계했을 때 · 미확인 항목 확인과 수정 (2026-09-17)

**요청**: 43절에서 확인 못 한 것 전부 진행 + «한 워크플로우에 ETL 노드와 CDC 노드가 병렬로 같이 있으면 어떻게 처리되나».
상세 표는 `docs/2026-09-10-cdc-workflow-node-design.md` 7-3·7-4.

**확인(수정 전 실측)** - 운영 워크플로우·파이프라인은 건드리지 않고 검증용 워크플로우 5개 + target-db 테스트 파이프라인 1개로.
1. 로그 화면 «이 작업만 실행»(CDC 노드) → `Dag Run already exists` 로 실패, 워크플로우 Run 전체 failed, 지시 안 감.
2. 스케줄(0 3 * * *) 있는 ETL 워크플로우에 CDC 파이프라인을 끌어 놓고 저장 → **스케줄이 경고 없이 NULL**. 게시는 V12 로 거부(문구가
   «CDC 워크플로우에는 CDC 노드만» + «스케줄 금지» 라 ETL 워크플로우에 엉뚱한 안내).
3. 혼합 스펙을 컨테이너에서 DAG 로 직접 생성 → 전체가 CDC 취급(NullTimetable, workflow_done 없음, job 태스크와 send 병렬).
4. GPT - CDC 노드 GPT 탭(CDC_PIPELINE) 정상, 섞인 워크플로우의 job 노드 정보 정상. WORKFLOW_NODE 수집기는 CDC 노드를 모르지만 화면이 그 경로를 안 씀.
5. 하위 워크플로우 노드로 CDC 워크플로우 → 검증 통과, 상위를 conf 없이 실행(정기 실행과 같은 조건) → 하위 CDC → 제어 DAG action=start →
   **파이프라인 READY→DEPLOYED**.
6. 선행 체이닝 - ETL 의 선행=CDC(완료 신호 없어 영영 안 돎), CDC 에 선행 지정(무시돼 자동 실행 안 됨) 둘 다 검증 통과.

**수정**
- 설계 화면: 섞는 드롭 3종(ETL 에 CDC 파이프라인 / CDC 에 job / 어디든 CDC 워크플로우를 하위로) 놓는 순간 경고·거부. 팔레트 워크플로우 줄 «· CDC».
  «CDC 워크플로우» 판정을 «전부 CDC» 로 바꿔 섞인 상태에서 스케줄을 비우지 않음. 목록 응답 `cdcNodeCount` 추가.
- 게시 검증: V8(하위=CDC 워크플로우) · V10(선행=CDC 워크플로우) · V12(CDC 워크플로우에 선행 / 다른 워크플로우의 하위·선행으로 쓰이는데 CDC 로 게시)
  추가, 섞임 문구 중립화 + 적은 쪽 노드 짚기 + 섞였으면 CDC 전용 규칙 생략.
- DAG: CDC send 재시도면 `…__{노드키}__retryN` 새 제어 Run(첫 시도 이름 유지, reset_dag_run 은 감시를 끊어 쓰지 않음).
- 테스트: `WorkflowCdcNodeTest` 8→13건 전부 통과. 프런트 tsc 통과.

**수정 후 실측**: 드롭 3종 모두 경고·노드 안 붙음·쓰기 요청 0, 저장 후 스케줄 0 3 * * * 유지, 이미 섞인 데이터를 열어 저장해도 스케줄 유지,
검증 5종(섞임 · 하위 · 선행 · CDC 에 선행+역참조 · 순수 CDC 통과) 의도대로, CDC 노드 재실행 → `__retry2` 제어 Run 건너뜀 · 워크플로우 Run success.
기존 게시 워크플로우 total · dz_com_daily · dz_pop_daily 검증 그대로 통과.

**정리**: 검증용 워크플로우 39~43 게시취소·삭제, 파이프라인 9 삭제(이번엔 슬롯 정상 삭제), target-db 테스트 스키마·테이블 삭제 후 중지.
삭제된 파이프라인 9 의 감시 Run 은 DAG 가 사라지자 Airflow 가 «Dag not found during start up» 으로 failed 처리(무한히 남지 않음).

**함께 처리한 것**: 22:19 KST 다시 재부팅 → 같은 7개 서비스 복구. 이번엔 메모리 절차대로 먼저 확인 - 다른 세션 없음, WORK_LOG 보류 없음,
게시된 정기 워크플로우는 root_total(오늘 11:15 실행, 다음 09-18 10:30)뿐·dz_com_daily 자체 스케줄은 상위 때문에 꺼짐·dw_pop_daily 미게시 →
밀린 회차 없음 판단 후 복구, 복구 뒤 scheduled/backfill 0건 확인. 검증 중 본 것(기존 동작): 지운 워크플로우의 키가 새 워크플로우에 다시 쓰여
(`etl_wf_cdc`) 예전 실행 이력이 새 워크플로우 DAG 에 이어 보인다.

## 45. ETL 관리 화면과 같은 패널 모양 - CDC 상세 창 · 워크플로우 설계 · AI 어시스턴트, 화면 머리줄 제거 (2026-09-17)

**요청**: ① CDC 파이프라인 상세 창의 탭 구조와 GPT(AI 어시스턴트) 모양을 ETL 관리 화면 속성창과 같게 ② 워크플로우 설계 화면의 왼쪽 트리 ·
가운데 캔버스 · 오른쪽 속성 · AI 어시스턴트를 ETL 관리 화면과 같게(정보 · 기능 변경 없이, 워크플로우 트리의 점선은 유지) ③ 화면 위
«메뉴명 + 하위 메뉴» 줄을 모든 화면에서 제거.

- **머리줄**: `MenuPageHeader`(WorkflowPageHeader · StdPage 공용)가 제목 · 하위 탭을 그리지 않고 오른쪽 조작만 남긴다(로그 · 로그2 ·
  워크플로우 관리의 동기화 · 새 워크플로우 등). 조작이 없는 화면(CDC · 설정 · GPT)은 머리줄 자체가 없다. MENU_TABS 표는 남겨 둠.
- **AI 어시스턴트**(`GptChatPanel`): ETL 관리의 nifi-ai-* 마크업 - 요약 카드(GPT 이름 · «○○의 정보와 함께 질문을 보냅니다» · 배치/마스킹 ·
  전송될 정보 보기) → 대화 칸(남은 높이) → 둥근 보내기 버튼이 붙은 입력칸(`GptAskBox variant="composer"`, Enter 보내기 · Shift+Enter 줄바꿈 그대로).
  대화 줄은 «나 / AI» 말풍선. 중지 · 새 대화 · 요약 안내 · 전송 정보 창은 그대로. 로그 실패 창의 GPT 분석(GptErrorAnalysis)은 기존 모양.
- **CDC 상세 창**(`PipelinesPage` CdcDetailShell): 위 «상세정보 | AI 어시스턴트»(nifi-detail-tabs), 상세정보 안에 기존 5탭(기본정보 · 운영 상태 ·
  Connector · 정합성 · 이력). AI 어시스턴트는 선택된 GPT 가 있을 때만(예전 GPT 탭 조건), 파이프라인마다 대화 새로 시작(key) 동일.
- **워크플로우 설계**(`WorkflowCanvasPage`): 바깥 카드는 머리 띠만 남김, 왼쪽 «job 팔레트» → nifi-tree-panel(제목 · 새로고침 버튼 · 스크롤,
  PaletteTree 점선 그대로), 캔버스 → console-frame-shell, 오른쪽 → nifi-detail-panel(제목 줄 · «상세정보 | AI 어시스턴트» 탭 · 구역 제목 «■ »).
  노드의 [속성][GPT] Tabs 를 윗단 탭 상태(nodePanelTab)로 옮김 - CDC 노드면 CDC_PIPELINE 대상으로 묻는 분기(44절 작업) 그대로.
  워크플로우 전체 · 연결 조건을 볼 때는 «상세정보» 탭만.
- **확인**(qa-playwright 1920, 질문 전송 · 저장 안 함, 페이지 오류 0): 10개 화면 제목 · 하위 탭 0, 조작 줄 유지 · CDC 상세 윗단 2탭 · 안쪽 5탭 ·
  AI 요약 · 전송 정보 창 열림 · 입력 시 보내기 활성 · 워크플로우 total 캔버스 패널 제목/탭, dz_com_daily 노드 구역 6개 · AI 탭 · 빈 곳 클릭 시
  «워크플로우 속성 / 상세정보» 복귀, ETL 관리 화면과 나란히 캡처 비교.


## 46. 파이프라인 삭제 때 복제 slot 이 남던 문제 + 지운 워크플로우 키 재사용 (2026-09-17)

**요청**: 43절 «고치지 않고 남긴 것»의 slot 문제 해결, 44절에서 본 워크플로우 키 재사용 처리.

**1) 복제 slot**(`PostgresReplicationCleanupService`)
- 원인 실측: 소스가 도는 파이프라인의 커넥터를 Kafka Connect 에서 DELETE(204) 한 직후 `pg_drop_replication_slot` →
  `replication slot "…" is active for PID 54`. walsender 가 1~2초 더 slot 을 잡는다. 코드는 커넥터 삭제 직후 바로 지우고 SQLException 을
  주석대로 «조용히» 삼켜서, 타이밍이 늦으면 slot 이 남았다(43절 8번). 주석의 «남아도 이름만 안 겹치면 문제 없다»도 틀림 - 남은 논리 slot 은
  WAL 을 붙잡아 소스 DB 디스크가 찬다.
- 수정: slot 이 풀릴 때까지 최대 15초(0.5초 간격) 기다렸다 삭제 → 그래도 사용 중이면 잡고 있는 연결을 `pg_terminate_backend` 로 끊고 5초 더
  기다려 삭제(slot 이름이 파이프라인 전용 · 커넥터는 이미 지웠으므로 그 연결은 지운 커넥터가 남긴 것) → 실패하면 예외는 여전히 안 던지되
  WARN 로그에 slot 이름 · 소스 DB · 수동 정리 SQL. 주석 바로잡음.
- 실측(target-db 로컬 테스트 파이프라인, 검증 후 전부 삭제): 소스 가동 중인 파이프라인 2개 즉시 삭제 → 둘 다 slot 삭제(응답 3.6초) /
  `pg_recvlogical` 로 같은 이름 slot 을 붙잡아 둔 채 삭제 → 15초 뒤 «끊습니다» 경고 · slot 삭제(18.1초) / 소스 DB 를 꺼 둔 채 삭제 →
  삭제는 200, «정리 실패 … SELECT pg_drop_replication_slot('…')» 경고.
- MSA 반영 전 확인할 것: 이미 남아 있을 수 있는 slot - 소스 Postgres 에서 `SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))
  FROM pg_replication_slots WHERE slot_name LIKE 'dbz_source_%'` 로 지금 파이프라인에 없는 것을 찾는다(읽기 전용).

**2) 워크플로우 키 재사용**(`WorkflowService.generateKey` · `create`)
- 원인: «소프트 삭제된 키는 다시 쓸 수 있다(유일 인덱스가 deleted_at IS NULL 조건부)»로 일부러 재사용. 키가 dag_id(`etl_wf_{키}`) ·
  실행 원장(`etl_job_run.workflow_key`) · 알림 판정(AlertEngine 이 `etl_wf_`||workflow_key 로 dag_id 계산)을 묶어서, 지운 워크플로우의
  실행 이력 · 알림 판정 · 원장이 새 워크플로우에 이어 붙었다(44절 `etl_wf_cdc`).
- 수정: `existsByWorkflowKey`(지운 것 포함)로 자동 생성 키가 한 번 쓴 키를 건너뛰고, 직접 지정한 키도 지운 워크플로우 것이면 400.
  DB 유일 인덱스는 그대로(조건부) - 이미 `cdc` 가 지운 행 2개에 있어 전체 유일로 바꾸려면 데이터 정리가 먼저 필요하다.
- 실측: 지운 `cdc` · `cdc_2` 가 있는 상태에서 같은 이름으로 생성 → `cdc_3` / 키 `cdc` 직접 지정 → 400.

**테스트**: `WorkflowServiceKeyTest` 3건 추가, 백엔드 전체 266건 중 실패 1건(ConnectionRepositoryIT, 기존 환경 문제). 45절 디자인 세션 편집 뒤
CDC 워크플로우 노드 화면 코드 보존 확인 · 프런트 tsc 통과.

## 47. 워크플로우 설계 속성창 다듬기 + 워크플로우 전체 AI 어시스턴트(WORKFLOW 대상) (2026-09-18)

**요청**: 노드 · 워크플로우 속성창의 스케줄 안내를 «상위 워크플로우가 부를 때만 실행됩니다.» 한 줄로, 노드 «워크플로우 바로가기» 삭제,
상위 경로 표기에 «상위 워크플로우» 단어 사용, 최근 실행 2개, 설계 중인 워크플로우 속성창에도 AI 어시스턴트 탭, CDC 노드 속성창에
로그 화면 CDC «기본정보» 항목. 구역 제목의 «■»는 유지.

- 화면(`WorkflowInfoSections` · `WorkflowCanvasPage`): 안내 한 줄(자체 스케줄 · 최상위 기준 다음 실행 줄과 그 계산 제거), 버튼 삭제(하위 캔버스는
  노드 더블클릭으로 열림), 제목 옆 · 이름 칸 아래 경로를 «상위 워크플로우 · total», RECENT_LIMIT 2. AI 탭은 노드를 골랐을 때(그 노드) 또는
  아무것도 안 골랐을 때(저장된 워크플로우 전체, 초안 · 연결선 선택 시 없음). CdcNodeSummary 의 «CDC 파이프라인» 구역을 «기본정보»
  (이름 · 유형 · 소스 · 타겟 · Topic · 스냅샷 모드 · 적재 방식 · 커넥터 상태 · 설명 · 생성/수정 시각 - 로그 화면과 같은 renderRuntimeStatus ·
  sourceLabel · targetLabel · loadModeLabel, 런타임 상태 조회 추가)로. 파이프라인을 못 찾는 분기와 «실행 방식» 구역은 그대로.
- 서버: `LlmTargetType.WORKFLOW`(NIFI 조회 권한) + `WorkflowContextCollector`(workflowId): 이름 · 설명 · 상위(미게시 표시 · 경로) · 스케줄과
  «게시된 상위가 있으면 돌지 않음» · 게시 · 실패 시 중단/동시 실행/catchup · 메모 · 노드 구성(job · 하위 워크플로우 · CDC 파이프라인 이름,
  선행 · 실행 조건) · job 단계. CDC 노드는 실패하지 않고 이름으로 적고, 전부 CDC 면 «실행 때 고른 지시를 모든 CDC 노드에 보냄»으로 적는다
  (CDC 워크플로우 노드 세션의 안내 반영). 예전에는 워크플로우 전체를 물을 대상이 없어 노드 대상으로 보내면 «하위 노드 · 저장 안 된 새 워크플로우»로
  잘못 설명됐다. ErrorAnalysisAgent.targetLabel 에 WORKFLOW.
- 확인: 백엔드 컴파일 + llm 테스트 10개 클래스 통과, 프런트 tsc · oxlint 0. 배포(pipeline-api · cerebroetl-ui) 후 qa-playwright -
  dz_com_daily 속성(안내 한 줄 · «상위 워크플로우 · total» · 최근 실행 2줄), AI 탭 «전송될 정보 보기» 200(위 내용 1,143자 확인),
  total 캔버스 dz_pop_daily 노드(바로가기 0 · 최근 실행 2줄). 질문 전송 · 저장 안 함, 페이지 오류 0.
  **CDC 노드 기본정보는 화면으로 못 봤다** - 로컬에 CDC 노드가 든 워크플로우가 지금 없다(목록: dw_com_daily · dz_com_daily · dz_pop_daily · total).
  → CDC 워크플로우 노드 세션이 00:12 배포본에서 확인(검증용 CDC 워크플로우 45, 파이프라인 2 · 3, 실행 안 함 · 확인 후 삭제): «기본정보» 항목
    빠짐없음 · 커넥터 상태 «실행 대기 · Source STOPPED · Sink STOPPED» · «실행 방식» · «CDC 파이프라인 바로가기» 유지, 페이지 오류 · 쓰기 요청 0.
    WORKFLOW 미리보기도 «실행 방식: CDC 워크플로우 …» + 파이프라인 이름 2개(277자) 정상. 생성/수정 시각은 로그 화면과 같은 원문 형식(요청 이미지 기준)이라 그대로 둠.
- 배포 메모: 첫 배포 명령이 사용자 중단 표시 뒤에도 끝까지 돌아(00:12 두 컨테이너 재생성 · healthy) 두 번째 명령이 이름 충돌 오류를 냈다 - 결과에 영향 없음.

## 48. 속성창 AI 어시스턴트 윗줄 단순화 (2026-09-18)

**요청**: ETL 을 뺀 속성창 AI 어시스턴트(CDC 상세 창 · 워크플로우 노드 · 워크플로우 전체)의 «이 노드의 정보와 함께 질문을 보냅니다» · 배치/마스킹 ·
«저장하지 않은 변경 …» 문구를 빼고, 윗줄을 왼쪽 GPT 이름 · 오른쪽 «전송 정보» 버튼으로.

- `GptChatPanel` 윗줄만 바꿈(`gpt-chat-head`, 버튼 이름 «전송될 정보 보기» → «전송 정보», 여는 창은 같음). targetNoun · notice 는 호출부 호환용으로
  남기고 화면에는 안 쓴다. 대화 칸 빈 안내 · 입력칸 · 중지/새 대화 · ETL 관리(ConsoleFramePage 자체 패널) · 로그 실패 창 GPT 분석은 그대로.
- 확인(qa-playwright, 배포본): 워크플로우 노드 · 워크플로우 전체 · CDC 상세 세 곳 윗줄이 «gemma4 | 전송 정보», 버튼으로 «GPT 에 보내는 정보» 창 열림,
  tsc · oxlint 0, 페이지 오류 0, 질문 전송 안 함.

## 49. 워크플로우 로그 화면 - 현황 접기 · 가운데 실행 목록(로그2 방식) · 속성창 상세정보/AI 어시스턴트 탭 (2026-09-18)

**요청**: 로그(`/airflow/dashboard`, AirflowDashboardPage)를 로그2 를 따라 바꾼다. 윗줄 현황은 접기/펼치기(기본 접힘). 가운데는 로그2 실행 목록처럼 -
실행 목록/타임라인 전환 · 상태 칩 · 워크플로우 검색은 빼고 기간 필터만. 가운데 아래는 «실행 이력»만, 기간 필터 없음. 오른쪽 속성창은 그대로 두고
워크플로우 설계처럼 «상세정보 | AI 어시스턴트» 탭(선택한 워크플로우 질의응답).

- 현황: `airflow-business-status` 를 로그2 커튼(`lv2-curtain`)으로 감쌈. 접힌 줄에 CDC(실행 중 · 실패 · 대기) │ ETL(실행 중 · 성공 · 실패) 요약,
  «현황 펼치기». 카드 · 지표 클릭(범위 바꾸기)은 그대로. 로그2 자체 커튼(기본 펼침)은 손대지 않음.
- 가운데: ETL 범위(트리의 ETL 워크플로우 · ETL 지표)일 때만 새 `pages/workflowLog/EtlRunsPane.tsx` - 로그2 의 `LogV2Grid`(GridHead · RunRow) ·
  `logV2Data` 를 그대로 써서 실행 → job/하위 호출 → 단계 펼침, 기간(오늘 · 어제 · 최근 7일 · 최근 30일 · 기간 지정). 하위 워크플로우 실행은
  상위 실행 아래에 들어간다(`manual__{부모 runId}__{노드}` 규칙). 아래 «실행 이력»은 고른 워크플로우(하위 호출 행을 고르면 그 하위 워크플로우)의
  대시보드 실행 목록 - 기간 필터 없음, 접기 · 높이 끌기. CDC 범위(파이프라인 표) · 행 동작 버튼(이 작업만 실행 등)은 기존 그대로.
- 속성창: `airflow-property-shell` 에 탭. 상세정보 = 기존 PropertyPanel(기본정보 · 실행 설정/실행 이력 · 알림 규칙 · 바로가기) 그대로.
  AI 어시스턴트 = GptChatPanel - CDC 파이프라인(고른 파이프라인 또는 `kafka_pipeline_{id}_control` DAG) → CDC_PIPELINE, 워크플로우 DAG →
  WORKFLOW(§47 수집기). 둘 다 아니거나 GPT 꺼져 있으면 탭 없음.
- 로그2 공용: CDC 지시 노드(`*.send`)를 JobKind «cdc»로 - 유형 «CDC 지시», 결과 «지시 보냄»(CDC 워크플로우 노드 세션 요청).
- 확인: tsc · oxlint(기존 canRunTop 경고 1건 외 0), 배포 후 qa-playwright - 커튼 기본 접힘/펼침, 트리 total → 실행 목록(칩 · 검색 · 타임라인 0,
  최근 7일 실행 6 · 하위 호출 2), 실행 이력 total 17건, 하위 호출 01 dz_com_daily 선택 → 속성창 · 실행 이력이 dz_com_daily 로, AI 탭 «gemma4 | 전송 정보»
  미리보기(워크플로우 dz_com_daily 1,143자), 전체 파이프라인 → 파이프라인 표 유지 + AI 탭, ETL «성공» 지표 → 실행 목록. 페이지 오류 0,
  쓰기 요청은 화면 진입 DAG 동기화 · GPT 미리보기뿐(질문 전송 · 실행 · 저장 안 함).
  - QA 중 고친 것: 하위 호출 행을 골라도 속성창이 상위(total)에 머물던 것, 첫 진입 때 자동 선택된 CDC 제어 DAG 에 AI 탭이 없던 것.
  - CDC 워크플로우 재확인(2026-09-18 16:00, CDC 워크플로우 노드 세션이 만든 검증용 «검증 로그 CDC» id 47 · etl_wf_cdc_6, stop 1회 실행분 - 이쪽은 실행 · 저장 안 함):
    가운데 칩 «CDC»(아래 수정), 실행 1건 → «01 cdc_a · CDC 지시 · 성공 · 지시 보냄», «02 cdc_b» 동일, 실행 이력 1건. 속성창 업무 구분 «CDC 워크플로우» ·
    CDC 파이프라인(2) 실행 상태 중지 · 파이프라인별 «실행 대기 · Source/Sink STOPPED» · 바로가기 «CDC 파이프라인». 실행 설정 선택지 시작 · 중지 · 재시작 · 배포.
    AI 탭 «gemma4 | 전송 정보», 미리보기 283자(실행 방식: CDC 워크플로우 … · 노드 구성 AAA-tb_imp018m · AAA-tb_imp060l). 페이지 오류 0, 쓰기 요청은 동기화 · 미리보기뿐.
  - 추가 수정: EtlRunsPane `cdcWorkflow` - 범위가 CDC 워크플로우 하나면 머리줄 칩 «CDC»(속성창 표기와 맞춤). 실행 설정 창 동작 버튼(`airflow-execution-actions`)이
    좁은 칸에서 «재시작 (Restart)»가 두 줄로 칸 밖에 넘치던 것 - 칸 높이가 글자를 따라가게(CSS만). CDC 워크플로우 4칸 · CDC 파이프라인 5칸 · ETL 2칸 모두 넘침 없음 확인.

## 50. 로그 화면 가운데 실행 / 아래 태스크 분리 · 옆 패널 접기와 너비 조절 · 생성 화면 AI 어시스턴트(WORKFLOW_DRAFT) (2026-09-18)

**요청**
1. 로그 화면: 가운데 위에서 워크플로우를 눌러 그 아래에 태스크가 펼쳐지던 것을 없앤다. 가운데 위는 워크플로우 실행 목록만, 고르면 아래 «실행 이력»에 그 실행의 태스크만 순서대로(워크플로우 줄 없이).
2. 워크플로우 설계 · 로그 화면의 왼쪽 트리 · 오른쪽 속성창을 ETL 관리처럼 옆으로 접고 펴기 + 너비 조절.
3. 워크플로우 생성 화면 속성창에도 AI 어시스턴트 탭 - 보낼 내용 · 에이전트는 분석해서 정한다.

- 1 (`workflowLog/EtlRunsPane`, `LogV2Grid`): RunRow 에 `expandable`(기본 true - 로그2 · 상세 창은 그대로). 로그 화면은 false 로 ▶ 없이 실행 줄만.
  아래 «실행 이력» = 고른 실행의 `RunBody`(job → 단계, 하위 호출 펼치면 하위 job) - 머리줄은 «워크플로우 이름 · MM-DD HH:mm:ss 실행».
  예전 «그 워크플로우의 최근 실행 전부» 목록은 없앴다. 다른 실행을 고르면 아래 펼침 · 선택을 비운다. 속성창은 고른 실행의 워크플로우.
- 2 (`components/SideDock.tsx` + `useSideDock.ts`): ETL 관리(ConsoleFramePage)의 `.console-curtain-toggle` · `.console-resize-handle` 을 그대로 쓰는
  칸 틀. 격자 열 폭을 상태로(접히면 30px), 너비는 최소 ~ min(최대, 화면 45%). 기본 폭은 예전 열과 같다(설계 260/300, 로그 230/320 · 노트북 220/280).
  **접혀도 패널을 지우지 않고 CSS 로 숨긴다** - 속성 입력 · AI 대화가 남아야 한다(확인: AI 입력칸 글자 유지). 설계 패널의 `flex:none` 을 이기도록
  `.side-dock.side-dock > :not(button)` 로 칸 폭을 채움. 버튼은 칸 사이 틈으로 조금 더 내어(-18px) 트리 새로고침 · 속성 제목을 안 가림.
- 3 설계 판단:
  - **대상**: 새 `WORKFLOW_DRAFT`(NIFI 조회 권한). 생성 중인 워크플로우는 서버에 없어 기존 수집기(저장된 설계 기준)로는 모을 게 없다.
    화면이 저장 본문과 같은 재료(속성 입력 · 노드 · 연결, 좌표 제외)를 `LlmChatRequest.draft`(JSON, 최대 100,000자)로 보내고,
    job 이름 · 단계 · 하위 워크플로우 구성 · CDC 파이프라인 이름은 서버가 id 로 찾는다 - 보낼 내용은 서버가 정한다는 원칙 유지, 마스킹 · 길이 제한 동일.
    질문 · 전송 정보 볼 때마다 그 순간의 화면 설계를 만든다(`draft` 는 함수로 넘김).
  - **수집기** `WorkflowDraftContextCollector`: 작성 중 속성(이름 비어 있음 · 같은 이름 있음 표시, 스케줄 · 실행 설정 · 메모, CDC 전용이면 실행 방식) ·
    노드 구성(선행 · 연결 조건 · 실행 조건 · 재시도) · 구조 요약(시작 · 마지막 · 연결 안 된 노드) · **검증 결과**(게시 검증 `WorkflowValidator` 를
    저장 안 한 엔티티에 그대로 - 조회만 함, 실패하면 구역만 뺌) · job 단계 · 하위 워크플로우 구성. 노드 300 · 연결 1,000 개 상한.
  - **에이전트**: 따로 만들지 않고 속성창 자유 질문 `JOB_ANALYSIS`(긴 대화 요약 흐름 포함). 대상만 다르므로 `JobAnalysisAgent.systemPrompt(type)` 가
    초안일 때 «저장된 설계» 규칙을 «작성 중 설계» 로 바꾸고 «저장 · 게시 전 설계 검토» 지침(검증 오류 → 실행 순서 → 실행 조건 → 스케줄 · 실행 설정 →
    job 단계 겹침)을 붙인다. 입력 길이 계산도 같은 지시문으로.
  - 화면: 초안에서도 노드 · 연결선을 안 골랐으면 AI 탭. AI 탭을 보는 동안 WorkflowSettings 를 지우지 않고 숨긴다 - 지웠다 다시 그리면 초안에 적던
    설명 · 스케줄이 빈 값으로 돌아가고 onDraftChange 가 그 빈 값을 덮어써서 저장 때 사라진다(저장된 워크플로우도 같은 방식이 됨).
- 확인: 백엔드 llm 테스트 11개 클래스 통과(새 `WorkflowDraftContextCollectorTest` 3 · `JobAnalysisAgentTest` 초안 1 포함), 프런트 tsc · oxlint 0(기존 canRunTop 경고 1 외).
  pipeline-api · cerebroetl-ui 배포 후 qa-playwright:
  로그 total 최근 7일 - 위 실행 6줄(job · 호출 줄 0, ▶ 0), 아래 «total 09-17 02:15:45 실행» 태스크 3줄(실행 줄 0), 두 번째 실행 고르면 아래 교체, 하위 호출 펼침 6줄.
  옆 패널 - 로그 230,320 → 끌기 350,470 → 접기 30,30 → 펴기 350,470(패널 내용도 같은 폭), 설계 260,300 → 380,450 → 30,30 → 380,450, 캔버스 노드 유지.
  생성 화면 - 이름 입력 + 팔레트에서 두 개 끌어 놓기 → AI 탭 «gemma4 | 전송 정보», 미리보기 832자(작성 중 속성 · 노드 구성 · 구조 요약 · 검증 결과 ·
  하위 구성), 상세정보로 돌아와도 이름 유지. 페이지 오류 0, 쓰기 요청은 동기화 · 미리보기뿐(저장 · 실행 · 질문 전송 안 함).
  - CDC 재확인(16:42, CDC 워크플로우 노드 세션의 검증용 «검증 로그 CDC» id 48 · etl_wf_cdc_7, stop 1회분 - 이쪽은 실행 · 저장 안 함):
    로그 - 칩 «CDC», 가운데 실행 1줄(job 줄 · ▶ 0), 아래 «검증 로그 CDC 09-17 16:41:49 실행»에 «01 cdc_a · CDC 지시 · 성공 · 지시 보냄», «02 cdc_b» 동일,
    속성창 «CDC 워크플로우» · CDC 파이프라인(2). 생성 화면 - 팔레트 CDC 두 개 끌어 놓고 AI 전송 정보 358자: 실행 방식 CDC 워크플로우 · 노드
    AAA-tb_imp060l · AAA-tb_imp018m (CDC 파이프라인) · 검증 오류 없음. 페이지 오류 0. 초안 CDC 노드 단위 테스트(`cdcDraftNodes_areNamedByPipeline_notRejected`) 추가 · 통과.


## 51. 발송 채널을 화면에서 설정 + 카카오 알림톡 채널 추가 (2026-09-20)

**요청**: ① 로그 화면(ETL 제외)의 «GPT 분석» → «GPT 해석» ② 발송 채널에 카카오톡 추가.
어느 망(폐쇄망 · 사내망 · 외부망)에 들어갈지 정해지지 않아 **화면에서 설정하면 그곳 설비로 나가야** 한다.
다음 주 시연에서 알림톡 실발송을 보여야 한다. 설계 `docs/2026-09-20-notification-channel-settings.md`.
사용자 결정: 알림톡 · 대행사 미정(가입 예정) · 위험 등급만 · 문자 대체발송 없음.

- **문구**: TaskFailureLogModal · CdcLogsPage · GptErrorAnalysis · llm.ts 실패 문구 → «GPT 해석». ETL 관리 화면엔 이 문구가 없어 손대지 않음. 배포 번들에서 «GPT 해석» 4 / «GPT 분석» 0 확인.
- **설정을 DB 로**(V78): `notification_channel_config.config_json` · `encrypted_secret`(V36 에 있으나 쓰지 않던 자리) + `secret_hint` 신설.
  `ChannelSettingsService` 가 **DB 값 우선 · 없으면 환경변수**로 합쳐 준다(10초 캐시, 저장 시 즉시 무효화).
  비밀값은 `PasswordCryptoService` 로 암호화, 화면에는 끝 4자리만(GPT 제공자 등록과 같은 규칙). 비워 저장하면 기존 값 유지.
- **발송기**: 메일은 화면 설정으로 `JavaMailSenderImpl` 을 만들어 보낸다(Spring 빈은 기동 시점 고정이라 화면 변경이 반영 안 됨).
  문자 · 카카오는 공통 `relayGateway` 로 합쳤다 - 규격 차이(주소 · 헤더 · Content-Type · 본문 템플릿)는 전부 설정.
  카카오는 추가로 `{senderKey}` `{templateCode}` 치환자와 **성공 판정 규칙**(`successPath`/`successValue`)을 받는다 -
  대행사는 HTTP 200 에 실패를 실어 보내는 일이 흔하다. 실패 코드 `KAKAO_ERROR`.
- **대상 · 구독**: 알림톡도 휴대폰 번호로 보내므로 `notification_recipient.phone` 재사용(새 컬럼 없음).
  수신자 생성 시 전화번호가 있으면 SMS 와 함께 KAKAO 기본 구독, 구독 토글 · 테스트 발송에 KAKAO 분기.
  등급 정책은 메일 · 문자와 동일(CRITICAL 만).
- **화면**: 발송 채널 탭에 [설정] 버튼 → 채널별 입력 창(메일 6항목 / 문자 5항목 / 카카오 9항목 + 비밀값), 창 안에서 [테스트 발송].
  수신자 탭에 «카카오톡 수신» 토글(전화번호 없으면 비활성). `api/config.ts` 에 채널 설정 API 3종.
- **설정 파일**: `application.yml` `notification.kakao.*`, `.env.example` 카카오 섹션(화면이 기본이고 .env 는 폴백임을 명시).

**검증**
- 단위 `NotificationGatewayTest` 4건: 화면 설정대로 URL · 헤더 · 본문이 나가는지(비밀값 치환 포함), 200 인데 본문이 실패면 RETRY(KAKAO_ERROR),
  성공 규칙 없으면 200 만으로 성공, 주소 없으면 NO_RELAY. 백엔드 전체 275건 중 실패 1건은 기존 환경 문제(ConnectionRepositoryIT).
- 로컬 E2E: 화면과 같은 API 로 카카오 설정 저장(대행사 자리에 sms-echo) → 채널 켜기 → 검증용 수신자 생성(전화번호) → 테스트 발송 →
  **SENT**, 목 서버가 받은 본문에 senderKey · templateCode · to · title · text, 헤더 `Authorization: Bearer DEMO-API-KEY-7788` 확인.
- 화면: 발송 채널 탭에 «카카오톡(알림톡)» 행, [설정] 창에 9항목 + 비밀값(저장됨 ****7788 표시), 페이지 오류 0.
- 정리: 검증용 수신자 · 테스트 발송 이력 삭제, 카카오 채널은 다시 끄고 설정 · 비밀값 비움(대행사 값이 들어올 자리).

**수정한 기존 코드 한 줄**: 재시도 백오프 SQL 을 `interval '2 minutes'` → 표준 `INTERVAL '2' MINUTE` 로.
Postgres 전용 표기는 단위 테스트의 H2 에서 조용히 실패해 «재시도로 안 바뀜»이라는 가짜 실패를 만든다. 두 표기 결과가 Postgres 에서 같음을 확인했다.

**남은 것(사용자 진행)**: 대행사 가입 · 카카오톡 채널 비즈니스 인증 · 알림톡 템플릿 등록/승인(영업일 1~3일).
준비되면 화면에서 값만 넣으면 실발송으로 전환된다(재배포 없음).
**주의**: 아웃박스에 이중 발송 방지(`FOR UPDATE SKIP LOCKED`)가 없다 - 알림톡은 건당 과금이라 운영에서 서버 2대로 띄우기 전에 반드시 처리해야 한다.


## 52. 대시보드 실패 상세를 로그 화면과 같은 형식으로 + 각각 GPT 해석 (2026-09-21)

**요청**: 대시보드 CDC · ETL · Airflow 카드의 실패 버튼을 누르면, 각 로그 화면에서 실패 줄을 펼쳤을 때와
**같은 형식**으로 보이게 하고 GPT 해석도 각각 붙일 것.

**예전 동작**: 세 카드 모두 공용 `HistoryModal` 로 열려 «오류 원문 한 덩어리»만 보였다. 어느 파이프라인 · 어느 job 의
오류인지 알려면 로그 화면에서 다시 찾아야 했고 GPT 해석도 없었다.

- **새 공용 창** `components/FailureDetailModal`: 줄을 펼치면 로그 화면과 같은 차례 - 항목(이름 / 값) → 오류 원문 → [GPT 해석].
  GPT 대상은 부르는 쪽이 정한다(수집기가 없는 대상이면 버튼을 그리지 않는다).
- **CDC 카드(장애 N건)**: 파이프라인 · 커넥터 · 상태(실패 태스크 수) · 소스→타깃 · Topic + 커넥터 trace. 커넥터 이름
  (`source-16-…`)의 두 번째 조각으로 파이프라인을 찾아 이름을 적고 GPT 대상(CDC_PIPELINE)으로 쓴다.
- **ETL 카드(오류 그룹 N건)**: 그룹 · job · 프로세서 + 오류 원문, GPT 대상 ETL_JOB(jobId, 현재 오류 줄은 nifiPgId).
- **Airflow 카드(실패 N건 보기)**: 목록 줄마다 [로그] - 누르면 **워크플로우 로그 화면과 같은 창**(`TaskFailureLogModal`)이 뜬다.
  시도별 로그 · Airflow 기록 · 하위 워크플로우 추적 · GPT 해석(WORKFLOW_TASK)이 그 안에 그대로 있다.
  `HistoryModal` 에 선택적 `rowAction` 을 더해 실패 타일에서만 버튼을 그린다.
- **ETL GPT 수집기 신설**(`EtlJobContextCollector`): 지금까지 ETL_JOB 은 수집기가 없어 «아직 지원하지 않는 분석 대상»으로
  거절됐다(2026-09-17 확인). job 기본 정보 + 단계(SQL · 대상 테이블) + 최근 실행을 모은다 - 단계 · 실행 이력은 워크플로우 task
  수집기가 쓰는 `EtlJobContextText` 를 그대로 써서 어디서 묻든 같은 내용이 가게 했다. 연결정보는 보내지 않는다.

**검증**(로컬, 검증 후 전부 삭제)
- ETL: 대시보드 «오류 그룹 3건» → 줄 펼침 → 대상 · 프로세스 그룹 · 오류 원문 · [GPT 해석] 확인.
- CDC: target-db 테스트 파이프라인 16 을 만들어 타깃에 CHECK 제약으로 싱크 태스크 FAILED 유도 → «장애 1건» →
  파이프라인 · 커넥터 · 상태 · 소스→타깃 · Topic + ConnectException trace · [GPT 해석] 확인.
- 워크플로우: 같은 파이프라인에 monitor 를 붙여 제어 DAG 실패 생성 → «실패 1건 보기» → [로그] →
  «e2e-dash 실패 로그» 창(실패 원인 · 실행 로그 · GPT 해석) 확인.
- ETL_JOB 수집기: `POST /api/llm/chat/preview` 로 848자 수집 확인(대상 DZ_COM001M, 단계 · 최근 실행 포함).
- 프런트 tsc 통과, 페이지 오류 0, 화면에서 저장 · 실행 등 쓰기 요청 0.
- 정리: 파이프라인 16 삭제, target-db 테스트 스키마 · 테이블 삭제(슬롯 · 커넥터 0), target-db 중지.

**고친 것**: 실패 목록 줄의 id 가 `dagId:runId` 인데 run_id 안에도 콜론이 있어(`manual__2026-09-21T06:35:27…`)
단순 split 으로는 runId 가 잘렸다 - 첫 콜론만 구분자로 쓴다.

## 53. 로그 화면 트리 - 최상단 CDC · ETL 을 누르면 그 가지 전부를 조회 (2026-09-21)

**요청**: 워크플로우 로그 화면에서 최상단 «CDC», «ETL» 폴더를 누르면 그 안의 모든 워크플로우 내역이 조회되게 할 것
(그때까지는 폴더가 접히고 펴지기만 했다).

- 목록 범위(`ListScope`)에 `{ kind: "category" }` 를 더했다. 아래 폴더들(전체 파이프라인 · 연결 · 스키마)과 같은 방식이다.
- **ETL 최상단** → 가운데가 실행 목록(`EtlRunsPane`)으로, 범위는 ETL 가지의 DAG 전부다. 하위 워크플로우도 범위 안이라
  부른 job 아래로 접히고 머리줄에 «하위 워크플로우 포함»이 붙는다.
- **CDC 최상단** → 파이프라인 표 전부(«전체 파이프라인»과 같은 표).
- 폴더 동작: 접혀 있으면 펴면서 목록을 띄우고, **이미 보고 있는 가지를 다시 누를 때만** 접는다 -
  목록을 보려다 하위가 사라지지 않게 했다. 최상단 줄에도 건수 배지를 붙였다(CDC=파이프라인 수, ETL=DAG 수).
- 가지 전체를 볼 때는 앞서 고른 파이프라인 선택을 놓는다. 고른 DAG 는 기존 효과가 곧바로 첫 DAG 를 다시 고르므로 그대로 둔다.

**검증**(로컬, 읽기만): ETL 최상단 클릭 → «ETL 전체 워크플로우 · 하위 워크플로우 포함», 기간 «최근 30일» 에서 total
실행 24줄 조회. CDC 최상단 클릭 → «CDC 전체 파이프라인» 표(파이프라인 2건). 첫 클릭은 펼침 유지, 같은 가지 두 번째
클릭에서만 접힘 확인. 프런트 tsc 통과, 페이지 오류 0.

## 54. 로그 화면 - «이름» 대신 실행 차수 · 상세 실행이력 머리줄 · 실패 로그 창에 차수 표기 (2026-09-21)

**요청**: 실행 내역의 «이름» 열을 «실행 차수»로 바꾸고 워크플로우명은 빼기(머리줄에 이미 있다),
차수 칸에는 «날짜·시·분·초 + 그날의 실행 차수»를 적기. 아래 «실행 이력»은 «상세 실행이력»으로,
오른쪽 문구도 워크플로우명 대신 차수·실행시각으로. 실패 로그 창 상단에는 어느 차수의 로그인지 적기.

- **실행 차수 계산**(`EtlRunsPane.ordinalByRun`): 같은 워크플로우 · 같은 날짜의 실행을 시각 오름차순으로 세어
  1차, 2차… 로 매긴다. 고른 기간 안의 실행만 세므로 기간을 하루 중간부터 잡으면 그 앞 실행은 세지 않는다.
- **가운데 목록**: 머리 `이름` → `실행 차수`(`GridHead` 에 `nameLabel` 을 더했다 - 로그2 화면과 아래 상세는 «이름» 그대로).
  줄은 `09-18 01:54:59 · 1차`. 단, **여러 워크플로우를 한 번에 보는 범위**(ETL 최상단 · ETL 지표)에서는 이름을 남긴다 -
  이름이 없으면 어느 워크플로우의 실행인지 구분할 수 없다.
- **아래 머리줄**: `실행 이력` → `상세 실행이력`, 뒤 문구는 `09-14 03:39:45 · 1차 실행`(워크플로우명 제거).
- **실패 로그 창**: `FailureLogTarget.runLabel` 을 더해 제목 아래 한 줄로 `total · 09-14 03:39:45 · 1차 실행` 을 적는다.
  창을 열면 목록이 가려져 «어느 차수의 로그인지»를 알 수 없던 자리다. 값은 `GridCtx.runLabel` 로 목록 쪽에서 내려 준다
  (대시보드에서 여는 같은 창은 이 값이 없으면 줄을 그리지 않는다).

**검증**(로컬, 읽기만): total · 최근 30일 - 머리 `실행 차수`, 줄 `09-13 08:46:59 · 2차` 등 하루 2회 실행에 1·2차 부여 확인.
상세 머리줄 `상세 실행이력 09-18 01:54:59 · 1차 실행`. 실패 실행(09-14)에서 [로그] → 창 상단 `total · 09-14 03:39:45 · 1차 실행` 확인.
프런트 tsc 통과, 페이지 오류 0.

## 55. GitLab msa-integration 병합(16커밋) 위에 진행 중이던 작업 얹기 (2026-09-22)

원격이 `7b59c13` → `90c1a7d` 로 16커밋(35파일) 앞서 있어, 커밋하지 않은 작업을 보관했다가 다시 얹었다.
받은 것: ETL 관리 AI 어시스턴트 · 툴바, 목록 화면 표준화(`utils/listSort.ts`, `StdPage`, 전체높이 · 표 위 검색 · 열 정렬),
NiFi 숨김, 상단 로고 회사명 제거, 백엔드 `EtlAssistantContextCollector` 신설.

- **충돌 1건** - `SettingsPage.tsx` 발송 채널 탭의 닫는 태그(원격은 `Space` → `div` 로 바꿨고, 우리는 그 자리에
  채널 설정 창을 넣었다). 원격의 틀을 두고 설정 창을 그 안으로 넣어 해소했다.
- **같은 이름 새 파일** - `EtlJobContextCollector` 를 양쪽이 각자 만들었다. 삭제 job 걸러내기 · 상위 그룹 · DAG id ·
  상태 카운트까지 붙는 **원격판을 채택**하고 우리 것을 버렸다. 대신 수집기가 받는 열쇠가 `jobId` 아니면 `groupId` 라,
  대시보드 ETL 오류가 보내던 `nifiPgId` 를 `groupId` 로 고쳤다.
- 나머지 7개 파일(AirflowDashboardPage · EtlRunsPane · DashboardPage · index.css · api/llm.ts · CdcLogsPage 등)은
  자동 병합됐다. 워크플로우 로그 가운데 띠에는 원격의 «동기화» 버튼과 우리 «실행 차수»가 함께 올라왔다.

**검증**: 프런트 tsc 통과. 백엔드 테스트 275건 중 274건 통과(1건 `ConnectionRepositoryIT` 는 빌드 컨테이너 안에서
Testcontainers 가 Docker 를 못 찾아 나는 환경 문제로, 코드와 무관하다). 백엔드 · UI 재빌드 후 화면 확인 -
워크플로우 로그(실행 차수 · 상세 실행이력 · 동기화 버튼 공존), 설정 발송 채널 3행 + 카카오 설정 창(입력 9칸),
대시보드 ETL 오류 상세 + GPT 해석. `groupId` 로 ETL 컨텍스트 960자 수집 확인(DZ_COM001M).

**남은 확인거리**: 대시보드 ETL «현재 오류» 항목은 NiFi 그룹 목록에서 오는데, 그 그룹 id 가 등록된 job 이 아니면
(`etl_job.nifi_pg_id` 에 없거나 삭제된 job) GPT 해석이 «Job을 찾을 수 없습니다» 로 떨어진다. 병합 전에도 같았다.

## 56. 워크플로우 로그 조회기간을 ETL 로그와 같은 모양으로 (2026-09-22)

**요청**: 워크플로우 로그의 조회기간 UI(오늘 · 어제 · 최근 7일 · 최근 30일 · 기간 지정 칩)를 ETL 로그 화면과 같은
«당일 · 전일 + 날짜 달력»으로 바꾸고, 기본은 당일.

- `EtlRunsPane` 의 `period`/`customRange` 두 state 를 날짜 범위 하나(`dateRange`, 기본 `[오늘, 오늘]`)로 줄였다.
- 버튼 선택 표시는 별도 state 없이 «지금 기간이 그 날 하루인가»로 판정한다 - ETL 로그가 쓰는 `activeDayPresetByDate`
  를 그대로 썼다(달력이 날짜 단위라 시각까지 비교하는 `activeDayPreset` 로는 절대 일치하지 않는다).
- 로그2 화면(`WorkflowLogV2Page`)은 그대로 둔다 - `periodRange`/`PeriodKey` 는 거기서 계속 쓴다.

**검증**: 기본이 «당일(선택)» + 2026-09-21~2026-09-21, [전일] 누르면 2026-09-20 으로 바뀌고 목록이 다시 조회됨.
프런트 tsc 통과, 페이지 오류 0.

## 57. 아웃박스 이중 발송 방지(집기) + 도커 재시작 복구 스크립트 (2026-09-27)

### 57-1. 알림 한 건을 두 대가 같이 보내던 문제

**왜 지금**: 알림톡은 **건당 과금**이고, MSA 는 앱을 두 대로 띄운다. 지금 `dispatch()` 는 후보를 읽고 곧바로
보내므로, 두 대가 같은 20건을 나란히 읽어 **둘 다 보낸다**. 통합검증 M1-1(분산락 전무)과 같은 뿌리다.

- **집기(claim)**: 후보를 읽은 뒤 `status='SENDING'`, `claimed_by`, `claim_expires_at` 으로 먼저 집고,
  **성사된 인스턴스만** 발송한다. 조건절 `WHERE id=? AND status IN ('PENDING','RETRY')` 가 자물쇠다 -
  잠금 힌트(`FOR UPDATE SKIP LOCKED`)를 쓰지 않아 **DB 종류를 가리지 않는다**(H2 테스트도 같은 경로를 탄다).
- **회수**: 집어 놓고 끝내지 못한 건(인스턴스 종료 · 크래시 · 배포 중 교체)은 `claim_expires_at`(5분)이 지나면
  재시도로 되돌린다. 없으면 그 건은 `SENDING` 인 채로 영영 남는다.
- `claimed_by` · `claim_expires_at` 컬럼과 `idx_delivery_sending` 인덱스는 **V36 이 만들어 두고 아무도 쓰지 않던
  자리**다 - 마이그레이션 없이 그대로 썼다.
- 한계(의도): 보내고 나서 `SENT` 를 찍기 전에 죽으면 회수 뒤 한 번 더 갈 수 있다(at-least-once).
  영영 안 나가는 것보다 낫다고 봤다.

**검증**
- 단위 테스트 3건 추가(총 7건 통과): 집은 인스턴스가 남고 발송 / 남이 집어 둔 건은 보내지 않음 /
  만료된 집기는 회수돼 다시 나감.
- **실제 Postgres 동시 경합**: 세션 A 가 트랜잭션을 열고 집은 채 3초 쥐고, B 가 겹쳐서 같은 건을 집으려 하면
  B 는 A 의 커밋까지 대기했다가 **0행**으로 밀린다(`UPDATE 1` / `UPDATE 0`, claimed_by=app-A).
- **옛 코드의 이중 발송을 눈으로 확인**: 위 실험 중 배포돼 있던 구버전이 A 가 집은 그 행을 그대로 보내고
  상태를 `SENT` 로 덮어썼다. 새 코드에서는 같은 시나리오에서 건드리지 않는다.
- 라이브 확인(새 빌드): 남이 집은 건 → 손대지 않음 / 끊긴 집기 → 회수 후 발송 / 새 건 → 집고 발송.

### 57-2. `scripts/docker-recover.sh`

호스트 재시작 뒤 같은 복구를 **네 번** 손으로 했다(2026-08~09, 마지막 09-27). 깨지는 모양은 둘이다.
① 단일 파일 마운트(`filebeat.yml` 등)가 사라져 컨테이너가 `exit 127` ② `airflow/dags` 디렉터리가 **빈 폴더**로
떠서 컨테이너는 healthy 인데 DAG 가 전부 stale(화면은 «DAG 준비 중»). 둘 다 `--force-recreate --no-deps` 로 낫는다.

- `--check` 진단만 / 옵션 없이 진단+복구(확인 질문) / `--yes` 무확인(재부팅 직후 자동 실행용).
- 탐지: `exited(127)` · `created` · `dead` + 컨테이너 안 DAG 파일 0개. 일부러 내려 둔 것(`exited(0)`)은 건드리지 않는다.
- **하지 않는 것**: 멈춘 배치 · 커넥터를 켜지 않고, 일시 중지된 DAG 도 풀지 않는다(밀린 회차가 돈다).
  볼륨은 손대지 않는다.
- 검증: 정상 상태에서 «복구할 것이 없다» 확인 → `filebeat.yml` 을 잠시 옮겨 깨뜨린 뒤(컨테이너가 `created` 로 멈춤)
  스크립트가 탐지·재생성해 healthy 로 되돌리는 것까지 확인. 이때 도커가 만든 빈 디렉터리를 치우고 파일을 원복했다.

## 58. CDC 상세의 «이력» 탭을 제어 이력으로 쓸 수 있게 (2026-09-27)

**요청**: 건너뛴 레코드(제어) 이력을 CDC 관리 화면 상세에서 볼 수 있게 할 것.

**확인**: 탭은 **이미 있었다**(상세 → 이력). 다만 이력을 보는 뜻을 못 살리고 있었다.
- **요청자가 DTO 에서 빠져 있었다** - 컬럼(`requested_by`)은 있는데 화면까지 오지 않았다.
  건너뛰기처럼 데이터를 버리는 조치는 «누가 했는가»가 감사의 핵심인데 그게 없었다.
- 30초 주기 자동 감시(`RUNTIME_MONITOR`)가 2,778건이라 사람이 한 제어가 묻혔다. 게다가 **전부 내려받았다**(제한 없음).
- 명령이 코드 그대로(`PREPARE`·`SINK_CONFIG_REFRESH`), 시각은 ISO 원문, 메시지 열은 폭에 눌려 «메.» 로 잘렸다.

**고친 것**
- `PipelineCommandHistoryResponse` 에 `requestedBy` 추가.
- `GET /pipelines/{id}/history?includeAuto=&limit=` - 기본은 **사람이 한 제어만 최근 200건**.
  자동 감시는 화면 체크박스로 켠다(`findRecent` 쿼리 신설, 상한 1000).
- 탭 이름 «이력» → «제어 이력». 열: 명령(한글) · 결과 · 요청자 · 메시지 · 요청 시각(초까지).
  줄을 펼치면 메시지 전문. 건너뛰기는 빨간 태그 «건너뛰기(손실)» + 행 배경 강조(`.cdc-history-skip`).

**검증**: 화면에서 탭 5개(기본정보 · 운영 상태 · Connector · 정합성 · 제어 이력) 확인, 기본 4건 /
`includeAuto=true` 23건(API 실측). 시험용 `SKIP_RECORD` 행을 넣어 빨간 태그 · 요청자(cktnqhd15) ·
펼친 메시지 전문까지 확인하고 삭제했다. 프런트 tsc 통과, 백엔드 278건 중 277건 통과
(`ConnectionRepositoryIT` 는 빌드 컨테이너에 Docker 가 없어 나는 환경 문제).

**시간대 확인**(이번에 짚음): metadata-db 의 Postgres `timezone` 설정은 **UTC** 다(컨테이너 TZ 는 KST).
`requested_at` 은 naive 컬럼이라 DB 기본값 `now()` 로 들어가면 UTC 가 된다. 다만 앱은
`PipelineCommandHistoryRecorder` 가 `LocalDateTime.now()`(JVM=KST)로 **직접 넣으므로** 저장값은 KST 이고,
화면에 그대로 보여 주는 것이 맞다. psql 로 직접 넣은 시험 행만 9시간 어긋나 보였던 것이다.

## 59. 워크플로우 로그 속성창에서 일시 중지 해제 (2026-09-27)

**요청**: 속성창 «상태»가 일시 중지면 그 오른쪽에 [해제] 버튼을 두고, 누르면 풀리게 할 것.

**왜 필요했나**: 화면은 일시 중지 상태를 **보여 주기만** 했다. 푸는 방법은 Airflow 웹(`/airflow`)이나
CLI 뿐이었고, 화면의 «실행 설정 → 시작»은 수동 Run 을 만드는 것이라 **일시 중지 상태에서는 queued 에 갇힌다**
(실측: 일시 중지된 DAG 에 트리거 → `queued`, start_date 없음 → 해제하자 그 Run 이 곧바로 실행·성공).

- 백엔드: `AirflowDagRunClient.setPaused(dagId, paused)`(Airflow `PATCH /dags/{id}`),
  `POST /api/airflow/dag-catalog/{dagId}/paused`. Airflow 에 직접 쏘지 않고 pipeline-api 를 경유해 권한(AIRFLOW WRITE)을 태운다.
- 화면: 상태가 «일시 중지»이고 쓰기 권한이 있을 때만 [해제]. 누르면 확인창에
  «해제하면 **밀린 마지막 회차가 곧바로 한 번** 실행됩니다 (스케줄 …). 그 뒤로는 정상 스케줄로 돕니다.»
  를 띄운다 - catchup 을 꺼 두어도 Airflow 가 빠진 회차를 재개 직후 만든다(2026-09-11 · 09-16 실측).
  성공하면 대시보드를 다시 받아 상태 · 다음 실행이 바로 반영된다.

**검증**: API 로 `test_hello` 를 해제(200 → is_paused=f). 화면에서 total(일시 중지)을 고르면
«상태 | 일시 중지 | 해제» 가 보이고 확인창 문구까지 확인했다. **실제 해제는 누르지 않았다** -
root_total 은 해제하면 실제 적재가 돌아 사용자 결정 사항이다. 프런트 tsc · 백엔드 컴파일 통과.

**일시 중지 경위 조사(확증 실패)**: Airflow 감사 로그(`log`)에 root_total 의 pause 이벤트가 **없다**.
`cli_dag_pause` 는 2건뿐이고 오늘 내 실험(test_hello)과 8/30 다른 DAG 다. Airflow 3 은 UI · REST 로 한
일시 중지를 `log` 에 남기지 않아 누가 · 언제 했는지 확증할 수 없다. 정황은 마지막 성공 실행이 9/18 10:54 이고
9/19 부터 실행 0건, 9/20~21 도커 복구 때 «배치는 돌리지 말고 서비스만 올려라»는 요청이 있었다는 점이다 -
복구 시 밀린 회차가 도는 것을 막으려 누른 것으로 보이나 **WORK_LOG 에 기록이 없다**. 앞으로 같은 조치는 기록할 것.

## 60. AI 어시스턴트 탭을 열면 대상 요약부터 내어 준다 (2026-09-27)

**요청**: 로그 화면의 [GPT 해석] 이 누르자마자 답을 주듯, 속성창 «AI 어시스턴트» 탭도 열면 그 워크플로우 · CDC
파이프라인의 요약이 먼저 나오게 할 것. 대상은 **CDC 관리 속성창 · 워크플로우 캔버스 속성창 · 워크플로우 로그 속성창** 셋.

**방식**: 새 에이전트를 만들지 않았다. 속성창 질문(JOB_ANALYSIS)에서 **질문 없이 온 첫 요청**을 «탭을 연 것»으로 보고
서버가 가진 요약 요청(`JobAnalysisAgent.SUMMARY_REQUEST`)으로 바꿔 답한다 - 질문 문구를 화면이 들고 있으면
화면마다 답 모양이 달라진다. 형식은 고정하되 **없는 항목은 «확인되지 않음»** 으로 적게 해 지어내지 않게 했다
(한 줄 요약 / 구성 / 지금 상태 / 등록 · 변경 / 눈여겨볼 점).

**요약이 말할 수 있도록 수집기를 보강**했다. 없던 정보는 답할 수 없으므로 이쪽이 실제 작업의 대부분이다.
- 워크플로우: 등록 시각 · 최종 수정 · Airflow DAG id, **최근 실행 5회**(Airflow 에서 직접 - 상태 · 시작 · 종료).
  게시 전이거나 Airflow 가 답하지 않으면 그 사실을 문구로 돌린다(요약 전체가 실패하지 않게).
  작성자 컬럼은 없어서 게시자가 사람 정보의 전부다.
- CDC: 등록자 · 등록 시각 · 최종 수정, **배포된 커넥터**(역할 · 이름 · 저장 상태 · 배포 시각),
  **오프셋 · 지연**(파티션 수 · 끝 오프셋 · 커밋 오프셋 · lag · 해소 예상 · 처리량 · 마지막 처리 시각).
  `RealtimePipelineMetricService.getMetric(id)` 단건 조회를 새로 열었다(기존엔 전체 목록만 있었다).

**검증**(로컬, 읽기만)
- 수집 내용: CDC 1,925자 · 워크플로우 966자(미리보기 API). 요청하신 항목이 모두 들어간 것을 눈으로 확인.
- 세 화면에서 탭을 열자 실제로 요약이 스스로 나왔다.
  워크플로우는 «최신 3회 성공, 그 이전 2회(9/13~14) 실패»까지 짚었고, CDC 는 소스→타깃 · Topic · 커넥터 이름 ·
  lag 0 · 마지막 처리 시각을 적었다. 등록자가 빈 옛 데이터는 «확인되지 않음» 으로 나왔다.
- 프런트 tsc 통과, 백엔드 278건 중 277건 통과(`ConnectionRepositoryIT` 는 빌드 컨테이너에 Docker 가 없어 나는 환경 문제).

## 61. CDC 로그 [+] 를 펼침이 아니라 «상세 창»으로 + 실패 로그 창 머리줄 (2026-09-27)

### 61-1. CDC 로그 상세를 팝업으로

**요청**: [+] 를 누르면 표 아래에 펼쳐지던 상세를, 워크플로우 로그의 실패 로그 창처럼 **팝업**으로 열 것.
로그 정보가 나오고 **GPT 해석도 바로** 시작되게. GPT 설정이 없으면 로그 정보만.

- 표는 더 이상 줄을 펴지 않는다 - `expandedRowKeys` 를 늘 빈 배열로 두고 `expandIcon` 만 우리가 그려,
  [+] 를 누르면 `CdcLogDetailModal` 이 열린다. 볼 것이 없는 줄은 아이콘 자리만 비워 둔다(표가 들쭉날쭉해 보이지 않게).
- 창 안: 구분 · 시각 · 소스→타깃 · Topic · 커넥터 상태 · Lag · 일 누적 · 요청자 + 메시지 원문.
  **실패 줄이면 GPT 해석이 창을 열 때 바로 시작**한다 - 실패 줄을 여는 이유가 «왜 실패했나»라서 한 번 더 누를 까닭이 없다.
- **삭제된 파이프라인은 해석하지 않는다**: 수집기가 대상을 못 찾아 500 이 떴다(실측: pipeline 16 삭제분).
  화면이 파이프라인 목록에 그 id 가 있는지 보고, 없으면 «삭제돼 해석할 수 없습니다»를 적는다.

### 61-2. 실패 로그 창 머리줄

**요청**: 창 맨 위에 «total · 09-14 12:39:45 · 1차 실행»이 보이게 하고, 본문에 있던 같은 문구 줄은 지울 것.

제목을 `{차수} 실행 · 실패 로그` 로 바꾸고 본문의 회색 줄(`.failure-log-run`)과 그 CSS 를 지웠다.
아래에 이미 «{task} · run» 이 있어 제목에 task 이름을 또 쓰면 같은 말이 두 번이었다.
차수 정보가 없이 열린 경우(대시보드 실패 카드)는 예전처럼 task 이름을 제목에 쓴다.

**검증**(로컬, 읽기만)
- CDC 로그: 표 20줄 · [+] 20개 · **인라인 펼침 0**. ① 삭제된 파이프라인 실패 줄 → 로그 + 안내(해석 호출 없음)
  ② 살아 있는 파이프라인 실패 줄(AAA-tb_imp060l, ORA-12541) → 창이 열리고 **GPT 해석이 스스로 돌아** 원인 추정 ·
  확인할 것까지 출력 ③ 성공 줄 → 로그 정보만.
- 워크플로우 로그: 9/14 실패 실행의 [로그] → 제목 «total · 09-14 03:39:45 · 1차 실행 · 실패 로그»(브라우저 TZ 가 UTC 라
  03:39 로 보인다 - 화면은 KST 로 12:39), 본문은 «wf_dz_com_daily · run» 부터 시작.
- 프런트 tsc 통과, 페이지 오류 0.

**곁다리로 본 것**: `GET /api/cdc/logs/events` 를 34일(8/24~9/27) 로 부르면 **0건**이 온다(10일 범위는 정상).
`validateRange` 의 최대 기간 처리로 보인다 - 화면 기본이 당일/전일이라 실사용 영향은 없어 손대지 않았다.

**머리줄 다듬기(같은 날 추가 요청)**
- 실패 로그 창 제목에서 «· 실패 로그»를 뺐다 → «total · 09-14 12:39:45 · 1차 실행». 창을 연 맥락이 이미 실패 줄이라
  «실패 로그»는 덧말이었다. 차수 정보 없이 열린 경우(대시보드)는 예전 제목(«{task} 실패 로그») 그대로다.
- CDC 상세 창 제목에서 구분을 뺐다 → «AAA-tb_imp060l». 본문 «구분» 줄에 «런타임 감시 · FAILED» 가 이미 있다.
- CDC 상세 창에서 «요청자» 줄을 뺐다. 그에 맞춰 [+] 를 그리는 조건에서도 «요청자만 있는 줄»을 뺐다 -
  창에서 뺐으니 그 줄은 더 보여 줄 것이 없다.

## 62. CDC 제어 DAG 정리 검토 + 워크플로우 실행이 «성공»인데 CDC 가 안 돈 건 (2026-09-27)

**요청**: ① CDC 를 워크플로우로 관리하니 파이프라인별 제어 DAG 는 이제 필요 없지 않나 ② 그러면 현황도
CDC/ETL 로 나누지 말고 워크플로우 단위로만 두면 되지 않나 ③ 방금 `cdc_imp` 를 돌렸는데 태스크는 성공인데
CDC 가 실행 중이 아니다 ④ 태스크 로그에 무관한 `dz_com_daily` 문구가 섞여 나온다.

검토 결과는 [docs/2026-09-27-cdc-control-dag-retirement.md](docs/2026-09-27-cdc-control-dag-retirement.md) 로 남겼다. 요지만:

- **2026-09-10 설계서에서 이 물음은 다뤄지지 않았다.** 제어 DAG 는 검토 대상이 아니라 출발점이었고
  (§2 «있는 것을 활용»), §5 는 «파이프라인 개별 제어는 그대로», §6 은 «지금 구조를 그대로 살린다»였다.
  사용자의 애초 의도(정리)와 어긋난 채 구현이 진행된 지점이다.
- **걷어낼 수 있다.** 감시(`monitor_cdc_runtime`)의 실체는 이미 백엔드(`KafkaPipelineStateSynchronizer`)에 있고,
  센서가 하는 일은 «start Run 을 살려 둬 화면에 실행 중으로 보이게」 하는 표시 기능이다. 나머지(호출 · skip ·
  verify · 스냅샷)는 백엔드/노드로 옮길 자리가 분명하다. 3~3.5일.
- **현황 카드는 제어 DAG 를 걷으면 자연히 하나가 된다.** 다만 «CDC 가 지금 도는가»는 워크플로우 실행 상태로는
  알 수 없다(지시만 보내고 끝나 항상 성공) - 그 표시를 어디에 둘지는 결정 사항.

**③ 실측 원인**(2026-09-27 23:37): 워크플로우 태스크는 «제어 DAG 를 불렀다»까지만 책임진다(방안 B).
그 아래 `kafka_pipeline_2/3_control` 은 **둘 다 실패**했다 - `apply_action` 이 `POST /api/pipelines/{id}/start`
에서 502(`KAFKA_CONNECT_ERROR`). 백엔드가 시작을 거부하고 이전 상태로 되돌린 것이고, 거부 사유는 소스 커넥터
태스크 FAILED 였다. 커넥터를 잠깐 깨워 trace 를 보니:

```
io.debezium.DebeziumException: … starting at OracleOffsetContext [scn=31626389, …],
but this is no longer available on the server. Reconfigure the connector to use a snapshot mode when needed.
```

마지막 처리가 8-24 인데 그 뒤 한 달 넘게 멈춰 있는 사이 **원천 Oracle 의 redo/archive 가 그 SCN 을 지나갔다.**
이어 읽기가 불가능해 스냅샷 재수행이나 오프셋 리셋이 필요하다(전환 작업과 별개). 확인 뒤 커넥터 · 파이프라인
상태는 원래대로(STOPPED · READY) 되돌렸다.

**④ 로그 잡음 - 조치 완료**: 그 문구는 `etl_workflows_dynamic.py:365` 의 **모듈 수준 print** 였다. 워커가
태스크를 돌리려 DagBag 을 채우면 파일 전체가 파싱되어 **모든 워크플로우의 파싱 메시지가 그 태스크 로그에 섞였다.**

두 팩토리(`etl_workflows_dynamic.py` · `kafka_pipelines_dynamic.py`)의 파싱 메시지를 모듈 로거로 옮겼다.
정상 동작 안내(상위가 있어 스케줄을 끈다)는 `debug`, 오류(spec 파싱 실패 · index 파싱 실패 · DAG 생성 실패)는
`warning` 이다 - 오류까지 감추면 파싱이 조용히 깨진 것을 못 본다. 태스크 안에서 찍는 print(노드 통과 ·
워크플로우 완료 · 액션 결과)는 그 태스크 자신의 로그라 그대로 뒀다.

**검증**: 파싱 오류 0건 · 정상 DAG 8개 유지. `cdc_imp` 를 **stop**(이미 멈춘 상태라 건너뛰는 안전한 액션)으로
한 번 돌려 같은 태스크 로그를 다시 보니 **남의 워크플로우 문구 0건**, 로그가 9줄로 줄었다.
워크플로우 · 제어 DAG 3개 모두 success, 파이프라인 상태는 READY 그대로(건드리지 않음).

**결정 사항 3가지**(CDC 단독 운용 허용 여부 · 가동 상태를 둘 자리 · 멈춘 파이프라인 복구 방법)는 각각의 안과
권고를 위 문서 5절에 정리했다.

## 63. GPT 해석 답변 형식 - «확신도» 제거 · 제목 문구 정리 (2026-09-28)

**요청**: GPT 해석 답변의 «확신도» 구역을 없애고, «원인 추정» → «원인», «확인할 것» → «확인사항» 으로.
워크플로우 · CDC · 대시보드 로그 모두 같게.

- 형식은 서버 한 곳(`ErrorAnalysisAgent.SYSTEM_PROMPT`)에서 정하고 화면 넷이 같은 컴포넌트
  (`GptErrorAnalysis` → agentType `ERROR_ANALYSIS`)를 쓴다. **프롬프트만 고치면 네 화면에 동시에 적용된다**
  (워크플로우 로그 실패 창 · CDC 로그 상세 창 · 대시보드 실패 상세 · ETL 로그).
- «확신도»를 뺀 이유는 문구 취향이 아니다 - 모델이 스스로 매기는 값이라 근거가 없고, «높음»이 붙었다고 더 맞지도
  않다. 판단 근거는 «원인» 안에서 로그 문구를 인용하게 하는 쪽이 낫다(그 지시는 그대로 뒀다).
- **규칙 줄에 남아 있던 옛 제목도 고쳤다**: `- 확인이 필요한 것은 «확인할 것» 으로 돌린다` 를 그대로 두었더니,
  모델이 항목마다 «확인할 것» 을 덧붙여 «1. «확인할 것» 소스 DB 서버…» 처럼 답했다(실측). 제목을 바꿀 때
  프롬프트 본문의 참조까지 같이 봐야 한다는 사례.
- 화면의 제목 인식(`GptConversationView.HEADINGS`)에는 새 제목을 넣고 **옛 제목도 남겼다** - 모델이 예전
  형식으로 답하거나 이전 답을 다시 그릴 때 제목이 본문으로 섞이지 않게.
- `EtlJobContextCollector` 의 안내 문구도 같은 이름으로 맞췄다.

**검증**: CDC 로그 실패 줄에서 실제 해석 - «원인 / 확인사항 / 조치 방법» 세 구역, 확신도 없음,
항목 안의 제목 덧붙임도 사라짐. 백엔드 280건 중 279건 통과(`ConnectionRepositoryIT` 는 빌드 컨테이너에
Docker 가 없어 나는 환경 문제), 프런트 tsc 통과.

## 64. 트리 연결선을 ├─ · └─ 모양으로 (2026-09-28)

**요청**: 하위 묶음 왼쪽에 점선 한 줄이 끝까지 내려오던 것을, 손그림처럼 **줄마다 가로선으로 잇고 마지막 줄에서
꺾이는** 모양으로.

- `.tree-sub-group` 의 `border-left` 를 걷고, 자식 `.tree-node` 마다 가상요소 둘로 그린다.
  `::before` 가 세로(위 줄에서 내려오는 선), `::after` 가 가로(줄 가운데에서 왼쪽 끝까지).
  **마지막 자식은 세로를 제 줄 가운데(17px)에서 끊어** └─ 가 된다.
- 치수는 실측으로 맞췄다 - 줄 높이 30.8px 이라 가로선은 `top: 15px`, 세로선은 묶음 `gap: 2px` 를 덮도록
  `top: -2px` 에서 시작한다. 가로선 길이는 11px 로 **줄 왼쪽 끝에 딱 붙인다** - 더 늘이면 고른 줄(빨간 배경)
  위로 선이 지나가 지저분해진다.
- 같은 CSS 를 쓰는 트리 셋에 모두 적용된다(`.wf-tree-scroll` 워크플로우 관리 · CDC 관리,
  `.airflow-tree-body` 워크플로우 로그, `.wf-palette-tree` 캔버스 팔레트).

**검증**: 워크플로우 관리(total → dz_com_daily ├─ · dz_pop_daily └─)와 워크플로우 로그
(CDC → 전체 파이프라인 → oracle-source, ETL → cdc_imp · total) 둘 다 확대 캡처로 확인. 페이지 오류 0.

**이어서 - 펼칠 것 없는 줄의 빈 화살표 자리 제거**(같은 날 추가 요청): job · 파이프라인처럼 하위가 없는 줄은
화살표를 `visibility: hidden` 으로 감추기만 해서 **자리(14px + 줄 간격 6px = 20px)를 그대로 차지**했다.
그래서 연결선과 아이콘 사이가 넓게 비었고, 한 묶음이 전부 job 이면 그 빈칸이 통째로 남았다.
`display: none` 으로 바꿔 자리 자체를 없앴다.

- 딸려 오는 것: 같은 깊이에 폴더와 job 이 섞이면 **job 아이콘이 폴더의 화살표 자리에 선다**(폴더 아이콘보다
  20px 왼쪽). 파일 탐색기들이 쓰는 방식이라 어색하지 않지만, 아이콘 열을 맞추고 싶으면 빈 자리를 되살리는 쪽으로
  되돌리면 된다.
- 실측: leaf 줄의 아이콘 x 가 41px(= 같은 깊이 폴더의 화살표 자리), 폴더 아이콘은 61px.

## 65. 대시보드 오류 표시를 «조회 기간의 실행 실패»로 통일 (2026-09-28)

**요청**: 대시보드의 CDC · ETL · 워크플로우 오류는 모두 **조회 기간 안에서 실행에 실패한 건**이어야 한다.
설정이 덜 된 그룹은 오류가 아니다. 각 카드의 [로그] 로 들어갔을 때 보이는 실패와 **같은 것**이어야 하고,
대상 표기도 CDC = 파이프라인명, ETL = job 명, 워크플로우 = 워크플로우명이어야 한다.

**무엇이 어긋나 있었나**
- **ETL**: 카드 숫자가 «지금 Invalid 인 프로세스 그룹 수»였다. 오늘 아무것도 돌리지 않아도 3건이 잡혔고
  (Incremental · File · Initial - 템플릿 성격의 그룹), 실행 실패와 섞여 상세 목록에도 «(현재 오류)» 로 들어갔다.
- **CDC**: 카드 숫자가 «지금 FAILED 인 커넥터 수»였다. 기간과 무관했고, 기간 안에 났다가 복구된 실패는 안 보였다.
- **워크플로우**: 기간 기반은 맞았지만 **CDC 제어 DAG 실행까지 같이 셌다.** 같은 실패가 CDC 카드와 겹쳤고,
  CDC 감시 Run 이 «실행 중»에 계속 쌓였다. 이름도 dag_id 원문(`etl_wf_etl_root_total`)이 나왔다.

**고친 것**
- CDC: `listCdcEventLogs` · `listCdcProcessingLogs` 를 **조회 기간으로** 받아 실패만 센다(CDC 로그 화면과 같은 재료).
  대상 = 파이프라인명, GPT 대상은 살아 있는 파이프라인에만 붙인다(지워진 파이프라인은 수집기가 못 찾는다).
- ETL: 기간 안의 NiFi 실행 로그 실패만 센다(ETL 로그 화면과 같은 재료). 대상 = job 명.
  «지금 Invalid» 는 카드 안쪽 지표 «설정 확인 필요» 로 옮겼다 - 정보는 남기되 오류로 세지 않는다.
- 워크플로우: 집계에서 CDC 제어 DAG(`kafka_pipeline_*_control`)를 뺀다. 이름은 `listWorkflows()` 로 받아
  워크플로우명으로 적는다. 커넥터의 현재 FAILED 수도 CDC 카드 안쪽 지표 «커넥터 실패(현재)» 로 남겼다.
- 창 제목을 «CDC 실패 상세 · 기간» · «ETL 실패 상세 · 기간» 으로 맞췄다.

**검증**(로컬, 읽기만) - API 실측과 화면을 같은 기간으로 대조했다.

| 기간 | 항목 | API 실측 | 대시보드 |
|---|---|---|---|
| 09-27 하루 | CDC | 3 | **3** |
| 09-27 하루 | ETL | 0 | **0**(칩 없음, «설정 확인 필요 3» 지표로) |
| 09-01~09-30 | CDC | 23 | **23** |
| 09-01~09-30 | ETL | 1 | **1**(대상 DZ_COM001M, 펼치면 그룹 · job · 프로세서 · 원문) |
| 09-01~09-30 | 워크플로우 | 10 → 8 | **8** |

워크플로우 10 vs 8 은 **지워진 워크플로우(stale DAG)의 실행 2건** 차이다. 워크플로우 로그 화면도 살아 있는 DAG 만
보여 주므로 두 화면이 같은 값이다. 프런트 tsc 통과, 페이지 오류 0.

## 66. 워크플로우 실패 창을 ETL · CDC 와 같은 모양으로 (2026-09-28)

**요청**: 대시보드의 «워크플로우 실패 보기» 창만 모양이 달랐다(기본 내용 · 구분 · 일시 · [접기] · 검은 로그 칸).
ETL · CDC 가 쓰는 창과 같게.

- 실패 타일만 `FailureDetailModal` 로 바꿨다 - 열 «대상 · 시각», 줄을 펼치면 항목(워크플로우 · 구분 · DAG) →
  실행 로그 원문. 제목도 «워크플로우 실패 상세 · 기간» 으로 셋을 맞췄다.
- 나머지 타일(실행 중 · 성공 · DAG 목록)은 실패가 아니라 목록이라 예전 창(`HistoryModal`) 그대로 둔다.
- `FailureDetailModal` 에 **선택적 줄 버튼**(`rowAction`)을 더해 [로그] 를 살렸다. 워크플로우는 한 실행 안에
  실패 task 가 여럿이고 시도별 로그 · 하위 워크플로우까지 따라가야 해서, 펼친 줄의 원문만으로는 모자란다 -
  누르면 로그 화면과 같은 전용 창(`TaskFailureLogModal`)이 열리고 그 안에 task 별 GPT 해석이 있다.

**검증**: 09-01~09-30 기준 워크플로우 실패 8건이 새 창에 뜨고, 머리(대상 · 시각 · 버튼) · 펼친 내용
(워크플로우 total · 구분 ETL · DAG etl_wf_etl_root_total + 실행 로그)까지 ETL · CDC 창과 같은 차례로 나온다.
프런트 tsc 통과, 페이지 오류 0.

## 67. 팝업 상단 강조선 · GPT 를 설정 안으로 · 알림/발송 소메뉴화와 레이아웃 · 알림 이력 정렬 (2026-09-28)

**요청**(차례대로)
1. 팝업 맨 위에 빨간 강조선(먼저 워크플로우 로그의 «로그» 창만). 띠 위 흰 줄 없애고 두께 4px.
2. GPT 등록 화면 «외부 (Claude · ChatGPT · Gemini)» → «외부».
3. 대메뉴 «GPT 설정»을 설정 안 중메뉴 «GPT»(연결정보 다음)로. 올리면 오른쪽에 소메뉴 등록 · 관리.
4. 설정 > 알림/발송의 화면 안 탭도 같은 방식(소메뉴)으로. 네 화면 모두 다른 목록 화면처럼 바닥까지 채우기.
5. 발송 채널 · 수신자 · 계정 화면의 상단 안내문구 정리(수신자는 «추가» 옆 물음표로, 계정은 비밀번호 초기화 창으로).
6. 알림 이력 표에 항목별 정렬 추가.

- 1 팝업 띠(`index.css .accent-modal`, `TaskFailureLogModal` rootClassName): 창 위 테두리를 띠 색으로 칠하고 그 안에 3px 띠 -
  합쳐서 4px. 처음에는 띠를 테두리 밖으로 1px 내밀었는데, 창이 `overflow:hidden` 이라 잘려 흰 줄이 남았다.
  다른 창은 rootClassName 에 `accent-modal` 만 붙이면 같이 적용된다(ETL 메뉴 제외 예정).
- 3·4 메뉴(`AppLayout`): `NavLeaf.children` 을 더해 중메뉴 안 소메뉴를 만든다. 줄 모양은 다른 중메뉴와 같고 오른쪽 끝에 ›,
  올리면(또는 포커스가 들어오면) 오른쪽에 같은 모양의 목록이 뜬다. 줄 자체는 이동하지 않는다.
  · GPT: 대메뉴에서 빼고 설정 > 연결정보 다음(등록 · 관리). 주소는 그대로(/gpt/register · /gpt/manage).
  · 알림/발송: 소메뉴 4개(알림 이력 · 알림 규칙 · 발송 채널 · 수신자)를 `?tab=` 으로 연다. `SettingsPage` 의 화면 안 Tabs 는 걷고
    고른 화면 하나만 그린다(제목이 그 화면 이름이 된다). 소메뉴 활성 표시는 직접 판정한다 - react-router 는 `?tab=` 을 보지 않아
    그대로 두면 넷이 한꺼번에 켜진다.
  · `/gpt/**` 는 설정 밖 경로라, 그룹 활성 판정에 소메뉴 경로까지 넣었다.
- 4 레이아웃: 탭 줄을 걷으면서 `.std-page--fill > .std-tab-body` 규칙을 더했다(예전에는 `> *:not(.std-table-card)` 에 걸려
  본문이 늘어나지 않았다). 네 화면 모두 표 카드가 화면 바닥까지 내려온다.
- 5 안내문구: 발송 채널 상단 안내 삭제(경고 알림은 유지). 수신자 안내는 «추가» 옆 물음표(`.std-hint-mark`)에 넣어 올렸을 때만 보인다.
  계정 화면 상단 안내는 지우고, 그 중 필요한 한 줄을 비밀번호 초기화 창 입력칸 아래로 옮겼다. 창 제목 «비밀번호 초기화 — id» → «비밀번호 초기화».
- 6 정렬(`AlertQueueController.history` + `AlertHistoryTab`): 서버 정렬이다 - 한 페이지(10건)만 줄 세우면 «전체에서 제일 위험한 것»이 아니라
  «이 페이지에서 제일 위험한 것»이 올라온다. `sort`(occurred · severity · target · summary · status · ack) · `dir` 을 받고,
  ORDER BY 문구는 서버의 표(ORDER_COLUMNS)에서만 고른다 - 화면 값이 SQL 에 그대로 들어가지 않게. 심각도는 글자순이 아니라 위험한 순.
  같은 값이면 id DESC 로 묶어 페이지가 흔들리지 않게 했다. 정렬을 바꾸면 1페이지부터.
- 확인: 프런트 tsc · oxlint 0(기존 경고 외), 백엔드 컴파일 0. 배포 후 qa-playwright -
  설정 중메뉴 6개 · GPT/알림/발송 소메뉴 펼침과 이동, 화면 안 탭줄 0개, 네 화면 표 카드 아래끝 1066px(화면 1080)로 채움,
  수신자 물음표 말풍선 문구, 계정 안내 0개 · 초기화 창 제목과 입력칸 아래 안내, 정렬 클릭 시 `sort=severity&dir=asc → desc → 해제`.
  API 직접 확인 - severity asc 는 CRITICAL 먼저, occurred asc 는 오래된 것 먼저, 없는 열 이름을 보내면 기본 순서(주입 없음).
  로그 창 띠는 윗변부터 4px 빨강, 흰 줄 없음. 저장 · 발송 · 삭제는 누르지 않음, 페이지 오류 0.
- 사용자 확인에서 나온 것: 알림/발송 소메뉴 넷이 한꺼번에 빨갛게 켜졌다. NavLink 에 className 을 문자열로 주거나 아예 안 주면
  react-router 가 «경로가 겹치면 active» 를 스스로 덧붙인다(넷 다 /settings). 함수로 넘겨 직접 판정만 쓰도록 고쳤다 -
  확인: /settings · ?tab=별로 한 줄씩만, /gpt/register · /gpt/manage 도 한 줄씩만, 설정 밖 화면(/admin/users)에서는 하나도 안 켜짐.
- 중간에 낸 오류: ORDER BY 를 텍스트 블록 끝에 붙여 «BYCOALESCE» SQL 오류(텍스트 블록은 줄 끝 공백을 지운다). 정렬 구문을 한 줄로 만들어 고쳤고,
  고친 뒤 기본 조회 · 각 정렬 200 을 확인했다.

## 68. 카카오 알림톡 설정 창에 «설정 방법» 안내 (2026-09-29)

**요청**: 발송 채널 → 카카오톡 설정 창 제목 옆에 물음표를 두고, 올리면 **알림톡 사용 방법**을 보여줄 것
(개인 «나에게 보내기»가 아니라 알림톡 설정 안내여야 한다).

- `CHANNEL_SETUP_HINT` 를 새로 두고 제목 옆 `std-hint-mark`(다른 화면에서 쓰는 물음표와 같은 모양)에 얹었다.
  채널마다 안내가 다를 수 있어 지도(map)로 두었고, 안내가 있는 채널에만 물음표를 그린다(지금은 KAKAO).
- 안내 내용: ① 카카오 쪽 준비(채널 개설 → 비즈니스 인증 → 대행사 계약 → 발신프로필키 → **템플릿 등록·심사
  승인**, 영업일 1~3일) ② 이 창에 넣는 값이 어디서 오는지 ③ 보내지는 조건(수신자 전화번호 · 카카오톡 수신 켬 ·
  **위험 등급만**) ④ 주의(승인된 템플릿 문구만 나간다 - 템플릿에 없는 내용은 카카오가 거부).
- 우리 화면만 채운다고 나가지 않는다는 점이 핵심이다. 그 순서를 모르면 «설정은 다 넣었는데 왜 안 가지»가 된다.

**검증**: 설정 → 발송 채널 → 카카오톡 [설정] → 제목 옆 물음표에 마우스를 올려 안내가 뜨는 것 확인.
프런트 tsc 통과, 페이지 오류 0.

**확인한 것 하나**: 지금 저장돼 있는 카카오 설정 값은 **«나에게 보내기»(카카오 메모 API)** 다 -
주소가 `/memo/default/send`, 본문이 `template_object=…`, Content-Type 이 `x-www-form-urlencoded`.
테스트로 넣어 둔 값으로 보이며, 알림톡으로 보내려면 대행사 값으로 바꿔야 한다(시연 전 확인 필요).

## 69. NiFi 템플릿에 완료 콜백 붙이기 - 1단계(로컬 Initial 템플릿) (2026-09-29)

**왜**: 워크플로우의 job 완료 판정이 전부 **추측**이다. 최근 45일 `completion_source` 57건 모두 OBSERVED,
CALLBACK 0건. await 태스크 51건 중 **24건이 186초**에 몰려 있는데, 이는 «활동을 한 번도 못 봤으니 180초는
기다려 보자»(`WAIT_START_GRACE_SECONDS`)를 그대로 태운 값이다. 적재는 진작 끝났는데 버리는 시간이다.
15초 폴링이 짧은 job 을 놓치면 **빠른 job 일수록 더 오래 기다리는** 역설이 생긴다.

**확인한 것**
- job 은 **템플릿 그룹을 통째로 복제**해서 만들어진다(`NifiClient.createProcessGroupSnippet` →
  `instantiateSnippet`). 템플릿에 프로세서를 더하면 **그 뒤에 만드는 job 에 자동으로 따라간다.**
- 코드가 이름으로 찾는 템플릿 5종: `Initial`(INSERT) · `Incremental`(UPSERT) · `truncate_initial`(TRUNCATE) ·
  `File` · `HTTP`. 셋 다 **마지막이 PutDatabaseRecord/PutSQL 하나**이고 소스가 `ExecuteSQLRecord` 라
  조회 1회 = FlowFile 1개다 - 끝에 InvokeHTTP 를 붙이면 **job 당 콜백 1회**가 된다.
- 콜백 받는 자리는 이미 있다: `POST /api/etl/job-runs/by-pg/{pgId}/complete`(본문 `{"rows":n}` 선택,
  재전송 안전, `X-Service-Token` 필요). **실제로 불러 확인** - 원장이 `completion_source=CALLBACK` 으로 닫혔다.
- **MSA 확인(읽기만)**: 루트 `Template` 폴더에 19개가 있고 **같은 이름이 두 벌**이다
  (`Initial` 프로세서 0개와 3개, `Incremental` 도 마찬가지). `findChildProcessGroup` 은 이름으로 첫 번째를
  고르므로 순서가 바뀌면 빈 템플릿을 복제하게 된다 - 콜백과 별개로 정리가 필요하다(ETL 담당자 영역).

**한 것(로컬 NiFi 캔버스 - git 에 안 남는 변경이라 여기 적는다)**
- `Template > Initial`(id `02a1ab2b-…`)에 프로세서 **`99_완료알림`**(InvokeHTTP, id `ec3ee4e6-…`) 추가.
  POST · `HTTP URL = http://pipeline-api:8081/api/etl/job-runs/by-pg/__PG_ID__/complete` ·
  `Request Body Enabled=false`(본문 없이 호출) · 사용자 정의 속성 `X-Service-Token`(요청 헤더로 나간다) ·
  관계 5종 자동 종료.
- 연결선 `03_타깃테이블 --success--> 99_완료알림` 추가. 프로세서 검증 오류 0.

**검증**: 백엔드가 job 생성에 쓰는 **바로 그 snippet API** 로 템플릿을 복제해 보니 복제본에 `99_완료알림` 과
연결선이 그대로 들어 있었다(URL 자리표시자까지). 확인 뒤 복제본은 삭제했고, 콜백 시험으로 생긴 원장 행도 지웠다.

**남은 것(2단계)**
1. job 생성 때 백엔드가 URL 의 `__PG_ID__` 를 **실제 프로세스 그룹 id** 로 치환(이미 SQL · DBCP 를 채우는
   자리가 있어 거기에 한 줄 는다). 지금 상태로는 복제만 되고 콜백은 안 간다.
2. 나머지 템플릿 4종에도 같은 처리 + 실패 콜백(`/fail`)도 함께.
3. **이미 만들어진 job 은 소급되지 않는다** - 보정(프로세서 추가) 또는 재생성이 필요하다.
4. MSA 반영은 ETL 담당자와 협의(템플릿은 그쪽 소유, 중복 이름 정리도 같이).

## 70. 완료 콜백 구조 영향 분석 + NiFi 템플릿 4종에 콜백 붙이기 - 2단계 템플릿 쪽 (2026-09-30)

**요청**: ① 9/29 목표 구조(콜백 → 트리거러 대기 → Kafka 가속 → 백엔드 집계)를 적용하면 ETL · 워크플로우 어디가 달라지는지,
② 단일 브로커를 늘려도 되는지(CDC 파이프라인 100개 대비) → `docs/2026-09-30-callback-architecture-impact.md`.
③ NiFi 템플릿 작업만 먼저 진행(ETL 담당자와 협의용) → `docs/2026-09-30-nifi-template-callback-handoff.md` + `docs/nifi-templates/*.flow.json`.

**확인한 것(영향)**
- Airflow 3.2.2 스케줄러 소스: `DEFERRED` 는 DAG `max_active_tasks` · 워커 슬롯 집계에서 제외(태스크 단위 `max_active_tis_per_dag` 에만 셈),
  `default_pool.include_deferred=False`, 트리거러 capacity 1000, `aiohttp 3.13.5` 있음, 트리거러 → pipeline-api 200 · 서비스 토큰 주입 확인.
  Kafka provider 는 없음(common-messaging 만).
- 지금 `await` 의 7가지 일(원장 조회 · NiFi 상태 폴링 · 스레드 로그 · bulletin 검사 · 유휴/유예 · 카운터 적재 건수 · observe 보고)이 영향 범위의 전부.
  로그 화면 적재 건수는 태스크 로그 «이번 실행 적재 건수 (총 N행)» 정규식이라 문구를 유지하면 무변경.
- **새로 생기는 문제**: 관측기(15초)가 콜백으로 방금 닫힌 실행의 꼬리 증가분을 보고 새 행을 만든다 → 실행 1건이 2행. 관측기 보정 필요.
- 브로커: apache/kafka 3.8.0 KRaft 1노드, 토픽 전부 RF=1, Debezium 히스토리 토픽 RF 는 브로커 기본값을 따름(jar 확인), `DebeziumOracleTemplate.BOOTSTRAP_SERVERS` 하드코딩.
  MSA 호스트 31GB/8GB 여유, kafka-connect 는 커넥터 6개로 1.1/1.25GiB → 100개의 병목은 Connect 워커 + Oracle LogMiner 세션 수. 파이프라인 2 · 3 이 `topic.prefix` 공유.

**§69 정정**: «조회 1회 = FlowFile 1개» 는 틀림. ExecuteSQLRecord `Max Rows Per Flow File` 50,000(Initial · Incremental) · 100,000(truncate_initial) 이라
큰 테이블은 조각 여러 개(166만 행 = 34개). 조각마다 완료를 보내면 첫 조각에서 원장이 닫힌다.

**한 것(로컬 NiFi 캔버스, `Template >` 4종 - git 에 안 남는 변경)**
- `97_완료판정`(RouteOnAttribute): `last` = `${fragment.count:isNull():or(${fragment.index:plus(1):equals(${fragment.count:toNumber()})})}`, unmatched 자동 종료.
- `98_실패알림`(InvokeHTTP `…/by-pg/__PG_ID__/fail?error=<EL: putdatabaserecord.error → executesql.error.message, 공백 정리 · 500자 · urlEncode>`),
  `99_완료알림`(`…/complete`). 둘 다 본문 없음 · `X-Service-Token` 동적 속성 · 관계 5종 자동 종료 · Retry 탭(Retry+Failure, 5회, Penalize, 최대 10분).
- 실패 모음 funnel → 98. 각 템플릿의 자동 종료돼 있던 failure/retry(File 은 not.found · permission.denied 포함)를 funnel 로, 적재 처리기 success 를 97 로.
  File 은 «전체 파일 수»를 알 수 없어 완료 콜백 없이 실패 콜백만. HTTP 템플릿은 로컬에 없음.
- 헬퍼는 airflow-scheduler 컨테이너 안에서 실행(`/tmp/nifi_tpl.py` · `/tmp/nifi_cb.py`, 세션 스크래치패드 원본). 호스트에서 NiFi 8443 이 안 열려 있고,
  자격 증명을 명령줄에 넣는 방식은 차단되므로 컨테이너 환경변수(NIFI_USERNAME/PASSWORD)로 읽는다.

**검증**
- 4종 모두 97 · 98 · 99 VALID. 임시 그룹에서 `fragment.index` 0·1·2(count 3) + 조각 없음 → 97 이 2·조각없음만 통과(0·1 은 unmatched 2건), 99 두 건 HTTP 200,
  원장 CALLBACK 행 생성. 한글 · 줄바꿈 · 탭이 든 오류로 98 → 200, URL `error=ORA-01400%3A+%EB%84%90+…` 인코딩 확인, 원장 FAILED(백엔드가 쿼리 파라미터를 아직 안 읽어 기본 문구).
- 백엔드가 쓰는 snippet 복제 API 로 `truncate_initial` 복제 → 9 프로세서 · 13 연결 · funnel 1 그대로 따라옴.
- flow definition 4개 내보내기(`GET /process-groups/{id}/download`) → 토큰 `__SERVICE_TOKEN__` 치환(각 파일 98 · 99 자리 확인), 비밀번호 값 없음.
- 임시 그룹 · 시험 원장 행 3건(156~158) 삭제.

**남은 것**: ETL 담당자 협의(retry 관계 · 실패 의미 · 토큰 노출 · 행수 · bulletin 레벨 · Max Rows) → MSA 반영(중복 템플릿 이름 정리 포함) → 기존 job 보정.
백엔드: `__PG_ID__` 치환 · `/fail?error=` 읽기 · 관측기 이중 행 방지. Airflow: deferrable await · bulletin 검사에서 `9x_` 제외.
로컬 `Template` 폴더의 `DEMO_*` 5개(이전 실험 사본, 실제 DB 접속 보유)는 정리 대상 - 사용자 확인 뒤 삭제.

## 71. CDC 파이프라인별 제어 DAG 폐지 - 워크플로우로만 제어 (2026-10-01)

**요청**: 파이프라인을 만들면 생기던 제어 DAG(`kafka_pipeline_{id}_control`)를 없애고 CDC 는 워크플로우로만 제어(9/27 검토 문서 결정 1-A).
«워크플로우는 성공인데 CDC 는 안 돈다»는 큰 오류라 반드시 막는다. 워크플로우 안에서 파이프라인마다 시작·중지·재시작을 따로 줄 수 있어야 한다.
어느 워크플로우에도 없는 파이프라인은 실행할 수 없게 한다(결정 (a)).

**원인 정리**: CDC 노드가 제어 DAG 를 `TriggerDagRunOperator(wait_for_completion=False)` 로 쏘기만 했다 - 제어 DAG 가 502 로 실패해도
워크플로우는 성공. 제어 DAG 의 `monitor_cdc_runtime` 센서는 CDC 수명 내내 Run 을 «실행 중»으로 붙잡았지만, 실제 판정은 백엔드
`KafkaPipelineStateSynchronizer` 가 하고 센서는 그 결과를 DAG 색으로 옮겨 적을 뿐이었다(감시 중복 차단 코드까지 따로 필요했다).

**변경**
- Airflow `cdc_runtime.py`(신규): 노드 = `apply`(이번 실행의 지시를 고름 → 이미 그 상태면 건너뜀 → 백엔드 지시, 거절 사유를 그대로 실패 메시지로)
  → `verify`(Kafka Connect 라이브 상태가 기대값이 될 때까지 최대 60초, 아니면 실패). 성공 = «실제로 그 상태가 됐다». 이후 «계속 도는지»는
  런타임 상태 + 알림(CONNECTOR_FAILED)이 맡는다.
- 노드별 지시: 실행 conf `node_actions`({"노드키": "start" | "stop" | "restart" | "deploy"}), 없는 노드는 건너뜀. 비어 있으면 `action` 이 전체에(옛 일괄 지시 호환).
  CDC 노드 진입 규칙은 `none_failed`(앞 노드가 건너뜀이어도 진행, 실패면 막음). CDC 워크플로우 `max_active_runs=2 → 1`.
- 삭제: `airflow/dags/kafka_pipelines_dynamic.py`, 백엔드 `CdcDagSpecPublisher`, 카탈로그 컨트롤러의 감시 중복 차단 · `watch-runs` API,
  카탈로그 동기화의 `kafka_pipeline_*_control` → CDC 분류, 컴파일러 spec 의 `cdc_dag_id`. Airflow Variable `cdc_pipeline_index` · `cdc_pipeline_spec__*` 13개 삭제.
  옛 제어 DAG 는 Airflow 에서 지우지 않았다(파일이 없어 stale, 실행 이력은 남음).
- 백엔드 워크플로우 목록에 `cdcNodes`(노드 키 · 파이프라인 id) 추가 - 화면이 «이 파이프라인이 어느 워크플로우의 어느 노드인가»를 찾는다.
- 로그 화면(`AirflowDashboardPage`): CDC 워크플로우 «실행 설정» = 파이프라인별 지시 표(그대로/시작/중지/재시작/배포 + 전체 적용).
  트리의 CDC 파이프라인 우클릭 «실행 설정» = 소속 워크플로우 창이 그 노드만 «시작»으로 골라진 채 열림. 워크플로우에 없으면 «실행 설정 (워크플로우에 없음)» 비활성.
  CDC 목록의 «워크플로우 상태» 열은 소속 워크플로우의 최근 실행. CDC 관리 · 캔버스 안내 문구를 워크플로우 기준으로 고침.

**검증(로컬, 워크플로우 `etl_wf_cdc_imp` = 파이프라인 2 · 3, UI 와 같은 백엔드 실행 API 로)**
- `node_actions={018m: deploy}` → 018m `apply · verify` 성공(«전부 STOPPED» 1회차 확인), 060l 은 «이번 실행의 지시 대상이 아닙니다»로 건너뜀, 워크플로우 success.
- `node_actions={018m: start}` → 018m 은 SCN 만료로 실제로 안 뜨는 파이프라인 → apply 실패
  «start 실패(HTTP 502): Kafka Connect가 실패 상태입니다 … taskStates=[FAILED]» → **워크플로우 failed**(예전 같으면 success 였다). 파이프라인 상태는 READY 그대로.
  처음엔 사유 추출이 `error` 객체를 문자열로 잘라 KeyError 를 냈다 - 고친 뒤 재시험.
- `action=stop`(옛 일괄 방식) → 둘 다 «이미 멈춰 있습니다(READY)»로 건너뜀, success.
- 백엔드 테스트(Workflow · AirflowDag · Pipeline) 통과(카탈로그 동기화 테스트를 새 규칙으로 고침). UI 타입체크 통과, 배포 번들에 새 창 포함 확인.

**복구 지점**: git 태그 `before-cdc-workflow-only`(7a93fc1), 백업 `~/data-pipeline-backup/2026-10-01-before-cdc-workflow-only/`
(워크플로우 · 파이프라인 · 카탈로그 테이블 덤프, Airflow Variable 전체, 커넥터 · 파이프라인 상태).

**남은 것**: «시작 성공» 경로는 정상 파이프라인이 없어 실측 못 함(2 · 3 은 SCN 만료 - 재스냅샷 필요, 9/27 문서 결정 3). 로그2 화면(`/workflows/logs2`)은
아직 옛 제어 DAG 기준이라 CDC 파이프라인 실행 버튼이 안 보인다. 상단 CDC 현황 카드는 제어 DAG 실행을 세던 것이라 0 으로 보인다(9/27 결정 2 - «가동 N/M» 한 칸).
MSA 반영은 별도(push 요청 시).

## 72. CDC 워크플로우 실행 창 정리 · 기능 시험 · 로그2 삭제 · 현황 카드 통합 (2026-10-02)

**요청**: ① 실행 설정 창 안내 문구 삭제, 파이프라인 · 현재 상태 필터, «그대로»→«유지» ② CDC 트리에 남은 것이 옛 제어 DAG 인지
③ CDC job 2개를 워크플로우로 태스크별 실행 · 중지 · 배포 · 재배포 · 유지 기능 시험 ④ 로그2 화면 · 메뉴 삭제
⑤ 현황 카드 CDC/ETL 구분 없이 «실행 중 · 성공 · 실패 · 대기» 한 장, 대기 = 실행 중 워크플로우의 후행 ⑥ 트리 «전체 접기 ↔ 전체 열기» 전환.

**변경**
- 실행 설정 창: 문구 삭제, 이름 입력 · 현재 상태 선택 필터(«전체 적용»은 걸러진 줄에만), «유지».
- ② 로그 화면 CDC 가지(«전체 파이프라인»)는 DAG 가 아니라 CDC 관리의 파이프라인 목록이다. 옛 제어 DAG 17개는 Airflow 메타 DB 에
  실행 이력과 함께 남아 있지만(파일 없음) 카탈로그에서 disabled 라 화면에 안 나온다.
- 로그2: `WorkflowLogV2Page` · `LogV2Tree` · `LogV2Detail` 삭제, 라우트 `/workflows/logs2` · 메뉴 2곳 삭제. 기존 로그 화면이 같이 쓰는
  `LogV2Grid` · `logV2Data` · `logV2Hooks` · `LogV2Parts` 는 남김.
- 현황 카드: 한 장(실행 중 · 성공 · 실패 · 대기), 커튼 요약 줄도 한 줄. 대기 = 큐에 오른 실행 + 실행 중 워크플로우의 후행
  (선행 연결이 실행 중인 워크플로우, 실행 중인 상위의 이번 실행에서 아직 시작 안 한 하위). 백엔드 워크플로우 목록에 `upstreamWorkflowIds` 추가.
- 트리 머리 «전체 접기» 단추가 누를 때마다 «전체 열기»와 바뀐다(TreeHead + 워크플로우 로그 · 관리 · 설계 팔레트 · CDC 관리 트리).
  다른 세션(data-pipeline-fe)의 미커밋 트리 작업 위에 얹은 것이라 그쪽 커밋에 함께 들어간다.

**기능 시험(로컬, `etl_wf_cdc_imp` = 파이프라인 2 `018m` · 3 `060l`, UI 와 같은 실행 API)**

| # | 지시 | 결과 |
|---|---|---|
| 1 | 018m 배포, 060l 유지 | 018m 배포 → 전부 STOPPED 확인 · 060l 건너뜀 → success |
| 2 | 060l만 배포 | 060l 배포 확인 · 018m 건너뜀 → success |
| 3 | 둘 다 재배포 | 둘 다 배포 확인 → success |
| 4 | 018m 중지 | «이미 멈춰 있습니다(READY)» 건너뜀 → success |
| 5 | 060l 재시작 | «재시작 대상이 아닙니다(READY)» 건너뜀 → success |
| 6 | 둘 다 유지 | 둘 다 건너뜀 → success |
| 7 | 060l 시작 | 502 «Kafka Connect가 실패 상태 … taskStates=[FAILED]» → **failed** |
| 8 | 018m 시작 + 060l 배포 | 060l 배포 성공, 018m 시작 실패 → **failed**(다른 노드는 영향 없이 진행) |
| 9 | 옛 일괄 action=deploy | 둘 다 배포 확인 → success |

끝난 뒤 파이프라인 2 · 3 READY, 커넥터 전부 STOPPED(시험 전과 같음). **시작 · 실행 중 중지 · 실행 중 재시작의 성공 경로는 못 봤다** -
두 파이프라인 모두 Debezium 오프셋(SCN)이 원천에서 지나가 시작 자체가 안 된다(9/27 문서 결정 3, 재스냅샷 필요).

## 73. 로그 창 3종 통일(워크플로우 · ETL · CDC) + ETL GPT 추가 질문 무응답 수정 (2026-10-02)

**요청**: 세 로그 창을 섞어서 통일 - 맨 위 강조 띠는 워크플로우 창 것, ETL 창 세로 빨간 줄 삭제, 제목은 job/파이프라인 이름만(«실패 로그» 삭제,
워크플로우는 차수 그대로), ETL «실패 원인» 제목 · CDC 구분/시각/오류로그 줄 삭제 → 열자마자 GPT 해석, 그 아래 로그(워크플로우 «실패 원인» 상자 모양),
창 폭 · 글꼴 · 색 통일, GPT 답 순서 원인 → 조치방법 → 확인할 것, 워크플로우 창도 열면 바로 GPT 해석, ETL GPT 추가 질문 무응답 조치.

**ETL 추가 질문 무응답 원인(두 가지)**
1. ETL 창은 GPT 답을 «원인 추정 / 조치 방법 / 확인할 것» 제목으로 **골라서만** 그렸다. 프롬프트는 9/28 에 «원인 / 확인사항 / 조치 방법»으로 바뀌어
   첫 답도 «조치방법»만 보였고(사용자 캡처), 추가 질문의 답은 제목 없이 오니 **통째로 걸러져 빈칸**이었다.
2. 창 컴포넌트가 페이지 컴포넌트 **안에서** 선언돼 페이지가 다시 그려질 때마다 새로 만들어졌다(대화 상태 유실 위험).

**변경**
- `ErrorAnalysisAgent` 첫 답 형식: 원인 → 조치 방법 → 확인할 것(테스트 · ETL 수집기 안내 문구 같이). 대시보드 실패 창도 같은 에이전트라 같이 바뀐다.
- `GptConversationView`: 제목 골라 그리기는 제목이 있는 답에만, 옛 제목 별칭(원인 추정→원인, 확인사항→확인할 것), 공통 순서 `ERROR_ANALYSIS_HEADINGS` · 라벨(조치방법).
  `GptErrorAnalysis` 기본값으로 사용 - 화면별 제목 설정 제거.
- ETL 창(`EtlLogsPage`): 모듈 수준으로 이동, 제목 = job 이름, `accent-modal`, GPT → «에러 로그»(failure-log-exception 상자), GPT 닫으면 «GPT 해석» 단추. 세로 빨간 줄 CSS 삭제.
- CDC 창(`CdcLogsPage`): 폭 860→960, `accent-modal`, 구분 · 시각 줄 삭제, GPT → «에러 로그» 상자. 실패가 아닌 줄(처리 이력)은 그 값들을 «로그» 상자에.
- 워크플로우 창(`TaskFailureLogModal`): 열면 GPT 해석 자동 시작(닫으면 단추로 다시).

**검증(로컬, 실제 브라우저)**: 세 창 모두 폭 960 · 강조 띠 · 제목 16px · 로그 상자 12px #991b1b, GPT 가 로그보다 위, 제목 «원인 · 조치방법 · 확인할 것».
ETL(DZ_COM001M 9/5 실패) 추가 질문 «한 문장으로 요약해줘» → 답이 그대로 보인다. CDC(AAA-tb_imp018m 시작 실패) · 워크플로우(cdc_imp 12차) 동일 확인.
백엔드 LLM 관련 테스트 통과.

다른 세션(data-pipeline-fe)의 미커밋 GPT 작업(MD 저장하기 · 프롬프트 편집 · 질문 줄)과 같은 파일이라 내 덩어리만 골라 커밋했다.

## 74. GPT 해석 제목 번호 · 워크플로우 로그 실행 목록 열 정리 (2026-10-02)

- GPT 오류 분석(워크플로우 · ETL · CDC 공통) 제목을 «1. 원인 / 2. 조치방법 / 3. 확인할것»으로, 그 안의 «1. …» 항목은 «(1) …» + 14px 들여쓰기로
  (제목 번호와 항목 번호가 같은 모양이라 헷갈림). 사용자가 적은 «오류 → 1. 오류»는 지금 제목이 «원인»이라 «1. 원인»으로 읽었다.
- 워크플로우 로그 위쪽 실행 목록(EtlRunsPane · RunRow): «유형»(전부 워크플로우) · «동작»(줄을 누르면 아래 상세 실행이력이 나와 «실행 이력» 단추가
  필요 없음) 열 삭제, 실패 줄 «적재/결과»의 «로그» 단추 삭제(열은 유지, 합계만). 아래 상세 실행이력 표는 그대로.
- 브라우저 확인: 실행 목록 머리줄 8칸 · 단추 없음, CDC 실패 창 GPT 제목 번호 · «(1)» 들여쓰기(14px).

## 75. GPT 해석 본문 들여쓰기 · ETL 로그 화면을 CDC 로그 디자인으로 · 상태 배지 통일 (2026-10-02)

- GPT 오류 분석: 제목(«1. 원인» 등) 아래 본문 전체를 14px 들여쓰기(번호 항목 «(1)»만 들여 써 있던 것을 일반 문장까지).
- ETL 로그(`EtlLogsPage`): CDC 로그와 같은 틀 - `StdPage`(cdc-log-page) · `std-card std-table-card` · `std-filter-card`/`std-date-preset` ·
  `std-full-table`(middle) · «총 N건» · 상태 왼쪽 [+](`cdc-log-open`). 내용 · 기능(필터 · 실패 줄 클릭 · 상태 배지 클릭 · 경로 링크)은 그대로.
  [+] 가 안 보이던 원인: `expandedRowRender` 가 없으면 antd 가 펼침 열을 아예 그리지 않는다(CDC 는 빈 함수를 넣어 두었다).
- 상태 배지: ETL 쪽 모양(4px 모서리 · 12px · antd 기본 색)을 `log-status-tag` 로 두 화면 공통 적용 - 표준 틀의 알약 배지보다 우선.
- 브라우저 실측: 두 화면 표 머리 12px #475569 · 본문 12.5px #1e293b · 행 48px · 조회 버튼 32px · 배지 4px/12px 동일.
