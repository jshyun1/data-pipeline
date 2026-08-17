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
