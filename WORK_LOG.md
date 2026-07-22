# WORK LOG — Kafka 파이프라인 웹서비스 구축

> 갱신: 2026-07-22 (Keycloak 기반 포털/NiFi/Airflow SSO + Airflow 통합 웹 iframe + GitLab CI 보안 테스트 수정 — 16~17절 참고. 그 이전 갱신: 2026-07-21, NiFi 유실/미완성 플로우 3건 복구 + WSL2 메모리 부족 이슈 진단 — 15절 참고)
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
- 커밋 `2c0bbd4` 후 `feature/airflow-oidc`를 `dev`에 fast-forward 병합하고 원격 push 완료.
