# Kafka 기반 실시간 데이터 연동 웹서비스 설계서

> 전제 조건: 최종 배포 방식은 **외부망에서 Docker 이미지를 빌드한 뒤 `docker save`로 tar 파일을 만들고,
> 폐쇄망에서 `docker load` 후 실행하는 방식**으로 통일한다. 폐쇄망 내부에서는 Maven/NPM/Connector
> 다운로드나 Docker 이미지 빌드를 수행하지 않는다.

## 0. 문서 목적

본 문서는 현재 진행 중인 `data-pipeline` 프로젝트를 기준으로, **NiFi 구성은 유지**하고
**Kafka/Kafka Connect 영역만 웹서비스화**하기 위한 기능 및 설계 내용을 정리한다.

최종 목표는 다음과 같다.

- Oracle / PostgreSQL을 소스 또는 타겟으로 선택할 수 있는 실시간 테이블 CDC 파이프라인 생성
- 특정 로그파일을 Kafka로 실시간 전송하는 로그 파이프라인 생성
- 생성된 파이프라인을 Kafka Connect 또는 로그 수집 에이전트에 배포
- 파이프라인 상태, 로그, 오류, 처리량, 지연 상태 모니터링
- 폐쇄망에 Docker 이미지 형태로 반입하여 바로 사용할 수 있는 패키지 구조 구성

---

## 1. 현재 프로젝트 분석 결과

### 1.1 현재 프로젝트 구성

새로 만드는 웹서비스 코드는 아직 없는 상태이며, 다음 인프라가 구성되어 있다.

```text
현재 data-pipeline/
├── docker-compose.yml
├── .env.example
├── kafka-connect/
│   ├── Dockerfile
│   └── connectors/
│       ├── oracle-cdc-source.json.template
│       ├── postgres-cdc-sink.json.template
│       └── postgres-cdc-sink-clob-test-data.json.template
├── connect-init/
│   └── Dockerfile
├── scripts/
│   ├── register-connector.sh
│   └── setup-local-dev.sh
├── db/
│   ├── oracle-init/
│   └── target-init/
├── nifi/
│   ├── Dockerfile
│   └── FLOW_RUNBOOK.md
└── docs/
    └── kafka-cdc-pipeline.md
```

현재 CDC 파이프라인은 다음 구조다.

```text
Oracle
  → Debezium Oracle Source Connector
  → Kafka Topic
  → Debezium JDBC Sink Connector
  → PostgreSQL 기반 Target DB, TarantulaDB 대체
```

NiFi는 `FLOW_RUNBOOK.md` 기준으로 정형 CDC의 메인 적재 경로가 아니라, 아래 용도로 유지된다.

```text
- 비정형 데이터 수집
- HTTP 수집
- 파일/텍스트 수집
- Oracle EMPLOYEES → PostgreSQL 배치 동기화 예시
```

따라서 웹서비스 설계에서는 **NiFi를 직접 제어하지 않고**, Kafka/Kafka Connect 기반 파이프라인 관리 기능을 우선 구현한다.

---

### 1.2 현재 프로젝트의 장점

| 항목 | 내용 |
|---|---|
| Docker Compose 기반 | Kafka, Kafka Connect, NiFi, Oracle POC DB, Target DB를 한 번에 실행 가능 |
| Kafka KRaft 사용 | Zookeeper 없는 단일 Kafka 구성이 이미 반영됨 |
| Custom Kafka Connect 이미지 | Debezium Oracle Source Connector와 Debezium JDBC Sink Connector가 포함됨 |
| Connector 자동 등록 | `connect-init` 컨테이너가 기동 시 Connector 템플릿을 등록함 |
| 폐쇄망 전환 가능성 | 외부망에서 사전 빌드한 Docker 이미지를 tar로 반입하는 방식으로 전환 가능 |
| NiFi 분리 가능 | NiFi는 신설하는 Kafka 웹서비스와 독립적으로 운영 가능 |

---

### 1.3 현재 프로젝트의 보완 필요점

| 구분 | 현재 상태 | 보완 방향 |
|---|---|---|
| 파이프라인 정의 | Connector JSON 템플릿이 고정 | 사용자가 실제 DB/테이블 기준으로 동적 생성 |
| 대상 테이블 | `APPUSER.CUSTOMERS`, `APPUSER.CLOB_TEST_DATA` 고정 | 화면에서 schema/table 선택 |
| 소스 DB | Oracle 중심 | Oracle, PostgreSQL 모두 소스 가능하도록 확장 |
| 타겟 DB | PostgreSQL target-db 중심 | Oracle, PostgreSQL 모두 타겟 가능하도록 확장 |
| JDBC Sink Driver | 현재 JDBC Sink 플러그인에 PostgreSQL Driver 중심 | Oracle 타겟 지원을 위해 JDBC Sink 플러그인 경로에 `ojdbc11` 추가 필요 |
| 로그파일 적재 | NiFi 중심 가이드만 있음 | Kafka 웹서비스에서 로그 수집 Agent 또는 Kafka Connect 기반으로 별도 관리 |
| 관리 UI | 없음 | React 기반 웹 UI 추가 |
| API 서버 | 없음 | Spring Boot 기반 Backend 추가 |
| 메타데이터 DB | 없음 | 파이프라인 정의/상태/이력 저장용 PostgreSQL 추가 |
| 폐쇄망 대응 | Dockerfile의 외부망 빌드 단계에서만 Maven Central/NPM 등을 사용 | 폐쇄망에는 빌드 완료 이미지 tar만 반입하고 `docker load` 후 실행 |
| 보안 | `.env` 기반 평문 | 최소한 암호화 저장, 이후엔 Vault 또는 외부 Secret 연계 고려 |

---

## 2. 기존 실시간 연동 웹서비스 벤치마크 기준 기능 보완

Confluent Control Center, Redpanda Console, Airbyte, Aiven Kafka Connect, NiFi 같은 도구들의 공통 기능을 기준으로 보면, 단순히 "파이프라인 생성/삭제" 만으로는 완성된 서비스가 되기 어렵다.

기존 서비스들의 공통 기능은 다음과 같다.

```text
- Source / Destination 연결 관리
- Connector 또는 Pipeline 생성
- 실행 상태 모니터링
- Consumer Lag / 처리 지연 확인
- 실패 로그 확인
- Connector pause/resume/delete
- Topic / Consumer Group 확인
- 연결 검증
- 동기화 방식 선택: CDC, 증분, 전체 적재
- 실행 이력 / 감사 로그 관리
```

이를 반영해 사용자가 요청한 기능을 다음과 같이 재정리한다.

---

## 3. 확정된 기능 목록

### 3.1 기능 그룹 요약

| 대분류 | 기능 | 1차 MVP 여부 |
|---|---|---|
| 대시보드 | 전체 파이프라인 상태, 오류, 지연 현황 | 포함 |
| 연결정보 관리 | Oracle/PostgreSQL 접속정보 등록, 수정, 삭제, 테스트 | 포함 |
| 테이블 파이프라인 생성 | Source DB → Target DB 실시간 적재 설정 | 포함 |
| 로그 파이프라인 생성 | 특정 로그파일 tailing 후 Kafka Topic 적재 | 2차 포함, 설계는 1차 반영 |
| 배포 관리 | Connector/Agent 설정 생성 및 배포 | 포함 |
| 실행 제어 | 실행, 중지, 일시정지, 재시작, 삭제 | 포함 |
| 모니터링 | Connector 상태, Task 상태, Topic, Consumer Lag, 처리량, 로그 | 포함 |
| 장애/재처리 | DLQ 조회, 실패 원인 확인, 재처리 | 2차 |
| 권한/감사 | 사용자, 역할, 변경 이력 | 2차 |
| 폐쇄망 패키지 | 외부망 이미지 빌드, `docker save`, 폐쇄망 `docker load`, 설치 스크립트 | 포함 |

---

### 3.2 기능 1. 실시간 테이블 적재 파이프라인 생성

#### 기존 요구

```text
소스DB -> 타겟DB를 설정
Oracle, PostgreSQL 두 DB 모두 소스/타겟 가능
대상 테이블 지정
```

#### 상세 기능

```text
1. DB 연결정보 선택
   - Source DB: Oracle / PostgreSQL
   - Target DB: Oracle / PostgreSQL

2. 적재 방식 선택
   - CDC 실시간 적재
   - 초기 Snapshot 포함 여부
   - Snapshot 없이 변경분만 수집
   - 향후 Polling 방식 옵션 추가 가능

3. 대상 테이블 선택
   - Schema 선택
   - Table 선택
   - 다중 테이블 선택
   - PK 존재 여부 확인
   - Supplemental Logging 또는 Replication 설정 확인

4. Topic 설정
   - Topic prefix 자동 생성
   - Topic명 수동 지정
   - Partition 수
   - Retention 설정
   - DLQ Topic 설정

5. 타겟 적재 설정
   - Insert only
   - Upsert
   - Delete 반영 여부
   - Target Schema/Table 지정
   - 컬럼 매핑
   - 타입 매핑 검증

6. 배포 전 검증
   - Source DB 접속 가능 여부
   - Target DB 접속 가능 여부
   - Source Table 존재 여부
   - Target Table 존재 여부 또는 자동 생성 여부
   - PK 존재 여부
   - CDC 권한 확인
   - Kafka Connect Plugin 존재 여부
   - Topic 생성 권한 확인
```

---

### 3.3 기능 2. 로그파일 실시간 적재 파이프라인 생성

#### 기존 요구

```text
특정 로그파일을 읽어 실시간 로그 적재
```

#### 상세 기능

로그파일 적재는 DB CDC와 다르게 Kafka Connect Source Connector 또는 별도 Log Agent가 필요하다. 폐쇄망 이슈와 배포 편의성을 고려하면, 다음 2가지 방식을 지원하는 구조가 적합하다.

| 방식 | 설명 | 추천 단계 |
|---|---|---|
| Kafka Connect File Source 계열 Connector | Connect의 로그파일 Source Connector 배포 | 2차 |
| Log Agent 방식 | Fluent Bit 또는 Custom log-tail-agent가 파일을 읽어 Kafka로 전송 | 2차 추천 |

1차 웹서비스에서는 DB CDC 관리 기능을 먼저 구현하고, 로그파일 적재는 API/DB 설계에 포함하는 선에서 실제 실행은 2차에서 구현한다.

#### 로그 파이프라인 설정 항목

```text
- 파이프라인명
- 수집 서버명
- 파일 경로
- 파일명 패턴
- 읽기 시작 위치: beginning / end
- 로그 포맷: plain / json / regex / delimiter
- 인코딩: UTF-8 / EUC-KR 등
- 멀티라인 로그 여부
- Kafka Topic명
- 실패 로그 저장 여부
- Agent 배포 대상
```

---

### 3.4 기능 3. 생성된 파이프라인 배포

#### 기존 표현

```text
생성된 파이프라인 소스를 배포
```

#### 수정 표현

```text
생성된 파이프라인의 실행 엔진에 배포
```

이유는 실제 배포 대상이 Source DB가 아니라 실행 엔진이기 때문이다.

```text
- Kafka Connect
- Log Agent
- 향후 Kubernetes Job / Docker Container
```

#### 배포 방식

| 파이프라인 유형 | 배포 대상 | 배포 방식 |
|---|---|---|
| Oracle Source CDC | Kafka Connect | Debezium Oracle Connector config 생성 후 REST API 등록 |
| PostgreSQL Source CDC | Kafka Connect | Debezium PostgreSQL Connector config 생성 후 REST API 등록 |
| Oracle Target Sink | Kafka Connect | JDBC Sink Connector config 생성 후 REST API 등록 |
| PostgreSQL Target Sink | Kafka Connect | JDBC Sink Connector config 생성 후 REST API 등록 |
| 로그파일 수집 | Log Agent 또는 Kafka Connect | Agent config 생성/배포 또는 Connector config 등록 |

---

### 3.5 기능 4. 파이프라인 모니터링

#### 기존 요구

```text
각 파이프라인 모니터링 기능, 로그정보 등
```

#### 상세 기능

```text
1. 파이프라인 상태
   - CREATED
   - VALIDATED
   - DEPLOYING
   - DEPLOYED
   - RUNNING
   - PAUSED
   - STOPPING
   - STOPPED
   - FAILED
   - DELETED

2. Kafka Connect 상태
   - Connector state
   - Task state
   - Task failure trace
   - Connector config

3. Kafka 상태
   - Topic 존재 여부
   - Topic partition 수
   - Consumer Group
   - Consumer Lag
   - latest offset
   - committed offset

4. 처리 진행
   - 누적 처리 건수
   - 분당 처리 건수
   - 초당 처리량
   - 마지막 이벤트 시각
   - 마지막 적재 시각
   - 오류 건수

5. 로그정보
   - Connector 에러 메시지
   - 최근 실행 명령 이력
   - 배포 이력
   - API 요청 이력
```

---

### 3.6 기능 5. 파이프라인 관리

#### 기존 요구

```text
파이프라인 조회, 생성, 수정, 삭제
파이프라인 실행, 취소, 중지
```

#### 상세 기능

```text
- 조회
- 생성
- 수정
- 검증
- 배포
- 실행
- 일시정지
- 재시작
- 중지
- 삭제
- 강제 삭제
- 배포 이력 조회
- 설정 비교
- 재배포
```

Kafka Connect 기준으로는 `start`라는 API가 따로 있는 것이 아니라, Connector 등록 또는 resume이 실행에 해당한다. 따라서 웹 UI에서는 사용자 친화적으로 "실행"이라고 표시하되, 내부적으로는 아래처럼 매핑한다.

| UI 버튼 | 내부 동작 |
|---|---|
| 배포 | `PUT /connectors/{name}/config` |
| 실행 | 신규 Connector 등록 또는 `resume` |
| 일시정지 | `PUT /connectors/{name}/pause` |
| 재시작 | task restart 또는 connector 재등록 |
| 중지 | pause 또는 delete 정책 선택 |
| 삭제 | `DELETE /connectors/{name}` + metadata 상태 변경 |
| 취소 | DEPLOYING 상태에서 작업 중단 또는 rollback |

---

## 4. 목표 시스템 아키텍처

### 4.1 전체 구조

```text
[사용자 브라우저]
        ↓
[Kafka Web UI - React]
        ↓ REST API
[Pipeline API - Spring Boot]
        ↓
[Metadata DB - PostgreSQL]
        ↓
 ┌─────────────────────────────────────────┐
 │ 실행 엔진 제어                            │
 │ - Kafka Connect REST API                 │
 │ - Kafka AdminClient                      │
 │ - Log Agent Manager, 2차                  │
 └─────────────────────────────────────────┘
        ↓
[Kafka / Kafka Connect]
        ↓
[Oracle / PostgreSQL Source & Target]

[NiFi]
- 기존 구성 유지
- 웹서비스 1차 범위에서는 직접 제어하지 않음
- 별도 운영 UI로 유지
```

---

### 4.2 현재 프로젝트에 추가할 디렉터리 구조

기존 프로젝트를 유지하면서 아래 구조를 추가한다.

```text
data-pipeline/
├── docker-compose.yml                 # 기존 유지, web profile 추가
├── kafka-connect/                     # 기존 유지, Oracle target 지원 보강
├── nifi/                              # 기존 유지
├── db/                                # 기존 유지
├── web/
│   ├── backend/
│   │   ├── build.gradle
│   │   └── src/main/java/com/company/pipeline/
│   │       ├── PipelineWebApplication.java
│   │       ├── connection/
│   │       ├── pipeline/
│   │       ├── connector/
│   │       ├── kafka/
│   │       ├── monitoring/
│   │       ├── security/
│   │       └── common/
│   └── frontend/
│       ├── package.json
│       └── src/
│           ├── pages/
│           ├── components/
│           ├── api/
│           ├── hooks/
│           └── types/
├── metadata-db/
│   └── init/
│       └── 01_schema.sql
├── offline/
│   ├── image-manifest.json
│   ├── save-images.sh
│   ├── load-images.sh
│   └── install-offline.sh
└── docs/
    ├── kafka-webservice-design.md
    ├── api-spec.md
    ├── screen-spec.md
    └── offline-install-guide.md
```

---

## 5. 기술 스택

### 5.1 Backend

| 항목 | 추천 |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot 3 |
| API | Spring Web |
| DB Access | Spring Data JPA + QueryDSL 선택 |
| Metadata DB | PostgreSQL |
| Kafka 제어 | Kafka AdminClient |
| Kafka Connect 제어 | WebClient 또는 RestClient |
| 인증 | 1차: 단순 로그인 또는 Basic Auth, 2차: JWT/RBAC |
| 빌드 | Gradle |

---

### 5.2 Frontend

| 항목 | 추천 |
|---|---|
| Framework | React + TypeScript |
| Build | Vite |
| UI | Ant Design 또는 MUI |
| API Client | Axios |
| 서버 상태 | TanStack Query |
| Flow 시각화 | React Flow, 2차 |
| 배포 | Nginx 기반 정적 이미지 |

---

### 5.3 Infra

| 항목 | 현재/추천 |
|---|---|
| Kafka | 기존 `apache/kafka:3.8.0` 유지 |
| Kafka Connect | 기존 custom image 유지, 플러그인 보강 |
| NiFi | 기존 유지 |
| Metadata DB | PostgreSQL 16 추가 |
| 폐쇄망 배포 | 외부망 사전 빌드 이미지 tar 반입 후 docker load |

---

## 6. Backend 주요 컴포넌트 설계

### 6.1 패키지 구조

```text
com.company.pipeline
├── connection
│   ├── ConnectionController
│   ├── ConnectionService
│   ├── ConnectionInfo
│   └── ConnectionTestService
├── pipeline
│   ├── PipelineController
│   ├── PipelineService
│   ├── PipelineDefinition
│   ├── PipelineDeployService
│   └── PipelineStateMachine
├── connector
│   ├── KafkaConnectClient
│   ├── ConnectorConfigRenderer
│   ├── DebeziumOracleTemplate
│   ├── DebeziumPostgresTemplate
│   └── JdbcSinkTemplate
├── kafka
│   ├── KafkaAdminService
│   ├── TopicService
│   └── ConsumerLagService
├── monitoring
│   ├── MonitoringController
│   ├── MetricCollector
│   ├── ConnectorStatusCollector
│   └── PipelineLogService
├── offline
│   └── ImageManifestService
└── common
    ├── ApiResponse
    ├── ErrorCode
    └── GlobalExceptionHandler
```

---

### 6.2 KafkaConnectClient 역할

Backend는 Kafka Connect REST API를 직접 호출한다.

```text
- Connector 목록 조회
- Connector config 조회
- Connector 생성/수정
- Connector 상태 조회
- Connector pause
- Connector resume
- Connector delete
- Connector task restart
- Connector plugin 목록 조회
- Connector config validate
```

---

### 6.3 ConnectorConfigRenderer 역할

사용자가 화면에서 입력한 값을 기반으로 Connector JSON을 생성한다.

```text
입력:
- Source DB type
- Source connection
- Target DB type
- Target connection
- Source schema/table
- Target schema/table
- Topic prefix
- Snapshot mode
- Delete handling

출력:
- Source Connector config JSON
- Sink Connector config JSON
```

Connector 이름 규칙은 다음처럼 통일한다.

```text
Source Connector:
source-{pipelineId}-{sourceDbType}-{schema}-{table}

Sink Connector:
sink-{pipelineId}-{targetDbType}-{schema}-{table}

Topic:
{topicPrefix}.{sourceSchema}.{sourceTable}
```

예시:

```text
source-12-oracle-appuser-customers
sink-12-postgres-cdc_landing-customers
oracle-cdc.APPUSER.CUSTOMERS
```

---

## 7. Metadata DB 설계

### 7.1 pipeline_connection

DB 접속정보를 저장한다. 비밀번호는 평문 저장 금지다.

```sql
CREATE TABLE pipeline_connection (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    db_type VARCHAR(30) NOT NULL, -- ORACLE, POSTGRESQL
    host VARCHAR(200) NOT NULL,
    port INTEGER NOT NULL,
    database_name VARCHAR(100),
    service_name VARCHAR(100),
    schema_name VARCHAR(100),
    username VARCHAR(100) NOT NULL,
    encrypted_password TEXT NOT NULL,
    jdbc_url TEXT,
    status VARCHAR(30) DEFAULT 'UNKNOWN',
    last_tested_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
```

---

### 7.2 pipeline_definition

```sql
CREATE TABLE pipeline_definition (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    pipeline_type VARCHAR(50) NOT NULL, -- TABLE_CDC, LOG_FILE
    source_connection_id BIGINT,
    target_connection_id BIGINT,
    source_db_type VARCHAR(30),
    target_db_type VARCHAR(30),
    source_schema VARCHAR(100),
    source_table VARCHAR(100),
    target_schema VARCHAR(100),
    target_table VARCHAR(100),
    topic_name VARCHAR(200),
    status VARCHAR(30) DEFAULT 'CREATED',
    snapshot_mode VARCHAR(50),
    insert_enabled BOOLEAN DEFAULT true,
    update_enabled BOOLEAN DEFAULT true,
    delete_enabled BOOLEAN DEFAULT true,
    description TEXT,
    created_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
```

---

### 7.3 pipeline_connector

```sql
CREATE TABLE pipeline_connector (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id BIGINT NOT NULL,
    connector_role VARCHAR(30) NOT NULL, -- SOURCE, SINK
    connector_name VARCHAR(200) NOT NULL,
    connector_class VARCHAR(300) NOT NULL,
    connector_config_json TEXT NOT NULL,
    connect_cluster_url VARCHAR(300) DEFAULT 'http://kafka-connect:8083',
    status VARCHAR(30) DEFAULT 'CREATED',
    last_status_json TEXT,
    deployed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
```

---

### 7.4 pipeline_command_history

```sql
CREATE TABLE pipeline_command_history (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id BIGINT NOT NULL,
    command VARCHAR(50) NOT NULL, -- VALIDATE, DEPLOY, START, PAUSE, STOP, RESTART, DELETE
    result VARCHAR(30),
    message TEXT,
    requested_by VARCHAR(100),
    requested_at TIMESTAMP DEFAULT now(),
    completed_at TIMESTAMP
);
```

---

### 7.5 pipeline_metric_snapshot

```sql
CREATE TABLE pipeline_metric_snapshot (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id BIGINT NOT NULL,
    collected_at TIMESTAMP DEFAULT now(),
    connector_state VARCHAR(30),
    task_state VARCHAR(30),
    topic_name VARCHAR(200),
    partition_count INTEGER,
    end_offset BIGINT,
    committed_offset BIGINT,
    consumer_lag BIGINT,
    error_count BIGINT DEFAULT 0,
    last_error_message TEXT
);
```

---

### 7.6 log_pipeline_source

로그파일 적재 2차 기능을 위한 테이블이다.

```sql
CREATE TABLE log_pipeline_source (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id BIGINT NOT NULL,
    agent_host VARCHAR(200),
    file_path TEXT NOT NULL,
    file_pattern VARCHAR(200),
    read_from VARCHAR(30) DEFAULT 'END', -- BEGINNING, END
    parse_type VARCHAR(30) DEFAULT 'PLAIN', -- PLAIN, JSON, REGEX, DELIMITER
    encoding VARCHAR(30) DEFAULT 'UTF-8',
    multiline_enabled BOOLEAN DEFAULT false,
    topic_name VARCHAR(200) NOT NULL,
    created_at TIMESTAMP DEFAULT now()
);
```

---

## 8. API 설계

### 8.1 연결정보 API

```http
GET    /api/connections
POST   /api/connections
GET    /api/connections/{id}
PUT    /api/connections/{id}
DELETE /api/connections/{id}
POST   /api/connections/{id}/test
GET    /api/connections/{id}/schemas
GET    /api/connections/{id}/schemas/{schema}/tables
GET    /api/connections/{id}/schemas/{schema}/tables/{table}/columns
```

---

### 8.2 파이프라인 API

```http
GET    /api/pipelines
POST   /api/pipelines
GET    /api/pipelines/{id}
PUT    /api/pipelines/{id}
DELETE /api/pipelines/{id}
POST   /api/pipelines/{id}/validate
POST   /api/pipelines/{id}/deploy
POST   /api/pipelines/{id}/start
POST   /api/pipelines/{id}/pause
POST   /api/pipelines/{id}/stop
POST   /api/pipelines/{id}/restart
POST   /api/pipelines/{id}/cancel
```

---

### 8.3 모니터링 API

```http
GET /api/dashboard/summary
GET /api/pipelines/{id}/status
GET /api/pipelines/{id}/metrics
GET /api/pipelines/{id}/logs
GET /api/pipelines/{id}/errors
GET /api/kafka/topics
GET /api/kafka/topics/{topicName}/offsets
GET /api/kafka/consumer-groups
GET /api/kafka/consumer-groups/{groupId}/lag
GET /api/connect/connectors
GET /api/connect/connectors/{connectorName}/status
```

---

### 8.4 폐쇄망 이미지 관리 API, 선택 기능

```http
GET  /api/system/images
GET  /api/system/version
GET  /api/system/health
POST /api/system/preflight-check
```

---

## 9. 화면 설계

### 9.1 대시보드

구성 요소:

```text
- 전체 파이프라인 수
- RUNNING 수
- FAILED 수
- PAUSED 수
- 지연 발생 수
- 최근 오류 목록
- Kafka Connect 상태
- Kafka Broker 상태
- 최근 배포 이력
```

---

### 9.2 연결정보 관리 화면

목록 컬럼:

```text
연결명 | DB 유형 | Host | Port | DB/Service | Schema | 상태 | 마지막 테스트 | 관리
```

버튼:

```text
신규 등록
수정
삭제
연결 테스트
Schema 조회
```

---

### 9.3 파이프라인 생성 화면

Wizard 방식으로 구현한다.

```text
Step 1. 파이프라인 유형 선택
  - 테이블 CDC 파이프라인
  - 로그파일 적재 파이프라인

Step 2. Source 설정
  - Source DB 선택
  - Source Connection
  - Schema
  - Table

Step 3. Target 설정
  - Target DB 선택
  - Target Connection
  - Target Schema
  - Target Table
  - Insert/Update/Delete 반영 여부

Step 4. Kafka 설정
  - Topic Prefix
  - Topic Name
  - Partition
  - Retention
  - DLQ 사용 여부

Step 5. 검증
  - DB 연결
  - 테이블 존재
  - PK 존재
  - Connector plugin 존재
  - 권한 점검

Step 6. 저장/배포
  - 저장만 하기
  - 저장 후 배포
  - 저장 후 즉시 실행
```

---

### 9.4 파이프라인 상세 화면

탭 구성:

```text
기본정보
Connector 설정
실행상태
Kafka Topic
로그
이력
오류
```

주요 버튼:

```text
검증
배포
실행
일시정지
중지
재시작
삭제
```

---

## 10. 파이프라인 상태 제어

```text
CREATED
  → VALIDATED
  → DEPLOYING
  → DEPLOYED
  → RUNNING
  → PAUSED
  → RUNNING
  → STOPPING
  → STOPPED

어느 단계에서든 오류 발생:
  → FAILED

삭제:
  → DELETING
  → DELETED
```

---

## 11. Connector 생성 정책

### 11.1 Oracle Source → Kafka

현재 프로젝트의 `oracle-cdc-source.json.template`를 동적 생성 방식으로 변경한다.

현재 고정값:

```json
"schema.include.list": "APPUSER",
"table.include.list": "APPUSER.CUSTOMERS,APPUSER.CLOB_TEST_DATA"
```

변경 방향:

```json
"schema.include.list": "${sourceSchema}",
"table.include.list": "${sourceSchema}.${sourceTable}"
```

---

### 11.2 PostgreSQL Source → Kafka

신규 템플릿 추가:

```text
kafka-connect/connectors/postgres-cdc-source.json.template
```

필수 설정 항목:

```text
connector.class=io.debezium.connector.postgresql.PostgresConnector
plugin.name=pgoutput
database.hostname
database.port
와 database/dbname 관련 설정
database.user
schema.include.list
table.include.list
topic.prefix
slot.name
publication.name
```

PostgreSQL CDC는 DB에서 `wal_level=logical`, replication 권한, publication/slot 설정이 필요하다.

---

### 11.3 Kafka → PostgreSQL Target

현재 `postgres-cdc-sink.json.template`를 유지하되, 하드코딩을 제거한다.

현재:

```json
"connection.url": "jdbc:postgresql://target-db:5432/${TARGET_DB_NAME}",
"topics": "oracle-cdc.APPUSER.CUSTOMERS",
"table.name.format": "cdc_landing.customers"
```

변경:

```json
"connection.url": "jdbc:postgresql://${targetHost}:${targetPort}/${targetDatabase}",
"topics": "${topicName}",
"table.name.format": "${targetSchema}.${targetTable}"
```

---

### 11.4 Kafka → Oracle Target

신규 템플릿 추가:

```text
kafka-connect/connectors/oracle-cdc-sink.json.template
```

설정 보강:

현재 `kafka-connect/Dockerfile`은 Debezium JDBC Sink 플러그인 경로에 PostgreSQL JDBC Driver만 추가한다. Oracle 타겟을 지원하려면 **Debezium JDBC Sink 플러그인 경로에도 `ojdbc11`을 추가**해야 한다.

수정 보강 방향:

```dockerfile
RUN curl -fsSL -o /usr/share/confluent-hub-components/debezium-debezium-connector-jdbc/ojdbc11-${OJDBC_VERSION}.jar \
    "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/${OJDBC_VERSION}/ojdbc11-${OJDBC_VERSION}.jar"
```

폐쇄망 서버에서는 이 다운로드를 수행하지 않는다. Oracle 타겟 지원에 필요한 `ojdbc11` 포함 작업은 **외부망 이미지 빌드 작업 시점에 완료**하고, 폐쇄망에는 완성된 `kafka-connect` 이미지만 tar로 반입한다.

---

## 12. 로그파일 적재 설계

### 12.1 권장 방식

1차에서는 DB CDC 웹서비스를 우선 구현하고, 로그파일 적재는 2차로 구현한다. 다만 설계와 DB 모델은 1차에 포함한다.

추천 구조:

```text
[로그 파일 서버]
  → [Log Agent]
  → [Kafka Topic]
  → [Consumer 또는 Target Sink]
```

Log Agent 후보:

```text
- Fluent Bit
- Filebeat
- Custom Spring Boot log-tail-agent
- Kafka Connect File Source 계열 Connector
```

폐쇄망에서는 외부 플러그인 의존성이 적고 실제 파일을 작성하기 쉬운 **Custom log-tail-agent** 또는 **Fluent Bit 이미지 사전 반입 방식**이 현실적이다.

---

## 13. 폐쇄망 배포 설계, 핵심

### 13.1 최종 배포 원칙

폐쇄망 배포 방식은 **외부망에서 모든 Docker 이미지를 빌드한 뒤 이미지 tar 파일로 반입하는 방식**으로 통일한다.

따라서 폐쇄망 서버에서는 다음 작업을 하지 않는다.

```text
- docker compose build
- Maven Central 다운로드
- npm install / npm ci
- Gradle dependency 다운로드
- Debezium Connector 다운로드
- JDBC Driver 다운로드
- 외부 Docker Registry pull
```

폐쇄망 서버에서는 다음 작업만 수행한다.

```text
1. 외부망에서 전달받은 이미지 tar 파일 반입
2. docker load 실행
3. .env.closed 작성
4. docker compose up -d 실행
5. healthcheck.sh로 기동 확인
```

---

### 13.2 외부망 빌드 서버 작업 흐름

외부망 개발 PC 또는 빌드 서버에서 최종 릴리스 이미지를 생성한다.

```bash
# 1. 소스 최신화
git clone <gitlab-url>/data-pipeline.git
cd data-pipeline

# 2. 릴리스 버전 지정
export APP_VERSION=1.0.0

# 3. 외부망에서 전체 이미지 빌드
# kafka-connect, nifi, pipeline-api, pipeline-ui 등 커스텀 이미지를 모두 빌드한다.
docker compose -f docker-compose.yml build

# 4. 빌드 결과 확인
docker images | grep data-pipeline
```

외부망 빌드 작업 시에는 기존 Dockerfile의 `curl`, Maven, npm, Gradle 다운로드를 사용할 수 있다. 단, 이 다운로드는 **외부망 빌드에서만 허용**되며 폐쇄망에서는 수행하지 않는다.

---

### 13.3 이미지 목록 고정

릴리스에 포함할 이미지는 manifest로 고정한다.

예시 `offline/image-manifest.json`:

```json
{
  "releaseVersion": "1.0.0",
  "createdAt": "YYYY-MM-DD",
  "images": [
    "data-pipeline/kafka-connect:1.0.0",
    "data-pipeline/nifi:1.0.0",
    "data-pipeline/pipeline-api:1.0.0",
    "data-pipeline/pipeline-ui:1.0.0",
    "apache/kafka:3.8.0",
    "postgres:16-alpine"
  ]
}
```

Oracle POC DB, Target DB, Kafbat UI, Redis 같은 선택 이미지가 compose에 포함될 경우 manifest에 반드시 추가한다.

---

### 13.4 이미지 저장 스크립트

외부망에서 `offline/save-images.sh`를 실행해 하나의 tar 파일을 생성한다.

```bash
#!/usr/bin/env bash
set -euo pipefail

APP_VERSION=${APP_VERSION:-1.0.0}
RELEASE_DIR="release/data-pipeline-${APP_VERSION}"
IMAGE_TAR="${RELEASE_DIR}/images/data-pipeline-images-${APP_VERSION}.tar"

mkdir -p "${RELEASE_DIR}/images"

IMAGES=(
  "data-pipeline/kafka-connect:${APP_VERSION}"
  "data-pipeline/nifi:${APP_VERSION}"
  "data-pipeline/pipeline-api:${APP_VERSION}"
  "data-pipeline/pipeline-ui:${APP_VERSION}"
  "apache/kafka:3.8.0"
  "postgres:16-alpine"
)

docker save "${IMAGES[@]}" -o "${IMAGE_TAR}"

echo "Saved images to ${IMAGE_TAR}"
```

이미지 tar가 생성되면 함께 반입할 compose, env sample, 문서, 설치 스크립트를 같은 release 폴더에 모은다.

---

### 13.5 폐쇄망 반입 패키지 구조

최종 폐쇄망 반입 패키지는 다음 구조로 고정한다.

```text
data-pipeline-release-YYYYMMDD/
├── images/
│   └── data-pipeline-images-1.0.0.tar
├── compose/
│   ├── docker-compose.yml
│   ├── .env.closed.sample
│   └── .env.closed
├── scripts/
│   ├── load-images.sh
│   ├── install.sh
│   ├── start.sh
│   ├── stop.sh
│   └── healthcheck.sh
├── docs/
│   ├── offline-install-guide.md
│   ├── operation-guide.md
│   └── troubleshooting.md
└── manifest.json
```

반입 패키지에는 소스 전체나 Dockerfile이 포함될 수 있지만, 폐쇄망에서 빌드하지 않는 것을 원칙으로 한다. 원칙적으로는 `images/*.tar`, `compose/*.yml`, `.env.closed`, `scripts/*.sh`만으로 서비스를 기동할 수 있어야 한다.

---

### 13.6 폐쇄망 서버 설치 절차

폐쇄망 서버에서 실행하는 절차는 다음으로 고정한다.

```bash
# 1. 패키지 압축 해제
cd /opt
tar -xzf data-pipeline-release-YYYYMMDD.tar.gz
cd data-pipeline-release-YYYYMMDD

# 2. 이미지 로드
bash scripts/load-images.sh

# 3. 환경파일 작성
cp compose/.env.closed.sample compose/.env.closed
vi compose/.env.closed

# 4. 서비스 기동
bash scripts/install.sh

# 5. 상태 확인
bash scripts/healthcheck.sh
```

`load-images.sh` 예시:

```bash
#!/usr/bin/env bash
set -euo pipefail

docker load -i images/data-pipeline-images-1.0.0.tar
docker images | grep -E 'data-pipeline|apache/kafka|postgres'
```

`install.sh` 예시:

```bash
#!/usr/bin/env bash
set -euo pipefail

cd compose
docker compose --env-file .env.closed up -d
```

---

### 13.7 폐쇄망 운영 원칙

```text
- 폐쇄망에서는 docker pull 금지
- 폐쇄망에서는 docker compose build 금지
- 폐쇄망에서는 Maven/NPM/Gradle 외부 다운로드 금지
- 신규 버전 배포 시 외부망에서 다시 빌드 후 이미지 tar 재반입
- 이미지 태그는 latest 금지, APP_VERSION 기반 고정 태그 사용
- 반입 전 manifest.json과 docker images 결과를 대조
- 설치 후 healthcheck.sh 결과를 운영 승인 기준으로 사용
```

---

### 13.8 현재 Dockerfile 관련 정리

현재 프로젝트의 Dockerfile이 Maven Central, Debezium 릴리스, JDBC Driver를 다운로드하는 구조라면 수정하지 않아도 된다. 단, 그 Dockerfile은 **외부망 이미지 빌드 단계에서만 실행**된다.

폐쇄망에서 Dockerfile을 실행하지 않기 때문에, 폐쇄망 대응을 위해 Dockerfile을 반드시 `COPY offline-artifacts` 방식으로 바꿀 필요는 없다.

다만 재현성과 장기 유지보수를 위해 외부망 빌드 서버에서는 다음을 고정한다.

```text
- Debezium Connector 버전
- Oracle JDBC Driver 버전
- PostgreSQL JDBC Driver 버전
- Kafka 이미지 버전
- NiFi 이미지 버전
- Backend JDK 버전
- Frontend Node 버전
```

버전 고정 정보는 `manifest.json`과 `docs/offline-install-guide.md`에 함께 기록한다.

---

## 14. docker-compose 확장 설계

기존 compose에 아래 서비스를 추가한다.

```yaml
metadata-db:
  image: postgres:16-alpine
  container_name: metadata-db
  environment:
    POSTGRES_DB: pipeline_meta
    POSTGRES_USER: pipeline_admin
    POSTGRES_PASSWORD: ChangeMe_Meta_2026!
  volumes:
    - metadata-db-data:/var/lib/postgresql/data
    - ./metadata-db/init:/docker-entrypoint-initdb.d
  networks: [pipeline-net]

pipeline-api:
  image: data-pipeline/pipeline-api:${APP_VERSION}
  container_name: pipeline-api
  depends_on:
    kafka-connect:
      condition: service_healthy
    metadata-db:
      condition: service_started
  environment:
    SPRING_PROFILES_ACTIVE: docker
    METADATA_DB_URL: jdbc:postgresql://metadata-db:5432/pipeline_meta
    KAFKA_CONNECT_URL: http://kafka-connect:8083
    KAFKA_BOOTSTRAP_SERVERS: kafka:9092
  ports:
    - "18080:8080"
  networks: [pipeline-net]

pipeline-ui:
  image: data-pipeline/pipeline-ui:${APP_VERSION}
  container_name: pipeline-ui
  depends_on:
    - pipeline-api
  ports:
    - "13000:80"
  networks: [pipeline-net]
```

---

## 15. VS Code 기반 바이브코딩 개발 계획

### 15.1 기본 원칙

```text
- 한 번에 전체를 만들지 않는다.
- Backend CRUD → Kafka Connect 연동 → UI → 모니터링 순서로 진행한다.
- NiFi 관련 파일은 수정하지 않는다.
- 기존 docker-compose.yml은 백엔드 반영 web profile 방식으로 확장한다.
- 실제 비밀번호는 커밋하지 않는다.
```

---

### 15.2 추천 VS Code 확장

```text
- WSL
- Extension Pack for Java
- Spring Boot Extension Pack
- Gradle for Java
- Docker
- GitLab Workflow
- ESLint
- Prettier
- REST Client 또는 Thunder Client
- YAML
```

---

### 15.3 개발 단계별 AI 코딩 프롬프트

#### Step 1. Backend 프로젝트 생성

```text
현재 data-pipeline 프로젝트의 web/backend 폴더에 Spring Boot 3 + Java 21 + Gradle 프로젝트를 만들어줘.
패키지는 com.company.pipeline로 하고, PostgreSQL metadata DB를 사용하는 기본 설정을 만들어줘.
NiFi 관련 파일은 수정하지 마.
```

#### Step 2. Metadata Entity 생성

```text
pipeline_connection, pipeline_definition, pipeline_connector, pipeline_command_history, pipeline_metric_snapshot 테이블 기준으로 JPA Entity, Repository, DTO, Service, Controller를 만들어줘.
CRUD API를 우선 구현하고, 비밀번호는 encrypted_password 필드로만 저장하게 해줘.
```

#### Step 3. DB 연결 테스트 기능

```text
Oracle/PostgreSQL 연결정보를 받아 JDBC 연결 테스트를 수행하는 ConnectionTestService를 만들어줘.
Oracle은 service_name 기반 URL, PostgreSQL은 database_name 기반 URL을 생성하게 해줘.
```

#### Step 4. Kafka Connect Client 구현

```text
Spring WebClient로 Kafka Connect REST API를 호출하는 KafkaConnectClient를 만들어줘.
기능은 connector 목록, plugin 목록, config validate, create/update, status, pause, resume, delete야.
```

#### Step 5. Connector Config Renderer 구현

```text
사용자가 입력한 PipelineDefinition 기준으로 Debezium Oracle Source, Debezium PostgreSQL Source, JDBC Sink 설정 JSON을 생성하는 ConnectorConfigRenderer를 만들어줘.
현재 kafka-connect/connectors/*.template의 설정을 참고하되, schema/table/topic/target DB는 동적으로 넣어줘.
```

#### Step 6. Pipeline Deploy Service 구현

```text
PipelineDeployService를 만들어줘.
validate → connector config 생성 → Kafka Connect에 source/sink connector 등록 → 상태 조회 → metadata 상태 업데이트 순서로 동작하게 해줘.
실패 시 pipeline 상태를 FAILED로 바꾸고 command_history에 에러 메시지를 저장해줘.
```

#### Step 7. Frontend 프로젝트 생성

```text
web/frontend에 React + TypeScript + Vite 프로젝트를 만들어줘.
페이지는 Dashboard, Connections, Pipelines, PipelineDetail을 만들고, API 호출은 axios 기반으로 분리해줘.
```

#### Step 8. 파이프라인 생성 Wizard UI

```text
파이프라인 생성 Wizard 화면을 만들어줘.
Step은 유형 선택, Source 선택, Target 선택, Kafka 설정, 검증/배포 순서야.
Oracle/PostgreSQL 선택에 따라 입력 필드가 달라지게 해줘.
```

#### Step 9. 모니터링 화면

```text
Pipeline 상세 화면에 Connector 상태, Task 상태, Topic offset, Consumer Lag, 최근 오류 메시지를 보여주는 Monitoring 탭을 추가해줘.
```

#### Step 10. 폐쇄망 패키지 스크립트

```text
offline 폴더에 save-images.sh, load-images.sh, install.sh, healthcheck.sh를 만들어줘.
폐쇄망 배포 방식은 외부망에서 이미지를 빌드한 뒤 docker save로 tar를 만들고, 폐쇄망에서 docker load 후 docker compose up -d로 실행하는 방식으로 고정해줘.
폐쇄망에서는 docker compose build, npm install, gradle dependency 다운로드, docker pull을 하지 않게 설계해줘.
```

---

## 16. 개발 우선순위

### 1차 MVP

```text
1. Metadata DB 추가
2. Backend API 추가
3. Frontend UI 추가
4. 연결정보 관리
5. Kafka Connect 상태 조회
6. 테이블 CDC 파이프라인 생성
7. Connector 배포/중지/삭제
8. 기본 대시보드
9. 폐쇄망 이미지 save/load 스크립트
```

### 2차

```text
1. PostgreSQL Source CDC 지원
2. Oracle Target Sink 지원
3. 로그파일 적재 Agent 연동
4. DLQ 조회
5. 재처리 기능
6. 사용자/권한 관리
7. 알림 기능
```

### 3차

```text
1. Flow 시각화
2. 컬럼 매핑 고도화
3. 적합성 검증 자동화
4. 파이프라인 버전 관리/롤백
5. 이미지 릴리스/버전 관리 자동화
```

---

## 17. 2인 개발 역할 분담

### 담당자 A: Backend / Kafka Connect

```text
- Spring Boot Backend 구성
- Metadata DB 설계/구현
- KafkaConnectClient 구현
- ConnectorConfigRenderer 구현
- PipelineDeployService 구현
- 폐쇄망 이미지 패키지 스크립트
```

### 담당자 B: Frontend / 화면 / 테스트

```text
- React Frontend 구성
- 대시보드 화면
- 연결정보 관리 화면
- 파이프라인 생성 Wizard
- 파이프라인 상세/모니터링 화면
- 화면 테스트 및 운영 문서 작성
```

공통 작업:

```text
- API 명세 확정
- DB 컬럼 명세
- Connector 설정 테스트
- 폐쇄망 설치 리허설
```

---

## 18. 구현 시 주의사항

### 18.1 기존 NiFi 유지

```text
- nifi/Dockerfile 수정 최소화
- nifi/FLOW_RUNBOOK.md 유지
- 웹서비스에서 NiFi Flow를 직접 제어하지 않음
- 확장 시 2차에서 NiFi 상태 링크만 제공
```

### 18.2 Connector 템플릿 고정값 제거

아래 값은 웹 입력값으로 동적 생성해야 한다.

```text
schema.include.list
table.include.list
topics
table.name.format
connection.url
connection.username
connection.password
```

### 18.3 폐쇄망 대응

```text
- Dockerfile의 curl/Maven/NPM 다운로드는 외부망 빌드 단계에서만 허용하고, 폐쇄망에서는 이미지 빌드를 수행하지 않음
- npm install, gradle dependency download, connector/JDBC 다운로드는 외부망에서 완료 후 이미지화
- 이미지 tar에 포함된 모든 이미지명/태그/버전 manifest 작성
- 폐쇄망에서는 load-images.sh → install.sh → healthcheck.sh 순서로 실행 가능하게 구성
```

### 18.4 보안

```text
- .env 커밋 금지
- DB 비밀번호 화면 노출 금지
- Connector config 조회 시 password masking
- API 요청/응답 로그에 비밀번호 남기지 않기
- 이후 단계에서는 사용자 권한 분리
```

---

## 19. 최종 결론

현재 프로젝트는 Kafka, Kafka Connect, NiFi 기반 실시간 파이프라인 POC로는 기반이 충분하다. 다만 웹서비스로 전환하려면 **정형 Connector 템플릿을 동적으로 생성/배포하는 Backend API와 이를 조작하는 UI**가 필요하다.

최종 설계 방향은 다음과 같다.

```text
- NiFi는 유지한다.
- Kafka/Kafka Connect 영역만 웹서비스화한다.
- 웹서비스는 파이프라인 메타데이터를 저장하고 Kafka Connect REST API를 제어한다.
- Oracle/PostgreSQL 모두 Source/Target으로 확장한다.
- 로그파일 적재는 2차 기능으로 Log Agent 또는 Kafka Connect Source 방식으로 구현한다.
- 폐쇄망 배포는 외부망에서 사전 빌드한 Docker 이미지 tar를 반입한 뒤 `docker load`로 적재하는 방식으로 통일한다.
```

1차 MVP는 다음 범위로 진행하는 것이 가장 현실적이다.

```text
DB 연결정보 관리
테이블 CDC 파이프라인 생성
Kafka Connect 배포/중지/삭제
Connector 상태 모니터링
기본 대시보드
폐쇄망 이미지 패키지
```
