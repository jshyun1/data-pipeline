# CDC 파이프라인 및 Kafka Topic 질의응답 정리

- 작성일: 2026-07-23
- 대상 메뉴: `CDC > 파이프라인`
- 기준: 현재 `data-pipeline` 프로젝트 구현

## 1. 파이프라인을 신규 생성하면 Kafka Topic과 Connector가 바로 생성되는가?

아니다. 현재 구현에서는 **신규 생성**과 **배포**가 분리되어 있다.

```text
신규 생성
  → 파이프라인 정의를 Metadata DB에 저장
  → 상태: CREATED
  → Kafka Connect Connector: 생성되지 않음
  → Kafka Topic: 명시적으로 생성되지 않음

배포
  → Source Connector 등록
  → Sink Connector 등록
  → Connector 실행 과정에서 Kafka Topic 사용 및 자동 생성
  → 상태: DEPLOYED
```

### 1.1 신규 생성 시 처리 내용

화면에서 신규 생성 양식을 제출하면 다음 API가 호출된다.

```http
POST /api/pipelines
```

백엔드는 다음 작업만 수행한다.

1. Source 및 Target 연결정보가 존재하는지 확인한다.
2. Oracle이 Source라면 CDC 계정이 `C##`로 시작하는 공통 사용자인지 확인한다.
3. Source/Target 스키마와 테이블, Topic Prefix 등의 파이프라인 정의를 저장한다.
4. 파이프라인 상태를 `CREATED`로 저장한다.
5. 빈 Connector 목록을 반환한다.

이 단계에서는 Kafka Connect REST API를 호출하지 않는다. 화면에서도 생성 완료 후 `파이프라인을 생성했습니다. 이제 배포하세요.`라고 안내한다.

예를 들어 다음과 같이 입력했다고 가정한다.

```text
Topic Prefix: oracle-cdc
Source Schema: APPUSER
Source Table: CUSTOMERS
```

Metadata DB의 파이프라인 정의에는 Topic Prefix인 `oracle-cdc`가 저장된다.

### 1.2 배포 시 처리 내용

파이프라인 목록에서 별도의 **배포** 버튼을 누르면 다음 API가 호출된다.

```http
POST /api/pipelines/{pipelineId}/deploy
```

처리 순서는 다음과 같다.

1. 파이프라인 상태를 `DEPLOYING`으로 변경한다.
2. Source/Target 연결정보와 복호화된 접속정보로 Connector 설정을 만든다.
3. Kafka Connect에 Source Connector를 등록한다.
4. Source Connector 상태와 설정을 Metadata DB에 기록한다.
5. Kafka Connect에 Sink Connector를 등록한다.
6. Sink Connector 상태와 설정을 Metadata DB에 기록한다.
7. 성공하면 파이프라인 상태를 `DEPLOYED`로 변경한다.
8. 배포 성공 이력을 기록한다.

Kafka Connect에는 다음 REST API로 Connector를 등록한다.

```http
PUT /connectors/{connectorName}/config
```

이 API는 upsert 방식으로 동작한다.

- Connector가 없으면 새로 생성한다.
- Connector가 있으면 설정을 갱신한다.

### 1.3 생성되는 Connector

테이블 CDC 파이프라인 하나를 배포하면 일반적으로 Connector 두 개가 생성된다.

예를 들어 파이프라인 ID가 `12`인 Oracle → PostgreSQL 파이프라인이라면 다음과 같은 이름을 사용한다.

```text
Source Connector: source-12-oracle-appuser-customers
Sink Connector:   sink-12-postgresql-cdc_landing-customers
```

Source Connector의 역할:

```text
Oracle 또는 PostgreSQL 변경 감지
  → Debezium CDC 이벤트 생성
  → Kafka Topic에 발행
```

Sink Connector의 역할:

```text
Kafka Topic 구독
  → Debezium 이벤트 해석
  → Target DB 테이블에 upsert 또는 delete
```

사용하는 Connector 클래스는 다음과 같다.

| 구분 | DB 유형 | Connector 클래스 |
|---|---|---|
| Source | Oracle | `io.debezium.connector.oracle.OracleConnector` |
| Source | PostgreSQL | `io.debezium.connector.postgresql.PostgresConnector` |
| Sink | Oracle/PostgreSQL | `io.debezium.connector.jdbc.JdbcSinkConnector` |

### 1.4 실제 Kafka Topic 이름

CDC 데이터 Topic 이름은 다음 규칙으로 결정된다.

```text
{topicPrefix}.{sourceSchema}.{sourceTable}
```

예:

```text
Topic Prefix:      oracle-cdc
Source Schema:     APPUSER
Source Table:      CUSTOMERS
실제 Kafka Topic: oracle-cdc.APPUSER.CUSTOMERS
```

화면의 Topic 열에는 CDC 파이프라인의 경우 전체 Topic 이름이 아니라 저장된 Topic Prefix가 표시될 수 있으므로 구분해야 한다.

현재 백엔드에는 `AdminClient.createTopics()`나 `kafka-topics --create`와 같이 데이터 Topic을 명시적으로 생성하는 코드가 없다. Connector가 배포되고 해당 Topic을 사용하면서 Kafka의 자동 Topic 생성 기능에 의해 물리 Topic이 생성된다.

Oracle Source Connector는 데이터 Topic 외에 커넥터별 스키마 이력 Topic도 사용한다.

```text
schema-changes.source-12-oracle-appuser-customers
```

### 1.5 배포 실패 시 주의점

Connector 등록은 Kafka Connect라는 외부 시스템에 대한 작업이므로 DB 트랜잭션만으로 완전히 되돌릴 수 없다.

예를 들어 다음 상황이 발생할 수 있다.

```text
Source Connector 등록 성공
Sink Connector 등록 실패
```

이 경우 가능한 결과는 다음과 같다.

- 파이프라인 상태는 `FAILED`가 된다.
- Source Connector는 Kafka Connect에 남아 있을 수 있다.
- Sink Connector는 생성되지 않았을 수 있다.
- Source Connector가 사용한 Topic이 이미 생성되었을 수 있다.
- 실패 원인은 파이프라인 명령 이력에 기록된다.

다시 배포하면 Connector 등록 API가 upsert 방식이므로 기존 Source 설정을 갱신하고 Sink 등록을 다시 시도한다.

## 2. Topic은 Source Connector가 이벤트를 받을 때 생성되고 Consumer가 읽으면 삭제되는가?

Topic은 Connector 실행 과정에서 자동 생성될 수 있지만, **Consumer가 읽었다고 메시지나 Topic이 삭제되지는 않는다.**

```text
Source Connector가 변경 이벤트를 Kafka에 발행
  → Topic에 메시지 저장
  → Sink Connector가 메시지를 읽음
  → Consumer Group의 Offset 이동
  → 메시지는 Retention 정책에 따라 계속 보존
```

### 2.1 Topic의 정확한 생성 시점

데이터 Topic을 명시적으로 생성하는 코드가 없으므로 Kafka 자동 생성을 사용한다.

다만 반드시 **첫 번째 이벤트가 발행되는 바로 그 순간**에만 생성된다고 단정할 수는 없다. 다음 과정에서 먼저 생성될 수도 있다.

- Source Connector가 Topic 메타데이터를 요청할 때
- Sink Connector가 구독할 Topic의 메타데이터를 요청할 때
- Source Connector가 첫 번째 레코드를 발행할 때

따라서 정확한 표현은 다음과 같다.

> Connector 배포 및 실행 과정에서 Topic이 필요해지는 시점에 Kafka 자동 생성 기능으로 만들어진다.

### 2.2 Consumer가 읽으면 Offset만 이동한다

Kafka Consumer가 메시지를 읽으면 메시지를 삭제하는 대신 Consumer Group별 Offset을 기록한다.

```text
Topic 메시지:     그대로 남음
Consumer Offset:  100 → 101
```

따라서 다른 Consumer Group은 동일한 데이터를 별도로 읽을 수 있다.

```text
sink-connector-group  → 현재 Offset부터 계속 처리
debug-consumer-group  → 필요하면 처음부터 별도로 처리
```

같은 Consumer Group도 Offset을 이전 위치로 재설정하면 보존 중인 메시지를 다시 읽을 수 있다.

### 2.3 메시지가 삭제되는 기준

메시지 삭제 여부는 Consumer가 읽었는지가 아니라 Kafka의 Retention 및 Cleanup 정책으로 결정된다.

대표적인 설정은 다음과 같다.

| 설정 | 의미 |
|---|---|
| `retention.ms` | 지정된 보존 시간이 지난 오래된 메시지 정리 |
| `retention.bytes` | Partition 크기가 한도를 넘으면 오래된 데이터 정리 |
| `cleanup.policy=delete` | Retention 조건에 따라 오래된 로그 세그먼트 삭제 |
| `cleanup.policy=compact` | 동일한 Key의 과거 값을 정리하고 최신 값 위주로 유지 |

현재 프로젝트의 CDC 데이터 Topic에는 파이프라인별 Retention 설정을 지정하는 코드가 없다. 따라서 Kafka Broker의 기본 설정을 따른다.

Retention 정책으로 메시지가 모두 정리되더라도 Topic 자체는 계속 남는다.

```text
오래된 메시지 → Retention 정책에 따라 삭제 가능
Topic         → 명시적으로 삭제하지 않는 한 계속 존재
```

### 2.4 파이프라인을 삭제하면 Topic도 삭제되는가?

현재 구현에서는 삭제되지 않는다.

파이프라인 삭제 시 다음 리소스를 정리한다.

- Source Connector
- Sink Connector
- Metadata DB의 Connector 정보
- Metadata DB의 파이프라인 정의
- PostgreSQL Source인 경우 Debezium replication slot 및 publication

하지만 다음 Kafka Topic은 명시적으로 삭제하지 않는다.

- CDC 데이터 Topic
- Oracle 스키마 이력 Topic

즉, 현재 삭제 흐름은 다음과 같다.

```text
파이프라인 삭제
  ├─ Source/Sink Connector 삭제
  ├─ 관련 Metadata 삭제
  └─ Kafka Topic은 남음
```

Topic 자체를 삭제하려면 Kafka 관리 명령이나 별도의 Topic 관리 기능을 사용해야 한다.

예:

```bash
kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --delete \
  --topic oracle-cdc.APPUSER.CUSTOMERS
```

Topic 삭제는 보존된 CDC 이벤트와 재처리 가능성을 제거하는 작업이므로 운영 환경에서는 삭제 전에 영향 범위를 확인해야 한다.

## 3. 전체 요약

| 동작 | Metadata DB | Kafka Connect Connector | Kafka Topic/메시지 |
|---|---|---|---|
| 신규 생성 | 파이프라인 정의 저장, 상태 `CREATED` | 생성 안 됨 | 생성 안 됨 |
| 배포 | Connector 정보와 상태 저장 | Source/Sink 생성 또는 갱신 | 실행 과정에서 자동 생성 가능 |
| Consumer 읽기 | 필요 시 상태 정보 갱신 | Sink가 계속 실행 | 메시지 삭제 없이 Offset만 이동 |
| Retention 만료 | 영향 없음 | 영향 없음 | 오래된 메시지만 정리, Topic은 유지 |
| 재배포 | 설정과 상태 갱신 | upsert | 기존 Topic 계속 사용 |
| 파이프라인 삭제 | 정의 및 Connector Metadata 삭제 | Source/Sink 삭제 | 현재 구현에서는 Topic 유지 |

핵심 문장으로 정리하면 다음과 같다.

> 신규 생성은 파이프라인 설계를 Metadata DB에 저장하는 단계이고, 배포는 실제 Kafka Connect 리소스를 생성하고 실행하는 단계다. Consumer가 이벤트를 읽으면 메시지가 삭제되는 것이 아니라 Offset만 이동하며, 메시지는 Kafka Retention 정책에 따라 보존·정리된다. Topic 자체는 파이프라인을 삭제해도 현재 구현에서는 자동 삭제되지 않는다.
