# Kafka CDC 파이프라인 동작 순서

Oracle → Kafka → Postgres 실시간 CDC 파이프라인이 기동되는 순서와, 각 단계에서
어떤 파일의 어떤 설정이 그 동작을 만드는지 정리한 문서입니다.

## 1단계 — 컨테이너 기동 순서

`docker-compose.yml`의 `depends_on: condition: service_healthy` 체인이 순서를
강제합니다:

```
oracle-db, target-db (healthy)
        ↓
kafka (healthy) ← kafka-connect가 이걸 기다림
        ↓
kafka-connect (healthy) ← connect-init, nifi가 이걸 기다림
        ↓
connect-init (1회성, 실행 후 종료)
```

## 2단계 — Kafka Connect가 뜨면서 CDC 플러그인 로드

`kafka-connect/Dockerfile`이 빌드 시점에:
- `debezium-connector-oracle` 플러그인 (Maven Central에서 tar.gz 다운로드)
- `debezium-connector-jdbc` 플러그인 + Postgres/Oracle JDBC 드라이버

를 `/usr/share/confluent-hub-components/`에 심어두고, `docker-compose.yml`의
`CONNECT_PLUGIN_PATH` 환경변수가 Kafka Connect 워커한테 "여기서 플러그인
찾아라"라고 알려줍니다.

## 3단계 — `connect-init`이 커넥터를 자동 등록

`scripts/register-connector.sh`가 Kafka Connect REST API(`:8083`)가 뜰 때까지
기다렸다가, `kafka-connect/connectors/*.json.template` 파일들을 `.env` 값으로
채워서(`envsubst`) `PUT /connectors/<이름>/config`로 등록합니다. 지금 등록되는
3개:
- `oracle-cdc-source.json.template` → 소스
- `postgres-cdc-sink.json.template` → 싱크(customers)
- `postgres-cdc-sink-clob-test-data.json.template` → 싱크(clob_test_data)

## 4단계 — 소스 커넥터(`oracle-cdc-source`)가 실제로 캡처

`oracle-cdc-source.json.template`의 핵심 설정들이 각각 이 역할을 합니다:

| 설정 | 하는 일 |
|---|---|
| `connector.class: io.debezium.connector.oracle.OracleConnector` | Debezium Oracle 커넥터 클래스 지정 |
| `database.connection.adapter: logminer` | Oracle **LogMiner**로 redo log를 읽는 방식 사용 |
| `log.mining.strategy: online_catalog` | online redo log를 실시간으로 읽음 (아카이브 로그가 아니라) |
| `table.include.list: APPUSER.CUSTOMERS,APPUSER.CLOB_TEST_DATA` | 캡처 대상 테이블 |
| `lob.enabled: true` | CLOB/BLOB 컬럼도 캡처 |
| `schema.history.internal.skip.unparseable.ddl`, `event.processing.failure.handling.mode: warn` | DDL 파싱 크래시/이벤트 재구성 실패 시에도 커넥터가 죽지 않고 계속 진행하도록 함 |

이게 동작하려면 **Oracle 쪽 사전 준비**가 돼있어야 하는데, 그게
`db/oracle-init/`에 있습니다:
- `01_enable_archivelog.sql` → ARCHIVELOG 모드 (LogMiner 필수조건)
- `02_create_cdc_user.sql` → `C##DBZUSER` 전용 계정 + LogMiner 권한
- `03_sample_schema.sql` → 테이블별 `SUPPLEMENTAL LOG DATA (ALL) COLUMNS` (컬럼 전체 캡처 조건)

Oracle에서 커밋이 발생하면 → LogMiner가 redo log에서 그 변경분을 읽고 →
Debezium이 이걸 `oracle-cdc.APPUSER.CUSTOMERS` 같은 Kafka 토픽에 이벤트
(before/after/op/ts_ms 포함)로 발행합니다.

## 5단계 — 싱크 커넥터가 Postgres에 반영

`postgres-cdc-sink*.json.template`이 그 토픽을 구독해서 실행:

| 설정 | 하는 일 |
|---|---|
| `connector.class: io.debezium.connector.jdbc.JdbcSinkConnector` | JDBC로 타겟에 직접 씀 |
| `topics: oracle-cdc.APPUSER.CUSTOMERS` | 구독할 토픽 |
| `insert.mode: upsert`, `primary.key.mode: record_key` | PK 기준 upsert |
| `table.name.format: cdc_landing.customers` | 쓸 타겟 테이블 |
| `delete.enabled: true` | DELETE 이벤트도 실제 DELETE로 반영 |

타겟 테이블 자체는 `db/target-init/01_schema.sql`이 target-db 컨테이너 최초
기동 시 만들어둡니다.

## 6단계 — 메시지 형식 변환

`docker-compose.yml`의 `kafka-connect` 서비스에 있는
`CONNECT_KEY_CONVERTER`/`CONNECT_VALUE_CONVERTER: JsonConverter` +
`SCHEMAS_ENABLE: true`가, Kafka에 저장되는 메시지에 **스키마 정보까지 같이
실어서** 싱크 커넥터가 컬럼 타입을 알고 정확한 SQL을 만들 수 있게 해줍니다.

## 요약 흐름

```
Oracle 커밋
  → (db/oracle-init/*.sql로 준비된) LogMiner가 redo log에서 변경 감지
  → oracle-cdc-source.json.template 설정대로 Debezium이 Kafka 토픽에 이벤트 발행
  → postgres-cdc-sink*.json.template 설정대로 JDBC Sink가 그 이벤트를 구독
  → db/target-init/01_schema.sql로 만들어둔 착지 테이블에 UPSERT/DELETE 실행
```

전체가 사람 개입 없이 이 순서 그대로 계속 반복 동작하는 게 지금의 "실시간
CDC"입니다.
