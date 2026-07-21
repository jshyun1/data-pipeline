# NiFi 플로우 구성 가이드 (수동 빌드)

정형 CDC 경로(Oracle → Postgres)는 NiFi가 아니라 **Kafka Connect의
`postgres-cdc-sink`(Debezium JDBC Sink) 커넥터**가 처리합니다. `connect-init`
컨테이너가 기동 시 `scripts/register-connector.sh`로 소스/싱크 커넥터를 자동
등록하므로 별도 작업이 필요 없습니다. 커넥터 설정은
`kafka-connect/connectors/oracle-cdc-source.json.template`,
`kafka-connect/connectors/postgres-cdc-sink.json.template` 참고.

NiFi는 **비정형 데이터 수집(파일/텍스트 추출 등)** 전용으로 사용합니다. 아래
순서대로 NiFi UI(`https://localhost:8443/nifi`, 로그인 계정은 `.env`의
`NIFI_SINGLE_USER_USERNAME`/`PASSWORD`)에서 직접 구성하는 것을 권장합니다.

## 0-1. 현재 구축된 최소 동작 플로우 (테스트 완료)

아래 구성이 NiFi REST API로 이미 만들어져 있고 RUNNING 상태입니다 (UI에서
캔버스 열어서 그대로 확인 가능):

```
ListenHTTP (포트 ${NIFI_LISTENHTTP_PORT}, Base Path: contentListener)
        │ success
        ▼
PutDatabaseRecord (Record Reader: json-reader / JsonTreeReader)
  └─ cp-target-db 사용, unstructured_landing.file_objects 에 INSERT
     (source_path / mime_type / extracted_text 컬럼만 채움.
      raw_object, id, ingested_at 은 비워두거나 DB 기본값 사용)
```

테스트 방법:
```
docker compose exec -T nifi curl -X POST "http://localhost:${NIFI_LISTENHTTP_PORT}/contentListener" \
  -H "Content-Type: application/json" \
  -d '{"source_path": "test/hello.txt", "mime_type": "text/plain", "extracted_text": "샘플 텍스트"}'

docker exec target-db psql -U ${TARGET_DB_USER} -d ${TARGET_DB_NAME} \
  -c "select * from unstructured_landing.file_objects;"
```

**한계 / 다음 단계**: 지금은 클라이언트가 JSON으로 이미 추출된 텍스트를
보내야 합니다. 실제 파일(PDF/DOCX 등) 원본을 받아서 Tika로 텍스트를
추출하고 원본 바이너리까지 `raw_object`에 채우려면, 아래 1번 섹션대로
`ExtractText`/`PutTikaExtract` + 원본 바이너리 바인딩 단계를 추가로
구성해야 합니다.

## 0-2. Oracle EMPLOYEES → Postgres 배치 동기화 (1시간 주기, 테스트 완료)

정형 CDC(customers, clob_test_data)와 달리 이 테이블은 Kafka Connect가 아니라
NiFi가 **1시간마다 전체를 다시 읽어서 UPSERT**하는 방식으로 처리합니다
(Oracle EMPLOYEES에 변경 시각을 추적할 컬럼/PK가 없어서 증분 폴링 대신 매번
풀스캔 + UPSERT로 단순화함):

```
QueryDatabaseTable-employees (Timer Driven, 1 hr, cp-oracle-db, Table: EMPLOYEES)
        │ success (Avro)
        ▼
PutDatabaseRecord-employees (avro-reader, cp-target-db, db-type: PostgreSQL,
                              Statement Type: UPSERT, Update Keys: employee_id)
  └─ batch_landing.employees 에 UPSERT (INSERT 또는 employee_id 기준 UPDATE)
```

**한계**: DELETE는 반영되지 않습니다 (Oracle에서 지운 행이 Postgres에 남아있음).
삭제 전파가 필요해지면 풀 스캔 결과와 착지 테이블을 diff해서 없어진
employee_id를 지우는 단계를 추가해야 합니다. 또한 `synced_at`은 "최초 적재
시각"만 기록하고 UPDATE 시에는 갱신되지 않습니다(Ignore Unmatched Columns라
UPSERT의 SET 절에 포함 안 됨) — 마지막 동기화 시각이 필요하면 이 컬럼도
Update Keys 대상에서 제외하고 명시적으로 SET 하도록 바꿔야 합니다.

테스트 방법: Timer Driven 프로세서는 시작하자마자 즉시 1회 실행되므로, 즉시
검증하려면 `QueryDatabaseTable-employees`를 STOPPED → RUNNING으로 한 번
껐다 켜면 됩니다 (실제 운영 중엔 그냥 1시간마다 자동 실행됨).

## 0. 공통 Controller Service 등록

Process Group 우클릭 → Configure → Controller Services 탭에서 추가:

| 이름 | 타입 | 주요 설정 |
|---|---|---|
| `cp-target-db` | `DBCPConnectionPool` | Database Connection URL: `jdbc:postgresql://target-db:5432/tarantula`<br>Driver Class Name: `org.postgresql.Driver`<br>Driver Location(s): `/opt/nifi/nifi-current/drivers/postgresql-*.jar`<br>User/Password: `.env`의 `TARGET_DB_USER` / `TARGET_DB_PASSWORD` |
| `cp-oracle-db` | `DBCPConnectionPool` | URL: `jdbc:oracle:thin:@oracle-db:1521/XEPDB1`<br>Driver Class Name: `oracle.jdbc.OracleDriver`<br>Driver Location(s): `/opt/nifi/nifi-current/drivers/ojdbc11-*.jar`<br>(0-2 섹션의 EMPLOYEES 배치 동기화와 역방향 동기화 양쪽에 사용) |
| `avro-reader` | `AvroReader` | 기본값 그대로 (QueryDatabaseTable 출력용) |

## 1. Process Group: 비정형 데이터 수집

```
ListFile / ListenHTTP (또는 GetFTP 등 소스에 맞게 선택)
        │
        ▼
FetchFile
        │
        ▼
(선택) ExtractText / Apache Tika(PutTikaExtract 등) 로 텍스트 추출
        │
        ▼
PutDatabaseRecord 또는 PutSQL
  └─ cp-target-db 사용, unstructured_landing.file_objects 테이블에 메타데이터 +
     추출 텍스트 적재. 원본 바이너리는 raw_object(BYTEA) 컬럼 또는 별도 오브젝트
     스토리지(S3/MinIO 등)에 저장 후 경로만 적재하는 방식으로 확장 가능.
```

- 실시간 처리가 필요한 소스(ListenHTTP 등)는 Scheduling Strategy를 Timer
  Driven(기본 0 sec)으로, 일 배치 등 스케줄이 필요한 소스는 CRON Driven으로
  프로세서 단위로 설정하면 됩니다.

## 2. 정형 CDC 경로 검증 방법

1. `docker compose logs -f kafka-connect` 로 커넥터 상태 확인
2. `curl http://localhost:8083/connectors/oracle-cdc-source/status` /
   `curl http://localhost:8083/connectors/postgres-cdc-sink/status` 로 둘 다
   RUNNING 확인
3. Oracle에 `INSERT/UPDATE/DELETE` 발생
4. `docker exec -it target-db psql -U <TARGET_DB_USER> -d tarantula -c "select * from cdc_landing.customers;"` 로 적재 확인

## 3. 다음 단계 (타란툴라DB → Oracle 역방향)

`cp-oracle-db` 컨트롤러 서비스를 이용해 대칭 구조로 구성:
타란툴라DB 변경분(트리거/타임스탬프 컬럼 기반 폴링 또는 논리적 CDC) → NiFi →
`PutDatabaseRecord`(cp-oracle-db)로 Oracle에 적재. PostgreSQL 계열은 Debezium
Postgres 커넥터(logical replication slot)로 실시간 CDC도 가능하므로, 요구사항이
확정되면 Kafka Connect에 `debezium-connector-postgres`를 추가하는 대칭 구조로
확장하면 됩니다.
