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

## 1. Process Group: 비정형 (구축 완료, 5종 검증 완료)

HTTP로 파일을 받아서 **종류별로 나눠 타란툴라DB에 적재**하는 그룹이다.
이미지/동영상은 원본 바이트를 그대로, 로그/XML/CSV는 파싱해서 컬럼 단위 레코드로
넣는다. 적재 대상은 `192.168.50.12:7432 / postgres` 의 `unstructured` 스키마
(DDL은 `db/tarantula-init/01_unstructured_schema.sql`).

```
ListenHTTP-unstructured   (포트 8444, Base Path: unstructured, 모든 요청헤더를 attribute로)
        │ success
        ▼
UpdateAttribute-normalize  raw POST(Content-Type/filename 헤더)와 multipart(http.multipart.*)의
        │                  이름·MIME을 orig.filename / orig.mime 하나로 통일
        ▼
UpdateAttribute-classify   data.type 결정: X-Data-Type 헤더 > Content-Type > 파일 확장자 순
        │
        ▼
RouteOnAttribute-type ──image──▶ [unstructured-image] ─▶ unstructured.image_files
        │             ──video──▶ [unstructured-video] ─▶ unstructured.video_files
        │             ──log────▶ [unstructured-log]   ─▶ unstructured.log_records
        │             ──xml────▶ [unstructured-xml]   ─▶ unstructured.xml_records
        │             ──csv────▶ [unstructured-csv]   ─▶ unstructured.csv_records
        └─unmatched──▶ LogAttribute-unmatched (WARN 한 줄만 남기고 폐기)
```

### 1-1. 서브그룹 내부

**이미지 / 동영상 (바이너리 원본)**

```
[in] ─▶ store-image-binary (ExecuteGroovyScript, CTL.tarandb = cp-tarantula-192-168-50-12)
          success ─▶ 자동 종료
          failure ─▶ log-failed (ERROR 로그 → NiFi Bulletin → ETL 로그 화면에 FAILED로 노출)
```

PutDatabaseRecord를 쓰지 않고 Groovy를 쓰는 이유: PutDatabaseRecord는 레코드
기반이라 바이너리를 넣으려면 Base64로 바꿔 **파일 전체를 attribute/JSON으로 올려야**
하는데, attribute는 힙에 상주해서 동영상 한 개로 NiFi가 OOM에 빠질 수 있다.
스크립트는 FlowFile의 InputStream을 `PreparedStatement.setBinaryStream()`에 그대로
넘기므로 파일 크기와 무관하게 힙을 쓰지 않는다.

스크립트 끝에서 `session.adjustCounter('INSERT updates performed', 1, false)`를
호출하는데, 이건 백엔드 `NifiPipelineMetricScheduler`가 집계하는 카운터 이름과
맞춘 것이다(아래 "한계" 참고).

**로그 / XML / CSV (파싱 → 구조화)**

```
[in] ─▶ parse-<종류>-and-tag (UpdateRecord: <종류>Reader → json-writer-unstructured,
        │                     /source_file = ${orig.filename} 을 레코드에 심는다)
        │ success
        ▼
     load-<종류>-records (PutDatabaseRecord: json-reader-unstructured → unstructured.<테이블>)
          success ─▶ 자동 종료
          failure/retry ─▶ log-failed
```

파싱만 하면 "어느 파일에서 온 행인지"가 사라진다(attribute는 DB로 안 따라감).
그래서 파싱과 동시에 `source_file` 필드를 레코드 안에 넣어주는 UpdateRecord를
한 단 두었다.

### 1-2. 공용 Controller Service (비정형 그룹 레벨)

| 이름 | 타입 | 설정 |
|---|---|---|
| `csv-reader` | CSVReader | Schema Access = infer-schema, Skip Header Line = true |
| `xml-reader` | XMLReader | Schema Access = infer-schema, `record_format` = true (루트 밑 반복 엘리먼트 = 레코드) |
| `grok-reader-log` | GrokReader | 아래 Grok 표현식, no-match-behavior = raw-line |
| `json-writer-unstructured` | JsonRecordSetWriter | 기본값 (inherit-record-schema) |
| `json-reader-unstructured` | JsonTreeReader | 기본값 (infer-schema) |

DB 연결은 루트에 이미 있는 `cp-tarantula-192-168-50-12`(DBCPConnectionPool)를
상속해서 쓴다. 별도로 만들지 않았다.

Grok 표현식:

```
%{TIMESTAMP_ISO8601:log_time}%{SPACE}%{LOGLEVEL:log_level}%{SPACE}\[%{DATA:logger}\]%{SPACE}%{GREEDYDATA:message}
```

### 1-3. 보내는 법

샘플 파일은 `nifi/samples/unstructured/`에 있다. **그룹이 RUNNING이어야 받는다**
(시작은 Airflow가 시킨다 - 아래 1-5 참고).

호스트에서 (compose에 `18444:8444` 노출):

```bash
curl -X POST http://localhost:18444/unstructured \
  -H "Content-Type: image/png" -H "filename: sample.png" \
  --data-binary @nifi/samples/unstructured/sample.png
```

종류별로 필요한 건 Content-Type 하나뿐이다:

| 종류 | Content-Type | 또는 확장자 |
|---|---|---|
| 이미지 | `image/*` | — |
| 동영상 | `video/*` | — |
| CSV | `text/csv` | `.csv` |
| XML | `application/xml`, `text/xml` | `.xml` |
| 로그 | (판별 불가) | `.log` |

Content-Type이 애매하면 `-H "X-Data-Type: log"` 처럼 헤더로 강제 지정할 수 있고,
이 헤더가 항상 최우선이다. 브라우저 업로드(`multipart/form-data`)도 그대로 받는다:

```bash
curl -X POST http://localhost:18444/unstructured -F "file=@sample.csv;type=text/csv"
```

어디에도 안 걸리면 `unmatched`로 빠져서 WARN 로그만 남고 버려진다(적재 안 됨).

### 1-4. XML / CSV 입력 형식

파싱 결과 필드명이 그대로 컬럼명에 매칭되므로(PutDatabaseRecord의
`Column Name Translation Strategy = REMOVE_UNDERSCORE`) 형식을 맞춰야 한다.
`id`는 DB가 채우는 PK라 넣지 않는다.

```xml
<records>
  <record>
    <record_id>X-001</record_id><title>...</title><category>A</category>
    <amount>1500.75</amount><reg_date>2026-07-28</reg_date>
  </record>
</records>
```

```csv
record_id,title,category,amount,reg_date
C-001,...,A,1500.75,2026-07-28
```

다른 형식을 넣으려면 테이블 컬럼을 그 형식에 맞춰 추가하면 된다. 매칭 안 되는
필드/컬럼은 양쪽 다 `Ignore`라 에러 없이 무시된다.

### 1-5. 스케줄은 걸지 않는다

이 그룹에는 CRON Driven 프로세서가 하나도 없다(전부 Timer Driven `0 sec` =
"그룹이 켜져 있는 동안 일이 있으면 처리"). 시작/정지는 다른 그룹과 똑같이
Airflow 제어 DAG(`nifi_pipeline_a7c2d807_control`)가 담당한다 - 이 DAG는
`nifi_pipelines_dynamic.py`가 루트 그룹을 훑어서 자동 생성하고, Variable로
스케줄을 넣지 않는 한 수동 트리거 전용이다.

`nifi.flowcontroller.autoResumeState=false`가 강제돼 있어서 컨테이너가 재시작되면
이 그룹도 STOPPED로 올라온다. HTTP 수신을 상시 열어두려면 제어 DAG를 한 번
트리거해야 한다.

### 1-6. 한계

- **이미지/동영상은 ETL 로그 화면의 적재 건수에 안 잡힌다.** 백엔드
  `NifiPipelineMetricScheduler`가 `PutDatabaseRecord` **타입인 프로세서만** 훑기
  때문이다(카운터 이름은 맞춰뒀으므로, 그 타입 필터에 `ExecuteGroovyScript`를
  추가하면 바로 잡힌다). 로그/XML/CSV는 지금도 정상적으로 잡힌다.
- 동영상을 bytea로 넣는 건 시연 목적이다. 운영 규모에서는 원본을 오브젝트
  스토리지에 두고 경로만 적재하는 쪽이 맞다(DB 백업/복제 비용).
- Grok 표현식에 안 맞는 로그 줄은 버려지지 않고 전 컬럼 NULL인 행으로 들어간다
  (`no-match-behavior = raw-line`). 로그 포맷이 다르면 `grok-reader-log`의
  표현식을 소스에 맞게 바꿔야 한다.

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
