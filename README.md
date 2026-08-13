# Cerebro ETL — Oracle ↔ 타란툴라DB 실시간 데이터 파이프라인

Kafka + NiFi + Airflow 기반 CDC/ETL 파이프라인과, 그 위에서 파이프라인을
생성·배포·관리하는 웹 서비스("Cerebro ETL")입니다. 1단계 목표는
**Oracle → 타란툴라DB(PostgreSQL 기반)** 실시간 적재이며, 최종 목표는
**양방향(타란툴라DB → Oracle 포함)** 연동입니다.

## 아키텍처

```
[Oracle]  --(LogMiner, 실시간 CDC)-->  [Debezium / Kafka Connect]  -->  [Kafka]
                                                                          │
                                                    ConsumeKafkaRecord     │
                                                                          ▼
                                                                      [NiFi ETL]
                                                (정형: CDC 이벤트 변환/라우팅)
                                                (비정형: 파일 수집/텍스트 추출)
                                                (로그: Filebeat → Kafka → JDBC Sink)
                                                                          │
                                                                          ▼
                                                       [타란툴라DB (PostgreSQL 기반)]

[Airflow]  -- 파이프라인 시작/중지/재시작/스케줄 제어 (제어 플레인) -->  [Kafka Connect / NiFi]
[Cerebro ETL 웹]  -- 파이프라인 생성/배포/삭제, 각 도구 화면 통합 -->  [위 스택 전체]
```

- **CDC 실시간 수집**: Debezium Oracle/Postgres 커넥터가 redo log/논리 복제를
  읽어 Kafka 토픽(`oracle-cdc.<SCHEMA>.<TABLE>` 등)에 변경 이벤트를 발행합니다.
- **정형 데이터 ETL**: NiFi가 Kafka 토픽을 구독해 Debezium 이벤트를 파싱/평탄화하고
  `PutDatabaseRecord`로 타깃 DB에 upsert합니다.
- **비정형 데이터 수집**: NiFi의 파일/HTTP 기반 프로세서(ListFile, ListenHTTP 등)로
  수집 후 필요 시 텍스트 추출하여 별도 테이블에 적재합니다. 상세는
  [`nifi/FLOW_RUNBOOK.md`](nifi/FLOW_RUNBOOK.md) 참고.
- **로그파일 실시간 적재**: Filebeat가 로그 파일을 tailing해서 Kafka에 쓰고,
  기존 JDBC Sink 커넥터가 그대로 타깃 DB에 적재합니다.
- **파이프라인 웹 서비스**(`web/backend` + `web/cerebroetl-ui`):
  화면/API로 Kafka Connect 커넥터를 동적으로 생성·배포·삭제합니다. 자세한 설계는
  [`docs/kafka-webservice-design.md`](docs/kafka-webservice-design.md) 참고.
- **Airflow**: 이미 배포된 파이프라인의 시작/중지/재시작 및 스케줄을 담당하는
  제어 플레인입니다. Kafka/NiFi 파이프라인마다 DAG가 자동 생성됩니다
  (`airflow/dags/kafka_pipelines_dynamic.py`, `nifi_pipelines_dynamic.py`).
  생성/삭제는 여전히 각 도구(웹 화면 또는 NiFi 캔버스)에서 담당합니다.

## "타란툴라DB"에 대한 안내

요청하신 사내 "타란툴라DB"는 PostgreSQL 기반 제품으로 확인되어, 이 저장소에서는
표준 `postgres` 이미지로 대체 구성했습니다. 실제 제품이 PostgreSQL과 다른 JDBC
드라이버/방언을 요구하는 경우, `docker-compose.yml`의 `target-db` 서비스 이미지와
`nifi/Dockerfile`의 드라이버 다운로드 URL만 교체하면 됩니다(연동 로직은 표준 JDBC
기반이라 대부분 그대로 재사용 가능).

## 사전 준비물

- Docker / Docker Compose v2
- (선택, 커스텀 프로세서/커넥터 개발 시) JDK 21 — `scripts/setup-local-dev.sh` 참고
- 각 컨테이너(Kafka Connect: Confluent 이미지, NiFi 2.x)는 자체 JRE를 내장하므로
  파이프라인 실행 자체에는 로컬 JDK 설치가 필수는 아닙니다.
- JDBC 드라이버: Oracle은 **ojdbc11**(JDK 11+ 대응, JDK 21과 호환), 타깃 DB는
  PostgreSQL 공식 JDBC 드라이버를 사용합니다. 버전은 `.env`에서 조정 가능합니다.

## 1) 최초 기동

```bash
cp .env.example .env
# 필요 시 .env의 비밀번호 값들을 수정하세요.

docker compose up -d --build
```

기동 후 Cerebro ETL 통합 웹(아래 표)의 "연결정보" 화면에서 CDC 소스/타겟 DB
연결정보를 등록하고, "파이프라인" 화면에서 생성/배포하면 됩니다. 로컬에 데모용
타깃 DB 컨테이너도 함께 띄우고 싶다면 `--profile poc`를 추가하세요
(`docker compose --profile poc up -d --build`).

## 웹 서비스 / Airflow 접속

`.env`에서 실제 포트를 확인하세요(기본값 기준):

| 서비스 | URL | 용도 |
|---|---|---|
| Cerebro ETL 통합 웹 | `http://localhost:${CEREBROETL_UI_PORT}` | 연결/파이프라인 관리 + NiFi/Airflow/Kafka Connect를 한 화면에서 |
| Airflow | `http://localhost:${AIRFLOW_WEBSERVER_PORT}` | 파이프라인 시작/중지/재시작/스케줄 |
| NiFi | `https://localhost:${NIFI_PUBLIC_HTTPS_PORT}/nifi` | NiFi 캔버스 직접 접속 |

Kafka Connect REST API(:8083)는 인증이 없어 호스트에 노출하지 않습니다 - 상태 조회는
Cerebro ETL 통합 웹의 Kafka Connect 화면이나 `docker exec kafka-connect curl localhost:8083/connectors`로 확인하세요.

## 2) 사내 Oracle을 CDC 소스로 연결하기

Cerebro ETL 웹의 "연결정보" 화면에서 사내 Oracle 접속 정보를 등록하면 됩니다.
등록 전에 DBA와 함께 아래 사항을 사내 Oracle에 반영해야 합니다:

- ARCHIVELOG 모드 활성화 (LogMiner 필수 전제조건)
- 대상 테이블에 Supplemental Logging (ALL) COLUMNS 설정
- Debezium 전용 CDC 계정 생성 (CDB 공통 계정 표기법: `C##`로 시작하는 사용자,
  LogMiner 관련 권한 부여) — appuser 같은 PDB 로컬 사용자는 LogMiner가 CDB
  레벨에서 인증하기 때문에 비밀번호가 맞아도 인증되지 않습니다.
- 컨테이너에서 사내 DB로의 네트워크 접근(방화벽/VPN) 확인

주의:
- `.env`는 git에 포함되지 않지만 평문 저장이므로, 운영 단계에서는 비밀 관리
  도구 연동을 권장합니다(연결정보 자체의 비밀번호는 metadata-db에 암호화되어 저장됩니다).
- NiFi 2.x는 HTTPS + Single-User 인증으로만 기동됩니다(`NIFI_HTTPS_PORT`/
  `NIFI_PUBLIC_HTTPS_PORT`, 자체 서명 인증서). 사내 배포 시 LDAP 등 다른 인증
  방식이 필요하면 별도 전환이 필요합니다.

## 디렉터리 구조

```
db/target-init/       타깃 DB(PostgreSQL) 랜딩 스키마
kafka-connect/         Debezium 커넥터 포함 Kafka Connect 이미지
nifi/                  JDBC 드라이버 포함 NiFi 이미지 + 플로우 구성 가이드 (FLOW_RUNBOOK.md)
airflow/dags/          Kafka/NiFi 파이프라인 제어용 동적 DAG + 배선 검증용 DAG
filebeat/              로그파일 실시간 적재 파이프라인의 소스 설정
web/backend/           파이프라인 웹 서비스 백엔드 (Spring Boot)
web/cerebroetl-ui/     통합 웹 UI ("Cerebro ETL") - 연결/파이프라인 관리 + NiFi/Airflow/Kafka Connect
docs/                  설계 문서 (kafka-webservice-design.md 등)
offline/               폐쇄망 배포 패키징 스크립트
scripts/               로컬 개발환경 셋업 스크립트
.gitlab-ci.yml         GitLab CI (compose 유효성 검증 + 이미지 빌드 + 백엔드 테스트)
```

## 알려진 제약사항

- NiFi 플로우는 재현성과 안정성을 위해 UI에서 수동으로 구성하도록 안내합니다
  (`nifi/FLOW_RUNBOOK.md`). 자동 임포트용 `flow.json`은 추후 팀 내 검증된 플로우를
  export한 뒤 저장소에 추가하는 것을 권장합니다.

## NiFi conf 백업 적용

`nifi-conf-backup/conf`에 백업해둔 NiFi 설정/플로우를 현재 Docker NiFi에 적용하려면
NiFi를 멈춘 뒤 `conf` 내용을 컨테이너의 conf 볼륨으로 복사하고 다시 시작합니다.

```bash
docker compose stop nifi
docker cp nifi-conf-backup/conf/. nifi:/opt/nifi/nifi-current/conf/
docker compose start nifi
```

현재 캔버스 구조는 주로 `flow.json.gz`로 복원됩니다. 암호화된 비밀번호/토큰을
정상 복호화하려면 백업의 `nifi.properties` 안에 있는 `nifi.sensitive.props.key`도
같이 적용되어야 합니다.
