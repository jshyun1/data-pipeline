# Oracle ↔ 타란툴라DB 실시간 데이터 파이프라인

Kafka + NiFi 기반 CDC/ETL 파이프라인. 1단계 목표는 **Oracle → 타란툴라DB(PostgreSQL 기반)**
실시간 적재이며, 최종 목표는 **양방향(타란툴라DB → Oracle 포함)** 연동입니다.

## 아키텍처

```
[Oracle]  --(LogMiner, 실시간 CDC)-->  [Debezium / Kafka Connect]  -->  [Kafka]
                                                                          │
                                                    ConsumeKafkaRecord     │
                                                                          ▼
                                                                      [NiFi ETL]
                                                (정형: CDC 이벤트 변환/라우팅)
                                                (비정형: 파일 수집/텍스트 추출)
                                                                          │
                                                                          ▼
                                                       [타란툴라DB (PostgreSQL 기반)]
```

- **CDC 실시간 수집**: Debezium Oracle 커넥터(LogMiner 어댑터)가 Oracle redo log를
  읽어 Kafka 토픽(`oracle-cdc.<SCHEMA>.<TABLE>`)에 변경 이벤트를 발행합니다.
- **정형 데이터 ETL**: NiFi가 Kafka 토픽을 구독해 Debezium 이벤트를 파싱/평탄화하고
  `PutDatabaseRecord`로 타깃 DB에 upsert합니다.
- **비정형 데이터 수집**: NiFi의 파일/HTTP 기반 프로세서(ListFile, ListenHTTP 등)로
  수집 후 필요 시 텍스트 추출하여 별도 테이블에 적재합니다. 상세는
  [`nifi/FLOW_RUNBOOK.md`](nifi/FLOW_RUNBOOK.md) 참고.

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

## 1) 로컬 POC 실행 (Oracle + 타깃 DB 컨테이너 포함)

```bash
cp .env.example .env
# 필요 시 .env의 비밀번호 값들을 수정하세요.
# 단, ORACLE_CDC_USER / ORACLE_CDC_USER_PASSWORD를 바꾸면
# db/oracle-init/02_create_cdc_user.sql도 함께 수정해야 합니다.

docker compose --profile poc up -d --build
docker compose logs -f kafka-connect   # 커넥터 등록/기동 로그 확인
```

확인:

```bash
# Debezium 커넥터 상태
curl -s http://localhost:8083/connectors/oracle-cdc-source/status | jq

# NiFi UI (초기 실행 시 이미지 빌드로 몇 분 소요될 수 있음)
open http://localhost:8080/nifi   # 접속 후 nifi/FLOW_RUNBOOK.md 순서로 플로우 구성
```

Oracle 테이블(`appuser.customers`)에 INSERT/UPDATE/DELETE를 발생시키면 Kafka
토픽 → NiFi를 거쳐 타깃 DB(`cdc_landing.customers`)에 반영되는지 확인합니다.

## 2) 회사 DB 연동 단계 (로컬 컨테이너 → 실제 사내 DB)

로컬 Oracle/타깃DB 컨테이너 없이, Kafka/Kafka Connect/NiFi만 띄우고 실제 사내
DB에 연결합니다.

```bash
# .env 수정: ORACLE_HOST/PORT, TARGET_DB_HOST/PORT 및 계정 정보를
# 사내 실제 DB 값으로 변경
docker compose up -d --build   # --profile poc 를 빼면 오라클/타깃DB 컨테이너는 생성되지 않음
```

주의:
- 사내 Oracle에도 동일하게 ARCHIVELOG + Supplemental Logging + LogMiner 전용
  계정/권한이 필요합니다 (`db/oracle-init/*.sql` 내용을 DBA와 함께 반영).
- 컨테이너에서 사내 DB로의 네트워크 접근(방화벽/VPN) 및 자격 증명 관리(사내 Vault 등)를
  별도로 검토하세요. `.env`는 git에 포함되지 않지만 평문 저장이므로, 운영 단계에서는
  비밀 관리 도구 연동을 권장합니다.
- NiFi는 현재 HTTP(비TLS)로 구성되어 있습니다(로컬 POC 전용). 사내 DB 연동 시에는
  `NIFI_WEB_HTTPS_PORT`/인증서/Single-User 또는 LDAP 인증으로 전환하세요.

## 3) 최종 목표: 타란툴라DB → Oracle 역방향

1단계(Oracle → 타란툴라DB)가 검증되면, `nifi/FLOW_RUNBOOK.md` 4절 안내에 따라
대칭 구조(Kafka Connect에 Debezium PostgreSQL 커넥터 추가 + NiFi
`PutDatabaseRecord`로 Oracle 적재)로 확장합니다.

## 디렉터리 구조

```
db/oracle-init/       Oracle CDC 활성화 + 샘플 스키마 (컨테이너 최초 기동 시 1회 실행)
db/target-init/       타깃 DB(PostgreSQL) 랜딩 스키마
kafka-connect/         Debezium Oracle 커넥터 포함 Kafka Connect 이미지 + 커넥터 설정 템플릿
nifi/                  JDBC 드라이버 포함 NiFi 이미지 + 플로우 구성 가이드
scripts/               로컬 개발환경 셋업, 커넥터 등록 스크립트
.gitlab-ci.yml         GitLab CI (compose 유효성 검증 + 이미지 빌드)
```

## 알려진 제약사항

- Oracle LogMiner 초기화(ARCHIVELOG 전환 등)는 Oracle 이미지/버전에 따라 동작이
  달라질 수 있어, 최초 기동 시 `docker compose logs oracle-db`로 정상 완료 여부를
  꼭 확인하세요.
- NiFi 플로우는 재현성과 안정성을 위해 UI에서 수동으로 구성하도록 안내합니다
  (`nifi/FLOW_RUNBOOK.md`). 자동 임포트용 `flow.json`은 추후 팀 내 검증된 플로우를
  export한 뒤 저장소에 추가하는 것을 권장합니다.
