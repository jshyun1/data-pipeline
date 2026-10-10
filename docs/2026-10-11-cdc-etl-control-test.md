# CDC · ETL 워크플로우 제어 기능 시험 결과 (2026-10-10 ~ 11, 로컬)

## 1. 무엇을 시험했나

- 목적: 파이프라인별 제어 DAG 를 없애고(10/1) CDC 를 워크플로우(CDC 노드)로만 제어하는 지금 구조에서, 파이프라인 제어에 문제가 없는지 여러 방향으로 확인한다. ETL(NiFi) 워크플로우도 같이 본다.
- 범위
  - CDC 테이블 파이프라인 4종: UPSERT(삭제 반영) · UPSERT(삭제 미반영, 스냅샷 없음) · 델타 append · 델타 upsert.
  - 로그파일 파이프라인 1종.
  - ETL DB→DB 잡(TRUNCATE) 2개 체인.
- 환경: 로컬 스택 + poc 프로필 `target-db`(Postgres 16, `wal_level=logical`)를 원천 · 타깃으로 썼다.
  - 기존 AAA-* 파이프라인과 DZ 워크플로우는 MSA 쪽 DB(119.204.108.98 · 192.168.50.x)를 쓰므로 건드리지 않았다.
- 방법
  - 화면과 같은 길로 호출했다: cerebroetl-ui → pipeline-api / Airflow API, 사용자 로그인 토큰.
  - 워크플로우는 실제로 실행했고(노드별 지시 `node_actions`), 결과는 원천 · 타깃 DB 를 직접 대조해 판정했다.
  - 시험 객체 이름은 `QA1010_` 로 시작한다. 끝난 뒤 모두 지웠다(7절).
- 참고한 CDC 시험 관점(인터넷)
  - 초기 스냅샷 vs 스트리밍 경계.
  - INSERT/UPDATE/DELETE 반영.
  - 재시도 · 장애 뒤 중복(멱등 싱크)과 키 단위 순서.
  - 재시작 뒤 오프셋 이어받기(유실 없음).
  - 스키마 변경(DDL).
  - 원천 · 타깃 건수 대조.
  - 싱크의 upsert · delete.enabled · schema.evolution 동작.

## 2. 한눈에

| 구분 | 건수 |
|---|---|
| 통과 | 62 |
| 결함 · 주의 | 8 |
| 관찰(설계상 한계 · 확인 필요) | 10 |
| 합계 | 80 |

- **CDC 제어(시작 · 중지 · 재시작 · 배포 · 유지)와 데이터 정합(스냅샷 · I/U/D · 대량 · 트랜잭션 · 순서 · 복구 · 스키마 변경)은 정상**이다.
- 문제는 네 곳이다:
  - ETL 시작 실패 때 일부만 실행되는 것(중대).
  - 델타 append 적재 방식.
  - CDC 시작 확인 창이 짧은 것.
  - 로컬 ETL 템플릿 두 개가 깨져 있는 것.

## 3. 결함 · 주의(우선순위 순)

### 3-1. [중대] ETL 시작(start_pg)이 409 로 «실패»해도 그룹 앞부분은 실행돼 타깃이 비고, 남은 적재는 나중에 워크플로우 밖에서 저절로 돈다 (E9)

- 재현: 타깃 1050행 → 루트 `AvroReader` 만 끄고 워크플로우 실행.
  - start_pg 가 NiFi `409 Conflict` 로 실패하고, 재시도 뒤 워크플로우는 failed, stop_pg 는 success.
  - 그런데 그룹의 유효한 프로세서(시작 → TRUNCATE → SELECT)는 실행됐다. **타깃 0행**, SELECT 결과 1건이 대기열에 남았다.
- 이어서 워크플로우 없이 `AvroReader` 만 켰다.
  - 무효였던 04-1 · 05 프로세서가 **저절로 RUNNING** 이 되어 남은 건을 적재했다(40초 뒤 1050행).
  - 원장(etl_job_run) 기록이 없고, stop_pg 통제 밖에서 계속 RUNNING 으로 남았다.
- 운영 영향
  - 실패한 실행이 타깃 테이블을 비워 둔다.
  - 나중에 누가 서비스를 켜면 예정에 없던 적재가 돈다.
- 개선 방향(제안)
  - start_pg 가 시작 전에 그룹 안 모든 프로세서 VALID · 참조 서비스 ENABLED 를 먼저 확인한다. 안 되면 아무것도 켜지 않고, 어떤 프로세서 · 서비스가 문제인지 실패 사유로 남긴다.
  - 실패 · 정지 때는 그룹 대기열을 비우고 프로세서를 개별로 STOPPED 로 확정한다.
  - 참조 서비스를 켜 주는 것은 10/7 에 미결로 남긴 «start_pg 가 서비스 켜기» 안과 같이 결정한다.

### 3-2. [높음] CDC 델타 append 가 동작하지 않는다 (D10)

- 배포 때 `cdc_seq` · `cdc_op` 두 열만 가진 뼈대 테이블을 먼저 만든다(`DeltaTargetTableService`).
- 첫 레코드에서 싱크가 원천 컬럼을 `ALTER ADD` 하려다 거부된다: «Cannot ALTER table … because field 'id' is not optional but has no default value».
- 원천에 PK(= NOT NULL 컬럼)가 있으면 항상 재현된다. 실데이터 끝까지 시험은 처음이었다(9/3 도입 때 «사용자 검증 예정»).
- 개선 방향
  - 뼈대에 원천 컬럼을 NULL 허용으로 함께 만든다(타입 매핑 필요).
  - 또는 뼈대를 만들지 않고 싱크가 테이블을 만들게 한 뒤 구분컬럼 위치 요구를 다시 정한다.

### 3-3. [높음] «워크플로우는 성공인데 CDC 는 안 돈다»가 싱크 쪽 오류에서는 여전히 생긴다 (F1 · C4b · F2c)

- **F1 · F2c:** verify 가 «Source · Sink 가 한 번 RUNNING 이면 통과»다.
  - 싱크가 데이터를 실제로 쓰다 나는 오류는 재시작 뒤 약 60초 뒤에 FAILED 로 드러난다(14:46:17 재시작 → 14:46:22 verify 통과 → 14:47:17 FAILED).
- **C4b:** CDC 노드 apply 의 기본 재시도(1회, 30초)가 백엔드의 올바른 거절을 덮는다.
  - 백엔드가 «Kafka Connect가 실패 상태»로 정확히 거절해도, 재시도가 FAILED 파이프라인에 start 를 다시 보내 싱크를 새로 띄운다. 그 순간 RUNNING 이라 통과한다.
- 감시는 동작한다: 상태 FAILED 동기화 약 24초, CONNECTOR_FAILED 알림 약 16초. 다만 워크플로우 결과만 보는 사람에게는 성공으로 보인다.
- 개선 방향
  - verify 를 «기대 상태가 N초(싱크 재시도 시간보다 길게) 유지되고, 밀린 레코드가 있으면 싱크 오프셋이 전진함»으로 바꾼다.
  - FAILED 상태에서 start 가 거절된 경우는 재시도하지 않게 한다.

### 3-4. [중간] 로컬 ETL 템플릿 Initial · Incremental 이 없는 서비스를 가리켜, INSERT/UPSERT 로 만든 잡은 처음부터 INVALID (E1x)

- `Template > Incremental · Initial` 의 프로세서가 NiFi 어디에도 없는 서비스를 참조한다.
  - Record Reader(a944f74a…) · Writer(1a55f587…) · DBCP(dffe7709…).
- 생성 API 는 DBCP 만 바꾸고 Reader/Writer 는 템플릿 값을 복사한다. 그래서 새 잡이 «Property references a Controller Service that does not exist» 가 된다.
- TRUNCATE(`truncate_initial`)는 루트 Avro 서비스를 쓰며 정상 생성된다. 그 서비스가 꺼져 있으면 3-1 로 이어진다.
- 템플릿은 ETL 담당 영역이라 고치지 않았다. MSA 템플릿은 확인하지 않았다.

### 3-5. [중간] start_pg 가 참조 서비스를 켜지 않고, NiFi 409 의 사유를 버린다 (E3 · E4x)

- 연결 풀 · Avro 서비스가 DISABLED 면 start_pg 는 409 로 실패한다.
  - Airflow 로그에는 «409 Client Error: Conflict» 만 남는다. 무엇이 무효인지는 NiFi 를 직접 봐야 안다.
- NiFi 재기동 뒤(autoResumeState=false) 서비스가 꺼지는 문제와 같은 뿌리다.

### 3-6. [낮음] 그 밖

- **L2:** 로그파일 파이프라인은 배포 전(CREATED)에 워크플로우 «시작»을 주면 거절된다.
  - 거절 사유: «먼저 /deploy를 호출하세요». 테이블 CDC 는 READY 에서 바로 시작(배포+시작)이 된다. 같은 지시가 종류마다 다르다.
  - 거절 문구가 Airflow 태스크 로그 본문엔 안 보이고, 구조화 로그의 오류 항목에만 있다.
- **CL2:** 실패 상태로 지운 파이프라인의 알림(CONNECTOR_FAILED)은 대상이 없어진 뒤에도 FIRING 으로 남는다. 시험 알림 #196 은 확인(ack) 처리했다.
- **E1n:** ETL 생성 입력 오류(적재로직 SQL 누락)가 HTTP 502 로 온다(입력 오류는 400).

## 4. 관찰(설계상 한계 · 확인 필요)

| # | 내용 |
|---|---|
| D8b | 삭제 미반영 파이프라인에서 PK 를 바꾸면 옛 키 행도 남는다(두 행) - 삭제를 반영하지 않는 설계의 결과 |
| D12 | 정합성 점검 API 는 정확한 COUNT 가 아니라 DB 통계 추정치(STATISTICS_ESTIMATE) 비교다(10100=10100 MATCH) |
| M2b | 커넥터를 플랫폼 밖에서 지우면 대시보드 «어긋남»은 잡히지만 파이프라인 상태는 DEPLOYED 그대로, restart 는 실패(재배포 → 시작으로 복구됨) |
| E10b | 새로 만든 잡의 첫 실행은 완료 판정이 콜백(3~4초)이 아니라 관측(OBSERVED)으로 약 3분 뒤 닫혔다. 두 번째부터 정상 - NAR 완료 감시가 새 그룹 첫 관측 때 기준값만 잡는 것으로 보임, 확인 필요 |
| O1a | DB 30초 장애 뒤 Debezium 소스 · JDBC 싱크가 스스로 다시 붙어 변경이 반영됐다(재시작 불필요) |
| X1 | ETL 잡과 CDC 노드는 한 워크플로우에 섞을 수 없다(검증 V12, 문구 명확) - 설계대로 |

## 5. 통과한 시험(요약)

| 분류 | 시험 | 결과 |
|---|---|---|
| 제어 | 배포만 → 커넥터 전부 STOPPED, 다른 노드 건너뜀 | 통과 |
| 제어 | 시작 → Source · Sink RUNNING · 초기 스냅샷 100건 · 값 일치(NULL · 한글) | 통과 |
| 제어 | 이미 실행 중에 시작 · 멈춘 것에 중지 · 멈춘 것에 재시작 → 건너뜀 | 통과 |
| 제어 | 배포 전(READY) 테이블 CDC 에 바로 시작 → 배포+시작 | 통과 |
| 제어 | 전부 «유지» → 모두 건너뜀 · 한 노드 실패가 같은 실행의 다른 노드를 막지 않음 | 통과 |
| 제어 | 연달아 두 번 실행 → 겹치지 않고 차례로(동시 1) | 통과 |
| 데이터 | 스냅샷 NO_DATA(기존 50건 안 넘어옴) · INSERT · UPDATE(NULL 포함) · DELETE(반영/미반영) | 통과(3초 안) |
| 데이터 | 대량 1만 건 한 트랜잭션 | 통과(13.4초, 약 750건/초) |
| 데이터 | ROLLBACK 은 안 넘어오고 COMMIT 묶음은 넘어옴 · 같은 키 200회 연속 UPDATE 마지막 값 | 통과 |
| 데이터 | PK 변경(삭제 반영) · 자료형 경계값(numeric 최대 · int 최소 · 1만 자 · 이모지 · 줄바꿈 · 타임존 µs) | 통과 |
| 데이터 | 델타 upsert(PK당 한 행 · 마지막 op · cdc_ts) · 실시간 지표(지연 · 마지막 처리) | 통과 |
| 복구 | 중지 중 원천 변경(I 300 · U 10 · D 5) → 타깃 그대로 → 시작 뒤 20초 안에 전부 따라잡음 | 통과 |
| 복구 | 실행 중 재시작 · 커넥터를 Kafka Connect 에서 직접 재시작(그 사이 200건) → 유실 · 중복 0 | 통과 |
| 복구 | 타깃이 거부(CHECK 위반) → FAILED 감지 · 알림 → 원인 고친 뒤 재시작 → 거부된 건부터 이어서 적재 | 통과 |
| 복구 | 커넥터를 밖에서 지움 → 대시보드 어긋남 감지 → 재배포 · 시작으로 복구 | 통과 |
| 스키마 | 컬럼 추가 · NOT NULL+DEFAULT 추가 · 길이 늘리기 · 삭제 · 이름 변경 | 통과(5/5) |
| 로그파일 | 배포 → 시작 → 처음부터 적재 · 꼬리 읽기 · 중지 중 추가분 시작 뒤 전부(파일 90줄 = 타깃 90행) | 통과 |
| ETL | 잡 2개 체인 정상 실행(각 1050건 · 값 일치) · 재실행 멱등 · 원천 변경 반영(1040) | 통과 |
| ETL | 두 번째 잡 실패 → 재시도 → 워크플로우 failed, 원장 FAILED + 원인 문구 → 고친 뒤 성공 | 통과 |
| ETL | 연달아 두 번 → 차례로 · 실행 뒤 그룹 멈춤 · 대기열 0 · 원장 SUCCESS + 건수 | 통과 |
| 보호 | 워크플로우(초안 포함)에 든 파이프라인 삭제 거절 · 사용 중인 워크플로우 이름 안내 | 통과 |
| 정리 | 파이프라인 삭제 → 커넥터 · 복제 슬롯 · 퍼블리케이션 · 토픽 · filebeat 입력 모두 정리 | 통과 |

## 6. 하지 않은 것

- **Kafka Connect 워커 전체 재시작**
  - 기존 AAA-* 커넥터가 MSA Oracle 을 가리켜, 워커를 다시 띄우면 MSA 에 붙는다(Debezium Oracle 은 원천에 flush 테이블을 쓴다).
  - 대신 시험 커넥터만 Kafka Connect REST 로 재시작했다.
- Oracle · MySQL 원천, MSA 환경, Kafka 브로커 장애, 대용량(수백만 건) · 장시간 운전, NiFi 재기동 중 실행.
- 코드 수정. 이번 작업은 시험 · 원인 확인까지다. 3절의 개선 방향은 제안이다.

## 7. 정리한 것

- 워크플로우 3(QA1010_cdc · _etl · _mix), 파이프라인 5(17~21)를 삭제했다.
  - 복제 슬롯 4 · 퍼블리케이션 · 토픽 · filebeat 입력이 남지 않은 것을 확인했다.
- NiFi `QA1010_ETL` 그룹(잡 3)을 삭제했다.
  - 잡 카탈로그는 동기화로 삭제 표시됐다. 시험 원장(etl_job_run) 23행은 지웠다.
- NiFi 서비스: 시험 동안 켠 `cdc-3-target-db` · 루트 `AvroReader` · `AvroRecordSetWriter` 를 다시 DISABLED 로 돌렸다.
  - 전체 ENABLED 6 / DISABLED 15 로, 시험 전과 같다.
- target-db 의 `qa_src` · `qa_tgt` 스키마와 `log-sources/qa1010` 을 지웠다. target-db 는 다시 정지했다(시험 전 상태).
- 시험 도구(스크립트 · 결과 jsonl)는 세션 작업 폴더에만 있다(저장소 밖).

## 8. 참고 자료

- [Debezium connector for JDBC (upsert · delete.enabled · schema.evolution)](https://debezium.io/documentation/reference/3.3/connectors/jdbc.html)
- [Debezium Initial Snapshot vs Streaming: Best Practices](https://risingwave.com/blog/debezium-initial-snapshot-vs-streaming)
- [How to Handle Debezium Schema Changes](https://oneuptime.com/blog/post/2026-01-28-debezium-schema-changes/view)
- [Why PostgreSQL CDC breaks in production](https://hackernoon.com/why-postgresql-cdc-breaks-in-production)
- [CDC vs Dual Writes(멱등 싱크 · 키 단위 순서)](https://risingwave.com/blog/cdc-vs-dual-writes/)
