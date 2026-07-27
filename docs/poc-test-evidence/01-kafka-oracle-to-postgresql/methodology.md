# 테스트 1 — Kafka 경로 Oracle → PostgreSQL (1만 건)

## 대상 테이블

**Oracle**: `CSB.KAFKA_PERF_TEST`
```sql
CREATE TABLE CSB.KAFKA_PERF_TEST (
  order_id NUMBER PRIMARY KEY,
  order_date DATE,
  customer_name VARCHAR2(100),
  product_name VARCHAR2(100),
  quantity NUMBER,
  unit_price NUMBER(12,2),
  updated_at TIMESTAMP
);
ALTER TABLE CSB.KAFKA_PERF_TEST ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```
(테이블별 SUPPLEMENTAL LOG DATA 미설정 시 LogMiner가 변경분을 캡처하지 못함 — `docs/kafka-cdc-pipeline.md` 3단계 참고)

**PostgreSQL (target-db)**: `public.kafka_perf_test` (동일 컬럼 구조)

## 데이터 생성 방식

Python으로 생성한 10,000건을 1,000건씩 10개 배치로 나누어 Oracle의 `INSERT ALL ... SELECT * FROM dual` 다중행 삽입 구문으로 실행 (원본 스크립트: `gen_orders_sql.py`, 생성된 SQL 배치 10개: `source-files/orders_batch_00.sql` ~ `09.sql`). customer_name/product_name/quantity/unit_price는 고정 시드(42)로 재현 가능하게 랜덤 생성.

실행은 NiFi에 이미 구성되어 있던 Oracle 커넥션 풀(`cp-oracle-192-168-204-128`)을 재사용해 PutSQL 프로세서로 배치당 1회씩 실행.

## Kafka Connect 커넥터

- Source: `perf-test-v2-source-oracle` (Debezium Oracle Connector, LogMiner, `table.include.list: CSB.KAFKA_PERF_TEST`)
- Sink: `perf-test-v2-sink-postgresql` (Debezium JDBC Sink, `insert.mode: insert`, topic `perf-test-v2.CSB.KAFKA_PERF_TEST`)

두 커넥터 모두 테스트 종료 시점 기준 Kafka Connect에 등록된 상태로 남겨둠(RUNNING, idle).

## 측정 방법

1. 배치 0~9를 순차 실행하며 각 배치 트리거 완료 시각 기록 (t=0부터 경과 시간)
2. 10개 배치 실행 완료 시점을 "Oracle INSERT 완료" 시각으로 기록
3. 이후 target-db의 `kafka_perf_test` row count를 1초 간격으로 폴링하여 10,000건에 도달하는 시각을 "E2E 완료" 시각으로 기록

**주의**: "Oracle INSERT 소요" 수치에는 배치마다 NiFi REST API를 통해 프로세서 설정을 갱신하는 오케스트레이션 오버헤드(배치당 약 3.5~4초)가 포함되어 있어, 순수 Oracle 삽입 성능이 아니라 이 테스트 하네스 자체의 오버헤드가 대부분을 차지합니다. **CDC 전파 구간(Oracle INSERT 완료 → Postgres 10,000건 반영)이 실질적인 파이프라인 성능 측정치**입니다.

## 결과

| 구간 | 소요 시간 |
|---|---|
| Oracle INSERT 10,000건 (오케스트레이션 오버헤드 포함) | 38.78초 |
| CDC 전파 (Oracle INSERT 완료 → Postgres 10,000건 반영) | 2.34초 |
| 전체 E2E | 41.12초 |

검증: `target-db.kafka_perf_test` count=10000, order_id 1~10000 연속, 에러 0건. 원본 실행 로그: `kafka_test_result_v2.log`.

## 재현 시 참고사항

- Oracle 계정(`CSB`)은 이 세션 중 두 차례 `ORA-01017`(비밀번호 불일치로 추정, 계정 잠금과는 별개)을 겪었음 — NiFi 컨트롤러 서비스의 저장된 비밀번호가 실제 값과 다를 경우 재현 전 반드시 `config/verification-requests` API로 먼저 연결 확인할 것.
- NiFi `PutSQL`은 세미콜론으로 여러 문장을 구분해도 **분리 실행하지 않고 통째로 한 문장으로 전송**함 — 다건 삽입은 반드시 Oracle의 `INSERT ALL` 구문처럼 단일 문장으로 묶어야 함.
