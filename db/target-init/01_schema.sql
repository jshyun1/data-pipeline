-- "타란툴라DB"(PostgreSQL 기반) 랜딩 스키마.
-- CDC 이벤트(Debezium payload)를 NiFi가 적재할 착지 테이블입니다.

CREATE SCHEMA IF NOT EXISTS cdc_landing;

CREATE TABLE IF NOT EXISTS cdc_landing.customers (
  id          BIGINT PRIMARY KEY,
  name        VARCHAR(100) NOT NULL,
  email       VARCHAR(150),
  updated_at  TIMESTAMP,
  cdc_op      CHAR(1),      -- Debezium op: c(insert)/u(update)/d(delete)/r(snapshot)
  cdc_ts_ms   BIGINT
);

-- CLOB 컬럼 CDC 전파 테스트용 착지 테이블
CREATE TABLE IF NOT EXISTS cdc_landing.clob_test_data (
  id          BIGINT PRIMARY KEY,
  name        VARCHAR(10),
  file2       TEXT,
  update_at   TIMESTAMP,
  cdc_op      CHAR(1),
  cdc_ts_ms   BIGINT
);

-- NiFi가 1시간 주기로 폴링/전체 재적재하는 배치 랜딩 테이블 (CDC 아님)
CREATE SCHEMA IF NOT EXISTS batch_landing;

CREATE TABLE IF NOT EXISTS batch_landing.employees (
  employee_id     BIGINT PRIMARY KEY,
  first_name      VARCHAR(20),
  last_name       VARCHAR(25) NOT NULL,
  email           VARCHAR(25) NOT NULL,
  phone_number    VARCHAR(20),
  hire_date       DATE NOT NULL,
  job_id          VARCHAR(10) NOT NULL,
  salary          NUMERIC,
  commission_pct  NUMERIC,
  manager_id      BIGINT,
  department_id   BIGINT,
  synced_at       TIMESTAMP DEFAULT now()
);

-- 비정형(파일) 데이터 랜딩용 메타데이터 테이블
CREATE SCHEMA IF NOT EXISTS unstructured_landing;

CREATE TABLE IF NOT EXISTS unstructured_landing.file_objects (
  id           BIGSERIAL PRIMARY KEY,
  source_path  TEXT NOT NULL,
  mime_type    VARCHAR(100),
  extracted_text TEXT,
  raw_object   BYTEA,
  ingested_at  TIMESTAMP DEFAULT now()
);

-- logfile 디렉터리 감시 플로우(NiFi ListFile/FetchFile) 착지 테이블. 로그 형식을
-- 구분하지 않고 파일의 모든 줄을 그대로 한 행씩 적재한다(Kafka/Filebeat 로그
-- 파이프라인과 동일한 "포맷 불문 통짜 적재" 방식).
CREATE TABLE IF NOT EXISTS unstructured_landing.log_file_lines (
  id           BIGSERIAL PRIMARY KEY,
  message      TEXT NOT NULL,
  source_file  TEXT NOT NULL,
  ingested_at  TIMESTAMP DEFAULT now()
);
