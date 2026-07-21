-- 정형 데이터 CDC 테스트용 샘플 테이블 (appuser 스키마, XEPDB1)
ALTER SESSION SET CONTAINER = XEPDB1;

CREATE TABLE appuser.customers (
  id         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name       VARCHAR2(100) NOT NULL,
  email      VARCHAR2(150),
  updated_at TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- 테이블 단위 보충 로깅 (PK/UK 변경분까지 포함해 전체 컬럼 캡처)
ALTER TABLE appuser.customers ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO appuser.customers (name, email) VALUES ('Hong Gildong', 'hong@example.com');
INSERT INTO appuser.customers (name, email) VALUES ('Kim Cheolsu', 'kim@example.com');
COMMIT;

-- CLOB 컬럼 CDC 전파 테스트용 테이블
CREATE TABLE appuser.clob_test_data (
  id         NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name       VARCHAR2(10),
  file2      CLOB,
  update_at  TIMESTAMP
);

ALTER TABLE appuser.clob_test_data ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
