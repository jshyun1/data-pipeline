-- 비정형 데이터 적재 대상 스키마 (타란툴라DB: 192.168.50.12:7432 / database=postgres)
--
-- NiFi "비정형" 프로세스그룹이 HTTP로 수신한 파일을 종류별로 이 테이블들에 넣는다.
--   이미지/동영상 : 원본 바이트를 그대로 bytea 로 보관 (파싱하지 않음)
--   로그/XML/CSV  : 파싱해서 컬럼 단위 구조화 레코드로 보관
--
-- 이 파일은 자동 실행되지 않는다(타란툴라DB는 우리 compose가 띄우는 DB가 아님).
-- 적용:
--   PGPASSWORD=... psql -h 192.168.50.12 -p 7432 -U tarandb -d postgres -f 01_unstructured_schema.sql

CREATE SCHEMA IF NOT EXISTS unstructured;

-- ---------------------------------------------------------------------------
-- 1) 이미지 - 바이너리 원본
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS unstructured.image_files (
    id           bigserial PRIMARY KEY,
    file_name    text        NOT NULL,
    mime_type    text,
    byte_size    bigint,
    content      bytea       NOT NULL,
    source_host  text,
    ingested_at  timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE unstructured.image_files IS '비정형-이미지: HTTP로 수신한 이미지 원본 바이너리';

-- ---------------------------------------------------------------------------
-- 2) 동영상 - 바이너리 원본
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS unstructured.video_files (
    id           bigserial PRIMARY KEY,
    file_name    text        NOT NULL,
    mime_type    text,
    byte_size    bigint,
    content      bytea       NOT NULL,
    source_host  text,
    ingested_at  timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE unstructured.video_files IS '비정형-동영상: HTTP로 수신한 동영상 원본 바이너리';

-- ---------------------------------------------------------------------------
-- 3) 로그 - 한 줄이 한 행. GrokReader가 파싱한 결과.
--    컬럼명은 NiFi PutDatabaseRecord의 이름 매칭에 그대로 쓰이므로
--    GrokReader의 캡처 이름(log_time/log_level/logger/message)과 일치시킨다.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS unstructured.log_records (
    id           bigserial PRIMARY KEY,
    log_time     text,
    log_level    text,
    logger       text,
    message      text,
    source_file  text,
    ingested_at  timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE unstructured.log_records IS '비정형-로그: 로그 파일을 Grok으로 파싱한 구조화 레코드';
COMMENT ON COLUMN unstructured.log_records.log_time IS 'Grok이 뽑은 원본 시각 문자열(포맷이 소스마다 달라 text로 보관)';

-- ---------------------------------------------------------------------------
-- 4) XML - <records><record>...</record></records> 구조를 파싱한 결과
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS unstructured.xml_records (
    id           bigserial PRIMARY KEY,
    record_id    text,
    title        text,
    category     text,
    amount       numeric(18,2),
    reg_date     date,
    source_file  text,
    ingested_at  timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE unstructured.xml_records IS '비정형-XML: XMLReader가 파싱한 구조화 레코드';

-- ---------------------------------------------------------------------------
-- 5) CSV - 헤더가 있는 CSV를 파싱한 결과 (XML과 동일 스키마)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS unstructured.csv_records (
    id           bigserial PRIMARY KEY,
    record_id    text,
    title        text,
    category     text,
    amount       numeric(18,2),
    reg_date     date,
    source_file  text,
    ingested_at  timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE unstructured.csv_records IS '비정형-CSV: CSVReader가 파싱한 구조화 레코드';
