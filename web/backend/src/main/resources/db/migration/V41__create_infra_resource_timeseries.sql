-- =============================================================================
-- V41__create_infra_resource_timeseries.sql  (설계서 4-8절, U36)
-- =============================================================================
-- 서버 리소스 이력 저장소(지금까지 0곳). Prometheus 대신 PostgreSQL 한 테이블 + 보존/다운샘플.
-- 시각은 전부 TIMESTAMPTZ(3-3절). 1분 해상도는 무한 insert/delete 라 파티션 + DROP PARTITION.
-- 안전망으로 DEFAULT 파티션을 둬 일 파티션이 없어도 INSERT 가 실패하지 않게 한다(수집 무중단);
-- 평시엔 PartitionMaintenanceJob 이 일 파티션을 미리 만든다.
-- -----------------------------------------------------------------------------
CREATE TABLE infra_resource_sample (
    id             BIGSERIAL,
    scope          VARCHAR(20)  NOT NULL,
    target_key     VARCHAR(200) NOT NULL,
    metric         VARCHAR(30)  NOT NULL,
    sampled_at     TIMESTAMPTZ  NOT NULL,   -- 분 단위 절삭(시리즈당 1분 1행)
    used_bytes     BIGINT,
    total_bytes    BIGINT,
    used_percent   NUMERIC(5,2),            -- 0~100. CPU/PSI 처럼 바이트 없는 지표도 여기
    value_num      NUMERIC(18,3),           -- 퍼센트로 표현 불가한 값(load1/코어수/OOM/PSI avg60)
    -- OK / PARTIAL(부분값, 상향판정만) / SUSPECT(범위밖, 기록만) / UNAVAILABLE(deadline 초과)
    quality        VARCHAR(12)  NOT NULL DEFAULT 'OK',
    is_lower_bound BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (id, sampled_at)
) PARTITION BY RANGE (sampled_at);

-- DEFAULT 파티션(안전망). 일 파티션이 없어도 INSERT 실패 방지.
CREATE TABLE infra_resource_sample_pdefault
    PARTITION OF infra_resource_sample DEFAULT;

CREATE UNIQUE INDEX uq_infra_resource_sample
    ON infra_resource_sample (scope, target_key, metric, sampled_at);
CREATE INDEX idx_infra_resource_sample_time ON infra_resource_sample (sampled_at);

-- 다운샘플(HOUR/DAY 한 테이블). avg 만 남기면 순간 피크가 사라지므로 min/max 도 남긴다.
-- sample_count 는 결손 판정용(1시간 60개 정상인데 12개면 흐리게 그린다).
CREATE TABLE infra_resource_rollup (
    id               BIGSERIAL    PRIMARY KEY,
    scope            VARCHAR(20)  NOT NULL,
    target_key       VARCHAR(200) NOT NULL,
    metric           VARCHAR(30)  NOT NULL,
    granularity      VARCHAR(6)   NOT NULL,          -- HOUR | DAY
    bucket_start     TIMESTAMPTZ  NOT NULL,
    sample_count     INTEGER      NOT NULL,
    partial_count    INTEGER      NOT NULL DEFAULT 0,
    avg_percent      NUMERIC(5,2),
    min_percent      NUMERIC(5,2),
    max_percent      NUMERIC(5,2),
    avg_used_bytes   BIGINT,
    max_used_bytes   BIGINT,
    last_total_bytes BIGINT,     -- 용량 한도는 변하므로 평균이 아니라 최종값
    avg_value        NUMERIC(18,3),
    max_value        NUMERIC(18,3),
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT ck_infra_rollup_gran CHECK (granularity IN ('HOUR','DAY'))
);
CREATE UNIQUE INDEX uq_infra_resource_rollup
    ON infra_resource_rollup (scope, target_key, metric, granularity, bucket_start);
CREATE INDEX idx_infra_resource_rollup_scan
    ON infra_resource_rollup (granularity, bucket_start DESC, scope, target_key, metric);
CREATE INDEX idx_infra_resource_rollup_purge
    ON infra_resource_rollup (granularity, bucket_start);

-- 보존 시드 2행(C10). sample 1분 해상도 3일(파티션 DROP), rollup DAY 730일.
INSERT INTO retention_policy (table_name, time_column, retention_days, min_retention_days, purge_mode, updated_by)
VALUES ('infra_resource_sample', 'sampled_at',   3,  1, 'PARTITION_DROP', 'system'),
       ('infra_resource_rollup', 'bucket_start', 730, 90, 'DELETE_BATCH',  'system')
ON CONFLICT (table_name) DO NOTHING;
