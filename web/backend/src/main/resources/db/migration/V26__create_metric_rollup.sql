-- =============================================================================
-- V26__create_metric_rollup.sql  (통합 운영 대시보드 / 설계서 4-5절, U7)
-- =============================================================================
-- 누적형 KPI · 차트 · 베이스라인 학습의 단일 소스.
--
-- ■ 핵심 결함 해소: "0건 행이 없다"
--   행이 없으면 "적재 0건"과 "수집이 죽었다"를 구분할 수 없다(사고 1의 데이터모델 뿌리).
--     loaded_count=0, observation_count=180  -> "180번 봤고 정말 0건" (확정)
--     행 없음                                -> "본 적이 없다"        (미관측)
--   호출부(Kafka/Nifi MetricScheduler.checkOne)의 `if (delta>0)` 가드를 제거하고
--   recordObservation(...)로 delta==0 에도 호출한다(U7).
--
-- ■ 버킷 경계는 install_info.display_zone 로컬 정시로 자른다(저장은 TIMESTAMPTZ 절대시각).
--
-- D-5 결정: env_kind/pipeline_env_tag 는 두지 않는다(PERF 잔여물은 고객사에 없음, WORK_LOG §19.1).
-- -----------------------------------------------------------------------------
CREATE TABLE pipeline_load_rollup (
    id                BIGSERIAL    PRIMARY KEY,
    granularity       VARCHAR(6)   NOT NULL,          -- MIN5 / HOUR / DAY
    bucket_start      TIMESTAMPTZ  NOT NULL,
    pipeline_source   VARCHAR(20)  NOT NULL,          -- KAFKA / NIFI
    -- KAFKA=pipeline_definition.id, NIFI=프로세서 UUID(프로세스그룹 아님).
    pipeline_key      VARCHAR(200) NOT NULL,
    task_key          VARCHAR(200) NOT NULL,
    pipeline_label    VARCHAR(200),
    -- 딥링크·잡 조인을 위한 식별자. 프로세서 UUID 만으로는 잡(pg_id 축)과 조인되지 않는다.
    etl_job_id        BIGINT,
    loaded_count      BIGINT       NOT NULL DEFAULT 0,
    -- 이 버킷에서 수집을 시도해 값을 얻은 횟수. 0이면 행이 없다.
    observation_count INTEGER      NOT NULL DEFAULT 0,
    error_count       INTEGER      NOT NULL DEFAULT 0,
    first_observed_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    last_observed_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT ck_rollup_gran   CHECK (granularity IN ('MIN5','HOUR','DAY')),
    CONSTRAINT ck_rollup_nonneg CHECK (loaded_count >= 0 AND observation_count >= 0)
)
-- DAY 버킷 1행은 20초 주기로 하루 4,320회 갱신된다. fillfactor 100 이면 HOT 갱신이 실패하고
-- 인덱스가 비대해진다(384MB 컨테이너에서 autovacuum 이 못 따라감).
WITH (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.02);

CREATE UNIQUE INDEX uq_pipeline_load_rollup
    ON pipeline_load_rollup (granularity, bucket_start, pipeline_source, pipeline_key, task_key);
-- 대시보드 기본 조회: granularity + 최근 24시간 + 소스별 합계(env_kind 제거).
CREATE INDEX idx_rollup_scan  ON pipeline_load_rollup (granularity, bucket_start DESC, pipeline_source);
-- 파이프라인 단위(딥링크 상세, 베이스라인 학습).
CREATE INDEX idx_rollup_key   ON pipeline_load_rollup (pipeline_source, pipeline_key, granularity, bucket_start DESC);
CREATE INDEX idx_rollup_purge ON pipeline_load_rollup (granularity, bucket_start);

-- 보존(전용 로직, RetentionService 가 granularity 별로): MIN5 3일 / HOUR 90일(30d 프리셋 보정에
-- 60일 전 HOUR 필요, 최소 65일) / DAY 1095일.
INSERT INTO retention_policy (table_name, time_column, retention_days, min_retention_days, purge_mode, updated_by)
VALUES ('pipeline_load_rollup', 'bucket_start', 1095, 65, 'DELETE_BATCH', 'system')
ON CONFLICT (table_name) DO NOTHING;
