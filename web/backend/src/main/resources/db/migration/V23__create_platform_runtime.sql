-- =============================================================================
-- V23__create_platform_runtime.sql  (통합 운영 대시보드 개편 / 설계서 4-3절)
-- =============================================================================
-- 하트비트 · 결측 구간 · 보존 정책 · 설치 식별. 전부 신규 테이블이라 TIMESTAMPTZ 사용.

-- -----------------------------------------------------------------------------
-- 수집기·엔진 생존 상태. 컴포넌트당 1행, UPDATE in place 라 행 수가 유계다.
--
-- ■ 왜 DB 인가: 현재 유일한 생존 근거인 NifiPipelineMetricScheduler.lastCounterCheckAt 은
--   JVM 메모리 필드(volatile LocalDateTime)라 재기동되면 UNKNOWN 부터 다시 시작한다.
-- ■ 생존 판정은 "행을 썼는가"가 아니라 "주기를 완주했는가"다. 각 수집기는 주기 종료 시
--   성공/실패/대상0건 무관하게 이 행을 갱신한다(그래야 신규 설치 첫날 CRITICAL 오탐이 없다).
-- ■ 시드하지 않는다: last_beat_at 을 now() 로 시드하면 metadata-db(UTC)/앱(KST) 9시간 차로
--   마이그레이션 직후 전 컴포넌트가 DOWN 오탐을 낸다. nullable 로 두고 앱이 첫 실행 때 UPSERT.
-- -----------------------------------------------------------------------------
CREATE TABLE system_heartbeat (
    component_key             VARCHAR(60)  PRIMARY KEY,
    component_label           VARCHAR(120) NOT NULL,
    -- NIFI / KAFKA / AIRFLOW / INFRA / ENGINE  (차트가 결측을 계열별로 판정한다)
    metric_source             VARCHAR(20)  NOT NULL,
    last_beat_at              TIMESTAMPTZ,                  -- 시드하지 않는다
    -- 이 수집기가 처음 관측을 남긴 시각. 이보다 이른 구간은 차트에서 무조건 NO_DATA.
    first_observed_at         TIMESTAMPTZ,
    expected_interval_seconds INTEGER      NOT NULL,
    -- 직전 10주기 실측 간격의 중앙값. 실효 임계 = max(설정값, observed x 3).
    observed_interval_seconds INTEGER,
    last_result               VARCHAR(20)  NOT NULL DEFAULT 'OK',   -- OK / FAILED
    last_error                TEXT,
    consecutive_failures      INTEGER      NOT NULL DEFAULT 0,
    -- 다중 인스턴스 실수 탐지용. 값이 계속 바뀌면 2개가 떠 있는 것이다.
    instance_id               VARCHAR(40),
    updated_at                TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- 결측 구간. 차트가 "이 버킷에 관측이 있었나"를 판정하는 진짜 소스다.
-- "값이 없다" = ① 원천 무변경(0이 정답) / ② 수집기 죽음(모름) / ③ 테이블 부재기(모름).
-- 값만 봐선 구분 불가. 배포 재기동도 결측으로 기록하되 detected_by 로 화면 표현을 구분한다.
-- -----------------------------------------------------------------------------
CREATE TABLE collector_outage (
    id            BIGSERIAL   PRIMARY KEY,
    component_key VARCHAR(60) NOT NULL REFERENCES system_heartbeat (component_key),
    started_at    TIMESTAMPTZ NOT NULL,   -- 마지막 성공 시각. 그 이후가 모르는 구간
    ended_at      TIMESTAMPTZ,            -- 재개 후 첫 성공. 미복구면 NULL
    -- GAP(주기 3회 연속 이탈) / STARTUP(앱 재기동, 정상 배포 포함) / FAILURE(예외)
    detected_by   VARCHAR(20) NOT NULL,
    reason        TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_collector_outage_window ON collector_outage (component_key, started_at DESC);
-- 한 수집기에 열린 결측 구간은 하나뿐이다(V15/V18 의 부분 UNIQUE 규약과 동일).
CREATE UNIQUE INDEX uq_collector_outage_open
    ON collector_outage (component_key) WHERE ended_at IS NULL;

-- -----------------------------------------------------------------------------
-- 보존 정책.
--
-- ■ 여기는 시드를 한다(app_setting 과 반대): 정책 행이 없다 = 그 테이블은 영원히 정리
--   되지 않는다. app_setting 은 행이 없어도 코드 기본값이 동작하므로 시드가 불필요했다.
-- ■ 배치 상한을 "횟수"가 아니라 "시간 예산"으로 잡는다(잔량 0까지 돌되 1회 실행 20분 상한,
--   주기도 일 1회가 아니라 매시간). 횟수 상한은 유입이 더 크면 backlog 를 쌓으면서도
--   자가진단은 초록으로 뜨는 함정이 있다.
-- ■ 정리 가능한 테이블은 코드 화이트리스트가 원천이다(table_name/time_column 은 동적 SQL
--   조립이라 바인딩 불가 → quote_ident 로 감싸고 화이트리스트 밖 이름은 무시+자가진단 표시).
-- -----------------------------------------------------------------------------
CREATE TABLE retention_policy (
    table_name         VARCHAR(63)  PRIMARY KEY,
    time_column        VARCHAR(63)  NOT NULL,
    retention_days     INTEGER      NOT NULL,
    min_retention_days INTEGER      NOT NULL DEFAULT 1,   -- 고객이 이보다 짧게 못 줄인다
    batch_rows         INTEGER      NOT NULL DEFAULT 5000,
    -- PARTITION_DROP 이면 DELETE 대신 파티션을 떨군다(pipeline_metric_snapshot, V27 전환 후).
    purge_mode         VARCHAR(20)  NOT NULL DEFAULT 'DELETE_BATCH',
    enabled            BOOLEAN      NOT NULL DEFAULT true,
    last_run_at        TIMESTAMPTZ,
    last_deleted_rows  BIGINT       NOT NULL DEFAULT 0,
    last_duration_ms   BIGINT,
    -- 이번 실행 후 남은 보존초과 행수. 0이 아닌 채 유지되면 정리가 유입을 못 따라간다.
    backlog_rows       BIGINT       NOT NULL DEFAULT 0,
    last_error         TEXT,
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_by         VARCHAR(255) NOT NULL DEFAULT 'system',
    CONSTRAINT ck_retention_days       CHECK (retention_days >= min_retention_days),
    CONSTRAINT ck_retention_batch_rows CHECK (batch_rows BETWEEN 100 AND 50000),
    CONSTRAINT ck_retention_purge_mode CHECK (purge_mode IN ('DELETE_BATCH','PARTITION_DROP'))
);

-- 보존일 근거: metric_snapshot 30일(파티션 DROP), nifi_execution_log/processor_run 90일,
-- command_history 365일, collector_outage 180일. pipeline_daily_load_metric 은 정리 안 함(일 롤업).
-- 주의: pipeline_metric_snapshot 의 PARTITION_DROP 은 V27 파티션 전환 이후에만 유효하다.
INSERT INTO retention_policy
    (table_name, time_column, retention_days, min_retention_days, purge_mode, updated_by)
VALUES
    ('pipeline_metric_snapshot', 'collected_at',  30,  3, 'PARTITION_DROP', 'system'),
    ('nifi_execution_log',       'occurred_at',   90,  7, 'DELETE_BATCH',   'system'),
    ('nifi_processor_run',       'started_at',    90,  7, 'DELETE_BATCH',   'system'),
    ('pipeline_command_history', 'requested_at', 365, 30, 'DELETE_BATCH',   'system'),
    ('collector_outage',         'started_at',   180, 30, 'DELETE_BATCH',   'system')
ON CONFLICT (table_name) DO NOTHING;

-- -----------------------------------------------------------------------------
-- 설치 식별 + 온보딩 상태 + 표시 타임존 (싱글턴).
--
-- 멀티테넌시(tenant_id)를 넣지 않는 대신 두는 식별자다. 알림 제목 접두어로 어디 설치인지
-- 구분한다:  [Cerebro/부산공장] 위험: 메모리 임계 초과 (92%)
-- display_zone: DAILY 평가 / 09:00 데드맨 요약 / 베이스라인 버킷 / 롤업 버킷 경계를 전부 이
-- ZoneId 로 계산한다. JVM 기본 TZ 에 맡기면 UTC 컨테이너 고객에서 이 4가지가 9시간 밀린다.
-- gen_random_uuid() 는 PostgreSQL 16 내장이라 pgcrypto 확장이 불필요하다.
-- -----------------------------------------------------------------------------
CREATE TABLE install_info (
    id                      SMALLINT     PRIMARY KEY DEFAULT 1,
    install_id              VARCHAR(40)  NOT NULL,
    install_label           VARCHAR(120),                      -- 알림 제목 접두어
    display_zone            VARCHAR(60)  NOT NULL DEFAULT 'Asia/Seoul',
    -- 알림 딥링크에 붙일 외부 접근 주소. 비어 있으면 링크 생략(열리지 않는 localhost 링크 금지).
    console_base_url        VARCHAR(300),
    installed_at            TIMESTAMPTZ  NOT NULL DEFAULT now(),
    app_version             VARCHAR(20),
    -- 복제본 탐지용. 운영 덤프로 만든 검증계가 같은 install_id 를 갖는 것을 막는다.
    host_fingerprint        VARCHAR(80),
    onboarding_completed_at TIMESTAMPTZ,
    onboarding_completed_by VARCHAR(255),
    updated_at              TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT ck_install_info_singleton CHECK (id = 1)
);

INSERT INTO install_info (id, install_id)
VALUES (1, replace(gen_random_uuid()::text, '-', ''))
ON CONFLICT (id) DO NOTHING;
