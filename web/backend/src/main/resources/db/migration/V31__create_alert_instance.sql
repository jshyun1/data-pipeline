-- =============================================================================
-- V31__create_alert_instance.sql  (설계서 4-6절, U9)
-- =============================================================================
-- 알림 인스턴스 = 조치 대기열 항목 = 상태기계 본체.
--
-- 부분 UNIQUE 를 "해소되지 않음(closed_at IS NULL)" 기준으로 잡되, RESOLVED 즉시 closed_at 을
-- 채운다. RESOLVED 인스턴스가 24시간 인덱스를 점유하면 재발화가 unique_violation 으로 막혀 하루
-- 두 번 이상 재발하는 장애가 첫 번째 이후 무음이 된다. "24시간 화면 유지"는 display_until 로,
-- flapping 카운트는 alert_instance_event 의 FIRED 개수로 센다.
-- -----------------------------------------------------------------------------
CREATE TABLE alert_instance (
    id                 BIGSERIAL    PRIMARY KEY,
    rule_id            BIGINT       NOT NULL REFERENCES alert_rule (id),
    rule_type_code     VARCHAR(60)  NOT NULL,   -- 대기열 조회 3-way 조인 회피용 승격
    kpi_axis           VARCHAR(20)  NOT NULL,
    target_key         VARCHAR(200) NOT NULL,
    target_label       VARCHAR(200),            -- 대상 삭제돼도 이력에서 이름을 잃지 않는다
    component_code     VARCHAR(60),
    severity           VARCHAR(20)  NOT NULL,
    state              VARCHAR(20)  NOT NULL,
    condition_since    TIMESTAMPTZ  NOT NULL,   -- 조건이 처음 참이 된 시각(재기동 생존)
    -- 연속 TRUE 로 실제 관측된 누적 초. for 판정을 벽시계가 아니라 이 값으로 한다.
    -- UNKNOWN/STALE 구간엔 누적을 멈춘다(수집기 복구 순간 전 대상 동시 오탐 방지).
    true_observed_sec  INTEGER      NOT NULL DEFAULT 0,
    false_observed_sec INTEGER      NOT NULL DEFAULT 0,
    started_at         TIMESTAMPTZ,
    resolved_at        TIMESTAMPTZ,
    closed_at          TIMESTAMPTZ,             -- RESOLVED 즉시 채운다
    display_until      TIMESTAMPTZ,             -- 화면 유지 시한(해소 +24h)
    last_evaluated_at  TIMESTAMPTZ  NOT NULL,
    last_transition_at TIMESTAMPTZ  NOT NULL,
    observed_value     NUMERIC(20,4),
    threshold_value    NUMERIC(20,4),
    clear_value        NUMERIC(20,4),
    summary            VARCHAR(300) NOT NULL,   -- 컬럼 폭에 맞춰 안전 절단, 원본은 detail_json
    detail_json        JSONB,
    deep_link          VARCHAR(300),
    suppressed_by      BIGINT       REFERENCES alert_instance (id) ON DELETE SET NULL,
    flapping           BOOLEAN      NOT NULL DEFAULT FALSE,
    ack_by             VARCHAR(255),
    ack_at             TIMESTAMPTZ,
    ack_comment        VARCHAR(500),
    snooze_until       TIMESTAMPTZ,
    snooze_by          VARCHAR(255),
    notify_count       SMALLINT     NOT NULL DEFAULT 0,
    last_notified_at   TIMESTAMPTZ,
    last_failed_run_id BIGINT,                  -- 사건 기반 재발송(JOB): 같은 run 중복 방지
    fail_run_count     INTEGER      NOT NULL DEFAULT 0,
    resolve_reason     VARCHAR(30),
    version            BIGINT       NOT NULL DEFAULT 0,   -- 평가기 vs ack 낙관적 락
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uq_alert_instance_open ON alert_instance (rule_id, target_key) WHERE closed_at IS NULL;
CREATE INDEX idx_alert_instance_queue     ON alert_instance (state, severity, last_transition_at DESC) WHERE closed_at IS NULL;
CREATE INDEX idx_alert_instance_display   ON alert_instance (display_until DESC) WHERE display_until IS NOT NULL;
CREATE INDEX idx_alert_instance_component ON alert_instance (component_code, state) WHERE closed_at IS NULL;
CREATE INDEX idx_alert_instance_history   ON alert_instance (started_at DESC);
CREATE INDEX idx_alert_instance_closed    ON alert_instance (closed_at) WHERE closed_at IS NOT NULL;

CREATE TABLE alert_instance_event (
    id           BIGSERIAL    PRIMARY KEY,
    instance_id  BIGINT       NOT NULL REFERENCES alert_instance (id) ON DELETE CASCADE,
    -- CREATED/FIRED/ACKED/UNACKED/SNOOZED/ESCALATED/SEVERITY_RAISED/SUPPRESSED/FLAPPING/
    -- RECURRED/RESOLVED/CLOSED/NOTIFIED/NOTIFY_FAILED
    event_type   VARCHAR(30)  NOT NULL,
    from_state   VARCHAR(20),
    to_state     VARCHAR(20),
    actor        VARCHAR(255) NOT NULL,   -- app_user.user_id 또는 'SYSTEM'
    comment      VARCHAR(500),
    payload_json JSONB,
    occurred_at  TIMESTAMPTZ  NOT NULL
);
CREATE INDEX idx_alert_instance_event ON alert_instance_event (instance_id, occurred_at DESC);
CREATE INDEX idx_alert_event_fired ON alert_instance_event (event_type, occurred_at DESC) WHERE event_type = 'FIRED';

INSERT INTO retention_policy (table_name, time_column, retention_days, min_retention_days, purge_mode, updated_by)
VALUES ('alert_instance', 'closed_at', 180, 30, 'DELETE_BATCH', 'system')
ON CONFLICT (table_name) DO NOTHING;
