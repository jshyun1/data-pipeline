-- =============================================================================
-- V36__create_notification_core.sql  (설계서 4-7절, U11~U14)
-- =============================================================================
-- 아웃박스: 알림 "판정"(alert_*)과 "발송"(notification_*)을 분리한다. 엔진은 자기 트랜잭션에서
-- notification_delivery 행만 INSERT(수 ms)하고, 실제 발송은 controlPlaneScheduler 워커가 가져간다.
-- 인메모리 큐를 안 쓰는 이유: 재기동 시 큐가 사라져 "발생했는데 아무도 못 받은" 알림이 생긴다.
-- R3(조용시간 없음)/R4(EMAIL·SMS·IN_APP 만) 반영.
-- -----------------------------------------------------------------------------
CREATE TABLE notification_channel_config (
    id                     BIGSERIAL    PRIMARY KEY,
    channel_type           VARCHAR(20)  NOT NULL,   -- IN_APP / EMAIL / SMS
    enabled                BOOLEAN      NOT NULL DEFAULT false,
    config_json            JSONB        NOT NULL DEFAULT '{}'::jsonb,
    -- AesPasswordCryptoService 로 암호화. API 응답에 절대 안 넣는다(secretSet 불리언만).
    encrypted_secret       TEXT,
    secret_key_fingerprint VARCHAR(16),   -- 복호화 전 키 세대 불일치 판정용
    -- 심각도별 분리 쿼터(전역 단일 쿼터면 광역 장애에서 CRITICAL 이 요약으로 강등된다).
    rate_critical_per_min  INTEGER      NOT NULL DEFAULT 10,
    rate_other_per_min     INTEGER      NOT NULL DEFAULT 10,
    hard_limit_per_hour    INTEGER      NOT NULL DEFAULT 200,
    window_start           TIMESTAMPTZ,   -- 원자적 예약 카운터
    window_count           INTEGER      NOT NULL DEFAULT 0,
    circuit_state          VARCHAR(20)  NOT NULL DEFAULT 'CLOSED',  -- CLOSED/OPEN/HALF_OPEN/HALF_OPEN_PROBING
    circuit_opened_at      TIMESTAMPTZ,
    circuit_open_streak    INTEGER      NOT NULL DEFAULT 0,
    last_critical_probe_at TIMESTAMPTZ,   -- OPEN 중 CRITICAL 10분 1회 프로브(폴링마다 프로브 방지)
    consecutive_failures   INTEGER      NOT NULL DEFAULT 0,
    last_success_at        TIMESTAMPTZ,
    last_failure_at        TIMESTAMPTZ,
    last_failure_reason    VARCHAR(40),
    last_failure_detail    VARCHAR(1000),
    remaining_quota        BIGINT,
    remaining_quota_at     TIMESTAMPTZ,
    updated_by             VARCHAR(255),
    created_at             TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uq_notification_channel_type ON notification_channel_config (channel_type);
-- 화면 알림은 외부 의존이 없어 처음부터 켜 둔다. 나머지는 고객이 설정 후 켠다.
INSERT INTO notification_channel_config (channel_type, enabled, rate_critical_per_min, rate_other_per_min, hard_limit_per_hour)
VALUES ('IN_APP', true, 100000, 100000, 100000),
       ('EMAIL',  false,    10,     10,    200),
       ('SMS',    false,     5,      5,     50);

CREATE TABLE notification_recipient (
    id                  BIGSERIAL    PRIMARY KEY,
    -- 로그인 계정 없는 외부 담당자도 등록 가능해야 하므로 nullable.
    user_id             VARCHAR(255) REFERENCES app_user (user_id),
    display_name        VARCHAR(100) NOT NULL,
    email               VARCHAR(255),
    phone               VARCHAR(30),   -- 개인정보. 조회 시 부분 마스킹, 보존만료 시 익명화.
    locale              VARCHAR(10),
    enabled             BOOLEAN      NOT NULL DEFAULT true,
    -- 영구 실패(SMTP 550 등) 3회 누적 시 자동 중지.
    email_failure_count INTEGER      NOT NULL DEFAULT 0,
    email_disabled_at   TIMESTAMPTZ,
    sms_failure_count   INTEGER      NOT NULL DEFAULT 0,
    sms_disabled_at     TIMESTAMPTZ,
    deleted_at          TIMESTAMPTZ,
    created_by          VARCHAR(255),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT ck_recipient_contact CHECK (email IS NOT NULL OR phone IS NOT NULL)
);
CREATE UNIQUE INDEX uq_recipient_user ON notification_recipient (user_id)
    WHERE user_id IS NOT NULL AND deleted_at IS NULL;

-- 구독 = 수신자 x 채널. 발송 여부는 심각도 x min_severity 로 결정(근무/조용시간 없음, R3).
CREATE TABLE notification_subscription (
    id           BIGSERIAL    PRIMARY KEY,
    recipient_id BIGINT       NOT NULL REFERENCES notification_recipient (id),
    channel_type VARCHAR(20)  NOT NULL,
    min_severity VARCHAR(10)  NOT NULL DEFAULT 'WARNING',
    categories   JSONB        NOT NULL DEFAULT '[]'::jsonb,   -- [] = 전체
    enabled      BOOLEAN      NOT NULL DEFAULT true,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uq_notification_subscription ON notification_subscription (recipient_id, channel_type);

-- 발송 아웃박스 + 이력.
CREATE TABLE notification_delivery (
    id                  BIGSERIAL    PRIMARY KEY,
    event_key           VARCHAR(120) NOT NULL,   -- "ALERT-{instanceId}-{stateSeq}"
    -- sha256(event_key|channel|recipient_id|coalesce(address,'')). recipient_id 필수(IN_APP 은
    -- address 가 없어 수신자 전원이 같은 해시가 되는 것 방지).
    dedup_key           CHAR(64)     NOT NULL,
    channel_type        VARCHAR(20)  NOT NULL,
    severity            VARCHAR(10)  NOT NULL,
    severity_rank       SMALLINT     NOT NULL,   -- CRITICAL=0/WARNING=1/INFO=2 (정렬용 승격)
    category            VARCHAR(20)  NOT NULL,    -- JOB/SERVER/SERVICE/DATA/SYSTEM
    recipient_id        BIGINT       REFERENCES notification_recipient (id),
    target_address      VARCHAR(300),   -- 마스킹 스냅샷(개인정보 최소 보유)
    template_key        VARCHAR(60),
    locale              VARCHAR(10),
    subject             VARCHAR(300),
    body                VARCHAR(4000),
    deep_link           VARCHAR(500),
    status              VARCHAR(20)  NOT NULL,   -- PENDING/BATCHING/BATCHED/SENDING/SENT/RETRY/DEAD/EXPIRED/CANCELED/SUPPRESSED
    attempt_count       INTEGER      NOT NULL DEFAULT 0,
    max_attempts        INTEGER      NOT NULL DEFAULT 5,
    next_attempt_at     TIMESTAMPTZ  NOT NULL,   -- NOT NULL(누락 시 영원히 claim 안 됨)
    expires_at          TIMESTAMPTZ  NOT NULL,   -- 기본 사건+6h
    first_attempt_at    TIMESTAMPTZ,
    sent_at             TIMESTAMPTZ,
    read_at             TIMESTAMPTZ,             -- IN_APP 전용
    read_by             VARCHAR(255),
    failure_reason      VARCHAR(40),
    failure_detail      VARCHAR(1000),
    provider_message_id VARCHAR(200),
    batch_id            BIGINT       REFERENCES notification_delivery (id) ON DELETE SET NULL,
    self_alarm          BOOLEAN      NOT NULL DEFAULT false,   -- 자기알림 무한증식 차단
    escalated_from      VARCHAR(20),   -- SMS 하드상한 초과 CRITICAL 을 메일 폴백 시 원 채널
    attempts_json       JSONB        NOT NULL DEFAULT '[]'::jsonb,
    claimed_by          VARCHAR(40),   -- 배포 중 구/신 컨테이너 중복발송 방지
    claim_expires_at    TIMESTAMPTZ,
    occurred_at         TIMESTAMPTZ  NOT NULL,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uq_notification_delivery_dedup ON notification_delivery (dedup_key);
CREATE INDEX idx_delivery_dispatch ON notification_delivery (severity_rank, next_attempt_at)
    WHERE status IN ('PENDING','RETRY');
CREATE INDEX idx_delivery_batching ON notification_delivery (next_attempt_at) WHERE status = 'BATCHING';
CREATE INDEX idx_delivery_sending  ON notification_delivery (claim_expires_at) WHERE status = 'SENDING';
CREATE INDEX idx_delivery_unread   ON notification_delivery (recipient_id, created_at DESC)
    WHERE channel_type = 'IN_APP' AND read_at IS NULL;
CREATE INDEX idx_delivery_created  ON notification_delivery (created_at DESC);
CREATE INDEX idx_delivery_event    ON notification_delivery (event_key);
INSERT INTO retention_policy (table_name, time_column, retention_days, min_retention_days, purge_mode, updated_by)
VALUES ('notification_delivery', 'created_at', 90, 30, 'DELETE_BATCH', 'system')
ON CONFLICT (table_name) DO NOTHING;

-- 템플릿. {{변수}} 단순 치환만(조건/반복/표현식 없음 - 템플릿 오류로 CRITICAL 이 막히면 안 되고
-- 표현식 엔진은 RCE 경로다). 링크 유무 분기는 서버가 만든 {{deepLinkLine}} 으로 처리.
CREATE TABLE notification_template (
    id               BIGSERIAL    PRIMARY KEY,
    template_key     VARCHAR(60)  NOT NULL,   -- ALERT_DEFAULT/DIGEST/CHANNEL_TEST/SELF_ALARM/HEARTBEAT
    channel_type     VARCHAR(20)  NOT NULL,
    locale           VARCHAR(10)  NOT NULL DEFAULT 'ko',
    subject_template VARCHAR(300),
    body_template    TEXT         NOT NULL,
    builtin          BOOLEAN      NOT NULL DEFAULT false,
    enabled          BOOLEAN      NOT NULL DEFAULT true,
    updated_by       VARCHAR(255),
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uq_notification_template ON notification_template (template_key, channel_type, locale);
