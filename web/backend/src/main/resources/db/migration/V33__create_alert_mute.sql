-- =============================================================================
-- V33__create_alert_mute.sql  (설계서 4-6절, U9)
-- =============================================================================
-- 음소거. 필수 알림을 못 끄게 하는 대신 주는 탈출구. 서비스 계층이 강제(DB CHECK 로는 불가):
--   mandatory 규칙은 (a) target_key NOT NULL 필수 (b) 최대 72시간 (c) IN_APP 은 계속 표시(발송만
--   억제) (d) 생성/갱신 자체가 CRITICAL IN_APP 알림 + alert_instance_event 기록.
-- -----------------------------------------------------------------------------
CREATE TABLE alert_mute (
    id         BIGSERIAL    PRIMARY KEY,
    rule_id    BIGINT       REFERENCES alert_rule (id) ON DELETE CASCADE,
    target_key VARCHAR(200),
    reason     VARCHAR(300) NOT NULL,
    created_by VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ  NOT NULL,
    revoked_at TIMESTAMPTZ,
    revoked_by VARCHAR(255),
    CONSTRAINT ck_alert_mute_scope CHECK (rule_id IS NOT NULL OR target_key IS NOT NULL)
);
CREATE INDEX idx_alert_mute_active ON alert_mute (expires_at) WHERE revoked_at IS NULL;
