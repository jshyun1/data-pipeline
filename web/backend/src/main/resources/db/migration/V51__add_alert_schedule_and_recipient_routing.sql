-- 알림 규칙 스케줄 + 수신자별 알림 라우팅.
--
-- 1) 스케줄: 지금은 모든 규칙이 20초 평가 루프에서 상시 판정된다. 관제 상주 인력이 없는
--    현장에서는 "매일 아침 09:00에만 점검해서 보내라"는 요구가 있어, 규칙마다 켜고 끌 수
--    있는 일별 점검 시각을 둔다. schedule_enabled=false 면 종전과 완전히 동일하게 동작한다.
-- 2) 라우팅: 지금은 심각도/채널 구독만 맞으면 모든 수신자가 모든 알림을 받는다. 담당이
--    갈린 현장(수신자1=A job, 수신자2=B job)을 표현할 수 없어 수신자×규칙×대상 표를 만든다.

ALTER TABLE alert_rule
    ADD COLUMN schedule_enabled       boolean NOT NULL DEFAULT false,
    -- 로컬 시각(HH:MM). 앱 JVM 과 DB 가 같은 TZ 를 쓴다는 기존 전제를 그대로 따른다.
    ADD COLUMN schedule_time          time,
    -- 하루 한 번만 돌게 하는 빗장. 평가 루프가 20초마다 도는데 이게 없으면
    -- 09:00 이후 매 20초마다 다시 점검·발송한다.
    ADD COLUMN schedule_last_fired_on date;

ALTER TABLE alert_rule
    ADD CONSTRAINT ck_alert_rule_schedule
    CHECK (NOT schedule_enabled OR schedule_time IS NOT NULL);

COMMENT ON COLUMN alert_rule.schedule_enabled IS '켜면 상시 판정 대신 schedule_time 에 하루 한 번만 판정한다';
COMMENT ON COLUMN alert_rule.schedule_time IS '일별 점검 시각(로컬). schedule_enabled 일 때 필수';
COMMENT ON COLUMN alert_rule.schedule_last_fired_on IS '마지막으로 점검을 돌린 날짜(하루 1회 보장)';

-- 수신자별 알림 라우팅. 행이 하나도 없는 수신자는 종전대로 «전부» 받는다
-- (마이그레이션 직후 아무도 알림을 못 받는 사고를 막는 기본값).
CREATE TABLE notification_recipient_rule (
    id           bigserial PRIMARY KEY,
    recipient_id bigint      NOT NULL REFERENCES notification_recipient(id),
    rule_id      bigint      NOT NULL REFERENCES alert_rule(id),
    -- alert_rule.scope_json 과 같은 모양 {kind, ids, idKind}. 규칙이 감시하는 대상 중
    -- «이 수신자에게 보낼» 부분집합을 고른다. ALL 이면 그 규칙의 알림을 전부 받는다.
    scope_json   jsonb       NOT NULL DEFAULT '{"kind":"ALL"}'::jsonb,
    enabled      boolean     NOT NULL DEFAULT true,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX uq_notification_recipient_rule ON notification_recipient_rule (recipient_id, rule_id);
-- 발송 팬아웃이 «이 규칙에 라우팅을 건 수신자»를 매번 찾는다.
CREATE INDEX idx_notification_recipient_rule_rule ON notification_recipient_rule (rule_id) WHERE enabled;

COMMENT ON TABLE notification_recipient_rule IS '수신자별 알림 라우팅. 행이 없는 수신자는 전부 수신(종전 동작)';
