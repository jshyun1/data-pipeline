-- =============================================================================
-- V56__create_alert_rule_eval_log.sql
-- =============================================================================
-- 규칙별 «평가 이력».
--
-- 지금까지 규칙이 어떻게 돌았는지는 alert_rule.last_eval_error 한 칸에 «마지막 것만»
-- 덮어써 왔다. 그래서 "어제 이 규칙이 왜 안 울렸나"를 물으면 답할 수 없었다. 평가가 실패해도
-- 로그 한 줄만 남기고 넘어가므로(AlertEngine.evalSafely) 화면에서는 아무 일도 없었던 것처럼
-- 보인다.
--
-- 남기는 것은 «의미 있는 사건»만이다. 20초 평가 루프를 매번 적으면 규칙 하나당 하루 4,320행이
-- 되어 조회도 보존도 감당하기 어렵다:
--   FAILED   평가 자체가 예외로 끝남 (가장 중요)
--   NO_SIGNAL 신호를 못 읽어 판정을 보류함 - "모름"을 "정상"으로 칠하지 않은 구간
--   FIRED    조건이 성립해 알림을 냄
--   RESOLVED 조건이 풀려 해소함
-- 조건이 계속 정상인 평상시(=아무 변화 없음)는 적지 않는다.
-- -----------------------------------------------------------------------------
CREATE TABLE alert_rule_eval_log (
    id             BIGSERIAL PRIMARY KEY,
    rule_id        BIGINT REFERENCES alert_rule (id) ON DELETE CASCADE,
    -- 규칙이 아직 시드되지 않았거나 지워진 뒤에도 유형별 추적이 되도록 코드도 같이 남긴다.
    rule_type_code VARCHAR(60),
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    result         VARCHAR(20) NOT NULL,
    -- 이번 평가에서 조건에 걸린 대상 수. FAILED 면 NULL.
    matched_count  INTEGER,
    duration_ms    INTEGER,
    -- 실패 사유 또는 요약 한 줄.
    message        TEXT
);

-- 화면은 «한 규칙의 최근 이력»만 본다.
CREATE INDEX idx_alert_rule_eval_log_rule ON alert_rule_eval_log (rule_id, occurred_at DESC);
-- 보존 정리용(오래된 것부터 지운다).
CREATE INDEX idx_alert_rule_eval_log_occurred ON alert_rule_eval_log (occurred_at);

COMMENT ON TABLE alert_rule_eval_log IS
    '규칙 평가 이력. 실패·신호없음·발화·해소만 남긴다(평상시 정상은 남기지 않는다).';
COMMENT ON COLUMN alert_rule_eval_log.result IS
    'FAILED | NO_SIGNAL | FIRED | RESOLVED';
