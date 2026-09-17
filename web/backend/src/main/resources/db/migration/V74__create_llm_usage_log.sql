-- =============================================================================
-- V74__create_llm_usage_log.sql  (GPT 2단계 / docs/2026-09-13-gpt-settings-design.md 4-2)
-- =============================================================================
-- GPT 호출 기록. 대화 본문(질문·답변·전송 정보)은 저장하지 않는다 - SQL·오류 원문이 DB 에 쌓이면 그 자체가
-- 보안 관리 대상이 된다. 누가·언제·무엇에·어느 LLM 으로 물었는지와 비용 파악용 토큰 수만 남긴다.
--
-- ■ 제공자·모델·배치 위치를 스냅샷으로 둔 이유
--   설정을 지우거나 바꿔도 «그때 어디로 보냈는지»가 기록에서 읽혀야 한다. 설정 FK 는 SET NULL.
--
-- 시각 규약: 신규 테이블은 TIMESTAMPTZ(V22·V73 과 같다).
-- =============================================================================
CREATE TABLE llm_usage_log (
    id                  BIGSERIAL    PRIMARY KEY,
    user_id             VARCHAR(255) NOT NULL,
    agent_type          VARCHAR(30)  NOT NULL,              -- ERROR_ANALYSIS | JOB_ANALYSIS
    target_type         VARCHAR(30)  NOT NULL,              -- WORKFLOW_TASK | CDC_PIPELINE | ...
    target_id           VARCHAR(300),
    provider_config_id  BIGINT       REFERENCES llm_provider_config (id) ON DELETE SET NULL,
    provider            VARCHAR(30)  NOT NULL,
    model_name          VARCHAR(200) NOT NULL,
    deployment          VARCHAR(20)  NOT NULL,
    masked              BOOLEAN      NOT NULL,
    input_chars         INTEGER,                            -- 실제 전송한 글자 수(잘라낸 뒤)
    input_tokens        INTEGER,
    output_tokens       INTEGER,
    duration_ms         INTEGER,
    status              VARCHAR(20)  NOT NULL,              -- SUCCESS | FAILED | TIMEOUT | CANCELLED
    error_code          VARCHAR(40),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX ix_llm_usage_log_created ON llm_usage_log (created_at DESC);
CREATE INDEX ix_llm_usage_log_user ON llm_usage_log (user_id, created_at DESC);
