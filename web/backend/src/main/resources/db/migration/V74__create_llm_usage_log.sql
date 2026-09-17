-- GPT 어시스턴트 호출 기록. 대화 본문은 저장하지 않는다.
CREATE TABLE llm_usage_log (
    id                  BIGSERIAL    PRIMARY KEY,
    user_id             VARCHAR(255) NOT NULL,
    agent_type          VARCHAR(30)  NOT NULL,
    target_type         VARCHAR(30)  NOT NULL,
    target_id           VARCHAR(100),
    provider_config_id  BIGINT       REFERENCES llm_provider_config (id) ON DELETE SET NULL,
    provider            VARCHAR(30)  NOT NULL,
    model_name          VARCHAR(200) NOT NULL,
    deployment          VARCHAR(20)  NOT NULL,
    masked              BOOLEAN      NOT NULL,
    input_tokens        INTEGER,
    output_tokens       INTEGER,
    duration_ms         INTEGER,
    status              VARCHAR(20)  NOT NULL,
    error_code          VARCHAR(40),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX ix_llm_usage_log_created_at ON llm_usage_log (created_at DESC);
CREATE INDEX ix_llm_usage_log_target ON llm_usage_log (target_type, target_id);
