CREATE TABLE pipeline_consistency_check (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id BIGINT NOT NULL REFERENCES pipeline_definition(id) ON DELETE CASCADE,
    check_mode VARCHAR(30) NOT NULL,
    source_count BIGINT,
    target_count BIGINT,
    result VARCHAR(30) NOT NULL,
    message TEXT,
    checked_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_pipeline_consistency_check_pipeline_time
    ON pipeline_consistency_check (pipeline_id, checked_at DESC);
