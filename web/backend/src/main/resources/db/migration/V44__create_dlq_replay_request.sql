CREATE TABLE dlq_replay_request (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id BIGINT NOT NULL REFERENCES pipeline_definition(id) ON DELETE CASCADE,
    dlq_topic VARCHAR(255) NOT NULL,
    dlq_partition INTEGER NOT NULL,
    dlq_offset BIGINT NOT NULL,
    original_topic VARCHAR(255) NOT NULL,
    risk_level VARCHAR(20) NOT NULL,
    status VARCHAR(30) NOT NULL,
    reason TEXT NOT NULL,
    requested_by VARCHAR(255) NOT NULL,
    requested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    approved_by VARCHAR(255),
    approved_at TIMESTAMP,
    executed_at TIMESTAMP,
    result_message TEXT,
    CONSTRAINT uk_dlq_replay_source UNIQUE (dlq_topic, dlq_partition, dlq_offset)
);

CREATE INDEX idx_dlq_replay_status_time ON dlq_replay_request (status, requested_at DESC);
