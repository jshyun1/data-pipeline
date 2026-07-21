CREATE TABLE pipeline_metric_snapshot (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id BIGINT NOT NULL,
    collected_at TIMESTAMP DEFAULT now(),
    connector_state VARCHAR(30),
    task_state VARCHAR(30),
    topic_name VARCHAR(200),
    partition_count INTEGER,
    end_offset BIGINT,
    committed_offset BIGINT,
    consumer_lag BIGINT,
    error_count BIGINT DEFAULT 0,
    last_error_message TEXT
);
