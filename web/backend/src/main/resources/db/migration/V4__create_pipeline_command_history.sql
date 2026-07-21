CREATE TABLE pipeline_command_history (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id BIGINT NOT NULL,
    command VARCHAR(50) NOT NULL, -- VALIDATE, DEPLOY, START, PAUSE, STOP, RESTART, DELETE
    result VARCHAR(30),
    message TEXT,
    requested_by VARCHAR(100),
    requested_at TIMESTAMP DEFAULT now(),
    completed_at TIMESTAMP
);
