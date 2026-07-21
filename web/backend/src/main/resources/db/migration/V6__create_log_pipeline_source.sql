CREATE TABLE log_pipeline_source (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id BIGINT NOT NULL,
    agent_host VARCHAR(200),
    file_path TEXT NOT NULL,
    file_pattern VARCHAR(200),
    read_from VARCHAR(30) DEFAULT 'END', -- BEGINNING, END
    parse_type VARCHAR(30) DEFAULT 'PLAIN', -- PLAIN, JSON, REGEX, DELIMITER (이번 증분은 PLAIN만 지원)
    encoding VARCHAR(30) DEFAULT 'UTF-8',
    multiline_enabled BOOLEAN DEFAULT false,
    topic_name VARCHAR(200) NOT NULL,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
