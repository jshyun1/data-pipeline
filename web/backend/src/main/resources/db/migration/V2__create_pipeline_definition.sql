CREATE TABLE pipeline_definition (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    pipeline_type VARCHAR(50) NOT NULL, -- TABLE_CDC, LOG_FILE
    source_connection_id BIGINT,
    target_connection_id BIGINT,
    source_db_type VARCHAR(30),
    target_db_type VARCHAR(30),
    source_schema VARCHAR(100),
    source_table VARCHAR(100),
    target_schema VARCHAR(100),
    target_table VARCHAR(100),
    topic_name VARCHAR(200),
    status VARCHAR(30) DEFAULT 'CREATED',
    snapshot_mode VARCHAR(50),
    insert_enabled BOOLEAN DEFAULT true,
    update_enabled BOOLEAN DEFAULT true,
    delete_enabled BOOLEAN DEFAULT true,
    description TEXT,
    created_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
