CREATE TABLE pipeline_connector (
    id BIGSERIAL PRIMARY KEY,
    pipeline_id BIGINT NOT NULL,
    connector_role VARCHAR(30) NOT NULL, -- SOURCE, SINK
    connector_name VARCHAR(200) NOT NULL,
    connector_class VARCHAR(300) NOT NULL,
    connector_config_json TEXT NOT NULL,
    connect_cluster_url VARCHAR(300) DEFAULT 'http://kafka-connect:8083',
    status VARCHAR(30) DEFAULT 'CREATED',
    last_status_json TEXT,
    deployed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
