CREATE TABLE pipeline_connection (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    db_type VARCHAR(30) NOT NULL, -- ORACLE, POSTGRESQL
    host VARCHAR(200) NOT NULL,
    port INTEGER NOT NULL,
    database_name VARCHAR(100),
    service_name VARCHAR(100),
    schema_name VARCHAR(100),
    username VARCHAR(100) NOT NULL,
    encrypted_password TEXT NOT NULL,
    jdbc_url TEXT,
    status VARCHAR(30) DEFAULT 'UNKNOWN',
    last_tested_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
