CREATE TABLE airflow_dag_catalog (
    id              BIGSERIAL PRIMARY KEY,
    dag_id          VARCHAR(250) NOT NULL UNIQUE,
    business_group  VARCHAR(20)  NOT NULL CHECK (business_group IN ('CDC', 'ETL')),
    business_folder VARCHAR(200) NOT NULL,
    display_name    VARCHAR(200) NOT NULL,
    description     TEXT,
    sort_order      INTEGER      NOT NULL DEFAULT 0,
    enabled         BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP    NOT NULL DEFAULT now()
);

CREATE INDEX idx_airflow_dag_catalog_group_sort
    ON airflow_dag_catalog (business_group, business_folder, sort_order, display_name);
