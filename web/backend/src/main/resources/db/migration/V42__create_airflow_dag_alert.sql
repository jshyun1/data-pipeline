ALTER TABLE airflow_dag_catalog
    ADD COLUMN IF NOT EXISTS monitoring_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS consecutive_failure_threshold INTEGER NOT NULL DEFAULT 3,
    ADD COLUMN IF NOT EXISTS stale_days_threshold INTEGER NOT NULL DEFAULT 7,
    ADD COLUMN IF NOT EXISTS duration_multiplier NUMERIC(5, 2) NOT NULL DEFAULT 3.00,
    ADD COLUMN IF NOT EXISTS sla_minutes INTEGER;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_airflow_dag_catalog_failure_threshold') THEN
        ALTER TABLE airflow_dag_catalog ADD CONSTRAINT chk_airflow_dag_catalog_failure_threshold
            CHECK (consecutive_failure_threshold > 0);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_airflow_dag_catalog_stale_days') THEN
        ALTER TABLE airflow_dag_catalog ADD CONSTRAINT chk_airflow_dag_catalog_stale_days
            CHECK (stale_days_threshold > 0);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_airflow_dag_catalog_duration_multiplier') THEN
        ALTER TABLE airflow_dag_catalog ADD CONSTRAINT chk_airflow_dag_catalog_duration_multiplier
            CHECK (duration_multiplier >= 1);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_airflow_dag_catalog_sla_minutes') THEN
        ALTER TABLE airflow_dag_catalog ADD CONSTRAINT chk_airflow_dag_catalog_sla_minutes
            CHECK (sla_minutes IS NULL OR sla_minutes > 0);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS airflow_dag_alert (
    id               BIGSERIAL PRIMARY KEY,
    dag_id           VARCHAR(250) NOT NULL
        REFERENCES airflow_dag_catalog (dag_id) ON DELETE CASCADE,
    rule_type        VARCHAR(30) NOT NULL
        CHECK (rule_type IN ('CONSECUTIVE_FAILURE', 'STALE', 'DURATION_ANOMALY', 'SLA_EXCEEDED')),
    severity         VARCHAR(10) NOT NULL
        CHECK (severity IN ('INFO', 'WARNING', 'DANGER')),
    status           VARCHAR(20) NOT NULL DEFAULT 'OPEN'
        CHECK (status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')),
    message          TEXT NOT NULL,
    detected_at      TIMESTAMP NOT NULL DEFAULT now(),
    last_detected_at TIMESTAMP NOT NULL DEFAULT now(),
    acknowledged_at TIMESTAMP,
    resolved_at      TIMESTAMP,
    created_at       TIMESTAMP NOT NULL DEFAULT now(),
    updated_at       TIMESTAMP NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_airflow_dag_alert_active_rule
    ON airflow_dag_alert (dag_id, rule_type)
    WHERE status IN ('OPEN', 'ACKNOWLEDGED');

CREATE INDEX IF NOT EXISTS idx_airflow_dag_alert_status_severity
    ON airflow_dag_alert (status, severity, last_detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_airflow_dag_alert_dag_detected
    ON airflow_dag_alert (dag_id, detected_at DESC);

COMMENT ON COLUMN airflow_dag_catalog.monitoring_enabled IS 'DAG 이상 감지 활성 여부';
COMMENT ON COLUMN airflow_dag_catalog.consecutive_failure_threshold IS '연속 실패 감지 횟수';
COMMENT ON COLUMN airflow_dag_catalog.stale_days_threshold IS '미실행 정체 감지 일수';
COMMENT ON COLUMN airflow_dag_catalog.duration_multiplier IS '평균 실행시간 대비 이상 감지 배수';
COMMENT ON COLUMN airflow_dag_catalog.sla_minutes IS 'SLA 제한시간(분), NULL이면 감지하지 않음';
COMMENT ON TABLE airflow_dag_alert IS 'Airflow DAG 이상 감지 및 조치 처리 이력';
