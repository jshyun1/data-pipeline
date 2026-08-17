-- =============================================================================
-- V29__create_airflow_mirror_and_indexes.sql  (설계서 4-5절, U26/보강)
-- =============================================================================
-- Airflow DagRun 미러. 현재 백엔드는 GET /api/v2/monitor/health 만 호출하고 나머지는 브라우저가
-- nginx 로 직접 호출(공유 admin 쿠키). maxRuns 상한 절삭 데이터로 성공률을 계산하는 문제도 해소.
-- D-5: env_kind 컬럼은 두지 않는다.
-- -----------------------------------------------------------------------------
CREATE TABLE airflow_dag_run (
    id               BIGSERIAL    PRIMARY KEY,
    dag_id           VARCHAR(250) NOT NULL,
    dag_run_id       VARCHAR(250) NOT NULL,
    run_type         VARCHAR(50),
    state            VARCHAR(20)  NOT NULL,   -- queued / running / success / failed
    logical_date     TIMESTAMPTZ,             -- Airflow 는 전부 UTC ISO-8601
    start_date       TIMESTAMPTZ,
    end_date         TIMESTAMPTZ,
    duration_seconds BIGINT,
    -- etl_job.airflow_dag_id 와 동등 비교로만 채운다(prefix 매칭 이식 금지).
    etl_job_id       BIGINT       REFERENCES etl_job (id),
    pipeline_id      BIGINT,      -- kafka_pipeline_{id}_control 패턴에서 추출(FK 없음)
    -- 장기 reschedule Sensor 여부. "실행 중" 집계에서 분리한다.
    long_running     BOOLEAN      NOT NULL DEFAULT false,
    synced_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uq_airflow_dag_run     ON airflow_dag_run (dag_id, dag_run_id);
CREATE INDEX idx_airflow_dag_run_window    ON airflow_dag_run (start_date DESC);
CREATE INDEX idx_airflow_dag_run_problem   ON airflow_dag_run (state, start_date DESC)
    WHERE state IN ('failed','running','queued');
CREATE INDEX idx_airflow_dag_run_dag       ON airflow_dag_run (dag_id, start_date DESC);

CREATE TABLE airflow_sync_cursor (
    id                   SMALLINT    PRIMARY KEY DEFAULT 1,
    last_synced_at       TIMESTAMPTZ,
    last_success_at      TIMESTAMPTZ,
    -- 예외 원문 금지(관리자 계정 URL/응답 노출). 사전 코드만: AUTH_FAILED/TIMEOUT/TRUNCATED/HTTP_5XX
    last_error_code      VARCHAR(40),
    consecutive_failures INTEGER     NOT NULL DEFAULT 0,
    truncated            BOOLEAN     NOT NULL DEFAULT false,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_airflow_sync_cursor_single CHECK (id = 1)
);
INSERT INTO airflow_sync_cursor (id) VALUES (1) ON CONFLICT DO NOTHING;

-- 기존 테이블 인덱스 보강 -------------------------------------------------------
-- 알림 엔진이 "최근 실패한 잡 실행"을 20초마다 찾는다.
CREATE INDEX idx_etl_job_run_status_ended ON etl_job_run (status, ended_at DESC);
-- 실패 추이/처리량 추이가 같은 테이블을 status 로 갈라 읽는다(실패 쪽이 작아 부분 스캔 유리).
CREATE INDEX idx_nifi_execution_log_status_occurred ON nifi_execution_log (status, occurred_at)
    INCLUDE (job_id, inserted_count, group_id, group_name);
-- DashboardController.summary() 가 요청마다 pipeline_command_history 를 3회 카운트한다.
CREATE INDEX idx_pipeline_command_history_requested ON pipeline_command_history (requested_at DESC)
    INCLUDE (pipeline_id, command, result);
