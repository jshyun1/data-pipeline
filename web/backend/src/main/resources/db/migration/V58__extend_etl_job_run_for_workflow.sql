-- 워크플로우 실행이 잡 실행 원장(etl_job_run)과 이어지도록 확장한다.
--
-- 기존 etl_job_run은 관측기(NifiProcessorRunTracker)가 NiFi 활동을 보고 여닫는 것이었다.
-- 이제 Airflow가 "이 실행은 내가 지시했다"를 표시하고(run_token/dag_run_id), 나중에
-- NiFi가 종단에서 콜백을 보내면 그 결과로 닫히게 된다.
--
-- completion_source가 신뢰도 지표다:
--   CALLBACK - NiFi가 끝났다고 알려줬다(확정)
--   OBSERVED - 큐/스레드가 비는 것을 보고 끝났다고 추정했다(현행 방식)
--   TIMEOUT  - 기다리다 지쳤다
-- 화면에서 이 둘을 구분해 보여주면 "왜 초록불인데 데이터가 없나"를 설명할 수 있다.

ALTER TABLE etl_job_run
    ADD COLUMN run_token         VARCHAR(64),
    ADD COLUMN workflow_key      VARCHAR(80),
    ADD COLUMN node_key          VARCHAR(80),
    ADD COLUMN airflow_task_id   VARCHAR(250),
    ADD COLUMN completion_source VARCHAR(20),
    ADD COLUMN rows_processed    BIGINT,
    ADD COLUMN error_message     TEXT;

CREATE UNIQUE INDEX uq_etl_job_run_token
    ON etl_job_run (run_token) WHERE run_token IS NOT NULL;

-- "한 job에 열린 실행은 하나"는 새로 만들 필요가 없다 - V18의 uq_etl_job_run_open이
-- 이미 같은 정의로 걸어두고 있다(job_id WHERE ended_at IS NULL). 그 제약이 있어서
-- NiFi 완료 콜백이 프로세스 그룹 id만으로 대상 실행을 유일하게 특정할 수 있다.
