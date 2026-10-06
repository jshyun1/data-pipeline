-- 워크플로우 로그 화면이 실행 중인 job 의 «지금까지 적재 건수»를 dag run id 로 읽는다(2026-10-06).
-- 실행 중에는 3초마다 읽을 수 있어 원장 전체를 훑지 않게 한다.
CREATE INDEX IF NOT EXISTS idx_etl_job_run_dag_run
    ON etl_job_run (airflow_dag_run_id)
    WHERE airflow_dag_run_id IS NOT NULL;
