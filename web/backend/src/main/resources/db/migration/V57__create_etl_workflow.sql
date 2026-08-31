-- ETL 워크플로우 캔버스 (설계서 2026-08-26 §6 Part C).
--
-- NiFi 캔버스는 job(프로세스 그룹) 하나만 그리고, "job을 어떤 순서로 돌릴지"는 여기에만 둔다.
-- 기존에는 NiFi 출력포트 연결로 잡 순서를 추론했는데(build_group_chain), 그러면 원장이 둘로
-- 갈라져 화면과 실제가 어긋난다. 오케스트레이션 원장을 이 테이블로 일원화한다.
--
-- 워크플로우 1개 = Airflow DAG 1개 = 스케줄 1개. 그 안의 노드(job)는 DAG의 TaskGroup이 된다.

CREATE TABLE etl_workflow (
    id                BIGSERIAL PRIMARY KEY,
    -- 불변 키. dag_id(etl_wf_{key})의 축이라 이름을 바꿔도 실행 이력이 끊기지 않는다.
    workflow_key      VARCHAR(80)  NOT NULL,
    name              VARCHAR(200) NOT NULL,
    description       TEXT,
    -- 대응하는 NiFi 그룹. NULL이면 여러 그룹의 job을 모아 만든 논리 워크플로우다.
    nifi_group_pg_id  VARCHAR(100),
    -- 스케줄의 단일 소스. 예전처럼 Airflow Variable에 흩어두지 않는다. NULL이면 수동 전용.
    schedule_cron     VARCHAR(120),
    timezone          VARCHAR(64)  NOT NULL DEFAULT 'Asia/Seoul',
    catchup           BOOLEAN      NOT NULL DEFAULT false,
    max_active_runs   INTEGER      NOT NULL DEFAULT 1,
    suspend_on_error  BOOLEAN      NOT NULL DEFAULT true,
    -- 컴파일에 성공한 게시본. Airflow 팩토리는 이것만 읽는다(draft는 DAG를 만들지 않는다).
    published_spec    JSONB,
    published_at      TIMESTAMP,
    published_by      VARCHAR(100),
    -- 레거시 dag_id를 승계할 때만 채운다(nifi_pipeline_* 이력 연속성).
    dag_id_override   VARCHAR(200),
    deleted_at        TIMESTAMP,
    created_at        TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at        TIMESTAMP    NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX uq_etl_workflow_key
    ON etl_workflow (workflow_key) WHERE deleted_at IS NULL;
CREATE INDEX idx_etl_workflow_published
    ON etl_workflow (published_at) WHERE deleted_at IS NULL AND published_at IS NOT NULL;

-- 캔버스 위의 노드. JOB이면 etl_job을 "참조"한다(복사가 아니라 참조 - NiFi는 참조 재사용을
-- 지원하지 않으므로 재사용은 이 계층에서만 성립한다).
CREATE TABLE etl_workflow_node (
    id              BIGSERIAL PRIMARY KEY,
    workflow_id     BIGINT       NOT NULL REFERENCES etl_workflow (id),
    -- 캔버스 안에서 불변. Airflow TaskGroup id가 된다.
    node_key        VARCHAR(80)  NOT NULL,
    node_type       VARCHAR(20)  NOT NULL DEFAULT 'JOB',
    job_id          BIGINT       REFERENCES etl_job (id),
    sub_workflow_id BIGINT       REFERENCES etl_workflow (id),
    trigger_rule    VARCHAR(40)  NOT NULL DEFAULT 'ALL_SUCCESS',
    branch_expr     TEXT,
    retries         INTEGER      NOT NULL DEFAULT 0,
    retry_delay_sec INTEGER      NOT NULL DEFAULT 60,
    display_x       DOUBLE PRECISION,
    display_y       DOUBLE PRECISION,
    deleted_at      TIMESTAMP,
    created_at      TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP    NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX uq_etl_wf_node
    ON etl_workflow_node (workflow_id, node_key) WHERE deleted_at IS NULL;
-- job 삭제 가드용: 이 job을 참조하는 노드가 있는지 빠르게 확인한다.
CREATE INDEX idx_etl_wf_node_job
    ON etl_workflow_node (job_id) WHERE deleted_at IS NULL;

-- 노드 사이의 실행 순서. Informatica의 link condition에 해당한다.
CREATE TABLE etl_workflow_edge (
    id             BIGSERIAL PRIMARY KEY,
    workflow_id    BIGINT       NOT NULL REFERENCES etl_workflow (id),
    from_node_key  VARCHAR(80)  NOT NULL,
    to_node_key    VARCHAR(80)  NOT NULL,
    condition_type VARCHAR(20)  NOT NULL DEFAULT 'SUCCESS',
    condition_expr TEXT,
    created_at     TIMESTAMP    NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX uq_etl_wf_edge
    ON etl_workflow_edge (workflow_id, from_node_key, to_node_key);
CREATE INDEX idx_etl_wf_edge_workflow
    ON etl_workflow_edge (workflow_id);
