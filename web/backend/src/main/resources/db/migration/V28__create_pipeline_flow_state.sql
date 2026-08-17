-- =============================================================================
-- V28__create_pipeline_flow_state.sql  (설계서 4-5절, U10)
-- =============================================================================
-- 진행형 KPI 의 "언제부터 이 상태였나". 판정은 매 주기 계산하되 "STALLED 가 120초 이상
-- 지속됐는가"와 "이 상태가 된 시각"은 상태를 기억해야 한다(JVM 메모리에 두지 않는다).
--
-- 판정 주체는 전용 스케줄러다(FlowStateEvaluationScheduler, controlPlaneScheduler 20초). 요청 시
-- 계산하면 (a) 새벽에 화면을 안 보면 판정이 안 일어나고 (b) 지속 판정이 탭 수에 좌우된다. GET
-- /kpi/live 는 이 테이블을 읽기만 한다. 지속시간 판정은 now - since_at 경과로 한다.
-- -----------------------------------------------------------------------------
CREATE TABLE pipeline_flow_state (
    id                BIGSERIAL    PRIMARY KEY,
    target_kind       VARCHAR(30)  NOT NULL,   -- KAFKA_PIPELINE / NIFI_JOB / AIRFLOW_DAG
    target_key        VARCHAR(200) NOT NULL,
    flow_state        VARCHAR(30)  NOT NULL,
    since_at          TIMESTAMPTZ  NOT NULL,    -- 상태가 바뀐 시각. 같으면 갱신하지 않는다
    evaluated_at      TIMESTAMPTZ  NOT NULL,
    consecutive_count INTEGER      NOT NULL DEFAULT 1,   -- 진단용
    detail_json       JSONB,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uq_pipeline_flow_state ON pipeline_flow_state (target_kind, target_key);
CREATE INDEX idx_pipeline_flow_state_problem ON pipeline_flow_state (flow_state, since_at)
    WHERE flow_state NOT IN ('HEALTHY','IDLE','IDLE_NO_SOURCE_CHANGE','RUNNING','PAUSED_BY_OPERATOR','NOT_MONITORED');

-- 상태 전이 이력(append-only). UPSERT 만으로는 "언제 어떤 전이가 있었나"가 남지 않는다.
CREATE TABLE pipeline_flow_state_event (
    id           BIGSERIAL    PRIMARY KEY,
    target_kind  VARCHAR(30)  NOT NULL,
    target_key   VARCHAR(200) NOT NULL,
    from_state   VARCHAR(30),
    to_state     VARCHAR(30)  NOT NULL,
    occurred_at  TIMESTAMPTZ  NOT NULL,
    detail_json  JSONB
);
CREATE INDEX idx_flow_state_event ON pipeline_flow_state_event (target_kind, target_key, occurred_at DESC);
