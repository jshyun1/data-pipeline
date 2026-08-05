-- NiFi 잡(프로세스 그룹) 카탈로그 - NiFi 원본 + DB 미러.
--
-- 지금까지 NiFi 잡의 정의는 nifi-conf 볼륨의 flow.json.gz 안에만 있었다. 그래서
--   - "어떤 잡이 어느 테이블을 어디로 옮기는지"를 SQL로 물어볼 수 없고
--   - 화면에 잡 목록을 띄우려면 매번 NiFi REST를 긁어야 하고
--   - 캔버스가 유실되면(메모리 압박 하에서 재시작 시 실제로 겪음) 복구 근거가 백업 파일뿐이었다.
--
-- 이 테이블들은 캔버스를 5분 주기로 읽어 만든 "읽기 전용 사본"이다. 원본은 여전히 NiFi다.
-- 여기 값을 고쳐도 캔버스에는 반영되지 않으며, 다음 동기화에서 덮어써진다.
-- (파라미터만 나중에 DB -> NiFi 푸시로 바꿀 수 있도록 etl_job_param.sync_direction을 미리 둔다.)
--
-- Kafka CDC/로그 파이프라인은 기존 pipeline_definition이 계속 원장이고, 조회만
-- v_etl_all_jobs 뷰에서 합친다.

-- ---------------------------------------------------------------------------
-- 잡 = NiFi 프로세스 그룹 하나
-- ---------------------------------------------------------------------------
CREATE TABLE etl_job (
    id                     BIGSERIAL PRIMARY KEY,
    -- NiFi 프로세스 그룹 id. 캔버스에서 이름을 바꿔도 유지되는 자연키.
    nifi_pg_id             VARCHAR(100) NOT NULL,
    parent_pg_id           VARCHAR(100),
    job_name               VARCHAR(200) NOT NULL,
    -- 지금은 NIFI만. Kafka/로그 파이프라인은 pipeline_definition에 남는다.
    engine                 VARCHAR(20)  NOT NULL DEFAULT 'NIFI',
    comments               TEXT,
    parameter_context_id   VARCHAR(100),
    parameter_context_name VARCHAR(200),
    -- 캔버스 좌표. 화면에서 배치를 그대로 재현하거나 유실 후 복구할 때 쓴다.
    x_pos                  DOUBLE PRECISION,
    y_pos                  DOUBLE PRECISION,
    step_count             INTEGER      NOT NULL DEFAULT 0,
    running_count          INTEGER      NOT NULL DEFAULT 0,
    stopped_count          INTEGER      NOT NULL DEFAULT 0,
    invalid_count          INTEGER      NOT NULL DEFAULT 0,
    -- airflow의 동적 DAG 이름 규칙(nifi_pipeline_{그룹id 앞 8자}_control)을 그대로 계산해 둔다.
    -- 잡 <-> DAG 실행이력을 조인할 때 매번 문자열을 만들지 않기 위함.
    airflow_dag_id         VARCHAR(200),
    first_seen_at          TIMESTAMP    NOT NULL DEFAULT now(),
    last_synced_at         TIMESTAMP    NOT NULL DEFAULT now(),
    -- NiFi에서 사라져도 행을 지우지 않는다(캔버스 유실과 의도적 삭제를 구분할 수 없으므로).
    deleted_at             TIMESTAMP,
    created_at             TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at             TIMESTAMP    NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX uq_etl_job_nifi_pg_id ON etl_job (nifi_pg_id);
CREATE INDEX idx_etl_job_name ON etl_job (job_name);
CREATE INDEX idx_etl_job_live ON etl_job (deleted_at) WHERE deleted_at IS NULL;

-- ---------------------------------------------------------------------------
-- 스텝 = 프로세서 하나
--
-- 프로세서 설정 전체는 props_json(jsonb)에 통째로 담고, 조회에 자주 쓰는 값만
-- 컬럼으로 승격한다. 승격 대상은 "어느 DB의 무엇을 어디로 넣는가"에 답하는 값들이다.
-- ---------------------------------------------------------------------------
CREATE TABLE etl_job_step (
    id                  BIGSERIAL PRIMARY KEY,
    job_id              BIGINT       NOT NULL REFERENCES etl_job (id),
    nifi_processor_id   VARCHAR(100) NOT NULL,
    step_name           VARCHAR(200) NOT NULL,
    -- 짧은 타입명(ExecuteSQL, PutDatabaseRecord ...). 전체 클래스명은 props_json에.
    step_type           VARCHAR(100) NOT NULL,
    scheduling_strategy VARCHAR(30),
    scheduling_period   VARCHAR(50),
    -- ExecuteSQL의 조회 쿼리 / PutSQL의 실행문. 스크립트 프로세서는 null.
    sql_text            TEXT,
    -- PutDatabaseRecord 적재 대상 (schema.table 또는 table).
    target_table        VARCHAR(200),
    -- INSERT / UPSERT / UPDATE ... (PutDatabaseRecord)
    statement_type      VARCHAR(30),
    -- UPSERT 충돌 키. 파라미터 참조(#{...})면 그 표현식 그대로 들어온다.
    update_keys         VARCHAR(400),
    -- 어떤 DBCP 커넥션 풀을 쓰는지(컨트롤러 서비스 id). 원천/타깃 추적용.
    dbcp_service_id     VARCHAR(100),
    x_pos               DOUBLE PRECISION,
    y_pos               DOUBLE PRECISION,
    validation_status   VARCHAR(20),
    run_status          VARCHAR(20),
    props_json          JSONB,
    deleted_at          TIMESTAMP,
    created_at          TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at          TIMESTAMP    NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX uq_etl_job_step_processor ON etl_job_step (nifi_processor_id);
CREATE INDEX idx_etl_job_step_job ON etl_job_step (job_id);
CREATE INDEX idx_etl_job_step_target ON etl_job_step (target_table);

-- ---------------------------------------------------------------------------
-- 연결선 = 프로세서 사이 큐. 어떤 관계(success/failure/retry...)로 이어졌는지가 핵심.
-- ---------------------------------------------------------------------------
CREATE TABLE etl_job_link (
    id                 BIGSERIAL PRIMARY KEY,
    job_id             BIGINT       NOT NULL REFERENCES etl_job (id),
    nifi_connection_id VARCHAR(100) NOT NULL,
    from_component_id  VARCHAR(100) NOT NULL,
    from_name          VARCHAR(200),
    to_component_id    VARCHAR(100) NOT NULL,
    to_name            VARCHAR(200),
    -- 쉼표로 이어붙인 관계 이름. 예: "failure,retry"
    relationships      VARCHAR(200),
    deleted_at         TIMESTAMP,
    created_at         TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at         TIMESTAMP    NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX uq_etl_job_link_connection ON etl_job_link (nifi_connection_id);
CREATE INDEX idx_etl_job_link_job ON etl_job_link (job_id);

-- ---------------------------------------------------------------------------
-- 파라미터 = 그룹에 바인딩된 파라미터 컨텍스트의 값
--
-- 지금은 NiFi에서 읽어오기만 한다(sync_direction='FROM_NIFI'). 나중에 화면에서 값을
-- 바꾸는 단계로 갈 때 'TO_NIFI'로 바꾸고 푸시 로직만 붙이면 되도록 컬럼을 미리 둔다.
-- ---------------------------------------------------------------------------
CREATE TABLE etl_job_param (
    id             BIGSERIAL PRIMARY KEY,
    job_id         BIGINT       NOT NULL REFERENCES etl_job (id),
    param_name     VARCHAR(200) NOT NULL,
    param_value    TEXT,
    sensitive      BOOLEAN      NOT NULL DEFAULT false,
    description    TEXT,
    sync_direction VARCHAR(20)  NOT NULL DEFAULT 'FROM_NIFI',
    deleted_at     TIMESTAMP,
    created_at     TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at     TIMESTAMP    NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX uq_etl_job_param ON etl_job_param (job_id, param_name);

-- ---------------------------------------------------------------------------
-- 변경 스냅샷 - 구조가 실제로 바뀐 주기에만 1행.
--
-- 매 동기화마다 남기면 5분마다 쌓이므로, 잡 전체를 직렬화한 해시가 직전과 다를 때만
-- 넣는다. "언제 무엇이 바뀌었나"와 캔버스 유실 시 복구 근거를 동시에 담당한다.
-- ---------------------------------------------------------------------------
CREATE TABLE etl_job_snapshot (
    id           BIGSERIAL PRIMARY KEY,
    job_id       BIGINT       NOT NULL REFERENCES etl_job (id),
    captured_at  TIMESTAMP    NOT NULL DEFAULT now(),
    content_hash VARCHAR(64)  NOT NULL,
    snapshot     JSONB        NOT NULL
);

CREATE INDEX idx_etl_job_snapshot_job ON etl_job_snapshot (job_id, captured_at DESC);

-- ---------------------------------------------------------------------------
-- 통합 조회 뷰 - NiFi 잡 + Kafka/로그 파이프라인을 한 목록으로.
-- 원장은 각자 유지하고 조회만 합친다.
-- ---------------------------------------------------------------------------
CREATE VIEW v_etl_all_jobs AS
SELECT
    'NIFI'                                   AS engine,
    j.nifi_pg_id                             AS external_id,
    j.job_name                               AS job_name,
    j.airflow_dag_id                         AS airflow_dag_id,
    NULL::VARCHAR                            AS target_object,
    NULL::VARCHAR                            AS status,
    j.step_count                             AS step_count,
    j.last_synced_at                         AS last_seen_at
FROM etl_job j
WHERE j.deleted_at IS NULL
UNION ALL
SELECT
    p.pipeline_type,
    p.id::VARCHAR,
    p.name,
    'kafka_pipeline_' || p.id || '_control',
    COALESCE(p.target_schema || '.' || p.target_table, p.target_table),
    p.status,
    NULL::INTEGER,
    p.updated_at
FROM pipeline_definition p;
