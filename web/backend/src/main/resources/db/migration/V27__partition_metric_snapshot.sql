-- =============================================================================
-- V27__partition_metric_snapshot.sql  (설계서 4-5절, U6/U7)
-- =============================================================================
-- pipeline_metric_snapshot 을 일 단위 RANGE 파티션으로 전환한다. DELETE 대신 DROP PARTITION
-- 을 쓰면 384MB 컨테이너의 WAL 폭증/락/autovacuum 문제가 동시에 사라진다. 지금 행 수가 적어
-- 전환 비용이 사실상 0이다.
--
-- 안전장치: DEFAULT 파티션을 둬서 일 파티션이 없어도 INSERT 가 절대 실패하지 않게 한다(적재
-- 무중단). 평시엔 MetricSnapshotPartitionService 가 일 파티션을 미리 만들어 데이터가 DEFAULT 로
-- 안 떨어지고, RetentionService(PARTITION_DROP)가 보존 초과 일 파티션을 떨군다.
--
-- 절차: 신규 파티션 테이블 생성 -> 인덱스 -> DEFAULT 파티션 -> INSERT SELECT -> 원자적 RENAME 교체.
-- 구 테이블/인덱스는 _v0 로 남긴다(롤백 안전, 다음 릴리스에서 제거).
-- =============================================================================
CREATE TABLE pipeline_metric_snapshot_p (
    id                     bigint       NOT NULL DEFAULT nextval('pipeline_metric_snapshot_id_seq'),
    pipeline_id            bigint       NOT NULL,
    -- 파티션 키는 NOT NULL 이어야 한다(구 테이블은 nullable 이었으나 앱이 항상 값을 넣는다).
    collected_at           timestamp without time zone NOT NULL DEFAULT now(),
    connector_state        varchar(30),
    task_state             varchar(30),
    topic_name             varchar(200),
    partition_count        integer,
    end_offset             bigint,
    committed_offset       bigint,
    consumer_lag           bigint,
    error_count            bigint       DEFAULT 0,
    last_error_message     text,
    source_connector_state varchar(30),
    sink_connector_state   varchar(30),
    -- 파티션 테이블의 PK/UNIQUE 는 파티션 키를 포함해야 한다.
    PRIMARY KEY (id, collected_at)
) PARTITION BY RANGE (collected_at);

CREATE INDEX idx_metric_snapshot_pipeline_collected_p
    ON pipeline_metric_snapshot_p (pipeline_id, collected_at);

-- DEFAULT 파티션(안전망). 일 파티션이 매칭 안 되는 행을 여기서 받아 INSERT 실패를 막는다.
CREATE TABLE pipeline_metric_snapshot_pdefault
    PARTITION OF pipeline_metric_snapshot_p DEFAULT;

-- 데이터 이관(컬럼 순서 명시).
INSERT INTO pipeline_metric_snapshot_p
    (id, pipeline_id, collected_at, connector_state, task_state, topic_name, partition_count,
     end_offset, committed_offset, consumer_lag, error_count, last_error_message,
     source_connector_state, sink_connector_state)
SELECT id, pipeline_id, coalesce(collected_at, now()), connector_state, task_state, topic_name,
       partition_count, end_offset, committed_offset, consumer_lag, error_count, last_error_message,
       source_connector_state, sink_connector_state
FROM pipeline_metric_snapshot;

-- 원자적 교체.
ALTER INDEX idx_metric_snapshot_pipeline_collected RENAME TO idx_metric_snapshot_pipeline_collected_v0;
ALTER TABLE pipeline_metric_snapshot RENAME TO pipeline_metric_snapshot_v0;
ALTER TABLE pipeline_metric_snapshot_p RENAME TO pipeline_metric_snapshot;
ALTER INDEX idx_metric_snapshot_pipeline_collected_p RENAME TO idx_metric_snapshot_pipeline_collected;

-- 시퀀스 소유권을 새 테이블로 옮긴다(안 그러면 다음 릴리스에서 _v0 DROP 시 시퀀스가 함께 사라진다).
ALTER SEQUENCE pipeline_metric_snapshot_id_seq OWNED BY pipeline_metric_snapshot.id;
