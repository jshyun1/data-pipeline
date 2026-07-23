-- 대시보드의 "일자별/파이프라인별/태스크별 실제 적재 건수"를 위한 롤업 테이블.
-- Kafka(15~30초 간격 Spring 스케줄러)/NiFi(1분 간격 Airflow DAG) 양쪽이 새 건수를
-- 감지할 때마다 UPSERT로 오늘 날짜 row에 누적한다. 원시 이벤트를 매번 다시 합산하는
-- 대신, 조회 시엔 항상 "날짜 수 x 파이프라인 수"만큼의 적은 row만 읽으면 된다.
CREATE TABLE pipeline_daily_load_metric (
    id BIGSERIAL PRIMARY KEY,
    pipeline_source VARCHAR(20) NOT NULL,
    pipeline_key VARCHAR(200) NOT NULL,
    task_key VARCHAR(200) NOT NULL,
    pipeline_label VARCHAR(200),
    load_date DATE NOT NULL,
    loaded_count BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT uq_pipeline_daily_load_metric UNIQUE (pipeline_source, pipeline_key, task_key, load_date)
);

CREATE INDEX idx_pipeline_daily_load_metric_load_date ON pipeline_daily_load_metric (load_date);
