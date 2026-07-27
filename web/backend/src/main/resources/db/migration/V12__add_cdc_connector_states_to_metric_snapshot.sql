-- CDC 처리 로그에서 Source와 Sink의 상태를 따로 보여주기 위한 관측값.
-- 기존 connector_state는 이전 코드/데이터 호환을 위해 유지한다.
ALTER TABLE pipeline_metric_snapshot
    ADD COLUMN source_connector_state VARCHAR(30),
    ADD COLUMN sink_connector_state VARCHAR(30);
