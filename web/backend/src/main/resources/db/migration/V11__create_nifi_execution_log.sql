-- NiFi 적재 프로세서(PutDatabaseRecord)의 실행 이력을 한 행씩 남긴다. NiFi 자체엔
-- "이 프로세서가 언제 한 번 실행됐다"는 이력 개념이 없어서(Provenance는 이 환경에서
-- 인덱스/이벤트파일 불일치 버그로 조회 불가로 확인됨, 프로세서 단위 Status History도
-- 항상 비어서 조회 불가로 확인됨), NifiPipelineMetricScheduler가 이미 60초마다 확인하는
-- "INSERT updates performed" 누적 카운터의 증가분(delta > 0)을 "그 주기에 실행됨"으로
-- 간주해서 한 행씩 기록한다.
CREATE TABLE nifi_execution_log (
    id BIGSERIAL PRIMARY KEY,
    processor_id VARCHAR(100) NOT NULL,
    processor_name VARCHAR(200) NOT NULL,
    group_id VARCHAR(100),
    group_name VARCHAR(200),
    occurred_at TIMESTAMP NOT NULL,
    inserted_count BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_nifi_execution_log_occurred_at ON nifi_execution_log (occurred_at);
CREATE INDEX idx_nifi_execution_log_processor_id ON nifi_execution_log (processor_id);
