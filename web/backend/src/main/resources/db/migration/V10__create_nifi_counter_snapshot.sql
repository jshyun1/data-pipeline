-- NiFi PutDatabaseRecord 프로세서별 "INSERT updates performed" 카운터의 마지막
-- 확인 값을 저장한다. NiFi 카운터는 재시작하면 0으로 리셋되는 인메모리 값이라,
-- 다음 폴링 주기에 직전 값과 비교해 증가분만 pipeline_daily_load_metric에 반영하려면
-- 이 마지막 값을 어딘가에 남겨둬야 한다(Kafka의 committed offset 스냅샷과 같은 역할).
CREATE TABLE nifi_counter_snapshot (
    processor_id VARCHAR(100) PRIMARY KEY,
    processor_name VARCHAR(200),
    last_value BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);
