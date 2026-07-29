-- ETL 로그를 "실행 구간"으로 볼 수 있게 하는 테이블.
--
-- 기존 nifi_execution_log의 한 행은 "이 60초 구간에 카운터가 N만큼 늘었다"라는 뜻이라,
-- 6분 걸린 적재 하나가 7행으로 흩어졌다(실측: load-dw-POP001L이 14:03:35~14:10:14에
-- 66초 간격 7행). 시작/종료/소요/처리량을 알 방법이 없고 화면도 29페이지가 됐다.
--
-- NiFi에서 실행 구간을 직접 얻을 방법은 이 환경에 없다 - Provenance는 이벤트를 0건
-- 반환하고(재시작/저장소 재구축 후에도 재현), 프로세서 단위 Status History는 항상
-- 비어 있다. 그래서 백엔드가 15초마다 activeThreadCount와 적재 카운터를 관측해서
-- 구간을 직접 만든다(NifiProcessorRunTracker). 시작/종료는 관측값이라 폴링 주기만큼
-- 오차가 있다.

CREATE TABLE nifi_processor_run (
    id             BIGSERIAL PRIMARY KEY,
    processor_id   VARCHAR(100) NOT NULL,
    processor_name VARCHAR(200) NOT NULL,
    -- PutDatabaseRecord / ExecuteGroovyScript 등. 과거 소급분은 알 수 없어 NULL.
    processor_type VARCHAR(100),
    group_id       VARCHAR(100),
    group_name     VARCHAR(200),
    -- 적재 대상(schema.table). 프로세서 설정에서 읽어온다. 스크립트 기반 적재는
    -- 테이블명이 코드 안에 있어 알 수 없으므로 NULL.
    target_table   VARCHAR(200),
    started_at     TIMESTAMP NOT NULL,
    -- 진행 중이면 NULL. 유휴가 확인된 뒤 마지막 활동 관측 시각으로 채운다.
    ended_at       TIMESTAMP,
    inserted_count BIGINT NOT NULL DEFAULT 0,
    -- RUNNING / SUCCESS. 실패는 bulletin 기반이라 nifi_execution_log가 담당한다.
    status         VARCHAR(20) NOT NULL,
    -- 마지막으로 "돌고 있다"를 관측한 시각. 종료 판정과 ended_at의 근거값.
    last_seen_at   TIMESTAMP NOT NULL,
    created_at     TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_nifi_processor_run_started_at ON nifi_processor_run (started_at);
CREATE INDEX idx_nifi_processor_run_processor_id ON nifi_processor_run (processor_id);
-- 열려 있는(진행 중) 구간은 프로세서당 하나뿐이어야 한다.
CREATE UNIQUE INDEX uq_nifi_processor_run_open
    ON nifi_processor_run (processor_id)
    WHERE ended_at IS NULL;

-- ---------------------------------------------------------------------------
-- 과거 이력 소급 병합
--
-- 기존 SUCCESS 행을 프로세서별로 시간순으로 훑어서, 앞 행과의 간격이 임계(150초)
-- 이내면 같은 실행으로 묶는다. 수집 주기가 60초였으므로 정상적으로 이어지는 구간은
-- 60~70초 간격이고, 150초를 넘으면 사이에 최소 한 주기는 아무 적재가 없었다는 뜻이라
-- 다른 실행으로 본다.
--
-- 한계: 여기서 만들어지는 started_at은 "적재를 처음 관측한 시각"이라 실제 시작보다
-- 최대 60초 늦다. 그리고 한 주기 안에 끝난 적재는 started_at = ended_at이 되어
-- 소요 0초로 보인다(처리량 계산 불가). 앞으로 쌓이는 데이터는 15초 주기라 이 오차가
-- 훨씬 작다.
-- ---------------------------------------------------------------------------
INSERT INTO nifi_processor_run
    (processor_id, processor_name, group_id, group_name,
     started_at, ended_at, inserted_count, status, last_seen_at)
SELECT
    processor_id,
    MAX(processor_name)                AS processor_name,
    MAX(group_id)                      AS group_id,
    MAX(group_name)                    AS group_name,
    MIN(occurred_at)                   AS started_at,
    MAX(occurred_at)                   AS ended_at,
    SUM(inserted_count)                AS inserted_count,
    'SUCCESS'                          AS status,
    MAX(occurred_at)                   AS last_seen_at
FROM (
    SELECT *,
           SUM(is_new_run) OVER (PARTITION BY processor_id ORDER BY occurred_at) AS run_no
    FROM (
        SELECT
            processor_id, processor_name, group_id, group_name, occurred_at, inserted_count,
            CASE
                WHEN occurred_at
                     - LAG(occurred_at) OVER (PARTITION BY processor_id ORDER BY occurred_at)
                     > INTERVAL '150 seconds'
                THEN 1 ELSE 0
            END AS is_new_run
        FROM nifi_execution_log
        WHERE status = 'SUCCESS' AND inserted_count IS NOT NULL
    ) marked
) grouped
GROUP BY processor_id, run_no;
