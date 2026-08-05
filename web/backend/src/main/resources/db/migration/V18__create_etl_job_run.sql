-- 잡 실행 이력 - "어느 잡의 몇 번째 실행인가"를 담는다.
--
-- 지금까지 실행 이력은 프로세서 단위로만 있었다(nifi_processor_run = 구간,
-- nifi_execution_log = 시점). 그래서 "load-dw-POP001L이 118초에 166만 건"은 알 수 있어도
-- "DZ 잡의 어제 실행이 총 몇 초 걸렸고 몇 건 처리했나"에는 답할 수 없었다.
--
-- V17에서 만든 잡 카탈로그 덕분에 프로세서 -> 잡 해석이 가능해졌으므로, 여기서
--   1) 잡 실행 1회를 etl_job_run 한 행으로 만들고
--   2) 기존 두 이력 테이블에 잡/실행 참조를 붙인다.
--
-- NiFi에는 "실행 1회" 개념이 없다(Provenance 0건, 프로세서 Status History 빈 응답).
-- 그래서 이 행도 관측으로 만든다 - 잡 소속 프로세서 중 하나라도 활동을 시작하면 열고,
-- 모두 조용해지면 닫는다(NifiProcessorRunTracker의 15초 폴링을 잡 단위로 확장).

CREATE TABLE etl_job_run (
    id                 BIGSERIAL PRIMARY KEY,
    job_id             BIGINT      NOT NULL REFERENCES etl_job (id),
    -- Airflow DagRun과의 연결. 백엔드에 Airflow API 클라이언트가 아직 없어(헬스 체크만
    -- 무인증으로 호출) 이번 증분에서는 채우지 않는다. 채우게 되면 "누가/무엇이 이 실행을
    -- 지시했나"가 완성된다.
    airflow_dag_run_id VARCHAR(250),
    -- OBSERVED: NiFi 활동을 관측해서 만든 실행(현재 전부 이 값)
    -- AIRFLOW / MANUAL: DagRun 매칭을 붙인 뒤 구분
    trigger_source     VARCHAR(20) NOT NULL DEFAULT 'OBSERVED',
    started_at         TIMESTAMP   NOT NULL,
    -- 진행 중이면 NULL.
    ended_at           TIMESTAMP,
    -- 마지막으로 활동을 관측한 시각. 종료 판정 근거.
    last_seen_at       TIMESTAMP   NOT NULL,
    -- RUNNING / SUCCESS / FAILED
    status             VARCHAR(20) NOT NULL,
    step_run_count     INTEGER     NOT NULL DEFAULT 0,
    total_inserted     BIGINT      NOT NULL DEFAULT 0,
    failed_step_count  INTEGER     NOT NULL DEFAULT 0,
    created_at         TIMESTAMP   NOT NULL DEFAULT now(),
    updated_at         TIMESTAMP   NOT NULL DEFAULT now()
);

CREATE INDEX idx_etl_job_run_job_started ON etl_job_run (job_id, started_at DESC);
-- 한 잡에 열린 실행은 하나뿐이어야 한다(nifi_processor_run의 열린 구간 제약과 같은 규칙).
CREATE UNIQUE INDEX uq_etl_job_run_open ON etl_job_run (job_id) WHERE ended_at IS NULL;

-- ---------------------------------------------------------------------------
-- 기존 이력 테이블에 참조 추가
--
-- job_id는 job_run_id가 있어도 따로 둔다. 소급분처럼 실행 묶음을 만들 수 없는 행도
-- "어느 잡의 이력인지"는 알 수 있어야 하기 때문이다.
-- ---------------------------------------------------------------------------
ALTER TABLE nifi_processor_run ADD COLUMN job_id     BIGINT;
ALTER TABLE nifi_processor_run ADD COLUMN job_run_id BIGINT;
ALTER TABLE nifi_execution_log ADD COLUMN job_id     BIGINT;

CREATE INDEX idx_nifi_processor_run_job     ON nifi_processor_run (job_id, started_at DESC);
CREATE INDEX idx_nifi_processor_run_job_run ON nifi_processor_run (job_run_id);
CREATE INDEX idx_nifi_execution_log_job     ON nifi_execution_log (job_id, occurred_at DESC);

-- ---------------------------------------------------------------------------
-- 소급 1) 잡 귀속
--
-- 두 경로를 순서대로 쓴다.
--   (a) group_id가 최상위 잡 그룹인 경우 - 대부분의 행(DW/DZ/http-ingest 등)
--   (b) 하위 그룹 소속이라 (a)로 안 붙는 행 - 프로세서 id로 잡을 찾는다
-- 지금은 없는 그룹(initial-load-batch 등)이나 삭제된 프로세서의 행은 NULL로 남는다.
-- ---------------------------------------------------------------------------
UPDATE nifi_execution_log l
   SET job_id = j.id
  FROM etl_job j
 WHERE j.nifi_pg_id = l.group_id AND l.job_id IS NULL;

UPDATE nifi_execution_log l
   SET job_id = s.job_id
  FROM etl_job_step s
 WHERE s.nifi_processor_id = l.processor_id AND l.job_id IS NULL;

UPDATE nifi_processor_run r
   SET job_id = j.id
  FROM etl_job j
 WHERE j.nifi_pg_id = r.group_id AND r.job_id IS NULL;

UPDATE nifi_processor_run r
   SET job_id = s.job_id
  FROM etl_job_step s
 WHERE s.nifi_processor_id = r.processor_id AND r.job_id IS NULL;

-- ---------------------------------------------------------------------------
-- 소급 2) 과거 실행 묶기
--
-- 잡별로 프로세서 구간을 시간순으로 훑어, 앞 구간이 끝난 뒤 120초 안에 다음 구간이
-- 시작되면 같은 실행으로 본다. DZ 체인처럼 테이블을 순차 처리하는 잡에서 스텝 사이
-- 공백이 그 정도이기 때문이다(관측 주기 15초 + 다음 프로세서 기동).
--
-- 이건 추정이다 - 실제 트리거 단위를 알 수 없으므로 경계가 어긋날 수 있고, 상태는
-- 일괄 SUCCESS로 둔다(실패 판정 근거인 bulletin은 5분만 남아 소급이 불가능).
-- 앞으로 쌓이는 실행은 관측으로 직접 만들어지므로 이 추정이 적용되지 않는다.
-- ---------------------------------------------------------------------------
WITH marked AS (
    SELECT
        r.id,
        r.job_id,
        r.started_at,
        COALESCE(r.ended_at, r.started_at) AS ended_at,
        r.inserted_count,
        CASE
            WHEN r.started_at - LAG(COALESCE(r.ended_at, r.started_at))
                     OVER (PARTITION BY r.job_id ORDER BY r.started_at) > INTERVAL '120 seconds'
                 OR LAG(r.started_at) OVER (PARTITION BY r.job_id ORDER BY r.started_at) IS NULL
            THEN 1 ELSE 0
        END AS is_new_run
    FROM nifi_processor_run r
    WHERE r.job_id IS NOT NULL AND r.ended_at IS NOT NULL
),
grouped AS (
    SELECT *, SUM(is_new_run) OVER (PARTITION BY job_id ORDER BY started_at) AS run_no
    FROM marked
),
runs AS (
    SELECT job_id, run_no,
           MIN(started_at)        AS started_at,
           MAX(ended_at)          AS ended_at,
           COUNT(*)               AS step_run_count,
           SUM(inserted_count)    AS total_inserted
    FROM grouped
    GROUP BY job_id, run_no
),
inserted AS (
    INSERT INTO etl_job_run
        (job_id, trigger_source, started_at, ended_at, last_seen_at, status,
         step_run_count, total_inserted, failed_step_count)
    SELECT job_id, 'OBSERVED', started_at, ended_at, ended_at, 'SUCCESS',
           step_run_count, COALESCE(total_inserted, 0), 0
    FROM runs
    RETURNING id, job_id, started_at, ended_at
)
UPDATE nifi_processor_run r
   SET job_run_id = i.id
  FROM inserted i
 WHERE r.job_id = i.job_id
   AND r.ended_at IS NOT NULL
   AND r.started_at >= i.started_at
   AND COALESCE(r.ended_at, r.started_at) <= i.ended_at;
