-- bulletin 수집이 NiFi 재시작 뒤 조용히 멈추던 것을 고친다.
--
-- V13은 "NiFi가 bulletin마다 매기는 단조 증가 id"를 전제로 두 가지를 했다.
--   1) 다음 조회를 max(bulletin_id) 이후로만 요청 (after 파라미터)
--   2) bulletin_id에 UNIQUE 인덱스
-- 그런데 이 id는 **NiFi 프로세스 안에서만** 단조 증가한다. 컨테이너를 재시작하면
-- 1부터 다시 시작한다.
--
-- 그래서 실제로 이런 일이 났다(2026-07-29):
--   - 전날까지 수집한 max(bulletin_id) = 5
--   - NiFi 재시작 -> 09:04에 ORA-12170으로 원천 조회가 4건 실패, 새 bulletin id는 1~5
--   - after=5로 조회하니 NiFi가 전부 걸러서 반환 -> 한 건도 못 가져옴
--   - 설령 가져왔어도 UNIQUE 인덱스가 막았을 것
--   - 결과: DZ 5개 테이블이 통째로 비워진 사고가 ETL 로그에 한 줄도 안 남음
--
-- 고치는 방향: id를 전역 키로 믿지 않는다. 같은 id라도 시간이 충분히 벌어져 있으면
-- 다른 NiFi 세션의 다른 사건이므로 별개 행으로 남겨야 한다. 중복 판정은 애플리케이션이
-- "최근 N분 안에 같은 id가 있었나"로 하고(NifiPipelineMetricScheduler 참고),
-- 여기서는 그 조회가 빠르도록 인덱스만 맞춰준다.

DROP INDEX IF EXISTS uq_nifi_execution_log_bulletin_id;

-- (bulletin_id, occurred_at) 조회용. UNIQUE가 아니다 - 재시작 후 재사용되는 id를
-- 막으면 안 되기 때문이다.
CREATE INDEX idx_nifi_execution_log_bulletin_id_occurred_at
    ON nifi_execution_log (bulletin_id, occurred_at)
    WHERE bulletin_id IS NOT NULL;
