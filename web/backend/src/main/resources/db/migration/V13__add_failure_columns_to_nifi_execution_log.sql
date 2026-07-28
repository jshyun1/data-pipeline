-- nifi_execution_log는 여태 "적재 카운터가 늘었을 때"만 행을 남겨서 status가 항상
-- SUCCESS였다. 즉 실패는 구조적으로 기록될 수 없었고, 화면의 "상태" 컬럼과 실패
-- 배지는 도달할 수 없는 코드였다. 실제로 DB 인증 실패로 truncate가 30초마다
-- 롤백되던 날에도, ExecuteSQL이 OOM으로 34번 죽던 날에도 이 표에는 아무것도
-- 남지 않았다.
--
-- NiFi의 Bulletin Board(경고/에러 알림)는 이 환경에서 정상 동작하는 것이 확인돼서
-- (Provenance는 이벤트 0건, 프로세서 단위 Status History는 항상 빈 응답), 그걸
-- 주기적으로 긁어 FAILED 행으로 남긴다. bulletin은 NiFi 메모리에 5분 남짓만
-- 보관되므로 놓치지 않으려면 짧은 주기로 가져와 여기에 영속화해야 한다.

ALTER TABLE nifi_execution_log
    -- bulletin 원문. 어떤 예외였는지가 여기 들어간다(길어서 넉넉히 잡음).
    ADD COLUMN message TEXT,
    -- NiFi가 bulletin마다 매기는 단조 증가 id. 다음 조회를 이 값 이후로만
    -- 요청해서(=after 파라미터) 같은 bulletin을 두 번 적재하지 않는다.
    ADD COLUMN bulletin_id BIGINT,
    ADD COLUMN level VARCHAR(20);

-- 같은 bulletin이 중복 저장되지 않도록. 카운터 기반 SUCCESS 행은 bulletin_id가
-- NULL이고, Postgres의 UNIQUE는 NULL을 서로 다른 값으로 취급하므로 영향 없다.
CREATE UNIQUE INDEX uq_nifi_execution_log_bulletin_id
    ON nifi_execution_log (bulletin_id)
    WHERE bulletin_id IS NOT NULL;

-- inserted_count는 적재 건수라 실패 행에는 의미가 없다(0으로 남긴다).
ALTER TABLE nifi_execution_log ALTER COLUMN inserted_count DROP NOT NULL;
