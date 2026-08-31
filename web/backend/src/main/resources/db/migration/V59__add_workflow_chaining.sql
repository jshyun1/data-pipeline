-- 워크플로우 → 워크플로우 연결(Asset 체이닝).
--
-- "DZ 일배치가 끝나면 DW 일배치가 돈다"를 표현한다. 두 워크플로우를 직접 묶지 않고
-- Airflow Asset(데이터 산출물)을 매개로 연결하는 이유:
--   - 생산자는 소비자를 모른다. 나중에 리포트 워크플로우를 하나 더 붙여도 DZ는 안 바뀐다.
--   - 여러 선행을 AND로 기다릴 수 있다(둘 다 끝나야 시작).
--   - Airflow UI의 Asset 그래프에 계보가 그대로 보인다.
--
-- produces_asset_uri 는 게시할 때 시스템이 채운다(cerebro://etl/{workflow_key}).
-- 사용자는 화면에서 "선행 워크플로우"만 고르고, URI는 볼 일이 없다.

ALTER TABLE etl_workflow
    ADD COLUMN produces_asset_uri VARCHAR(300),
    -- 선행 워크플로우 id 배열. 비어 있으면 스케줄(또는 수동)로만 실행된다.
    ADD COLUMN upstream_workflow_ids JSONB,
    -- ALL = 선행이 모두 끝나야 시작(기본), ANY = 하나라도 끝나면 시작
    ADD COLUMN upstream_mode VARCHAR(10) NOT NULL DEFAULT 'ALL';

-- 게시된 워크플로우의 Asset URI는 유일해야 한다(중복이면 엉뚱한 트리거가 섞인다).
CREATE UNIQUE INDEX uq_etl_workflow_asset
    ON etl_workflow (produces_asset_uri)
    WHERE deleted_at IS NULL AND produces_asset_uri IS NOT NULL;
