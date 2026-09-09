-- CDC 파이프라인을 담는 «그룹». 사용자가 직접 만들고 이름 붙이는 폴더다.
--
-- 예전에는 관리 화면 트리를 소스 DB > 스키마로 자동으로 묶어 보여줬다. 그런데 그건
-- 파이프라인이 «어디서 오는가»일 뿐, 운영자가 묶어서 보고 싶은 단위(업무·과제·담당)와
-- 다르다. ETL 쪽이 NiFi 그룹으로 폴더를 만들 듯 CDC 도 자기 폴더를 갖는다.
--
-- 계층은 self-reference 로 잡는다. 최상단(«전체 파이프라인»)은 행이 아니라 화면이 그리는
-- 가상 뿌리이고, parent_id 가 NULL 인 그룹이 그 바로 아래 칸이다.
CREATE TABLE pipeline_group (
    id         BIGSERIAL PRIMARY KEY,
    parent_id  BIGINT       REFERENCES pipeline_group (id),
    name       VARCHAR(100) NOT NULL,
    created_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE pipeline_group IS 'CDC 파이프라인 그룹(사용자가 만드는 폴더). 최상단은 화면이 그리는 가상 뿌리라 행이 없다.';
COMMENT ON COLUMN pipeline_group.parent_id IS '상위 그룹. NULL 이면 «전체 파이프라인» 바로 아래.';

-- 같은 부모 밑에서 이름이 겹치면 트리에서 구분이 안 된다. parent_id 가 NULL 인 행끼리도
-- 막아야 하는데 NULL 은 서로 다른 값으로 취급되므로, 0 으로 접어서 비교한다(0 은 BIGSERIAL
-- 이 쓰지 않는 값이라 실제 그룹 id 와 겹치지 않는다).
CREATE UNIQUE INDEX ux_pipeline_group_parent_name
    ON pipeline_group (COALESCE(parent_id, 0), name);

CREATE INDEX ix_pipeline_group_parent ON pipeline_group (parent_id);

-- 파이프라인이 속한 그룹. NULL 이면 «그룹 미지정»으로, 트리에서 전체 파이프라인 바로 밑에
-- 놓인다. 기존 파이프라인은 전부 여기서 시작한다(그룹은 나중에 지정하면 된다).
ALTER TABLE pipeline_definition
    ADD COLUMN group_id BIGINT REFERENCES pipeline_group (id);

COMMENT ON COLUMN pipeline_definition.group_id IS 'CDC 파이프라인 그룹. NULL 이면 그룹 미지정.';

CREATE INDEX ix_pipeline_definition_group ON pipeline_definition (group_id);
