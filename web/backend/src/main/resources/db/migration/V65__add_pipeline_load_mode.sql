-- CDC 파이프라인 적재 방식. UPSERT(기존 동작, 타깃을 소스와 같은 모습으로 유지) 또는
-- DELTA_APPEND(변경 이벤트를 구분컬럼과 함께 append-only 델타 테이블에 쌓음).
ALTER TABLE pipeline_definition ADD COLUMN load_mode VARCHAR(30) NOT NULL DEFAULT 'UPSERT';
-- DELTA_APPEND 에서 insert/update/delete 를 구분하는 컬럼명(타깃 델타 테이블 맨 앞).
ALTER TABLE pipeline_definition ADD COLUMN delta_op_column VARCHAR(100);
