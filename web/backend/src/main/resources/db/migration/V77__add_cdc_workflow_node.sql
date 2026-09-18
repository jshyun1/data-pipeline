-- 워크플로우 캔버스에 CDC 노드를 얹는다(2026-09-10 설계 «방안 B» - 부르고 끝낸다).
--
-- 워크플로우는 «N개 파이프라인에 같은 지시를 한 번에» 쏘는 발사대다. CDC 노드 하나가
-- 파이프라인 하나의 제어 DAG(kafka_pipeline_{id}_control)를 부른다. 기다리지 않는다 -
-- start 제어 Run 은 CDC 수명만큼 살기 때문에 기다리면 워커 슬롯이 영구히 묶인다.
--
-- 워크플로우가 «CDC 워크플로우»인지는 따로 적지 않는다. 노드 구성에서 읽는다
-- (CDC 노드가 하나라도 있으면 CDC 워크플로우). 종류를 열 때 미리 고르게 하면
-- 빈 워크플로우에 «아직 아무것도 아님» 상태가 하나 더 생긴다.
ALTER TABLE etl_workflow_node
    ADD COLUMN pipeline_id BIGINT REFERENCES pipeline_definition (id);

-- 파이프라인을 지울 때 «이 파이프라인을 쓰는 노드»를 찾는 경로. job_id 쪽과 같은 모양이다.
CREATE INDEX idx_etl_wf_node_pipeline ON etl_workflow_node (pipeline_id) WHERE deleted_at IS NULL;

-- node_type 은 문자열이고 기본값이 'JOB' 이다(JOB · SUBWF · 이번에 더하는 CDC).
-- 값 검증은 애플리케이션이 한다 - 여기서 CHECK 로 묶으면 종류가 늘 때마다 마이그레이션이 필요하다.
COMMENT ON COLUMN etl_workflow_node.pipeline_id IS 'CDC 노드가 가리키는 CDC 파이프라인(node_type=CDC 일 때만)';
