-- 삭제된 파이프라인의 "표시용" 메타데이터 보존.
-- CDC 처리로그/이벤트로그는 pipeline_metric_snapshot·pipeline_command_history 에서 파생되는데,
-- 이 테이블들은 파이프라인 삭제 후에도 보존정리(30일) 전까지 남아 있다. 그런데 화면 렌더에 필요한
-- 이름·소스/타겟 경로는 pipeline_definition 에만 있어 삭제 즉시 사라졌다. 삭제 시점에 그 표시값을
-- 여기 저장해 두면, 보존정리로 스냅샷이 지워지기 전까지 삭제된 파이프라인의 로그도 조회할 수 있다.
CREATE TABLE pipeline_metadata_archive (
    pipeline_id   BIGINT       PRIMARY KEY,
    name          VARCHAR(150) NOT NULL,
    pipeline_type VARCHAR(50),
    source_path   VARCHAR(255),
    target_path   VARCHAR(255),
    archived_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_pipeline_metadata_archive_archived_at ON pipeline_metadata_archive (archived_at);
