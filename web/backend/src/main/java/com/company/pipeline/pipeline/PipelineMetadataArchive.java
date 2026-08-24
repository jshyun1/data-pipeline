package com.company.pipeline.pipeline;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;

/**
 * 삭제된 파이프라인의 표시용 메타데이터(설계서 §CDC 로그). 파이프라인을 지워도 CDC 처리로그/이벤트로그가
 * 스냅샷 보존정리(30일) 전까지 이름·경로를 보여줄 수 있게, 삭제 시점에 여기 보존한다.
 */
@Entity
@Table(name = "pipeline_metadata_archive")
public class PipelineMetadataArchive {

    @Id
    @Column(name = "pipeline_id")
    private Long pipelineId;

    @Column(nullable = false, length = 150)
    private String name;

    @Column(name = "pipeline_type", length = 50)
    private String pipelineType;

    @Column(name = "source_path", length = 255)
    private String sourcePath;

    @Column(name = "target_path", length = 255)
    private String targetPath;

    // DB default now() 가 채운다(삽입/수정에서 제외).
    @Column(name = "archived_at", nullable = false, insertable = false, updatable = false)
    private OffsetDateTime archivedAt;

    protected PipelineMetadataArchive() {
    }

    public PipelineMetadataArchive(Long pipelineId, String name, String pipelineType,
                                   String sourcePath, String targetPath) {
        this.pipelineId = pipelineId;
        this.name = name;
        this.pipelineType = pipelineType;
        this.sourcePath = sourcePath;
        this.targetPath = targetPath;
    }

    public Long getPipelineId() {
        return pipelineId;
    }

    public String getName() {
        return name;
    }

    public String getPipelineType() {
        return pipelineType;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public String getTargetPath() {
        return targetPath;
    }

    public OffsetDateTime getArchivedAt() {
        return archivedAt;
    }
}
