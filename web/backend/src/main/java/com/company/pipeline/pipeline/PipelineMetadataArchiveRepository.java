package com.company.pipeline.pipeline;

import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PipelineMetadataArchiveRepository extends JpaRepository<PipelineMetadataArchive, Long> {

    /** 최근에 삭제된(=스냅샷이 아직 남아 있을 수 있는) 파이프라인 메타. */
    List<PipelineMetadataArchive> findByArchivedAtAfter(OffsetDateTime cutoff);
}
