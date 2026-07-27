package com.company.pipeline.monitoring;

import java.util.List;
import java.util.Optional;
import java.time.LocalDateTime;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PipelineMetricSnapshotRepository extends JpaRepository<PipelineMetricSnapshot, Long> {

    Optional<PipelineMetricSnapshot> findTopByPipelineIdOrderByCollectedAtDesc(Long pipelineId);

    List<PipelineMetricSnapshot> findTop2ByPipelineIdOrderByCollectedAtDesc(Long pipelineId);

    Optional<PipelineMetricSnapshot> findTopByPipelineIdAndCollectedAtBeforeOrderByCollectedAtDesc(
            Long pipelineId, LocalDateTime collectedAt);

    List<PipelineMetricSnapshot> findByPipelineIdInAndCollectedAtBetweenOrderByCollectedAtAsc(
            List<Long> pipelineIds, LocalDateTime from, LocalDateTime to);
}
