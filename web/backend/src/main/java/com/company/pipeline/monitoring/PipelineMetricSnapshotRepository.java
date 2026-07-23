package com.company.pipeline.monitoring;

import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PipelineMetricSnapshotRepository extends JpaRepository<PipelineMetricSnapshot, Long> {

    Optional<PipelineMetricSnapshot> findTopByPipelineIdOrderByCollectedAtDesc(Long pipelineId);
}
