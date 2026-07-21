package com.company.pipeline.monitoring;

import org.springframework.data.jpa.repository.JpaRepository;

public interface PipelineMetricSnapshotRepository extends JpaRepository<PipelineMetricSnapshot, Long> {
}
