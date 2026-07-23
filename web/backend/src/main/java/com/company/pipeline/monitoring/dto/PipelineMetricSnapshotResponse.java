package com.company.pipeline.monitoring.dto;

import com.company.pipeline.monitoring.PipelineMetricSnapshot;
import java.time.LocalDateTime;

public record PipelineMetricSnapshotResponse(
        Long pipelineId,
        LocalDateTime collectedAt,
        String topicName,
        Long committedOffset
) {
    public static PipelineMetricSnapshotResponse from(PipelineMetricSnapshot entity) {
        return new PipelineMetricSnapshotResponse(
                entity.getPipelineId(),
                entity.getCollectedAt(),
                entity.getTopicName(),
                entity.getCommittedOffset());
    }
}
