package com.company.pipeline.monitoring.dto;

import java.time.LocalDateTime;

/** CDC 파이프라인의 배포/시작/중지/장애 등 상태 이벤트. */
public record CdcEventLogResponse(
        Long id,
        Long pipelineId,
        String pipelineName,
        String command,
        String result,
        String message,
        String requestedBy,
        LocalDateTime occurredAt,
        LocalDateTime completedAt
) {
}
