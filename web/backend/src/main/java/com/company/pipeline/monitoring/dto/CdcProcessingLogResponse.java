package com.company.pipeline.monitoring.dto;

import java.time.LocalDateTime;

/** 1분 단위로 묶은 CDC Sink 소비 추정 이력. 실제 타깃 DB 커밋 행 수는 아니다. */
public record CdcProcessingLogResponse(
        Long pipelineId,
        String pipelineName,
        String source,
        String target,
        String topicName,
        LocalDateTime occurredAt,
        Long processedCount,
        Long committedOffset,
        Long dailyProcessedCount,
        Long consumerLag,
        String sourceState,
        String sinkState,
        String status,
        String message
) {
}
