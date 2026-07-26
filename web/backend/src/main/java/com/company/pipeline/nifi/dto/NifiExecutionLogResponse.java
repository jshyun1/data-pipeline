package com.company.pipeline.nifi.dto;

import com.company.pipeline.monitoring.NifiExecutionLogEntry;
import java.time.LocalDateTime;

public record NifiExecutionLogResponse(
        Long id,
        String processorId,
        String processorName,
        String groupId,
        String groupName,
        LocalDateTime occurredAt,
        Long insertedCount,
        String status
) {
    public static NifiExecutionLogResponse from(NifiExecutionLogEntry entity) {
        return new NifiExecutionLogResponse(
                entity.getId(),
                entity.getProcessorId(),
                entity.getProcessorName(),
                entity.getGroupId(),
                entity.getGroupName(),
                entity.getOccurredAt(),
                entity.getInsertedCount(),
                entity.getStatus()
        );
    }
}
