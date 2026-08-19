package com.company.pipeline.nifi.dto;

import com.company.pipeline.monitoring.NifiExecutionLogEntry;
import java.time.LocalDateTime;

public record NifiExecutionLogResponse(
        Long id,
        String processorId,
        String processorName,
        String groupId,
        String groupName,
        Long jobId,
        String jobName,
        String rootGroupName,
        LocalDateTime occurredAt,
        Long insertedCount,
        String status,
        /** 실패 행의 원인 메시지(NiFi bulletin 원문). 성공 행은 null. */
        String message,
        /** 실패 행의 심각도(ERROR/WARNING). 성공 행은 null. */
        String level
) {
    public static NifiExecutionLogResponse from(NifiExecutionLogEntry entity) {
        return from(entity, null);
    }

    public static NifiExecutionLogResponse from(NifiExecutionLogEntry entity, String jobName) {
        return new NifiExecutionLogResponse(
                entity.getId(),
                entity.getProcessorId(),
                entity.getProcessorName(),
                entity.getGroupId(),
                entity.getGroupName(),
                entity.getJobId(),
                jobName,
                jobName,
                entity.getOccurredAt(),
                entity.getInsertedCount(),
                entity.getStatus(),
                entity.getMessage(),
                entity.getLevel()
        );
    }
}
