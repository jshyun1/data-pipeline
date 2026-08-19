package com.company.pipeline.monitoring.dto;

import com.company.pipeline.monitoring.DlqReplayRequest;
import java.time.LocalDateTime;

public record DlqReplayResponse(Long id, Long pipelineId, String dlqTopic, Integer dlqPartition, Long dlqOffset,
        String originalTopic, String riskLevel, String status, String reason, String requestedBy,
        LocalDateTime requestedAt, String approvedBy, LocalDateTime approvedAt, LocalDateTime executedAt,
        String resultMessage) {
    public static DlqReplayResponse from(DlqReplayRequest value) {
        return new DlqReplayResponse(value.getId(), value.getPipelineId(), value.getDlqTopic(),
                value.getDlqPartition(), value.getDlqOffset(), value.getOriginalTopic(), value.getRiskLevel(),
                value.getStatus(), value.getReason(), value.getRequestedBy(), value.getRequestedAt(),
                value.getApprovedBy(), value.getApprovedAt(), value.getExecutedAt(), value.getResultMessage());
    }
}
