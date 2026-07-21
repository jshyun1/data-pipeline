package com.company.pipeline.pipeline.dto;

import com.company.pipeline.pipeline.PipelineCommandHistory;
import java.time.LocalDateTime;

public record PipelineCommandHistoryResponse(
        Long id,
        Long pipelineId,
        String command,
        String result,
        String message,
        LocalDateTime requestedAt,
        LocalDateTime completedAt
) {
    public static PipelineCommandHistoryResponse from(PipelineCommandHistory entity) {
        return new PipelineCommandHistoryResponse(
                entity.getId(),
                entity.getPipelineId(),
                entity.getCommand(),
                entity.getResult(),
                entity.getMessage(),
                entity.getRequestedAt(),
                entity.getCompletedAt()
        );
    }
}
