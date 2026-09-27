package com.company.pipeline.pipeline.dto;

import com.company.pipeline.pipeline.PipelineCommandHistory;
import java.time.LocalDateTime;

public record PipelineCommandHistoryResponse(
        Long id,
        Long pipelineId,
        String command,
        String result,
        String message,
        /** 누가 했는지. 건너뛰기(SKIP_RECORD)처럼 데이터를 버리는 조치는 이 값이 감사의 핵심이다. */
        String requestedBy,
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
                entity.getRequestedBy(),
                entity.getRequestedAt(),
                entity.getCompletedAt()
        );
    }
}
