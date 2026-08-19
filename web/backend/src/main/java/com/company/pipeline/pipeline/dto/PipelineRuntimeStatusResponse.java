package com.company.pipeline.pipeline.dto;

import java.time.LocalDateTime;
import java.util.List;

/** Kafka Connect 실측 상태와 metadata-db 저장 상태, 최근 제어 요청을 분리해 보여주는 읽기 모델. */
public record PipelineRuntimeStatusResponse(
        Long pipelineId,
        String storedStatus,
        String runtimeStatus,
        String sourceConnectorState,
        List<String> sourceTaskStates,
        String sinkConnectorState,
        List<String> sinkTaskStates,
        LocalDateTime runtimeCheckedAt,
        boolean statusMismatch,
        String runtimeStatusReason,
        String lastCommand,
        String lastCommandResult,
        String lastCommandMessage,
        LocalDateTime lastCommandAt
) {
}
