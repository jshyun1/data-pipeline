package com.company.pipeline.logpipeline.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * paths는 filebeat 컨테이너 기준 경로여야 한다(호스트 경로 아님, docker-compose의
 * ./log-sources가 filebeat 컨테이너 안에서 /var/log/app로 마운트됨).
 * topicName을 비우면 서비스 레이어에서 자동 생성한다.
 * parseType/multilineEnabled는 이번 증분 범위 밖(PLAIN만 지원) - 다른 값을 넣으면
 * PipelineService.createLogFilePipeline에서 VALIDATION_ERROR로 거절한다.
 */
public record LogPipelineCreateRequest(
        @NotBlank String name,
        String agentHost,
        @NotBlank String filePath,
        String filePattern,
        String readFrom,
        String parseType,
        String encoding,
        Boolean multilineEnabled,
        @NotNull Long targetConnectionId,
        @NotBlank String targetSchema,
        @NotBlank String targetTable,
        String topicName,
        String description
) {
}
