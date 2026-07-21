package com.company.pipeline.pipeline.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record PipelineCreateRequest(
        @NotBlank String name,
        @NotNull Long sourceConnectionId,
        @NotNull Long targetConnectionId,
        @NotBlank String sourceSchema,
        @NotBlank String sourceTable,
        @NotBlank String targetSchema,
        @NotBlank String targetTable,
        @NotBlank String topicPrefix,
        Boolean deleteEnabled,
        String description
) {
}
