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
        String snapshotMode,
        Boolean deleteEnabled,
        String description
) {
    public PipelineCreateRequest(String name, Long sourceConnectionId, Long targetConnectionId,
            String sourceSchema, String sourceTable, String targetSchema, String targetTable,
            String topicPrefix, Boolean deleteEnabled, String description) {
        this(name, sourceConnectionId, targetConnectionId, sourceSchema, sourceTable, targetSchema, targetTable,
                topicPrefix, "INITIAL", deleteEnabled, description);
    }
}
