package com.company.pipeline.pipeline.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;

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
        List<String> excludedColumns,
        List<String> maskedColumns,
        Boolean deleteEnabled,
        String description
) {
    public PipelineCreateRequest(String name, Long sourceConnectionId, Long targetConnectionId,
            String sourceSchema, String sourceTable, String targetSchema, String targetTable,
            String topicPrefix, String snapshotMode, Boolean deleteEnabled, String description) {
        this(name, sourceConnectionId, targetConnectionId, sourceSchema, sourceTable, targetSchema, targetTable,
                topicPrefix, snapshotMode, List.of(), List.of(), deleteEnabled, description);
    }

    public PipelineCreateRequest(String name, Long sourceConnectionId, Long targetConnectionId,
            String sourceSchema, String sourceTable, String targetSchema, String targetTable,
            String topicPrefix, Boolean deleteEnabled, String description) {
        this(name, sourceConnectionId, targetConnectionId, sourceSchema, sourceTable, targetSchema, targetTable,
                topicPrefix, "INITIAL", List.of(), List.of(), deleteEnabled, description);
    }
}
