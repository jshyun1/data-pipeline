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
        String description,
        /** UPSERT(기본) 또는 DELTA_APPEND. {@link com.company.pipeline.pipeline.PipelineLoadMode} */
        String loadMode,
        /** DELTA_APPEND 의 구분컬럼명. 비우면 cdc_op. */
        String deltaOpColumn,
        /** 관리 화면 트리에서 놓일 그룹. 비우면 «그룹 미지정». */
        Long groupId
) {
    public PipelineCreateRequest(String name, Long sourceConnectionId, Long targetConnectionId,
            String sourceSchema, String sourceTable, String targetSchema, String targetTable,
            String topicPrefix, String snapshotMode, Boolean deleteEnabled, String description) {
        this(name, sourceConnectionId, targetConnectionId, sourceSchema, sourceTable, targetSchema, targetTable,
                topicPrefix, snapshotMode, List.of(), List.of(), deleteEnabled, description, null, null, null);
    }

    public PipelineCreateRequest(String name, Long sourceConnectionId, Long targetConnectionId,
            String sourceSchema, String sourceTable, String targetSchema, String targetTable,
            String topicPrefix, Boolean deleteEnabled, String description) {
        this(name, sourceConnectionId, targetConnectionId, sourceSchema, sourceTable, targetSchema, targetTable,
                topicPrefix, "INITIAL", List.of(), List.of(), deleteEnabled, description, null, null, null);
    }
}
