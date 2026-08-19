package com.company.pipeline.pipeline.dto;

import com.company.pipeline.connection.DbType;
import com.company.pipeline.connector.PipelineConnector;
import com.company.pipeline.pipeline.PipelineDefinition;
import com.company.pipeline.pipeline.PipelineStatus;
import java.time.LocalDateTime;
import java.util.List;

public record PipelineResponse(
        Long id,
        String name,
        String pipelineType,
        Long sourceConnectionId,
        Long targetConnectionId,
        DbType sourceDbType,
        DbType targetDbType,
        String sourceSchema,
        String sourceTable,
        String targetSchema,
        String targetTable,
        String topicName,
        PipelineStatus status,
        String snapshotMode,
        String excludedColumns,
        String maskedColumns,
        Boolean deleteEnabled,
        String description,
        List<PipelineConnectorSummary> connectors,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
) {
    public static PipelineResponse from(PipelineDefinition entity, List<PipelineConnector> connectors) {
        return new PipelineResponse(
                entity.getId(),
                entity.getName(),
                entity.getPipelineType(),
                entity.getSourceConnectionId(),
                entity.getTargetConnectionId(),
                entity.getSourceDbType(),
                entity.getTargetDbType(),
                entity.getSourceSchema(),
                entity.getSourceTable(),
                entity.getTargetSchema(),
                entity.getTargetTable(),
                entity.getTopicName(),
                entity.getStatus(),
                entity.getSnapshotMode() != null ? entity.getSnapshotMode() : "INITIAL",
                entity.getExcludedColumns(),
                entity.getMaskedColumns(),
                entity.getDeleteEnabled(),
                entity.getDescription(),
                connectors.stream().map(PipelineConnectorSummary::from).toList(),
                entity.getCreatedAt(),
                entity.getUpdatedAt()
        );
    }
}
