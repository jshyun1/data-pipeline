package com.company.pipeline.pipeline.dto;

import com.company.pipeline.connector.PipelineConnector;

public record PipelineConnectorSummary(
        Long id,
        String connectorRole,
        String connectorName,
        String connectorClass,
        String status,
        String connectorConfigJson,
        String lastStatusJson
) {
    public static PipelineConnectorSummary from(PipelineConnector entity) {
        return new PipelineConnectorSummary(
                entity.getId(),
                entity.getConnectorRole(),
                entity.getConnectorName(),
                entity.getConnectorClass(),
                entity.getStatus(),
                entity.getConnectorConfigJson(),
                entity.getLastStatusJson()
        );
    }
}
