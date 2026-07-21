package com.company.pipeline.connector.dto;

import com.company.pipeline.connection.DbType;

public record SinkConnectorRequest(
        Long pipelineId,
        DbType targetDbType,
        String hostname,
        int port,
        String username,
        String password,
        String databaseName,
        String serviceName,
        String targetSchema,
        String targetTable,
        String sourceSchema,
        String sourceTable,
        String topicPrefix,
        boolean deleteEnabled
) {
}
