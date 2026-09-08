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
        DbType sourceDbType,
        String sourceSchema,
        String sourceTable,
        String topicPrefix,
        boolean deleteEnabled,
        /** UPSERT(기본) 또는 DELTA_APPEND. null 이면 UPSERT. */
        String loadMode,
        /** DELTA_APPEND 구분컬럼명. */
        String deltaOpColumn
) {
    /** 기존 호출부/테스트 호환: 적재 방식 UPSERT. */
    public SinkConnectorRequest(Long pipelineId, DbType targetDbType, String hostname, int port,
            String username, String password, String databaseName, String serviceName,
            String targetSchema, String targetTable, DbType sourceDbType, String sourceSchema,
            String sourceTable, String topicPrefix, boolean deleteEnabled) {
        this(pipelineId, targetDbType, hostname, port, username, password, databaseName, serviceName,
                targetSchema, targetTable, sourceDbType, sourceSchema, sourceTable, topicPrefix, deleteEnabled,
                null, null);
    }
}
