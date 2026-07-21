package com.company.pipeline.connector.dto;

import com.company.pipeline.connection.DbType;

/**
 * ConnectorConfigRenderer 입력. pipeline_connection에서 풀어낸 접속정보 +
 * pipeline_definition에서 온 스키마/테이블/토픽 정보를 합친 것.
 * databaseName은 PostgreSQL(dbname)에서, serviceName은 Oracle(PDB service name)에서 쓰인다.
 */
public record SourceConnectorRequest(
        Long pipelineId,
        DbType sourceDbType,
        String hostname,
        int port,
        String username,
        String password,
        String databaseName,
        String serviceName,
        String schema,
        String table,
        String topicPrefix
) {
}
