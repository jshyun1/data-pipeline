package com.company.pipeline.connector.dto;

import com.company.pipeline.connection.DbType;

/**
 * SinkConnectorRequest의 로그 파이프라인용 형제 record. CDC 싱크와 달리 소스
 * 스키마/테이블 조합이 아니라 log_pipeline_source.topic_name을 그대로 쓰고,
 * delete는 항상 false(로그는 append-only)라 별도 필드로 안 받는다.
 */
public record LogSinkConnectorRequest(
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
        String topicName
) {
}
