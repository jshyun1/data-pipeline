package com.company.pipeline.connector;

/**
 * docs/kafka-webservice-design.md §6.3의 명명 규칙:
 *   source-{pipelineId}-{sourceDbType}-{schema}-{table}
 *   sink-{pipelineId}-{targetDbType}-{schema}-{table}
 *   topic: {topicPrefix}.{sourceSchema}.{sourceTable}
 */
final class ConnectorNaming {

    private ConnectorNaming() {
    }

    static String sourceConnectorName(Long pipelineId, String dbType, String schema, String table) {
        return "source-%d-%s".formatted(pipelineId, slug(dbType, schema, table));
    }

    static String sinkConnectorName(Long pipelineId, String dbType, String schema, String table) {
        return "sink-%d-%s".formatted(pipelineId, slug(dbType, schema, table));
    }

    static String topicName(String topicPrefix, String schema, String table) {
        return "%s.%s.%s".formatted(topicPrefix, schema, table);
    }

    private static String slug(String dbType, String schema, String table) {
        return (dbType + "-" + schema + "-" + table).toLowerCase();
    }

    /** Postgres replication slot/publication 이름 제약(소문자/숫자/밑줄만)에 맞춘 정규화. */
    static String sanitizeForPostgresIdentifier(String value) {
        return value.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }

    /**
     * DebeziumPostgresTemplate이 만드는 slot.name/publication.name과 동일한 값.
     * PostgresReplicationCleanupService가 파이프라인 삭제 시 같은 이름을 재계산해서
     * 지워야 해서 두 곳이 공유하는 단일 공식으로 뽑아뒀다.
     */
    static String postgresSlotAndPublicationName(String connectorName) {
        return "dbz_" + sanitizeForPostgresIdentifier(connectorName);
    }
}
