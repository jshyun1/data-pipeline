package com.company.pipeline.connector;

import com.company.pipeline.connection.DbType;

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

    /**
     * Debezium이 실제로 만드는 CDC 이벤트 토픽 이름은 사용자가 화면에 입력한 대소문자가
     * 아니라 소스 DB가 카탈로그에 저장한 실제 대소문자를 따른다 - Oracle은 따옴표 없는
     * 식별자를 항상 대문자로 접기 때문에 항상 대문자, Postgres는 반대로 항상 소문자로
     * 접는다. 여기서 그 규칙을 맞추지 않으면 싱크 커넥터가 소스와 다른(존재하지 않는)
     * 토픽을 구독하게 되어 데이터가 조용히 안 흐른다.
     */
    static String topicName(String topicPrefix, DbType sourceDbType, String schema, String table) {
        if (sourceDbType == DbType.ORACLE) {
            schema = schema.toUpperCase();
            table = table.toUpperCase();
        } else if (sourceDbType == DbType.POSTGRESQL) {
            schema = schema.toLowerCase();
            table = table.toLowerCase();
        }
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
